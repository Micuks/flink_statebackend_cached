/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BatchableKeyedFunction;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StatePrefetcher;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runtime local pre-aggregation dispatch (2026-06-25). When {@code
 * state.backend.cachekit.local-preagg.enabled} is true and the buffered batch's head operator wraps
 * a {@link BatchableKeyedFunction} (e.g. our overridden {@code GroupAggFunction}), groups the batch
 * by key and folds each key's records in one state round-trip with one collapsed emit — folding
 * same-key inputs into one state round-trip with one collapsed emit, at the runtime layer on the
 * prefetch buffer, with no planner change.
 *
 * <p>Self-contained in the (already-deployed) buffer via minimal reflection: {@code
 * getUserFunction()} and {@code setCurrentKey(Object)} are public API; only the operator's {@code
 * output} field and the {@code stateKeySelector1} field are reflected. Returns false (fall back to
 * per-record dispatch) for any operator that is not a {@link BatchableKeyedFunction}, or if any
 * reflection step fails.
 */
public final class LocalPreagg {

    public static final boolean ENABLED =
            GlobalConfiguration.loadConfiguration()
                    .getBoolean("state.backend.cachekit.local-preagg.enabled", false);

    private static final ConcurrentHashMap<Class<?>, Field> KEY_SELECTOR_FIELD_CACHE =
            new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Class<?>, Field> OUTPUT_FIELD_CACHE =
            new ConcurrentHashMap<>();
    // Per-operator-instance cached collector wrapping the operator's output.
    private static final ConcurrentHashMap<Integer, TimestampedCollector<Object>> COLLECTOR_CACHE =
            new ConcurrentHashMap<>();
    // Throwaway firing counter: confirms the bundling path actually fires (vs silent fallback).
    private static final AtomicLong DISPATCH_COUNT = new AtomicLong();
    private static final AtomicLong RECORDS_BUNDLED = new AtomicLong();
    private static final AtomicLong GROUPS_EMITTED = new AtomicLong();
    private static final Field NO_FIELD;

    static {
        Field f;
        try {
            f = LocalPreagg.class.getDeclaredField("NO_FIELD");
        } catch (NoSuchFieldException e) {
            f = null;
        }
        NO_FIELD = f;
    }

    private LocalPreagg() {}

    /**
     * Returns whether this operator is a structural candidate for local pre-aggregation.
     *
     * <p>This intentionally stops before the reflective selector/output checks performed by {@link
     * #dispatch}. The prefetch buffer uses it only as a conservative exclusion: once a batchable
     * function may consume the whole batch, issuing speculative state reads while the batch is
     * still filling is more expensive than occasionally forgoing prefetch if dispatch later falls
     * back.
     */
    public static boolean mayHandle(Input<?> headOperator) {
        return ENABLED && hasBatchableTarget(headOperator);
    }

    static boolean hasBatchableTarget(Input<?> headOperator) {
        if (!(headOperator instanceof AbstractStreamOperator)) {
            return false;
        }
        if (headOperator instanceof BatchableKeyedFunction) {
            return true;
        }
        if (!(headOperator instanceof AbstractUdfStreamOperator)) {
            return false;
        }
        try {
            return ((AbstractUdfStreamOperator<?, ?>) headOperator).getUserFunction()
                    instanceof BatchableKeyedFunction;
        } catch (Throwable t) {
            return false;
        }
    }

    /**
     * Attempt to dispatch the batch via key-grouped pre-aggregation.
     *
     * @return true if the batch was fully handled here; false to fall back to per-record dispatch.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static boolean dispatch(
            Input<?> headOperator, StreamRecord<?>[] buf, int n, Counter numRecordsIn) {
        if (!ENABLED || n <= 0 || headOperator == null) {
            return false;
        }
        if (!(headOperator instanceof AbstractStreamOperator)) {
            return false;
        }
        final AbstractStreamOperator<?> op = (AbstractStreamOperator<?>) headOperator;
        final Object batchTarget;
        if (headOperator instanceof BatchableKeyedFunction) {
            batchTarget = headOperator;
        } else if (headOperator instanceof AbstractUdfStreamOperator) {
            try {
                batchTarget = ((AbstractUdfStreamOperator<?, ?>) op).getUserFunction();
            } catch (Throwable t) {
                return false;
            }
        } else {
            return false;
        }
        if (!(batchTarget instanceof BatchableKeyedFunction)) {
            return false;
        }
        final BatchableKeyedFunction batchable = (BatchableKeyedFunction) batchTarget;

        final KeySelector selector = extractStateKeySelector1(op);
        if (selector == null) {
            return false;
        }
        final TimestampedCollector collector = collectorFor(op);
        if (collector == null) {
            return false;
        }

        try {
            // Extract one aligned key/value vector. CacheKit may return stable first-seen group
            // ids from its native runtime; every unsupported/error case retains the existing Java
            // LinkedHashMap grouping path below.
            final ArrayList<Object> recordKeys = new ArrayList<>(n);
            final ArrayList<Object> recordValues = new ArrayList<>(n);
            StreamRecord<?> lastRec = null;
            for (int i = 0; i < n; i++) {
                StreamRecord<?> rec = buf[i];
                if (rec == null) {
                    continue;
                }
                lastRec = rec;
                Object value = rec.getValue();
                Object key = selector.getKey(value);
                recordKeys.add(key);
                recordValues.add(value);
            }
            if (recordKeys.isEmpty()) {
                return false;
            }
            final GroupedInputs groups =
                    groupInputs(recordKeys, recordValues, StatePrefetcher.groupKeysNatively(headOperator, recordKeys));
            // The exact set of state keys is now known and deduplicated. CacheKit can issue one
            // synchronous MultiGet so processBatchForKey observes warm ValueState, without the
            // wasted per-record speculation that used to run before grouping.
            StatePrefetcher.prefetchKeysImmediately(headOperator, groups.keys);
            // Preserve the batch's timestamp context for emitted rows (agg results are not
            // event-time keyed downstream, but keep parity with the per-record path).
            if (lastRec != null && lastRec.hasTimestamp()) {
                collector.setAbsoluteTimestamp(lastRec.getTimestamp());
            } else {
                collector.eraseTimestamp();
            }
            for (int group = 0; group < groups.keys.size(); group++) {
                Object key = groups.keys.get(group);
                op.setCurrentKey(key);
                batchable.processBatchForKey(key, groups.values.get(group), collector);
            }
            if (numRecordsIn != null) {
                numRecordsIn.inc(n);
            }
            long c = DISPATCH_COUNT.incrementAndGet();
            long recs = RECORDS_BUNDLED.addAndGet(n);
            long grps = GROUPS_EMITTED.addAndGet(groups.keys.size());
            if (c % 5000L == 1L) {
                double collapse = grps == 0 ? 0 : (double) recs / grps;
                System.err.println(
                        String.format(
                                "[LOCAL-PREAGG] FIRING op=%s dispatches=%d records=%d groups=%d collapse=%.2fx",
                                op.getClass().getSimpleName(), c, recs, grps, collapse));
            }
            return true;
        } catch (Throwable t) {
            // A mid-batch failure cannot be safely replayed (some keys already processed). Surface
            // it rather than silently double-processing.
            throw new RuntimeException("local-preagg dispatch failed", t);
        }
    }

    static GroupedInputs groupInputs(
            List<Object> recordKeys, List<Object> recordValues, int[] nativePlan) {
        if (nativePlan != null && nativePlan.length == recordKeys.size() + 1) {
            int groupCount = nativePlan[0];
            if (groupCount > 0 && groupCount <= recordKeys.size()) {
                ArrayList<Object> keys = new ArrayList<>(groupCount);
                ArrayList<List<Object>> values = new ArrayList<>(groupCount);
                boolean[] assigned = new boolean[groupCount];
                for (int group = 0; group < groupCount; group++) {
                    keys.add(null);
                    values.add(new ArrayList<>());
                }
                boolean valid = true;
                int nextGroup = 0;
                for (int source = 0; source < recordKeys.size(); source++) {
                    int group = nativePlan[source + 1];
                    if (group < 0 || group >= groupCount) {
                        valid = false;
                        break;
                    }
                    if (!assigned[group]) {
                        // Native group ids are part of the observable first-seen ordering
                        // contract. Reject sparse or permuted ids instead of silently
                        // reordering keys when a corrupt JNI plan is returned.
                        if (group != nextGroup) {
                            valid = false;
                            break;
                        }
                        keys.set(group, recordKeys.get(source));
                        assigned[group] = true;
                        nextGroup++;
                    } else if (!Objects.equals(keys.get(group), recordKeys.get(source))) {
                        valid = false;
                        break;
                    }
                    values.get(group).add(recordValues.get(source));
                }
                for (int group = 0; group < groupCount; group++) {
                    valid &= assigned[group] && !values.get(group).isEmpty();
                }
                if (valid) {
                    return new GroupedInputs(keys, values, true);
                }
            }
        }
        LinkedHashMap<Object, List<Object>> javaGroups = new LinkedHashMap<>();
        for (int source = 0; source < recordKeys.size(); source++) {
            javaGroups.computeIfAbsent(recordKeys.get(source), ignored -> new ArrayList<>())
                    .add(recordValues.get(source));
        }
        return new GroupedInputs(
                new ArrayList<>(javaGroups.keySet()),
                new ArrayList<>(javaGroups.values()),
                false);
    }

    static final class GroupedInputs {
        final List<Object> keys;
        final List<List<Object>> values;
        final boolean nativeGrouped;

        private GroupedInputs(
                List<Object> keys, List<List<Object>> values, boolean nativeGrouped) {
            this.keys = keys;
            this.values = values;
            this.nativeGrouped = nativeGrouped;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static TimestampedCollector collectorFor(AbstractStreamOperator<?> op) {
        int id = System.identityHashCode(op);
        TimestampedCollector cached = COLLECTOR_CACHE.get(id);
        if (cached != null) {
            return cached;
        }
        Field f = OUTPUT_FIELD_CACHE.computeIfAbsent(op.getClass(), LocalPreagg::findOutputField);
        if (f == NO_FIELD || f == null) {
            return null;
        }
        try {
            Output<StreamRecord<Object>> output = (Output<StreamRecord<Object>>) f.get(op);
            if (output == null) {
                return null;
            }
            TimestampedCollector c = new TimestampedCollector(output);
            COLLECTOR_CACHE.put(id, c);
            return c;
        } catch (Throwable t) {
            return null;
        }
    }

    private static Field findOutputField(Class<?> opClass) {
        Class<?> c = opClass;
        while (c != null && c != Object.class) {
            try {
                Field f = c.getDeclaredField("output");
                f.setAccessible(true);
                return f;
            } catch (NoSuchFieldException ignored) {
                c = c.getSuperclass();
            }
        }
        return NO_FIELD;
    }

    private static KeySelector<?, ?> extractStateKeySelector1(AbstractStreamOperator<?> op) {
        Field f =
                KEY_SELECTOR_FIELD_CACHE.computeIfAbsent(
                        op.getClass(), LocalPreagg::findKeySelectorField);
        if (f == NO_FIELD || f == null) {
            return null;
        }
        try {
            return (KeySelector<?, ?>) f.get(op);
        } catch (IllegalAccessException e) {
            return null;
        }
    }

    private static Field findKeySelectorField(Class<?> opClass) {
        Class<?> c = opClass;
        while (c != null && c != Object.class) {
            try {
                Field f = c.getDeclaredField("stateKeySelector1");
                f.setAccessible(true);
                return f;
            } catch (NoSuchFieldException ignored) {
                c = c.getSuperclass();
            }
        }
        return NO_FIELD;
    }
}
