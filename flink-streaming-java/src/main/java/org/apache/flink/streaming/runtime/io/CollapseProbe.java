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
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * THROWAWAY DIAGNOSTIC (2026-06-25). Measures the per-batch "collapse ratio" = records-in /
 * distinct-keys for whatever keyed operator the {@link StreamRecordBatchOutput} buffer wraps. This
 * is the hard upper bound on the emit-reduction achievable by runtime local pre-aggregation (the
 * "prefetch-buffer local pre-agg" direction): a batch of N records with K distinct keys can
 * collapse N emits into K. Enabled only when {@code state.backend.cachekit.collapse-probe.enabled:
 * true}; otherwise observe() is a cheap no-op. Does NOT change correctness — it only counts keys
 * before the normal dispatch.
 *
 * <p>Stats are accumulated per operator instance (identityHashCode) and dumped to stderr (TM log)
 * every {@link #DUMP_EVERY} batches with a running collapse ratio. Run one nexmark query at a time
 * with a large mailbox-batch size to read off that query's collapse curve.
 */
public final class CollapseProbe {

    public static final boolean ENABLED =
            GlobalConfiguration.loadConfiguration()
                    .getBoolean("state.backend.cachekit.collapse-probe.enabled", false);

    private static final long DUMP_EVERY = 5000L; // batches per operator instance between dumps

    private static final ConcurrentHashMap<Integer, Stat> STATS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Class<?>, Field> KEY_SELECTOR_FIELD_CACHE =
            new ConcurrentHashMap<>();
    private static final Field NO_FIELD;

    static {
        Field f;
        try {
            f = CollapseProbe.class.getDeclaredField("NO_FIELD");
        } catch (NoSuchFieldException e) {
            f = null;
        }
        NO_FIELD = f;
    }

    private CollapseProbe() {}

    private static final class Stat {
        final String label;
        long batches;
        long records;
        long distinctSum; // sum over batches of distinct-keys-in-batch
        long maxBatch;

        Stat(String label) {
            this.label = label;
        }
    }

    /** Observe one flushed batch. No-op unless ENABLED. Never throws into the hot path. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void observe(Input<?> headOperator, StreamRecord<?>[] buf, int n) {
        if (!ENABLED || n <= 0 || headOperator == null) {
            return;
        }
        try {
            if (!(headOperator instanceof AbstractStreamOperator)) {
                return;
            }
            AbstractStreamOperator<?> op = (AbstractStreamOperator<?>) headOperator;
            KeySelector selector = extractStateKeySelector1(op);
            if (selector == null) {
                return;
            }
            HashSet<Object> keys = new HashSet<>(Math.max(8, n * 2));
            for (int i = 0; i < n; i++) {
                StreamRecord<?> rec = buf[i];
                if (rec == null) {
                    continue;
                }
                Object key = selector.getKey(rec.getValue());
                if (key != null) {
                    keys.add(key);
                }
            }
            int distinct = keys.size();
            int id = System.identityHashCode(op);
            Stat s = STATS.computeIfAbsent(id, k -> new Stat(labelFor(op)));
            synchronized (s) {
                s.batches++;
                s.records += n;
                s.distinctSum += distinct;
                if (n > s.maxBatch) {
                    s.maxBatch = n;
                }
                if (s.batches % DUMP_EVERY == 0L) {
                    dump(s);
                }
            }
        } catch (Throwable t) {
            // best-effort diagnostic; never disturb the pipeline
        }
    }

    private static void dump(Stat s) {
        double avgBatch = s.batches == 0 ? 0 : (double) s.records / s.batches;
        // collapse = records / distinct-keys (aggregate over batches). >1 means emit-reduction.
        double collapse = s.distinctSum == 0 ? 0 : (double) s.records / s.distinctSum;
        double avgDistinct = s.batches == 0 ? 0 : (double) s.distinctSum / s.batches;
        System.err.println(
                String.format(
                        "[COLLAPSE-PROBE] op=%s batches=%d records=%d avgBatch=%.0f avgDistinct/batch=%.1f "
                                + "maxBatch=%d COLLAPSE=%.2fx",
                        s.label,
                        s.batches,
                        s.records,
                        avgBatch,
                        avgDistinct,
                        s.maxBatch,
                        collapse));
    }

    private static String labelFor(AbstractStreamOperator<?> op) {
        String opName = op.getClass().getSimpleName();
        String fn = "";
        if (op instanceof AbstractUdfStreamOperator) {
            try {
                Object f = ((AbstractUdfStreamOperator<?, ?>) op).getUserFunction();
                if (f != null) {
                    fn = "/" + f.getClass().getSimpleName();
                }
            } catch (Throwable ignored) {
                // ignore
            }
        }
        return opName + fn;
    }

    private static KeySelector<?, ?> extractStateKeySelector1(AbstractStreamOperator<?> op) {
        Field f =
                KEY_SELECTOR_FIELD_CACHE.computeIfAbsent(
                        op.getClass(), CollapseProbe::findKeySelectorField);
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
