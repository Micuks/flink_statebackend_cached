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
import org.apache.flink.runtime.state.BatchKeyGroupingSupport;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BatchableKeyedFunction;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StatePrefetcher;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;
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

    // Only the experimental native treatment reuses the extraction vectors. Keeping the Java
    // FullOpt/control path unchanged makes the ARM screen attributable and avoids retaining
    // record references when native grouping is disabled.
    private static final boolean NATIVE_EXTRACTION_REUSE_ENABLED =
            GlobalConfiguration.loadConfiguration()
                    .getBoolean("state.backend.cachekit.native.local-preagg.enabled", false);
    private static final ThreadLocal<ExtractionBuffers> NATIVE_EXTRACTION_BUFFERS =
            ThreadLocal.withInitial(ExtractionBuffers::new);
    private static final ThreadLocal<NativeGroupingWorkspace> NATIVE_GROUPING_WORKSPACE =
            ThreadLocal.withInitial(NativeGroupingWorkspace::new);

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

        final ExtractionBuffers extractionBuffers =
                NATIVE_EXTRACTION_REUSE_ENABLED
                        ? NATIVE_EXTRACTION_BUFFERS.get()
                        : new ExtractionBuffers();
        final ArrayList<Object> recordKeys = extractionBuffers.keys;
        final ArrayList<Object> recordValues = extractionBuffers.values;
        final NativeGroupingWorkspace groupingWorkspace =
                NATIVE_EXTRACTION_REUSE_ENABLED ? NATIVE_GROUPING_WORKSPACE.get() : null;
        if (groupingWorkspace != null) {
            groupingWorkspace.prepare(n);
        }
        recordKeys.clear();
        recordValues.clear();
        recordKeys.ensureCapacity(n);
        recordValues.ensureCapacity(n);
        try {
            // Extract one aligned key/value vector. CacheKit may return stable first-seen group
            // ids from its native runtime; every unsupported/error case retains the existing Java
            // LinkedHashMap grouping path below.
            StreamRecord<?> lastRec = null;
            for (int i = 0; i < n; i++) {
                StreamRecord<?> rec = buf[i];
                if (rec == null) {
                    continue;
                }
                lastRec = rec;
                Object value = rec.getValue();
                Object key = selector.getKey(value);
                if (groupingWorkspace != null) {
                    groupingWorkspace.putToken(recordKeys.size(), Objects.hashCode(key));
                }
                recordKeys.add(key);
                recordValues.add(value);
            }
            if (recordKeys.isEmpty()) {
                return false;
            }
            GroupedInputs groups = null;
            if (groupingWorkspace != null) {
                int groupCount =
                        StatePrefetcher.groupHashTokensNatively(
                                headOperator,
                                groupingWorkspace.tokens,
                                recordKeys.size(),
                                groupingWorkspace.plan);
                if (groupCount > 0) {
                    groups =
                            groupInputsPacked(
                                    recordKeys, recordValues, groupingWorkspace, groupCount);
                }
            }
            if (groups == null) {
                groups = groupInputs(recordKeys, recordValues, null);
            }
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
        } finally {
            // A task thread may live for hours. Clear references after every dispatch so the
            // reusable arrays do not pin records or their backing byte segments.
            recordKeys.clear();
            recordValues.clear();
        }
    }

    private static final class ExtractionBuffers {
        private final ArrayList<Object> keys = new ArrayList<>();
        private final ArrayList<Object> values = new ArrayList<>();
    }

    static final class NativeGroupingWorkspace {
        private ByteBuffer tokens = directBuffer(Integer.BYTES);
        private ByteBuffer plan = directBuffer(BatchKeyGroupingSupport.requiredPackedPlanBytes(1));
        private int[] positions = new int[1];

        void prepare(int sourceCapacity) {
            if (sourceCapacity <= 0) {
                return;
            }
            int tokenBytes = Math.multiplyExact(sourceCapacity, Integer.BYTES);
            int planBytes = BatchKeyGroupingSupport.requiredPackedPlanBytes(sourceCapacity);
            if (tokens.capacity() < tokenBytes) {
                tokens = directBuffer(grownCapacity(tokens.capacity(), tokenBytes));
            }
            if (plan.capacity() < planBytes) {
                plan = directBuffer(grownCapacity(plan.capacity(), planBytes));
            }
            if (positions.length < sourceCapacity) {
                positions = new int[grownCapacity(positions.length, sourceCapacity)];
            }
            tokens.clear();
            plan.clear();
            plan.putInt(0, 0);
        }

        private void putToken(int source, int token) {
            tokens.putInt(source * Integer.BYTES, token);
        }

        ByteBuffer planBuffer() {
            return plan;
        }

        private static ByteBuffer directBuffer(int bytes) {
            return ByteBuffer.allocateDirect(bytes).order(ByteOrder.nativeOrder());
        }

        private static int grownCapacity(int current, int required) {
            int capacity = Math.max(1, current);
            while (capacity < required) {
                if (capacity > Integer.MAX_VALUE / 2) {
                    return required;
                }
                capacity *= 2;
            }
            return capacity;
        }
    }

    /**
     * Validates a native packed plan and scatters values once.
     *
     * <p>No record is processed before this method returns. Any malformed layout, unstable group
     * order, or unequal-key hash collision therefore falls back for the whole batch.
     */
    static GroupedInputs groupInputsPacked(
            List<Object> recordKeys,
            List<Object> recordValues,
            NativeGroupingWorkspace workspace,
            int returnedGroupCount) {
        if (recordKeys.size() != recordValues.size() || recordKeys.isEmpty()) {
            return null;
        }
        final ByteBuffer plan = workspace.plan;
        final int sourceCount = recordKeys.size();
        if (plan.getInt(0) != BatchKeyGroupingSupport.PACKED_PLAN_MAGIC
                || plan.getInt(Integer.BYTES) != BatchKeyGroupingSupport.PACKED_PLAN_VERSION
                || plan.getInt(2 * Integer.BYTES) != sourceCount
                || plan.getInt(3 * Integer.BYTES) != returnedGroupCount) {
            return null;
        }
        final int groupCount = returnedGroupCount;
        if (groupCount <= 0 || groupCount > sourceCount) {
            return null;
        }
        final int firstSourceBase = BatchKeyGroupingSupport.PACKED_PLAN_HEADER_BYTES;
        final int offsetsBase = firstSourceBase + groupCount * Integer.BYTES;
        final int sourceGroupBase = offsetsBase + (groupCount + 1) * Integer.BYTES;
        final int requiredBytes = sourceGroupBase + sourceCount * Integer.BYTES;
        if (requiredBytes > plan.capacity()) {
            return null;
        }

        ArrayList<Object> keys = new ArrayList<>(groupCount);
        for (int group = 0; group < groupCount; group++) {
            keys.add(null);
            int firstSource = plan.getInt(firstSourceBase + group * Integer.BYTES);
            if (firstSource < 0
                    || firstSource >= sourceCount
                    || (group == 0 && firstSource != 0)
                    || (group > 0
                            && firstSource
                                    <= plan.getInt(
                                            firstSourceBase + (group - 1) * Integer.BYTES))) {
                return null;
            }
        }
        if (plan.getInt(offsetsBase) != 0
                || plan.getInt(offsetsBase + groupCount * Integer.BYTES) != sourceCount) {
            return null;
        }
        for (int group = 0; group < groupCount; group++) {
            int begin = plan.getInt(offsetsBase + group * Integer.BYTES);
            int end = plan.getInt(offsetsBase + (group + 1) * Integer.BYTES);
            if (begin < 0 || begin >= end || end > sourceCount) {
                return null;
            }
            workspace.positions[group] = begin;
        }

        Object[] flatValues = new Object[sourceCount];
        int nextGroup = 0;
        for (int source = 0; source < sourceCount; source++) {
            int group = plan.getInt(sourceGroupBase + source * Integer.BYTES);
            if (group < 0 || group >= groupCount) {
                return null;
            }
            int firstSource = plan.getInt(firstSourceBase + group * Integer.BYTES);
            Object key = recordKeys.get(source);
            if (source == firstSource) {
                if (group != nextGroup) {
                    return null;
                }
                keys.set(group, key);
                nextGroup++;
            } else if (source < firstSource
                    || group >= nextGroup
                    || !Objects.equals(keys.get(group), key)) {
                return null;
            }
            int position = workspace.positions[group]++;
            int groupEnd = plan.getInt(offsetsBase + (group + 1) * Integer.BYTES);
            if (position >= groupEnd) {
                return null;
            }
            flatValues[position] = recordValues.get(source);
        }
        if (nextGroup != groupCount) {
            return null;
        }
        for (int group = 0; group < groupCount; group++) {
            if (workspace.positions[group]
                    != plan.getInt(offsetsBase + (group + 1) * Integer.BYTES)) {
                return null;
            }
        }

        ArrayList<List<Object>> values = new ArrayList<>(groupCount);
        for (int group = 0; group < groupCount; group++) {
            values.add(
                    new MutableArraySliceList(
                            flatValues,
                            plan.getInt(offsetsBase + group * Integer.BYTES),
                            plan.getInt(offsetsBase + (group + 1) * Integer.BYTES)));
        }
        return new GroupedInputs(keys, values, true);
    }

    static GroupedInputs groupInputs(
            List<Object> recordKeys, List<Object> recordValues, int[] nativePlan) {
        if (recordKeys.size() != recordValues.size()) {
            throw new IllegalArgumentException("record key/value vectors must have equal length");
        }
        if (nativePlan != null && nativePlan.length == recordKeys.size() + 1) {
            int groupCount = nativePlan[0];
            if (groupCount > 0 && groupCount <= recordKeys.size()) {
                ArrayList<Object> keys = new ArrayList<>(groupCount);
                boolean[] assigned = new boolean[groupCount];
                int[] counts = new int[groupCount];
                for (int group = 0; group < groupCount; group++) {
                    keys.add(null);
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
                    counts[group]++;
                }
                for (int group = 0; group < groupCount; group++) {
                    valid &= assigned[group] && counts[group] > 0;
                }
                if (valid) {
                    // Scatter values once into one dense array. The former implementation built
                    // one independently growing ArrayList per group and copied each record into
                    // it. On q15 this allocation/copy phase sits directly before the accumulator
                    // fold and amplified Kunpeng CPU despite the native grouping kernel. Fixed
                    // array slices retain first-seen group order and per-group arrival order while
                    // eliminating the nested backing arrays and their growth copies.
                    int[] offsets = new int[groupCount + 1];
                    for (int group = 0; group < groupCount; group++) {
                        offsets[group + 1] = offsets[group] + counts[group];
                    }
                    int[] positions = offsets.clone();
                    Object[] flatValues = new Object[recordValues.size()];
                    for (int source = 0; source < recordValues.size(); source++) {
                        int group = nativePlan[source + 1];
                        flatValues[positions[group]++] = recordValues.get(source);
                    }
                    ArrayList<List<Object>> values = new ArrayList<>(groupCount);
                    for (int group = 0; group < groupCount; group++) {
                        values.add(
                                new MutableArraySliceList(
                                        flatValues, offsets[group], offsets[group + 1]));
                    }
                    return new GroupedInputs(keys, values, true);
                }
            }
        }
        LinkedHashMap<Object, List<Object>> javaGroups = new LinkedHashMap<>();
        for (int source = 0; source < recordKeys.size(); source++) {
            javaGroups
                    .computeIfAbsent(recordKeys.get(source), ignored -> new ArrayList<>())
                    .add(recordValues.get(source));
        }
        return new GroupedInputs(
                new ArrayList<>(javaGroups.keySet()), new ArrayList<>(javaGroups.values()), false);
    }

    /**
     * Fixed-capacity mutable view over one group's region in the dense value array.
     *
     * <p>{@link org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction} removes
     * leading retract records through {@link java.util.Iterator#remove()}, so the slice cannot be
     * immutable. Removal shifts only inside this group's non-overlapping region and does not
     * allocate another backing array.
     */
    static final class MutableArraySliceList extends AbstractList<Object> implements RandomAccess {
        private final Object[] values;
        private final int start;
        private int size;

        private MutableArraySliceList(Object[] values, int start, int end) {
            this.values = values;
            this.start = start;
            this.size = end - start;
        }

        @Override
        public Object get(int index) {
            checkElementIndex(index);
            return values[start + index];
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public Object set(int index, Object element) {
            checkElementIndex(index);
            int absoluteIndex = start + index;
            Object previous = values[absoluteIndex];
            values[absoluteIndex] = element;
            return previous;
        }

        @Override
        public Object remove(int index) {
            checkElementIndex(index);
            int absoluteIndex = start + index;
            Object previous = values[absoluteIndex];
            int moved = size - index - 1;
            if (moved > 0) {
                System.arraycopy(values, absoluteIndex + 1, values, absoluteIndex, moved);
            }
            values[start + --size] = null;
            modCount++;
            return previous;
        }

        private void checkElementIndex(int index) {
            if (index < 0 || index >= size) {
                throw new IndexOutOfBoundsException("index=" + index + ", size=" + size);
            }
        }
    }

    static final class GroupedInputs {
        final List<Object> keys;
        final List<List<Object>> values;
        final boolean nativeGrouped;

        private GroupedInputs(List<Object> keys, List<List<Object>> values, boolean nativeGrouped) {
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
