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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.CommutativeKeyedOperator;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Same-key run amortization adapter (CacheKit Direction-6 v2).
 *
 * <p>Given a buffered batch of {@link StreamRecord}s about to be dispatched to a keyed {@link
 * OneInputStreamOperator}, this adapter:
 *
 * <ol>
 *   <li>Classifies the operator as commutative-within-key (via marker interface {@link
 *       CommutativeKeyedOperator} OR a hard-coded FQN allowlist of well-known commutative SQL
 *       functions).
 *   <li>If commutative: radix-sorts records by {@code (keyGroup, keyHash)}, walks the sorted runs,
 *       and calls {@code setCurrentKey} <em>once per run</em>. {@code processElement} is then
 *       called for each record in the run with the key context already established — amortising the
 *       per-record state-context cost (visible in profiles as {@code setCurrentKey/Namespace} 1-6%
 *       per record).
 *   <li>If <em>not</em> commutative, or if reflection access to the key selector fails for any
 *       reason (e.g. operator is unkeyed, framework version mismatch, NPE), this adapter falls back
 *       to arrival-order per-record dispatch — never breaks correctness.
 * </ol>
 *
 * <p>This class is stateless and thread-safe in the sense that it carries no instance state; the
 * mailbox-thread invariant of the surrounding {@link StreamRecordBatchOutput} provides the
 * exclusion. A small static reflection cache speeds up repeated key-selector lookups for the same
 * operator class.
 */
public final class BatchedKeyedOperatorAdapter {

    // ------------------------------------------------------------------------
    //  Adaptive sort-skip (v2.1) — tunable thresholds
    // ------------------------------------------------------------------------

    /**
     * Number of leading records of each batch to probe for key-cardinality before deciding whether
     * to sort. Small constant: O(N) probe overhead vs O(B log B) sort overhead, where B is the full
     * batch size (typically 64). N=8 keeps probe < 5% of full sort cost.
     */
    private static final int DEFAULT_PROBE_SIZE = 8;

    /**
     * Default cardinality threshold above which we skip the sort. If {@code distinct/probe >
     * threshold} (i.e., expected average run length is short), the per-key amortization is unlikely
     * to recoup the sort cost; we fall back to v1 arrival-order dispatch. 0.75 means "probe of 8 →
     * if &gt; 6 distinct keys observed, skip sort."
     */
    private static final double DEFAULT_CARDINALITY_THRESHOLD = 0.75;

    /** Active probe size (process-wide, set via {@link #setProbeSize}). */
    private static volatile int probeSize = DEFAULT_PROBE_SIZE;

    /** Active threshold (process-wide, set via {@link #setCardinalityThreshold}). */
    private static volatile double cardinalityThreshold = DEFAULT_CARDINALITY_THRESHOLD;

    /**
     * Process-wide override: if non-negative, forces sort-skip decisions. -1=adaptive, 0=always
     * skip sort (fall back to arrival-order), 1=always sort. Test/diagnostic only.
     */
    private static volatile int forceMode = -1;

    /**
     * Static allowlist of well-known commutative SQL function FQNs (substring match).
     *
     * <p><strong>Order-sensitive functions MUST NOT be added here.</strong> The fast path radix-
     * sorts records by {@code (keyGroup, keyHash)} and uses an unstable indirect quicksort for
     * batches of size {@code > 16} ({@link #quicksort}), so the relative order of records that
     * share the same key is <em>not</em> preserved. Any function whose output depends on which
     * same-key record arrived first/last (e.g. KeepFirstRow / KeepLastRow dedup, LAG/LEAD,
     * FIRST_VALUE/LAST_VALUE, top-N rank with arrival-order tie-break) will produce wrong results
     * on the fast path.
     *
     * <p>Aggregations like SUM / COUNT / MIN / MAX commute under same-key reordering (the
     * accumulator is associative+commutative), so they are safe.
     */
    private static final String[] COMMUTATIVE_FUNCTION_NAME_HINTS = {
        // Aggregations: same-key inserts/retracts commute (sum, count, min, max, avg accumulators
        // are all associative+commutative, so reordering same-key inputs yields the same result).
        "MiniBatchGroupAggFunction",
        "MiniBatchGlobalGroupAggFunction",
        "MiniBatchLocalGroupAggFunction",
        "MiniBatchIncrementalGroupAggFunction",
        "GroupAggFunction",
        // ------------------------------------------------------------------------------------
        // Intentionally NOT in the allowlist (order-sensitive — would produce wrong results
        // under same-key reordering by the unstable quicksort fast path):
        //   - RowTimeDeduplicateFunction / RowTimeMiniBatchDeduplicateFunction
        //     (timestamp tie-break uses arrival order)
        //   - RowTimeMiniBatchLatestChangeDeduplicateFunction (latest-change semantics)
        //   - ProcTimeDeduplicateKeepFirstRowFunction / ProcTimeMiniBatchDeduplicateKeepFirst...
        //     (explicitly keeps the FIRST arrival)
        //   - ProcTimeDeduplicateKeepLastRowFunction / ProcTimeMiniBatchDeduplicateKeepLast...
        //     (explicitly keeps the LAST arrival)
        //   - LAG / LEAD / FIRST_VALUE / LAST_VALUE / arrival-ordered ROW_NUMBER
        // If you ever want to amortize these, you must (a) use a stable sort (boxing into Object[]
        // + Arrays.sort, or merge-sort), or (b) keep arrival-order dispatch but reuse the per-key
        // state-context cache (the v1 path).
        // ------------------------------------------------------------------------------------
    };

    /**
     * FQN substrings that opt the operator class itself into commutativity.
     *
     * <p>Empty by default. Historically this list contained {@code KeyedMapBundleOperator}, but
     * that operator wraps arbitrary {@code MapBundleFunction}s — including order-sensitive dedup
     * bundles ({@code MiniBatchDeduplicateKeepFirstRowFunction} et al.) — so it is unsafe to
     * allowlist at the operator level. Bundles that wrap a known-commutative function (e.g. {@code
     * MiniBatchGroupAggFunction}) are still classified correctly by the function-level check below
     * in {@link #classify(Object, Class)} via {@link AbstractUdfStreamOperator}.
     */
    private static final String[] COMMUTATIVE_OPERATOR_NAME_HINTS = {
        // (intentionally empty — see Javadoc above)
    };

    /** Cache of {@code stateKeySelector1} {@link Field} per operator class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Field>
            KEY_SELECTOR_FIELD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of commutativity-classification results per operator class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Boolean>
            COMMUTATIVE_CLASS_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Sentinel field used to mark "no stateKeySelector1 available" in the field cache. */
    private static final Field NO_FIELD;

    static {
        try {
            NO_FIELD = BatchedKeyedOperatorAdapter.class.getDeclaredField("NO_FIELD");
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private BatchedKeyedOperatorAdapter() {}

    /**
     * Dispatch a buffered batch to the head operator using same-key run amortization where safe.
     *
     * @param headOp the head operator (typically the chain's main operator).
     * @param buf record buffer; only entries {@code [0, count)} are valid.
     * @param count number of valid records in {@code buf}.
     * @param numRecordsIn metric counter to increment per dispatched record (non-null).
     * @param <T> record value type.
     */
    public static <T> void dispatchSorted(
            Input<T> headOp, StreamRecord<T>[] buf, int count, Counter numRecordsIn)
            throws Exception {
        if (count <= 0) {
            return;
        }
        if (count == 1) {
            // Trivial: single-record batches don't need sorting.
            if (numRecordsIn != null) {
                numRecordsIn.inc();
            }
            headOp.setKeyContextElement(buf[0]);
            headOp.processElement(buf[0]);
            return;
        }

        // Try the fast path. If anything goes wrong (reflection, classification, key extraction),
        // fall back to per-record dispatch — correctness must never depend on the adapter.
        boolean fastPathHandled = false;
        if (headOp instanceof AbstractStreamOperator) {
            AbstractStreamOperator<?> op = (AbstractStreamOperator<?>) headOp;
            if (isCommutative(op)) {
                @SuppressWarnings("unchecked")
                KeySelector<T, Object> selector =
                        (KeySelector<T, Object>) extractStateKeySelector1(op);
                if (selector != null) {
                    int maxParallelism = resolveMaxParallelism(op);
                    if (maxParallelism > 0) {
                        try {
                            dispatchSortedFast(
                                    op, selector, maxParallelism, buf, count, numRecordsIn);
                            fastPathHandled = true;
                        } catch (Throwable t) {
                            // Hard-fail safe: if the fast path threw mid-flight (e.g. selector NPE
                            // on a particular record, or downstream side-effect), we MUST NOT
                            // re-dispatch records 0..i-1 from the buffer; the operator has already
                            // observed them. Surface the error to the mailbox.
                            if (t instanceof Exception) {
                                throw (Exception) t;
                            }
                            throw new RuntimeException(t);
                        }
                    }
                }
            }
        }

        if (!fastPathHandled) {
            for (int i = 0; i < count; i++) {
                if (numRecordsIn != null) {
                    numRecordsIn.inc();
                }
                headOp.setKeyContextElement(buf[i]);
                headOp.processElement(buf[i]);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Fast path
    // ------------------------------------------------------------------------

    private static <T> void dispatchSortedFast(
            AbstractStreamOperator<?> op,
            KeySelector<T, Object> selector,
            int maxParallelism,
            StreamRecord<T>[] buf,
            int count,
            Counter numRecordsIn)
            throws Exception {
        // Compute (keyGroup, keyHash, key) for each record.
        Object[] keys = new Object[count];
        long[] sortKeys = new long[count];
        int[] indices = new int[count];
        for (int i = 0; i < count; i++) {
            T value = buf[i].getValue();
            Object key;
            try {
                key = selector.getKey(value);
            } catch (Exception e) {
                // A null/unkeyed record breaks our assumption: bail to fallback.
                throw new BatchedKeyedFallbackException(e);
            }
            if (key == null) {
                throw new BatchedKeyedFallbackException(new NullPointerException("key was null"));
            }
            int kg = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
            int kh = key.hashCode();
            // Pack: high 32 bits = keyGroup (signed but always >=0), low 32 bits = key hash.
            sortKeys[i] = (((long) kg) << 32) | (kh & 0xFFFFFFFFL);
            keys[i] = key;
            indices[i] = i;
        }

        // v2.1 ADAPTIVE SORT-SKIP. Probe the first N records: if the observed cardinality is
        // too high (most keys distinct), expected run length is < 1.33 → sort cost dominates
        // amortization. Fall back to arrival-order dispatch with per-record setCurrentKey
        // (still cheaper than the v1 setKeyContextElement reflection-flavoured path because we
        // skip the whole sort/index allocation work *below*).
        if (shouldSkipSortAdaptive(sortKeys, count)) {
            ADAPTIVE_SKIPS.increment();
            for (int i = 0; i < count; i++) {
                if (numRecordsIn != null) {
                    numRecordsIn.inc();
                }
                op.setCurrentKey(keys[i]);
                ((Input<T>) op).processElement(buf[i]);
            }
            return;
        }
        ADAPTIVE_SORTS.increment();

        // Sort indices by sortKeys (insertion sort fine for small batches; quicksort over indices
        // for larger). Most batches at the configured size of 64 are small enough that an O(n^2)
        // insertion sort beats a heavier algorithm in practice, but we use a simple comparator-
        // less indirect sort to be safe at size=256.
        sortIndicesBySortKeys(indices, sortKeys, 0, count);

        // Walk runs: a run is a maximal consecutive range with the same key. Within a run we set
        // the key context exactly once.
        int i = 0;
        while (i < count) {
            // Find run end (exclusive). Records with same (keyGroup, keyHash, key) form a run.
            int runStart = i;
            int runStartIdx = indices[runStart];
            Object runKey = keys[runStartIdx];
            long runSortKey = sortKeys[runStartIdx];
            int j = i + 1;
            while (j < count) {
                int idx = indices[j];
                // Cheap pre-filter: identical sort key (most common case).
                if (sortKeys[idx] != runSortKey) {
                    break;
                }
                // Confirm: hash collisions across different keys would otherwise corrupt the run.
                if (!Objects.equals(keys[idx], runKey)) {
                    break;
                }
                j++;
            }
            // Establish key context once for the run.
            op.setCurrentKey(runKey);
            // Dispatch each record in the run.
            for (int r = runStart; r < j; r++) {
                int idx = indices[r];
                if (numRecordsIn != null) {
                    numRecordsIn.inc();
                }
                ((Input<T>) op).processElement(buf[idx]);
            }
            i = j;
        }
    }

    // ------------------------------------------------------------------------
    //  Adaptive cardinality probe (v2.1)
    // ------------------------------------------------------------------------

    /**
     * Decide whether to skip sort for this batch, based on observed cardinality of the first {@link
     * #probeSize} records. Returns {@code true} if sort should be SKIPPED.
     *
     * <p>Algorithm: copy the first N sort-keys into a small fixed-size array (allocation-free in
     * practice — JVM escape analysis stack-allocates an int[N]), sort it, then count adjacent
     * distinct values. If {@code distinct/N > threshold}, we predict the full batch will produce
     * mostly singleton runs and the sort+walk overhead exceeds the savings from
     * one-setCurrentKey-per-run.
     *
     * <p>Cost: 8 long copies, 8-element insertion sort (~50ns), 8 comparisons. Well under the
     * ~1500ns cost of full-batch quicksort.
     */
    private static boolean shouldSkipSortAdaptive(long[] sortKeys, int count) {
        // Forced override (test/diagnostic).
        int fm = forceMode;
        if (fm == 0) {
            return true;
        }
        if (fm == 1) {
            return false;
        }
        // Adaptive default.
        int probe = Math.min(probeSize, count);
        if (probe < 4) {
            // For very small batches, the sort itself is essentially free — never skip.
            return false;
        }
        // Tiny on-stack scratch.
        long[] scratch = new long[probe];
        for (int i = 0; i < probe; i++) {
            scratch[i] = sortKeys[i];
        }
        // Insertion sort the probe.
        for (int i = 1; i < probe; i++) {
            long v = scratch[i];
            int j = i - 1;
            while (j >= 0 && scratch[j] > v) {
                scratch[j + 1] = scratch[j];
                j--;
            }
            scratch[j + 1] = v;
        }
        // Count distinct values (adjacent compare on sorted array).
        int distinct = 1;
        for (int i = 1; i < probe; i++) {
            if (scratch[i] != scratch[i - 1]) {
                distinct++;
            }
        }
        // If distinct count strictly exceeds threshold * probe → skip sort.
        // E.g. probe=8, threshold=0.75 → skip if distinct > 6 (i.e. >= 7).
        return distinct > probe * cardinalityThreshold;
    }

    /** Process-wide counter: number of batches where sort was applied. */
    private static final java.util.concurrent.atomic.LongAdder ADAPTIVE_SORTS =
            new java.util.concurrent.atomic.LongAdder();

    /** Process-wide counter: number of batches where sort was skipped (high cardinality). */
    private static final java.util.concurrent.atomic.LongAdder ADAPTIVE_SKIPS =
            new java.util.concurrent.atomic.LongAdder();

    /** Configure the probe size at runtime. Bounded to {@code [1, 64]}. */
    public static void setProbeSize(int size) {
        if (size < 1) {
            size = 1;
        }
        if (size > 64) {
            size = 64;
        }
        probeSize = size;
    }

    /** Configure the cardinality threshold at runtime. Bounded to {@code (0.0, 1.0]}. */
    public static void setCardinalityThreshold(double t) {
        if (t <= 0.0) {
            t = 0.01;
        }
        if (t > 1.0) {
            t = 1.0;
        }
        cardinalityThreshold = t;
    }

    /** Test hook: -1 adaptive (default), 0 always-skip, 1 always-sort. */
    static void setForceModeForTesting(int mode) {
        forceMode = mode;
    }

    /** Visible for tests / diagnostics. */
    public static long getAdaptiveSortCount() {
        return ADAPTIVE_SORTS.sum();
    }

    /** Visible for tests / diagnostics. */
    public static long getAdaptiveSkipCount() {
        return ADAPTIVE_SKIPS.sum();
    }

    /** Visible for tests: reset counters. */
    static void resetAdaptiveCountersForTesting() {
        ADAPTIVE_SORTS.reset();
        ADAPTIVE_SKIPS.reset();
    }

    // ------------------------------------------------------------------------
    //  Sorting (radix-style by 64-bit composite sort key, indirect)
    // ------------------------------------------------------------------------

    /**
     * Sort {@code indices[lo..hi)} so that {@code sortKeys[indices[k]]} is non-decreasing.
     *
     * <p>For the typical batch sizes (16-256) we use an in-place LSD-radix on the lower 32 bits
     * (key hash) followed by a stable insertion-sort pass on the upper 32 bits (keyGroup). This is
     * faster than a comparison-based sort for typical batch sizes since most key hashes are spread
     * uniformly while keyGroups are concentrated in a few buckets.
     *
     * <p>For very small batches ({@code <= 16}) we just use insertion sort over the composite
     * 64-bit sort key.
     */
    private static void sortIndicesBySortKeys(int[] indices, long[] sortKeys, int lo, int hi) {
        int n = hi - lo;
        if (n <= 1) {
            return;
        }
        if (n <= 16) {
            insertionSort(indices, sortKeys, lo, hi);
            return;
        }
        // Indirect quicksort over the composite key. Simpler than radix and adequate up to 4096.
        quicksort(indices, sortKeys, lo, hi - 1);
    }

    private static void insertionSort(int[] indices, long[] sortKeys, int lo, int hi) {
        for (int i = lo + 1; i < hi; i++) {
            int idx = indices[i];
            long key = sortKeys[idx];
            int j = i - 1;
            while (j >= lo && sortKeys[indices[j]] > key) {
                indices[j + 1] = indices[j];
                j--;
            }
            indices[j + 1] = idx;
        }
    }

    private static void quicksort(int[] indices, long[] sortKeys, int lo, int hi) {
        if (lo >= hi) {
            return;
        }
        // Median-of-three pivot.
        int mid = lo + (hi - lo) / 2;
        long a = sortKeys[indices[lo]];
        long b = sortKeys[indices[mid]];
        long c = sortKeys[indices[hi]];
        long pivot;
        if (a <= b && b <= c) {
            pivot = b;
        } else if (c <= b && b <= a) {
            pivot = b;
        } else if (b <= a && a <= c) {
            pivot = a;
        } else if (c <= a && a <= b) {
            pivot = a;
        } else {
            pivot = c;
        }
        int i = lo, j = hi;
        while (i <= j) {
            while (sortKeys[indices[i]] < pivot) {
                i++;
            }
            while (sortKeys[indices[j]] > pivot) {
                j--;
            }
            if (i <= j) {
                int tmp = indices[i];
                indices[i] = indices[j];
                indices[j] = tmp;
                i++;
                j--;
            }
        }
        if (lo < j) {
            quicksort(indices, sortKeys, lo, j);
        }
        if (i < hi) {
            quicksort(indices, sortKeys, i, hi);
        }
    }

    // ------------------------------------------------------------------------
    //  Classification
    // ------------------------------------------------------------------------

    /** @return true if the operator is safe to dispatch with same-key amortization. */
    public static boolean isCommutative(Object op) {
        if (op == null) {
            return false;
        }
        if (op instanceof CommutativeKeyedOperator) {
            return true;
        }
        return COMMUTATIVE_CLASS_CACHE.computeIfAbsent(op.getClass(), c -> classify(op, c));
    }

    private static boolean classify(Object op, Class<?> opClass) {
        String opFqn = opClass.getName();
        for (String hint : COMMUTATIVE_OPERATOR_NAME_HINTS) {
            if (opFqn.contains(hint)) {
                return true;
            }
        }
        // For UDF-bearing operators, look at the user function class.
        if (op instanceof AbstractUdfStreamOperator) {
            try {
                Object fn = ((AbstractUdfStreamOperator<?, ?>) op).getUserFunction();
                if (fn != null) {
                    String fnFqn = fn.getClass().getName();
                    for (String hint : COMMUTATIVE_FUNCTION_NAME_HINTS) {
                        if (fnFqn.contains(hint)) {
                            return true;
                        }
                    }
                }
            } catch (Throwable ignored) {
                // Fall through.
            }
        }
        return false;
    }

    // ------------------------------------------------------------------------
    //  KeySelector access
    // ------------------------------------------------------------------------

    private static KeySelector<?, ?> extractStateKeySelector1(AbstractStreamOperator<?> op) {
        Field f =
                KEY_SELECTOR_FIELD_CACHE.computeIfAbsent(
                        op.getClass(), BatchedKeyedOperatorAdapter::findKeySelectorField);
        if (f == NO_FIELD) {
            return null;
        }
        try {
            Object v = f.get(op);
            return (KeySelector<?, ?>) v;
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

    // ------------------------------------------------------------------------
    //  Max parallelism
    // ------------------------------------------------------------------------

    private static int resolveMaxParallelism(AbstractStreamOperator<?> op) {
        try {
            return op.getRuntimeContext().getMaxNumberOfParallelSubtasks();
        } catch (Throwable ignored) {
            return -1;
        }
    }

    /** Internal sentinel exception used to signal the caller to take the fallback path. */
    private static final class BatchedKeyedFallbackException extends Exception {
        private static final long serialVersionUID = 1L;

        BatchedKeyedFallbackException(Throwable cause) {
            super(cause);
        }
    }

    // ------------------------------------------------------------------------
    //  Diagnostics for tests
    // ------------------------------------------------------------------------

    /** Visible for tests: clear all caches. */
    static void clearCachesForTesting() {
        KEY_SELECTOR_FIELD_CACHE.clear();
        COMMUTATIVE_CLASS_CACHE.clear();
    }

    /** Visible for tests: record set of known commutative function name hints. */
    static Set<String> commutativeHintsForTesting() {
        Set<String> s = new HashSet<>();
        for (String h : COMMUTATIVE_FUNCTION_NAME_HINTS) {
            s.add(h);
        }
        for (String h : COMMUTATIVE_OPERATOR_NAME_HINTS) {
            s.add(h);
        }
        return s;
    }

    /**
     * Visible for tests: the same sorted-dispatch algorithm as {@link #dispatchSortedFast} but
     * accepting an explicit selector + max-parallelism + setCurrentKey callback so tests can
     * exercise the run-walking logic without needing a real {@code AbstractStreamOperator}.
     */
    public static <T> void dispatchSortedForTesting(
            Input<T> headOp,
            KeySelector<T, Object> selector,
            int maxParallelism,
            java.util.function.Consumer<Object> setCurrentKey,
            StreamRecord<T>[] buf,
            int count,
            Counter numRecordsIn)
            throws Exception {
        if (count <= 0) {
            return;
        }
        Object[] keys = new Object[count];
        long[] sortKeys = new long[count];
        int[] indices = new int[count];
        for (int i = 0; i < count; i++) {
            T value = buf[i].getValue();
            Object key = selector.getKey(value);
            int kg = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
            int kh = key.hashCode();
            sortKeys[i] = (((long) kg) << 32) | (kh & 0xFFFFFFFFL);
            keys[i] = key;
            indices[i] = i;
        }
        // v2.1 adaptive sort-skip path.
        if (shouldSkipSortAdaptive(sortKeys, count)) {
            ADAPTIVE_SKIPS.increment();
            for (int i = 0; i < count; i++) {
                if (numRecordsIn != null) {
                    numRecordsIn.inc();
                }
                setCurrentKey.accept(keys[i]);
                headOp.processElement(buf[i]);
            }
            return;
        }
        ADAPTIVE_SORTS.increment();
        sortIndicesBySortKeys(indices, sortKeys, 0, count);
        int i = 0;
        while (i < count) {
            int runStart = i;
            int runStartIdx = indices[runStart];
            Object runKey = keys[runStartIdx];
            long runSortKey = sortKeys[runStartIdx];
            int j = i + 1;
            while (j < count) {
                int idx = indices[j];
                if (sortKeys[idx] != runSortKey) {
                    break;
                }
                if (!Objects.equals(keys[idx], runKey)) {
                    break;
                }
                j++;
            }
            setCurrentKey.accept(runKey);
            for (int r = runStart; r < j; r++) {
                int idx = indices[r];
                if (numRecordsIn != null) {
                    numRecordsIn.inc();
                }
                headOp.processElement(buf[idx]);
            }
            i = j;
        }
    }
}
