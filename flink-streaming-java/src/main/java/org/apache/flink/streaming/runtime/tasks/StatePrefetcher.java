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
import org.apache.flink.runtime.state.BatchKeyGroupingSupport;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Backpressure-driven state prefetch (key extraction + submission side).
 *
 * <p>Given a lookahead buffer of upcoming {@link StreamRecord}s — accumulated by {@link
 * org.apache.flink.streaming.runtime.io.StreamRecordBatchOutput} in prefetch mode — this helper
 * extracts each record's keyed-state key (via the head operator's {@code stateKeySelector1}, reused
 * from the {@link BatchedKeyedOperatorAdapter} reflection pattern), dedups it, and hands the key
 * set to the keyed-state backend through an optional {@code prefetch(Collection)} method. Whether
 * the backend fetches synchronously or on a dedicated worker thread is the backend's choice; the
 * CacheKit backend enqueues the reads to a shared off-mailbox worker so they overlap with record
 * dispatch and backpressure waits. The batch is then dispatched <em>in arrival order</em> (no
 * reorder) by the caller.
 *
 * <p>Correctness gates (all enforced):
 *
 * <ul>
 *   <li><b>No reorder</b> — this class only warms the cache; record dispatch order is the caller's
 *       unchanged arrival-order replay.
 *   <li><b>Stale-safe</b> — key extraction and task submission run on the mailbox thread; CacheKit
 *       performs RocksDB reads on its worker and only publishes speculative staging entries. The
 *       ValueState wrapper checks the captured write generation before promotion and falls back to
 *       the authoritative read after any intervening mutation.
 *   <li><b>Best-effort</b> — every path is wrapped in try/catch; a failed prefetch never touches
 *       the authoritative read path or the {@code emitRecord} dispatch.
 * </ul>
 *
 * <p>This class is stateless; the mailbox-thread invariant of the surrounding output provides the
 * exclusion. A small static reflection cache mirrors {@link BatchedKeyedOperatorAdapter}.
 */
public final class StatePrefetcher {

    private static final AtomicLong PREFETCH_ATTEMPTS = new AtomicLong();
    private static final AtomicLong INVALID_INPUTS = new AtomicLong();
    private static final AtomicLong NON_ABSTRACT_OPERATORS = new AtomicLong();
    private static final AtomicLong BACKEND_ACCESS_FAILURES = new AtomicLong();
    private static final AtomicLong MISSING_BACKENDS = new AtomicLong();
    private static final AtomicLong MISSING_PREFETCH_METHODS = new AtomicLong();
    private static final AtomicLong NO_PREFETCHABLE_STATES = new AtomicLong();
    private static final AtomicLong MISSING_KEY_SELECTORS = new AtomicLong();
    private static final AtomicLong KEY_EXTRACTION_FAILURES = new AtomicLong();
    private static final AtomicLong EMPTY_KEY_BATCHES = new AtomicLong();
    private static final AtomicLong BACKEND_INVOCATION_ATTEMPTS = new AtomicLong();
    private static final AtomicLong BACKEND_INVOCATIONS = new AtomicLong();
    private static final AtomicLong FAILURES = new AtomicLong();

    /** Cache of {@code stateKeySelector1} {@link Field} per operator class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Field>
            KEY_SELECTOR_FIELD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional {@code prefetch(Collection)} {@link Method} per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            PREFETCH_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional completion-bearing prefetch methods per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            COMPLETION_PREFETCH_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional ready-gate metric snapshots per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            READY_METRICS_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional synchronous local-preagg bulk-prefetch methods per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            IMMEDIATE_PREFETCH_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional immediate-prefetch hooks that fuse exact reservation revocation. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            IMMEDIATE_PREFETCH_AFTER_DISPATCH_METHOD_CACHE =
                    new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of synchronous, record-key-safe immediate-prefetch hooks per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            RECORD_IMMEDIATE_PREFETCH_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of record-immediate backend metric snapshots per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            RECORD_IMMEDIATE_METRICS_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional exact dispatch-time reservation cancellation methods. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            DISPATCH_CANCEL_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional {@code hasPrefetchableState()} {@link Method} per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            HAS_PREFETCHABLE_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional native mailbox-batch capability probes per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            NATIVE_MAILBOX_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional native LocalPreagg stable-group planners per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            NATIVE_PREAGG_GROUP_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional resident-mutation batch begin hooks per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            NATIVE_MUTATION_BATCH_BEGIN_METHOD_CACHE =
                    new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional resident-mutation batch end hooks per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            NATIVE_MUTATION_BATCH_END_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of resident-mutation batch capability probes per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            NATIVE_MUTATION_BATCH_ENABLED_METHOD_CACHE =
                    new java.util.concurrent.ConcurrentHashMap<>();

    /** Sentinel field used to mark "no stateKeySelector1 available" in the field cache. */
    private static final Field NO_FIELD;

    /** Sentinel method used to mark "no prefetch hook available" in the method cache. */
    private static final Method NO_METHOD;

    static {
        try {
            NO_FIELD = StatePrefetcher.class.getDeclaredField("NO_FIELD");
            NO_METHOD =
                    StatePrefetcher.class.getDeclaredMethod(
                            "noopPrefetch", java.util.Collection.class);
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        } catch (NoSuchMethodException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private StatePrefetcher() {}

    @SuppressWarnings("unused")
    private static void noopPrefetch(java.util.Collection<?> ignored) {}

    /**
     * Best-effort prefetch of the keyed state for the first {@code n} records of {@code buf}.
     *
     * @param headOperator the head operator (typically the chain's main operator).
     * @param buf record buffer; only entries {@code [0, n)} are valid.
     * @param n number of valid records in {@code buf}.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void prefetch(Input<?> headOperator, StreamRecord<?>[] buf, int n) {
        prefetch(headOperator, buf, 0, n);
    }

    /**
     * Best-effort prefetch for a live range in the caller-owned record buffer. The call is
     * synchronous only through key extraction and backend submission; CacheKit performs the actual
     * RocksDB reads on its worker. Keeping the range avoids allocating a copied record slice for
     * every early-lookahead chunk.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void prefetch(
            Input<?> headOperator, StreamRecord<?>[] buf, int fromIndex, int toIndex) {
        PREFETCH_ATTEMPTS.incrementAndGet();
        if (headOperator == null
                || buf == null
                || fromIndex < 0
                || toIndex > buf.length
                || toIndex - fromIndex <= 1) {
            INVALID_INPUTS.incrementAndGet();
            return; // single record gains nothing from a batched prefetch
        }
        try {
            if (!(headOperator instanceof AbstractStreamOperator)) {
                NON_ABSTRACT_OPERATORS.incrementAndGet();
                return;
            }
            AbstractStreamOperator<?> op = (AbstractStreamOperator<?>) headOperator;

            KeyedStateBackend<?> ksb;
            try {
                ksb = op.getKeyedStateBackend();
            } catch (Throwable t) {
                BACKEND_ACCESS_FAILURES.incrementAndGet();
                return;
            }
            if (ksb == null) {
                MISSING_BACKENDS.incrementAndGet();
                return;
            }
            Method prefetchMethod = findPrefetchMethod(ksb);
            if (prefetchMethod == null) {
                MISSING_PREFETCH_METHODS.incrementAndGet();
                return;
            }

            // Skip the whole per-batch key extraction when the backend has nothing to warm
            // (e.g. window operators whose namespaced states are never wrapped). Wrappers
            // register lazily, so this is re-checked on every batch, not cached.
            if (!hasPrefetchableState(ksb)) {
                NO_PREFETCHABLE_STATES.incrementAndGet();
                return;
            }

            KeySelector selector = extractStateKeySelector1(op);
            if (selector == null) {
                MISSING_KEY_SELECTORS.incrementAndGet();
                return;
            }

            // Native mailbox mode preserves the raw arrival-order key vector so the selected
            // AArch64 kernel can compact exact duplicates after serialization. Other backends keep
            // the original LinkedHashSet behavior.
            java.util.Collection keys = newKeyCollection(ksb, Math.max(2, toIndex - fromIndex));
            if (!extractKeys(selector, buf, fromIndex, toIndex, keys)) {
                KEY_EXTRACTION_FAILURES.incrementAndGet();
                return;
            }
            if (keys.isEmpty()) {
                EMPTY_KEY_BATCHES.incrementAndGet();
                return;
            }
            BACKEND_INVOCATION_ATTEMPTS.incrementAndGet();
            prefetchMethod.invoke(ksb, keys);
            BACKEND_INVOCATIONS.incrementAndGet();
        } catch (Throwable t) {
            FAILURES.incrementAndGet();
            // best-effort: prefetch must never affect the authoritative dispatch path.
        }
    }

    /**
     * Starts a completion-bearing prefetch for a future ordered dispatch batch.
     *
     * <p>The returned future resolves to true only when the backend explicitly proves the batch
     * ready. Unsupported backends, invalid input, missing state/selectors, extraction failure, and
     * malformed reflective results resolve to false. A backend future's exceptional completion is
     * preserved so the caller can count an authoritative fallback without blocking the mailbox.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static CompletableFuture<Boolean> prefetchWithCompletion(
            Input<?> headOperator, StreamRecord<?>[] buf, int fromIndex, int toIndex) {
        if (headOperator == null
                || buf == null
                || fromIndex < 0
                || toIndex > buf.length
                || toIndex - fromIndex <= 1
                || !(headOperator instanceof AbstractStreamOperator)) {
            return CompletableFuture.completedFuture(false);
        }
        try {
            AbstractStreamOperator<?> operator = (AbstractStreamOperator<?>) headOperator;
            KeyedStateBackend<?> backend = operator.getKeyedStateBackend();
            if (backend == null
                    || findCompletionPrefetchMethod(backend) == null
                    || !hasPrefetchableState(backend)) {
                return CompletableFuture.completedFuture(false);
            }
            KeySelector selector = extractStateKeySelector1(operator);
            if (selector == null) {
                return CompletableFuture.completedFuture(false);
            }
            java.util.Collection keys = newKeyCollection(backend, Math.max(2, toIndex - fromIndex));
            if (!extractKeys(selector, buf, fromIndex, toIndex, keys) || keys.isEmpty()) {
                return CompletableFuture.completedFuture(false);
            }
            return prefetchKeysWithCompletion(backend, keys);
        } catch (Throwable failure) {
            return CompletableFuture.completedFuture(false);
        }
    }

    @SuppressWarnings("unchecked")
    static CompletableFuture<Boolean> prefetchKeysWithCompletion(
            KeyedStateBackend<?> backend, java.util.Collection<?> keys) {
        if (backend == null || keys == null || keys.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }
        try {
            Method method = findCompletionPrefetchMethod(backend);
            if (method == null) {
                return CompletableFuture.completedFuture(false);
            }
            Object result = method.invoke(backend, keys);
            if (!(result instanceof CompletionStage)) {
                return CompletableFuture.completedFuture(false);
            }
            return ((CompletionStage<Boolean>) result).toCompletableFuture();
        } catch (Throwable failure) {
            return CompletableFuture.completedFuture(false);
        }
    }

    /** Reads one counter from the optional CacheKit ready-gate metric snapshot. */
    public static long getReadyGatedBackendMetric(Input<?> headOperator, int metricIndex) {
        if (!(headOperator instanceof AbstractStreamOperator) || metricIndex < 0) {
            return 0L;
        }
        try {
            KeyedStateBackend<?> backend =
                    ((AbstractStreamOperator<?>) headOperator).getKeyedStateBackend();
            if (backend == null) {
                return 0L;
            }
            Method method =
                    READY_METRICS_METHOD_CACHE.computeIfAbsent(
                            backend.getClass(), StatePrefetcher::lookupReadyMetricsMethod);
            if (method == NO_METHOD) {
                return 0L;
            }
            Object result = method.invoke(backend);
            if (!(result instanceof long[])) {
                return 0L;
            }
            long[] metrics = (long[]) result;
            return metricIndex < metrics.length ? metrics[metricIndex] : 0L;
        } catch (Throwable failure) {
            return 0L;
        }
    }

    /** Reads one counter from the optional CacheKit record-immediate metric snapshot. */
    public static long getRecordImmediateBackendMetric(Input<?> headOperator, int metricIndex) {
        if (!(headOperator instanceof AbstractStreamOperator) || metricIndex < 0) {
            return 0L;
        }
        try {
            KeyedStateBackend<?> backend =
                    ((AbstractStreamOperator<?>) headOperator).getKeyedStateBackend();
            if (backend == null) {
                return 0L;
            }
            Method method =
                    RECORD_IMMEDIATE_METRICS_METHOD_CACHE.computeIfAbsent(
                            backend.getClass(),
                            StatePrefetcher::lookupRecordImmediateMetricsMethod);
            if (method == NO_METHOD) {
                return 0L;
            }
            Object result = method.invoke(backend);
            if (!(result instanceof long[])) {
                return 0L;
            }
            long[] metrics = (long[]) result;
            return metricIndex < metrics.length ? metrics[metricIndex] : 0L;
        } catch (Throwable failure) {
            return 0L;
        }
    }

    public static long getPrefetchAttempts() {
        return PREFETCH_ATTEMPTS.get();
    }

    public static long getInvalidInputs() {
        return INVALID_INPUTS.get();
    }

    public static long getNonAbstractOperators() {
        return NON_ABSTRACT_OPERATORS.get();
    }

    public static long getBackendAccessFailures() {
        return BACKEND_ACCESS_FAILURES.get();
    }

    public static long getMissingBackends() {
        return MISSING_BACKENDS.get();
    }

    public static long getMissingPrefetchMethods() {
        return MISSING_PREFETCH_METHODS.get();
    }

    public static long getNoPrefetchableStates() {
        return NO_PREFETCHABLE_STATES.get();
    }

    public static long getMissingKeySelectors() {
        return MISSING_KEY_SELECTORS.get();
    }

    public static long getKeyExtractionFailures() {
        return KEY_EXTRACTION_FAILURES.get();
    }

    public static long getEmptyKeyBatches() {
        return EMPTY_KEY_BATCHES.get();
    }

    public static long getBackendInvocationAttempts() {
        return BACKEND_INVOCATION_ATTEMPTS.get();
    }

    public static long getBackendInvocations() {
        return BACKEND_INVOCATIONS.get();
    }

    public static long getFailures() {
        return FAILURES.get();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static boolean extractKeys(
            KeySelector selector,
            StreamRecord<?>[] buf,
            int fromIndex,
            int toIndex,
            java.util.Collection keys) {
        for (int i = fromIndex; i < toIndex; i++) {
            StreamRecord<?> rec = buf[i];
            if (rec == null) {
                continue;
            }
            Object key;
            try {
                key = selector.getKey(rec.getValue());
            } catch (Throwable t) {
                return false; // an unkeyed/odd record: bail, the prefetch is optional
            }
            if (key != null) {
                keys.add(key);
            }
        }
        return true;
    }

    /**
     * Revoke prepared-key prefetch ownership for the exact records selected for dispatch.
     *
     * <p>This runs before LocalPreagg or ordinary record replay. It deliberately does not cancel a
     * worker thread or delete an already-published staging value. Backends without the optional
     * exact cancellation hook are left unchanged.
     *
     * @return number of exact reservations revoked, or {@code -1} when unsupported/failed.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static int cancelPrefetchForDispatch(
            Input<?> headOperator, StreamRecord<?>[] buf, int fromIndex, int toIndex) {
        if (headOperator == null
                || buf == null
                || fromIndex < 0
                || toIndex > buf.length
                || toIndex <= fromIndex
                || !(headOperator instanceof AbstractStreamOperator)) {
            return -1;
        }
        try {
            AbstractStreamOperator<?> op = (AbstractStreamOperator<?>) headOperator;
            KeyedStateBackend<?> backend = op.getKeyedStateBackend();
            KeySelector selector = extractStateKeySelector1(op);
            if (backend == null || selector == null) {
                return -1;
            }
            java.util.LinkedHashSet keys =
                    new java.util.LinkedHashSet(Math.max(2, toIndex - fromIndex));
            if (!extractKeys(selector, buf, fromIndex, toIndex, keys) || keys.isEmpty()) {
                return -1;
            }
            return cancelPrefetchForDispatch(backend, keys);
        } catch (Throwable failure) {
            return -1;
        }
    }

    static int cancelPrefetchForDispatch(
            KeyedStateBackend<?> backend, java.util.Collection<?> keys) {
        if (backend == null || keys == null || keys.isEmpty()) {
            return -1;
        }
        try {
            Method method =
                    DISPATCH_CANCEL_METHOD_CACHE.computeIfAbsent(
                            backend.getClass(), StatePrefetcher::lookupDispatchCancelMethod);
            if (method == NO_METHOD) {
                return -1;
            }
            Object result = method.invoke(backend, keys);
            return result instanceof Number ? ((Number) result).intValue() : -1;
        } catch (Throwable failure) {
            return -1;
        }
    }

    /**
     * Revoke exact reservations for a caller-owned, already-deduplicated key set.
     *
     * <p>LocalPreagg uses this after grouping, avoiding a second KeySelector pass and a second
     * LinkedHashSet allocation on the mailbox hot path.
     */
    public static int cancelPrefetchKeysForDispatch(
            Input<?> headOperator, java.util.Collection<?> keys) {
        if (headOperator == null
                || keys == null
                || keys.isEmpty()
                || !(headOperator instanceof AbstractStreamOperator)) {
            return -1;
        }
        try {
            KeyedStateBackend<?> backend =
                    ((AbstractStreamOperator<?>) headOperator).getKeyedStateBackend();
            return cancelPrefetchForDispatch(backend, keys);
        } catch (Throwable failure) {
            return -1;
        }
    }

    /** Opens an optional native mutation batch for an already-deduplicated dispatch key set. */
    public static boolean beginNativeResidentMutationBatch(
            Input<?> headOperator, java.util.Collection<?> keys) {
        if (!(headOperator instanceof AbstractStreamOperator) || keys == null || keys.isEmpty()) {
            return false;
        }
        try {
            KeyedStateBackend<?> backend =
                    ((AbstractStreamOperator<?>) headOperator).getKeyedStateBackend();
            if (!nativeResidentMutationBatchEnabled(backend)) {
                return false;
            }
            Method method =
                    NATIVE_MUTATION_BATCH_BEGIN_METHOD_CACHE.computeIfAbsent(
                            backend.getClass(),
                            StatePrefetcher::lookupNativeMutationBatchBeginMethod);
            if (method == NO_METHOD) {
                return false;
            }
            Object result = method.invoke(backend, keys);
            return result instanceof Number && ((Number) result).intValue() > 0;
        } catch (Throwable failure) {
            return false;
        }
    }

    /** Extracts and deduplicates record keys before opening an optional native mutation batch. */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static boolean beginNativeResidentMutationBatch(
            Input<?> headOperator, StreamRecord<?>[] buf, int fromIndex, int toIndex) {
        if (!(headOperator instanceof AbstractStreamOperator)
                || buf == null
                || fromIndex < 0
                || toIndex > buf.length
                || toIndex <= fromIndex) {
            return false;
        }
        try {
            AbstractStreamOperator<?> operator = (AbstractStreamOperator<?>) headOperator;
            if (!nativeResidentMutationBatchEnabled(operator.getKeyedStateBackend())) {
                return false;
            }
            KeySelector selector = extractStateKeySelector1(operator);
            if (selector == null) {
                return false;
            }
            java.util.LinkedHashSet keys =
                    new java.util.LinkedHashSet(Math.max(2, toIndex - fromIndex));
            if (!extractKeys(selector, buf, fromIndex, toIndex, keys) || keys.isEmpty()) {
                return false;
            }
            return beginNativeResidentMutationBatch(headOperator, keys);
        } catch (Throwable failure) {
            return false;
        }
    }

    /** Ends an optional native mutation batch previously opened for this dispatch. */
    public static void endNativeResidentMutationBatch(Input<?> headOperator) {
        if (!(headOperator instanceof AbstractStreamOperator)) {
            return;
        }
        try {
            KeyedStateBackend<?> backend =
                    ((AbstractStreamOperator<?>) headOperator).getKeyedStateBackend();
            Method method =
                    NATIVE_MUTATION_BATCH_END_METHOD_CACHE.computeIfAbsent(
                            backend.getClass(),
                            StatePrefetcher::lookupNativeMutationBatchEndMethod);
            if (method != NO_METHOD) {
                method.invoke(backend);
            }
        } catch (Throwable failure) {
            // Optional performance path: the backend itself fails closed on native errors.
        }
    }

    private static Method lookupNativeMutationBatchBeginMethod(Class<?> backendClass) {
        try {
            Method method =
                    backendClass.getMethod(
                            "beginNativeResidentMutationBatch", java.util.Collection.class);
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException ignored) {
            return NO_METHOD;
        }
    }

    private static Method lookupNativeMutationBatchEndMethod(Class<?> backendClass) {
        try {
            Method method = backendClass.getMethod("endNativeResidentMutationBatch");
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException ignored) {
            return NO_METHOD;
        }
    }

    private static boolean nativeResidentMutationBatchEnabled(KeyedStateBackend<?> backend) {
        if (backend == null) {
            return false;
        }
        try {
            Method method =
                    NATIVE_MUTATION_BATCH_ENABLED_METHOD_CACHE.computeIfAbsent(
                            backend.getClass(),
                            StatePrefetcher::lookupNativeMutationBatchEnabledMethod);
            if (method == NO_METHOD) {
                return false;
            }
            Object result = method.invoke(backend);
            return result instanceof Boolean && (Boolean) result;
        } catch (Throwable failure) {
            return false;
        }
    }

    private static Method lookupNativeMutationBatchEnabledMethod(Class<?> backendClass) {
        try {
            Method method = backendClass.getMethod("nativeResidentMutationBatchEnabled");
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException ignored) {
            return NO_METHOD;
        }
    }

    private static Method lookupDispatchCancelMethod(Class<?> backendClass) {
        Class<?> current = backendClass;
        while (current != null && current != Object.class) {
            try {
                Method method =
                        current.getDeclaredMethod(
                                "cancelPrefetchForDispatch", java.util.Collection.class);
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException ignored) {
                current = current.getSuperclass();
            }
        }
        return NO_METHOD;
    }

    /**
     * Bulk-load the already grouped keys immediately before local pre-aggregation consumes them.
     *
     * <p>Unlike record lookahead, these keys are no longer speculative: {@code LocalPreagg} has
     * already built its exact group set and will access each key once. CacheKit may therefore use a
     * blocking RocksDB MultiGet here; other backends simply lack the optional reflective hook.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static boolean prefetchKeysImmediately(
            Input<?> headOperator, java.util.Collection<?> keys) {
        return prefetchKeysImmediately(headOperator, keys, false);
    }

    /**
     * Bulk-load grouped keys, optionally fusing exact reservation revocation into that same scan.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static boolean prefetchKeysImmediately(
            Input<?> headOperator, java.util.Collection<?> keys, boolean cancelPrefetchOnDispatch) {
        if (headOperator == null
                || keys == null
                || keys.isEmpty()
                || !(headOperator instanceof AbstractStreamOperator)) {
            return false;
        }
        try {
            KeyedStateBackend<?> backend =
                    ((AbstractStreamOperator<?>) headOperator).getKeyedStateBackend();
            return prefetchKeysImmediately(backend, keys, cancelPrefetchOnDispatch);
        } catch (Throwable t) {
            return false;
        }
    }

    static boolean prefetchKeysImmediately(
            KeyedStateBackend<?> backend, java.util.Collection<?> keys) {
        return prefetchKeysImmediately(backend, keys, false);
    }

    /**
     * Synchronously bulk-loads the exact keys of an ordinary arrival-order replay batch.
     *
     * <p>The backend hook is distinct from LocalPreagg immediate prefetch: it must reject
     * namespaced state and sub-MultiGet miss sets because a record key alone does not identify a
     * future window/session namespace and a synchronous point-read loop cannot amortize dispatch.
     * Unsupported hooks and every reflection/key-extraction failure fail closed to ordinary replay.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static boolean prefetchRecordsImmediately(
            Input<?> headOperator, StreamRecord<?>[] buf, int fromIndex, int toIndex) {
        if (headOperator == null
                || buf == null
                || fromIndex < 0
                || toIndex > buf.length
                || toIndex - fromIndex <= 1
                || !(headOperator instanceof AbstractStreamOperator)) {
            return false;
        }
        try {
            AbstractStreamOperator<?> operator = (AbstractStreamOperator<?>) headOperator;
            KeyedStateBackend<?> backend = operator.getKeyedStateBackend();
            if (backend == null || !hasPrefetchableState(backend)) {
                return false;
            }
            KeySelector selector = extractStateKeySelector1(operator);
            if (selector == null) {
                return false;
            }
            java.util.LinkedHashSet keys =
                    new java.util.LinkedHashSet(Math.max(2, toIndex - fromIndex));
            if (!extractKeys(selector, buf, fromIndex, toIndex, keys) || keys.isEmpty()) {
                return false;
            }
            return prefetchRecordKeysImmediately(backend, keys);
        } catch (Throwable failure) {
            return false;
        }
    }

    static boolean prefetchRecordKeysImmediately(
            KeyedStateBackend<?> backend, java.util.Collection<?> keys) {
        if (backend == null || keys == null || keys.isEmpty() || !hasPrefetchableState(backend)) {
            return false;
        }
        try {
            Method method =
                    RECORD_IMMEDIATE_PREFETCH_METHOD_CACHE.computeIfAbsent(
                            backend.getClass(),
                            StatePrefetcher::lookupRecordImmediatePrefetchMethod);
            if (method == NO_METHOD) {
                return false;
            }
            Object result = method.invoke(backend, keys);
            return result instanceof Boolean && (Boolean) result;
        } catch (Throwable failure) {
            return false;
        }
    }

    static boolean prefetchKeysImmediately(
            KeyedStateBackend<?> backend,
            java.util.Collection<?> keys,
            boolean cancelPrefetchOnDispatch) {
        if (backend == null || keys == null || keys.isEmpty() || !hasPrefetchableState(backend)) {
            return false;
        }
        try {
            Method method;
            if (cancelPrefetchOnDispatch) {
                method =
                        IMMEDIATE_PREFETCH_AFTER_DISPATCH_METHOD_CACHE.computeIfAbsent(
                                backend.getClass(),
                                StatePrefetcher::lookupImmediatePrefetchAfterDispatchMethod);
                if (method == NO_METHOD) {
                    // Keep older/foreign backends on the established immediate-prefetch path.
                    // Preserve the earlier two-call protocol when that backend exposes the exact
                    // cancellation hook but not the newer fused entry point.
                    cancelPrefetchForDispatch(backend, keys);
                    method =
                            IMMEDIATE_PREFETCH_METHOD_CACHE.computeIfAbsent(
                                    backend.getClass(),
                                    StatePrefetcher::lookupImmediatePrefetchMethod);
                }
            } else {
                method =
                        IMMEDIATE_PREFETCH_METHOD_CACHE.computeIfAbsent(
                                backend.getClass(), StatePrefetcher::lookupImmediatePrefetchMethod);
            }
            if (method == NO_METHOD) {
                return false;
            }
            method.invoke(backend, keys);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    /** Returns {@code [groupCount, groupId0, ...]} or null for the Java grouping fallback. */
    public static int[] groupKeysNatively(Input<?> headOperator, java.util.List<?> keys) {
        if (headOperator == null
                || keys == null
                || keys.isEmpty()
                || !(headOperator instanceof AbstractStreamOperator)) {
            return null;
        }
        try {
            KeyedStateBackend<?> backend =
                    ((AbstractStreamOperator<?>) headOperator).getKeyedStateBackend();
            return groupKeysNatively(backend, keys);
        } catch (Throwable failure) {
            return null;
        }
    }

    static int[] groupKeysNatively(KeyedStateBackend<?> backend, java.util.List<?> keys) {
        if (backend == null || keys == null || keys.isEmpty()) {
            return null;
        }
        try {
            Method method =
                    NATIVE_PREAGG_GROUP_METHOD_CACHE.computeIfAbsent(
                            backend.getClass(), StatePrefetcher::lookupNativePreaggGroupMethod);
            if (method == NO_METHOD) {
                return null;
            }
            Object result = method.invoke(backend, keys);
            if (!(result instanceof int[])) {
                return null;
            }
            int[] plan = (int[]) result;
            return plan.length == keys.size() + 1 ? plan : null;
        } catch (Throwable failure) {
            return null;
        }
    }

    /**
     * Groups caller-owned 32-bit hash tokens without reflection or a heap plan copy.
     *
     * <p>A negative return value means that the caller must retain its Java grouping path.
     */
    public static int groupHashTokensNatively(
            Input<?> headOperator, ByteBuffer tokens, int count, ByteBuffer packedPlan) {
        if (headOperator == null
                || count <= 0
                || tokens == null
                || packedPlan == null
                || !(headOperator instanceof AbstractStreamOperator)) {
            return -1;
        }
        try {
            KeyedStateBackend<?> backend =
                    ((AbstractStreamOperator<?>) headOperator).getKeyedStateBackend();
            return groupHashTokensNatively(backend, tokens, count, packedPlan);
        } catch (Throwable failure) {
            return -1;
        }
    }

    /** Returns the job-scoped backend capability for the reusable indexed batch consumer. */
    public static boolean indexedBatchFoldEnabled(Input<?> headOperator) {
        if (!(headOperator instanceof AbstractStreamOperator)) {
            return false;
        }
        try {
            KeyedStateBackend<?> backend =
                    ((AbstractStreamOperator<?>) headOperator).getKeyedStateBackend();
            return indexedBatchFoldEnabled(backend);
        } catch (Throwable failure) {
            return false;
        }
    }

    static boolean indexedBatchFoldEnabled(KeyedStateBackend<?> backend) {
        return backend instanceof BatchKeyGroupingSupport
                && ((BatchKeyGroupingSupport) backend).indexedBatchFoldEnabled();
    }

    static int groupHashTokensNatively(
            KeyedStateBackend<?> backend, ByteBuffer tokens, int count, ByteBuffer packedPlan) {
        if (!(backend instanceof BatchKeyGroupingSupport)) {
            return -1;
        }
        BatchKeyGroupingSupport grouping = (BatchKeyGroupingSupport) backend;
        if (count > grouping.maxGroupingEntries()) {
            return -1;
        }
        try {
            return grouping.groupHashTokens(tokens, count, packedPlan);
        } catch (Throwable failure) {
            return -1;
        }
    }

    private static Method lookupNativePreaggGroupMethod(Class<?> backendClass) {
        try {
            Method method = backendClass.getMethod("nativePreaggGroupIds", java.util.List.class);
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException ignored) {
            return NO_METHOD;
        }
    }

    private static Method lookupImmediatePrefetchMethod(Class<?> backendClass) {
        try {
            Method method =
                    backendClass.getMethod("prefetchForImmediateUse", java.util.Collection.class);
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException ignored) {
            return NO_METHOD;
        }
    }

    private static Method lookupImmediatePrefetchAfterDispatchMethod(Class<?> backendClass) {
        try {
            Method method =
                    backendClass.getMethod(
                            "prefetchForImmediateUseAfterDispatch", java.util.Collection.class);
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException ignored) {
            return NO_METHOD;
        }
    }

    private static Method lookupRecordImmediatePrefetchMethod(Class<?> backendClass) {
        try {
            Method method =
                    backendClass.getMethod(
                            "prefetchRecordKeysForImmediateUse", java.util.Collection.class);
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException ignored) {
            return NO_METHOD;
        }
    }

    /** Best-effort {@code hasPrefetchableState()} probe; defaults to true when absent. */
    private static boolean hasPrefetchableState(KeyedStateBackend<?> backend) {
        try {
            Method method =
                    HAS_PREFETCHABLE_METHOD_CACHE.computeIfAbsent(
                            backend.getClass(), StatePrefetcher::lookupHasPrefetchableMethod);
            if (method == NO_METHOD) {
                return true; // backend without the probe: keep the old behavior
            }
            Object result = method.invoke(backend);
            return !(result instanceof Boolean) || (Boolean) result;
        } catch (Throwable t) {
            return true;
        }
    }

    private static Method lookupHasPrefetchableMethod(Class<?> backendClass) {
        Class<?> c = backendClass;
        while (c != null && c != Object.class) {
            try {
                Method method = c.getDeclaredMethod("hasPrefetchableState");
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException ignored) {
                c = c.getSuperclass();
            }
        }
        return NO_METHOD;
    }

    private static boolean usesNativeMailboxBatch(KeyedStateBackend<?> backend) {
        try {
            Method method =
                    NATIVE_MAILBOX_METHOD_CACHE.computeIfAbsent(
                            backend.getClass(), StatePrefetcher::lookupNativeMailboxMethod);
            if (method == NO_METHOD) {
                return false;
            }
            Object result = method.invoke(backend);
            return result instanceof Boolean && (Boolean) result;
        } catch (Throwable t) {
            return false;
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static java.util.Collection newKeyCollection(
            KeyedStateBackend<?> backend, int expectedEntries) {
        return usesNativeMailboxBatch(backend)
                ? new java.util.ArrayList(expectedEntries)
                : new java.util.LinkedHashSet(expectedEntries);
    }

    private static Method lookupNativeMailboxMethod(Class<?> backendClass) {
        try {
            Method method = backendClass.getMethod("nativeMailboxBatchEnabled");
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException ignored) {
            return NO_METHOD;
        }
    }

    private static Method findPrefetchMethod(KeyedStateBackend<?> backend) {
        if (backend == null) {
            return null;
        }
        Method method =
                PREFETCH_METHOD_CACHE.computeIfAbsent(
                        backend.getClass(), StatePrefetcher::lookupPrefetchMethod);
        return method == NO_METHOD ? null : method;
    }

    private static Method findCompletionPrefetchMethod(KeyedStateBackend<?> backend) {
        if (backend == null) {
            return null;
        }
        Method method =
                COMPLETION_PREFETCH_METHOD_CACHE.computeIfAbsent(
                        backend.getClass(), StatePrefetcher::lookupCompletionPrefetchMethod);
        return method == NO_METHOD ? null : method;
    }

    private static Method lookupPrefetchMethod(Class<?> backendClass) {
        Class<?> c = backendClass;
        while (c != null && c != Object.class) {
            try {
                Method method = c.getDeclaredMethod("prefetch", java.util.Collection.class);
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException ignored) {
                c = c.getSuperclass();
            }
        }
        return NO_METHOD;
    }

    private static Method lookupCompletionPrefetchMethod(Class<?> backendClass) {
        Class<?> current = backendClass;
        while (current != null && current != Object.class) {
            try {
                Method method =
                        current.getDeclaredMethod(
                                "prefetchWithCompletion", java.util.Collection.class);
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException ignored) {
                current = current.getSuperclass();
            }
        }
        return NO_METHOD;
    }

    private static Method lookupReadyMetricsMethod(Class<?> backendClass) {
        Class<?> current = backendClass;
        while (current != null && current != Object.class) {
            try {
                Method method = current.getDeclaredMethod("readyGatedPrefetchMetrics");
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException ignored) {
                current = current.getSuperclass();
            }
        }
        return NO_METHOD;
    }

    private static Method lookupRecordImmediateMetricsMethod(Class<?> backendClass) {
        Class<?> current = backendClass;
        while (current != null && current != Object.class) {
            try {
                Method method = current.getDeclaredMethod("recordImmediatePrefetchMetrics");
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException ignored) {
                current = current.getSuperclass();
            }
        }
        return NO_METHOD;
    }

    // ------------------------------------------------------------------------
    //  KeySelector access (mirrors BatchedKeyedOperatorAdapter)
    // ------------------------------------------------------------------------

    private static KeySelector<?, ?> extractStateKeySelector1(AbstractStreamOperator<?> op) {
        Field f =
                KEY_SELECTOR_FIELD_CACHE.computeIfAbsent(
                        op.getClass(), StatePrefetcher::findKeySelectorField);
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
}
