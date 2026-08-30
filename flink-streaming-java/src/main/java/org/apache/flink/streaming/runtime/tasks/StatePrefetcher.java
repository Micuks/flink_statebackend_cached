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
import org.apache.flink.streaming.runtime.io.MailboxStableKeySidecar;
import org.apache.flink.streaming.runtime.io.TransientKeySelector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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

    private static final ThreadLocal<PrefetchGroupingWorkspace> PREFETCH_GROUPING_WORKSPACE =
            ThreadLocal.withInitial(PrefetchGroupingWorkspace::new);
    private static final java.util.concurrent.atomic.AtomicLong PREFETCH_KEY_DEDUP_WINDOWS =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong PREFETCH_KEY_DEDUP_SOURCES =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong PREFETCH_KEY_DEDUP_STABLE_KEYS =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong PREFETCH_KEY_DEDUP_FALLBACKS =
            new java.util.concurrent.atomic.AtomicLong();

    /** Cache of {@code stateKeySelector1} {@link Field} per operator class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Field>
            KEY_SELECTOR_FIELD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional {@code prefetch(Collection)} {@link Method} per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            PREFETCH_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional exact key/namespace prefetch hooks per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            EXACT_NAMESPACE_PREFETCH_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional exact namespace capability probes per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            EXACT_NAMESPACE_ENABLED_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    private static final java.util.concurrent.atomic.AtomicLong EXACT_NAMESPACE_WINDOWS =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong EXACT_NAMESPACE_RECORDS =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong EXACT_NAMESPACE_PAIRS =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong EXACT_NAMESPACE_FAILURES =
            new java.util.concurrent.atomic.AtomicLong();

    /** Cache of optional synchronous local-preagg bulk-prefetch methods per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            IMMEDIATE_PREFETCH_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional immediate-prefetch hooks that fuse exact reservation revocation. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            IMMEDIATE_PREFETCH_AFTER_DISPATCH_METHOD_CACHE =
                    new java.util.concurrent.ConcurrentHashMap<>();

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
        prefetch(headOperator, buf, fromIndex, toIndex, null);
    }

    /**
     * Best-effort prefetch that may also retain the stable selector result beside the
     * mailbox-owned record buffer. The sidecar is written only after a successful selector call;
     * a partial extraction failure clears the affected range so authoritative dispatch can fall
     * back to its ordinary selector path.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void prefetch(
            Input<?> headOperator,
            StreamRecord<?>[] buf,
            int fromIndex,
            int toIndex,
            MailboxStableKeySidecar stableKeySidecar) {
        if (headOperator == null
                || buf == null
                || fromIndex < 0
                || toIndex > buf.length
                || toIndex - fromIndex <= 1
                || (stableKeySidecar != null && stableKeySidecar.capacity() < toIndex)) {
            return; // single record gains nothing from a batched prefetch
        }
        try {
            if (!(headOperator instanceof AbstractStreamOperator)) {
                return;
            }
            AbstractStreamOperator<?> op = (AbstractStreamOperator<?>) headOperator;

            KeyedStateBackend<?> ksb;
            try {
                ksb = op.getKeyedStateBackend();
            } catch (Throwable t) {
                return;
            }
            if (exactNamespacePrefetchEnabled(ksb)
                    && headOperator instanceof StateNamespaceLookahead) {
                prefetchExactKeyNamespaces(
                        (StateNamespaceLookahead) headOperator,
                        op,
                        ksb,
                        buf,
                        fromIndex,
                        toIndex,
                        stableKeySidecar);
                // Exact mode is fail-closed. A projection failure must fall back to the later
                // authoritative operator read, not to the ambiguous key-only predictor.
                return;
            }
            Method prefetchMethod = findPrefetchMethod(ksb);
            if (prefetchMethod == null) {
                return;
            }

            // Skip the whole per-batch key extraction when the backend has nothing to warm
            // (e.g. window operators whose namespaced states are never wrapped). Wrappers
            // register lazily, so this is re-checked on every batch, not cached.
            if (!hasPrefetchableState(ksb)) {
                return;
            }

            KeySelector selector = extractStateKeySelector1(op);
            if (selector == null) {
                return;
            }

            // Native mailbox mode preserves the raw arrival-order key vector so the selected
            // AArch64 kernel can compact exact duplicates after serialization. Other backends keep
            // the original LinkedHashSet behavior.
            java.util.Collection keys = newKeyCollection(ksb, Math.max(2, toIndex - fromIndex));
            boolean extracted = false;
            if (stableKeySidecar != null && selector instanceof TransientKeySelector) {
                extracted =
                        extractRepresentativeStableKeys(
                                ksb,
                                selector,
                                (TransientKeySelector) selector,
                                buf,
                                fromIndex,
                                toIndex,
                                keys,
                                stableKeySidecar);
                if (!extracted) {
                    PREFETCH_KEY_DEDUP_FALLBACKS.incrementAndGet();
                    keys.clear();
                    stableKeySidecar.clearRange(fromIndex, toIndex);
                }
            }
            if (!extracted) {
                extracted =
                        extractKeys(
                                selector,
                                buf,
                                fromIndex,
                                toIndex,
                                keys,
                                stableKeySidecar);
            }
            if (extracted && !keys.isEmpty()) {
                prefetchMethod.invoke(ksb, keys);
            }
        } catch (Throwable t) {
            // best-effort: prefetch must never affect the authoritative dispatch path.
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void prefetchExactKeyNamespaces(
            StateNamespaceLookahead lookahead,
            AbstractStreamOperator<?> operator,
            KeyedStateBackend<?> backend,
            StreamRecord<?>[] buf,
            int fromIndex,
            int toIndex,
            MailboxStableKeySidecar stableKeySidecar) {
        java.util.ArrayList<Object> keys =
                new java.util.ArrayList<>(Math.max(2, toIndex - fromIndex));
        java.util.ArrayList<Object> namespaces =
                new java.util.ArrayList<>(Math.max(2, toIndex - fromIndex));
        try {
            Method method =
                    EXACT_NAMESPACE_PREFETCH_METHOD_CACHE.computeIfAbsent(
                            backend.getClass(), StatePrefetcher::lookupExactNamespacePrefetchMethod);
            if (method == NO_METHOD) {
                EXACT_NAMESPACE_FAILURES.incrementAndGet();
                return;
            }
            KeySelector selector = extractStateKeySelector1(operator);
            if (selector == null) {
                EXACT_NAMESPACE_FAILURES.incrementAndGet();
                return;
            }
            int records = 0;
            for (int index = fromIndex; index < toIndex; index++) {
                StreamRecord<?> record = buf[index];
                if (record == null) {
                    continue;
                }
                Object key;
                if (stableKeySidecar != null && stableKeySidecar.isReady(index, selector)) {
                    key = stableKeySidecar.keyAt(index);
                } else {
                    key = selector.getKey(record.getValue());
                    if (stableKeySidecar != null) {
                        stableKeySidecar.capture(index, key, selector);
                    }
                }
                if (key == null) {
                    continue;
                }
                int beforeKeys = keys.size();
                int beforeNamespaces = namespaces.size();
                lookahead.appendStatePrefetchKeyNamespaces(record, key, keys, namespaces);
                if (keys.size() != namespaces.size()
                        || keys.size() < beforeKeys
                        || namespaces.size() < beforeNamespaces) {
                    throw new IllegalStateException("Unbalanced state namespace lookahead output.");
                }
                records++;
            }
            if (keys.isEmpty()) {
                return;
            }
            method.invoke(backend, keys, namespaces);
            long windows = EXACT_NAMESPACE_WINDOWS.incrementAndGet();
            long totalRecords = EXACT_NAMESPACE_RECORDS.addAndGet(records);
            long totalPairs = EXACT_NAMESPACE_PAIRS.addAndGet(keys.size());
            if (windows % 5000L == 1L) {
                System.err.println(
                        String.format(
                                "[CACHEKIT EXACT NAMESPACE SIDECAR] windows=%d records=%d pairs=%d failures=%d",
                                windows,
                                totalRecords,
                                totalPairs,
                                EXACT_NAMESPACE_FAILURES.get()));
            }
        } catch (Throwable failure) {
            EXACT_NAMESPACE_FAILURES.incrementAndGet();
            if (stableKeySidecar != null) {
                stableKeySidecar.clearRange(fromIndex, toIndex);
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static boolean extractKeys(
            KeySelector selector,
            StreamRecord<?>[] buf,
            int fromIndex,
            int toIndex,
            java.util.Collection keys) {
        return extractKeys(selector, buf, fromIndex, toIndex, keys, null);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static boolean extractKeys(
            KeySelector selector,
            StreamRecord<?>[] buf,
            int fromIndex,
            int toIndex,
            java.util.Collection keys,
            MailboxStableKeySidecar stableKeySidecar) {
        for (int i = fromIndex; i < toIndex; i++) {
            StreamRecord<?> rec = buf[i];
            if (rec == null) {
                if (stableKeySidecar != null) {
                    stableKeySidecar.invalidate(i);
                }
                continue;
            }
            Object key;
            try {
                key = selector.getKey(rec.getValue());
            } catch (Throwable t) {
                if (stableKeySidecar != null) {
                    stableKeySidecar.clearRange(fromIndex, toIndex);
                }
                return false; // an unkeyed/odd record: bail, the prefetch is optional
            }
            if (stableKeySidecar != null) {
                stableKeySidecar.capture(i, key, selector);
            }
            if (key != null) {
                keys.add(key);
            }
        }
        return true;
    }

    /**
     * Uses the existing native hash-grouping kernel to retain only one stable copied key per exact
     * Java key. Hashes are merely scheduling tokens: a second transient-selector pass validates
     * every Java equality before any representative is exposed to the backend or sidecar.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static boolean extractRepresentativeStableKeys(
            KeyedStateBackend<?> backend,
            KeySelector stableSelector,
            TransientKeySelector transientSelector,
            StreamRecord<?>[] buf,
            int fromIndex,
            int toIndex,
            java.util.Collection keys,
            MailboxStableKeySidecar stableKeySidecar) {
        if (backend == null
                || stableSelector == null
                || transientSelector == null
                || buf == null
                || keys == null
                || stableKeySidecar == null
                || fromIndex < 0
                || toIndex > buf.length
                || toIndex <= fromIndex
                || stableKeySidecar.capacity() < toIndex) {
            return false;
        }
        final PrefetchGroupingWorkspace workspace = PREFETCH_GROUPING_WORKSPACE.get();
        workspace.prepare(toIndex - fromIndex);
        int sourceCount = 0;
        try {
            for (int bufferIndex = fromIndex; bufferIndex < toIndex; bufferIndex++) {
                StreamRecord<?> record = buf[bufferIndex];
                if (record == null) {
                    continue;
                }
                Object transientKey =
                        transientSelector.getTransientKey(record.getValue());
                workspace.putSource(
                        sourceCount, bufferIndex, java.util.Objects.hashCode(transientKey));
                sourceCount++;
            }
            if (sourceCount <= 1) {
                return false;
            }
            int groupCount =
                    groupHashTokensNatively(
                            backend, workspace.tokens, sourceCount, workspace.plan);
            if (!workspace.validateHeader(sourceCount, groupCount)) {
                return false;
            }

            final int firstSourceBase = BatchKeyGroupingSupport.PACKED_PLAN_HEADER_BYTES;
            final int offsetsBase = firstSourceBase + groupCount * Integer.BYTES;
            final int sourceGroupBase = offsetsBase + (groupCount + 1) * Integer.BYTES;
            int previousFirst = -1;
            for (int group = 0; group < groupCount; group++) {
                int firstSource = workspace.plan.getInt(firstSourceBase + group * Integer.BYTES);
                if (firstSource < 0
                        || firstSource >= sourceCount
                        || (group == 0 && firstSource != 0)
                        || firstSource <= previousFirst) {
                    return false;
                }
                previousFirst = firstSource;
                int bufferIndex = workspace.bufferIndexes[firstSource];
                StreamRecord<?> record = buf[bufferIndex];
                if (record == null) {
                    return false;
                }
                workspace.groupKeys[group] = stableSelector.getKey(record.getValue());
            }
            if (workspace.plan.getInt(offsetsBase) != 0
                    || workspace.plan.getInt(offsetsBase + groupCount * Integer.BYTES)
                            != sourceCount) {
                return false;
            }
            for (int group = 0; group < groupCount; group++) {
                int begin = workspace.plan.getInt(offsetsBase + group * Integer.BYTES);
                int end = workspace.plan.getInt(offsetsBase + (group + 1) * Integer.BYTES);
                if (begin < 0 || begin >= end || end > sourceCount) {
                    return false;
                }
            }

            int nextGroup = 0;
            for (int source = 0; source < sourceCount; source++) {
                int group = workspace.plan.getInt(sourceGroupBase + source * Integer.BYTES);
                if (group < 0 || group >= groupCount) {
                    return false;
                }
                int firstSource = workspace.plan.getInt(firstSourceBase + group * Integer.BYTES);
                if (source == firstSource) {
                    if (group != nextGroup) {
                        return false;
                    }
                    nextGroup++;
                } else {
                    int bufferIndex = workspace.bufferIndexes[source];
                    Object transientKey =
                            transientSelector.getTransientKey(buf[bufferIndex].getValue());
                    if (source < firstSource
                            || group >= nextGroup
                            || !java.util.Objects.equals(workspace.groupKeys[group], transientKey)) {
                        return false;
                    }
                }
            }
            if (nextGroup != groupCount) {
                return false;
            }

            stableKeySidecar.clearRange(fromIndex, toIndex);
            for (int group = 0; group < groupCount; group++) {
                int firstSource = workspace.plan.getInt(firstSourceBase + group * Integer.BYTES);
                int bufferIndex = workspace.bufferIndexes[firstSource];
                Object stableKey = workspace.groupKeys[group];
                stableKeySidecar.capture(bufferIndex, stableKey, stableSelector);
                if (stableKey != null) {
                    keys.add(stableKey);
                }
            }
            long windows = PREFETCH_KEY_DEDUP_WINDOWS.incrementAndGet();
            long sources = PREFETCH_KEY_DEDUP_SOURCES.addAndGet(sourceCount);
            long stable = PREFETCH_KEY_DEDUP_STABLE_KEYS.addAndGet(groupCount);
            if (windows % 5000L == 1L) {
                System.err.println(
                        String.format(
                                "[CACHEKIT PREFETCH KEY SIDECAR] windows=%d sources=%d stableKeys=%d reduction=%.2fx fallbacks=%d",
                                windows,
                                sources,
                                stable,
                                stable == 0 ? 0.0 : (double) sources / stable,
                                PREFETCH_KEY_DEDUP_FALLBACKS.get()));
            }
            return !keys.isEmpty();
        } catch (Throwable failure) {
            stableKeySidecar.clearRange(fromIndex, toIndex);
            return false;
        } finally {
            workspace.clear(sourceCount);
        }
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

    /** Returns the bounded, job-scoped cross-key pipeline distance. */
    public static int crossKeyPipelineLookaheadGroups(Input<?> headOperator) {
        if (!(headOperator instanceof AbstractStreamOperator)) {
            return 0;
        }
        try {
            KeyedStateBackend<?> backend =
                    ((AbstractStreamOperator<?>) headOperator).getKeyedStateBackend();
            return crossKeyPipelineLookaheadGroups(backend);
        } catch (Throwable failure) {
            return 0;
        }
    }

    static int crossKeyPipelineLookaheadGroups(KeyedStateBackend<?> backend) {
        if (!(backend instanceof BatchKeyGroupingSupport)) {
            return 0;
        }
        int configured = ((BatchKeyGroupingSupport) backend).crossKeyPipelineLookaheadGroups();
        return Math.max(
                0,
                Math.min(
                        BatchKeyGroupingSupport.MAX_CROSS_KEY_PIPELINE_LOOKAHEAD_GROUPS,
                        configured));
    }

    /** Returns the bounded number of disjoint read waves allowed to overlap. */
    public static int crossKeyPipelineWaveLimit(Input<?> headOperator) {
        if (!(headOperator instanceof AbstractStreamOperator)) {
            return 1;
        }
        try {
            KeyedStateBackend<?> backend =
                    ((AbstractStreamOperator<?>) headOperator).getKeyedStateBackend();
            return crossKeyPipelineWaveLimit(backend);
        } catch (Throwable failure) {
            return 1;
        }
    }

    static int crossKeyPipelineWaveLimit(KeyedStateBackend<?> backend) {
        if (!(backend instanceof BatchKeyGroupingSupport)) {
            return 1;
        }
        return Math.max(
                1, Math.min(2, ((BatchKeyGroupingSupport) backend).crossKeyPipelineWaveLimit()));
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

    private static boolean exactNamespacePrefetchEnabled(KeyedStateBackend<?> backend) {
        if (backend == null) {
            return false;
        }
        try {
            Method method =
                    EXACT_NAMESPACE_ENABLED_METHOD_CACHE.computeIfAbsent(
                            backend.getClass(), StatePrefetcher::lookupExactNamespaceEnabledMethod);
            if (method == NO_METHOD) {
                return false;
            }
            Object result = method.invoke(backend);
            return result instanceof Boolean && (Boolean) result;
        } catch (Throwable failure) {
            return false;
        }
    }

    private static Method lookupExactNamespaceEnabledMethod(Class<?> backendClass) {
        try {
            Method method = backendClass.getMethod("exactNamespacePrefetchEnabled");
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException ignored) {
            return NO_METHOD;
        }
    }

    private static Method lookupExactNamespacePrefetchMethod(Class<?> backendClass) {
        try {
            Method method =
                    backendClass.getMethod(
                            "prefetchKeyNamespaces",
                            java.util.Collection.class,
                            java.util.Collection.class);
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

    private static final class PrefetchGroupingWorkspace {
        private ByteBuffer tokens = directBuffer(Integer.BYTES);
        private ByteBuffer plan =
                directBuffer(BatchKeyGroupingSupport.requiredPackedPlanBytes(1));
        private int[] bufferIndexes = new int[1];
        private Object[] groupKeys = new Object[1];

        private void prepare(int capacity) {
            int bounded = Math.max(1, capacity);
            int tokenBytes = Math.multiplyExact(bounded, Integer.BYTES);
            int planBytes = BatchKeyGroupingSupport.requiredPackedPlanBytes(bounded);
            if (tokens.capacity() < tokenBytes) {
                tokens = directBuffer(grownCapacity(tokens.capacity(), tokenBytes));
            }
            if (plan.capacity() < planBytes) {
                plan = directBuffer(grownCapacity(plan.capacity(), planBytes));
            }
            if (bufferIndexes.length < bounded) {
                int grown = grownCapacity(bufferIndexes.length, bounded);
                bufferIndexes = new int[grown];
                groupKeys = new Object[grown];
            }
            tokens.clear();
            plan.clear();
            plan.putInt(0, 0);
        }

        private void putSource(int source, int bufferIndex, int token) {
            bufferIndexes[source] = bufferIndex;
            tokens.putInt(source * Integer.BYTES, token);
        }

        private boolean validateHeader(int sourceCount, int groupCount) {
            if (sourceCount <= 0 || groupCount <= 0 || groupCount > sourceCount) {
                return false;
            }
            int required = BatchKeyGroupingSupport.requiredPackedPlanBytes(sourceCount);
            return required <= plan.capacity()
                    && plan.getInt(0) == BatchKeyGroupingSupport.PACKED_PLAN_MAGIC
                    && plan.getInt(Integer.BYTES)
                            == BatchKeyGroupingSupport.PACKED_PLAN_VERSION
                    && plan.getInt(2 * Integer.BYTES) == sourceCount
                    && plan.getInt(3 * Integer.BYTES) == groupCount;
        }

        private void clear(int sourceCount) {
            int bounded = Math.max(0, Math.min(sourceCount, groupKeys.length));
            for (int index = 0; index < bounded; index++) {
                groupKeys[index] = null;
            }
        }

        private static ByteBuffer directBuffer(int bytes) {
            return ByteBuffer.allocateDirect(bytes).order(ByteOrder.nativeOrder());
        }

        private static int grownCapacity(int current, int required) {
            int grown = Math.max(1, current);
            while (grown < required) {
                grown = Math.multiplyExact(grown, 2);
            }
            return grown;
        }
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
