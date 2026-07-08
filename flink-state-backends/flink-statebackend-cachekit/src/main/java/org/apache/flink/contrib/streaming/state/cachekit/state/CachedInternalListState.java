/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.contrib.streaming.state.cachekit.state.CurrentKeyProvider;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * Cachekit wrapper over delegate {@link InternalListState} that implements:
 *
 * <ul>
 *   <li><b>Batch-merge optimization</b>: {@code add()} / {@code addAll()} buffer into a pending map.
 *       Each key's pending list is merged into a single {@code delegate.addAll()} call at flush time,
 *       reducing the number of RocksDB write operations.
 *   <li><b>Synchronous flush (方案 A)</b>: Flush is always synchronous on the Flink Task thread.
 *       This eliminates all cross-thread keyContext issues (LS-1, LS-4), broken async Future tracking
 *       (LS-2), and the need for complex checkpoint coordination.
 *   <li><b>RYW with correct clear semantics</b> (LS-3 fix): {@code clear()} now syncs to the delegate,
 *       so checkpoint does not contain stale data and clearedKeys LRU eviction does not bring back old
 *       data.
 *   <li><b>Bounded cleared-keys</b>: uses an LRU-linked-hash-map with a configurable capacity.
 *       When an entry is evicted from clearedKeys, the key's data is deleted from the delegate
 *       (preventing LS-3 reappear issue).
 *   <li><b>Reference snapshot at add time</b> (LS-5 fix): pendingMap stores deep copies of values
 *       at add time, so later mutation of the original objects does not corrupt buffered data.
 * </ul>
 *
 * <p>COW and RYW can be independently enabled:
 *
 * <ul>
 *   <li>{@code cow=true, ryw=true}: Full COW + RYW. pendingMap buffers writes; cleared keys read
 *       from pendingMap directly.
 *   <li>{@code cow=true, ryw=false}: COW-only. pendingMap buffers writes; no cleared-key fast path.
 *   <li>{@code cow=false, ryw=true}: RYW-only. pendingMap is still maintained (synchronously flushed),
 *       enabling the cleared-key fast path. Batch merge still applies.
 *   <li>{@code cow=false, ryw=false}: passthrough to delegate (wrapper does nothing).
 * </ul>
 *
 * @param <K> The Flink key type.
 * @param <N> The namespace type.
 * @param <V> The element type stored in lists.
 */
public class CachedInternalListState<K, N, V> implements InternalListState<K, N, V> {

    private static final Logger LOG = LoggerFactory.getLogger(CachedInternalListState.class);

    // ---- delegate ----
    private final InternalListState<K, N, V> delegate;
    private final CurrentKeyProvider<K> currentKeyProvider;
    private final java.util.function.Consumer<K> keyContextSetter;
    private N currentNamespace;

    // ---- serializer ----
    private final TypeSerializer<V> elementSerializer;
    private final TypeSerializer<List<V>> listSerializer;

    // ---- COW config ----
    private final boolean cowEnabled;
    private final boolean rywEnabled;

    // ---- COW: sync pending buffer ----
    /** Flush threshold: when pending map reaches this size, trigger sync flush. */
    private static final int FLUSH_THRESHOLD = 4096;
    /** Per-list threshold: when a single list reaches this size, flush that key sync. */
    private static final int MAX_PENDING_LIST_SIZE = 500;

    /** Pending buffer: key → list of values. Values are copied at add time (LS-5 fix). */
    private volatile ConcurrentHashMap<NamespaceKeyWrapper, List<V>> pendingMap =
            new ConcurrentHashMap<>();

    // ---- RYW: cleared key tracking with bounded LRU ----
    private final int clearedKeysCapacity;
    private final Map<NamespaceKeyWrapper, Boolean> clearedKeys;

    // ---- cached wrapper for hot-path allocation reduction ----
    private NamespaceKeyWrapper cachedWrapper;

    public CachedInternalListState(
            InternalListState<K, N, V> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
            boolean cowEnabled,
            boolean rywEnabled,
            TypeSerializer<V> elementSerializer,
            ExecutorService flushExecutor,
            int clearedKeysCapacity) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.currentKeyProvider = Objects.requireNonNull(currentKeyProvider, "currentKeyProvider");
        this.keyContextSetter = Objects.requireNonNull(keyContextSetter, "keyContextSetter");
        this.cowEnabled = cowEnabled;
        this.rywEnabled = rywEnabled;
        this.elementSerializer = Objects.requireNonNull(elementSerializer, "elementSerializer");
        this.listSerializer = new ListSerializer<>(elementSerializer);

        // Bounded cleared-keys map with LRU eviction (access-order LinkedHashMap)
        // LS-3 fix: when an entry is evicted, we flush the delete to the delegate so the data
        // does not reappear. This is done lazily in the eviction callback.
        this.clearedKeysCapacity = clearedKeysCapacity;
        if (rywEnabled) {
            this.clearedKeys =
                    Collections.synchronizedMap(
                            new LinkedHashMap<NamespaceKeyWrapper, Boolean>(clearedKeysCapacity, 0.75f, true) {
                                @Override
                                protected boolean removeEldestEntry(
                                        Map.Entry<NamespaceKeyWrapper, Boolean> eldest) {
                                    if (size() > clearedKeysCapacity) {
                                        // LS-3 fix: evicted cleared-key means we lost the "deleted" marker.
                                        // Flush a delete to the delegate so stale data does not reappear.
                                        flushClearedKeyToDelegate(eldest.getKey());
                                    }
                                    return size() > clearedKeysCapacity;
                                }
                            });
        } else {
            this.clearedKeys = null;
        }

        LOG.info(
                "[CACHEKIT COW] CachedInternalListState created: cow={}, ryw={}, clearedKeysCap={}",
                cowEnabled,
                rywEnabled,
                clearedKeysCapacity);
    }

    // ------------------------------------------------------------------------
    //  Public API (ListState / InternalListState contract)
    // ------------------------------------------------------------------------

    @Override
    public void add(V value) throws Exception {
        Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
        if (!cowEnabled) {
            delegate.add(value);
            return;
        }

        // LS-5 fix: copy the value at add time to avoid reference aliasing
        V copiedValue = copyValue(value);

        NamespaceKeyWrapper wrapped = wrapCurrentKey();
        boolean[] needsSingleFlush = {false};
        boolean[] needsGlobalFlush = {false};

        pendingMap.compute(
                wrapped,
                (k, existing) -> {
                    List<V> list = existing != null ? existing : new ArrayList<>();
                    list.add(copiedValue);
                    if (list.size() >= MAX_PENDING_LIST_SIZE) {
                        needsSingleFlush[0] = true;
                    }
                    if (pendingMap.size() >= FLUSH_THRESHOLD) {
                        needsGlobalFlush[0] = true;
                    }
                    return list;
                });

        // Flush synchronously (方案 A)
        if (needsSingleFlush[0]) {
            flushSingleKeySync(wrapped);
        } else if (needsGlobalFlush[0]) {
            flushAllSync();
        }
    }

    @Override
    public void addAll(List<V> values) throws Exception {
        if (values == null || values.isEmpty()) {
            return;
        }
        if (!cowEnabled) {
            delegate.addAll(values);
            return;
        }

        // LS-5 fix: copy all values at addAll time
        List<V> copiedValues = new ArrayList<>(values.size());
        for (V v : values) {
            copiedValues.add(copyValue(v));
        }

        NamespaceKeyWrapper wrapped = wrapCurrentKey();
        boolean[] needsGlobalFlush = {false};

        pendingMap.compute(
                wrapped,
                (k, existing) -> {
                    List<V> list = existing != null ? existing : new ArrayList<>();
                    list.addAll(copiedValues);
                    if (pendingMap.size() >= FLUSH_THRESHOLD) {
                        needsGlobalFlush[0] = true;
                    }
                    return list;
                });

        if (needsGlobalFlush[0]) {
            flushAllSync();
        }
    }

    @Override
    public List<V> get() throws Exception {
        List<V> internal = getInternal();
        return internal != null ? new ArrayList<>(internal) : null;
    }

    @Override
    public List<V> getInternal() throws Exception {
        if (!cowEnabled) {
            // passthrough
            Iterable<V> result = delegate.get();
            if (result == null) return null;
            List<V> list = new ArrayList<>();
            for (V item : result) list.add(item);
            return list;
        }

        NamespaceKeyWrapper wrapped = wrapCurrentKey();

        // RYW fast path: key was cleared, all data lives in pendingMap
        if (rywEnabled && clearedKeys.containsKey(wrapped)) {
            List<V> pending = pendingMap.get(wrapped);
            return (pending != null && !pending.isEmpty()) ? new ArrayList<>(pending) : null;
        }

        // Slow path: flush pending then read from delegate
        flushPendingMapSync();

        // Merge any pending data for this key into delegate before reading
        List<V> pendingForKey = pendingMap.remove(wrapped);
        if (pendingForKey != null && !pendingForKey.isEmpty()) {
            // LS-3.1 fix: clearedKeys is only non-null when rywEnabled=true
            boolean wasCleared = false;
            if (rywEnabled) {
                synchronized (clearedKeys) {
                    wasCleared = clearedKeys.remove(wrapped) != null;
                }
            }
            // LS-1/LS-4 fix: set keyContext before delegate call
            K originalKey = currentKeyProvider.getCurrentKey();
            N originalNamespace = currentNamespace;
            try {
                keyContextSetter.accept((K) wrapped.key);
                delegate.setCurrentNamespace((N) wrapped.namespace);
                if (wasCleared) {
                    delegate.updateInternal(pendingForKey);
                } else {
                    delegate.addAll(pendingForKey);
                }
            } finally {
                // restore original context
                keyContextSetter.accept(originalKey);
                delegate.setCurrentNamespace(originalNamespace);
            }
        }

        Iterable<V> result = delegate.get();
        if (result == null) return null;
        List<V> list = new ArrayList<>();
        for (V item : result) list.add(item);
        return list;
    }

    @Override
    public void update(List<V> values) throws Exception {
        updateInternal(values);
    }

    @Override
    public void updateInternal(List<V> values) throws Exception {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");
        if (!cowEnabled) {
            delegate.updateInternal(values);
            return;
        }

        if (!values.isEmpty()) {
            // LS-1/LS-4 fix: flush before replacing, with keyContext set
            flushPendingMapSync();

            NamespaceKeyWrapper wrapped = wrapCurrentKey();
            pendingMap.remove(wrapped);
            if (rywEnabled) {
                clearedKeys.remove(wrapped);
            }
            // LS-1/LS-4 fix: set keyContext
            K originalKey = currentKeyProvider.getCurrentKey();
            N originalNamespace = currentNamespace;
            try {
                keyContextSetter.accept((K) wrapped.key);
                delegate.setCurrentNamespace((N) wrapped.namespace);
                delegate.updateInternal(values);
            } finally {
                keyContextSetter.accept(originalKey);
                delegate.setCurrentNamespace(originalNamespace);
            }
        } else {
            clear();
        }
    }

    @Override
    public void clear() {
        NamespaceKeyWrapper wrapped = wrapCurrentKey();

        // Discard buffered data
        pendingMap.remove(wrapped);

        if (cowEnabled) {
            // LS-3 fix: synchronous flush before marking cleared
            // This ensures the delegate state is also cleared before checkpoint.
            flushPendingMapSync();

            // LS-3.2 fix: delegate.clear() must be called whenever COW is enabled,
            // not just when RYW is enabled. When rywEnabled=true we also track the cleared
            // key for the RYW fast path.
            if (rywEnabled) {
                clearedKeys.put(wrapped, Boolean.TRUE);
            }

            // LS-1/LS-4 fix: set keyContext before delegate.clear()
            K originalKey = currentKeyProvider.getCurrentKey();
            N originalNamespace = currentNamespace;
            try {
                keyContextSetter.accept((K) wrapped.key);
                delegate.setCurrentNamespace((N) wrapped.namespace);
                delegate.clear();
            } finally {
                keyContextSetter.accept(originalKey);
                delegate.setCurrentNamespace(originalNamespace);
            }
        }

        LOG.debug(
                "[CACHEKIT COW] clear() flushed and deleted delegate state for key={}, namespace={}",
                wrapped.key,
                wrapped.namespace);
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (sources == null || sources.isEmpty()) return;
        if (!cowEnabled) {
            delegate.mergeNamespaces(target, sources);
            return;
        }
        // Sync flush before merge to avoid race with pending data
        flushPendingMapSync();
        delegate.mergeNamespaces(target, sources);
    }

    // ------------------------------------------------------------------------
    //  Internal helpers
    // ------------------------------------------------------------------------

    /**
     * LS-5 fix: produce a deep copy of the value using the element serializer.
     * This prevents mutation of the caller's object from corrupting the pending buffer.
     */
    private V copyValue(V value) {
        try {
            return elementSerializer.copy(value);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to copy value for pending buffer", e);
        }
    }

    /**
     * Returns a cached NamespaceKeyWrapper for the current (key, namespace). Reuses the same
     * wrapper instance across calls to avoid allocation pressure on the hot path.
     *
     * <p>LS-5 fix: since pendingMap stores references to the same wrapper objects as its keys,
     * we must NOT return a mutable wrapper whose contents could change after insertion.
     * The wrapper is immutable (final fields) so this is safe.
     */
    private NamespaceKeyWrapper wrapCurrentKey() {
        K key = currentKeyProvider.getCurrentKey();
        if (cachedWrapper != null && cachedWrapper.matches(key, currentNamespace)) {
            return cachedWrapper;
        }
        cachedWrapper = new NamespaceKeyWrapper(currentNamespace, key);
        return cachedWrapper;
    }

    /**
     * LS-3 fix: called when a cleared-key entry is evicted from the LRU clearedKeys map.
     * Flushes a delete to the delegate so that stale data does not reappear.
     */
    private void flushClearedKeyToDelegate(NamespaceKeyWrapper k) {
        try {
            K originalKey = currentKeyProvider.getCurrentKey();
            N originalNamespace = currentNamespace;
            try {
                keyContextSetter.accept((K) k.key);
                delegate.setCurrentNamespace((N) k.namespace);
                delegate.clear();
            } finally {
                keyContextSetter.accept(originalKey);
                delegate.setCurrentNamespace(originalNamespace);
            }
            LOG.debug(
                    "[CACHEKIT COW] ClearedKey evicted, flushed delete to delegate: key={}, ns={}",
                    k.key,
                    k.namespace);
        } catch (Exception e) {
            LOG.warn(
                    "[CACHEKIT COW] Failed to flush cleared-key delete on eviction; "
                            + "stale data may reappear",
                    e);
        }
    }

    /**
     * Synchronous flush of a single key's pending list to the delegate (方案 A).
     * KeyContext is set before the delegate call (LS-1/LS-4 fix).
     */
    private void flushSingleKeySync(NamespaceKeyWrapper wrapped) {
        List<V> snapshot = pendingMap.remove(wrapped);
        if (snapshot == null || snapshot.isEmpty()) return;

        // LS-3.1 fix: clearedKeys is only non-null when rywEnabled=true
        boolean wasCleared = false;
        if (rywEnabled) {
            synchronized (clearedKeys) {
                wasCleared = clearedKeys.remove(wrapped) != null;
            }
        }

        K originalKey = currentKeyProvider.getCurrentKey();
        N originalNamespace = currentNamespace;
        try {
            keyContextSetter.accept((K) wrapped.key);
            delegate.setCurrentNamespace((N) wrapped.namespace);
            if (wasCleared) {
                delegate.updateInternal(snapshot);
            } else {
                delegate.addAll(snapshot);
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to sync-flush ListState single key", e);
        } finally {
            keyContextSetter.accept(originalKey);
            delegate.setCurrentNamespace(originalNamespace);
        }
    }

    /**
     * Synchronous flush of all pending data to the delegate (方案 A).
     * LS-1/LS-4 fix: sets keyContext for each entry before the delegate call.
     */
    private void flushAllSync() {
        ConcurrentHashMap<NamespaceKeyWrapper, List<V>> snapshot = pendingMap;
        pendingMap = new ConcurrentHashMap<>();
        if (snapshot.isEmpty()) return;

        K originalKey = currentKeyProvider.getCurrentKey();
        N originalNamespace = currentNamespace;

        for (Map.Entry<NamespaceKeyWrapper, List<V>> entry : snapshot.entrySet()) {
            List<V> values = entry.getValue();
            if (values == null || values.isEmpty()) continue;

            NamespaceKeyWrapper k = entry.getKey();
            // LS-3.1 fix: clearedKeys is only non-null when rywEnabled=true
            boolean wasCleared = false;
            if (rywEnabled) {
                synchronized (clearedKeys) {
                    wasCleared = clearedKeys.remove(k) != null;
                }
            }

            try {
                keyContextSetter.accept((K) k.key);
                delegate.setCurrentNamespace((N) k.namespace);
                if (wasCleared) {
                    delegate.updateInternal(values);
                } else {
                    delegate.addAll(values);
                }
            } catch (Exception e) {
                throw new FlinkRuntimeException("Failed to sync-flush ListState pending map", e);
            }
        }

        // restore original context
        keyContextSetter.accept(originalKey);
        delegate.setCurrentNamespace(originalNamespace);
    }

    /**
     * Synchronously flush all pending data. Called during getInternal(), updateInternal(),
     * clear(), and checkpoint.
     *
     * <p>LS-1/LS-4 fix: sets keyContext for each entry before the delegate call.
     */
    private void flushPendingMapSync() {
        ConcurrentHashMap<NamespaceKeyWrapper, List<V>> snapshot = pendingMap;
        pendingMap = new ConcurrentHashMap<>();
        if (snapshot.isEmpty()) return;

        K originalKey = currentKeyProvider.getCurrentKey();
        N originalNamespace = currentNamespace;

        for (Map.Entry<NamespaceKeyWrapper, List<V>> entry : snapshot.entrySet()) {
            List<V> values = entry.getValue();
            if (values == null || values.isEmpty()) continue;

            NamespaceKeyWrapper k = entry.getKey();
            // LS-3.1 fix: clearedKeys is only non-null when rywEnabled=true
            boolean wasCleared = false;
            if (rywEnabled) {
                synchronized (clearedKeys) {
                    wasCleared = clearedKeys.remove(k) != null;
                }
            }

            try {
                keyContextSetter.accept((K) k.key);
                delegate.setCurrentNamespace((N) k.namespace);
                if (wasCleared) {
                    delegate.updateInternal(values);
                } else {
                    delegate.addAll(values);
                }
            } catch (Exception e) {
                throw new FlinkRuntimeException("Failed to sync-flush ListState pending map", e);
            }
        }

        // restore original context
        keyContextSetter.accept(originalKey);
        delegate.setCurrentNamespace(originalNamespace);
    }

    // ------------------------------------------------------------------------
    //  Checkpoint / lifecycle
    // ------------------------------------------------------------------------

    /**
     * Flushes all pending data to the delegate state synchronously. Called by the keyed state
     * backend during snapshots.
     *
     * <p>No async Future tracking is needed because all flushes are synchronous.
     */
    public void flushToUnderlyingState() throws IOException {
        try {
            flushPendingMapSync();
        } catch (Exception e) {
            throw new IOException("Failed to flush ListState to delegate", e);
        }
    }

    /** Closes resources. Called by CacheKitKeyedStateBackend.dispose(). */
    public void close() {
        try {
            flushToUnderlyingState();
        } catch (IOException e) {
            LOG.warn("[CACHEKIT COW] Failed to flush on close", e);
        }
    }

    // ------------------------------------------------------------------------
    //  Passthrough delegate methods
    // ------------------------------------------------------------------------

    @Override
    public void setCurrentNamespace(N namespace) {
        this.currentNamespace = namespace;
        delegate.setCurrentNamespace(namespace);
        cachedWrapper = null; // invalidate cached wrapper when namespace changes
    }

    public N getCurrentNamespace() {
        return currentNamespace;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return delegate.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return delegate.getNamespaceSerializer();
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeSerializer<List<V>> getValueSerializer() {
        return (TypeSerializer<List<V>>) (TypeSerializer<?>) listSerializer;
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<List<V>> safeValueSerializer)
            throws Exception {
        // Flush before serialized access to ensure consistency
        flushToUnderlyingState();
        return delegate.getSerializedValue(
                serializedKeyAndNamespace,
                safeKeySerializer,
                safeNamespaceSerializer,
                safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, List<V>> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        // Flush before incremental visitor to ensure consistency
        try {
            flushToUnderlyingState();
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    "Failed to flush caches before creating state incremental visitor", e);
        }
        return delegate.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    public StateSnapshotTransformer<List<V>> getSnapshotTransformer() {
        return null;
    }

    // ------------------------------------------------------------------------
    //  NamespaceKeyWrapper: object-based key for pendingMap (no byte serialization)
    // ------------------------------------------------------------------------

    /**
     * Object-based key for the pending map. Combines (namespace, key) without requiring
     * serialization, avoiding coupling with RocksDB key-group-prefix encoding.
     *
     * <p>Hash is cached at construction time for performance on the hot add/get path.
     * Declared static to avoid referencing outer-class type parameters.
     */
    public static final class NamespaceKeyWrapper {
        private final Object namespace;
        private final Object key;
        private final int hash; // cached

        <N2, K2> NamespaceKeyWrapper(N2 namespace, K2 key) {
            this.namespace = namespace;
            this.key = key;
            int h = Objects.hashCode(namespace);
            h = 31 * h + Objects.hashCode(key);
            this.hash = h;
        }

        <K2, N2> boolean matches(K2 key, N2 namespace) {
            return Objects.equals(this.namespace, namespace) && Objects.equals(this.key, key);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof NamespaceKeyWrapper)) return false;
            NamespaceKeyWrapper other = (NamespaceKeyWrapper) o;
            return Objects.equals(namespace, other.namespace) && Objects.equals(key, other.key);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public String toString() {
            return "NamespaceKeyWrapper{ns=" + namespace + ", key=" + key + '}';
        }
    }
}
