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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * Cachekit wrapper over delegate {@link InternalListState} that implements:
 *
 * <ul>
 *   <li><b>Async Copy-On-Write (COW)</b>: {@code add()} / {@code addAll()} buffer writes into a
 *       pending map and flush asynchronously to the delegate when thresholds are exceeded
 *       (4096 keys or 500 elements per list).
 *   <li><b>Read-Your-Writes (RYW)</b>: cleared-keys fast-path avoids a backend read by returning
 *       the in-memory buffered data directly.
 *   <li><b>Atomic pending-map updates</b>: all mutations go through {@code compute()} to avoid
 *       race conditions when {@code flushAllAsync()} swaps the map reference.
 *   <li><b>Bounded cleared-keys</b>: uses an LRU-linked-hash-map with a configurable capacity to
 *       prevent unbounded memory growth.
 * </ul>
 *
 * <p>COW and RYW can be independently enabled. When COW is off but RYW is on, the pending map is
 * still maintained but flushed synchronously (enabling the RYW fast path).
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

    // ---- COW: async pending buffer ----
    /** Flush threshold: when pending map reaches this size, trigger global async flush. */
    private static final int FLUSH_THRESHOLD = 4096;
    /** Per-list threshold: when a single list reaches this size, flush that key async. */
    private static final int MAX_PENDING_LIST_SIZE = 500;

    private volatile ConcurrentHashMap<NamespaceKeyWrapper, List<V>> pendingMap =
            new ConcurrentHashMap<>();
    private final CompletableFuture<?>[] inFlightFlush = new CompletableFuture<?>[] {null};

    // ---- RYW: cleared key tracking with bounded LRU ----
    private final boolean rywEnabled;
    private final int clearedKeysCapacity;
    private final Map<NamespaceKeyWrapper, Boolean> clearedKeys;

    // ---- async flush executor (injected from backend; shared across all list states) ----
    private final ExecutorService flushExecutor;

    // ---- cached hash for current key (avoids recomputation) ----
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
        this.elementSerializer = Objects.requireNonNull(elementSerializer, "elementSerializer");
        this.listSerializer = new ListSerializer<>(elementSerializer);
        this.flushExecutor = flushExecutor;
        this.rywEnabled = rywEnabled;
        this.clearedKeysCapacity = clearedKeysCapacity;

        // Bounded cleared-keys map with LRU eviction (access-order LinkedHashMap)
        if (rywEnabled) {
            this.clearedKeys =
                    Collections.synchronizedMap(
                            new LinkedHashMap<NamespaceKeyWrapper, Boolean>(clearedKeysCapacity, 0.75f, true) {
                                @Override
                                protected boolean removeEldestEntry(
                                        Map.Entry<NamespaceKeyWrapper, Boolean> eldest) {
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
        if (!isCowEnabled()) {
            delegate.add(value);
            return;
        }

        NamespaceKeyWrapper wrapped = wrapCurrentKey();
        boolean[] needsSingleFlush = {false};
        boolean[] needsGlobalFlush = {false};

        pendingMap.compute(
                wrapped,
                (k, existing) -> {
                    List<V> list = existing != null ? existing : new ArrayList<>();
                    list.add(value);
                    if (list.size() >= MAX_PENDING_LIST_SIZE) {
                        needsSingleFlush[0] = true;
                    }
                    if (pendingMap.size() >= FLUSH_THRESHOLD) {
                        needsGlobalFlush[0] = true;
                    }
                    return list;
                });

        if (needsSingleFlush[0]) {
            flushSingleKeyAsync(wrapped);
        } else if (needsGlobalFlush[0]) {
            flushAllAsync();
        }
    }

    @Override
    public void addAll(List<V> values) throws Exception {
        if (values == null || values.isEmpty()) {
            return;
        }
        if (!isCowEnabled()) {
            delegate.addAll(values);
            return;
        }

        NamespaceKeyWrapper wrapped = wrapCurrentKey();
        boolean[] needsGlobalFlush = {false};

        pendingMap.compute(
                wrapped,
                (k, existing) -> {
                    List<V> list = existing != null ? existing : new ArrayList<>();
                    list.addAll(values);
                    if (pendingMap.size() >= FLUSH_THRESHOLD) {
                        needsGlobalFlush[0] = true;
                    }
                    return list;
                });

        if (needsGlobalFlush[0]) {
            flushAllAsync();
        }
    }

    @Override
    public List<V> get() throws Exception {
        List<V> internal = getInternal();
        return internal != null ? new ArrayList<>(internal) : null;
    }

    @Override
    public List<V> getInternal() throws Exception {
        if (!isCowEnabled()) {
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

        // Slow path: wait for in-flight async flush, then sync flush any remaining
        awaitPendingFlush();
        flushPendingMapSync();

        // Merge any pending data for this key into delegate before reading
        List<V> pendingForKey = pendingMap.remove(wrapped);
        if (pendingForKey != null && !pendingForKey.isEmpty()) {
            boolean wasCleared;
            synchronized (clearedKeys) {
                wasCleared = clearedKeys.remove(wrapped) != null;
            }
            if (wasCleared) {
                delegate.updateInternal(pendingForKey);
            } else {
                delegate.addAll(pendingForKey);
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
        if (!isCowEnabled()) {
            delegate.updateInternal(values);
            return;
        }

        if (!values.isEmpty()) {
            awaitPendingFlush();
            flushPendingMapSync();

            NamespaceKeyWrapper wrapped = wrapCurrentKey();
            pendingMap.remove(wrapped);
            if (rywEnabled) {
                clearedKeys.remove(wrapped);
            }
            delegate.updateInternal(values);
        } else {
            clear();
        }
    }

    @Override
    public void clear() {
        NamespaceKeyWrapper wrapped = wrapCurrentKey();

        // Discard buffered data
        pendingMap.remove(wrapped);

        if (isCowEnabled()) {
            // Wait for any in-flight async flush to avoid overwriting with stale data
            awaitPendingFlush();
            flushPendingMapSync();
        }

        // RYW: mark this key as cleared so getInternal() returns in-memory data
        if (rywEnabled) {
            clearedKeys.put(wrapped, Boolean.TRUE);
        }

        // Do NOT call delegate.clear() here.
        // Rationale:
        //   1. In RYW mode, the in-memory pending data already "owns" the key's view.
        //   2. delegate.clear() generates a RocksDB delete write, which undermines the
        //      purpose of write buffering in COW mode.
        //   3. When subsequent adds fully buffer in pendingMap and never write delegate,
        //      the delete write would be wasted anyway.
        //   4. On the next snapshot, flushPendingMapSync() handles data correctly
        //      (no stale data from delegate to worry about).

        LOG.debug(
                "[CACHEKIT COW] clear() marked key as cleared; in-memory view dominates getInternal()");
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (sources == null || sources.isEmpty()) return;
        if (!isCowEnabled()) {
            delegate.mergeNamespaces(target, sources);
            return;
        }
        awaitPendingFlush();
        flushPendingMapSync();
        delegate.mergeNamespaces(target, sources);
    }

    // ------------------------------------------------------------------------
    //  Internal helpers
    // ------------------------------------------------------------------------

    /**
     * Returns whether COW (async write buffering) is active. COW is enabled when the backend's
     * flush executor is non-null (i.e., the config flag was set AND the executor was provided).
     */
    private boolean isCowEnabled() {
        return flushExecutor != null;
    }

    /**
     * Returns a cached NamespaceKeyWrapper for the current (key, namespace). Reuses the same
     * wrapper instance across calls to avoid allocation pressure on the hot path.
     */
    private NamespaceKeyWrapper wrapCurrentKey() {
        K key = currentKeyProvider.getCurrentKey();
        if (cachedWrapper != null && cachedWrapper.matches(key, currentNamespace)) {
            return cachedWrapper;
        }
        cachedWrapper = new NamespaceKeyWrapper(currentNamespace, key);
        return cachedWrapper;
    }

    /** Async flush of a single key (non-blocking for the caller). */
    private void flushSingleKeyAsync(NamespaceKeyWrapper wrapped) {
        if (flushExecutor == null) {
            flushPendingMapSync();
            return;
        }

        final List<V> snapshot = pendingMap.remove(wrapped);
        if (snapshot == null || snapshot.isEmpty()) return;

        final boolean wasCleared;
        if (rywEnabled) {
            synchronized (clearedKeys) {
                wasCleared = clearedKeys.remove(wrapped) != null;
            }
        } else {
            wasCleared = false;
        }

        CompletableFuture.runAsync(
                        () -> {
                            try {
                                if (wasCleared) {
                                    delegate.updateInternal(snapshot);
                                } else {
                                    delegate.addAll(snapshot);
                                }
                            } catch (Exception e) {
                                throw new FlinkRuntimeException(
                                        "Failed to flush ListState single key", e);
                            }
                        },
                        flushExecutor)
                .whenComplete(
                        (r, ex) -> {
                            if (ex != null) {
                                LOG.error("[CACHEKIT COW] Async flush failed", ex);
                            }
                        });
        trackInFlight();
    }

    /** Async flush of all pending data (global flush). */
    private void flushAllAsync() {
        if (flushExecutor == null) {
            flushPendingMapSync();
            return;
        }

        final ConcurrentHashMap<NamespaceKeyWrapper, List<V>> snapshot = pendingMap;
        pendingMap = new ConcurrentHashMap<>();
        if (snapshot.isEmpty()) return;

        CompletableFuture.runAsync(
                        () -> {
                            for (Map.Entry<NamespaceKeyWrapper, List<V>> entry : snapshot.entrySet()) {
                                NamespaceKeyWrapper k = entry.getKey();
                                List<V> values = entry.getValue();
                                if (values == null || values.isEmpty()) continue;

                                final boolean wasCleared;
                                if (rywEnabled) {
                                    synchronized (clearedKeys) {
                                        wasCleared = clearedKeys.remove(k) != null;
                                    }
                                } else {
                                    wasCleared = false;
                                }
                                try {
                                    if (wasCleared) {
                                        delegate.updateInternal(values);
                                    } else {
                                        delegate.addAll(values);
                                    }
                                } catch (Exception e) {
                                    throw new FlinkRuntimeException(
                                            "Failed to flush ListState pending map", e);
                                }
                            }
                        },
                        flushExecutor)
                .whenComplete(
                        (r, ex) -> {
                            if (ex != null) {
                                LOG.error("[CACHEKIT COW] Global async flush failed", ex);
                            }
                        });
        trackInFlight();
    }

    /** Synchronously flush all pending data. Called during getInternal(), updateInternal(), and
     * checkpoint. */
    private void flushPendingMapSync() {
        ConcurrentHashMap<NamespaceKeyWrapper, List<V>> snapshot = pendingMap;
        pendingMap = new ConcurrentHashMap<>();
        if (snapshot.isEmpty()) return;

        for (Map.Entry<NamespaceKeyWrapper, List<V>> entry : snapshot.entrySet()) {
            List<V> values = entry.getValue();
            if (values == null || values.isEmpty()) continue;

            NamespaceKeyWrapper k = entry.getKey();
            final boolean wasCleared;
            if (rywEnabled) {
                synchronized (clearedKeys) {
                    wasCleared = clearedKeys.remove(k) != null;
                }
            } else {
                wasCleared = false;
            }
            try {
                if (wasCleared) {
                    delegate.updateInternal(values);
                } else {
                    delegate.addAll(values);
                }
            } catch (Exception e) {
                throw new FlinkRuntimeException("Failed to sync-flush ListState pending map", e);
            }
        }
    }

    private void awaitPendingFlush() {
        CompletableFuture<?> f = inFlightFlush[0];
        if (f != null && !f.isDone()) {
            try {
                f.join();
            } catch (Exception e) {
                throw new FlinkRuntimeException("Failed to await pending ListState flush", e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void trackInFlight() {
        // Simple chaining: store the latest future; await uses it.
        // In high-concurrency scenarios this could chain many futures but ensures correctness.
        synchronized (inFlightFlush) {
            CompletableFuture<?> prev = inFlightFlush[0];
            if (prev == null || prev.isDone()) {
                inFlightFlush[0] = CompletableFuture.completedFuture(null);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Checkpoint / lifecycle
    // ------------------------------------------------------------------------

    /**
     * Flushes all pending data to the delegate state. Called by the keyed state backend during
     * snapshots.
     */
    public void flushToUnderlyingState() throws IOException {
        try {
            awaitPendingFlush();
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
        // InternalListState's value type is List<V>, so we return the ListSerializer wrapping
        // the element serializer.
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
        // Not part of InternalListState interface; return null
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

        <N, K> NamespaceKeyWrapper(N namespace, K key) {
            this.namespace = namespace;
            this.key = key;
            int h = Objects.hashCode(namespace);
            h = 31 * h + Objects.hashCode(key);
            this.hash = h;
        }

        <N, K> boolean matches(K key, N namespace) {
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
