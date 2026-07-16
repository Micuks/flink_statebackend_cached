/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalListState;
import org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalMapState;
import org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalPriorityQueueSet;
import org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalValueState;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PresenceCacheImplementation;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Minimal delegating {@link AbstractKeyedStateBackend} wrapper that adds
 * caching for ValueState.
 *
 * <p>
 * This class is intentionally small and only intercepts
 * {@link #getOrCreateKeyedState} to wrap
 * {@link InternalValueState} instances.
 */
public class CacheKitKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CacheKitKeyedStateBackend.class);

    private final AbstractKeyedStateBackend<K> delegate;
    private final int valueCacheMaxEntries;
    private final CachePolicyType valueCachePolicy;
    private final int valueCacheLruOverflow;
    private final boolean valueBypassEnabled;
    private final double valueHitRateThreshold;
    private final int valueHitRateWindow;
    private final int mapPresenceCacheMaxEntries;
    private final CachePolicyType mapPresenceCachePolicy;
    private final int mapPresenceCacheLruOverflow;
    private final PresenceCacheImplementation mapPresenceCacheImplementation;
    private final int mapCacheMaxEntries;
    private final CachePolicyType mapCachePolicy;
    private final int mapCacheLruOverflow;
    private final boolean mapBypassEnabled;
    private final double mapHitRateThreshold;
    private final int mapHitRateWindow;
    private final boolean mapIterationCacheFillEnabled;
    private final int mapSnapshotCacheMaxEntries;
    private final boolean listStateCowEnabled;
    private final boolean listStateRywEnabled;
    private final int listStateClearedKeysCapacity;
    private final boolean priorityQueueOptEnabled;

    // --- fullOpt: shared flush executors (N wrappers share one thread each) ---
    private final ExecutorService listStateFlushExecutor;
    private final ExecutorService pqFlushExecutor;

    private final Map<Object, Object> wrappersByDelegateIdentity = new IdentityHashMap<>();

    /**
     * Serializes terminal backend operations that Flink may invoke from different task threads.
     *
     * <p>During cancellation, {@code StreamTask} can call {@link #close()} while operator teardown
     * calls {@link #dispose()} concurrently. The latter releases RocksDB column-family handles, so
     * a wrapper flush must finish before delegate disposal begins.
     */
    private final Object lifecycleLock = new Object();

    /** Guarded by {@link #lifecycleLock}. */
    private boolean closed;

    /** Guarded by {@link #lifecycleLock}. */
    private boolean disposed;

    public CacheKitKeyedStateBackend(
            AbstractKeyedStateBackend<K> delegate,
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            CloseableRegistry cancelStreamRegistry,
            int valueCacheMaxEntries,
            CachePolicyType valueCachePolicy,
            int valueCacheLruOverflow,
            boolean valueBypassEnabled,
            double valueHitRateThreshold,
            int valueHitRateWindow,
            int mapPresenceCacheMaxEntries,
            CachePolicyType mapPresenceCachePolicy,
            int mapPresenceCacheLruOverflow,
            PresenceCacheImplementation mapPresenceCacheImplementation,
            int mapCacheMaxEntries,
            CachePolicyType mapCachePolicy,
            int mapCacheLruOverflow,
            boolean mapBypassEnabled,
            double mapHitRateThreshold,
            int mapHitRateWindow,
            boolean mapIterationCacheFillEnabled,
            int mapSnapshotCacheMaxEntries,
            boolean listStateCowEnabled,
            boolean listStateRywEnabled,
            int listStateClearedKeysCapacity,
            boolean priorityQueueOptEnabled) {
        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                Preconditions.checkNotNull(delegate, "delegate").getLatencyTrackingStateConfig(),
                cancelStreamRegistry,
                delegate.getKeyGroupCompressionDecorator(),
                delegate.getKeyContext());

        this.delegate = delegate;
        this.valueCacheMaxEntries = valueCacheMaxEntries;
        this.valueCachePolicy = valueCachePolicy;
        this.valueCacheLruOverflow = valueCacheLruOverflow;
        this.valueBypassEnabled = valueBypassEnabled;
        this.valueHitRateThreshold = valueHitRateThreshold;
        this.valueHitRateWindow = valueHitRateWindow;
        this.mapPresenceCacheMaxEntries = mapPresenceCacheMaxEntries;
        this.mapPresenceCachePolicy = mapPresenceCachePolicy;
        this.mapPresenceCacheLruOverflow = mapPresenceCacheLruOverflow;
        this.mapPresenceCacheImplementation = mapPresenceCacheImplementation;
        this.mapCacheMaxEntries = mapCacheMaxEntries;
        this.mapCachePolicy = mapCachePolicy;
        this.mapCacheLruOverflow = mapCacheLruOverflow;
        this.mapBypassEnabled = mapBypassEnabled;
        this.mapHitRateThreshold = mapHitRateThreshold;
        this.mapHitRateWindow = mapHitRateWindow;
        this.mapIterationCacheFillEnabled = mapIterationCacheFillEnabled;
        this.mapSnapshotCacheMaxEntries = mapSnapshotCacheMaxEntries;
        this.listStateCowEnabled = listStateCowEnabled;
        this.listStateRywEnabled = listStateRywEnabled;
        this.listStateClearedKeysCapacity = listStateClearedKeysCapacity;
        this.priorityQueueOptEnabled = priorityQueueOptEnabled;

        // fullOpt: initialize shared flush executors (daemon threads)
        this.listStateFlushExecutor = listStateCowEnabled
                ? Executors.newSingleThreadExecutor(r -> {
                        Thread t = new Thread(r, "cachekit-list-state-flush");
                        t.setDaemon(true);
                        return t;
                  })
                : null;
        this.pqFlushExecutor = priorityQueueOptEnabled
                ? Executors.newSingleThreadExecutor(r -> {
                        Thread t = new Thread(r, "cachekit-pq-flush");
                        t.setDaemon(true);
                        return t;
                  })
                : null;

        LOG.info(
                "[CACHEKIT fullOpt] Backend created: listStateCow={}, listStateRyw={}, "
                        + "clearedKeysCap={}, priorityQueueOpt={}",
                listStateCowEnabled,
                listStateRywEnabled,
                listStateClearedKeysCapacity,
                priorityQueueOptEnabled);
    }

    @Override
    public void setCurrentKey(K newKey) {
        super.setCurrentKey(newKey);
        delegate.setCurrentKey(newKey);
    }

    @Nonnull
    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <N, S extends State, V> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, V> stateDescriptor)
            throws Exception {
        synchronized (lifecycleLock) {
            ensureOpen();
            return getOrCreateKeyedStateInternal(namespaceSerializer, stateDescriptor);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <N, S extends State, V> S getOrCreateKeyedStateInternal(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, V> stateDescriptor)
            throws Exception {
        S state = delegate.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        if (!(state instanceof InternalKvState)) {
            return state;
        }

        InternalKvState<K, N, ?> internal = (InternalKvState<K, N, ?>) state;
        if (stateDescriptor.getType() == StateDescriptor.Type.VALUE
                && internal instanceof InternalValueState
                && valueCacheMaxEntries > 0) {
            Object existing = wrappersByDelegateIdentity.get(internal);
            if (existing != null) {
                return (S) existing;
            }
            InternalValueState<K, N, V> delegateValue = (InternalValueState<K, N, V>) internal;
            CachedInternalValueState<K, N, V> wrapped = new CachedInternalValueState<>(
                    delegateValue,
                    this::getCurrentKey,
                    this::setCurrentKey,
                    valueCacheMaxEntries,
                    valueCachePolicy,
                    valueCacheLruOverflow,
                    valueBypassEnabled,
                    valueHitRateThreshold,
                    valueHitRateWindow,
                    BP_PREFETCH_LAZY_MATERIALIZATION);
            wrappersByDelegateIdentity.put(internal, wrapped);
            return (S) wrapped;
        }

        if (stateDescriptor.getType() == StateDescriptor.Type.MAP
                && internal instanceof InternalMapState
                && (mapPresenceCacheMaxEntries > 0 || mapCacheMaxEntries > 0 || mapSnapshotCacheMaxEntries > 0)) {
            Object existing = wrappersByDelegateIdentity.get(internal);
            if (existing != null) {
                return (S) existing;
            }
            InternalMapState<K, N, Object, Object> delegateMap = (InternalMapState<K, N, Object, Object>) internal;
            CachedInternalMapState<K, N, Object, Object> wrapped = new CachedInternalMapState<>(
                    delegateMap,
                    this::getCurrentKey,
                    this::setCurrentKey,
                    mapPresenceCacheMaxEntries,
                    mapPresenceCachePolicy,
                    mapPresenceCacheLruOverflow,
                    mapPresenceCacheImplementation,
                    mapCacheMaxEntries,
                    mapCachePolicy,
                    mapCacheLruOverflow,
                    mapBypassEnabled,
                    mapHitRateThreshold,
                    mapHitRateWindow,
                    mapIterationCacheFillEnabled,
                    mapSnapshotCacheMaxEntries);
            wrappersByDelegateIdentity.put(internal, wrapped);
            return (S) wrapped;
        }

        // fullOpt: ListState COW + RYW wrapper
        if (stateDescriptor.getType() == StateDescriptor.Type.LIST
                && internal instanceof InternalListState
                && (listStateCowEnabled || listStateRywEnabled)) {
            Object existing = wrappersByDelegateIdentity.get(internal);
            if (existing != null) {
                return (S) existing;
            }
            @SuppressWarnings("unchecked")
            InternalListState<K, N, Object> delegateList =
                    (InternalListState<K, N, Object>) internal;
            // Element serializer from ListStateDescriptor (not from delegate API)
            @SuppressWarnings("unchecked")
            TypeSerializer<Object> elementSerializer =
                    (TypeSerializer<Object>)
                            ((ListStateDescriptor<?>) stateDescriptor).getElementSerializer();
            Preconditions.checkNotNull(
                    elementSerializer,
                    "ListState must have an element serializer configured");
            CachedInternalListState<K, N, Object> wrapped =
                    new CachedInternalListState<>(
                            delegateList,
                            this::getCurrentKey,
                            this::setCurrentKey,
                            listStateCowEnabled,
                            listStateRywEnabled,
                            elementSerializer,
                            listStateFlushExecutor,
                            listStateClearedKeysCapacity);
            wrappersByDelegateIdentity.put(internal, wrapped);
            return (S) wrapped;
        }

        return state;
    }

    @Override
    public <N> Stream<K> getKeys(String stateName, N namespace) {
        return delegate.getKeys(stateName, namespace);
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String stateName) {
        return delegate.getKeysAndNamespaces(stateName);
    }

    @Override
    public int numKeyValueStateEntries() {
        return delegate.numKeyValueStateEntries();
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, SV> stateDesc,
            StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> stateSnapshotTransformFactory)
            throws Exception {
        synchronized (lifecycleLock) {
            ensureOpen();
            return createOrUpdateInternalStateInternal(
                    namespaceSerializer, stateDesc, stateSnapshotTransformFactory);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalStateInternal(
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, SV> stateDesc,
            StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> stateSnapshotTransformFactory)
            throws Exception {
        IS state = delegate.createOrUpdateInternalState(
                namespaceSerializer, stateDesc, stateSnapshotTransformFactory);

        // Wrap ValueState with cache layer
        if (!(state instanceof InternalKvState)) {
            return state;
        }

        InternalKvState<K, N, ?> internal = (InternalKvState<K, N, ?>) state;
        if (stateDesc.getType() == StateDescriptor.Type.VALUE
                && internal instanceof InternalValueState
                && valueCacheMaxEntries > 0) {
            Object existing = wrappersByDelegateIdentity.get(internal);
            if (existing != null) {
                return (IS) existing;
            }
            InternalValueState<K, N, SV> delegateValue = (InternalValueState<K, N, SV>) internal;
            CachedInternalValueState<K, N, SV> wrapped = new CachedInternalValueState<>(
                    delegateValue,
                    this::getCurrentKey,
                    this::setCurrentKey,
                    valueCacheMaxEntries,
                    valueCachePolicy,
                    valueCacheLruOverflow,
                    valueBypassEnabled,
                    valueHitRateThreshold,
                    valueHitRateWindow,
                    BP_PREFETCH_LAZY_MATERIALIZATION);
            wrappersByDelegateIdentity.put(internal, wrapped);
            return (IS) wrapped;
        }

        if (stateDesc.getType() == StateDescriptor.Type.MAP
                && internal instanceof InternalMapState
                && (mapPresenceCacheMaxEntries > 0 || mapCacheMaxEntries > 0 || mapSnapshotCacheMaxEntries > 0)) {
            Object existing = wrappersByDelegateIdentity.get(internal);
            if (existing != null) {
                return (IS) existing;
            }
            InternalMapState<K, N, Object, Object> delegateMap = (InternalMapState<K, N, Object, Object>) internal;
            CachedInternalMapState<K, N, Object, Object> wrapped = new CachedInternalMapState<>(
                    delegateMap,
                    this::getCurrentKey,
                    this::setCurrentKey,
                    mapPresenceCacheMaxEntries,
                    mapPresenceCachePolicy,
                    mapPresenceCacheLruOverflow,
                    mapPresenceCacheImplementation,
                    mapCacheMaxEntries,
                    mapCachePolicy,
                    mapCacheLruOverflow,
                    mapBypassEnabled,
                    mapHitRateThreshold,
                    mapHitRateWindow,
                    mapIterationCacheFillEnabled,
                    mapSnapshotCacheMaxEntries);
            wrappersByDelegateIdentity.put(internal, wrapped);
            return (IS) wrapped;
        }

        // fullOpt: ListState COW + RYW wrapper
        if (stateDesc.getType() == StateDescriptor.Type.LIST
                && internal instanceof InternalListState
                && (listStateCowEnabled || listStateRywEnabled)) {
            Object existing = wrappersByDelegateIdentity.get(internal);
            if (existing != null) {
                return (IS) existing;
            }
            @SuppressWarnings("unchecked")
            InternalListState<K, N, Object> delegateList =
                    (InternalListState<K, N, Object>) internal;
            @SuppressWarnings("unchecked")
            TypeSerializer<Object> elementSerializer =
                    (TypeSerializer<Object>)
                            ((ListStateDescriptor<?>) stateDesc).getElementSerializer();
            Preconditions.checkNotNull(
                    elementSerializer,
                    "ListState must have an element serializer configured");
            CachedInternalListState<K, N, Object> wrapped =
                    new CachedInternalListState<>(
                            delegateList,
                            this::getCurrentKey,
                            this::setCurrentKey,
                            listStateCowEnabled,
                            listStateRywEnabled,
                            elementSerializer,
                            listStateFlushExecutor,
                            listStateClearedKeysCapacity);
            wrappersByDelegateIdentity.put(internal, wrapped);
            return (IS) wrapped;
        }

        return state;
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>> KeyGroupedInternalPriorityQueue<T> create(
            String stateName, TypeSerializer<T> byteOrderedElementSerializer) {
        synchronized (lifecycleLock) {
            ensureOpen();
            KeyGroupedInternalPriorityQueue<T> delegateQueue =
                    delegate.create(stateName, byteOrderedElementSerializer);
            return wrapPriorityQueue(delegateQueue, byteOrderedElementSerializer);
        }
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>> KeyGroupedInternalPriorityQueue<T> create(
            String stateName, TypeSerializer<T> byteOrderedElementSerializer, boolean allowFutureMetadataUpdates) {
        synchronized (lifecycleLock) {
            ensureOpen();
            KeyGroupedInternalPriorityQueue<T> delegateQueue =
                    delegate.create(
                            stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
            return wrapPriorityQueue(delegateQueue, byteOrderedElementSerializer);
        }
    }

    /**
     * fullOpt: wrap PriorityQueue with async buffer if enabled and delegate is Heap-backed.
     * RocksDB-backed PQ already has its own async buffer; wrapping would cause double-buffering.
     *
     * <p>We use a Heap whitelist (instanceof HeapPriorityQueueSet) instead of a RocksDB blacklist.
     * This correctly handles:
     * <ul>
     *   <li>RocksDB Timer → KeyGroupPartitionedPriorityQueue (non-Heap) → skipped ✅
     *   <li>RocksDB configured with Heap Timer → HeapPriorityQueueSet → optimized ✅
     *   <li>Heap Timer → HeapPriorityQueueSet → optimized ✅
     *   <li>Unrecognized new implementations → skipped by default ✅
     * </ul>
     */
    private <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> wrapPriorityQueue(
                    KeyGroupedInternalPriorityQueue<T> delegateQueue,
                    TypeSerializer<T> elementSerializer) {
        if (!priorityQueueOptEnabled) {
            return delegateQueue;
        }
        // PQ-1 fix: use Heap whitelist instead of RocksDB blacklist
        if (!(delegateQueue instanceof HeapPriorityQueueSet)) {
            LOG.info(
                    "[CACHEKIT PQ] Non-Heap PriorityQueue detected ({}); "
                            + "skipping wrapper to avoid double-buffering or unsupported backend.",
                    delegateQueue.getClass().getName());
            return delegateQueue;
        }
        // PQ-3.3 fix: register the PQ wrapper so snapshot/close/dispose can flush it
        CachedInternalPriorityQueueSet<T> pqWrapper =
                new CachedInternalPriorityQueueSet<>(
                        delegateQueue, elementSerializer, pqFlushExecutor, priorityQueueOptEnabled);
        wrappersByDelegateIdentity.put(pqWrapper, pqWrapper);
        return pqWrapper;
    }

    @Override
    public void dispose() {
        synchronized (lifecycleLock) {
            if (disposed) {
                return;
            }
            disposed = true;
            closed = true;
            try {
                super.dispose();
            } finally {
                closeWrappers();
                shutdownFlushExecutors();
                delegate.dispose();
            }
        }
    }

    private void closeWrappers() {
        for (Object wrapper : wrappersByDelegateIdentity.values()) {
            try {
                if (wrapper instanceof CachedInternalValueState) {
                    // Quiesce async bp-prefetch BEFORE delegate.dispose()/close() frees the
                    // RocksDB db + ColumnFamilyHandles (else a shared-executor prefetch worker
                    // reads a freed handle and SIGSEGVs in librocksdbjni).
                    ((CachedInternalValueState<?, ?, ?>) wrapper).close();
                } else if (wrapper instanceof CachedInternalMapState) {
                    ((CachedInternalMapState<?, ?, ?, ?>) wrapper).close();
                } else if (wrapper instanceof CachedInternalListState) {
                    ((CachedInternalListState<?, ?, ?>) wrapper).close();
                } else if (wrapper instanceof CachedInternalPriorityQueueSet) {
                    ((CachedInternalPriorityQueueSet<?>) wrapper).close();
                }
            } catch (Exception ignored) {
                // log and continue
            }
        }
        wrappersByDelegateIdentity.clear();
    }

    private void shutdownFlushExecutors() {
        if (listStateFlushExecutor != null) {
            listStateFlushExecutor.shutdownNow();
        }
        if (pqFlushExecutor != null) {
            pqFlushExecutor.shutdownNow();
        }
    }

    private void ensureOpen() {
        if (closed || disposed) {
            throw new IllegalStateException("CacheKit keyed state backend is closed.");
        }
    }

    /**
     * When true (default), ValueState prefetch is submitted to the shared off-mailbox worker
     * thread instead of being executed synchronously in the mailbox critical path.
     */
    private static final boolean BP_PREFETCH_ASYNC =
            loadBooleanFlag("state.backend.cachekit.bp-prefetch.async.enabled", true);

    /** Defers prefetched value deserialization until the mailbox actually consumes the value. */
    private static final boolean BP_PREFETCH_LAZY_MATERIALIZATION =
            loadBooleanFlag(
                    "state.backend.cachekit.bp-prefetch.lazy-materialization.enabled", false);

    /**
     * MapState snapshot prefetch is disabled by default: it flushes dirty entries and pays a full
     * RocksDB prefix-iterator per key on the mailbox thread, while only single-entry maps are ever
     * cached — a net loss on multi-entry (join) MapState.
     */
    private static final boolean BP_PREFETCH_MAP_SNAPSHOTS =
            loadBooleanFlag("state.backend.cachekit.bp-prefetch.map-snapshots.enabled", false);

    private static boolean loadBooleanFlag(String key, boolean defaultValue) {
        try {
            return org.apache.flink.configuration.GlobalConfiguration.loadConfiguration()
                    .get(
                            org.apache.flink.configuration.ConfigOptions.key(key)
                                    .booleanType()
                                    .defaultValue(defaultValue));
        } catch (Throwable t) {
            return defaultValue;
        }
    }

    /**
     * True when at least one cached-state wrapper exists, i.e. a prefetch could land somewhere.
     * Called reflectively by StatePrefetcher BEFORE it pays the per-batch key extraction: window
     * operators only hold namespaced states (never wrapped under the VoidNamespace gate), so
     * without this check every lookahead batch would extract and dedup up to `distance` keys for
     * a prefetch that is guaranteed to be a no-op. Wrappers register lazily on first state
     * access, so this must be re-evaluated per call, not cached by the caller.
     */
    public boolean hasPrefetchableState() {
        synchronized (lifecycleLock) {
            return !closed && !disposed && !wrappersByDelegateIdentity.isEmpty();
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void prefetch(Collection<? extends K> keys) {
        synchronized (lifecycleLock) {
            if (closed || disposed || keys == null || keys.isEmpty() || wrappersByDelegateIdentity.isEmpty()) {
                return;
            }
            if (BP_PREFETCH_ASYNC) {
                // Off-mailbox path: only (key, namespace) serialization happens here; RocksDB reads
                // and value deserialization run on the shared prefetch worker. No key-context
                // save/restore needed — submission never touches the backend key context.
                for (Object wrapper : wrappersByDelegateIdentity.values()) {
                    if (wrapper instanceof CachedInternalValueState) {
                        Runnable task =
                                ((CachedInternalValueState) wrapper).buildAsyncPrefetchTask(keys);
                        if (task != null) {
                            PrefetchExecutor.trySubmit(task);
                        }
                    }
                }
                if (!BP_PREFETCH_MAP_SNAPSHOTS) {
                    return;
                }
            }
            K previousKey = getCurrentKey();
            try {
                for (Object wrapper : wrappersByDelegateIdentity.values()) {
                    if (!BP_PREFETCH_ASYNC && wrapper instanceof CachedInternalValueState) {
                        ((CachedInternalValueState) wrapper).prefetch(keys);
                    } else if (BP_PREFETCH_MAP_SNAPSHOTS
                            && wrapper instanceof CachedInternalMapState) {
                        ((CachedInternalMapState) wrapper).prefetchSnapshots(keys);
                    }
                }
            } catch (Throwable ignored) {
                // Best-effort cache warmup. Authoritative state access remains unchanged.
            } finally {
                setCurrentKey(previousKey);
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (lifecycleLock) {
            if (closed || disposed) {
                return;
            }
            closed = true;
            try {
                flushWrappers();
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException("Failed to flush CacheKit state wrappers before close.", e);
            } finally {
                closeWrappers();
                shutdownFlushExecutors();
                delegate.close();
            }
        }
    }

    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {
        synchronized (lifecycleLock) {
            ensureOpen();
            flushWrappers();
            return delegate.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
        }
    }

    private void flushWrappers() throws Exception {
        List<Exception> flushErrors = new ArrayList<>();
        for (Object wrapper : wrappersByDelegateIdentity.values()) {
            try {
                if (wrapper instanceof CachedInternalValueState) {
                    ((CachedInternalValueState<?, ?, ?>) wrapper).flush();
                } else if (wrapper instanceof CachedInternalMapState) {
                    ((CachedInternalMapState<?, ?, ?, ?>) wrapper).flush();
                } else if (wrapper instanceof CachedInternalListState) {
                    ((CachedInternalListState<?, ?, ?>) wrapper).flushToUnderlyingState();
                } else if (wrapper instanceof CachedInternalPriorityQueueSet) {
                    // PQ-2.5 fix: checkpoint must flush all pending PQ operations
                    ((CachedInternalPriorityQueueSet<?>) wrapper).flushAllPending();
                }
            } catch (Exception e) {
                flushErrors.add(e);
            }
        }
        if (!flushErrors.isEmpty()) {
            Exception first = flushErrors.get(0);
            for (int i = 1; i < flushErrors.size(); i++) {
                first.addSuppressed(flushErrors.get(i));
            }
            throw new FlinkRuntimeException(
                    "Failed to flush one or more cached states before snapshot", first);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        delegate.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        delegate.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void notifyCheckpointSubsumed(long checkpointId) throws Exception {
        delegate.notifyCheckpointSubsumed(checkpointId);
    }

    @Override
    public SavepointResources<K> savepoint() throws Exception {
        return delegate.savepoint();
    }
}
