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
import org.apache.flink.contrib.streaming.state.RocksDBBatchValueReader;
import org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalListState;
import org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalMapState;
import org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalPriorityQueueSet;
import org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalValueState;
import org.apache.flink.contrib.streaming.state.cachekit.state.MapSnapshotCacheMetrics;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneCoordinator;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneOptions;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PresenceCacheImplementation;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
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
    private final MapSnapshotCacheMetrics mapSnapshotCacheMetrics;
    private final NativeRequestPlaneCoordinator nativeRequestPlaneCoordinator;
    private int nextNativeStateId = 1;

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
            MetricGroup metricGroup,
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
            boolean priorityQueueOptEnabled,
            boolean diagnosticsEnabled) {
        this(
                delegate,
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                cancelStreamRegistry,
                metricGroup,
                valueCacheMaxEntries,
                valueCachePolicy,
                valueCacheLruOverflow,
                valueBypassEnabled,
                valueHitRateThreshold,
                valueHitRateWindow,
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
                mapSnapshotCacheMaxEntries,
                listStateCowEnabled,
                listStateRywEnabled,
                listStateClearedKeysCapacity,
                priorityQueueOptEnabled,
                diagnosticsEnabled,
                NativeRequestPlaneOptions.disabled());
    }

    public CacheKitKeyedStateBackend(
            AbstractKeyedStateBackend<K> delegate,
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            CloseableRegistry cancelStreamRegistry,
            MetricGroup metricGroup,
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
            boolean priorityQueueOptEnabled,
            boolean diagnosticsEnabled,
            NativeRequestPlaneOptions nativeRequestPlaneOptions) {
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
        this.mapSnapshotCacheMetrics =
                MapSnapshotCacheMetrics.create(metricGroup, diagnosticsEnabled);
        Preconditions.checkNotNull(nativeRequestPlaneOptions, "nativeRequestPlaneOptions");
        Preconditions.checkArgument(
                !nativeRequestPlaneOptions.enabled()
                        || valueCacheMaxEntries > 0
                        || nativeRequestPlaneOptions.mapCacheEnabled()
                        || nativeRequestPlaneOptions.mapSnapshotEnabled(),
                "CacheKit ValueState native features require a positive ValueState cache; "
                        + "only isolated MapState native features may run without it.");

        // fullOpt: initialize shared flush executors (daemon threads)
        ExecutorService initializedListExecutor = null;
        ExecutorService initializedPqExecutor = null;
        NativeRequestPlaneCoordinator initializedNativeCoordinator = null;
        try {
            initializedListExecutor =
                    listStateCowEnabled
                            ? Executors.newSingleThreadExecutor(
                                    r -> {
                                        Thread t =
                                                new Thread(r, "cachekit-list-state-flush");
                                        t.setDaemon(true);
                                        return t;
                                    })
                            : null;
            initializedPqExecutor =
                    priorityQueueOptEnabled
                            ? Executors.newSingleThreadExecutor(
                                    r -> {
                                        Thread t = new Thread(r, "cachekit-pq-flush");
                                        t.setDaemon(true);
                                        return t;
                                    })
                            : null;
            initializedNativeCoordinator =
                    NativeRequestPlaneCoordinator.open(nativeRequestPlaneOptions);
        } catch (RuntimeException | Error failure) {
            if (initializedNativeCoordinator != null) {
                initializedNativeCoordinator.close();
            }
            if (initializedListExecutor != null) {
                initializedListExecutor.shutdownNow();
            }
            if (initializedPqExecutor != null) {
                initializedPqExecutor.shutdownNow();
            }
            throw failure;
        }
        this.listStateFlushExecutor = initializedListExecutor;
        this.pqFlushExecutor = initializedPqExecutor;
        this.nativeRequestPlaneCoordinator = initializedNativeCoordinator;

        LOG.info(
                "[CACHEKIT fullOpt] Backend created: listStateCow={}, listStateRyw={}, "
                        + "clearedKeysCap={}, priorityQueueOpt={}, nativeRequestPlane={}, "
                        + "nativeKernel={}, nativeFeatureBits={}, nativeFeatures={}",
                listStateCowEnabled,
                listStateRywEnabled,
                listStateClearedKeysCapacity,
                priorityQueueOptEnabled,
                nativeRequestPlaneCoordinator != null,
                nativeRequestPlaneCoordinator == null
                        ? "disabled"
                        : nativeRequestPlaneCoordinator.selectedKernel(),
                nativeRequestPlaneCoordinator == null
                        ? "0x0000000000000000"
                        : nativeRequestPlaneCoordinator.detectedFeatureBitsHex(),
                nativeRequestPlaneCoordinator == null
                        ? "disabled"
                        : nativeRequestPlaneCoordinator.detectedFeatures());
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
            requireNativeCapableValueState(delegateValue);
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
                    BP_PREFETCH_MULTIGET,
                    VALUE_STICKY_UPDATE_IN_PLACE,
                    VALUE_LAZY_STAGING,
                    nativeRequestPlaneCoordinator,
                    allocateNativeStateId());
            wrappersByDelegateIdentity.put(internal, wrapped);
            return (S) wrapped;
        }

        if (stateDescriptor.getType() == StateDescriptor.Type.MAP
                && internal instanceof InternalMapState
                && (mapPresenceCacheMaxEntries > 0
                        || mapCacheMaxEntries > 0
                        || mapSnapshotCacheMaxEntries > 0
                        || (nativeRequestPlaneCoordinator != null
                                && (nativeRequestPlaneCoordinator.options().mapCacheEnabled()
                                        || nativeRequestPlaneCoordinator
                                                .options()
                                                .mapSnapshotEnabled())))) {
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
                    mapSnapshotCacheMaxEntries,
                    mapSnapshotCacheMetrics,
                    nativeRequestPlaneCoordinator,
                    nativeRequestPlaneCoordinator != null
                                    && nativeRequestPlaneCoordinator.options().mapCacheEnabled()
                            ? allocateNativeStateId()
                            : 0,
                    nativeRequestPlaneCoordinator != null
                            && nativeRequestPlaneCoordinator.options().mapCacheEnabled(),
                    nativeRequestPlaneCoordinator != null
                                    && nativeRequestPlaneCoordinator.options().mapSnapshotEnabled()
                            ? allocateNativeStateId()
                            : 0,
                    nativeRequestPlaneCoordinator != null
                            && nativeRequestPlaneCoordinator.options().mapSnapshotEnabled());
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
            requireNativeCapableValueState(delegateValue);
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
                    BP_PREFETCH_MULTIGET,
                    VALUE_STICKY_UPDATE_IN_PLACE,
                    VALUE_LAZY_STAGING,
                    nativeRequestPlaneCoordinator,
                    allocateNativeStateId());
            wrappersByDelegateIdentity.put(internal, wrapped);
            return (IS) wrapped;
        }

        if (stateDesc.getType() == StateDescriptor.Type.MAP
                && internal instanceof InternalMapState
                && (mapPresenceCacheMaxEntries > 0
                        || mapCacheMaxEntries > 0
                        || mapSnapshotCacheMaxEntries > 0
                        || (nativeRequestPlaneCoordinator != null
                                && (nativeRequestPlaneCoordinator.options().mapCacheEnabled()
                                        || nativeRequestPlaneCoordinator
                                                .options()
                                                .mapSnapshotEnabled())))) {
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
                    mapSnapshotCacheMaxEntries,
                    mapSnapshotCacheMetrics,
                    nativeRequestPlaneCoordinator,
                    nativeRequestPlaneCoordinator != null
                                    && nativeRequestPlaneCoordinator.options().mapCacheEnabled()
                            ? allocateNativeStateId()
                            : 0,
                    nativeRequestPlaneCoordinator != null
                            && nativeRequestPlaneCoordinator.options().mapCacheEnabled(),
                    nativeRequestPlaneCoordinator != null
                                    && nativeRequestPlaneCoordinator.options().mapSnapshotEnabled()
                            ? allocateNativeStateId()
                            : 0,
                    nativeRequestPlaneCoordinator != null
                            && nativeRequestPlaneCoordinator.options().mapSnapshotEnabled());
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
                closeNativeRequestPlane();
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

    private int allocateNativeStateId() {
        if (nativeRequestPlaneCoordinator == null) {
            return 0;
        }
        if (nextNativeStateId == Integer.MAX_VALUE) {
            throw new IllegalStateException("CacheKit native ValueState id space is exhausted.");
        }
        return nextNativeStateId++;
    }

    private void requireNativeCapableValueState(InternalValueState<?, ?, ?> state) {
        if (nativeRequestPlaneCoordinator != null
                && !(state instanceof RocksDBBatchValueReader<?, ?, ?>)) {
            throw new IllegalStateException(
                    "CacheKit native request plane requires a RocksDB ValueState delegate "
                            + "that exposes exact prepared-key batch access, but got "
                            + state.getClass().getName());
        }
    }

    private void closeNativeRequestPlane() {
        if (nativeRequestPlaneCoordinator != null) {
            nativeRequestPlaneCoordinator.close();
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

    /** Uses ordered, incrementally published RocksDB MultiGet chunks for async ValueState reads. */
    private static final boolean BP_PREFETCH_MULTIGET =
            loadBooleanFlag("state.backend.cachekit.bp-prefetch.multiget.enabled", false);
    /**
     * Reuses the L1-owned sticky ValueState wrapper for repeated updates to the same key/namespace.
     * Disabled by default until Nexmark validates that the allocation reduction exceeds its extra
     * ownership check.
     */
    private static final boolean VALUE_STICKY_UPDATE_IN_PLACE =
            loadBooleanFlag(
                    "state.backend.cachekit.value.sticky-update-in-place.enabled", false);
    /**
     * Defers materialization of speculative RocksDB ValueState results until mailbox promotion.
     * Synchronous local-preagg prefetch remains eager. Disabled by default pending Nexmark A/B.
     */
    private static final boolean VALUE_LAZY_STAGING =
            loadBooleanFlag("state.backend.cachekit.value.lazy-staging.enabled", false);
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
     * True when at least one cached ValueState wrapper exists, i.e. a prefetch could land.
     * Called reflectively by StatePrefetcher before it pays the per-batch key extraction. Wrappers
     * register lazily on first state access, so this must be re-evaluated per call, not cached by
     * the caller.
     */
    public boolean hasPrefetchableState() {
        synchronized (lifecycleLock) {
            if (closed || disposed) {
                return false;
            }
            for (Object wrapper : wrappersByDelegateIdentity.values()) {
                if (wrapper instanceof CachedInternalValueState
                        && ((CachedInternalValueState<?, ?, ?>) wrapper)
                                .supportsRecordKeyPrefetch()) {
                    return true;
                }
            }
            return false;
        }
    }

    /** Reflection seam used by the streaming mailbox to defer exact-key deduplication to JNI. */
    public boolean nativeMailboxBatchEnabled() {
        return nativeRequestPlaneCoordinator != null
                && nativeRequestPlaneCoordinator.isActive()
                && nativeRequestPlaneCoordinator.options().mailboxBatchEnabled();
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
                    if (wrapper instanceof CachedInternalValueState
                            && ((CachedInternalValueState<?, ?, ?>) wrapper)
                                    .supportsRecordKeyPrefetch()) {
                        Runnable task =
                                ((CachedInternalValueState) wrapper).buildAsyncPrefetchTask(keys);
                        if (task != null) {
                            PrefetchExecutor.trySubmit(task);
                        }
                    }
                }
                return;
            }
            K previousKey = getCurrentKey();
            try {
                for (Object wrapper : wrappersByDelegateIdentity.values()) {
                    if (!BP_PREFETCH_ASYNC
                            && wrapper instanceof CachedInternalValueState
                            && ((CachedInternalValueState<?, ?, ?>) wrapper)
                                    .supportsRecordKeyPrefetch()) {
                        ((CachedInternalValueState) wrapper).prefetch(keys);
                    }
                }
            } catch (Throwable ignored) {
                // Best-effort cache warmup. Authoritative state access remains unchanged.
            } finally {
                setCurrentKey(previousKey);
            }
        }
    }

    /**
     * Synchronously bulk-load keys that local pre-aggregation has already committed to consume.
     *
     * <p>This is intentionally separate from speculative record lookahead. It only touches
     * ValueState wrappers observed in the preceding dispatch and captures each wrapper's exact
     * current namespace when it prepares the batch. MapState remains excluded. Generic
     * record-lookahead still requires VoidNamespace because it cannot infer future namespaces.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void prefetchForImmediateUse(Collection<? extends K> keys) {
        synchronized (lifecycleLock) {
            if (closed || disposed || keys == null || keys.isEmpty()) {
                return;
            }
            for (Object wrapper : wrappersByDelegateIdentity.values()) {
                if (wrapper instanceof CachedInternalValueState) {
                    CachedInternalValueState<?, ?, ?> valueState =
                            (CachedInternalValueState<?, ?, ?>) wrapper;
                    if (valueState.consumeImmediatePrefetchAccessObserved()) {
                        ((CachedInternalValueState) valueState).prefetchForImmediateUse(keys);
                    }
                }
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
                closeNativeRequestPlane();
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
