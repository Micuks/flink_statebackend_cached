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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBBatchValueReader;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PresenceCacheImplementation;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneCoordinator;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneOptions;
import org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalListState;
import org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalMapState;
import org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalPriorityQueueSet;
import org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalValueState;
import org.apache.flink.contrib.streaming.state.cachekit.state.MapSnapshotCacheMetrics;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.BatchKeyGroupingSupport;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal delegating {@link AbstractKeyedStateBackend} wrapper that adds caching for ValueState.
 *
 * <p>This class is intentionally small and only intercepts {@link #getOrCreateKeyedState} to wrap
 * {@link InternalValueState} instances.
 */
public class CacheKitKeyedStateBackend<K> extends AbstractKeyedStateBackend<K>
        implements BatchKeyGroupingSupport {

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
    private final int mapSnapshotSmallMaxEntries;
    private final boolean listStateCowEnabled;
    private final boolean listStateRywEnabled;
    private final int listStateClearedKeysCapacity;
    private final boolean priorityQueueOptEnabled;
    private final MapSnapshotCacheMetrics mapSnapshotCacheMetrics;
    private final NativeRequestPlaneCoordinator nativeRequestPlaneCoordinator;
    private final boolean keyScopedPrefetchInvalidationEnabled;
    private final boolean nativePrefetchAccessGuidedStateEnabled;
    private final boolean nativeMapDistinctBatchPrefetchEnabled;
    private final boolean nativeMapDistinctBatchPrefetchDirectArenaEnabled;
    private final boolean nativeMapDistinctBatchPrefetchCrossKeyPipelineEnabled;
    private final boolean nativeMapDistinctBatchPrefetchWorkFirstEnabled;
    private final int nativeMapDistinctBatchPrefetchAsyncMinUniqueKeys;
    private final int nativeMapDistinctBatchPrefetchLookaheadGroups;
    private int nextNativeStateId = 1;
    private long nativePreaggGroupBatches;
    private long nativePreaggInputKeys;
    private long nativePreaggGroups;
    private long nativePreaggFallbacks;
    private long nativePreaggThresholdFallbacks;

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
                1,
                listStateCowEnabled,
                listStateRywEnabled,
                listStateClearedKeysCapacity,
                priorityQueueOptEnabled,
                diagnosticsEnabled,
                nativeRequestPlaneOptions,
                false,
                false,
                false,
                false,
                false,
                false,
                8,
                1);
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
            int mapSnapshotSmallMaxEntries,
            boolean listStateCowEnabled,
            boolean listStateRywEnabled,
            int listStateClearedKeysCapacity,
            boolean priorityQueueOptEnabled,
            boolean diagnosticsEnabled,
            NativeRequestPlaneOptions nativeRequestPlaneOptions) {
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
                mapSnapshotSmallMaxEntries,
                listStateCowEnabled,
                listStateRywEnabled,
                listStateClearedKeysCapacity,
                priorityQueueOptEnabled,
                diagnosticsEnabled,
                nativeRequestPlaneOptions,
                false,
                false,
                false,
                false,
                false,
                false,
                8,
                1);
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
            int mapSnapshotSmallMaxEntries,
            boolean listStateCowEnabled,
            boolean listStateRywEnabled,
            int listStateClearedKeysCapacity,
            boolean priorityQueueOptEnabled,
            boolean diagnosticsEnabled,
            NativeRequestPlaneOptions nativeRequestPlaneOptions,
            boolean keyScopedPrefetchInvalidationEnabled,
            boolean nativePrefetchAccessGuidedStateEnabled,
            boolean nativeMapDistinctBatchPrefetchEnabled,
            boolean nativeMapDistinctBatchPrefetchDirectArenaEnabled,
            boolean nativeMapDistinctBatchPrefetchCrossKeyPipelineEnabled,
            boolean nativeMapDistinctBatchPrefetchWorkFirstEnabled,
            int nativeMapDistinctBatchPrefetchAsyncMinUniqueKeys,
            int nativeMapDistinctBatchPrefetchLookaheadGroups) {
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
        this.mapSnapshotSmallMaxEntries = Math.max(1, Math.min(16, mapSnapshotSmallMaxEntries));
        this.listStateCowEnabled = listStateCowEnabled;
        this.listStateRywEnabled = listStateRywEnabled;
        this.listStateClearedKeysCapacity = listStateClearedKeysCapacity;
        this.priorityQueueOptEnabled = priorityQueueOptEnabled;
        this.keyScopedPrefetchInvalidationEnabled = keyScopedPrefetchInvalidationEnabled;
        this.nativeMapDistinctBatchPrefetchEnabled = nativeMapDistinctBatchPrefetchEnabled;
        this.nativeMapDistinctBatchPrefetchDirectArenaEnabled =
                nativeMapDistinctBatchPrefetchDirectArenaEnabled;
        this.nativeMapDistinctBatchPrefetchCrossKeyPipelineEnabled =
                nativeMapDistinctBatchPrefetchCrossKeyPipelineEnabled;
        this.nativeMapDistinctBatchPrefetchWorkFirstEnabled =
                nativeMapDistinctBatchPrefetchWorkFirstEnabled;
        this.nativeMapDistinctBatchPrefetchAsyncMinUniqueKeys =
                Math.max(2, nativeMapDistinctBatchPrefetchAsyncMinUniqueKeys);
        this.nativeMapDistinctBatchPrefetchLookaheadGroups =
                Math.max(1, Math.min(8, nativeMapDistinctBatchPrefetchLookaheadGroups));
        this.mapSnapshotCacheMetrics =
                MapSnapshotCacheMetrics.create(metricGroup, diagnosticsEnabled);
        Preconditions.checkNotNull(nativeRequestPlaneOptions, "nativeRequestPlaneOptions");
        Preconditions.checkArgument(
                !nativeRequestPlaneOptions.enabled()
                        || !nativeRequestPlaneOptions.requiresValueCache()
                        || valueCacheMaxEntries > 0,
                "Native ValueState cache and prefetch require a positive ValueState cache; "
                        + "mailbox, pre-aggregation, and MapState native features may run "
                        + "without it.");
        Preconditions.checkArgument(
                !nativeMapDistinctBatchPrefetchDirectArenaEnabled
                        || nativeMapDistinctBatchPrefetchEnabled,
                "MapState DISTINCT direct-arena transport requires MapState DISTINCT batch prefetch.");
        Preconditions.checkArgument(
                !nativeMapDistinctBatchPrefetchDirectArenaEnabled
                        || (nativeRequestPlaneOptions.enabled()
                                && nativeRequestPlaneOptions.directArenaMultiGetEnabled()),
                "MapState DISTINCT direct-arena transport requires the native request plane and direct-arena MultiGet.");

        // fullOpt: initialize shared flush executors (daemon threads)
        ExecutorService initializedListExecutor = null;
        ExecutorService initializedPqExecutor = null;
        NativeRequestPlaneCoordinator initializedNativeCoordinator = null;
        try {
            initializedListExecutor =
                    listStateCowEnabled
                            ? Executors.newSingleThreadExecutor(
                                    r -> {
                                        Thread t = new Thread(r, "cachekit-list-state-flush");
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
                    NativeRequestPlaneCoordinator.open(
                            nativeRequestPlaneOptions,
                            nativeMapDistinctBatchPrefetchEnabled
                                            && nativeMapDistinctBatchPrefetchCrossKeyPipelineEnabled
                                    ? this.nativeMapDistinctBatchPrefetchLookaheadGroups
                                    : 0);
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
        this.nativePrefetchAccessGuidedStateEnabled =
                nativePrefetchAccessGuidedStateEnabled
                        && initializedNativeCoordinator != null
                        && initializedNativeCoordinator.isActive()
                        && initializedNativeCoordinator.options().prefetchEnabled()
                        && initializedNativeCoordinator.options().mailboxBatchEnabled();

        LOG.info(
                "[CACHEKIT fullOpt] Backend created: listStateCow={}, listStateRyw={}, "
                        + "clearedKeysCap={}, priorityQueueOpt={}, keyScopedInvalidation={}, accessGuidedPrefetch={}, nativeRequestPlane={}, "
                        + "nativeKernel={}, nativeFeatureBits={}, nativeFeatures={}",
                listStateCowEnabled,
                listStateRywEnabled,
                listStateClearedKeysCapacity,
                priorityQueueOptEnabled,
                keyScopedPrefetchInvalidationEnabled,
                nativePrefetchAccessGuidedStateEnabled,
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
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <N, S extends State, V> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, V> stateDescriptor)
            throws Exception {
        synchronized (lifecycleLock) {
            ensureOpen();
            return getOrCreateKeyedStateInternal(namespaceSerializer, stateDescriptor);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
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
                && shouldWrapValueState()) {
            Object existing = wrappersByDelegateIdentity.get(internal);
            if (existing != null) {
                return (S) existing;
            }
            InternalValueState<K, N, V> delegateValue = (InternalValueState<K, N, V>) internal;
            requireNativeCapableValueState(delegateValue);
            CachedInternalValueState<K, N, V> wrapped =
                    new CachedInternalValueState<>(
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
                            keyScopedPrefetchInvalidationEnabled,
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
                        || nativeMapDistinctBatchPrefetchEnabled
                        || (nativeRequestPlaneCoordinator != null
                                && (nativeRequestPlaneCoordinator.options().mapCacheEnabled()
                                        || nativeRequestPlaneCoordinator
                                                .options()
                                                .mapSnapshotEnabled())))) {
            Object existing = wrappersByDelegateIdentity.get(internal);
            if (existing != null) {
                return (S) existing;
            }
            InternalMapState<K, N, Object, Object> delegateMap =
                    (InternalMapState<K, N, Object, Object>) internal;
            CachedInternalMapState<K, N, Object, Object> wrapped =
                    new CachedInternalMapState<>(
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
                                            && (nativeRequestPlaneCoordinator
                                                            .options()
                                                            .mapCacheEnabled()
                                                    || nativeMapDistinctBatchPrefetchDirectArenaEnabled)
                                    ? allocateNativeStateId()
                                    : 0,
                            nativeRequestPlaneCoordinator != null
                                    && nativeRequestPlaneCoordinator.options().mapCacheEnabled(),
                            nativeRequestPlaneCoordinator != null
                                            && nativeRequestPlaneCoordinator
                                                    .options()
                                                    .mapSnapshotEnabled()
                                    ? allocateNativeStateId()
                                    : 0,
                            nativeRequestPlaneCoordinator != null
                                    && nativeRequestPlaneCoordinator.options().mapSnapshotEnabled(),
                            mapSnapshotSmallMaxEntries,
                            nativeRequestPlaneCoordinator != null
                                    && nativeRequestPlaneCoordinator
                                            .options()
                                            .mapSnapshotAdaptiveBypassEnabled(),
                            nativeRequestPlaneCoordinator == null
                                    ? 8192
                                    : nativeRequestPlaneCoordinator
                                            .options()
                                            .mapSnapshotAdaptiveWindowProbes(),
                            nativeRequestPlaneCoordinator == null
                                    ? 0.02
                                    : nativeRequestPlaneCoordinator
                                            .options()
                                            .mapSnapshotAdaptiveMinUsefulHitRate(),
                            nativeRequestPlaneCoordinator == null
                                    ? 262144
                                    : nativeRequestPlaneCoordinator
                                            .options()
                                            .mapSnapshotAdaptiveResampleIntervalProbes());
            wrapped.enableNativeDistinctBatchPrefetch(
                    nativeMapDistinctBatchPrefetchEnabled,
                    nativeMapDistinctBatchPrefetchDirectArenaEnabled,
                    nativeMapDistinctBatchPrefetchAsyncMinUniqueKeys,
                    nativeMapDistinctBatchPrefetchWorkFirstEnabled);
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
                    elementSerializer, "ListState must have an element serializer configured");
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

    /**
     * Whether ValueState needs CacheKit's wrapper even when its Java cache has zero capacity.
     * Native mailbox compaction consumes the wrapper's prepared-key batch hook but is otherwise
     * intentionally independent of the Java ValueState cache.
     */
    private boolean shouldWrapValueState() {
        return valueCacheMaxEntries > 0
                || (nativeRequestPlaneCoordinator != null
                        && nativeRequestPlaneCoordinator.options().mailboxBatchEnabled());
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
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, SV> stateDesc,
            StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                    stateSnapshotTransformFactory)
            throws Exception {
        synchronized (lifecycleLock) {
            ensureOpen();
            return createOrUpdateInternalStateInternal(
                    namespaceSerializer, stateDesc, stateSnapshotTransformFactory);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalStateInternal(
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, SV> stateDesc,
            StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                    stateSnapshotTransformFactory)
            throws Exception {
        IS state =
                delegate.createOrUpdateInternalState(
                        namespaceSerializer, stateDesc, stateSnapshotTransformFactory);

        // Wrap ValueState with cache layer
        if (!(state instanceof InternalKvState)) {
            return state;
        }

        InternalKvState<K, N, ?> internal = (InternalKvState<K, N, ?>) state;
        if (stateDesc.getType() == StateDescriptor.Type.VALUE
                && internal instanceof InternalValueState
                && shouldWrapValueState()) {
            Object existing = wrappersByDelegateIdentity.get(internal);
            if (existing != null) {
                return (IS) existing;
            }
            InternalValueState<K, N, SV> delegateValue = (InternalValueState<K, N, SV>) internal;
            requireNativeCapableValueState(delegateValue);
            CachedInternalValueState<K, N, SV> wrapped =
                    new CachedInternalValueState<>(
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
                            keyScopedPrefetchInvalidationEnabled,
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
                        || nativeMapDistinctBatchPrefetchEnabled
                        || (nativeRequestPlaneCoordinator != null
                                && (nativeRequestPlaneCoordinator.options().mapCacheEnabled()
                                        || nativeRequestPlaneCoordinator
                                                .options()
                                                .mapSnapshotEnabled())))) {
            Object existing = wrappersByDelegateIdentity.get(internal);
            if (existing != null) {
                return (IS) existing;
            }
            InternalMapState<K, N, Object, Object> delegateMap =
                    (InternalMapState<K, N, Object, Object>) internal;
            CachedInternalMapState<K, N, Object, Object> wrapped =
                    new CachedInternalMapState<>(
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
                                            && (nativeRequestPlaneCoordinator
                                                            .options()
                                                            .mapCacheEnabled()
                                                    || nativeMapDistinctBatchPrefetchDirectArenaEnabled)
                                    ? allocateNativeStateId()
                                    : 0,
                            nativeRequestPlaneCoordinator != null
                                    && nativeRequestPlaneCoordinator.options().mapCacheEnabled(),
                            nativeRequestPlaneCoordinator != null
                                            && nativeRequestPlaneCoordinator
                                                    .options()
                                                    .mapSnapshotEnabled()
                                    ? allocateNativeStateId()
                                    : 0,
                            nativeRequestPlaneCoordinator != null
                                    && nativeRequestPlaneCoordinator.options().mapSnapshotEnabled(),
                            mapSnapshotSmallMaxEntries,
                            nativeRequestPlaneCoordinator != null
                                    && nativeRequestPlaneCoordinator
                                            .options()
                                            .mapSnapshotAdaptiveBypassEnabled(),
                            nativeRequestPlaneCoordinator == null
                                    ? 8192
                                    : nativeRequestPlaneCoordinator
                                            .options()
                                            .mapSnapshotAdaptiveWindowProbes(),
                            nativeRequestPlaneCoordinator == null
                                    ? 0.02
                                    : nativeRequestPlaneCoordinator
                                            .options()
                                            .mapSnapshotAdaptiveMinUsefulHitRate(),
                            nativeRequestPlaneCoordinator == null
                                    ? 262144
                                    : nativeRequestPlaneCoordinator
                                            .options()
                                            .mapSnapshotAdaptiveResampleIntervalProbes());
            wrapped.enableNativeDistinctBatchPrefetch(
                    nativeMapDistinctBatchPrefetchEnabled,
                    nativeMapDistinctBatchPrefetchDirectArenaEnabled,
                    nativeMapDistinctBatchPrefetchAsyncMinUniqueKeys,
                    nativeMapDistinctBatchPrefetchWorkFirstEnabled);
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
                    elementSerializer, "ListState must have an element serializer configured");
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
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    String stateName, TypeSerializer<T> byteOrderedElementSerializer) {
        synchronized (lifecycleLock) {
            ensureOpen();
            KeyGroupedInternalPriorityQueue<T> delegateQueue =
                    delegate.create(stateName, byteOrderedElementSerializer);
            return wrapPriorityQueue(delegateQueue, byteOrderedElementSerializer);
        }
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    String stateName,
                    TypeSerializer<T> byteOrderedElementSerializer,
                    boolean allowFutureMetadataUpdates) {
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
     *
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
                LOG.info(
                        "[CACHEKIT MAP SNAPSHOT CACHE] {}",
                        mapSnapshotCacheMetrics.diagnosticSummary());
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
            LOG.info(
                    "[CACHEKIT NATIVE PREAGG SUMMARY] batches={} inputKeys={} groups={} "
                            + "fallbacks={} thresholdFallbacks={} minBatchSize={} kernel={}",
                    nativePreaggGroupBatches,
                    nativePreaggInputKeys,
                    nativePreaggGroups,
                    nativePreaggFallbacks,
                    nativePreaggThresholdFallbacks,
                    nativeRequestPlaneCoordinator.options().minBatchSize(),
                    nativeRequestPlaneCoordinator.selectedKernel());
            nativeRequestPlaneCoordinator.close();
        }
    }

    private void ensureOpen() {
        if (closed || disposed) {
            throw new IllegalStateException("CacheKit keyed state backend is closed.");
        }
    }

    /**
     * When true (default), ValueState prefetch is submitted to the shared off-mailbox worker thread
     * instead of being executed synchronously in the mailbox critical path.
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
            loadBooleanFlag("state.backend.cachekit.value.sticky-update-in-place.enabled", false);
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
     * True when at least one cached ValueState wrapper exists, i.e. a prefetch could land. Called
     * reflectively by StatePrefetcher before it pays the per-batch key extraction. Wrappers
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
                                .isRecordKeyPrefetchEligible(
                                        nativePrefetchAccessGuidedStateEnabled)) {
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

    /** Reflection seam that avoids dispatch-key extraction when mutation batching is disabled. */
    public boolean nativeResidentMutationBatchEnabled() {
        return nativeRequestPlaneCoordinator != null
                && nativeRequestPlaneCoordinator.isActive()
                && nativeRequestPlaneCoordinator.options().residentMutationBatchEnabled();
    }

    @Override
    public int maxGroupingEntries() {
        NativeRequestPlaneCoordinator coordinator = nativeRequestPlaneCoordinator;
        return coordinator != null
                        && coordinator.isActive()
                        && coordinator.options().preaggEnabled()
                ? coordinator.options().batchEntries()
                : 0;
    }

    @Override
    public boolean indexedBatchFoldEnabled() {
        NativeRequestPlaneCoordinator coordinator = nativeRequestPlaneCoordinator;
        return coordinator != null
                && coordinator.isActive()
                && coordinator.options().preaggEnabled()
                && coordinator.options().indexedFoldEnabled();
    }

    @Override
    public int crossKeyPipelineLookaheadGroups() {
        return nativeMapDistinctBatchPrefetchEnabled
                        && nativeMapDistinctBatchPrefetchCrossKeyPipelineEnabled
                ? nativeMapDistinctBatchPrefetchLookaheadGroups
                : 0;
    }

    /**
     * Groups caller-owned Java hash tokens directly into a caller-owned packed plan.
     *
     * <p>The native result is deliberately not trusted as Java key identity. LocalPreagg validates
     * every source key with {@link Objects#equals(Object, Object)} before it processes any record;
     * an unequal-key hash collision therefore causes a whole-batch Java fallback.
     */
    @Override
    public int groupHashTokens(ByteBuffer tokens, int count, ByteBuffer packedPlan) {
        synchronized (lifecycleLock) {
            if (closed
                    || disposed
                    || count <= 0
                    || nativeRequestPlaneCoordinator == null
                    || !nativeRequestPlaneCoordinator.isActive()
                    || !nativeRequestPlaneCoordinator.options().preaggEnabled()
                    || count > nativeRequestPlaneCoordinator.options().batchEntries()) {
                nativePreaggFallbacks++;
                return -1;
            }
            if (count < nativeRequestPlaneCoordinator.options().minBatchSize()) {
                nativePreaggFallbacks++;
                nativePreaggThresholdFallbacks++;
                return -1;
            }
            try {
                int groupCount =
                        nativeRequestPlaneCoordinator.groupHashTokens(tokens, count, packedPlan);
                nativePreaggGroupBatches++;
                nativePreaggInputKeys += count;
                nativePreaggGroups += groupCount;
                if (nativePreaggGroupBatches % 5000L == 1L) {
                    LOG.info(
                            "[CACHEKIT NATIVE PREAGG] batches={} inputKeys={} groups={} "
                                    + "fallbacks={} kernel={} groupingKernel=token32-scalar",
                            nativePreaggGroupBatches,
                            nativePreaggInputKeys,
                            nativePreaggGroups,
                            nativePreaggFallbacks,
                            nativeRequestPlaneCoordinator.selectedKernel());
                }
                return groupCount;
            } catch (Throwable failure) {
                nativePreaggFallbacks++;
                return -1;
            }
        }
    }

    /**
     * Reflection seam used by LocalPreagg to obtain stable first-seen group ids from the native
     * runtime. A null result means the caller must use its existing Java LinkedHashMap path.
     */
    public int[] nativePreaggGroupIds(List<?> keys) {
        synchronized (lifecycleLock) {
            if (closed
                    || disposed
                    || keys == null
                    || keys.isEmpty()
                    || nativeRequestPlaneCoordinator == null
                    || !nativeRequestPlaneCoordinator.isActive()
                    || !nativeRequestPlaneCoordinator.options().preaggEnabled()
                    || keys.size() > nativeRequestPlaneCoordinator.options().batchEntries()) {
                nativePreaggFallbacks++;
                return null;
            }
            if (keys.size() < nativeRequestPlaneCoordinator.options().minBatchSize()) {
                nativePreaggFallbacks++;
                nativePreaggThresholdFallbacks++;
                return null;
            }
            NativeRequestPlaneCoordinator.BatchSlot slot =
                    nativeRequestPlaneCoordinator.tryAcquireBatchSlot();
            if (slot == null) {
                nativePreaggFallbacks++;
                return null;
            }
            try {
                slot.prepareLatestDirect(
                        Integer.MAX_VALUE,
                        0L,
                        keys.size(),
                        (index, output) -> output.writeInt(nativePreaggHashToken(keys.get(index))));
                int groupCount = nativeRequestPlaneCoordinator.group(slot);
                int[] plan = new int[keys.size() + 1];
                plan[0] = groupCount;
                for (int source = 0; source < keys.size(); source++) {
                    plan[source + 1] = slot.sourceGroupIndex(source);
                }
                nativePreaggGroupBatches++;
                nativePreaggInputKeys += keys.size();
                nativePreaggGroups += groupCount;
                if (nativePreaggGroupBatches % 5000L == 1L) {
                    LOG.info(
                            "[CACHEKIT NATIVE PREAGG] batches={} inputKeys={} groups={} "
                                    + "fallbacks={} kernel={}",
                            nativePreaggGroupBatches,
                            nativePreaggInputKeys,
                            nativePreaggGroups,
                            nativePreaggFallbacks,
                            nativeRequestPlaneCoordinator.selectedKernel());
                }
                return plan;
            } catch (Throwable failure) {
                nativePreaggFallbacks++;
                return null;
            } finally {
                slot.close();
            }
        }
    }

    /**
     * Produces the compact token consumed by the native preaggregation grouping kernel.
     *
     * <p>Equal keys must have equal Java hash codes, so the token preserves every valid grouping.
     * Hash collisions can only over-group unequal keys; LocalPreagg validates the returned plan
     * with Java equality and falls back to its LinkedHashMap path whenever that happens. This lets
     * the native path avoid serializing the complete generic key without weakening correctness.
     */
    static int nativePreaggHashToken(Object key) {
        return Objects.hashCode(key);
    }

    long getNativePreaggGroupBatchesForTesting() {
        return nativePreaggGroupBatches;
    }

    long getNativePreaggInputKeysForTesting() {
        return nativePreaggInputKeys;
    }

    long getNativePreaggGroupsForTesting() {
        return nativePreaggGroups;
    }

    long getNativePreaggFallbacksForTesting() {
        return nativePreaggFallbacks;
    }

    long getNativePreaggThresholdFallbacksForTesting() {
        return nativePreaggThresholdFallbacks;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void prefetch(Collection<? extends K> keys) {
        synchronized (lifecycleLock) {
            if (closed
                    || disposed
                    || keys == null
                    || keys.isEmpty()
                    || wrappersByDelegateIdentity.isEmpty()) {
                return;
            }
            if (BP_PREFETCH_ASYNC) {
                // Off-mailbox path: only (key, namespace) serialization happens here; RocksDB reads
                // and value deserialization run on the shared prefetch worker. No key-context
                // save/restore needed — submission never touches the backend key context.
                for (Object wrapper : wrappersByDelegateIdentity.values()) {
                    if (wrapper instanceof CachedInternalValueState) {
                        CachedInternalValueState<?, ?, ?> valueState =
                                (CachedInternalValueState<?, ?, ?>) wrapper;
                        if (!valueState.shouldReceiveRecordKeyPrefetch(
                                nativePrefetchAccessGuidedStateEnabled)) {
                            continue;
                        }
                        Runnable task =
                                ((CachedInternalValueState) valueState)
                                        .buildAsyncPrefetchTask(keys);
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
        prefetchForImmediateUse(keys, false);
    }

    /**
     * Immediate prefetch for a dispatched LocalPreagg batch. Exact cancellation is fused into the
     * per-wrapper scan that immediate prefetch already has to perform.
     */
    public void prefetchForImmediateUseAfterDispatch(Collection<? extends K> keys) {
        prefetchForImmediateUse(keys, true);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void prefetchForImmediateUse(
            Collection<? extends K> keys, boolean cancelPrefetchOnDispatch) {
        synchronized (lifecycleLock) {
            if (closed || disposed || keys == null || keys.isEmpty()) {
                return;
            }
            for (Object wrapper : wrappersByDelegateIdentity.values()) {
                if (wrapper instanceof CachedInternalValueState) {
                    CachedInternalValueState<?, ?, ?> valueState =
                            (CachedInternalValueState<?, ?, ?>) wrapper;
                    if (valueState.consumeImmediatePrefetchAccessObserved()) {
                        ((CachedInternalValueState) valueState)
                                .prefetchForImmediateUse(keys, cancelPrefetchOnDispatch);
                    } else if (cancelPrefetchOnDispatch
                            && valueState.hasInFlightDispatchPrefetchReservations()) {
                        // Keep exact cancellation coverage for a state not observed by an earlier
                        // batch (or only reached conditionally). This remains inside the single
                        // backend wrapper traversal; only such inactive states scan the keys.
                        ((CachedInternalValueState) valueState).cancelPrefetchForDispatch(keys);
                    }
                }
            }
        }
    }

    /**
     * Opens resident-only native mutation batches for the exact mailbox-dispatch key set.
     *
     * <p>This optional hook is discovered reflectively by the streaming runtime, preserving the
     * generic keyed-state backend interface. Wrappers that are inactive or unsupported simply do
     * not participate.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public int beginNativeResidentMutationBatch(Collection<? extends K> keys) {
        synchronized (lifecycleLock) {
            if (closed || disposed || keys == null || keys.isEmpty()) {
                return 0;
            }
            int started = 0;
            for (Object wrapper : wrappersByDelegateIdentity.values()) {
                if (wrapper instanceof CachedInternalValueState
                        && ((CachedInternalValueState) wrapper)
                                .beginNativeResidentMutationBatch(keys)) {
                    started++;
                }
            }
            return started;
        }
    }

    /** Flushes every resident-only native mutation batch opened for the current dispatch. */
    public int endNativeResidentMutationBatch() {
        synchronized (lifecycleLock) {
            if (closed || disposed) {
                return 0;
            }
            int visited = 0;
            for (Object wrapper : wrappersByDelegateIdentity.values()) {
                if (wrapper instanceof CachedInternalValueState) {
                    ((CachedInternalValueState<?, ?, ?>) wrapper).endNativeResidentMutationBatch();
                    visited++;
                }
            }
            return visited;
        }
    }

    /**
     * Revokes still-speculative prepared-key reservations for records selected by the mailbox.
     *
     * <p>Only wrappers that can prove exact record-key prepared-MultiGet ownership participate.
     * Generic query-wire prefetch and namespaced state fail closed. Published staging values are
     * intentionally retained for the selected record to consume.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public int cancelPrefetchForDispatch(Collection<? extends K> keys) {
        synchronized (lifecycleLock) {
            if (closed || disposed || keys == null || keys.isEmpty()) {
                return 0;
            }
            int cancelled = 0;
            for (Object wrapper : wrappersByDelegateIdentity.values()) {
                if (wrapper instanceof CachedInternalValueState) {
                    CachedInternalValueState valueState = (CachedInternalValueState) wrapper;
                    if (valueState.hasInFlightDispatchPrefetchReservations()) {
                        cancelled += valueState.cancelPrefetchForDispatch(keys);
                    }
                }
            }
            return cancelled;
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
