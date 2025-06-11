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

package org.apache.flink.contrib.streaming.state;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricMonitor;
import org.apache.flink.contrib.streaming.state.RocksDBResourceContainer;
import org.apache.flink.contrib.streaming.state.RocksDBWriteBatchWrapper;
import org.apache.flink.contrib.streaming.state.snapshot.RocksDBSnapshotStrategyBase;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ResourceGuard;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.FileUtils;





// Added for metrics

// Added for logging

/**
 * The keyed state backend that implements caching. It wraps a delegate AbstractKeyedStateBackend
 * (e.g., RocksDBKeyedStateBackend) and creates CachingInternal*State objects.
 */
public class CachingKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CachingKeyedStateBackend.class);

    private final AbstractKeyedStateBackend<K> delegateKeyedStateBackend; // RocksDBKeyedStateBackend
    private final int l1EntryCacheSize;
    private final int l2EntryCacheSize;
    private final int maxActiveNamespaceOrPerKeyCacheContainers;
    private final long maxCacheMemoryMb;
    private final CachingStateBackendFactory.CachePolicyType cachePolicyType;
    private final int mapL1KeyPresenceCacheSize;
    private final int mapL2KeyPresenceCacheSize;
    private final MetricGroup metricGroup;

    private final double mapCacheHitRateThreshold;
    private final long mapCacheHitRateWindowSize;
    private final long mapCacheMinAccessesForBypassCheck;
    private final boolean mapKeyPresenceCacheEnabled;
    private final boolean mapBypassEnabled;

    private final List<CachingInternalState<K, ?, ?, ?>> registeredStates;
    private final List<CachingKeyGroupedInternalPriorityQueue<?>> registeredPqs;

    // Added for memory capping
    private transient AtomicLong currentEstimatedCacheSizeBytes;
    private transient long maxConfiguredCacheSizeBytes;
    // Using the static ValueSizeUtils for now, but a Function could be injected here
    // private transient Function<Object, Long> valueSizeEstimator;

    public CachingKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            @Nonnull CloseableRegistry cancelStreamRegistry,
            AbstractKeyedStateBackend<K> delegateKeyedStateBackend,
            int l1EntryCacheSize,
            int l2EntryCacheSize,
            int maxActiveNamespaceOrPerKeyCacheContainers,
            long maxCacheMemoryMb, CachingStateBackendFactory.CachePolicyType cachePolicyType,
            int mapL1KeyPresenceCacheSize, int mapL2KeyPresenceCacheSize,
            double mapCacheHitRateThreshold, long mapCacheHitRateWindowSize, long mapCacheMinAccessesForBypassCheck,
            boolean mapKeyPresenceCacheEnabled, boolean mapBypassEnabled) {

        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                CachingKeyedStateBackend.buildLatencyTrackingConfig(metricGroup, executionConfig),
                cancelStreamRegistry,
                delegateKeyedStateBackend.getKeyGroupCompressionDecorator(),
                delegateKeyedStateBackend.getKeyContext());

        this.delegateKeyedStateBackend = delegateKeyedStateBackend;
        this.l1EntryCacheSize = l1EntryCacheSize;
        this.l2EntryCacheSize = l2EntryCacheSize;
        this.maxActiveNamespaceOrPerKeyCacheContainers = maxActiveNamespaceOrPerKeyCacheContainers;
        this.maxCacheMemoryMb = maxCacheMemoryMb;
        this.registeredStates = new ArrayList<>();
        this.cachePolicyType = cachePolicyType;
        this.mapL1KeyPresenceCacheSize = mapL1KeyPresenceCacheSize;
        this.mapL2KeyPresenceCacheSize = mapL2KeyPresenceCacheSize;
        this.metricGroup = metricGroup;
        this.mapCacheHitRateThreshold = mapCacheHitRateThreshold;
        this.mapCacheHitRateWindowSize = mapCacheHitRateWindowSize;
        this.mapCacheMinAccessesForBypassCheck = mapCacheMinAccessesForBypassCheck;
        this.mapKeyPresenceCacheEnabled = mapKeyPresenceCacheEnabled;
        this.mapBypassEnabled = mapBypassEnabled;

        // Initialize memory capping fields
        this.currentEstimatedCacheSizeBytes = new AtomicLong(0L);
        this.maxConfiguredCacheSizeBytes = this.maxCacheMemoryMb * 1024L * 1024L;
        // this.valueSizeEstimator = ValueSizeUtils::estimate; // Example if Function was used

        this.registeredPqs = new ArrayList<>();
    }

    // create a new RocksDB backend
    public CachingKeyedStateBackend(
            ClassLoader userCodeClassLoader,
            File instanceBasePath,
            RocksDBResourceContainer optionsContainer,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            RocksDB db,
            LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            int keyGroupPrefixBytes,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            ResourceGuard rocksDBResourceGuard,
            RocksDBSnapshotStrategyBase<K, ?> checkpointSnapshotStrategy,
            RocksDBWriteBatchWrapper writeBatchWrapper,
            ColumnFamilyHandle defaultColumnFamilyHandle,
            RocksDBNativeMetricMonitor nativeMetricMonitor,
            SerializedCompositeKeyBuilder<K> sharedRocksKeyBuilder,
            PriorityQueueSetFactory priorityQueueFactory,
            RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            InternalKeyContext<K> keyContext,
            @Nonnegative long writeBatchSize,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            int l1EntryCacheSize,
            int l2EntryCacheSize,
            int maxActiveNamespaceOrPerKeyCacheContainers,
            long maxCacheMemoryMb,
            CachingStateBackendFactory.CachePolicyType cachePolicyType,
            int mapL1KeyPresenceCacheSize, int mapL2KeyPresenceCacheSize,
            double mapCacheHitRateThreshold, long mapCacheHitRateWindowSize, long mapCacheMinAccessesForBypassCheck,
            boolean mapKeyPresenceCacheEnabled, boolean mapBypassEnabled
    ) {
        // Call super constructor first, using direct parameters where available
        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader, // direct parameter
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig, // direct parameter
                cancelStreamRegistry, // direct parameter
                keyGroupCompressionDecorator, // direct parameter
                keyContext); // direct parameter

        // Now initialize the delegateKeyedStateBackend
        this.delegateKeyedStateBackend = new RocksDBKeyedStateBackend<K>(
                userCodeClassLoader,
                instanceBasePath,
                optionsContainer,
                columnFamilyOptionsFactory,
                kvStateRegistry,
                keySerializer,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                db,
                kvStateInformation,
                registeredPQStates,
                keyGroupPrefixBytes,
                cancelStreamRegistry,
                keyGroupCompressionDecorator,
                rocksDBResourceGuard,
                checkpointSnapshotStrategy,
                writeBatchWrapper,
                defaultColumnFamilyHandle,
                nativeMetricMonitor,
                sharedRocksKeyBuilder,
                priorityQueueFactory,
                ttlCompactFiltersManager,
                keyContext,
                writeBatchSize
        );
        this.metricGroup = metricGroup;

        this.l1EntryCacheSize = l1EntryCacheSize;
        this.l2EntryCacheSize = l2EntryCacheSize;
        this.maxActiveNamespaceOrPerKeyCacheContainers = maxActiveNamespaceOrPerKeyCacheContainers;
        this.maxCacheMemoryMb = maxCacheMemoryMb;
        this.registeredStates = new ArrayList<>();
        this.cachePolicyType = cachePolicyType;
        this.mapL1KeyPresenceCacheSize = mapL1KeyPresenceCacheSize;
        this.mapL2KeyPresenceCacheSize = mapL2KeyPresenceCacheSize;
        this.mapCacheHitRateThreshold = mapCacheHitRateThreshold;
        this.mapCacheHitRateWindowSize = mapCacheHitRateWindowSize;
        this.mapCacheMinAccessesForBypassCheck = mapCacheMinAccessesForBypassCheck;
        this.mapKeyPresenceCacheEnabled = mapKeyPresenceCacheEnabled;
        this.mapBypassEnabled = mapBypassEnabled;

        // Initialize memory capping fields
        this.currentEstimatedCacheSizeBytes = new AtomicLong(0L);
        this.maxConfiguredCacheSizeBytes = this.maxCacheMemoryMb * 1024L * 1024L;
        // this.valueSizeEstimator = ValueSizeUtils::estimate; // Example if Function was used

        this.registeredPqs = new ArrayList<>();
    }

    public int getMaxActiveNamespaceOrPerKeyCacheContainers() {
        return maxActiveNamespaceOrPerKeyCacheContainers;
    }

    @Nonnull
    @Override
    @SuppressWarnings({
        "unchecked",
        "rawtypes"
    }) // V_SD is the value in StateDescriptor, UK/UV for Map, V_ELE for List
    public <N, S extends State, V_SD> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, V_SD> stateDescriptor)
            throws Exception {

        // Check if a caching state for this descriptor already exists
        synchronized(registeredStates) {
            for (CachingInternalState<K, ?, ?, ?> registeredState : registeredStates) {
                // This check needs to be robust. Comparing delegate state might be one way,
                // or comparing based on state name and namespace serializer.
                // For now, assume getDelegateState().getDescriptorName() or similar is available or use state name
                if (registeredState.getDelegateState() instanceof InternalKvState) {
                    // This comparison is a bit simplistic and might need refinement based on how InternalKvState identifies itself
                    // For instance, comparing state names might be more direct if delegate state holds its descriptor name
                    Object delegateFromRegistered = registeredState.getDelegateState();
                    // A more robust check would be needed here, potentially involving the state descriptor name and type.
                    // This is a placeholder for a proper check to see if the state is already created and cached.
                    // For this example, let's assume we need to create it if not found via a more specific lookup.
                }
            }
        }

        S actualState = delegateKeyedStateBackend.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);

        if (!(actualState instanceof InternalKvState)) {
            return actualState; // Return directly if not an InternalKvState, cannot cache
        }

        InternalKvState<K, N, ?> actualStateRaw = (InternalKvState<K, N, ?>) actualState;
        CachingInternalState<K, N, ?, ?> cachingStateToRegister = null;

        if (stateDescriptor.getType() == StateDescriptor.Type.VALUE && actualStateRaw instanceof InternalValueState) {
            InternalValueState<K, N, V_SD> actualStateValue = (InternalValueState<K, N, V_SD>) actualStateRaw;
            cachingStateToRegister = new CachingInternalValueState<>(
                    actualStateValue, this, l1EntryCacheSize, l2EntryCacheSize,
                    maxActiveNamespaceOrPerKeyCacheContainers, this.maxCacheMemoryMb, this.cachePolicyType,
                    0.0, 0, 0, false, this.metricGroup.addGroup("state").addGroup(stateDescriptor.getName()).addGroup("cache"));
        } else if (stateDescriptor.getType() == StateDescriptor.Type.MAP && actualStateRaw instanceof InternalMapState) {
            InternalMapState<K, N, ?, ?> actualDelegateMapState = (InternalMapState<K, N, ?, ?>) actualStateRaw;
            String stateName = stateDescriptor.getName(); // Get state name for metrics
            MetricGroup mapMetricsGroup = this.metricGroup.addGroup("state").addGroup(stateName).addGroup("cache");

            cachingStateToRegister = new CachingInternalMapState<>(
                    actualDelegateMapState, this, l1EntryCacheSize, l2EntryCacheSize,
                    maxActiveNamespaceOrPerKeyCacheContainers, this.maxCacheMemoryMb, this.cachePolicyType,
                    this.mapL1KeyPresenceCacheSize, this.mapL2KeyPresenceCacheSize,
                    mapMetricsGroup,
                    this.mapCacheHitRateThreshold, this.mapCacheHitRateWindowSize, this.mapCacheMinAccessesForBypassCheck,
                    this.mapKeyPresenceCacheEnabled, this.mapBypassEnabled);
        } else if (stateDescriptor.getType() == StateDescriptor.Type.LIST && actualStateRaw instanceof InternalListState) {
            InternalListState<K, N, V_SD> actualDelegateListState = (InternalListState<K, N, V_SD>) actualStateRaw;
            cachingStateToRegister = new CachingInternalListState<>(
                    actualDelegateListState, this, l1EntryCacheSize, l2EntryCacheSize,
                    maxActiveNamespaceOrPerKeyCacheContainers, this.cachePolicyType); // Max memory mb was missing here for list
        } else if (stateDescriptor.getType() == StateDescriptor.Type.AGGREGATING && actualStateRaw instanceof InternalAggregatingState) {
            InternalAggregatingState actualDelegateAggState = (InternalAggregatingState) actualStateRaw;
            AggregatingStateDescriptor aggStateDesc = (AggregatingStateDescriptor) stateDescriptor;
            String stateName = aggStateDesc.getName();
            MetricGroup aggMetricsGroup = this.metricGroup.addGroup("state").addGroup(stateName).addGroup("cache");
            cachingStateToRegister = new CachingInternalAggregatingState(
                actualDelegateAggState,
                this,
                aggStateDesc.getAggregateFunction(),
                l1EntryCacheSize,
                l2EntryCacheSize,
                this.cachePolicyType,
                aggMetricsGroup);
        } else {
            // For unsupported types or if actualStateRaw is not an instance of the expected internal type,
            // return the raw state from the delegate directly.
            LOG.warn("State type {} not supported for caching or type mismatch. Returning raw state.", stateDescriptor.getType());
            return (S) actualStateRaw;
        }

        if (cachingStateToRegister != null) {
            synchronized (registeredStates) {
                boolean alreadyExists = registeredStates.stream()
                        .anyMatch(st -> st.getDelegateState() == actualStateRaw);
                if (!alreadyExists) {
                    registeredStates.add(cachingStateToRegister);
                }
            }
            return (S) cachingStateToRegister;
        } else {
             // Should not happen if logic above is correct and creates a caching wrapper
            LOG.error("Failed to create a caching wrapper for a supported state type: {}", stateDescriptor.getType());
            return (S) actualStateRaw; // Fallback, though indicates an issue
        }
    }

    private void registerCachingState(CachingInternalState<K, ?, ?, ?> cachingState) {
        synchronized (registeredStates) {
            boolean alreadyExists =
                    registeredStates.stream()
                            .anyMatch(st -> st.getDelegateState() == cachingState.getDelegateState());
            if (!alreadyExists) {
                registeredStates.add(cachingState);
            }
        }
    }

    // --- Methods to delegate to underlyingKeyedStateBackend ---
    // Most methods of AbstractKeyedStateBackend should be delegated. Some might require
    // interaction with the cache (e.g., snapshotting).

    @Override
    public void setCurrentKey(K newKey) {
        if (newKey == null) {
            // The CachingKeyedStateBackend manages its perception of the current key via its keyContext.
            getKeyContext().setCurrentKey(null); // InternalKeyContext can handle null.
            // Explicitly DO NOT call delegateKeyedStateBackend.setCurrentKey(null) here,
            // as AbstractKeyedStateBackend (and thus RocksDBKeyedStateBackend) throws NPE.
        } else {
            super.setCurrentKey(newKey); // This is for non-null keys, should be safe.
            if (delegateKeyedStateBackend != null) {
                delegateKeyedStateBackend.setCurrentKey(newKey);
            }
        }
    }

    @Override
    public void dispose() {
        super.dispose();
        delegateKeyedStateBackend.dispose();
        synchronized (registeredStates) {
            registeredStates.clear();
        }
        registeredPqs.clear();
    }

    @Nonnull
    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull
                    StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                            snapshotTransformFactory)
            throws Exception {
        return delegateKeyedStateBackend.createOrUpdateInternalState(
                namespaceSerializer, stateDesc, snapshotTransformFactory);
    }

    @Nonnull
    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull
            StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                    snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws Exception {
        return delegateKeyedStateBackend.createOrUpdateInternalState(
                namespaceSerializer, stateDesc, snapshotTransformFactory, allowFutureMetadataUpdates);
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        final KeyGroupedInternalPriorityQueue<T> delegateQueue =
                delegateKeyedStateBackend.create(stateName, byteOrderedElementSerializer);
        final CachingKeyGroupedInternalPriorityQueue<T> cachingPq =
                new CachingKeyGroupedInternalPriorityQueue<>(
                        delegateQueue,
                        getKeyContext(),
                        this,
                        byteOrderedElementSerializer);
        registeredPqs.add(cachingPq);
        return cachingPq;
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
    KeyGroupedInternalPriorityQueue<T> create(
            @Nonnull String stateName,
            @Nonnull TypeSerializer<T> byteOrderedElementSerializer,
            boolean allowFutureMetadataUpdates) {
        final KeyGroupedInternalPriorityQueue<T> delegateQueue =
                delegateKeyedStateBackend.create(
                        stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
        final CachingKeyGroupedInternalPriorityQueue<T> cachingPq =
                new CachingKeyGroupedInternalPriorityQueue<>(
                        delegateQueue,
                        getKeyContext(),
                        this,
                        byteOrderedElementSerializer);
        registeredPqs.add(cachingPq);
        return cachingPq;
    }

    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {
        for (CachingKeyGroupedInternalPriorityQueue<?> pq : registeredPqs) {
            pq.flush();
        }
        for (CachingInternalState<K, ?, ?, ?> cachingState : registeredStates) {
            cachingState.flushToUnderlyingState();
        }
        return delegateKeyedStateBackend.snapshot(
                checkpointId,
                timestamp,
                streamFactory,
                checkpointOptions);
    }

    // Other delegated methods (many of them, simplified here for brevity)
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        delegateKeyedStateBackend.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        delegateKeyedStateBackend.notifyCheckpointAborted(checkpointId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        return delegateKeyedStateBackend.getKeys(state, namespace);
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String stateName) {
        return delegateKeyedStateBackend.getKeysAndNamespaces(stateName);
    }

    @VisibleForTesting
    ColumnFamilyHandle getColumnFamilyHandle(String state) {
        if (delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend) {
            return ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend)
                    .getColumnFamilyHandle(state);
        }
        throw new UnsupportedOperationException("Delegate is not a RocksDBKeyedStateBackend");
    }

    public int getKeyGroupPrefixBytes() {
        if (delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend) {
            return ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend)
                    .getKeyGroupPrefixBytes();
        }
        throw new UnsupportedOperationException("Delegate is not a RocksDBKeyedStateBackend");
    }

    @VisibleForTesting
    PriorityQueueSetFactory getPriorityQueueFactory() {
        if (delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend) {
            return ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend)
                    .getPriorityQueueFactory();
        }
        throw new UnsupportedOperationException("Delegate is not a RocksDBKeyedStateBackend");
    }

    public WriteOptions getWriteOptions() {
        if (delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend) {
            return ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend).getWriteOptions();
        }
        throw new UnsupportedOperationException("Delegate is not a RocksDBKeyedStateBackend");
    }

    public ReadOptions getReadOptions() {
        if (delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend) {
            return ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend).getReadOptions();
        }
        throw new UnsupportedOperationException("Delegate is not a RocksDBKeyedStateBackend");
    }

    SerializedCompositeKeyBuilder<K> getSharedRocksKeyBuilder() {
        if (delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend) {
            return ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend)
                    .getSharedRocksKeyBuilder();
        }
        throw new UnsupportedOperationException("Delegate is not a RocksDBKeyedStateBackend");
    }

    @VisibleForTesting
    boolean isDisposed(){
        return ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend).isDisposed();
    }



    @Override
    public void registerKeySelectionListener(KeySelectionListener<K> listener) {
        delegateKeyedStateBackend.registerKeySelectionListener(listener);
    }

    @Override
    public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
        return delegateKeyedStateBackend.deregisterKeySelectionListener(listener);
    }

    @Override
    public SavepointResources<K> savepoint() throws Exception {
        // Now, we need to flush cache states to delegate state backend.
        // TODO: FLINK-13492.
        // Note: The following code is not thread-safe.
        // It's caller's responsibility to make sure the call is thread-safe.
        for (CachingKeyGroupedInternalPriorityQueue<?> pq : registeredPqs) {
            pq.flush();
        }
        for (CachingInternalState<K, ?, ?, ?> cachingState : registeredStates) {
            cachingState.flushToUnderlyingState();
        }
        return delegateKeyedStateBackend.savepoint();
    }

    @Override
    @org.apache.flink.annotation.VisibleForTesting
    public int numKeyValueStateEntries() {
        return delegateKeyedStateBackend.numKeyValueStateEntries();
    }

    @Override
    public boolean requiresLegacySynchronousTimerSnapshots(SnapshotType checkpointType) {
        return delegateKeyedStateBackend.requiresLegacySynchronousTimerSnapshots(checkpointType);
    }

    @Override
    public boolean isSafeToReuseKVState(){
        return delegateKeyedStateBackend.isSafeToReuseKVState();
    }

    @VisibleForTesting
    public void compactState(StateDescriptor<?, ?> stateDesc) throws RocksDBException {
        if (delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend) {
            ((RocksDBKeyedStateBackend<?>) delegateKeyedStateBackend).compactState(stateDesc);
        }
    }

    public static class CachingKvStateInfo extends RocksDBKeyedStateBackend.RocksDbKvStateInfo {
        public CachingKvStateInfo(
                ColumnFamilyHandle columnFamilyHandle,
                RegisteredStateMetaInfoBase metaInfo) {
                super(columnFamilyHandle, metaInfo);
        }
    }

    @Nonnegative
    long getWriteBatchSize() {
        if (delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend) {
            return ((RocksDBKeyedStateBackend<?>) delegateKeyedStateBackend).getWriteBatchSize();
        }
        return 0L;
    }

    private static LatencyTrackingStateConfig buildLatencyTrackingConfig(
            MetricGroup metricGroup, ExecutionConfig executionConfig) {
        return LatencyTrackingStateConfig.newBuilder()
                .setEnabled(
                        executionConfig.isLatencyTrackingConfigured()
                                && executionConfig
                                .getLatencyTrackingInterval() > 0)
                .setSampleInterval((int) executionConfig.getLatencyTrackingInterval())
                .setMetricGroup(metricGroup)
                .build();
    }

    public void reportCacheMemoryAdded(long sizeBytes) {
        if (maxCacheMemoryMb > 0) {
            long newSize = currentEstimatedCacheSizeBytes.addAndGet(sizeBytes);
            if (newSize > maxConfiguredCacheSizeBytes) {
                checkAndTriggerGlobalEviction();
            }
        }
    }

    public void reportCacheMemoryReleased(long sizeBytes) {
        if (maxCacheMemoryMb > 0) {
            currentEstimatedCacheSizeBytes.addAndGet(-sizeBytes);
            checkAndTriggerGlobalEviction();
        }
    }

    private void checkAndTriggerGlobalEviction() {
        long currentSize = currentEstimatedCacheSizeBytes.get();
        if (currentSize > maxConfiguredCacheSizeBytes) {
            long bytesToFree = currentSize - maxConfiguredCacheSizeBytes;
            LOG.info(
                    "Total cache size ({} bytes) exceeds limit ({} bytes). Triggering eviction to free {} bytes.",
                    currentSize,
                    maxConfiguredCacheSizeBytes,
                    bytesToFree);
            long totalFreed = 0;
            for (CachingInternalState<K, ?, ?, ?> state : registeredStates) {
                totalFreed += state.evictEntriesToFreeMemory(bytesToFree - totalFreed);
                if (totalFreed >= bytesToFree) {
                    break;
                }
            }
        }
    }

    @VisibleForTesting
    long getCurrentEstimatedCacheSizeBytesValue() {
        return currentEstimatedCacheSizeBytes.get();
    }

    @VisibleForTesting
    long getMaxConfiguredCacheSizeBytesValue() {
        return maxConfiguredCacheSizeBytes;
    }

    public double getMapCacheHitRateThreshold() {
        return mapCacheHitRateThreshold;
    }

    public long getMapCacheHitRateWindowSize() {
        return mapCacheHitRateWindowSize;
    }

    public long getMapCacheMinAccessesForBypassCheck() {
        return mapCacheMinAccessesForBypassCheck;
    }

    public boolean isMapKeyPresenceCacheEnabled() {
        return mapKeyPresenceCacheEnabled;
    }

    /** Utility: calculate key group index for the given key object. */
    public int getKeyGroupIndexForKey(Object key) {
        return KeyGroupRangeAssignment.assignToKeyGroup(key, getNumberOfKeyGroups());
    }

    /**
     * Groups a collection of {@code Keyed} elements by their key-group index.
     *
     * @param elements elements to group
     * @return map keyGroupId -> elements belonging to that key group
     */
    public <T extends Keyed<?>> java.util.Map<Integer, java.util.Collection<? extends T>> groupElementsbyKeyGroup(java.util.Collection<? extends T> elements) {
        java.util.Map<Integer, java.util.Collection<T>> out = new java.util.HashMap<>();
        for (T e : elements) {
            Object k = e.getKey();
            if (k == null) {
                k = getCurrentKey();
            }
            int kg = getKeyGroupIndexForKey(k);
            out.computeIfAbsent(kg, idx -> new java.util.ArrayList<>()).add(e);
        }
        return (java.util.Map) out;
    }

    // ---------------------------------------------------------------------
    //  Mini-batch support
    // ---------------------------------------------------------------------

    /**
     * Flushes buffered writes of all registered caching states and priority queues. Operators can
     * call this once at the end of their mini-batch loop to persist the batch atomically to the
     * underlying backend (RocksDB). It is <b>cheap</b>; if nothing is dirty nothing is written.
     */
    public void flushOnMiniBatchEnd() throws java.io.IOException {
        for (CachingKeyGroupedInternalPriorityQueue<?> pq : registeredPqs) {
            pq.flush();
        }
        for (CachingInternalState<K, ?, ?, ?> cachingState : registeredStates) {
            cachingState.flushOnMiniBatchEnd();
        }
    }
}