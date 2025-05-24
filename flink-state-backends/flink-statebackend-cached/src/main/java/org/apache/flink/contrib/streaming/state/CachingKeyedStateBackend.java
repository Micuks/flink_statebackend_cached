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

import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.snapshot.RocksDBSnapshotStrategyBase;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nonnegative;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;
import java.util.Set;
import java.util.function.Function;

import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBResourceContainer;
import org.apache.flink.contrib.streaming.state.snapshot.RocksDBSnapshotStrategyBase;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricMonitor;
import org.apache.flink.contrib.streaming.state.RocksDBWriteBatchWrapper;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.rocksdb.ReadOptions;

/**
 * The keyed state backend that implements caching. It wraps a delegate AbstractKeyedStateBackend
 * (e.g., RocksDBKeyedStateBackend) and creates CachingInternal*State objects.
 */
public class CachingKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private static final long serialVersionUID = 1L;

    private final AbstractKeyedStateBackend<K> delegateKeyedStateBackend; // RocksDBKeyedStateBackend
    private final int l1EntryCacheSize;
    private final int l2EntryCacheSize;
    private final int maxActiveNamespaceOrPerKeyCacheContainers;
    private final long maxCacheMemoryMb;
    private final CachingStateBackendFactory.CachePolicyType cachePolicyType;

    private final List<CachingInternalState<K, ?, ?, ?>> registeredStates;

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
            long maxCacheMemoryMb, CachingStateBackendFactory.CachePolicyType cachePolicyType) {

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
            CachingStateBackendFactory.CachePolicyType cachePolicyType
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

        this.l1EntryCacheSize = l1EntryCacheSize;
        this.l2EntryCacheSize = l2EntryCacheSize;
        this.maxActiveNamespaceOrPerKeyCacheContainers = maxActiveNamespaceOrPerKeyCacheContainers;
        this.maxCacheMemoryMb = maxCacheMemoryMb;
        this.registeredStates = new ArrayList<>();
        this.cachePolicyType = cachePolicyType;
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

        InternalKvState<K, N, ?> actualStateRaw =
                (InternalKvState<K, N, ?>)
                        delegateKeyedStateBackend.getOrCreateKeyedState(
                                namespaceSerializer, stateDescriptor);

        if (stateDescriptor.getType() == StateDescriptor.Type.VALUE
                && actualStateRaw instanceof InternalValueState) {
            InternalValueState<K, N, V_SD> actualState =
                    (InternalValueState<K, N, V_SD>) actualStateRaw;
            CachingInternalValueState<K, N, V_SD> cachingState =
                    new CachingInternalValueState<K, N, V_SD>(
                            actualState,
                            this,
                            l1EntryCacheSize,
                            l2EntryCacheSize,
                            maxActiveNamespaceOrPerKeyCacheContainers,
                            this.maxCacheMemoryMb, this.cachePolicyType);
            synchronized (registeredStates) {
                boolean alreadyExists =
                        registeredStates.stream()
                                .anyMatch(st -> st.getDelegateState() == actualState);
                if (!alreadyExists) {
                    registeredStates.add(cachingState);
                }
            }
            return (S) cachingState;
        } else if (stateDescriptor.getType() == StateDescriptor.Type.MAP
                && actualStateRaw instanceof InternalMapState) {
            // For MapStateDescriptor<UK, UV>, V_SD is Map<UK, UV>.
            // We need to cast actualStateRaw to its specific InternalMapState type.
            InternalMapState<K, N, Object, Object> actualState =
                    (InternalMapState<K, N, Object, Object>) actualStateRaw;

            CachingInternalMapState<K, N, Object, Object> cachingMapState =
                    new CachingInternalMapState<>(actualState, this, l1EntryCacheSize,
                            l2EntryCacheSize, maxActiveNamespaceOrPerKeyCacheContainers,
                            this.maxCacheMemoryMb, this.cachePolicyType);
            synchronized (registeredStates) {
                boolean alreadyExists = registeredStates.stream()
                        .anyMatch(st -> st.getDelegateState() == actualState);
                if (!alreadyExists) {
                    registeredStates.add(cachingMapState);
                }
            }
            return (S) cachingMapState;
        } else if (stateDescriptor.getType() == StateDescriptor.Type.LIST
                && actualStateRaw instanceof InternalListState) {
            // For ListStateDescriptor<V_ELE>, V_SD is List<V_ELE>.
            InternalListState<K, N, Object> actualState =
                    (InternalListState<K, N, Object>) actualStateRaw;

            CachingInternalListState<K, N, Object> cachingListState =
                    new CachingInternalListState<>(actualState, this, l1EntryCacheSize, // Max
                                                                                        // K->List
                                                                                        // entries
                                                                                        // in L1 per
                                                                                        // Namespace
                            l2EntryCacheSize, // Max K->List entries in L2 per Namespace
                            maxActiveNamespaceOrPerKeyCacheContainers, // Max Namespaces for L1/L2
                                                                       // of N->(K->List)
                            this.cachePolicyType);
            synchronized (registeredStates) {
                boolean alreadyExists = registeredStates.stream()
                        .anyMatch(st -> st.getDelegateState() == actualState);
                if (!alreadyExists) {
                    registeredStates.add(cachingListState);
                }
            }
            return (S) cachingListState;
        }
        return (S) actualStateRaw;
    }

    // --- Methods to delegate to underlyingKeyedStateBackend ---
    // Most methods of AbstractKeyedStateBackend should be delegated. Some might require
    // interaction with the cache (e.g., snapshotting).

    @Override
    public void setCurrentKey(K newKey) {
        super.setCurrentKey(newKey);
        delegateKeyedStateBackend.setCurrentKey(newKey); // Keep delegate in sync
    }

    @Override
    public void dispose() {
        super.dispose();
        delegateKeyedStateBackend.dispose();
        synchronized (registeredStates) {
            registeredStates.clear();
        }
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
        KeyGroupedInternalPriorityQueue<T> delegateQueue =
                delegateKeyedStateBackend.create(stateName, byteOrderedElementSerializer);

        // Return a proxy that ensures isEmpty() is consistent with poll() operations
        return new KeyGroupedInternalPriorityQueue<T>() {
            private int elementCount = 0;

            @Override
            public T poll() {
                T result = delegateQueue.poll();
                if (result != null) {
                    elementCount--;
                }
                return result;
            }

            @Override
            public T peek() {
                return delegateQueue.peek();
            }

            @Override
            public boolean add(@Nonnull T toAdd) {
                boolean result = delegateQueue.add(toAdd);
                if (result) {
                    elementCount++;
                }
                return result;
            }

            @Override
            public boolean remove(@Nonnull T toRemove) {
                boolean result = delegateQueue.remove(toRemove);
                if (result) {
                    elementCount--;
                }
                return result;
            }

            @Override
            public boolean isEmpty() {
                return elementCount == 0;
            }

            @Override
            public int size() {
                return elementCount;
            }

            @Override
            public void addAll(@Nonnull Collection<? extends T> toAdd) {
                for (T element : toAdd) {
                    add(element);
                }
            }

            @Nonnull
            @Override
            public CloseableIterator<T> iterator() {
                return delegateQueue.iterator();
            }

            @Nonnull
            @Override
            public Set<T> getSubsetForKeyGroup(int keyGroupId) {
                return delegateQueue.getSubsetForKeyGroup(keyGroupId);
            }
        };
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
    KeyGroupedInternalPriorityQueue<T> create(
            @Nonnull String stateName,
            @Nonnull TypeSerializer<T> byteOrderedElementSerializer,
            boolean allowFutureMetadataUpdates) {
        KeyGroupedInternalPriorityQueue<T> delegateQueue = delegateKeyedStateBackend
                .create(stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);

        // Return a proxy that ensures isEmpty() is consistent with poll() operations
        return new KeyGroupedInternalPriorityQueue<T>() {
            private int elementCount = 0;

            @Override
            public T poll() {
                T result = delegateQueue.poll();
                if (result != null) {
                    elementCount--;
                }
                return result;
            }

            @Override
            public T peek() {
                return delegateQueue.peek();
            }

            @Override
            public boolean add(@Nonnull T toAdd) {
                boolean result = delegateQueue.add(toAdd);
                if (result) {
                    elementCount++;
                }
                return result;
            }

            @Override
            public boolean remove(@Nonnull T toRemove) {
                boolean result = delegateQueue.remove(toRemove);
                if (result) {
                    elementCount--;
                }
                return result;
            }

            @Override
            public boolean isEmpty() {
                return elementCount == 0;
            }

            @Override
            public int size() {
                return elementCount;
            }

            @Override
            public void addAll(@Nonnull Collection<? extends T> toAdd) {
                for (T element : toAdd) {
                    add(element);
                }
            }

            @Nonnull
            @Override
            public CloseableIterator<T> iterator() {
                return delegateQueue.iterator();
            }

            @Nonnull
            @Override
            public Set<T> getSubsetForKeyGroup(int keyGroupId) {
                return delegateQueue.getSubsetForKeyGroup(keyGroupId);
            }
        };
    }

    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {

        synchronized (registeredStates) {
            for (CachingInternalState<K, ?, ?, ?> state : registeredStates) {
                try {
                    state.flushToUnderlyingState();
                } catch (IOException e) {
                    System.err.println(
                            "Error flushing state "
                                    + state
                                    + " during snapshot: "
                                    + e.getMessage());
                }
            }
        }
        return delegateKeyedStateBackend.snapshot(
                checkpointId, timestamp, streamFactory, checkpointOptions);
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
        synchronized (registeredStates) {
            for (CachingInternalState<K, ?, ?, ?> state : registeredStates) {
                try {
                    state.flushToUnderlyingState();
                } catch (IOException e) {
                    System.err.println(
                            "Error flushing state "
                                    + state
                                    + " during savepoint: "
                                    + e.getMessage());
                }
            }
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
        return true;
    }

    @VisibleForTesting
    public void compactState(StateDescriptor<?, ?> stateDesc) throws RocksDBException {
        if (delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend) {
            ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend).compactState(stateDesc);
        } else {
            throw new UnsupportedOperationException("Delegate is not a RocksDBKeyedStateBackend");
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
            return ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend).getWriteBatchSize();
        }
        throw new UnsupportedOperationException("Delegate is not a RocksDBKeyedStateBackend");
    }

    private static LatencyTrackingStateConfig buildLatencyTrackingConfig(
            MetricGroup metricGroup, ExecutionConfig executionConfig) {
        LatencyTrackingStateConfig.Builder latencyBuilder = LatencyTrackingStateConfig.newBuilder();
        if (metricGroup != null) {
            latencyBuilder.setMetricGroup(metricGroup);
        }
        return latencyBuilder
                .setEnabled(executionConfig.getLatencyTrackingInterval() > 0)
                .build();
    }
}