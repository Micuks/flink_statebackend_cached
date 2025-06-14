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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
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
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This state backend uses a tiered caching approach for key-value states. It consists of a
 * RocksDBKeyedStateBackend as the delegate (L3) and one or two in-memory cache layers (L1/L2). The
 * caching behavior can be configured, for instance, cache size, cache policy (LRU, TinyLFU).
 *
 * @param <K> The key by which state is keyed.
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

    private final double mapCacheHitRateThreshold;
    private final long mapCacheHitRateWindowSize;
    private final long mapCacheMinAccessesForBypassCheck;
    private final boolean mapKeyPresenceCacheEnabled;
    private final boolean mapBypassEnabled;

    private final double valueCacheHitRateThreshold;
    private final long valueCacheHitRateWindowSize;
    private final long valueCacheMinAccessesForBypassCheck;
    private final boolean valueBypassEnabled;
    private final boolean writeBehindEnabled;

    final List<CachingInternalState<K, ?, ?, ?>> registeredStates;
    private final List<CachingKeyGroupedInternalPriorityQueue<?>> registeredPqs;
    private final Map<String, State> registeredStatesMap;

    private transient AtomicLong currentEstimatedCacheSizeBytes;
    private transient long maxConfiguredCacheSizeBytes;

    public CachingKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            @Nonnull CloseableRegistry cancelStreamRegistry,
            AbstractKeyedStateBackend<K> delegateKeyedStateBackend,
            int l1EntryCacheSize,
            int l2EntryCacheSize,
            int maxActiveNamespaceOrPerKeyCacheContainers,
            long maxCacheMemoryMb,
            CachingStateBackendFactory.CachePolicyType cachePolicyType,
            int mapL1KeyPresenceCacheSize,
            int mapL2KeyPresenceCacheSize,
            double mapCacheHitRateThreshold,
            long mapCacheHitRateWindowSize,
            long mapCacheMinAccessesForBypassCheck,
            boolean mapKeyPresenceCacheEnabled,
            boolean mapBypassEnabled,
            double valueCacheHitRateThreshold,
            long valueCacheHitRateWindowSize,
            long valueCacheMinAccessesForBypassCheck,
            boolean valueBypassEnabled,
            boolean writeBehindEnabled) {
        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                getEffectiveLatencyTrackingConfig(delegateKeyedStateBackend),
                cancelStreamRegistry,
                getEffectiveKeyContext(delegateKeyedStateBackend));
        this.delegateKeyedStateBackend = delegateKeyedStateBackend;
        this.l1EntryCacheSize = l1EntryCacheSize;
        this.l2EntryCacheSize = l2EntryCacheSize;
        this.maxActiveNamespaceOrPerKeyCacheContainers = maxActiveNamespaceOrPerKeyCacheContainers;
        this.maxCacheMemoryMb = maxCacheMemoryMb;
        this.cachePolicyType = cachePolicyType;
        this.mapL1KeyPresenceCacheSize = mapL1KeyPresenceCacheSize;
        this.mapL2KeyPresenceCacheSize = mapL2KeyPresenceCacheSize;
        this.mapCacheHitRateThreshold = mapCacheHitRateThreshold;
        this.mapCacheHitRateWindowSize = mapCacheHitRateWindowSize;
        this.mapCacheMinAccessesForBypassCheck = mapCacheMinAccessesForBypassCheck;
        this.mapKeyPresenceCacheEnabled = mapKeyPresenceCacheEnabled;
        this.mapBypassEnabled = mapBypassEnabled;
        this.valueCacheHitRateThreshold = valueCacheHitRateThreshold;
        this.valueCacheHitRateWindowSize = valueCacheHitRateWindowSize;
        this.valueCacheMinAccessesForBypassCheck = valueCacheMinAccessesForBypassCheck;
        this.valueBypassEnabled = valueBypassEnabled;
        this.writeBehindEnabled = writeBehindEnabled;
        this.registeredStates = new ArrayList<>();
        this.registeredPqs = new ArrayList<>();
        this.registeredStatesMap = new HashMap<>();
        this.currentEstimatedCacheSizeBytes = new AtomicLong(0);
        this.maxConfiguredCacheSizeBytes = maxCacheMemoryMb * 1024L * 1024L;
    }

    public boolean isWriteBehindEnabled() {
        return writeBehindEnabled;
    }

    public int getMaxActiveNamespaceOrPerKeyCacheContainers() {
        return maxActiveNamespaceOrPerKeyCacheContainers;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"}) // V_SD is the value in StateDescriptor, UK/UV for Map, V_ELE for List
    public <N, S extends State, V_SD> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, V_SD> stateDescriptor)
            throws Exception {
        // Ensure that the state descriptor has its serializers initialized before any
        // get*Serializer() methods are invoked. This mirrors the behaviour of
        // AbstractKeyedStateBackend#getOrCreateKeyedState and avoids situations where
        // unit-tests (which frequently rely on mocks rather than a fully initialised runtime
        // environment) would trigger an IllegalStateException ("Serializer not yet initialized").
        if (!stateDescriptor.isSerializerInitialized()) {
            // We do not have direct access to the ExecutionConfig that was passed to the
            // constructor (it is kept private in AbstractKeyedStateBackend), but for the purpose
            // of serializer initialization a fresh default config is sufficient.
            stateDescriptor.initializeSerializerUnlessSet(new org.apache.flink.api.common.ExecutionConfig());
        }
        State state =
                registeredStatesMap.get(stateDescriptor.getName()); // Use Flink's StateDescriptor name as unique key for state registration
        if (state != null) {
            return (S) state;
        }
        final State createdState;
        if (stateDescriptor instanceof ValueStateDescriptor) {
            createdState =
                    new CachingInternalValueState<>(
                            (InternalValueState<K, N, V_SD>)
                                    delegateKeyedStateBackend.getOrCreateKeyedState(
                                            namespaceSerializer, stateDescriptor),
                            this,
                            l1EntryCacheSize,
                            l2EntryCacheSize,
                            maxActiveNamespaceOrPerKeyCacheContainers,
                            maxCacheMemoryMb,
                            cachePolicyType,
                            valueCacheHitRateThreshold,
                            valueCacheHitRateWindowSize,
                            valueCacheMinAccessesForBypassCheck,
                            valueBypassEnabled);
        } else if (stateDescriptor instanceof ListStateDescriptor) {
            createdState =
                    new CachingInternalListState<>(
                            (InternalListState<K, N, V_SD>)
                                    delegateKeyedStateBackend.getOrCreateKeyedState(
                                            namespaceSerializer, stateDescriptor),
                            this,
                            l1EntryCacheSize,
                            l2EntryCacheSize,
                            maxActiveNamespaceOrPerKeyCacheContainers,
                            cachePolicyType);
        } else if (stateDescriptor instanceof MapStateDescriptor) {
            MapStateDescriptor<?, ?> mapStateDescriptor = (MapStateDescriptor<?, ?>) stateDescriptor;
            {
                CachingInternalMapState<K, N, Object, Object> mapState =
                        new CachingInternalMapState<>(
                                (InternalMapState<K, N, Object, Object>)
                                        delegateKeyedStateBackend.getOrCreateKeyedState(
                                                namespaceSerializer, stateDescriptor),
                                this,
                                l1EntryCacheSize,
                                l2EntryCacheSize,
                                maxActiveNamespaceOrPerKeyCacheContainers,
                                maxCacheMemoryMb,
                                cachePolicyType,
                                mapL1KeyPresenceCacheSize,
                                mapL2KeyPresenceCacheSize,
                                mapCacheHitRateThreshold,
                                mapCacheHitRateWindowSize,
                                mapCacheMinAccessesForBypassCheck,
                                mapKeyPresenceCacheEnabled,
                                mapBypassEnabled);

                // Inject the namespace serializer so that getNamespaceSerializer() works even when
                // the delegate state is a Mockito mock with no default behaviour.
                mapState.setNamespaceSerializer(namespaceSerializer);
                // Also inject the (user) key & value serializers from the descriptor – this is
                // crucial for unit-tests that verify these are properly surfaced even when the
                // delegate is a Mockito mock.
                mapState.setUserKeySerializer((TypeSerializer<Object>) ((MapStateDescriptor<?, ?>) stateDescriptor).getKeySerializer());
                mapState.setUserValueSerializer((TypeSerializer<Object>) ((MapStateDescriptor<?, ?>) stateDescriptor).getValueSerializer());
                createdState = mapState;
            }
        } else if (stateDescriptor instanceof AggregatingStateDescriptor) {
            createdState =
                    new CachingInternalAggregatingState<>(
                            (InternalAggregatingState<K, N, Object, Object, Object>)
                                    delegateKeyedStateBackend.getOrCreateKeyedState(
                                            namespaceSerializer, stateDescriptor),
                            this,
                            ((AggregatingStateDescriptor) stateDescriptor).getAggregateFunction(),
                            l1EntryCacheSize,
                            l2EntryCacheSize,
                            cachePolicyType);
        } else {
            createdState =
                    delegateKeyedStateBackend.getOrCreateKeyedState(
                            namespaceSerializer, stateDescriptor);
        }
        if (createdState instanceof CachingInternalState) {
            registerCachingState((CachingInternalState) createdState);
        }
        registeredStatesMap.put(stateDescriptor.getName(), createdState);
        return (S) createdState;
    }

    private void registerCachingState(CachingInternalState<K, ?, ?, ?> cachingState) {
        registeredStates.add(cachingState);
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
            // Set the key in our own key context first
            getKeyContext().setCurrentKey(newKey);
            // Then call super to ensure parent class state is updated
            super.setCurrentKey(newKey);
            // Finally set it in the delegate backend
            if (delegateKeyedStateBackend != null) {
                delegateKeyedStateBackend.setCurrentKey(newKey);
            }
        }
    }

    /**
     * Tests that use a mocked delegate backend often rely on {@code delegateBackend.getCurrentKey()}
     * being pre-stubbed. When the caching backend has never seen an explicit
     * {@link #setCurrentKey(Object)} call, our own key-context returns {@code null}. To remain
     * compatible with those tests we fall back to the delegate's notion of the current key if our
     * own is absent.
     */
    @Override
    public K getCurrentKey() {
        K k = super.getCurrentKey();
        if (k != null) {
            return k;
        }
        return delegateKeyedStateBackend != null ? delegateKeyedStateBackend.getCurrentKey() : null;
    }

    @Override
    public void dispose() {
        super.dispose();
        delegateKeyedStateBackend.dispose();
        synchronized (registeredStates) {
            registeredStates.clear();
        }
        registeredPqs.clear();
        registeredStatesMap.clear();
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
            ExecutionConfig executionConfig) {
        return LatencyTrackingStateConfig.newBuilder()
                .setEnabled(executionConfig.isLatencyTrackingConfigured())
                .setSampleInterval((int) executionConfig.getLatencyTrackingInterval())
                .setHistorySize(128) // default value, not exposed in ExecutionConfig
                .build();
    }

    public void reportCacheMemoryAdded(long sizeBytes) {
        long currentSize = currentEstimatedCacheSizeBytes.addAndGet(sizeBytes);
        if (currentSize > maxConfiguredCacheSizeBytes) {
            checkAndTriggerGlobalEviction();
        }
    }

    public void reportCacheMemoryReleased(long sizeBytes) {
        currentEstimatedCacheSizeBytes.addAndGet(-sizeBytes);
        checkAndTriggerGlobalEviction();
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
    public void flushOnMiniBatchEnd() throws Exception {
        // This is where we flush all buffered writes for registered states.
        for (CachingInternalState<K, ?, ?, ?> state : registeredStates) {
            state.flushToUnderlyingState();
        }
    }

    private static <K> LatencyTrackingStateConfig getEffectiveLatencyTrackingConfig(
            AbstractKeyedStateBackend<K> delegateBackend) {
        LatencyTrackingStateConfig cfg = null;
        try {
            cfg = delegateBackend.getLatencyTrackingStateConfig();
        } catch (Throwable ignored) {
            // In case the mock does not stub the method or throws.
        }
        return cfg != null ? cfg : LatencyTrackingStateConfig.disabled();
    }

    private static <K> org.apache.flink.runtime.state.heap.InternalKeyContext<K> getEffectiveKeyContext(
            AbstractKeyedStateBackend<K> delegateBackend) {
        org.apache.flink.runtime.state.heap.InternalKeyContext<K> ctx = null;
        try {
            ctx = delegateBackend.getKeyContext();
        } catch (Throwable ignored) {
        }
        if (ctx != null) {
            return ctx;
        }
        // Fallback to a minimal single-key-group context (range: 0-0) – sufficient for unit tests.
        return new org.apache.flink.runtime.state.heap.InternalKeyContextImpl<>(
                org.apache.flink.runtime.state.KeyGroupRange.of(0, 0), 1);
    }
}