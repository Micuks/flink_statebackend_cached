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
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalMapState;
import org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalValueState;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PresenceCacheImplementation;
import org.apache.flink.contrib.streaming.state.cachekit.metrics.SnapshotCacheMetrics;
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
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
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
    private final Map<Object, Object> wrappersByDelegateIdentity = new IdentityHashMap<>();

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
            int mapSnapshotCacheMaxEntries) {
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
                    valueHitRateWindow);
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
                    valueHitRateWindow);
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

        return state;
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>> KeyGroupedInternalPriorityQueue<T> create(
            String stateName, TypeSerializer<T> byteOrderedElementSerializer) {
        return delegate.create(stateName, byteOrderedElementSerializer);
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>> KeyGroupedInternalPriorityQueue<T> create(
            String stateName, TypeSerializer<T> byteOrderedElementSerializer, boolean allowFutureMetadataUpdates) {
        return delegate.create(stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
    }

    @Override
    public void dispose() {
        flushAllSnapshotMetrics();
        wrappersByDelegateIdentity.clear();
        delegate.dispose();
    }

    @Override
    public void close() throws IOException {
        flushAllSnapshotMetrics();
        wrappersByDelegateIdentity.clear();
        delegate.close();
    }

    private void flushAllSnapshotMetrics() {
        for (Object wrapper : wrappersByDelegateIdentity.values()) {
            if (wrapper instanceof CachedInternalMapState) {
                SnapshotCacheMetrics metrics =
                        ((CachedInternalMapState<?, ?, ?, ?>) wrapper).getSnapshotCacheMetrics();
                if (metrics != null) {
                    metrics.flushToFile();
                }
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
        for (Object wrapper : wrappersByDelegateIdentity.values()) {
            if (wrapper instanceof CachedInternalValueState) {
                ((CachedInternalValueState<?, ?, ?>) wrapper).flush();
            } else if (wrapper instanceof CachedInternalMapState) {
                ((CachedInternalMapState<?, ?, ?, ?>) wrapper).flush();
            }
        }
        return delegate.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
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
