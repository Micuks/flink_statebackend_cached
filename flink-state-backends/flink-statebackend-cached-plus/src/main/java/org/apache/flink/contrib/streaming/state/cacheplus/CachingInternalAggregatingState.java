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

package org.apache.flink.contrib.streaming.state.cacheplus;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.CachingPlusKeyedStateBackend;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalKvState.StateIncrementalVisitor;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * An {@link InternalAggregatingState} that uses an L1/L2 cache for its accumulator.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <IN> The type of the input value.
 * @param <ACC> The type of the accumulator.
 * @param <OUT> The type of the output value.
 */
public class CachingInternalAggregatingState<K, N, IN, ACC, OUT>
        implements InternalAggregatingState<K, N, IN, ACC, OUT>,
        CachingInternalState<K, N, ACC, InternalAggregatingState<K, N, IN, ACC, OUT>> {

    private final InternalAggregatingState<K, N, IN, ACC, OUT> delegateState;
    private final CachingPlusKeyedStateBackend<K> backend;
    private final AggregateFunction<IN, ACC, OUT> aggFunction;
    private final CachePolicy<N, CachePolicy<K, CacheEntry<ACC>>> namespaceCachesL1;
    private final CachePolicy<N, CachePolicy<K, CacheEntry<ACC>>> namespaceCachesL2;

    private final int l1CacheSizePerKeyPerNamespace;
    private final int l2CacheSizePerKeyPerNamespace;
    private final CachingPlusStateBackendFactory.CachePolicyType cachePolicyType;

    private N currentNamespace;

    // Metrics
    private final Counter cacheHits;
    private final Counter cacheMisses;

    public CachingInternalAggregatingState(
            InternalAggregatingState<K, N, IN, ACC, OUT> delegateState,
            CachingPlusKeyedStateBackend<K> backend,
            AggregateFunction<IN, ACC, OUT> aggFunction,
            int l1CacheSize,
            int l2CacheSize,
            CachingPlusStateBackendFactory.CachePolicyType cachePolicyType,
            MetricGroup metricsGroup) {
        this.delegateState = delegateState;
        this.backend = backend;
        this.aggFunction = aggFunction;
        this.l1CacheSizePerKeyPerNamespace = l1CacheSize;
        this.l2CacheSizePerKeyPerNamespace = l2CacheSize;
        this.cachePolicyType = cachePolicyType;

        if (metricsGroup != null) {
            this.cacheHits = metricsGroup.counter("hits");
            this.cacheMisses = metricsGroup.counter("misses");
        } else {
            Counter dummyCounter = new Counter() {
                @Override public void inc() {}
                @Override public void inc(long n) {}
                @Override public void dec() {}
                @Override public void dec(long n) {}
                @Override public long getCount() { return 0; }
            };
            this.cacheHits = dummyCounter;
            this.cacheMisses = dummyCounter;
        }

        this.namespaceCachesL1 = createCachePolicyWithEvictionListener(backend.getMaxActiveNamespaceOrPerKeyCacheContainers(),
                evictedNsEntry -> flushCacheForNamespace(evictedNsEntry.getKey(), evictedNsEntry.getValue()));
        this.namespaceCachesL2 = createCachePolicyWithEvictionListener(backend.getMaxActiveNamespaceOrPerKeyCacheContainers(),
                evictedNsEntry -> flushCacheForNamespace(evictedNsEntry.getKey(), evictedNsEntry.getValue()));
    }

    // Overloaded constructor with explicit maxActiveNamespaces override
    public CachingInternalAggregatingState(
            InternalAggregatingState<K, N, IN, ACC, OUT> delegateState,
            CachingPlusKeyedStateBackend<K> backend,
            AggregateFunction<IN, ACC, OUT> aggFunction,
            int l1CacheSize,
            int l2CacheSize,
            CachingPlusStateBackendFactory.CachePolicyType cachePolicyType,
            MetricGroup metricsGroup,
            int maxActiveNamespaces) {
        this.delegateState = delegateState;
        this.backend = backend;
        this.aggFunction = aggFunction;
        this.l1CacheSizePerKeyPerNamespace = l1CacheSize;
        this.l2CacheSizePerKeyPerNamespace = l2CacheSize;
        this.cachePolicyType = cachePolicyType;

        if (metricsGroup != null) {
            this.cacheHits = metricsGroup.counter("hits");
            this.cacheMisses = metricsGroup.counter("misses");
        } else {
            Counter dummyCounter = new Counter() {
                @Override public void inc() {}
                @Override public void inc(long n) {}
                @Override public void dec() {}
                @Override public void dec(long n) {}
                @Override public long getCount() { return 0; }
            };
            this.cacheHits = dummyCounter;
            this.cacheMisses = dummyCounter;
        }

        this.namespaceCachesL1 = createCachePolicyWithEvictionListener(maxActiveNamespaces,
                evictedNsEntry -> flushCacheForNamespace(evictedNsEntry.getKey(), evictedNsEntry.getValue()));
        this.namespaceCachesL2 = createCachePolicyWithEvictionListener(maxActiveNamespaces,
                evictedNsEntry -> flushCacheForNamespace(evictedNsEntry.getKey(), evictedNsEntry.getValue()));
    }

    private <CK, CV> CachePolicy<CK, CV> createCachePolicy(int capacity) {
        switch (cachePolicyType) {
            case TINYLFU:
                return new TinyLFUMap<>(capacity);
            case LRU:
            default:
                return new LRUMap<>(capacity);
        }
    }

    private <CK, CV> CachePolicy<CK, CV> createCachePolicyWithEvictionListener(int capacity,
                                                                               Consumer<Map.Entry<CK, CV>> evictionListener) {
        switch (cachePolicyType) {
            case TINYLFU:
                return new TinyLFUMap<>(capacity, evictionListener);
            case LRU:
            default:
                return new LRUMap<>(capacity, evictionListener);
        }
    }

    private void flushCacheForNamespace(N namespace, CachePolicy<K, CacheEntry<ACC>> cache) {
        K originalKey = backend.getCurrentKey();
        N originalNamespace = getCurrentNamespace();
        try {
            setCurrentNamespace(namespace);
            for (Map.Entry<K, CacheEntry<ACC>> entry : cache.entrySet()) {
                if (entry.getValue().isDirty()) {
                    backend.setCurrentKey(entry.getKey());
                    try {
                        delegateState.updateInternal(entry.getValue().getValue());
                        entry.getValue().setDirty(false);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to flush dirty entry: " + entry.getKey(), e);
                    }
                }
            }
        } finally {
            setCurrentNamespace(originalNamespace);
            backend.setCurrentKey(originalKey);
        }
    }

    private CachePolicy<K, CacheEntry<ACC>> getL1CacheForNamespace(N namespace) {
        return namespaceCachesL1.computeIfAbsent(namespace, ns ->
                createCachePolicyWithEvictionListener(l1CacheSizePerKeyPerNamespace, evictedL1Entry -> {
                    CachePolicy<K, CacheEntry<ACC>> l2Cache = getL2CacheForNamespace(ns);
                    if (evictedL1Entry.getValue().isDirty()) {
                        K originalKey = backend.getCurrentKey();
                        N originalNamespace = getCurrentNamespace();
                        try {
                            backend.setCurrentKey(evictedL1Entry.getKey());
                            delegateState.setCurrentNamespace(ns);
                            delegateState.updateInternal(evictedL1Entry.getValue().getValue());
                            evictedL1Entry.getValue().setDirty(false);
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to flush L1 entry to delegate on eviction", e);
                        } finally {
                            backend.setCurrentKey(originalKey);
                            setCurrentNamespace(originalNamespace);
                        }
                    }
                    l2Cache.put(evictedL1Entry.getKey(), evictedL1Entry.getValue());
                }));
    }

    private CachePolicy<K, CacheEntry<ACC>> getL2CacheForNamespace(N namespace) {
        return namespaceCachesL2.computeIfAbsent(namespace, ns -> createCachePolicy(l2CacheSizePerKeyPerNamespace));
    }

    @Override
    public OUT get() throws Exception {
        return aggFunction.getResult(getInternal());
    }

    @Override
    public void add(IN value) throws Exception {
        if (value == null) {
            return;
        }

        ACC currentAccumulator = getInternal();
        if (currentAccumulator == null) {
            currentAccumulator = aggFunction.createAccumulator();
        }
        updateInternal(aggFunction.add(value, currentAccumulator));
    }

    @Override
    public ACC getInternal() throws Exception {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        CachePolicy<K, CacheEntry<ACC>> l1Cache = getL1CacheForNamespace(currentNamespace);
        CacheEntry<ACC> l1Entry = l1Cache.get(currentKey);

        if (l1Entry != null) {
            cacheHits.inc();
            return l1Entry.getValue();
        }

        CachePolicy<K, CacheEntry<ACC>> l2Cache = getL2CacheForNamespace(currentNamespace);
        CacheEntry<ACC> l2Entry = l2Cache.remove(currentKey);

        if (l2Entry != null) {
            cacheHits.inc();
            CacheEntry<ACC> oldL1Entry = l1Cache.put(currentKey, l2Entry);
            if (oldL1Entry != null) {
                backend.reportCacheMemoryReleased(oldL1Entry.getEstimatedSizeBytes());
            }
            backend.reportCacheMemoryAdded(l2Entry.getEstimatedSizeBytes());
            return l2Entry.getValue();
        }

        cacheMisses.inc();
        ACC valueFromDelegate = delegateState.getInternal();
        if (valueFromDelegate != null) {
            CacheEntry<ACC> newEntry = CacheEntry.clean(valueFromDelegate);
            CacheEntry<ACC> oldL1Entry = l1Cache.put(currentKey, newEntry);
            if (oldL1Entry != null) {
                backend.reportCacheMemoryReleased(oldL1Entry.getEstimatedSizeBytes());
            }
            backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
        }
        return valueFromDelegate;
    }

    @Override
    public void updateInternal(ACC valueToStore) throws Exception {
        // AggregatingState is heavily intertwined with runtime operators which may
        // read the delegate state immediately after an update. To guarantee correctness
        // under all call paths without disabling caching wholesale, we use write-through
        // semantics here: update the delegate first, then keep a clean L1 copy for locality.

        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        // Write-through to delegate to ensure visibility
        delegateState.updateInternal(valueToStore);

        // Maintain a clean entry in L1 for fast subsequent reads
        CachePolicy<K, CacheEntry<ACC>> l1Cache = getL1CacheForNamespace(currentNamespace);
        CacheEntry<ACC> cleanEntry = CacheEntry.clean(valueToStore);
        CacheEntry<ACC> oldL1Entry = l1Cache.put(currentKey, cleanEntry);
        if (oldL1Entry != null) {
            backend.reportCacheMemoryReleased(oldL1Entry.getEstimatedSizeBytes());
        }
        backend.reportCacheMemoryAdded(cleanEntry.getEstimatedSizeBytes());

        // Remove any stale copy from L2
        CachePolicy<K, CacheEntry<ACC>> l2Cache = getL2CacheForNamespace(currentNamespace);
        CacheEntry<ACC> oldL2 = l2Cache.remove(currentKey);
        if (oldL2 != null) {
            backend.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes());
        }
    }

    @Override
    public void clear() {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        CachePolicy<K, CacheEntry<ACC>> l1Cache = getL1CacheForNamespace(currentNamespace);
        CacheEntry<ACC> oldL1Entry = l1Cache.remove(currentKey);
        if (oldL1Entry != null) {
            backend.reportCacheMemoryReleased(oldL1Entry.getEstimatedSizeBytes());
        }

        CachePolicy<K, CacheEntry<ACC>> l2Cache = getL2CacheForNamespace(currentNamespace);
        CacheEntry<ACC> oldL2Entry = l2Cache.remove(currentKey);
        if (oldL2Entry != null) {
            backend.reportCacheMemoryReleased(oldL2Entry.getEstimatedSizeBytes());
        }

        delegateState.clear();
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (sources == null || sources.isEmpty()) {
            return;
        }

        flushToUnderlyingState();

        clearCacheForNamespace(target);
        for (N source : sources) {
            clearCacheForNamespace(source);
        }

        delegateState.mergeNamespaces(target, sources);
    }

    private void clearCacheForNamespace(N namespace) {
        CachePolicy<K, CacheEntry<ACC>> l1Cache = namespaceCachesL1.get(namespace);
        if (l1Cache != null) {
            for (Map.Entry<K, CacheEntry<ACC>> entry : l1Cache.entrySet()) {
                if (entry.getValue() != null) {
                    backend.reportCacheMemoryReleased(entry.getValue().getEstimatedSizeBytes());
                }
            }
            l1Cache.clear();
        }
        CachePolicy<K, CacheEntry<ACC>> l2Cache = namespaceCachesL2.get(namespace);
        if (l2Cache != null) {
            for (Map.Entry<K, CacheEntry<ACC>> entry : l2Cache.entrySet()) {
                if (entry.getValue() != null) {
                    backend.reportCacheMemoryReleased(entry.getValue().getEstimatedSizeBytes());
                }
            }
            l2Cache.clear();
        }
    }

    @Override
    public void flushToUnderlyingState() throws IOException {
        K originalKey = backend.getCurrentKey();
        N originalNamespace = getCurrentNamespace();
        try {
            for (Map.Entry<N, CachePolicy<K, CacheEntry<ACC>>> nsEntry : namespaceCachesL1.entrySet()) {
                N namespace = nsEntry.getKey();
                CachePolicy<K, CacheEntry<ACC>> l1Cache = nsEntry.getValue();
                setCurrentNamespace(namespace);

                List<Map.Entry<K, CacheEntry<ACC>>> currentL1Entries = new ArrayList<>();
                for (Map.Entry<K, CacheEntry<ACC>> e : l1Cache.entrySet()) {
                    currentL1Entries.add(e);
                }

                for (Map.Entry<K, CacheEntry<ACC>> mapEntry : currentL1Entries) {
                    K key = mapEntry.getKey();
                    CacheEntry<ACC> entry = mapEntry.getValue();
                    if (entry.isDirty()) {
                        if (key != null) {
                            backend.setCurrentKey(key);
                            try {
                                delegateState.updateInternal(entry.getValue());
                                entry.setDirty(false);
                            } catch (Exception e) {
                                throw new IOException("Failed to flush entry for key: " + key, e);
                            }
                        }
                    }
                }
            }
        } finally {
            backend.setCurrentKey(originalKey);
            if (originalNamespace != null) {
                setCurrentNamespace(originalNamespace);
            } else {
                this.currentNamespace = null;
            }
        }
    }

    @Override
    public InternalAggregatingState<K, N, IN, ACC, OUT> getDelegateState() {
        return delegateState;
    }

    @Override
    public long evictEntriesToFreeMemory(long targetBytesToFreeThisState) {
        long bytesFreed = 0;
        if (targetBytesToFreeThisState <= 0) return 0;

        List<N> l2Namespaces = new ArrayList<>();
        for (Map.Entry<N, CachePolicy<K, CacheEntry<ACC>>> entry : namespaceCachesL2.entrySet()) {
            l2Namespaces.add(entry.getKey());
        }

        for (N namespace : l2Namespaces) {
            CachePolicy<K, CacheEntry<ACC>> l2Cache = namespaceCachesL2.get(namespace);
            if (l2Cache == null || l2Cache.isEmpty()) continue;

            Iterator<Map.Entry<K, CacheEntry<ACC>>> l2Iter = l2Cache.entrySet().iterator();
            while (l2Iter.hasNext() && bytesFreed < targetBytesToFreeThisState) {
                Map.Entry<K, CacheEntry<ACC>> entry = l2Iter.next();
                long estimatedSize = entry.getValue().getEstimatedSizeBytes();
                l2Iter.remove();
                backend.reportCacheMemoryReleased(estimatedSize);
                bytesFreed += estimatedSize;
            }
            if (bytesFreed >= targetBytesToFreeThisState) return bytesFreed;
        }

        // Additional eviction from L1 caches if needed
        List<N> l1Namespaces = new ArrayList<>();
        for (Map.Entry<N, CachePolicy<K, CacheEntry<ACC>>> entry : namespaceCachesL1.entrySet()) {
            l1Namespaces.add(entry.getKey());
        }

        for (N namespace : l1Namespaces) {
            if (bytesFreed >= targetBytesToFreeThisState) break;

            CachePolicy<K, CacheEntry<ACC>> l1Cache = namespaceCachesL1.get(namespace);
            if (l1Cache == null || l1Cache.isEmpty()) continue;

            Iterator<Map.Entry<K, CacheEntry<ACC>>> iterClean = l1Cache.entrySet().iterator();
            List<Map.Entry<K, CacheEntry<ACC>>> dirtyEntries = new ArrayList<>();

            while (iterClean.hasNext() && bytesFreed < targetBytesToFreeThisState) {
                Map.Entry<K, CacheEntry<ACC>> entry = iterClean.next();
                CacheEntry<ACC> cacheEntry = entry.getValue();
                if (!cacheEntry.isDirty()) {
                    long est = cacheEntry.getEstimatedSizeBytes();
                    iterClean.remove();
                    backend.reportCacheMemoryReleased(est);
                    bytesFreed += est;
                } else {
                    dirtyEntries.add(entry);
                }
            }

            if (bytesFreed >= targetBytesToFreeThisState) return bytesFreed;

            for (Map.Entry<K, CacheEntry<ACC>> dirtyEntry : dirtyEntries) {
                if (bytesFreed >= targetBytesToFreeThisState) break;
                K key = dirtyEntry.getKey();
                CacheEntry<ACC> cacheEntry = dirtyEntry.getValue();
                long est = cacheEntry.getEstimatedSizeBytes();
                K originalKey = backend.getCurrentKey();
                N originalNs = getCurrentNamespace();
                try {
                    backend.setCurrentKey(key);
                    setCurrentNamespace(namespace);
                    delegateState.updateInternal(cacheEntry.getValue());
                    cacheEntry.setDirty(false);

                    l1Cache.remove(key);
                    backend.reportCacheMemoryReleased(est);
                    bytesFreed += est;

                } catch (Exception e) {
                    // Ignore flush failure during eviction
                } finally {
                    backend.setCurrentKey(originalKey);
                    setCurrentNamespace(originalNs);
                }
            }
        }

        return bytesFreed;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return delegateState.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return delegateState.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<ACC> getValueSerializer() {
        return delegateState.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(@Nonnull N namespace) {
        this.currentNamespace = namespace;
        if (namespace != null) {
            delegateState.setCurrentNamespace(namespace);
        }
    }

    @Nonnull
    public N getCurrentNamespace() {
        return this.currentNamespace;
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<ACC> safeValueSerializer)
            throws Exception {
        return delegateState.getSerializedValue(
                serializedKeyAndNamespace,
                safeKeySerializer,
                safeNamespaceSerializer,
                safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, ACC> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        try {
            flushToUnderlyingState();
        } catch (IOException e) {
            throw new RuntimeException("Error flushing state before creating visitor.", e);
        }
        return delegateState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }
} 
