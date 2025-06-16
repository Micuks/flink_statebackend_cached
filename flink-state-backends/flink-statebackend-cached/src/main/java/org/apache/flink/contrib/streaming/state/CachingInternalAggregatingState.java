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

package org.apache.flink.contrib.streaming.state;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalKvState.StateIncrementalVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An {@link InternalAggregatingState} that uses an L1/L2 cache for its accumulator.
 *
 * <p>The write-behind caching behavior can be configured via {@link
 * CachingStateBackendFactory#WRITE_BEHIND_ENABLED_CONFIG}. When disabled, writes will be performed
 * synchronously (write-through).
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

    private static final Logger LOG = LoggerFactory.getLogger(CachingInternalAggregatingState.class);
    private final InternalAggregatingState<K, N, IN, ACC, OUT> delegateState;
    private final CachingKeyedStateBackend<K> backend;
    private final AggregateFunction<IN, ACC, OUT> aggFunction;
    private final CachePolicy<N, CachePolicy<K, CacheEntry<ACC>>> namespaceCachesL1;
    private final CachePolicy<N, CachePolicy<K, CacheEntry<ACC>>> namespaceCachesL2;

    private final int l1CacheSizePerKeyPerNamespace;
    private final int l2CacheSizePerKeyPerNamespace;
    private final CachingStateBackendFactory.CachePolicyType cachePolicyType;

    private N currentNamespace;

    private final CachePolicy<N, Map<K, ACC>> namespaceWriteBuffers;

    private final boolean writeBehindEnabled;

    // Metrics
    private final AtomicLong cacheHits = new AtomicLong(0);
    private final AtomicLong cacheMisses = new AtomicLong(0);
    private final AtomicLong accessCount = new AtomicLong(0);
    private static final long LOG_HIT_RATE_EVERY_N_ACCESSES = 10000;

    public CachingInternalAggregatingState(
            InternalAggregatingState<K, N, IN, ACC, OUT> delegateState,
            CachingKeyedStateBackend<K> backend,
            AggregateFunction<IN, ACC, OUT> aggFunction,
            int l1CacheSize,
            int l2CacheSize,
            CachingStateBackendFactory.CachePolicyType cachePolicyType) {
        this.delegateState = delegateState;
        this.backend = backend;
        this.aggFunction = aggFunction;
        this.l1CacheSizePerKeyPerNamespace = l1CacheSize;
        this.l2CacheSizePerKeyPerNamespace = l2CacheSize;
        this.cachePolicyType = cachePolicyType;
        this.writeBehindEnabled = backend.isWriteBehindEnabled();

        this.namespaceWriteBuffers = createCachePolicyWithEvictionListener(backend.getMaxActiveNamespaceOrPerKeyCacheContainers(),
                evictedNsEntry -> {
                    try {
                        flushWriteBufferForNamespace(evictedNsEntry.getKey(), evictedNsEntry.getValue());
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to flush write-behind buffer for evicted namespace: " + evictedNsEntry.getKey(), e);
                    }
                });

        this.namespaceCachesL1 = createCachePolicyWithEvictionListener(backend.getMaxActiveNamespaceOrPerKeyCacheContainers(),
                evictedNsEntry -> flushCacheForNamespace(evictedNsEntry.getKey(), evictedNsEntry.getValue()));
        this.namespaceCachesL2 = createCachePolicyWithEvictionListener(backend.getMaxActiveNamespaceOrPerKeyCacheContainers(),
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

    private void flushWriteBufferForNamespace(N namespace, Map<K, ACC> writeBuffer) throws Exception {
        if (writeBuffer == null || writeBuffer.isEmpty()) {
            return;
        }

        N originalNamespace = getCurrentNamespace();
        K originalKey = backend.getCurrentKey();
        boolean keyWasSet = originalKey != null;

        setCurrentNamespace(namespace);

        for (Map.Entry<K, ACC> entry : writeBuffer.entrySet()) {
            backend.setCurrentKey(entry.getKey());
            updateInternal(entry.getValue());
        }
        writeBuffer.clear();

        setCurrentNamespace(originalNamespace);
        if (keyWasSet) {
            backend.setCurrentKey(originalKey);
        } else {
            backend.setCurrentKey(null);
        }
    }

    private void flushCacheForNamespace(N namespace, CachePolicy<K, CacheEntry<ACC>> cache) {
        try {
            N originalNamespace = getCurrentNamespace();
            K originalKey = backend.getCurrentKey();

            setCurrentNamespace(namespace);
            for (Map.Entry<K, CacheEntry<ACC>> entry : cache.entrySet()) {
                if (entry.getValue().isDirty()) {
                    backend.setCurrentKey(entry.getKey());
                    delegateState.updateInternal(entry.getValue().getValue());
                    entry.getValue().setDirty(false);
                }
            }
            setCurrentNamespace(originalNamespace);
            backend.setCurrentKey(originalKey);
        } catch (Exception e) {
            throw new RuntimeException("Failed to flush dirty entries for evicted namespace: " + namespace, e);
        }
    }

    private CachePolicy<K, CacheEntry<ACC>> getL1CacheForNamespace(N namespace) {
        return namespaceCachesL1.computeIfAbsent(namespace, ns ->
                createCachePolicyWithEvictionListener(l1CacheSizePerKeyPerNamespace, evictedL1Entry -> {
                    CachePolicy<K, CacheEntry<ACC>> l2Cache = getL2CacheForNamespace(ns);
                    if (evictedL1Entry.getValue().isDirty()) {
                        try {
                            backend.setCurrentKey(evictedL1Entry.getKey());
                            delegateState.setCurrentNamespace(ns);
                            delegateState.updateInternal(evictedL1Entry.getValue().getValue());
                            evictedL1Entry.getValue().setDirty(false);
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to flush L1 entry to delegate on eviction", e);
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

        if (writeBehindEnabled) {
            K currentKey = backend.getCurrentKey();
            N currentNamespace = getCurrentNamespace();
            Map<K, ACC> writeBuffer = getWriteBufferForNamespace(currentNamespace);

            ACC currentAccumulator = writeBuffer.get(currentKey);
            if (currentAccumulator == null) {
                currentAccumulator = getInternal(); // Check caches/delegate
                if (currentAccumulator == null) {
                    currentAccumulator = aggFunction.createAccumulator();
                }
            }

            currentAccumulator = aggFunction.add(value, currentAccumulator);
            writeBuffer.put(currentKey, currentAccumulator);
        } else {
            ACC current = getInternal();
            if (current == null) {
                current = aggFunction.createAccumulator();
            }
            current = aggFunction.add(value, current);
            updateInternal(current);
        }
    }

    @Override
    public ACC getInternal() throws Exception {
        if (accessCount.incrementAndGet() % LOG_HIT_RATE_EVERY_N_ACCESSES == 0) {
            logCacheHitRate();
        }
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        Map<K, ACC> writeBuffer = namespaceWriteBuffers.get(currentNamespace);
        if (writeBuffer != null && writeBuffer.containsKey(currentKey)) {
            return writeBuffer.get(currentKey);
        }

        CachePolicy<K, CacheEntry<ACC>> l1Cache = getL1CacheForNamespace(currentNamespace);
        CacheEntry<ACC> l1Entry = l1Cache.get(currentKey);

        if (l1Entry != null) {
            cacheHits.incrementAndGet();
            return l1Entry.getValue();
        }

        CachePolicy<K, CacheEntry<ACC>> l2Cache = getL2CacheForNamespace(currentNamespace);
        CacheEntry<ACC> l2Entry = l2Cache.get(currentKey);

        if (l2Entry != null) {
            cacheHits.incrementAndGet();
            l1Cache.put(currentKey, l2Entry);
            l2Cache.remove(currentKey);
            return l2Entry.getValue();
        }

        cacheMisses.incrementAndGet();
        ACC valueFromDelegate = delegateState.getInternal();
        if (valueFromDelegate != null) {
            l1Cache.put(currentKey, CacheEntry.clean(valueFromDelegate));
        }
        return valueFromDelegate;
    }

    @Override
    public void updateInternal(ACC valueToStore) throws Exception {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        if (writeBehindEnabled) {
            // Write-behind: update L1 cache, mark as dirty.
            Map<K, ACC> writeBuffer = namespaceWriteBuffers.get(currentNamespace);
            if (writeBuffer != null) {
                writeBuffer.remove(currentKey);
            }
            CachePolicy<K, CacheEntry<ACC>> l1Cache = getL1CacheForNamespace(currentNamespace);
            l1Cache.put(currentKey, CacheEntry.dirty(valueToStore));
            CachePolicy<K, CacheEntry<ACC>> l2Cache = getL2CacheForNamespace(currentNamespace);
            l2Cache.remove(currentKey); // Invalidate L2, as L1 is now dirty and the source of truth.
        } else {
            // Write-through: update delegate and then cache.
            delegateState.updateInternal(valueToStore);
            CachePolicy<K, CacheEntry<ACC>> l1Cache = getL1CacheForNamespace(currentNamespace);
            l1Cache.put(currentKey, CacheEntry.clean(valueToStore));
            CachePolicy<K, CacheEntry<ACC>> l2Cache = getL2CacheForNamespace(currentNamespace);
            l2Cache.remove(currentKey); // Invalidate L2, as L1 now has the clean value.
        }
    }

    private void doUpdateInternal(ACC valueToStore) throws Exception {
        updateInternal(valueToStore);
    }

    @Override
    public void clear() {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        Map<K, ACC> writeBuffer = getWriteBufferForNamespace(currentNamespace);
        writeBuffer.remove(currentKey);
        doClear();
    }

    private void doClear() {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        CachePolicy<K, CacheEntry<ACC>> l1Cache = getL1CacheForNamespace(currentNamespace);
        l1Cache.remove(currentKey);

        CachePolicy<K, CacheEntry<ACC>> l2Cache = getL2CacheForNamespace(currentNamespace);
        l2Cache.remove(currentKey);

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
        Map<K, ACC> writeBuffer = namespaceWriteBuffers.get(namespace);
        if (writeBuffer != null) {
            writeBuffer.clear();
        }

        CachePolicy<K, CacheEntry<ACC>> l1Cache = namespaceCachesL1.get(namespace);
        if (l1Cache != null) {
            l1Cache.clear();
        }
        CachePolicy<K, CacheEntry<ACC>> l2Cache = namespaceCachesL2.get(namespace);
        if (l2Cache != null) {
            l2Cache.clear();
        }
    }

    @Override
    public void flushToUnderlyingState() throws IOException {
        List<Map.Entry<N, Map<K, ACC>>> nsEntries = new ArrayList<>();
        for (Map.Entry<N, Map<K, ACC>> entry : namespaceWriteBuffers.entrySet()) {
            nsEntries.add(entry);
        }
        try {
            for (Map.Entry<N, Map<K, ACC>> nsEntry : nsEntries) {
                flushWriteBufferForNamespace(nsEntry.getKey(), nsEntry.getValue());
            }
        } catch (Exception e) {
            throw new IOException("Failed to flush write-behind buffers", e);
        }
        namespaceWriteBuffers.clear();

        for (Map.Entry<N, CachePolicy<K, CacheEntry<ACC>>> nsEntry : namespaceCachesL1.entrySet()) {
            N namespace = nsEntry.getKey();
            CachePolicy<K, CacheEntry<ACC>> l1Cache = nsEntry.getValue();
            setCurrentNamespace(namespace);

            List<Map.Entry<K, CacheEntry<ACC>>> currentL1Entries = new ArrayList<>();
            for (Map.Entry<K, CacheEntry<ACC>> entry : l1Cache.entrySet()) {
                currentL1Entries.add(entry);
            }

            for (Map.Entry<K, CacheEntry<ACC>> mapEntry : currentL1Entries) {
                K key = mapEntry.getKey();
                CacheEntry<ACC> entry = mapEntry.getValue();
                if (entry.isDirty()) {
                    backend.setCurrentKey(key);
                    try {
                        delegateState.updateInternal(entry.getValue());
                    } catch (Exception e) {
                        throw new IOException(e);
                    }
                    entry.setDirty(false);
                }
            }
        }
    }

    private Map<K, ACC> getWriteBufferForNamespace(N namespace) {
        return namespaceWriteBuffers.computeIfAbsent(namespace, ns -> new LinkedHashMap<>());
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
        delegateState.setCurrentNamespace(namespace);
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

    private void logCacheHitRate() {
        long hits = cacheHits.get();
        long misses = cacheMisses.get();
        long total = hits + misses;
        if (total > 0) {
            LOG.info("AggregatingState Cache Hit Rate: {} (Hits: {}, Misses: {})",
                    (double) hits / total, hits, misses);
        }
    }
} 