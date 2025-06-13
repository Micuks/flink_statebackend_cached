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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState.StateIncrementalVisitor;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link InternalValueState} that uses an L1/L2 cache for its value.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class CachingInternalValueState<K, N, V>
        implements InternalValueState<K, N, V>,
                CachingInternalState<K, N, V, InternalValueState<K, N, V>> {

    private static final Logger LOG = LoggerFactory.getLogger(CachingInternalValueState.class);
    private final InternalValueState<K, N, V> delegateState;
    private final CachingKeyedStateBackend<K> backend; // For accessing current key
    private final CachePolicy<N, CachePolicy<K, CacheEntry<V>>> namespaceCachesL1; // Namespace ->
                                                                                   // Key -> L1
                                                                                   // CacheEntry
    private final CachePolicy<N, CachePolicy<K, CacheEntry<V>>> namespaceCachesL2; // Namespace ->
                                                                                   // Key -> L2
                                                                                   // CacheEntry

    private final int l1CacheSizePerKeyPerNamespace;
    private final int l2CacheSizePerKeyPerNamespace;
    private final int maxActiveNamespacesInCache;
    private final long maxCacheMemoryMb;
    private N currentNamespace;
    private final CachingStateBackendFactory.CachePolicyType cachePolicyType;

    // Configuration for cache bypass
    private final double cacheHitRateThreshold;
    private final long cacheHitRateWindowSize;
    private final long cacheMinAccessesForBypassCheck;

    // State for cache bypass logic
    private transient AtomicLong accessesForHitRateWindow;
    private transient AtomicLong hitsInHitRateWindow;
    private transient AtomicLong totalAccessesForBypassEligibility;

    private volatile boolean bypassCache = false;
    private final boolean bypassEnabled;

    private final CachePolicy<N, Map<K, V>> namespaceWriteBuffers;

    public CachingInternalValueState(
            InternalValueState<K, N, V> delegateState,
            CachingKeyedStateBackend<K> backend,
            int l1CacheSize,
            int l2CacheSize,
            int maxActiveNamespacesInCache,
            long maxCacheMemoryMb,
            CachingStateBackendFactory.CachePolicyType cachePolicyType,
            double cacheHitRateThreshold,
            long cacheHitRateWindowSize,
            long cacheMinAccessesForBypassCheck,
            boolean bypassEnabled) {
        this.delegateState = delegateState;
        this.backend = backend;
        this.l1CacheSizePerKeyPerNamespace = l1CacheSize;
        this.l2CacheSizePerKeyPerNamespace = l2CacheSize;
        this.maxActiveNamespacesInCache = maxActiveNamespacesInCache;
        this.maxCacheMemoryMb = maxCacheMemoryMb;
        this.cachePolicyType = cachePolicyType;
        this.bypassEnabled = bypassEnabled;
        if(this.bypassEnabled) {
            this.cacheHitRateThreshold = cacheHitRateThreshold;
            this.cacheHitRateWindowSize = cacheHitRateWindowSize;
            this.cacheMinAccessesForBypassCheck = cacheMinAccessesForBypassCheck;
            this.accessesForHitRateWindow = new AtomicLong(0);
            this.hitsInHitRateWindow = new AtomicLong(0);
            this.totalAccessesForBypassEligibility = new AtomicLong(0);
        } else {
            this.cacheHitRateThreshold = 0;
            this.cacheHitRateWindowSize = 0;
            this.cacheMinAccessesForBypassCheck = 0;
        }

        this.namespaceWriteBuffers = createCachePolicyWithEvictionListener(this.maxActiveNamespacesInCache,
                evictedNamespaceL1Entry -> {
                    try {
                        flushWriteBufferForNamespace(evictedNamespaceL1Entry.getKey(), evictedNamespaceL1Entry.getValue());
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Failed to flush/park L1 entries for evicted namespace: " + evictedNamespaceL1Entry.getKey(), e);
                    }
                });

        // Create namespace caches with eviction listeners that flush dirty entries
        this.namespaceCachesL1 = createCachePolicyWithEvictionListener(maxActiveNamespacesInCache,
                evictedNsEntry -> {
                    // Flush any dirty entries in the evicted namespace before removing it
                    N evictedNamespace = evictedNsEntry.getKey();
                    CachePolicy<K, CacheEntry<V>> evictedL1Cache = evictedNsEntry.getValue();
                    flushCacheForNamespace(evictedNamespace, evictedL1Cache);
                });
        this.namespaceCachesL2 = createCachePolicyWithEvictionListener(maxActiveNamespacesInCache,
                evictedNsEntry -> {
                    // Not flushing L2, as it should only contain clean entries.
                    // Report memory released for the entire evicted L2 cache.
                    N evictedNamespace = evictedNsEntry.getKey();
                    CachePolicy<K, CacheEntry<V>> evictedPerNsL2Cache = evictedNsEntry.getValue();
                    if (evictedPerNsL2Cache != null) {
                        for (Map.Entry<K, CacheEntry<V>> entry : evictedPerNsL2Cache.entrySet()) {
                            if (entry.getValue() != null) {
                                backend.reportCacheMemoryReleased(entry.getValue().getEstimatedSizeBytes());
                            }
                        }
                    }
                });
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

    private void flushCacheForNamespace(N namespace, CachePolicy<K, CacheEntry<V>> cache) {
        try {
            N originalNamespace = getCurrentNamespace();
            K originalKey = backend.getCurrentKey();
            boolean keyWasSet = originalKey != null;

            setCurrentNamespace(namespace);

            // Create a copy of entries to avoid ConcurrentModificationException
            java.util.List<Map.Entry<K, CacheEntry<V>>> entries = new java.util.ArrayList<>();
            for (Map.Entry<K, CacheEntry<V>> entry : cache.entrySet()) {
                entries.add(entry);
            }

            for (Map.Entry<K, CacheEntry<V>> entry : entries) {
                K key = entry.getKey();
                CacheEntry<V> cacheEntry = entry.getValue();
                if (cacheEntry != null && cacheEntry.isDirty()) {
                    V value = cacheEntry.getValue();
                    backend.setCurrentKey(key);
                    delegateState.update(value);
                    cacheEntry.setDirty(false); // Mark clean after flushing
                }
            }

            setCurrentNamespace(originalNamespace);
            if (keyWasSet) {
                backend.setCurrentKey(originalKey);
            } else {
                backend.setCurrentKey(null);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush dirty entries for evicted namespace: " + namespace, e);
        }
    }

    private void flushWriteBufferForNamespace(N namespace, Map<K, V> writeBuffer) throws IOException {
        if (writeBuffer == null || writeBuffer.isEmpty()) {
            return;
        }

        N originalNamespace = getCurrentNamespace();
        K originalKey = backend.getCurrentKey();
        boolean keyWasSet = originalKey != null;

        setCurrentNamespace(namespace);

        for (Map.Entry<K, V> entry : writeBuffer.entrySet()) {
            backend.setCurrentKey(entry.getKey());
            if (entry.getValue() == null) {
                doClear();
            } else {
                doUpdate(entry.getValue());
            }
        }
        writeBuffer.clear();

        setCurrentNamespace(originalNamespace);
        if (keyWasSet) {
            backend.setCurrentKey(originalKey);
        } else {
            backend.setCurrentKey(null);
        }
    }

    private void updateCacheBypassCondition(boolean resolvedByCache) {
        if (!bypassEnabled || cacheHitRateThreshold <= 0.0) {
            this.bypassCache = false;
            return;
        }
        
        if (totalAccessesForBypassEligibility.get() % 1000 == 0) {
            LOG.info(getCacheStats());
        }

        long totalAccesses = totalAccessesForBypassEligibility.incrementAndGet();

        if (resolvedByCache) {
            hitsInHitRateWindow.incrementAndGet();
        }

        long currentWindowAccesses = accessesForHitRateWindow.incrementAndGet();

        if (totalAccesses >= cacheMinAccessesForBypassCheck && currentWindowAccesses >= cacheHitRateWindowSize) {
            double currentHitRate = (double) hitsInHitRateWindow.get() / currentWindowAccesses;
            String currentNamespaceForLog = "unavailable";
            try {
                currentNamespaceForLog = String.valueOf(getCurrentNamespace());
            } catch (Exception e) {
                // Ignore, namespace might not be set in all contexts
            }
            if (currentHitRate < this.cacheHitRateThreshold) {
                if (!bypassCache) {
                    bypassCache = true;
                    LOG.info(
                            "Cache bypass activated for value state. Hit rate {}% ({} hits / {} accesses) is below threshold {}%. Namespace: {}.",
                            String.format("%.2f", currentHitRate * 100),
                            hitsInHitRateWindow.get(),
                            accessesForHitRateWindow.get(),
                            String.format("%.2f", this.cacheHitRateThreshold * 100),
                            currentNamespaceForLog);
                }
            } else if (bypassCache) {
                bypassCache = false;
                LOG.info(
                        "Cache bypass deactivated for value state. Hit rate {}% ({} hits / {} accesses) is above threshold {}%. Namespace: {}.",
                        String.format("%.2f", currentHitRate * 100),
                        hitsInHitRateWindow.get(),
                        accessesForHitRateWindow.get(),
                        String.format("%.2f", this.cacheHitRateThreshold * 100),
                        currentNamespaceForLog);
            }
            accessesForHitRateWindow.set(0);
            hitsInHitRateWindow.set(0);
        }
    }

    @Override
    public InternalValueState<K, N, V> getDelegateState() {
        return delegateState;
    }

    private CachePolicy<K, CacheEntry<V>> getL1CacheForNamespace(N namespace) {
        return namespaceCachesL1.computeIfAbsent(
                namespace,
                ns -> createCachePolicyWithEvictionListener(l1CacheSizePerKeyPerNamespace,
                        evictedL1Entry -> {
                            CachePolicy<K, CacheEntry<V>> l2Cache = getL2CacheForNamespace(ns);
                            K evictedKey = evictedL1Entry.getKey();
                            CacheEntry<V> evictedValueWrapper = evictedL1Entry.getValue();
                            V evictedValue = evictedValueWrapper.getValue();
                            long estimatedSize = evictedValueWrapper.getEstimatedSizeBytes();

                            backend.reportCacheMemoryReleased(estimatedSize);

                            if (evictedValueWrapper.isDirty()) {
                                try {
                                    N originalNamespace = getCurrentNamespace();
                                    K originalKey = backend.getCurrentKey();

                                    backend.setCurrentKey(evictedKey);
                                    this.setCurrentNamespace(ns);
                                    delegateState.update(evictedValue);
                                    evictedValueWrapper.setDirty(false);

                                    backend.setCurrentKey(originalKey); 
                                    this.setCurrentNamespace(originalNamespace);

                                    CacheEntry<V> entryToL2 = CacheEntry.clean(evictedValue);
                                    CacheEntry<V> oldL2Entry = l2Cache.put(evictedKey, entryToL2); 
                                    if (oldL2Entry != null) {
                                        backend.reportCacheMemoryReleased(oldL2Entry.getEstimatedSizeBytes());
                                    }
                                    backend.reportCacheMemoryAdded(entryToL2.getEstimatedSizeBytes());

                                } catch (IOException e) {
                                    throw new RuntimeException(
                                            "Failed to flush L1 entry to delegate on L1 eviction for key: "
                                                    + evictedKey + " in ns: " + ns,
                                            e);
                                }
                            } else {
                                CacheEntry<V> oldL2Entry = l2Cache.put(evictedKey, evictedValueWrapper);
                                if (oldL2Entry != null) {
                                    backend.reportCacheMemoryReleased(oldL2Entry.getEstimatedSizeBytes());
                                }
                                backend.reportCacheMemoryAdded(evictedValueWrapper.getEstimatedSizeBytes());
                            }
                        }));
    }

    private CachePolicy<K, CacheEntry<V>> getL2CacheForNamespace(N namespace) {
        return namespaceCachesL2.computeIfAbsent(
                namespace,
                ns -> createCachePolicy(l2CacheSizePerKeyPerNamespace));
    }

    @Override
    public V value() throws IOException {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        if (bypassEnabled && bypassCache) {
            updateCacheBypassCondition(false);
            return delegateState.value();
        }

        Map<K, V> writeBuffer = namespaceWriteBuffers.get(currentNamespace);
        if (writeBuffer != null && writeBuffer.containsKey(currentKey)) {
            updateCacheBypassCondition(true);
            return writeBuffer.get(currentKey);
        }

        CachePolicy<K, CacheEntry<V>> l1Cache = getL1CacheForNamespace(currentNamespace);
        CacheEntry<V> l1Entry = l1Cache.get(currentKey);

        if (l1Entry != null) {
            updateCacheBypassCondition(true);
            return l1Entry.getValue();
        }

        CachePolicy<K, CacheEntry<V>> l2Cache = getL2CacheForNamespace(currentNamespace);
        CacheEntry<V> l2Entry = l2Cache.get(currentKey);

        if (l2Entry != null) {
            updateCacheBypassCondition(true);
            l2Cache.remove(currentKey);
            CacheEntry<V> entryToL1 = CacheEntry.clean(l2Entry.getValue());
            CacheEntry<V> oldL1Entry = l1Cache.put(currentKey, entryToL1);
            if (oldL1Entry != null) {
                backend.reportCacheMemoryReleased(oldL1Entry.getEstimatedSizeBytes());
            }
            backend.reportCacheMemoryAdded(entryToL1.getEstimatedSizeBytes());
            return entryToL1.getValue();
        }

        updateCacheBypassCondition(false);
        V valueFromDelegate = delegateState.value();
        if (valueFromDelegate != null) { // Only cache non-null
            CacheEntry<V> newEntry = CacheEntry.clean(valueFromDelegate);
            CacheEntry<V> oldL1Entry = l1Cache.put(currentKey, newEntry);
            if(oldL1Entry != null) {
                backend.reportCacheMemoryReleased(oldL1Entry.getEstimatedSizeBytes());
            }
            backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
        }
        return valueFromDelegate;
    }

    @Override
    public void update(V value) throws IOException {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        if (value == null) {
            clear();
            return;
        }
        
        if (bypassEnabled && bypassCache) {
            delegateState.update(value);
            return;
        }

        getWriteBufferForNamespace(currentNamespace).put(currentKey, value);
    }

    private void doUpdate(V value) throws IOException {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        if (value == null) { // As per Flink ValueState contract
            doClear();
            return;
        }

        if (bypassEnabled && bypassCache) {
            updateCacheBypassCondition(true);
            delegateState.update(value);
            return;
        }

        updateCacheBypassCondition(true);
        CachePolicy<K, CacheEntry<V>> l1Cache = getL1CacheForNamespace(currentNamespace);
        CacheEntry<V> newEntry = CacheEntry.dirty(value);
        CacheEntry<V> oldL1Entry = l1Cache.put(currentKey, newEntry);
        if (oldL1Entry != null) {
            backend.reportCacheMemoryReleased(oldL1Entry.getEstimatedSizeBytes());
        }
        backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());

        // If L2 had this key, it's now stale, remove it.
        CachePolicy<K, CacheEntry<V>> l2Cache = getL2CacheForNamespace(currentNamespace);
        CacheEntry<V> oldL2Entry = l2Cache.remove(currentKey);
        if (oldL2Entry != null) {
            // L2 entries are implicitly managed by L1 evictions or direct stale removal like here.
            // Their memory was accounted for when they moved from L1 to L2 (L1 released, L2 added - though we simplified this)
            // Or when loaded to L2 directly. When removing from L2 here because L1 got an update,
            // we should report its memory as released if it wasn't already part of L1's old entry.
            // Simplified: Assume L2 entries are clean and their removal directly translates to released memory
            // if they weren't the source for the L1 update that just happened.
            // However, simpler just to let their L1 eviction listener handle the release when they were put there.
            // The current logic in L1 eviction listener (getL1CacheForNamespace) moves to L2 and L2 doesn't have
            // an aggressive release reporting on its own removals. This explicit remove should report.
            backend.reportCacheMemoryReleased(oldL2Entry.getEstimatedSizeBytes());
        }
    }

    @Override
    public void clear() {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        getWriteBufferForNamespace(currentNamespace).put(currentKey, null);
    }

    private void doClear() {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        CachePolicy<K, CacheEntry<V>> l1Cache = getL1CacheForNamespace(currentNamespace);
        CacheEntry<V> oldL1Entry = l1Cache.remove(currentKey);
        if (oldL1Entry != null) {
            backend.reportCacheMemoryReleased(oldL1Entry.getEstimatedSizeBytes());
        }

        CachePolicy<K, CacheEntry<V>> l2Cache = getL2CacheForNamespace(currentNamespace);
        CacheEntry<V> oldL2Entry = l2Cache.remove(currentKey);
        if (oldL2Entry != null) {
            backend.reportCacheMemoryReleased(oldL2Entry.getEstimatedSizeBytes());
        }

        delegateState.clear(); // Clear the underlying state
    }

    @Override
    public void flushToUnderlyingState() throws IOException {
        // Flush write-behind buffer
        List<Map.Entry<N, Map<K, V>>> nsEntries = new ArrayList<>();
        for (Map.Entry<N, Map<K, V>> entry : namespaceWriteBuffers.entrySet()) {
            nsEntries.add(entry);
        }

        for (Map.Entry<N, Map<K, V>> nsEntry : nsEntries) {
            flushWriteBufferForNamespace(nsEntry.getKey(), nsEntry.getValue());
        }
        namespaceWriteBuffers.clear();

        // Flush L1 caches
        for (Map.Entry<N, CachePolicy<K, CacheEntry<V>>> nsEntry : namespaceCachesL1.entrySet()) {
            N namespace = nsEntry.getKey();
            CachePolicy<K, CacheEntry<V>> l1Cache = nsEntry.getValue();
            setCurrentNamespace(namespace);

            // Iterate over a defensive copy of entries to avoid
            // ConcurrentModificationException
            java.util.List<Map.Entry<K, CacheEntry<V>>> currentL1Entries = new java.util.ArrayList<>();
            for (Map.Entry<K, CacheEntry<V>> entry : l1Cache.entrySet()) {
                currentL1Entries.add(entry);
            }

            for (Map.Entry<K, CacheEntry<V>> mapEntry : currentL1Entries) {
                K key = mapEntry.getKey();
                CacheEntry<V> entry = mapEntry.getValue(); // Use the entry directly from the snapshot
                if (entry.isDirty()) { // No need for null check if it came from entrySet
                    V value = entry.getValue();
                    if (key != null) { // Guard against null key
                        backend.setCurrentKey(key);
                        delegateState.update(value);
                        entry.setDirty(false);
                    }
                }
            }
        }

        // Flush L2 caches
        for (Map.Entry<N, CachePolicy<K, CacheEntry<V>>> nsEntry : namespaceCachesL2.entrySet()) {
            N namespace = nsEntry.getKey();
            CachePolicy<K, CacheEntry<V>> l2Cache = nsEntry.getValue();
            setCurrentNamespace(namespace);

            // Iterate over a defensive copy of entries to avoid
            // ConcurrentModificationException
            java.util.List<Map.Entry<K, CacheEntry<V>>> currentL2Entries = new java.util.ArrayList<>();
            for (Map.Entry<K, CacheEntry<V>> entry : l2Cache.entrySet()) {
                currentL2Entries.add(entry);
            }

            for (Map.Entry<K, CacheEntry<V>> mapEntry : currentL2Entries) {
                K key = mapEntry.getKey();
                CacheEntry<V> entry = mapEntry.getValue();
                if (entry.isDirty()) { // L2 entries ideally shouldn't be dirty with current logic
                    V value = entry.getValue();
                    if (key != null) { // Guard against null key
                        backend.setCurrentKey(key);
                        delegateState.update(value);
                        entry.setDirty(false);
                    }
                }
            }
        }
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
    public TypeSerializer<V> getValueSerializer() {
        return delegateState.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(@Nonnull N namespace) {
        this.currentNamespace = namespace;
        delegateState.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<V> safeValueSerializer)
            throws Exception {
        return delegateState.getSerializedValue(
                serializedKeyAndNamespace,
                safeKeySerializer,
                safeNamespaceSerializer,
                safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        try {
            flushToUnderlyingState();
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush caches before creating state visitor", e);
        }
        return delegateState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    @Nonnull
    public N getCurrentNamespace() {
        if (currentNamespace == null) {
            throw new IllegalStateException(
                    "Namespace has not been set. Typically, you should call "
                            + "setCurrentNamespace" + " first.");
        }
        return this.currentNamespace;
    }

    @Override
    public long evictEntriesToFreeMemory(long targetBytesToFreeThisState) {
        long bytesFreed = 0;
        if (targetBytesToFreeThisState <= 0) return 0;

        // Iterate over a snapshot of L2 namespaces to avoid concurrent modification if map supports it
        List<N> l2Namespaces = new ArrayList<>();
        for (Map.Entry<N, CachePolicy<K, CacheEntry<V>>> entry : namespaceCachesL2.entrySet()) {
            l2Namespaces.add(entry.getKey());
        }

        for (N namespace : l2Namespaces) {
            CachePolicy<K, CacheEntry<V>> l2Cache = namespaceCachesL2.get(namespace);
            if (l2Cache == null || l2Cache.isEmpty()) continue;

            Iterator<Map.Entry<K, CacheEntry<V>>> l2Iter = l2Cache.entrySet().iterator();
            while (l2Iter.hasNext() && bytesFreed < targetBytesToFreeThisState) {
                Map.Entry<K, CacheEntry<V>> entry = l2Iter.next();
                CacheEntry<V> cacheValue = entry.getValue();
                long estimatedSize = cacheValue.getEstimatedSizeBytes();
                l2Iter.remove();
                backend.reportCacheMemoryReleased(estimatedSize);
                bytesFreed += estimatedSize;
            }
            if (bytesFreed >= targetBytesToFreeThisState) return bytesFreed;
        }

        // Iterate over L1 namespaces
        List<N> l1Namespaces = new ArrayList<>();
        for (Map.Entry<N, CachePolicy<K, CacheEntry<V>>> entry : namespaceCachesL1.entrySet()) {
            l1Namespaces.add(entry.getKey());
        }

        for (N namespace : l1Namespaces) {
            CachePolicy<K, CacheEntry<V>> l1Cache = namespaceCachesL1.get(namespace);
            if (l1Cache == null || l1Cache.isEmpty()) continue; // Optimization: skip empty L1 caches

            // Evict clean L1 entries first
            Iterator<Map.Entry<K, CacheEntry<V>>> l1IterClean = l1Cache.entrySet().iterator();
            List<Map.Entry<K, CacheEntry<V>>> dirtyL1EntriesToConsider = new ArrayList<>();
            while (l1IterClean.hasNext() && bytesFreed < targetBytesToFreeThisState) {
                Map.Entry<K, CacheEntry<V>> entry = l1IterClean.next();
                CacheEntry<V> cacheValue = entry.getValue();
                if (!cacheValue.isDirty()) {
                    long estimatedSize = cacheValue.getEstimatedSizeBytes();
                    l1IterClean.remove();
                    backend.reportCacheMemoryReleased(estimatedSize);
                    bytesFreed += estimatedSize;
                } else {
                    dirtyL1EntriesToConsider.add(entry); // Collect dirty ones for later pass
                }
            }
            if (bytesFreed >= targetBytesToFreeThisState) return bytesFreed;

            // Evict dirty L1 entries if still needed (requires flushing)
            // This is a simplified flush for the global eviction context.
            // Proper context setting for delegateState.update is complex here.
            // For a truly "lightweight" initial pass, one might choose to *not* evict dirty entries here,
            // or only evict them without a guaranteed flush if that's acceptable for the memory cap goal.
            Iterator<Map.Entry<K, CacheEntry<V>>> dirtyIter = dirtyL1EntriesToConsider.iterator(); // Use separate iterator if modifying original list
            while (dirtyIter.hasNext() && bytesFreed < targetBytesToFreeThisState) {
                Map.Entry<K, CacheEntry<V>> dirtyEntryTuple = dirtyIter.next();
                K key = dirtyEntryTuple.getKey();
                CacheEntry<V> dirtyEntry = dirtyEntryTuple.getValue();
                V value = dirtyEntry.getValue();
                long estimatedSize = dirtyEntry.getEstimatedSizeBytes();

                try {
                    // Simplified: Attempt to flush. In a real scenario, this needs robust context management.
                    N originalCurrentNamespace = this.currentNamespace; // Store current NS of this state object
                    K originalBackendKey = backend.getCurrentKey();

                    backend.setCurrentKey(key);       // Set key for backend & delegate
                    setCurrentNamespace(namespace); // Set NS for delegate
                    delegateState.update(value);    // Flush to delegate
                    dirtyEntry.setDirty(false);     // Mark as clean if flush was successful

                    // Restore context
                    setCurrentNamespace(originalCurrentNamespace);
                    backend.setCurrentKey(originalBackendKey);

                    // Now evict from L1
                    l1Cache.remove(key); // Ensure removal, iterator might be tricky if map reorders
                    backend.reportCacheMemoryReleased(estimatedSize);
                    bytesFreed += estimatedSize;

                } catch (Exception e) {
                    // Log or handle: Failed to flush dirty entry, cannot evict it reliably to free memory yet.
                    // System.err.println("Global eviction: Failed to flush/evict dirty L1 entry for key " + key + " in NS " + namespace + ": " + e.getMessage());
                }
            }
            if (bytesFreed >= targetBytesToFreeThisState) return bytesFreed;
        }
        return bytesFreed;
    }

    // Debug method to get cache statistics
    public String getCacheStats() {
        if (!bypassEnabled) {
            return "ValueState Cache: Bypass feature disabled.";
        }
        long hits = hitsInHitRateWindow != null ? hitsInHitRateWindow.get() : 0;
        long accesses = accessesForHitRateWindow != null ? accessesForHitRateWindow.get() : 0;
        long total = totalAccessesForBypassEligibility != null ? totalAccessesForBypassEligibility.get() : 0;
        return String.format(
                "ValueState Cache Stats: Total Accesses: %d, Window Accesses: %d, Window Hits: %d, Hit Rate: %.2f, Bypassing: %s",
                total,
                accesses,
                hits,
                (accesses > 0) ? (double) hits / accesses : 0.0,
                bypassCache);
    }

    private Map<K, V> getWriteBufferForNamespace(N namespace) {
        return namespaceWriteBuffers.computeIfAbsent(namespace, ns -> new LinkedHashMap<>());
    }
}
