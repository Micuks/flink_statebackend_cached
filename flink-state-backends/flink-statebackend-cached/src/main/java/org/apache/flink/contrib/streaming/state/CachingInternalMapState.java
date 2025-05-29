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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.AbstractHeapState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

/**
 * An {@link InternalMapState} that uses an L1/L2 cache for its entries. Caches individual (UK, UV)
 * pairs for each Flink state key (K) and namespace (N).
 *
 * @param <K> The type of the Flink key.
 * @param <N> The type of the namespace.
 * @param <UK> The type of the user key in the map.
 * @param <UV> The type of the user value in the map.
 */
public class CachingInternalMapState<K, N, UK, UV> implements InternalMapState<K, N, UK, UV>,
        CachingInternalState<K, N, Map<UK, UV>, InternalMapState<K, N, UK, UV>> {  // Now using Flink's Closeable

    private static final Logger LOG = LoggerFactory.getLogger(CachingInternalMapState.class);
    private final InternalMapState<K, N, UK, UV> delegateState;
    private final CachingKeyedStateBackend<K> backend;
    private N currentNamespace;

    // Cache structure: Namespace -> Flink Key -> L1/L2 Caches for UserKey-UserValue
    // pairs
    // LRUMap<Namespace, LRUMap<FlinkKey, PerKeyMapCache>>
    private final CachePolicy<N, CachePolicy<K, PerKeyMapCache<UK, UV, K, N>>> namespaceCaches;

    private final TypeSerializer<UK> userKeySerializer;
    private final TypeSerializer<UV> userValueSerializer;

    private final int l1CacheSizePerMap; // Max L1 entries (UK-UV pairs) per Flink Key/Namespace
    private final int l2CacheSizePerMap; // Max L2 entries (UK-UV pairs) per Flink Key/Namespace
    private final int maxFlinkKeysWithActiveCachesPerNamespace; // Max Flink Keys with active map
                                                                // caches for a Namespace
    private final int maxActiveNamespacesInCache; // Max Namespaces with active caches
    private final CachingStateBackendFactory.CachePolicyType cachePolicyType;

    // Helper class to hold L1 and L2 caches for a specific Flink Key/Namespace's
    // map entries
    private static class PerKeyMapCache<UK_C, UV_C, K_F, N_F> {
        final CachePolicy<UK_C, CacheEntry<UV_C>> l1MapEntries;
        final CachePolicy<UK_C, CacheEntry<UV_C>> l2MapEntries; // Should only hold clean entries
        boolean fullyLoaded = false;
        private final N_F mainContextDelegateNamespace;
        private final CachingKeyedStateBackend<K_F> ownerBackend;
        private final K_F flinkKeyForEvictionContext;
        private final N_F namespaceForEvictionContext;

        PerKeyMapCache(int l1Size, int l2Size, InternalMapState<K_F, N_F, UK_C, UV_C> delegateState,
                CachingKeyedStateBackend<K_F> ownerBackend, K_F flinkKey, N_F cacheNamespace,
                N_F mainContextDelegateNamespace,
                CachingStateBackendFactory.CachePolicyType cachePolicyType) {
            this.l2MapEntries = createCachePolicyInstance(cachePolicyType, l2Size, null);
            this.mainContextDelegateNamespace = mainContextDelegateNamespace;
            this.ownerBackend = ownerBackend;
            this.flinkKeyForEvictionContext = flinkKey;
            this.namespaceForEvictionContext = cacheNamespace;

            this.l1MapEntries =
                    createCachePolicyInstance(cachePolicyType, l1Size, evictedL1MapEntry -> {
                        UK_C evictedUK = evictedL1MapEntry.getKey();
                        CacheEntry<UV_C> evictedUVWrapper = evictedL1MapEntry.getValue();
                        long estimatedSize = evictedUVWrapper.getEstimatedSizeBytes();

                        this.ownerBackend.reportCacheMemoryReleased(estimatedSize);

                        if (evictedUVWrapper.isDirty()) { 
                            K_F originalKeyContext = null;
                            N_F originalDelegateNamespaceContext = null;
                            try {
                                originalKeyContext = ownerBackend.getCurrentKey();
                                originalDelegateNamespaceContext =
                                        this.mainContextDelegateNamespace;

                                ownerBackend.setCurrentKey(flinkKeyForEvictionContext);
                                delegateState.setCurrentNamespace(namespaceForEvictionContext);

                                if (evictedUVWrapper.getValue() == null) { // Is a tombstone
                                    delegateState.remove(evictedUK);
                                } else {
                                    delegateState.put(evictedUK, evictedUVWrapper.getValue());
                                    this.l2MapEntries.put(evictedUK,
                                            CacheEntry.clean(evictedUVWrapper.getValue()));
                                }
                                // After successful flush, the CacheEntry instance itself should be marked clean.
                                evictedUVWrapper.setDirty(false);
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Failed to flush L1 map entry to delegate for user key: "
                                                + evictedUK,
                                        e);
                            } finally {
                                if (originalKeyContext != null) {
                                    ownerBackend.setCurrentKey(originalKeyContext);
                                }
                                if (originalDelegateNamespaceContext != null) {
                                    delegateState
                                            .setCurrentNamespace(originalDelegateNamespaceContext);
                                }
                            }
                        } else { // Original else branch for non-dirty entries
                            if (evictedUVWrapper.getValue() != null) {
                                this.l2MapEntries.put(evictedUK, evictedUVWrapper);
                            }
                        }
                    });
        }

        private static <CK, CV> CachePolicy<CK, CV> createCachePolicyInstance(
                CachingStateBackendFactory.CachePolicyType policyType, int capacity,
                Consumer<Map.Entry<CK, CV>> evictionListener) {
            switch (policyType) {
                case TINYLFU:
                    return new TinyLFUMap<>(capacity, evictionListener);
                case LRU:
                default:
                    if (evictionListener != null) {
                        return new LRUMap<>(capacity, evictionListener);
                    }
                    return new LRUMap<>(capacity);
            }
        }
    }

    public CachingInternalMapState(InternalMapState<K, N, UK, UV> delegateState,
            CachingKeyedStateBackend<K> backend, int l1CacheSizePerMap, int l2CacheSizePerMap,
            int maxFlinkKeysWithActiveCachesPerNamespace, long maxCacheMemoryMb,
            CachingStateBackendFactory.CachePolicyType cachePolicyType) {
        this.delegateState = delegateState;
        this.backend = backend;
        this.l1CacheSizePerMap = l1CacheSizePerMap;
        this.l2CacheSizePerMap = l2CacheSizePerMap;
        this.maxFlinkKeysWithActiveCachesPerNamespace = maxFlinkKeysWithActiveCachesPerNamespace;
        this.maxActiveNamespacesInCache = backend.getMaxActiveNamespaceOrPerKeyCacheContainers();
        this.cachePolicyType = cachePolicyType;

        this.namespaceCaches = createCachePolicyWithEvictionListener(
                this.maxActiveNamespacesInCache, evictedNamespaceEntry -> {
                    CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> keyCachesToFlush =
                            evictedNamespaceEntry.getValue();
                    for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> keyCacheEntry : keyCachesToFlush
                            .entrySet()) {
                        try {
                            flushL1Entries(keyCacheEntry.getValue(), keyCacheEntry.getKey(),
                                    evictedNamespaceEntry.getKey(),
                                    this.backend, this.delegateState);
                        } catch (Exception e) {
                            throw new RuntimeException(
                                    "Failed to flush PerKeyMapCache on namespace eviction for Flink key: "
                                            + keyCacheEntry.getKey(),
                                    e);
                        }
                    }
                });

        TypeSerializer<Map<UK, UV>> mapValueSerializer = delegateState.getValueSerializer();
        if (mapValueSerializer instanceof MapSerializer) {
            this.userKeySerializer =
                    ((MapSerializer<UK, UV>) mapValueSerializer).getKeySerializer();
            this.userValueSerializer =
                    ((MapSerializer<UK, UV>) mapValueSerializer).getValueSerializer();
        } else {
            throw new IllegalArgumentException(
                    "The value serializer for the delegate MapState must be a MapSerializer.");
        }
        // TODO: Investigate proper registration with CloseableRegistry
        // Temporarily commented out to fix compilation
        // this.backend.getCloseableRegistry().register((Closeable) this);
    }

    private <CK, CV> CachePolicy<CK, CV> createCachePolicy(int capacity) {
        return PerKeyMapCache.createCachePolicyInstance(this.cachePolicyType, capacity, null);
    }

    private <CK, CV> CachePolicy<CK, CV> createCachePolicyWithEvictionListener(int capacity,
            Consumer<Map.Entry<CK, CV>> evictionListener) {
        return PerKeyMapCache.createCachePolicyInstance(this.cachePolicyType, capacity,
                evictionListener);
    }

    private PerKeyMapCache<UK, UV, K, N> getOrCreatePerKeyMapCache() {
        K currentKey = backend.getCurrentKey();
        N currentNs = getCurrentNamespace();
        if (currentNs == null) {
            throw new IllegalStateException(
                    "Current namespace is not set. Call setCurrentNamespace first.");
        }

        CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> keyCaches = namespaceCaches
                .computeIfAbsent(currentNs, ns -> createCachePolicyWithEvictionListener(
                        maxFlinkKeysWithActiveCachesPerNamespace, evictedKeyCacheEntry -> {
                            K evictedFlinkKey = evictedKeyCacheEntry.getKey();
                            PerKeyMapCache<UK, UV, K, N> perKeyCacheToFlush =
                                    evictedKeyCacheEntry.getValue();
                            try {
                                flushL1Entries(perKeyCacheToFlush, evictedFlinkKey, ns,
                                        this.backend, this.delegateState);
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Failed to flush PerKeyMapCache on its eviction for Flink key: "
                                                + evictedFlinkKey,
                                        e);
                            }
                        }));
        return keyCaches.computeIfAbsent(currentKey,
                k -> new PerKeyMapCache<>(l1CacheSizePerMap, l2CacheSizePerMap, delegateState,
                        backend, k, currentNs, currentNs, this.cachePolicyType));
    }

    @Override
    public UV get(UK userKey) throws Exception {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        CacheEntry<UV> l1Entry = perKeyCache.l1MapEntries.get(userKey);
        if (l1Entry != null) {
            return l1Entry.getValue();
        }

        CacheEntry<UV> l2Entry = perKeyCache.l2MapEntries.get(userKey);
        if (l2Entry != null) {
            perKeyCache.l2MapEntries.remove(userKey);
            CacheEntry<UV> oldL1 = perKeyCache.l1MapEntries.put(userKey, l2Entry);
            if(oldL1 != null) backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());
            backend.reportCacheMemoryAdded(l2Entry.getEstimatedSizeBytes());
            return l2Entry.getValue();
        }

        if (perKeyCache.fullyLoaded) {
            return null;
        }

        UV valueFromDelegate = delegateState.get(userKey);
        if (valueFromDelegate != null) {
            CacheEntry<UV> newEntry = CacheEntry.clean(valueFromDelegate);
            CacheEntry<UV> oldL1 = perKeyCache.l1MapEntries.put(userKey, newEntry);
            if(oldL1 != null) backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());
            backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
        }
        return valueFromDelegate;
    }

    private void flushReplacedDirtyL1Entry(UK userKey, CacheEntry<UV> replacedEntry, PerKeyMapCache<UK, UV, K, N> perKeyCache) throws Exception {
        if (replacedEntry != null && replacedEntry.isDirty()) {
            K originalKeyContext = backend.getCurrentKey();
            N originalNamespaceContext = this.getCurrentNamespace(); // This is CachingInternalMapState's current namespace
            
            // The perKeyCache was created with a specific Flink key and namespace context.
            // Operations on the delegateState for entries related to this perKeyCache
            // must use that specific context.
            K flushKeyContext = perKeyCache.flinkKeyForEvictionContext;
            N flushNamespaceContext = perKeyCache.namespaceForEvictionContext;

            try {
                backend.setCurrentKey(flushKeyContext);
                delegateState.setCurrentNamespace(flushNamespaceContext);

                if (replacedEntry.getValue() == null) { // It was a dirty tombstone
                    delegateState.remove(userKey);
                } else { // It was a dirty value
                    delegateState.put(userKey, replacedEntry.getValue());
                }
                // After successful flush, mark the CacheEntry as clean.
                replacedEntry.setDirty(false);
            } finally {
                // Restore the original Flink key and namespace context that was active
                // for the CachingInternalMapState before this flush operation.
                backend.setCurrentKey(originalKeyContext);
                delegateState.setCurrentNamespace(originalNamespaceContext);
            }
        }
    }

    @Override
    public void put(UK userKey, UV userValue) throws Exception {
        if (userValue == null) {
            remove(userKey);
            return;
        }

        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        CacheEntry<UV> oldL2 = perKeyCache.l2MapEntries.remove(userKey);
        if(oldL2 != null) backend.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes());
        
        CacheEntry<UV> newEntry = CacheEntry.dirty(userValue);
        // The put operation on the L1 cache might replace an existing entry for the same userKey.
        // This 'oldL1' is the entry that was previously associated with userKey in L1, if any.
        CacheEntry<UV> oldL1 = perKeyCache.l1MapEntries.put(userKey, newEntry);

        if (oldL1 != null) {
            // If the replaced L1 entry (oldL1) was dirty, it must be flushed to the delegate state
            // before its memory is simply released. Otherwise, a dirty update is lost.
            // This handles cases where the cache policy's put doesn't trigger eviction listener for replacements.
            if (oldL1.isDirty()) {
                flushReplacedDirtyL1Entry(userKey, oldL1, perKeyCache);
            }
            backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());
        }
        backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
        perKeyCache.fullyLoaded = false;
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        if (map == null || map.isEmpty()) {
            return;
        }
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();
        for (Map.Entry<UK, UV> entry : map.entrySet()) {
            UK userKey = entry.getKey();
            UV userValue = entry.getValue();

            CacheEntry<UV> newCacheEntry;
            if (userValue == null) {
                // This represents a "remove" operation for this userKey within the putAll.
                // The CacheEntry will be a dirty tombstone.
                newCacheEntry = CacheEntry.dirty(null);
            } else {
                newCacheEntry = CacheEntry.dirty(userValue);
            }

            // Remove from L2 cache if present
            CacheEntry<UV> oldL2 = perKeyCache.l2MapEntries.remove(userKey);
            if(oldL2 != null) backend.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes());

            // Put the new entry into L1, potentially replacing an old L1 entry for the same userKey.
            CacheEntry<UV> oldL1 = perKeyCache.l1MapEntries.put(userKey, newCacheEntry);
            
            if (oldL1 != null) {
                // If the replaced L1 entry (oldL1) was dirty, flush it.
                if (oldL1.isDirty()) {
                    flushReplacedDirtyL1Entry(userKey, oldL1, perKeyCache);
                }
                backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());
            }
            backend.reportCacheMemoryAdded(newCacheEntry.getEstimatedSizeBytes());
        }
        perKeyCache.fullyLoaded = false;
    }

    @Override
    public void remove(UK userKey) throws Exception {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();
        
        // Invalidate from L2 cache first
        CacheEntry<UV> oldL2 = perKeyCache.l2MapEntries.remove(userKey);
        if(oldL2 != null) backend.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes());

        // Create a dirty tombstone entry for L1.
        CacheEntry<UV> tombstone = CacheEntry.dirty(null);
        // Put the tombstone into L1. This might replace an existing entry (oldL1).
        CacheEntry<UV> oldL1 = perKeyCache.l1MapEntries.put(userKey, tombstone);

        if (oldL1 != null) {
            // If the entry replaced by the tombstone (oldL1) was a dirty *value* 
            // (i.e., not already a tombstone itself), then this dirty value must be flushed to delegate.
            // The new tombstone will be handled by the L1 eviction listener if it's later evicted.
            if (oldL1.isDirty() && oldL1.getValue() != null) {
                // oldL1 was a dirty value, not a tombstone. Flush it as a PUT.
                flushReplacedDirtyL1Entry(userKey, oldL1, perKeyCache);
            }
            // Account for memory released by oldL1.
            // This happens regardless of whether oldL1 was flushed or not; its L1 slot is gone.
            backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());
        }
        
        // Account for memory added by the new tombstone in L1.
        backend.reportCacheMemoryAdded(tombstone.getEstimatedSizeBytes());

        perKeyCache.fullyLoaded = false;
    }

    @Override
    public boolean contains(UK userKey) throws Exception {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        CacheEntry<UV> l1Entry = perKeyCache.l1MapEntries.get(userKey);
        if (l1Entry != null) {
            return l1Entry.getValue() != null;
        }

        CacheEntry<UV> l2Entry = perKeyCache.l2MapEntries.get(userKey);
        if (l2Entry != null) {
            perKeyCache.l2MapEntries.remove(userKey);
            CacheEntry<UV> oldL1 = perKeyCache.l1MapEntries.put(userKey, l2Entry);
            if(oldL1 != null) backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());
            backend.reportCacheMemoryAdded(l2Entry.getEstimatedSizeBytes());
            return true;
        }

        if (perKeyCache.fullyLoaded) {
            return false;
        }

        boolean delegateContains = delegateState.contains(userKey);
        if (delegateContains) {
            UV valueFromDelegate = delegateState.get(userKey);
            if (valueFromDelegate != null) {
                CacheEntry<UV> newEntry = CacheEntry.clean(valueFromDelegate);
                CacheEntry<UV> oldL1 = perKeyCache.l1MapEntries.put(userKey, newEntry);
                if(oldL1 != null) backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());
                backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
            }
        }
        return delegateContains;
    }

    private void loadAllEntriesToCache(PerKeyMapCache<UK, UV, K, N> perKeyCache) throws Exception {
        if (perKeyCache.fullyLoaded) {
            return;
        }
        K currentFK = backend.getCurrentKey();
        N currentNS = getCurrentNamespace();

        flushL1Entries(perKeyCache, currentFK, currentNS, this.backend, this.delegateState);

        Iterable<Map.Entry<UK, UV>> delegateEntries = delegateState.entries();
        if (delegateEntries != null) {
            for (Map.Entry<UK, UV> entry : delegateEntries) {
                UK userKey = entry.getKey();
                UV userValue = entry.getValue();

                CacheEntry<UV> l2Cached = perKeyCache.l2MapEntries.get(userKey);
                if (l2Cached != null) {
                    perKeyCache.l2MapEntries.remove(userKey);
                    CacheEntry<UV> oldL1 = perKeyCache.l1MapEntries.put(userKey, l2Cached);
                    if (oldL1 != null) backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());
                    backend.reportCacheMemoryAdded(l2Cached.getEstimatedSizeBytes());
                } else {
                    CacheEntry<UV> newEntry = CacheEntry.clean(userValue);
                    CacheEntry<UV> oldL1 = perKeyCache.l1MapEntries.put(userKey, newEntry);
                    if (oldL1 != null) backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());
                    backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
                }
            }
        }
        perKeyCache.fullyLoaded = true;
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();
        if (!perKeyCache.fullyLoaded) {
            loadAllEntriesToCache(perKeyCache);
        }

        Map<UK, UV> allEntriesMap = new HashMap<>();
        for (Map.Entry<UK, CacheEntry<UV>> l1Entry : perKeyCache.l1MapEntries.entrySet()) {
            if (l1Entry.getValue().getValue() != null) {
                allEntriesMap.put(l1Entry.getKey(), l1Entry.getValue().getValue());
            }
        }
        if (perKeyCache.fullyLoaded) {
            for (Map.Entry<UK, CacheEntry<UV>> l2Entry : perKeyCache.l2MapEntries.entrySet()) {
                if (!allEntriesMap.containsKey(l2Entry.getKey())) {
                    allEntriesMap.put(l2Entry.getKey(), l2Entry.getValue().getValue());
                }
            }
        }
        return allEntriesMap.entrySet();
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        List<UK> keysList = new ArrayList<>();
        for (Map.Entry<UK, UV> entry : entries()) {
            keysList.add(entry.getKey());
        }
        return keysList;
    }

    @Override
    public Iterable<UV> values() throws Exception {
        List<UV> valuesList = new ArrayList<>();
        for (Map.Entry<UK, UV> entry : entries()) {
            valuesList.add(entry.getValue());
        }
        return valuesList;
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        return entries().iterator();
    }

    private static <K_F, N_F, UK_C, UV_C> void flushL1Entries(
            PerKeyMapCache<UK_C, UV_C, K_F, N_F> perKeyCache, K_F flinkKey, N_F namespace,
            CachingKeyedStateBackend<K_F> backendForContext,
            InternalMapState<K_F, N_F, UK_C, UV_C> delegateStateForContext) throws Exception {

        if (flinkKey == null) {
            return;
        }

        List<Map.Entry<UK_C, CacheEntry<UV_C>>> dirtyL1EntriesToFlush = new ArrayList<>();
        List<Map.Entry<UK_C, CacheEntry<UV_C>>> cleanL1EntriesToL2 = new ArrayList<>();

        for (Map.Entry<UK_C, CacheEntry<UV_C>> l1Entry : perKeyCache.l1MapEntries.entrySet()) {
            if (l1Entry.getValue().isDirty()) {
                dirtyL1EntriesToFlush.add(l1Entry);
            } else if (l1Entry.getValue().getValue() != null) {
                cleanL1EntriesToL2.add(l1Entry);
            }
        }
        perKeyCache.l1MapEntries.clear();

        K_F originalKey = backendForContext.getCurrentKey();
        N_F originalNamespace = namespace;
        backendForContext.setCurrentKey(flinkKey);
        delegateStateForContext.setCurrentNamespace(namespace);

        try {
            for (Map.Entry<UK_C, CacheEntry<UV_C>> dirtyEntry : dirtyL1EntriesToFlush) {
                UK_C userKey = dirtyEntry.getKey();
                UV_C userValue = dirtyEntry.getValue().getValue();
                if (userValue == null) {
                    delegateStateForContext.remove(userKey);
                } else {
                    delegateStateForContext.put(userKey, userValue);
                    perKeyCache.l2MapEntries.put(userKey, CacheEntry.clean(userValue));
                }
                dirtyEntry.getValue().setDirty(false);
            }

            for (Map.Entry<UK_C, CacheEntry<UV_C>> cleanEntry : cleanL1EntriesToL2) {
                perKeyCache.l2MapEntries.put(cleanEntry.getKey(), cleanEntry.getValue());
            }

        } finally {
            backendForContext.setCurrentKey(originalKey);
            if (originalNamespace != null) {
                delegateStateForContext.setCurrentNamespace(originalNamespace);
            }
        }
    }

    @Override
    public boolean isEmpty() throws Exception {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        for (CacheEntry<UV> l1EntryValue : perKeyCache.l1MapEntries.values()) {
            if (l1EntryValue.getValue() != null) {
                return false;
            }
        }
        if (!perKeyCache.l2MapEntries.isEmpty()) {
            return false;
        }

        if (perKeyCache.fullyLoaded) {
            return true;
        }

        return delegateState.isEmpty();
    }

    @Override
    public void clear() {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();
        
        for (CacheEntry<UV> entry : perKeyCache.l1MapEntries.values()) {
            if(entry != null) backend.reportCacheMemoryReleased(entry.getEstimatedSizeBytes());
        }
        perKeyCache.l1MapEntries.clear();

        for (CacheEntry<UV> entry : perKeyCache.l2MapEntries.values()) {
            if(entry != null) backend.reportCacheMemoryReleased(entry.getEstimatedSizeBytes());
        }
        perKeyCache.l2MapEntries.clear();

        delegateState.clear();
        perKeyCache.fullyLoaded = false;
    }

    @Override
    public void flushToUnderlyingState() throws IOException {
        for (Map.Entry<N, CachePolicy<K, PerKeyMapCache<UK, UV, K, N>>> nsEntry : namespaceCaches
                .entrySet()) {
            N namespace = nsEntry.getKey();
            CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> keyCaches = nsEntry.getValue();

            List<K> flinkKeysInCache = new ArrayList<>();
            for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> keyEntry : keyCaches.entrySet()) {
                flinkKeysInCache.add(keyEntry.getKey());
            }

            for (K flinkKey : flinkKeysInCache) {
                PerKeyMapCache<UK, UV, K, N> perKeyCache = keyCaches.get(flinkKey);
                if (perKeyCache != null) {
                    try {
                        if (flinkKey != null) {
                            flushL1Entries(perKeyCache, flinkKey, namespace, this.backend,
                                    this.delegateState);
                        }
                    } catch (Exception e) {
                        throw new IOException("Failed to flush map entries for Flink key: "
                                + flinkKey + " in namespace: " + namespace, e);
                    }
                }
            }
        }
    }

    @Override
    public InternalMapState<K, N, UK, UV> getDelegateState() {
        return delegateState;
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
    public TypeSerializer<Map<UK, UV>> getValueSerializer() {
        return delegateState.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        this.currentNamespace = namespace;
        this.delegateState.setCurrentNamespace(namespace);
    }

    public N getCurrentNamespace() {
        if (currentNamespace == null) {
            throw new IllegalStateException(
                    "Namespace has not been set. Typically, you should call "
                            + "setCurrentNamespace" + " first.");
        }
        return currentNamespace;
    }

    public TypeSerializer<UK> getUserKeySerializer() {
        return this.userKeySerializer;
    }

    public TypeSerializer<UV> getUserValueSerializer() {
        return this.userValueSerializer;
    }

    @Override
    public byte[] getSerializedValue(final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<Map<UK, UV>> safeValueSerializer) throws Exception {
        throw new UnsupportedOperationException(
                "getSerializedValue directly is not supported by CachingInternalMapState due to cache structure.");
    }

    @Override
    public StateIncrementalVisitor<K, N, Map<UK, UV>> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        try {
            flushToUnderlyingState();
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush caches before creating state visitor", e);
        }
        return delegateState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    @Override
    public long evictEntriesToFreeMemory(long targetBytesToFreeThisState) {
        long bytesFreed = 0;
        if (targetBytesToFreeThisState <= 0) return 0;

        List<N> nsToIterate = new ArrayList<>();
        synchronized (namespaceCaches) {
            for(Map.Entry<N, CachePolicy<K, PerKeyMapCache<UK, UV, K, N>>> nsEntry : namespaceCaches.entrySet()) {
                nsToIterate.add(nsEntry.getKey());
            }
        }

        for (N namespace : nsToIterate) {
            CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> keyCaches = namespaceCaches.get(namespace);
            if (keyCaches == null) continue;

            List<K> flinkKeysToIterate = new ArrayList<>();
            synchronized(keyCaches) {
                for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> pkEntry : keyCaches.entrySet()) {
                    flinkKeysToIterate.add(pkEntry.getKey());
                }
            }

            for (K flinkKey : flinkKeysToIterate) {
                PerKeyMapCache<UK, UV, K, N> perKeyCache = keyCaches.get(flinkKey);
                if (perKeyCache == null) continue;

                Iterator<Map.Entry<UK, CacheEntry<UV>>> l2Iter = perKeyCache.l2MapEntries.entrySet().iterator();
                while (l2Iter.hasNext() && bytesFreed < targetBytesToFreeThisState) {
                    Map.Entry<UK, CacheEntry<UV>> entry = l2Iter.next();
                    long estimatedSize = entry.getValue().getEstimatedSizeBytes();
                    l2Iter.remove();
                    backend.reportCacheMemoryReleased(estimatedSize);
                    bytesFreed += estimatedSize;
                }
                if (bytesFreed >= targetBytesToFreeThisState) return bytesFreed;

                Iterator<Map.Entry<UK, CacheEntry<UV>>> l1IterClean = perKeyCache.l1MapEntries.entrySet().iterator();
                List<Map.Entry<UK, CacheEntry<UV>>> dirtyL1Entries = new ArrayList<>();
                while (l1IterClean.hasNext() && bytesFreed < targetBytesToFreeThisState) {
                    Map.Entry<UK, CacheEntry<UV>> entry = l1IterClean.next();
                    if (!entry.getValue().isDirty()) {
                        long estimatedSize = entry.getValue().getEstimatedSizeBytes();
                        l1IterClean.remove();
                        backend.reportCacheMemoryReleased(estimatedSize);
                        bytesFreed += estimatedSize;
                    } else {
                        dirtyL1Entries.add(entry);
                    }
                }
                if (bytesFreed >= targetBytesToFreeThisState) return bytesFreed;

                Iterator<Map.Entry<UK, CacheEntry<UV>>> dirtyIter = dirtyL1Entries.iterator();
                while (dirtyIter.hasNext() && bytesFreed < targetBytesToFreeThisState) {
                    Map.Entry<UK, CacheEntry<UV>> dirtyEntryTuple = dirtyIter.next();
                    UK userKey = dirtyEntryTuple.getKey();
                    CacheEntry<UV> dirtyEntry = dirtyEntryTuple.getValue();
                    UV userValue = dirtyEntry.getValue();
                    long estimatedSize = dirtyEntry.getEstimatedSizeBytes();
                    try {
                        K originalBackendKey = backend.getCurrentKey();
                        N originalDelegateNamespace = this.currentNamespace;

                        backend.setCurrentKey(flinkKey);
                        delegateState.setCurrentNamespace(namespace);
                        if (userValue == null) {
                            delegateState.remove(userKey);
                        } else {
                            delegateState.put(userKey, userValue);
                        }
                        dirtyEntry.setDirty(false);

                        backend.setCurrentKey(originalBackendKey);
                        delegateState.setCurrentNamespace(originalDelegateNamespace);
                        
                        perKeyCache.l1MapEntries.remove(userKey);
                        backend.reportCacheMemoryReleased(estimatedSize);
                        bytesFreed += estimatedSize;

                    } catch (Exception e) {
                        // Error during global eviction flush
                    }
                }
                if (bytesFreed >= targetBytesToFreeThisState) return bytesFreed;
            }
        }
        return bytesFreed;
    }
}
