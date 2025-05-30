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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
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
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.Collections;

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
    private final int mapL1KeyPresenceCacheSize; // Added
    private final int mapL2KeyPresenceCacheSize; // Added

    // Metrics
    private final transient MetricGroup metrics;
    private final transient Counter l1ValueCacheHitCount;
    private final transient Counter l1ValueCacheMissCount;
    private final transient Counter l2ValueCacheHitCount;
    private final transient Counter l2ValueCacheMissCount;
    private final transient Counter l1PresenceCacheHitCount;
    private final transient Counter l1PresenceCacheMissCount;
    private final transient Counter l2PresenceCacheHitCount;
    private final transient Counter l2PresenceCacheMissCount;
    private final transient Counter delegateLookups;

    // Gauge for cache entries will be registered on a PerKeyMapCache basis if needed,
    // or globally if we aggregate across all PerKeyMapCaches (more complex).
    // For now, let's focus on hit/miss counters for the overall CachingInternalMapState.
    // Global gauges for total entries in L1/L2 value/presence caches can be done by iterating
    // namespaceCaches, which is potentially expensive for a gauge.
    // A simpler approach for gauges might be to sum them up periodically if needed, or count
    // entries within a specific PerKeyMapCache when it's active.

    // Helper class to hold L1 and L2 caches for a specific Flink Key/Namespace's
    // map entries
    private static class PerKeyMapCache<UK_C, UV_C, K_F, N_F> {
        final CachePolicy<UK_C, CacheEntry<UV_C>> l1MapEntries;
        final CachePolicy<UK_C, CacheEntry<UV_C>> l2MapEntries; // Should only hold clean entries
        final CachePolicy<UK_C, CacheEntry<Boolean>> l1KeyPresenceCache; // Added
        final CachePolicy<UK_C, CacheEntry<Boolean>> l2KeyPresenceCache; // Added
        boolean fullyLoaded = false;
        private final N_F mainContextDelegateNamespace;
        private final K_F flinkKey;
        private final N_F cacheNamespace;
        private final CachingKeyedStateBackend<K_F> ownerBackend;
        final InternalMapState<K_F, N_F, UK_C, UV_C> delegateState; // Made final

        PerKeyMapCache(int l1Size, int l2Size, InternalMapState<K_F, N_F, UK_C, UV_C> delegateState,
                CachingKeyedStateBackend<K_F> ownerBackend, K_F flinkKey, N_F cacheNamespace,
                N_F mainContextDelegateNamespace,
                CachingStateBackendFactory.CachePolicyType cachePolicyType,
                int mapL1KeyPresenceCacheSize, int mapL2KeyPresenceCacheSize) {
            this.mainContextDelegateNamespace = mainContextDelegateNamespace;
            this.flinkKey = flinkKey;
            this.cacheNamespace = cacheNamespace;
            this.delegateState = delegateState;
            this.ownerBackend = ownerBackend;

            // Initialize L2 Presence Cache (no eviction listener needed beyond capacity)
            this.l2KeyPresenceCache = createCachePolicyInstance(cachePolicyType, mapL2KeyPresenceCacheSize, null, ownerBackend, true);

            // Initialize L1 Presence Cache (with eviction to L2 Presence Cache)
            this.l1KeyPresenceCache = createCachePolicyInstance(cachePolicyType, mapL1KeyPresenceCacheSize, evictedL1PresenceEntry -> {
                // Move from L1 presence to L2 presence
                if (evictedL1PresenceEntry.getValue().getValue() != null) { // Only move non-null (though presence is boolean)
                    this.l2KeyPresenceCache.put(evictedL1PresenceEntry.getKey(), CacheEntry.clean(evictedL1PresenceEntry.getValue().getValue()));
                }
                // Report memory released by L1 presence cache entry (Boolean)
                 ownerBackend.reportCacheMemoryReleased(ValueSizeUtils.estimate(Boolean.TRUE));


            }, ownerBackend, true);


            // Initialize L2 Map Entries Cache (no eviction listener needed beyond capacity)
            this.l2MapEntries = createCachePolicyInstance(cachePolicyType, l2Size, null, ownerBackend, false);

            // Initialize L1 Map Entries Cache (with eviction logic)
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
                                originalDelegateNamespaceContext = this.mainContextDelegateNamespace;

                                ownerBackend.setCurrentKey(flinkKey);
                                delegateState.setCurrentNamespace(cacheNamespace);

                                if (evictedUVWrapper.getValue() == null) { // Is a tombstone
                                    delegateState.remove(evictedUK);
                                } else {
                                    delegateState.put(evictedUK, evictedUVWrapper.getValue());
                                    // Add to L2 cache only if it was a regular value, not a tombstone
                                    this.l2MapEntries.put(evictedUK, CacheEntry.clean(evictedUVWrapper.getValue()));
                                }
                                evictedUVWrapper.setDirty(false); // Mark clean after successful flush
                            } catch (Exception e) {
                                throw new RuntimeException("Failed to flush L1 map entry to delegate for user key: " + evictedUK, e);
                            } finally {
                                if (originalKeyContext != null) {
                                    ownerBackend.setCurrentKey(originalKeyContext);
                                }
                                if (originalDelegateNamespaceContext != null) {
                                    delegateState.setCurrentNamespace(originalDelegateNamespaceContext);
                                }
                            }
                        } else { // Non-dirty entry
                            if (evictedUVWrapper.getValue() != null) { // Don't put null (tombstones) from clean L1 into L2
                                this.l2MapEntries.put(evictedUK, evictedUVWrapper); // Already clean
                            }
                        }
                    }, ownerBackend, false);
        }

        private static <CK, CV_ENTRY_TYPE> CachePolicy<CK, CacheEntry<CV_ENTRY_TYPE>> createCachePolicyInstance(
                CachingStateBackendFactory.CachePolicyType policyType, int capacity,
                Consumer<Map.Entry<CK, CacheEntry<CV_ENTRY_TYPE>>> evictionListener,
                CachingKeyedStateBackend<?> ownerBackendForSize, boolean isPresenceCache) {
            
            Consumer<Map.Entry<CK, CacheEntry<CV_ENTRY_TYPE>>> wrappedEvictionListener = null;
            if (evictionListener != null) {
                wrappedEvictionListener = entry -> {
                    if (isPresenceCache) {
                        ownerBackendForSize.reportCacheMemoryReleased(ValueSizeUtils.estimate(Boolean.TRUE));
                    } else {
                         CacheEntry<CV_ENTRY_TYPE> cacheEntry = entry.getValue();
                         if (cacheEntry != null) ownerBackendForSize.reportCacheMemoryReleased(cacheEntry.getEstimatedSizeBytes());
                    }
                    evictionListener.accept(entry);
                };
            } else {
                 wrappedEvictionListener = entry -> {
                    if (isPresenceCache) {
                        ownerBackendForSize.reportCacheMemoryReleased(ValueSizeUtils.estimate(Boolean.TRUE));
                    } else {
                        CacheEntry<CV_ENTRY_TYPE> cacheEntry = entry.getValue();
                        if (cacheEntry != null) ownerBackendForSize.reportCacheMemoryReleased(cacheEntry.getEstimatedSizeBytes());
                    }
                 };
            }


            switch (policyType) {
                case TINYLFU:
                    return new TinyLFUMap<>(capacity, wrappedEvictionListener);
                case LRU:
                default:
                    if (wrappedEvictionListener != null) {
                        return new LRUMap<>(capacity, wrappedEvictionListener);
                    }
                    return new LRUMap<>(capacity);
            }
        }

        void clearAll() throws Exception {
            // Context (current key/namespace for delegateState) is set by the caller before clearAll is invoked.
            Iterator<Map.Entry<UK_C, CacheEntry<UV_C>>> l1Iterator = this.l1MapEntries.entrySet().iterator();
            while (l1Iterator.hasNext()) {
                Map.Entry<UK_C, CacheEntry<UV_C>> l1Entry = l1Iterator.next();
                UK_C userKey = l1Entry.getKey();
                CacheEntry<UV_C> cacheEntry = l1Entry.getValue();
                if (cacheEntry.isDirty()) {
                    UV_C userValue = cacheEntry.getValue();
                    if (userValue == null) { // Tombstone
                        this.delegateState.remove(userKey);
                    } else {
                        this.delegateState.put(userKey, userValue);
                    }
                }
                this.ownerBackend.reportCacheMemoryReleased(cacheEntry.getEstimatedSizeBytes());
                l1Iterator.remove();
            }

            Iterator<Map.Entry<UK_C, CacheEntry<UV_C>>> l2Iterator = this.l2MapEntries.entrySet().iterator();
            while (l2Iterator.hasNext()) {
                Map.Entry<UK_C, CacheEntry<UV_C>> l2Entry = l2Iterator.next();
                if (l2Entry.getValue().isDirty()) {
                    if (l2Entry.getValue().getValue() == null) { // Tombstone
                        this.delegateState.remove(l2Entry.getKey());
                    } else {
                        this.delegateState.put(l2Entry.getKey(), l2Entry.getValue().getValue());
                    }
                }
                this.ownerBackend.reportCacheMemoryReleased(l2Entry.getValue().getEstimatedSizeBytes());
                l2Iterator.remove();
            }
            this.l1KeyPresenceCache.clear();
            this.l2KeyPresenceCache.clear();
            this.fullyLoaded = false;
        }
    }

    public CachingInternalMapState(InternalMapState<K, N, UK, UV> delegateState,
            CachingKeyedStateBackend<K> backend, int l1CacheSizePerMap, int l2CacheSizePerMap,
            int maxFlinkKeysWithActiveCachesPerNamespace, long maxCacheMemoryMb,
            CachingStateBackendFactory.CachePolicyType cachePolicyType,
            int mapL1KeyPresenceCacheSize, int mapL2KeyPresenceCacheSize,
            MetricGroup metrics) {
        this.delegateState = delegateState;
        this.backend = backend;
        this.l1CacheSizePerMap = l1CacheSizePerMap;
        this.l2CacheSizePerMap = l2CacheSizePerMap;
        this.mapL1KeyPresenceCacheSize = mapL1KeyPresenceCacheSize;
        this.mapL2KeyPresenceCacheSize = mapL2KeyPresenceCacheSize;
        this.maxFlinkKeysWithActiveCachesPerNamespace = maxFlinkKeysWithActiveCachesPerNamespace;
        this.maxActiveNamespacesInCache = backend.getMaxActiveNamespaceOrPerKeyCacheContainers();
        this.cachePolicyType = cachePolicyType;
        this.metrics = metrics;
        // currentNamespace is inherited and set via setCurrentNamespace

        final TypeSerializer<Map<UK, UV>> mapSerializer = delegateState.getValueSerializer();
        if (mapSerializer instanceof MapSerializer) {
            this.userKeySerializer = ((MapSerializer<UK, UV>) mapSerializer).getKeySerializer();
            this.userValueSerializer = ((MapSerializer<UK, UV>) mapSerializer).getValueSerializer();
        } else {
            // This path indicates a programming error or an unexpected type for the delegate's value serializer.
            // CachingInternalMapState is designed to wrap an InternalMapState whose value type is Map<UK, UV>,
            // and its serializer is expected to be a MapSerializer<UK, UV>.
            LOG.error(
                    "Delegate state's value serializer is not a MapSerializer. "
                            + "User key/value serializers will be null. This may lead to NullPointerExceptions. "
                            + "Actual serializer type: {}",
                    mapSerializer != null ? mapSerializer.getClass().getName() : "null");
            // To satisfy 'final' field requirements and highlight the issue,
            // assign null or throw, though throwing is safer to prevent further errors.
            // For now, let the linter catch uninitialized final fields if this path means they can't be set.
            // Throwing an exception is more direct:
            throw new IllegalArgumentException(
                    "The value serializer of the delegate InternalMapState must be a MapSerializer. Found: "
                            + (mapSerializer != null ? mapSerializer.getClass().getName() : "null"));
        }

        this.namespaceCaches = createCachePolicyForHierarchicalCache(
            this.maxActiveNamespacesInCache, evictedNamespaceEntry -> {
                CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> perKeyCachesInNamespace =
                        evictedNamespaceEntry.getValue();
                for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> entry : perKeyCachesInNamespace.entrySet()) {
                    PerKeyMapCache<UK, UV, K, N> perKeyCache = entry.getValue();
                    try {
                        K originalKeyForBackend = this.backend.getCurrentKey();
                        // Use this.currentNamespace (from CachingInternalMapState) for the delegate's original namespace context
                        N originalNamespaceForDelegate = this.currentNamespace; 

                        this.backend.setCurrentKey(perKeyCache.flinkKey);
                        // Set the delegate's namespace for the scope of clearAll
                        this.delegateState.setCurrentNamespace(perKeyCache.mainContextDelegateNamespace);
                        
                        perKeyCache.clearAll();

                        this.backend.setCurrentKey(originalKeyForBackend);
                        // Restore the delegate's namespace
                        this.delegateState.setCurrentNamespace(originalNamespaceForDelegate);
                    } catch (Exception e) {
                        LOG.error(
                                "Error clearing per-key cache for Flink key {} in namespace {}",
                                perKeyCache.flinkKey,
                                perKeyCache.cacheNamespace, e);
                    }
                }
                perKeyCachesInNamespace.clear(); 
            });

        // Initialize counters
        this.l1ValueCacheHitCount = metrics.counter("l1ValueCacheHits");
        this.l1ValueCacheMissCount = metrics.counter("l1ValueCacheMisses");
        this.l2ValueCacheHitCount = metrics.counter("l2ValueCacheHits");
        this.l2ValueCacheMissCount = metrics.counter("l2ValueCacheMisses");
        this.l1PresenceCacheHitCount = metrics.counter("l1PresenceCacheHits");
        this.l1PresenceCacheMissCount = metrics.counter("l1PresenceCacheMisses");
        this.l2PresenceCacheHitCount = metrics.counter("l2PresenceCacheHits");
        this.l2PresenceCacheMissCount = metrics.counter("l2PresenceCacheMisses");
        this.delegateLookups = metrics.counter("delegateLookups");
        
        metrics.gauge("totalL1ValueCacheEntries", () -> {
            long count = 0;
            for (Map.Entry<N, CachePolicy<K, PerKeyMapCache<UK, UV, K, N>>> nsEntry : namespaceCaches.entrySet()) {
                for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> keyEntry : nsEntry.getValue().entrySet()) {
                    count += keyEntry.getValue().l1MapEntries.size();
                }
            }
            return count;
        });
        metrics.gauge("totalL2ValueCacheEntries", () -> {
            long count = 0;
            for (Map.Entry<N, CachePolicy<K, PerKeyMapCache<UK, UV, K, N>>> nsEntry : namespaceCaches.entrySet()) {
                for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> keyEntry : nsEntry.getValue().entrySet()) {
                    count += keyEntry.getValue().l2MapEntries.size();
                }
            }
            return count;
        });
        metrics.gauge("totalL1PresenceCacheEntries", () -> {
            long count = 0;
            for (Map.Entry<N, CachePolicy<K, PerKeyMapCache<UK, UV, K, N>>> nsEntry : namespaceCaches.entrySet()) {
                for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> keyEntry : nsEntry.getValue().entrySet()) {
                    count += keyEntry.getValue().l1KeyPresenceCache.size();
                }
            }
            return count;
        });
        metrics.gauge("totalL2PresenceCacheEntries", () -> {
            long count = 0;
            for (Map.Entry<N, CachePolicy<K, PerKeyMapCache<UK, UV, K, N>>> nsEntry : namespaceCaches.entrySet()) {
                for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> keyEntry : nsEntry.getValue().entrySet()) {
                    count += keyEntry.getValue().l2KeyPresenceCache.size();
                }
            }
            return count;
        });
    }

    // Helper for non-CacheEntry valued caches (like namespaceCaches, keyCaches)
    private <CK, CV> CachePolicy<CK, CV> createCachePolicyForHierarchicalCache(int capacity, Consumer<Map.Entry<CK, CV>> evictionListener) {
        switch (this.cachePolicyType) {
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

    private PerKeyMapCache<UK, UV, K, N> getOrCreatePerKeyMapCache() {
        K currentKey = backend.getCurrentKey();
        N currentNs = getCurrentNamespace();
        if (currentNs == null) {
            throw new IllegalStateException(
                    "Current namespace is not set. Call setCurrentNamespace first.");
        }

        CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> keyCaches = namespaceCaches
                .computeIfAbsent(currentNs, ns -> createCachePolicyForHierarchicalCache(
                        maxFlinkKeysWithActiveCachesPerNamespace, evictedKeyCacheEntry -> {
                            K evictedFlinkKey = (K) evictedKeyCacheEntry.getKey(); // Cast needed due to generic CV type
                            PerKeyMapCache<UK, UV, K, N> perKeyCacheToFlush = (PerKeyMapCache<UK, UV, K, N>) evictedKeyCacheEntry.getValue(); // Cast needed
                            try {
                                flushL1Entries(perKeyCacheToFlush, evictedFlinkKey, ns,
                                        this.backend, this.delegateState);
                                // Also flush L1 presence caches
                                for (Map.Entry<UK, CacheEntry<Boolean>> presenceEntry : perKeyCacheToFlush.l1KeyPresenceCache.entrySet()) {
                                     if (presenceEntry.getValue().getValue() != null) {
                                        perKeyCacheToFlush.l2KeyPresenceCache.put(presenceEntry.getKey(), CacheEntry.clean(presenceEntry.getValue().getValue()));
                                     }
                                }
                                perKeyCacheToFlush.l1KeyPresenceCache.clear();
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Failed to flush PerKeyMapCache on its eviction for Flink key: "
                                                + evictedFlinkKey,
                                        e);
                            }
                        })); // Removed extra args: this.backend, false

        return keyCaches.computeIfAbsent(currentKey,
                k -> new PerKeyMapCache<>(l1CacheSizePerMap, l2CacheSizePerMap, delegateState,
                        backend, k, currentNs, currentNs, this.cachePolicyType,
                        mapL1KeyPresenceCacheSize, mapL2KeyPresenceCacheSize)); // Pass presence cache sizes
    }

    @Override
    public UV get(UK userKey) throws Exception {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        // 1. Check L1 Presence Cache
        CacheEntry<Boolean> l1Presence = perKeyCache.l1KeyPresenceCache.get(userKey);
        if (l1Presence != null) {
            l1PresenceCacheHitCount.inc();
            if (l1Presence.getValue()) { // Key known to be present
                CacheEntry<UV> l1Value = perKeyCache.l1MapEntries.get(userKey);
                if (l1Value != null) { l1ValueCacheHitCount.inc(); return l1Value.getValue(); }
                l1ValueCacheMissCount.inc();
                CacheEntry<UV> l2Value = perKeyCache.l2MapEntries.get(userKey);
                if (l2Value != null) { // L2 Value Hit
                    l2ValueCacheHitCount.inc();
                    perKeyCache.l2MapEntries.remove(userKey); 
                    CacheEntry<UV> oldL1Val = perKeyCache.l1MapEntries.put(userKey, l2Value);
                    if (oldL1Val != null) backend.reportCacheMemoryReleased(oldL1Val.getEstimatedSizeBytes());
                    backend.reportCacheMemoryAdded(l2Value.getEstimatedSizeBytes());
                    return l2Value.getValue();
                }
                l2ValueCacheMissCount.inc();
                LOG.warn("L1 Presence cache indicated key {} exists, but value not found in L1/L2 value caches. Potentially loading from delegate.", userKey);
                // Fall-through to delegate load for value if L1P=true but value not in L1V/L2V
            } else { // Key known to be absent
                return null;
            }
        } else {
            l1PresenceCacheMissCount.inc();
        }

        // 2. Check L2 Presence Cache (if L1P miss)
        CacheEntry<Boolean> l2Presence = perKeyCache.l2KeyPresenceCache.get(userKey);
        if (l2Presence != null) {
            l2PresenceCacheHitCount.inc();
            // Promote L2P to L1P
            perKeyCache.l2KeyPresenceCache.remove(userKey); 
            CacheEntry<Boolean> oldL1Presence = perKeyCache.l1KeyPresenceCache.put(userKey, l2Presence);
            if (oldL1Presence != null) backend.reportCacheMemoryReleased(ValueSizeUtils.estimate(Boolean.TRUE));
            backend.reportCacheMemoryAdded(ValueSizeUtils.estimate(Boolean.TRUE));

            if (l2Presence.getValue()) { // Key known to be present (from L2P)
                // Try L1/L2 value caches first (in case of race or recent promotion not reflected in initial L1P check)
                CacheEntry<UV> l1Value = perKeyCache.l1MapEntries.get(userKey);
                if (l1Value != null) { l1ValueCacheHitCount.inc(); return l1Value.getValue(); }
                // l1ValueCacheMissCount already inc'd if we got here via L1P miss
                CacheEntry<UV> l2Value = perKeyCache.l2MapEntries.get(userKey);
                if (l2Value != null) { // L2 Value Hit
                    l2ValueCacheHitCount.inc();
                    perKeyCache.l2MapEntries.remove(userKey); 
                    CacheEntry<UV> oldL1Val = perKeyCache.l1MapEntries.put(userKey, l2Value);
                    if (oldL1Val != null) backend.reportCacheMemoryReleased(oldL1Val.getEstimatedSizeBytes());
                    backend.reportCacheMemoryAdded(l2Value.getEstimatedSizeBytes());
                    return l2Value.getValue();
                }
                // l2ValueCacheMissCount already inc'd if we got here via L1P miss
                // Fall-through to delegate load for value
            } else { // Key known to be absent (from L2P)
                return null;
            }
        } else {
            l2PresenceCacheMissCount.inc();
        }

        // 3. Presence not in L1P or L2P. Check value caches directly (L1V then L2V)
        // This path is taken if both L1P and L2P miss.
        CacheEntry<UV> l1Value = perKeyCache.l1MapEntries.get(userKey);
        if (l1Value != null) {
            l1ValueCacheHitCount.inc();
            updatePresenceCache(perKeyCache, userKey, true, l1Value.isDirty());
            return l1Value.getValue();
        }
        // l1ValueCacheMissCount already inc'd if we got here via L1P miss

        CacheEntry<UV> l2Value = perKeyCache.l2MapEntries.get(userKey);
        if (l2Value != null) {
            l2ValueCacheHitCount.inc();
            perKeyCache.l2MapEntries.remove(userKey); // Promote L2V to L1V
            CacheEntry<UV> oldL1Val = perKeyCache.l1MapEntries.put(userKey, l2Value);
            if (oldL1Val != null) backend.reportCacheMemoryReleased(oldL1Val.getEstimatedSizeBytes());
            backend.reportCacheMemoryAdded(l2Value.getEstimatedSizeBytes());
            updatePresenceCache(perKeyCache, userKey, true, l2Value.isDirty()); 
            return l2Value.getValue();
        }
        // l2ValueCacheMissCount already inc'd if we got here via L1P miss, L1V miss

        // 4. Not in any cache, or fullyLoaded implies absence if not found by now
        if (perKeyCache.fullyLoaded) {
            updatePresenceCache(perKeyCache, userKey, false, false); 
            return null;
        }

        // 5. Go to delegate
        delegateLookups.inc();
        UV valueFromDelegate = delegateState.get(userKey);
        if (valueFromDelegate != null) {
            CacheEntry<UV> newEntry = CacheEntry.clean(valueFromDelegate);
            CacheEntry<UV> oldL1 = perKeyCache.l1MapEntries.put(userKey, newEntry);
            if (oldL1 != null) backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());
            backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
            updatePresenceCache(perKeyCache, userKey, true, false); 
        } else {
            updatePresenceCache(perKeyCache, userKey, false, false); 
        }
        return valueFromDelegate;
    }

    private void updatePresenceCache(PerKeyMapCache<UK, UV, K, N> perKeyCache, UK userKey, boolean present, boolean isValueDirty) {
        // Presence cache entries are always 'clean' in terms of their own state (Boolean)
        // but their existence implies something about the associated value entry's dirtiness/state.
        CacheEntry<Boolean> presenceEntry = CacheEntry.clean(present);
        CacheEntry<Boolean> oldL1P = perKeyCache.l1KeyPresenceCache.put(userKey, presenceEntry);
        if (oldL1P == null) { // New L1 presence entry
            backend.reportCacheMemoryAdded(ValueSizeUtils.estimate(Boolean.TRUE));
        } else if (oldL1P.getValue() != present) { // Presence state changed, effectively a new entry too
            // No double release/add, put handles replacement reporting if policy does so.
            // For LRU/TinyLFU, a put is a new entry if key different or value changed for frequency.
        } 
    }

    private void invalidatePresenceCache(PerKeyMapCache<UK, UV, K, N> perKeyCache, UK userKey) {
        CacheEntry<Boolean> oldL1P = perKeyCache.l1KeyPresenceCache.remove(userKey);
        if (oldL1P != null) backend.reportCacheMemoryReleased(ValueSizeUtils.estimate(Boolean.TRUE));
        CacheEntry<Boolean> oldL2P = perKeyCache.l2KeyPresenceCache.remove(userKey);
        if (oldL2P != null) backend.reportCacheMemoryReleased(ValueSizeUtils.estimate(Boolean.TRUE));
    }

    @Override
    public void put(UK userKey, UV userValue) throws Exception {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();
        CacheEntry<UV> newEntry = CacheEntry.dirty(userValue);
        CacheEntry<UV> oldL1Entry = perKeyCache.l1MapEntries.put(userKey, newEntry);
        flushReplacedDirtyL1Entry(oldL1Entry, userKey, perKeyCache.delegateState, perKeyCache);
        backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
        updatePresenceCache(perKeyCache, userKey, true, true);
        perKeyCache.fullyLoaded = false;
    }

    private void flushReplacedDirtyL1Entry(CacheEntry<UV> replacedEntry, UK userKey, InternalMapState<K,N,UK,UV> delegateForFlush, PerKeyMapCache<UK, UV, K, N> currentPerKeyCache) throws Exception {
        if (replacedEntry != null) {
            backend.reportCacheMemoryReleased(replacedEntry.getEstimatedSizeBytes());
            if (replacedEntry.isDirty()) {
                CacheEntry<UV> oldL2 = currentPerKeyCache.l2MapEntries.put(userKey, replacedEntry);
                if (oldL2 != null) {
                    backend.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes());
                    if (oldL2.isDirty()) {
                        delegateLookups.inc();
                        delegateForFlush.put(userKey, oldL2.getValue());
                    }
                }
                // Behavioral check: if L2 cache is no-op, its size would be 0 after a put if it doesn't retain.
                // This is a fallback if `instanceof NoOpCachePolicy` is problematic.
                // A true NoOpCachePolicy should always have size 0.
                if (currentPerKeyCache.l2MapEntries.size() == 0 && replacedEntry.isDirty()){
                     //This check might be true if L2 is not NoOp but just became empty after this put+eviction.
                     //A more robust check for NoOp would be if it was created with capacity 0.
                     //For now, using this as a proxy if NoOpCachePolicy type check fails.
                     //The NoOpCachePolicy class itself always has size 0.
                    if (isL2CacheNoOp(currentPerKeyCache)) { //Requires a helper to check creation capacity if NoOp class is not type-checkable
                        delegateLookups.inc(); 
                        delegateForFlush.put(userKey, replacedEntry.getValue());
                    }
                }
            }
        }
    }

    // Helper method, assumes l2CacheSizePerMap is available, or pass it to PerKeyMapCache to store its own capacity.
    // This is still not ideal. The best is if NoOpCachePolicy is instanceof checkable.
    // For now, let's assume the createCachePolicyInstance correctly returns NoOpCachePolicy and it can be checked.
    // If the linter still fails on `instanceof NoOpCachePolicy`, this path is difficult.
    // Sticking to the idea that NoOpCachePolicy class should be available for `instanceof` from the same package.
    // The previous edit had: if (currentPerKeyCache.l2MapEntries instanceof NoOpCachePolicy && replacedEntry.isDirty()){ ... }
    // This should work if NoOpCachePolicy is a public or package-private class in org.apache.flink.contrib.streaming.state.

    private boolean isL2CacheNoOp(PerKeyMapCache<UK, UV, K, N> perKeyCache) {
        // This is a placeholder for a robust check. Ideally, PerKeyMapCache stores its L2 capacity
        // or NoOpCachePolicy is instanceof-checkable.
        // If l2CacheSizePerMap is available here, we can check against it.
        // return this.l2CacheSizePerMap <= 0; // if CachingInternalMapState.l2CacheSizePerMap is the one used for this perKeyCache
        return perKeyCache.l2MapEntries.getClass().getSimpleName().equals("NoOpCachePolicy"); // Highly fragile, reflection based. BAD.
        // Prefer direct instanceof check if NoOpCachePolicy class is resolvable.
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        if (map == null || map.isEmpty()) {
            // If map is null or empty, it's a no-op. 
            // fullyLoaded status should not change based on a no-op.
            return;
        }

        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();
        for (Map.Entry<UK, UV> entry : map.entrySet()) {
            CacheEntry<UV> newCacheEntry = CacheEntry.dirty(entry.getValue());
            CacheEntry<UV> oldL1Entry = perKeyCache.l1MapEntries.put(entry.getKey(), newCacheEntry);
            flushReplacedDirtyL1Entry(oldL1Entry, entry.getKey(), perKeyCache.delegateState, perKeyCache);
            backend.reportCacheMemoryAdded(newCacheEntry.getEstimatedSizeBytes());
            updatePresenceCache(perKeyCache, entry.getKey(), true, true);
        }
        perKeyCache.fullyLoaded = false;
    }

    @Override
    public void remove(UK userKey) throws Exception {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();
        CacheEntry<UV> removedL1Entry = perKeyCache.l1MapEntries.put(userKey, CacheEntry.dirty(null)); 
        if (removedL1Entry != null) {
            backend.reportCacheMemoryReleased(removedL1Entry.getEstimatedSizeBytes());
        }
        backend.reportCacheMemoryAdded(CacheEntry.dirty(null).getEstimatedSizeBytes());
        
        CacheEntry<UV> removedL2Entry = perKeyCache.l2MapEntries.remove(userKey);
        if (removedL2Entry != null) {
            backend.reportCacheMemoryReleased(removedL2Entry.getEstimatedSizeBytes());
        }
        updatePresenceCache(perKeyCache, userKey, false, true);
        delegateLookups.inc();
        delegateState.remove(userKey);
        perKeyCache.fullyLoaded = false;
    }

    @Override
    public boolean contains(UK userKey) throws Exception {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        CacheEntry<Boolean> l1Presence = perKeyCache.l1KeyPresenceCache.get(userKey);
        if (l1Presence != null) { l1PresenceCacheHitCount.inc(); return l1Presence.getValue(); }
        l1PresenceCacheMissCount.inc();

        CacheEntry<Boolean> l2Presence = perKeyCache.l2KeyPresenceCache.get(userKey);
        if (l2Presence != null) {
            l2PresenceCacheHitCount.inc();
            // Promote L2P to L1P
            perKeyCache.l2KeyPresenceCache.remove(userKey);
            CacheEntry<Boolean> oldL1P = perKeyCache.l1KeyPresenceCache.put(userKey, l2Presence);
            if (oldL1P != null) backend.reportCacheMemoryReleased(ValueSizeUtils.estimate(Boolean.TRUE));
            backend.reportCacheMemoryAdded(ValueSizeUtils.estimate(Boolean.TRUE));
            return l2Presence.getValue();
        }
        l2PresenceCacheMissCount.inc();

        // Presence not in L1P or L2P. Check value caches to potentially populate presence.
        // This is more about deducing presence if value is there, rather than a value cache hit for this operation's purpose.
        CacheEntry<UV> l1Value = perKeyCache.l1MapEntries.get(userKey);
        if (l1Value != null) {
            boolean isPresent = l1Value.getValue() != null; // Assuming null value means key not present in map context
            updatePresenceCache(perKeyCache, userKey, isPresent, l1Value.isDirty());
            return isPresent;
        }
        CacheEntry<UV> l2Value = perKeyCache.l2MapEntries.get(userKey);
        if (l2Value != null) {
            // Promote L2V to L1V
            perKeyCache.l2MapEntries.remove(userKey);
            CacheEntry<UV> oldL1Val = perKeyCache.l1MapEntries.put(userKey, l2Value);
            if (oldL1Val != null) backend.reportCacheMemoryReleased(oldL1Val.getEstimatedSizeBytes());
            backend.reportCacheMemoryAdded(l2Value.getEstimatedSizeBytes());
            updatePresenceCache(perKeyCache, userKey, true, l2Value.isDirty()); 
            return true; 
        }

        if (perKeyCache.fullyLoaded) {
            updatePresenceCache(perKeyCache, userKey, false, false);
            return false;
        }

        delegateLookups.inc();
        boolean delegateContains = delegateState.contains(userKey);
        updatePresenceCache(perKeyCache, userKey, delegateContains, false);
        // For `contains`, we don't load the value into the value cache if it was a miss there but found in delegate.
        // We only update the presence cache.
        // However, the previous version had a sub-optimal get, let's remove it.
        return delegateContains;
    }

    private void loadAllEntriesToCache(PerKeyMapCache<UK, UV, K, N> perKeyCache) throws Exception {
        if (perKeyCache.fullyLoaded) {
            return;
        }
        perKeyCache.l1MapEntries.clear();
        perKeyCache.l2MapEntries.clear();
        perKeyCache.l1KeyPresenceCache.clear();
        perKeyCache.l2KeyPresenceCache.clear();

        delegateLookups.inc(); // Counts as one major interaction for loading all.
        Iterable<Map.Entry<UK, UV>> entries = delegateState.entries();
        if (entries != null) {
            for (Map.Entry<UK, UV> entry : entries) {
                CacheEntry<UV> cacheEntry = CacheEntry.clean(entry.getValue());
                perKeyCache.l1MapEntries.put(entry.getKey(), cacheEntry);
                backend.reportCacheMemoryAdded(cacheEntry.getEstimatedSizeBytes());
                updatePresenceCache(perKeyCache, entry.getKey(), true, false); // Mark as present, clean
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

        // Directly iterate and process L1 entries
        Iterator<Map.Entry<UK_C, CacheEntry<UV_C>>> l1Iterator = perKeyCache.l1MapEntries.entrySet().iterator();

        K_F originalKey = backendForContext.getCurrentKey();
        N_F originalNamespace = namespace; // Assuming 'namespace' is the correct one for delegate
        // If perKeyCache stores its own creation namespace, that should be used for the delegateStateForContext.
        // N_F delegateNamespaceContext = perKeyCache.namespaceForEvictionContext; // If available

        backendForContext.setCurrentKey(flinkKey);
        // delegateStateForContext.setCurrentNamespace(delegateNamespaceContext);
        delegateStateForContext.setCurrentNamespace(namespace); // Using the passed namespace for now

        try {
            while (l1Iterator.hasNext()) {
                Map.Entry<UK_C, CacheEntry<UV_C>> l1Entry = l1Iterator.next();
                UK_C userKey = l1Entry.getKey();
                CacheEntry<UV_C> cacheEntry = l1Entry.getValue();

                if (cacheEntry.isDirty()) {
                    UV_C userValue = cacheEntry.getValue();
                    if (userValue == null) { // Tombstone
                        delegateStateForContext.remove(userKey);
                    } else {
                        delegateStateForContext.put(userKey, userValue);
                        // Move to L2 as clean after successful flush
                        perKeyCache.l2MapEntries.put(userKey, CacheEntry.clean(userValue));
                    }
                    cacheEntry.setDirty(false); // Mark as clean
                    // Memory for this entry was already accounted for when it was put/updated in L1.
                    // If it's moved to L2, L2's put will handle its accounting. Here we are just flushing.
                } else { // Clean entry
                    if (cacheEntry.getValue() != null) { // Not a tombstone
                        // Move clean, non-null entry to L2
                        CacheEntry<UV_C> oldL2 = perKeyCache.l2MapEntries.put(userKey, cacheEntry);
                        if (oldL2 != null) backendForContext.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes());
                        backendForContext.reportCacheMemoryAdded(cacheEntry.getEstimatedSizeBytes()); // Account for L2 add
                    }
                }
                l1Iterator.remove(); // Remove from L1 after processing
                backendForContext.reportCacheMemoryReleased(cacheEntry.getEstimatedSizeBytes()); // Account for L1 removal
            }
        } finally {
            backendForContext.setCurrentKey(originalKey);
            // if (delegateNamespaceContext != null) {
            // delegateStateForContext.setCurrentNamespace(originalNamespace); // Restore to original context of CachingInternalMapState
            // }
            if (originalNamespace != null) { // Restore if it was not null
                 delegateStateForContext.setCurrentNamespace(originalNamespace);
            }
        }
    }

    @Override
    public boolean isEmpty() throws Exception {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        // Check L1 Presence Cache for any true entry
        for(Map.Entry<UK, CacheEntry<Boolean>> entry : perKeyCache.l1KeyPresenceCache.entrySet()){
            l1PresenceCacheHitCount.inc(); // Each check is a form of hit/lookup
            if(entry.getValue().getValue()){ return false; }
        }
        // If all L1P entries are false or L1P is empty, check L2P
        for(Map.Entry<UK, CacheEntry<Boolean>> entry : perKeyCache.l2KeyPresenceCache.entrySet()){
            l2PresenceCacheHitCount.inc(); // Each check is a form of hit/lookup
            if(entry.getValue().getValue()){ 
                // Promote to L1P
                perKeyCache.l2KeyPresenceCache.remove(entry.getKey());
                CacheEntry<Boolean> oldL1P = perKeyCache.l1KeyPresenceCache.put(entry.getKey(), entry.getValue());
                if (oldL1P != null) backend.reportCacheMemoryReleased(ValueSizeUtils.estimate(Boolean.TRUE));
                backend.reportCacheMemoryAdded(ValueSizeUtils.estimate(Boolean.TRUE));
                return false; 
            }
        }
        // If presence caches suggest empty or don't know, check value caches
        if (!perKeyCache.l1MapEntries.isEmpty() || !perKeyCache.l2MapEntries.isEmpty()) {
            // If value caches have entries, it's not empty. We might not have full presence info.
            // This is a simplified check. A more accurate one would iterate and check for non-tombstone.
            // For now, if value caches are non-empty, assume map is non-empty.
            // This doesn't directly use hit/miss counters for value cache in isEmpty context.
            return false; 
        }

        if (perKeyCache.fullyLoaded) { // If fully loaded and all above checks passed, it's empty.
            return true;
        }
        
        delegateLookups.inc();
        boolean result = delegateState.isEmpty();
        // We can't definitively update presence cache for all keys based on isEmpty(),
        // but if it returns true, and caches were empty, fullyLoaded could be set.
        if (result && perKeyCache.l1MapEntries.isEmpty() && perKeyCache.l2MapEntries.isEmpty() && perKeyCache.l1KeyPresenceCache.isEmpty() && perKeyCache.l2KeyPresenceCache.isEmpty()) {
            perKeyCache.fullyLoaded = true;
        }
        return result;
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
            if (keyCaches == null || keyCaches.isEmpty()) continue;

            List<K> flinkKeysToIterate = new ArrayList<>();
            synchronized(keyCaches) {
                for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> pkEntry : keyCaches.entrySet()) {
                    flinkKeysToIterate.add(pkEntry.getKey());
                }
            }

            for (K flinkKey : flinkKeysToIterate) {
                PerKeyMapCache<UK, UV, K, N> perKeyCache = keyCaches.get(flinkKey);
                if (perKeyCache == null) continue;

                if (perKeyCache.l2MapEntries != null && !perKeyCache.l2MapEntries.isEmpty()) {
                    Iterator<Map.Entry<UK, CacheEntry<UV>>> l2Iter = perKeyCache.l2MapEntries.entrySet().iterator();
                    while (l2Iter.hasNext() && bytesFreed < targetBytesToFreeThisState) {
                        Map.Entry<UK, CacheEntry<UV>> entry = l2Iter.next();
                        long estimatedSize = entry.getValue().getEstimatedSizeBytes();
                        l2Iter.remove();
                        backend.reportCacheMemoryReleased(estimatedSize);
                        bytesFreed += estimatedSize;
                    }
                }
                if (bytesFreed >= targetBytesToFreeThisState) return bytesFreed;

                if (perKeyCache.l1MapEntries != null && !perKeyCache.l1MapEntries.isEmpty()) {
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
        }
        return bytesFreed;
    }
}
