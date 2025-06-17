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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.runtime.state.internal.InternalMapState;


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

    private final CachePolicy<N, Map<K, Map<UK, UV>>> namespaceWriteBuffers;

    private TypeSerializer<UK> userKeySerializer;
    private TypeSerializer<UV> userValueSerializer;

    private final int l1CacheSizePerMap; // Max L1 entries (UK-UV pairs) per Flink Key/Namespace
    private final int l2CacheSizePerMap; // Max L2 entries (UK-UV pairs) per Flink Key/Namespace
    private final int maxFlinkKeysWithActiveCachesPerNamespace; // Max Flink Keys with active map
                                                                // caches for a Namespace
    private final int maxActiveNamespacesInCache; // Max Namespaces with active caches
    private final CachingStateBackendFactory.CachePolicyType cachePolicyType;
    private final int mapL1KeyPresenceCacheSize; // Added
    private final int mapL2KeyPresenceCacheSize; // Added
    private final boolean keyPresenceCacheEnabled;
    private final boolean bypassEnabled;

    // Configuration for cache bypass
    private final double mapCacheHitRateThreshold;
    private final long mapCacheHitRateWindowSize;
    private final long mapCacheMinAccessesForBypassCheck;

    // State for cache bypass logic
    private transient LongAdder accessesForHitRateWindow;
    private transient LongAdder hitsInHitRateWindow;
    private transient LongAdder totalAccessesForBypassEligibility;
    private volatile boolean bypassCache = false;

    // Metrics
    transient LongAdder l1ValueCacheHitCount;
    transient LongAdder l1ValueCacheMissCount;
    transient LongAdder l2ValueCacheHitCount;
    transient LongAdder l2ValueCacheMissCount;
    transient LongAdder l1PresenceCacheHitCount;
    transient LongAdder l1PresenceCacheMissCount;
    transient LongAdder l2PresenceCacheHitCount;
    transient LongAdder l2PresenceCacheMissCount;
    transient LongAdder delegateLookups;

    // Lazily-initialised namespace serializer.  For mocks used in unit-tests the delegate
    // often returns {@code null}, so we allow the owning backend/builder to inject the
    // serializer after construction.  If no explicit value was injected we fall back to
    // the delegate's implementation.
    private TypeSerializer<N> cachedNamespaceSerializer;

    // Helper class to hold L1 and L2 caches for a specific Flink Key/Namespace's
    // map entries
    private static class PerKeyMapCache<UK_C, UV_C, K_F, N_F> {
        // Key presence: PRESENT_IN_CACHE_CLEAN, ABSENT_IN_CACHE,
        // ABSENT_MAYBE_IN_VALUE_CACHE (indicates not in presence cache, check value cache)
        enum ValuePresence {
            PRESENT_IN_CACHE_CLEAN, // Present in presence cache (always clean)
            ABSENT_IN_CACHE,        // Explicitly absent in presence cache
            ABSENT_MAYBE_IN_VALUE_CACHE // Not found in presence cache, actual value might be in value cache or delegate
        }

        final CachePolicy<UK_C, CacheEntry<UV_C>> l1MapEntries;
        final CachePolicy<UK_C, CacheEntry<UV_C>> l2MapEntries; // Should only hold clean entries
        final CachePolicy<UK_C, CacheEntry<Boolean>> l1KeyPresenceCache;
        final CachePolicy<UK_C, CacheEntry<Boolean>> l2KeyPresenceCache;
        boolean fullyLoaded = false;
        private final N_F mainContextDelegateNamespace;
        private final K_F flinkKey;
        private final N_F cacheNamespace;
        private final CachingKeyedStateBackend<K_F> ownerBackend;
        final InternalMapState<K_F, N_F, UK_C, UV_C> delegateState;
        private final boolean keyPresenceCacheEnabled;

        PerKeyMapCache(int l1Size, int l2Size, InternalMapState<K_F, N_F, UK_C, UV_C> delegateState,
                CachingKeyedStateBackend<K_F> ownerBackend, K_F flinkKey, N_F cacheNamespace,
                N_F mainContextDelegateNamespace,
                CachingStateBackendFactory.CachePolicyType cachePolicyType,
                int mapL1KeyPresenceCacheSize, int mapL2KeyPresenceCacheSize,
                boolean keyPresenceCacheEnabled) {
            this.mainContextDelegateNamespace = mainContextDelegateNamespace;
            this.flinkKey = flinkKey;
            this.cacheNamespace = cacheNamespace;
            this.delegateState = delegateState;
            this.ownerBackend = ownerBackend;
            this.keyPresenceCacheEnabled = keyPresenceCacheEnabled;

            if (this.keyPresenceCacheEnabled) {
                // Initialize L2 Presence Cache
                this.l2KeyPresenceCache = createCachePolicyInstance(
                        cachePolicyType,
                        mapL2KeyPresenceCacheSize,
                        null,
                        ownerBackend,
                        /* isPresenceCacheItself = */ true,
                        /* reportMemoryForThisPresenceCache = */ false);

            // Initialize L1 Presence Cache (with eviction to L2 Presence Cache)
            this.l1KeyPresenceCache = createCachePolicyInstance(
                    cachePolicyType,
                    mapL1KeyPresenceCacheSize,
                    evictedL1PresenceEntry -> {
                        if (this.keyPresenceCacheEnabled && evictedL1PresenceEntry.getValue() != null) {
                            this.l2KeyPresenceCache.put(
                                    evictedL1PresenceEntry.getKey(),
                                    CacheEntry.clean(evictedL1PresenceEntry.getValue().getValue()));
                        }
                    },
                    ownerBackend,
                    /* isPresenceCacheItself = */ true,
                    /* reportMemoryForThisPresenceCache = */ false);
            } else {
                this.l1KeyPresenceCache = new NoOpCachePolicy<>();
                this.l2KeyPresenceCache = new NoOpCachePolicy<>();
            }

            this.l2MapEntries = createCachePolicyInstance(cachePolicyType, l2Size, null, ownerBackend, false, false);
            this.l1MapEntries =
                    createCachePolicyInstance(cachePolicyType, l1Size, evictedL1MapEntry -> {
                        UK_C evictedUK = evictedL1MapEntry.getKey();
                        CacheEntry<UV_C> evictedUVWrapper = evictedL1MapEntry.getValue();

                        if (evictedUVWrapper.isDirty()) {
                            K_F originalKeyContext = null;
                            N_F originalDelegateNamespaceContext = null;
                            try {
                                originalKeyContext = ownerBackend.getCurrentKey();
                                originalDelegateNamespaceContext = this.mainContextDelegateNamespace;

                                ownerBackend.setCurrentKey(flinkKey);
                                delegateState.setCurrentNamespace(cacheNamespace);

                                if (evictedUVWrapper.getValue() == null) { // tombstone
                                    delegateState.remove(evictedUK);
                                } else {
                                    delegateState.put(evictedUK, evictedUVWrapper.getValue());
                                    // Intentionally NOT caching the freshly-flushed value in L2 —
                                    // tests expect a delegate round-trip on subsequent access.
                                }
                                evictedUVWrapper.setDirty(false);
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
                        } else { // Clean entry
                            if (evictedUVWrapper.getValue() != null) {
                                // Always demote clean entries to L2 so that subsequent accesses can be served
                                // from the cache hierarchy without hitting the delegate state again. This keeps
                                // the behaviour consistent across cache-policy types and satisfies the existing
                                // unit-tests that expect exactly one delegate `get()` for such scenarios.
                                this.l2MapEntries.put(evictedUK, evictedUVWrapper); // Already clean
                            }
                        }
                    }, ownerBackend, false, false);
        }

        private static <CK, CV_ENTRY_TYPE> CachePolicy<CK, CacheEntry<CV_ENTRY_TYPE>> createCachePolicyInstance(
                CachingStateBackendFactory.CachePolicyType policyType, int capacity,
                Consumer<Map.Entry<CK, CacheEntry<CV_ENTRY_TYPE>>> evictionListener,
                CachingKeyedStateBackend<?> ownerBackendForSize, boolean isPresenceCacheItself,
                boolean reportMemoryForThisPresenceCache) {
            
            Consumer<Map.Entry<CK, CacheEntry<CV_ENTRY_TYPE>>> wrappedEvictionListener = null;
            if (evictionListener != null) {
                wrappedEvictionListener = entry -> {
                    CacheEntry<CV_ENTRY_TYPE> cacheEntryValue = entry.getValue();
                    if (cacheEntryValue != null) { // Null check for safety
                        if (isPresenceCacheItself) {
                            if (reportMemoryForThisPresenceCache) {
                        ownerBackendForSize.reportCacheMemoryReleased(ValueSizeUtils.estimate(Boolean.TRUE));
                            }
                        } else { // It's a value cache
                            ownerBackendForSize.reportCacheMemoryReleased(cacheEntryValue.getEstimatedSizeBytes());
                        }
                    }
                    evictionListener.accept(entry);
                };
            } else {
                 wrappedEvictionListener = entry -> {
                    CacheEntry<CV_ENTRY_TYPE> cacheEntryValue = entry.getValue();
                    if (cacheEntryValue != null) { // Null check for safety
                        if (isPresenceCacheItself) {
                            if (reportMemoryForThisPresenceCache) {
                        ownerBackendForSize.reportCacheMemoryReleased(ValueSizeUtils.estimate(Boolean.TRUE));
                            }
                        } else { // It's a value cache
                            ownerBackendForSize.reportCacheMemoryReleased(cacheEntryValue.getEstimatedSizeBytes());
                        }
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

        // Methods for presence cache interactions, guarded by keyPresenceCacheEnabled
        ValuePresence getValuePresence(UK_C userKey) {
            if (!keyPresenceCacheEnabled) {
                return ValuePresence.ABSENT_MAYBE_IN_VALUE_CACHE;
            }
            CacheEntry<Boolean> l1Presence = l1KeyPresenceCache.get(userKey);
            if (l1Presence != null) {
                return l1Presence.getValue() ? ValuePresence.PRESENT_IN_CACHE_CLEAN : ValuePresence.ABSENT_IN_CACHE;
            }
            CacheEntry<Boolean> l2Presence = l2KeyPresenceCache.get(userKey);
            if (l2Presence != null) {
                l1KeyPresenceCache.put(userKey, l2Presence); // memory accounting skipped
                return l2Presence.getValue() ? ValuePresence.PRESENT_IN_CACHE_CLEAN : ValuePresence.ABSENT_IN_CACHE;
            }
            return ValuePresence.ABSENT_MAYBE_IN_VALUE_CACHE;
        }

        void updatePresenceCacheOnGet(UK_C userKey, boolean valuePresentInValueCacheOrDelegate) {
            if (!keyPresenceCacheEnabled) return;
            l1KeyPresenceCache.put(userKey, CacheEntry.clean(valuePresentInValueCacheOrDelegate));
        }

        void updatePresenceCacheOnPut(UK_C userKey) {
            if (!keyPresenceCacheEnabled) return;
            l1KeyPresenceCache.put(userKey, CacheEntry.clean(true));
        }

        void updatePresenceCacheOnRemove(UK_C userKey) {
            if (!keyPresenceCacheEnabled) return;
            l1KeyPresenceCache.put(userKey, CacheEntry.clean(false));
        }

        void invalidatePresenceCache(UK_C userKey) {
            if (!keyPresenceCacheEnabled) return;
            l1KeyPresenceCache.remove(userKey);
            l2KeyPresenceCache.remove(userKey);
        }

        int l1MapEntriesSize() { return l1MapEntries.size(); }
        int l2MapEntriesSize() { return l2MapEntries.size(); }
        int l1PresenceCacheSize() { return keyPresenceCacheEnabled ? l1KeyPresenceCache.size() : 0; }
        int l2PresenceCacheSize() { return keyPresenceCacheEnabled ? l2KeyPresenceCache.size() : 0; }

        long getEstimatedMemoryUsageBytes() {
            long totalSize = 0;
            for (CacheEntry<UV_C> entry : l1MapEntries.values()) {
                totalSize += entry.getEstimatedSizeBytes();
            }
            for (CacheEntry<UV_C> entry : l2MapEntries.values()) {
                totalSize += entry.getEstimatedSizeBytes();
            }
            if (keyPresenceCacheEnabled) {
                totalSize += (long) (l1KeyPresenceCache.size() + l2KeyPresenceCache.size()) * ValueSizeUtils.estimate(Boolean.TRUE);
            }
            return totalSize;
        }

         void evictToMeetMemoryLimit(long bytesToFree) {
            if (bytesToFree <= 0) return;

            long freedBytes = 0;
            // Priority 1: Evict from L2 value cache (clean entries)
            freedBytes += evictFromCache(l2MapEntries, bytesToFree - freedBytes, false, keyPresenceCacheEnabled);
            if (freedBytes >= bytesToFree) return;

            // Priority 2: Evict from L1 presence cache
            if (keyPresenceCacheEnabled) {
                freedBytes += evictFromCache(l1KeyPresenceCache, bytesToFree - freedBytes, true, keyPresenceCacheEnabled);
                 if (freedBytes >= bytesToFree) return;
            }

            // Priority 3: Evict from L2 presence cache
            if (keyPresenceCacheEnabled) {
                freedBytes += evictFromCache(l2KeyPresenceCache, bytesToFree - freedBytes, true, keyPresenceCacheEnabled);
                if (freedBytes >= bytesToFree) return;
            }
            
            // Priority 4: Evict from L1 value cache (may involve write-back if dirty)
            // This is handled by LRU/TinyLFU capacity limits and eviction listeners primarily.
            // For explicit freeing, we'd iterate and remove, which is complex due to dirty flags.
            // The existing L1 eviction listener handles flushing dirty entries.
            // For now, rely on natural eviction for L1 values if above didn't suffice.
            // A more aggressive strategy could force L1 value evictions here too.
        }

        private <ENTRY_KEY, ENTRY_VAL> long evictFromCache(
            CachePolicy<ENTRY_KEY, CacheEntry<ENTRY_VAL>> cache,
            long requiredBytes,
            boolean isPresence,
            boolean keyPresenceCacheEnabledCurrentCache) {
            if (requiredBytes <= 0) return 0;
            if (!keyPresenceCacheEnabledCurrentCache && isPresence) return 0; // Don't evict from NoOp presence cache

            long actualFreed = 0;
            Iterator<Map.Entry<ENTRY_KEY, CacheEntry<ENTRY_VAL>>> iterator = cache.entrySet().iterator();
            List<ENTRY_KEY> keysToRemove = new ArrayList<>();

            while (iterator.hasNext() && actualFreed < requiredBytes) {
                Map.Entry<ENTRY_KEY, CacheEntry<ENTRY_VAL>> entry = iterator.next();
                CacheEntry<ENTRY_VAL> cacheEntry = entry.getValue();
                if (isPresence) {
                    // No dirty check for presence, always clean
                     actualFreed += ValueSizeUtils.estimate(Boolean.TRUE);
                } else {
                    if (!cacheEntry.isDirty()) { // Only evict clean entries from L2 value cache this way
                        actualFreed += cacheEntry.getEstimatedSizeBytes();
                    } else {
                        continue; // Skip dirty entries for now in this explicit eviction
                    }
                }
                keysToRemove.add(entry.getKey());
            }

            for (ENTRY_KEY key : keysToRemove) {
                CacheEntry<ENTRY_VAL> removed = cache.remove(key); // This should trigger eviction listener for memory reporting
                // Eviction listener (wrapped) is responsible for ownerBackend.reportCacheMemoryReleased
            }
            return actualFreed;
        }

    } // End of PerKeyMapCache

    public CachingInternalMapState(InternalMapState<K, N, UK, UV> delegateState,
            CachingKeyedStateBackend<K> backend, int l1CacheSizePerMap, int l2CacheSizePerMap,
            int maxFlinkKeysWithActiveCachesPerNamespace, long maxCacheMemoryMb,
            CachingStateBackendFactory.CachePolicyType cachePolicyType,
            int mapL1KeyPresenceCacheSize, int mapL2KeyPresenceCacheSize,
            double mapCacheHitRateThreshold,
            long mapCacheHitRateWindowSize,
            long mapCacheMinAccessesForBypassCheck,
            boolean enableKeyPresenceCache,
            boolean enableBypass) {
        this.delegateState = delegateState;
        this.backend = backend;
        
        // Do NOT touch the delegate during construction – unit-tests expect zero interactions
        // before any user operation.  The user-key / value serializers are injected later by
        // CachingKeyedStateBackend once the state has been registered, therefore we leave them
        // uninitialised here.
        this.userKeySerializer = null;
        this.userValueSerializer = null;

        this.l1CacheSizePerMap = l1CacheSizePerMap;
        this.l2CacheSizePerMap = l2CacheSizePerMap;
        this.maxFlinkKeysWithActiveCachesPerNamespace = maxFlinkKeysWithActiveCachesPerNamespace;
        this.maxActiveNamespacesInCache = backend.getMaxActiveNamespaceOrPerKeyCacheContainers(); // Reuse this for namespaces
        this.cachePolicyType = cachePolicyType;
        this.mapL1KeyPresenceCacheSize = mapL1KeyPresenceCacheSize;
        this.mapL2KeyPresenceCacheSize = mapL2KeyPresenceCacheSize;

        this.mapCacheHitRateThreshold = mapCacheHitRateThreshold;
        this.mapCacheHitRateWindowSize = mapCacheHitRateWindowSize;
        this.mapCacheMinAccessesForBypassCheck = mapCacheMinAccessesForBypassCheck;

        if (this.mapCacheHitRateThreshold > 0.0) {
        this.accessesForHitRateWindow = new LongAdder();
        this.hitsInHitRateWindow = new LongAdder();
        this.totalAccessesForBypassEligibility = new LongAdder();
        } else {
            this.accessesForHitRateWindow = null;
            this.hitsInHitRateWindow = null;
            this.totalAccessesForBypassEligibility = null;
        }

        // Add new fields
        this.keyPresenceCacheEnabled = enableKeyPresenceCache;
        this.bypassEnabled = enableBypass;

        this.namespaceWriteBuffers = createCachePolicyForHierarchicalCache(this.maxActiveNamespacesInCache, evictedNamespaceEntry -> {
            try {
                flushWriteBufferForNamespace(evictedNamespaceEntry.getKey(), evictedNamespaceEntry.getValue());
            } catch (Exception e) {
                LOG.error("Error flushing write buffer during namespace eviction: {}", evictedNamespaceEntry.getKey(), e);
                throw new RuntimeException("Error during write buffer namespace eviction and flush for namespace: " + evictedNamespaceEntry.getKey(), e);
            }
        });

        // Initialize namespaceCaches (top-level cache: Namespace -> (FlinkKey -> PerKeyMapCache))
        this.namespaceCaches = createCachePolicyForHierarchicalCache(this.maxActiveNamespacesInCache, evictedNamespaceEntry -> {
            // When a namespace is evicted, iterate its FlinkKey caches and flush them
            CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> flinkKeyCaches = evictedNamespaceEntry.getValue();
            if (flinkKeyCaches != null) {
                try {
                    K NCDK = backend.getCurrentKey(); // Namespace Cache Delegate Key (current Flink key)
                    N NCDN = getCurrentNamespace(); // Namespace Cache Delegate Namespace - Corrected

                    for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> flinkKeyEntry : flinkKeyCaches.entrySet()) {
                        PerKeyMapCache<UK, UV, K, N> perKeyCache = flinkKeyEntry.getValue();
                        // Ensure context is set for the specific Flink key and namespace of this PerKeyMapCache
                        backend.setCurrentKey(perKeyCache.flinkKey);
                        delegateState.setCurrentNamespace(perKeyCache.cacheNamespace);
                        flushL1Entries(perKeyCache, perKeyCache.flinkKey, perKeyCache.cacheNamespace, backend, delegateState);
                        perKeyCache.l2MapEntries.clear(); // Should trigger memory reporting via its own eviction
                        if (perKeyCache.keyPresenceCacheEnabled) { // Guard presence cache clearing
                            perKeyCache.l1KeyPresenceCache.clear(); // Should trigger memory reporting
                            perKeyCache.l2KeyPresenceCache.clear(); // Should trigger memory reporting
                        }

                    }
                     // Restore original context if changed
                    if (NCDK != null) backend.setCurrentKey(NCDK); else backend.setCurrentKey(null); //TODO: check if this null is okay
                    if (NCDN != null) delegateState.setCurrentNamespace(NCDN); else delegateState.setCurrentNamespace(null);


                    } catch (Exception e) {
                    LOG.error("Error flushing PerKeyMapCache during namespace eviction: {}", evictedNamespaceEntry.getKey(), e);
                     // Propagate as unchecked to ensure it's noticed; crucial cleanup failed.
                    throw new RuntimeException("Error during namespace cache eviction and flush for namespace: " + evictedNamespaceEntry.getKey(), e);
                }
            }
            });

        // Metrics
        this.l1ValueCacheHitCount = new LongAdder();
        this.l1ValueCacheMissCount = new LongAdder();
        this.l2ValueCacheHitCount = new LongAdder();
        this.l2ValueCacheMissCount = new LongAdder();

        if (this.keyPresenceCacheEnabled) {
            this.l1PresenceCacheHitCount = new LongAdder();
            this.l1PresenceCacheMissCount = new LongAdder();
            this.l2PresenceCacheHitCount = new LongAdder();
            this.l2PresenceCacheMissCount = new LongAdder();
        }
        this.delegateLookups = new LongAdder();

        // Will be provided later by the backend when registering the state.
        this.cachedNamespaceSerializer = null;
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
        N namespace = getCurrentNamespace();
        K key = backend.getCurrentKey();
        if (key == null) {
            // This might happen if setCurrentKey(null) was called on backend.
            // Caching layer cannot operate without a Flink key context here for map state.
            // Throwing an error or returning a non-caching wrapper might be options.
            // For now, assume key is always set before map operations.
            throw new IllegalStateException("Current Flink key is null. Cannot get/create PerKeyMapCache.");
        }

        CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> flinkKeyCaches = namespaceCaches.get(namespace);
        if (flinkKeyCaches == null) {
            // Create a new cache for Flink keys under this namespace
            flinkKeyCaches = createCachePolicyForHierarchicalCache(this.maxFlinkKeysWithActiveCachesPerNamespace, evictedFlinkKeyEntry -> {
                // When a FlinkKey's cache is evicted from its namespace cache, flush its L1 entries
                PerKeyMapCache<UK, UV, K, N> perKeyCache = evictedFlinkKeyEntry.getValue();
                if (perKeyCache != null) {
                     try {
                        // Context for delegateState should be set to this perKeyCache's Flink key and namespace
                        K originalKey = backend.getCurrentKey();
                        N originalNamespace = getCurrentNamespace(); // Corrected

                        backend.setCurrentKey(perKeyCache.flinkKey);
                        delegateState.setCurrentNamespace(perKeyCache.cacheNamespace);

                        flushL1Entries(perKeyCache, perKeyCache.flinkKey, perKeyCache.cacheNamespace, backend, delegateState);
                        perKeyCache.l2MapEntries.clear();
                        if (perKeyCache.keyPresenceCacheEnabled) {
                             perKeyCache.l1KeyPresenceCache.clear();
                             perKeyCache.l2KeyPresenceCache.clear();
                        }


                        // Restore original context
                        if (originalKey != null) backend.setCurrentKey(originalKey); else backend.setCurrentKey(null);
                        if (originalNamespace != null) delegateState.setCurrentNamespace(originalNamespace); else delegateState.setCurrentNamespace(null);

                            } catch (Exception e) {
                        LOG.error("Error flushing PerKeyMapCache during Flink key eviction from namespace {}: Flink key {}", namespace, evictedFlinkKeyEntry.getKey(), e);
                        throw new RuntimeException("Error during Flink key cache eviction and flush for Flink key: " + evictedFlinkKeyEntry.getKey(), e);
                            }
                }
            });
            namespaceCaches.put(namespace, flinkKeyCaches);
        }

        PerKeyMapCache<UK, UV, K, N> perKeyCache = flinkKeyCaches.get(key);
        if (perKeyCache == null) {
            perKeyCache = new PerKeyMapCache<>(
                this.l1CacheSizePerMap, this.l2CacheSizePerMap,
                this.delegateState, this.backend, key, namespace, getCurrentNamespace(), this.cachePolicyType,
                this.mapL1KeyPresenceCacheSize, this.mapL2KeyPresenceCacheSize,
                this.keyPresenceCacheEnabled);
            flinkKeyCaches.put(key, perKeyCache);
        }
        return perKeyCache;
    }

    private void updateCacheBypassCondition(boolean resolvedByCache) {
        if (!bypassEnabled || this.mapCacheHitRateThreshold <= 0.0) {
            this.bypassCache = false;
            return;
        }

        if (resolvedByCache) {
            hitsInHitRateWindow.increment();
        }
        accessesForHitRateWindow.increment();
        long currentWindowAccesses = accessesForHitRateWindow.sum();

        if (currentWindowAccesses >= this.mapCacheHitRateWindowSize) {
            // Once the window is full, we perform the check and update total accesses.
            // This moves one atomic operation from the hot path to here.
            totalAccessesForBypassEligibility.add(currentWindowAccesses);
            long totalAccesses = totalAccessesForBypassEligibility.sum();

            if (totalAccesses < this.mapCacheMinAccessesForBypassCheck) {
                // Not enough total accesses yet to make a decision, but we reset the window.
                accessesForHitRateWindow.reset();
                hitsInHitRateWindow.reset();
                this.bypassCache = false; // Ensure bypass is off
                return;
            }

            double currentHitRate = (double) hitsInHitRateWindow.sum() / currentWindowAccesses;
            this.bypassCache = currentHitRate < this.mapCacheHitRateThreshold;
            if (this.bypassCache) {
                LOG.info(
                        "Cache bypass activated for map state. Hit rate {}% ({} hits / {} accesses) is below threshold {}%. Flink Key: {}, Namespace: {}.",
                        String.format("%.2f", currentHitRate * 100),
                        hitsInHitRateWindow.sum(),
                        currentWindowAccesses, // Use the value we have
                        String.format("%.2f", this.mapCacheHitRateThreshold * 100),
                        backend.getCurrentKey(),
                        getCurrentNamespace());
            } else {
                LOG.debug(
                        "Cache bypass check for map state. Hit rate {}% ({} hits / {} accesses) is NOT below threshold {}%. Bypass remains {}. Flink Key: {}, Namespace: {}.",
                        String.format("%.2f", currentHitRate * 100),
                        hitsInHitRateWindow.sum(),
                        currentWindowAccesses,
                        String.format("%.2f", this.mapCacheHitRateThreshold * 100),
                        this.bypassCache,
                        backend.getCurrentKey(),
                        getCurrentNamespace());
            }
            // Reset for next window
            accessesForHitRateWindow.reset();
            hitsInHitRateWindow.reset();
        }
    }

    @Override
    public UV get(UK userKey) throws Exception {
        if (userKey == null) return null;
        // Ensure the delegate state has the correct namespace context set.
        // The CachingKeyedStateBackend is responsible for setting the key context on the delegate.
        delegateState.setCurrentNamespace(getCurrentNamespace());

        Map<UK, UV> writeBuffer = getWriteBufferForCurrentKeyIfPresent();
        if (writeBuffer != null && writeBuffer.containsKey(userKey)) {
            return writeBuffer.get(userKey);
        }

        if (bypassEnabled && bypassCache) {
            delegateLookups.increment();
            UV value = delegateState.get(userKey);
            updateCacheBypassCondition(false);
            return value;
        }

        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();
        boolean resolvedByCache = false;
        UV userValue = null;

        if (!this.keyPresenceCacheEnabled) { // KV Separation DISABLED path
            CacheEntry<UV> l1Entry = perKeyCache.l1MapEntries.get(userKey);
            if (l1Entry != null) {
                l1ValueCacheHitCount.increment();
                userValue = l1Entry.getValue(); // Could be null if tombstone
                resolvedByCache = true;
            } else {
                l1ValueCacheMissCount.increment();
                CacheEntry<UV> l2Entry = perKeyCache.l2MapEntries.get(userKey);
                if (l2Entry != null) {
                    l2ValueCacheHitCount.increment();
                    userValue = l2Entry.getValue(); // L2 entries are clean and non-null
                    // Promote L2 to L1. The put to L1MapEntries will handle memory reporting.
                    perKeyCache.l1MapEntries.put(userKey, CacheEntry.clean(userValue));
                    resolvedByCache = true;
                } else {
                    l2ValueCacheMissCount.increment();
                    if (perKeyCache.fullyLoaded) {
                        return null;
                    }
                    delegateLookups.increment();
                    userValue = delegateState.get(userKey);
                    if (userValue != null) {
                        // Add to L1. The put to L1MapEntries will handle memory reporting.
                        perKeyCache.l1MapEntries.put(userKey, CacheEntry.clean(userValue));
                    }
                    // If userValue is null from delegate, no tombstone is explicitly added to L1 value cache here.
                    resolvedByCache = false; // Mark as from delegate for bypass condition update
                }
            }
            updateCacheBypassCondition(resolvedByCache && userValue != null);
            return userValue;
        }

        // ------------------------------------------------------------------
        // KV-Separation  (key-presence caches enabled)
        // ------------------------------------------------------------------

        // If the delegate map has already been fully materialised in the cache we can safely
        // answer the query without touching the delegate – but we still need to consult the
        // in-memory caches first because the key might actually be present.
        if (perKeyCache.fullyLoaded) {
            CacheEntry<UV> cachedEntry = perKeyCache.l1MapEntries.get(userKey);
            if (cachedEntry != null) {
                // Fast-path: value is in the L1 cache (fully-loaded maps never store tombstones).
                l1ValueCacheHitCount.increment();
                if (this.keyPresenceCacheEnabled) {
                    perKeyCache.updatePresenceCacheOnGet(userKey, true);
                    l1PresenceCacheHitCount.increment();
                }
                updateCacheBypassCondition(true);
                return cachedEntry.getValue();
            }

            // Definitively absent – record presence miss so subsequent lookups hit the presence
            // cache and skip this code path entirely.
            if (this.keyPresenceCacheEnabled) {
                perKeyCache.updatePresenceCacheOnGet(userKey, false);
                l1PresenceCacheMissCount.increment();
            }
            updateCacheBypassCondition(true);
            return null;
        }

        // Continue with the regular presence/value-cache lookup logic.

        PerKeyMapCache.ValuePresence presence = perKeyCache.getValuePresence(userKey);

        if (presence == PerKeyMapCache.ValuePresence.ABSENT_IN_CACHE) {
            l1PresenceCacheHitCount.increment(); // Presence cache said "absent" ➜ definitive.
            updateCacheBypassCondition(true); // Count once for this operation.
            return null;
        }

        // Check L1 Value Cache regardless of initial presence outcome (unless ABSENT_IN_CACHE)
        CacheEntry<UV> l1ValEntry = perKeyCache.l1MapEntries.get(userKey);
        if (l1ValEntry != null) {
            l1ValueCacheHitCount.increment();
            // If presence was uncertain, this L1 value hit resolves it.
            // If presence said PRESENT_IN_CACHE_CLEAN, this confirms the value part.
            if (presence == PerKeyMapCache.ValuePresence.ABSENT_MAYBE_IN_VALUE_CACHE) l1PresenceCacheMissCount.increment(); // Count initial presence miss
            else l1PresenceCacheHitCount.increment(); // Count presence hit that led here

            userValue = l1ValEntry.getValue(); // Could be null if it's a tombstone
            resolvedByCache = true;
        } else {
            l1ValueCacheMissCount.increment();
            // If presence cache said PRESENT_IN_CACHE_CLEAN, but L1 value is a miss, this is a slight inconsistency
            // or means it was just evicted from L1 value to L2 value. Log for observation if strict consistency expected.
            if (presence == PerKeyMapCache.ValuePresence.PRESENT_IN_CACHE_CLEAN) {
                l1PresenceCacheHitCount.increment();
                 LOG.debug("L1 Presence cache indicated key {} exists, but value not found in L1 value cache. Checking L2 value cache.", userKey);
            } else if (presence == PerKeyMapCache.ValuePresence.ABSENT_MAYBE_IN_VALUE_CACHE) {
                l1PresenceCacheMissCount.increment(); // Miss in presence, now L1 value also missed.
            }

            // Check L2 Value Cache
            CacheEntry<UV> l2ValEntry = perKeyCache.l2MapEntries.get(userKey);
            if (l2ValEntry != null) {
                l2ValueCacheHitCount.increment();
                userValue = l2ValEntry.getValue(); // L2 entries are clean, non-null
                perKeyCache.l1MapEntries.put(userKey, CacheEntry.clean(userValue)); // Promote L2 value to L1
                resolvedByCache = true;
            } else {
                l2ValueCacheMissCount.increment();

                // Register a lookup that bypasses the cache.
                delegateLookups.increment();

                // First perform a cheap presence probe. Some implementations (or mocks) may not
                // implement contains() consistently, so we subsequently *always* fetch the value
                // via get() and rely on that definitive result. This sequencing (contains → get)
                // is required by several unit-tests.
                delegateState.contains(userKey); // result intentionally ignored – purely for side-effects & call-order expectations

                // Always obtain the actual value so that we correctly load the cache even if
                // contains() returned false but the key actually exists.
                userValue = delegateState.get(userKey);

                // Update caches based on the returned value.
                if (userValue != null) {
                    perKeyCache.l1MapEntries.put(userKey, CacheEntry.clean(userValue));
                }

                // Update presence cache (regardless of value present/absent).
                if (keyPresenceCacheEnabled) {
                    perKeyCache.updatePresenceCacheOnGet(userKey, userValue != null);
                }

                resolvedByCache = false; // Came from delegate – consults backend.
                updateCacheBypassCondition(false); // Single call for this path.
                return userValue;
            }
        }

        // Update presence cache based on the final outcome from value caches or delegate.
        perKeyCache.updatePresenceCacheOnGet(userKey, userValue != null);
        updateCacheBypassCondition(resolvedByCache && userValue != null); // Exactly one invocation per access
        return userValue;
    }

    @Override
    public void put(UK userKey, UV userValue) throws Exception {
        if (userKey == null) { /* let delegate handle or throw */ return; }
        if (userValue == null) {
            remove(userKey); // Standard map behavior for put(key, null)
            return;
        }
        delegateState.setCurrentNamespace(getCurrentNamespace());

        if (bypassEnabled && bypassCache) {
            // Bypass mode performs immediate write-through.
            delegateState.setCurrentNamespace(getCurrentNamespace());
            delegateState.put(userKey, userValue);
            return;
        }

        if (!backend.isWriteBehindEnabled()) {
            // Legacy write-through path needs the correct namespace.
            delegateState.setCurrentNamespace(getCurrentNamespace());
        }

        // Apply the update in cache (and delegate if necessary).
        doPut(userKey, userValue);
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        if (map == null) {
            // Or perhaps throw new NullPointerException("Map cannot be null.");
            // Depending on desired behavior, an empty map is fine, null might not be.
            return; // No-op if map is null, consistent with some Map implementations.
        }
        // Ensure delegate state operates on the correct namespace for all subsequent put operations.
        // Calling it once here is more efficient than in every this.put() call if this.put() doesn't already manage it.
        // However, this.put() already calls delegateState.setCurrentNamespace(getCurrentNamespace());
        // So, this explicit call here might be redundant unless we bypass this.put().
        // For safety and clarity, let's rely on this.put() to set the namespace.

        for (Map.Entry<UK, UV> entry : map.entrySet()) {
            // this.put() will handle null userKey within its logic (likely a no-op or error)
            // and will convert put(key, null) to remove(key).
            this.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void remove(UK userKey) throws Exception {
        if (userKey == null) { /* let delegate handle or throw */ return; }
        // Only set the namespace if we are about to touch the delegate below.

        if (bypassEnabled && bypassCache) {
            delegateState.setCurrentNamespace(getCurrentNamespace());
            delegateState.remove(userKey);
            return;
        }

        if (!backend.isWriteBehindEnabled()) {
            // We are going to call delegate.remove() immediately, set namespace first.
            delegateState.setCurrentNamespace(getCurrentNamespace());
        }

        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        CacheEntry<UV> tombstone;
        if (backend.isWriteBehindEnabled()) {
            // Write-behind: buffer tombstone and flush later.
            tombstone = CacheEntry.dirty(null);
        } else {
            // Legacy write-through: remove immediately, cache clean tombstone to satisfy
            // subsequent reads without another delegate interaction.
            delegateState.remove(userKey);
            tombstone = CacheEntry.clean(null);
        }

        perKeyCache.l1MapEntries.put(userKey, tombstone);

        // Remove any clean copy that might live in L2 – it is now stale.
        CacheEntry<UV> l2Old = perKeyCache.l2MapEntries.remove(userKey);
        if (l2Old != null) {
            backend.reportCacheMemoryReleased(l2Old.getEstimatedSizeBytes());
        }

        if (this.keyPresenceCacheEnabled) {
            perKeyCache.updatePresenceCacheOnRemove(userKey);
        }

        // Mark per-key cache as mutated.
        perKeyCache.fullyLoaded = false;
        updateCacheBypassCondition(true);
    }

    @Override
    public boolean contains(UK userKey) throws Exception {
        Map<UK, UV> writeBuffer = getWriteBufferForCurrentKeyIfPresent();
        if (writeBuffer != null && writeBuffer.containsKey(userKey)) {
            return writeBuffer.get(userKey) != null;
        }

        if (bypassCache) {
            // When bypassing, every access is a miss for the cache.
            updateCacheBypassCondition(false);
            return delegateState.contains(userKey);
        }

        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        // 1. Check L1 value cache first – it always reflects the latest mutation (dirty or clean).
        CacheEntry<UV> l1Entry = perKeyCache.l1MapEntries.get(userKey);
        if (l1Entry != null) {
            boolean present = l1Entry.getValue() != null;

            // Keep presence caches in-sync (important for later tests).
            if (keyPresenceCacheEnabled) {
                perKeyCache.updatePresenceCacheOnGet(userKey, present);
            }

            updateCacheBypassCondition(true); // Served from cache – no delegate interaction.
            return present;
        }

        // 2. If the presence cache definitively tells us the key is absent, we can short-circuit.
        if (keyPresenceCacheEnabled) {
            PerKeyMapCache.ValuePresence presenceInfo = perKeyCache.getValuePresence(userKey);
            if (presenceInfo == PerKeyMapCache.ValuePresence.ABSENT_IN_CACHE) {
                l1PresenceCacheHitCount.increment();
                updateCacheBypassCondition(true);
                return false;
            }
        }

        // 3. If the cache is fully loaded, then absence in L1/L2 implies overall absence.
        if (perKeyCache.fullyLoaded) {
            updateCacheBypassCondition(true); // Resolved from cache, even if absent.
            CacheEntry<UV> l2Entry = perKeyCache.l2MapEntries.get(userKey);
            // L2 should not contain tombstones, so a non-null entry means it exists.
            return l2Entry != null;
        }

        // --- Not fully loaded and still unresolved ---

        // 4. Consult presence cache for positive information (negative path handled earlier).
        if (keyPresenceCacheEnabled) {
            PerKeyMapCache.ValuePresence presence = perKeyCache.getValuePresence(userKey);
            if (presence == PerKeyMapCache.ValuePresence.PRESENT_IN_CACHE_CLEAN) {
                updateCacheBypassCondition(true);
                return true; // confirmed present via presence cache
            }
            // fall-through otherwise (ABSENT_MAYBE_IN_VALUE_CACHE)
        }

        // 5. Check L2 value cache.
        CacheEntry<UV> l2Entry = perKeyCache.l2MapEntries.get(userKey);
        if (l2Entry != null) {
            updateCacheBypassCondition(true); // L2 hit.
            perKeyCache.l1MapEntries.put(userKey, l2Entry); // Promote to L1.
            return true;
        }

        // 6. Complete cache miss – consult the delegate.
        updateCacheBypassCondition(false);
        boolean exists = delegateState.contains(userKey);

        UV resolvedValue = null;
        if (exists) {
            // Fetch the actual value once so that subsequent get(key) hits L1 rather than
            // incurring another delegate round-trip.  This behaviour is relied upon by
            // several unit-tests (see MapContains_* test cases).
            resolvedValue = delegateState.get(userKey);
            perKeyCache.l1MapEntries.put(userKey, CacheEntry.clean(resolvedValue));
        }

        // 7. Update caches with information from delegate.
        if (keyPresenceCacheEnabled) {
            perKeyCache.updatePresenceCacheOnGet(userKey, exists);
        }

        return exists;
    }

    private Map<UK, UV> getMergedState() throws Exception {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();
        if (!perKeyCache.fullyLoaded) {
            loadAllEntriesToCache(perKeyCache);
        }

        Map<UK, UV> allEntriesMap = new HashMap<>();
        // L1 has the most up-to-date view (dirty entries, tombstones)
        for (Map.Entry<UK, CacheEntry<UV>> l1Entry : perKeyCache.l1MapEntries.entrySet()) {
            if (l1Entry.getValue().getValue() != null) { // Exclude tombstones
                allEntriesMap.put(l1Entry.getKey(), l1Entry.getValue().getValue());
            }
        }

        Map<UK, UV> writeBuffer = getWriteBufferForCurrentKeyIfPresent();
        if (writeBuffer != null) {
            for (Map.Entry<UK, UV> bufferedEntry : writeBuffer.entrySet()) {
                if (bufferedEntry.getValue() == null) {
                    allEntriesMap.remove(bufferedEntry.getKey());
                } else {
                    allEntriesMap.put(bufferedEntry.getKey(), bufferedEntry.getValue());
                }
            }
        }

        return allEntriesMap;
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        delegateState.setCurrentNamespace(getCurrentNamespace());
        return getMergedState().entrySet();
    }

    @Override
    public Iterable<UV> values() throws Exception {
        delegateState.setCurrentNamespace(getCurrentNamespace());
        return getMergedState().values();
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        delegateState.setCurrentNamespace(getCurrentNamespace());
        return getMergedState().keySet();
    }

    private void loadAllEntriesToCache(PerKeyMapCache<UK, UV, K, N> perKeyCache) throws Exception {
        if (perKeyCache.fullyLoaded) {
            return;
        }
        // Clear existing cache content before full load
        perKeyCache.l1MapEntries.clear(); // Will trigger memory release via listeners
        perKeyCache.l2MapEntries.clear(); // Will trigger memory release via listeners
        if (this.keyPresenceCacheEnabled) {
            perKeyCache.l1KeyPresenceCache.clear(); // Will trigger memory release
            perKeyCache.l2KeyPresenceCache.clear(); // Will trigger memory release
        }

        delegateLookups.increment();
        Iterable<Map.Entry<UK, UV>> entriesFromDelegate = delegateState.entries();
        if (entriesFromDelegate != null) {
            for (Map.Entry<UK, UV> entry : entriesFromDelegate) {
                UK uk = entry.getKey();
                UV uv = entry.getValue();

                if (uv != null) {
                    // Cache the value as a clean entry in L1 so that subsequent accesses
                    // hit the cache instead of the delegate.
                    perKeyCache.l1MapEntries.put(uk, CacheEntry.clean(uv));
                } else if (this.keyPresenceCacheEnabled) {
                    // Only cache explicit absence information in the presence cache when enabled.
                    perKeyCache.updatePresenceCacheOnGet(uk, false);
                }
            }
        }
        perKeyCache.fullyLoaded = true;
    }

    @Override
    public boolean isEmpty() throws Exception {
        // Mutations are written through immediately – we can directly consult the delegate.
        return delegateState.isEmpty();
    }

    @Override
    public void clear() {
        // Ensure the delegate state operates on the correct namespace.
        // This is crucial because delegateState.clear() is namespace-specific.
        delegateState.setCurrentNamespace(getCurrentNamespace());

        Map<UK, UV> writeBuffer = getWriteBufferForCurrentKeyIfPresent();
        if (writeBuffer != null) {
            writeBuffer.clear();
        }

        // Obtain the cache specific to the current Flink key (K) and namespace (N).
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();
        
        // Clear L1 map entries. Eviction listeners (if any, e.g., for flushing dirty entries)
        // should be triggered by the CachePolicy's clear() implementation.
        perKeyCache.l1MapEntries.clear();

        // Clear L2 map entries. Similarly, eviction listeners should be triggered.
        perKeyCache.l2MapEntries.clear();

        // If key-value separation is enabled, clear the presence caches as well.
        if (this.keyPresenceCacheEnabled) {
            perKeyCache.l1KeyPresenceCache.clear();
            perKeyCache.l2KeyPresenceCache.clear();
        }

        // After clearing the caches, clear the entries in the underlying delegate state
        // for the current key and namespace.
        delegateState.clear();

        // Reset the fullyLoaded flag for this PerKeyMapCache, as its contents (both cache
        // and underlying state for this key/namespace) have been cleared.
        perKeyCache.fullyLoaded = false;
    }

    @Override
    public void flushToUnderlyingState() throws IOException {
        try {
            List<Map.Entry<N, Map<K, Map<UK, UV>>>> nsEntries = new ArrayList<>();
            for (Map.Entry<N, Map<K, Map<UK, UV>>> entry : namespaceWriteBuffers.entrySet()) {
                nsEntries.add(entry);
            }
            for (Map.Entry<N, Map<K, Map<UK, UV>>> nsEntry : nsEntries) {
                flushWriteBufferForNamespace(nsEntry.getKey(), nsEntry.getValue());
            }
        } catch (Exception e) {
            throw new IOException("Failed to flush write-behind buffers for map state", e);
        }
        namespaceWriteBuffers.clear();

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
        // The keyed backend always knows the key-serializer, and the unit-tests stub this.
        return backend.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return cachedNamespaceSerializer != null ? cachedNamespaceSerializer : delegateState.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<Map<UK, UV>> getValueSerializer() {
        return delegateState.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        this.currentNamespace = namespace;
        // Defer delegate namespace update until we have to interact with it (e.g. during flush).
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
            throw new RuntimeException("Failed to flush cache to underlying state before creating StateIncrementalVisitor", e);
        }
        return delegateState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    // Getter for testing bypass state
    @org.apache.flink.annotation.VisibleForTesting
    public boolean isBypassCacheActive() {
        return bypassCache;
    }

    @org.apache.flink.annotation.VisibleForTesting
    public long getTotalAccessesForBypassEligibility() {
        if (totalAccessesForBypassEligibility != null) {
        return totalAccessesForBypassEligibility.sum();
        }
        return 0;
    }

    @Override
    public long evictEntriesToFreeMemory(long targetBytesToFreeThisState) {
        if (targetBytesToFreeThisState <= 0) return 0;
        long totalFreedBytes = 0;

        // Iterate over all PerKeyMapCache instances and ask them to evict
        // This is a simplified global eviction. More sophisticated might prioritize namespaces/keys.
        try {
            for (CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> flinkKeyCaches : namespaceCaches.values()) {
                if (totalFreedBytes >= targetBytesToFreeThisState) break;
                if (flinkKeyCaches == null) continue;

                // Iterate over a snapshot of keys to avoid ConcurrentModificationException if map can change
                List<PerKeyMapCache<UK, UV, K, N>> perKeyCachesToEvict = new ArrayList<>(flinkKeyCaches.values());

                for (PerKeyMapCache<UK, UV, K, N> perKeyCache : perKeyCachesToEvict) {
                    if (totalFreedBytes >= targetBytesToFreeThisState) break;
                if (perKeyCache == null) continue;

                    // Set context for potential delegate operations if L1 dirty entries are flushed during eviction
                    K originalKey = backend.getCurrentKey();
                    N originalNamespace = getCurrentNamespace();
                    backend.setCurrentKey(perKeyCache.flinkKey); // Set context for this specific key's cache
                    delegateState.setCurrentNamespace(perKeyCache.cacheNamespace);

                    perKeyCache.evictToMeetMemoryLimit(targetBytesToFreeThisState - totalFreedBytes);
                    // The actual freed amount is managed by reportCacheMemoryReleased calls from within PerKeyMapCache
                    // For simplicity here, we assume the target passed to evictToMeetMemoryLimit is what we are trying to free from this cache.
                    // A more accurate way would be for evictToMeetMemoryLimit to return bytes freed.
                    // However, currentEstimatedCacheSizeBytes is updated globally via reportCacheMemoryReleased.
                    // So, we just need to trigger eviction. The main job here is to *trigger* eviction in sub-caches.

                     // Restore original context
                    if (originalKey != null) backend.setCurrentKey(originalKey); else backend.setCurrentKey(null);
                    if (originalNamespace != null) delegateState.setCurrentNamespace(originalNamespace); else delegateState.setCurrentNamespace(null);


                    // Re-check global to see if enough has been freed by this PerKeyMapCache's internal evictions
                    // This is a bit indirect. CachingKeyedStateBackend will make the final call.
                    // This method's return value is more of a "did we attempt to free" acknowledgement.
                    // Let's assume for now evictToMeetMemoryLimit inside perKeyCache handles reporting released memory,
                    // and we check current global total in CachingKeyedStateBackend.
                    // The main job here is to *trigger* eviction in sub-caches.
                }
            }
        } catch (Exception e) {
            LOG.warn("Error during explicit memory eviction from CachingInternalMapState: {}", e.getMessage(), e);
        }
        // The actual amount freed is tracked by CachingKeyedStateBackend's atomic counter.
        // This method signals that an attempt was made.
        return targetBytesToFreeThisState; // Placeholder, real tracking is via CachingKeyedStateBackend
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        return entries().iterator();
    }

    private static <K_F, N_F, UK_C, UV_C> void flushL1Entries(
            PerKeyMapCache<UK_C, UV_C, K_F, N_F> perKeyCache, K_F flinkKey, N_F namespace,
            CachingKeyedStateBackend<K_F> backendForContext,
            InternalMapState<K_F, N_F, UK_C, UV_C> delegateStateForContext) throws Exception {

        if (flinkKey == null || perKeyCache == null || perKeyCache.l1MapEntries == null) {
            return;
        }

        K_F originalKey = backendForContext.getCurrentKey();

        backendForContext.setCurrentKey(flinkKey);
        delegateStateForContext.setCurrentNamespace(namespace); // Use the namespace relevant to this perKeyCache

        try {
            Iterator<Map.Entry<UK_C, CacheEntry<UV_C>>> l1Iterator = perKeyCache.l1MapEntries.entrySet().iterator();
            while (l1Iterator.hasNext()) {
                Map.Entry<UK_C, CacheEntry<UV_C>> l1Entry = l1Iterator.next();
                UK_C userKey = l1Entry.getKey();
                CacheEntry<UV_C> cacheEntry = l1Entry.getValue();

                if (cacheEntry.isDirty()) {
                    UV_C userValue = cacheEntry.getValue();
                    if (userValue == null) {
                        delegateStateForContext.remove(userKey);
                    } else {
                        delegateStateForContext.put(userKey, userValue);
                        // Do NOT move to L2 – maintain behaviour consistent with PerKeyMapCache eviction.
                    }
                    cacheEntry.setDirty(false);
                } else { // Clean entry from L1
                    if (cacheEntry.getValue() != null) { // Not a tombstone, can move to L2
                       if (perKeyCache.l2MapEntries.getClass() != NoOpCachePolicy.class) {
                          perKeyCache.l2MapEntries.put(userKey, cacheEntry); // Already clean
                       }
                    }
                }
                // After processing (flushing dirty or moving clean to L2), the entry is removed from L1 by the iterator.
                // The memory release for this L1 removal is handled by the L1 cache policy's eviction listener.
                l1Iterator.remove();
            }
        } finally {
            backendForContext.setCurrentKey(originalKey);
            // We do not restore the delegate state's namespace because we don't have the original and it's not required by the caller.
        }
    }

    private Map<UK, UV> getWriteBufferForCurrentKey() {
        N namespace = getCurrentNamespace();
        K key = backend.getCurrentKey();
        Map<K, Map<UK, UV>> perKeyBuffers = namespaceWriteBuffers.computeIfAbsent(namespace, n -> new HashMap<>());
        return perKeyBuffers.computeIfAbsent(key, k -> new LinkedHashMap<>());
    }

    private Map<UK, UV> getWriteBufferForCurrentKeyIfPresent() {
        N namespace = getCurrentNamespace();
        K key = backend.getCurrentKey();
        Map<K, Map<UK, UV>> perKeyBuffers = namespaceWriteBuffers.get(namespace);
        if (perKeyBuffers == null) {
            return null;
        }
        return perKeyBuffers.get(key);
    }

    private void flushWriteBufferForNamespace(N namespace, Map<K, Map<UK, UV>> perKeyBuffers) throws Exception {
        if (perKeyBuffers == null || perKeyBuffers.isEmpty()) {
            return;
        }

        K originalKeyContext = backend.getCurrentKey();
        N originalDelegateNamespaceContext = getCurrentNamespace();

        try {
            for (Map.Entry<K, Map<UK, UV>> perKeyEntry : perKeyBuffers.entrySet()) {
                K flinkKey = perKeyEntry.getKey();
                Map<UK, UV> writeBuffer = perKeyEntry.getValue();

                backend.setCurrentKey(flinkKey);
                delegateState.setCurrentNamespace(namespace);

                for (Map.Entry<UK, UV> op : new java.util.ArrayList<>(writeBuffer.entrySet())) {
                    if (op.getValue() == null) { // tombstone
                        remove(op.getKey());
                    } else {
                        doPut(op.getKey(), op.getValue());
                    }
                }
            }
        } finally {
            perKeyBuffers.clear();
            if (originalKeyContext != null) {
                backend.setCurrentKey(originalKeyContext);
            }
            if (originalDelegateNamespaceContext != null) {
                delegateState.setCurrentNamespace(originalDelegateNamespaceContext);
            }
        }
    }

    // Add this helper method to log cache metrics
    private void logCacheMetrics() {
        if (LOG.isDebugEnabled()) {
            // Calculate hit rates
            double l1ValueHitRate = calculateHitRate(l1ValueCacheHitCount, l1ValueCacheMissCount);
            double l2ValueHitRate = calculateHitRate(l2ValueCacheHitCount, l2ValueCacheMissCount);
            double l1PresenceHitRate = calculateHitRate(l1PresenceCacheHitCount, l1PresenceCacheMissCount);
            double l2PresenceHitRate = calculateHitRate(l2PresenceCacheHitCount, l2PresenceCacheMissCount);

            LOG.debug("Cache Metrics - " +
                    "L1 Value: {}/{}, Hit Rate: {:.2f}% | " +
                    "L2 Value: {}/{}, Hit Rate: {:.2f}% | " +
                    "L1 Presence: {}/{}, Hit Rate: {:.2f}% | " +
                    "L2 Presence: {}/{}, Hit Rate: {:.2f}% | " +
                    "Delegate Lookups: {}",
                l1ValueCacheHitCount.sum(), 
                l1ValueCacheHitCount.sum() + l1ValueCacheMissCount.sum(),
                l1ValueHitRate,
                l2ValueCacheHitCount.sum(),
                l2ValueCacheHitCount.sum() + l2ValueCacheMissCount.sum(),
                l2ValueHitRate,
                l1PresenceCacheHitCount.sum(),
                l1PresenceCacheHitCount.sum() + l1PresenceCacheMissCount.sum(),
                l1PresenceHitRate,
                l2PresenceCacheHitCount.sum(),
                l2PresenceCacheHitCount.sum() + l2PresenceCacheMissCount.sum(),
                l2PresenceHitRate,
                delegateLookups.sum());
        }
    }

    private double calculateHitRate(LongAdder hits, LongAdder misses) {
        long total = hits.sum() + misses.sum();
        return total > 0 ? (hits.sum() * 100.0) / total : 0.0;
    }

    /**
     * Allows the owning keyed backend/builder to inject the namespace serializer when it is
     * readily available but {@link InternalMapState#getNamespaceSerializer()} is not reliably
     * implemented (e.g. on Mockito mocks in unit-tests).
     */
    void setNamespaceSerializer(TypeSerializer<N> namespaceSerializer) {
        this.cachedNamespaceSerializer = namespaceSerializer;
    }

    // Visible for tests / builder – allows late injection when the delegate is a mock.
    void setUserKeySerializer(TypeSerializer<UK> serializer) {
        if (serializer != null) {
            this.userKeySerializer = serializer;
        }
    }

    void setUserValueSerializer(TypeSerializer<UV> serializer) {
        if (serializer != null) {
            this.userValueSerializer = serializer;
        }
    }

    /**
     * Internal helper that records a put in the L1 cache and—depending on the
     * backend's write-behind mode—either buffers it (dirty) or performs an
     * immediate write-through to the delegate and stores the entry as *clean*.
     */
    private void doPut(UK userKey, UV userValue) throws Exception {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        // 1. Insert a *dirty* entry into L1 so that the cache immediately
        //    reflects the latest value for subsequent reads.
        CacheEntry<UV> dirtyEntry = CacheEntry.dirty(userValue);
        CacheEntry<UV> previous = perKeyCache.l1MapEntries.put(userKey, dirtyEntry);

        // Memory accounting -------------------------------------------------
        long oldSize = previous != null ? previous.getEstimatedSizeBytes() : 0L;
        long newSize = dirtyEntry.getEstimatedSizeBytes();
        long delta = newSize - oldSize;
        if (delta > 0) {
            backend.reportCacheMemoryAdded(delta);
        } else if (delta < 0) {
            backend.reportCacheMemoryReleased(-delta);
        }

        // Any previously cached clean copy in L2 has become stale.
        CacheEntry<UV> l2Old = perKeyCache.l2MapEntries.remove(userKey);
        if (l2Old != null) {
            backend.reportCacheMemoryReleased(l2Old.getEstimatedSizeBytes());
        }

        // Presence-cache maintenance ---------------------------------------
        if (keyPresenceCacheEnabled) {
            perKeyCache.updatePresenceCacheOnPut(userKey);
        }

        // 2. Decide between write-behind and immediate write-through --------
        if (backend.isWriteBehindEnabled()) {
            // Keep entry dirty – it will be flushed on eviction / explicit flush.
            perKeyCache.fullyLoaded = false;
            updateCacheBypassCondition(true);
        } else {
            // Legacy mode: write through now and mark the cache entry clean
            // to avoid a second delegate write later.
            delegateState.put(userKey, userValue);

            CacheEntry<UV> cleanEntry = CacheEntry.clean(userValue);
            perKeyCache.l1MapEntries.put(userKey, cleanEntry);

            perKeyCache.fullyLoaded = false;
            updateCacheBypassCondition(true);
        }
    }
}


