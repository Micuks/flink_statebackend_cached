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
import org.apache.flink.runtime.state.internal.InternalMapState;

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
        CachingInternalState<K, N, Map<UK, UV>, InternalMapState<K, N, UK, UV>> {

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
        // Flag to indicate if the entire map for this (FlinkKey, Namespace) has been
        // fully loaded from delegate
        boolean fullyLoaded = false;
        private final N_F mainContextDelegateNamespace; // Added to store the main operation's
                                                        // namespace

        PerKeyMapCache(int l1Size, int l2Size, InternalMapState<K_F, N_F, UK_C, UV_C> delegateState,
                CachingKeyedStateBackend<K_F> ownerBackend, K_F flinkKey, N_F cacheNamespace,
                N_F mainContextDelegateNamespace,
                CachingStateBackendFactory.CachePolicyType cachePolicyType) { // Added
                                                                              // mainContextDelegateNamespace
            this.l2MapEntries = createCachePolicyInstance(cachePolicyType, l2Size, null);
            this.mainContextDelegateNamespace = mainContextDelegateNamespace; // Store it
            this.l1MapEntries =
                    createCachePolicyInstance(cachePolicyType, l1Size, evictedL1MapEntry -> {
                        // On L1 eviction for a single entry
                        UK_C evictedUK = evictedL1MapEntry.getKey();
                        CacheEntry<UV_C> evictedUVWrapper = evictedL1MapEntry.getValue();

                        if (evictedUVWrapper.isDirty()) {
                            K_F originalKeyContext = null;
                            N_F originalDelegateNamespaceContext = null;
                            try {
                                // Ensure context for delegate operation for this single evicted
                                // entry
                                originalKeyContext = ownerBackend.getCurrentKey();
                                // Capture the delegate's namespace *before* changing it for this
                                // eviction
                                // operation
                                // This is tricky as the delegateState is shared. The most reliable
                                // would be
                                // to get it from
                                // the CachingInternalMapState's currentNamespace if this was not a
                                // static
                                // class.
                                // Since it is static, we rely on the passed
                                // mainContextDelegateNamespace
                                // for restoration.
                                originalDelegateNamespaceContext =
                                        this.mainContextDelegateNamespace;


                                ownerBackend.setCurrentKey(flinkKey); // Set Flink key context for
                                                                      // backend
                                delegateState.setCurrentNamespace(cacheNamespace); // Set Namespace
                                                                                   // context
                                                                                   // for delegate
                                                                                   // FOR THIS
                                                                                   // EVICTION

                                if (evictedUVWrapper.getValue() == null) { // Is a tombstone
                                    delegateState.remove(evictedUK);
                                } else {
                                    delegateState.put(evictedUK, evictedUVWrapper.getValue());
                                    // After successful flush of a dirty put, add clean to L2
                                    this.l2MapEntries.put(evictedUK,
                                            CacheEntry.clean(evictedUVWrapper.getValue()));
                                }
                                // evictedUVWrapper.setDirty(false); // No longer needed, new clean
                                // entry in
                                // L2

                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Failed to flush L1 map entry to delegate for user key: "
                                                + evictedUK,
                                        e);
                            } finally {
                                // Restore context
                                if (originalKeyContext != null) {
                                    ownerBackend.setCurrentKey(originalKeyContext);
                                }
                                // Restore the delegate's namespace to what it was for the main
                                // operation
                                if (originalDelegateNamespaceContext != null) {
                                    delegateState
                                            .setCurrentNamespace(originalDelegateNamespaceContext);
                                }
                            }
                        } else {
                            // If not dirty and not null (wasn't a prior removal), move to L2
                            if (evictedUVWrapper.getValue() != null) {
                                this.l2MapEntries.put(evictedUK, evictedUVWrapper); // Put original
                                                                                    // clean
                                                                                    // wrapper
                            }
                        }
                    });
        }

        private static <CK, CV> CachePolicy<CK, CV> createCachePolicyInstance(
                CachingStateBackendFactory.CachePolicyType policyType, int capacity,
                Consumer<Map.Entry<CK, CV>> evictionListener) {
            switch (policyType) {
                case TINYLFU:
                    // TODO: Adapt TinyLFU for eviction listener if necessary
                    return new TinyLFUMap<>(capacity);
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
        this.cachePolicyType = cachePolicyType; // Store it

        this.namespaceCaches = createCachePolicyWithEvictionListener(
                this.maxActiveNamespacesInCache, evictedNamespaceEntry -> {
                    // Namespace cache itself is being evicted, flush all its Flink key caches
                    CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> keyCachesToFlush =
                            evictedNamespaceEntry.getValue();
                    // TODO: This iteration might need adjustment based on CachePolicy.entrySet()
                    // behavior
                    for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> keyCacheEntry : keyCachesToFlush
                            .entrySet()) {
                        try {
                            flushL1Entries(keyCacheEntry.getValue(), keyCacheEntry.getKey(),
                                    evictedNamespaceEntry.getKey(), // This is the namespace
                                    this.backend, this.delegateState);
                        } catch (Exception e) {
                            throw new RuntimeException(
                                    "Failed to flush PerKeyMapCache on namespace eviction for Flink key: "
                                            + keyCacheEntry.getKey(),
                                    e);
                        }
                    }
                });

        // Derive user key/value serializers
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
        N currentNs = getCurrentNamespace(); // Ensures currentNamespace is not null
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
                                flushL1Entries(perKeyCacheToFlush, evictedFlinkKey, ns, // current
                                                                                        // namespace
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
                        backend, k, currentNs, currentNs, this.cachePolicyType)); // Pass
        // currentNs
        // as
        // mainContextDelegateNamespace
        // and cachePolicyType
    }

    @Override
    public UV get(UK userKey) throws Exception {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        // Check L1 cache
        CacheEntry<UV> l1Entry = perKeyCache.l1MapEntries.get(userKey); // Directly get
        if (l1Entry != null) { // If entry exists (could be a tombstone)
            // LRUMap get should also mark it as recently used.
            return l1Entry.getValue(); // Return the value, which might be null for a tombstone
        }

        // Check L2 cache
        CacheEntry<UV> l2Entry = perKeyCache.l2MapEntries.get(userKey);
        if (l2Entry != null) {
            // L2 entries are always clean and non-null
            perKeyCache.l2MapEntries.remove(userKey); // Remove from L2
            perKeyCache.l1MapEntries.put(userKey, l2Entry); // Promote to L1
            return l2Entry.getValue(); // L2 Hit
        }

        if (perKeyCache.fullyLoaded) {
            return null; // Not in L1/L2 and map was fully loaded, so not present
        }

        // L1 and L2 Miss, not fully loaded. Fetch from delegate.
        UV valueFromDelegate = delegateState.get(userKey);
        if (valueFromDelegate != null) {
            perKeyCache.l1MapEntries.put(userKey, CacheEntry.clean(valueFromDelegate));
        }
        // If valueFromDelegate is null, it means the key doesn't exist in the delegate.
        // We don't cache this null, subsequent 'get' or 'contains' would re-query
        // delegate
        // until 'fullyLoaded' becomes true (e.g., after an iterator call).
        return valueFromDelegate;
    }

    @Override
    public void put(UK userKey, UV userValue) throws Exception {
        if (userValue == null) {
            // As per MapState#put Javadoc: "If the map previously contained a mapping for
            // the key,
            // the old value is replaced by the specified value. (A map m is said to contain
            // a
            // mapping for a key k if and only if m.containsKey(k) would return true.)"
            // And for Map#put: "If the map previously contained a mapping for the key, the
            // old
            // value is replaced by the specified value."
            // Standard Java Map behavior is to remove the key if value is null for some
            // implementations like ConcurrentHashMap, but not all.
            // However, Flink's MapState API does not explicitly define put(key, null) as
            // remove.
            // For safety and explicit control, we should rely on remove(key).
            // The tests imply put(key, null) should act as remove.
            remove(userKey);
            return;
        }
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        perKeyCache.l2MapEntries.remove(userKey); // Invalidate from L2
        perKeyCache.l1MapEntries.put(userKey, CacheEntry.dirty(userValue));
        perKeyCache.fullyLoaded = false; // State changed, so not fully loaded anymore
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
            if (userValue == null) { // Consistent with single put
                perKeyCache.l2MapEntries.remove(userKey);
                perKeyCache.l1MapEntries.put(userKey, CacheEntry.dirty(null)); // Tombstone
            } else {
                perKeyCache.l2MapEntries.remove(userKey);
                perKeyCache.l1MapEntries.put(userKey, CacheEntry.dirty(userValue));
            }
        }
        perKeyCache.fullyLoaded = false; // State changed
    }

    @Override
    public void remove(UK userKey) throws Exception {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();
        perKeyCache.l2MapEntries.remove(userKey); // Invalidate from L2
        perKeyCache.l1MapEntries.put(userKey, CacheEntry.dirty(null)); // Tombstone in L1
        perKeyCache.fullyLoaded = false; // State changed
    }

    @Override
    public boolean contains(UK userKey) throws Exception {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        CacheEntry<UV> l1Entry = perKeyCache.l1MapEntries.get(userKey);
        if (l1Entry != null) {
            return l1Entry.getValue() != null; // True if L1 has it and it's not a tombstone
        }

        CacheEntry<UV> l2Entry = perKeyCache.l2MapEntries.get(userKey);
        if (l2Entry != null) {
            // L2 entries are clean and non-null. Promote to L1.
            perKeyCache.l2MapEntries.remove(userKey);
            perKeyCache.l1MapEntries.put(userKey, l2Entry);
            return true; // L2 Hit
        }

        if (perKeyCache.fullyLoaded) {
            return false; // Not in L1/L2, and map was fully loaded
        }

        // L1 and L2 Miss, not fully loaded. Check delegate.
        boolean delegateContains = delegateState.contains(userKey);
        if (delegateContains) {
            // If delegate contains, load it to L1 to keep cache consistent for subsequent
            // gets.
            // This might be slightly inefficient if only 'contains' is called repeatedly
            // without 'get'.
            UV valueFromDelegate = delegateState.get(userKey);
            if (valueFromDelegate != null) { // Should not be null if contains is true
                perKeyCache.l1MapEntries.put(userKey, CacheEntry.clean(valueFromDelegate));
            }
        }
        return delegateContains;
    }

    private void loadAllEntriesToCache(PerKeyMapCache<UK, UV, K, N> perKeyCache) throws Exception {
        if (perKeyCache.fullyLoaded) {
            return;
        }

        // Step 1: Flush dirty L1 entries to ensure delegate is up-to-date before load
        // And move clean L1 to L2.
        // This flushL1Entries will also handle putting flushed dirty entries into L2 as
        // clean.
        flushL1Entries(perKeyCache, backend.getCurrentKey(), getCurrentNamespace(), this.backend,
                this.delegateState);

        // Step 2: Clear L1 (L2 was populated by flush or already had clean entries from
        // delegate)
        // We are about to load everything from delegate, L1 will be repopulated.
        // L2 entries that were already clean and from delegate are fine.
        // New L1 will be a superset of what L2 might have for this key.
        // For simplicity, let's clear L1. L2 contains a subset of delegate's clean
        // data.
        perKeyCache.l1MapEntries.clear();

        // Step 3: Load all from delegate into L1 as clean entries
        Iterable<Map.Entry<UK, UV>> delegateEntries = delegateState.entries();
        if (delegateEntries != null) {
            for (Map.Entry<UK, UV> entry : delegateEntries) {
                // If L2 already has this key (e.g. from a previous flush during this loadAll),
                // L1 should still get it. LRUMap handles duplicates by updating.
                perKeyCache.l1MapEntries.put(entry.getKey(), CacheEntry.clean(entry.getValue()));
                // No need to explicitly remove from L2 here if L1 gets it.
                // If L2 had it, it's just a more recent access in L1 now.
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

        // If fullyLoaded, L1 + L2 is the complete view.
        // Give precedence to L1, then add L2 entries not in L1.
        Map<UK, UV> allEntriesMap = new HashMap<>();

        // Add L1 entries (non-tombstone)
        for (Map.Entry<UK, CacheEntry<UV>> l1Entry : perKeyCache.l1MapEntries.entrySet()) {
            if (l1Entry.getValue().getValue() != null) { // Not a tombstone
                allEntriesMap.put(l1Entry.getKey(), l1Entry.getValue().getValue());
            }
        }

        // Add L2 entries not already covered by L1
        // (L2 entries are always clean and non-null)
        if (perKeyCache.fullyLoaded) { // Only consider L2 if fully loaded, otherwise L1 from
                                       // delegate is enough
            for (Map.Entry<UK, CacheEntry<UV>> l2Entry : perKeyCache.l2MapEntries.entrySet()) {
                if (!allEntriesMap.containsKey(l2Entry.getKey())) {
                    allEntriesMap.put(l2Entry.getKey(), l2Entry.getValue().getValue());
                }
            }
        }
        return allEntriesMap.entrySet(); // Return as an iterable Set of Map.Entry
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        // Re-use entries() logic and extract keys
        List<UK> keysList = new ArrayList<>();
        for (Map.Entry<UK, UV> entry : entries()) {
            keysList.add(entry.getKey());
        }
        return keysList;
    }

    @Override
    public Iterable<UV> values() throws Exception {
        // Re-use entries() logic and extract values
        List<UV> valuesList = new ArrayList<>();
        for (Map.Entry<UK, UV> entry : entries()) {
            valuesList.add(entry.getValue());
        }
        return valuesList;
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        // This provides an iterator over the entries() iterable.
        return entries().iterator();
    }

    // Internal helper to flush L1 of a specific PerKeyMapCache.
    // This is called during L1 eviction from PerKeyMapCache itself,
    // and when a PerKeyMapCache is evicted from its parent LRUMap (keyCaches).
    private static <K_F, N_F, UK_C, UV_C> void flushL1Entries(
            PerKeyMapCache<UK_C, UV_C, K_F, N_F> perKeyCache, K_F flinkKey, N_F namespace,
            CachingKeyedStateBackend<K_F> backendForContext, // Changed type
            InternalMapState<K_F, N_F, UK_C, UV_C> delegateStateForContext) throws Exception {

        // Temporarily store entries to flush to avoid concurrent modification if LRUMap
        // eviction
        // triggers this during iteration.
        List<Map.Entry<UK_C, CacheEntry<UV_C>>> dirtyL1EntriesToFlush = new ArrayList<>();
        List<Map.Entry<UK_C, CacheEntry<UV_C>>> cleanL1EntriesToL2 = new ArrayList<>();

        for (Map.Entry<UK_C, CacheEntry<UV_C>> l1Entry : perKeyCache.l1MapEntries.entrySet()) {
            if (l1Entry.getValue().isDirty()) {
                dirtyL1EntriesToFlush.add(l1Entry);
            } else if (l1Entry.getValue().getValue() != null) { // Clean and not a tombstone
                cleanL1EntriesToL2.add(l1Entry);
            }
        }
        perKeyCache.l1MapEntries.clear(); // Clear L1 after collecting entries

        K_F originalKey = backendForContext.getCurrentKey();
        N_F originalNamespace = namespace; // Use parameter namespace
        backendForContext.setCurrentKey(flinkKey);
        delegateStateForContext.setCurrentNamespace(namespace);

        try {
            for (Map.Entry<UK_C, CacheEntry<UV_C>> dirtyEntry : dirtyL1EntriesToFlush) {
                UK_C userKey = dirtyEntry.getKey();
                UV_C userValue = dirtyEntry.getValue().getValue();
                if (userValue == null) { // Tombstone
                    delegateStateForContext.remove(userKey);
                } else {
                    delegateStateForContext.put(userKey, userValue);
                    // After successful flush of a dirty put, add clean to L2
                    perKeyCache.l2MapEntries.put(userKey, CacheEntry.clean(userValue));
                }
                dirtyEntry.getValue().setDirty(false); // Mark as clean
            }

            for (Map.Entry<UK_C, CacheEntry<UV_C>> cleanEntry : cleanL1EntriesToL2) {
                // Move clean, non-null entries to L2
                perKeyCache.l2MapEntries.put(cleanEntry.getKey(), cleanEntry.getValue());
            }

        } finally {
            backendForContext.setCurrentKey(originalKey);
            if (originalNamespace != null) {
                delegateStateForContext.setCurrentNamespace(originalNamespace);
            }
        }
        // fullyLoaded remains as it was, flushing doesn't change its status.
        // If it was false, it's still false. If true, it means L1 (+ what went to L2)
        // was complete.
    }

    @Override
    public boolean isEmpty() throws Exception {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        // Check L1 for any non-tombstone entries
        for (CacheEntry<UV> l1EntryValue : perKeyCache.l1MapEntries.values()) {
            if (l1EntryValue.getValue() != null) {
                return false; // Found a non-tombstone in L1
            }
        }
        // Check L2 (always non-null, clean entries)
        if (!perKeyCache.l2MapEntries.isEmpty()) {
            return false; // Found entries in L2
        }

        // If L1 has only tombstones and L2 is empty:
        if (perKeyCache.fullyLoaded) {
            // And if the map was fully loaded, then all L1 tombstones mean it's truly
            // empty.
            // (because L2 is also empty and no other entries exist in delegate)
            return true;
        }

        // Not fully loaded, and L1/L2 appear empty or only have tombstones in L1.
        // Must consult delegate.
        return delegateState.isEmpty();
    }

    @Override
    public void clear() {
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();
        if (perKeyCache.fullyLoaded) {
            // If fully loaded, clearing L1 and L2 is enough if we also mark all items for delegate
            // deletion.
            // However, simpler to just clear delegate and then caches.
            delegateState.clear();
            perKeyCache.l1MapEntries.clear();
            perKeyCache.l2MapEntries.clear();
            perKeyCache.fullyLoaded = false; // Reset full load status
        } else {
            // Not fully loaded, so we don't know all keys. Must clear delegate.
            delegateState.clear();
            // And clear whatever we have in caches.
            perKeyCache.l1MapEntries.clear();
            perKeyCache.l2MapEntries.clear();
            // fullyLoaded remains false or becomes false if it was true and we did not full-clear
        }
        // After clearing, the cache for this specific FlinkKey/Namespace should reflect an empty
        // state.
        // The PerKeyMapCache itself is not removed from its parent `keyCaches` map here,
        // that happens on LRU eviction of the `keyCaches` map itself.
    }

    @Override
    public void flushToUnderlyingState() throws IOException {
        N currentGlobalNamespace = getCurrentNamespace();
        K currentGlobalKey = backend.getCurrentKey();

        // Iterate over all namespace caches
        for (Map.Entry<N, CachePolicy<K, PerKeyMapCache<UK, UV, K, N>>> nsEntry : namespaceCaches
                .entrySet()) {
            N namespace = nsEntry.getKey();
            CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> keyCaches = nsEntry.getValue();

            // Iterate over all Flink key caches within this namespace
            // TODO: Adapt if CachePolicy.entrySet() differs significantly from Map.entrySet()
            List<K> flinkKeysInCache = new ArrayList<>();
            for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> keyEntry : keyCaches.entrySet()) {
                flinkKeysInCache.add(keyEntry.getKey());
            }

            for (K flinkKey : flinkKeysInCache) {
                PerKeyMapCache<UK, UV, K, N> perKeyCache = keyCaches.get(flinkKey);
                if (perKeyCache != null) {
                    try {
                        // Use the namespace and flinkKey from the iteration context for flushing
                        flushL1Entries(perKeyCache, flinkKey, namespace, this.backend,
                                this.delegateState);
                    } catch (Exception e) {
                        throw new IOException("Failed to flush map entries for Flink key: "
                                + flinkKey + " in namespace: " + namespace, e);
                    }
                }
            }
        }
        // Restore original context if it was set
        if (currentGlobalNamespace != null) {
            setCurrentNamespace(currentGlobalNamespace);
        }
        if (currentGlobalKey != null) {
            backend.setCurrentKey(currentGlobalKey);
        } else {
            // If the original key was null (e.g. before any key-specific operation),
            // ensure the backend reflects this. This might depend on Flink's internal
            // expectations for AbstractKeyedStateBackend when no key is active.
            backend.setCurrentKey(null); // Or appropriate method to clear current key context
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
        // TODO: Caching for incremental visitor? Very complex.
        // Needs to merge visitor results from delegate with cached L1/L2 state.
        // For now, delegate. This means visitor bypasses cache for reads.
        // Ensure caches are flushed before allowing visitor to read from delegate for
        // consistency.
        try {
            flushToUnderlyingState(); // Ensure consistency before direct delegate access
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush caches before creating state visitor", e);
        }
        return delegateState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }
}
