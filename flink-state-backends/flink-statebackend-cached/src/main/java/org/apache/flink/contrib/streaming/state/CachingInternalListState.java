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
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.internal.InternalListState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.Iterator;

/**
 * An {@link InternalListState} that uses an L1/L2 cache for its list values. The entire list is
 * cached as a single entry per Flink key (K) and namespace (N).
 *
 * @param <K> The type of the Flink key.
 * @param <N> The type of the namespace.
 * @param <V_ELE> The type of elements in the list.
 */
public class CachingInternalListState<K, N, V_ELE> implements InternalListState<K, N, V_ELE>,
        CachingInternalState<K, N, List<V_ELE>, InternalListState<K, N, V_ELE>> {

    private final InternalListState<K, N, V_ELE> delegateState;
    private final CachingKeyedStateBackend<K> backend;

    private N currentNamespace;

    // Cache structure: Namespace -> Flink Key -> CacheEntry<List<V_ELE>>
    private final CachePolicy<N, CachePolicy<K, CacheEntry<List<V_ELE>>>> namespaceCachesL1;
    private final CachePolicy<N, CachePolicy<K, CacheEntry<List<V_ELE>>>> namespaceCachesL2;

    private final int l1CacheSizePerNamespace; // Max Flink Keys with cached lists in L1 for a
    // Namespace
    private final int l2CacheSizePerNamespace; // Max Flink Keys with cached lists in L2 for a
    // Namespace
    private final int maxActiveNamespacesInCache; // Max Namespaces with active caches
    private final CachingStateBackendFactory.CachePolicyType cachePolicyType;

    public CachingInternalListState(InternalListState<K, N, V_ELE> delegateState,
            CachingKeyedStateBackend<K> backend, int l1CacheSize, int l2CacheSize,
            int maxActiveNamespaces, CachingStateBackendFactory.CachePolicyType cachePolicyType) {
        this.delegateState = delegateState;
        this.backend = backend;
        this.l1CacheSizePerNamespace = l1CacheSize; // Max K->List entries in L1 per Namespace
        this.l2CacheSizePerNamespace = l2CacheSize; // Max K->List entries in L2 per Namespace
        this.maxActiveNamespacesInCache = maxActiveNamespaces;
        this.cachePolicyType = cachePolicyType;

        this.namespaceCachesL1 = createCachePolicyWithEvictionListener(this.maxActiveNamespacesInCache,
                evictedNamespaceL1Entry -> {
                    N evictedNamespace = evictedNamespaceL1Entry.getKey();
                    CachePolicy<K, CacheEntry<List<V_ELE>>> evictedPerNsL1Cache = evictedNamespaceL1Entry.getValue();
                    try {
                        flushAndParkL1NamespaceCacheEntries(evictedNamespace, evictedPerNsL1Cache);
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Failed to flush/park L1 entries for evicted namespace: " + evictedNamespace, e);
                    }
                });
        this.namespaceCachesL2 = createCachePolicyWithEvictionListener(this.maxActiveNamespacesInCache,
                evictedNamespaceL2Entry -> {
                    // When a namespace's L2 key-cache is evicted, its entries are lost.
                    // Report memory released.
                    N evictedNamespace = evictedNamespaceL2Entry.getKey();
                    CachePolicy<K, CacheEntry<List<V_ELE>>> evictedPerNsL2Cache = evictedNamespaceL2Entry.getValue();
                    if (evictedPerNsL2Cache != null) {
                        for (Map.Entry<K, CacheEntry<List<V_ELE>>> entry : evictedPerNsL2Cache.entrySet()) {
                            if (entry.getValue() != null) {
                                this.backend.reportCacheMemoryReleased(entry.getValue().getEstimatedSizeBytes());
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

    private void flushAndParkL1NamespaceCacheEntries(N namespace, CachePolicy<K, CacheEntry<List<V_ELE>>> perNsL1Cache) throws Exception {
        if (perNsL1Cache == null) {
            return;
        }
        CachePolicy<K, CacheEntry<List<V_ELE>>> perNsL2Cache = getL2CacheForNamespace(namespace);
        K originalKeyContextForDelegate = backend.getCurrentKey();
        N originalNamespaceContextForDelegate = getCurrentNamespace();

        // Iterate over a copy of entries to avoid ConcurrentModificationException if underlying map disallows it
        List<Map.Entry<K, CacheEntry<List<V_ELE>>>> entriesToProcess = new ArrayList<>();
        for (Map.Entry<K, CacheEntry<List<V_ELE>>> entry : perNsL1Cache.entrySet()) {
            entriesToProcess.add(entry);
        }

        for (Map.Entry<K, CacheEntry<List<V_ELE>>> l1EntryTuple : entriesToProcess) {
            K key = l1EntryTuple.getKey();
            CacheEntry<List<V_ELE>> l1Entry = l1EntryTuple.getValue();

            if (l1Entry == null) continue;

            // Report memory released from L1 for this specific entry, as it's being processed out of its original L1 cache.
            // This balances out the add to L2 or if it's just flushed and dropped.
            // Note: The PerKeyCache itself is being evicted, so its total memory will be effectively released
            // from the backend's perspective once this listener finishes and the PerKeyCache is no longer referenced
            // by namespaceCachesL1. Individual reporting here helps track movement.
            // backend.reportCacheMemoryReleased(l1Entry.getEstimatedSizeBytes()); // This might be redundant if the whole PerKeyCache memory is reclaimed.

            List<V_ELE> listValue = l1Entry.getValue();

            if (l1Entry.isDirty()) {
                backend.setCurrentKey(key);
                this.setCurrentNamespace(namespace); // Set context for delegate
                delegateState.update(listValue);
                l1Entry.setDirty(false); // Mark as clean

                // Move to L2 as clean
                CacheEntry<List<V_ELE>> entryToL2 = CacheEntry.clean(listValue != null ? new ArrayList<>(listValue) : null);
                CacheEntry<List<V_ELE>> oldL2 = perNsL2Cache.put(key, entryToL2);
                if (oldL2 != null) backend.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes());
                backend.reportCacheMemoryAdded(entryToL2.getEstimatedSizeBytes());

            } else { // Clean entry
                if (listValue != null) { // Only move non-null (even if empty list) to L2
                    CacheEntry<List<V_ELE>> oldL2 = perNsL2Cache.put(key, l1Entry); // l1Entry is already clean
                    if (oldL2 != null) backend.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes());
                    backend.reportCacheMemoryAdded(l1Entry.getEstimatedSizeBytes());
                }
            }
        }
        // Restore original context
        backend.setCurrentKey(originalKeyContextForDelegate);
        this.setCurrentNamespace(originalNamespaceContextForDelegate);
        // perNsL1Cache.clear(); // The cache object itself is being discarded by the caller (TinyLFU/LRUMap)
    }

    private CachePolicy<K, CacheEntry<List<V_ELE>>> getL1CacheForNamespace(N namespace) {
        return namespaceCachesL1.computeIfAbsent(namespace,
                ns -> createCachePolicyWithEvictionListener(l1CacheSizePerNamespace,
                        evictedL1Entry -> { // Eviction from L1
                            CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(ns);
                            K evictedKey = evictedL1Entry.getKey();
                            CacheEntry<List<V_ELE>> evictedListWrapper = evictedL1Entry.getValue();
                            List<V_ELE> evictedList = evictedListWrapper.getValue();
                            long estimatedSize = evictedListWrapper.getEstimatedSizeBytes();

                            backend.reportCacheMemoryReleased(estimatedSize); // Report L1 release for this specific K,V pair

                            if (evictedListWrapper.isDirty()) {
                                try {
                                    N originalNamespaceContextForDelegate = getCurrentNamespace();
                                    K originalKeyContextForDelegate = backend.getCurrentKey();

                                    backend.setCurrentKey(evictedKey);
                                    this.setCurrentNamespace(ns); 
                                    delegateState.update(evictedList);
                                    evictedListWrapper.setDirty(false); 

                                    CacheEntry<List<V_ELE>> entryToL2 = CacheEntry.clean(evictedList != null ? new ArrayList<>(evictedList) : null);
                                    CacheEntry<List<V_ELE>> oldL2 = l2Cache.put(evictedKey, entryToL2);
                                    if (oldL2 != null) backend.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes());
                                    backend.reportCacheMemoryAdded(entryToL2.getEstimatedSizeBytes());

                                    backend.setCurrentKey(originalKeyContextForDelegate);
                                    this.setCurrentNamespace(originalNamespaceContextForDelegate);
                                } catch (Exception e) {
                                    throw new RuntimeException(
                                            "Failed to flush L1 list entry to delegate/L2 on eviction for key: "
                                                    + evictedKey + " in ns: " + ns,
                                            e);
                                }
                            } else {
                                if (evictedList != null) { // Only move non-null (even if empty list) to L2
                                    // evictedListWrapper is already clean
                                    // No need to create a new CacheEntry, pass the existing clean one.
                                    CacheEntry<List<V_ELE>> oldL2 = l2Cache.put(evictedKey, evictedListWrapper);
                                    if (oldL2 != null) backend.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes());
                                    backend.reportCacheMemoryAdded(evictedListWrapper.getEstimatedSizeBytes());
                                }
                            }
                        }));
    }

    private CachePolicy<K, CacheEntry<List<V_ELE>>> getL2CacheForNamespace(N namespace) {
        return namespaceCachesL2.computeIfAbsent(namespace,
                ns -> createCachePolicy(l2CacheSizePerNamespace));
    }

    @Override
    public Iterable<V_ELE> get() throws Exception {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        CachePolicy<K, CacheEntry<List<V_ELE>>> l1Cache = getL1CacheForNamespace(currentNamespace);
        CacheEntry<List<V_ELE>> l1Entry = l1Cache.get(currentKey);

        if (l1Entry != null) {
            List<V_ELE> value = l1Entry.getValue();
            return value != null ? new ArrayList<>(value) : null; 
        }

        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(currentNamespace);
        CacheEntry<List<V_ELE>> l2Entry = l2Cache.get(currentKey);

        if (l2Entry != null) {
            // L2 entry is clean. Remove from L2, copy to L1.
            // Report memory released from L2 and added to L1.
            backend.reportCacheMemoryReleased(l2Entry.getEstimatedSizeBytes()); // Releasing from L2
            l2Cache.remove(currentKey); 

            CacheEntry<List<V_ELE>> entryToL1 = CacheEntry.clean(l2Entry.getValue() != null ? new ArrayList<>(l2Entry.getValue()) : null);
            CacheEntry<List<V_ELE>> oldL1 = l1Cache.put(currentKey, entryToL1);
            if (oldL1 != null) backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());
            backend.reportCacheMemoryAdded(entryToL1.getEstimatedSizeBytes()); // Adding to L1
            return entryToL1.getValue() != null ? new ArrayList<>(entryToL1.getValue()) : null;
        }

        Iterable<V_ELE> iterableFromDelegate = delegateState.get();
        if (iterableFromDelegate == null) {
            CacheEntry<List<V_ELE>> newEntry = CacheEntry.clean(null);
            CacheEntry<List<V_ELE>> oldL1 = l1Cache.put(currentKey, newEntry); 
            if (oldL1 != null) backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());
            backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
            return null; 
        } else {
            List<V_ELE> listFromDelegate = new ArrayList<>();
            for (V_ELE item : iterableFromDelegate) {
                listFromDelegate.add(item);
            }
            CacheEntry<List<V_ELE>> newEntry = CacheEntry.clean(new ArrayList<>(listFromDelegate));
            CacheEntry<List<V_ELE>> oldL1 = l1Cache.put(currentKey, newEntry);
            if (oldL1 != null) backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());
            backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
            return new ArrayList<>(listFromDelegate);
        }
    }

    @Override
    public void update(List<V_ELE> values) throws Exception {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        CachePolicy<K, CacheEntry<List<V_ELE>>> l1Cache = getL1CacheForNamespace(currentNamespace);
        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(currentNamespace); // Ensure L2 cache for namespace exists

        CacheEntry<List<V_ELE>> newEntry = CacheEntry.dirty(values != null ? new ArrayList<>(values) : null);
        CacheEntry<List<V_ELE>> oldL1 = l1Cache.put(currentKey, newEntry);
        if (oldL1 != null) backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());
        backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());

        // If L2 had this key, it's now stale due to the L1 update. Remove it.
        CacheEntry<List<V_ELE>> oldL2 = l2Cache.remove(currentKey);
        if (oldL2 != null) backend.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes());
    }

    @Override
    public void addAll(List<V_ELE> values) throws Exception {
        if (values == null || values.isEmpty()) {
            return;
        }
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        CachePolicy<K, CacheEntry<List<V_ELE>>> l1Cache = getL1CacheForNamespace(currentNamespace);
        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(currentNamespace); // Added for L2 invalidation

        List<V_ELE> currentList = null;
        CacheEntry<List<V_ELE>> l1Entry = l1Cache.get(currentKey);
        boolean l1HadIt = false;

        if (l1Entry != null && l1Entry.getValue() != null) {
            currentList = l1Entry.getValue(); 
            l1HadIt = true;
        } else {
            CacheEntry<List<V_ELE>> l2Entry = l2Cache.get(currentKey);
            if (l2Entry != null && l2Entry.getValue() != null) {
                currentList = new ArrayList<>(l2Entry.getValue()); 
                // Report L2 release when its content is read to be modified and moved to L1
                backend.reportCacheMemoryReleased(l2Entry.getEstimatedSizeBytes());
                CacheEntry<List<V_ELE>> oldL2 = l2Cache.remove(currentKey);
                // if (oldL2 != null) backend.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes()); // Already reported
            } else {
                Iterable<V_ELE> iterableFromDelegate = delegateState.get(); 
                if (iterableFromDelegate != null) {
                    currentList = new ArrayList<>();
                    for (V_ELE item : iterableFromDelegate) {
                        currentList.add(item);
                    }
                } else {
                    currentList = new ArrayList<>();
                }
            }
        }
        currentList.addAll(values);
        CacheEntry<List<V_ELE>> newEntry = CacheEntry.dirty(currentList);
        CacheEntry<List<V_ELE>> oldL1 = l1Cache.put(currentKey, newEntry);
        if (l1HadIt && oldL1 != null) { // Only release if L1 actually had an entry that was replaced
            backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());
        }
        backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
         // If L1 didn't have it, but L2 did (and was removed), its memory release was handled.
        // If neither had it, then this is a new add to L1.
    }

    @Override
    public void add(V_ELE value) throws Exception {
        if (value == null) { 
            return;
        }
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        CachePolicy<K, CacheEntry<List<V_ELE>>> l1Cache = getL1CacheForNamespace(currentNamespace);
        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(currentNamespace);

        List<V_ELE> currentList = null;
        CacheEntry<List<V_ELE>> l1Entry = l1Cache.get(currentKey);
        boolean l1HadIt = false;

        if (l1Entry != null && l1Entry.getValue() != null) {
            currentList = l1Entry.getValue();
            l1HadIt = true;
        } else {
            CacheEntry<List<V_ELE>> l2Entry = l2Cache.get(currentKey);
            if (l2Entry != null && l2Entry.getValue() != null) {
                currentList = new ArrayList<>(l2Entry.getValue());
                // Report L2 release
                backend.reportCacheMemoryReleased(l2Entry.getEstimatedSizeBytes());
                CacheEntry<List<V_ELE>> oldL2 = l2Cache.remove(currentKey);
                // if (oldL2 != null) backend.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes()); // Already reported
            } else {
                Iterable<V_ELE> iterableFromDelegate = delegateState.get();
                if (iterableFromDelegate != null) {
                    currentList = new ArrayList<>();
                    for (V_ELE item : iterableFromDelegate) {
                        currentList.add(item);
                    }
                } else {
                    currentList = new ArrayList<>();
                }
            }
        }
        currentList.add(value);
        CacheEntry<List<V_ELE>> newEntry = CacheEntry.dirty(currentList);
        CacheEntry<List<V_ELE>> oldL1 = l1Cache.put(currentKey, newEntry);
        if (l1HadIt && oldL1 != null) {
             backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());
        }
        backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
    }

    @Override
    public void clear() {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        CachePolicy<K, CacheEntry<List<V_ELE>>> l1Cache = getL1CacheForNamespace(currentNamespace);
        CacheEntry<List<V_ELE>> oldL1 = l1Cache.remove(currentKey);
        if (oldL1 != null) backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());

        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(currentNamespace);
        CacheEntry<List<V_ELE>> oldL2 = l2Cache.remove(currentKey);
        if (oldL2 != null) backend.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes());

        delegateState.clear();
    }

    @Override
    public void flushToUnderlyingState() throws IOException {
        for (Map.Entry<N, CachePolicy<K, CacheEntry<List<V_ELE>>>> nsEntry : namespaceCachesL1
                .entrySet()) {
            N namespace = nsEntry.getKey();
            CachePolicy<K, CacheEntry<List<V_ELE>>> l1Cache = nsEntry.getValue();
            this.setCurrentNamespace(namespace);

            // Iterate over a copy of keys to avoid ConcurrentModificationException
            java.util.List<K> keysToFlush = new java.util.ArrayList<>();
            for (Map.Entry<K, CacheEntry<List<V_ELE>>> entry : l1Cache.entrySet()) {
                keysToFlush.add(entry.getKey());
            }

            for (K key : keysToFlush) {
                CacheEntry<List<V_ELE>> entry = l1Cache.get(key); // Re-fetch in case it was evicted by another operation during iteration prep
                if (entry != null && entry.isDirty()) {
                    List<V_ELE> listValue = entry.getValue();
                    try {
                        // Temporarily set context for delegate state operation
                        K originalKey = backend.getCurrentKey();
                        N originalNamespace = this.getCurrentNamespace(); // Caching state's current
                        // NS

                        if (key != null) { // Guard against null key
                            backend.setCurrentKey(key);
                            this.setCurrentNamespace(namespace); // Sets on CachingListState and
                            // delegate

                            delegateState.update(listValue);
                            entry.setDirty(false); // Mark as clean

                            // Optionally move to L2 after successful flush
                            CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache =
                                    getL2CacheForNamespace(namespace);
                            // Ensure listValue is not null before creating a new ArrayList for L2
                            if (listValue != null) {
                                l2Cache.put(key, CacheEntry.clean(new ArrayList<>(listValue)));
                            } else {
                                l2Cache.put(key, CacheEntry.clean(null)); // explicitly cache null if listValue was null
                            }

                            // Restore context
                            backend.setCurrentKey(originalKey);
                            this.setCurrentNamespace(originalNamespace);
                        } else {
                            // Handle or log the case where key is null, if necessary.
                            // System.err.println("Skipping flush for null key in namespace: " + namespace);
                        }
                    } catch (Exception e) { // Catch Exception from delegateState.update()
                        throw new IOException("Failed to flush dirty list entry for key: " + key
                                + " in namespace: " + namespace, e);
                    }
                }
            }
        }
        // Consider clearing L1 caches after flushing, or let LRU manage them.
        // L2 caches only hold clean data, so no flush needed for L2 itself.
    }

    @Override
    public InternalListState<K, N, V_ELE> getDelegateState() {
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
    public TypeSerializer<List<V_ELE>> getValueSerializer() {
        return delegateState.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        this.currentNamespace = namespace;
        delegateState.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<List<V_ELE>> safeValueSerializer) throws Exception {
        // Similar to CachingInternalMapState, direct serialized value access bypasses
        // caching logic
        // and is complex to make cache-aware for ListState as well.
        // The cache holds List<V_ELE>, not its serialized form.
        throw new UnsupportedOperationException(
                "getSerializedValue directly is not supported by CachingInternalListState.");
    }

    @Override
    public StateIncrementalVisitor<K, N, List<V_ELE>> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        try {
            flushToUnderlyingState(); // Ensure consistency before direct delegate access
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush caches before creating state visitor", e);
        }
        return delegateState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    @Override
    public List<V_ELE> getInternal() throws Exception {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace(); // this.currentNamespace

        CachePolicy<K, CacheEntry<List<V_ELE>>> l1Cache = getL1CacheForNamespace(currentNamespace);
        CacheEntry<List<V_ELE>> l1Entry = l1Cache.get(currentKey);
        if (l1Entry != null) {
            return l1Entry.getValue(); // Return direct from L1
        }

        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(currentNamespace);
        CacheEntry<List<V_ELE>> l2Entry = l2Cache.get(currentKey);
        if (l2Entry != null) {
            // Report L2 release, then remove, then add to L1 (which reports L1 add & any L1 eviction release)
            backend.reportCacheMemoryReleased(l2Entry.getEstimatedSizeBytes());
            l2Cache.remove(currentKey); // Remove from L2
            List<V_ELE> listFromL2 = l2Entry.getValue();
            
            CacheEntry<List<V_ELE>> entryToL1 = CacheEntry.clean(listFromL2);
            CacheEntry<List<V_ELE>> oldL1FromPut = l1Cache.put(currentKey, entryToL1);
            if(oldL1FromPut != null) backend.reportCacheMemoryReleased(oldL1FromPut.getEstimatedSizeBytes());
            backend.reportCacheMemoryAdded(entryToL1.getEstimatedSizeBytes());
            return listFromL2; 
        }

        // Fetch from delegate
        List<V_ELE> listFromDelegate = delegateState.getInternal(); // Use getInternal for direct
                                                                    // list access

        if (listFromDelegate != null) {
            // Store as is in L1 (no copy, as it's internal access)
            l1Cache.put(currentKey, CacheEntry.clean(listFromDelegate));
        }
        return listFromDelegate; // Return direct from delegate
    }

    @Override
    public void updateInternal(List<V_ELE> valueToStore) throws Exception {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace(); // this.currentNamespace

        CachePolicy<K, CacheEntry<List<V_ELE>>> l1Cache = getL1CacheForNamespace(currentNamespace);
        // Store the provided list directly, mark dirty
        l1Cache.put(currentKey, CacheEntry.dirty(valueToStore));

        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(currentNamespace);
        l2Cache.remove(currentKey); // Invalidate L2
    }

    public N getCurrentNamespace() {
        if (currentNamespace == null) {
            throw new IllegalStateException(
                    "Namespace has not been set. Typically, you should call "
                            + "setCurrentNamespace" + " first.");
        }
        return currentNamespace;
    }

    // Implementation for InternalMergingState
    @Override
    public void mergeNamespaces(N targetNamespace, java.util.Collection<N> sourceNamespaces)
            throws Exception {
        // Delegate the actual state merge to the wrapped state
        delegateState.mergeNamespaces(targetNamespace, sourceNamespaces);

        // Cache management: Invalidate caches for merged namespaces.
        // A more complex strategy would try to merge cached entries, but invalidation
        // is
        // safer/simpler.
        if (targetNamespace != null) {
            clearCacheForNamespace(targetNamespace);
        }
        if (sourceNamespaces != null) {
            for (N sourceNamespace : sourceNamespaces) {
                if (sourceNamespace != null) {
                    clearCacheForNamespace(sourceNamespace);
                }
            }
        }
    }

    private void clearCacheForNamespace(N namespace) {
        CachePolicy<K, CacheEntry<List<V_ELE>>> l1Cache = namespaceCachesL1.get(namespace);
        if (l1Cache != null) {
            l1Cache.clear();
        }
        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = namespaceCachesL2.get(namespace);
        if (l2Cache != null) {
            // Report memory for all entries in L2 before clearing
            for (Map.Entry<K, CacheEntry<List<V_ELE>>> entry : l2Cache.entrySet()) {
                if (entry.getValue() != null) {
                    backend.reportCacheMemoryReleased(entry.getValue().getEstimatedSizeBytes());
                }
            }
            l2Cache.clear();
        }
        // Removing from the top-level map ensures the namespace-specific maps are eligible for GC
        // And their eviction listeners (if any on namespaceCachesL1/L2 themselves) would have fired.
        namespaceCachesL1.remove(namespace);
        namespaceCachesL2.remove(namespace);
    }

    @Override
    public long evictEntriesToFreeMemory(long targetBytesToFreeThisState) {
        long bytesFreed = 0;
        if (targetBytesToFreeThisState <= 0) return 0;

        List<N> l2Namespaces = new ArrayList<>();
        for (Map.Entry<N, CachePolicy<K, CacheEntry<List<V_ELE>>>> entry : namespaceCachesL2.entrySet()) {
            l2Namespaces.add(entry.getKey());
        }

        for (N namespace : l2Namespaces) {
            CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = namespaceCachesL2.get(namespace);
            if (l2Cache == null || l2Cache.isEmpty()) continue; // Optimization: skip empty L2 caches
            Iterator<Map.Entry<K, CacheEntry<List<V_ELE>>>> l2Iter = l2Cache.entrySet().iterator();
            while (l2Iter.hasNext() && bytesFreed < targetBytesToFreeThisState) {
                Map.Entry<K, CacheEntry<List<V_ELE>>> entry = l2Iter.next();
                CacheEntry<List<V_ELE>> cacheValue = entry.getValue();
                long estimatedSize = cacheValue.getEstimatedSizeBytes();
                l2Iter.remove(); // This should trigger l2Cache's own eviction listener if it had one that reports memory.
                                 // If not, explicit reporting is needed. TinyLFUMap/LRUMap do call listener on removeEldest.
                                 // For explicit remove(), listener might not be called by underlying map.
                backend.reportCacheMemoryReleased(estimatedSize);
                bytesFreed += estimatedSize;
            }
            if (bytesFreed >= targetBytesToFreeThisState) return bytesFreed;
        }

        List<N> l1Namespaces = new ArrayList<>();
        for (Map.Entry<N, CachePolicy<K, CacheEntry<List<V_ELE>>>> entry : namespaceCachesL1.entrySet()) {
            l1Namespaces.add(entry.getKey());
        }

        for (N namespace : l1Namespaces) {
            CachePolicy<K, CacheEntry<List<V_ELE>>> l1Cache = namespaceCachesL1.get(namespace);
            if (l1Cache == null || l1Cache.isEmpty()) continue; // Optimization: skip empty L1 caches

            Iterator<Map.Entry<K, CacheEntry<List<V_ELE>>>> l1IterClean = l1Cache.entrySet().iterator();
            List<Map.Entry<K, CacheEntry<List<V_ELE>>>> dirtyL1EntriesToConsider = new ArrayList<>();
            while (l1IterClean.hasNext() && bytesFreed < targetBytesToFreeThisState) {
                Map.Entry<K, CacheEntry<List<V_ELE>>> entry = l1IterClean.next();
                CacheEntry<List<V_ELE>> cacheValue = entry.getValue();
                if (!cacheValue.isDirty()) {
                    long estimatedSize = cacheValue.getEstimatedSizeBytes();
                    l1IterClean.remove(); // Similar to L2, explicit remove may not trigger listener for memory reporting.
                    backend.reportCacheMemoryReleased(estimatedSize);
                    bytesFreed += estimatedSize;
                } else {
                    dirtyL1EntriesToConsider.add(entry);
                }
            }
            if (bytesFreed >= targetBytesToFreeThisState) return bytesFreed;

            Iterator<Map.Entry<K, CacheEntry<List<V_ELE>>>> dirtyIter = dirtyL1EntriesToConsider.iterator();
            while (dirtyIter.hasNext() && bytesFreed < targetBytesToFreeThisState) {
                Map.Entry<K, CacheEntry<List<V_ELE>>> dirtyEntryTuple = dirtyIter.next();
                K key = dirtyEntryTuple.getKey();
                CacheEntry<List<V_ELE>> dirtyEntry = dirtyEntryTuple.getValue();
                List<V_ELE> listValue = dirtyEntry.getValue();
                long estimatedSize = dirtyEntry.getEstimatedSizeBytes();
                try {
                    N originalCurrentNamespace = this.currentNamespace;
                    K originalBackendKey = backend.getCurrentKey();
                    backend.setCurrentKey(key);
                    setCurrentNamespace(namespace);
                    delegateState.update(listValue);
                    dirtyEntry.setDirty(false);
                    setCurrentNamespace(originalCurrentNamespace);
                    backend.setCurrentKey(originalBackendKey);

                    l1Cache.remove(key); // Remove from the per-namespace L1 cache.
                    backend.reportCacheMemoryReleased(estimatedSize);
                    bytesFreed += estimatedSize;
                } catch (Exception e) {
                    // Log or handle
                }
            }
            if (bytesFreed >= targetBytesToFreeThisState) return bytesFreed;
        }
        return bytesFreed;
    }
}
