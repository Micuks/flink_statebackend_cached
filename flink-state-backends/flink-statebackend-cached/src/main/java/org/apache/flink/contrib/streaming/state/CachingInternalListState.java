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

        this.namespaceCachesL1 = createCachePolicy(this.maxActiveNamespacesInCache);
        this.namespaceCachesL2 = createCachePolicy(this.maxActiveNamespacesInCache);
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
                // TODO: Add eviction listener support to TinyLFU or adapt
                return new TinyLFUMap<>(capacity);
            case LRU:
            default:
                return new LRUMap<>(capacity, evictionListener);
        }
    }

    private CachePolicy<K, CacheEntry<List<V_ELE>>> getL1CacheForNamespace(N namespace) {
        return namespaceCachesL1.computeIfAbsent(namespace,
                ns -> createCachePolicyWithEvictionListener(l1CacheSizePerNamespace,
                        evictedL1Entry -> { // Eviction from L1
                            // for namespace
                            // 'ns'
                    CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(ns);
                    K evictedKey = evictedL1Entry.getKey();
                    CacheEntry<List<V_ELE>> evictedListWrapper = evictedL1Entry.getValue();
                    List<V_ELE> evictedList = evictedListWrapper.getValue();

                    if (evictedListWrapper.isDirty()) {
                        try {
                            N originalNamespaceContextForDelegate = getCurrentNamespace();
                            K originalKeyContextForDelegate = backend.getCurrentKey();

                            backend.setCurrentKey(evictedKey);
                            this.setCurrentNamespace(ns); // Use the namespace of the L1 cache being
                                                          // processed

                            delegateState.update(evictedList);

                            evictedListWrapper.setDirty(false); // Mark as clean
                            // Add to L2 as clean after successful update
                            if (evictedList != null) { // Should not be null if dirty, but check for
                                                       // safety for L2
                                l2Cache.put(evictedKey,
                                        CacheEntry.clean(new ArrayList<>(evictedList)));
                            }

                            // Restore context
                            backend.setCurrentKey(originalKeyContextForDelegate);
                            this.setCurrentNamespace(originalNamespaceContextForDelegate);

                        } catch (Exception e) {
                            throw new RuntimeException(
                                    "Failed to flush L1 list entry to delegate/L2 on eviction for key: "
                                            + evictedKey + " in ns: " + ns,
                                    e);
                        }
                    } else {
                        // If it wasn't dirty, and it's not null, try to move to L2.
                        // CacheEntry in L2 should always be clean.
                        if (evictedList != null) {
                            l2Cache.put(evictedKey, CacheEntry.clean(new ArrayList<>(evictedList)));
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
            // If L1 has an entry, it's the source of truth.
            // If value is null, it means it was explicitly set to null.
            // If non-null (could be empty list), return a copy.
            return value != null ? new ArrayList<>(value) : null; // MODIFIED: if null in cache, return null
        }

        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(currentNamespace);
        CacheEntry<List<V_ELE>> l2Entry = l2Cache.get(currentKey);

        if (l2Entry != null) {
            l2Cache.remove(currentKey); // Remove from L2
            List<V_ELE> listFromL2 = l2Entry.getValue(); // This is a clean copy from L2

            // Promote to L1. If listFromL2 is null, CacheEntry.clean(null) is put.
            l1Cache.put(currentKey, CacheEntry.clean(listFromL2 != null ? new ArrayList<>(listFromL2) : null));
            return listFromL2 != null ? new ArrayList<>(listFromL2) : null; // Return copy or null
        }

        // Not in L1 or L2, fetch from delegate
        Iterable<V_ELE> iterableFromDelegate = delegateState.get();

        if (iterableFromDelegate == null) {
            // Delegate returned null. Cache and return null.
            l1Cache.put(currentKey, CacheEntry.clean(null)); // Cache null (as clean)
            return null; // Return null
        } else {
            // Delegate returned something. Convert to list.
            List<V_ELE> listFromDelegate = new ArrayList<>();
            for (V_ELE item : iterableFromDelegate) {
                listFromDelegate.add(item);
            }
            // Store a new copy in L1, marked as clean.
            l1Cache.put(currentKey, CacheEntry.clean(new ArrayList<>(listFromDelegate)));
            // Return a new copy to the user.
            return new ArrayList<>(listFromDelegate);
        }
    }

    @Override
    public void update(List<V_ELE> values) throws Exception {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        CachePolicy<K, CacheEntry<List<V_ELE>>> l1Cache = getL1CacheForNamespace(currentNamespace);
        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(currentNamespace);

        if (values == null) {
            // Mark the entry as dirty with a null value in L1.
            // This signifies that the state should become null, and this change needs to be
            // flushed.
            l1Cache.put(currentKey, CacheEntry.dirty(null));
            // Invalidate L2 for this key, as L1 now holds the most up-to-date (pending null) state.
            l2Cache.remove(currentKey);
            // The actual clear/update(null) on the delegateState will happen upon L1 eviction
            // or an explicit flushToUnderlyingState(), consistent with other dirty entries.
        } else {
            // Store a mutable copy of the new list, marked as dirty, in L1.
            l1Cache.put(currentKey, CacheEntry.dirty(new ArrayList<>(values)));
            // Invalidate L2 for this key.
            l2Cache.remove(currentKey);
        }
    }

    @Override
    public void addAll(List<V_ELE> values) throws Exception {
        if (values == null || values.isEmpty()) {
            return;
        }
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        CachePolicy<K, CacheEntry<List<V_ELE>>> l1Cache = getL1CacheForNamespace(currentNamespace);

        CacheEntry<List<V_ELE>> l1Entry = l1Cache.get(currentKey);
        List<V_ELE> currentList;
        if (l1Entry != null && l1Entry.getValue() != null) {
            currentList = l1Entry.getValue(); // This is already a mutable copy held by L1
        } else {
            // Try L2 or delegate
            CacheEntry<List<V_ELE>> l2Entry =
                    getL2CacheForNamespace(currentNamespace).get(currentKey);
            if (l2Entry != null && l2Entry.getValue() != null) {
                currentList = new ArrayList<>(l2Entry.getValue()); // Copy from L2
                getL2CacheForNamespace(currentNamespace).remove(currentKey); // Invalidate L2, will
                // be promoted to L1
            } else {
                Iterable<V_ELE> iterableFromDelegate = delegateState.get(); // Fetch from delegate
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
        l1Cache.put(currentKey, CacheEntry.dirty(currentList)); // Mark L1 as dirty
    }

    @Override
    public void add(V_ELE value) throws Exception {
        if (value == null) { // Or throw, depending on ListState spec for adding nulls
            return;
        }
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        CachePolicy<K, CacheEntry<List<V_ELE>>> l1Cache = getL1CacheForNamespace(currentNamespace);

        CacheEntry<List<V_ELE>> l1Entry = l1Cache.get(currentKey);
        List<V_ELE> currentList;
        if (l1Entry != null && l1Entry.getValue() != null) {
            currentList = l1Entry.getValue();
        } else {
            CacheEntry<List<V_ELE>> l2Entry =
                    getL2CacheForNamespace(currentNamespace).get(currentKey);
            if (l2Entry != null && l2Entry.getValue() != null) {
                currentList = new ArrayList<>(l2Entry.getValue());
                getL2CacheForNamespace(currentNamespace).remove(currentKey);
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
        l1Cache.put(currentKey, CacheEntry.dirty(currentList));
    }

    @Override
    public void clear() {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        CachePolicy<K, CacheEntry<List<V_ELE>>> l1Cache = getL1CacheForNamespace(currentNamespace);
        l1Cache.remove(currentKey);

        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(currentNamespace);
        l2Cache.remove(currentKey);

        delegateState.clear();
    }

    @Override
    public void flushToUnderlyingState() throws IOException {
        N currentNamespaceForFlush = getCurrentNamespace();
        K currentKeyForFlush = backend.getCurrentKey();

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
                CacheEntry<List<V_ELE>> entry = l1Cache.get(key); // Re-fetch
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
            l2Cache.remove(currentKey); // Remove from L2
            List<V_ELE> listFromL2 = l2Entry.getValue();
            // Promote to L1 as is (no copy, as it's internal access)
            l1Cache.put(currentKey, CacheEntry.clean(listFromL2));
            return listFromL2; // Return direct from L2
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
            l2Cache.clear();
        }
        // Removing from the top-level map ensures the namespace-specific maps are eligible for GC
        namespaceCachesL1.remove(namespace);
        namespaceCachesL2.remove(namespace);
    }
}
