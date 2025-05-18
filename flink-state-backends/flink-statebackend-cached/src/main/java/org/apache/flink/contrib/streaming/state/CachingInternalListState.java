package com.micuks.flink.cachingstate;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map; // For flushToUnderlyingState iteration

/**
 * An {@link InternalListState} that uses an L1/L2 cache for its entire list content.
 *
 * @param <K>   The type of the Flink key.
 * @param <N>   The type of the namespace.
 * @param <V_ELE> The type of the elements in the list.
 */
public class CachingInternalListState<K, N, V_ELE>
        implements InternalListState<K, N, V_ELE>, CachingInternalState<K, N, List<V_ELE>, InternalListState<K, N, V_ELE>> {

    private final InternalListState<K, N, V_ELE> delegateState;
    private final CachingKeyedStateBackend<K> backend;

    // Cache structure: Namespace -> Flink Key -> L1/L2 CacheEntry for the List<V_ELE>
    private final LRUMap<N, LRUMap<K, CacheEntry<List<V_ELE>>>> namespaceCachesL1;
    private final LRUMap<N, LRUMap<K, CacheEntry<List<V_ELE>>>> namespaceCachesL2;

    private final int l1CacheSizePerNamespaceKey; // Max L1 entries (Lists) per Flink Key/Namespace
    private final int l2CacheSizePerNamespaceKey; // Max L2 entries (Lists) per Flink Key/Namespace
    private final int maxActiveNamespacesInCache; // Max active Namespaces with L1/L2 caches

    public CachingInternalListState(
            InternalListState<K, N, V_ELE> delegateState,
            CachingKeyedStateBackend<K> backend,
            int l1CacheSizePerNamespaceKey,
            int l2CacheSizePerNamespaceKey,
            int maxActiveNamespacesInCache) {
        this.delegateState = delegateState;
        this.backend = backend;
        this.l1CacheSizePerNamespaceKey = l1CacheSizePerNamespaceKey;
        this.l2CacheSizePerNamespaceKey = l2CacheSizePerNamespaceKey;
        this.maxActiveNamespacesInCache = maxActiveNamespacesInCache;

        this.namespaceCachesL1 = new LRUMap<>(this.maxActiveNamespacesInCache);
        this.namespaceCachesL2 = new LRUMap<>(this.maxActiveNamespacesInCache);
    }

    private LRUMap<K, CacheEntry<List<V_ELE>>> getL1CacheForNamespace(N namespace) {
        return namespaceCachesL1.computeIfAbsent(namespace, ns ->
                new LRUMap<>(l1CacheSizePerNamespaceKey, evictedL1Entry -> {
                    LRUMap<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(ns);
                    K evictedKey = evictedL1Entry.getKey();
                    CacheEntry<List<V_ELE>> evictedListWrapper = evictedL1Entry.getValue();

                    if (evictedListWrapper.isDirty()) {
                        try {
                            N originalNamespace = getCurrentNamespace();
                            K originalKey = backend.getCurrentKey();

                            backend.setCurrentKey(evictedKey);
                            setCurrentNamespace(ns);
                            delegateState.update(evictedListWrapper.getValue());

                            backend.setCurrentKey(originalKey);
                            setCurrentNamespace(originalNamespace);

                            l2Cache.put(evictedKey, CacheEntry.clean(evictedListWrapper.getValue()));
                            evictedListWrapper.setDirty(false);
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to flush L1 list entry to delegate/L2 on eviction for key: " + evictedKey, e);
                        }
                    } else {
                        l2Cache.put(evictedKey, evictedListWrapper);
                    }
                }));
    }

    private LRUMap<K, CacheEntry<List<V_ELE>>> getL2CacheForNamespace(N namespace) {
        return namespaceCachesL2.computeIfAbsent(namespace, ns -> new LRUMap<>(l2CacheSizePerNamespaceKey));
    }

    @Override
    public List<V_ELE> get() throws IOException {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        LRUMap<K, CacheEntry<List<V_ELE>>> l1Cache = getL1CacheForNamespace(currentNamespace);
        CacheEntry<List<V_ELE>> l1Entry = l1Cache.get(currentKey);

        if (l1Entry != null) {
            // Return a copy to prevent external modification of cached list if mutable
            return l1Entry.getValue() == null ? null : new ArrayList<>(l1Entry.getValue());
        }

        LRUMap<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(currentNamespace);
        CacheEntry<List<V_ELE>> l2Entry = l2Cache.get(currentKey);

        if (l2Entry != null) {
            l2Cache.remove(currentKey);
            l1Cache.put(currentKey, l2Entry); // Promote to L1 (it's clean)
            return l2Entry.getValue() == null ? null : new ArrayList<>(l2Entry.getValue());
        }

        List<V_ELE> listFromDelegate = delegateState.get();
        if (listFromDelegate != null) { // Cache even empty lists, but not null lists unless intended
            l1Cache.put(currentKey, CacheEntry.clean(new ArrayList<>(listFromDelegate))); // Store a copy
        }
        return listFromDelegate; // Original list from delegate
    }

    @Override
    public void add(V_ELE value) throws IOException {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        LRUMap<K, CacheEntry<List<V_ELE>>> l1Cache = getL1CacheForNamespace(currentNamespace);

        CacheEntry<List<V_ELE>> l1Entry = l1Cache.get(currentKey);
        List<V_ELE> list;
        if (l1Entry != null) {
            list = l1Entry.getValue();
        } else {
            // Check L2 before fetching from delegate
            LRUMap<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(currentNamespace);
            CacheEntry<List<V_ELE>> l2Entry = l2Cache.get(currentKey);
            if (l2Entry != null) {
                l2Cache.remove(currentKey); // Will be promoted to L1
                list = l2Entry.getValue();
            } else {
                list = delegateState.get(); // Fetch from delegate
            }
            list = (list == null) ? new ArrayList<>() : new ArrayList<>(list); // Work with a mutable copy
        }
        
        if (list == null) list = new ArrayList<>(); // Ensure list is not null
        list.add(value);
        l1Cache.put(currentKey, CacheEntry.dirty(list));
        // Ensure L2 is cleared for this key if it existed, as L1 now has the dirty version
        getL2CacheForNamespace(currentNamespace).remove(currentKey);
    }

    @Override
    public void addAll(List<V_ELE> values) throws IOException {
        if (values == null || values.isEmpty()) {
            return;
        }
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        LRUMap<K, CacheEntry<List<V_ELE>>> l1Cache = getL1CacheForNamespace(currentNamespace);

        CacheEntry<List<V_ELE>> l1Entry = l1Cache.get(currentKey);
        List<V_ELE> list;
        if (l1Entry != null) {
            list = l1Entry.getValue();
        } else {
            LRUMap<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(currentNamespace);
            CacheEntry<List<V_ELE>> l2Entry = l2Cache.get(currentKey);
            if (l2Entry != null) {
                l2Cache.remove(currentKey);
                list = l2Entry.getValue();
            } else {
                list = delegateState.get();
            }
            list = (list == null) ? new ArrayList<>() : new ArrayList<>(list);
        }
        if (list == null) list = new ArrayList<>();
        list.addAll(values);
        l1Cache.put(currentKey, CacheEntry.dirty(list));
        getL2CacheForNamespace(currentNamespace).remove(currentKey);
    }

    @Override
    public void update(List<V_ELE> values) throws IOException {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        LRUMap<K, CacheEntry<List<V_ELE>>> l1Cache = getL1CacheForNamespace(currentNamespace);
        
        // Ensure we are caching a copy
        List<V_ELE> listToCache = (values == null) ? null : new ArrayList<>(values);
        l1Cache.put(currentKey, CacheEntry.dirty(listToCache)); 
        // If list is null, it means clear, which is handled by Flink by updating to null / empty list
        // Or should call clear() explicitly? InternalListState.update(null) often means clear.
        // For now, assume update(null) is valid and cache it as dirty null.

        getL2CacheForNamespace(currentNamespace).remove(currentKey);
    }

    @Override
    public Iterable<V_ELE> getList() throws IOException {
        return get(); // Reuses the get() method which returns a copy
    }

    @Override
    public void clear() {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        LRUMap<K, CacheEntry<List<V_ELE>>> l1Cache = getL1CacheForNamespace(currentNamespace);
        l1Cache.remove(currentKey);
        LRUMap<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(currentNamespace);
        l2Cache.remove(currentKey);

        delegateState.clear();
    }

    // --- CachingInternalState methods ---
    @Override
    public void flushToUnderlyingState() throws IOException {
        N originalNamespace = getCurrentNamespace();
        K originalKey = backend.getCurrentKey();

        for (Map.Entry<N, LRUMap<K, CacheEntry<List<V_ELE>>>> nsEntry : namespaceCachesL1.entrySet()) {
            N namespace = nsEntry.getKey();
            LRUMap<K, CacheEntry<List<V_ELE>>> l1Cache = nsEntry.getValue();
            setCurrentNamespace(namespace); // Set for delegate state access

            for (K key : new ArrayList<>(l1Cache.keySet())) { // Iterate copy of keyset
                CacheEntry<List<V_ELE>> entry = l1Cache.get(key);
                if (entry != null && entry.isDirty()) {
                    backend.setCurrentKey(key);
                    delegateState.update(entry.getValue());
                    entry.setDirty(false);
                    // After successful flush, entry in L1 is clean. Optionally move to L2.
                    // The L1 eviction listener handles L2 promotion.
                    // Here, we can explicitly update L2 if desired:
                    if (entry.getValue() != null) { // Don't put null lists in L2 if they mean "cleared"
                        getL2CacheForNamespace(namespace).put(key, CacheEntry.clean(new ArrayList<>(entry.getValue())));
                    }
                }
            }
        }
        setCurrentNamespace(originalNamespace);
        backend.setCurrentKey(originalKey);
    }

    @Override
    public InternalListState<K, N, V_ELE> getDelegateState() {
        return delegateState;
    }

    // --- InternalKvState methods ---
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
        return delegateState.getValueSerializer(); // InternalListState has this
    }

    @Override
    public void setCurrentNamespace(@Nonnull N namespace) {
        delegateState.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<List<V_ELE>> safeValueSerializer) throws Exception {
        // Flush and delegate is safest for now if we cache the List object directly.
        System.err.println("CachingInternalListState.getSerializedValue() - flushing before delegating.");
        flushToUnderlyingState(); // Ensure delegate is up-to-date
        return delegateState.getSerializedValue(serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer, safeValueSerializer);
    }

    @Override
    @Nonnull
    public N getCurrentNamespace() {
        return delegateState.getCurrentNamespace();
    }

    @Override
    public StateIncrementalVisitor<K, N, List<V_ELE>> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
        System.err.println("CachingInternalListState.getStateIncrementalVisitor() - flushing before delegating for simplicity.");
        try {
            flushToUnderlyingState();
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush state before creating incremental visitor", e);
        }
        return delegateState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }
} 