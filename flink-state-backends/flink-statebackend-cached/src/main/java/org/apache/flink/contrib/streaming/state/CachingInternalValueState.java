package com.micuks.flink.cachingstate;

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend; // For context
// Assuming LRUMap and CacheEntry are in the same package (default for now)

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;

/**
 * An {@link InternalValueState} that uses an L1/L2 cache for its values.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class CachingInternalValueState<K, N, V>
        implements InternalValueState<K, N, V>, CachingInternalState<K, N, V, InternalValueState<K, N, V>> {

    private final InternalValueState<K, N, V> delegateState;
    private final CachingKeyedStateBackend<K> backend; // For accessing current key
    private final LRUMap<N, LRUMap<K, CacheEntry<V>>> namespaceCachesL1; // Namespace -> Key -> L1 CacheEntry
    private final LRUMap<N, LRUMap<K, CacheEntry<V>>> namespaceCachesL2; // Namespace -> Key -> L2 CacheEntry

    private final int l1CacheSizePerKeyPerNamespace;
    private final int l2CacheSizePerKeyPerNamespace;
    private final int maxActiveNamespacesInCache;
    private final long maxCacheMemoryMb;

    public CachingInternalValueState(
            InternalValueState<K, N, V> delegateState,
            CachingKeyedStateBackend<K> backend,
            int l1CacheSize, 
            int l2CacheSize,
            long maxCacheMemoryMb
    ) {
        this.delegateState = delegateState;
        this.backend = backend;
        this.l1CacheSizePerKeyPerNamespace = l1CacheSize; 
        this.l2CacheSizePerKeyPerNamespace = l2CacheSize; 
        this.maxActiveNamespacesInCache = 100; // Example, make configurable
        this.maxCacheMemoryMb = maxCacheMemoryMb; // Initialize field

        this.namespaceCachesL1 = new LRUMap<>(maxActiveNamespacesInCache);
        this.namespaceCachesL2 = new LRUMap<>(maxActiveNamespacesInCache);
    }

    // Method for CachingKeyedStateBackend to access the delegate for registration checks
    @Override
    public InternalValueState<K, N, V> getDelegateState() {
        return delegateState;
    }

    private LRUMap<K, CacheEntry<V>> getL1CacheForNamespace(N namespace) {
        return namespaceCachesL1.computeIfAbsent(namespace, ns -> 
            new LRUMap<>(l1CacheSizePerKeyPerNamespace, evictedL1Entry -> {
                LRUMap<K, CacheEntry<V>> l2Cache = getL2CacheForNamespace(ns);
                K evictedKey = evictedL1Entry.getKey();
                CacheEntry<V> evictedValueWrapper = evictedL1Entry.getValue();
                V evictedValue = evictedValueWrapper.getValue();

                if (evictedValueWrapper.isDirty()) {
                    try {
                        N originalNamespace = getCurrentNamespace();
                        K originalKey = backend.getCurrentKey();
                        
                        backend.setCurrentKey(evictedKey);
                        setCurrentNamespace(ns); 
                        delegateState.update(evictedValue);
                        
                        backend.setCurrentKey(originalKey); // Restore
                        setCurrentNamespace(originalNamespace); // Restore

                        l2Cache.put(evictedKey, CacheEntry.clean(evictedValue)); 
                        evictedValueWrapper.setDirty(false); // It's now clean in L2 context
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to flush L1 entry to delegate/L2 on eviction for key: " + evictedKey, e);
                    }
                } else {
                     l2Cache.put(evictedKey, evictedValueWrapper); 
                }
            })
        );
    }

    private LRUMap<K, CacheEntry<V>> getL2CacheForNamespace(N namespace) {
        return namespaceCachesL2.computeIfAbsent(namespace, ns -> 
            new LRUMap<>(l2CacheSizePerKeyPerNamespace) // L2 eviction doesn't trigger further writes here
        );
    }

    @Override
    public V value() throws IOException {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        LRUMap<K, CacheEntry<V>> l1Cache = getL1CacheForNamespace(currentNamespace);
        CacheEntry<V> l1Entry = l1Cache.get(currentKey);

        if (l1Entry != null) {
            return l1Entry.getValue();
        }

        LRUMap<K, CacheEntry<V>> l2Cache = getL2CacheForNamespace(currentNamespace);
        CacheEntry<V> l2Entry = l2Cache.get(currentKey);

        if (l2Entry != null) {
            l2Cache.remove(currentKey); 
            l1Cache.put(currentKey, l2Entry); 
            return l2Entry.getValue();
        }

        V valueFromDelegate = delegateState.value();
        if (valueFromDelegate != null) { // Only cache non-null, Flink state differentiates null from empty
            l1Cache.put(currentKey, CacheEntry.clean(valueFromDelegate));
        }
        return valueFromDelegate;
    }

    @Override
    public void update(V value) throws IOException {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        
        if (value == null) { // As per Flink ValueState contract
            clear();
            return;
        }

        LRUMap<K, CacheEntry<V>> l1Cache = getL1CacheForNamespace(currentNamespace);
        l1Cache.put(currentKey, CacheEntry.dirty(value));
        
        // If L2 had this key, it's now stale, remove it.
        LRUMap<K, CacheEntry<V>> l2Cache = getL2CacheForNamespace(currentNamespace);
        l2Cache.remove(currentKey);
    }

    @Override
    public void clear() {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        LRUMap<K, CacheEntry<V>> l1Cache = getL1CacheForNamespace(currentNamespace);
        l1Cache.remove(currentKey);

        LRUMap<K, CacheEntry<V>> l2Cache = getL2CacheForNamespace(currentNamespace);
        l2Cache.remove(currentKey);

        delegateState.clear(); // Clear the underlying state
    }
    
    @Override
    public void flushToUnderlyingState() throws IOException {
        N originalNamespace = getCurrentNamespace();
        K originalKey = backend.getCurrentKey();
        boolean keyWasSet = originalKey != null; // Check if key was actually set

        for (Map.Entry<N, LRUMap<K, CacheEntry<V>>> nsEntry : namespaceCachesL1.entrySet()) {
            N namespace = nsEntry.getKey();
            LRUMap<K, CacheEntry<V>> l1Cache = nsEntry.getValue();
            setCurrentNamespace(namespace); 

            // Iterate over a copy of keys to avoid ConcurrentModificationException if map is modified by L1 eviction
            for (K key : new java.util.ArrayList<>(l1Cache.keySet())) { 
                CacheEntry<V> entry = l1Cache.get(key); // Re-fetch, as it might have been evicted then re-added
                if (entry != null && entry.isDirty()) {
                    V value = entry.getValue();
                    backend.setCurrentKey(key); 
                    delegateState.update(value); 
                    entry.setDirty(false); 
                    // After successful flush, entry in L1 is clean. L2 is write-through on L1 eviction.
                }
            }
        }
        setCurrentNamespace(originalNamespace);
        if(keyWasSet) backend.setCurrentKey(originalKey);
        else backend.setCurrentKey(null); // Or whatever Flink expects for un-setting a key
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
        // Set for the delegate, the cache keying already uses the namespace.
        delegateState.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<V> safeValueSerializer) throws Exception {
        return delegateState.getSerializedValue(serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer, safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
        return delegateState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    @Override
    @Nonnull
    public N getCurrentNamespace() {
        return delegateState.getCurrentNamespace();
    }
} 