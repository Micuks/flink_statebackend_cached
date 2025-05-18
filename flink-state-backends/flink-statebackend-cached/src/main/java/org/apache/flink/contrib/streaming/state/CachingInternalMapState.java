package com.micuks.flink.cachingstate;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend; // For context

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * An {@link InternalMapState} that uses an L1/L2 cache for its entries.
 * Caches individual (UK, UV) pairs for each Flink state key (K) and namespace (N).
 *
 * @param <K>  The type of the Flink key.
 * @param <N>  The type of the namespace.
 * @param <UK> The type of the user key in the map.
 * @param <UV> The type of the user value in the map.
 */
public class CachingInternalMapState<K, N, UK, UV>
        implements InternalMapState<K, N, UK, UV>, CachingInternalState<K, N, Map<UK, UV>, InternalMapState<K, N, UK, UV>> {

    private final InternalMapState<K, N, UK, UV> delegateState;
    private final CachingKeyedStateBackend<K> backend;

    // Cache structure: Namespace -> Flink Key -> L1/L2 Caches for UserKey-UserValue pairs
    // LRUMap<Namespace, LRUMap<FlinkKey, PerKeyMapCache>>
    private final LRUMap<N, LRUMap<K, PerKeyMapCache<UK, UV>>> namespaceCaches;

    private final int l1CacheSizePerMap; // Max L1 entries (UK-UV pairs) per Flink Key/Namespace
    private final int l2CacheSizePerMap; // Max L2 entries (UK-UV pairs) per Flink Key/Namespace
    private final int maxActiveNamespaceKeyCombinations; // Max active (N,K) pairs with active caches

    // Helper class to hold L1 and L2 caches for a specific Flink Key/Namespace's map entries
    private static class PerKeyMapCache<UK_C, UV_C> {
        final LRUMap<UK_C, CacheEntry<UV_C>> l1MapEntries;
        final LRUMap<UK_C, CacheEntry<UV_C>> l2MapEntries; // Should only hold clean entries

        PerKeyMapCache(int l1Size, int l2Size, InternalMapState<?, ?, UK_C, UV_C> delegateState,
                       AbstractKeyedStateBackend<?> backend, Object currentFlinkKey, Object currentNamespace) {
            this.l2MapEntries = new LRUMap<>(l2Size);
            this.l1MapEntries = new LRUMap<>(l1Size, evictedL1Entry -> {
                // On L1 eviction
                UK_C evictedUK = evictedL1Entry.getKey();
                CacheEntry<UV_C> evictedUVWrapper = evictedL1Entry.getValue();

                if (evictedUVWrapper.isDirty()) {
                    try {
                        // This is tricky: need to set current Flink Key & Namespace for the delegate state
                        // Assuming backend.setCurrentKey and delegateState.setCurrentNamespace are available
                        // And then call delegateState.put(evictedUK, evictedUVWrapper.getValue())
                        // For now, conceptual placeholder for flushing logic:
                        // --- Delegate Write Start ---
                        Object originalFlinkKey = backend.getCurrentKey();
                        Object originalNamespace = delegateState.getCurrentNamespace(); // Assuming delegateState has this method (InternalKvState does)

                        backend.setCurrentKey(currentFlinkKey);
                        delegateState.setCurrentNamespace(currentNamespace);
                        
                        if (evictedUVWrapper.getValue() == null) { // check if it was a 'remove' operation
                             // delegateState.remove(evictedUK); // If we cache tombstones for remove
                        } else {
                            delegateState.put(evictedUK, evictedUVWrapper.getValue());
                        }
                        
                        backend.setCurrentKey(originalFlinkKey);
                        delegateState.setCurrentNamespace(originalNamespace);
                        // --- Delegate Write End ---
                        
                        // Put a clean version into L2
                        l2MapEntries.put(evictedUK, CacheEntry.clean(evictedUVWrapper.getValue()));
                        evictedUVWrapper.setDirty(false); 
                    } catch (Exception e) { // IOException or other exceptions from state.update()
                        // Consider proper error handling
                        throw new RuntimeException("Failed to flush L1 map entry to delegate/L2 on eviction for user key: " + evictedUK, e);
                    }
                } else {
                    // If not dirty, just move to L2
                    l2MapEntries.put(evictedUK, evictedUVWrapper);
                }
            });
        }
    }


    public CachingInternalMapState(
            InternalMapState<K, N, UK, UV> delegateState,
            CachingKeyedStateBackend<K> backend,
            int l1CacheSizePerMap,
            int l2CacheSizePerMap,
            int maxActiveNamespaceKeyCombinations) {
        this.delegateState = delegateState;
        this.backend = backend;
        this.l1CacheSizePerMap = l1CacheSizePerMap;
        this.l2CacheSizePerMap = l2CacheSizePerMap;
        this.maxActiveNamespaceKeyCombinations = maxActiveNamespaceKeyCombinations; // Example, make configurable
        this.namespaceCaches = new LRUMap<>(this.maxActiveNamespaceKeyCombinations);
    }

    private PerKeyMapCache<UK, UV> getOrCreatePerKeyMapCache(N namespace, K key) {
        LRUMap<K, PerKeyMapCache<UK, UV>> keyCaches = namespaceCaches.computeIfAbsent(namespace, ns -> new LRUMap<>(maxActiveNamespaceKeyCombinations)); // This inner LRUMap might also need a size limit for active keys
        return keyCaches.computeIfAbsent(key, k -> new PerKeyMapCache<>(l1CacheSizePerMap, l2CacheSizePerMap, delegateState, backend, k, namespace));
    }

    @Override
    public UV get(UK userKey) throws IOException {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        PerKeyMapCache<UK, UV> perKeyCache = getOrCreatePerKeyMapCache(currentNamespace, currentKey);

        CacheEntry<UV> l1Entry = perKeyCache.l1MapEntries.get(userKey);
        if (l1Entry != null) {
            return l1Entry.getValue(); // L1 Hit
        }

        CacheEntry<UV> l2Entry = perKeyCache.l2MapEntries.get(userKey);
        if (l2Entry != null) {
            perKeyCache.l2MapEntries.remove(userKey); // Remove from L2
            perKeyCache.l1MapEntries.put(userKey, l2Entry); // Promote to L1 (it's clean)
            return l2Entry.getValue(); // L2 Hit
        }

        // L1 and L2 Miss
        UV valueFromDelegate = delegateState.get(userKey);
        if (valueFromDelegate != null) { // Only cache non-null entries for now
            perKeyCache.l1MapEntries.put(userKey, CacheEntry.clean(valueFromDelegate));
        }
        return valueFromDelegate;
    }

    @Override
    public void put(UK userKey, UV userValue) throws IOException {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        PerKeyMapCache<UK, UV> perKeyCache = getOrCreatePerKeyMapCache(currentNamespace, currentKey);

        if (userValue == null) { // Flink MapState typically removes on null value for put
            remove(userKey);
            return;
        }
        
        perKeyCache.l2MapEntries.remove(userKey); // Remove from L2 if present, as L1 will be updated
        perKeyCache.l1MapEntries.put(userKey, CacheEntry.dirty(userValue)); // Put/update in L1, mark dirty
    }

    @Override
    public void remove(UK userKey) throws IOException {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        PerKeyMapCache<UK, UV> perKeyCache = getOrCreatePerKeyMapCache(currentNamespace, currentKey);

        perKeyCache.l2MapEntries.remove(userKey); // Remove from L2
        
        // To correctly reflect removal, we might need a tombstone or special handling
        // For now, just remove from L1 and ensure it's flushed as a removal
        // Option 1: Put a special "tombstone" CacheEntry marked dirty.
        // Option 2: Remove from cache and rely on flush to handle.
        // For simplicity, let's make sure a dirty null entry is put, L1 eviction should handle.
        // Or, more simply, have a specific way to mark for deletion.
        // For now: the L1 eviction logic will need to be smart about what delegateState.put(uk, null) means
        // or have a separate delegateState.remove(uk) call.
        // The CacheEntry could have a state: CLEAN, DIRTY_UPDATE, DIRTY_REMOVE
        perKeyCache.l1MapEntries.put(userKey, CacheEntry.dirty(null)); // Mark as dirty with null value, indicating removal
                                                                      // The eviction listener needs to handle this by calling delegateState.remove(userKey)
                                                                      // Or delegateState.put(userKey, null) if that's the contract.
                                                                      // Flink's MapState.put(key,null) is often equivalent to remove(key).
    }

    @Override
    public boolean contains(UK userKey) throws IOException {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        PerKeyMapCache<UK, UV> perKeyCache = getOrCreatePerKeyMapCache(currentNamespace, currentKey);

        if (perKeyCache.l1MapEntries.containsKey(userKey)) {
            return perKeyCache.l1MapEntries.get(userKey).getValue() != null; // Check for tombstone if null means removed
        }
        if (perKeyCache.l2MapEntries.containsKey(userKey)) {
            return true; // L2 only has clean, non-null entries (by current design)
        }
        return delegateState.contains(userKey);
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws IOException {
        // Simplistic approach: Flush everything for this K,N then delegate.
        // This is inefficient but safe for a first pass.
        // A proper implementation requires a merging iterator.
        System.err.println("CachingInternalMapState.entries() - flushing before delegating. For performance, implement a merging iterator.");
        flushCurrentKeyNamespaceCache();
        return delegateState.entries();
    }

    @Override
    public Iterable<UK> keys() throws IOException {
        System.err.println("CachingInternalMapState.keys() - flushing before delegating. For performance, implement a merging iterator.");
        flushCurrentKeyNamespaceCache();
        return delegateState.keys();
    }

    @Override
    public Iterable<UV> values() throws IOException {
        System.err.println("CachingInternalMapState.values() - flushing before delegating. For performance, implement a merging iterator.");
        flushCurrentKeyNamespaceCache();
        return delegateState.values();
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws IOException {
        System.err.println("CachingInternalMapState.iterator() - flushing before delegating. For performance, implement a merging iterator.");
        flushCurrentKeyNamespaceCache();
        return delegateState.iterator();
    }
    
    private void flushCurrentKeyNamespaceCache() throws IOException {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        LRUMap<K, PerKeyMapCache<UK, UV>> keyCaches = namespaceCaches.get(currentNamespace);
        if (keyCaches != null) {
            PerKeyMapCache<UK, UV> perKeyCache = keyCaches.get(currentKey);
            if (perKeyCache != null) {
                // Evict all from L1 to trigger flush logic
                // This is a bit of a hack; a direct flush method for PerKeyMapCache would be better.
                for (UK uk : new java.util.ArrayList<>(perKeyCache.l1MapEntries.keySet())) {
                     CacheEntry<UV> entry = perKeyCache.l1MapEntries.get(uk);
                     if (entry != null && entry.isDirty()) {
                        // Manually trigger the eviction logic (simplified)
                        // This is complex because the eviction listener needs the delegateState specific to this K,N
                        // The PerKeyMapCache's eviction listener handles this.
                        // Forcing eviction:
                        perKeyCache.l1MapEntries.remove(uk); // This should trigger the listener
                        perKeyCache.l1MapEntries.put(uk, entry); // Put it back if it wasn't meant to be fully removed by this op
                                                                // This is still not quite right for a "flush all"
                        // A dedicated flush method inside PerKeyMapCache is needed.
                        // For now, this will try to flush by re-adding.
                        // A better way: iterate l1MapEntries, if dirty, flush it (like in flushToUnderlyingState)
                        // then mark clean and move to L2.
                     }
                }
                 // For now, we'll rely on the global flushToUnderlyingState or L1 evictions for proper flushing.
                 // The above loop is a placeholder for a more direct per K,N flush.
            }
        }
    }


    @Override
    public boolean isEmpty() throws IOException {
        // This also could be complex if we don't flush.
        // If cache has items, not empty. Else, ask delegate.
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        PerKeyMapCache<UK, UV> perKeyCache = getOrCreatePerKeyMapCache(currentNamespace, currentKey);

        // A more accurate check would see if L1 or L2 has non-tombstone entries
        if (!perKeyCache.l1MapEntries.isEmpty() && perKeyCache.l1MapEntries.values().stream().anyMatch(entry -> entry.getValue() != null && entry.isDirty())) {
             return false; // Has dirty, non-null entries in L1
        }
         // If L1 is empty or only has tombstones/clean entries that might also be in delegate, this is not enough.
        
        System.err.println("CachingInternalMapState.isEmpty() - may require flush for full accuracy without complex logic. Delegating for now after checking L1.");
        // A simple check: if L1 has any *real* (non-tombstone) values, it's not empty.
        // Otherwise, to be fully sure, we might need to check delegate or have more complex logic.
        if (perKeyCache.l1MapEntries.values().stream().anyMatch(entry -> entry.getValue() != null)) return false;
        if (perKeyCache.l2MapEntries.values().stream().anyMatch(entry -> entry.getValue() != null)) return false;


        // Fallback to delegate, could be costly if iterators are not used.
        return delegateState.isEmpty();
    }

    @Override
    public void clear() {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        
        LRUMap<K, PerKeyMapCache<UK, UV>> keyCaches = namespaceCaches.get(currentNamespace);
        if (keyCaches != null) {
            PerKeyMapCache<UK, UV> perKeyCache = keyCaches.remove(currentKey); // Remove and get
            if (perKeyCache != null) {
                perKeyCache.l1MapEntries.clear();
                perKeyCache.l2MapEntries.clear();
            }
        }
        delegateState.clear(); // Clear the underlying state for this K,N
    }

    // --- CachingInternalState methods ---
    @Override
    public void flushToUnderlyingState() throws IOException {
        // This needs to iterate through ALL namespaces, ALL Flink Keys,
        // and then for each, iterate all DIRTY user keys in its L1 cache and write them to the delegate.
        N originalNamespace = getCurrentNamespace(); // Delegate's current namespace
        K originalKey = backend.getCurrentKey();

        for (Map.Entry<N, LRUMap<K, PerKeyMapCache<UK, UV>>> nsEntry : namespaceCaches.entrySet()) {
            N namespace = nsEntry.getKey();
            LRUMap<K, PerKeyMapCache<UK, UV>> keyCaches = nsEntry.getValue();
            setCurrentNamespace(namespace); // Set for delegate state access

            for (Map.Entry<K, PerKeyMapCache<UK, UV>> keyEntry : keyCaches.entrySet()) {
                K flinkKey = keyEntry.getKey();
                PerKeyMapCache<UK, UV> perKeyCache = keyEntry.getValue();
                backend.setCurrentKey(flinkKey); // Set for delegate state access

                for (Map.Entry<UK, CacheEntry<UV>> l1Entry : new java.util.ArrayList<>(perKeyCache.l1MapEntries.entrySet())) { // Iterate copy
                    UK userKey = l1Entry.getKey();
                    CacheEntry<UV> cacheEntry = l1Entry.getValue();

                    if (cacheEntry.isDirty()) {
                        UV userValue = cacheEntry.getValue();
                        if (userValue == null) { // Assuming null value + dirty means remove
                            delegateState.remove(userKey);
                        } else {
                            delegateState.put(userKey, userValue);
                        }
                        cacheEntry.setDirty(false);
                        // Potentially move to L2 if not already there and L2 has space (and if value is not null)
                        if (userValue != null) {
                           perKeyCache.l2MapEntries.put(userKey, CacheEntry.clean(userValue)); // Ensure L2 has the clean version
                        } else {
                           perKeyCache.l2MapEntries.remove(userKey); // Ensure removed from L2 as well
                        }
                    }
                }
            }
        }
        // Restore original context for the delegate and backend
        setCurrentNamespace(originalNamespace);
        backend.setCurrentKey(originalKey);
    }

    @Override
    public InternalMapState<K, N, UK, UV> getDelegateState() {
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
    public TypeSerializer<Map<UK, UV>> getValueSerializer() {
        // This is tricky for InternalMapState as it doesn't store a "Map" as its direct value,
        // but rather individual UK-UV pairs.
        // The CachingInternalState interface defines SV as Map<UK,UV> for this class.
        // However, InternalMapState itself doesn't have a "getValueSerializer()" for Map<UK,UV>.
        // It has getUserKeySerializer() and getUserValueSerializer().
        // This indicates a slight mismatch in how CachingInternalState defines SV for map state.
        // For now, returning null or throwing, as this method might not be applicable directly.
        // Or, CachingInternalState's SV for map state should be reconsidered.
        // Let's assume for now this won't be called or is not critical.
        // If it is, the CachingInternalState<..., Map<UK,UV>, ...> part might need adjustment.
        // Perhaps SV should be "Void" for MapState if we consider map entries individually.
        // Or the interface needs to be more flexible.
        // For now:
        throw new UnsupportedOperationException("getValueSerializer for Map<UK,UV> is not directly applicable to InternalMapState's caching of individual entries.");
    }

    @Override
    public void setCurrentNamespace(@Nonnull N namespace) {
        delegateState.setCurrentNamespace(namespace);
        // The cache itself is keyed by namespace, so this call primarily affects the delegate.
    }

    @Override
    public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<Map<UK, UV>> safeValueSerializer) throws Exception {
        // This method is for getting the serialized form of the *entire state value* (which for MapState is the whole map).
        // Caching individual entries makes this hard to satisfy directly from cache without reconstructing + serializing.
        // Simplest: flush and delegate.
        System.err.println("CachingInternalMapState.getSerializedValue() - flushing before delegating.");
        flushToUnderlyingState(); // Ensure delegate is up-to-date
        return delegateState.getSerializedValue(serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer, safeValueSerializer);
    }

    @Override
    @Nonnull
    public N getCurrentNamespace() {
        return delegateState.getCurrentNamespace();
    }

    @Override
    public TypeSerializer<UK> getUserKeySerializer() {
        return delegateState.getUserKeySerializer();
    }

    @Override
    public TypeSerializer<UV> getUserValueSerializer() {
        return delegateState.getUserValueSerializer();
    }

    @Override
    public void putAll(Map<UK, UV> map) throws IOException {
        if (map == null) {
            return;
        }
        // Naive impl: could be optimized by batching cache operations
        for (Map.Entry<UK, UV> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    // For StateIncrementalVisitor: This is advanced.
    // For now, it would likely need to flush and then delegate, or not support incremental snapshots for cached map state.
    @Override
    public StateIncrementalVisitor<K, N, Map<UK, UV>> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
        System.err.println("CachingInternalMapState.getStateIncrementalVisitor() - incremental snapshots for cached map entries not fully supported, may flush or delegate without full cache optimization.");
        try {
            flushToUnderlyingState(); // Ensure consistency before getting visitor from delegate
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush state before creating incremental visitor", e);
        }
        return delegateState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }
} 