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
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalKvState.StateIncrementalVisitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An {@link InternalListState} that uses an L1/L2 cache with write-behind + incremental flush for its list values.
 *
 * <p>Design: Each (Flink key, namespace) pair is cached as a {@link DirtyBufferEntry}, which holds:
 * <ul>
 *   <li>{@code flushedList}: the fully-flushed base list stored in RocksDB (may be null).</li>
 *   <li>{@code dirtyBuffer}: new elements appended since the last flush (never null).</li>
 * </ul>
 *
 * <p>This design enables {@code add()} to append to {@code dirtyBuffer} without reading the flushed base from
 * RocksDB. On checkpoint or eviction, {@code dirtyBuffer} is flushed via {@code addAll()} (which leverages
 * RocksDB's merge semantics for O(append) writes), rather than reading and serializing the full list.
 * This eliminates the O(N) deserialize cost from the hot {@code add()} path.
 *
 * @param <K> The type of the Flink key.
 * @param <N> The type of the namespace.
 * @param <V_ELE> The type of elements in the list.
 */
public class CachingInternalListState<K, N, V_ELE>
        implements InternalListState<K, N, V_ELE>,
                CachingInternalState<K, N, List<V_ELE>, InternalListState<K, N, V_ELE>> {

    private final InternalListState<K, N, V_ELE> delegateState;
    private final CachingKeyedStateBackend<K> backend;
    private N currentNamespace;

    // L1: Namespace -> Flink Key -> DirtyBufferEntry<V_ELE>
    private final CachePolicy<N, CachePolicy<K, DirtyBufferEntry<V_ELE>>> namespaceCachesL1;
    // L2: Namespace -> Flink Key -> CacheEntry<List<V_ELE>>
    private final CachePolicy<N, CachePolicy<K, CacheEntry<List<V_ELE>>>> namespaceCachesL2;
    // Cached (namespace, l1Cache, l2Cache) tuples to avoid repeated computeIfAbsent + eviction listener setup
    private final Map<N, NamespaceCachePair> namespaceCachePairCache;

    /**
     * Holds the L1 and L2 per-namespace caches as an immutable pair, created atomically
     * under the namespace cache's eviction lock.
     */
    private static final class NamespaceCachePair {
        final CachePolicy<?, ?> l1Cache;
        final CachePolicy<?, ?> l2Cache;
        NamespaceCachePair(Object l1, Object l2) {
            this.l1Cache = (CachePolicy<?, ?>) l1;
            this.l2Cache = (CachePolicy<?, ?>) l2;
        }
    }

    private final int l1CacheSizePerNamespace;
    private final int l2CacheSizePerNamespace;
    private final int maxActiveNamespacesInCache;
    private final CachingStateBackendFactory.CachePolicyType cachePolicyType;
    private final int maxElementsPerEntry;
    private final int incrementalFlushThreshold;
    private final boolean cachingEnabled;

    // Debug counter: logs config once every 10,000 add() calls
    private static final AtomicLong addCounter = new AtomicLong(0);

    public CachingInternalListState(
            InternalListState<K, N, V_ELE> delegateState,
            CachingKeyedStateBackend<K> backend,
            int l1CacheSize,
            int l2CacheSize,
            int maxActiveNamespaces,
            CachingStateBackendFactory.CachePolicyType cachePolicyType,
            int maxElementsPerEntry,
            int incrementalFlushThreshold,
            boolean cachingEnabled) {
        this.delegateState = delegateState;
        this.backend = backend;
        this.l1CacheSizePerNamespace = l1CacheSize;
        this.l2CacheSizePerNamespace = l2CacheSize;
        this.maxActiveNamespacesInCache = maxActiveNamespaces;
        this.cachePolicyType = cachePolicyType;
        this.maxElementsPerEntry = maxElementsPerEntry;
        this.incrementalFlushThreshold = incrementalFlushThreshold;
        this.cachingEnabled = cachingEnabled;

        if (!cachingEnabled) {
            // When caching is disabled, use no-op cache policies that immediately evict entries.
            // This effectively bypasses the cache while keeping the wrapper structure intact.
            this.namespaceCachesL1 = createNamespaceCachePolicyWithL1EvictionListener(1, null);
            this.namespaceCachesL2 = createNamespaceCachePolicy(1, null);
            this.namespaceCachePairCache = null;
        } else {
            this.namespaceCachesL1 = createNamespaceCachePolicyWithL1EvictionListener(
                    this.maxActiveNamespacesInCache,
                    evictedNamespaceL1Entry -> {
                        N evictedNamespace = evictedNamespaceL1Entry.getKey();
                        CachePolicy<K, DirtyBufferEntry<V_ELE>> evictedPerNsL1Cache = evictedNamespaceL1Entry.getValue();
                        try {
                            flushAndParkL1NamespaceCacheEntries(evictedNamespace, evictedPerNsL1Cache);
                        } catch (Exception e) {
                            throw new RuntimeException(
                                    "Failed to flush/park L1 entries for evicted namespace: " + evictedNamespace, e);
                        }
                    });

            this.namespaceCachesL2 = createNamespaceCachePolicy(
                    this.maxActiveNamespacesInCache,
                    evictedNamespaceL2Entry -> {
                        N evictedNamespace = evictedNamespaceL2Entry.getKey();
                        CachePolicy<K, CacheEntry<List<V_ELE>>> evictedPerNsL2Cache = evictedNamespaceL2Entry.getValue();
                        if (evictedPerNsL2Cache != null) {
                            // Also remove corresponding L1 entries to keep L1/L2 consistent.
                            CachePolicy<K, DirtyBufferEntry<V_ELE>> l1Cache = namespaceCachesL1.get(evictedNamespace);
                            for (Map.Entry<K, CacheEntry<List<V_ELE>>> entry : evictedPerNsL2Cache.entrySet()) {
                                if (entry.getValue() != null) {
                                    this.backend.reportCacheMemoryReleased(entry.getValue().getEstimatedSizeBytes());
                                }
                                // Remove the corresponding L1 entry to prevent stale L1 references.
                                if (l1Cache != null) {
                                    DirtyBufferEntry<V_ELE> l1Entry = l1Cache.get(entry.getKey());
                                    if (l1Entry != null) {
                                        this.backend.reportCacheMemoryReleased(l1Entry.getEstimatedSizeBytes());
                                        l1Cache.remove(entry.getKey());
                                    }
                                }
                            }
                        }
                    });
            this.namespaceCachePairCache = new IdentityHashMap<>();
        }
    }

    // --- Cache policy factory methods ---

    private <CK, CV> CachePolicy<CK, CV> createCachePolicy(int capacity) {
        switch (cachePolicyType) {
            case TINYLFU:
                return new TinyLFUMap<>(capacity);
            case LRU:
            default:
                return new LRUMap<>(capacity);
        }
    }

    private <CK, CV> CachePolicy<CK, CV> createCachePolicy(int capacity, Consumer<Map.Entry<CK, CV>> evictionListener) {
        switch (cachePolicyType) {
            case TINYLFU:
                return new TinyLFUMap<>(capacity, evictionListener);
            case LRU:
            default:
                return new LRUMap<>(capacity, evictionListener);
        }
    }

    private <CK, CV> CachePolicy<CK, CV> createNamespaceCachePolicy(
            int maxNamespaces, Consumer<Map.Entry<CK, CV>> namespaceEvictionListener) {
        switch (cachePolicyType) {
            case TINYLFU:
                return new TinyLFUMap<>(maxNamespaces, namespaceEvictionListener);
            case LRU:
            default:
                return new LRUMap<>(maxNamespaces, namespaceEvictionListener);
        }
    }

    private CachePolicy<N, CachePolicy<K, DirtyBufferEntry<V_ELE>>>
            createNamespaceCachePolicyWithL1EvictionListener(
                    int maxNamespaces,
                    Consumer<Map.Entry<N, CachePolicy<K, DirtyBufferEntry<V_ELE>>>> namespaceEvictionListener) {
        switch (cachePolicyType) {
            case TINYLFU:
                return new TinyLFUMap<>(maxNamespaces, namespaceEvictionListener);
            case LRU:
            default:
                return new LRUMap<>(maxNamespaces, namespaceEvictionListener);
        }
    }

    // --- Namespace-level cache access (optimized: single computeIfAbsent for both L1 and L2) ---

    /**
     * Returns both L1 and L2 per-namespace caches in one call.
     * Uses a cached pair to avoid repeated computeIfAbsent + eviction listener creation overhead.
     * When cachingEnabled is false, returns null pair (delegates bypass cache).
     */
    @SuppressWarnings("unchecked")
    private NamespaceCachePair getNamespaceCaches(N namespace) {
        if (!cachingEnabled) {
            return null;
        }
        Map<N, NamespaceCachePair> pairCache = namespaceCachePairCache;
        NamespaceCachePair pair = pairCache.get(namespace);
        if (pair != null) {
            return pair;
        }
        CachePolicy<K, DirtyBufferEntry<V_ELE>> l1 = getL1CacheForNamespace(namespace);
        CachePolicy<K, CacheEntry<List<V_ELE>>> l2 = getL2CacheForNamespace(namespace);
        pair = new NamespaceCachePair(l1, l2);
        pairCache.put(namespace, pair);
        return pair;
    }

    private CachePolicy<K, DirtyBufferEntry<V_ELE>> getL1CacheForNamespace(N namespace) {
        return namespaceCachesL1.computeIfAbsent(
                namespace,
                ns -> createCachePolicy(
                        l1CacheSizePerNamespace,
                            evictedL1Entry -> {
                            K evictedKey = evictedL1Entry.getKey();
                            DirtyBufferEntry<V_ELE> evictedEntry = evictedL1Entry.getValue();
                            CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(ns);
                            backend.reportCacheMemoryReleased(evictedEntry.getEstimatedSizeBytes());

                            // CRITICAL: Always flush dirty entries before eviction to prevent data loss.
                            // This is the only safe way to evict L1 entries — we must persist dirty data to
                            // RocksDB (via the incremental flush path) before dropping the entry.
                            // Phase 4 optimization is REVERTED: do NOT skip dirty flush on eviction.
                            if (evictedEntry.isDirty()) {
                                try {
                                    K origKey = backend.getCurrentKey();
                                    N origNs = getCurrentNamespace();
                                    try {
                                        backend.setCurrentKey(evictedKey);
                                        setCurrentNamespace(ns);
                                        incrementalFlushToDelegate(evictedKey, ns, evictedEntry, l2Cache);
                                    } finally {
                                        backend.setCurrentKey(origKey);
                                        setCurrentNamespace(origNs);
                                    }
                                } catch (Exception e) {
                                    throw new RuntimeException(
                                            "Failed to flush dirty entry during L1 eviction for key: " + evictedKey, e);
                                }
                            }

                            // Park the flushed/merged list into L2 for future L2-hit reads.
                            List<V_ELE> flushedSnapshot = evictedEntry.getMergedList();
                            if (flushedSnapshot != null && !flushedSnapshot.isEmpty()) {
                                CacheEntry<List<V_ELE>> l2Entry = CacheEntry.clean(flushedSnapshot);
                                CacheEntry<List<V_ELE>> old = l2Cache.put(evictedKey, l2Entry);
                                if (old != null) {
                                    backend.reportCacheMemoryReleased(old.getEstimatedSizeBytes());
                                }
                                backend.reportCacheMemoryAdded(l2Entry.getEstimatedSizeBytes());
                            } else {
                                l2Cache.remove(evictedKey);
                            }
                        }));
    }

    private CachePolicy<K, CacheEntry<List<V_ELE>>> getL2CacheForNamespace(N namespace) {
        return namespaceCachesL2.computeIfAbsent(namespace, ns -> createCachePolicy(l2CacheSizePerNamespace));
    }

    // --- Incremental flush helper ---

    /**
     * Incrementally flushes a single dirty entry to RocksDB.
     * - For append entries (dirtyBuffer non-empty): uses addAll (RocksDB merge semantics) for O(append) writes.
     * - For full-replace entries (isUpdated=true, dirtyBuffer empty): uses update() to write the new list.
     * After flushing, merges dirtyBuffer into flushedList and updates L2 cache.
     */
    private void incrementalFlushToDelegate(
            K key, N namespace, DirtyBufferEntry<V_ELE> entry,
            CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache) throws Exception {
        if (entry == null || !entry.isDirty()) {
            return;
        }

        if (entry.isUpdated()) {
            // Full-replace entry: flushedList contains the new complete state, dirtyBuffer is empty
            // (but may have been appended to via add() after the update()).
            // Write the merged list via update() (or clear() if null/empty).
            List<V_ELE> merged = entry.getMergedList();
            if (merged != null && !merged.isEmpty()) {
                delegateState.update(merged);
            } else {
                delegateState.clear();
            }
            // Apply truncation to the merged state.
            if (maxElementsPerEntry > 0) {
                entry.truncateToMaxSize(maxElementsPerEntry);
            }
            // Reset entry to clean state.
            List<V_ELE> finalList = entry.getMergedList();
            entry.resetToFlushed(finalList);
            // Update L2 with the fully-flushed snapshot (including truncation).
            if (finalList != null && !finalList.isEmpty()) {
                CacheEntry<List<V_ELE>> l2Entry = CacheEntry.clean(finalList);
                CacheEntry<List<V_ELE>> old = l2Cache.put(key, l2Entry);
                if (old != null) {
                    backend.reportCacheMemoryReleased(old.getEstimatedSizeBytes());
                }
                backend.reportCacheMemoryAdded(l2Entry.getEstimatedSizeBytes());
            } else {
                l2Cache.remove(key);
            }
        } else {
            // Append entry: dirtyBuffer has new elements. Use addAll (RocksDB merge semantics).
            if (!entry.getDirtyBuffer().isEmpty()) {
                delegateState.addAll(entry.getDirtyBuffer());
            }
            entry.mergeDirtyIntoFlushed();
            // Apply truncation before updating L2.
            if (maxElementsPerEntry > 0) {
                entry.truncateToMaxSize(maxElementsPerEntry);
            }
            List<V_ELE> fullyFlushed = entry.getMergedList();
            CacheEntry<List<V_ELE>> l2Entry = CacheEntry.clean(fullyFlushed);
            CacheEntry<List<V_ELE>> old = l2Cache.put(key, l2Entry);
            if (old != null) {
                backend.reportCacheMemoryReleased(old.getEstimatedSizeBytes());
            }
            backend.reportCacheMemoryAdded(l2Entry.getEstimatedSizeBytes());
        }
    }

    // --- Namespace-level flush (on namespace eviction) ---

    private void flushAndParkL1NamespaceCacheEntries(
            N namespace, CachePolicy<K, DirtyBufferEntry<V_ELE>> perNsL1Cache) throws Exception {
        if (perNsL1Cache == null) {
            return;
        }
        CachePolicy<K, CacheEntry<List<V_ELE>>> perNsL2Cache = getL2CacheForNamespace(namespace);
        K originalKeyContext = backend.getCurrentKey();
        N originalNamespaceContext = getCurrentNamespace();

        try {
            List<K> keysToFlush = new ArrayList<>();
            for (Map.Entry<K, DirtyBufferEntry<V_ELE>> e : perNsL1Cache.entrySet()) {
                keysToFlush.add(e.getKey());
            }

            for (K key : keysToFlush) {
                DirtyBufferEntry<V_ELE> entry = perNsL1Cache.get(key);
                if (entry == null) {
                    continue;
                }
                if (entry.isDirty()) {
                    backend.setCurrentKey(key);
                    setCurrentNamespace(namespace);
                    incrementalFlushToDelegate(key, namespace, entry, perNsL2Cache);
                }
                // Remove empty entries from L1 to release memory.
                if (entry.getMergedList() == null) {
                    perNsL1Cache.remove(key);
                }
            }
        } finally {
            backend.setCurrentKey(originalKeyContext);
            setCurrentNamespace(originalNamespaceContext);
        }
    }

    // --- Public API: get() ---

    @Override
    public Iterable<V_ELE> get() throws Exception {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        NamespaceCachePair pair = getNamespaceCaches(currentNamespace);
        if (pair == null) {
            // Caching disabled: delegate directly.
            Iterable<V_ELE> result = delegateState.get();
            if (result == null) {
                return null;
            }
            List<V_ELE> list = new ArrayList<>();
            for (V_ELE item : result) {
                list.add(item);
            }
            return list;
        }
        @SuppressWarnings("unchecked")
        CachePolicy<K, DirtyBufferEntry<V_ELE>> l1Cache = (CachePolicy<K, DirtyBufferEntry<V_ELE>>) pair.l1Cache;
        @SuppressWarnings("unchecked")
        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = (CachePolicy<K, CacheEntry<List<V_ELE>>>) pair.l2Cache;

        DirtyBufferEntry<V_ELE> entry = l1Cache != null ? l1Cache.get(currentKey) : null;

        if (entry != null) {
            // Return a defensive copy so callers cannot mutate internal cache state.
            List<V_ELE> merged = entry.getMergedList();
            return merged != null ? new ArrayList<>(merged) : null;
        }

        // L1 miss -> try L2
        CacheEntry<List<V_ELE>> l2Entry = l2Cache != null ? l2Cache.get(currentKey) : null;

        if (l2Entry != null && l2Entry.getValue() != null) {
            backend.reportCacheMemoryReleased(l2Entry.getEstimatedSizeBytes());
            l2Cache.remove(currentKey);

            List<V_ELE> l2Value = l2Entry.getValue();
            // Promote to L1 with a clean DirtyBufferEntry (no dirty buffer since L2 holds flushed data).
            DirtyBufferEntry<V_ELE> newEntry =
                    new DirtyBufferEntry<>(new ArrayList<>(l2Value), new ArrayList<>(), false, false);
            DirtyBufferEntry<V_ELE> old = l1Cache.put(currentKey, newEntry);
            if (old != null) {
                backend.reportCacheMemoryReleased(old.getEstimatedSizeBytes());
            }
            backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
            // Return a defensive copy of the promoted value.
            return new ArrayList<>(l2Value);
        }

        // L1 & L2 miss: fetch from RocksDB
        Iterable<V_ELE> iterableFromDelegate = delegateState.get();
        if (iterableFromDelegate == null) {
            DirtyBufferEntry<V_ELE> nullEntry = DirtyBufferEntry.empty();
            DirtyBufferEntry<V_ELE> old = l1Cache.put(currentKey, nullEntry);
            if (old != null) {
                backend.reportCacheMemoryReleased(old.getEstimatedSizeBytes());
            }
            backend.reportCacheMemoryAdded(nullEntry.getEstimatedSizeBytes());
            return null;
        }

        List<V_ELE> listFromDelegate = new ArrayList<>();
        for (V_ELE item : iterableFromDelegate) {
            listFromDelegate.add(item);
        }

        // Cache a clean entry: flushedList holds the data, dirtyBuffer is empty.
        DirtyBufferEntry<V_ELE> newEntry =
                new DirtyBufferEntry<>(new ArrayList<>(listFromDelegate), new ArrayList<>(), false, false);
        DirtyBufferEntry<V_ELE> old = l1Cache.put(currentKey, newEntry);
        if (old != null) {
            backend.reportCacheMemoryReleased(old.getEstimatedSizeBytes());
        }
        backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
        // Return a defensive copy so callers cannot mutate the cached entry.
        return new ArrayList<>(listFromDelegate);
    }

    // --- Public API: add() ---

    @Override
    public void add(V_ELE value) throws Exception {
        // Debug log: print config once every 10,000 add() calls
        long count = addCounter.incrementAndGet();
        if (count % 10000 == 0) {
            System.out.println("[DEBUG ListState] add() #" + count
                + ", incrementalFlushThreshold=" + incrementalFlushThreshold
                + ", cachingEnabled=" + cachingEnabled
                + ", maxElementsPerEntry=" + maxElementsPerEntry);
        }
        if (value == null) {
            return;
        }

        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        NamespaceCachePair pair = getNamespaceCaches(currentNamespace);
        if (pair == null) {
            // Caching disabled or cachingEnabled=false: delegate directly.
            backend.setCurrentKey(currentKey);
            setCurrentNamespace(currentNamespace);
            delegateState.add(value);
            return;
        }
        @SuppressWarnings("unchecked")
        CachePolicy<K, DirtyBufferEntry<V_ELE>> l1Cache = (CachePolicy<K, DirtyBufferEntry<V_ELE>>) pair.l1Cache;
        @SuppressWarnings("unchecked")
        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = (CachePolicy<K, CacheEntry<List<V_ELE>>>) pair.l2Cache;

        DirtyBufferEntry<V_ELE> entry = l1Cache.get(currentKey);
        if (entry == null) {
            entry = DirtyBufferEntry.empty();
            DirtyBufferEntry<V_ELE> old = l1Cache.put(currentKey, entry);
            if (old != null) {
                backend.reportCacheMemoryReleased(old.getEstimatedSizeBytes());
            }
            backend.reportCacheMemoryAdded(entry.getEstimatedSizeBytes());
        }

        long sizeBefore = entry.getEstimatedSizeBytes();
        entry.addToDirty(value);
        if (maxElementsPerEntry > 0) {
            entry.truncateToMaxSize(maxElementsPerEntry);
        }
        backend.reportCacheMemoryAdded(entry.getEstimatedSizeBytes() - sizeBefore);

        // Trigger incremental flush if threshold is reached
        if (incrementalFlushThreshold > 0 && entry.getDirtyBuffer().size() >= incrementalFlushThreshold) {
            K origKey = backend.getCurrentKey();
            N origNs = getCurrentNamespace();
            try {
                backend.setCurrentKey(currentKey);
                setCurrentNamespace(currentNamespace);
                incrementalFlushToDelegate(currentKey, currentNamespace, entry, l2Cache);
            } catch (Exception e) {
                throw new RuntimeException("Failed incremental flush for key: " + currentKey, e);
            } finally {
                backend.setCurrentKey(origKey);
                setCurrentNamespace(origNs);
            }
        }
    }

    // --- Public API: addAll() ---

    @Override
    public void addAll(List<V_ELE> values) throws Exception {
        if (values == null || values.isEmpty()) {
            return;
        }

        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        NamespaceCachePair pair = getNamespaceCaches(currentNamespace);
        if (pair == null) {
            // Caching disabled: delegate directly.
            backend.setCurrentKey(currentKey);
            setCurrentNamespace(currentNamespace);
            delegateState.addAll(values);
            return;
        }
        @SuppressWarnings("unchecked")
        CachePolicy<K, DirtyBufferEntry<V_ELE>> l1Cache = (CachePolicy<K, DirtyBufferEntry<V_ELE>>) pair.l1Cache;
        @SuppressWarnings("unchecked")
        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = (CachePolicy<K, CacheEntry<List<V_ELE>>>) pair.l2Cache;

        DirtyBufferEntry<V_ELE> entry = l1Cache.get(currentKey);
        if (entry == null) {
            entry = DirtyBufferEntry.empty();
            DirtyBufferEntry<V_ELE> old = l1Cache.put(currentKey, entry);
            if (old != null) {
                backend.reportCacheMemoryReleased(old.getEstimatedSizeBytes());
            }
            backend.reportCacheMemoryAdded(entry.getEstimatedSizeBytes());
        }
        long sizeBefore = entry.getEstimatedSizeBytes();
        entry.addAllToDirty(values);
        if (maxElementsPerEntry > 0) {
            entry.truncateToMaxSize(maxElementsPerEntry);
        }
        backend.reportCacheMemoryAdded(entry.getEstimatedSizeBytes() - sizeBefore);

        if (incrementalFlushThreshold > 0 && entry.getDirtyBuffer().size() >= incrementalFlushThreshold) {
            K origKey = backend.getCurrentKey();
            N origNs = getCurrentNamespace();
            try {
                backend.setCurrentKey(currentKey);
                setCurrentNamespace(currentNamespace);
                incrementalFlushToDelegate(currentKey, currentNamespace, entry, l2Cache);
            } catch (Exception e) {
                throw new RuntimeException("Failed incremental flush for key: " + currentKey, e);
            } finally {
                backend.setCurrentKey(origKey);
                setCurrentNamespace(origNs);
            }
        }
    }

    // --- Public API: update() ---

    @Override
    public void update(List<V_ELE> values) throws Exception {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        NamespaceCachePair pair = getNamespaceCaches(currentNamespace);

        if (pair == null) {
            // Caching disabled: delegate directly.
            if (values == null || values.isEmpty()) {
                delegateState.clear();
            } else {
                delegateState.update(values);
            }
            return;
        }

        @SuppressWarnings("unchecked")
        CachePolicy<K, DirtyBufferEntry<V_ELE>> l1Cache = (CachePolicy<K, DirtyBufferEntry<V_ELE>>) pair.l1Cache;
        @SuppressWarnings("unchecked")
        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = (CachePolicy<K, CacheEntry<List<V_ELE>>>) pair.l2Cache;

        List<V_ELE> listToCache = null;
        if (values != null) {
            listToCache = new ArrayList<>(values);
            if (maxElementsPerEntry > 0 && listToCache.size() > maxElementsPerEntry) {
                listToCache = new ArrayList<>(listToCache.subList(listToCache.size() - maxElementsPerEntry, listToCache.size()));
            }
        }

        // Mark as dirty so flushToUnderlyingState() will persist the new state.
        DirtyBufferEntry<V_ELE> newEntry = new DirtyBufferEntry<>(listToCache, new ArrayList<>(), true, true);

        CacheEntry<List<V_ELE>> oldL2 = l2Cache.remove(currentKey);
        if (oldL2 != null) {
            backend.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes());
        }

        DirtyBufferEntry<V_ELE> old = l1Cache.put(currentKey, newEntry);
        if (old != null) {
            backend.reportCacheMemoryReleased(old.getEstimatedSizeBytes());
        }
        backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
    }

    // --- Public API: clear() ---

    @Override
    public void clear() {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        NamespaceCachePair pair = getNamespaceCaches(currentNamespace);
        if (pair != null) {
            @SuppressWarnings("unchecked")
            CachePolicy<K, DirtyBufferEntry<V_ELE>> l1Cache = (CachePolicy<K, DirtyBufferEntry<V_ELE>>) pair.l1Cache;
            @SuppressWarnings("unchecked")
            CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = (CachePolicy<K, CacheEntry<List<V_ELE>>>) pair.l2Cache;
            DirtyBufferEntry<V_ELE> old = l1Cache.remove(currentKey);
            if (old != null) {
                backend.reportCacheMemoryReleased(old.getEstimatedSizeBytes());
            }

            CacheEntry<List<V_ELE>> oldL2 = l2Cache.remove(currentKey);
            if (oldL2 != null) {
                backend.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes());
            }
        }

        delegateState.clear();
    }

    // --- Public API: flushToUnderlyingState() ---

    @Override
    public void flushToUnderlyingState() throws IOException {
        K originalFlushKey = backend.getCurrentKey();
        N originalFlushNamespace = this.currentNamespace;
        try {
            for (Map.Entry<N, CachePolicy<K, DirtyBufferEntry<V_ELE>>> nsEntry : namespaceCachesL1.entrySet()) {
                N namespace = nsEntry.getKey();
                CachePolicy<K, DirtyBufferEntry<V_ELE>> l1Cache = nsEntry.getValue();
                CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(namespace);
                this.setCurrentNamespace(namespace);

                List<K> keysToFlush = new ArrayList<>();
                for (Map.Entry<K, DirtyBufferEntry<V_ELE>> e : l1Cache.entrySet()) {
                    keysToFlush.add(e.getKey());
                }

                for (K key : keysToFlush) {
                    DirtyBufferEntry<V_ELE> entry = l1Cache.get(key);
                    if (entry == null) {
                        continue;
                    }

                        K originalKey = backend.getCurrentKey();
                    try {
                        if (key == null) {
                            continue;
                        }
                        backend.setCurrentKey(key);

                        // Only write entries that have actual unflushed modifications.
                        // - dirtyBuffer non-empty: has append operations not yet written to RocksDB
                        // - isUpdated=true: has a full replace operation not yet written
                        // Skip clean entries (loaded from delegate, never modified): they're already in RocksDB.
                        if (!entry.isDirty()) {
                            // Entry is clean: already persisted to RocksDB, nothing to do.
                            continue;
                        }

                        // Flush this entry using the same logic as incrementalFlushToDelegate.
                        // This avoids the double-write bug where addAll() was followed by update().
                        // - isUpdated=true: use update() for full replacement
                        // - isUpdated=false: use addAll() for incremental append (merge semantics)
                        incrementalFlushToDelegate(key, namespace, entry, l2Cache);

                        // Step 6: Remove empty entries from L1 to release memory.
                        if (entry.getMergedList() == null) {
                            l1Cache.remove(key);
                        }

                    } catch (Exception e) {
                        throw new IOException(
                                "Failed to flush dirty list entry for key: " + key + " in namespace: " + namespace, e);
                    } finally {
                        backend.setCurrentKey(originalKey);
                    }
                }
            }
        } finally {
            backend.setCurrentKey(originalFlushKey);
            if (originalFlushNamespace != null) {
                this.setCurrentNamespace(originalFlushNamespace);
            } else {
                this.currentNamespace = null;
            }
        }
    }

    // --- Public API: getInternal() ---

    @Override
    public List<V_ELE> getInternal() throws Exception {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        NamespaceCachePair pair = getNamespaceCaches(currentNamespace);
        if (pair == null) {
            // Caching disabled: delegate directly.
            return delegateState.getInternal();
        }
        @SuppressWarnings("unchecked")
        CachePolicy<K, DirtyBufferEntry<V_ELE>> l1Cache = (CachePolicy<K, DirtyBufferEntry<V_ELE>>) pair.l1Cache;
        @SuppressWarnings("unchecked")
        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = (CachePolicy<K, CacheEntry<List<V_ELE>>>) pair.l2Cache;

        DirtyBufferEntry<V_ELE> entry = l1Cache != null ? l1Cache.get(currentKey) : null;

        if (entry != null) {
            // Return a defensive copy so callers cannot mutate internal cache state.
            List<V_ELE> merged = entry.getMergedList();
            return merged != null ? new ArrayList<>(merged) : null;
        }

        CacheEntry<List<V_ELE>> l2Entry = l2Cache != null ? l2Cache.get(currentKey) : null;

        if (l2Entry != null && l2Entry.getValue() != null) {
            backend.reportCacheMemoryReleased(l2Entry.getEstimatedSizeBytes());
            l2Cache.remove(currentKey);

            List<V_ELE> l2Value = l2Entry.getValue();
            // Promote to L1 with a clean DirtyBufferEntry.
            DirtyBufferEntry<V_ELE> newEntry =
                    new DirtyBufferEntry<>(new ArrayList<>(l2Value), new ArrayList<>(), false, false);
            DirtyBufferEntry<V_ELE> old = l1Cache.put(currentKey, newEntry);
            if (old != null) {
                backend.reportCacheMemoryReleased(old.getEstimatedSizeBytes());
            }
            backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
            return new ArrayList<>(l2Value);
        }

        List<V_ELE> listFromDelegate = delegateState.getInternal();
        if (listFromDelegate != null) {
            DirtyBufferEntry<V_ELE> newEntry =
                    new DirtyBufferEntry<>(new ArrayList<>(listFromDelegate), new ArrayList<>(), false, false);
            DirtyBufferEntry<V_ELE> old = l1Cache.put(currentKey, newEntry);
            if (old != null) {
                backend.reportCacheMemoryReleased(old.getEstimatedSizeBytes());
            }
            backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
            return new ArrayList<>(listFromDelegate);
        }
        return listFromDelegate;
    }

    // --- Public API: updateInternal() ---

    @Override
    public void updateInternal(List<V_ELE> valueToStore) throws Exception {
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        NamespaceCachePair pair = getNamespaceCaches(currentNamespace);

        if (pair == null) {
            // Caching disabled: delegate directly.
            if (valueToStore == null) {
                delegateState.clear();
            } else {
                delegateState.updateInternal(valueToStore);
            }
            return;
        }

        @SuppressWarnings("unchecked")
        CachePolicy<K, DirtyBufferEntry<V_ELE>> l1Cache = (CachePolicy<K, DirtyBufferEntry<V_ELE>>) pair.l1Cache;
        @SuppressWarnings("unchecked")
        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = (CachePolicy<K, CacheEntry<List<V_ELE>>>) pair.l2Cache;

        DirtyBufferEntry<V_ELE> newEntry =
                new DirtyBufferEntry<>(
                        valueToStore != null ? new ArrayList<>(valueToStore) : null,
                        new ArrayList<>(),
                        false, false);

        CacheEntry<List<V_ELE>> oldL2 = l2Cache.remove(currentKey);
        if (oldL2 != null) {
            backend.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes());
        }

        DirtyBufferEntry<V_ELE> old = l1Cache.put(currentKey, newEntry);
        if (old != null) {
            backend.reportCacheMemoryReleased(old.getEstimatedSizeBytes());
        }
        backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
    }

    // --- Public API: getDelegateState() ---

    @Override
    public InternalListState<K, N, V_ELE> getDelegateState() {
        return delegateState;
    }

    // --- Serializers ---

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

    // --- Namespace context ---

    @Override
    public void setCurrentNamespace(N namespace) {
        this.currentNamespace = namespace;
        if (namespace != null) {
            delegateState.setCurrentNamespace(namespace);
        }
    }

    public N getCurrentNamespace() {
        return currentNamespace;
    }

    // --- Serialized access (unsupported) ---

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<List<V_ELE>> safeValueSerializer) throws Exception {
        throw new UnsupportedOperationException(
                "getSerializedValue directly is not supported by CachingInternalListState.");
    }

    // --- Incremental visitor ---

    @Override
    public StateIncrementalVisitor<K, N, List<V_ELE>> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        try {
            flushToUnderlyingState();
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush caches before creating state visitor", e);
        }
        return delegateState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    // --- Namespace merge ---

    @Override
    public void mergeNamespaces(N targetNamespace, java.util.Collection<N> sourceNamespaces) throws Exception {
        delegateState.mergeNamespaces(targetNamespace, sourceNamespaces);
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
        CachePolicy<K, DirtyBufferEntry<V_ELE>> l1Cache = namespaceCachesL1.get(namespace);
        if (l1Cache != null) {
            for (Map.Entry<K, DirtyBufferEntry<V_ELE>> entry : l1Cache.entrySet()) {
                if (entry.getValue() != null) {
                    backend.reportCacheMemoryReleased(entry.getValue().getEstimatedSizeBytes());
                }
            }
            l1Cache.clear();
        }
        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = namespaceCachesL2.get(namespace);
        if (l2Cache != null) {
            for (Map.Entry<K, CacheEntry<List<V_ELE>>> entry : l2Cache.entrySet()) {
                if (entry.getValue() != null) {
                    backend.reportCacheMemoryReleased(entry.getValue().getEstimatedSizeBytes());
                }
            }
            l2Cache.clear();
        }
        namespaceCachesL1.remove(namespace);
        namespaceCachesL2.remove(namespace);
    }

    // --- Memory eviction ---

    @Override
    public long evictEntriesToFreeMemory(long targetBytesToFreeThisState) {
        long bytesFreed = 0;
        if (targetBytesToFreeThisState <= 0) {
            return 0;
        }

        // Evict from L2 first (clean data)
        List<N> l2Namespaces = new ArrayList<>();
        for (Map.Entry<N, CachePolicy<K, CacheEntry<List<V_ELE>>>> entry : namespaceCachesL2.entrySet()) {
            l2Namespaces.add(entry.getKey());
        }

        for (N namespace : l2Namespaces) {
            CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = namespaceCachesL2.get(namespace);
            if (l2Cache == null || l2Cache.isEmpty()) {
                continue;
            }
            Iterator<Map.Entry<K, CacheEntry<List<V_ELE>>>> l2Iter = l2Cache.entrySet().iterator();
            while (l2Iter.hasNext() && bytesFreed < targetBytesToFreeThisState) {
                Map.Entry<K, CacheEntry<List<V_ELE>>> entry = l2Iter.next();
                long estimatedSize = entry.getValue().getEstimatedSizeBytes();
                l2Iter.remove();
                backend.reportCacheMemoryReleased(estimatedSize);
                bytesFreed += estimatedSize;
            }
            if (bytesFreed >= targetBytesToFreeThisState) {
                return bytesFreed;
            }
        }

        // Evict from L1 (flush dirty entries first, then remove clean ones)
        List<N> l1Namespaces = new ArrayList<>();
        for (Map.Entry<N, CachePolicy<K, DirtyBufferEntry<V_ELE>>> entry : namespaceCachesL1.entrySet()) {
            l1Namespaces.add(entry.getKey());
        }

        for (N namespace : l1Namespaces) {
            CachePolicy<K, DirtyBufferEntry<V_ELE>> l1Cache = namespaceCachesL1.get(namespace);
            if (l1Cache == null || l1Cache.isEmpty()) {
                continue;
            }

            Iterator<Map.Entry<K, DirtyBufferEntry<V_ELE>>> l1Iter = l1Cache.entrySet().iterator();
            while (l1Iter.hasNext() && bytesFreed < targetBytesToFreeThisState) {
                Map.Entry<K, DirtyBufferEntry<V_ELE>> entry = l1Iter.next();
                K key = entry.getKey();
                DirtyBufferEntry<V_ELE> dirtyEntry = entry.getValue();
                long estimatedSize = dirtyEntry.getEstimatedSizeBytes();

                if (dirtyEntry.isDirty()) {
                    N origNs = getCurrentNamespace();
                    K origKey = backend.getCurrentKey();
                    try {
                        backend.setCurrentKey(key);
                        setCurrentNamespace(namespace);
                        CachePolicy<K, CacheEntry<List<V_ELE>>> l2Cache = getL2CacheForNamespace(namespace);
                        incrementalFlushToDelegate(key, namespace, dirtyEntry, l2Cache);
                    } catch (Exception e) {
                        // Log and continue
                    } finally {
                        backend.setCurrentKey(origKey);
                        setCurrentNamespace(origNs);
                    }
                }

                l1Iter.remove();
                backend.reportCacheMemoryReleased(estimatedSize);
                bytesFreed += estimatedSize;
            }
            if (bytesFreed >= targetBytesToFreeThisState) {
                return bytesFreed;
            }
        }
        return bytesFreed;
    }
}
