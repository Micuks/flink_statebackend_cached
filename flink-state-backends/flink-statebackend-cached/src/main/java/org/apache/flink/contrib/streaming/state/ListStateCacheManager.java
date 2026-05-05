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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A dedicated cache manager for ListState that provides a large-capacity, memory-aware LRU cache.
 *
 * <p>This manager is designed to be independent from the L1/L2 caching used by ValueState and MapState,
 * providing better cache capacity for ListState workloads with many keys (e.g., 370,000 keys).
 *
 * <p>Key features:
 * <ul>
 *   <li>Large-capacity LRU cache (configurable, default 50K entries / 256MB)</li>
 *   <li>Memory-aware eviction based on estimated heap size</li>
 *   <li>Automatic dirty data flush on eviction</li>
 *   <li>Access tracking for LRU decisions</li>
 *   <li>Statistics collection for monitoring</li>
 * </ul>
 *
 * @param <K> The type of the Flink key.
 * @param <N> The type of the namespace.
 * @param <V> The type of elements in the list.
 */
@Internal
public class ListStateCacheManager<K, N, V> {

    private static final Logger LOG = LoggerFactory.getLogger(ListStateCacheManager.class);

    // ========== Configuration ==========
    private final long maxMemoryBytes;
    private final int maxEntries;
    private final long entryExpirationMillis;
    private final boolean cacheEnabled;

    // ========== Core cache (ConcurrentHashMap + LRU order) ==========
    private final ConcurrentHashMap<ListStateCacheKey, ListStateCacheEntry<V>> cache;
    private final LRUOrder lruOrder;

    // ========== Statistics ==========
    private final AtomicLong hitCount = new AtomicLong(0);
    private final AtomicLong missCount = new AtomicLong(0);
    private final AtomicLong evictionCount = new AtomicLong(0);
    private final AtomicLong currentMemoryUsage = new AtomicLong(0);
    private final AtomicLong flushCount = new AtomicLong(0);
    private final AtomicLong totalAccessCount = new AtomicLong(0);

    // ========== Degraded mode (OOM fallback) ==========
    private volatile boolean degraded = false;

    // ========== State backend reference (for RocksDB operations) ==========
    private final InternalListState<K, N, V> delegateState;
    private final CachingKeyedStateBackend<K> backend;

    // ========== Constructor ==========

    /**
     * Creates a new ListStateCacheManager.
     *
     * @param maxMemoryMb Maximum memory in MB for the cache.
     * @param maxEntries Maximum number of entries (0 = unlimited).
     * @param entryExpirationMillis Entry expiration time in milliseconds (0 = no expiration).
     * @param cacheEnabled Whether caching is enabled.
     * @param delegateState The delegate InternalListState for RocksDB operations.
     * @param backend The CachingKeyedStateBackend reference.
     */
    public ListStateCacheManager(
            long maxMemoryMb,
            int maxEntries,
            long entryExpirationMillis,
            boolean cacheEnabled,
            InternalListState<K, N, V> delegateState,
            CachingKeyedStateBackend<K> backend) {

        this.maxMemoryBytes = maxMemoryMb * 1024 * 1024;
        this.maxEntries = maxEntries;
        this.entryExpirationMillis = entryExpirationMillis;
        this.cacheEnabled = cacheEnabled;
        this.delegateState = delegateState;
        this.backend = backend;
        this.cache = new ConcurrentHashMap<>();
        this.lruOrder = new LRUOrder();
    }

    // ========== Core API ==========

    /**
     * Gets or creates a cache entry for the given key and namespace.
     *
     * <p>If the entry exists in cache, it is returned and access statistics are updated.
     * If not found, the entry is loaded from RocksDB (or created empty) and cached.
     *
     * @param key The Flink key.
     * @param namespace The namespace.
     * @return The cache entry.
     */
    public ListStateCacheEntry<V> getOrCreate(K key, N namespace) {
        if (!cacheEnabled || degraded) {
            return ListStateCacheEntry.empty();
        }

        ListStateCacheKey cacheKey = new ListStateCacheKey(key, namespace);

        // 1. Try to get from cache
        ListStateCacheEntry<V> entry = cache.get(cacheKey);
        if (entry != null) {
            // Update LRU order and record access
            lruOrder.touch(cacheKey);
            entry.recordAccess();
            hitCount.incrementAndGet();
            totalAccessCount.incrementAndGet();
            return entry;
        }

        // 2. Cache miss - load from RocksDB or create new entry
        missCount.incrementAndGet();
        totalAccessCount.incrementAndGet();

        ListStateCacheEntry<V> newEntry = new ListStateCacheEntry<>();

        // Load existing data from RocksDB
        try {
            List<V> rocksDBList = loadFromRocksDB(key, namespace);
            if (rocksDBList != null && !rocksDBList.isEmpty()) {
                newEntry.setFlushedList(rocksDBList);
            }
        } catch (Exception e) {
            LOG.warn("Failed to load ListState from RocksDB for key={}, namespace={}. Creating empty entry.",
                    key, namespace, e);
        }

        // 3. Put into cache (may trigger eviction)
        put(cacheKey, newEntry);

        return newEntry;
    }

    /**
     * Puts an entry into the cache, enforcing capacity constraints.
     */
    private void put(ListStateCacheKey key, ListStateCacheEntry<V> entry) {
        // Check capacity and evict if needed
        enforceCapacity(key, entry);

        // Put into cache
        ListStateCacheEntry<V> old = cache.put(key, entry);
        if (old != null) {
            currentMemoryUsage.addAndGet(-old.getEstimatedSizeBytes());
            lruOrder.remove(key);
        }
        currentMemoryUsage.addAndGet(entry.getEstimatedSizeBytes());
        lruOrder.touch(key);
    }

    /**
     * Removes an entry from the cache.
     */
    public ListStateCacheEntry<V> remove(K key, N namespace) {
        ListStateCacheKey cacheKey = new ListStateCacheKey(key, namespace);
        ListStateCacheEntry<V> removed = cache.remove(cacheKey);
        if (removed != null) {
            currentMemoryUsage.addAndGet(-removed.getEstimatedSizeBytes());
            lruOrder.remove(cacheKey);
        }
        return removed;
    }

    /**
     * Checks if the cache contains an entry for the given key and namespace.
     */
    public boolean contains(K key, N namespace) {
        ListStateCacheKey cacheKey = new ListStateCacheKey(key, namespace);
        return cache.containsKey(cacheKey);
    }

    // ========== Capacity management ==========

    /**
     * Enforces capacity constraints by evicting old entries if necessary.
     */
    private void enforceCapacity(ListStateCacheKey currentKey, ListStateCacheEntry<V> currentEntry) {
        long newSize = currentEntry.getEstimatedSizeBytes();

        while (true) {
            long currentMem = currentMemoryUsage.get();
            int currentSize = cache.size();

            // Check memory limit
            if (currentMem + newSize > maxMemoryBytes) {
                if (!evictOldest()) {
                    // Cannot evict more, but try to proceed anyway
                    break;
                }
                continue;
            }

            // Check entry count limit
            if (maxEntries > 0 && currentSize >= maxEntries) {
                if (!evictOldest()) {
                    break;
                }
                continue;
            }

            break;
        }
    }

    /**
     * Evicts the least recently used entry.
     *
     * @return true if an entry was evicted, false if cache is empty.
     */
    private boolean evictOldest() {
        Object oldestKeyObj = lruOrder.pollOldest();
        if (oldestKeyObj == null) {
            return false;
        }
        ListStateCacheKey oldestKey = (ListStateCacheKey) oldestKeyObj;

        ListStateCacheEntry<V> evicted = cache.remove(oldestKey);
        if (evicted == null) {
            return true; // Already removed
        }

        // Flush dirty data before eviction
        if (evicted.isDirty()) {
            try {
                flushToRocksDB(oldestKey, evicted);
            } catch (Exception e) {
                LOG.error("Failed to flush dirty entry during eviction for key={}. Data may be lost.",
                        oldestKey, e);
            }
        }

        currentMemoryUsage.addAndGet(-evicted.getEstimatedSizeBytes());
        evictionCount.incrementAndGet();

        LOG.debug("Evicted ListState entry: key={}, memoryReleased={}",
                oldestKey, evicted.getEstimatedSizeBytes());

        return true;
    }

    // ========== RocksDB operations ==========

    /**
     * Loads the list for the given key and namespace from RocksDB.
     */
    private List<V> loadFromRocksDB(K key, N namespace) throws Exception {
        // Use the delegate state to load data
        // First set the key context
        K origKey = backend.getCurrentKey();
        try {
            backend.setCurrentKey(key);
            Iterable<V> iterable = delegateState.get();
            if (iterable == null) {
                return null;
            }
            List<V> list = new ArrayList<>();
            for (V item : iterable) {
                list.add(item);
            }
            return list.isEmpty() ? null : list;
        } finally {
            backend.setCurrentKey(origKey);
        }
    }

    /**
     * Flushes dirty data to RocksDB.
     *
     * @param key The cache key.
     * @param entry The entry to flush.
     */
    private void flushToRocksDB(ListStateCacheKey key, ListStateCacheEntry<V> entry) throws Exception {
        // Set key context
        @SuppressWarnings("unchecked")
        K k = (K) key.getKey();
        @SuppressWarnings("unchecked")
        N n = (N) key.getNamespace();

        K origKey = backend.getCurrentKey();
        try {
            backend.setCurrentKey(k);

            if (entry.isUpdated()) {
                // Full replace via update()
                List<V> merged = entry.getMergedList();
                if (merged != null && !merged.isEmpty()) {
                    delegateState.update(merged);
                } else {
                    delegateState.clear();
                }
            } else {
                // Append via addAll (RocksDB merge semantics)
                List<V> dirtyBuffer = entry.getDirtyBuffer();
                if (dirtyBuffer != null && !dirtyBuffer.isEmpty()) {
                    delegateState.addAll(dirtyBuffer);
                }
            }

            flushCount.incrementAndGet();
        } finally {
            backend.setCurrentKey(origKey);
        }
    }

    // ========== Flush operations ==========

    /**
     * Flushes all dirty entries to RocksDB.
     * This should be called before checkpoint.
     */
    public void flushAll() {
        for (Map.Entry<ListStateCacheKey, ListStateCacheEntry<V>> e : cache.entrySet()) {
            ListStateCacheEntry<V> entry = e.getValue();
            if (entry.isDirty()) {
                try {
                    flushToRocksDB(e.getKey(), entry);
                    entry.markFlushed();
                } catch (Exception ex) {
                    LOG.error("Failed to flush entry during flushAll for key={}.", e.getKey(), ex);
                }
            }
        }
    }

    /**
     * Clears all entries from the cache.
     * This flushes all dirty data first.
     */
    public void clear() {
        flushAll();
        cache.clear();
        lruOrder.clear();
        currentMemoryUsage.set(0);
    }

    // ========== Statistics ==========

    public double getHitRate() {
        long hits = hitCount.get();
        long misses = missCount.get();
        long total = hits + misses;
        return total > 0 ? hits / (double) total : 0.0;
    }

    public long getCurrentMemoryUsage() {
        return currentMemoryUsage.get();
    }

    public int getCacheSize() {
        return cache.size();
    }

    public long getHitCount() {
        return hitCount.get();
    }

    public long getMissCount() {
        return missCount.get();
    }

    public long getEvictionCount() {
        return evictionCount.get();
    }

    public long getFlushCount() {
        return flushCount.get();
    }

    public long getTotalAccessCount() {
        return totalAccessCount.get();
    }

    public CacheStats getStats() {
        return new CacheStats(
                hitCount.get(),
                missCount.get(),
                evictionCount.get(),
                currentMemoryUsage.get(),
                cache.size(),
                flushCount.get(),
                maxMemoryBytes,
                maxEntries
        );
    }

    public boolean isDegraded() {
        return degraded;
    }

    // ========== Cache key class ==========

    /**
     * Cache key that combines Flink key and namespace.
     */
    public static class ListStateCacheKey {
        private final Object key;
        private final Object namespace;

        public ListStateCacheKey(Object key, Object namespace) {
            this.key = key;
            this.namespace = namespace;
        }

        public Object getKey() {
            return key;
        }

        public Object getNamespace() {
            return namespace;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ListStateCacheKey that = (ListStateCacheKey) o;
            return Objects.equals(key, that.key) && Objects.equals(namespace, that.namespace);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, namespace);
        }

        @Override
        public String toString() {
            return "ListStateCacheKey{key=" + key + ", namespace=" + namespace + "}";
        }
    }

    // ========== Cache statistics class ==========

    /**
     * Statistics for the cache.
     */
    public static class CacheStats {
        public final long hits;
        public final long misses;
        public final long evictions;
        public final long memoryUsageBytes;
        public final int cacheSize;
        public final long flushCount;
        public final long maxMemoryBytes;
        public final int maxEntries;

        public CacheStats(long hits, long misses, long evictions,
                         long memoryUsageBytes, int cacheSize, long flushCount,
                         long maxMemoryBytes, int maxEntries) {
            this.hits = hits;
            this.misses = misses;
            this.evictions = evictions;
            this.memoryUsageBytes = memoryUsageBytes;
            this.cacheSize = cacheSize;
            this.flushCount = flushCount;
            this.maxMemoryBytes = maxMemoryBytes;
            this.maxEntries = maxEntries;
        }

        public double getHitRate() {
            long total = hits + misses;
            return total > 0 ? hits / (double) total : 0.0;
        }

        public double getMemoryUsageRatio() {
            return maxMemoryBytes > 0 ? memoryUsageBytes / (double) maxMemoryBytes : 0.0;
        }
    }

    // ========== LRU order tracking (lock-free) ==========

    /**
     * A lock-free LRU order tracker.
     * Uses a concurrent doubly-linked list with atomic operations.
     */
    private static class LRUOrder {
        private final AtomicReference<Node> head = new AtomicReference<>(null);
        private final AtomicReference<Node> tail = new AtomicReference<>(null);
        private final ConcurrentHashMap<Object, Node> nodeMap = new ConcurrentHashMap<>();

        /**
         * Touches a key, moving it to the head (most recently used).
         */
        public void touch(Object key) {
            Node existing = nodeMap.get(key);
            if (existing != null) {
                moveToHead(existing);
            } else {
                Node newNode = new Node(key);
                Node oldHead = head.get();
                newNode.next = oldHead;
                if (oldHead != null) {
                    oldHead.prev = newNode;
                } else {
                    tail.set(newNode);
                }
                if (head.compareAndSet(oldHead, newNode)) {
                    nodeMap.put(key, newNode);
                } else {
                    // Concurrent modification, retry
                    touch(key);
                }
            }
        }

        /**
         * Removes a key from the LRU order.
         */
        public void remove(Object key) {
            Node node = nodeMap.remove(key);
            if (node != null) {
                unlink(node);
            }
        }

        /**
         * Polls the oldest (least recently used) key.
         */
        public Object pollOldest() {
            Node oldest = tail.get();
            if (oldest == null) {
                return null;
            }

            Object key = oldest.key;
            nodeMap.remove(key);
            unlink(oldest);
            return key;
        }

        /**
         * Clears all entries.
         */
        public void clear() {
            head.set(null);
            tail.set(null);
            nodeMap.clear();
        }

        private void moveToHead(Node node) {
            Node currentHead = head.get();
            if (node == currentHead) {
                return; // Already at head
            }

            // Unlink from current position
            unlink(node);

            // Insert at head
            node.prev = null;
            node.next = currentHead;
            if (currentHead != null) {
                currentHead.prev = node;
            } else {
                tail.set(node);
            }
            head.set(node);
        }

        private void unlink(Node node) {
            Node prev = node.prev;
            Node next = node.next;

            if (prev != null) {
                prev.next = next;
            } else {
                // This node is head
                head.set(next);
            }

            if (next != null) {
                next.prev = prev;
            } else {
                // This node is tail
                tail.set(prev);
            }

            node.prev = null;
            node.next = null;
        }

        private static class Node {
            final Object key;
            volatile Node prev;
            volatile Node next;

            Node(Object key) {
                this.key = key;
            }
        }
    }
}
