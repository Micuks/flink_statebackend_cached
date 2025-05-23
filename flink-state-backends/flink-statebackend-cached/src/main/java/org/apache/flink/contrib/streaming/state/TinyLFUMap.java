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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An implementation of the W-TinyLFU cache eviction policy.
 * 
 * W-TinyLFU is a variant of the TinyLFU algorithm that combines a small window cache with a larger
 * main cache, protected by a frequency sketch. This implementation achieves high hit rates for both
 * recency-biased and frequency-biased workloads.
 * 
 * @param <K> Type of keys
 * @param <V> Type of values
 */
public class TinyLFUMap<K, V> implements CachePolicy<K, V> {

    // Frequency sketch for tracking item access frequencies
    private final CountMinSketchInternal sketch;

    // Window cache size as a fraction of total capacity (typically 1%)
    private static final double WINDOW_CACHE_RATIO = 0.01;

    // Minimum window cache size
    private static final int MIN_WINDOW_SIZE = 1;

    // Window cache (LRU) - admits all new entries
    private final LinkedHashMap<K, V> windowLruCache;

    // Main cache (LRU) - protected by the frequency sketch
    private final LinkedHashMap<K, V> mainLruCache;

    // Window cache capacity
    private final int windowCacheCapacity;

    // Main cache capacity
    private final int mainCacheCapacity;

    // Total maximum capacity
    private final int maxCapacity;

    // Counter for total number of accesses
    private final AtomicLong accessCounter;

    /**
     * Creates a new W-TinyLFU cache with the specified maximum capacity.
     * 
     * @param maxCapacity Maximum total capacity of the cache
     */
    public TinyLFUMap(int maxCapacity) {
        this.maxCapacity = maxCapacity;
        this.accessCounter = new AtomicLong();

        // Calculate window cache size (at least 1 element)
        this.windowCacheCapacity =
                Math.max(MIN_WINDOW_SIZE, (int) Math.ceil(maxCapacity * WINDOW_CACHE_RATIO));

        // Rest of capacity goes to main cache
        this.mainCacheCapacity = maxCapacity - windowCacheCapacity;

        // Initialize frequency sketch
        this.sketch = new CountMinSketchInternal(maxCapacity);

        // Create access-ordered window cache (LRU eviction)
        this.windowLruCache = new LinkedHashMap<K, V>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                // When window cache is full, try to admit the eldest entry to main cache
                if (size() > windowCacheCapacity) {
                    tryAdmitToMainCache(eldest.getKey(), eldest.getValue());
                    return true;
                }
                return false;
            }
        };

        // Create access-ordered main cache (LRU eviction)
        this.mainLruCache = new LinkedHashMap<K, V>(16, 0.75f, true);
    }

    /**
     * Tries to admit an entry from the window cache to the main cache. Uses TinyLFU's admission
     * policy based on frequency estimation.
     * 
     * @param key The key to admit
     * @param value The value to admit
     */
    private void tryAdmitToMainCache(K key, V value) {
        // If main cache has space, admit directly
        if (mainLruCache.size() < mainCacheCapacity) {
            mainLruCache.put(key, value);
            return;
        }

        // Otherwise, use the TinyLFU admission policy
        Iterator<Map.Entry<K, V>> it = mainLruCache.entrySet().iterator();
        if (!it.hasNext()) {
            // Shouldn't happen if capacities are set correctly
            return;
        }

        // Get the victim (LRU item from main cache)
        Map.Entry<K, V> victim = it.next();
        K victimKey = victim.getKey();

        // Compare estimated frequency of candidate vs victim
        long candidateFreq = sketch.estimate(key);
        long victimFreq = sketch.estimate(victimKey);

        // If candidate is accessed more frequently, evict victim
        if (candidateFreq > victimFreq) {
            it.remove();
            mainLruCache.put(key, value);
        }
        // Otherwise, the candidate is not admitted and is evicted
    }

    @Override
    public V get(K key) {
        // Record access for frequency counting
        recordAccess(key);

        // Check window cache first
        V value = windowLruCache.get(key);
        if (value != null) {
            // Found in window cache, promote to main cache
            windowLruCache.remove(key);
            tryAdmitToMainCache(key, value);
            return value;
        }

        // Check main cache
        return mainLruCache.get(key);
    }

    @Override
    public V put(K key, V value) {
        // Record access for frequency counting
        recordAccess(key);

        // Check if key already exists in either cache
        V oldValue = null;

        // Check window cache
        if (windowLruCache.containsKey(key)) {
            oldValue = windowLruCache.remove(key);
            windowLruCache.put(key, value);
            return oldValue;
        }

        // Check main cache
        if (mainLruCache.containsKey(key)) {
            oldValue = mainLruCache.put(key, value);
            return oldValue;
        }

        // New entry, add to window cache
        // If window cache is full, removeEldestEntry will handle admission
        windowLruCache.put(key, value);
        return null;
    }

    @Override
    public V remove(K key) {
        // Check both caches
        V removedValue = windowLruCache.remove(key);
        if (removedValue != null) {
            return removedValue;
        }

        return mainLruCache.remove(key);
    }

    @Override
    public boolean containsKey(K key) {
        return windowLruCache.containsKey(key) || mainLruCache.containsKey(key);
    }

    @Override
    public int size() {
        return windowLruCache.size() + mainLruCache.size();
    }

    @Override
    public void clear() {
        windowLruCache.clear();
        mainLruCache.clear();
        sketch.reset();
        accessCounter.set(0);
    }

    @Override
    public Iterable<Map.Entry<K, V>> entrySet() {
        // Combine entries from both caches
        List<Map.Entry<K, V>> entries = new ArrayList<>(size());
        entries.addAll(windowLruCache.entrySet());
        entries.addAll(mainLruCache.entrySet());
        return entries;
    }

    @Override
    public V computeIfAbsent(K key,
            java.util.function.Function<? super K, ? extends V> mappingFunction) {
        // First check if the key already exists
        if (containsKey(key)) {
            return get(key);
        }

        // Apply the mapping function
        V value = mappingFunction.apply(key);
        if (value != null) {
            put(key, value);
        }
        return value;
    }

    @Override
    public Collection<V> values() {
        List<V> values = new ArrayList<>(size());
        values.addAll(windowLruCache.values());
        values.addAll(mainLruCache.values());
        return values;
    }

    @Override
    public boolean isEmpty() {
        return windowLruCache.isEmpty() && mainLruCache.isEmpty();
    }

    /**
     * Records an access to the given key in the frequency sketch. Also periodically checks for
     * reset conditions.
     * 
     * @param key The key being accessed
     */
    private void recordAccess(K key) {
        // Increment frequency count for this key
        sketch.increment(key);

        // Check if we need to reset the counters (every maxCapacity * 10 accesses)
        long count = accessCounter.incrementAndGet();
        if (count % (maxCapacity * 10) == 0) {
            sketch.reset();
        }
    }

    /**
     * A space-efficient probabilistic data structure used to count frequencies of events. This
     * implementation uses 4-bit counters to track access frequencies.
     */
    private static class CountMinSketchInternal {
        // Number of hash functions (rows)
        private static final int HASH_COUNT = 4;

        // Number of counters per hash function (columns) - must be a power of 2
        private final int counterSize;

        // Maximum value of a counter before saturation (15 for 4-bit counters)
        private static final int MAX_COUNTER_VALUE = 15;

        // Array to hold packed counters (16 counters per long)
        private final long[] counters;

        // Mask for column indexing (counterSize - 1)
        private final int columnMask;

        /**
         * Creates a CountMinSketch sized appropriately for the given capacity.
         * 
         * @param capacity Expected number of distinct elements
         */
        public CountMinSketchInternal(int capacity) {
            // Calculate counter size as next power of 2 >= 4*capacity
            int desiredCounters = Math.max(16, Integer.highestOneBit(capacity * 4 - 1) << 1);
            this.counterSize = desiredCounters;
            this.columnMask = counterSize - 1;

            // Each long holds 16 4-bit counters
            int counterWords = (counterSize * HASH_COUNT + 15) / 16;
            this.counters = new long[counterWords];
        }

        /**
         * Increments the counters for the given key.
         * 
         * @param key The key to increment counters for
         */
        public void increment(Object key) {
            if (key == null) {
                return;
            }

            int hashCode = key.hashCode();

            // Update all hash functions
            for (int i = 0; i < HASH_COUNT; i++) {
                // Use different hash seeds for each function
                int hash = rehash(hashCode + i * 1500450271);
                int position = hash & columnMask;

                // Calculate word and offset within the word
                int wordIndex = (i * counterSize + position) / 16;
                int wordOffset = ((i * counterSize + position) % 16) * 4;

                // Get current counter value
                long word = counters[wordIndex];
                int currentCount = (int) ((word >>> wordOffset) & MAX_COUNTER_VALUE);

                // Increment if not saturated
                if (currentCount < MAX_COUNTER_VALUE) {
                    // Clear current value and set incremented value
                    word = word & ~(((long) MAX_COUNTER_VALUE) << wordOffset);
                    word = word | ((long) (currentCount + 1)) << wordOffset;
                    counters[wordIndex] = word;
                }
            }
        }

        /**
         * Estimates the frequency of the given key.
         * 
         * @param key The key to estimate
         * @return The estimated frequency (the minimum of all counter values)
         */
        public long estimate(Object key) {
            if (key == null) {
                return 0;
            }

            int hashCode = key.hashCode();
            long min = Long.MAX_VALUE;

            // Check all hash functions and take the minimum count
            for (int i = 0; i < HASH_COUNT; i++) {
                int hash = rehash(hashCode + i * 1500450271);
                int position = hash & columnMask;

                // Calculate word and offset within the word
                int wordIndex = (i * counterSize + position) / 16;
                int wordOffset = ((i * counterSize + position) % 16) * 4;

                // Extract counter value
                long count = (counters[wordIndex] >>> wordOffset) & MAX_COUNTER_VALUE;
                min = Math.min(min, count);
            }

            return min;
        }

        /**
         * Reset all counters by dividing them by 2. This implements the "aging" mechanism for the
         * sketch.
         */
        public void reset() {
            for (int i = 0; i < counters.length; i++) {
                long word = counters[i];

                // For each 4-bit counter in the word, divide by 2
                long newWord = 0;
                for (int j = 0; j < 16; j++) {
                    int offset = j * 4;
                    int value = (int) ((word >>> offset) & MAX_COUNTER_VALUE);
                    int newValue = value >>> 1; // Divide by 2
                    newWord |= ((long) newValue) << offset;
                }

                counters[i] = newWord;
            }
        }

        /**
         * A simple rehash function based on MurmurHash.
         * 
         * @param h Initial hash code
         * @return Rehashed value
         */
        private static int rehash(int h) {
            h ^= h >>> 16;
            h *= 0x85ebca6b;
            h ^= h >>> 13;
            h *= 0xc2b2ae35;
            h ^= h >>> 16;
            return h;
        }
    }
}
