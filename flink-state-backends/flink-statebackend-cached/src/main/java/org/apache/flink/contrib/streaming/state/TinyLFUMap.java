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
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

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

    // Main cache split into probation (admission frontier) and protected (frequently-used)
    private final LinkedHashMap<K, V> mainProbationLru;
    private final LinkedHashMap<K, V> mainProtectedLru;

    // Window cache capacity
    private final int windowCacheCapacity;

    // Main cache capacity and its split
    private final int mainCacheCapacity;
    private final int probationCapacity;
    private final int protectedCapacity;

    // Total maximum capacity
    private final int maxCapacity;

    // Counter for total number of accesses
    private final LongAdder accessCounter;

    // Eviction listener
    private final Consumer<Map.Entry<K, V>> evictionListener;

    /**
     * Creates a new W-TinyLFU cache with the specified maximum capacity.
     * 
     * @param maxCapacity Maximum total capacity of the cache
     */
    public TinyLFUMap(int maxCapacity) {
        this(maxCapacity, null);
    }

    /**
     * Creates a new W-TinyLFU cache with the specified maximum capacity and eviction listener.
     *
     * @param maxCapacity Maximum total capacity of the cache
     * @param evictionListener A consumer that will be called with evicted entries
     */
    public TinyLFUMap(int maxCapacity, Consumer<Map.Entry<K, V>> evictionListener) {
        this.maxCapacity = maxCapacity;
        this.accessCounter = new LongAdder();
        this.evictionListener = evictionListener;

        // Calculate window cache size (at least 1 element)
        this.windowCacheCapacity =
                Math.max(MIN_WINDOW_SIZE, (int) Math.ceil(maxCapacity * WINDOW_CACHE_RATIO));

        // Rest of capacity goes to main cache
        this.mainCacheCapacity = Math.max(0, maxCapacity - windowCacheCapacity);

        // Initialize frequency sketch
        this.sketch = new CountMinSketchInternal(maxCapacity);

        // Create access-ordered window cache (LRU eviction)
        this.windowLruCache = new LinkedHashMap<K, V>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                // When window cache is full, try to admit the eldest entry to main cache
                if (size() > windowCacheCapacity) {
                    boolean admittedToMain = false;
                    if (mainCacheCapacity > 0) {
                        admittedToMain = tryAdmitToMainCache(eldest.getKey(), eldest.getValue());
                    }
                    // If not admitted to main, it's a true eviction from the overall cache system via window
                    if (!admittedToMain && evictionListener != null) {
                        // Pass the original eldest entry directly to the listener.
                        evictionListener.accept(eldest);
                    }
                    // Always remove from window cache, whether admitted to main or not
                    return true;
                }
                return false;
            }
        };

        // Create access-ordered main caches (LRU eviction)
        // - probation: entries newly admitted from window
        // - protected: entries that proved frequent (promotion on hit)
        // Evictions call the eviction listener.
        // Split main into 20% probation, 80% protected (typical W-TinyLFU default)
        int probationCap = (int) Math.floor(mainCacheCapacity * 0.20);
        if (probationCap < 1 && mainCacheCapacity > 0) {
            probationCap = 1;
        }
        int protectedCap = Math.max(0, mainCacheCapacity - probationCap);
        this.probationCapacity = probationCap;
        this.protectedCapacity = protectedCap;

        this.mainProbationLru = new LinkedHashMap<K, V>(16, 0.75f, true);
        this.mainProtectedLru = new LinkedHashMap<K, V>(16, 0.75f, true);
    }

    /**
     * Tries to admit an entry from the window cache to the main cache. Uses
     * TinyLFU's admission
     * policy based on frequency estimation.
     * 
     * @param key   The key to admit
     * @param value The value to admit
     * @return true if the entry was admitted to main cache, false otherwise
     */
    private boolean tryAdmitToMainCache(K key, V value) {
        // If main cache has space, admit to probation
        int mainSize = mainProbationLru.size() + mainProtectedLru.size();
        if (mainSize < mainCacheCapacity) {
            admitToProbation(key, value);
            return true;
        }

        if (mainCacheCapacity == 0) {
            // No main region available
            return false;
        }

        // Choose a victim from probation (its LRU). If probation is empty, demote the LRU of
        // protected into probation to create a victim there.
        ensureProbationVictimExists();

        if (mainProbationLru.isEmpty()) {
            // Could not create a probation victim (e.g., capacities are pathological)
            return false;
        }

        Map.Entry<K, V> victim = oldestEntry(mainProbationLru);
        if (victim == null) {
            return false;
        }

        long candidateFreq = sketch.estimate(key);
        long victimFreq = sketch.estimate(victim.getKey());

        if (candidateFreq >= victimFreq) {
            // Evict victim and admit candidate to probation
            mainProbationLru.remove(victim.getKey());
            if (evictionListener != null) {
                evictionListener.accept(victim);
            }
            admitToProbation(key, value);
            return true;
        } else {
            // Reject candidate
            return false;
        }
    }

    private void admitToProbation(K key, V value) {
        mainProbationLru.put(key, value);
        // Enforce probation capacity by evicting its LRU (not from protected)
        if (mainProbationLru.size() > probationCapacity) {
            Map.Entry<K, V> ev = oldestEntry(mainProbationLru);
            if (ev != null) {
                mainProbationLru.remove(ev.getKey());
                if (evictionListener != null) {
                    evictionListener.accept(ev);
                }
            }
        }
    }

    private void ensureProbationVictimExists() {
        if (!mainProbationLru.isEmpty()) return;
        if (mainProtectedLru.isEmpty()) return;
        // Demote protected's LRU into probation to create a victim there
        Map.Entry<K, V> ev = oldestEntry(mainProtectedLru);
        if (ev != null) {
            mainProtectedLru.remove(ev.getKey());
            mainProbationLru.put(ev.getKey(), ev.getValue());
        }
    }

    private static <K, V> Map.Entry<K, V> oldestEntry(LinkedHashMap<K, V> lru) {
        Iterator<Map.Entry<K, V>> it = lru.entrySet().iterator();
        return it.hasNext() ? it.next() : null;
    }

    @Override
    public V get(K key) {
        // Record access for frequency counting
        recordAccess(key);

        // Check window cache first
        V value = windowLruCache.get(key);
        if (value != null) {
            // Found in window cache, return without trying to promote
            // Promotion will happen naturally during window eviction
            return value;
        }

        // Check protected
        V v = mainProtectedLru.get(key);
        if (v != null) {
            return v; // access-order map updates recency
        }

        // Check probation; if hit, promote to protected
        v = mainProbationLru.remove(key);
        if (v != null) {
            promoteToProtected(key, v);
            return v;
        }

        return null;
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

        // Check protected
        if (mainProtectedLru.containsKey(key)) {
            return mainProtectedLru.put(key, value);
        }

        // Check probation
        if (mainProbationLru.containsKey(key)) {
            // Update in place; we keep it probationary until next hit promotes it
            return mainProbationLru.put(key, value);
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
            if (evictionListener != null) {
                // Create an ad-hoc entry for the listener
                // This assumes that a direct removal is also an "eviction" in the context of the listener's purpose (e.g., resource cleanup)
                final K finalKey = key;
                final V finalRemovedValueWindow = removedValue;
                Map.Entry<K,V> evictedEntry = new Map.Entry<K,V>() {
                    @Override public K getKey() { return finalKey; }
                    @Override public V getValue() { return finalRemovedValueWindow; }
                    @Override public V setValue(V value) { throw new UnsupportedOperationException(); }
                    @Override public boolean equals(Object o) { return (o instanceof Map.Entry) && Objects.equals(finalKey, ((Map.Entry<?,?>)o).getKey()) && Objects.equals(finalRemovedValueWindow, ((Map.Entry<?,?>)o).getValue()); }
                    @Override public int hashCode() { return Objects.hashCode(finalKey) ^ Objects.hashCode(finalRemovedValueWindow); }
                };
                evictionListener.accept(evictedEntry);
            }
            return removedValue;
        }

        removedValue = mainProtectedLru.remove(key);
        if (removedValue != null && evictionListener != null) {
            final K finalKeyMain = key;
            final V finalRemovedValueMain = removedValue;
            Map.Entry<K,V> evictedEntry = new Map.Entry<K,V>() {
                @Override public K getKey() { return finalKeyMain; }
                @Override public V getValue() { return finalRemovedValueMain; }
                @Override public V setValue(V value) { throw new UnsupportedOperationException(); }
                @Override public boolean equals(Object o) { return (o instanceof Map.Entry) && Objects.equals(finalKeyMain, ((Map.Entry<?,?>)o).getKey()) && Objects.equals(finalRemovedValueMain, ((Map.Entry<?,?>)o).getValue()); }
                @Override public int hashCode() { return Objects.hashCode(finalKeyMain) ^ Objects.hashCode(finalRemovedValueMain); }
            };
            evictionListener.accept(evictedEntry);
        }

        V removedProb = mainProbationLru.remove(key);
        if (removedProb != null && evictionListener != null) {
            final K finalKeyMain = key;
            final V finalRemovedValueMain = removedProb;
            Map.Entry<K,V> evictedEntry = new Map.Entry<K,V>() {
                @Override public K getKey() { return finalKeyMain; }
                @Override public V getValue() { return finalRemovedValueMain; }
                @Override public V setValue(V value) { throw new UnsupportedOperationException(); }
                @Override public boolean equals(Object o) { return (o instanceof Map.Entry) && Objects.equals(finalKeyMain, ((Map.Entry<?,?>)o).getKey()) && Objects.equals(finalRemovedValueMain, ((Map.Entry<?,?>)o).getValue()); }
                @Override public int hashCode() { return Objects.hashCode(finalKeyMain) ^ Objects.hashCode(finalRemovedValueMain); }
            };
            evictionListener.accept(evictedEntry);
        }
        return removedValue != null ? removedValue : removedProb;
    }

    @Override
    public boolean containsKey(K key) {
        return windowLruCache.containsKey(key)
                || mainProbationLru.containsKey(key)
                || mainProtectedLru.containsKey(key);
    }

    @Override
    public int size() {
        return windowLruCache.size() + mainProbationLru.size() + mainProtectedLru.size();
    }

    @Override
    public void clear() {
        windowLruCache.clear();
        mainProbationLru.clear();
        mainProtectedLru.clear();
        sketch.reset();
        accessCounter.reset();
    }

    @Override
    public Iterable<Map.Entry<K, V>> entrySet() {
        return () ->
                new Iterator<Map.Entry<K, V>>() {
                    private Iterator<Map.Entry<K, V>> current = windowLruCache.entrySet().iterator();
                    private int phase = 0;

                    @Override
                    public boolean hasNext() {
                        advanceIfNeeded();
                        return current.hasNext();
                    }

                    @Override
                    public Map.Entry<K, V> next() {
                        advanceIfNeeded();
                        return current.next();
                    }

                    private void advanceIfNeeded() {
                        while (!current.hasNext() && phase < 2) {
                            phase++;
                            switch (phase) {
                                case 1:
                                    current = mainProbationLru.entrySet().iterator();
                                    break;
                                case 2:
                                    current = mainProtectedLru.entrySet().iterator();
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                };
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
        values.addAll(mainProbationLru.values());
        values.addAll(mainProtectedLru.values());
        return values;
    }

    @Override
    public boolean isEmpty() {
        return windowLruCache.isEmpty() && mainProbationLru.isEmpty() && mainProtectedLru.isEmpty();
    }

    private void promoteToProtected(K key, V value) {
        mainProtectedLru.put(key, value);
        // Enforce protected capacity; demote its LRU to probation first
        if (mainProtectedLru.size() > protectedCapacity) {
            Map.Entry<K, V> demoted = oldestEntry(mainProtectedLru);
            if (demoted != null) {
                mainProtectedLru.remove(demoted.getKey());
                mainProbationLru.put(demoted.getKey(), demoted.getValue());
                // If probation overflows due to demotion, evict probation LRU
                if (mainProbationLru.size() > probationCapacity) {
                    Map.Entry<K, V> ev = oldestEntry(mainProbationLru);
                    if (ev != null) {
                        mainProbationLru.remove(ev.getKey());
                        if (evictionListener != null) {
                            evictionListener.accept(ev);
                        }
                    }
                }
            }
        }
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
        accessCounter.increment();
        long count = accessCounter.sum();
        // Use a safe interval to avoid division by zero and int overflow
        long resetInterval = (long) maxCapacity * 10L;
        if (resetInterval > 0 && count % resetInterval == 0) {
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

        // Maximum number of 4-bit counters we allow for the sketch. Capping this value prevents
        // the sketch (and its associated CPU work) from growing linearly with very large cache
        // capacities. With 1,048,576 counters and 4 hash rows we need at most ~1 MiB of memory
        // (1,048,576 * 4 / 16 * 8 bytes).
        private static final int MAX_COUNTERS = 1 << 20; // 1 Mi counters → ~1 MiB RAM

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
            /*
             * The original implementation sized the sketch to 4 × the cache capacity, then
             * rounded up to the next power-of-two. That means a cache that is configured for
             * millions of entries allocates proportionally huge arrays and touches them on every
             * access.  In practice we get diminishing returns beyond a certain table size, so we
             * cap the number of counters at MAX_COUNTERS. This keeps memory usage bounded and
             * avoids excessive CPU work when we reset() the sketch.
             */

            // Calculate counter size as next power of 2 ≥ 4 × capacity, then cap it.
            int desiredCounters = Math.max(16, Integer.highestOneBit(capacity * 4 - 1) << 1);
            int effectiveCounters = Math.min(desiredCounters, MAX_COUNTERS);

            // Ensure power-of-two invariant without exceeding the capped value.
            if ((effectiveCounters & (effectiveCounters - 1)) != 0) { // not already power of two
                effectiveCounters = Integer.highestOneBit(effectiveCounters);
            }

            this.counterSize = effectiveCounters;
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
