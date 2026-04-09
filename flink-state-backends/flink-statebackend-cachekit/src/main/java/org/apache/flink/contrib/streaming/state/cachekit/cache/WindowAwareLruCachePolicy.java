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

package org.apache.flink.contrib.streaming.state.cachekit.cache;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.ToLongFunction;

/**
 * Window-aware LRU cache that overrides eviction to use window ETT (estimated trigger time)
 * priority instead of pure access-order LRU.
 *
 * <p>When the cache exceeds capacity, instead of evicting the least-recently-used entry (which
 * might belong to a window about to trigger), this policy evicts the entry whose window is
 * farthest from triggering (lowest eviction priority).
 *
 * <p>Falls back to standard LRU when the priority function returns equal values for all entries
 * (e.g., when namespace is not a TimeWindow).
 *
 * @param <K> Cache key type (e.g., KeyNamespaceKey)
 * @param <V> Cache value type (e.g., CachedValue)
 */
public final class WindowAwareLruCachePolicy<K, V> implements CachePolicy<K, V> {

    private final int maxEntries;
    private final int maxEntriesWithOverflow;
    private final LinkedHashMap<K, V> map;
    private final BiConsumer<K, V> evictionListener;

    /**
     * Function that extracts eviction priority from a cache key.
     * Higher value = keep longer (protect). Lower value = evict first.
     */
    private final ToLongFunction<K> evictionPriorityFunction;

    /** Statistics for monitoring window-aware eviction effectiveness. */
    private long windowAwareEvictions;
    private long lruFallbackEvictions;

    public WindowAwareLruCachePolicy(
            int maxEntries,
            int overflowEntries,
            BiConsumer<K, V> evictionListener,
            ToLongFunction<K> evictionPriorityFunction) {
        this.maxEntries = Math.max(0, maxEntries);
        int overflow = Math.max(0, overflowEntries);
        this.maxEntriesWithOverflow = this.maxEntries > 0 ? this.maxEntries + overflow : 0;
        this.evictionListener = evictionListener;
        this.evictionPriorityFunction = Objects.requireNonNull(evictionPriorityFunction);
        // access-ordered LinkedHashMap for LRU fallback
        this.map = new LinkedHashMap<>(16, 0.75f, true);
    }

    @Override
    public V get(K key) {
        return map.get(key);
    }

    @Override
    public V put(K key, V value) {
        Objects.requireNonNull(key, "key");
        V previous = map.put(key, value);
        evictIfNeeded();
        return previous;
    }

    @Override
    public V remove(K key) {
        return map.remove(key);
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public Iterable<Map.Entry<K, V>> entries() {
        return Collections.unmodifiableSet(map.entrySet());
    }

    public long getWindowAwareEvictions() {
        return windowAwareEvictions;
    }

    public long getLruFallbackEvictions() {
        return lruFallbackEvictions;
    }

    private void evictIfNeeded() {
        if (maxEntries <= 0 || map.size() <= maxEntriesWithOverflow) {
            return;
        }

        while (map.size() > maxEntries) {
            K victim = selectVictim();
            if (victim == null) {
                break;
            }
            V value = map.remove(victim);
            if (evictionListener != null && value != null) {
                evictionListener.accept(victim, value);
            }
        }
    }

    /**
     * Selects the eviction victim based on window ETT priority.
     *
     * <p>Scans entries to find the one with the LOWEST eviction priority (farthest from
     * triggering). If all priorities are equal (no window info available), falls back to
     * LRU order (first entry in access-ordered LinkedHashMap = least recently used).
     */
    private K selectVictim() {
        if (map.isEmpty()) {
            return null;
        }

        K bestVictim = null;
        long lowestPriority = Long.MAX_VALUE;
        boolean allEqual = true;
        long firstPriority = -1;

        Iterator<Map.Entry<K, V>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<K, V> entry = iter.next();
            long priority = evictionPriorityFunction.applyAsLong(entry.getKey());

            if (firstPriority == -1) {
                firstPriority = priority;
            } else if (priority != firstPriority) {
                allEqual = false;
            }

            if (priority < lowestPriority) {
                lowestPriority = priority;
                bestVictim = entry.getKey();
            }
        }

        if (allEqual) {
            // All entries have same priority (no window info) -> fall back to LRU
            lruFallbackEvictions++;
            return map.entrySet().iterator().next().getKey();
        }

        windowAwareEvictions++;
        return bestVictim;
    }
}
