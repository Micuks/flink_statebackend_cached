/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.cache;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Simple access-ordered LRU cache with max entry count. */
public final class LruCachePolicy<K, V> implements CachePolicy<K, V> {

    private final int maxEntries;
    private final int maxEntriesWithOverflow;
    private final LinkedHashMap<K, V> map;
    private final java.util.function.BiConsumer<K, V> evictionListener;

    public LruCachePolicy(int maxEntries) {
        this(maxEntries, Math.max(1, maxEntries / 16), (k, v) -> {
        });
    }

    public LruCachePolicy(int maxEntries, java.util.function.BiConsumer<K, V> evictionListener) {
        this(maxEntries, Math.max(1, maxEntries / 16), evictionListener);
    }

    public LruCachePolicy(
            int maxEntries,
            int overflowEntries,
            java.util.function.BiConsumer<K, V> evictionListener) {
        this.maxEntries = Math.max(0, maxEntries);
        int overflow = Math.max(0, overflowEntries);
        this.maxEntriesWithOverflow = this.maxEntries > 0 ? this.maxEntries + overflow : 0;
        this.evictionListener = evictionListener;
        this.map = new LinkedHashMap<K, V>(16, 0.75f, true);
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

    private void evictIfNeeded() {
        if (maxEntries <= 0 || map.size() <= maxEntriesWithOverflow) {
            return;
        }
        java.util.Iterator<Map.Entry<K, V>> iterator = map.entrySet().iterator();
        while (map.size() > maxEntries && iterator.hasNext()) {
            Map.Entry<K, V> entry = iterator.next();
            if (evictionListener != null) {
                evictionListener.accept(entry.getKey(), entry.getValue());
            }
            // Listener completion is the eviction commit point. A write-back listener may throw;
            // retaining the entry makes the payload available for retry instead of silently
            // discarding dirty state.
            iterator.remove();
        }
    }
}
