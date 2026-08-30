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
    public synchronized V get(K key) {
        return map.get(key);
    }

    @Override
    public synchronized V put(K key, V value) {
        Objects.requireNonNull(key, "key");
        V previous = map.put(key, value);
        evictIfNeeded();
        return previous;
    }

    @Override
    public synchronized V remove(K key) {
        return map.remove(key);
    }

    /** Removes {@code key} only when it still maps to the exact object accepted by a listener. */
    public synchronized boolean removeIfSame(K key, V expectedValue) {
        V current = map.get(key);
        if (current != expectedValue) {
            return false;
        }
        map.remove(key);
        return true;
    }

    /** Batch form of {@link #removeIfSame(Object, Object)} with one cache lock acquisition. */
    public synchronized int removeAllIfSame(
            java.util.List<? extends Map.Entry<K, V>> expectedEntries) {
        int removed = 0;
        for (Map.Entry<K, V> entry : expectedEntries) {
            V current = map.get(entry.getKey());
            if (current == entry.getValue()) {
                map.remove(entry.getKey());
                removed++;
            }
        }
        return removed;
    }

    @Override
    public synchronized void clear() {
        map.clear();
    }

    @Override
    public synchronized int size() {
        return map.size();
    }

    @Override
    public synchronized Iterable<Map.Entry<K, V>> entries() {
        java.util.List<Map.Entry<K, V>> snapshot = new java.util.ArrayList<>(map.size());
        for (Map.Entry<K, V> entry : map.entrySet()) {
            snapshot.add(
                    new java.util.AbstractMap.SimpleImmutableEntry<>(
                            entry.getKey(), entry.getValue()));
        }
        return Collections.unmodifiableList(snapshot);
    }

    private void evictIfNeeded() {
        if (maxEntries <= 0 || map.size() <= maxEntriesWithOverflow) {
            return;
        }
        BatchEvictionListener<K, V> batchListener = null;
        if (evictionListener instanceof BatchEvictionListener<?, ?>) {
            @SuppressWarnings("unchecked")
            BatchEvictionListener<K, V> castListener =
                    (BatchEvictionListener<K, V>) evictionListener;
            batchListener = castListener;
        }
        java.util.List<Map.Entry<K, V>> candidates = new java.util.ArrayList<>();
        java.util.Iterator<Map.Entry<K, V>> iterator = map.entrySet().iterator();
        int removalsNeeded = map.size() - maxEntries;
        while (candidates.size() < removalsNeeded && iterator.hasNext()) {
            Map.Entry<K, V> entry = iterator.next();
            if (batchListener != null
                    && batchListener.retainAfterAccept(entry.getKey(), entry.getValue())) {
                continue;
            }
            candidates.add(
                    new java.util.AbstractMap.SimpleImmutableEntry<>(
                            entry.getKey(), entry.getValue()));
        }
        if (candidates.isEmpty()) {
            return;
        }
        if (batchListener != null) {
            batchListener.acceptAll(Collections.unmodifiableList(candidates));
        } else if (evictionListener != null) {
            for (Map.Entry<K, V> entry : candidates) {
                evictionListener.accept(entry.getKey(), entry.getValue());
            }
        }
        // Listener completion is the eviction commit point. A write-back listener may throw;
        // retaining every candidate makes the complete batch available for retry instead of
        // silently discarding dirty state.
        for (Map.Entry<K, V> entry : candidates) {
            if (batchListener == null
                    || !batchListener.retainAfterAccept(entry.getKey(), entry.getValue())) {
                map.remove(entry.getKey());
            }
        }
    }
}
