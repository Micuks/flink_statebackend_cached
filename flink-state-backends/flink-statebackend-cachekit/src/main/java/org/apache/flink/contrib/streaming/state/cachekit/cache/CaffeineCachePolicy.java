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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import java.util.Map;
import java.util.Objects;

/** Caffeine-backed cache policy with maximum entry count. */
public final class CaffeineCachePolicy<K, V> implements CachePolicy<K, V> {

    private final Cache<K, V> cache;

    public CaffeineCachePolicy(long maxEntries, java.util.function.BiConsumer<K, V> evictionListener) {
        Objects.requireNonNull(evictionListener, "evictionListener");
        if (maxEntries <= 0) {
            this.cache = Caffeine.newBuilder().executor(Runnable::run).maximumSize(0).build();
            return;
        }
        long initialCapacity = Math.min(1024, Math.max(16, maxEntries));
        this.cache =
                Caffeine.newBuilder()
                        // CacheKit state backend isn't thread-safe; avoid invoking removal listener
                        // on Caffeine's async executor (which can lead to off-thread RocksDB writes).
                        .executor(Runnable::run)
                        .initialCapacity((int) Math.min(Integer.MAX_VALUE, initialCapacity))
                        .maximumSize(maxEntries)
                        .removalListener(
                                (K key, V value, RemovalCause cause) -> {
                                    if (cause.wasEvicted() && key != null && value != null) {
                                        evictionListener.accept(key, value);
                                    }
                                })
                        .build();
    }

    @Override
    public V get(K key) {
        return cache.getIfPresent(key);
    }

    @Override
    public V put(K key, V value) {
        Objects.requireNonNull(key, "key");
        V previous = cache.getIfPresent(key);
        cache.put(key, value);
        return previous;
    }

    @Override
    public V remove(K key) {
        return cache.asMap().remove(key);
    }

    @Override
    public void clear() {
        cache.invalidateAll();
    }

    @Override
    public int size() {
        return cache.asMap().size();
    }

    @Override
    public Iterable<Map.Entry<K, V>> entries() {
        return cache.asMap().entrySet();
    }
}
