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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

/**
 * A cache policy that performs no operations and stores no entries.
 * Useful for disabling a cache layer.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class NoOpCachePolicy<K, V> implements CachePolicy<K, V> {

    @Override
    public V get(K key) {
        return null;
    }

    @Override
    public V put(K key, V value) {
        // Typically, put returns the previous value associated with key, or null if there was no mapping for key.
        // For a NoOp cache, there's never a previous value.
        return null;
    }

    @Override
    public V remove(K key) {
        // Typically, remove returns the previous value associated with key, or null if there was no mapping for key.
        return null;
    }

    @Override
    public boolean containsKey(K key) {
        return false;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public void clear() {
        // No-op
    }

    @Override
    public Iterable<Map.Entry<K, V>> entrySet() {
        return Collections.emptySet();
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        // A NoOp cache never finds the key, so it would "compute" the value.
        // It doesn't store it, so it's effectively a pass-through for the computation.
        return mappingFunction.apply(key);
    }

    @Override
    public Collection<V> values() {
        return Collections.emptyList();
    }

    @Override
    public boolean isEmpty() {
        return true;
    }
} 