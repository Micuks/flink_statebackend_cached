/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * A simple LRU (Least Recently Used) cache map.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class LRUMap<K, V> extends LinkedHashMap<K, V> {
    private final int maxCapacity;
    private final Consumer<Map.Entry<K, V>> evictionListener;

    public LRUMap(int maxCapacity) {
        this(maxCapacity, null);
    }

    public LRUMap(int maxCapacity, Consumer<Map.Entry<K, V>> evictionListener) {
        super(maxCapacity, 0.75f, true); // true for access-order
        this.maxCapacity = maxCapacity;
        this.evictionListener = evictionListener;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        boolean remove = size() > maxCapacity;
        if (remove && evictionListener != null) {
            evictionListener.accept(eldest);
        }
        return remove;
    }

    public V getOrDefault(Object key, V defaultValue) {
        V v;
        return (((v = get(key)) != null) || containsKey(key)) ? v : defaultValue;
    }
}
