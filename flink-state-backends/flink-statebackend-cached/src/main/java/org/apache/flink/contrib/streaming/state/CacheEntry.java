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

/**
 * Represents an entry in the cache.
 *
 * @param <V> The type of the cached value.
 */
public class CacheEntry<V> {
    private V value;
    private boolean dirty;

    public CacheEntry(V value, boolean dirty) {
        this.value = value;
        this.dirty = dirty;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public static <V> CacheEntry<V> clean(V value) {
        return new CacheEntry<>(value, false);
    }

    public static <V> CacheEntry<V> dirty(V value) {
        return new CacheEntry<>(value, true);
    }
}
