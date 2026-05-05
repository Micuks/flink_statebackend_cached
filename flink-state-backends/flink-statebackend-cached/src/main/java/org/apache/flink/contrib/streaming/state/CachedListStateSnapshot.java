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
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */

package org.apache.flink.contrib.streaming.state;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks which (key, namespace) pairs have been explicitly cleared in ListState caching.
 *
 * <p>This is used to optimize the case where {@code clear()} is followed by {@code add()}.
 * Without this tracking, the {@code add()} would need to read from RocksDB first (to establish
 * the correct base state), even though we know the result will be a single-element list.
 *
 * <p>With this tracker, when {@code add()} is called after {@code clear()}, we can:
 * <ol>
 *   <li>Skip the RocksDB read (since we know the state is empty)</li>
 *   <li>Directly add the element to the cache</li>
 *   <li>Delegate to RocksDB with the single element</li>
 * </ol>
 *
 * <p>The tracking is done using a weak set (via {@link ConcurrentHashMap#newKeySet()})
 * to avoid memory leaks - entries are eligible for GC when no other references exist.
 *
 * @param <K> Flink key type
 * @param <N> Namespace type
 */
public class CachedListStateSnapshot<K, N> {

    /**
     * Tracks cleared (key, namespace) pairs.
     * Uses a Set backed by ConcurrentHashMap for concurrent access.
     */
    private final Set<NamespaceKeyPair> clearedKeys =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * Marks a (key, namespace) pair as cleared.
     *
     * @param key the Flink key
     * @param namespace the namespace
     */
    public void markCleared(K key, N namespace) {
        clearedKeys.add(new NamespaceKeyPair(key, namespace));
    }

    /**
     * Checks if a (key, namespace) pair was recently cleared.
     *
     * @param key the Flink key
     * @param namespace the namespace
     * @return true if the pair was cleared and has not been unmarked
     */
    public boolean isCleared(K key, N namespace) {
        return clearedKeys.contains(new NamespaceKeyPair(key, namespace));
    }

    /**
     * Removes the cleared marker for a (key, namespace) pair.
     * Called when add() is invoked after clear().
     *
     * @param key the Flink key
     * @param namespace the namespace
     */
    public void unmarkCleared(K key, N namespace) {
        clearedKeys.remove(new NamespaceKeyPair(key, namespace));
    }

    /**
     * Clears all tracked cleared pairs.
     */
    public void clearAll() {
        clearedKeys.clear();
    }

    /**
     * Returns the number of tracked cleared pairs.
     * Useful for monitoring/debugging.
     */
    public int size() {
        return clearedKeys.size();
    }

    /**
     * Represents a (key, namespace) pair for equality and hashing.
     */
    private static class NamespaceKeyPair {
        final Object key;
        final Object namespace;

        NamespaceKeyPair(Object key, Object namespace) {
            this.key = key;
            this.namespace = namespace;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NamespaceKeyPair that = (NamespaceKeyPair) o;
            return Objects.equals(key, that.key) && Objects.equals(namespace, that.namespace);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, namespace);
        }

        @Override
        public String toString() {
            return "NamespaceKeyPair{key=" + key + ", namespace=" + namespace + "}";
        }
    }
}
