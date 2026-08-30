/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.cache;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/** Eviction listener that can commit one LRU overflow as a single ordered batch. */
public interface BatchEvictionListener<K, V> extends BiConsumer<K, V> {

    /** Commits all candidates before the cache removes any of them. */
    void acceptAll(List<Map.Entry<K, V>> entries);

    /**
     * Whether a candidate must remain in the cache after {@link #acceptAll(List)} returns.
     *
     * <p>The default preserves the ordinary write-back contract. An asynchronous listener may
     * retain an entry as the authoritative read-your-writes image until its durable write
     * completes. The cache also consults this method before selecting a later overflow so an
     * already retained entry is not submitted twice.
     */
    default boolean retainAfterAccept(K key, V value) {
        return false;
    }

    @Override
    default void accept(K key, V value) {
        acceptAll(
                Collections.singletonList(
                        new java.util.AbstractMap.SimpleImmutableEntry<>(key, value)));
    }
}
