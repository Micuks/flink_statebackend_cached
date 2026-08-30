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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class LruCachePolicyTest {

    @Test
    void testOverflowIsCommittedAsOneOrderedBatch() {
        List<List<String>> batches = new ArrayList<>();
        BatchEvictionListener<String, Integer> listener =
                entries -> {
                    List<String> keys = new ArrayList<>();
                    for (Map.Entry<String, Integer> entry : entries) {
                        keys.add(entry.getKey());
                    }
                    batches.add(keys);
                };
        LruCachePolicy<String, Integer> cache = new LruCachePolicy<>(2, 2, listener);

        cache.put("a", 1);
        cache.put("b", 2);
        cache.put("c", 3);
        cache.put("d", 4);
        cache.put("e", 5);

        assertEquals(1, batches.size());
        assertEquals(java.util.Arrays.asList("a", "b", "c"), batches.get(0));
        assertEquals(2, cache.size());
    }

    @Test
    void testBatchFailureRetainsEveryEvictionCandidate() {
        BatchEvictionListener<String, Integer> listener =
                entries -> {
                    throw new IllegalStateException("injected batch failure");
                };
        LruCachePolicy<String, Integer> cache = new LruCachePolicy<>(1, 1, listener);

        cache.put("a", 1);
        cache.put("b", 2);
        assertThrows(IllegalStateException.class, () -> cache.put("c", 3));

        assertEquals(3, cache.size());
        assertEquals(1, cache.get("a"));
        assertEquals(2, cache.get("b"));
        assertEquals(3, cache.get("c"));
    }

    @Test
    void testAsyncBatchCanRetainThenRemoveExactAcceptedObjects() {
        Set<String> pending = new HashSet<>();
        BatchEvictionListener<String, Integer> listener =
                new BatchEvictionListener<String, Integer>() {
                    @Override
                    public void acceptAll(List<Map.Entry<String, Integer>> entries) {
                        for (Map.Entry<String, Integer> entry : entries) {
                            pending.add(entry.getKey());
                        }
                    }

                    @Override
                    public boolean retainAfterAccept(String key, Integer value) {
                        return pending.contains(key);
                    }
                };
        LruCachePolicy<String, Integer> cache = new LruCachePolicy<>(2, 2, listener);

        cache.put("a", 1);
        cache.put("b", 2);
        cache.put("c", 3);
        cache.put("d", 4);
        cache.put("e", 5);

        assertEquals(5, cache.size());
        assertEquals(new HashSet<>(java.util.Arrays.asList("a", "b", "c")), pending);
        assertTrue(cache.removeIfSame("a", 1));
        assertFalse(cache.removeIfSame("b", 99));
        assertEquals(2, cache.get("b"));
    }
}
