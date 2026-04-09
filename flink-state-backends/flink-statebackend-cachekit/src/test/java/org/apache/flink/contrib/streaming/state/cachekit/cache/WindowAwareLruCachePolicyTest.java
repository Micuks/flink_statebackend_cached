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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

/** Tests for {@link WindowAwareLruCachePolicy}. */
class WindowAwareLruCachePolicyTest {

    /** Simple key that carries a priority value for testing. */
    static class PriorityKey {
        final String id;
        final long priority; // higher = keep, lower = evict

        PriorityKey(String id, long priority) {
            this.id = id;
            this.priority = priority;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PriorityKey)) return false;
            return id.equals(((PriorityKey) o).id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        @Override
        public String toString() {
            return id + "(pri=" + priority + ")";
        }
    }

    @Test
    void testEvictsLowestPriorityEntry() {
        List<PriorityKey> evicted = new ArrayList<>();

        WindowAwareLruCachePolicy<PriorityKey, String> cache = new WindowAwareLruCachePolicy<>(
                3, 0,
                (k, v) -> evicted.add(k),
                k -> k.priority);

        // Fill cache
        cache.put(new PriorityKey("near", 100), "near-value");    // high priority (keep)
        cache.put(new PriorityKey("medium", 50), "medium-value");  // medium priority
        cache.put(new PriorityKey("far", 10), "far-value");        // low priority (evict first)

        assertEquals(3, cache.size());
        assertTrue(evicted.isEmpty());

        // Trigger eviction by adding one more entry
        cache.put(new PriorityKey("new", 80), "new-value");

        // "far" (priority=10) should be evicted, not the LRU entry
        assertEquals(1, evicted.size());
        assertEquals("far", evicted.get(0).id);

        // "near" should still be in cache
        assertNotNull(cache.get(new PriorityKey("near", 0)));
    }

    @Test
    void testFallsBackToLruWhenAllPrioritiesEqual() {
        List<PriorityKey> evicted = new ArrayList<>();

        WindowAwareLruCachePolicy<PriorityKey, String> cache = new WindowAwareLruCachePolicy<>(
                3, 0,
                (k, v) -> evicted.add(k),
                k -> 42L); // All same priority -> LRU fallback

        cache.put(new PriorityKey("first", 42), "v1");
        cache.put(new PriorityKey("second", 42), "v2");
        cache.put(new PriorityKey("third", 42), "v3");

        // Access "first" to make it recently used
        cache.get(new PriorityKey("first", 42));

        // Trigger eviction
        cache.put(new PriorityKey("fourth", 42), "v4");

        // Should evict LRU entry ("second", since "first" was recently accessed)
        assertEquals(1, evicted.size());
        assertEquals("second", evicted.get(0).id);

        assertTrue(cache.getWindowAwareEvictions() == 0);
        assertTrue(cache.getLruFallbackEvictions() == 1);
    }

    @Test
    void testProtectsHighPriorityFromEviction() {
        List<PriorityKey> evicted = new ArrayList<>();

        WindowAwareLruCachePolicy<PriorityKey, String> cache = new WindowAwareLruCachePolicy<>(
                2, 0,
                (k, v) -> evicted.add(k),
                k -> k.priority);

        // "protected" has very high priority (about to trigger)
        cache.put(new PriorityKey("protected", Long.MAX_VALUE), "important");
        cache.put(new PriorityKey("expendable", 1), "not-important");

        // Trigger eviction
        cache.put(new PriorityKey("new", 50), "new-value");

        // "expendable" should be evicted despite being more recently added
        assertEquals(1, evicted.size());
        assertEquals("expendable", evicted.get(0).id);

        // "protected" should still be there
        assertNotNull(cache.get(new PriorityKey("protected", 0)));
    }

    @Test
    void testOverflowBatchEviction() {
        List<PriorityKey> evicted = new ArrayList<>();

        // maxEntries=3, overflow=2 -> eviction triggers at size 6, evicts down to 3
        WindowAwareLruCachePolicy<PriorityKey, String> cache = new WindowAwareLruCachePolicy<>(
                3, 2,
                (k, v) -> evicted.add(k),
                k -> k.priority);

        cache.put(new PriorityKey("a", 10), "v");
        cache.put(new PriorityKey("b", 50), "v");
        cache.put(new PriorityKey("c", 30), "v");
        cache.put(new PriorityKey("d", 90), "v");
        cache.put(new PriorityKey("e", 20), "v");

        // Size = 5, within maxEntries + overflow = 5, no eviction yet
        assertEquals(5, cache.size());
        assertTrue(evicted.isEmpty());

        // Size = 6, exceeds 5 -> evict down to 3
        cache.put(new PriorityKey("f", 70), "v");

        assertEquals(3, cache.size());
        assertEquals(3, evicted.size());

        // The three lowest priority entries should be evicted: a(10), e(20), c(30)
        List<String> evictedIds = new ArrayList<>();
        for (PriorityKey k : evicted) {
            evictedIds.add(k.id);
        }
        assertTrue(evictedIds.contains("a"), "a (pri=10) should be evicted");
        assertTrue(evictedIds.contains("e"), "e (pri=20) should be evicted");
        assertTrue(evictedIds.contains("c"), "c (pri=30) should be evicted");
    }

    @Test
    void testBasicOperations() {
        WindowAwareLruCachePolicy<PriorityKey, String> cache = new WindowAwareLruCachePolicy<>(
                10, 0, (k, v) -> {}, k -> k.priority);

        PriorityKey key = new PriorityKey("k1", 50);
        cache.put(key, "value1");
        assertEquals("value1", cache.get(key));
        assertEquals(1, cache.size());

        cache.put(key, "value2");
        assertEquals("value2", cache.get(key));
        assertEquals(1, cache.size());

        cache.remove(key);
        assertNull(cache.get(key));
        assertEquals(0, cache.size());

        cache.put(new PriorityKey("a", 1), "v");
        cache.put(new PriorityKey("b", 2), "v");
        cache.clear();
        assertEquals(0, cache.size());
    }
}
