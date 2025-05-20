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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class LRUMapTest {

    private LRUMap<String, String> lruMap;
    private final int maxEntries = 3;

    @BeforeEach
    void setUp() {
        lruMap = new LRUMap<>(maxEntries);
    }

    @Test
    void testPutAndGet() {
        lruMap.put("key1", "value1");
        Assertions.assertEquals("value1", lruMap.get("key1"));
        lruMap.put("key2", "value2");
        Assertions.assertEquals("value2", lruMap.get("key2"));
        Assertions.assertEquals("value1", lruMap.get("key1"));
        Assertions.assertNull(lruMap.get("nonExistentKey"));
    }

    @Test
    void testLruEvictionOrder() {
        lruMap.put("key1", "value1");
        lruMap.put("key2", "value2");
        lruMap.put("key3", "value3");
        lruMap.put("key4", "value4");
        Assertions.assertNull(lruMap.get("key1"));
        Assertions.assertEquals("value2", lruMap.get("key2"));
        Assertions.assertEquals("value3", lruMap.get("key3"));
        Assertions.assertEquals("value4", lruMap.get("key4"));
    }

    @Test
    void testGetPromotesEntry() {
        lruMap.put("key1", "value1");
        lruMap.put("key2", "value2");
        lruMap.put("key3", "value3");
        lruMap.get("key1");
        lruMap.put("key4", "value4");
        Assertions.assertEquals("value1", lruMap.get("key1"));
        Assertions.assertNull(lruMap.get("key2"));
        Assertions.assertEquals("value3", lruMap.get("key3"));
        Assertions.assertEquals("value4", lruMap.get("key4"));
    }

    @Test
    void testPutExistingKeyUpdatesValueAndPromotes() {
        lruMap.put("key1", "value1");
        lruMap.put("key2", "value2");
        lruMap.put("key3", "value3");
        lruMap.put("key1", "updatedValue1");
        lruMap.put("key4", "value4");
        Assertions.assertEquals("updatedValue1", lruMap.get("key1"));
        Assertions.assertNull(lruMap.get("key2"));
        Assertions.assertEquals("value3", lruMap.get("key3"));
        Assertions.assertEquals("value4", lruMap.get("key4"));
    }

    @Test
    void testRemove() {
        lruMap.put("key1", "value1");
        lruMap.put("key2", "value2");
        Assertions.assertEquals("value1", lruMap.remove("key1"));
        Assertions.assertNull(lruMap.get("key1"));
        Assertions.assertEquals(1, lruMap.size());
        Assertions.assertNull(lruMap.remove("nonExistentKey"));
    }

    @Test
    void testClear() {
        lruMap.put("key1", "value1");
        lruMap.put("key2", "value2");
        lruMap.clear();
        Assertions.assertEquals(0, lruMap.size());
        Assertions.assertTrue(lruMap.isEmpty());
        Assertions.assertNull(lruMap.get("key1"));
        Assertions.assertNull(lruMap.get("key2"));
    }

    @Test
    void testSize() {
        Assertions.assertEquals(0, lruMap.size());
        lruMap.put("key1", "value1");
        Assertions.assertEquals(1, lruMap.size());
        lruMap.put("key2", "value2");
        Assertions.assertEquals(2, lruMap.size());
        lruMap.remove("key1");
        Assertions.assertEquals(1, lruMap.size());
        lruMap.clear();
        Assertions.assertEquals(0, lruMap.size());
    }

    @Test
    void testIsEmpty() {
        Assertions.assertTrue(lruMap.isEmpty());
        lruMap.put("key1", "value1");
        Assertions.assertFalse(lruMap.isEmpty());
        lruMap.remove("key1");
        Assertions.assertTrue(lruMap.isEmpty());
    }

    @Test
    void testContainsKey() {
        Assertions.assertFalse(lruMap.containsKey("key1"));
        lruMap.put("key1", "value1");
        Assertions.assertTrue(lruMap.containsKey("key1"));
        lruMap.remove("key1");
        Assertions.assertFalse(lruMap.containsKey("key1"));
    }

    @Test
    void testEvictionListenerIsCalled() {
        AtomicReference<String> evictedKey = new AtomicReference<>();
        AtomicReference<String> evictedValue = new AtomicReference<>();
        LRUMap<String, String> lruMapWithListener =
                new LRUMap<>(
                        maxEntries,
                        entry -> {
                            evictedKey.set(entry.getKey());
                            evictedValue.set(entry.getValue());
                        });
        lruMapWithListener.put("key1", "value1");
        lruMapWithListener.put("key2", "value2");
        lruMapWithListener.put("key3", "value3");
        lruMapWithListener.put("key4", "value4");
        Assertions.assertEquals("key1", evictedKey.get());
        Assertions.assertEquals("value1", evictedValue.get());
    }

    @Test
    void testComputeIfAbsent_NewKey() {
        AtomicInteger callCount = new AtomicInteger(0);
        String result =
                lruMap.computeIfAbsent(
                        "key1",
                        k -> {
                            callCount.incrementAndGet();
                            return "computed" + k;
                        });
        Assertions.assertEquals("computedkey1", result);
        Assertions.assertEquals(1, callCount.get());
        Assertions.assertEquals("computedkey1", lruMap.get("key1"));
    }

    @Test
    void testComputeIfAbsent_ExistingKey() {
        lruMap.put("key1", "value1");
        AtomicInteger callCount = new AtomicInteger(0);
        String result =
                lruMap.computeIfAbsent(
                        "key1",
                        k -> {
                            callCount.incrementAndGet();
                            return "computed" + k;
                        });
        Assertions.assertEquals("value1", result);
        Assertions.assertEquals(0, callCount.get());
    }

    @Test
    void testZeroCapacityMap() {
        LRUMap<String, String> zeroCapacityMap = new LRUMap<>(0);
        zeroCapacityMap.put("key1", "value1");
        Assertions.assertNull(zeroCapacityMap.get("key1"));
        Assertions.assertEquals(0, zeroCapacityMap.size());
    }

    @Test
    void testPutAll() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");
        lruMap.putAll(map);
        Assertions.assertEquals(2, lruMap.size());
        Assertions.assertEquals("value1", lruMap.get("key1"));
        Assertions.assertEquals("value2", lruMap.get("key2"));
    }

    @Test
    void testKeySet() {
        lruMap.put("key1", "value1");
        lruMap.put("key2", "value2");
        Assertions.assertEquals(2, lruMap.keySet().size());
        Assertions.assertTrue(lruMap.keySet().contains("key1"));
        Assertions.assertTrue(lruMap.keySet().contains("key2"));
    }

    @Test
    void testValues() {
        lruMap.put("key1", "value1");
        lruMap.put("key2", "value2");
        Assertions.assertEquals(2, lruMap.values().size());
        Assertions.assertTrue(lruMap.values().contains("value1"));
        Assertions.assertTrue(lruMap.values().contains("value2"));
    }

    @Test
    void testEntrySet() {
        lruMap.put("key1", "value1");
        lruMap.put("key2", "value2");
        Assertions.assertEquals(2, lruMap.entrySet().size());
        boolean foundKey1 = false;
        boolean foundKey2 = false;
        for (Map.Entry<String, String> entry : lruMap.entrySet()) {
            if (entry.getKey().equals("key1") && entry.getValue().equals("value1")) {
                foundKey1 = true;
            }
            if (entry.getKey().equals("key2") && entry.getValue().equals("value2")) {
                foundKey2 = true;
            }
        }
        Assertions.assertTrue(foundKey1);
        Assertions.assertTrue(foundKey2);
    }
}
