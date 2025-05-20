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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CacheEntryTest {

    @Test
    void testCleanEntry() {
        String testValue = "testValue";
        CacheEntry<String> entry = CacheEntry.clean(testValue);
        assertEquals(testValue, entry.getValue());
        assertFalse(entry.isDirty());
    }

    @Test
    void testDirtyEntry() {
        String testValue = "testValue";
        CacheEntry<String> entry = CacheEntry.dirty(testValue);
        assertEquals(testValue, entry.getValue());
        assertTrue(entry.isDirty());
    }

    @Test
    void testGetValue() {
        String testValue = "testValue";
        CacheEntry<String> entry = new CacheEntry<>(testValue, false);
        assertEquals(testValue, entry.getValue());
        String newValue = "newValue";
        entry.setValue(newValue);
        assertEquals(newValue, entry.getValue());
    }

    @Test
    void testIsDirty() {
        CacheEntry<String> cleanEntry = new CacheEntry<>("value", false);
        assertFalse(cleanEntry.isDirty());
        CacheEntry<String> dirtyEntry = new CacheEntry<>("value", true);
        assertTrue(dirtyEntry.isDirty());
    }

    @Test
    void testSetDirty() {
        CacheEntry<String> entry = new CacheEntry<>("value", false);
        assertFalse(entry.isDirty());
        entry.setDirty(true);
        assertTrue(entry.isDirty());
        entry.setDirty(false);
        assertFalse(entry.isDirty());
    }

    @Test
    void testCacheNullValue() {
        CacheEntry<String> cleanNullEntry = CacheEntry.clean(null);
        assertNull(cleanNullEntry.getValue());
        assertFalse(cleanNullEntry.isDirty());
        CacheEntry<String> dirtyNullEntry = CacheEntry.dirty(null);
        assertNull(dirtyNullEntry.getValue());
        assertTrue(dirtyNullEntry.isDirty());
    }
}
