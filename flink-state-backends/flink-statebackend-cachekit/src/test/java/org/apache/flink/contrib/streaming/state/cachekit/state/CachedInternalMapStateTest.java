/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PresenceCacheImplementation;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.internal.InternalMapState;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CachedInternalMapStateTest {

    @Test
    void testPresenceCacheSkipsDelegateOnAbsentContains() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        when(delegate.contains("uk1")).thenReturn(false);

        CachedInternalMapState<String, VoidNamespace, String, Integer> state = new CachedInternalMapState<>(
                delegate,
                currentKey::get,
                100,
                CachePolicyType.LRU,
                0,
                PresenceCacheImplementation.PRIMITIVE,
                0,
                CachePolicyType.LRU,
                0,
                false,
                0.0,
                1,
                true,
                false, 0, null, 0);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertFalse(state.contains("uk1"));
        assertFalse(state.contains("uk1"));

        verify(delegate, times(1)).contains("uk1");
    }

    @Test
    void testPresenceCacheUpdatedOnPutAndRemove() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);

        CachedInternalMapState<String, VoidNamespace, String, Integer> state = new CachedInternalMapState<>(
                delegate,
                currentKey::get,
                100,
                CachePolicyType.LRU,
                0,
                PresenceCacheImplementation.PRIMITIVE,
                0,
                CachePolicyType.LRU,
                0,
                false,
                0.0,
                1,
                true,
                false, 0, null, 0);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        state.put("uk1", 1);
        assertTrue(state.contains("uk1"));
        verify(delegate, times(0)).contains(any());

        clearInvocations(delegate);
        state.remove("uk1");
        assertFalse(state.contains("uk1"));
        verify(delegate, times(0)).contains(any());
    }

    @Test
    void testPresenceCacheShortCircuitsGetAfterAbsent() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        when(delegate.contains("uk1")).thenReturn(false);

        CachedInternalMapState<String, VoidNamespace, String, Integer> state = new CachedInternalMapState<>(
                delegate,
                currentKey::get,
                100,
                CachePolicyType.LRU,
                0,
                PresenceCacheImplementation.PRIMITIVE,
                0,
                CachePolicyType.LRU,
                0,
                false,
                0.0,
                1,
                true,
                false, 0, null, 0);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertFalse(state.contains("uk1"));
        clearInvocations(delegate);

        assertNull(state.get("uk1"));
        verify(delegate, times(0)).get(any());
    }

    @Test
    void testBypassEntersAndExitsOnHitRate() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        when(delegate.get(any())).thenReturn(1);

        CachedInternalMapState<String, VoidNamespace, String, Integer> state = new CachedInternalMapState<>(
                delegate,
                currentKey::get,
                0,
                CachePolicyType.LRU,
                0,
                PresenceCacheImplementation.PRIMITIVE,
                100,
                CachePolicyType.LRU,
                0,
                true,
                0.5,
                1,
                true,
                false, 0, null, 0);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        state.get("u1");

        clearInvocations(delegate);
        state.get("u1");
        verify(delegate, times(1)).get("u1");

        state.put("hot", 42);
        clearInvocations(delegate);
        for (int i = 0; i < 98; i++) {
            state.get("hot");
        }
        state.get("hot");
        verify(delegate, times(98)).get("hot");

        clearInvocations(delegate);
        state.get("hot");
        verify(delegate, times(0)).get("hot");
    }

    @Test
    void testIterationCacheFillToggleDisablesBackfill() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        Map<String, Integer> entries = new java.util.HashMap<>();
        entries.put("uk1", 1);
        when(delegate.entries()).thenReturn(entries.entrySet());
        when(delegate.iterator()).thenReturn(Collections.emptyIterator());

        CachedInternalMapState<String, VoidNamespace, String, Integer> state = new CachedInternalMapState<>(
                delegate,
                currentKey::get,
                100,
                CachePolicyType.LRU,
                0,
                PresenceCacheImplementation.PRIMITIVE,
                0,
                CachePolicyType.LRU,
                0,
                false,
                0.0,
                1,
                false,
                false, 0, null, 0);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        for (Map.Entry<String, Integer> ignored : state.entries()) {
            // Consume iterator
        }

        state.contains("uk1");
        verify(delegate, times(1)).contains("uk1");
    }

    @Test
    void testIteratorServedFromCache() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        Map<String, Integer> mockEntries = new java.util.HashMap<>();
        mockEntries.put("A", 1);
        mockEntries.put("B", 2);
        when(delegate.iterator()).thenAnswer(i -> mockEntries.entrySet().iterator());

        CachedInternalMapState<String, VoidNamespace, String, Integer> state = new CachedInternalMapState<>(
                delegate,
                currentKey::get,
                100, CachePolicyType.LRU, 0, PresenceCacheImplementation.PRIMITIVE,
                0, CachePolicyType.LRU, 0, false, 0.0, 1, true,
                true, 100, CachePolicyType.LRU, 1000);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        // First iteration: populates cache
        int count1 = 0;
        for (Map.Entry<String, Integer> ignored : state.entries()) {
            count1++;
        }
        verify(delegate, times(1)).iterator();

        // Second iteration: should be from cache
        clearInvocations(delegate);
        int count2 = 0;
        for (Map.Entry<String, Integer> ignored : state.entries()) {
            count2++;
        }
        // delegate.iterator() should NOT be called again
        verify(delegate, times(0)).iterator();
    }

    @Test
    void testIteratorCacheUpdates() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        Map<String, Integer> mockEntries = new java.util.HashMap<>();
        mockEntries.put("A", 1);
        when(delegate.iterator()).thenAnswer(i -> mockEntries.entrySet().iterator());
        doAnswer(invocation -> {
            mockEntries.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(delegate).put(any(), any());

        CachedInternalMapState<String, VoidNamespace, String, Integer> state = new CachedInternalMapState<>(
                delegate,
                currentKey::get,
                100, CachePolicyType.LRU, 0, PresenceCacheImplementation.PRIMITIVE,
                0, CachePolicyType.LRU, 0, false, 0.0, 1, true,
                true, 100, CachePolicyType.LRU, 1000);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        // Populate cache
        for (Map.Entry<String, Integer> ignored : state.entries()) {
        }

        // Add new item
        state.put("B", 2);
        // Delegate put OK to be called
        verify(delegate, times(1)).put("B", 2);

        // Iterate again
        clearInvocations(delegate);
        Map<String, Integer> result = new java.util.HashMap<>();
        for (Map.Entry<String, Integer> e : state.entries()) {
            result.put(e.getKey(), e.getValue());
        }

        // Should contain both
        assertTrue(result.containsKey("A"));
        assertTrue(result.containsKey("B"));
        // Should not call delegate iterator
        verify(delegate, times(0)).iterator();
    }
}
