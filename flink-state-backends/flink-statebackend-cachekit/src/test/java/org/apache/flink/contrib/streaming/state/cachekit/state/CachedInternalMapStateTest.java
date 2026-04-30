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

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
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
                currentKey::set,
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
                0,
                "test");
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
                currentKey::set,
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
                0,
                "test");
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
                currentKey::set,
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
                0,
                "test");
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
                currentKey::set,
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
                0,
                "test");
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
        when(delegate.contains("uk1")).thenReturn(true);

        CachedInternalMapState<String, VoidNamespace, String, Integer> state = new CachedInternalMapState<>(
                delegate,
                currentKey::get,
                currentKey::set,
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
                0,
                "test");
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        for (Map.Entry<String, Integer> ignored : state.entries()) {
            // Consume iterator
        }

        state.contains("uk1");
        verify(delegate, times(1)).contains("uk1");
    }

    @Test
    void testFlushWritesBackDirtyEntries() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);

        CachedInternalMapState<String, VoidNamespace, String, Integer> state = new CachedInternalMapState<>(
                delegate,
                currentKey::get,
                currentKey::set,
                0,
                CachePolicyType.LRU,
                0,
                PresenceCacheImplementation.PRIMITIVE,
                100,
                CachePolicyType.LRU,
                0,
                false,
                0.0,
                1,
                true,
                0,
                "test");
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        state.put("uk1", 42);
        verify(delegate, times(0)).put(any(), any());

        state.flush();
        verify(delegate, times(1)).put("uk1", 42);
    }
}
