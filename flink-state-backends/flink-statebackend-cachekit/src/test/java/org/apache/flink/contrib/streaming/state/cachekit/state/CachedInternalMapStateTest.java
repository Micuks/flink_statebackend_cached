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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PresenceCacheImplementation;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    void testNativeSnapshotSingleBackfillAndShortCircuit() throws Exception {
        String library = System.getProperty("cachekit.native.snapshot.library");
        Assumptions.assumeTrue(library != null && Files.isRegularFile(Path.of(library)));
        Assumptions.assumeTrue(
                NativeMapSnapshotCache.snapshotFeatureAvailable(library, true));

        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        Map<String, Integer> entries = new LinkedHashMap<>();
        entries.put("uk1", 1);
        when(delegate.entries()).thenReturn(entries.entrySet());
        when(delegate.get("uk1")).thenReturn(1);
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer())
                .thenReturn(new MapSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE));
        MapSnapshotCacheMetrics metrics = MapSnapshotCacheMetrics.forTesting();

        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        0,
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
                        100,
                        metrics,
                        true,
                        "SCALAR",
                        library);
        try {
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            for (Map.Entry<String, Integer> ignored : state.entries()) {
                // Consume the delegate traversal and populate the Native SINGLE snapshot.
            }

            clearInvocations(delegate);
            assertEquals("uk1", state.entries().iterator().next().getKey());

            verify(delegate, times(0)).entries();
            verify(delegate, times(1)).get("uk1");
            assertEquals(1, metrics.hits());
            assertEquals(1, metrics.singleShortCircuits());
        } finally {
            state.close();
        }
    }

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
                0);
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
                0);
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
                0);
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
                0);
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
                0);
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
                0);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        state.put("uk1", 42);
        verify(delegate, times(0)).put(any(), any());

        state.flush();
        verify(delegate, times(1)).put("uk1", 42);
    }

    @Test
    void testFlushDoesNotWriteAfterClose() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);

        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
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
                        0);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        state.put("uk1", 42);

        state.close();
        state.flush();

        verify(delegate, times(0)).put(any(), any());
    }

    @Test
    void testEntriesIteratorRemoveUsesConsumedDelegateAndInvalidatesValueCache()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        Map<String, Integer> entries = new LinkedHashMap<>();
        entries.put("uk1", 1);
        when(delegate.entries()).thenReturn(entries.entrySet());

        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
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
                        100);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        Iterator<Map.Entry<String, Integer>> iterator = state.entries().iterator();
        assertEquals(1, iterator.next().getValue());
        iterator.remove();

        assertTrue(entries.isEmpty());
        assertNull(state.get("uk1"));
        verify(delegate, times(0)).get("uk1");
    }

    @Test
    void testDirectIteratorRemoveInvalidatesPresenceCache() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        Map<String, Integer> entries = new LinkedHashMap<>();
        entries.put("uk1", 1);
        when(delegate.iterator()).thenAnswer(ignored -> entries.entrySet().iterator());

        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
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
                        100);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        Iterator<Map.Entry<String, Integer>> iterator = state.iterator();
        assertEquals(1, iterator.next().getValue());
        iterator.remove();

        assertTrue(entries.isEmpty());
        assertFalse(state.contains("uk1"));
        verify(delegate, times(0)).contains("uk1");
    }

    @Test
    void testEntriesIteratorRemoveWorksWithoutSnapshotCache() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        Map<String, Integer> entries = new LinkedHashMap<>();
        entries.put("uk1", 1);
        when(delegate.entries()).thenReturn(entries.entrySet());

        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
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
                        0);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        Iterator<Map.Entry<String, Integer>> iterator = state.entries().iterator();
        iterator.next();
        iterator.remove();

        assertTrue(entries.isEmpty());
        assertNull(state.get("uk1"));
    }

    @Test
    void testEntriesIteratorRemoveInvalidatesExistingCacheWhenIterationFillDisabled()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        Map<String, Integer> entries = new LinkedHashMap<>();
        entries.put("uk1", 1);
        when(delegate.entries()).thenReturn(entries.entrySet());
        when(delegate.get("uk1")).thenReturn(1);

        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
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
                        false,
                        0);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertEquals(1, state.get("uk1"));
        Iterator<Map.Entry<String, Integer>> iterator = state.entries().iterator();
        iterator.next();
        iterator.remove();

        assertTrue(entries.isEmpty());
        assertNull(state.get("uk1"));
        verify(delegate, times(1)).get("uk1");
    }

    @Test
    void testSnapshotHitIteratorRemoveUsesMapStateRemove() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        Map<String, Integer> entries = new LinkedHashMap<>();
        entries.put("uk1", 1);
        when(delegate.entries()).thenReturn(entries.entrySet());

        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
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
                        100);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        for (Map.Entry<String, Integer> ignored : state.entries()) {
            // Consume the first traversal to backfill a SINGLE snapshot.
        }

        Iterator<Map.Entry<String, Integer>> iterator = state.entries().iterator();
        assertEquals(1, iterator.next().getValue());
        iterator.remove();

        assertNull(state.get("uk1"));
        state.flush();
        verify(delegate).remove("uk1");
    }

    @Test
    void testSnapshotReadPathsAvoidFlushingOtherKeysDirtyMapCache() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        when(delegate.entries()).thenReturn(java.util.Collections.emptyList());

        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
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
                        false,
                        100);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        // A complete empty traversal creates the K1 EMPTY snapshot.
        assertFalse(state.entries().iterator().hasNext());

        currentKey.set("k2");
        state.put("dirty-entries", 1);
        currentKey.set("k1");
        clearInvocations(delegate);
        assertFalse(state.entries().iterator().hasNext());
        verify(delegate, times(0)).put(any(), any());

        currentKey.set("k2");
        state.put("dirty-keys", 2);
        currentKey.set("k1");
        clearInvocations(delegate);
        assertFalse(state.keys().iterator().hasNext());
        verify(delegate, times(0)).put(any(), any());

        currentKey.set("k2");
        state.put("dirty-values", 3);
        currentKey.set("k1");
        clearInvocations(delegate);
        assertFalse(state.values().iterator().hasNext());
        verify(delegate, times(0)).put(any(), any());

        currentKey.set("k2");
        state.put("dirty-iterator", 4);
        currentKey.set("k1");
        clearInvocations(delegate);
        assertFalse(state.iterator().hasNext());
        verify(delegate, times(0)).put(any(), any());

        currentKey.set("k2");
        state.put("dirty-is-empty", 5);
        currentKey.set("k1");
        clearInvocations(delegate);
        assertTrue(state.isEmpty());
        verify(delegate, times(0)).put(any(), any());

        // A snapshot miss must still flush deferred writes for the current key before using delegate.
        currentKey.set("k2");
        clearInvocations(delegate);
        assertFalse(state.entries().iterator().hasNext());
        verify(delegate, times(5)).put(any(), any());
    }

    @Test
    void testSnapshotMissFlushesOnlyCurrentKeyDirtyMapCache() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        when(delegate.entries()).thenReturn(java.util.Collections.emptyList());

        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
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
                        false,
                        100);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        currentKey.set("k2");
        state.put("dirty-k2", 1);
        currentKey.set("k1");

        clearInvocations(delegate);
        assertFalse(state.entries().iterator().hasNext());
        verify(delegate, times(0)).put(any(), any());

        currentKey.set("k2");
        clearInvocations(delegate);
        assertFalse(state.entries().iterator().hasNext());
        verify(delegate).put("dirty-k2", 1);
    }

    @Test
    void testClearDiscardsDirtyEntriesFromScopedFlushIndex() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        when(delegate.entries()).thenReturn(java.util.Collections.emptyList());

        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
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
                        false,
                        100);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        state.put("dirty", 1);
        state.clear();
        clearInvocations(delegate);

        assertFalse(state.entries().iterator().hasNext());
        verify(delegate, times(0)).put(any(), any());
    }

    @Test
    void testScopedFlushWritesRemainingDirtyEntriesAfterL1Eviction() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        when(delegate.entries()).thenReturn(java.util.Collections.emptyList());

        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
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
                        false,
                        0);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        // L1 has a minimum size of 128. The 129th write flushes exactly one dirty entry on eviction.
        for (int i = 0; i < 129; i++) {
            state.put("dirty-" + i, i);
        }
        clearInvocations(delegate);

        assertFalse(state.entries().iterator().hasNext());
        verify(delegate, times(128)).put(any(), any());
    }

    @Test
    void testDisabledSnapshotMetricsDoNotRecord() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        when(delegate.entries()).thenReturn(java.util.Collections.emptyList());
        MapSnapshotCacheMetrics metrics = MapSnapshotCacheMetrics.disabled();

        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        0,
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
                        100,
                        metrics);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertFalse(state.entries().iterator().hasNext());
        assertFalse(state.entries().iterator().hasNext());

        assertEquals(0, metrics.probes());
        assertEquals(0, metrics.hits());
        assertEquals(0, metrics.misses());
        assertEquals(0, metrics.emptyShortCircuits());
        assertEquals(0, metrics.storesEmpty());
    }

    @Test
    void testSnapshotMetricsRecordEmptyBackfillAndShortCircuit() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        when(delegate.entries()).thenReturn(java.util.Collections.emptyList());
        MapSnapshotCacheMetrics metrics = MapSnapshotCacheMetrics.forTesting();

        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        0,
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
                        100,
                        metrics);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        for (Map.Entry<String, Integer> ignored : state.entries()) {
            // Snapshot backfill remains enabled even when element cache fill is disabled.
        }

        assertEquals(1, metrics.probes());
        assertEquals(1, metrics.misses());
        assertEquals(1, metrics.storesEmpty());

        clearInvocations(delegate);
        assertFalse(state.entries().iterator().hasNext());

        verify(delegate, times(0)).entries();
        assertEquals(2, metrics.probes());
        assertEquals(1, metrics.hits());
        assertEquals(1, metrics.emptyShortCircuits());
    }

    @Test
    void testSnapshotMetricsRecordSingleBackfillAndShortCircuit() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate = mock(InternalMapState.class);
        Map<String, Integer> entries = new LinkedHashMap<>();
        entries.put("uk1", 1);
        when(delegate.entries()).thenReturn(entries.entrySet());
        when(delegate.get("uk1")).thenReturn(1);
        MapSnapshotCacheMetrics metrics = MapSnapshotCacheMetrics.forTesting();

        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        0,
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
                        100,
                        metrics);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        for (Map.Entry<String, Integer> ignored : state.entries()) {
            // Consume the first traversal to backfill a SINGLE snapshot.
        }

        assertEquals(1, metrics.probes());
        assertEquals(1, metrics.misses());
        assertEquals(1, metrics.storesSingle());

        clearInvocations(delegate);
        assertEquals("uk1", state.entries().iterator().next().getKey());

        verify(delegate, times(0)).entries();
        verify(delegate, times(1)).get("uk1");
        assertEquals(2, metrics.probes());
        assertEquals(1, metrics.hits());
        assertEquals(1, metrics.singleShortCircuits());
    }
}
