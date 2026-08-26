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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import static org.mockito.Mockito.withSettings;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.contrib.streaming.state.RocksDBBatchMapReader;
import org.apache.flink.contrib.streaming.state.RocksDBBatchValueReader;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PresenceCacheImplementation;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlane;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneCoordinator;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneOptions;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.junit.jupiter.api.Test;

class CachedInternalMapStateTest {

    @Test
    @SuppressWarnings("unchecked")
    void testExactDistinctBatchPrefetchStagesFoundAndMissingValues() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(
                        InternalMapState.class,
                        withSettings().extraInterfaces(RocksDBBatchMapReader.class));
        when(delegate.getValueSerializer())
                .thenReturn(
                        new MapSerializer<>(
                                org.apache.flink.api.common.typeutils.base.StringSerializer
                                        .INSTANCE,
                                IntSerializer.INSTANCE));
        RocksDBBatchMapReader<String> reader = (RocksDBBatchMapReader<String>) delegate;
        when(reader.getSerializedValuesByUserKeys(Arrays.asList("u1", "u2")))
                .thenReturn(Arrays.asList(serializedMapValue(7), null));

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
                        0);
        state.enableNativeDistinctBatchPrefetch(true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertTrue(state.beginPrefetchCurrentKeys(Arrays.asList("u1", "u2", "u1")));
        assertEquals(7, state.get("u1"));
        assertNull(state.get("u2"));
        assertTrue(state.contains("u1"));
        assertFalse(state.contains("u2"));
        assertEquals(1, state.getBatchPrefetchBatchesForTesting());
        assertEquals(2, state.getBatchPrefetchUniqueKeysForTesting());
        assertEquals(4, state.getBatchPrefetchHitsForTesting());
        verify(delegate, times(0)).get(any());
        verify(delegate, times(0)).contains(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testExactDistinctDirectValuesAvoidSecondStagingMap() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(
                        InternalMapState.class,
                        withSettings().extraInterfaces(RocksDBBatchMapReader.class));
        when(delegate.getValueSerializer())
                .thenReturn(
                        new MapSerializer<>(
                                org.apache.flink.api.common.typeutils.base.StringSerializer
                                        .INSTANCE,
                                IntSerializer.INSTANCE));
        RocksDBBatchMapReader<String> reader = (RocksDBBatchMapReader<String>) delegate;
        when(reader.getSerializedValuesByUserKeys(Arrays.asList("u1", "u2")))
                .thenReturn(Arrays.asList(serializedMapValue(7), null));

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
                        0);
        state.enableNativeDistinctBatchPrefetch(true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertTrue(state.supportsDirectPrefetchedValues());
        assertEquals(
                Arrays.asList(7, null),
                state.prefetchCurrentUniqueKeyValues(Arrays.asList("u1", "u2")));
        assertEquals(1, state.getBatchPrefetchBatchesForTesting());
        assertEquals(2, state.getBatchPrefetchUniqueKeysForTesting());
        assertEquals(2, state.getBatchPrefetchDirectOverlayValuesForTesting());
        assertEquals(0, state.getBatchPrefetchHitsForTesting());
        verify(delegate, times(0)).get(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testExactDistinctDirectArenaMaterializesFoundAndMissingWithoutHeapValueList()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(
                        InternalMapState.class,
                        withSettings().extraInterfaces(RocksDBBatchMapReader.class));
        when(delegate.getValueSerializer())
                .thenReturn(new MapSerializer<>(
                        org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE,
                        IntSerializer.INSTANCE));
        RocksDBBatchMapReader<String> reader = (RocksDBBatchMapReader<String>) delegate;
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        when(reader.directArenaMultiGetMaxBatch())
                .thenReturn(RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH);
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u1", "u2")))
                .thenReturn(Arrays.asList(new byte[] {1}, new byte[] {2}));
        byte[] present = serializedMapValue(7);
        doAnswer(
                        invocation -> {
                            ByteBuffer descriptors =
                                    ((ByteBuffer) invocation.getArgument(1))
                                            .duplicate()
                                            .order(ByteOrder.nativeOrder());
                            int count = invocation.getArgument(2);
                            ByteBuffer values = ((ByteBuffer) invocation.getArgument(3)).duplicate();
                            int stride = invocation.getArgument(4);
                            assertEquals(2, count);
                            assertTrue(stride >= present.length);
                            values.position(0);
                            values.put(present);
                            descriptors.putInt(
                                    RocksDBBatchValueReader.DIRECT_ARENA_RESULT_OFFSET,
                                    present.length);
                            descriptors.putInt(
                                    RocksDBBatchValueReader.DIRECT_ARENA_DESCRIPTOR_BYTES
                                            + RocksDBBatchValueReader.DIRECT_ARENA_RESULT_OFFSET,
                                    RocksDBBatchValueReader.DIRECT_ARENA_NOT_FOUND);
                            return 1;
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeyArena(
                        any(ByteBuffer.class),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.eq(2),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.anyInt());

        NativeRequestPlane plane = mock(NativeRequestPlane.class);
        when(plane.selectedKernel()).thenReturn("test");
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), plane);
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
                        0,
                        MapSnapshotCacheMetrics.disabled(),
                        coordinator,
                        31,
                        false,
                        0,
                        false);
        state.enableNativeDistinctBatchPrefetch(true, true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        // An asynchronous prefetch may occupy every regular slot. Exact-DISTINCT direct-arena
        // transport owns a separate bounded mailbox-thread slot and must remain closed.
        try (NativeRequestPlaneCoordinator.BatchSlot regular =
                coordinator.tryAcquireBatchSlot()) {
            assertEquals(
                    Arrays.asList(7, null),
                    state.prefetchCurrentUniqueKeyValues(Arrays.asList("u1", "u2")));
        }
        assertEquals(2, state.getBatchPrefetchDirectArenaCompletedKeysForTesting());
        assertEquals(0, state.getBatchPrefetchDirectArenaFallbacksForTesting());
        verify(reader, times(0)).getSerializedValuesByUserKeys(any());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testExactDistinctBatchPrefetchInvalidatesOnOuterKeyChangeAndTracksWrites()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(
                        InternalMapState.class,
                        withSettings().extraInterfaces(RocksDBBatchMapReader.class));
        when(delegate.getValueSerializer())
                .thenReturn(
                        new MapSerializer<>(
                                org.apache.flink.api.common.typeutils.base.StringSerializer
                                        .INSTANCE,
                                IntSerializer.INSTANCE));
        RocksDBBatchMapReader<String> reader = (RocksDBBatchMapReader<String>) delegate;
        when(reader.getSerializedValuesByUserKeys(Arrays.asList("u1", "u2")))
                .thenReturn(Arrays.asList(serializedMapValue(1), serializedMapValue(2)));
        when(delegate.get("u1")).thenReturn(99);

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
                        0);
        state.enableNativeDistinctBatchPrefetch(true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertTrue(state.beginPrefetchCurrentKeys(Arrays.asList("u1", "u2")));
        state.put("u1", 11);
        assertEquals(11, state.get("u1"));
        state.remove("u2");
        assertFalse(state.contains("u2"));

        currentKey.set("k2");
        assertEquals(99, state.get("u1"));
        verify(delegate, times(1)).get("u1");
    }

    @Test
    void testExactDistinctBatchPrefetchFailsClosedWithoutRocksDBCapability() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
        when(delegate.getValueSerializer())
                .thenReturn(
                        new MapSerializer<>(
                                org.apache.flink.api.common.typeutils.base.StringSerializer
                                        .INSTANCE,
                                IntSerializer.INSTANCE));
        when(delegate.get("u1")).thenReturn(5);
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
                        0);
        state.enableNativeDistinctBatchPrefetch(true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertFalse(state.beginPrefetchCurrentKeys(Arrays.asList("u1", "u2")));
        assertEquals(5, state.get("u1"));
        assertEquals(1, state.getBatchPrefetchFallbacksForTesting());
    }

    @Test
    void testPresenceCacheSkipsDelegateOnAbsentContains() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
        when(delegate.contains("uk1")).thenReturn(false);

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
                0);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertFalse(state.contains("uk1"));
        assertFalse(state.contains("uk1"));

        verify(delegate, times(1)).contains("uk1");
    }

    @Test
    void testPresenceCacheUpdatedOnPutAndRemove() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);

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
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
        when(delegate.contains("uk1")).thenReturn(false);

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
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
        when(delegate.get(any())).thenReturn(1);

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
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
        Map<String, Integer> entries = new java.util.HashMap<>();
        entries.put("uk1", 1);
        when(delegate.entries()).thenReturn(entries.entrySet());
        when(delegate.contains("uk1")).thenReturn(true);

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
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);

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
        verify(delegate, times(0)).put(any(), any());

        state.flush();
        verify(delegate, times(1)).put("uk1", 42);
    }

    @Test
    void testFlushDoesNotWriteAfterClose() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);

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
    void testEntriesIteratorRemoveUsesConsumedDelegateAndInvalidatesValueCache() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
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
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
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
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
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
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
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
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
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
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
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

        // A snapshot miss must still flush deferred writes for the current key before using
        // delegate.
        currentKey.set("k2");
        clearInvocations(delegate);
        assertFalse(state.entries().iterator().hasNext());
        verify(delegate, times(5)).put(any(), any());
    }

    @Test
    void testSnapshotMissFlushesOnlyCurrentKeyDirtyMapCache() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
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
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
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
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
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

        // L1 has a minimum size of 128. The 129th write flushes exactly one dirty entry on
        // eviction.
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
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
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
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
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
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
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

    @Test
    void testSmallSnapshotShortCircuitPreservesCapturedMutationContext() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
        Map<String, Integer> entries = new LinkedHashMap<>();
        entries.put("uk1", 1);
        entries.put("uk2", 2);
        when(delegate.entries()).thenReturn(entries.entrySet());
        when(delegate.get("uk1")).thenReturn(1);
        when(delegate.get("uk2")).thenReturn(2);
        doAnswer(
                        invocation -> {
                            assertEquals("k1", currentKey.get());
                            return null;
                        })
                .when(delegate)
                .put("uk1", 10);
        doAnswer(
                        invocation -> {
                            assertEquals("k1", currentKey.get());
                            return null;
                        })
                .when(delegate)
                .remove("uk2");
        MapSnapshotCacheMetrics metrics = MapSnapshotCacheMetrics.forTesting();
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createSmallSnapshotState(delegate, currentKey, metrics);

        for (Map.Entry<String, Integer> ignored : state.entries()) {
            // Complete traversal publishes the bounded two-entry snapshot.
        }
        assertEquals(1, metrics.storesSmall());
        assertTrue(metrics.diagnosticSummary().contains("storesSmall=1"));

        Iterable<Map.Entry<String, Integer>> snapshotEntries = state.entries();
        Iterator<Map.Entry<String, Integer>> iterator = snapshotEntries.iterator();
        currentKey.set("later-key");
        Map.Entry<String, Integer> first = iterator.next();
        assertEquals(1, first.setValue(10));
        assertEquals("later-key", currentKey.get());
        assertEquals(10, snapshotEntries.iterator().next().getValue());
        iterator.next();
        iterator.remove();
        assertEquals("later-key", currentKey.get());
        assertEquals(1, consumeEntries(snapshotEntries));

        verify(delegate, times(1)).entries();
        verify(delegate, times(1)).put("uk1", 10);
        verify(delegate, times(1)).remove("uk2");
        assertEquals(1, metrics.smallShortCircuits());
    }

    @Test
    void testSmallSnapshotKeysIteratorForwardsRemove() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
        Map<String, Integer> entries = new LinkedHashMap<>();
        entries.put("uk1", 1);
        entries.put("uk2", 2);
        when(delegate.entries()).thenReturn(entries.entrySet());
        when(delegate.get("uk1")).thenReturn(1);
        when(delegate.get("uk2")).thenReturn(2);
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createSmallSnapshotState(
                        delegate, currentKey, MapSnapshotCacheMetrics.forTesting());

        for (Map.Entry<String, Integer> ignored : state.entries()) {
            // Backfill.
        }
        Iterator<String> keys = state.keys().iterator();
        assertEquals("uk1", keys.next());
        keys.remove();

        verify(delegate, times(1)).remove("uk1");
        verify(delegate, times(1)).entries();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSnapshotDoesNotExposeMutableInternalUserKey() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, byte[], Integer> delegate =
                mock(InternalMapState.class);
        when(delegate.getValueSerializer())
                .thenReturn(
                        new MapSerializer<>(
                                BytePrimitiveArraySerializer.INSTANCE, IntSerializer.INSTANCE));
        byte[] original = new byte[] {1, 2};
        Map<byte[], Integer> entries = new LinkedHashMap<>();
        entries.put(original, 7);
        when(delegate.entries()).thenReturn(entries.entrySet());
        when(delegate.get(any(byte[].class))).thenReturn(7);
        CachedInternalMapState<String, VoidNamespace, byte[], Integer> state =
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
                        MapSnapshotCacheMetrics.forTesting(),
                        null,
                        0,
                        false,
                        0,
                        false,
                        2);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        for (Map.Entry<byte[], Integer> ignored : state.entries()) {
            // Backfill a copied SINGLE key.
        }
        Iterator<Map.Entry<byte[], Integer>> iterator = state.entries().iterator();
        Map.Entry<byte[], Integer> entry = iterator.next();
        entry.getKey()[0] = 99;
        iterator.remove();

        verify(delegate)
                .remove(
                        org.mockito.ArgumentMatchers.argThat(
                                value -> {
                                    assertArrayEquals(new byte[] {1, 2}, value);
                                    return true;
                                }));
    }

    @Test
    void testDelegateKeysRemoveInvalidatesFilledCaches() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
        Map<String, Integer> entries = new LinkedHashMap<>();
        entries.put("uk1", 1);
        when(delegate.entries()).thenReturn(entries.entrySet());
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
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

        Iterator<String> keys = state.keys().iterator();
        assertEquals("uk1", keys.next());
        keys.remove();

        assertNull(state.get("uk1"));
        assertFalse(state.contains("uk1"));
        verify(delegate, times(0)).get("uk1");
        verify(delegate, times(0)).contains("uk1");
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDelegateKeysIteratorCapturesMutableStateKey() throws Exception {
        MutableKey originalKey = new MutableKey(1);
        AtomicReference<MutableKey> currentKey = new AtomicReference<>(originalKey);
        InternalMapState<MutableKey, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
        TypeSerializer<MutableKey> keySerializer = mock(TypeSerializer.class);
        when(keySerializer.copy(any(MutableKey.class)))
                .thenAnswer(
                        invocation -> {
                            MutableKey source = invocation.getArgument(0);
                            return new MutableKey(source.value);
                        });
        when(delegate.getKeySerializer()).thenReturn(keySerializer);
        Map<String, Integer> entries = new LinkedHashMap<>();
        entries.put("uk1", 1);
        when(delegate.entries()).thenReturn(entries.entrySet());
        CachedInternalMapState<MutableKey, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        PresenceCacheImplementation.OBJECT,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.0,
                        1,
                        true,
                        0);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        Iterator<String> keys = state.keys().iterator();
        assertEquals("uk1", keys.next());
        originalKey.value = 2;
        keys.remove();

        currentKey.set(new MutableKey(1));
        assertFalse(state.contains("uk1"));
        verify(delegate, times(0)).contains("uk1");
    }

    private static int consumeEntries(Iterable<Map.Entry<String, Integer>> entries) {
        int count = 0;
        for (Map.Entry<String, Integer> ignored : entries) {
            count++;
        }
        return count;
    }

    private static byte[] serializedMapValue(Integer value) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(16);
        out.writeBoolean(value == null);
        if (value != null) {
            IntSerializer.INSTANCE.serialize(value, out);
        }
        return out.getCopyOfBuffer();
    }

    private static NativeRequestPlaneOptions directArenaOptions() {
        return new NativeRequestPlaneOptions(
                true,
                "",
                "auto",
                128,
                1 << 20,
                1 << 20,
                16,
                1 << 20,
                1 << 20,
                1,
                2,
                false,
                false,
                false,
                false,
                false,
                true,
                true,
                false,
                true,
                true,
                false,
                8192,
                0.02,
                262144);
    }

    private static final class MutableKey {
        private int value;

        private MutableKey(int value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof MutableKey && value == ((MutableKey) other).value;
        }

        @Override
        public int hashCode() {
            return value;
        }
    }

    private static CachedInternalMapState<String, VoidNamespace, String, Integer>
            createSmallSnapshotState(
                    InternalMapState<String, VoidNamespace, String, Integer> delegate,
                    AtomicReference<String> currentKey,
                    MapSnapshotCacheMetrics metrics) {
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
                        null,
                        0,
                        false,
                        0,
                        false,
                        3);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        return state;
    }
}
