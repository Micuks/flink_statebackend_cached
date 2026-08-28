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
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.contrib.streaming.state.RocksDBBatchMapReader;
import org.apache.flink.contrib.streaming.state.RocksDBBatchValueReader;
import org.apache.flink.contrib.streaming.state.cachekit.PrefetchExecutor;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PresenceCacheImplementation;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlane;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneCoordinator;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneOptions;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.internal.BatchPrefetchableMapState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.junit.jupiter.api.Test;

class CachedInternalMapStateTest {

    @Test
    @SuppressWarnings("unchecked")
    void testDeferredWaveLeavesAsyncThresholdGroupOnEstablishedAsyncPath() throws Exception {
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
        List<String> userKeys = eightUserKeys();
        List<byte[]> rocksDBKeys = eightSerializedKeys();
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        when(reader.directArenaMultiGetMaxBatch())
                .thenReturn(RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH);
        when(reader.serializeRocksDBKeysByUserKeys(userKeys)).thenReturn(rocksDBKeys);
        byte[] present = serializedMapValue(7);
        doAnswer(
                        invocation -> {
                            writeDirectResults(invocation, present, false);
                            return 1;
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeyArena(
                        any(ByteBuffer.class),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.eq(8),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.anyInt());

        NativeRequestPlane plane = mock(NativeRequestPlane.class);
        when(plane.selectedKernel()).thenReturn("test");
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createDeferredWaveState(delegate, currentKey, coordinator);

        BatchPrefetchableMapState.PreparedValues prepared =
                state.prepareCurrentUniqueKeyValues(userKeys);

        assertNull(prepared.waveOwner());
        assertEquals(
                BatchPrefetchableMapState.PreparedValues.WaveParticipation.INELIGIBLE,
                prepared.waveParticipation());
        assertEquals(
                Arrays.asList(7, null, null, null, null, null, null, null), prepared.awaitValues());
        assertEquals(1, state.getAsyncBatchPrefetchSubmittedForTesting());
        assertEquals(0, state.getDeferredWaveWindowsForTesting());
        assertEquals(0, state.getDeferredSyncBatchPrefetchBatchesForTesting());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPreparedExactDistinctAsyncDirectResultReturnsSlotAfterMaterialization()
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
        List<String> userKeys = eightUserKeys();
        List<byte[]> rocksDBKeys = eightSerializedKeys();
        when(reader.serializeRocksDBKeysByUserKeys(userKeys)).thenReturn(rocksDBKeys);
        byte[] present = serializedMapValue(7);
        doAnswer(
                        invocation -> {
                            writeDirectResults(invocation, present, false);
                            return 1;
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeyArena(
                        any(ByteBuffer.class),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.eq(8),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.anyInt());

        NativeRequestPlane plane = mock(NativeRequestPlane.class);
        when(plane.selectedKernel()).thenReturn("test");
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createDirectAsyncState(delegate, currentKey, coordinator);

        BatchPrefetchableMapState.PreparedValues prepared =
                state.prepareCurrentUniqueKeyValues(userKeys);
        assertEquals(
                Arrays.asList(7, null, null, null, null, null, null, null), prepared.awaitValues());
        assertEquals(1, coordinator.mapDistinctAsyncReadLeases());
        assertEquals(0, coordinator.mapDistinctAsyncReadLeaseMisses());
        verify(reader, times(0))
                .getSerializedValuesByRocksDBKeys(
                        any(),
                        org.mockito.ArgumentMatchers.anyInt(),
                        org.mockito.ArgumentMatchers.anyInt());
        verify(reader, times(0)).getSerializedValuesByUserKeys(any());

        NativeRequestPlaneCoordinator.BatchSlot returned =
                coordinator.tryAcquireMapDistinctAsyncReadSlot();
        assertNotNull(returned);
        returned.close();
        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPreparedExactDistinctAsyncLeaseMissFallsBackToHeapBatchRead() throws Exception {
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
        List<String> userKeys = eightUserKeys();
        List<byte[]> rocksDBKeys = eightSerializedKeys();
        when(reader.serializeRocksDBKeysByUserKeys(userKeys)).thenReturn(rocksDBKeys);
        when(reader.getSerializedValuesByRocksDBKeys(
                        any(),
                        org.mockito.ArgumentMatchers.eq(0),
                        org.mockito.ArgumentMatchers.eq(8)))
                .thenAnswer(
                        invocation -> {
                            List<byte[]> actualKeys = invocation.getArgument(0);
                            assertEquals(8, actualKeys.size());
                            for (int index = 0; index < actualKeys.size(); index++) {
                                assertArrayEquals(rocksDBKeys.get(index), actualKeys.get(index));
                            }
                            return Arrays.asList(
                                    serializedMapValue(11),
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null,
                                    null);
                        });

        NativeRequestPlane plane = mock(NativeRequestPlane.class);
        when(plane.selectedKernel()).thenReturn("test");
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createDirectAsyncState(delegate, currentKey, coordinator);

        try (NativeRequestPlaneCoordinator.BatchSlot occupiedFirst =
                        coordinator.tryAcquireMapDistinctAsyncReadSlot();
                NativeRequestPlaneCoordinator.BatchSlot occupiedSecond =
                        coordinator.tryAcquireMapDistinctAsyncReadSlot()) {
            assertNotNull(occupiedFirst);
            assertNotNull(occupiedSecond);
            BatchPrefetchableMapState.PreparedValues prepared =
                    state.prepareCurrentUniqueKeyValues(userKeys);
            List<?> values = prepared.awaitValues();
            verify(reader, times(1))
                    .getSerializedValuesByRocksDBKeys(
                            any(),
                            org.mockito.ArgumentMatchers.eq(0),
                            org.mockito.ArgumentMatchers.eq(8));
            assertEquals(Arrays.asList(11, null, null, null, null, null, null, null), values);
        }
        assertEquals(1, coordinator.mapDistinctAsyncReadLeaseMisses());
        verify(reader, times(0))
                .getSerializedValuesByRocksDBKeyArena(
                        any(ByteBuffer.class),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.anyInt(),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.anyInt());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPreparedExactDistinctAsyncOverflowFailsClosedAndReleasesSlot() throws Exception {
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
        List<String> userKeys = eightUserKeys();
        List<byte[]> rocksDBKeys = eightSerializedKeys();
        when(reader.serializeRocksDBKeysByUserKeys(userKeys)).thenReturn(rocksDBKeys);
        byte[] oversized = serializedMapValue(23);
        doAnswer(
                        invocation -> {
                            writeDirectResults(invocation, oversized, true);
                            return 1;
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeyArena(
                        any(ByteBuffer.class),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.eq(8),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.anyInt());

        NativeRequestPlane plane = mock(NativeRequestPlane.class);
        when(plane.selectedKernel()).thenReturn("test");
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createDirectAsyncState(delegate, currentKey, coordinator);

        BatchPrefetchableMapState.PreparedValues prepared =
                state.prepareCurrentUniqueKeyValues(userKeys);
        assertNull(prepared.awaitValues());
        verify(reader, times(0))
                .getSerializedValuesByRocksDBKeys(
                        any(),
                        org.mockito.ArgumentMatchers.anyInt(),
                        org.mockito.ArgumentMatchers.anyInt());

        NativeRequestPlaneCoordinator.BatchSlot returned =
                coordinator.tryAcquireMapDistinctAsyncReadSlot();
        assertNotNull(returned);
        returned.close();
        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPreparedExactDistinctReadUsesImmutableRocksDBKeysOffMailbox() throws Exception {
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
        List<byte[]> immutableKeys = Arrays.asList(new byte[] {1}, new byte[] {2});
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u1", "u2")))
                .thenReturn(immutableKeys);
        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        when(reader.getSerializedValuesByRocksDBKeys(immutableKeys, 0, 2))
                .thenAnswer(
                        ignored -> {
                            workerEntered.countDown();
                            assertTrue(releaseWorker.await(10, TimeUnit.SECONDS));
                            return Arrays.asList(serializedMapValue(7), null);
                        });

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

        BatchPrefetchableMapState.PreparedValues prepared =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));
        assertTrue(workerEntered.await(10, TimeUnit.SECONDS));
        releaseWorker.countDown();
        assertEquals(Arrays.asList(7, null), prepared.awaitValues());
        verify(reader, times(1)).serializeRocksDBKeysByUserKeys(Arrays.asList("u1", "u2"));
        verify(reader, times(1)).getSerializedValuesByRocksDBKeys(immutableKeys, 0, 2);
        verify(reader, times(0)).getSerializedValuesByUserKeys(any());
        state.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCloseWaitsForRunningPreparedExactDistinctRead() throws Exception {
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
        List<byte[]> immutableKeys = Arrays.asList(new byte[] {1}, new byte[] {2});
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u1", "u2")))
                .thenReturn(immutableKeys);
        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        when(reader.getSerializedValuesByRocksDBKeys(immutableKeys, 0, 2))
                .thenAnswer(
                        ignored -> {
                            workerEntered.countDown();
                            assertTrue(releaseWorker.await(10, TimeUnit.SECONDS));
                            return Arrays.asList(null, null);
                        });

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
        assertTrue(state.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2")) != null);
        assertTrue(workerEntered.await(10, TimeUnit.SECONDS));

        CountDownLatch closeReturned = new CountDownLatch(1);
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closeThread =
                new Thread(
                        () -> {
                            try {
                                state.close();
                            } catch (Throwable failure) {
                                closeFailure.set(failure);
                            } finally {
                                closeReturned.countDown();
                            }
                        });
        closeThread.setDaemon(true);
        closeThread.start();
        try {
            assertFalse(closeReturned.await(200, TimeUnit.MILLISECONDS));
        } finally {
            releaseWorker.countDown();
        }
        assertTrue(closeReturned.await(10, TimeUnit.SECONDS));
        assertNull(closeFailure.get());
    }

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
                .thenReturn(
                        new MapSerializer<>(
                                org.apache.flink.api.common.typeutils.base.StringSerializer
                                        .INSTANCE,
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
                            ByteBuffer values =
                                    ((ByteBuffer) invocation.getArgument(3)).duplicate();
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
        try (NativeRequestPlaneCoordinator.BatchSlot regular = coordinator.tryAcquireBatchSlot()) {
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
    void testPreparedSmallBatchDefersToMailboxDirectArenaInsteadOfAsyncWorker() throws Exception {
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
                            ByteBuffer values =
                                    ((ByteBuffer) invocation.getArgument(3)).duplicate();
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
        state.enableNativeDistinctBatchPrefetch(true, true, 8);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        BatchPrefetchableMapState.PreparedValues prepared =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));

        assertEquals(Arrays.asList(7, null), prepared.awaitValues());
        assertEquals(1, state.getDeferredSyncBatchPrefetchBatchesForTesting());
        assertEquals(1, state.getDeferredSyncBatchPrefetchCompletedForTesting());
        assertEquals(0, state.getAsyncBatchPrefetchSubmittedForTesting());
        assertEquals(2, state.getBatchPrefetchDirectArenaCompletedKeysForTesting());
        verify(reader, times(0))
                .getSerializedValuesByRocksDBKeys(
                        any(),
                        org.mockito.ArgumentMatchers.anyInt(),
                        org.mockito.ArgumentMatchers.anyInt());
        verify(reader, times(0)).getSerializedValuesByUserKeys(any());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDeferredDirectBatchFailsClosedAfterOuterKeyOrNamespaceChange() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, String, String, Integer> delegate =
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
        NativeRequestPlane plane = mock(NativeRequestPlane.class);
        when(plane.selectedKernel()).thenReturn("test");
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), plane);
        CachedInternalMapState<String, String, String, Integer> state =
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
        state.enableNativeDistinctBatchPrefetch(true, true, 8);
        state.setCurrentNamespace("n1");

        BatchPrefetchableMapState.PreparedValues wrongKey =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));
        currentKey.set("k2");
        assertNull(wrongKey.awaitValues());

        currentKey.set("k1");
        state.setCurrentNamespace("n1");
        BatchPrefetchableMapState.PreparedValues wrongNamespace =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));
        state.setCurrentNamespace("n2");
        assertNull(wrongNamespace.awaitValues());

        assertEquals(2, state.getDeferredSyncBatchPrefetchBatchesForTesting());
        assertEquals(0, state.getDeferredSyncBatchPrefetchCompletedForTesting());
        assertEquals(2, state.getDeferredSyncBatchPrefetchFallbacksForTesting());
        assertEquals(2, state.getDeferredSyncBatchPrefetchContextMismatchesForTesting());
        assertEquals(0, state.getAsyncBatchPrefetchSubmittedForTesting());
        verify(reader, times(0))
                .getSerializedValuesByRocksDBKeyArena(
                        any(ByteBuffer.class),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.anyInt(),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.anyInt());
        verify(reader, times(0)).getSerializedValuesByUserKeys(any());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDeferredWaveCombinesDifferentOuterKeysIntoOneAsyncRawMultiGet() throws Exception {
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
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        when(reader.directArenaMultiGetMaxBatch())
                .thenReturn(RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH);
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u1", "u2")))
                .thenReturn(Arrays.asList(new byte[] {1}, new byte[] {2}));
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u3", "u4")))
                .thenReturn(Arrays.asList(new byte[] {3}, new byte[] {4}));
        byte[] first = serializedMapValue(11);
        byte[] second = serializedMapValue(22);
        when(reader.getSerializedValuesByRocksDBKeys(
                        any(),
                        org.mockito.ArgumentMatchers.eq(0),
                        org.mockito.ArgumentMatchers.eq(4)))
                .thenReturn(Arrays.asList(first, null, second, null));

        NativeRequestPlane plane = mock(NativeRequestPlane.class);
        when(plane.selectedKernel()).thenReturn("test");
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createDeferredWaveState(delegate, currentKey, coordinator);

        BatchPrefetchableMapState.PreparedValues firstToken =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));
        currentKey.set("k2");
        BatchPrefetchableMapState.PreparedValues secondToken =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u3", "u4"));

        assertTrue(firstToken.executeWave(Arrays.asList(firstToken, secondToken)));
        currentKey.set("k1");
        assertEquals(Arrays.asList(11, null), firstToken.awaitValues());
        currentKey.set("k2");
        assertEquals(Arrays.asList(22, null), secondToken.awaitValues());

        assertEquals(1, state.getDeferredWaveWindowsForTesting());
        assertEquals(2, state.getDeferredWaveGroupsForTesting());
        assertEquals(4, state.getDeferredWaveKeysForTesting());
        assertEquals(1, state.getDeferredWaveJniCallsForTesting());
        assertEquals(2, state.getDeferredWaveCompletedGroupsForTesting());
        assertEquals(0, state.getDeferredWaveFallbackWindowsForTesting());
        verify(reader, times(1))
                .getSerializedValuesByRocksDBKeys(
                        any(),
                        org.mockito.ArgumentMatchers.eq(0),
                        org.mockito.ArgumentMatchers.eq(4));
        verify(reader, times(0))
                .getSerializedValuesByRocksDBKeyArena(
                        any(ByteBuffer.class),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.anyInt(),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.anyInt());
        verify(reader, times(0)).getSerializedValuesByUserKeys(any());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDeferredWaveDirectArenaPublishesSlicesAndReleasesSlotAfterLastToken()
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
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        when(reader.directArenaMultiGetMaxBatch())
                .thenReturn(RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH);
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u1", "u2")))
                .thenReturn(Arrays.asList(new byte[] {1}, new byte[] {2}));
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u3", "u4")))
                .thenReturn(Arrays.asList(new byte[] {3}, new byte[] {4}));
        byte[] first = serializedMapValue(11);
        byte[] second = serializedMapValue(22);
        doAnswer(
                        invocation -> {
                            ByteBuffer descriptors =
                                    ((ByteBuffer) invocation.getArgument(1))
                                            .duplicate()
                                            .order(ByteOrder.nativeOrder());
                            ByteBuffer values =
                                    ((ByteBuffer) invocation.getArgument(3)).duplicate();
                            int stride = invocation.getArgument(4);
                            values.position(0);
                            values.put(first);
                            values.position(2 * stride);
                            values.put(second);
                            for (int index = 0; index < 4; index++) {
                                int result =
                                        index == 0
                                                ? first.length
                                                : index == 2
                                                        ? second.length
                                                        : RocksDBBatchValueReader
                                                                .DIRECT_ARENA_NOT_FOUND;
                                descriptors.putInt(
                                        index
                                                        * RocksDBBatchValueReader
                                                                .DIRECT_ARENA_DESCRIPTOR_BYTES
                                                + RocksDBBatchValueReader
                                                        .DIRECT_ARENA_RESULT_OFFSET,
                                        result);
                            }
                            return 2;
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeyArena(
                        any(ByteBuffer.class),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.eq(4),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.anyInt());

        NativeRequestPlane plane = mock(NativeRequestPlane.class);
        when(plane.selectedKernel()).thenReturn("test");
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createDeferredWaveState(delegate, currentKey, coordinator);
        state.enableNativeDistinctBatchPrefetch(true, true, 8, false, true, true, true, false);

        BatchPrefetchableMapState.PreparedValues firstToken =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));
        currentKey.set("k2");
        BatchPrefetchableMapState.PreparedValues secondToken =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u3", "u4"));

        assertTrue(firstToken.executeWave(Arrays.asList(firstToken, secondToken)));
        currentKey.set("k1");
        assertEquals(Arrays.asList(11, null), firstToken.awaitValues());
        assertEquals(1, coordinator.mapDistinctAsyncReadLeases());
        currentKey.set("k2");
        assertEquals(Arrays.asList(22, null), secondToken.awaitValues());

        verify(reader, times(1))
                .getSerializedValuesByRocksDBKeyArena(
                        any(ByteBuffer.class),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.eq(4),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.anyInt());
        verify(reader, times(0))
                .getSerializedValuesByRocksDBKeys(
                        any(),
                        org.mockito.ArgumentMatchers.anyInt(),
                        org.mockito.ArgumentMatchers.anyInt());
        NativeRequestPlaneCoordinator.BatchSlot returned =
                coordinator.tryAcquireMapDistinctAsyncReadSlot();
        assertNotNull(returned);
        returned.close();

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDeferredWaveDirectArenaReleasesSlotWhenStateClosesBeforeSubmission()
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
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        when(reader.directArenaMultiGetMaxBatch())
                .thenReturn(RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH);
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u1", "u2")))
                .thenReturn(Arrays.asList(new byte[] {1}, new byte[] {2}));
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u3", "u4")))
                .thenReturn(Arrays.asList(new byte[] {3}, new byte[] {4}));

        NativeRequestPlane plane = mock(NativeRequestPlane.class);
        when(plane.selectedKernel()).thenReturn("test");
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createDeferredWaveState(delegate, currentKey, coordinator);
        state.enableNativeDistinctBatchPrefetch(true, true, 8, false, true, true, true, false);

        BatchPrefetchableMapState.PreparedValues first =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));
        currentKey.set("k2");
        BatchPrefetchableMapState.PreparedValues second =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u3", "u4"));

        Field monitorField =
                CachedInternalMapState.class.getDeclaredField("asyncBatchPrefetchMonitor");
        monitorField.setAccessible(true);
        Object monitor = monitorField.get(state);
        Field closedField = CachedInternalMapState.class.getDeclaredField("closed");
        closedField.setAccessible(true);
        AtomicReference<Boolean> submitted = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread submitter =
                new Thread(
                        () -> {
                            try {
                                submitted.set(first.executeWave(Arrays.asList(first, second)));
                            } catch (Throwable currentFailure) {
                                failure.set(currentFailure);
                            }
                        },
                        "deferred-wave-close-race-test");

        synchronized (monitor) {
            submitter.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (coordinator.mapDistinctAsyncReadLeases() < 1
                    && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(1, coordinator.mapDistinctAsyncReadLeases());
            closedField.setBoolean(state, true);
        }
        submitter.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(submitter.isAlive());
        assertNull(failure.get());
        assertEquals(Boolean.FALSE, submitted.get());

        NativeRequestPlaneCoordinator.BatchSlot firstReturned =
                coordinator.tryAcquireMapDistinctAsyncReadSlot();
        NativeRequestPlaneCoordinator.BatchSlot secondReturned =
                coordinator.tryAcquireMapDistinctAsyncReadSlot();
        assertNotNull(firstReturned);
        assertNotNull(secondReturned);
        assertNull(coordinator.tryAcquireMapDistinctAsyncReadSlot());
        firstReturned.close();
        secondReturned.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDeferredWaveReadOverlapsMailboxAndNeverRunsOnCaller() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        AtomicReference<String> readerThread = new AtomicReference<>();
        CountDownLatch readerEntered = new CountDownLatch(1);
        CountDownLatch releaseReader = new CountDownLatch(1);
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
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u1", "u2")))
                .thenReturn(Arrays.asList(new byte[] {1}, new byte[] {2}));
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u3", "u4")))
                .thenReturn(Arrays.asList(new byte[] {3}, new byte[] {4}));
        doAnswer(
                        invocation -> {
                            readerThread.set(Thread.currentThread().getName());
                            readerEntered.countDown();
                            assertTrue(releaseReader.await(5, TimeUnit.SECONDS));
                            return Arrays.asList(
                                    serializedMapValue(11), null, serializedMapValue(22), null);
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeys(
                        any(),
                        org.mockito.ArgumentMatchers.eq(0),
                        org.mockito.ArgumentMatchers.eq(4));

        NativeRequestPlane plane = mock(NativeRequestPlane.class);
        when(plane.selectedKernel()).thenReturn("test");
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createDeferredWaveState(delegate, currentKey, coordinator);
        BatchPrefetchableMapState.PreparedValues first =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));
        currentKey.set("k2");
        BatchPrefetchableMapState.PreparedValues second =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u3", "u4"));

        assertTrue(first.executeWave(Arrays.asList(first, second)));
        assertTrue(readerEntered.await(5, TimeUnit.SECONDS));
        assertTrue(readerThread.get().startsWith("cachekit-bp-prefetch"));
        assertEquals(1, state.getDeferredWaveSubmittedForTesting());
        releaseReader.countDown();

        currentKey.set("k1");
        assertEquals(Arrays.asList(11, null), first.awaitValues());
        currentKey.set("k2");
        assertEquals(Arrays.asList(22, null), second.awaitValues());
        assertEquals(2, state.getDeferredWaveAwaitsForTesting());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDeferredWaveContextMismatchDoesNotInvalidateSiblingSlice() throws Exception {
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
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u1", "u2")))
                .thenReturn(Arrays.asList(new byte[] {1}, new byte[] {2}));
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u3", "u4")))
                .thenReturn(Arrays.asList(new byte[] {3}, new byte[] {4}));
        when(reader.getSerializedValuesByRocksDBKeys(
                        any(),
                        org.mockito.ArgumentMatchers.eq(0),
                        org.mockito.ArgumentMatchers.eq(4)))
                .thenReturn(
                        Arrays.asList(serializedMapValue(11), null, serializedMapValue(22), null));

        NativeRequestPlane plane = mock(NativeRequestPlane.class);
        when(plane.selectedKernel()).thenReturn("test");
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createDeferredWaveState(delegate, currentKey, coordinator);
        BatchPrefetchableMapState.PreparedValues first =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));
        currentKey.set("k2");
        BatchPrefetchableMapState.PreparedValues second =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u3", "u4"));

        assertTrue(first.executeWave(Arrays.asList(first, second)));
        currentKey.set("wrong");
        assertNull(first.awaitValues());
        currentKey.set("k2");
        assertEquals(Arrays.asList(22, null), second.awaitValues());
        assertEquals(1, state.getDeferredSyncBatchPrefetchContextMismatchesForTesting());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDeferredWaveRejectsPartialRawResultWithoutPublishingAnySlice() throws Exception {
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
        when(reader.serializeRocksDBKeysByUserKeys(any()))
                .thenReturn(Arrays.asList(new byte[] {1}, new byte[] {2}));
        when(reader.getSerializedValuesByRocksDBKeys(
                        any(),
                        org.mockito.ArgumentMatchers.eq(0),
                        org.mockito.ArgumentMatchers.eq(4)))
                .thenReturn(Arrays.asList(serializedMapValue(11), null, null));

        NativeRequestPlane plane = mock(NativeRequestPlane.class);
        when(plane.selectedKernel()).thenReturn("test");
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createDeferredWaveState(delegate, currentKey, coordinator);
        BatchPrefetchableMapState.PreparedValues first =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));
        currentKey.set("k2");
        BatchPrefetchableMapState.PreparedValues second =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u3", "u4"));

        assertTrue(first.executeWave(Arrays.asList(first, second)));
        currentKey.set("k1");
        assertNull(first.awaitValues());
        currentKey.set("k2");
        assertNull(second.awaitValues());
        assertEquals(0, state.getDeferredWaveWindowsForTesting());
        assertEquals(1, state.getDeferredWaveProtocolFailuresForTesting());
        assertEquals(1, state.getDeferredWaveFallbackWindowsForTesting());

        state.close();
        coordinator.close();
    }

    @Test
    void testDeferredWaveQueuedCloseDropsTaskAndDoesNotWaitForUnrelatedWorker() throws Exception {
        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        PrefetchExecutor.trySubmit(
                () -> {
                    workerEntered.countDown();
                    try {
                        releaseWorker.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                });
        assertTrue(workerEntered.await(5, TimeUnit.SECONDS));

        DeferredWaveTestContext context = createDeferredWaveTestContext();
        try {
            assertTrue(context.first.executeWave(Arrays.asList(context.first, context.second)));
            context.state.close();
            assertEquals(1, context.state.getDeferredWaveDroppedForTesting());
            assertEquals(1, context.state.getDeferredWaveFallbackWindowsForTesting());
            verify(context.reader, times(0))
                    .getSerializedValuesByRocksDBKeys(
                            any(),
                            org.mockito.ArgumentMatchers.eq(0),
                            org.mockito.ArgumentMatchers.eq(4));
        } finally {
            releaseWorker.countDown();
            context.coordinator.close();
        }
    }

    @Test
    void testDeferredWaveAllTokensCancelledReleaseQueuedTask() throws Exception {
        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        PrefetchExecutor.trySubmit(
                () -> {
                    workerEntered.countDown();
                    try {
                        releaseWorker.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                });
        assertTrue(workerEntered.await(5, TimeUnit.SECONDS));

        DeferredWaveTestContext context = createDeferredWaveTestContext();
        try {
            assertTrue(context.first.executeWave(Arrays.asList(context.first, context.second)));
            context.first.cancel();
            context.second.cancel();
            assertEquals(1, context.state.getDeferredWaveDroppedForTesting());
            assertEquals(2, context.state.getDeferredWaveCancelledGroupsForTesting());
            context.state.close();
        } finally {
            releaseWorker.countDown();
            context.coordinator.close();
        }
    }

    @Test
    void testDeferredWaveRunningCloseWaitsForOwnedReadAndThenCompletes() throws Exception {
        DeferredWaveTestContext context = createDeferredWaveTestContext();
        CountDownLatch readerEntered = new CountDownLatch(1);
        CountDownLatch releaseReader = new CountDownLatch(1);
        CountDownLatch closeReturned = new CountDownLatch(1);
        when(context.reader.getSerializedValuesByRocksDBKeys(
                        any(),
                        org.mockito.ArgumentMatchers.eq(0),
                        org.mockito.ArgumentMatchers.eq(4)))
                .thenAnswer(
                        ignored -> {
                            readerEntered.countDown();
                            assertTrue(releaseReader.await(5, TimeUnit.SECONDS));
                            return Arrays.asList(
                                    serializedMapValue(11), null, serializedMapValue(22), null);
                        });

        Thread closer =
                new Thread(
                        () -> {
                            context.state.close();
                            closeReturned.countDown();
                        },
                        "cachekit-map-wave-close-test");
        try {
            assertTrue(context.first.executeWave(Arrays.asList(context.first, context.second)));
            assertTrue(readerEntered.await(5, TimeUnit.SECONDS));
            closer.start();
            assertFalse(closeReturned.await(100, TimeUnit.MILLISECONDS));
            releaseReader.countDown();
            assertTrue(closeReturned.await(5, TimeUnit.SECONDS));
            closer.join(5000L);
            assertFalse(closer.isAlive());
            assertEquals(1, context.state.getDeferredWaveWindowsForTesting());
            assertEquals(0, context.state.getDeferredWaveDroppedForTesting());
        } finally {
            releaseReader.countDown();
            if (closer.isAlive()) {
                closer.join(5000L);
            }
            context.coordinator.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDeferredWaveRejectsMixedOwnersWithoutNativeRead() throws Exception {
        AtomicReference<String> firstKey = new AtomicReference<>("k1");
        AtomicReference<String> secondKey = new AtomicReference<>("k2");
        InternalMapState<String, VoidNamespace, String, Integer> firstDelegate =
                mock(
                        InternalMapState.class,
                        withSettings().extraInterfaces(RocksDBBatchMapReader.class));
        InternalMapState<String, VoidNamespace, String, Integer> secondDelegate =
                mock(
                        InternalMapState.class,
                        withSettings().extraInterfaces(RocksDBBatchMapReader.class));
        for (InternalMapState<String, VoidNamespace, String, Integer> delegate :
                Arrays.asList(firstDelegate, secondDelegate)) {
            when(delegate.getValueSerializer())
                    .thenReturn(
                            new MapSerializer<>(
                                    org.apache.flink.api.common.typeutils.base.StringSerializer
                                            .INSTANCE,
                                    IntSerializer.INSTANCE));
            RocksDBBatchMapReader<String> reader = (RocksDBBatchMapReader<String>) delegate;
            when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
            when(reader.directArenaMultiGetMaxBatch())
                    .thenReturn(RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH);
            when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u1", "u2")))
                    .thenReturn(Arrays.asList(new byte[] {1}, new byte[] {2}));
        }
        NativeRequestPlane plane = mock(NativeRequestPlane.class);
        when(plane.selectedKernel()).thenReturn("test");
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> firstState =
                createDeferredWaveState(firstDelegate, firstKey, coordinator);
        CachedInternalMapState<String, VoidNamespace, String, Integer> secondState =
                createDeferredWaveState(secondDelegate, secondKey, coordinator);

        BatchPrefetchableMapState.PreparedValues firstToken =
                firstState.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));
        BatchPrefetchableMapState.PreparedValues secondToken =
                secondState.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));

        assertFalse(firstToken.executeWave(Arrays.asList(firstToken, secondToken)));
        assertEquals(1, firstState.getDeferredWaveOwnerRejectsForTesting());
        assertEquals(1, firstState.getDeferredWaveFallbackWindowsForTesting());
        verify((RocksDBBatchMapReader<String>) firstDelegate, times(0))
                .getSerializedValuesByRocksDBKeyArena(
                        any(ByteBuffer.class),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.anyInt(),
                        any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.anyInt());

        firstToken.cancel();
        secondToken.cancel();
        firstState.close();
        secondState.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDeferredCohortWaveFusesTwoStateColumnsIntoOneCrossColumnMultiGet()
            throws Exception {
        AtomicReference<String> firstKey = new AtomicReference<>("k1");
        AtomicReference<String> secondKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> firstDelegate =
                mock(
                        InternalMapState.class,
                        withSettings().extraInterfaces(RocksDBBatchMapReader.class));
        InternalMapState<String, VoidNamespace, String, Integer> secondDelegate =
                mock(
                        InternalMapState.class,
                        withSettings().extraInterfaces(RocksDBBatchMapReader.class));
        for (InternalMapState<String, VoidNamespace, String, Integer> delegate :
                Arrays.asList(firstDelegate, secondDelegate)) {
            when(delegate.getValueSerializer())
                    .thenReturn(
                            new MapSerializer<>(
                                    org.apache.flink.api.common.typeutils.base.StringSerializer
                                            .INSTANCE,
                                    IntSerializer.INSTANCE));
        }
        RocksDBBatchMapReader<String> firstReader =
                (RocksDBBatchMapReader<String>) firstDelegate;
        RocksDBBatchMapReader<String> secondReader =
                (RocksDBBatchMapReader<String>) secondDelegate;
        Object databaseOwner = new Object();
        when(firstReader.multiColumnReadOwner()).thenReturn(databaseOwner);
        when(secondReader.multiColumnReadOwner()).thenReturn(databaseOwner);
        when(firstReader.serializeRocksDBKeysByUserKeys(Arrays.asList("u1", "u2")))
                .thenReturn(Arrays.asList(new byte[] {1}, new byte[] {2}));
        when(firstReader.serializeRocksDBKeysByUserKeys(Arrays.asList("u3", "u4")))
                .thenReturn(Arrays.asList(new byte[] {3}, new byte[] {4}));
        when(secondReader.serializeRocksDBKeysByUserKeys(Arrays.asList("v1", "v2")))
                .thenReturn(Arrays.asList(new byte[] {5}, new byte[] {6}));
        when(secondReader.serializeRocksDBKeysByUserKeys(Arrays.asList("v3", "v4")))
                .thenReturn(Arrays.asList(new byte[] {7}, new byte[] {8}));
        when(firstReader.getSerializedValuesAcrossColumns(any(), any()))
                .thenReturn(
                        Arrays.asList(
                                serializedMapValue(11),
                                null,
                                serializedMapValue(22),
                                null,
                                serializedMapValue(33),
                                null,
                                serializedMapValue(44),
                                null));

        NativeRequestPlane plane = mock(NativeRequestPlane.class);
        when(plane.selectedKernel()).thenReturn("test");
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> firstState =
                createDeferredWaveState(firstDelegate, firstKey, coordinator);
        CachedInternalMapState<String, VoidNamespace, String, Integer> secondState =
                createDeferredWaveState(secondDelegate, secondKey, coordinator);
        firstState.enableNativeDistinctBatchPrefetch(true, true, 8, false, true, true, true);
        secondState.enableNativeDistinctBatchPrefetch(true, true, 8, false, true, true, true);

        BatchPrefetchableMapState.PreparedValues firstA =
                firstState.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));
        firstKey.set("k2");
        BatchPrefetchableMapState.PreparedValues firstB =
                firstState.prepareCurrentUniqueKeyValues(Arrays.asList("u3", "u4"));
        BatchPrefetchableMapState.PreparedValues secondA =
                secondState.prepareCurrentUniqueKeyValues(Arrays.asList("v1", "v2"));
        secondKey.set("k2");
        BatchPrefetchableMapState.PreparedValues secondB =
                secondState.prepareCurrentUniqueKeyValues(Arrays.asList("v3", "v4"));

        assertTrue(
                firstA.executeCohortWave(
                        Arrays.asList(
                                Arrays.asList(firstA, firstB),
                                Arrays.asList(secondA, secondB))));
        firstKey.set("k1");
        assertEquals(Arrays.asList(11, null), firstA.awaitValues());
        firstKey.set("k2");
        assertEquals(Arrays.asList(22, null), firstB.awaitValues());
        secondKey.set("k1");
        assertEquals(Arrays.asList(33, null), secondA.awaitValues());
        secondKey.set("k2");
        assertEquals(Arrays.asList(44, null), secondB.awaitValues());

        assertEquals(1, firstState.getDeferredCohortWaveAttemptsForTesting());
        assertEquals(1, firstState.getDeferredCohortWaveSubmittedForTesting());
        assertEquals(2, firstState.getDeferredCohortWaveColumnsForTesting());
        assertEquals(4, firstState.getDeferredCohortWaveGroupsForTesting());
        assertEquals(8, firstState.getDeferredCohortWaveKeysForTesting());
        assertEquals(1, firstState.getDeferredCohortWaveJniCallsForTesting());
        assertEquals(0, firstState.getDeferredCohortWaveRejectsForTesting());
        verify(firstReader, times(1)).getSerializedValuesAcrossColumns(any(), any());
        verify(firstReader, times(0))
                .getSerializedValuesByRocksDBKeys(
                        any(),
                        org.mockito.ArgumentMatchers.anyInt(),
                        org.mockito.ArgumentMatchers.anyInt());
        verify(secondReader, times(0))
                .getSerializedValuesByRocksDBKeys(
                        any(),
                        org.mockito.ArgumentMatchers.anyInt(),
                        org.mockito.ArgumentMatchers.anyInt());

        firstState.close();
        secondState.close();
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

    @Test
    void testExactDistinctResidentWriteBackIsolatesOuterKeyAndNamespaceAcrossBatches()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, String, String, Integer> delegate = mock(InternalMapState.class);
        CachedInternalMapState<String, String, String, Integer> state =
                new CachedInternalMapState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        0,
                        CachePolicyType.LRU,
                        0,
                        PresenceCacheImplementation.OBJECT,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.0,
                        1,
                        false,
                        0);
        state.enableExactDistinctResidentWriteBack(true);

        state.setCurrentNamespace("ns1");
        state.put("u", 11);
        state.setCurrentNamespace("ns2");
        state.put("u", 12);
        currentKey.set("k2");
        state.setCurrentNamespace("ns1");
        state.put("u", 21);

        clearInvocations(delegate);
        currentKey.set("k1");
        state.setCurrentNamespace("ns1");
        assertEquals(11, state.get("u"));
        state.setCurrentNamespace("ns2");
        assertEquals(12, state.get("u"));
        currentKey.set("k2");
        state.setCurrentNamespace("ns1");
        assertEquals(21, state.get("u"));
        verify(delegate, times(0)).get(any());
        assertEquals(3, state.getExactDistinctResidentMutationCountForTesting());
        assertEquals(3, state.getExactDistinctResidentReadHitsForTesting());

        state.flush();
        verify(delegate, times(3)).put(org.mockito.ArgumentMatchers.eq("u"), any());
        assertEquals(3, state.getExactDistinctResidentExplicitFlushesForTesting());
    }

    @Test
    void testExactDistinctResidentWriteBackRetriesFlushFailureWithoutLosingDirtyEntry()
            throws Exception {
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
                        PresenceCacheImplementation.OBJECT,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.0,
                        1,
                        false,
                        0);
        state.enableExactDistinctResidentWriteBack(true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        state.put("u", 1);
        org.mockito.Mockito.doThrow(new Exception("injected flush failure"))
                .doNothing()
                .when(delegate)
                .put("u", 1);

        org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, state::flush);
        assertEquals(1, state.getExactDistinctResidentFlushFailuresForTesting());
        state.flush();
        verify(delegate, times(2)).put("u", 1);
        assertEquals(1, state.getExactDistinctResidentExplicitFlushesForTesting());
    }

    @Test
    void testExactDistinctResidentWriteBackCountsDirtyEvictionFlush() throws Exception {
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
                        PresenceCacheImplementation.OBJECT,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.0,
                        1,
                        false,
                        0);
        state.enableExactDistinctResidentWriteBack(true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        for (int index = 0; index < 129; index++) {
            state.put("u" + index, index);
        }

        assertEquals(1, state.getExactDistinctResidentEvictionFlushesForTesting());
        assertEquals(129, state.getExactDistinctResidentMutationCountForTesting());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testExactDistinctResidentPrefetchKeepsDirtyValueAndReadsOnlyMisses() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(
                        InternalMapState.class,
                        withSettings().extraInterfaces(RocksDBBatchMapReader.class));
        when(delegate.getValueSerializer())
                .thenReturn(
                        new MapSerializer<>(
                                org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE,
                                IntSerializer.INSTANCE));
        RocksDBBatchMapReader<String> reader = (RocksDBBatchMapReader<String>) delegate;
        when(reader.getSerializedValuesByUserKeys(Arrays.asList("miss")))
                .thenReturn(Arrays.asList(serializedMapValue(3)));
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        0,
                        CachePolicyType.LRU,
                        0,
                        PresenceCacheImplementation.OBJECT,
                        500,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.0,
                        1,
                        false,
                        0);
        state.enableNativeDistinctBatchPrefetch(true);
        state.enableExactDistinctResidentWriteBack(true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        state.put("resident", 9);
        assertTrue(state.beginPrefetchCurrentKeys(Arrays.asList("resident", "miss")));

        assertEquals(9, state.get("resident"));
        assertEquals(3, state.get("miss"));
        verify(reader, times(1)).getSerializedValuesByUserKeys(Arrays.asList("miss"));
        verify(delegate, times(0)).put("resident", 9);
        assertEquals(1, state.getExactDistinctResidentPrefetchHitsForTesting());
        assertEquals(1, state.getExactDistinctResidentRocksDbPrefetchMissesForTesting());
        assertEquals(1, state.getExactDistinctResidentDirtyAvoidedFlushesForTesting());
    }

    @Test
    void testExactDistinctResidentCheckpointFlushAndFreshWrapperStartsEmpty() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> beforeCheckpoint =
                mock(InternalMapState.class);
        CachedInternalMapState<String, VoidNamespace, String, Integer> first =
                createResidentState(beforeCheckpoint, currentKey, 500);
        first.put("u", 17);

        // CacheKitKeyedStateBackend invokes this same flush seam before delegate snapshot.
        first.flush();
        verify(beforeCheckpoint).put("u", 17);

        InternalMapState<String, VoidNamespace, String, Integer> restoredDelegate =
                mock(InternalMapState.class);
        when(restoredDelegate.get("u")).thenReturn(17);
        CachedInternalMapState<String, VoidNamespace, String, Integer> restored =
                createResidentState(restoredDelegate, currentKey, 500);
        assertEquals(17, restored.get("u"));
        verify(restoredDelegate).get("u");
        assertEquals(0, restored.getExactDistinctResidentReadHitsForTesting());
    }

    private static CachedInternalMapState<String, VoidNamespace, String, Integer>
            createResidentState(
                    InternalMapState<String, VoidNamespace, String, Integer> delegate,
                    AtomicReference<String> currentKey,
                    int mapCacheBackingEntries) {
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                new CachedInternalMapState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        0,
                        CachePolicyType.LRU,
                        0,
                        PresenceCacheImplementation.OBJECT,
                        mapCacheBackingEntries,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.0,
                        1,
                        false,
                        0);
        state.enableExactDistinctResidentWriteBack(true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        return state;
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPreparedCommitIsDisabledByDefault() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                preparedCommitDelegate();
        RocksDBBatchMapReader<String> reader = (RocksDBBatchMapReader<String>) delegate;
        List<byte[]> rocksDBKeys = Arrays.asList(new byte[] {1}, new byte[] {2});
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u1", "u2")))
                .thenReturn(rocksDBKeys);
        when(reader.supportsPreparedMutations()).thenReturn(true);

        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createDeferredWaveState(delegate, currentKey, null);
        BatchPrefetchableMapState.PreparedValues token =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));

        assertFalse(token.supportsPreparedCommit());
        assertFalse(
                token.commitPreparedValues(
                        new Object[] {10, 20},
                        new boolean[] {true, true},
                        new boolean[] {false, false}));
        verify(reader, times(0))
                .prepareSerializedMutations(any(), any(), any(), any());
        state.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPreparedCommitWritesOnceAndCannotReplay() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                preparedCommitDelegate();
        RocksDBBatchMapReader<String> reader = (RocksDBBatchMapReader<String>) delegate;
        List<byte[]> rocksDBKeys = Arrays.asList(new byte[] {1}, new byte[] {2});
        Object writeOwner = new Object();
        RocksDBBatchMapReader.PreparedMutation mutation =
                new RocksDBBatchMapReader.PreparedMutation(
                        reader,
                        rocksDBKeys,
                        new byte[][] {new byte[] {10}, new byte[] {20}},
                        new boolean[] {true, true},
                        new boolean[] {false, false});
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u1", "u2")))
                .thenReturn(rocksDBKeys);
        when(reader.supportsPreparedMutations()).thenReturn(true);
        when(reader.preparedWriteOwner()).thenReturn(writeOwner);
        when(reader.prepareSerializedMutations(any(), any(), any(), any()))
                .thenReturn(mutation);

        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createDeferredWaveState(delegate, currentKey, null);
        state.enableNativeDistinctPreparedCommit(true);
        BatchPrefetchableMapState.PreparedValues token =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));
        Object[] values = new Object[] {10, 20};
        boolean[] dirty = new boolean[] {true, true};
        boolean[] removed = new boolean[] {false, false};

        assertTrue(token.supportsPreparedCommit());
        assertTrue(token.commitPreparedValues(values, dirty, removed));
        assertFalse(token.commitPreparedValues(values, dirty, removed));
        verify(reader, times(1)).prepareSerializedMutations(rocksDBKeys, values, dirty, removed);
        verify(reader, times(1)).commitPreparedMutations(any());
        state.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPreparedCommitRejectsStaleGenerationBeforeWrite() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                preparedCommitDelegate();
        RocksDBBatchMapReader<String> reader = (RocksDBBatchMapReader<String>) delegate;
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u1", "u2")))
                .thenReturn(Arrays.asList(new byte[] {1}, new byte[] {2}));
        when(reader.supportsPreparedMutations()).thenReturn(true);
        when(reader.preparedWriteOwner()).thenReturn(new Object());

        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createDeferredWaveState(delegate, currentKey, null);
        state.enableNativeDistinctPreparedCommit(true);
        BatchPrefetchableMapState.PreparedValues token =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));
        state.put("outside", 7);

        assertFalse(
                token.commitPreparedValues(
                        new Object[] {10, 20},
                        new boolean[] {true, true},
                        new boolean[] {false, false}));
        verify(reader, times(0))
                .prepareSerializedMutations(any(), any(), any(), any());
        verify(reader, times(0)).commitPreparedMutations(any());
        state.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPreparedCommitCohortUsesOneBackendWrite() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> firstDelegate =
                preparedCommitDelegate();
        InternalMapState<String, VoidNamespace, String, Integer> secondDelegate =
                preparedCommitDelegate();
        RocksDBBatchMapReader<String> firstReader =
                (RocksDBBatchMapReader<String>) firstDelegate;
        RocksDBBatchMapReader<String> secondReader =
                (RocksDBBatchMapReader<String>) secondDelegate;
        List<byte[]> firstKeys = Arrays.asList(new byte[] {1}, new byte[] {2});
        List<byte[]> secondKeys = Arrays.asList(new byte[] {3}, new byte[] {4});
        Object writeOwner = new Object();
        when(firstReader.serializeRocksDBKeysByUserKeys(Arrays.asList("u1", "u2")))
                .thenReturn(firstKeys);
        when(secondReader.serializeRocksDBKeysByUserKeys(Arrays.asList("v1", "v2")))
                .thenReturn(secondKeys);
        for (RocksDBBatchMapReader<String> reader : Arrays.asList(firstReader, secondReader)) {
            when(reader.supportsPreparedMutations()).thenReturn(true);
            when(reader.preparedWriteOwner()).thenReturn(writeOwner);
        }
        RocksDBBatchMapReader.PreparedMutation firstMutation =
                new RocksDBBatchMapReader.PreparedMutation(
                        firstReader,
                        firstKeys,
                        new byte[][] {new byte[] {1}, new byte[] {2}},
                        new boolean[] {true, true},
                        new boolean[] {false, false});
        RocksDBBatchMapReader.PreparedMutation secondMutation =
                new RocksDBBatchMapReader.PreparedMutation(
                        secondReader,
                        secondKeys,
                        new byte[][] {new byte[] {3}, new byte[] {4}},
                        new boolean[] {true, true},
                        new boolean[] {false, false});
        when(firstReader.prepareSerializedMutations(any(), any(), any(), any()))
                .thenReturn(firstMutation);
        when(secondReader.prepareSerializedMutations(any(), any(), any(), any()))
                .thenReturn(secondMutation);

        CachedInternalMapState<String, VoidNamespace, String, Integer> firstState =
                createDeferredWaveState(firstDelegate, currentKey, null);
        CachedInternalMapState<String, VoidNamespace, String, Integer> secondState =
                createDeferredWaveState(secondDelegate, currentKey, null);
        firstState.enableNativeDistinctPreparedCommit(true);
        secondState.enableNativeDistinctPreparedCommit(true);
        BatchPrefetchableMapState.PreparedValues firstToken =
                firstState.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));
        BatchPrefetchableMapState.PreparedValues secondToken =
                secondState.prepareCurrentUniqueKeyValues(Arrays.asList("v1", "v2"));

        assertTrue(
                firstToken.commitPreparedCohort(
                        Arrays.asList(firstToken, secondToken),
                        Arrays.asList(new Object[] {10, 20}, new Object[] {30, 40}),
                        Arrays.asList(
                                new boolean[] {true, true}, new boolean[] {true, true}),
                        Arrays.asList(
                                new boolean[] {false, false},
                                new boolean[] {false, false})));
        verify(firstReader, times(1)).commitPreparedMutations(any());
        verify(secondReader, times(0)).commitPreparedMutations(any());
        assertFalse(
                firstToken.commitPreparedValues(
                        new Object[] {10, 20},
                        new boolean[] {true, true},
                        new boolean[] {false, false}));
        firstState.close();
        secondState.close();
    }

    @SuppressWarnings("unchecked")
    private static InternalMapState<String, VoidNamespace, String, Integer>
            preparedCommitDelegate() {
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
        return delegate;
    }

    private static byte[] serializedMapValue(Integer value) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(16);
        out.writeBoolean(value == null);
        if (value != null) {
            IntSerializer.INSTANCE.serialize(value, out);
        }
        return out.getCopyOfBuffer();
    }

    private static List<String> eightUserKeys() {
        return Arrays.asList("u1", "u2", "u3", "u4", "u5", "u6", "u7", "u8");
    }

    private static List<byte[]> eightSerializedKeys() {
        return Arrays.asList(
                new byte[] {1},
                new byte[] {2},
                new byte[] {3},
                new byte[] {4},
                new byte[] {5},
                new byte[] {6},
                new byte[] {7},
                new byte[] {8});
    }

    private static void writeDirectResults(
            org.mockito.invocation.InvocationOnMock invocation, byte[] present, boolean overflow) {
        ByteBuffer descriptors =
                ((ByteBuffer) invocation.getArgument(1)).duplicate().order(ByteOrder.nativeOrder());
        ByteBuffer values = ((ByteBuffer) invocation.getArgument(3)).duplicate();
        values.position(0);
        values.put(present);
        for (int index = 0; index < 8; index++) {
            descriptors.putInt(
                    index * RocksDBBatchValueReader.DIRECT_ARENA_DESCRIPTOR_BYTES
                            + RocksDBBatchValueReader.DIRECT_ARENA_RESULT_OFFSET,
                    index == 0
                            ? (overflow
                                    ? RocksDBBatchValueReader.DIRECT_ARENA_OVERFLOW
                                    : present.length)
                            : RocksDBBatchValueReader.DIRECT_ARENA_NOT_FOUND);
        }
    }

    private static CachedInternalMapState<String, VoidNamespace, String, Integer>
            createDirectAsyncState(
                    InternalMapState<String, VoidNamespace, String, Integer> delegate,
                    AtomicReference<String> currentKey,
                    NativeRequestPlaneCoordinator coordinator) {
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
        state.enableNativeDistinctBatchPrefetch(true, true, 8);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        return state;
    }

    private static CachedInternalMapState<String, VoidNamespace, String, Integer>
            createDeferredWaveState(
                    InternalMapState<String, VoidNamespace, String, Integer> delegate,
                    AtomicReference<String> currentKey,
                    NativeRequestPlaneCoordinator coordinator) {
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
        state.enableNativeDistinctBatchPrefetch(true, true, 8, false, true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        return state;
    }

    @SuppressWarnings("unchecked")
    private static DeferredWaveTestContext createDeferredWaveTestContext() throws Exception {
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
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u1", "u2")))
                .thenReturn(Arrays.asList(new byte[] {1}, new byte[] {2}));
        when(reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u3", "u4")))
                .thenReturn(Arrays.asList(new byte[] {3}, new byte[] {4}));
        NativeRequestPlane plane = mock(NativeRequestPlane.class);
        when(plane.selectedKernel()).thenReturn("test");
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createDeferredWaveState(delegate, currentKey, coordinator);
        BatchPrefetchableMapState.PreparedValues first =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u1", "u2"));
        currentKey.set("k2");
        BatchPrefetchableMapState.PreparedValues second =
                state.prepareCurrentUniqueKeyValues(Arrays.asList("u3", "u4"));
        return new DeferredWaveTestContext(state, reader, first, second, coordinator);
    }

    private static NativeRequestPlaneOptions directArenaOptions() {
        return new NativeRequestPlaneOptions(
                true, "", "auto", 128, 1 << 20, 1 << 20, 16, 1 << 20, 1 << 20, 1, 2, false, false,
                false, false, false, true, true, false, true, true, false, 8192, 0.02, 262144);
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

    private static final class DeferredWaveTestContext {
        private final CachedInternalMapState<String, VoidNamespace, String, Integer> state;
        private final RocksDBBatchMapReader<String> reader;
        private final BatchPrefetchableMapState.PreparedValues first;
        private final BatchPrefetchableMapState.PreparedValues second;
        private final NativeRequestPlaneCoordinator coordinator;

        private DeferredWaveTestContext(
                CachedInternalMapState<String, VoidNamespace, String, Integer> state,
                RocksDBBatchMapReader<String> reader,
                BatchPrefetchableMapState.PreparedValues first,
                BatchPrefetchableMapState.PreparedValues second,
                NativeRequestPlaneCoordinator coordinator) {
            this.state = state;
            this.reader = reader;
            this.first = first;
            this.second = second;
            this.coordinator = coordinator;
        }
    }
}
