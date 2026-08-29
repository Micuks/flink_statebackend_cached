/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.PositionedDataOutputView;
import org.apache.flink.contrib.streaming.state.RocksDBBatchValueReader;
import org.apache.flink.contrib.streaming.state.cachekit.PrefetchExecutor;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlane;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneBridge;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneCoordinator;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneOptions;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.SerializedKeyBatch;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.junit.jupiter.api.Test;

class NativePreparedValueStateTest {

    @Test
    @SuppressWarnings("unchecked")
    void testCloseWaitsForCompleteNativeTaskAndSlotRelease() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);

        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));

        CountDownLatch probeEntered = new CountDownLatch(1);
        CountDownLatch releaseProbe = new CountDownLatch(1);
        CountDownLatch closeReturned = new CountDownLatch(1);
        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        fakePlane.blockNextProbe(probeEntered, releaseProbe);
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(testOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        128,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        8,
                        1 << 20,
                        false,
                        coordinator,
                        27);
        state.setCurrentNamespace("window-close-drain");

        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2"));
        assertNotNull(task);
        Thread worker = new Thread(task, "native-prefetch-close-drain");
        Thread closer =
                new Thread(
                        () -> {
                            state.close();
                            closeReturned.countDown();
                        },
                        "native-state-close");
        try {
            worker.start();
            assertTrue(probeEntered.await(5, TimeUnit.SECONDS));
            closer.start();
            assertFalse(closeReturned.await(200, TimeUnit.MILLISECONDS));
            assertEquals(0, fakePlane.closeCalls);
        } finally {
            releaseProbe.countDown();
            worker.join(5000);
            closer.join(5000);
        }
        assertFalse(worker.isAlive());
        assertFalse(closer.isAlive());
        assertEquals(0, closeReturned.getCount());

        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testIndependentNativeValueCacheServesPreparedPointHit() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("point-key");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(7);

        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(99);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        byte[] preparedKey =
                KvStateSerializer.serializeKeyAndNamespace(
                        "point-key",
                        StringSerializer.INSTANCE,
                        "point-ns",
                        StringSerializer.INSTANCE);
        fakePlane.preload(
                41, 0, preparedKey, KvStateSerializer.serializeValue(42, IntSerializer.INSTANCE));
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(valueCacheOptions(true), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        128,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        1,
                        1 << 20,
                        false,
                        coordinator,
                        41);
        state.setCurrentNamespace("point-ns");

        assertEquals(42, state.value());
        verify(delegate, never()).value();
        assertEquals(1, state.getNativeProbeKeysForTesting());
        assertEquals(1, state.getNativeHitsForTesting());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDisabledNativeValueCacheFallsThroughToDelegate() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("point-key");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(7);

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(valueCacheOptions(false), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        128,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        1,
                        1 << 20,
                        false,
                        coordinator,
                        42);
        state.setCurrentNamespace("point-ns");

        assertEquals(7, state.value());
        verify(delegate, times(1)).value();
        assertEquals(0, state.getNativeProbeKeysForTesting());
        assertEquals(0, state.getNativeFillKeysForTesting());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testProbeCompactsMissesAndReusesPositiveAndNegativeHitsWithNamespace() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(-1);

        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(99);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        doAnswer(
                        invocation -> {
                            byte[] serialized =
                                    KvStateSerializer.serializeKeyAndNamespace(
                                            invocation.getArgument(0),
                                            StringSerializer.INSTANCE,
                                            invocation.getArgument(1),
                                            StringSerializer.INSTANCE);
                            ((PositionedDataOutputView) invocation.getArgument(4))
                                    .write(serialized);
                            return null;
                        })
                .when(reader)
                .serializeBatchKeyAndNamespace(any(), any(), any(), any(), any());
        when(reader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                                null,
                                KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE)));

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(testOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        128,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        1,
                        1 << 20,
                        false,
                        coordinator,
                        7);
        state.setCurrentNamespace("window-7");

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "missing", "k2")).run();
        assertEquals(1, state.getStagingSizeForTesting());
        currentKey.set("k1");
        assertEquals(11, state.value());

        state.buildAsyncPrefetchTask(Arrays.asList("missing", "k2")).run();
        assertEquals(1, state.getStagingSizeForTesting());
        currentKey.set("missing");
        assertEquals(99, state.value());

        state.buildAsyncPrefetchTask(Arrays.asList("k2")).run();
        currentKey.set("k2");
        assertEquals(22, state.value());

        verify(reader, times(1)).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        verify(reader, never()).serializeBatchKeyAndNamespace(any(), any(), any(), any());
        verify(delegate, never()).value();
        assertEquals(3, state.getNativeBatchesActivatedForTesting());
        assertEquals(6, state.getNativeProbeKeysForTesting());
        assertEquals(2, state.getNativeHitsForTesting());
        assertEquals(0, state.getNativeHitBytesCopiedForTesting());
        assertEquals(
                2L * KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE).length,
                state.getNativeHitBytesDirectForTesting());
        assertEquals(1, state.getNativeNegativeHitsForTesting());
        assertEquals(3, state.getNativeMissesForTesting());
        assertEquals(1, state.getNativeFillBatchesForTesting());
        assertEquals(3, state.getNativeFillKeysForTesting());
        assertEquals(3, state.getNativeDirectPreparedBatchesForTesting());
        assertEquals(6, state.getNativeDirectPreparedKeysForTesting());
        assertEquals(0, state.getNativeDirectPreparedFallbacksForTesting());
        // Two native hits are deserialized directly from the leased arena. Only the two
        // RocksDB-miss values remain lazy-staged and are materialized on promotion.
        assertEquals(2, state.getPrefetchLazyValuesMaterializedForTesting());
        assertFalse(state.supportsRecordKeyPrefetch());

        state.close();
        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testNativeMailboxCompactsDuplicatesBeforeReservationAndMultiGet() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);

        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        when(reader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenAnswer(
                        invocation -> {
                            assertEquals(0, ((Integer) invocation.getArgument(1)).intValue());
                            assertEquals(2, ((Integer) invocation.getArgument(2)).intValue());
                            return Arrays.asList(
                                    KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                                    KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE));
                        });
        doAnswer(
                        invocation -> {
                            byte[] serialized =
                                    KvStateSerializer.serializeKeyAndNamespace(
                                            invocation.getArgument(0),
                                            StringSerializer.INSTANCE,
                                            invocation.getArgument(1),
                                            StringSerializer.INSTANCE);
                            ((PositionedDataOutputView) invocation.getArgument(4))
                                    .write(serialized);
                            return null;
                        })
                .when(reader)
                .serializeBatchKeyAndNamespace(any(), any(), any(), any(), any());

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(mailboxOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        0,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        1,
                        1 << 20,
                        false,
                        coordinator,
                        17);
        state.setCurrentNamespace("window-mailbox");

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2", "k1")).run();

        assertEquals(1, state.getNativeMailboxCompactBatchesForTesting());
        assertEquals(3, state.getNativeMailboxCompactInputKeysForTesting());
        assertEquals(2, state.getNativeMailboxCompactUniqueKeysForTesting());
        assertEquals(0, state.getNativeMailboxCompactFallbacksForTesting());
        assertEquals(0, state.getNativeMailboxCompactThresholdFallbacksForTesting());
        assertEquals(1, state.getPrefetchKeysDeduplicatedForTesting());
        verify(reader, times(1)).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());

        state.close();
        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCompactionScratchCoversFullSlotExhaustionAndReleasesBeforeMultiGet()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);

        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        when(reader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE)));
        stubDirectPreparedSerialization(reader);

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(mailboxScratchOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        0,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        1,
                        1 << 20,
                        false,
                        coordinator,
                        18);
        state.setCurrentNamespace("window-scratch");

        NativeRequestPlaneCoordinator.BatchSlot first = coordinator.tryAcquireBatchSlot();
        NativeRequestPlaneCoordinator.BatchSlot second = coordinator.tryAcquireBatchSlot();
        assertNotNull(first);
        assertNotNull(second);
        assertNull(coordinator.tryAcquireBatchSlot());

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2", "k1")).run();

        assertEquals(1, state.getNativeMailboxCompactBatchesForTesting());
        assertEquals(1, state.getNativeMailboxCompactScratchBatchesForTesting());
        assertEquals(0, state.getNativeMailboxCompactFallbacksForTesting());
        assertEquals(1, state.getPrefetchKeysDeduplicatedForTesting());
        assertEquals(1, coordinator.compactionScratchLeases());
        NativeRequestPlaneCoordinator.BatchSlot scratch =
                coordinator.tryAcquireCompactionScratchSlot();
        assertNotNull(scratch);
        scratch.close();
        verify(reader, times(1)).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());

        first.close();
        second.close();
        state.close();
        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testLargeCompactionScratchCoversBatchAboveRegularSlotCapacity() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);

        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        byte[] serializedValue = KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE);
        when(reader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenReturn(java.util.Collections.nCopies(10, serializedValue));
        stubDirectPreparedSerialization(reader);

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(mailboxLargeScratchOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        0,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        1,
                        1 << 20,
                        false,
                        coordinator,
                        19);
        state.setCurrentNamespace("window-large-scratch");
        java.util.ArrayList<String> keys = new java.util.ArrayList<>();
        for (int i = 0; i < 10; i++) {
            keys.add("k" + i);
            keys.add("k" + i);
        }

        Runnable task = state.buildAsyncPrefetchTask(keys);
        assertNotNull(task);
        task.run();

        assertEquals(1, state.getNativeMailboxCompactBatchesForTesting());
        assertEquals(1, state.getNativeMailboxCompactScratchBatchesForTesting());
        assertEquals(20, state.getNativeMailboxCompactInputKeysForTesting());
        assertEquals(10, state.getNativeMailboxCompactUniqueKeysForTesting());
        assertEquals(0, state.getNativeMailboxCompactFallbacksForTesting());
        assertEquals(0, state.getNativeMailboxCompactSlotMissFallbacksForTesting());
        assertEquals(0, state.getNativeMailboxCompactCapacityFallbacksForTesting());
        assertEquals(0, state.getNativeMailboxCompactOperationFallbacksForTesting());
        assertEquals(10, state.getPrefetchKeysDeduplicatedForTesting());

        state.close();
        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCompactSelectedProbeMaterializesOnlyTrueRocksDbMisses() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);

        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(99);
        doAnswer(
                        invocation -> {
                            byte[] serialized =
                                    KvStateSerializer.serializeKeyAndNamespace(
                                            invocation.getArgument(0),
                                            StringSerializer.INSTANCE,
                                            invocation.getArgument(1),
                                            StringSerializer.INSTANCE);
                            ((PositionedDataOutputView) invocation.getArgument(4))
                                    .write(serialized);
                            return null;
                        })
                .when(reader)
                .serializeBatchKeyAndNamespace(any(), any(), any(), any(), any());
        byte[] preparedK1 =
                KvStateSerializer.serializeKeyAndNamespace(
                        "k1", StringSerializer.INSTANCE, "window-fused", StringSerializer.INSTANCE);
        byte[] preparedK2 =
                KvStateSerializer.serializeKeyAndNamespace(
                        "k2", StringSerializer.INSTANCE, "window-fused", StringSerializer.INSTANCE);
        byte[] preparedK3 =
                KvStateSerializer.serializeKeyAndNamespace(
                        "k3", StringSerializer.INSTANCE, "window-fused", StringSerializer.INSTANCE);
        when(reader.getSerializedValueByRocksDBKey(any()))
                .thenAnswer(
                        invocation -> {
                            assertArrayEquals(preparedK3, invocation.getArgument(0));
                            return KvStateSerializer.serializeValue(33, IntSerializer.INSTANCE);
                        });

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        fakePlane.preload(
                19,
                0,
                preparedK1,
                KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE));
        fakePlane.preloadNegative(19, 0, preparedK2);
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(compactSelectedOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        0,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        8,
                        1 << 20,
                        false,
                        coordinator,
                        19);
        state.setCurrentNamespace("window-fused");

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2", "k1", "k3")).run();

        assertEquals(1, state.getNativeMailboxCompactBatchesForTesting());
        assertEquals(4, state.getNativeMailboxCompactInputKeysForTesting());
        assertEquals(3, state.getNativeMailboxCompactUniqueKeysForTesting());
        assertEquals(1, state.getNativeCompactSelectedProbeBatchesForTesting());
        assertEquals(3, state.getNativeCompactSelectedProbeKeysForTesting());
        assertEquals(1, state.getNativeCompactSelectedLazyHeapKeyCopiesForTesting());
        assertEquals(0, state.getNativeCompactPostCompactBytesRecopiedForTesting());
        assertEquals(0, state.getNativeMailboxDirectSerializationFallbackKeysForTesting());
        assertEquals(0, state.getNativeMailboxDirectSerializationFallbackBytesForTesting());
        assertEquals(1, state.getNativeHitsForTesting());
        assertEquals(1, state.getNativeNegativeHitsForTesting());
        assertEquals(1, state.getNativeMissesForTesting());
        assertEquals(1, state.getPrefetchKeysDeduplicatedForTesting());
        verify(reader, times(1)).getSerializedValueByRocksDBKey(any());
        verify(reader, never()).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        verify(reader, never()).serializeBatchKeyAndNamespace(any(), any(), any(), any());

        currentKey.set("k1");
        assertEquals(11, state.value());
        currentKey.set("k2");
        assertEquals(99, state.value());
        currentKey.set("k3");
        assertEquals(33, state.value());

        state.close();
        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDeferredReservationMaterializesOnlyCompactedUniqueKeys() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);

        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        stubDirectPreparedSerialization(reader);
        byte[] value17 = KvStateSerializer.serializeValue(17, IntSerializer.INSTANCE);
        when(reader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenAnswer(
                        invocation -> {
                            int start = invocation.getArgument(1);
                            int end = invocation.getArgument(2);
                            java.util.ArrayList<byte[]> values = new java.util.ArrayList<>();
                            for (int index = start; index < end; index++) {
                                values.add(value17);
                            }
                            return values;
                        });

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(
                        deferredReservationOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 29, 8, 2);
        state.setCurrentNamespace("window-deferred");

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2", "k1", "k3")).run();

        assertEquals(4, state.getNativeDeferredReservationInputKeysForTesting());
        assertEquals(3, state.getNativeDeferredReservationObjectsMaterializedForTesting());
        assertEquals(1, state.getNativeDeferredReservationObjectsAvoidedForTesting());
        assertEquals(3, state.getNativeMailboxCompactUniqueKeysForTesting());
        assertEquals(1, state.getPrefetchKeysDeduplicatedForTesting());

        currentKey.set("k1");
        assertEquals(17, state.value());
        currentKey.set("k2");
        assertEquals(17, state.value());
        currentKey.set("k3");
        assertEquals(17, state.value());

        state.close();
        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDirectArenaMultiGetPreservesOrderAndAvoidsHeapKeys() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(99);
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        stubDirectPreparedSerialization(reader);

        byte[][] expectedKeys =
                new byte[][] {
                    serializedKey("k1", "window-direct"),
                    serializedKey("k2", "window-direct"),
                    serializedKey("k3", "window-direct")
                };
        byte[][] expectedValues =
                new byte[][] {
                    KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                    null,
                    KvStateSerializer.serializeValue(33, IntSerializer.INSTANCE)
                };
        doAnswer(
                        invocation -> {
                            ByteBuffer keys = invocation.getArgument(0);
                            ByteBuffer descriptors =
                                    ((ByteBuffer) invocation.getArgument(1))
                                            .duplicate()
                                            .order(ByteOrder.nativeOrder());
                            int count = invocation.getArgument(2);
                            ByteBuffer values = invocation.getArgument(3);
                            int stride = invocation.getArgument(4);
                            assertEquals(3, count);
                            int present = 0;
                            for (int index = 0; index < count; index++) {
                                int base =
                                        index
                                                * RocksDBBatchValueReader
                                                        .DIRECT_ARENA_DESCRIPTOR_BYTES;
                                int keyOffset =
                                        descriptors.getInt(
                                                base
                                                        + RocksDBBatchValueReader
                                                                .DIRECT_ARENA_KEY_OFFSET);
                                int keyLength =
                                        descriptors.getInt(
                                                base
                                                        + RocksDBBatchValueReader
                                                                .DIRECT_ARENA_KEY_LENGTH_OFFSET);
                                byte[] actualKey = new byte[keyLength];
                                ByteBuffer keySource = keys.duplicate();
                                keySource.position(keyOffset);
                                keySource.get(actualKey);
                                assertArrayEquals(expectedKeys[index], actualKey);
                                byte[] value = expectedValues[index];
                                if (value == null) {
                                    descriptors.putInt(
                                            base
                                                    + RocksDBBatchValueReader
                                                            .DIRECT_ARENA_RESULT_OFFSET,
                                            RocksDBBatchValueReader.DIRECT_ARENA_NOT_FOUND);
                                } else {
                                    ByteBuffer valueTarget = values.duplicate();
                                    valueTarget.position(index * stride);
                                    valueTarget.put(value);
                                    descriptors.putInt(
                                            base
                                                    + RocksDBBatchValueReader
                                                            .DIRECT_ARENA_RESULT_OFFSET,
                                            value.length);
                                    present++;
                                }
                            }
                            return present;
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeyArena(
                        any(), any(), anyInt(), any(), anyInt());

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 51, 8, 2);
        state.setCurrentNamespace("window-direct");

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2", "k3")).run();

        assertEquals(1, state.getNativeDirectArenaMultiGetBatchesForTesting());
        assertEquals(3, state.getNativeDirectArenaMultiGetKeysForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetCompletedBatchesForTesting());
        assertEquals(3, state.getNativeDirectArenaMultiGetCompletedKeysForTesting());
        assertEquals(
                expectedValues[0].length + expectedValues[2].length,
                state.getNativeDirectArenaMultiGetValueBytesCopiedForTesting());
        assertEquals(2, state.getNativeDirectArenaMultiGetFoundForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetNotFoundForTesting());
        assertEquals(0, state.getNativeDirectArenaMultiGetOverflowStatusesForTesting());
        assertEquals(3, state.getPrefetchAsyncValuesReadForTesting());
        assertEquals(2, state.getPrefetchAsyncUsefulValuesForTesting());
        assertArrayEquals(
                new long[] {0, 1, 0, 0, 0, 0, 0},
                state.getNativeDirectArenaMultiGetBatchHistogramForTesting());
        assertEquals(0, state.getNativeDirectArenaMultiGetHeapKeyCopiesForTesting());
        assertEquals(0, state.getNativeDirectArenaMultiGetFallbackBatchesForTesting());
        verify(reader, never()).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        verify(reader, never()).getSerializedValueByRocksDBKey(any());

        currentKey.set("k1");
        assertEquals(11, state.value());
        currentKey.set("k2");
        assertEquals(99, state.value());
        currentKey.set("k3");
        assertEquals(33, state.value());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDirectArenaReadOnlySkipsNativeProbeAndFill() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(99);
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        stubDirectPreparedSerialization(reader);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                serializedKey(
                                        invocation.getArgument(0), invocation.getArgument(1)));
        byte[] first = KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE);
        doAnswer(
                        invocation -> {
                            ByteBuffer descriptors =
                                    ((ByteBuffer) invocation.getArgument(1))
                                            .duplicate()
                                            .order(ByteOrder.nativeOrder());
                            ByteBuffer values = invocation.getArgument(3);
                            int stride = invocation.getArgument(4);
                            values.duplicate().position(0).put(first);
                            descriptors.putInt(
                                    RocksDBBatchValueReader.DIRECT_ARENA_RESULT_OFFSET,
                                    first.length);
                            descriptors.putInt(
                                    RocksDBBatchValueReader.DIRECT_ARENA_DESCRIPTOR_BYTES
                                            + RocksDBBatchValueReader.DIRECT_ARENA_RESULT_OFFSET,
                                    RocksDBBatchValueReader.DIRECT_ARENA_NOT_FOUND);
                            assertEquals(2, (int) invocation.getArgument(2));
                            assertTrue(stride >= first.length);
                            return 1;
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeyArena(
                        any(), any(), anyInt(), any(), anyInt());

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaReadOnlyOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 59, 8, 2);
        state.setCurrentNamespace("window-direct-read-only");

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")).run();

        assertEquals(0, coordinator.probeCalls());
        assertEquals(0, coordinator.fillCalls());
        assertEquals(1, state.getNativeDirectArenaReadOnlyBatchesForTesting());
        assertEquals(2, state.getNativeDirectArenaReadOnlyKeysForTesting());
        assertEquals(0, state.getNativeDirectArenaReadOnlyCancelledKeysForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetBatchesForTesting());
        assertEquals(0, state.getNativeCompactSelectedProbeBatchesForTesting());
        assertEquals(2, state.getPrefetchAsyncValuesReadForTesting());
        assertEquals(1, state.getPrefetchAsyncUsefulValuesForTesting());
        verify(reader, never()).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());

        currentKey.set("k1");
        assertEquals(11, state.value());
        currentKey.set("k2");
        assertEquals(99, state.value());

        state.setCurrentNamespace("window-direct-read-only-immediate");
        state.prefetchForImmediateUse(Arrays.asList("k3", "k4"));
        assertEquals(0, coordinator.probeCalls());
        assertEquals(0, coordinator.fillCalls());
        assertEquals(2, state.getNativeDirectArenaReadOnlyBatchesForTesting());
        assertEquals(4, state.getNativeDirectArenaReadOnlyKeysForTesting());
        assertEquals(2, state.getPrefetchAsyncValuesReadForTesting());
        assertEquals(1, state.getPrefetchAsyncUsefulValuesForTesting());
        currentKey.set("k3");
        assertEquals(11, state.value());
        currentKey.set("k4");
        assertEquals(99, state.value());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDirectArenaMailboxBatchHandoffPromotesWithoutPerKeyStaging() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(99);
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        stubDirectPreparedSerialization(reader);
        byte[] first = KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE);
        stubDirectArenaValues(reader, first, null);

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaReadOnlyOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedCachedState(delegate, currentKey, coordinator, 76);
        state.setNativeMailboxBatchHandoffEnabledForTesting(true);
        state.setCurrentNamespace("window-mailbox-batch-handoff");

        // "Aa" and "BB" deliberately collide under String.hashCode(). Consume them in reverse
        // order to exercise both open-address collision handling and out-of-order exact lookup.
        state.buildAsyncPrefetchTask(Arrays.asList("Aa", "BB")).run();

        assertEquals(0, state.getStagingSizeForTesting());
        assertEquals(1, state.getNativeMailboxBatchHandoffReadyBatchesForTesting());
        assertEquals(1, state.getNativeMailboxBatchHandoffOfferedBatchesForTesting());
        assertEquals(2, state.getNativeMailboxBatchHandoffOfferedKeysForTesting());
        assertEquals(2, state.getNativeMailboxBatchHandoffLegacyPublicationsAvoidedForTesting());
        assertTrue(state.hasInFlightReservationForTesting("Aa", "window-mailbox-batch-handoff"));

        currentKey.set("BB");
        assertEquals(99, state.value());
        assertEquals(1, state.getNativeMailboxBatchHandoffReadyBatchesForTesting());
        assertEquals(1, state.getNativeMailboxBatchHandoffPromotedKeysForTesting());
        assertTrue(state.hasInFlightReservationForTesting("Aa", "window-mailbox-batch-handoff"));
        currentKey.set("Aa");
        assertEquals(11, state.value());
        assertEquals(0, state.getNativeMailboxBatchHandoffReadyBatchesForTesting());
        assertEquals(2, state.getNativeMailboxBatchHandoffPromotedKeysForTesting());
        assertEquals(0, state.getNativeMailboxBatchHandoffInvalidatedKeysForTesting());
        assertFalse(state.hasInFlightReservationForTesting("Aa", "window-mailbox-batch-handoff"));
        assertFalse(state.hasInFlightReservationForTesting("BB", "window-mailbox-batch-handoff"));
        verify(delegate, never()).value();

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDirectArenaMailboxBatchHandoffHonorsExactWriteAndCloseFences() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(99);
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        stubDirectPreparedSerialization(reader);
        byte[] first = KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE);
        byte[] second = KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE);
        stubDirectArenaValues(reader, first, second);

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaReadOnlyOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedCachedState(delegate, currentKey, coordinator, 77);
        state.setNativeMailboxBatchHandoffEnabledForTesting(true);
        state.setCurrentNamespace("window-mailbox-batch-fence");

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")).run();
        currentKey.set("k2");
        state.update(88);
        currentKey.set("k1");
        assertEquals(11, state.value());
        currentKey.set("k2");
        assertEquals(88, state.value());
        assertEquals(1, state.getNativeMailboxBatchHandoffPromotedKeysForTesting());
        assertEquals(1, state.getNativeMailboxBatchHandoffInvalidatedKeysForTesting());

        state.setCurrentNamespace("window-mailbox-batch-close");
        state.buildAsyncPrefetchTask(Arrays.asList("k3", "k4")).run();
        assertEquals(1, state.getNativeMailboxBatchHandoffReadyBatchesForTesting());
        state.close();
        assertEquals(0, state.getNativeMailboxBatchHandoffReadyBatchesForTesting());
        assertFalse(state.hasInFlightReservationForTesting("k3", "window-mailbox-batch-close"));
        assertFalse(state.hasInFlightReservationForTesting("k4", "window-mailbox-batch-close"));

        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDirectArenaMailboxBatchHandoffFallsBackWhenQueueIsFull() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(99);
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        stubDirectPreparedSerialization(reader);
        stubDirectArenaValues(
                reader,
                KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE));

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaReadOnlyOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedCachedState(delegate, currentKey, coordinator, 78);
        state.setNativeMailboxBatchHandoffEnabledForTesting(true);

        for (int batch = 0; batch < 5; batch++) {
            state.setCurrentNamespace("window-mailbox-batch-full-" + batch);
            state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")).run();
        }

        assertEquals(4, state.getNativeMailboxBatchHandoffReadyBatchesForTesting());
        assertEquals(1, state.getNativeMailboxBatchHandoffQueueFullFallbacksForTesting());
        assertEquals(2, state.getStagingSizeForTesting());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDirectArenaReadOnlyEagerMaterializesWithoutHeapValueCopy() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(99);
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        stubDirectPreparedSerialization(reader);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                serializedKey(
                                        invocation.getArgument(0), invocation.getArgument(1)));
        byte[] first = KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE);
        doAnswer(
                        invocation -> {
                            ByteBuffer descriptors =
                                    ((ByteBuffer) invocation.getArgument(1))
                                            .duplicate()
                                            .order(ByteOrder.nativeOrder());
                            ByteBuffer values = invocation.getArgument(3);
                            int stride = invocation.getArgument(4);
                            values.duplicate().position(0).put(first);
                            descriptors.putInt(
                                    RocksDBBatchValueReader.DIRECT_ARENA_RESULT_OFFSET,
                                    first.length);
                            descriptors.putInt(
                                    RocksDBBatchValueReader.DIRECT_ARENA_DESCRIPTOR_BYTES
                                            + RocksDBBatchValueReader.DIRECT_ARENA_RESULT_OFFSET,
                                    RocksDBBatchValueReader.DIRECT_ARENA_NOT_FOUND);
                            assertEquals(2, (int) invocation.getArgument(2));
                            assertTrue(stride >= first.length);
                            return 1;
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeyArena(
                        any(), any(), anyInt(), any(), anyInt());

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(
                        directArenaReadOnlyOptions(16, false, true), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 61, 8, 2);
        state.setCurrentNamespace("window-direct-read-only-eager");

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")).run();

        assertEquals(1, state.getNativeDirectArenaReadOnlyBatchesForTesting());
        assertEquals(2, state.getNativeDirectArenaReadOnlyKeysForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetBatchesForTesting());
        assertEquals(0, state.getNativeDirectArenaMultiGetValueBytesCopiedForTesting());
        assertEquals(1, state.getNativeDirectArenaEagerMaterializedValuesForTesting());
        assertEquals(
                first.length,
                state.getNativeDirectArenaEagerMaterializedValueBytesForTesting());
        assertEquals(1, state.getNativeDirectArenaEagerMissingValuesForTesting());
        assertEquals(0, state.getNativeDirectArenaEagerFallbackValuesForTesting());
        assertEquals(2, state.getPrefetchAsyncValuesReadForTesting());
        assertEquals(1, state.getPrefetchAsyncUsefulValuesForTesting());
        verify(reader, never()).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());

        currentKey.set("k1");
        assertEquals(11, state.value());
        currentKey.set("k2");
        assertEquals(99, state.value());

        state.setCurrentNamespace("window-direct-read-only-eager-immediate");
        state.prefetchForImmediateUse(Arrays.asList("k3", "k4"));
        currentKey.set("k3");
        assertEquals(11, state.value());
        currentKey.set("k4");
        assertEquals(99, state.value());
        assertEquals(2, state.getNativeDirectArenaReadOnlyBatchesForTesting());
        assertEquals(4, state.getNativeDirectArenaReadOnlyKeysForTesting());
        assertEquals(2, state.getNativeDirectArenaEagerMaterializedValuesForTesting());
        assertEquals(
                2L * first.length,
                state.getNativeDirectArenaEagerMaterializedValueBytesForTesting());
        assertEquals(2, state.getNativeDirectArenaEagerMissingValuesForTesting());
        assertEquals(2, state.getPrefetchAsyncValuesReadForTesting());
        assertEquals(1, state.getPrefetchAsyncUsefulValuesForTesting());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDirectArenaReadOnlyDropsPreCompactSpeculativeSmallBatch() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(77);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(99);
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        stubDirectPreparedSerialization(reader);

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaReadOnlyOptions(64), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 71, 64, 16);
        state.setCurrentNamespace("window-precompact-small");
        java.util.ArrayList<String> keys = new java.util.ArrayList<>();
        for (int index = 0; index < 15; index++) {
            keys.add("k" + index);
        }

        assertNull(state.buildAsyncPrefetchTask(keys));
        assertEquals(1, state.getNativeDirectArenaSpeculativePreCompactDropsForTesting());
        assertEquals(15, state.getNativeDirectArenaSpeculativePreCompactKeysForTesting());
        assertEquals(0, fakePlane.compactCalls);
        assertEquals(0, state.getNativeDirectArenaMultiGetBatchesForTesting());
        assertEquals(0, state.getStagingSizeForTesting());
        verify(reader, never())
                .getSerializedValuesByRocksDBKeyArena(any(), any(), anyInt(), any(), anyInt());
        verify(reader, never()).getSerializedValueByRocksDBKey(any());

        currentKey.set("k14");
        assertEquals(77, state.value());
        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDirectArenaNegativeHandoffPromotesDefaultAndExactWriteInvalidates() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(7);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(99);
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        stubDirectPreparedSerialization(reader);
        doAnswer(
                        invocation -> {
                            ByteBuffer descriptors =
                                    ((ByteBuffer) invocation.getArgument(1))
                                            .duplicate()
                                            .order(ByteOrder.nativeOrder());
                            int count = invocation.getArgument(2);
                            assertEquals(2, count);
                            for (int index = 0; index < count; index++) {
                                descriptors.putInt(
                                        index
                                                        * RocksDBBatchValueReader
                                                                .DIRECT_ARENA_DESCRIPTOR_BYTES
                                                + RocksDBBatchValueReader
                                                        .DIRECT_ARENA_RESULT_OFFSET,
                                        RocksDBBatchValueReader.DIRECT_ARENA_NOT_FOUND);
                            }
                            return 0;
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeyArena(
                        any(), any(), anyInt(), any(), anyInt());

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(
                        directArenaReadOnlyOptions(16, true), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 75, 8, 2, true);
        state.setCurrentNamespace("window-negative-handoff");

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")).run();
        assertEquals(2, state.getStagingSizeForTesting());
        assertEquals(2, state.getNativeDirectArenaNegativeHandoffStagedForTesting());
        assertEquals(0, state.getStagingRetainedBytesForTesting());

        currentKey.set("k1");
        assertEquals(99, state.value());
        assertEquals(1, state.getNativeDirectArenaNegativeHandoffPromotedForTesting());
        assertEquals(1, state.getStagingSizeForTesting());

        currentKey.set("k2");
        state.update(8);
        state.flush();
        assertEquals(0, state.getStagingSizeForTesting());
        assertEquals(1, state.getNativeDirectArenaNegativeHandoffInvalidatedForTesting());
        assertEquals(0, state.getNativeGenerationAdvancesForTesting());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDirectArenaReadOnlyDropsDuplicateHeavyPostCompactSmallBatch() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(88);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(99);
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        stubDirectPreparedSerialization(reader);

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaReadOnlyOptions(64), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 72, 64, 16);
        state.setCurrentNamespace("window-postcompact-small");
        String[] duplicateKeys = new String[16];
        Arrays.fill(duplicateKeys, "same");

        assertNull(state.buildAsyncPrefetchTask(Arrays.asList(duplicateKeys)));
        assertEquals(1, fakePlane.compactCalls);
        assertEquals(1, state.getNativeMailboxCompactBatchesForTesting());
        assertEquals(16, state.getNativeMailboxCompactInputKeysForTesting());
        assertEquals(1, state.getNativeMailboxCompactUniqueKeysForTesting());
        assertEquals(1, state.getNativeDirectArenaSpeculativePostCompactDropsForTesting());
        assertEquals(1, state.getNativeDirectArenaSpeculativePostCompactKeysForTesting());
        assertEquals(0, state.getNativeDirectArenaMultiGetBatchesForTesting());
        verify(reader, never())
                .getSerializedValuesByRocksDBKeyArena(any(), any(), anyInt(), any(), anyInt());
        verify(reader, never()).getSerializedValueByRocksDBKey(any());

        currentKey.set("same");
        assertEquals(88, state.value());
        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDirectArenaReadOnlyDropsOnlySpeculativeTail() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(66);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(99);
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        stubDirectPreparedSerialization(reader);
        doAnswer(
                        invocation -> {
                            ByteBuffer descriptors =
                                    ((ByteBuffer) invocation.getArgument(1))
                                            .duplicate()
                                            .order(ByteOrder.nativeOrder());
                            int count = invocation.getArgument(2);
                            assertEquals(64, count);
                            for (int index = 0; index < count; index++) {
                                descriptors.putInt(
                                        index
                                                        * RocksDBBatchValueReader
                                                                .DIRECT_ARENA_DESCRIPTOR_BYTES
                                                + RocksDBBatchValueReader
                                                        .DIRECT_ARENA_RESULT_OFFSET,
                                        RocksDBBatchValueReader.DIRECT_ARENA_NOT_FOUND);
                            }
                            return 0;
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeyArena(any(), any(), anyInt(), any(), anyInt());

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(
                        directArenaReadOnlyOptions(128), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 73, 128, 16);
        state.setCurrentNamespace("window-tail-small");
        java.util.ArrayList<String> keys = new java.util.ArrayList<>();
        for (int index = 0; index < 65; index++) {
            keys.add("k" + index);
        }

        Runnable task = state.buildAsyncPrefetchTask(keys);
        assertNotNull(task);
        task.run();
        assertEquals(1, state.getNativeDirectArenaSpeculativeTailDropsForTesting());
        assertEquals(1, state.getNativeDirectArenaSpeculativeTailKeysForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetBatchesForTesting());
        assertEquals(64, state.getNativeDirectArenaMultiGetKeysForTesting());
        assertEquals(64, state.getNativeDirectArenaReadOnlyKeysForTesting());
        verify(reader, never()).getSerializedValueByRocksDBKey(any());

        currentKey.set("k64");
        assertEquals(66, state.value());
        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDirectArenaReadOnlyKeepsImmediateSmallBatchAuthoritative() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(99);
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        stubDirectPreparedSerialization(reader);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                serializedKey(
                                        invocation.getArgument(0), invocation.getArgument(1)));
        when(reader.getSerializedValueByRocksDBKey(any()))
                .thenReturn(
                        KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                        KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE));

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaReadOnlyOptions(64), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 74, 64, 16);
        state.setCurrentNamespace("window-immediate-small");

        state.prefetchForImmediateUse(Arrays.asList("k1", "k2"));
        assertEquals(0, state.getNativeDirectArenaSpeculativePreCompactDropsForTesting());
        assertEquals(0, state.getNativeDirectArenaSpeculativePostCompactDropsForTesting());
        assertEquals(0, state.getNativeDirectArenaSpeculativeTailDropsForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetFallbackBatchesForTesting());
        assertEquals(2, state.getNativeDirectArenaMultiGetFallbackKeysForTesting());
        assertEquals(2, state.getNativeDirectArenaMultiGetHeapKeyCopiesForTesting());
        verify(reader, times(2)).getSerializedValueByRocksDBKey(any());

        currentKey.set("k1");
        assertEquals(11, state.value());
        currentKey.set("k2");
        assertEquals(22, state.value());
        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDirectArenaReadOnlyKeyScopedWriteCancelsMatchingReservation() throws Exception {
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(99);
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        stubDirectPreparedSerialization(reader);
        byte[] stale = KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE);
        byte[] survivor = KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE);
        doAnswer(
                        invocation -> {
                            readStarted.countDown();
                            if (!releaseRead.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("Timed out waiting to release direct read");
                            }
                            ByteBuffer descriptors =
                                    ((ByteBuffer) invocation.getArgument(1))
                                            .duplicate()
                                            .order(ByteOrder.nativeOrder());
                            ByteBuffer values = invocation.getArgument(3);
                            int stride = invocation.getArgument(4);
                            values.duplicate().position(0).put(stale);
                            values.duplicate().position(stride).put(survivor);
                            descriptors.putInt(
                                    RocksDBBatchValueReader.DIRECT_ARENA_RESULT_OFFSET,
                                    stale.length);
                            descriptors.putInt(
                                    RocksDBBatchValueReader.DIRECT_ARENA_DESCRIPTOR_BYTES
                                            + RocksDBBatchValueReader.DIRECT_ARENA_RESULT_OFFSET,
                                    survivor.length);
                            return 2;
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeyArena(
                        any(), any(), anyInt(), any(), anyInt());

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaReadOnlyOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 67, 8, 2, true);
        state.setCurrentNamespace("window-direct-keyscope");
        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("stale", "survivor"));
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        Thread worker =
                new Thread(
                        () -> {
                            try {
                                task.run();
                            } catch (Throwable failure) {
                                workerFailure.set(failure);
                            }
                        },
                        "direct-read-only-keyscope-race");
        try {
            worker.start();
            assertTrue(readStarted.await(5, TimeUnit.SECONDS));

            currentKey.set("stale");
            state.update(7);
            state.flush();
            releaseRead.countDown();
            worker.join(5000);

            assertFalse(worker.isAlive());
            assertNull(workerFailure.get());
            assertEquals(1, state.getPrefetchKeyScopedInvalidationsForTesting());
            assertEquals(1, state.getPrefetchKeyScopedInFlightCancelledForTesting());
            assertEquals(0, state.getNativeGenerationAdvancesForTesting());
            assertEquals(1, state.getPrefetchWorkerDiscardedAfterReadForTesting());
            assertEquals(1, state.getStagingSizeForTesting());
            currentKey.set("survivor");
            assertEquals(22, state.value());
            currentKey.set("stale");
            assertEquals(7, state.value());
        } finally {
            releaseRead.countDown();
            worker.join(5000);
            state.close();
            coordinator.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDirectArenaLinkageFailureLatchesOffAndFallsBackOnce() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        stubDirectPreparedSerialization(reader);
        doAnswer(
                        invocation -> {
                            throw new UnsatisfiedLinkError("injected missing JNI symbol");
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeyArena(
                        any(), any(), anyInt(), any(), anyInt());
        when(reader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenAnswer(
                        invocation -> {
                            java.util.List<byte[]> keys = invocation.getArgument(0);
                            java.util.ArrayList<byte[]> values =
                                    new java.util.ArrayList<>(keys.size());
                            for (byte[] key : keys) {
                                int value =
                                        Arrays.equals(
                                                        key,
                                                        serializedKey(
                                                                "k1", "window-linkage"))
                                                ? 11
                                                : Arrays.equals(
                                                                key,
                                                                serializedKey(
                                                                        "k2",
                                                                        "window-linkage"))
                                                        ? 22
                                                        : 33;
                                values.add(
                                        KvStateSerializer.serializeValue(
                                                value, IntSerializer.INSTANCE));
                            }
                            return values;
                        });
        when(reader.getSerializedValueByRocksDBKey(any()))
                .thenReturn(KvStateSerializer.serializeValue(33, IntSerializer.INSTANCE));

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 53, 8, 1);
        state.setCurrentNamespace("window-linkage");

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2", "k3")).run();

        assertTrue(state.isNativeDirectArenaMultiGetDisabledForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetLinkageFallbacksForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetFallbackBatchesForTesting());
        assertEquals(3, state.getNativeDirectArenaMultiGetFallbackKeysForTesting());
        assertEquals(1, state.getPrefetchMultiGetCallsForTesting());
        verify(reader, times(1))
                .getSerializedValuesByRocksDBKeyArena(
                        any(), any(), anyInt(), any(), anyInt());
        verify(reader, times(1)).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        verify(reader, never()).getSerializedValueByRocksDBKey(any());

        currentKey.set("k1");
        assertEquals(11, state.value());
        currentKey.set("k2");
        assertEquals(22, state.value());
        currentKey.set("k3");
        assertEquals(33, state.value());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testConfiguredDirectArenaMissingCapabilityIsVisibleAndFallsBack() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        // Do not stub supportsDirectArenaMultiGet(): this emulates an older implementation that
        // inherits the interface's default false capability advertisement.
        stubDirectPreparedSerialization(reader);
        when(reader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE)));

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 56, 8, 1);
        state.setCurrentNamespace("window-capability");

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")).run();

        assertTrue(state.isNativeDirectArenaMultiGetDisabledForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetCapabilityFallbacksForTesting());
        assertEquals(0, state.getNativeDirectArenaMultiGetBatchesForTesting());
        assertEquals(1, state.getPrefetchMultiGetCallsForTesting());
        verify(reader, never())
                .getSerializedValuesByRocksDBKeyArena(any(), any(), anyInt(), any(), anyInt());
        verify(reader, times(1)).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());

        currentKey.set("k1");
        assertEquals(11, state.value());
        currentKey.set("k2");
        assertEquals(22, state.value());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDirectArenaInvalidResultProtocolLatchesOffAndFallsBack() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        stubDirectPreparedSerialization(reader);
        doAnswer(
                        invocation -> {
                            ByteBuffer descriptors =
                                    ((ByteBuffer) invocation.getArgument(1))
                                            .duplicate()
                                            .order(ByteOrder.nativeOrder());
                            descriptors.putInt(
                                    RocksDBBatchValueReader.DIRECT_ARENA_RESULT_OFFSET,
                                    (Integer) invocation.getArgument(4) + 1);
                            return 1;
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeyArena(
                        any(), any(), anyInt(), any(), anyInt());
        when(reader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE)));

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 54, 8, 1);
        state.setCurrentNamespace("window-protocol");

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")).run();

        assertTrue(state.isNativeDirectArenaMultiGetDisabledForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetProtocolFallbacksForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetFallbackBatchesForTesting());
        assertEquals(2, state.getNativeDirectArenaMultiGetFallbackKeysForTesting());
        verify(reader, times(1)).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());

        currentKey.set("k1");
        assertEquals(11, state.value());
        currentKey.set("k2");
        assertEquals(22, state.value());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDirectArenaSplits65KeysWithoutReordering() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        stubDirectPreparedSerialization(reader);
        AtomicInteger nextValue = new AtomicInteger(1000);
        doAnswer(
                        invocation -> {
                            ByteBuffer descriptors =
                                    ((ByteBuffer) invocation.getArgument(1))
                                            .duplicate()
                                            .order(ByteOrder.nativeOrder());
                            int count = invocation.getArgument(2);
                            ByteBuffer values = invocation.getArgument(3);
                            int stride = invocation.getArgument(4);
                            for (int index = 0; index < count; index++) {
                                byte[] value =
                                        KvStateSerializer.serializeValue(
                                                nextValue.getAndIncrement(),
                                                IntSerializer.INSTANCE);
                                ByteBuffer target = values.duplicate();
                                target.position(index * stride);
                                target.put(value);
                                descriptors.putInt(
                                        index
                                                        * RocksDBBatchValueReader
                                                                .DIRECT_ARENA_DESCRIPTOR_BYTES
                                                + RocksDBBatchValueReader
                                                        .DIRECT_ARENA_RESULT_OFFSET,
                                        value.length);
                            }
                            return count;
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeyArena(
                        any(), any(), anyInt(), any(), anyInt());
        when(reader.getSerializedValueByRocksDBKey(any()))
                .thenReturn(KvStateSerializer.serializeValue(1064, IntSerializer.INSTANCE));

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(128), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 55, 128, 1);
        state.setCurrentNamespace("window-65");
        java.util.ArrayList<String> keys = new java.util.ArrayList<>(65);
        for (int index = 0; index < 65; index++) {
            keys.add("k" + index);
        }

        state.buildAsyncPrefetchTask(keys).run();

        assertEquals(1, state.getNativeDirectArenaMultiGetBatchesForTesting());
        assertEquals(64, state.getNativeDirectArenaMultiGetKeysForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetCompletedBatchesForTesting());
        assertEquals(64, state.getNativeDirectArenaMultiGetCompletedKeysForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetThresholdFallbacksForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetFallbackBatchesForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetFallbackKeysForTesting());
        assertArrayEquals(
                new long[] {0, 0, 0, 0, 0, 0, 1},
                state.getNativeDirectArenaMultiGetBatchHistogramForTesting());
        verify(reader, times(1))
                .getSerializedValuesByRocksDBKeyArena(
                        any(), any(), anyInt(), any(), anyInt());
        verify(reader, times(1)).getSerializedValueByRocksDBKey(any());

        for (int index = 0; index < 65; index++) {
            currentKey.set("k" + index);
            assertEquals(1000 + index, state.value());
        }

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDirectArenaExtendedCapabilityExecutesOne128KeyChunk() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        stubDirectPreparedSerialization(reader);
        when(reader.directArenaMultiGetMaxBatch()).thenReturn(128);
        AtomicInteger call = new AtomicInteger();
        doAnswer(
                        invocation -> {
                            int callIndex = call.getAndIncrement();
                            ByteBuffer descriptors =
                                    ((ByteBuffer) invocation.getArgument(1))
                                            .duplicate()
                                            .order(ByteOrder.nativeOrder());
                            int count = invocation.getArgument(2);
                            ByteBuffer values = invocation.getArgument(3);
                            int stride = invocation.getArgument(4);
                            assertEquals(128, count);
                            for (int index = 0; index < count; index++) {
                                byte[] value =
                                        KvStateSerializer.serializeValue(
                                                2000 + callIndex * 128 + index,
                                                IntSerializer.INSTANCE);
                                ByteBuffer target = values.duplicate();
                                target.position(index * stride);
                                target.put(value);
                                descriptors.putInt(
                                        index
                                                        * RocksDBBatchValueReader
                                                                .DIRECT_ARENA_DESCRIPTOR_BYTES
                                                + RocksDBBatchValueReader
                                                        .DIRECT_ARENA_RESULT_OFFSET,
                                        value.length);
                            }
                            return count;
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeyArena(any(), any(), anyInt(), any(), anyInt());

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(
                        directArenaOptions(128).withDirectArenaBatchSize(128), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 57, 128, 1);
        state.setCurrentNamespace("window-128");
        java.util.ArrayList<String> keys = new java.util.ArrayList<>(128);
        for (int index = 0; index < 128; index++) {
            keys.add("k" + index);
        }

        state.buildAsyncPrefetchTask(keys).run();

        assertEquals(1, state.getNativeDirectArenaMultiGetBatchesForTesting());
        assertEquals(128, state.getNativeDirectArenaMultiGetKeysForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetCompletedBatchesForTesting());
        assertEquals(128, state.getNativeDirectArenaMultiGetCompletedKeysForTesting());
        assertEquals(0, state.getNativeDirectArenaMultiGetFallbackBatchesForTesting());
        assertArrayEquals(
                new long[] {0, 0, 0, 0, 0, 0, 1},
                state.getNativeDirectArenaMultiGetBatchHistogramForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetBatch128ForTesting());
        verify(reader, times(1))
                .getSerializedValuesByRocksDBKeyArena(any(), any(), anyInt(), any(), anyInt());
        verify(reader, never()).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        verify(reader, never()).getSerializedValueByRocksDBKey(any());

        for (int index = 0; index < 128; index++) {
            currentKey.set("k" + index);
            assertEquals(2000 + index, state.value());
        }

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDirectArenaOverflowFallsBackForWholeChunk() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.supportsDirectArenaMultiGet()).thenReturn(true);
        stubDirectPreparedSerialization(reader);
        doAnswer(
                        invocation -> {
                            ByteBuffer descriptors =
                                    ((ByteBuffer) invocation.getArgument(1))
                                            .duplicate()
                                            .order(ByteOrder.nativeOrder());
                            descriptors.putInt(
                                    RocksDBBatchValueReader.DIRECT_ARENA_RESULT_OFFSET,
                                    RocksDBBatchValueReader.DIRECT_ARENA_OVERFLOW);
                            descriptors.putInt(
                                    RocksDBBatchValueReader.DIRECT_ARENA_DESCRIPTOR_BYTES
                                            + RocksDBBatchValueReader.DIRECT_ARENA_RESULT_OFFSET,
                                    4);
                            return 2;
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeyArena(
                        any(), any(), anyInt(), any(), anyInt());
        when(reader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenAnswer(
                        invocation -> {
                            java.util.List<byte[]> keys = invocation.getArgument(0);
                            assertArrayEquals(serializedKey("k1", "window-overflow"), keys.get(0));
                            assertArrayEquals(serializedKey("k2", "window-overflow"), keys.get(1));
                            return Arrays.asList(
                                    KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                                    KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE));
                        });

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 52, 8, 2);
        state.setCurrentNamespace("window-overflow");

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")).run();

        assertEquals(1, state.getNativeDirectArenaMultiGetOverflowsForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetFoundForTesting());
        assertEquals(0, state.getNativeDirectArenaMultiGetNotFoundForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetOverflowStatusesForTesting());
        assertEquals(1, state.getNativeDirectArenaMultiGetFallbackBatchesForTesting());
        assertEquals(2, state.getNativeDirectArenaMultiGetFallbackKeysForTesting());
        assertEquals(2, state.getNativeDirectArenaMultiGetHeapKeyCopiesForTesting());
        assertEquals(2, state.getNativeCompactSelectedLazyHeapKeyCopiesForTesting());
        assertFalse(state.isNativeDirectArenaMultiGetDisabledForTesting());
        verify(reader, times(1)).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());

        currentKey.set("k1");
        assertEquals(11, state.value());
        currentKey.set("k2");
        assertEquals(22, state.value());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testAdaptiveBypassSkipsZeroUsefulNativeProbeButKeepsRocksDbPath() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        doAnswer(
                        invocation -> {
                            byte[] serialized =
                                    KvStateSerializer.serializeKeyAndNamespace(
                                            invocation.getArgument(0),
                                            StringSerializer.INSTANCE,
                                            invocation.getArgument(1),
                                            StringSerializer.INSTANCE);
                            ((PositionedDataOutputView) invocation.getArgument(4))
                                    .write(serialized);
                            return null;
                        })
                .when(reader)
                .serializeBatchKeyAndNamespace(any(), any(), any(), any(), any());
        when(reader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenAnswer(
                        invocation ->
                                Arrays.asList(
                                        KvStateSerializer.serializeValue(
                                                11, IntSerializer.INSTANCE),
                                        KvStateSerializer.serializeValue(
                                                22, IntSerializer.INSTANCE)));

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(compactSelectedOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        0,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        8,
                        1 << 20,
                        false,
                        coordinator,
                        29);
        state.setCurrentNamespace("window-adaptive-bypass");
        AdaptiveNativeProbeController controller =
                new AdaptiveNativeProbeController(2, 1, 1, 16, 1, 0.01);
        state.setAdaptiveNativeProbeControllerForTesting(controller);

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")).run();
        assertEquals(AdaptiveNativeProbeController.Mode.BYPASS, controller.mode());
        assertEquals(1, coordinator.probeCalls());
        assertEquals(1, coordinator.fillCalls());

        state.buildAsyncPrefetchTask(Arrays.asList("k3", "k4")).run();
        assertEquals(1, coordinator.probeCalls());
        assertEquals(1, coordinator.fillCalls());
        assertEquals(1, controller.bypassedBatches());
        assertEquals(2, controller.bypassedKeys());
        assertEquals(2, state.getNativeMailboxCompactBatchesForTesting());
        verify(reader, times(2)).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());

        state.close();
        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testAdaptiveMailboxDensityBypassKeepsJavaPreparedMultiGetAndValues() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        when(reader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenAnswer(
                        invocation ->
                                Arrays.asList(
                                        KvStateSerializer.serializeValue(
                                                31, IntSerializer.INSTANCE),
                                        KvStateSerializer.serializeValue(
                                                42, IntSerializer.INSTANCE)));

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(compactSelectedOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        0,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        8,
                        1 << 20,
                        false,
                        coordinator,
                        30);
        state.setCurrentNamespace("window-mailbox-density-bypass");
        AdaptiveNativeMailboxDensityController controller =
                new AdaptiveNativeMailboxDensityController(2, 1, 1, 16, 0.10, 0.20);
        controller.recordCompaction(2, 0);
        assertEquals(AdaptiveNativeMailboxDensityController.Mode.BYPASS, controller.mode());
        state.setAdaptiveNativeMailboxDensityControllerForTesting(controller);

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")).run();

        assertEquals(0, fakePlane.compactCalls);
        assertEquals(0, coordinator.probeCalls());
        assertEquals(0, coordinator.fillCalls());
        assertEquals(0, state.getNativeMailboxCompactBatchesForTesting());
        assertEquals(1, controller.bypassedBatches());
        assertEquals(2, controller.bypassedInputKeys());
        verify(reader, times(1)).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());

        currentKey.set("k1");
        assertEquals(31, state.value());
        currentKey.set("k2");
        assertEquals(42, state.value());

        state.close();
        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testAdaptiveMailboxDensityCanDropOnlySpeculativeTaskBeforeKeyPreparation()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(77);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(compactSelectedOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        0,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        8,
                        1 << 20,
                        false,
                        coordinator,
                        31);
        state.setCurrentNamespace("window-mailbox-density-drop");
        AdaptiveNativeMailboxDensityController controller =
                new AdaptiveNativeMailboxDensityController(2, 1, 1, 16, 0.10, 0.20);
        controller.recordCompaction(2, 0);
        assertEquals(AdaptiveNativeMailboxDensityController.Mode.BYPASS, controller.mode());
        state.setAdaptiveNativeMailboxDensityControllerForTesting(controller);
        state.setAdaptiveNativeMailboxDropSpeculativePrefetchEnabledForTesting(true);

        assertNull(state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")));

        assertEquals(0, fakePlane.compactCalls);
        assertEquals(0, coordinator.probeCalls());
        assertEquals(0, coordinator.fillCalls());
        assertEquals(1, controller.bypassedBatches());
        assertEquals(0, controller.bypassedInputKeys());
        assertEquals(1, controller.droppedSpeculativePrefetchTasks());
        verify(reader, never()).serializeBatchKeyAndNamespace(any(), any(), any(), any());
        verify(reader, never()).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());

        currentKey.set("k1");
        assertEquals(77, state.value());
        verify(delegate, times(1)).value();

        state.close();
        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCompactSelectedNativeFiltersMailboxCancellationBeforeMissIoAndPublish()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(101);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        doAnswer(
                        invocation -> {
                            byte[] serialized =
                                    KvStateSerializer.serializeKeyAndNamespace(
                                            invocation.getArgument(0),
                                            StringSerializer.INSTANCE,
                                            invocation.getArgument(1),
                                            StringSerializer.INSTANCE);
                            ((PositionedDataOutputView) invocation.getArgument(4))
                                    .write(serialized);
                            return null;
                        })
                .when(reader)
                .serializeBatchKeyAndNamespace(any(), any(), any(), any(), any());

        byte[] preparedK1 =
                KvStateSerializer.serializeKeyAndNamespace(
                        "k1", StringSerializer.INSTANCE, "window-cancel", StringSerializer.INSTANCE);
        byte[] preparedK3 =
                KvStateSerializer.serializeKeyAndNamespace(
                        "k3", StringSerializer.INSTANCE, "window-cancel", StringSerializer.INSTANCE);
        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        fakePlane.preload(
                20,
                0,
                preparedK1,
                KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE));
        fakePlane.preload(
                20,
                0,
                preparedK3,
                KvStateSerializer.serializeValue(33, IntSerializer.INSTANCE));
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(compactSelectedOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        0,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        8,
                        1 << 20,
                        false,
                        coordinator,
                        20);
        state.setCurrentNamespace("window-cancel");

        Runnable dropped = state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2", "k3"));
        assertNotNull(dropped);
        ((PrefetchExecutor.DropAwareTask) dropped).onDrop();
        assertEquals(0, state.getNativeCompactSelectedProbeBatchesForTesting());
        assertEquals(0, state.getNativeCompactSelectedProbeKeysForTesting());

        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2", "k3"));
        assertNotNull(task);
        currentKey.set("k1");
        assertEquals(101, state.value());
        currentKey.set("k2");
        assertEquals(101, state.value());
        task.run();

        assertEquals(2, state.getPrefetchLiveReadCancellationsForTesting());
        assertEquals(1, state.getPrefetchWorkerCancelledBeforeReadForTesting());
        assertEquals(1, state.getPrefetchWorkerDiscardedAfterReadForTesting());
        assertEquals(1, state.getStagingSizeForTesting());
        assertEquals(1, state.getNativeCompactSelectedProbeBatchesForTesting());
        assertEquals(3, state.getNativeCompactSelectedProbeKeysForTesting());
        verify(reader, never()).getSerializedValueByRocksDBKey(any());
        verify(reader, never()).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        currentKey.set("k3");
        assertEquals(33, state.value());

        state.close();
        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testNativeMailboxFallsBackBeforeJniBelowConfiguredThreshold() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        when(reader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE)));

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(mailboxOptions(4), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        0,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        1,
                        1 << 20,
                        false,
                        coordinator,
                        18);
        state.setCurrentNamespace("window-mailbox-threshold");

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")).run();

        assertEquals(0, state.getNativeMailboxCompactBatchesForTesting());
        assertEquals(2, state.getNativeMailboxCompactInputKeysForTesting());
        assertEquals(1, state.getNativeMailboxCompactFallbacksForTesting());
        assertEquals(1, state.getNativeMailboxCompactThresholdFallbacksForTesting());
        assertEquals(0, fakePlane.compactCalls);
        verify(reader, times(1)).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testNativePrefetchOffKeepsPreparedMultiGetOnJavaPath() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        when(reader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE)));

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(prefetchOffOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        128,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        1,
                        1 << 20,
                        false,
                        coordinator,
                        18);
        state.setCurrentNamespace("window-java-prefetch");

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")).run();

        assertEquals(0, coordinator.probeCalls());
        assertEquals(0, state.getNativeBatchesActivatedForTesting());
        verify(reader, times(1)).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testProbeFailureDisablesNativeAndFallsBackToAuthoritativeMultiGet() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);

        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        when(reader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(42, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(43, IntSerializer.INSTANCE)));

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        fakePlane.failNextProbe = true;
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(testOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        128,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        8,
                        1 << 20,
                        false,
                        coordinator,
                        9);
        state.setCurrentNamespace("window-fallback");

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")).run();
        currentKey.set("k1");
        assertEquals(42, state.value());

        assertFalse(coordinator.isActive());
        assertEquals(1, state.getNativeRuntimeFailuresForTesting());
        assertEquals(1, state.getNativeFallbackBatchesForTesting());
        assertEquals(0, state.getNativeBatchesActivatedForTesting());
        assertEquals(1, fakePlane.closeCalls);
        verify(reader, times(1)).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());

        state.close();
        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testImmediatePreaggNativePathUsesCapturedNonVoidNamespaceEagerly() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);

        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        when(reader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(17, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(29, IntSerializer.INSTANCE)));

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(testOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        128,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        8,
                        1 << 20,
                        false,
                        coordinator,
                        11);
        state.setCurrentNamespace("session-42");

        state.prefetchForImmediateUse(Arrays.asList("a", "b"));
        currentKey.set("a");
        assertEquals(17, state.value());
        currentKey.set("b");
        assertEquals(29, state.value());

        assertFalse(state.supportsRecordKeyPrefetch());
        assertEquals(1, state.getNativeBatchesActivatedForTesting());
        assertEquals(2, state.getNativeMissesForTesting());
        assertEquals(0, state.getPrefetchLazyValuesMaterializedForTesting());
        verify(reader, times(1)).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        verify(delegate, never()).value();

        state.close();
        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testWriteHeavyExactKeyUpdatesKeepOtherKeyHotAndRejectOldFill() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        TypeSerializer<String> mutationKeySerializer = spy(StringSerializer.INSTANCE);
        TypeSerializer<String> mutationNamespaceSerializer = spy(StringSerializer.INSTANCE);
        TypeSerializer<Integer> mutationValueSerializer = spy(IntSerializer.INSTANCE);
        doReturn(mutationKeySerializer).when(mutationKeySerializer).duplicate();
        doReturn(mutationNamespaceSerializer).when(mutationNamespaceSerializer).duplicate();
        doReturn(mutationValueSerializer).when(mutationValueSerializer).duplicate();
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(mutationKeySerializer);
        when(delegate.getNamespaceSerializer()).thenReturn(mutationNamespaceSerializer);
        when(delegate.getValueSerializer()).thenReturn(mutationValueSerializer);

        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        when(reader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(10, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(20, IntSerializer.INSTANCE)));

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(testOptions(true), fakePlane);
        CachedInternalValueState<String, String, Integer> writer =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        128,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        128,
                        1 << 20,
                        false,
                        coordinator,
                        21);
        writer.setCurrentNamespace("window-write-heavy");
        writer.buildAsyncPrefetchTask(Arrays.asList("a", "b")).run();

        for (int value = 100; value < 132; value++) {
            currentKey.set("a");
            writer.update(value);
            writer.flush();
        }
        assertEquals(32, writer.getNativeGenerationAdvancesForTesting());
        assertEquals(32, writer.getNativeMutationAttemptsForTesting());
        assertEquals(32, writer.getNativeMutationAppliedForTesting());
        assertEquals(0, writer.getNativeMutationFailuresForTesting());
        verify(mutationKeySerializer, times(1)).duplicate();
        verify(mutationNamespaceSerializer, times(1)).duplicate();
        verify(mutationValueSerializer, times(1)).duplicate();

        byte[] preparedA =
                KvStateSerializer.serializeKeyAndNamespace(
                        "a",
                        StringSerializer.INSTANCE,
                        "window-write-heavy",
                        StringSerializer.INSTANCE);
        NativeRequestPlaneCoordinator.BatchSlot oldFill = coordinator.tryAcquireBatchSlot();
        assertNotNull(oldFill);
        oldFill.prepareFill(
                21,
                0,
                java.util.Collections.singletonList(preparedA),
                java.util.Collections.singletonList(
                        KvStateSerializer.serializeValue(10, IntSerializer.INSTANCE)));
        assertEquals(1, coordinator.fill(oldFill));
        assertEquals(
                NativeRequestPlaneBridge.FILL_REJECTED_STALE_GENERATION, oldFill.fillStatus(0));
        oldFill.close();

        CachedInternalValueState<String, String, Integer> readerState =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        128,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        128,
                        1 << 20,
                        false,
                        coordinator,
                        21);
        readerState.setCurrentNamespace("window-write-heavy");
        readerState.buildAsyncPrefetchTask(Arrays.asList("a", "b")).run();
        currentKey.set("a");
        assertEquals(131, readerState.value());
        currentKey.set("b");
        assertEquals(20, readerState.value());

        assertEquals(2, readerState.getNativeHitsForTesting());
        assertEquals(0, readerState.getNativeMissesForTesting());
        assertEquals(2, readerState.getPrefetchAsyncValuesReadForTesting());
        assertEquals(2, readerState.getPrefetchAsyncUsefulValuesForTesting());

        currentKey.set("a");
        writer.clear();
        writer.flush();
        assertEquals(33, writer.getNativeGenerationAdvancesForTesting());
        assertEquals(33, writer.getNativeMutationAttemptsForTesting());
        assertEquals(33, writer.getNativeMutationAppliedForTesting());
        assertEquals(1, writer.getNativeMutationTombstonesAppliedForTesting());

        CachedInternalValueState<String, String, Integer> afterClear =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        128,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        128,
                        1 << 20,
                        false,
                        coordinator,
                        21);
        afterClear.setCurrentNamespace("window-write-heavy");
        afterClear.buildAsyncPrefetchTask(Arrays.asList("a", "b")).run();
        currentKey.set("a");
        assertNull(afterClear.value());
        currentKey.set("b");
        assertEquals(20, afterClear.value());
        assertEquals(1, afterClear.getNativeNegativeHitsForTesting());
        assertEquals(1, afterClear.getNativeHitsForTesting());
        assertEquals(0, afterClear.getNativeMissesForTesting());
        assertEquals(2, afterClear.getPrefetchAsyncValuesReadForTesting());
        assertEquals(1, afterClear.getPrefetchAsyncUsefulValuesForTesting());

        verify(reader, times(1)).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        verify(delegate, never()).value();

        writer.close();
        readerState.close();
        afterClear.close();
        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReadActivatedResidentOnlyWriteThroughSkipsPureWritesThenUpdatesResidentKey()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("write-only");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(7);

        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        stubDirectPreparedSerialization(reader);

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(
                        readActivatedWriteThroughOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        128,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        128,
                        1 << 20,
                        false,
                        coordinator,
                        52);
        state.setCurrentNamespace("read-activated-ns");

        state.update(1);
        state.flush();
        assertEquals(1, state.getNativeMutationAttemptsForTesting());
        assertEquals(1, state.getNativeMutationReadInactiveSkippedForTesting());
        assertEquals(0, state.getNativeMutationAppliedForTesting());
        assertEquals(0, state.getNativeValueReadActivationsForTesting());

        currentKey.set("read-key");
        assertEquals(7, state.value());
        assertEquals(1, state.getNativeValueReadActivationsForTesting());
        assertTrue(coordinator.valueReadActivation(52).isActive());

        currentKey.set("read-key");
        state.update(2);
        state.flush();
        assertEquals(2, state.getNativeMutationAttemptsForTesting());
        assertEquals(1, state.getNativeMutationReadInactiveSkippedForTesting());
        assertEquals(0, state.getNativeMutationResidentMissSkippedForTesting());
        assertEquals(1, state.getNativeMutationAppliedForTesting());

        CachedInternalValueState<String, String, Integer> readerState =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        128,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        128,
                        1 << 20,
                        false,
                        coordinator,
                        52);
        readerState.setCurrentNamespace("read-activated-ns");
        assertEquals(2, readerState.value());
        assertEquals(1, readerState.getNativeHitsForTesting());

        state.close();
        readerState.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSlotExhaustedPointFallbackActivatesBeforeNativeFill() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("hot-key");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(7);

        RocksDBBatchValueReader<String, String, Integer> batchReader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(batchReader.getBatchDefaultValue()).thenReturn(null);
        when(batchReader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        stubDirectPreparedSerialization(batchReader);

        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(
                        readActivatedWriteThroughOptions(), new FakeNativeRequestPlane());
        CachedInternalValueState<String, String, Integer> state =
                newReadActivatedValueState(delegate, currentKey, coordinator, 56);
        state.setCurrentNamespace("read-activated-slot-exhaustion");

        NativeRequestPlaneCoordinator.BatchSlot first = coordinator.tryAcquireBatchSlot();
        NativeRequestPlaneCoordinator.BatchSlot second = coordinator.tryAcquireBatchSlot();
        assertNotNull(first);
        assertNotNull(second);
        assertNull(coordinator.tryAcquireBatchSlot());

        assertEquals(7, state.value());
        assertEquals(1, state.getNativeValueReadActivationsForTesting());
        assertEquals(1, state.getNativeFallbackBatchesForTesting());
        assertEquals(1, state.getNativeFillKeysForTesting());

        state.update(9);
        state.flush();
        assertEquals(1, state.getNativeMutationAppliedForTesting());
        assertEquals(0, state.getNativeMutationReadInactiveSkippedForTesting());

        first.close();
        second.close();
        CachedInternalValueState<String, String, Integer> reader =
                newReadActivatedValueState(delegate, currentKey, coordinator, 56);
        reader.setCurrentNamespace("read-activated-slot-exhaustion");
        assertEquals(9, reader.value());
        assertEquals(1, reader.getNativeHitsForTesting());
        verify(delegate, times(1)).value();

        state.close();
        reader.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPreparedNativeReadActivatesBeforeWorkerExecution() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        stubDirectPreparedSerialization(reader);

        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(
                        readActivatedWriteThroughOptions(), new FakeNativeRequestPlane());
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 53, 8, 1);
        state.setCurrentNamespace("prepared-activation-ns");

        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("a", "b"));
        assertNotNull(task);
        assertTrue(coordinator.valueReadActivation(53).isActive());
        assertEquals(1, state.getNativeValueReadActivationsForTesting());
        task.run();

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReadActivatedWriteThroughPreservesClearBeforeAndAfterActivation()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("victim");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(null);

        RocksDBBatchValueReader<String, String, Integer> batchReader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(batchReader.getBatchDefaultValue()).thenReturn(null);
        when(batchReader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        stubDirectPreparedSerialization(batchReader);

        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(
                        readActivatedWriteThroughOptions(), new FakeNativeRequestPlane());
        CachedInternalValueState<String, String, Integer> writer =
                newReadActivatedValueState(delegate, currentKey, coordinator, 54);
        writer.setCurrentNamespace("read-activated-clear");

        writer.clear();
        writer.flush();
        assertEquals(1, writer.getNativeMutationAttemptsForTesting());
        assertEquals(1, writer.getNativeMutationReadInactiveSkippedForTesting());
        assertEquals(0, writer.getNativeMutationAppliedForTesting());

        CachedInternalValueState<String, String, Integer> firstReader =
                newReadActivatedValueState(delegate, currentKey, coordinator, 54);
        firstReader.setCurrentNamespace("read-activated-clear");
        assertNull(firstReader.value());
        assertEquals(1, firstReader.getNativeMissesForTesting());
        assertEquals(1, firstReader.getNativeValueReadActivationsForTesting());

        writer.update(9);
        writer.flush();
        writer.clear();
        writer.flush();
        assertEquals(3, writer.getNativeMutationAttemptsForTesting());
        assertEquals(1, writer.getNativeMutationReadInactiveSkippedForTesting());
        assertEquals(2, writer.getNativeMutationAppliedForTesting());
        assertEquals(1, writer.getNativeMutationTombstonesAppliedForTesting());

        CachedInternalValueState<String, String, Integer> afterClear =
                newReadActivatedValueState(delegate, currentKey, coordinator, 54);
        afterClear.setCurrentNamespace("read-activated-clear");
        assertNull(afterClear.value());
        assertEquals(1, afterClear.getNativeNegativeHitsForTesting());
        verify(delegate, times(1)).value();

        writer.close();
        firstReader.close();
        afterClear.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReadActivationRejectsDelayedFillAfterMailboxMutation() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("a");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(99);
        RocksDBBatchValueReader<String, String, Integer> batchReader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(batchReader.getBatchDefaultValue()).thenReturn(null);
        when(batchReader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        stubDirectPreparedSerialization(batchReader);

        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(
                        readActivatedWriteThroughOptions(), new FakeNativeRequestPlane());
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 55, 8, 1);
        state.setCurrentNamespace("read-activated-race");

        Runnable delayedTask = state.buildAsyncPrefetchTask(Arrays.asList("a", "b"));
        assertNotNull(delayedTask);
        assertTrue(coordinator.valueReadActivation(55).isActive());

        byte[] preparedA =
                KvStateSerializer.serializeKeyAndNamespace(
                        "a",
                        StringSerializer.INSTANCE,
                        "read-activated-race",
                        StringSerializer.INSTANCE);
        NativeRequestPlaneCoordinator.BatchSlot delayedFill =
                coordinator.tryAcquireBatchSlot();
        assertNotNull(delayedFill);
        delayedFill.prepareFill(
                55,
                0,
                java.util.Collections.singletonList(preparedA),
                java.util.Collections.singletonList(
                        KvStateSerializer.serializeValue(7, IntSerializer.INSTANCE)));

        state.update(99);
        state.flush();
        assertEquals(0, state.getNativeMutationAppliedForTesting());
        assertEquals(1, state.getNativeMutationResidentMissSkippedForTesting());
        assertEquals(1, coordinator.fill(delayedFill));
        assertEquals(
                NativeRequestPlaneBridge.FILL_REJECTED_STALE_GENERATION,
                delayedFill.fillStatus(0));
        delayedFill.close();
        delayedTask.run();

        CachedInternalValueState<String, String, Integer> reader =
                newReadActivatedValueState(delegate, currentKey, coordinator, 55);
        reader.setCurrentNamespace("read-activated-race");
        assertEquals(99, reader.value());
        assertEquals(0, reader.getNativeHitsForTesting());
        assertEquals(1, reader.getNativeMissesForTesting());
        verify(delegate, times(1)).value();

        state.close();
        reader.close();
        coordinator.close();
    }

    @Test
    void testDelayedWorkerFillCannotRacePastPeerMutationAndEviction() throws Exception {
        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane(1);
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(testOptions(), fakePlane);
        byte[] key = new byte[] {1};
        byte[] other = new byte[] {2};

        assertEquals(
                NativeRequestPlaneBridge.FILL_INSERTED,
                coordinator.updateExactKey(37, 10, key, new byte[] {10}));
        assertEquals(
                NativeRequestPlaneBridge.FILL_INSERTED,
                coordinator.updateExactKey(37, 11, other, new byte[] {11}));

        try (NativeRequestPlaneCoordinator.BatchSlot delayed = coordinator.tryAcquireBatchSlot()) {
            assertNotNull(delayed);
            delayed.prepareFill(
                    37,
                    5,
                    java.util.Collections.singletonList(key),
                    java.util.Collections.singletonList(new byte[] {5}));
            assertEquals(1, coordinator.fill(delayed));
            assertEquals(
                    NativeRequestPlaneBridge.FILL_REJECTED_STALE_GENERATION, delayed.fillStatus(0));
            assertEquals(NativeRequestPlaneBridge.ERROR_OK, delayed.fillError(0));
        }

        try (NativeRequestPlaneCoordinator.BatchSlot probe = coordinator.tryAcquireBatchSlot()) {
            assertNotNull(probe);
            probe.prepareLatest(37, 11, java.util.Collections.singletonList(key));
            assertEquals(1, coordinator.probe(probe));
            assertEquals(NativeRequestPlaneBridge.PROBE_MISS, probe.probeStatus(0));
        }
        coordinator.close();
    }

    @Test
    void testResidentOnlyMutationSkipsAbsentSerializationAndUpdatesResidentKey()
            throws Exception {
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(
                        readActivatedWriteThroughOptions(), new FakeNativeRequestPlane());
        byte[] key = new byte[] {7};
        AtomicInteger valueSerializations = new AtomicInteger();

        assertEquals(
                NativeRequestPlaneBridge.FILL_NOT_PRESENT,
                coordinator.updateExactKeyIfPresent(
                        61,
                        1,
                        output -> output.write(key),
                        output -> {
                            valueSerializations.incrementAndGet();
                            output.writeByte(1);
                        }));
        assertEquals(0, valueSerializations.get());

        assertEquals(
                NativeRequestPlaneBridge.FILL_INSERTED,
                coordinator.updateExactKey(61, 2, key, new byte[] {1}));
        assertEquals(
                NativeRequestPlaneBridge.FILL_UPDATED,
                coordinator.updateExactKeyIfPresent(
                        61,
                        3,
                        output -> output.write(key),
                        output -> {
                            valueSerializations.incrementAndGet();
                            output.writeByte(9);
                        }));
        assertEquals(1, valueSerializations.get());

        try (NativeRequestPlaneCoordinator.BatchSlot probe =
                coordinator.tryAcquireBatchSlot()) {
            assertNotNull(probe);
            probe.prepareLatest(61, 3, java.util.Collections.singletonList(key));
            assertEquals(1, coordinator.probe(probe));
            assertEquals(NativeRequestPlaneBridge.PROBE_HIT, probe.probeStatus(0));
            assertArrayEquals(new byte[] {9}, probe.copyProbeValue(0));
        }
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testResidentMutationBatchAdvancesFenceOnlyOnWriteAndFlushesLatestResidentValue()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("resident");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(7);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        stubDirectPreparedSerialization(reader);

        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(
                        residentMutationBatchOptions(), new FakeNativeRequestPlane());
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 63, 8, 1);
        state.setCurrentNamespace("batch-ns");

        assertTrue(
                state.beginNativeResidentMutationBatch(
                        Arrays.asList("resident", "absent")));
        assertEquals(0, state.getNativeWriteEpochForTesting());
        assertEquals(7, state.value());
        state.update(8);
        assertEquals(0, state.getNativeWriteEpochForTesting());
        state.flush();
        assertEquals(1, state.getNativeWriteEpochForTesting());
        state.update(9);
        state.flush();
        state.endNativeResidentMutationBatch();

        byte[] prepared =
                KvStateSerializer.serializeKeyAndNamespace(
                        "resident",
                        StringSerializer.INSTANCE,
                        "batch-ns",
                        StringSerializer.INSTANCE);
        try (NativeRequestPlaneCoordinator.BatchSlot probe =
                coordinator.tryAcquireBatchSlot()) {
            assertNotNull(probe);
            probe.prepareLatest(63, 1L, java.util.Collections.singletonList(prepared));
            assertEquals(1, coordinator.probe(probe));
            assertEquals(NativeRequestPlaneBridge.PROBE_HIT, probe.probeStatus(0));
            org.apache.flink.core.memory.DataInputDeserializer input =
                    new org.apache.flink.core.memory.DataInputDeserializer(
                            probe.copyProbeValue(0));
            assertEquals(9, IntSerializer.INSTANCE.deserialize(input));
        }
        assertEquals(2, state.getNativeMutationAttemptsForTesting());
        assertEquals(1, state.getNativeMutationAppliedForTesting());
        assertEquals(0, state.getNativeMutationResidentMissSkippedForTesting());
        assertEquals(1, state.getNativeMutationBatchScopesForTesting());
        assertEquals(1, state.getNativeMutationBatchFlushesForTesting());
        assertEquals(1, state.getNativeMutationBatchCoalescedForTesting());
        assertEquals(2, state.getNativeMutationResidentHintChecksForTesting());
        assertEquals(2, state.getNativeMutationResidentHintPositivesForTesting());
        assertEquals(0, state.getNativeMutationResidentHintNegativesForTesting());
        assertEquals(0, state.getNativeMutationFenceOnlyFlushesForTesting());
        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testResidentMutationBatchChecksResidencyAtDispatchExitWithoutAValueRead()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("resident");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        stubDirectPreparedSerialization(reader);

        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(
                        residentMutationBatchOptions(), new FakeNativeRequestPlane());
        byte[] residentKey =
                KvStateSerializer.serializeKeyAndNamespace(
                        "resident",
                        StringSerializer.INSTANCE,
                        "batch-ns",
                        StringSerializer.INSTANCE);
        byte[] residentValue = KvStateSerializer.serializeValue(7, IntSerializer.INSTANCE);
        assertEquals(
                NativeRequestPlaneBridge.FILL_INSERTED,
                coordinator.updateExactKey(64, 0L, residentKey, residentValue));

        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 64, 8, 1);
        state.setCurrentNamespace("batch-ns");
        // Activate read-gated mutation publication before the dispatch. The dispatch itself has no
        // ValueState read, matching a key made resident by an earlier read or async prefetch.
        assertEquals(7, state.value());
        assertTrue(
                state.beginNativeResidentMutationBatch(
                        Arrays.asList("resident", "absent")));

        state.update(8);
        state.flush();
        currentKey.set("absent");
        state.update(9);
        state.flush();
        state.endNativeResidentMutationBatch();

        assertEquals(2, state.getNativeMutationAttemptsForTesting());
        assertEquals(0, state.getNativeMutationFailuresForTesting());
        assertEquals(0, state.getNativeMutationSupersededForTesting());
        assertEquals(1, state.getNativeMutationResidentMissSkippedForTesting());
        assertEquals(1, state.getNativeMutationAppliedForTesting());
        assertEquals(1, state.getNativeMutationBatchScopesForTesting());
        assertEquals(1, state.getNativeMutationBatchFlushesForTesting());
        assertEquals(0, state.getNativeMutationBatchCoalescedForTesting());
        assertEquals(2, state.getNativeMutationResidentHintChecksForTesting());
        assertEquals(1, state.getNativeMutationResidentHintPositivesForTesting());
        assertEquals(1, state.getNativeMutationResidentHintNegativesForTesting());
        assertEquals(0, state.getNativeMutationFenceOnlyFlushesForTesting());

        try (NativeRequestPlaneCoordinator.BatchSlot probe =
                coordinator.tryAcquireBatchSlot()) {
            assertNotNull(probe);
            probe.prepareLatest(64, 1L, java.util.Collections.singletonList(residentKey));
            assertEquals(1, coordinator.probe(probe));
            assertEquals(NativeRequestPlaneBridge.PROBE_HIT, probe.probeStatus(0));
            org.apache.flink.core.memory.DataInputDeserializer input =
                    new org.apache.flink.core.memory.DataInputDeserializer(
                            probe.copyProbeValue(0));
            assertEquals(8, IntSerializer.INSTANCE.deserialize(input));
        }
        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testHintNegativeBatchStillAdvancesFenceAgainstOlderFill() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("absent");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        stubDirectPreparedSerialization(reader);

        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(
                        residentMutationBatchOptions(), new FakeNativeRequestPlane());
        coordinator.valueReadActivation(65).activate();
        CachedInternalValueState<String, String, Integer> state =
                newNativePreparedState(delegate, currentKey, coordinator, 65, 8, 1);
        state.setCurrentNamespace("batch-ns");
        byte[] prepared =
                KvStateSerializer.serializeKeyAndNamespace(
                        "absent",
                        StringSerializer.INSTANCE,
                        "batch-ns",
                        StringSerializer.INSTANCE);

        assertTrue(state.beginNativeResidentMutationBatch(java.util.Collections.singleton("absent")));
        state.update(9);
        state.flush();
        // Model an older asynchronous read completing after the authoritative mutation but before
        // dispatch exit. The hint was negative when the mutation was queued.
        assertEquals(
                NativeRequestPlaneBridge.FILL_INSERTED,
                coordinator.updateExactKey(
                        65,
                        0L,
                        prepared,
                        KvStateSerializer.serializeValue(7, IntSerializer.INSTANCE)));
        state.endNativeResidentMutationBatch();

        assertEquals(1, state.getNativeMutationResidentHintChecksForTesting());
        assertEquals(0, state.getNativeMutationResidentHintPositivesForTesting());
        assertEquals(1, state.getNativeMutationResidentHintNegativesForTesting());
        assertEquals(1, state.getNativeMutationFenceOnlyFlushesForTesting());
        assertEquals(1, state.getNativeMutationResidentMissSkippedForTesting());
        assertEquals(0, state.getNativeMutationAppliedForTesting());
        assertEquals(0, state.getNativeMutationFailuresForTesting());

        try (NativeRequestPlaneCoordinator.BatchSlot probe =
                coordinator.tryAcquireBatchSlot()) {
            assertNotNull(probe);
            probe.prepareExact(65, 1L, output -> output.write(prepared));
            assertEquals(1, coordinator.probe(probe));
            assertEquals(NativeRequestPlaneBridge.PROBE_MISS, probe.probeStatus(0));
        }
        state.close();
        coordinator.close();
    }

    @Test
    void testResidentOnlyCheckAndUpdateExcludeConcurrentNativeProbe() throws Exception {
        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(
                        readActivatedWriteThroughOptions(), fakePlane);
        byte[] key = new byte[] {8};
        assertEquals(
                NativeRequestPlaneBridge.FILL_INSERTED,
                coordinator.updateExactKey(62, 1, key, new byte[] {1}));

        CountDownLatch valueWriterEntered = new CountDownLatch(1);
        CountDownLatch releaseValueWriter = new CountDownLatch(1);
        CountDownLatch probeCallStarted = new CountDownLatch(1);
        CountDownLatch probeEntered = new CountDownLatch(1);
        CountDownLatch releaseProbe = new CountDownLatch(1);
        fakePlane.blockNextProbe(probeEntered, releaseProbe);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (NativeRequestPlaneCoordinator.BatchSlot probe =
                coordinator.tryAcquireBatchSlot()) {
            assertNotNull(probe);
            probe.prepareLatest(62, 2, java.util.Collections.singletonList(key));
            Future<Integer> mutation =
                    executor.submit(
                            () ->
                                    coordinator.updateExactKeyIfPresent(
                                            62,
                                            2,
                                            output -> output.write(key),
                                            output -> {
                                                valueWriterEntered.countDown();
                                                try {
                                                    if (!releaseValueWriter.await(
                                                            5, TimeUnit.SECONDS)) {
                                                        throw new IOException(
                                                                "Timed out waiting to release value writer");
                                                    }
                                                } catch (InterruptedException interrupted) {
                                                    Thread.currentThread().interrupt();
                                                    throw new IOException(
                                                            "Interrupted while blocking value writer",
                                                            interrupted);
                                                }
                                                output.writeByte(2);
                                            }));
            assertTrue(valueWriterEntered.await(5, TimeUnit.SECONDS));
            Future<Integer> concurrentProbe =
                    executor.submit(
                            () -> {
                                probeCallStarted.countDown();
                                return coordinator.probe(probe);
                            });
            assertTrue(probeCallStarted.await(5, TimeUnit.SECONDS));
            assertFalse(probeEntered.await(100, TimeUnit.MILLISECONDS));

            releaseValueWriter.countDown();
            assertEquals(NativeRequestPlaneBridge.FILL_UPDATED, mutation.get().intValue());
            assertTrue(probeEntered.await(5, TimeUnit.SECONDS));
            releaseProbe.countDown();
            assertEquals(1, concurrentProbe.get().intValue());
            assertEquals(NativeRequestPlaneBridge.PROBE_HIT, probe.probeStatus(0));
            assertArrayEquals(new byte[] {2}, probe.copyProbeValue(0));
        } finally {
            releaseValueWriter.countDown();
            releaseProbe.countDown();
            executor.shutdownNow();
            coordinator.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMalformedProbeSliceDisablesNativeAndFallsBackBeforePublishing() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);

        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        when(reader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(42, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(43, IntSerializer.INSTANCE)));

        byte[] preparedKey =
                KvStateSerializer.serializeKeyAndNamespace(
                        "k1",
                        StringSerializer.INSTANCE,
                        "window-corrupt",
                        StringSerializer.INSTANCE);
        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        fakePlane.preload(
                13, 0, preparedKey, KvStateSerializer.serializeValue(7, IntSerializer.INSTANCE));
        fakePlane.corruptNextProbeSlice = true;
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(testOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        128,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        8,
                        1 << 20,
                        false,
                        coordinator,
                        13);
        state.setCurrentNamespace("window-corrupt");

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")).run();
        currentKey.set("k1");
        assertEquals(42, state.value());

        assertFalse(coordinator.isActive());
        assertEquals(1, state.getNativeRuntimeFailuresForTesting());
        assertEquals(1, state.getNativeFallbackBatchesForTesting());
        assertEquals(0, state.getNativeBatchesActivatedForTesting());
        assertEquals(1, fakePlane.closeCalls);
        verify(reader, times(1)).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());

        state.close();
        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testInternalFillErrorDisablesNativeButKeepsAuthoritativeRocksDBResult() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);

        RocksDBBatchValueReader<String, String, Integer> reader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(reader.getBatchDefaultValue()).thenReturn(null);
        when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        when(reader.getSerializedValueByRocksDBKey(any()))
                .thenReturn(KvStateSerializer.serializeValue(55, IntSerializer.INSTANCE));

        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        fakePlane.internalErrorNextFill = true;
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(testOptions(), fakePlane);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        128,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        8,
                        1 << 20,
                        false,
                        coordinator,
                        15);
        state.setCurrentNamespace("window-fill-error");

        state.buildAsyncPrefetchTask(Arrays.asList("k1")).run();
        currentKey.set("k1");
        assertEquals(55, state.value());

        assertFalse(coordinator.isActive());
        assertEquals(1, state.getNativeRuntimeFailuresForTesting());
        assertEquals(1, state.getNativeFillRejectedForTesting());
        assertEquals(1, fakePlane.closeCalls);
        verify(reader, times(1)).getSerializedValueByRocksDBKey(any());
        verify(delegate, never()).value();

        state.close();
        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
    }

    private static void stubDirectPreparedSerialization(
            RocksDBBatchValueReader<String, String, Integer> reader) throws Exception {
        when(reader.directArenaMultiGetMaxBatch())
                .thenReturn(RocksDBBatchValueReader.DIRECT_ARENA_DEFAULT_BATCH);
        doAnswer(
                        invocation -> {
                            byte[] serialized =
                                    serializedKey(
                                            invocation.getArgument(0),
                                            invocation.getArgument(1));
                            ((PositionedDataOutputView) invocation.getArgument(4))
                                    .write(serialized);
                            return null;
                        })
                .when(reader)
                .serializeBatchKeyAndNamespace(any(), any(), any(), any(), any());
    }

    private static void stubDirectArenaValues(
            RocksDBBatchValueReader<String, String, Integer> reader, byte[]... serializedValues)
            throws Exception {
        doAnswer(
                        invocation -> {
                            ByteBuffer descriptors =
                                    ((ByteBuffer) invocation.getArgument(1))
                                            .duplicate()
                                            .order(ByteOrder.nativeOrder());
                            ByteBuffer values = invocation.getArgument(3);
                            int count = invocation.getArgument(2);
                            int stride = invocation.getArgument(4);
                            int present = 0;
                            assertEquals(serializedValues.length, count);
                            for (int index = 0; index < count; index++) {
                                byte[] serializedValue = serializedValues[index];
                                int result = RocksDBBatchValueReader.DIRECT_ARENA_NOT_FOUND;
                                if (serializedValue != null) {
                                    values.duplicate().position(index * stride).put(serializedValue);
                                    result = serializedValue.length;
                                    present++;
                                }
                                descriptors.putInt(
                                        index
                                                        * RocksDBBatchValueReader
                                                                .DIRECT_ARENA_DESCRIPTOR_BYTES
                                                + RocksDBBatchValueReader
                                                        .DIRECT_ARENA_RESULT_OFFSET,
                                        result);
                            }
                            return present;
                        })
                .when(reader)
                .getSerializedValuesByRocksDBKeyArena(
                        any(), any(), anyInt(), any(), anyInt());
    }

    private static byte[] serializedKey(String key, String namespace) throws Exception {
        return KvStateSerializer.serializeKeyAndNamespace(
                key, StringSerializer.INSTANCE, namespace, StringSerializer.INSTANCE);
    }

    private static CachedInternalValueState<String, String, Integer> newNativePreparedState(
            InternalValueState<String, String, Integer> delegate,
            AtomicReference<String> currentKey,
            NativeRequestPlaneCoordinator coordinator,
            int stateId,
            int chunkSize,
            int minBatchSize) {
        return newNativePreparedState(
                delegate, currentKey, coordinator, stateId, chunkSize, minBatchSize, false);
    }

    private static CachedInternalValueState<String, String, Integer> newNativePreparedState(
            InternalValueState<String, String, Integer> delegate,
            AtomicReference<String> currentKey,
            NativeRequestPlaneCoordinator coordinator,
            int stateId,
            int chunkSize,
            int minBatchSize,
            boolean keyScopedPrefetchInvalidationEnabled) {
        return new CachedInternalValueState<>(
                delegate,
                currentKey::get,
                currentKey::set,
                0,
                CachePolicyType.LRU,
                0,
                false,
                0.05,
                1000,
                true,
                chunkSize,
                minBatchSize,
                false,
                true,
                Math.max(8, chunkSize),
                1 << 20,
                keyScopedPrefetchInvalidationEnabled,
                coordinator,
                stateId);
    }

    private static CachedInternalValueState<String, String, Integer>
            newNativePreparedCachedState(
                    InternalValueState<String, String, Integer> delegate,
                    AtomicReference<String> currentKey,
                    NativeRequestPlaneCoordinator coordinator,
                    int stateId) {
        return new CachedInternalValueState<>(
                delegate,
                currentKey::get,
                currentKey::set,
                128,
                CachePolicyType.LRU,
                0,
                false,
                0.05,
                1000,
                true,
                8,
                2,
                false,
                true,
                128,
                1 << 20,
                true,
                coordinator,
                stateId);
    }

    private static CachedInternalValueState<String, String, Integer> newReadActivatedValueState(
            InternalValueState<String, String, Integer> delegate,
            AtomicReference<String> currentKey,
            NativeRequestPlaneCoordinator coordinator,
            int stateId) {
        return new CachedInternalValueState<>(
                delegate,
                currentKey::get,
                currentKey::set,
                128,
                CachePolicyType.LRU,
                0,
                false,
                0.05,
                1000,
                true,
                8,
                2,
                false,
                true,
                128,
                1 << 20,
                false,
                coordinator,
                stateId);
    }

    private static NativeRequestPlaneOptions testOptions() {
        return testOptions(false);
    }

    private static NativeRequestPlaneOptions testOptions(boolean writeThroughMutations) {
        return new NativeRequestPlaneOptions(
                true,
                "",
                "auto",
                128,
                4096,
                4096,
                16,
                4096,
                4096,
                1,
                2,
                false,
                writeThroughMutations);
    }

    private static NativeRequestPlaneOptions valueCacheOptions(boolean valueCacheEnabled) {
        return new NativeRequestPlaneOptions(
                true,
                "",
                "auto",
                128,
                4096,
                4096,
                16,
                4096,
                4096,
                1,
                2,
                false,
                false,
                valueCacheEnabled,
                false,
                false,
                false,
                false,
                false);
    }

    private static NativeRequestPlaneOptions readActivatedWriteThroughOptions() {
        return new NativeRequestPlaneOptions(
                true,
                "",
                "auto",
                128,
                4096,
                4096,
                16,
                4096,
                4096,
                1,
                2,
                false,
                true,
                true,
                false,
                false,
                true,
                false,
                false,
                false,
                false,
                false,
                8192,
                0.02,
                262144,
                false,
                true);
    }

    private static NativeRequestPlaneOptions residentMutationBatchOptions() {
        return new NativeRequestPlaneOptions(
                true,
                "",
                "auto",
                128,
                4096,
                4096,
                16,
                4096,
                4096,
                1,
                2,
                false,
                true,
                true,
                false,
                false,
                true,
                false,
                false,
                false,
                false,
                false,
                8192,
                0.02,
                262144,
                false,
                true,
                true);
    }

    private static NativeRequestPlaneOptions mailboxOptions() {
        return mailboxOptions(1);
    }

    private static NativeRequestPlaneOptions mailboxOptions(int minBatchSize) {
        return new NativeRequestPlaneOptions(
                true,
                "",
                "auto",
                128,
                4096,
                4096,
                16,
                4096,
                4096,
                minBatchSize,
                2,
                false,
                false,
                false,
                false,
                true);
    }

    private static NativeRequestPlaneOptions mailboxScratchOptions() {
        return new NativeRequestPlaneOptions(
                true,
                "",
                "auto",
                128,
                4096,
                4096,
                16,
                4096,
                4096,
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
                false,
                false,
                false,
                8192,
                0.02,
                262144,
                false,
                false,
                false,
                false,
                false,
                false,
                true);
    }

    private static NativeRequestPlaneOptions mailboxLargeScratchOptions() {
        return new NativeRequestPlaneOptions(
                true,
                "",
                "auto",
                128,
                4096,
                4096,
                16,
                4096,
                4096,
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
                false,
                false,
                false,
                8192,
                0.02,
                262144,
                false,
                false,
                false,
                false,
                false,
                false,
                true,
                64,
                65536);
    }

    private static NativeRequestPlaneOptions compactSelectedOptions() {
        return new NativeRequestPlaneOptions(
                true,
                "",
                "auto",
                128,
                4096,
                4096,
                16,
                4096,
                4096,
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
                true);
    }

    private static NativeRequestPlaneOptions deferredReservationOptions() {
        return new NativeRequestPlaneOptions(
                true,
                "",
                "auto",
                128,
                4096,
                4096,
                16,
                4096,
                4096,
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
                false,
                false,
                8192,
                0.02,
                262144,
                false,
                false,
                false,
                false,
                false,
                true);
    }

    private static NativeRequestPlaneOptions directArenaOptions() {
        return directArenaOptions(16);
    }

    private static NativeRequestPlaneOptions directArenaOptions(int batchEntries) {
        return new NativeRequestPlaneOptions(
                true,
                "",
                "auto",
                128,
                1 << 20,
                1 << 20,
                batchEntries,
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

    private static NativeRequestPlaneOptions directArenaReadOnlyOptions() {
        return directArenaReadOnlyOptions(16);
    }

    private static NativeRequestPlaneOptions directArenaReadOnlyOptions(int batchEntries) {
        return directArenaReadOnlyOptions(batchEntries, false);
    }

    private static NativeRequestPlaneOptions directArenaReadOnlyOptions(
            int batchEntries, boolean negativeHandoffEnabled) {
        return directArenaReadOnlyOptions(batchEntries, negativeHandoffEnabled, false);
    }

    private static NativeRequestPlaneOptions directArenaReadOnlyOptions(
            int batchEntries,
            boolean negativeHandoffEnabled,
            boolean eagerMaterializationEnabled) {
        return new NativeRequestPlaneOptions(
                true,
                "",
                "auto",
                128,
                1 << 20,
                1 << 20,
                batchEntries,
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
                262144,
                false,
                false,
                false,
                true,
                negativeHandoffEnabled,
                false,
                false,
                batchEntries,
                1 << 20,
                eagerMaterializationEnabled);
    }

    private static NativeRequestPlaneOptions prefetchOffOptions() {
        return new NativeRequestPlaneOptions(
                true, "", "auto", 128, 4096, 4096, 16, 4096, 4096, 1, 2, false, false, false, false,
                false, false);
    }

    private static final class FakeNativeRequestPlane implements NativeRequestPlane {

        private final Map<NativeKey, StoredValue> values = new LinkedHashMap<>();
        private final Map<Integer, Long> stateGenerationWatermarks = new LinkedHashMap<>();
        private final int maxEntries;
        private boolean failNextProbe;
        private boolean corruptNextProbeSlice;
        private boolean internalErrorNextFill;
        private CountDownLatch probeEntered;
        private CountDownLatch releaseProbe;
        private int compactCalls;
        private int closeCalls;

        private FakeNativeRequestPlane() {
            this(Integer.MAX_VALUE);
        }

        private FakeNativeRequestPlane(int maxEntries) {
            this.maxEntries = maxEntries;
        }

        @Override
        public int fillBatch(
                SerializedKeyBatch<?, ?> keys,
                ByteBuffer valueArena,
                ByteBuffer valueMetadata,
                ByteBuffer fillResults) {
            ByteBuffer metadata = valueMetadata.duplicate().order(ByteOrder.nativeOrder());
            ByteBuffer results = fillResults.duplicate().order(ByteOrder.nativeOrder());
            for (int i = 0; i < keys.entryCount(); i++) {
                int base = i * NativeRequestPlaneBridge.FILL_VALUE_RECORD_BYTES;
                int offset =
                        metadata.getInt(base + NativeRequestPlaneBridge.FILL_VALUE_ARENA_OFFSET);
                int length =
                        metadata.getInt(base + NativeRequestPlaneBridge.FILL_VALUE_LENGTH_OFFSET);
                int flags =
                        metadata.getInt(base + NativeRequestPlaneBridge.FILL_VALUE_FLAGS_OFFSET);
                int controlFlags =
                        metadata.getInt(base + NativeRequestPlaneBridge.FILL_VALUE_RESERVED_OFFSET);
                boolean negative = (flags & NativeRequestPlaneBridge.FILL_VALUE_NEGATIVE_FLAG) != 0;
                boolean updateOnly =
                        (controlFlags & NativeRequestPlaneBridge.FILL_VALUE_UPDATE_ONLY_FLAG) != 0;
                boolean checkOnly =
                        (controlFlags & NativeRequestPlaneBridge.FILL_VALUE_CHECK_ONLY_FLAG) != 0;
                byte[] value = null;
                if (!negative) {
                    value = new byte[length];
                    ByteBuffer source = valueArena.duplicate();
                    source.position(offset);
                    source.get(value);
                }
                int resultBase = i * NativeRequestPlaneBridge.FILL_RESULT_RECORD_BYTES;
                if (internalErrorNextFill) {
                    results.putInt(
                            resultBase + NativeRequestPlaneBridge.FILL_RESULT_STATUS_OFFSET,
                            NativeRequestPlaneBridge.FILL_INTERNAL_ERROR);
                    results.putInt(
                            resultBase + NativeRequestPlaneBridge.FILL_RESULT_ERROR_OFFSET,
                            NativeRequestPlaneBridge.ERROR_INTERNAL);
                } else {
                    NativeKey key = nativeKey(keys, i);
                    StoredValue existing = values.get(key);
                    Long stateWatermark = stateGenerationWatermarks.get(key.stateId);
                    if (key.generation == NativeRequestPlaneBridge.PROBE_LATEST_GENERATION) {
                        results.putInt(
                                resultBase + NativeRequestPlaneBridge.FILL_RESULT_STATUS_OFFSET,
                                NativeRequestPlaneBridge.FILL_INVALID_ARGUMENT);
                        results.putInt(
                                resultBase + NativeRequestPlaneBridge.FILL_RESULT_ERROR_OFFSET,
                                NativeRequestPlaneBridge.ERROR_INVALID_ARGUMENT);
                    } else if (stateWatermark != null && key.generation < stateWatermark) {
                        results.putInt(
                                resultBase + NativeRequestPlaneBridge.FILL_RESULT_STATUS_OFFSET,
                                NativeRequestPlaneBridge.FILL_REJECTED_STALE_GENERATION);
                        results.putInt(
                                resultBase + NativeRequestPlaneBridge.FILL_RESULT_ERROR_OFFSET,
                                NativeRequestPlaneBridge.ERROR_OK);
                    } else {
                        if (stateWatermark == null || key.generation > stateWatermark) {
                            stateGenerationWatermarks.put(key.stateId, key.generation);
                        }
                        int status;
                        if (checkOnly) {
                            status =
                                    existing == null
                                            ? NativeRequestPlaneBridge.FILL_NOT_PRESENT
                                            : NativeRequestPlaneBridge.FILL_UPDATED;
                        } else if (updateOnly && existing == null) {
                            status = NativeRequestPlaneBridge.FILL_NOT_PRESENT;
                        } else {
                            if (existing == null && values.size() >= maxEntries) {
                                NativeKey oldest = values.keySet().iterator().next();
                                values.remove(oldest);
                            }
                            values.put(key, new StoredValue(key.generation, negative, value));
                            status =
                                    existing == null
                                            ? NativeRequestPlaneBridge.FILL_INSERTED
                                            : NativeRequestPlaneBridge.FILL_UPDATED;
                        }
                        results.putInt(
                                resultBase + NativeRequestPlaneBridge.FILL_RESULT_STATUS_OFFSET,
                                status);
                        results.putInt(
                                resultBase + NativeRequestPlaneBridge.FILL_RESULT_ERROR_OFFSET,
                                NativeRequestPlaneBridge.ERROR_OK);
                    }
                }
            }
            internalErrorNextFill = false;
            return keys.entryCount();
        }

        @Override
        public int probeBatch(
                SerializedKeyBatch<?, ?> keys, ByteBuffer valueOutput, ByteBuffer probeResults) {
            CountDownLatch entered = probeEntered;
            CountDownLatch release = releaseProbe;
            probeEntered = null;
            releaseProbe = null;
            if (entered != null) {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Timed out waiting to release native probe");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while blocking native probe", interrupted);
                }
            }
            if (failNextProbe) {
                failNextProbe = false;
                throw new IllegalStateException("injected probe failure");
            }
            ByteBuffer results = probeResults.duplicate().order(ByteOrder.nativeOrder());
            int valueOffset = 0;
            for (int i = 0; i < keys.entryCount(); i++) {
                NativeKey requestedKey = nativeKey(keys, i);
                StoredValue stored = values.get(requestedKey);
                if (stored != null
                        && requestedKey.generation
                                != NativeRequestPlaneBridge.PROBE_LATEST_GENERATION
                        && requestedKey.generation != stored.generation) {
                    stored = null;
                }
                int status =
                        stored == null
                                ? NativeRequestPlaneBridge.PROBE_MISS
                                : stored.negative
                                        ? NativeRequestPlaneBridge.PROBE_NEGATIVE
                                        : NativeRequestPlaneBridge.PROBE_HIT;
                int length = stored == null || stored.value == null ? 0 : stored.value.length;
                if (length > 0) {
                    ByteBuffer output = valueOutput.duplicate();
                    output.position(valueOffset);
                    output.put(stored.value);
                }
                int base = i * NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES;
                results.putInt(base + NativeRequestPlaneBridge.PROBE_RESULT_STATUS_OFFSET, status);
                results.putInt(base + NativeRequestPlaneBridge.PROBE_RESULT_ERROR_OFFSET, 0);
                results.putInt(
                        base + NativeRequestPlaneBridge.PROBE_RESULT_ARENA_OFFSET,
                        corruptNextProbeSlice && status == NativeRequestPlaneBridge.PROBE_HIT
                                ? valueOutput.capacity()
                                : length == 0 ? 0 : valueOffset);
                results.putInt(base + NativeRequestPlaneBridge.PROBE_RESULT_LENGTH_OFFSET, length);
                if (status == NativeRequestPlaneBridge.PROBE_HIT) {
                    corruptNextProbeSlice = false;
                }
                valueOffset += length;
            }
            return keys.entryCount();
        }

        @Override
        public int compactBatch(SerializedKeyBatch<?, ?> keys, ByteBuffer uniqueSourceIndexes) {
            compactCalls++;
            ByteBuffer indexes = uniqueSourceIndexes.duplicate().order(ByteOrder.nativeOrder());
            int written = 0;
            for (int candidate = 0; candidate < keys.entryCount(); candidate++) {
                NativeKey candidateKey = nativeKey(keys, candidate);
                boolean duplicate = false;
                for (int unique = 0; unique < written; unique++) {
                    if (candidateKey.equals(
                            nativeKey(keys, indexes.getInt(unique * Integer.BYTES)))) {
                        duplicate = true;
                        break;
                    }
                }
                if (!duplicate) {
                    indexes.putInt(written * Integer.BYTES, candidate);
                    written++;
                }
            }
            return written;
        }

        @Override
        public String selectedKernel() {
            return "fake-sve256";
        }

        @Override
        public long detectedFeatureBits() {
            return NativeRequestPlaneBridge.FEATURE_SVE
                    | NativeRequestPlaneBridge.FEATURE_SVE_VL256;
        }

        @Override
        public void close() {
            closeCalls++;
        }

        private void preload(int stateId, long generation, byte[] key, byte[] value) {
            stateGenerationWatermarks.merge(stateId, generation, Math::max);
            if (values.size() >= maxEntries) {
                NativeKey oldest = values.keySet().iterator().next();
                values.remove(oldest);
            }
            values.put(
                    new NativeKey(stateId, generation, Arrays.copyOf(key, key.length)),
                    new StoredValue(generation, false, Arrays.copyOf(value, value.length)));
        }

        private void blockNextProbe(CountDownLatch entered, CountDownLatch release) {
            this.probeEntered = entered;
            this.releaseProbe = release;
        }

        private void preloadNegative(int stateId, long generation, byte[] key) {
            stateGenerationWatermarks.merge(stateId, generation, Math::max);
            values.put(
                    new NativeKey(stateId, generation, Arrays.copyOf(key, key.length)),
                    new StoredValue(generation, true, null));
        }

        private static NativeKey nativeKey(SerializedKeyBatch<?, ?> batch, int index) {
            int offset = batch.arenaOffset(index);
            int length = batch.serializedLength(index);
            byte[] bytes = new byte[length];
            ByteBuffer arena = batch.arenaSlice();
            arena.position(offset);
            arena.get(bytes);
            return new NativeKey(batch.stateId(index), batch.generation(index), bytes);
        }
    }

    private static final class StoredValue {
        private final long generation;
        private final boolean negative;
        private final byte[] value;

        private StoredValue(long generation, boolean negative, byte[] value) {
            this.generation = generation;
            this.negative = negative;
            this.value = value;
        }
    }

    private static final class NativeKey {
        private final int stateId;
        private final long generation;
        private final byte[] bytes;

        private NativeKey(int stateId, long generation, byte[] bytes) {
            this.stateId = stateId;
            this.generation = generation;
            this.bytes = bytes;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof NativeKey)) {
                return false;
            }
            NativeKey that = (NativeKey) other;
            return stateId == that.stateId && Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stateId, Arrays.hashCode(bytes));
        }
    }
}
