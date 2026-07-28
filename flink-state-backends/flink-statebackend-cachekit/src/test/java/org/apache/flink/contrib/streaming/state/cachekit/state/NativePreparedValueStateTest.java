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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBBatchValueReader;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlane;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneBridge;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneCoordinator;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneOptions;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.SerializedKeyBatch;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class NativePreparedValueStateTest {

    @Test
    @SuppressWarnings("unchecked")
    void testProbeCompactsMissesAndReusesPositiveAndNegativeHitsWithNamespace()
            throws Exception {
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
        verify(delegate, never()).value();
        assertEquals(3, state.getNativeBatchesActivatedForTesting());
        assertEquals(6, state.getNativeProbeKeysForTesting());
        assertEquals(2, state.getNativeHitsForTesting());
        assertEquals(1, state.getNativeNegativeHitsForTesting());
        assertEquals(3, state.getNativeMissesForTesting());
        assertEquals(1, state.getNativeFillBatchesForTesting());
        assertEquals(3, state.getNativeFillKeysForTesting());
        assertEquals(3, state.getPrefetchLazyValuesMaterializedForTesting());
        assertFalse(state.supportsRecordKeyPrefetch());

        state.close();
        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
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
        when(reader.getSerializedValueByRocksDBKey(any()))
                .thenReturn(KvStateSerializer.serializeValue(42, IntSerializer.INSTANCE));

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
                        coordinator,
                        9);
        state.setCurrentNamespace("window-fallback");

        state.buildAsyncPrefetchTask(Arrays.asList("k1")).run();
        currentKey.set("k1");
        assertEquals(42, state.value());

        assertFalse(coordinator.isActive());
        assertEquals(1, state.getNativeRuntimeFailuresForTesting());
        assertEquals(1, state.getNativeFallbackBatchesForTesting());
        assertEquals(0, state.getNativeBatchesActivatedForTesting());
        assertEquals(1, fakePlane.closeCalls);
        verify(reader, times(1)).getSerializedValueByRocksDBKey(any());

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
                NativeRequestPlaneCoordinator.forTesting(testOptions(), fakePlane);
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
        NativeRequestPlaneCoordinator.BatchSlot oldFill =
                coordinator.tryAcquireBatchSlot();
        assertNotNull(oldFill);
        oldFill.prepareFill(
                21,
                0,
                java.util.Collections.singletonList(preparedA),
                java.util.Collections.singletonList(
                        KvStateSerializer.serializeValue(10, IntSerializer.INSTANCE)));
        assertEquals(1, coordinator.fill(oldFill));
        assertEquals(
                NativeRequestPlaneBridge.FILL_REJECTED_STALE_GENERATION,
                oldFill.fillStatus(0));
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
        when(reader.getSerializedValueByRocksDBKey(any()))
                .thenReturn(KvStateSerializer.serializeValue(42, IntSerializer.INSTANCE));

        byte[] preparedKey =
                KvStateSerializer.serializeKeyAndNamespace(
                        "k1",
                        StringSerializer.INSTANCE,
                        "window-corrupt",
                        StringSerializer.INSTANCE);
        FakeNativeRequestPlane fakePlane = new FakeNativeRequestPlane();
        fakePlane.preload(
                13,
                0,
                preparedKey,
                KvStateSerializer.serializeValue(7, IntSerializer.INSTANCE));
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
                        coordinator,
                        13);
        state.setCurrentNamespace("window-corrupt");

        state.buildAsyncPrefetchTask(Arrays.asList("k1")).run();
        currentKey.set("k1");
        assertEquals(42, state.value());

        assertFalse(coordinator.isActive());
        assertEquals(1, state.getNativeRuntimeFailuresForTesting());
        assertEquals(1, state.getNativeFallbackBatchesForTesting());
        assertEquals(0, state.getNativeBatchesActivatedForTesting());
        assertEquals(1, fakePlane.closeCalls);
        verify(reader, times(1)).getSerializedValueByRocksDBKey(any());

        state.close();
        coordinator.close();
        assertEquals(1, fakePlane.closeCalls);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testInternalFillErrorDisablesNativeButKeepsAuthoritativeRocksDBResult()
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

    private static NativeRequestPlaneOptions testOptions() {
        return new NativeRequestPlaneOptions(
                true, "", "auto", 128, 4096, 4096, 16, 4096, 4096, 1, 2);
    }

    private static final class FakeNativeRequestPlane implements NativeRequestPlane {

        private final Map<NativeKey, StoredValue> values = new HashMap<>();
        private boolean failNextProbe;
        private boolean corruptNextProbeSlice;
        private boolean internalErrorNextFill;
        private int closeCalls;

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
                        metadata.getInt(
                                base + NativeRequestPlaneBridge.FILL_VALUE_ARENA_OFFSET);
                int length =
                        metadata.getInt(base + NativeRequestPlaneBridge.FILL_VALUE_LENGTH_OFFSET);
                int flags =
                        metadata.getInt(base + NativeRequestPlaneBridge.FILL_VALUE_FLAGS_OFFSET);
                boolean negative =
                        (flags & NativeRequestPlaneBridge.FILL_VALUE_NEGATIVE_FLAG) != 0;
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
                    if (key.generation == NativeRequestPlaneBridge.PROBE_LATEST_GENERATION) {
                        results.putInt(
                                resultBase + NativeRequestPlaneBridge.FILL_RESULT_STATUS_OFFSET,
                                NativeRequestPlaneBridge.FILL_INVALID_ARGUMENT);
                        results.putInt(
                                resultBase + NativeRequestPlaneBridge.FILL_RESULT_ERROR_OFFSET,
                                NativeRequestPlaneBridge.ERROR_INVALID_ARGUMENT);
                    } else if (existing != null && key.generation < existing.generation) {
                        results.putInt(
                                resultBase + NativeRequestPlaneBridge.FILL_RESULT_STATUS_OFFSET,
                                NativeRequestPlaneBridge.FILL_REJECTED_STALE_GENERATION);
                        results.putInt(
                                resultBase + NativeRequestPlaneBridge.FILL_RESULT_ERROR_OFFSET,
                                NativeRequestPlaneBridge.ERROR_OK);
                    } else {
                        values.put(
                                key,
                                new StoredValue(key.generation, negative, value));
                        results.putInt(
                                resultBase + NativeRequestPlaneBridge.FILL_RESULT_STATUS_OFFSET,
                                existing == null
                                        ? NativeRequestPlaneBridge.FILL_INSERTED
                                        : NativeRequestPlaneBridge.FILL_UPDATED);
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
                SerializedKeyBatch<?, ?> keys,
                ByteBuffer valueOutput,
                ByteBuffer probeResults) {
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
                results.putInt(
                        base + NativeRequestPlaneBridge.PROBE_RESULT_STATUS_OFFSET, status);
                results.putInt(base + NativeRequestPlaneBridge.PROBE_RESULT_ERROR_OFFSET, 0);
                results.putInt(
                        base + NativeRequestPlaneBridge.PROBE_RESULT_ARENA_OFFSET,
                        corruptNextProbeSlice && status == NativeRequestPlaneBridge.PROBE_HIT
                                ? valueOutput.capacity()
                                : length == 0 ? 0 : valueOffset);
                results.putInt(
                        base + NativeRequestPlaneBridge.PROBE_RESULT_LENGTH_OFFSET, length);
                if (status == NativeRequestPlaneBridge.PROBE_HIT) {
                    corruptNextProbeSlice = false;
                }
                valueOffset += length;
            }
            return keys.entryCount();
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
            values.put(
                    new NativeKey(stateId, generation, Arrays.copyOf(key, key.length)),
                    new StoredValue(
                            generation, false, Arrays.copyOf(value, value.length)));
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
            return stateId == that.stateId
                    && Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stateId, Arrays.hashCode(bytes));
        }
    }
}
