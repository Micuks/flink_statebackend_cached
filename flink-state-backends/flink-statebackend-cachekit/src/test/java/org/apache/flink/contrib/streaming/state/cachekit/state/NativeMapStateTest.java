/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of the
 * License at
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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PresenceCacheImplementation;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlane;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneBridge;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneCoordinator;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneOptions;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.SerializedKeyBatch;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NativeMapStateTest {

    @Test
    @SuppressWarnings("unchecked")
    void testPointMissFillsNativeAndSecondReadUsesDirectArena() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer())
                .thenReturn(new MapSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE));
        when(delegate.get("uk1")).thenReturn(7);

        OneEntryPlane plane = new OneEntryPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createState(delegate, currentKey, coordinator);

        assertEquals(7, state.get("uk1"));
        assertEquals(7, state.get("uk1"));

        verify(delegate, times(1)).get("uk1");
        assertEquals(2, state.getNativeProbeAttemptsForTesting());
        assertEquals(1, state.getNativeMissesForTesting());
        assertEquals(1, state.getNativeFillsForTesting());
        assertEquals(1, state.getNativeHitsForTesting());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testNegativeEntryAvoidsSecondDelegateRead() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer())
                .thenReturn(new MapSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE));
        when(delegate.get("missing")).thenReturn(null);

        OneEntryPlane plane = new OneEntryPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createState(delegate, currentKey, coordinator);

        assertNull(state.get("missing"));
        assertNull(state.get("missing"));

        verify(delegate, times(1)).get("missing");
        assertEquals(1, state.getNativeNegativeHitsForTesting());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMutationGenerationInvalidatesOlderNativeEntry() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer())
                .thenReturn(new MapSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE));
        when(delegate.get("uk1")).thenReturn(7);

        OneEntryPlane plane = new OneEntryPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createState(delegate, currentKey, coordinator);

        assertEquals(7, state.get("uk1"));
        state.put("other", 9);
        assertEquals(7, state.get("uk1"));

        verify(delegate, times(2)).get("uk1");
        assertEquals(2, state.getNativeMissesForTesting());
        assertEquals(0, state.getNativeHitsForTesting());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testEmptySnapshotBackfillShortCircuitsSecondIteration() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
        configureSerializers(delegate);
        when(delegate.entries()).thenReturn(Collections.emptyList());

        OneEntryPlane plane = new OneEntryPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(snapshotOptions(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createSnapshotState(delegate, currentKey, coordinator);

        assertEquals(0, consume(state.entries()));
        assertEquals(0, consume(state.entries()));

        verify(delegate, times(1)).entries();
        assertEquals(1, state.getNativeSnapshotFillsForTesting());
        assertEquals(1, state.getNativeSnapshotNegativeHitsForTesting());

        state.close();
        coordinator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSingleSnapshotBackfillRestoresUserKeyFromDirectArena() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalMapState<String, VoidNamespace, String, Integer> delegate =
                mock(InternalMapState.class);
        configureSerializers(delegate);
        when(delegate.entries())
                .thenReturn(
                        Collections.singletonList(
                                new AbstractMap.SimpleImmutableEntry<>("uk1", 7)));
        when(delegate.get("uk1")).thenReturn(7);

        OneEntryPlane plane = new OneEntryPlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(snapshotOptions(), plane);
        CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                createSnapshotState(delegate, currentKey, coordinator);

        assertEquals(1, consume(state.entries()));
        assertEquals(1, consume(state.entries()));

        verify(delegate, times(1)).entries();
        verify(delegate, times(1)).get("uk1");
        assertEquals(1, state.getNativeSnapshotFillsForTesting());
        assertEquals(1, state.getNativeSnapshotHitsForTesting());

        state.close();
        coordinator.close();
    }

    private static void configureSerializers(
            InternalMapState<String, VoidNamespace, String, Integer> delegate) {
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer())
                .thenReturn(new MapSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE));
    }

    private static int consume(Iterable<Map.Entry<String, Integer>> entries) {
        int count = 0;
        for (Map.Entry<String, Integer> ignored : entries) {
            count++;
        }
        return count;
    }

    private static CachedInternalMapState<String, VoidNamespace, String, Integer> createState(
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
                        true,
                        0,
                        false);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        return state;
    }

    private static CachedInternalMapState<String, VoidNamespace, String, Integer>
            createSnapshotState(
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
                        0,
                        false,
                        41,
                        true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        return state;
    }

    private static NativeRequestPlaneOptions options() {
        return new NativeRequestPlaneOptions(
                true, "", "auto", 16, 4096, 4096, 4, 4096, 4096, 1, 1, false, false, true);
    }

    private static NativeRequestPlaneOptions snapshotOptions() {
        return new NativeRequestPlaneOptions(
                true,
                "",
                "auto",
                16,
                4096,
                4096,
                4,
                4096,
                4096,
                1,
                1,
                false,
                false,
                false,
                true);
    }

    /** Minimal deterministic native-plane model for one exact key/value. */
    private static final class OneEntryPlane implements NativeRequestPlane {
        private byte[] value;
        private boolean populated;
        private boolean negative;
        private long generation;

        @Override
        public int fillBatch(
                SerializedKeyBatch<?, ?> keys,
                ByteBuffer valueArena,
                ByteBuffer valueMetadata,
                ByteBuffer fillResults) {
            ByteBuffer metadata = valueMetadata.duplicate().order(ByteOrder.nativeOrder());
            int flags =
                    metadata.getInt(NativeRequestPlaneBridge.FILL_VALUE_FLAGS_OFFSET);
            negative = (flags & NativeRequestPlaneBridge.FILL_VALUE_NEGATIVE_FLAG) != 0;
            int offset =
                    metadata.getInt(NativeRequestPlaneBridge.FILL_VALUE_ARENA_OFFSET);
            int length = metadata.getInt(NativeRequestPlaneBridge.FILL_VALUE_LENGTH_OFFSET);
            value = null;
            if (!negative) {
                value = new byte[length];
                ByteBuffer source = valueArena.duplicate();
                source.position(offset);
                source.get(value);
            }
            populated = true;
            generation = keys.generation(0);
            ByteBuffer results = fillResults.duplicate().order(ByteOrder.nativeOrder());
            results.putInt(
                    NativeRequestPlaneBridge.FILL_RESULT_STATUS_OFFSET,
                    NativeRequestPlaneBridge.FILL_INSERTED);
            results.putInt(
                    NativeRequestPlaneBridge.FILL_RESULT_ERROR_OFFSET,
                    NativeRequestPlaneBridge.ERROR_OK);
            return keys.entryCount();
        }

        @Override
        public int probeBatch(
                SerializedKeyBatch<?, ?> keys,
                ByteBuffer valueOutput,
                ByteBuffer probeResults) {
            int status =
                    !populated || keys.generation(0) != generation
                            ? NativeRequestPlaneBridge.PROBE_MISS
                            : negative
                                    ? NativeRequestPlaneBridge.PROBE_NEGATIVE
                                    : NativeRequestPlaneBridge.PROBE_HIT;
            int length = value == null ? 0 : value.length;
            if (length > 0) {
                valueOutput.duplicate().put(value);
            }
            ByteBuffer results = probeResults.duplicate().order(ByteOrder.nativeOrder());
            results.putInt(NativeRequestPlaneBridge.PROBE_RESULT_STATUS_OFFSET, status);
            results.putInt(
                    NativeRequestPlaneBridge.PROBE_RESULT_ERROR_OFFSET,
                    NativeRequestPlaneBridge.ERROR_OK);
            results.putInt(NativeRequestPlaneBridge.PROBE_RESULT_ARENA_OFFSET, 0);
            results.putInt(NativeRequestPlaneBridge.PROBE_RESULT_LENGTH_OFFSET, length);
            return keys.entryCount();
        }

        @Override
        public String selectedKernel() {
            return "test";
        }

        @Override
        public long detectedFeatureBits() {
            return 0;
        }

        @Override
        public void close() {}
    }
}
