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

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBDirectValueAccess;
import org.apache.flink.contrib.streaming.state.cachekit.PrefetchExecutor;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalValueState;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DirectStateTransitFailurePathTest {

    @Test
    void directMissIsCountedAndFallsBackToAuthoritativeValue() throws Exception {
        DirectValueState delegate = new DirectValueState();
        delegate.authoritativeValue = 41;
        delegate.batchRead = DirectStateTransitFailurePathTest::publishMisses;
        AtomicReference<String> currentKey = new AtomicReference<>("a");
        MetricProbe metrics = new MetricProbe();
        CachedInternalValueState<String, VoidNamespace, Integer> state =
                directState(delegate, currentKey, metrics);

        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("a"));
        assertNotNull(task);
        task.run();

        assertEquals(1, delegate.directReadCalls.get());
        assertEquals(1L, metrics.value("dstl_single_jni_calls"));
        assertEquals(1L, metrics.value("dstl_misses"));
        assertEquals(41, state.value());
        assertEquals(1, delegate.authoritativeReadCalls.get());
    }

    @Test
    void valueOverflowDropsWholeBatchAndFallsBack() throws Exception {
        DirectValueState delegate = new DirectValueState();
        delegate.authoritativeValue = 42;
        delegate.batchRead = DirectStateTransitFailurePathTest::publishOverflow;
        AtomicReference<String> currentKey = new AtomicReference<>("a");
        MetricProbe metrics = new MetricProbe();
        CachedInternalValueState<String, VoidNamespace, Integer> state =
                directState(delegate, currentKey, metrics);

        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("a"));
        assertNotNull(task);
        task.run();

        assertEquals(1L, metrics.value("dstl_overflow_fallbacks"));
        assertEquals(0L, metrics.value("dstl_values_materialized"));
        assertEquals(42, state.value());
        assertEquals(1, delegate.authoritativeReadCalls.get());
    }

    @Test
    void nativeIOExceptionIsCountedFallsBackAndReleasesEnvelope() throws Exception {
        DirectValueState delegate = new DirectValueState();
        delegate.authoritativeValue = 43;
        delegate.batchRead =
                (descriptors, count, values, stride) -> {
                    throw new IOException("synthetic native failure");
                };
        AtomicReference<String> currentKey = new AtomicReference<>("a");
        MetricProbe metrics = new MetricProbe();
        CachedInternalValueState<String, VoidNamespace, Integer> state =
                directState(delegate, currentKey, metrics);

        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("a"));
        assertNotNull(task);
        task.run();

        assertEquals(1L, metrics.value("dstl_failure_fallbacks"));
        assertEquals(43, state.value());
        assertEquals(1, delegate.authoritativeReadCalls.get());

        delegate.batchRead = (descriptors, count, values, stride) -> publishIntValues(
                descriptors, count, values, stride, 80);
        Runnable next = state.buildAsyncPrefetchTask(Arrays.asList("b"));
        assertNotNull(next);
        ((PrefetchExecutor.DroppableTask) next).onDropped();
    }

    @Test
    void deserializationFailureIsCountedAndFallsBack() throws Exception {
        DirectValueState delegate = new DirectValueState();
        delegate.authoritativeValue = 44;
        delegate.batchRead = DirectStateTransitFailurePathTest::publishTruncatedValues;
        AtomicReference<String> currentKey = new AtomicReference<>("a");
        MetricProbe metrics = new MetricProbe();
        CachedInternalValueState<String, VoidNamespace, Integer> state =
                directState(delegate, currentKey, metrics);

        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("a"));
        assertNotNull(task);
        task.run();

        assertEquals(1L, metrics.value("dstl_failure_fallbacks"));
        assertEquals(0L, metrics.value("dstl_values_materialized"));
        assertEquals(44, state.value());
        assertEquals(1, delegate.authoritativeReadCalls.get());
    }

    @Test
    void queuedGenerationChangeSkipsNativeReadAndFallsBack() throws Exception {
        DirectValueState delegate = new DirectValueState();
        AtomicReference<String> currentKey = new AtomicReference<>("writer");
        MetricProbe metrics = new MetricProbe();
        CachedInternalValueState<String, VoidNamespace, Integer> state =
                directState(delegate, currentKey, metrics);

        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("prefetched"));
        assertNotNull(task);
        state.update(91);
        state.flush();
        task.run();

        assertEquals(0, delegate.directReadCalls.get());
        assertEquals(1L, metrics.value("dstl_generation_drops"));
        currentKey.set("prefetched");
        assertEquals(91, state.value());
        assertEquals(1, delegate.authoritativeReadCalls.get());
    }

    @Test
    void inFlightGenerationChangeDropsCompletedBatchAndFallsBack() throws Exception {
        DirectValueState delegate = new DirectValueState();
        CountDownLatch readEntered = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        delegate.batchRead =
                (descriptors, count, values, stride) -> {
                    readEntered.countDown();
                    await(releaseRead, "release in-flight direct read");
                    return publishIntValues(descriptors, count, values, stride, 70);
                };
        AtomicReference<String> currentKey = new AtomicReference<>("writer");
        MetricProbe metrics = new MetricProbe();
        CachedInternalValueState<String, VoidNamespace, Integer> state =
                directState(delegate, currentKey, metrics);
        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("prefetched"));
        assertNotNull(task);

        Thread worker = new Thread(task, "dstl-generation-test");
        worker.setDaemon(true);
        worker.start();
        try {
            assertTrue(readEntered.await(5, TimeUnit.SECONDS));
            state.update(92);
            state.flush();
        } finally {
            releaseRead.countDown();
        }
        worker.join(TimeUnit.SECONDS.toMillis(5));

        assertFalse(worker.isAlive());
        assertEquals(1, delegate.directReadCalls.get());
        assertEquals(1L, metrics.value("dstl_generation_drops"));
        assertEquals(0L, metrics.value("dstl_values_materialized"));
        currentKey.set("prefetched");
        assertEquals(92, state.value());
        assertEquals(1, delegate.authoritativeReadCalls.get());
    }

    @Test
    void closeWaitsForInFlightReadAndRejectsNewTasks() throws Exception {
        DirectValueState delegate = new DirectValueState();
        CountDownLatch readEntered = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        delegate.batchRead =
                (descriptors, count, values, stride) -> {
                    readEntered.countDown();
                    await(releaseRead, "release direct read before close");
                    return publishIntValues(descriptors, count, values, stride, 70);
                };
        AtomicReference<String> currentKey = new AtomicReference<>("a");
        MetricProbe metrics = new MetricProbe();
        CachedInternalValueState<String, VoidNamespace, Integer> state =
                directState(delegate, currentKey, metrics);
        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("a"));
        assertNotNull(task);

        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch closeReturned = new CountDownLatch(1);
        Thread worker = new Thread(task, "dstl-close-worker-test");
        Thread closer =
                new Thread(
                        () -> {
                            closeStarted.countDown();
                            state.close();
                            closeReturned.countDown();
                        },
                        "dstl-close-barrier-test");
        worker.setDaemon(true);
        closer.setDaemon(true);
        worker.start();
        try {
            assertTrue(readEntered.await(5, TimeUnit.SECONDS));
            closer.start();
            assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
            assertFalse(closeReturned.await(200, TimeUnit.MILLISECONDS));
        } finally {
            releaseRead.countDown();
        }
        worker.join(TimeUnit.SECONDS.toMillis(5));
        closer.join(TimeUnit.SECONDS.toMillis(5));

        assertFalse(worker.isAlive());
        assertFalse(closer.isAlive());
        assertEquals(1, delegate.directReadCalls.get());
        assertNull(state.buildAsyncPrefetchTask(Arrays.asList("b")));
    }

    @Test
    void defaultConstructorKeepsDirectDelegateOnLegacyPath() throws Exception {
        DirectValueState delegate = new DirectValueState();
        delegate.setSerializedValue(55);
        AtomicReference<String> currentKey = new AtomicReference<>("a");
        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("a"));
        assertNotNull(task);
        task.run();

        assertEquals(55, state.value());
        assertEquals(0, delegate.directReadCalls.get());
        assertEquals(1, delegate.serializedReadCalls.get());
        assertEquals(0, delegate.authoritativeReadCalls.get());
    }

    @Test
    void nonRocksDBDelegateUsesLegacyPathWhenDirectFlagIsRequested() throws Exception {
        PlainValueState delegate = new PlainValueState();
        delegate.setSerializedValue(56);
        AtomicReference<String> currentKey = new AtomicReference<>("a");
        MetricProbe metrics = new MetricProbe();
        CachedInternalValueState<String, VoidNamespace, Integer> state =
                requestedDirectState(delegate, currentKey, metrics);

        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("a"));
        assertNotNull(task);
        task.run();

        assertEquals(56, state.value());
        assertEquals(1, delegate.serializedReadCalls.get());
        assertEquals(0, delegate.authoritativeReadCalls.get());
        assertEquals(0L, metrics.value("dstl_batches_prepared"));
        assertEquals(0L, metrics.value("dstl_single_jni_calls"));
    }

    private static CachedInternalValueState<String, VoidNamespace, Integer> directState(
            DirectValueState delegate,
            AtomicReference<String> currentKey,
            MetricProbe metrics) {
        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        true,
                        4,
                        512,
                        128,
                        metrics.metrics);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        return state;
    }

    private static CachedInternalValueState<String, VoidNamespace, Integer> requestedDirectState(
            PlainValueState delegate,
            AtomicReference<String> currentKey,
            MetricProbe metrics) {
        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        true,
                        4,
                        512,
                        128,
                        metrics.metrics);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        return state;
    }

    private static void await(CountDownLatch latch, String description) throws IOException {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IOException("Timed out waiting to " + description);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting to " + description, interrupted);
        }
    }

    private static int publishMisses(
            ByteBuffer descriptors, int count, ByteBuffer values, int stride) {
        for (int index = 0; index < count; index++) {
            setStatus(descriptors, index, RocksDBDirectValueAccess.NOT_FOUND);
        }
        return 0;
    }

    private static int publishOverflow(
            ByteBuffer descriptors, int count, ByteBuffer values, int stride) {
        for (int index = 0; index < count; index++) {
            setStatus(descriptors, index, RocksDBDirectValueAccess.VALUE_OVERFLOW);
        }
        return count;
    }

    private static int publishTruncatedValues(
            ByteBuffer descriptors, int count, ByteBuffer values, int stride) {
        for (int index = 0; index < count; index++) {
            setStatus(descriptors, index, 1);
            values.put(index * stride, (byte) 1);
        }
        return count;
    }

    private static int publishIntValues(
            ByteBuffer descriptors, int count, ByteBuffer values, int stride, int firstValue) {
        ByteBuffer writableValues = values.duplicate().order(ByteOrder.BIG_ENDIAN);
        for (int index = 0; index < count; index++) {
            setStatus(descriptors, index, Integer.BYTES);
            writableValues.putInt(index * stride, firstValue + index);
        }
        return count;
    }

    private static void setStatus(ByteBuffer descriptors, int index, int status) {
        descriptors
                .duplicate()
                .order(ByteOrder.nativeOrder())
                .putInt(
                        index * RocksDBDirectValueAccess.DESCRIPTOR_BYTES
                                + RocksDBDirectValueAccess.VALUE_LENGTH_OR_STATUS_OFFSET,
                        status);
    }

    private interface BatchRead {
        int read(ByteBuffer descriptors, int count, ByteBuffer values, int stride)
                throws IOException;
    }

    private static class PlainValueState
            implements InternalValueState<String, VoidNamespace, Integer> {

        final AtomicInteger authoritativeReadCalls = new AtomicInteger();
        final AtomicInteger serializedReadCalls = new AtomicInteger();
        volatile Integer authoritativeValue = 99;
        private volatile byte[] serializedValue;

        @Override
        public Integer value() {
            authoritativeReadCalls.incrementAndGet();
            return authoritativeValue;
        }

        @Override
        public void update(Integer value) {
            authoritativeValue = value;
        }

        @Override
        public void clear() {
            authoritativeValue = null;
        }

        @Override
        public TypeSerializer<String> getKeySerializer() {
            return StringSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer<VoidNamespace> getNamespaceSerializer() {
            return VoidNamespaceSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer<Integer> getValueSerializer() {
            return IntSerializer.INSTANCE;
        }

        @Override
        public void setCurrentNamespace(VoidNamespace namespace) {}

        @Override
        public byte[] getSerializedValue(
                byte[] serializedKeyAndNamespace,
                TypeSerializer<String> safeKeySerializer,
                TypeSerializer<VoidNamespace> safeNamespaceSerializer,
                TypeSerializer<Integer> safeValueSerializer) {
            serializedReadCalls.incrementAndGet();
            return serializedValue;
        }

        @Override
        public InternalKvState.StateIncrementalVisitor<String, VoidNamespace, Integer>
                getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
            return null;
        }

        void setSerializedValue(int value) throws IOException {
            DataOutputSerializer output = new DataOutputSerializer(Integer.BYTES);
            IntSerializer.INSTANCE.serialize(value, output);
            serializedValue = output.getCopyOfBuffer();
        }
    }

    private static final class DirectValueState extends PlainValueState
            implements RocksDBDirectValueAccess<String, VoidNamespace> {

        private final AtomicInteger directReadCalls = new AtomicInteger();
        private volatile BatchRead batchRead =
                (descriptors, count, values, stride) ->
                        publishIntValues(descriptors, count, values, stride, 70);

        @Override
        public void writeKeyAndNamespace(
                String key, VoidNamespace namespace, DataOutputView target) throws IOException {
            StringSerializer.INSTANCE.serialize(key, target);
        }

        @Override
        public int readValueBatch(
                ByteBuffer keyArena,
                ByteBuffer descriptors,
                int count,
                ByteBuffer valueArena,
                int valueStride)
                throws IOException {
            directReadCalls.incrementAndGet();
            return batchRead.read(descriptors, count, valueArena, valueStride);
        }

        @Override
        public boolean usesSingleJniBatchRead() {
            return true;
        }
    }

    private static final class MetricProbe {
        private final Map<String, Gauge<?>> gauges = new HashMap<>();
        private final DirectStateTransitMetrics metrics;

        @SuppressWarnings({"rawtypes", "unchecked"})
        private MetricProbe() {
            MetricGroup root = mock(MetricGroup.class);
            MetricGroup cachekit = mock(MetricGroup.class);
            MetricGroup diagnostics = mock(MetricGroup.class);
            when(root.addGroup("cachekit")).thenReturn(cachekit);
            when(cachekit.addGroup("diagnostics")).thenReturn(diagnostics);
            doAnswer(
                            invocation -> {
                                gauges.put(
                                        invocation.getArgument(0),
                                        (Gauge<?>) invocation.getArgument(1));
                                return invocation.getArgument(1);
                            })
                    .when(diagnostics)
                    .gauge(anyString(), any(Gauge.class));
            metrics = DirectStateTransitMetrics.create(root, true);
        }

        private long value(String name) {
            Gauge<?> gauge = gauges.get(name);
            assertNotNull(gauge, "Missing diagnostic gauge " + name);
            return ((Number) gauge.getValue()).longValue();
        }
    }
}
