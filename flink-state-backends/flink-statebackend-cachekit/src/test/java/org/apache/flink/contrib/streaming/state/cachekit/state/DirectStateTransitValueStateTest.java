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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBDirectValueAccess;
import org.apache.flink.contrib.streaming.state.cachekit.PrefetchExecutor;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class DirectStateTransitValueStateTest {

    @Test
    @SuppressWarnings("unchecked")
    void stagesSingleJniResultsWithoutCallingAuthoritativeValuePath() throws Exception {
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBDirectValueAccess.class));
        RocksDBDirectValueAccess<String, VoidNamespace> direct =
                (RocksDBDirectValueAccess<String, VoidNamespace>) delegate;
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(direct.usesSingleJniBatchRead()).thenReturn(true);
        doAnswer(
                        invocation -> {
                            String key = invocation.getArgument(0);
                            DataOutputView output = invocation.getArgument(2);
                            StringSerializer.INSTANCE.serialize(key, output);
                            return null;
                        })
                .when(direct)
                .writeKeyAndNamespace(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.eq(VoidNamespace.INSTANCE),
                        org.mockito.ArgumentMatchers.any(DataOutputView.class));
        doAnswer(
                        invocation -> {
                            ByteBuffer descriptors = invocation.getArgument(1);
                            int count = invocation.getArgument(2);
                            ByteBuffer values = invocation.getArgument(3);
                            ByteBuffer descriptorView =
                                    descriptors.duplicate().order(ByteOrder.nativeOrder());
                            for (int index = 0; index < count; index++) {
                                int record = index * RocksDBDirectValueAccess.DESCRIPTOR_BYTES;
                                int valueOffset =
                                        descriptorView.getInt(
                                                record
                                                        + RocksDBDirectValueAccess
                                                                .VALUE_OFFSET_OFFSET);
                                descriptorView.putInt(
                                        record
                                                + RocksDBDirectValueAccess
                                                        .VALUE_LENGTH_OR_STATUS_OFFSET,
                                        Integer.BYTES);
                                values.duplicate()
                                        .order(ByteOrder.BIG_ENDIAN)
                                        .putInt(valueOffset, 100 + index);
                            }
                            return count;
                        })
                .when(direct)
                .readValueBatch(
                        org.mockito.ArgumentMatchers.any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.anyInt(),
                        org.mockito.ArgumentMatchers.any(ByteBuffer.class),
                        org.mockito.ArgumentMatchers.anyInt());

        AtomicReference<String> currentKey = new AtomicReference<>("a");
        DirectStateTransitMetrics metrics = DirectStateTransitMetrics.forTesting();
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
                        Integer.BYTES,
                        metrics);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("a", "b"));
        assertNotNull(task);
        task.run();

        assertEquals(100, state.value());
        currentKey.set("b");
        assertEquals(101, state.value());
        verify(delegate, never()).value();
        assertEquals(1, metrics.batchesPrepared());
        assertEquals(2, metrics.keysPrepared());
        assertEquals(1, metrics.singleJniCalls());
        assertEquals(0, metrics.perKeyJniCalls());
    }

    @Test
    @SuppressWarnings("unchecked")
    void droppedQueuedTaskReleasesEnvelopeForNextSubmission() throws Exception {
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBDirectValueAccess.class));
        RocksDBDirectValueAccess<String, VoidNamespace> direct =
                (RocksDBDirectValueAccess<String, VoidNamespace>) delegate;
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(direct.usesSingleJniBatchRead()).thenReturn(true);
        doAnswer(
                        invocation -> {
                            StringSerializer.INSTANCE.serialize(
                                    invocation.getArgument(0), invocation.getArgument(2));
                            return null;
                        })
                .when(direct)
                .writeKeyAndNamespace(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.eq(VoidNamespace.INSTANCE),
                        org.mockito.ArgumentMatchers.any(DataOutputView.class));

        AtomicReference<String> currentKey = new AtomicReference<>("a");
        DirectStateTransitMetrics metrics = DirectStateTransitMetrics.forTesting();
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
                        metrics);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        Runnable first = state.buildAsyncPrefetchTask(Arrays.asList("a"));
        assertNotNull(first);
        ((PrefetchExecutor.DroppableTask) first).onDropped();

        Runnable second = state.buildAsyncPrefetchTask(Arrays.asList("b"));
        assertNotNull(second);
        // A duplicate late notification for the old queue entry must not release or clear the
        // newly owned envelope.
        ((PrefetchExecutor.DroppableTask) first).onDropped();
        assertNull(state.buildAsyncPrefetchTask(Arrays.asList("c")));
        ((PrefetchExecutor.DroppableTask) second).onDropped();
        Runnable third = state.buildAsyncPrefetchTask(Arrays.asList("c"));
        assertNotNull(third);
        ((PrefetchExecutor.DroppableTask) third).onDropped();
        assertEquals(3, metrics.batchesPrepared());
    }
}
