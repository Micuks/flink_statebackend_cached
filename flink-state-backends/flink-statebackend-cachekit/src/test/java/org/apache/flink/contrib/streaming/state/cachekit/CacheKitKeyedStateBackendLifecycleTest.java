/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PresenceCacheImplementation;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CacheKitKeyedStateBackendLifecycleTest {

    @Test
    void testCloseRecordsNamespacedValueStateAndEmitsOneBackendSummary() throws Exception {
        AbstractKeyedStateBackend<String> delegate = mockDelegate();
        InternalValueState<String, Long, Integer> delegateState = mock(InternalValueState.class);
        when(delegateState.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegateState.getNamespaceSerializer()).thenReturn(LongSerializer.INSTANCE);
        when(delegateState.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        doReturn(delegateState).when(delegate).getOrCreateKeyedState(any(), any());

        CacheKitKeyedStateBackend<String> cacheKit = newCacheKitBackend(delegate);
        ValueStateDescriptor<Integer> descriptor =
                new ValueStateDescriptor<>("window-value", IntSerializer.INSTANCE);
        @SuppressWarnings("unchecked")
        InternalValueState<String, Long, Integer> cachedState =
                (InternalValueState<String, Long, Integer>)
                        cacheKit.getOrCreateKeyedState(LongSerializer.INSTANCE, descriptor);
        cachedState.setCurrentNamespace(7L);

        cacheKit.close();
        cacheKit.dispose();

        assertTrue(cacheKit.hasPrefetchCloseSummaryForTesting());
        assertEquals(1, cacheKit.getClosedValueStateWrapperCountForTesting());
        assertEquals(1, cacheKit.getClosedRecordKeyPrefetchWrapperCountForTesting());
        verify(delegate).close();
        verify(delegate).dispose();
    }

    @Test
    void testCloseWithoutValueStateStillEmitsBackendSummary() throws Exception {
        CacheKitKeyedStateBackend<String> cacheKit = newCacheKitBackend(mockDelegate());

        cacheKit.close();

        assertTrue(cacheKit.hasPrefetchCloseSummaryForTesting());
        assertEquals(0, cacheKit.getClosedValueStateWrapperCountForTesting());
        assertEquals(0, cacheKit.getClosedRecordKeyPrefetchWrapperCountForTesting());
    }

    @Test
    void testDisposeWaitsForConcurrentCloseFlush() throws Exception {
        AbstractKeyedStateBackend<String> delegate = mockDelegate();
        InternalValueState<String, VoidNamespace, Integer> delegateState = mock(InternalValueState.class);
        when(delegateState.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegateState.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegateState.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        doReturn(delegateState)
                .when(delegate)
                .getOrCreateKeyedState(any(), any());

        CountDownLatch flushEntered = new CountDownLatch(1);
        CountDownLatch allowFlushToFinish = new CountDownLatch(1);
        CountDownLatch disposeAttempted = new CountDownLatch(1);
        CountDownLatch delegateDisposed = new CountDownLatch(1);
        AtomicBoolean flushFinished = new AtomicBoolean();
        AtomicBoolean disposedDuringFlush = new AtomicBoolean();
        doAnswer(
                        ignored -> {
                            flushEntered.countDown();
                            if (!allowFlushToFinish.await(10, TimeUnit.SECONDS)) {
                                throw new AssertionError("timed out waiting to release cache flush");
                            }
                            flushFinished.set(true);
                            return null;
                        })
                .when(delegateState)
                .update(42);
        doAnswer(
                        ignored -> {
                            disposedDuringFlush.set(!flushFinished.get());
                            delegateDisposed.countDown();
                            return null;
                        })
                .when(delegate)
                .dispose();

        CacheKitKeyedStateBackend<String> cacheKit = newCacheKitBackend(delegate);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>("value", IntSerializer.INSTANCE);
            @SuppressWarnings("unchecked")
            InternalValueState<String, VoidNamespace, Integer> cachedState =
                    (InternalValueState<String, VoidNamespace, Integer>)
                            cacheKit.getOrCreateKeyedState(
                                    VoidNamespaceSerializer.INSTANCE, descriptor);
            cacheKit.setCurrentKey("key");
            cachedState.setCurrentNamespace(VoidNamespace.INSTANCE);
            cachedState.update(42);

            Future<Void> closeFuture =
                    executor.submit(
                            () -> {
                                cacheKit.close();
                                return null;
                            });
            assertTrue(flushEntered.await(10, TimeUnit.SECONDS));

            Future<?> disposeFuture =
                    executor.submit(
                            () -> {
                                disposeAttempted.countDown();
                                cacheKit.dispose();
                            });
            assertTrue(disposeAttempted.await(10, TimeUnit.SECONDS));
            assertFalse(
                    delegateDisposed.await(250, TimeUnit.MILLISECONDS),
                    "delegate disposal must wait for the close-triggered flush");

            allowFlushToFinish.countDown();
            closeFuture.get(10, TimeUnit.SECONDS);
            disposeFuture.get(10, TimeUnit.SECONDS);

            assertFalse(disposedDuringFlush.get());
            verify(delegate).dispose();
        } finally {
            allowFlushToFinish.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static AbstractKeyedStateBackend<String> mockDelegate() {
        AbstractKeyedStateBackend<String> delegate = mock(AbstractKeyedStateBackend.class);
        when(delegate.getLatencyTrackingStateConfig())
                .thenReturn(LatencyTrackingStateConfig.disabled());
        when(delegate.getKeyGroupCompressionDecorator())
                .thenReturn(UncompressedStreamCompressionDecorator.INSTANCE);
        when(delegate.getKeyContext())
                .thenReturn(new InternalKeyContextImpl<>(new KeyGroupRange(0, 0), 1));
        return delegate;
    }

    private static CacheKitKeyedStateBackend<String> newCacheKitBackend(
            AbstractKeyedStateBackend<String> delegate) {
        return new CacheKitKeyedStateBackend<>(
                delegate,
                null,
                StringSerializer.INSTANCE,
                CacheKitKeyedStateBackendLifecycleTest.class.getClassLoader(),
                new ExecutionConfig(),
                TtlTimeProvider.DEFAULT,
                new CloseableRegistry(),
                null,
                128,
                CachePolicyType.LRU,
                0,
                false,
                0.05,
                1000,
                0,
                CachePolicyType.LRU,
                0,
                PresenceCacheImplementation.OBJECT,
                0,
                CachePolicyType.LRU,
                0,
                false,
                0.05,
                1000,
                false,
                0,
                false,
                false,
                0,
                false,
                false);
    }
}
