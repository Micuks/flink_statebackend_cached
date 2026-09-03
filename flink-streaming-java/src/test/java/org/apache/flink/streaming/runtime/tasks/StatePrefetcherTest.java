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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.BatchKeyGroupingSupport;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

class StatePrefetcherTest {

    @Test
    @SuppressWarnings("unchecked")
    void testCompletionPrefetchInvokesOnlyCompletionBearingHook() {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings().extraInterfaces(CompletionPrefetchHook.class));
        Collection<Integer> keys = Arrays.asList(1, 2, 3);
        CompletableFuture<Boolean> completion = new CompletableFuture<>();
        org.mockito.Mockito.when(((CompletionPrefetchHook) backend).prefetchWithCompletion(keys))
                .thenReturn(completion);

        CompletableFuture<Boolean> result =
                StatePrefetcher.prefetchKeysWithCompletion(backend, keys);

        assertFalse(result.isDone());
        completion.complete(true);
        assertTrue(result.join());
        verify((CompletionPrefetchHook) backend).prefetchWithCompletion(keys);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCompletionPrefetchFailsClosedWhenUnsupportedOrInvocationFails() {
        Collection<Integer> keys = Arrays.asList(1, 2, 3);
        KeyedStateBackend<Object> unsupported = mock(KeyedStateBackend.class);
        assertFalse(StatePrefetcher.prefetchKeysWithCompletion(unsupported, keys).join());

        KeyedStateBackend<Object> failing =
                mock(
                        KeyedStateBackend.class,
                        withSettings().extraInterfaces(CompletionPrefetchHook.class));
        org.mockito.Mockito.when(((CompletionPrefetchHook) failing).prefetchWithCompletion(keys))
                .thenThrow(new IllegalStateException("reflection target failed"));
        assertFalse(StatePrefetcher.prefetchKeysWithCompletion(failing, keys).join());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testReadyGateMetricBridgeReadsOptionalBackendSnapshot() {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings().extraInterfaces(ReadyMetricsHook.class));
        org.mockito.Mockito.when(((ReadyMetricsHook) backend).readyGatedPrefetchMetrics())
                .thenReturn(new long[] {11L, 22L, 33L});
        AbstractStreamOperator operator =
                mock(AbstractStreamOperator.class, withSettings().extraInterfaces(Input.class));
        org.mockito.Mockito.when(operator.getKeyedStateBackend()).thenReturn(backend);

        assertEquals(22L, StatePrefetcher.getReadyGatedBackendMetric((Input<?>) operator, 1));
        assertEquals(0L, StatePrefetcher.getReadyGatedBackendMetric((Input<?>) operator, 9));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testCompletionPrefetchFailsClosedWhenSelectorFails() throws Exception {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings().extraInterfaces(CompletionPrefetchHook.class));
        AbstractStreamOperator operator =
                mock(AbstractStreamOperator.class, withSettings().extraInterfaces(Input.class));
        org.mockito.Mockito.when(operator.getKeyedStateBackend()).thenReturn(backend);
        java.lang.reflect.Field selectorField =
                AbstractStreamOperator.class.getDeclaredField("stateKeySelector1");
        selectorField.setAccessible(true);
        selectorField.set(
                operator,
                (KeySelector<Integer, Integer>)
                        value -> {
                            throw new IllegalArgumentException("bad key");
                        });
        StreamRecord<?>[] records = {new StreamRecord<>(1), new StreamRecord<>(2)};

        assertFalse(
                StatePrefetcher.prefetchWithCompletion(
                                (Input<?>) operator, records, 0, records.length)
                        .join());
        verify((CompletionPrefetchHook) backend, org.mockito.Mockito.never())
                .prefetchWithCompletion(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void testExtractKeysReadsOnlyRequestedRangeAndPreservesOrder() {
        StreamRecord<?>[] records = {
            new StreamRecord<>(100),
            new StreamRecord<>(11),
            null,
            new StreamRecord<>(22),
            new StreamRecord<>(13),
            new StreamRecord<>(200)
        };
        LinkedHashSet<Integer> keys = new LinkedHashSet<>();

        assertTrue(
                StatePrefetcher.extractKeys(
                        (KeySelector<Integer, Integer>) value -> value % 10, records, 1, 5, keys));
        assertEquals(new LinkedHashSet<>(Arrays.asList(1, 2, 3)), keys);
    }

    @Test
    void testExtractKeysAbortsRangeWhenSelectorFails() {
        StreamRecord<?>[] records = {new StreamRecord<>(1), new StreamRecord<>(2)};
        LinkedHashSet<Integer> keys = new LinkedHashSet<>();

        assertFalse(
                StatePrefetcher.extractKeys(
                        (KeySelector<Integer, Integer>)
                                value -> {
                                    throw new IllegalArgumentException("bad key");
                                },
                        records,
                        0,
                        records.length,
                        keys));
        assertTrue(keys.isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testImmediatePrefetchInvokesOptionalBackendHook() {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings().extraInterfaces(ImmediatePrefetchHook.class));
        Collection<Integer> keys = Arrays.asList(1, 2, 3);

        assertTrue(StatePrefetcher.prefetchKeysImmediately(backend, keys));
        verify((ImmediatePrefetchHook) backend).prefetchForImmediateUse(keys);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testImmediatePrefetchAfterDispatchInvokesFusedBackendHook() {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings()
                                .extraInterfaces(
                                        ImmediatePrefetchAfterDispatchHook.class,
                                        DispatchCancelHook.class));
        Collection<Integer> keys = Arrays.asList(1, 2, 3);

        assertTrue(StatePrefetcher.prefetchKeysImmediately(backend, keys, true));
        verify((ImmediatePrefetchAfterDispatchHook) backend)
                .prefetchForImmediateUseAfterDispatch(keys);
        verify((DispatchCancelHook) backend, org.mockito.Mockito.never())
                .cancelPrefetchForDispatch(keys);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testImmediatePrefetchAfterDispatchFallsBackToEstablishedHook() {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings()
                                .extraInterfaces(
                                        ImmediatePrefetchHook.class, DispatchCancelHook.class));
        Collection<Integer> keys = Arrays.asList(1, 2, 3);
        org.mockito.Mockito.when(((DispatchCancelHook) backend).cancelPrefetchForDispatch(keys))
                .thenReturn(2);

        assertTrue(StatePrefetcher.prefetchKeysImmediately(backend, keys, true));
        verify((DispatchCancelHook) backend).cancelPrefetchForDispatch(keys);
        verify((ImmediatePrefetchHook) backend).prefetchForImmediateUse(keys);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDispatchCancellationInvokesOnlyOptionalExactBackendHook() {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings().extraInterfaces(DispatchCancelHook.class));
        Collection<Integer> keys = Arrays.asList(1, 2, 3);
        org.mockito.Mockito.when(((DispatchCancelHook) backend).cancelPrefetchForDispatch(keys))
                .thenReturn(2);

        assertEquals(2, StatePrefetcher.cancelPrefetchForDispatch(backend, keys));
        verify((DispatchCancelHook) backend).cancelPrefetchForDispatch(keys);

        KeyedStateBackend<Object> unsupported = mock(KeyedStateBackend.class);
        assertEquals(-1, StatePrefetcher.cancelPrefetchForDispatch(unsupported, keys));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testDispatchCancellationReusesCallerOwnedGroupedKeys() {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings().extraInterfaces(DispatchCancelHook.class));
        Collection<Integer> groupedKeys = Arrays.asList(1, 2, 3);
        org.mockito.Mockito.when(
                        ((DispatchCancelHook) backend).cancelPrefetchForDispatch(groupedKeys))
                .thenReturn(2);
        AbstractStreamOperator operator =
                mock(AbstractStreamOperator.class, withSettings().extraInterfaces(Input.class));
        org.mockito.Mockito.when(operator.getKeyedStateBackend()).thenReturn(backend);

        assertEquals(
                2, StatePrefetcher.cancelPrefetchKeysForDispatch((Input<?>) operator, groupedKeys));
        verify((DispatchCancelHook) backend).cancelPrefetchForDispatch(groupedKeys);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testResidentMutationBatchRequiresCapabilityAndPairsBeginEnd() {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings()
                                .extraInterfaces(
                                        NativeMutationBatchCapability.class,
                                        NativeMutationBatchHook.class));
        NativeMutationBatchCapability capability = (NativeMutationBatchCapability) backend;
        NativeMutationBatchHook hook = (NativeMutationBatchHook) backend;
        org.mockito.Mockito.when(capability.nativeResidentMutationBatchEnabled()).thenReturn(true);
        Collection<Integer> keys = Arrays.asList(1, 2, 3);
        org.mockito.Mockito.when(hook.beginNativeResidentMutationBatch(keys)).thenReturn(1);
        AbstractStreamOperator operator =
                mock(AbstractStreamOperator.class, withSettings().extraInterfaces(Input.class));
        org.mockito.Mockito.when(operator.getKeyedStateBackend()).thenReturn(backend);

        assertTrue(StatePrefetcher.beginNativeResidentMutationBatch((Input<?>) operator, keys));
        StatePrefetcher.endNativeResidentMutationBatch((Input<?>) operator);

        verify(hook).beginNativeResidentMutationBatch(keys);
        verify(hook).endNativeResidentMutationBatch();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testNativeMailboxPreservesDuplicateArrivalKeysForJniCompaction() {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings().extraInterfaces(NativeMailboxHook.class));
        org.mockito.Mockito.when(((NativeMailboxHook) backend).nativeMailboxBatchEnabled())
                .thenReturn(true);

        Collection<Integer> keys = StatePrefetcher.newKeyCollection(backend, 4);
        keys.add(1);
        keys.add(2);
        keys.add(1);

        assertTrue(keys instanceof ArrayList);
        assertEquals(Arrays.asList(1, 2, 1), keys);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testNativePreaggInvokesOptionalBackendHook() {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings().extraInterfaces(NativePreaggHook.class));
        List<Integer> keys = Arrays.asList(7, 8, 7);
        org.mockito.Mockito.when(((NativePreaggHook) backend).nativePreaggGroupIds(keys))
                .thenReturn(new int[] {2, 0, 1, 0});

        assertArrayEquals(new int[] {2, 0, 1, 0}, StatePrefetcher.groupKeysNatively(backend, keys));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPackedNativeGroupingUsesDirectCapabilityWithoutReflection() {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings().extraInterfaces(BatchKeyGroupingSupport.class));
        BatchKeyGroupingSupport grouping = (BatchKeyGroupingSupport) backend;
        ByteBuffer tokens =
                ByteBuffer.allocateDirect(3 * Integer.BYTES).order(ByteOrder.nativeOrder());
        ByteBuffer plan =
                ByteBuffer.allocateDirect(BatchKeyGroupingSupport.requiredPackedPlanBytes(3))
                        .order(ByteOrder.nativeOrder());
        org.mockito.Mockito.when(grouping.maxGroupingEntries()).thenReturn(512);
        org.mockito.Mockito.when(grouping.groupHashTokens(tokens, 3, plan)).thenReturn(2);

        assertEquals(2, StatePrefetcher.groupHashTokensNatively(backend, tokens, 3, plan));
        verify(grouping).groupHashTokens(tokens, 3, plan);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testIndexedBatchFoldUsesJobScopedBackendCapability() {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings().extraInterfaces(BatchKeyGroupingSupport.class));
        BatchKeyGroupingSupport grouping = (BatchKeyGroupingSupport) backend;
        org.mockito.Mockito.when(grouping.indexedBatchFoldEnabled()).thenReturn(true);

        assertTrue(StatePrefetcher.indexedBatchFoldEnabled(backend));
        verify(grouping).indexedBatchFoldEnabled();
    }

    public interface ImmediatePrefetchHook {
        void prefetchForImmediateUse(Collection<?> keys);
    }

    public interface CompletionPrefetchHook {
        CompletableFuture<Boolean> prefetchWithCompletion(Collection<?> keys);
    }

    public interface ReadyMetricsHook {
        long[] readyGatedPrefetchMetrics();
    }

    public interface ImmediatePrefetchAfterDispatchHook {
        void prefetchForImmediateUseAfterDispatch(Collection<?> keys);
    }

    public interface DispatchCancelHook {
        int cancelPrefetchForDispatch(Collection<?> keys);
    }

    public interface NativeMailboxHook {
        boolean nativeMailboxBatchEnabled();
    }

    public interface NativePreaggHook {
        int[] nativePreaggGroupIds(List<?> keys);
    }

    public interface NativeMutationBatchCapability {
        boolean nativeResidentMutationBatchEnabled();
    }

    public interface NativeMutationBatchHook {
        int beginNativeResidentMutationBatch(Collection<?> keys);

        int endNativeResidentMutationBatch();
    }
}
