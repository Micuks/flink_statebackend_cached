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
import org.apache.flink.streaming.runtime.io.MailboxStableKeySidecar;
import org.apache.flink.streaming.runtime.io.TransientKeySelector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

class StatePrefetcherTest {

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testExactNamespaceLookaheadSubmitsAlignedPairs() throws Exception {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings()
                                .extraInterfaces(
                                        ExactNamespaceCapability.class,
                                        ExactNamespacePrefetchHook.class));
        org.mockito.Mockito.when(
                        ((ExactNamespaceCapability) backend).exactNamespacePrefetchEnabled())
                .thenReturn(true);

        AbstractStreamOperator operator =
                mock(
                        AbstractStreamOperator.class,
                        withSettings()
                                .extraInterfaces(Input.class, StateNamespaceLookahead.class));
        org.mockito.Mockito.when(operator.getKeyedStateBackend()).thenReturn(backend);
        java.lang.reflect.Field selectorField =
                AbstractStreamOperator.class.getDeclaredField("stateKeySelector1");
        selectorField.setAccessible(true);
        selectorField.set(operator, (KeySelector<Integer, Integer>) value -> value % 10);
        org.mockito.Mockito.doAnswer(
                        invocation -> {
                            StreamRecord<?> record = invocation.getArgument(0);
                            Object stableKey = invocation.getArgument(1);
                            List<Object> keys = invocation.getArgument(2);
                            List<Object> namespaces = invocation.getArgument(3);
                            keys.add(stableKey);
                            namespaces.add("window-" + record.getValue());
                            return null;
                        })
                .when((StateNamespaceLookahead) operator)
                .appendStatePrefetchKeyNamespaces(
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.any(),
                        org.mockito.ArgumentMatchers.anyList(),
                        org.mockito.ArgumentMatchers.anyList());

        AtomicReference<List<?>> capturedKeys = new AtomicReference<>();
        AtomicReference<List<?>> capturedNamespaces = new AtomicReference<>();
        org.mockito.Mockito.doAnswer(
                        invocation -> {
                            capturedKeys.set(new ArrayList<>((Collection<?>) invocation.getArgument(0)));
                            capturedNamespaces.set(
                                    new ArrayList<>((Collection<?>) invocation.getArgument(1)));
                            return null;
                        })
                .when((ExactNamespacePrefetchHook) backend)
                .prefetchKeyNamespaces(
                        org.mockito.ArgumentMatchers.anyCollection(),
                        org.mockito.ArgumentMatchers.anyCollection());

        StreamRecord<?>[] records = {new StreamRecord<>(11), new StreamRecord<>(22)};
        StatePrefetcher.prefetch((Input<?>) operator, records, records.length);

        assertEquals(Arrays.asList(1, 2), capturedKeys.get());
        assertEquals(Arrays.asList("window-11", "window-22"), capturedNamespaces.get());

        StreamRecord<?>[] second = {new StreamRecord<>(33), new StreamRecord<>(44)};
        StatePrefetcher.prefetch((Input<?>) operator, second, second.length);
        assertEquals(Arrays.asList(3, 4), capturedKeys.get());
        assertEquals(Arrays.asList("window-33", "window-44"), capturedNamespaces.get());
        verify((ExactNamespacePrefetchHook) backend, org.mockito.Mockito.times(2))
                .prefetchKeyNamespaces(
                        org.mockito.ArgumentMatchers.anyCollection(),
                        org.mockito.ArgumentMatchers.anyCollection());
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
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testRepresentativeExtractionCopiesOneStableKeyPerExactGroup() {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings().extraInterfaces(BatchKeyGroupingSupport.class));
        BatchKeyGroupingSupport grouping = (BatchKeyGroupingSupport) backend;
        stubPackedPlan(grouping, new int[] {0, 0, 1, 0}, new int[] {0, 2});
        CountingTransientSelector selector = new CountingTransientSelector(null);
        StreamRecord<?>[] records =
                new StreamRecord<?>[] {
                    new StreamRecord<>("a"),
                    new StreamRecord<>("a"),
                    new StreamRecord<>("b"),
                    new StreamRecord<>("a")
                };
        MailboxStableKeySidecar sidecar = new MailboxStableKeySidecar(records.length);
        List<TestKey> keys = new ArrayList<>();

        assertTrue(
                StatePrefetcher.extractRepresentativeStableKeys(
                        backend,
                        selector,
                        selector,
                        records,
                        0,
                        records.length,
                        keys,
                        sidecar));

        assertEquals(Arrays.asList(new TestKey("a", null), new TestKey("b", null)), keys);
        assertEquals(2, selector.stableCalls);
        assertEquals(6, selector.transientCalls);
        assertTrue(sidecar.isReady(0, selector));
        assertFalse(sidecar.isReady(1, selector));
        assertTrue(sidecar.isReady(2, selector));
        assertFalse(sidecar.isReady(3, selector));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testRepresentativeExtractionRejectsCollisionAndFallbackCapturesAllSlots() {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings().extraInterfaces(BatchKeyGroupingSupport.class));
        BatchKeyGroupingSupport grouping = (BatchKeyGroupingSupport) backend;
        stubPackedPlan(grouping, new int[] {0, 0}, new int[] {0});
        CountingTransientSelector selector = new CountingTransientSelector(7);
        StreamRecord<?>[] records =
                new StreamRecord<?>[] {
                    new StreamRecord<>("left"), new StreamRecord<>("right")
                };
        MailboxStableKeySidecar sidecar = new MailboxStableKeySidecar(records.length);
        List<TestKey> keys = new ArrayList<>();

        assertFalse(
                StatePrefetcher.extractRepresentativeStableKeys(
                        backend,
                        selector,
                        selector,
                        records,
                        0,
                        records.length,
                        keys,
                        sidecar));
        assertTrue(keys.isEmpty());
        assertFalse(sidecar.isReady(0, selector));
        assertFalse(sidecar.isReady(1, selector));

        assertTrue(
                StatePrefetcher.extractKeys(
                        selector, records, 0, records.length, keys, sidecar));
        assertEquals(
                Arrays.asList(new TestKey("left", 7), new TestKey("right", 7)), keys);
        assertTrue(sidecar.isReady(0, selector));
        assertTrue(sidecar.isReady(1, selector));
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

    @Test
    @SuppressWarnings("unchecked")
    void testCrossKeyPipelineLookaheadUsesBoundedJobScopedBackendCapability() {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings().extraInterfaces(BatchKeyGroupingSupport.class));
        BatchKeyGroupingSupport grouping = (BatchKeyGroupingSupport) backend;

        org.mockito.Mockito.when(grouping.crossKeyPipelineLookaheadGroups()).thenReturn(4, 99, -1);

        assertEquals(4, StatePrefetcher.crossKeyPipelineLookaheadGroups(backend));
        assertEquals(32, StatePrefetcher.crossKeyPipelineLookaheadGroups(backend));
        assertEquals(0, StatePrefetcher.crossKeyPipelineLookaheadGroups(backend));
        verify(grouping, org.mockito.Mockito.times(3)).crossKeyPipelineLookaheadGroups();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCrossKeyPipelineWaveLimitIsBoundedToTwo() {
        KeyedStateBackend<Object> backend =
                mock(
                        KeyedStateBackend.class,
                        withSettings().extraInterfaces(BatchKeyGroupingSupport.class));
        BatchKeyGroupingSupport grouping = (BatchKeyGroupingSupport) backend;

        org.mockito.Mockito.when(grouping.crossKeyPipelineWaveLimit()).thenReturn(2, 99, -1);

        assertEquals(2, StatePrefetcher.crossKeyPipelineWaveLimit(backend));
        assertEquals(2, StatePrefetcher.crossKeyPipelineWaveLimit(backend));
        assertEquals(1, StatePrefetcher.crossKeyPipelineWaveLimit(backend));
        verify(grouping, org.mockito.Mockito.times(3)).crossKeyPipelineWaveLimit();
    }

    private static void stubPackedPlan(
            BatchKeyGroupingSupport grouping, int[] sourceGroups, int[] firstSources) {
        org.mockito.Mockito.when(grouping.maxGroupingEntries()).thenReturn(512);
        org.mockito.Mockito.when(
                        grouping.groupHashTokens(
                                org.mockito.ArgumentMatchers.any(ByteBuffer.class),
                                org.mockito.ArgumentMatchers.eq(sourceGroups.length),
                                org.mockito.ArgumentMatchers.any(ByteBuffer.class)))
                .thenAnswer(
                        invocation -> {
                            ByteBuffer plan = invocation.getArgument(2);
                            int groupCount = firstSources.length;
                            int firstBase = BatchKeyGroupingSupport.PACKED_PLAN_HEADER_BYTES;
                            int offsetsBase = firstBase + groupCount * Integer.BYTES;
                            int sourceGroupsBase =
                                    offsetsBase + (groupCount + 1) * Integer.BYTES;
                            int[] counts = new int[groupCount];
                            for (int sourceGroup : sourceGroups) {
                                counts[sourceGroup]++;
                            }
                            int offset = 0;
                            for (int group = 0; group < groupCount; group++) {
                                plan.putInt(firstBase + group * Integer.BYTES, firstSources[group]);
                                plan.putInt(offsetsBase + group * Integer.BYTES, offset);
                                offset += counts[group];
                            }
                            plan.putInt(offsetsBase + groupCount * Integer.BYTES, offset);
                            for (int source = 0; source < sourceGroups.length; source++) {
                                plan.putInt(
                                        sourceGroupsBase + source * Integer.BYTES,
                                        sourceGroups[source]);
                            }
                            plan.putInt(Integer.BYTES, BatchKeyGroupingSupport.PACKED_PLAN_VERSION);
                            plan.putInt(2 * Integer.BYTES, sourceGroups.length);
                            plan.putInt(3 * Integer.BYTES, groupCount);
                            plan.putInt(0, BatchKeyGroupingSupport.PACKED_PLAN_MAGIC);
                            return groupCount;
                        });
    }

    private static final class CountingTransientSelector
            implements TransientKeySelector<String, TestKey> {
        private final TestKey reusable = new TestKey("", null);
        private final Integer forcedHash;
        private int stableCalls;
        private int transientCalls;

        private CountingTransientSelector(Integer forcedHash) {
            this.forcedHash = forcedHash;
        }

        @Override
        public TestKey getKey(String value) {
            stableCalls++;
            return new TestKey(value, forcedHash);
        }

        @Override
        public TestKey getTransientKey(String value) {
            transientCalls++;
            reusable.value = value;
            return reusable;
        }
    }

    private static final class TestKey {
        private String value;
        private final Integer forcedHash;

        private TestKey(String value, Integer forcedHash) {
            this.value = value;
            this.forcedHash = forcedHash;
        }

        @Override
        public int hashCode() {
            return forcedHash == null ? value.hashCode() : forcedHash;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof TestKey && value.equals(((TestKey) other).value);
        }

        @Override
        public String toString() {
            return value;
        }
    }

    public interface ImmediatePrefetchHook {
        void prefetchForImmediateUse(Collection<?> keys);
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

    public interface ExactNamespaceCapability {
        boolean exactNamespacePrefetchEnabled();
    }

    public interface ExactNamespacePrefetchHook {
        void prefetchKeyNamespaces(Collection<?> keys, Collection<?> namespaces);
    }
}
