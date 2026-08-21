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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

class StatePrefetcherTest {

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

    public interface NativeMailboxHook {
        boolean nativeMailboxBatchEnabled();
    }

    public interface NativePreaggHook {
        int[] nativePreaggGroupIds(List<?> keys);
    }
}
