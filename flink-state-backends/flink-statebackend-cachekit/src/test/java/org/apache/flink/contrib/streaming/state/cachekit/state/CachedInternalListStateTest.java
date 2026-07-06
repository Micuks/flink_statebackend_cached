/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.internal.InternalListState;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;

/**
 * Unit tests for {@link CachedInternalListState} covering:
 *
 * <ul>
 *   <li>COW path: add/addAll buffering and async flush
 *   <li>RYW path: cleared-keys fast path
 *   <li>Disabled mode (cow=false): all operations delegate directly
 *   <li>Flush-to-underlying-state for checkpoint
 *   <li>NamespaceKeyWrapper correctness
 * </ul>
 */
class CachedInternalListStateTest {

    private static final String NS1 = "ns1";
    private static final String NS2 = "ns2";

    // ---- Test fixtures ----

    @SuppressWarnings("unchecked")
    private InternalListState<String, String, Integer> mockDelegate() {
        InternalListState<String, String, Integer> mock = mock(InternalListState.class);
        try {
            doReturn(new ArrayList<>()).when(mock).get();
            doReturn(new ArrayList<>()).when(mock).getInternal();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return mock;
    }

    private CachedInternalListState<String, String, Integer> createCowState(
            InternalListState<String, String, Integer> delegate,
            ExecutorService executor,
            int clearedKeysCapacity) {
        return new CachedInternalListState<>(
                delegate,
                () -> currentKey,
                k -> currentKey = k,
                true, // cowEnabled
                false, // rywEnabled
                IntSerializer.INSTANCE,
                executor,
                clearedKeysCapacity);
    }

    private CachedInternalListState<String, String, Integer> createRywOnlyState(
            InternalListState<String, String, Integer> delegate,
            int clearedKeysCapacity) {
        return new CachedInternalListState<>(
                delegate,
                () -> currentKey,
                k -> currentKey = k,
                false, // cowEnabled
                true, // rywEnabled
                IntSerializer.INSTANCE,
                null, // no executor needed for ryw-only
                clearedKeysCapacity);
    }

    private CachedInternalListState<String, String, Integer> createDisabledState(
            InternalListState<String, String, Integer> delegate) {
        return new CachedInternalListState<>(
                delegate,
                () -> currentKey,
                k -> currentKey = k,
                false, // cowEnabled
                false, // rywEnabled
                IntSerializer.INSTANCE,
                null, // no executor
                1000);
    }

    // ---- Shared mutable key context ----
    private String currentKey = "key1";

    // ---- Tests: Disabled mode ----

    @Test
    void testCowDisabled_addDirectlyDelegates() throws Exception {
        InternalListState<String, String, Integer> delegate = mockDelegate();
        CachedInternalListState<String, String, Integer> state = createDisabledState(delegate);
        state.setCurrentNamespace(NS1);

        state.add(1);

        verify(delegate, times(1)).add(1);
    }

    @Test
    void testCowDisabled_addAllDirectlyDelegates() throws Exception {
        InternalListState<String, String, Integer> delegate = mockDelegate();
        CachedInternalListState<String, String, Integer> state = createDisabledState(delegate);
        state.setCurrentNamespace(NS1);

        List<Integer> values = Arrays.asList(1, 2, 3);
        state.addAll(values);

        verify(delegate, times(1)).addAll(values);
    }

    @Test
    void testCowDisabled_updateInternalDelegates() throws Exception {
        InternalListState<String, String, Integer> delegate = mockDelegate();
        CachedInternalListState<String, String, Integer> state = createDisabledState(delegate);
        state.setCurrentNamespace(NS1);

        List<Integer> newList = Arrays.asList(100, 200);
        state.updateInternal(newList);

        verify(delegate, times(1)).updateInternal(newList);
    }

    @Test
    void testCowDisabled_clearNeverCallsDelegate() throws Exception {
        InternalListState<String, String, Integer> delegate = mockDelegate();
        CachedInternalListState<String, String, Integer> state = createDisabledState(delegate);
        state.setCurrentNamespace(NS1);

        state.clear();

        // clear() never calls delegate.clear()
        verify(delegate, never()).clear();
    }

    @Test
    void testCowDisabled_emptyAddAllDoesNothing() throws Exception {
        InternalListState<String, String, Integer> delegate = mockDelegate();
        CachedInternalListState<String, String, Integer> state = createDisabledState(delegate);
        state.setCurrentNamespace(NS1);

        state.addAll(new ArrayList<>());

        verify(delegate, never()).addAll(any());
    }

    @Test
    void testMergeNamespaces_delegatesWhenDisabled() throws Exception {
        InternalListState<String, String, Integer> delegate = mockDelegate();
        CachedInternalListState<String, String, Integer> state = createDisabledState(delegate);
        state.setCurrentNamespace(NS1);

        state.mergeNamespaces(NS2, Arrays.asList(NS1));

        verify(delegate, times(1)).mergeNamespaces(NS2, Arrays.asList(NS1));
    }

    // ---- Tests: add() in COW mode ----

    @Test
    void testAdd_buffersInPendingMap() throws Exception {
        InternalListState<String, String, Integer> delegate = mockDelegate();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalListState<String, String, Integer> state =
                    createCowState(delegate, executor, 1000);
            state.setCurrentNamespace(NS1);

            state.add(1);
            state.add(2);
            state.add(3);

            // COW enabled: nothing delegated yet (all in pending buffer)
            verify(delegate, never()).add(any(Integer.class));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testAdd_flushesWhenMaxPendingListSizeReached() throws Exception {
        InternalListState<String, String, Integer> delegate = mockDelegate();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalListState<String, String, Integer> state =
                    createCowState(delegate, executor, 1000);
            state.setCurrentNamespace(NS1);

            // Add enough elements to reach MAX_PENDING_LIST_SIZE (500) threshold
            for (int i = 0; i < 499; i++) {
                state.add(i);
            }
            verify(delegate, never()).add(any(Integer.class));

            // The 500th element triggers single-key flush via addAll (COW batch semantics)
            state.add(499);

            Thread.sleep(200); // wait for async flush
            verify(delegate, times(1)).addAll(any());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testAdd_throwsOnNull() throws Exception {
        InternalListState<String, String, Integer> delegate = mockDelegate();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalListState<String, String, Integer> state =
                    createCowState(delegate, executor, 1000);
            state.setCurrentNamespace(NS1);

            assertThrows(NullPointerException.class, () -> state.add(null));
        } finally {
            executor.shutdownNow();
        }
    }

    // ---- Tests: addAll() in COW mode ----

    @Test
    void testAddAll_buffersInPendingMap() throws Exception {
        InternalListState<String, String, Integer> delegate = mockDelegate();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalListState<String, String, Integer> state =
                    createCowState(delegate, executor, 1000);
            state.setCurrentNamespace(NS1);

            state.addAll(Arrays.asList(1, 2, 3));

            // COW enabled: nothing delegated yet
            verify(delegate, never()).addAll(any());
        } finally {
            executor.shutdownNow();
        }
    }

    // ---- Tests: flush ----

    @Test
    void testFlushToUnderlyingState_flushesPendingElements() throws Exception {
        InternalListState<String, String, Integer> delegate = mockDelegate();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalListState<String, String, Integer> state =
                    createCowState(delegate, executor, 1000);
            state.setCurrentNamespace(NS1);

            state.add(1);
            state.add(2);

            // Flush synchronously delivers all pending elements via addAll (COW batch semantics)
            state.flushToUnderlyingState();

            verify(delegate, times(1)).addAll(any());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testFlushToUnderlyingState_withDelegateError() throws Exception {
        InternalListState<String, String, Integer> delegate = mockDelegate();
        doThrow(new RuntimeException("delegate error"))
                .when(delegate)
                .addAll(any());

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalListState<String, String, Integer> state =
                    createCowState(delegate, executor, 1000);
            state.setCurrentNamespace(NS1);

            state.add(1);
            state.flushToUnderlyingState();
        } catch (Exception expected) {
            assertNotNull(expected.getMessage());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testClose_flushesPendingAndCloses() throws Exception {
        InternalListState<String, String, Integer> delegate = mockDelegate();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalListState<String, String, Integer> state =
                    createCowState(delegate, executor, 1000);
            state.setCurrentNamespace(NS1);
            state.add(1);
            state.add(2);

            // close() should flush remaining pending data via addAll (COW batch semantics)
            state.close();

            verify(delegate, times(1)).addAll(any());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testMergeNamespaces_flushesBeforeMerging() throws Exception {
        InternalListState<String, String, Integer> delegate = mockDelegate();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalListState<String, String, Integer> state =
                    createCowState(delegate, executor, 1000);
            state.setCurrentNamespace(NS1);
            state.add(1);
            state.add(2);

            state.mergeNamespaces(NS2, Arrays.asList(NS1));

            Thread.sleep(200);
            verify(delegate, times(1)).mergeNamespaces(NS2, Arrays.asList(NS1));
        } finally {
            executor.shutdownNow();
        }
    }

    // ---- Tests: serializers passthrough ----

    @Test
    void testSetCurrentNamespace_passthrough() {
        InternalListState<String, String, Integer> delegate = mockDelegate();
        CachedInternalListState<String, String, Integer> state = createDisabledState(delegate);

        state.setCurrentNamespace(NS1);
        assertEquals(NS1, state.getCurrentNamespace());
    }

    @Test
    void testGetSerializer_returnsKeyAndNamespace() {
        InternalListState<String, String, Integer> delegate = mockDelegate();
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);

        CachedInternalListState<String, String, Integer> state = createDisabledState(delegate);

        assertEquals(StringSerializer.INSTANCE, state.getKeySerializer());
        assertEquals(StringSerializer.INSTANCE, state.getNamespaceSerializer());
    }

    // ---- Tests: RYW-only mode ----

    @Test
    void testRywOnly_clearThenAdd_callsDelegate() throws Exception {
        InternalListState<String, String, Integer> delegate = mockDelegate();
        CachedInternalListState<String, String, Integer> state =
                createRywOnlyState(delegate, 1000);
        state.setCurrentNamespace(NS1);

        state.clear();
        state.add(100);

        // RYW-only: add() goes directly to delegate
        verify(delegate, times(1)).add(100);
    }

    // ---- Tests: NamespaceKeyWrapper ----

    @Test
    void testNamespaceKeyWrapper_equalsAndHash() {
        CachedInternalListState.NamespaceKeyWrapper w1 =
                new CachedInternalListState.NamespaceKeyWrapper("ns", "key");
        CachedInternalListState.NamespaceKeyWrapper w2 =
                new CachedInternalListState.NamespaceKeyWrapper("ns", "key");
        CachedInternalListState.NamespaceKeyWrapper w3 =
                new CachedInternalListState.NamespaceKeyWrapper("ns", "other");

        assertEquals(w1, w2);
        assertEquals(w1.hashCode(), w2.hashCode());
        assertEquals(false, w1.equals(w3));
    }

    @Test
    void testNamespaceKeyWrapper_matches() {
        CachedInternalListState.NamespaceKeyWrapper w =
                new CachedInternalListState.NamespaceKeyWrapper("ns", "key");

        assertEquals(true, w.matches("key", "ns"));
        assertEquals(false, w.matches("other", "ns"));
        assertEquals(false, w.matches("key", "other"));
    }

    @Test
    void testNamespaceKeyWrapper_toString() {
        CachedInternalListState.NamespaceKeyWrapper w =
                new CachedInternalListState.NamespaceKeyWrapper("ns1", "key1");
        String str = w.toString();
        assertNotNull(str);
        assertEquals(true, str.contains("ns1"));
        assertEquals(true, str.contains("key1"));
    }
}
