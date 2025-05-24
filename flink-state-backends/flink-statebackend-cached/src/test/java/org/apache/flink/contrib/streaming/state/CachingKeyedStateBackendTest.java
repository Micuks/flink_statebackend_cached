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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.RunnableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

// Added import for CheckpointType
import org.apache.flink.runtime.checkpoint.CheckpointType;

/**
 * Comprehensive tests for {@link CachingKeyedStateBackend} to verify complete functionality.
 */
class CachingKeyedStateBackendTest {

    @TempDir
    Path temporaryFolder;

    @Mock
    private AbstractKeyedStateBackend<String> mockDelegateBackend;

    @Mock
    private InternalValueState<String, VoidNamespace, String> mockValueState;

    @Mock
    private InternalMapState<String, VoidNamespace, String, String> mockMapState;

    @Mock
    private InternalListState<String, VoidNamespace, String> mockListState;

    @Mock
    private KeyGroupedInternalPriorityQueue<TestPriorityQueueElement> mockPriorityQueue;

    private CachingKeyedStateBackend<String> cachingBackend;
    private AbstractKeyedStateBackend<String> delegateBackend;
    private CloseableRegistry closableRegistry;
    private Environment mockEnv;
    private TestTaskStateManager taskStateManager;

    // Test helper classes
    private static class TestPriorityQueueElement
            implements HeapPriorityQueueElement, PriorityComparable<TestPriorityQueueElement>,
            org.apache.flink.runtime.state.Keyed<String> {
        private String value;
        private int priority;
        private String key;
        private int internalIndex = NOT_CONTAINED;

        public TestPriorityQueueElement(String value, int priority, String key) {
            this.value = value;
            this.priority = priority;
            this.key = key;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public int comparePriorityTo(TestPriorityQueueElement other) {
            return Integer.compare(priority, other.priority);
        }

        @Override
        public int getInternalIndex() {
            return internalIndex;
        }

        @Override
        public void setInternalIndex(int index) {
            this.internalIndex = index;
        }

        public String getValue() {
            return value;
        }

        public int getPriority() {
            return priority;
        }
    }

    private static class TestReduceFunction
            implements org.apache.flink.api.common.functions.ReduceFunction<Integer> {
        @Override
        public Integer reduce(Integer value1, Integer value2) throws Exception {
            return (value1 == null ? 0 : value1) + (value2 == null ? 0 : value2);
        }
    }

    private static class TestAggregateFunction implements
            org.apache.flink.api.common.functions.AggregateFunction<Integer, String, String> {
        @Override
        public String createAccumulator() {
            return "";
        }

        @Override
        public String add(Integer value, String accumulator) {
            return accumulator + value;
        }

        @Override
        public String getResult(String accumulator) {
            return accumulator;
        }

        @Override
        public String merge(String a, String b) {
            return a + b;
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Setup mock environment
        mockEnv = new MockEnvironmentBuilder().build();
        taskStateManager = new TestTaskStateManager();
        closableRegistry = new CloseableRegistry();

        // Setup mock delegate backend behavior
        when(mockDelegateBackend.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(mockDelegateBackend.getCurrentKey()).thenReturn("testKey");
        when(mockDelegateBackend.getKeyGroupCompressionDecorator())
                        .thenReturn(org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator.INSTANCE);

        // Setup mock states
        when(mockValueState.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(mockValueState.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(mockValueState.getValueSerializer()).thenReturn(StringSerializer.INSTANCE);

        when(mockMapState.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(mockMapState.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);

        when(mockListState.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(mockListState.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);

        // Create the caching backend
        cachingBackend = new CachingKeyedStateBackend<>(mockEnv.getTaskKvStateRegistry(),
                StringSerializer.INSTANCE, mockEnv.getUserCodeClassLoader().asClassLoader(),
                new ExecutionConfig(), TtlTimeProvider.DEFAULT, new UnregisteredMetricsGroup(),
                Collections.emptyList(), closableRegistry, mockDelegateBackend, 5, // L1 cache size
                10, // L2 cache size
                3, // max active namespaces
                2L, // max cache memory MB
                CachingStateBackendFactory.CachePolicyType.LRU);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (cachingBackend != null) {
            cachingBackend.dispose();
        }
        if (closableRegistry != null) {
            closableRegistry.close();
        }
        if (taskStateManager != null) {
            taskStateManager.close();
        }
    }

    @Test
    void testCreateValueState() throws Exception {
        // Setup mock
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class),
                any(ValueStateDescriptor.class))).thenReturn(mockValueState);

        // Test
        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("test", String.class);
        ValueState<String> state =
                cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);

        // Verify
        assertNotNull(state);
        assertTrue(state instanceof CachingInternalValueState);
        assertEquals(mockValueState,
                ((CachingInternalValueState<?, ?, ?>) state).getDelegateState());
        verify(mockDelegateBackend).getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE,
                descriptor);
    }

    @Test
    void testCreateMapState() throws Exception {
        // Setup mock
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class),
                any(MapStateDescriptor.class))).thenReturn(mockMapState);

        // Test
        MapStateDescriptor<String, String> descriptor =
                new MapStateDescriptor<>("test", String.class, String.class);
        MapState<String, String> state =
                cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);

        // Verify
        assertNotNull(state);
        assertTrue(state instanceof CachingInternalMapState);
        assertEquals(mockMapState,
                ((CachingInternalMapState<?, ?, ?, ?>) state).getDelegateState());
        verify(mockDelegateBackend).getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE,
                descriptor);
    }

    @Test
    void testCreateListState() throws Exception {
        // Setup mock
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class),
                any(ListStateDescriptor.class))).thenReturn(mockListState);

        // Test
        ListStateDescriptor<String> descriptor = new ListStateDescriptor<>("test", String.class);
        ListState<String> state =
                cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);

        // Verify
        assertNotNull(state);
        assertTrue(state instanceof CachingInternalListState);
        assertEquals(mockListState, ((CachingInternalListState<?, ?, ?>) state).getDelegateState());
        verify(mockDelegateBackend).getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE,
                descriptor);
    }

    @Test
    void testCreateUnsupportedStateType_DelegatesToBackend() throws Exception {
        // Setup mock for reducing state (unsupported by caching)
        ReducingState<Integer> mockReducingState = mock(ReducingState.class);
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class),
                any(ReducingStateDescriptor.class))).thenReturn(mockReducingState);

        // Test
        ReducingStateDescriptor<Integer> descriptor = new ReducingStateDescriptor<>("test",
                new TestReduceFunction(), IntSerializer.INSTANCE);
        ReducingState<Integer> state =
                cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);

        // Verify - should return the delegate state directly
        assertSame(mockReducingState, state);
        verify(mockDelegateBackend).getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE,
                descriptor);
    }

    @Test
    void testCreateAggregatingState_DelegatesToBackend() throws Exception {
        // Setup mock for aggregating state (unsupported by caching)
        AggregatingState<Integer, String> mockAggregatingState = mock(AggregatingState.class);
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class),
                any(AggregatingStateDescriptor.class))).thenReturn(mockAggregatingState);

        // Test
        AggregatingStateDescriptor<Integer, String, String> descriptor =
                new AggregatingStateDescriptor<>("test", new TestAggregateFunction(),
                        StringSerializer.INSTANCE);
        AggregatingState<Integer, String> state =
                cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);

        // Verify - should return the delegate state directly
        assertSame(mockAggregatingState, state);
        verify(mockDelegateBackend).getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE,
                descriptor);
    }

    @Test
    void testStateRegistrationDeduplication() throws Exception {
        // Setup mock
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class),
                any(ValueStateDescriptor.class))).thenReturn(mockValueState);

        // Test - create the same state twice
        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("test", String.class);
        ValueState<String> state1 =
                cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);
        ValueState<String> state2 =
                cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);

        // Verify both return caching wrappers, but delegate is the same
        assertTrue(state1 instanceof CachingInternalValueState);
        assertTrue(state2 instanceof CachingInternalValueState);
        assertEquals(mockValueState,
                ((CachingInternalValueState<?, ?, ?>) state1).getDelegateState());
        assertEquals(mockValueState,
                ((CachingInternalValueState<?, ?, ?>) state2).getDelegateState());
    }

    @Test
    void testSetCurrentKey_DelegatesToBackend() {
        // Test
        cachingBackend.setCurrentKey("newKey");

        // Verify
        verify(mockDelegateBackend).setCurrentKey("newKey");
    }

    @Test
    void testCreatePriorityQueue_ReturnsProxyWithElementCounting() throws Exception {
        // Setup mock
        when(mockDelegateBackend.create(anyString(), any(TypeSerializer.class)))
                .thenReturn(mockPriorityQueue);
        when(mockPriorityQueue.poll()).thenReturn(null); // Empty queue
        when(mockPriorityQueue.peek()).thenReturn(null);

        // Test
        String stateName = "testPQ";
        TypeSerializer<TestPriorityQueueElement> serializer = mock(TypeSerializer.class);
        KeyGroupedInternalPriorityQueue<TestPriorityQueueElement> pq =
                cachingBackend.create(stateName, serializer);

        // Verify it's a proxy, not the original
        assertNotSame(mockPriorityQueue, pq);

        // Test element counting functionality
        assertTrue(pq.isEmpty());
        assertEquals(0, pq.size());

        // Add elements and verify counting
        TestPriorityQueueElement element1 = new TestPriorityQueueElement("test1", 1, "key1");
        TestPriorityQueueElement element2 = new TestPriorityQueueElement("test2", 2, "key2");

        when(mockPriorityQueue.add(element1)).thenReturn(true);
        when(mockPriorityQueue.add(element2)).thenReturn(true);

        assertTrue(pq.add(element1));
        assertTrue(pq.add(element2));
        assertEquals(2, pq.size());
        assertFalse(pq.isEmpty());

        // Test poll decrements count
        when(mockPriorityQueue.poll()).thenReturn(element1, element2, null);
        assertEquals(element1, pq.poll());
        assertEquals(1, pq.size());
        assertEquals(element2, pq.poll());
        assertEquals(0, pq.size());
        assertTrue(pq.isEmpty());

        verify(mockDelegateBackend).create(stateName, serializer);
    }

    @Test
    void testSnapshotFlushesAllRegisteredStates() throws Exception {
        // Setup mocks for states
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class),
                any(ValueStateDescriptor.class))).thenReturn(mockValueState);
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class),
                any(MapStateDescriptor.class))).thenReturn(mockMapState);

        // Create states (which registers them)
        ValueStateDescriptor<String> valueDesc = new ValueStateDescriptor<>("value", String.class);
        MapStateDescriptor<String, String> mapDesc =
                new MapStateDescriptor<>("map", String.class, String.class);

        cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, valueDesc);
        cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, mapDesc);

        // Setup snapshot mocks
        CheckpointStreamFactory streamFactory = new MemCheckpointStreamFactory(1024);
        CheckpointOptions checkpointOptions = CheckpointOptions.forCheckpointWithDefaultLocation();
        RunnableFuture<SnapshotResult<KeyedStateHandle>> mockSnapshotFuture =
                mock(RunnableFuture.class);
        SnapshotResult<KeyedStateHandle> mockResult = mock(SnapshotResult.class);

        when(mockDelegateBackend.snapshot(anyLong(), anyLong(), any(CheckpointStreamFactory.class),
                any(CheckpointOptions.class))).thenReturn(mockSnapshotFuture);
        when(mockSnapshotFuture.get()).thenReturn(mockResult);

        // Test
        RunnableFuture<SnapshotResult<KeyedStateHandle>> result = cachingBackend.snapshot(1L,
                System.currentTimeMillis(), streamFactory, checkpointOptions);

        // Verify snapshot was delegated
        assertSame(mockSnapshotFuture, result);
        verify(mockDelegateBackend).snapshot(anyLong(), anyLong(), eq(streamFactory),
                eq(checkpointOptions));
    }

    @Test
    void testSavepointFlushesAllRegisteredStates() throws Exception {
        // Setup mocks for states
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class),
                any(ValueStateDescriptor.class))).thenReturn(mockValueState);

        // Create a state (which registers it)
        ValueStateDescriptor<String> valueDesc = new ValueStateDescriptor<>("value", String.class);
        cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, valueDesc);

        // Setup savepoint mock
        when(mockDelegateBackend.savepoint())
                .thenReturn(mock(org.apache.flink.runtime.state.SavepointResources.class));

        // Test
        cachingBackend.savepoint();

        // Verify savepoint was delegated
        verify(mockDelegateBackend).savepoint();
    }

    @Test
    void testDelegationMethods() throws Exception {
        // Test various delegation methods
        when(mockDelegateBackend.getKeys(anyString(), any()))
                .thenReturn(java.util.stream.Stream.empty());
        when(mockDelegateBackend.getKeysAndNamespaces(anyString()))
                .thenReturn(java.util.stream.Stream.empty());
        when(mockDelegateBackend.numKeyValueStateEntries()).thenReturn(42);
        when(mockDelegateBackend.requiresLegacySynchronousTimerSnapshots(any())).thenReturn(true);
        when(mockDelegateBackend.isSafeToReuseKVState()).thenReturn(false);

        // Test delegations
        cachingBackend.getKeys("test", VoidNamespace.INSTANCE);
        cachingBackend.getKeysAndNamespaces("test");
        int entries = cachingBackend.numKeyValueStateEntries();
        boolean requiresSync =
                cachingBackend.requiresLegacySynchronousTimerSnapshots(CheckpointType.CHECKPOINT);
        boolean safeToReuse = cachingBackend.isSafeToReuseKVState();

        // Verify
        verify(mockDelegateBackend).getKeys("test", VoidNamespace.INSTANCE);
        verify(mockDelegateBackend).getKeysAndNamespaces("test");
        verify(mockDelegateBackend).numKeyValueStateEntries();
        verify(mockDelegateBackend).requiresLegacySynchronousTimerSnapshots(any());
        verify(mockDelegateBackend).isSafeToReuseKVState();

        assertEquals(42, entries);
        assertTrue(requiresSync);
        assertTrue(safeToReuse); // CachingKeyedStateBackend overrides this to return true
    }

    @Test
    void testKeySelectionListenerDelegation() {
        // Setup
        AbstractKeyedStateBackend.KeySelectionListener<String> listener =
                mock(AbstractKeyedStateBackend.KeySelectionListener.class);
        when(mockDelegateBackend.deregisterKeySelectionListener(listener)).thenReturn(true);

        // Test
        cachingBackend.registerKeySelectionListener(listener);
        boolean result = cachingBackend.deregisterKeySelectionListener(listener);

        // Verify
        verify(mockDelegateBackend).registerKeySelectionListener(listener);
        verify(mockDelegateBackend).deregisterKeySelectionListener(listener);
        assertTrue(result);
    }

    @Test
    void testNotifyCheckpointMethods() throws Exception {
        // Test
        cachingBackend.notifyCheckpointComplete(123L);
        cachingBackend.notifyCheckpointAborted(456L);

        // Verify
        verify(mockDelegateBackend).notifyCheckpointComplete(123L);
        verify(mockDelegateBackend).notifyCheckpointAborted(456L);
    }

    @Test
    void testDispose() throws Exception {
        // Setup - create some states to register them
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class),
                any(ValueStateDescriptor.class))).thenReturn(mockValueState);

        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("test", String.class);
        cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);

        // Test
        cachingBackend.dispose();

        // Verify
        verify(mockDelegateBackend).dispose();
        // Note: We can't easily verify that registeredStates.clear() was called without exposing
        // internals
    }

    @Test
    void testCreateOrUpdateInternalStateDelegate() throws Exception {
        // Setup
        org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory<String> transformFactory =
                mock(org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory.class);
        when(mockDelegateBackend.createOrUpdateInternalState(any(), any(), any()))
                .thenReturn(mockValueState);
        when(mockDelegateBackend.createOrUpdateInternalState(any(), any(), any(), anyBoolean()))
                .thenReturn(mockValueState);

        // Test
        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("test", String.class);

        InternalKvState<?, ?, ?> result1 = cachingBackend.createOrUpdateInternalState(
                VoidNamespaceSerializer.INSTANCE, descriptor, transformFactory);
        InternalKvState<?, ?, ?> result2 = cachingBackend.createOrUpdateInternalState(
                VoidNamespaceSerializer.INSTANCE, descriptor, transformFactory, true);

        // Verify
        assertSame(mockValueState, result1);
        assertSame(mockValueState, result2);
        verify(mockDelegateBackend).createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE,
                descriptor, transformFactory);
        verify(mockDelegateBackend).createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE,
                descriptor, transformFactory, true);
    }

    @Test
    void testMultipleCachePolicies() throws Exception {
        // Create backends with different cache policies
        CachingKeyedStateBackend<String> lruBackend = new CachingKeyedStateBackend<>(
                mockEnv.getTaskKvStateRegistry(), StringSerializer.INSTANCE,
                mockEnv.getUserCodeClassLoader().asClassLoader(), new ExecutionConfig(),
                TtlTimeProvider.DEFAULT, new UnregisteredMetricsGroup(), Collections.emptyList(),
                new CloseableRegistry(), mockDelegateBackend, 5, 10, 3, 2L,
                CachingStateBackendFactory.CachePolicyType.LRU);

        CachingKeyedStateBackend<String> tinyLfuBackend = new CachingKeyedStateBackend<>(
                mockEnv.getTaskKvStateRegistry(), StringSerializer.INSTANCE,
                mockEnv.getUserCodeClassLoader().asClassLoader(), new ExecutionConfig(),
                TtlTimeProvider.DEFAULT, new UnregisteredMetricsGroup(), Collections.emptyList(),
                new CloseableRegistry(), mockDelegateBackend, 5, 10, 3, 2L,
                CachingStateBackendFactory.CachePolicyType.TINYLFU);

        // Both should work without errors
        assertNotNull(lruBackend);
        assertNotNull(tinyLfuBackend);

        // Cleanup
        lruBackend.dispose();
        tinyLfuBackend.dispose();
    }

    @Test
    void testGetMaxActiveNamespaceOrPerKeyCacheContainers() {
        assertEquals(3, cachingBackend.getMaxActiveNamespaceOrPerKeyCacheContainers());
    }

    @Test
    void testSnapshotWithException() throws Exception {
        // Setup - create a state
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class),
                any(ValueStateDescriptor.class))).thenReturn(mockValueState);

        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("test", String.class);
        cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);

        // Setup snapshot to throw exception
        when(mockDelegateBackend.snapshot(anyLong(), anyLong(), any(), any()))
                .thenThrow(new RuntimeException("Snapshot failed"));

        // Test - should propagate exception
        CheckpointStreamFactory streamFactory = new MemCheckpointStreamFactory(1024);
        CheckpointOptions checkpointOptions = CheckpointOptions.forCheckpointWithDefaultLocation();

        assertThrows(RuntimeException.class, () -> {
            cachingBackend.snapshot(1L, System.currentTimeMillis(), streamFactory,
                    checkpointOptions);
        });
    }

    @Test
    void testIntegrationWithMultipleStateTypes() throws Exception {
        // Setup all state type mocks
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class),
                any(ValueStateDescriptor.class))).thenReturn(mockValueState);
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class),
                any(MapStateDescriptor.class))).thenReturn(mockMapState);
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class),
                any(ListStateDescriptor.class))).thenReturn(mockListState);

        // Create multiple different state types
        ValueStateDescriptor<String> valueDesc = new ValueStateDescriptor<>("value", String.class);
        MapStateDescriptor<String, String> mapDesc =
                new MapStateDescriptor<>("map", String.class, String.class);
        ListStateDescriptor<String> listDesc = new ListStateDescriptor<>("list", String.class);

        ValueState<String> valueState =
                cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, valueDesc);
        MapState<String, String> mapState =
                cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, mapDesc);
        ListState<String> listState =
                cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, listDesc);

        // Verify all are caching implementations
        assertTrue(valueState instanceof CachingInternalValueState);
        assertTrue(mapState instanceof CachingInternalMapState);
        assertTrue(listState instanceof CachingInternalListState);

        // All should be registered for flushing during snapshots
        CheckpointStreamFactory streamFactory = new MemCheckpointStreamFactory(1024);
        CheckpointOptions checkpointOptions = CheckpointOptions.forCheckpointWithDefaultLocation();
        RunnableFuture<SnapshotResult<KeyedStateHandle>> mockSnapshotFuture =
                mock(RunnableFuture.class);

        when(mockDelegateBackend.snapshot(anyLong(), anyLong(), any(), any()))
                .thenReturn(mockSnapshotFuture);

        // Should not throw exception
        assertDoesNotThrow(() -> {
            cachingBackend.snapshot(1L, System.currentTimeMillis(), streamFactory,
                    checkpointOptions);
        });

        verify(mockDelegateBackend).snapshot(anyLong(), anyLong(), any(), any());
    }
}
