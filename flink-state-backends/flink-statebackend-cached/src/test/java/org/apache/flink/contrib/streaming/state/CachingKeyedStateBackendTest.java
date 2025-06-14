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

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RunnableFuture;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

// Added import for CheckpointType

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

    private static class TestPriorityQueueElementSerializer extends TypeSerializer<TestPriorityQueueElement> {
        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public TypeSerializer<TestPriorityQueueElement> duplicate() {
            return this;
        }

        @Override
        public TestPriorityQueueElement createInstance() {
            return new TestPriorityQueueElement("", 0, "");
        }

        @Override
        public TestPriorityQueueElement copy(TestPriorityQueueElement from) {
            return new TestPriorityQueueElement(from.value, from.priority, from.key);
        }

        @Override
        public TestPriorityQueueElement copy(
                TestPriorityQueueElement from, TestPriorityQueueElement reuse) {
            reuse.value = from.value;
            reuse.priority = from.priority;
            reuse.key = from.key;
            return reuse;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(TestPriorityQueueElement record, DataOutputView target)
                throws IOException {}

        @Override
        public TestPriorityQueueElement deserialize(DataInputView source) throws IOException {
            return null;
        }

        @Override
        public TestPriorityQueueElement deserialize(
                TestPriorityQueueElement reuse, DataInputView source) throws IOException {
            return null;
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {}

        @Override
        public boolean equals(Object obj) {
            return obj instanceof TestPriorityQueueElementSerializer;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public TypeSerializerSnapshot<TestPriorityQueueElement> snapshotConfiguration() {
            return null;
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
        when(mockDelegateBackend.getLatencyTrackingStateConfig())
                .thenReturn(LatencyTrackingStateConfig.disabled());

        // Create proper KeyGroupRange and numberOfKeyGroups for InternalKeyContextImpl
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 15);
        int numberOfKeyGroups = 16;
        lenient().when(mockDelegateBackend.getKeyContext()).thenReturn(
                        new org.apache.flink.runtime.state.heap.InternalKeyContextImpl<>(
                                        keyGroupRange, numberOfKeyGroups));

        // Setup mock states
        when(mockValueState.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(mockValueState.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(mockValueState.getValueSerializer()).thenReturn(StringSerializer.INSTANCE);

        when(mockMapState.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(mockMapState.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(mockMapState.getValueSerializer())
                        .thenReturn(new org.apache.flink.api.common.typeutils.base.MapSerializer<>(
                                        StringSerializer.INSTANCE, StringSerializer.INSTANCE));

        when(mockListState.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(mockListState.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);

        // Default values for new cache size parameters, align with factory defaults
        long mapL1KeyPresenceCacheSize = CachingStateBackendFactory.MAP_L1_KEY_PRESENCE_CACHE_SIZE_CONFIG.defaultValue();
        long mapL2KeyPresenceCacheSize = CachingStateBackendFactory.MAP_L2_KEY_PRESENCE_CACHE_SIZE_CONFIG.defaultValue();
        double mapCacheHitRateThreshold = CachingStateBackendFactory.MAP_CACHE_HIT_RATE_THRESHOLD_CONFIG.defaultValue();
        long mapCacheHitRateWindowSize = CachingStateBackendFactory.MAP_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG.defaultValue();
        long mapCacheMinAccessesForBypassCheck = CachingStateBackendFactory.MAP_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG.defaultValue();
        boolean mapKeyPresenceCacheEnabled = CachingStateBackendFactory.MAP_KEY_PRESENCE_CACHE_ENABLED_CONFIG.defaultValue();
        boolean mapBypassEnabled = CachingStateBackendFactory.MAP_BYPASS_ENABLED_CONFIG.defaultValue();

        double valueCacheHitRateThreshold = CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_THRESHOLD_CONFIG.defaultValue();
        long valueCacheHitRateWindowSize = CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG.defaultValue();
        long valueCacheMinAccessesForBypassCheck = CachingStateBackendFactory.VALUE_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG.defaultValue();
        boolean valueBypassEnabled = CachingStateBackendFactory.VALUE_BYPASS_ENABLED_CONFIG.defaultValue();
boolean writeBehindEnabled = CachingStateBackendFactory.WRITE_BEHIND_ENABLED_CONFIG.defaultValue();

        cachingBackend = new CachingKeyedStateBackend<String>(
            mockEnv.getTaskKvStateRegistry(),
            StringSerializer.INSTANCE,
            mockEnv.getUserCodeClassLoader().asClassLoader(),
            mockEnv.getExecutionConfig(),
            TtlTimeProvider.DEFAULT,
            Collections.emptyList(),
            closableRegistry,
            mockDelegateBackend,
            10, // L1 cache size
            20, // L2 cache size
            5,  // Max active namespaces
            1L, // Max cache memory MB
            CachingStateBackendFactory.CachePolicyType.LRU, // Default policy
            (int) mapL1KeyPresenceCacheSize,
            (int) mapL2KeyPresenceCacheSize,
            mapCacheHitRateThreshold,
            mapCacheHitRateWindowSize,
            mapCacheMinAccessesForBypassCheck,
            mapKeyPresenceCacheEnabled,
            mapBypassEnabled,
            valueCacheHitRateThreshold,
            valueCacheHitRateWindowSize,
            valueCacheMinAccessesForBypassCheck,
            valueBypassEnabled,
            writeBehindEnabled // writeBehindEnabled
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        if (cachingBackend != null) {
            cachingBackend.dispose();
        }
        closableRegistry.close();
        if (delegateBackend != null) {
            delegateBackend.dispose();
        }
        taskStateManager.close();
    }

    @Test
    void testCreateValueState() throws Exception {
        // This test verifies that a CachingInternalValueState is created.
        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("testState", String.class);
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class), any(ValueStateDescriptor.class)))
                .thenReturn(mockValueState);

        ValueState<String> createdState = cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);
        assertTrue(createdState instanceof CachingInternalValueState, "Should create a caching value state.");
        // Ensure that the delegate backend was called to create the underlying state
        verify(mockDelegateBackend, times(1)).getOrCreateKeyedState(any(TypeSerializer.class), eq(descriptor));
    }

    @Test
    void testCreateMapState() throws Exception {
        // This test verifies that a CachingInternalMapState is created.
        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<>("testState", String.class, String.class);
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class), any(MapStateDescriptor.class)))
                .thenReturn(mockMapState);

        MapState<String, String> createdState = cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);
        assertTrue(createdState instanceof CachingInternalMapState, "Should create a caching map state.");
        verify(mockDelegateBackend, times(1)).getOrCreateKeyedState(any(TypeSerializer.class), eq(descriptor));
    }

    @Test
    void testCreateListState() throws Exception {
        // This test verifies that a CachingInternalListState is created.
        ListStateDescriptor<String> descriptor = new ListStateDescriptor<>("testState", String.class);
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class), any(ListStateDescriptor.class)))
                .thenReturn(mockListState);

        ListState<String> createdState = cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);
        assertTrue(createdState instanceof CachingInternalListState, "Should create a caching list state.");
        verify(mockDelegateBackend, times(1)).getOrCreateKeyedState(any(TypeSerializer.class), eq(descriptor));
    }

    @Test
    void testCreateUnsupportedStateType_DelegatesToBackend() throws Exception {
        // This test verifies that for unsupported state types, creation is delegated.
        ReducingStateDescriptor<Integer> descriptor = new ReducingStateDescriptor<>("testState", new TestReduceFunction(), IntSerializer.INSTANCE);
        ReducingState<Integer> mockReducingState = mock(ReducingState.class);
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class), any(ReducingStateDescriptor.class)))
                .thenReturn(mockReducingState);

        ReducingState<Integer> createdState = cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);
        // Should not be a caching state wrapper
        assertFalse(createdState instanceof CachingInternalState, "Should not create a caching wrapper for unsupported state types.");
        assertSame(mockReducingState, createdState, "Should delegate state creation for unsupported types.");
        verify(mockDelegateBackend, times(1)).getOrCreateKeyedState(any(TypeSerializer.class), eq(descriptor));
    }

    @Test
    void testCreateAggregatingState_DelegatesToBackend() throws Exception {
        StateDescriptor<AggregatingState<Integer, String>, String> stateDescriptor =
                new AggregatingStateDescriptor<>("test-agg-state", new TestAggregateFunction(), String.class);
        when(mockDelegateBackend.getOrCreateKeyedState(any(), eq(stateDescriptor)))
                .thenReturn(mock(InternalAggregatingState.class));

        AggregatingState<Integer, String> state =
                cachingBackend.getOrCreateKeyedState(new VoidNamespaceSerializer(), stateDescriptor);

        assertNotNull(state);
        assertTrue(state instanceof CachingInternalAggregatingState);
    }

    @Test
    void testStateRegistrationDeduplication() throws Exception {
        // This test verifies that the same state instance is returned for the same descriptor.
        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("testState", String.class);
        when(mockDelegateBackend.getOrCreateKeyedState(any(TypeSerializer.class), any(ValueStateDescriptor.class)))
                .thenReturn(mockValueState);

        ValueState<String> state1 = cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);
        ValueState<String> state2 = cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);

        assertSame(state1, state2, "Should return the same state instance for the same descriptor.");
        // Delegate should only be called once for creation
        verify(mockDelegateBackend, times(1)).getOrCreateKeyedState(any(TypeSerializer.class), eq(descriptor));
    }

    @Test
    void testSetCurrentKey_DelegatesToBackend() {
        String testKey = "new-key";
        cachingBackend.setCurrentKey(testKey);
        verify(mockDelegateBackend, times(1)).setCurrentKey(testKey);
    }

    @Test
    void testCreatePriorityQueue_ReturnsProxyWithElementCounting() throws Exception {
        when(mockDelegateBackend.create(anyString(), any(TypeSerializer.class)))
                .thenReturn(mockPriorityQueue);

        KeyGroupedInternalPriorityQueue<TestPriorityQueueElement> priorityQueue =
                cachingBackend.create("test-pq", new TestPriorityQueueElementSerializer());

        assertNotNull(priorityQueue);
        assertTrue(priorityQueue instanceof CachingKeyGroupedInternalPriorityQueue);

        TestPriorityQueueElement element = new TestPriorityQueueElement("a", 1, "key");
        cachingBackend.setCurrentKey("key");
        priorityQueue.add(element);

        // The caching priority queue might buffer elements, so we trigger a flush via savepoint.
        when(mockDelegateBackend.savepoint()).thenReturn(mock(org.apache.flink.runtime.state.SavepointResources.class));
        cachingBackend.savepoint();

        verify(mockPriorityQueue, times(1)).add(element);
    }

    @Test
    void testSnapshotFlushesAllRegisteredStates() throws Exception {
        when(mockDelegateBackend.getOrCreateKeyedState(any(), any(ValueStateDescriptor.class))).thenReturn(mockValueState);
        when(mockDelegateBackend.getOrCreateKeyedState(any(), any(MapStateDescriptor.class))).thenReturn(mockMapState);
        when(mockDelegateBackend.getOrCreateKeyedState(any(), any(ListStateDescriptor.class))).thenReturn(mockListState);
        cachingBackend.getOrCreateKeyedState(
                new VoidNamespaceSerializer(),
                new ValueStateDescriptor<>("value", String.class));
        cachingBackend.getOrCreateKeyedState(
                new VoidNamespaceSerializer(),
                new MapStateDescriptor<>("map", String.class, String.class));
        cachingBackend.getOrCreateKeyedState(
                new VoidNamespaceSerializer(),
                new ListStateDescriptor<>("list", String.class));

        List<CachingInternalState<String, ?, ?, ?>> registeredStates = getRegisteredStates();
        assertEquals(3, registeredStates.size());

        List<CachingInternalState<String, ?, ?, ?>> spiedStates = new ArrayList<>();
        for (CachingInternalState<String, ?, ?, ?> state : registeredStates) {
            spiedStates.add(spy(state));
        }

        registeredStates.clear();
        registeredStates.addAll(spiedStates);

        @SuppressWarnings("unchecked")
        RunnableFuture<SnapshotResult<KeyedStateHandle>> mockFuture = mock(RunnableFuture.class);
        when(mockDelegateBackend.snapshot(anyLong(), anyLong(), any(), any())).thenReturn(mockFuture);

        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                cachingBackend.snapshot(
                        1L, 1L, mock(CheckpointStreamFactory.class), CheckpointOptions.forCheckpointWithDefaultLocation());

        snapshotFuture.run();
        snapshotFuture.get();

        for (CachingInternalState<String, ?, ?, ?> state : spiedStates) {
            verify(state, times(1)).flushToUnderlyingState();
        }
    }

    @Test
    void testSavepointFlushesAllRegisteredStates() throws Exception {
        when(mockDelegateBackend.getOrCreateKeyedState(any(), any(ValueStateDescriptor.class))).thenReturn(mockValueState);
        when(mockDelegateBackend.getOrCreateKeyedState(any(), any(MapStateDescriptor.class))).thenReturn(mockMapState);
        when(mockDelegateBackend.getOrCreateKeyedState(any(), any(ListStateDescriptor.class))).thenReturn(mockListState);
        cachingBackend.getOrCreateKeyedState(
                new VoidNamespaceSerializer(),
                new ValueStateDescriptor<>("value", String.class));
        cachingBackend.getOrCreateKeyedState(
                new VoidNamespaceSerializer(),
                new MapStateDescriptor<>("map", String.class, String.class));
        cachingBackend.getOrCreateKeyedState(
                new VoidNamespaceSerializer(),
                new ListStateDescriptor<>("list", String.class));

        List<CachingInternalState<String, ?, ?, ?>> registeredStates = getRegisteredStates();
        assertEquals(3, registeredStates.size());

        List<CachingInternalState<String, ?, ?, ?>> spiedStates = new ArrayList<>();
        for (CachingInternalState<String, ?, ?, ?> state : registeredStates) {
            spiedStates.add(spy(state));
        }

        registeredStates.clear();
        registeredStates.addAll(spiedStates);

        cachingBackend.savepoint();

        for (CachingInternalState<String, ?, ?, ?> state : spiedStates) {
            verify(state, times(1)).flushToUnderlyingState();
        }
    }

    @Test
    void testDelegationMethods() throws Exception {
        // Test that simple delegation methods are called on the delegate backend
        cachingBackend.getKeys("testState", VoidNamespace.INSTANCE);
        verify(mockDelegateBackend, times(1)).getKeys("testState", VoidNamespace.INSTANCE);

        cachingBackend.getKeysAndNamespaces("testState");
        verify(mockDelegateBackend, times(1)).getKeysAndNamespaces("testState");

        cachingBackend.numKeyValueStateEntries();
        verify(mockDelegateBackend, times(1)).numKeyValueStateEntries();

        cachingBackend.isSafeToReuseKVState();
        verify(mockDelegateBackend, times(1)).isSafeToReuseKVState();

        CheckpointType checkpointType = CheckpointType.CHECKPOINT;
        cachingBackend.requiresLegacySynchronousTimerSnapshots(checkpointType);
        verify(mockDelegateBackend, times(1)).requiresLegacySynchronousTimerSnapshots(checkpointType);

        // The following part of the test is logically flawed and uses a removed class.
        // It tries to test compactState, which is a RocksDB-specific feature,
        // by creating a non-RocksDB delegate backend.
        // This has been removed to fix compilation and improve test clarity.
    }

    @Test
    void testKeySelectionListenerDelegation() {
        // Test delegation of key selection listeners
        AbstractKeyedStateBackend.KeySelectionListener<String> listener = mock(AbstractKeyedStateBackend.KeySelectionListener.class);
        cachingBackend.registerKeySelectionListener(listener);
        verify(mockDelegateBackend, times(1)).registerKeySelectionListener(listener);

        cachingBackend.deregisterKeySelectionListener(listener);
        verify(mockDelegateBackend, times(1)).deregisterKeySelectionListener(listener);
    }

    @Test
    void testNotifyCheckpointMethods() throws Exception {
        // Test delegation of checkpoint notification methods
        cachingBackend.notifyCheckpointComplete(1L);
        verify(mockDelegateBackend, times(1)).notifyCheckpointComplete(1L);

        cachingBackend.notifyCheckpointAborted(2L);
        verify(mockDelegateBackend, times(1)).notifyCheckpointAborted(2L);
    }

    @Test
    void testDispose() throws Exception {
        // Test that dispose calls dispose on the delegate
        cachingBackend.dispose();
        verify(mockDelegateBackend, times(1)).dispose();
    }

    @Test
    void testCreateOrUpdateInternalStateDelegate() throws Exception {
        // This test verifies that createOrUpdateInternalState is properly delegated when the state type is not one of the cached types.
        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("testState", StringSerializer.INSTANCE);
        TypeSerializer<String> namespaceSerializer = StringSerializer.INSTANCE;
        when(mockDelegateBackend.createOrUpdateInternalState(any(TypeSerializer.class), any(ValueStateDescriptor.class), any(StateSnapshotTransformer.StateSnapshotTransformFactory.class)))
                .thenReturn(mockValueState);

        InternalKvState<?, ?, ?> createdState = cachingBackend.createOrUpdateInternalState(namespaceSerializer, descriptor, StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());

        assertSame(mockValueState, createdState, "Should delegate state creation for non-cached types.");
        verify(mockDelegateBackend, times(1)).createOrUpdateInternalState(eq(namespaceSerializer), eq(descriptor), any(StateSnapshotTransformer.StateSnapshotTransformFactory.class));
    }

    @Test
    void testMultipleCachePolicies() throws Exception {
        when(mockDelegateBackend.getOrCreateKeyedState(any(), any(ValueStateDescriptor.class))).thenReturn(mockValueState);
        // Test with LRU
        CachingKeyedStateBackend<String> lruBackend = createCachingBackendWithPolicy(CachingStateBackendFactory.CachePolicyType.LRU);
        ValueState<String> lruValueState = lruBackend.getOrCreateKeyedState(
                VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>("test", String.class));
        assertTrue(((CachingInternalValueState<?, ?, ?>) lruValueState).getDelegateState() instanceof InternalValueState);
        // Could add more policy-specific assertions if behavior differs observably here

        // Test with TinyLFU
        CachingKeyedStateBackend<String> tinyLfuBackend = createCachingBackendWithPolicy(CachingStateBackendFactory.CachePolicyType.TINYLFU);
        ValueState<String> tinyLfuValueState = tinyLfuBackend.getOrCreateKeyedState(
                VoidNamespaceSerializer.INSTANCE,
                new ValueStateDescriptor<>("test", String.class));
        assertTrue(((CachingInternalValueState<?, ?, ?>) tinyLfuValueState).getDelegateState() instanceof InternalValueState);
    }

    private CachingKeyedStateBackend<String> createCachingBackendWithPolicy(CachingStateBackendFactory.CachePolicyType policy) throws IOException {
        long mapL1KeyPresenceCacheSize = CachingStateBackendFactory.MAP_L1_KEY_PRESENCE_CACHE_SIZE_CONFIG.defaultValue();
        long mapL2KeyPresenceCacheSize = CachingStateBackendFactory.MAP_L2_KEY_PRESENCE_CACHE_SIZE_CONFIG.defaultValue();
        double mapCacheHitRateThreshold = CachingStateBackendFactory.MAP_CACHE_HIT_RATE_THRESHOLD_CONFIG.defaultValue();
        long mapCacheHitRateWindowSize = CachingStateBackendFactory.MAP_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG.defaultValue();
        long mapCacheMinAccessesForBypassCheck = CachingStateBackendFactory.MAP_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG.defaultValue();
        boolean mapKeyPresenceCacheEnabled = CachingStateBackendFactory.MAP_KEY_PRESENCE_CACHE_ENABLED_CONFIG.defaultValue();
        boolean mapBypassEnabled = CachingStateBackendFactory.MAP_BYPASS_ENABLED_CONFIG.defaultValue();

        double valueCacheHitRateThreshold = CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_THRESHOLD_CONFIG.defaultValue();
        long valueCacheHitRateWindowSize = CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG.defaultValue();
        long valueCacheMinAccessesForBypassCheck = CachingStateBackendFactory.VALUE_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG.defaultValue();
        boolean valueBypassEnabled = CachingStateBackendFactory.VALUE_BYPASS_ENABLED_CONFIG.defaultValue();
        boolean writeBehindEnabled = CachingStateBackendFactory.WRITE_BEHIND_ENABLED_CONFIG.defaultValue();

        return new CachingKeyedStateBackend<String>(
                mockEnv.getTaskKvStateRegistry(),
                StringSerializer.INSTANCE,
                mockEnv.getUserCodeClassLoader().asClassLoader(),
                mockEnv.getExecutionConfig(),
                TtlTimeProvider.DEFAULT,
                Collections.emptyList(),
                new CloseableRegistry(),
                mockDelegateBackend,
                10,
                20,
                5,
                1L,
                policy, // Use the provided policy
                (int) mapL1KeyPresenceCacheSize,
                (int) mapL2KeyPresenceCacheSize,
                mapCacheHitRateThreshold,
                mapCacheHitRateWindowSize,
                mapCacheMinAccessesForBypassCheck,
                mapKeyPresenceCacheEnabled,
                mapBypassEnabled,
                valueCacheHitRateThreshold,
                valueCacheHitRateWindowSize,
                valueCacheMinAccessesForBypassCheck,
                valueBypassEnabled,
                writeBehindEnabled // writeBehindEnabled
        );
    }

    @Test
    void testGetMaxActiveNamespaceOrPerKeyCacheContainers() {
        assertEquals(5, cachingBackend.getMaxActiveNamespaceOrPerKeyCacheContainers());
    }

    @Test
    void testSnapshotWithException() throws Exception {
        // Setup a state that will throw an exception during flush
        ValueStateDescriptor<String> valueDesc = new ValueStateDescriptor<>("exploding-state", String.class);
        when(mockDelegateBackend.getOrCreateKeyedState(any(), eq(valueDesc))).thenReturn(mockValueState);

        CachingInternalValueState<String, VoidNamespace, String> valueState = (CachingInternalValueState<String, VoidNamespace, String>) cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, valueDesc);
        CachingInternalValueState<String, VoidNamespace, String> spiedValueState = spy(valueState);
        doThrow(new IOException("Test Exception on Flush")).when(spiedValueState).flushToUnderlyingState();

        getRegisteredStates().clear();
        getRegisteredStates().add(spiedValueState);

        // Take snapshot and expect an exception
        try {
            cachingBackend.snapshot(1L, 1L, new MemCheckpointStreamFactory(1024), CheckpointOptions.forCheckpointWithDefaultLocation());
            fail("Snapshot should have failed with an exception.");
        } catch (Exception e) {
            assertTrue(e instanceof IOException);
            assertEquals("Test Exception on Flush", e.getMessage());
        }

        verify(spiedValueState, times(1)).flushToUnderlyingState();
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testIntegrationWithMultipleStateTypes() throws Exception {
        // Mock state descriptors
        ValueStateDescriptor<String> valueDesc = new ValueStateDescriptor<>("value", String.class);
        MapStateDescriptor<String, String> mapDesc =
                new MapStateDescriptor<>("map", String.class, String.class);
        ListStateDescriptor<String> listDesc = new ListStateDescriptor<>("list", String.class);

        // Mock delegate backend behavior
        when(mockDelegateBackend.getOrCreateKeyedState(any(), eq(valueDesc)))
                .thenReturn(mockValueState);
        when(mockDelegateBackend.getOrCreateKeyedState(any(), eq(mapDesc)))
                .thenReturn(mockMapState);
        when(mockDelegateBackend.getOrCreateKeyedState(any(), eq(listDesc)))
                .thenReturn(mockListState);

        // Create and spy on the states using raw types to avoid compilation errors
        CachingInternalValueState valueState =
                spy(
                        (CachingInternalValueState)
                                cachingBackend.getOrCreateKeyedState(
                                        new VoidNamespaceSerializer(), valueDesc));
        CachingInternalMapState mapState =
                spy(
                        (CachingInternalMapState)
                                cachingBackend.getOrCreateKeyedState(
                                        new VoidNamespaceSerializer(), mapDesc));
        CachingInternalListState listState =
                spy(
                        (CachingInternalListState)
                                cachingBackend.getOrCreateKeyedState(
                                        new VoidNamespaceSerializer(), listDesc));

        // Replace registered states with spied ones
        List<CachingInternalState> registeredStates = (List) getRegisteredStates();
        registeredStates.clear();
        registeredStates.add(valueState);
        registeredStates.add(mapState);
        registeredStates.add(listState);

        // Interact with states to dirty them
        cachingBackend.setCurrentKey("testKey");
        ((InternalValueState) valueState).setCurrentNamespace(VoidNamespace.INSTANCE);
        valueState.update("dirtyValue");
        ((InternalMapState) mapState).setCurrentNamespace(VoidNamespace.INSTANCE);
        mapState.put("dirtyKey", "dirtyMapValue");
        ((InternalListState) listState).setCurrentNamespace(VoidNamespace.INSTANCE);
        listState.add("dirtyListValue");

        // Take snapshot and verify flushes
        cachingBackend.snapshot(
                1L,
                1L,
                mock(CheckpointStreamFactory.class),
                CheckpointOptions.forCheckpointWithDefaultLocation());

        verify(valueState, times(1)).flushToUnderlyingState();
        verify(mapState, times(1)).flushToUnderlyingState();
        verify(listState, times(1)).flushToUnderlyingState();
    }

    private List<CachingInternalState<String, ?, ?, ?>> getRegisteredStates() throws Exception {
        Field rsField = CachingKeyedStateBackend.class.getDeclaredField("registeredStates");
        rsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<CachingInternalState<String, ?, ?, ?>> registeredStates = (List<CachingInternalState<String, ?, ?, ?>>) rsField.get(cachingBackend);
        return registeredStates;
    }
}
