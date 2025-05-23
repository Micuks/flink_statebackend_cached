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
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.CachingStateBackendFactory;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.internal.InternalKvState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Disabled;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.RunnableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link CachingKeyedStateBackend} with {@link RocksDBStateBackend} as delegate.
 */
class CachingDelegateRocksDBStateBackendTest {

    @TempDir
    Path temporaryFolder;

    private RocksDBStateBackend rocksDbBackend;
    private CachingKeyedStateBackend<String> cachingBackend;
    private org.apache.flink.runtime.state.TestTaskStateManager actualTaskStateManager;
    private Environment mockEnv;

    // Helper methods to replace RocksDBTestUtils for list and map state checks
    private <T> boolean isListStateContains(Iterable<T> iterable, T... expected) {
        java.util.List<T> actualList = new java.util.ArrayList<>();
        if (iterable != null) {
            for (T item : iterable) {
                actualList.add(item);
            }
        }
        java.util.List<T> expectedList = java.util.Arrays.asList(expected);
        return actualList.containsAll(expectedList) && expectedList.containsAll(actualList);
    }

    private <T> boolean isListStateEmpty(Iterable<T> iterable) {
        return iterable == null || !iterable.iterator().hasNext();
    }

    private <K, V> boolean isMapStateEmpty(Iterable<java.util.Map.Entry<K, V>> iterable) {
        return iterable == null || !iterable.iterator().hasNext();
    }

    // --- Start of inlined helper classes from RocksDBTestUtils ---

    public static class MyReducingFunction
            implements org.apache.flink.api.common.functions.ReduceFunction<Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer reduce(Integer value1, Integer value2) throws Exception {
            if (value1 == null) {
                return value2;
            }
            if (value2 == null) {
                return value1;
            }
            return value1 + value2;
        }
    }

    public static class MyAggregateFunction implements
            org.apache.flink.api.common.functions.AggregateFunction<Integer, String, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String createAccumulator() {
            return "ACC:";
        }

        @Override
        public String add(Integer value, String accumulator) {
            if (value == null) {
                return accumulator;
            }
            return accumulator + value;
        }

        @Override
        public String getResult(String accumulator) {
            return accumulator;
        }

        @Override
        public String merge(String a, String b) {
            // This merge logic might not be perfectly what the original test expected,
            // but it's a plausible merge for string accumulators.
            // The original test didn't explicitly test merge for this AggregatingState.
            return a + b.replaceFirst("ACC:", "");
        }
    }

    public static class TestPriorityQueueElement implements HeapPriorityQueueElement,
            PriorityComparable<TestPriorityQueueElement>, Keyed<String> {
        public String value;
        public int priority;
        private String key; // For Keyed interface
        private int internalIndex = HeapPriorityQueueElement.NOT_CONTAINED; // For
                                                                            // HeapPriorityQueueElement

        public TestPriorityQueueElement() {}

        public TestPriorityQueueElement(String value, int priority) {
            this.value = value;
            this.priority = priority;
        }

        @Override
        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        @Override
        public int comparePriorityTo(TestPriorityQueueElement o) {
            return Integer.compare(this.priority, o.priority);
        }

        @Override
        public int getInternalIndex() {
            return internalIndex;
        }

        @Override
        public void setInternalIndex(int newIndex) {
            this.internalIndex = newIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            TestPriorityQueueElement that = (TestPriorityQueueElement) o;
            return priority == that.priority && java.util.Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(value, priority);
        }

        @Override
        public String toString() {
            return "TestPriorityQueueElement{" + "value='" + value + "'" + ", priority=" + priority
                    + '}';
        }
    }

    public static class TestPriorityQueueElementSerializer
            extends TypeSerializer<TestPriorityQueueElement> {
        private static final long serialVersionUID = 1L;
        public static final TestPriorityQueueElementSerializer INSTANCE =
                new TestPriorityQueueElementSerializer();
        private final org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer<TestPriorityQueueElement> kryoSerializer;

        public TestPriorityQueueElementSerializer() {
            // Fallback to Kryo for simplicity in this test context
            this.kryoSerializer =
                    new org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer<>(
                            TestPriorityQueueElement.class, new ExecutionConfig());
        }

        @Override
        public boolean isImmutableType() {
            return kryoSerializer.isImmutableType();
        }

        @Override
        public TypeSerializer<TestPriorityQueueElement> duplicate() {
            return this; // KryoSerializer is stateful, but for test instance sharing is fine.
        }

        @Override
        public TestPriorityQueueElement createInstance() {
            return kryoSerializer.createInstance();
        }

        @Override
        public TestPriorityQueueElement copy(TestPriorityQueueElement from) {
            return kryoSerializer.copy(from);
        }

        @Override
        public TestPriorityQueueElement copy(TestPriorityQueueElement from,
                TestPriorityQueueElement reuse) {
            return kryoSerializer.copy(from, reuse);
        }

        @Override
        public int getLength() {
            return kryoSerializer.getLength();
        }

        @Override
        public void serialize(TestPriorityQueueElement record,
                org.apache.flink.core.memory.DataOutputView target) throws java.io.IOException {
            kryoSerializer.serialize(record, target);
        }

        @Override
        public TestPriorityQueueElement deserialize(
                org.apache.flink.core.memory.DataInputView source) throws java.io.IOException {
            return kryoSerializer.deserialize(source);
        }

        @Override
        public TestPriorityQueueElement deserialize(TestPriorityQueueElement reuse,
                org.apache.flink.core.memory.DataInputView source) throws java.io.IOException {
            return kryoSerializer.deserialize(reuse, source);
        }

        @Override
        public void copy(org.apache.flink.core.memory.DataInputView source,
                org.apache.flink.core.memory.DataOutputView target) throws java.io.IOException {
            kryoSerializer.copy(source, target);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof TestPriorityQueueElementSerializer;
        }

        @Override
        public int hashCode() {
            return getClass().hashCode();
        }

        @Override
        public TypeSerializerSnapshot<TestPriorityQueueElement> snapshotConfiguration() {
            return kryoSerializer.snapshotConfiguration();
        }
    }

    // --- End of inlined helper classes ---

    @BeforeEach
    void setUp() throws Exception {
        File rocksDbDir = temporaryFolder.toFile();
        rocksDbBackend = new RocksDBStateBackend(rocksDbDir.toURI().toString());
        rocksDbBackend.setDbStoragePaths(rocksDbDir.getAbsolutePath());

        mockEnv = new MockEnvironmentBuilder().build();
        actualTaskStateManager = new org.apache.flink.runtime.state.TestTaskStateManager();
        JobID jobID = new JobID();

        ExecutionConfig executionConfig = new ExecutionConfig();

        TaskKvStateRegistry kvStateRegistry = mockEnv.getTaskKvStateRegistry();

        AbstractKeyedStateBackend<String> delegate = rocksDbBackend.createKeyedStateBackend(mockEnv,
                jobID, "testOperator", StringSerializer.INSTANCE, 1, // numberOfKeyGroups
                new KeyGroupRange(0, 0), kvStateRegistry, TtlTimeProvider.DEFAULT,
                new UnregisteredMetricsGroup(), Collections.emptyList(), new CloseableRegistry());

        cachingBackend = new CachingKeyedStateBackend<String>(kvStateRegistry,
                StringSerializer.INSTANCE, mockEnv.getUserCodeClassLoader().asClassLoader(),
                executionConfig, TtlTimeProvider.DEFAULT, new UnregisteredMetricsGroup(),
                Collections.emptyList(), new CloseableRegistry(), delegate, 10, // L1 cache size
                10, // L2 cache size
                10, // maxActiveNamespaceOrPerKeyCacheContainers
                1L, // maxCacheMemoryMb (explicitly long)
                CachingStateBackendFactory.CachePolicyType.LRU // Added cache policy
        );
        cachingBackend.setCurrentKey("testKey"); // Set a default key for tests
    }

    @AfterEach
    void tearDown() throws Exception {
        if (cachingBackend != null) {
            cachingBackend.dispose();
        }
        if (actualTaskStateManager != null) {
            actualTaskStateManager.close();
        }
    }

    @Test
    void testValueStateCaching() throws Exception {
        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>("testValueState", String.class);
        ValueState<String> valueState =
                cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);
        ((InternalKvState<?, VoidNamespace, ?>) valueState)
                .setCurrentNamespace(VoidNamespace.INSTANCE);

        // Test basic put/get
        valueState.update("hello");
        assertEquals("hello", valueState.value());

        // Further access should hit cache (difficult to verify directly without cache
        // metrics/spying on cache)
        assertEquals("hello", valueState.value());

        // Test clear
        valueState.clear();
        assertEquals(null, valueState.value());
    }

    @Test
    void testListStateCaching() throws Exception {
        ListStateDescriptor<String> descriptor =
                new ListStateDescriptor<>("testListState", String.class);
        ListState<String> listState =
                cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);
        ((InternalKvState<?, VoidNamespace, ?>) listState)
                .setCurrentNamespace(VoidNamespace.INSTANCE);

        listState.add("e1");
        listState.add("e2");
        assertTrue(isListStateContains(listState.get(), "e1", "e2"));

        listState.clear();
        assertTrue(isListStateEmpty(listState.get()));
    }

    @Test
    void testMapStateCaching() throws Exception {
        MapStateDescriptor<String, String> descriptor =
                new MapStateDescriptor<>("testMapState", String.class, String.class);
        MapState<String, String> mapState =
                cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);
        ((InternalKvState<?, VoidNamespace, ?>) mapState)
                .setCurrentNamespace(VoidNamespace.INSTANCE);

        mapState.put("k1", "v1");
        mapState.put("k2", "v2");
        assertEquals("v1", mapState.get("k1"));
        assertEquals("v2", mapState.get("k2"));
        assertTrue(mapState.contains("k1"));

        mapState.remove("k1");
        assertEquals(null, mapState.get("k1"));

        mapState.clear();
        assertTrue(isMapStateEmpty(mapState.entries()));
    }

    @Test
    void testReducingStateDelegation() throws Exception {
        ReducingStateDescriptor<Integer> descriptor = new ReducingStateDescriptor<>(
                "testReducingState", new MyReducingFunction(), IntSerializer.INSTANCE);
        ReducingState<Integer> reducingState =
                cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);
        ((InternalKvState<?, VoidNamespace, ?>) reducingState)
                .setCurrentNamespace(VoidNamespace.INSTANCE);

        reducingState.add(1);
        reducingState.add(2);
        assertEquals(Integer.valueOf(3), reducingState.get());

        reducingState.add(3);
        assertEquals(Integer.valueOf(6), reducingState.get());

        reducingState.clear();
        assertEquals(null, reducingState.get()); // Default for MyReducingFunction with no elements
    }

    @Test
    void testAggregatingStateDelegation() throws Exception {
        AggregatingStateDescriptor<Integer, String, String> descriptor =
                new AggregatingStateDescriptor<>("testAggregatingState", new MyAggregateFunction(),
                        StringSerializer.INSTANCE);
        AggregatingState<Integer, String> aggregatingState =
                cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);
        ((InternalKvState<?, VoidNamespace, ?>) aggregatingState)
                .setCurrentNamespace(VoidNamespace.INSTANCE);

        aggregatingState.add(1);
        aggregatingState.add(2);
        // MyAggregateFunction: "ACC:1,ACC:12" if initial accumulator is "ACC:"
        assertEquals("ACC:12", aggregatingState.get());

        aggregatingState.add(3);
        assertEquals("ACC:123", aggregatingState.get());

        aggregatingState.clear();
        // Initial accumulator for MyAggregateFunction is "ACC:", but get() after clear() should be
        // null.
        assertNull(aggregatingState.get());
    }

    @Test
    void testPriorityQueueDelegation() throws Exception {
        String pqStateName = "testPriorityQueue";
        KeyGroupedInternalPriorityQueue<TestPriorityQueueElement> priorityQueue =
                cachingBackend.<TestPriorityQueueElement>create(pqStateName,
                        TestPriorityQueueElementSerializer.INSTANCE);

        TestPriorityQueueElement e1 = new TestPriorityQueueElement("a", 1);
        TestPriorityQueueElement e2 = new TestPriorityQueueElement("b", 2);
        e1.setKey("testKey1");
        e2.setKey("testKey2");

        assertTrue(priorityQueue.add(e1));
        assertTrue(priorityQueue.add(e2));

        assertEquals(2, priorityQueue.size());
        assertEquals(e1, priorityQueue.peek());
        assertEquals(e1, priorityQueue.poll());
        assertEquals(e2, priorityQueue.poll());

        // Add a small delay to allow any background operations to complete
        Thread.sleep(100);

        // Trigger a snapshot to ensure all pending writes are flushed
        triggerSnapshotOnCachingBackend();

        // Verify the queue is empty by checking its size instead of using isEmpty()
        assertEquals(0, priorityQueue.size());
    }

    /**
     * Helper method to trigger a snapshot on the cachingBackend. This ensures that all pending
     * writes in the delegate backend are flushed.
     */
    private void triggerSnapshotOnCachingBackend() throws Exception {
        long checkpointId = 1L;
        long timestamp = System.currentTimeMillis();

        // Create a simple in-memory checkpoint storage
        CheckpointStreamFactory streamFactory = new MemCheckpointStreamFactory(1024 * 1024);
        CheckpointOptions checkpointOptions = CheckpointOptions.forCheckpointWithDefaultLocation();

        // Trigger snapshot and wait for completion
        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                cachingBackend.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);

        // Execute the snapshot synchronously
        snapshotFuture.run();

        // Wait for the result (and discard it)
        snapshotFuture.get();
    }
}
