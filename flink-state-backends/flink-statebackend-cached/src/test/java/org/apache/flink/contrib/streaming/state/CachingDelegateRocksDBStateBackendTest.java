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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link CachingKeyedStateBackend} with {@link RocksDBStateBackend} as delegate.
 */
class CachingDelegateRocksDBStateBackendTest {

    @TempDir
    Path temporaryFolder;

    private RocksDBStateBackend rocksDbBackend;
    private CachingKeyedStateBackend<String> cachingBackend;
    private TestTaskStateManager actualTaskStateManager;
    private Environment mockEnv;

    private <T> boolean isListStateContains(Iterable<T> iterable, T... expected) {
        List<T> actual = new java.util.ArrayList<>();
        iterable.forEach(actual::add);
        return Arrays.equals(expected, actual.toArray());
    }

    private <T> boolean isListStateEmpty(Iterable<T> iterable) {
        return !iterable.iterator().hasNext();
    }

    private <K, V> boolean isMapStateEmpty(Iterable<java.util.Map.Entry<K, V>> iterable) {
        return !iterable.iterator().hasNext();
    }

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
            return "";
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
            return a + b;
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
            return priority == that.priority && internalIndex == that.internalIndex
                    && java.util.Objects.equals(value, that.value)
                    && java.util.Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(value, priority, key, internalIndex);
        }

        @Override
        public String toString() {
            return "TestPriorityQueueElement{" + "value='" + value + '\'' + ", priority=" + priority
                    + (key != null ? ", key='" + key + '\'' : "") + '}';
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
            return false;
        }

        @Override
        public TypeSerializer<TestPriorityQueueElement> duplicate() {
            return new TestPriorityQueueElementSerializer();
        }

        @Override
        public TestPriorityQueueElement createInstance() {
            return new TestPriorityQueueElement();
        }

        @Override
        public TestPriorityQueueElement copy(TestPriorityQueueElement from) {
            TestPriorityQueueElement newElement =
                    new TestPriorityQueueElement(from.value, from.priority);
            newElement.setKey(from.getKey());
            newElement.setInternalIndex(from.getInternalIndex());
            return newElement;
        }

        @Override
        public TestPriorityQueueElement copy(TestPriorityQueueElement from,
                TestPriorityQueueElement reuse) {
            reuse.value = from.value;
            reuse.priority = from.priority;
            reuse.setKey(from.getKey());
            reuse.setInternalIndex(from.getInternalIndex());
            return reuse;
        }

        @Override
        public int getLength() {
            return -1;
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
            if (this == obj)
                return true;
            if (obj == null || getClass() != obj.getClass())
                return false;
            TestPriorityQueueElementSerializer that = (TestPriorityQueueElementSerializer) obj;
            return kryoSerializer.equals(that.kryoSerializer);
        }

        @Override
        public int hashCode() {
            return kryoSerializer.hashCode();
        }

        @Override
        public TypeSerializerSnapshot<TestPriorityQueueElement> snapshotConfiguration() {
            return new TestPriorityQueueElementSerializerSnapshot();
        }
    }


    public static class TestPriorityQueueElementSerializerSnapshot
            implements TypeSerializerSnapshot<TestPriorityQueueElement> {
        @Override
        public int getCurrentVersion() {
            return 1;
        }

        @Override
        public void writeSnapshot(org.apache.flink.core.memory.DataOutputView out)
                throws java.io.IOException {}

        @Override
        public void readSnapshot(int readVersion, org.apache.flink.core.memory.DataInputView in,
                ClassLoader userCodeClassLoader) throws java.io.IOException {
            if (readVersion != 1) {
                throw new java.io.IOException("Unsupported version: " + readVersion);
            }
        }

        @Override
        public TypeSerializer<TestPriorityQueueElement> restoreSerializer() {
            return TestPriorityQueueElementSerializer.INSTANCE;
        }

        @Override
        public TypeSerializerSchemaCompatibility<TestPriorityQueueElement> resolveSchemaCompatibility(
                TypeSerializer<TestPriorityQueueElement> newSerializer) {
            if (newSerializer instanceof TestPriorityQueueElementSerializer) {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }
            return TypeSerializerSchemaCompatibility.incompatible();
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        temporaryFolder = TempDirUtils.newFolder(temporaryFolder).toPath();
        rocksDbBackend = new RocksDBStateBackend(temporaryFolder.toString(), true);

        mockEnv = new DummyEnvironment("test", 1, 0);
        actualTaskStateManager = new TestTaskStateManager();

        CachingStateBackend cachingStateBackendUnderTest = new CachingStateBackend(rocksDbBackend,
                CachingStateBackendFactory.L1_CACHE_SIZE_CONFIG.defaultValue(),
                CachingStateBackendFactory.L2_CACHE_SIZE_CONFIG.defaultValue(),
                CachingStateBackendFactory.MAX_ACTIVE_NAMESPACES_CONFIG.defaultValue(),
                CachingStateBackendFactory.MAX_CACHE_MEMORY_MB_CONFIG.defaultValue(),
                CachingStateBackendFactory.CACHE_POLICY_CONFIG.defaultValue()
        );

        cachingBackend = (CachingKeyedStateBackend<String>) cachingStateBackendUnderTest
                .createKeyedStateBackend(mockEnv, new JobID(), "op", StringSerializer.INSTANCE, 1,
                        new KeyGroupRange(0, 0), mockEnv.getTaskKvStateRegistry(),
                        TtlTimeProvider.DEFAULT, new UnregisteredMetricsGroup(),
                        Collections.emptyList(), new CloseableRegistry());
        cachingBackend.setCurrentKey("testKey");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (cachingBackend != null) {
            cachingBackend.dispose();
        }
        if (rocksDbBackend != null) {
            ((java.io.Closeable) rocksDbBackend).close();
        }
        if (actualTaskStateManager != null) {
            actualTaskStateManager.close();
        }
    }

    @Test
    void testValueStateCaching() throws Exception {
        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>("valueState", String.class);
        InternalValueState<String, VoidNamespace, String> valueState = cachingBackend
                .createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE, descriptor,
                        StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
        assertNull(valueState.value());
        valueState.update("testValue1");
        assertEquals("testValue1", valueState.value());
        valueState.clear();
        assertNull(valueState.value());
    }


    @Test
    void testListStateCaching() throws Exception {
        ListStateDescriptor<String> descriptor =
                new ListStateDescriptor<>("testListState", String.class);
        InternalListState<String, VoidNamespace, String> listState = cachingBackend
                .createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE, descriptor,
                        StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());

        assertTrue(isListStateEmpty(listState.get()));
        listState.add("e1");
        listState.add("e2");
        assertTrue(isListStateContains(listState.get(), "e1", "e2"));
        triggerSnapshotOnCachingBackend();

        CachingStateBackend newCachingBackendInstance = new CachingStateBackend(rocksDbBackend,
                CachingStateBackendFactory.L1_CACHE_SIZE_CONFIG.defaultValue(),
                CachingStateBackendFactory.L2_CACHE_SIZE_CONFIG.defaultValue(),
                CachingStateBackendFactory.MAX_ACTIVE_NAMESPACES_CONFIG.defaultValue(),
                CachingStateBackendFactory.MAX_CACHE_MEMORY_MB_CONFIG.defaultValue(),
                CachingStateBackendFactory.CACHE_POLICY_CONFIG.defaultValue());
        CachingKeyedStateBackend<String> newCachingKeyedBackend =
                (CachingKeyedStateBackend<String>) newCachingBackendInstance
                        .createKeyedStateBackend(mockEnv, new JobID(), "op",
                                StringSerializer.INSTANCE, 1, new KeyGroupRange(0, 0),
                                mockEnv.getTaskKvStateRegistry(), TtlTimeProvider.DEFAULT,
                                new UnregisteredMetricsGroup(), Collections.emptyList(),
                                new CloseableRegistry());
        newCachingKeyedBackend.setCurrentKey("testKey");
        InternalListState<String, VoidNamespace, String> listStateAfterFlush =
                newCachingKeyedBackend.createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE,
                        descriptor,
                        StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());

        assertTrue(isListStateContains(listStateAfterFlush.get(), "e1", "e2"));
        listState.clear();
        assertTrue(isListStateEmpty(listState.get()));
        newCachingKeyedBackend.dispose();
    }

    @Test
    void testMapStateCaching() throws Exception {
        MapStateDescriptor<String, String> descriptor =
                new MapStateDescriptor<>("testMapState", String.class, String.class);
        InternalMapState<String, VoidNamespace, String, String> mapState = cachingBackend
                .createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE, descriptor,
                        StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());

        assertTrue(isMapStateEmpty(mapState.entries()));
        mapState.put("k1", "v1");
        mapState.put("k2", "v2");
        assertEquals("v1", mapState.get("k1"));
        assertEquals("v2", mapState.get("k2"));
        triggerSnapshotOnCachingBackend();

        CachingStateBackend newCachingBackendInstance = new CachingStateBackend(rocksDbBackend,
                CachingStateBackendFactory.L1_CACHE_SIZE_CONFIG.defaultValue(),
                CachingStateBackendFactory.L2_CACHE_SIZE_CONFIG.defaultValue(),
                CachingStateBackendFactory.MAX_ACTIVE_NAMESPACES_CONFIG.defaultValue(),
                CachingStateBackendFactory.MAX_CACHE_MEMORY_MB_CONFIG.defaultValue(),
                CachingStateBackendFactory.CACHE_POLICY_CONFIG.defaultValue());
        CachingKeyedStateBackend<String> newCachingKeyedBackend =
                (CachingKeyedStateBackend<String>) newCachingBackendInstance
                        .createKeyedStateBackend(mockEnv, new JobID(), "op",
                                StringSerializer.INSTANCE, 1, new KeyGroupRange(0, 0),
                                mockEnv.getTaskKvStateRegistry(), TtlTimeProvider.DEFAULT,
                                new UnregisteredMetricsGroup(), Collections.emptyList(),
                                new CloseableRegistry());
        newCachingKeyedBackend.setCurrentKey("testKey");
        InternalMapState<String, VoidNamespace, String, String> mapStateAfterFlush =
                newCachingKeyedBackend.createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE,
                        descriptor,
                        StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());

        assertEquals("v1", mapStateAfterFlush.get("k1"));
        assertEquals("v2", mapStateAfterFlush.get("k2"));
        mapState.clear();
        assertTrue(isMapStateEmpty(mapState.entries()));
        newCachingKeyedBackend.dispose();
    }

    @Test
    void testReducingStateDelegation() throws Exception {
        ReducingStateDescriptor<Integer> descriptor = new ReducingStateDescriptor<>("reducingState",
                new MyReducingFunction(), IntSerializer.INSTANCE);
        InternalReducingState<String, VoidNamespace, Integer> reducingState = cachingBackend
                .createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE, descriptor,
                        StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
        reducingState.add(1);
        reducingState.add(2);
        assertEquals(Integer.valueOf(3), reducingState.get());
        reducingState.clear();
        assertNull(reducingState.get());
    }


    @Test
    void testAggregatingStateDelegation() throws Exception {
        AggregatingStateDescriptor<Integer, String, String> descriptor =
                new AggregatingStateDescriptor<>("aggregatingState", new MyAggregateFunction(),
                        StringSerializer.INSTANCE);
        InternalAggregatingState<String, VoidNamespace, Integer, String, String> aggregatingState =
                cachingBackend.createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE,
                        descriptor,
                        StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
        aggregatingState.add(1);
        aggregatingState.add(2);
        assertEquals("12", aggregatingState.get());
        aggregatingState.clear();
        assertEquals("", aggregatingState.get());
    }



    @Test
    void testPriorityQueueDelegation() throws Exception {
        String pqStateName = "testPriorityQueue";
        KeyGroupedInternalPriorityQueue<TestPriorityQueueElement> priorityQueue =
                cachingBackend.<TestPriorityQueueElement>create(pqStateName,
                        TestPriorityQueueElementSerializer.INSTANCE);

        TestPriorityQueueElement e1 = new TestPriorityQueueElement("a", 1);
        TestPriorityQueueElement e2 = new TestPriorityQueueElement("b", 2);
        cachingBackend.setCurrentKey("pqTestKey");
        e1.setKey(cachingBackend.getCurrentKey());
        e2.setKey(cachingBackend.getCurrentKey());


        System.out.println("Adding e1: " + e1);
        boolean result1 = priorityQueue.add(e1);
        System.out.println("Result1: " + result1 + ", PQ size: " + priorityQueue.size()
                + ", PQ isEmpty: " + priorityQueue.isEmpty());
        assertTrue(result1, "First add() should return true");

        System.out.println("Adding e2: " + e2);
        boolean result2 = priorityQueue.add(e2);
        System.out.println("Result2: " + result2 + ", PQ size: " + priorityQueue.size()
                + ", PQ isEmpty: " + priorityQueue.isEmpty());
        assertTrue(result2, "Second add() should return true");

        assertEquals(2, priorityQueue.size());
        assertFalse(priorityQueue.isEmpty());

        TestPriorityQueueElement polled = priorityQueue.poll();


        assertEquals("a", polled.value);


        assertNotNull(polled);
        assertEquals("b", polled.value);

        assertNull(priorityQueue.poll());
        assertTrue(priorityQueue.isEmpty());
    }

    private void triggerSnapshotOnCachingBackend() throws Exception {
        long checkpointId = 1L;
        long timestamp = System.currentTimeMillis();
        CheckpointStreamFactory streamFactory = new MemCheckpointStreamFactory(1024 * 1024);
        CheckpointOptions checkpointOptions = CheckpointOptions.forCheckpointWithDefaultLocation();
        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                cachingBackend.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
        snapshotFuture.run();
        snapshotFuture.get();
    }
}
