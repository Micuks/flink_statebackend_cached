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
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.internal.*;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.RunnableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comparative tests for {@link CachingKeyedStateBackend} with {@link RocksDBStateBackend}
 * as delegate, comparing its behavior against a direct {@link RocksDBStateBackend}.
 */
@SuppressWarnings("serial")
class CachingRocksDBStateBackendComparativeTest
        extends StateBackendTestBase<CachingStateBackend> {

    private CachingStateBackendFactory.CachePolicyType cachePolicyType = CachingStateBackendFactory.CachePolicyType.LRU;

    // Fields for CachingStateBackend (managed by StateBackendTestBase and this class's getStateBackend())
    private org.apache.flink.contrib.streaming.state.RocksDBStateBackend rocksDbDelegateBackend; // Delegate for the CachingStateBackend under test
    protected AbstractKeyedStateBackend<String> keyedStateBackend; // Backend under test

    // Fields for the reference RocksDBStateBackend
    private org.apache.flink.contrib.streaming.state.RocksDBStateBackend referenceRocksDbRawBackend; // The raw reference RocksDBStateBackend
    private AbstractKeyedStateBackend<String> referenceKeyedStateBackend; // Keyed version of reference
    private Environment referenceEnv;

    // Common fields
    private CloseableRegistry cancelStreamRegistryForReference;
    private CloseableRegistry cancelStreamRegistry; // Added for the main backend under test

    // Temporary directories
    private File cachingDelegateRocksDbInstanceBasePath;
    private File referenceRocksDbInstanceBasePath;

    @TempDir
    public java.nio.file.Path temporaryFolder;

    private <T> void assertListEquals(Iterable<T> actualIterable, Iterable<T> expectedIterable) {
        List<T> actualList = new ArrayList<>();
        actualIterable.forEach(actualList::add);
        List<T> expectedList = new ArrayList<>();
        expectedIterable.forEach(expectedList::add);
        assertEquals(expectedList.size(), actualList.size());
        assertIterableEquals(expectedList, actualList);
    }
    
    private <T> boolean isListStateEmpty(Iterable<T> iterable) {
        return !iterable.iterator().hasNext();
    }

    private <K,V> void assertMapEquals(Iterable<Map.Entry<K,V>> actualIterable, Iterable<Map.Entry<K,V>> expectedIterable) {
        Map<K,V> actualMap = new HashMap<>();
        actualIterable.forEach(entry -> actualMap.put(entry.getKey(), entry.getValue()));
        Map<K,V> expectedMap = new HashMap<>();
        expectedIterable.forEach(entry -> expectedMap.put(entry.getKey(), entry.getValue()));
        assertEquals(expectedMap, actualMap);
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
        private String key; 
        private int internalIndex = HeapPriorityQueueElement.NOT_CONTAINED; 

        public TestPriorityQueueElement() {}

        public TestPriorityQueueElement(String value, int priority, String key) {
            this.value = value;
            this.priority = priority;
            this.key = key;
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
                    && Objects.equals(value, that.value)
                    && Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, priority, key, internalIndex);
        }

        @Override
        public String toString() {
            return "TestPriorityQueueElement{" + "value='" + value + "'" + ", priority=" + priority
                    + (key != null ? ", key='" + key + "'" : "") + "}";
        }
    }

    public static class TestPriorityQueueElementSerializer
            extends TypeSerializer<TestPriorityQueueElement> {
        private static final long serialVersionUID = 1L;
        public static final TestPriorityQueueElementSerializer INSTANCE =
                new TestPriorityQueueElementSerializer();
        // Using Kryo for simplicity in tests, a more robust serializer would be better for production
        private final org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer<TestPriorityQueueElement> kryoSerializer;

        public TestPriorityQueueElementSerializer() {
            this.kryoSerializer =
                    new org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer<>(
                            TestPriorityQueueElement.class, new ExecutionConfig());
        }

        @Override
        public boolean isImmutableType() { return false; }
        @Override
        public TypeSerializer<TestPriorityQueueElement> duplicate() { return new TestPriorityQueueElementSerializer(); }
        @Override
        public TestPriorityQueueElement createInstance() { return new TestPriorityQueueElement(); }
        @Override
        public TestPriorityQueueElement copy(TestPriorityQueueElement from) { return kryoSerializer.copy(from); }
        @Override
        public TestPriorityQueueElement copy(TestPriorityQueueElement from, TestPriorityQueueElement reuse) { return kryoSerializer.copy(from, reuse); }
        @Override
        public int getLength() { return -1; }
        @Override
        public void serialize(TestPriorityQueueElement record, DataOutputView target) throws IOException { kryoSerializer.serialize(record, target); }
        @Override
        public TestPriorityQueueElement deserialize(DataInputView source) throws IOException { return kryoSerializer.deserialize(source); }
        @Override
        public TestPriorityQueueElement deserialize(TestPriorityQueueElement reuse, DataInputView source) throws IOException { return kryoSerializer.deserialize(reuse, source); }
        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException { kryoSerializer.copy(source, target); }
        @Override
        public boolean equals(Object obj) { return obj instanceof TestPriorityQueueElementSerializer && kryoSerializer.equals(((TestPriorityQueueElementSerializer) obj).kryoSerializer); }
        @Override
        public int hashCode() { return kryoSerializer.hashCode(); }
        @Override
        public TypeSerializerSnapshot<TestPriorityQueueElement> snapshotConfiguration() { return new TestPriorityQueueElementSerializerSnapshot(); }
    }

    public static class TestPriorityQueueElementSerializerSnapshot
            implements TypeSerializerSnapshot<TestPriorityQueueElement> {
        @Override
        public int getCurrentVersion() { return 1; }
        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {}
        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
            if (readVersion != 1) throw new IOException("Unsupported version: " + readVersion);
        }
        @Override
        public TypeSerializer<TestPriorityQueueElement> restoreSerializer() { return TestPriorityQueueElementSerializer.INSTANCE; }
        @Override
        public TypeSerializerSchemaCompatibility<TestPriorityQueueElement> resolveSchemaCompatibility(TypeSerializer<TestPriorityQueueElement> newSerializer) {
            return newSerializer instanceof TestPriorityQueueElementSerializer ? TypeSerializerSchemaCompatibility.compatibleAsIs() : TypeSerializerSchemaCompatibility.incompatible();
        }
    }

    @Override
    protected CachingStateBackend getStateBackend() throws Exception {
        // Ensure delegate RocksDB is created first
        if (rocksDbDelegateBackend == null) {
            cachingDelegateRocksDbInstanceBasePath = TempDirUtils.newFolder(temporaryFolder);
            rocksDbDelegateBackend =
                    new org.apache.flink.contrib.streaming.state.RocksDBStateBackend(
                            cachingDelegateRocksDbInstanceBasePath.toURI().toString());
        }

        // Default values for new cache size parameters, align with factory defaults if possible
        long mapL1KeyPresenceCacheSize = CachingStateBackendFactory.MAP_L1_KEY_PRESENCE_CACHE_SIZE_CONFIG.defaultValue();
        long mapL2KeyPresenceCacheSize = CachingStateBackendFactory.MAP_L2_KEY_PRESENCE_CACHE_SIZE_CONFIG.defaultValue();
        double mapCacheHitRateThreshold = CachingStateBackendFactory.MAP_CACHE_HIT_RATE_THRESHOLD_CONFIG.defaultValue();
        long mapCacheHitRateWindowSize = CachingStateBackendFactory.MAP_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG.defaultValue();
        long mapCacheMinAccessesForBypassCheck = CachingStateBackendFactory.MAP_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG.defaultValue();
        boolean mapKeyPresenceCacheEnabled = CachingStateBackendFactory.MAP_KEY_PRESENCE_CACHE_ENABLED_CONFIG.defaultValue();
        boolean mapBypassEnabled = CachingStateBackendFactory.MAP_BYPASS_ENABLED_CONFIG.defaultValue();

        return new CachingStateBackend(
                rocksDbDelegateBackend,
                128L, // L1 cache size (entries)
                1024L, // L2 cache size (entries)
                100L, // Max active namespaces
                20L, // Max cache memory (MB)
                this.cachePolicyType, // Set by test parameter
                mapL1KeyPresenceCacheSize,
                mapL2KeyPresenceCacheSize,
                mapCacheHitRateThreshold,
                mapCacheHitRateWindowSize,
                mapCacheMinAccessesForBypassCheck,
                mapKeyPresenceCacheEnabled,
                mapBypassEnabled,
                CachingStateBackendFactory.PresenceCacheImplementation.DEFAULT,
                false);
    }

    @BeforeEach
    public void setUp() throws Exception {
        super.before(); // Call super.before() from StateBackendTestBase
        this.cancelStreamRegistry = new CloseableRegistry(); // Initialize it

        // Initialize the main keyedStateBackend for the CachingStateBackend under test
        // Ensure this.env is initialized by super.before()
        if (this.env == null) {
            throw new IllegalStateException("Environment not initialized by super.before(). Check StateBackendTestBase setup.");
        }
        
        // Create temporary directories
        this.cachingDelegateRocksDbInstanceBasePath =
            new File(temporaryFolder.toFile(), "caching-delegate-rocksdb-" + cachePolicyType.name());
        FileUtils.deleteDirectory(cachingDelegateRocksDbInstanceBasePath);
        Files.createDirectories(cachingDelegateRocksDbInstanceBasePath.toPath());
        
        this.referenceRocksDbInstanceBasePath =
            new File(temporaryFolder.toFile(), "reference-rocksdb");
        FileUtils.deleteDirectory(referenceRocksDbInstanceBasePath);
        Files.createDirectories(referenceRocksDbInstanceBasePath.toPath());
        
        // Initialize backends with the created directories
        rocksDbDelegateBackend = new org.apache.flink.contrib.streaming.state.RocksDBStateBackend(
            cachingDelegateRocksDbInstanceBasePath.toURI().toString()
        );
        
        referenceRocksDbRawBackend = new org.apache.flink.contrib.streaming.state.RocksDBStateBackend(
            referenceRocksDbInstanceBasePath.toURI().toString()
        );

        // Create the CachingStateBackend with the configured delegate
        CachingStateBackend cachingBackend = new CachingStateBackend(
            rocksDbDelegateBackend,
            10, // L1 cache size
            10, // L2 cache size
            5,  // Max active namespaces
            1,  // Max total cache memory in MB
            CachingStateBackendFactory.CachePolicyType.LRU,
            0L, // mapL1KeyPresenceCacheSize
            0L, // mapL2KeyPresenceCacheSize
            0.0d, // mapCacheHitRateThreshold
            1000L, // mapCacheHitRateWindowSize
            100_000L, // mapCacheMinAccessesForBypassCheck
            true, // mapKeyPresenceCacheEnabled
            true, // mapBypassEnabled
            CachingStateBackendFactory.PresenceCacheImplementation.DEFAULT,
            false
        );

        this.keyedStateBackend = createKeyedStateBackend(
            cachingBackend,
            this.env,
            new JobID(),
            "caching_operator",
            StringSerializer.INSTANCE,
            this.env.getExecutionConfig().getParallelism(),
            new KeyGroupRange(0, Math.max(0, this.env.getExecutionConfig().getParallelism() - 1)),
            this.env.getTaskKvStateRegistry(),
            TtlTimeProvider.DEFAULT,
            new UnregisteredMetricsGroup(),
            Collections.emptyList(),
            this.cancelStreamRegistry
        );

        this.referenceEnv = new MockEnvironmentBuilder()
                .setTaskName("reference-env")
                .setManagedMemorySize(4 * 1024 * 1024) // 4 MB
                .setTaskStateManager(new TestTaskStateManager()) // Use a new task state manager
                .setExecutionConfig(this.env.getExecutionConfig()) // Add execution config
                .build();
        this.cancelStreamRegistryForReference = new CloseableRegistry();

        this.referenceKeyedStateBackend = createKeyedStateBackend(
                this.referenceRocksDbRawBackend,
                this.referenceEnv,
                new JobID(),
                "reference_operator",
                StringSerializer.INSTANCE,
                this.env.getExecutionConfig().getParallelism(), // Use same parallelism as main env
                new KeyGroupRange(0, Math.max(0, this.env.getExecutionConfig().getParallelism() - 1)),
                this.referenceEnv.getTaskKvStateRegistry(),
                TtlTimeProvider.DEFAULT,
                new UnregisteredMetricsGroup(),
                Collections.emptyList(),
                this.cancelStreamRegistryForReference);
    }
    
    protected <K_SBT> AbstractKeyedStateBackend<K_SBT> createKeyedStateBackend(
        StateBackend backendToUse,
        Environment environment,
        JobID jobID,
        String operatorIdentifier,
        TypeSerializer<K_SBT> keySerializer,
        int numberOfKeyGroups,
        KeyGroupRange keyGroupRange,
        TaskKvStateRegistry kvStateRegistry,
        TtlTimeProvider ttlTimeProvider,
        MetricGroup metricGroup,
        @Nonnull Collection<KeyedStateHandle> stateHandles,
        CloseableRegistry cancelStreamRegistry) throws Exception {
            return (AbstractKeyedStateBackend<K_SBT>) backendToUse.createKeyedStateBackend(
                environment,
                jobID,
                operatorIdentifier,
                keySerializer,
                numberOfKeyGroups,
                keyGroupRange,
                kvStateRegistry,
                ttlTimeProvider,
                metricGroup,
                stateHandles,
                cancelStreamRegistry);
    }


    @AfterEach
    public void tearDown() throws Exception {
        super.after(); // Call super.after() from StateBackendTestBase

        IOUtils.closeQuietly(this.referenceKeyedStateBackend);
        this.referenceKeyedStateBackend = null;
        this.referenceRocksDbRawBackend = null; // It will be cleaned up by directory deletion
        IOUtils.closeQuietly(this.cancelStreamRegistryForReference);

        if (this.referenceRocksDbInstanceBasePath != null && this.referenceRocksDbInstanceBasePath.exists()) {
            FileUtils.deleteDirectory(this.referenceRocksDbInstanceBasePath); // Use FileUtils
        }
         if (this.cachingDelegateRocksDbInstanceBasePath != null && this.cachingDelegateRocksDbInstanceBasePath.exists()) {
            FileUtils.deleteDirectory(this.cachingDelegateRocksDbInstanceBasePath); // Use FileUtils
        }
    }


    @Override
    protected boolean supportsAsynchronousSnapshots() { return true; }

    @Override
    protected boolean isSerializerPresenceRequiredOnRestore() { return false; }

    @ParameterizedTest
    @EnumSource(CachingStateBackendFactory.CachePolicyType.class)
    void testValueStateComparative(CachingStateBackendFactory.CachePolicyType policy) throws Exception {
        this.cachePolicyType = policy;
        CachingStateBackend cachingBackend = getStateBackend();
        AbstractKeyedStateBackend<String> cachingKeyedBackend =
                createKeyedStateBackend(
                        cachingBackend,
                        this.env,
                        new JobID(),
                        "caching_operator",
                        StringSerializer.INSTANCE,
                        this.env.getExecutionConfig().getParallelism(),
                        new KeyGroupRange(0, Math.max(0, this.env.getExecutionConfig().getParallelism() - 1)),
                        this.env.getTaskKvStateRegistry(),
                        TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(),
                        Collections.emptyList(),
                        this.cancelStreamRegistry);

        cachingKeyedBackend.setCurrentKey("testKey");
        this.referenceKeyedStateBackend.setCurrentKey("testKey");

        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("valueState", String.class);
        
        InternalValueState<String, VoidNamespace, String> cachingValueState = cachingKeyedBackend
                .createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE, descriptor, StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
        InternalValueState<String, VoidNamespace, String> referenceValueState = this.referenceKeyedStateBackend
                .createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE, descriptor, StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());

        assertEquals(referenceValueState.value(), cachingValueState.value());
        assertNull(cachingValueState.value());

        cachingValueState.update("testValue1");
        referenceValueState.update("testValue1");
        assertEquals(referenceValueState.value(), cachingValueState.value());
        assertEquals("testValue1", cachingValueState.value());

        cachingValueState.clear();
        referenceValueState.clear();
        assertEquals(referenceValueState.value(), cachingValueState.value());
        assertNull(cachingValueState.value());
    }

    @ParameterizedTest
    @EnumSource(CachingStateBackendFactory.CachePolicyType.class)
    void testListStateComparativeAndSnapshotRestore(CachingStateBackendFactory.CachePolicyType policy) throws Exception {
        this.cachePolicyType = policy;
        CachingStateBackend cachingBackend = getStateBackend();
        AbstractKeyedStateBackend<String> cachingKeyedBackend =
                createKeyedStateBackend(
                        cachingBackend,
                        this.env,
                        new JobID(),
                        "caching_operator",
                        StringSerializer.INSTANCE,
                        this.env.getExecutionConfig().getParallelism(),
                        new KeyGroupRange(0, Math.max(0, this.env.getExecutionConfig().getParallelism() - 1)),
                        this.env.getTaskKvStateRegistry(),
                        TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(),
                        Collections.emptyList(),
                        this.cancelStreamRegistry);

        String key = "testKeyList";
        cachingKeyedBackend.setCurrentKey(key);
        this.referenceKeyedStateBackend.setCurrentKey(key);

        ListStateDescriptor<String> descriptor = new ListStateDescriptor<>("testListState", String.class);
        InternalListState<String, VoidNamespace, String> cachingListState = cachingKeyedBackend
                .createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE, descriptor, StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
        InternalListState<String, VoidNamespace, String> referenceListState = this.referenceKeyedStateBackend
                .createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE, descriptor, StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());

        assertTrue(isListStateEmpty(cachingListState.get()));
        assertListEquals(cachingListState.get(), referenceListState.get());

        cachingListState.add("e1");
        referenceListState.add("e1");
        cachingListState.add("e2");
        referenceListState.add("e2");
        assertListEquals(cachingListState.get(), referenceListState.get());
        
        List<String> expectedPreSnapshot = new ArrayList<>();
        referenceListState.get().forEach(expectedPreSnapshot::add);

        // Snapshot and restore the caching backend
        KeyedStateHandle snapshot = triggerSnapshot(); // Uses this.keyedStateBackend (caching)

        // Dispose the original caching backend (this.keyedStateBackend)
        // StateBackendTestBase.tearDown() would do this, but we need to do it mid-test
        if (this.keyedStateBackend != null) {
            this.keyedStateBackend.dispose();
        }
        this.keyedStateBackend = null; // Prevent double disposal


        // Create a new CachingStateBackend instance for restore, using the same delegate config
        CachingStateBackend newCachingBackendForRestore = getStateBackend(); // Reuses logic for backend creation
        
        MockEnvironment restoreEnv = new MockEnvironmentBuilder()
                                        .setTaskName("restore_op_env_list")
                                        .setManagedMemorySize(4 * 1024 * 1024)
                                        .setTaskStateManager(new TestTaskStateManager())
                                        .build();
        
        AbstractKeyedStateBackend<String> restoredCachingKeyedBackend =
                newCachingBackendForRestore.createKeyedStateBackend(
                        restoreEnv, new JobID(), "op_restored_list", StringSerializer.INSTANCE, 
                        restoreEnv.getExecutionConfig().getParallelism(), 
                        new KeyGroupRange(0, Math.max(0, restoreEnv.getExecutionConfig().getParallelism() - 1)),
                        restoreEnv.getTaskKvStateRegistry(), TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(), Collections.singletonList(snapshot), new CloseableRegistry());
        
        restoredCachingKeyedBackend.setCurrentKey(key);
        InternalListState<String, VoidNamespace, String> listStateAfterRestore =
                restoredCachingKeyedBackend.createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE, descriptor, StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());

        assertListEquals(listStateAfterRestore.get(), expectedPreSnapshot); // Compare with pre-snapshot state
        
        listStateAfterRestore.clear(); // Test clear on restored
        assertTrue(isListStateEmpty(listStateAfterRestore.get()));

        IOUtils.closeQuietly(restoredCachingKeyedBackend);
        if (newCachingBackendForRestore instanceof AutoCloseable) { // CachingStateBackend itself isn't AutoCloseable, but its delegate might be managed through dispose logic.
            IOUtils.closeQuietly((AutoCloseable)newCachingBackendForRestore);
        }
    }


    @ParameterizedTest
    @EnumSource(CachingStateBackendFactory.CachePolicyType.class)
    void testMapStateComparativeAndSnapshotRestore(CachingStateBackendFactory.CachePolicyType policy) throws Exception {
        this.cachePolicyType = policy;
        CachingStateBackend cachingBackend = getStateBackend();
        AbstractKeyedStateBackend<String> cachingKeyedBackend =
                createKeyedStateBackend(
                        cachingBackend,
                        this.env,
                        new JobID(),
                        "caching_operator",
                        StringSerializer.INSTANCE,
                        this.env.getExecutionConfig().getParallelism(),
                        new KeyGroupRange(0, Math.max(0, this.env.getExecutionConfig().getParallelism() - 1)),
                        this.env.getTaskKvStateRegistry(),
                        TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(),
                        Collections.emptyList(),
                        this.cancelStreamRegistry);

        String key = "testKeyMap";
        cachingKeyedBackend.setCurrentKey(key);
        this.referenceKeyedStateBackend.setCurrentKey(key);

        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<>("testMapState", String.class, String.class);
        InternalMapState<String, VoidNamespace, String, String> cachingMapState = cachingKeyedBackend
                .createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE, descriptor, StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
        InternalMapState<String, VoidNamespace, String, String> referenceMapState = this.referenceKeyedStateBackend
                .createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE, descriptor, StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
        
        assertTrue(isMapStateEmpty(cachingMapState.entries()));
        assertMapEquals(cachingMapState.entries(), referenceMapState.entries());

        cachingMapState.put("k1", "v1");
        referenceMapState.put("k1", "v1");
        cachingMapState.put("k2", "v2");
        referenceMapState.put("k2", "v2");

        assertEquals(referenceMapState.get("k1"), cachingMapState.get("k1"));
        assertEquals(referenceMapState.get("k2"), cachingMapState.get("k2"));
        assertMapEquals(cachingMapState.entries(), referenceMapState.entries());

        Map<String,String> expectedPreSnapshot = new HashMap<>();
        referenceMapState.entries().forEach(entry -> expectedPreSnapshot.put(entry.getKey(), entry.getValue()));

        KeyedStateHandle snapshot = triggerSnapshot();

        if (this.keyedStateBackend != null) {
            this.keyedStateBackend.dispose();
        }
        this.keyedStateBackend = null;

        CachingStateBackend newCachingBackendForRestore = getStateBackend();
        MockEnvironment restoreEnv = new MockEnvironmentBuilder()
                                        .setTaskName("restore_map_op_env")
                                        .setManagedMemorySize(4 * 1024 * 1024)
                                        .setTaskStateManager(new TestTaskStateManager())
                                        .build();
        
        AbstractKeyedStateBackend<String> restoredCachingKeyedBackend =
                newCachingBackendForRestore.createKeyedStateBackend(
                        restoreEnv, new JobID(), "op_restored_map", StringSerializer.INSTANCE,
                        restoreEnv.getExecutionConfig().getParallelism(), 
                        new KeyGroupRange(0, Math.max(0, restoreEnv.getExecutionConfig().getParallelism() - 1)),
                        restoreEnv.getTaskKvStateRegistry(), TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(), Collections.singletonList(snapshot), new CloseableRegistry());
        
        restoredCachingKeyedBackend.setCurrentKey(key);
        InternalMapState<String, VoidNamespace, String, String> mapStateAfterRestore =
                restoredCachingKeyedBackend.createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE, descriptor, StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());

        assertEquals(expectedPreSnapshot.get("k1"), mapStateAfterRestore.get("k1"));
        assertEquals(expectedPreSnapshot.get("k2"), mapStateAfterRestore.get("k2"));
        assertMapEquals(mapStateAfterRestore.entries(), expectedPreSnapshot.entrySet());

        mapStateAfterRestore.clear();
        assertTrue(isMapStateEmpty(mapStateAfterRestore.entries()));
        IOUtils.closeQuietly(restoredCachingKeyedBackend);
        if (newCachingBackendForRestore instanceof AutoCloseable) { // CachingStateBackend itself isn't AutoCloseable, but its delegate might be managed through dispose logic.
            IOUtils.closeQuietly((AutoCloseable)newCachingBackendForRestore);
        }
    }


    @ParameterizedTest
    @EnumSource(CachingStateBackendFactory.CachePolicyType.class)
    void testReducingStateComparative(CachingStateBackendFactory.CachePolicyType policy) throws Exception {
        this.cachePolicyType = policy;
        CachingStateBackend cachingBackend = getStateBackend();
        AbstractKeyedStateBackend<String> cachingKeyedBackend =
                createKeyedStateBackend(
                        cachingBackend,
                        this.env,
                        new JobID(),
                        "caching_operator",
                        StringSerializer.INSTANCE,
                        this.env.getExecutionConfig().getParallelism(),
                        new KeyGroupRange(0, Math.max(0, this.env.getExecutionConfig().getParallelism() - 1)),
                        this.env.getTaskKvStateRegistry(),
                        TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(),
                        Collections.emptyList(),
                        this.cancelStreamRegistry);

        cachingKeyedBackend.setCurrentKey("testKey");
        this.referenceKeyedStateBackend.setCurrentKey("testKey");

        ReducingStateDescriptor<Integer> descriptor = new ReducingStateDescriptor<>("reducingState", new MyReducingFunction(), IntSerializer.INSTANCE);
        InternalReducingState<String, VoidNamespace, Integer> cachingState = cachingKeyedBackend
                .createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE, descriptor, StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
        InternalReducingState<String, VoidNamespace, Integer> referenceState = this.referenceKeyedStateBackend
                .createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE, descriptor, StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());

        cachingState.add(1); referenceState.add(1);
        assertEquals(referenceState.get(), cachingState.get());
        cachingState.add(2); referenceState.add(2);
        assertEquals(Integer.valueOf(3), cachingState.get());
        assertEquals(referenceState.get(), cachingState.get());
        
        cachingState.clear(); referenceState.clear();
        assertNull(cachingState.get());
        assertEquals(referenceState.get(), cachingState.get());
    }

    @ParameterizedTest
    @EnumSource(CachingStateBackendFactory.CachePolicyType.class)
    void testAggregatingStateComparative(CachingStateBackendFactory.CachePolicyType policy) throws Exception {
        this.cachePolicyType = policy;
        CachingStateBackend cachingBackend = getStateBackend();
        AbstractKeyedStateBackend<String> cachingKeyedBackend =
                createKeyedStateBackend(
                        cachingBackend,
                        this.env,
                        new JobID(),
                        "caching_operator",
                        StringSerializer.INSTANCE,
                        this.env.getExecutionConfig().getParallelism(),
                        new KeyGroupRange(0, Math.max(0, this.env.getExecutionConfig().getParallelism() - 1)),
                        this.env.getTaskKvStateRegistry(),
                        TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(),
                        Collections.emptyList(),
                        this.cancelStreamRegistry);

        cachingKeyedBackend.setCurrentKey("testKey");
        this.referenceKeyedStateBackend.setCurrentKey("testKey");

        AggregatingStateDescriptor<Integer, String, String> descriptor =
                new AggregatingStateDescriptor<>("aggregatingState", new MyAggregateFunction(), StringSerializer.INSTANCE);
        InternalAggregatingState<String, VoidNamespace, Integer, String, String> cachingState =
                cachingKeyedBackend.createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE, descriptor, StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());
        InternalAggregatingState<String, VoidNamespace, Integer, String, String> referenceState =
                this.referenceKeyedStateBackend.createOrUpdateInternalState(VoidNamespaceSerializer.INSTANCE, descriptor, StateSnapshotTransformer.StateSnapshotTransformFactory.noTransform());

        cachingState.add(1); referenceState.add(1);
        assertEquals(referenceState.get(), cachingState.get());
        cachingState.add(2); referenceState.add(2);
        assertEquals("12", cachingState.get());
        assertEquals(referenceState.get(), cachingState.get());

        cachingState.clear(); referenceState.clear();
        assertEquals("", cachingState.get()); // Aggregating state returns empty accumulator
        assertEquals(referenceState.get(), cachingState.get());
    }

    @ParameterizedTest
    @EnumSource(CachingStateBackendFactory.CachePolicyType.class)
    void testPriorityQueueComparative(CachingStateBackendFactory.CachePolicyType policy) throws Exception {
        this.cachePolicyType = policy;
        CachingStateBackend cachingBackend = getStateBackend();
        AbstractKeyedStateBackend<String> cachingKeyedBackend =
                createKeyedStateBackend(
                        cachingBackend,
                        this.env,
                        new JobID(),
                        "caching_operator",
                        StringSerializer.INSTANCE,
                        this.env.getExecutionConfig().getParallelism(),
                        new KeyGroupRange(0, Math.max(0, this.env.getExecutionConfig().getParallelism() - 1)),
                        this.env.getTaskKvStateRegistry(),
                        TtlTimeProvider.DEFAULT,
                        new UnregisteredMetricsGroup(),
                        Collections.emptyList(),
                        this.cancelStreamRegistry);

        String key = "pqTestKey";
        cachingKeyedBackend.setCurrentKey(key);
        this.referenceKeyedStateBackend.setCurrentKey(key);
    
        String pqStateName = "testPriorityQueue";
        KeyGroupedInternalPriorityQueue<TestPriorityQueueElement> cachingPQ =
                cachingKeyedBackend.create(pqStateName, TestPriorityQueueElementSerializer.INSTANCE);
        KeyGroupedInternalPriorityQueue<TestPriorityQueueElement> referencePQ =
                this.referenceKeyedStateBackend.create(pqStateName, TestPriorityQueueElementSerializer.INSTANCE);
    
        TestPriorityQueueElement e1 = new TestPriorityQueueElement("a", 1, key);
        TestPriorityQueueElement e2 = new TestPriorityQueueElement("b", 2, key);
        TestPriorityQueueElement e0 = new TestPriorityQueueElement("c", 0, key); // Lower priority
    
        assertTrue(cachingPQ.add(e1));
        assertTrue(referencePQ.add(e1));
        assertEquals(referencePQ.size(), cachingPQ.size());
    
        assertTrue(cachingPQ.add(e2));
        assertTrue(referencePQ.add(e2));
        assertEquals(referencePQ.size(), cachingPQ.size());
    
        assertTrue(cachingPQ.add(e0));
        assertTrue(referencePQ.add(e0));
        assertEquals(referencePQ.size(), cachingPQ.size());
        assertEquals(3, cachingPQ.size());
    
        TestPriorityQueueElement polledCaching = cachingPQ.poll();
        TestPriorityQueueElement polledReference = referencePQ.poll();
        assertEquals(polledReference, polledCaching);
        assertEquals("c", polledCaching.value); // e0 should be first due to lowest priority
    
        polledCaching = cachingPQ.poll();
        polledReference = referencePQ.poll();
        assertEquals(polledReference, polledCaching);
        assertEquals("a", polledCaching.value); // e1 next
        
        polledCaching = cachingPQ.poll();
        polledReference = referencePQ.poll();
        assertEquals(polledReference, polledCaching);
        assertEquals("b", polledCaching.value); // e2 last
    
        assertNull(cachingPQ.poll());
        assertNull(referencePQ.poll());
        assertTrue(cachingPQ.isEmpty());
        assertTrue(referencePQ.isEmpty());
        assertEquals(referencePQ.size(), cachingPQ.size());
    }


    private KeyedStateHandle triggerSnapshot() throws Exception {
        // This method now uses this.keyedStateBackend which is the caching backend via super.setUp()
        long checkpointId = 1L;
        long timestamp = System.currentTimeMillis();
        CheckpointStreamFactory streamFactory = new MemCheckpointStreamFactory(1024 * 1024);
        CheckpointOptions checkpointOptions = CheckpointOptions.forCheckpointWithDefaultLocation();
        
        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshotFuture =
                this.keyedStateBackend.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
        snapshotFuture.run(); // Snapshots are now synchronous in the base class tests usually
        SnapshotResult<KeyedStateHandle> snapshotResult = snapshotFuture.get();
        assertNotNull(snapshotResult.getJobManagerOwnedSnapshot(), "Snapshot handle should not be null");
        return snapshotResult.getJobManagerOwnedSnapshot();
    }
}

/**
 * Helper Kryo-based serializer for TestPriorityQueueElement.
 * Note: For robust tests, a more explicit serializer (e.g., using DataOutputView/DataInputView directly)
 * would be better than relying on Kryo, especially if Kryo is not guaranteed on the classpath
 * or if specific serialization formats need to be tested. This is kept for brevity as in the original.
 */
class TestPriorityQueueElementKryoSerializer extends org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer<CachingRocksDBStateBackendComparativeTest.TestPriorityQueueElement> {
    public TestPriorityQueueElementKryoSerializer(ExecutionConfig executionConfig) {
        super(CachingRocksDBStateBackendComparativeTest.TestPriorityQueueElement.class, executionConfig);
    }
} 
