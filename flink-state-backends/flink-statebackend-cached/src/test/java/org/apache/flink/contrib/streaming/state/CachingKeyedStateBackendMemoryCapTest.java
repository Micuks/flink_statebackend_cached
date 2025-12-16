/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** Tests for the memory capping mechanism in {@link CachingKeyedStateBackend}. */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CachingKeyedStateBackendMemoryCapTest {

    @Mock
    private AbstractKeyedStateBackend<String> mockDelegateBackend;

    @Mock
    private InternalValueState<String, VoidNamespace, String> mockDelegateValueState;

    @Mock
    private InternalListState<String, VoidNamespace, String> mockDelegateListState;

    @Mock
    private InternalMapState<String, VoidNamespace, String, String> mockDelegateMapState;

    private CachingKeyedStateBackend<String> cachingBackend;
    private CachingKeyedStateBackend<String> spiedCachingBackend;
    private Environment mockEnv;
    private CloseableRegistry closableRegistry;

    private final String testKey1 = "testKey1";
    private final String testValue1 = "testValue1"; // Approx 2*10 + 16 = 36 bytes
    private final String testValue2 = "testValue2";
    private final String testValue3 = "testValue3";
    private final String testValue4 = "testValue4";
    private final String testValue5 = "testValue5";
    private final String testValue6 = "testValue6";

    // Define a small memory cap for testing eviction.
    // ValueSizeUtils.estimate("aString") is approx 30-40 bytes.
    private long maxCacheMemoryBytes;
    private double maxCacheMemoryMbForConstructor;

    private CachingStateBackendFactory.CachePolicyType currentCachePolicyType;

    static Stream<CachingStateBackendFactory.CachePolicyType> cachePolicies() {
        return Stream.of(CachingStateBackendFactory.CachePolicyType.LRU, CachingStateBackendFactory.CachePolicyType.TINYLFU);
    }

    @BeforeEach
    void setUp() throws Exception {
        if (currentCachePolicyType == null) {
            currentCachePolicyType = CachingStateBackendFactory.CachePolicyType.LRU;
        }
        MockitoAnnotations.openMocks(this);

        mockEnv = new MockEnvironmentBuilder().build();
        closableRegistry = new CloseableRegistry();

        when(mockDelegateBackend.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(mockDelegateBackend.getCurrentKey()).thenReturn(testKey1);
        when(mockDelegateBackend.getKeyGroupCompressionDecorator())
                .thenReturn(org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator.INSTANCE);
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
        when(mockDelegateBackend.getKeyContext()).thenReturn(
                new org.apache.flink.runtime.state.heap.InternalKeyContextImpl<>(keyGroupRange, 1));

        // Setup mock delegate value state
        when(mockDelegateValueState.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(mockDelegateValueState.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(mockDelegateValueState.getValueSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(mockDelegateBackend.getOrCreateKeyedState(any(VoidNamespaceSerializer.class),
                any(ValueStateDescriptor.class))).thenReturn(mockDelegateValueState);

        // Setup mock delegate list state
        when(mockDelegateListState.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(mockDelegateListState.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(mockDelegateListState.getValueSerializer()) // This serializer is for List<String>
                .thenReturn(new org.apache.flink.api.common.typeutils.base.ListSerializer<>(StringSerializer.INSTANCE));
        when(mockDelegateBackend.getOrCreateKeyedState(any(VoidNamespaceSerializer.class),
                any(ListStateDescriptor.class))).thenReturn(mockDelegateListState);
        
        // Setup mock delegate map state
        when(mockDelegateMapState.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(mockDelegateMapState.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(mockDelegateMapState.getValueSerializer()) // This serializer is for Map<String, String>
                .thenReturn(new org.apache.flink.api.common.typeutils.base.MapSerializer<>(StringSerializer.INSTANCE, StringSerializer.INSTANCE));
        when(mockDelegateBackend.getOrCreateKeyedState(any(VoidNamespaceSerializer.class),
                any(org.apache.flink.api.common.state.MapStateDescriptor.class))).thenReturn(mockDelegateMapState);

        // Add robust stubs for put/remove on the mock delegate map state
        doNothing().when(mockDelegateMapState).put(anyString(), anyString());
        doNothing().when(mockDelegateMapState).remove(anyString());

        // Instantiate the actual backend, pass 1MB to satisfy constructor, we will mock the getter for precise byte cap.
        long mapL1KeyPresenceCacheSize = CachingStateBackendFactory.MAP_L1_KEY_PRESENCE_CACHE_SIZE_CONFIG.defaultValue();
        long mapL2KeyPresenceCacheSize = CachingStateBackendFactory.MAP_L2_KEY_PRESENCE_CACHE_SIZE_CONFIG.defaultValue();
        double mapCacheHitRateThreshold = CachingStateBackendFactory.MAP_CACHE_HIT_RATE_THRESHOLD_CONFIG.defaultValue();
        long mapCacheHitRateWindowSize = CachingStateBackendFactory.MAP_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG.defaultValue();
        long mapCacheMinAccessesForBypassCheck = CachingStateBackendFactory.MAP_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG.defaultValue();
        boolean mapKeyPresenceCacheEnabled = CachingStateBackendFactory.MAP_KEY_PRESENCE_CACHE_ENABLED_CONFIG.defaultValue();
        boolean mapBypassEnabled = CachingStateBackendFactory.MAP_BYPASS_ENABLED_CONFIG.defaultValue();

        cachingBackend = new CachingKeyedStateBackend<String>(
                mockEnv.getTaskKvStateRegistry(),
                StringSerializer.INSTANCE,
                mockEnv.getUserCodeClassLoader().asClassLoader(),
                new ExecutionConfig(),
                TtlTimeProvider.DEFAULT,
                new UnregisteredMetricsGroup(),
                Collections.emptyList(),
                closableRegistry,
                mockDelegateBackend,
                5, // L1 cache size (entries)
                10, // L2 cache size (entries)
                3,  // max active namespaces (per-key caches for map state)
                1L, // Pass 1MB to constructor, MUST BE LONG
                currentCachePolicyType, // value policy
                currentCachePolicyType, // map policy
                currentCachePolicyType, // list policy
                currentCachePolicyType, // aggregating policy
                (int) mapL1KeyPresenceCacheSize, // Added
                (int) mapL2KeyPresenceCacheSize,  // Added
                mapCacheHitRateThreshold, // Added
                mapCacheHitRateWindowSize, // Added
                mapCacheMinAccessesForBypassCheck, // Added
                mapKeyPresenceCacheEnabled, // Added
                mapBypassEnabled, // Added
                CachingStateBackendFactory.PresenceCacheImplementation.DEFAULT,
                false,
                null,
                new org.apache.flink.configuration.Configuration(),
                0,
                0,
                0,
                0,
                0,
                0
                );
        
        // Spy the backend and mock getMaxConfiguredCacheSizeBytesValue to return our precise byte limit
        spiedCachingBackend = spy(cachingBackend);
        doReturn(maxCacheMemoryBytes).when(spiedCachingBackend).getMaxConfiguredCacheSizeBytesValue();
    }

    private void setPolicyAndSetup(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        this.currentCachePolicyType = policyType;
        setUp();
    }

    private void setupWithMemoryCap(long maxBytes) {
            this.maxCacheMemoryBytes = maxBytes;
            this.maxCacheMemoryMbForConstructor = (double) maxBytes / (1024.0 * 1024.0);
    }

    private void waitForEvictionToComplete() {
        // Wait for eviction to complete by checking cache size <= cap or timeout
        long currentSize;
        int attempts = 0;
        do {
            currentSize = spiedCachingBackend.getCurrentEstimatedCacheSizeBytesValue();
            if (currentSize <= spiedCachingBackend.getMaxConfiguredCacheSizeBytesValue()) {
                return;
            }
            try {
                Thread.sleep(10); // Short delay to allow eviction thread to run
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            attempts++;
        } while (attempts < 100); // Max 100 attempts (1 second)
    }

    @AfterEach
    void tearDown() throws Exception {
        if (spiedCachingBackend != null) {
            spiedCachingBackend.dispose();
        }
        closableRegistry.close();
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testValueState_MemoryReported_AndEvictionTriggered(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
            setupWithMemoryCap(100L); // Approx 3 string values
        setPolicyAndSetup(policyType);
        ValueStateDescriptor<String> valueDesc = new ValueStateDescriptor<>("testValueState", StringSerializer.INSTANCE);
        InternalValueState<String, VoidNamespace, String> valueState =
                (InternalValueState<String, VoidNamespace, String>) spiedCachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, valueDesc);
        valueState.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertEquals(0L, spiedCachingBackend.getCurrentEstimatedCacheSizeBytesValue(), "Initial cache size should be 0");

        // Add values under different keys to grow memory
        cachingBackend.setCurrentKey("key1");
        valueState.update(testValue1);
        long expectedSize1 = ValueSizeUtils.estimate(testValue1);
        assertEquals(expectedSize1, cachingBackend.getCurrentEstimatedCacheSizeBytesValue());

        cachingBackend.setCurrentKey("key2");
        valueState.update(testValue2);
        long expectedSize2 = expectedSize1 + ValueSizeUtils.estimate(testValue2);
        assertEquals(expectedSize2, cachingBackend.getCurrentEstimatedCacheSizeBytesValue());

        // This update should push the memory over the cap and trigger eviction
        cachingBackend.setCurrentKey("key3");
        valueState.update(testValue3);
        long sizeBeforeEviction = expectedSize2 + ValueSizeUtils.estimate(testValue3);

        waitForEvictionToComplete();
        long sizeAfterEviction = cachingBackend.getCurrentEstimatedCacheSizeBytesValue();

        assertTrue(sizeAfterEviction <= maxCacheMemoryBytes,
                        "Cache size should be less than or equal to max cap after eviction. Current: "
                                        + sizeAfterEviction + " Cap: " + maxCacheMemoryBytes);
        assertTrue(sizeAfterEviction < sizeBeforeEviction,
                        "Cache size should have been reduced by eviction. Before: "
                                        + sizeBeforeEviction + ", After: " + sizeAfterEviction);

        // Clear one state and check memory reduction
        cachingBackend.setCurrentKey("key1");
        long memoryBeforeClear = cachingBackend.getCurrentEstimatedCacheSizeBytesValue();
        valueState.clear();
        long sizeAfterClear = cachingBackend.getCurrentEstimatedCacheSizeBytesValue();

        assertTrue(sizeAfterClear <= memoryBeforeClear,
                        "Memory should decrease or stay same after clear. Before: "
                                        + memoryBeforeClear + " After: " + sizeAfterClear);
        assertTrue(sizeAfterClear <= maxCacheMemoryBytes, "Memory must remain under cap.");
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testListState_MemoryReported_AndEvictionTriggered(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
            setupWithMemoryCap(150L); // A few lists
        setPolicyAndSetup(policyType);
        ListStateDescriptor<String> listDesc = new ListStateDescriptor<>("testListState", StringSerializer.INSTANCE);
        InternalListState<String, VoidNamespace, String> listState =
                (InternalListState<String, VoidNamespace, String>) spiedCachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, listDesc);
        listState.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertEquals(0L, cachingBackend.getCurrentEstimatedCacheSizeBytesValue(), "Initial cache size should be 0");

        // Add lists for different keys to exceed cap
        cachingBackend.setCurrentKey("listKey1");
        List<String> list1 = Arrays.asList(testValue1, testValue2);
        listState.update(list1);
        long expectedSize1 = ValueSizeUtils.estimate(list1);
        assertEquals(expectedSize1, cachingBackend.getCurrentEstimatedCacheSizeBytesValue());

        cachingBackend.setCurrentKey("listKey2");
        List<String> list2 = Arrays.asList(testValue3, testValue4);
        listState.update(list2);
        long expectedSize2 = expectedSize1 + ValueSizeUtils.estimate(list2);
        assertEquals(expectedSize2, cachingBackend.getCurrentEstimatedCacheSizeBytesValue());

        // This update should trigger eviction
        cachingBackend.setCurrentKey("listKey3");
        List<String> list3 = Arrays.asList(testValue5, testValue6);
        listState.update(list3);
        long sizeBeforeEviction = expectedSize2 + ValueSizeUtils.estimate(list3);

        waitForEvictionToComplete();
        long sizeAfterEviction = cachingBackend.getCurrentEstimatedCacheSizeBytesValue();

        assertTrue(sizeAfterEviction <= maxCacheMemoryBytes,
                        "Cache size should be <= cap after adding three lists. Current: "
                                        + sizeAfterEviction + " Cap: " + maxCacheMemoryBytes);
        assertTrue(sizeAfterEviction < sizeBeforeEviction,
                        "Cache size should have been reduced by eviction. Before: "
                                        + sizeBeforeEviction + ", After: " + sizeAfterEviction);

        // Clear one list state
        cachingBackend.setCurrentKey("listKey1");
        long memoryBeforeClear = cachingBackend.getCurrentEstimatedCacheSizeBytesValue();
        listState.clear();
        waitForEvictionToComplete();
        long sizeAfterClear = cachingBackend.getCurrentEstimatedCacheSizeBytesValue();
        assertTrue(sizeAfterClear <= memoryBeforeClear,
                        "Memory should decrease or stay same after clear.");
        assertTrue(sizeAfterClear <= maxCacheMemoryBytes,
                        "Cache size should remain under cap after clear. Current: " + sizeAfterClear
                                        + " Cap: " + maxCacheMemoryBytes);
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMapState_MemoryReported_AndEvictionTriggered(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        MapStateDescriptor<String, String> mapDesc = new MapStateDescriptor<>("testMapState", StringSerializer.INSTANCE, StringSerializer.INSTANCE);
        InternalMapState<String, VoidNamespace, String, String> mapState =
                (InternalMapState<String, VoidNamespace, String, String>) spiedCachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, mapDesc);
        ((InternalMapState<String, VoidNamespace, String, String>) mapState).setCurrentNamespace(VoidNamespace.INSTANCE);

        assertEquals(0L, spiedCachingBackend.getCurrentEstimatedCacheSizeBytesValue(), "Initial cache size should be 0");

        // Add entries to one map state under one Flink key
        spiedCachingBackend.setCurrentKey("mapKey1");
        mapState.put("uk1", testValue1);
        long sizeAfterPut1 = spiedCachingBackend.getCurrentEstimatedCacheSizeBytesValue();
        assertTrue(sizeAfterPut1 > 0, "Cache size should be > 0 after one put");

        mapState.put("uk2", testValue2);
        long sizeAfterPut2 = spiedCachingBackend.getCurrentEstimatedCacheSizeBytesValue();
        assertTrue(sizeAfterPut2 > sizeAfterPut1, "Cache size should grow after second put");

        // Use a second Flink key
        spiedCachingBackend.setCurrentKey("mapKey2");
        mapState.put("uk1", testValue3);
        long sizeAfterPut3 = spiedCachingBackend.getCurrentEstimatedCacheSizeBytesValue();
        assertTrue(sizeAfterPut3 > sizeAfterPut2, "Cache size should grow after third put");

        // This final put should push memory over the cap and trigger eviction
        spiedCachingBackend.setCurrentKey("mapKey3");
        mapState.put("uk1", testValue4);
        long sizeBeforeFinalPut = spiedCachingBackend.getCurrentEstimatedCacheSizeBytesValue();
        mapState.put("uk2", testValue5);
        
        waitForEvictionToComplete();
        long sizeAfterEviction = spiedCachingBackend.getCurrentEstimatedCacheSizeBytesValue();

        assertTrue(sizeAfterEviction <= maxCacheMemoryBytes,
                        "Cache size should be less than or equal to max cap after eviction. Current: "
                                        + sizeAfterEviction + " Cap: " + maxCacheMemoryBytes);
        // Eviction should have reduced the size significantly
        assertTrue(sizeAfterEviction < sizeBeforeFinalPut,
                        "Eviction should have reduced cache size. Before: " + sizeBeforeFinalPut
                                        + ", After: " + sizeAfterEviction);

        // Clear one map state
        long memoryBeforeClear = spiedCachingBackend.getCurrentEstimatedCacheSizeBytesValue();
        spiedCachingBackend.setCurrentKey("mapKey1");
        mapState.clear(); 
        long sizeAfterClearMap = spiedCachingBackend.getCurrentEstimatedCacheSizeBytesValue();
        // If mapState was fully or partially in cache, memory should decrease.
        // If it was fully evicted, memory might not change much.
        assertTrue(sizeAfterClearMap <= memoryBeforeClear, "Memory should decrease or stay same after clear.");
        assertTrue(sizeAfterClearMap <= maxCacheMemoryBytes, "Memory must remain under cap.");
    }
} 
