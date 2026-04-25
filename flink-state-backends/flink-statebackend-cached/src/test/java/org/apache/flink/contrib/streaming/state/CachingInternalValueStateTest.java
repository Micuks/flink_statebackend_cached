/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.mockito.Mockito.lenient;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class CachingInternalValueStateTest {

    @Mock private InternalValueState<String, String, String> mockDelegateState;
    private CachingKeyedStateBackend<String> cachingKeyedStateBackend;
    @Mock private AbstractKeyedStateBackend<String> mockAbstractKeyedStateBackendDelegate;
    @Mock private TypeSerializer<String> mockKeySerializer;
    @Mock private TypeSerializer<String> mockNamespaceSerializer;
    @Mock private TypeSerializer<String> mockValueSerializer;

    private CachingInternalValueState<String, String, String> cachingState;

    private final int l1CacheSize = 2;
    private final int l2CacheSize = 2;
    private final int maxActiveNamespaces = 2;
    private final String testKey = "testKey";
    private final String testNamespace = "testNamespace";
    private final String testValue1 = "testValue1";
    private final String testValue2 = "testValue2";
    private final String testValue3 = "testValue3";

    private CachingStateBackendFactory.CachePolicyType currentCachePolicyType;

    static Stream<CachingStateBackendFactory.CachePolicyType> cachePolicies() {
        return Stream.of(CachingStateBackendFactory.CachePolicyType.LRU, CachingStateBackendFactory.CachePolicyType.TINYLFU);
    }

    @BeforeEach
    void setUp() {
        if (currentCachePolicyType == null) {
            currentCachePolicyType = CachingStateBackendFactory.CachePolicyType.LRU;
        }
        TaskKvStateRegistry kvStateRegistry = mock(TaskKvStateRegistry.class);
        ExecutionConfig executionConfig = new ExecutionConfig();
        TtlTimeProvider ttlTimeProvider = TtlTimeProvider.DEFAULT;
        MetricGroup metricGroup = new UnregisteredMetricsGroup();
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();

        lenient().when(mockKeySerializer.duplicate()).thenReturn(mockKeySerializer);
        lenient()
                .when(mockAbstractKeyedStateBackendDelegate.getKeySerializer())
                .thenReturn(mockKeySerializer);

        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 15);
        int numberOfKeyGroups = 16;
        lenient()
                .when(mockAbstractKeyedStateBackendDelegate.getKeyContext())
                .thenReturn(
                        new org.apache.flink.runtime.state.heap.InternalKeyContextImpl<>(
                                keyGroupRange, numberOfKeyGroups));

        long mapL1KeyPresenceCacheSize = CachingStateBackendFactory.MAP_L1_KEY_PRESENCE_CACHE_SIZE_CONFIG.defaultValue();
        long mapL2KeyPresenceCacheSize = CachingStateBackendFactory.MAP_L2_KEY_PRESENCE_CACHE_SIZE_CONFIG.defaultValue();
        double mapCacheHitRateThreshold = CachingStateBackendFactory.MAP_CACHE_HIT_RATE_THRESHOLD_CONFIG.defaultValue();
        long mapCacheHitRateWindowSize = CachingStateBackendFactory.MAP_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG.defaultValue();
        long mapCacheMinAccessesForBypassCheck = CachingStateBackendFactory.MAP_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG.defaultValue();
        boolean mapKeyPresenceCacheEnabled = CachingStateBackendFactory.MAP_KEY_PRESENCE_CACHE_ENABLED_CONFIG.defaultValue();
        boolean mapBypassEnabled = CachingStateBackendFactory.MAP_BYPASS_ENABLED_CONFIG.defaultValue();

        cachingKeyedStateBackend =
                new CachingKeyedStateBackend<String>(
                        kvStateRegistry,
                        mockKeySerializer,
                        CachingInternalValueStateTest.class.getClassLoader(),
                        executionConfig,
                        ttlTimeProvider,
                        metricGroup,
                        Collections.<KeyedStateHandle>emptyList(),
                        cancelStreamRegistry,
                        mockAbstractKeyedStateBackendDelegate,
                        l1CacheSize,
                        l2CacheSize,
                        maxActiveNamespaces,
                        10L,
                        currentCachePolicyType,
                        currentCachePolicyType,
                        currentCachePolicyType,
                        currentCachePolicyType,
                        (int) mapL1KeyPresenceCacheSize,
                        (int) mapL2KeyPresenceCacheSize,
                        mapCacheHitRateThreshold,
                        mapCacheHitRateWindowSize,
                        mapCacheMinAccessesForBypassCheck,
                        mapKeyPresenceCacheEnabled,
                        mapBypassEnabled,
                        CachingStateBackendFactory.PresenceCacheImplementation.DEFAULT,
                        false,
                        null,
                        new org.apache.flink.configuration.Configuration(),
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0
                        );
        cachingKeyedStateBackend.setCurrentKey(testKey);

        lenient().when(mockDelegateState.getKeySerializer()).thenReturn(mockKeySerializer);
        lenient().when(mockDelegateState.getNamespaceSerializer()).thenReturn(mockNamespaceSerializer);
        lenient().when(mockDelegateState.getValueSerializer()).thenReturn(mockValueSerializer);

        cachingState =
                new CachingInternalValueState<>(
                        mockDelegateState,
                        cachingKeyedStateBackend,
                        l1CacheSize,
                        l2CacheSize,
                        maxActiveNamespaces,
                        0L,
                        currentCachePolicyType,
                        mapCacheHitRateThreshold,
                        mapCacheHitRateWindowSize,
                        mapCacheMinAccessesForBypassCheck,
                        mapBypassEnabled,
                        false,
                        new UnregisteredMetricsGroup());
        cachingState.setCurrentNamespace(testNamespace);
    }

    private void setPolicyAndSetup(CachingStateBackendFactory.CachePolicyType policyType) {
        this.currentCachePolicyType = policyType;
        setUp();
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testValueGet_cacheMiss_loadFromDelegate_populateL1(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        when(mockDelegateState.value()).thenReturn(testValue1);

        String retrievedValue1 = cachingState.value();

        assertEquals(testValue1, retrievedValue1, "Value from first call should match delegate");
        verify(mockDelegateState, times(1)).value();

        String retrievedValue2 = cachingState.value();

        assertEquals(
                testValue1, retrievedValue2, "Value from second call should match cached value");
        verify(mockDelegateState, times(1))
                .value();
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testValueGet_L1Hit(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        when(mockDelegateState.value()).thenReturn(testValue1);
        cachingState.value();
        verify(mockDelegateState, times(1)).value();

        String retrievedValue = cachingState.value();

        assertEquals(
                testValue1,
                retrievedValue,
                "Value from L1 hit should match initially cached value");
        verify(mockDelegateState, times(1)).value();
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testValueGet_L1Miss_L2Hit_promoteToL1(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        when(mockDelegateState.value()).thenReturn(testValue1);
        cachingState.value();
        verify(mockDelegateState, times(1)).value();

        String anotherKey1 = "anotherKey1";
        String anotherValue1 = "anotherValue1";
        cachingKeyedStateBackend.setCurrentKey(anotherKey1);
        when(mockDelegateState.value())
                .thenReturn(anotherValue1);
        cachingState.value();
        verify(mockDelegateState, times(2)).value();

        String anotherKey2 = "anotherKey2";
        String anotherValue2 = "anotherValue2";
        cachingKeyedStateBackend.setCurrentKey(anotherKey2);
        when(mockDelegateState.value())
                .thenReturn(anotherValue2);
        cachingState.value();
        verify(mockDelegateState, times(3)).value();

        cachingKeyedStateBackend.setCurrentKey(testKey);

        String retrievedValue = cachingState.value();

        assertEquals(testValue1, retrievedValue, "Value from L2 hit should match original value");
        verify(mockDelegateState, times(3)).value();
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testUpdate_newValue_marksDirtyInL1_evictsL2IfExists(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.value()).thenReturn(testValue1);
        cachingState.value();
        verify(mockDelegateState, times(1)).value();

        String anotherKey1 = "anotherKey1";
        String anotherValue1 = "anotherValue1";
        cachingKeyedStateBackend.setCurrentKey(anotherKey1);
        when(mockDelegateState.value()).thenReturn(anotherValue1);
        cachingState.value();
        verify(mockDelegateState, times(2)).value();

        String anotherKey2 = "anotherKey2";
        String anotherValue2 = "anotherValue2";
        cachingKeyedStateBackend.setCurrentKey(anotherKey2);
        when(mockDelegateState.value()).thenReturn(anotherValue2);
        cachingState.value();
        verify(mockDelegateState, times(3)).value();

        cachingKeyedStateBackend.setCurrentKey(testKey);
        String testValue2_updated = "testValue2_updated";
        cachingState.update(testValue2_updated);
        assertEquals(
                testValue2_updated,
                cachingState.value(),
                "Value after update should be the new value from L1.");
        verify(mockDelegateState, times(3)).value();

        String forceEvictKey1 = "forceEvictKey1_for_update_test";
        String forceEvictValue1 = "forceEvictValue1_for_update_test";
        cachingKeyedStateBackend.setCurrentKey(forceEvictKey1);
        when(mockDelegateState.value()).thenReturn(forceEvictValue1);
        cachingState.value();
        verify(mockDelegateState, times(4)).value();

        if (policyType == CachingStateBackendFactory.CachePolicyType.TINYLFU) {
            for (int i = 0; i < 5; i++) {
                cachingState.value();
            }
        }

        String forceEvictKey2 = "forceEvictKey2_for_update_test";
        String forceEvictValue2 = "forceEvictValue2_for_update_test";
        cachingKeyedStateBackend.setCurrentKey(forceEvictKey2);
        when(mockDelegateState.value()).thenReturn(forceEvictValue2);
        cachingState.value();
        verify(mockDelegateState, times(5)).value();

        verify(mockDelegateState, times(1)).update(testValue2_updated);
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testUpdate_nullValue_clearsStateAndCache(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.value()).thenReturn(testValue1);
        cachingState.value();
        verify(mockDelegateState, times(1)).value();

        cachingState.update(null);

        verify(mockDelegateState, times(1)).clear();

        when(mockDelegateState.value()).thenReturn(null);
        assertEquals(null, cachingState.value(), "Value after update(null) should be null.");

        verify(mockDelegateState, times(2)).value();
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testL1Eviction_cleanEntry_moveToL2(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.value()).thenReturn(testValue1);
        cachingState.value();
        verify(mockDelegateState, times(1)).value();

        String anotherKey1 = "anotherKey1_L1EvictClean";
        String anotherValue1 = "anotherValue1_L1EvictClean";
        cachingKeyedStateBackend.setCurrentKey(anotherKey1);
        when(mockDelegateState.value()).thenReturn(anotherValue1);
        cachingState.value();
        verify(mockDelegateState, times(2)).value();

        String anotherKey2 = "anotherKey2_L1EvictClean";
        String anotherValue2 = "anotherValue2_L1EvictClean";
        cachingKeyedStateBackend.setCurrentKey(anotherKey2);
        when(mockDelegateState.value()).thenReturn(anotherValue2);
        cachingState.value();
        verify(mockDelegateState, times(3)).value();

        cachingKeyedStateBackend.setCurrentKey(testKey);
        assertEquals(
                testValue1, cachingState.value(), "Value should be retrieved from L2 (was clean)");
        verify(mockDelegateState, times(3)).value();
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testL1Eviction_dirtyEntry_flushToDelegate_moveToL2Clean(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        cachingKeyedStateBackend.setCurrentKey(testKey);
        cachingState.update(testValue1);
        assertEquals(testValue1, cachingState.value());
        verify(mockDelegateState, times(0))
                .value();

        String anotherKey1 = "anotherKey1_L1EvictDirty";
        String anotherValue1 = "anotherValue1_L1EvictDirty";
        cachingKeyedStateBackend.setCurrentKey(anotherKey1);
        when(mockDelegateState.value()).thenReturn(anotherValue1);
        cachingState.value();
        verify(mockDelegateState, times(1)).value();

        // For TinyLFU, ensure anotherKey1 (candidate from Window for Main cache)
        // has higher frequency than testKey (victim in Main cache).
        if (policyType == CachingStateBackendFactory.CachePolicyType.TINYLFU) {
            for (int i = 0; i < 5; i++) { // Arbitrary number of accesses
                cachingState.value(); // Accesses anotherKey1 in L1 (Window)
            }
             // After anotherKey1 is potentially in Main, access it more to ensure it stays
            // if it's chosen as a victim later, or if testKey is re-evaluated.
            // This also helps if the initial accesses kept it in Window and it's now being promoted.
            cachingKeyedStateBackend.setCurrentKey(anotherKey1); // Ensure context
             for (int i = 0; i < 5; i++) {
                 cachingState.value();
            }
        }

        String anotherKey2 = "anotherKey2_L1EvictDirty";
        String anotherValue2 = "anotherValue2_L1EvictDirty";
        cachingKeyedStateBackend.setCurrentKey(anotherKey2);
        when(mockDelegateState.value()).thenReturn(anotherValue2);
        cachingState.value();
        verify(mockDelegateState, times(2)).value();

        verify(mockDelegateState, times(1)).update(testValue1);

        cachingKeyedStateBackend.setCurrentKey(testKey);
        assertEquals(
                testValue1,
                cachingState.value(),
                "Value should be retrieved from L2 (was flushed and marked clean)");
        verify(mockDelegateState, times(2)).value();
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testL2Eviction(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.value()).thenReturn(testValue1);
        cachingState.value();
        int delegateGetCalls = 1;
        verify(mockDelegateState, times(delegateGetCalls)).value();

        String fillerKey1 = "l2_evict_filler1_for_testKey";
        cachingKeyedStateBackend.setCurrentKey(fillerKey1);
        when(mockDelegateState.value()).thenReturn("fv1");
        cachingState.value();
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).value();

        cachingKeyedStateBackend.setCurrentKey(fillerKey1);
        cachingState.value();
        verify(mockDelegateState, times(delegateGetCalls)).value();

        String fillerKey2_for_L1_evict_testKey = "l2_evict_filler2_for_testKey_L1_evict";
        cachingKeyedStateBackend.setCurrentKey(fillerKey2_for_L1_evict_testKey);
        when(mockDelegateState.value()).thenReturn("fv2_for_L1_evict");
        cachingState.value();
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).value();

        String keyL2_2 = "keyL2_2";
        String valL2_2 = "valL2_2";
        cachingKeyedStateBackend.setCurrentKey(keyL2_2);
        when(mockDelegateState.value()).thenReturn(valL2_2);
        cachingState.value();
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).value();

        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler1_for_keyL2_2");
        when(mockDelegateState.value()).thenReturn("fv_l2_f1");
        cachingState.value();
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).value();

        String keyL2_3 = "keyL2_3";
        String valL2_3 = "valL2_3";
        cachingKeyedStateBackend.setCurrentKey(keyL2_3);
        when(mockDelegateState.value()).thenReturn(valL2_3);
        cachingState.value();
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).value();

        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler1_for_keyL2_3");
        when(mockDelegateState.value()).thenReturn("fv_l2_f3");
        cachingState.value();
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).value();

        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler2_for_keyL2_3");
        when(mockDelegateState.value()).thenReturn("fv_l2_f4");
        cachingState.value();
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).value();

        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.value()).thenReturn(testValue1);
        assertEquals(
                testValue1,
                cachingState.value(),
                "Value should be retrieved from delegate after L2 eviction.");

        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).value();
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMultipleKeys_cachesAreSeparate(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        String key1 = "mk1";
        String val1 = "val_mk1";
        String key2 = "mk2";
        String val2 = "val_mk2";

        cachingKeyedStateBackend.setCurrentKey(testKey);

        cachingState.setCurrentNamespace(key1);
        when(mockDelegateState.value()).thenReturn(val1);
        assertEquals(
                val1,
                cachingState.value(),
                "Value for key in ns1 should be from delegate initially.");
        verify(mockDelegateState, times(1)).value();

        cachingState.setCurrentNamespace(key2);
        when(mockDelegateState.value()).thenReturn(val2);
        assertEquals(
                val2,
                cachingState.value(),
                "Value for key in ns2 should be from delegate initially.");
        verify(mockDelegateState, times(2)).value();

        cachingState.setCurrentNamespace(key1);
        assertEquals(
                val1,
                cachingState.value(),
                "Value for key in ns1 should be retrieved from its cache.");
        verify(mockDelegateState, times(2)).value();

        cachingState.setCurrentNamespace(key2);
        assertEquals(
                val2,
                cachingState.value(),
                "Value for key in ns2 should be retrieved from its cache.");
        verify(mockDelegateState, times(2)).value();
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMultipleNamespaces_cachesAreSeparate(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        String ns1 = "multi_ns_1";
        String valNs1 = "V_mns1";
        String ns2 = "multi_ns_2";
        String valNs2 = "V_mns2";

        cachingKeyedStateBackend.setCurrentKey(testKey);

        cachingState.setCurrentNamespace(ns1);
        when(mockDelegateState.value()).thenReturn(valNs1);
        assertEquals(
                valNs1,
                cachingState.value(),
                "Value for key in ns1 should be from delegate initially.");
        verify(mockDelegateState, times(1)).value();

        cachingState.setCurrentNamespace(ns2);
        when(mockDelegateState.value()).thenReturn(valNs2);
        assertEquals(
                valNs2,
                cachingState.value(),
                "Value for key in ns2 should be from delegate initially.");
        verify(mockDelegateState, times(2)).value();

        cachingState.setCurrentNamespace(ns1);
        assertEquals(
                valNs1,
                cachingState.value(),
                "Value for key in ns1 should be retrieved from its cache.");
        verify(mockDelegateState, times(2)).value();

        cachingState.setCurrentNamespace(ns2);
        assertEquals(
                valNs2,
                cachingState.value(),
                "Value for key in ns2 should be retrieved from its cache.");
        verify(mockDelegateState, times(2)).value();
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMaxActiveNamespaces_eviction(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        String ns1 = "max_ns_1";
        String valNs1 = "V_mns1";
        String ns2 = "max_ns_2";
        String valNs2 = "V_mns2";
        String ns3_evictor = "max_ns_3_evictor";
        String valNs3 = "V_mns3";

        cachingKeyedStateBackend.setCurrentKey(testKey);
        AtomicInteger delegateValueCallCount = new AtomicInteger(0);

        when(mockDelegateState.value())
                .thenAnswer(
                        invocation -> {
                            delegateValueCallCount.incrementAndGet();
                            String currentNs = cachingState.getCurrentNamespace();
                            if (ns1.equals(currentNs))
                                return valNs1;
                            if (ns2.equals(currentNs)) {
                                return valNs2;
                            }
                            if (ns3_evictor.equals(currentNs)) {
                                return valNs3;
                            }
                            throw new AssertionError(
                                    "Unexpected namespace in mockDelegateState.value(): "
                                    + currentNs);
                        });

        cachingState.setCurrentNamespace(ns1);
        assertEquals(valNs1, cachingState.value(), "Value for ns1 should be fetched initially.");
        assertEquals(1, delegateValueCallCount.get(), "Delegate should be called once for ns1 initial load.");

        cachingState.setCurrentNamespace(ns2);
        assertEquals(valNs2, cachingState.value(), "Value for ns2 should be fetched initially.");
        assertEquals(2, delegateValueCallCount.get(), "Delegate should be called for ns2 initial load.");

        cachingState.setCurrentNamespace(ns1);
        assertEquals(valNs1, cachingState.value(), "Value for ns1 should be from cache.");
        assertEquals(2, delegateValueCallCount.get(), "Delegate call count should not increase for ns1 cache hit.");

        // Access ns2 more to make it more frequent/recent than ns1 in the namespace-level cache
        if (policyType == CachingStateBackendFactory.CachePolicyType.TINYLFU) {
            for (int i = 0; i < 5; i++) {
                cachingState.setCurrentNamespace(ns2);
                assertEquals(valNs2, cachingState.value(), "Value for ns2 should be from cache during extra ns2 accesses.");
            }
        }
        // Reset to ns2 before ns3 to ensure ns1 is LRU if policy is LRU at namespace level
        cachingState.setCurrentNamespace(ns2);
        assertEquals(valNs2, cachingState.value(), "Value for ns2 should be from cache.");
        assertEquals(2, delegateValueCallCount.get(), "Delegate call count should not increase for ns2 cache hit.");

        cachingState.setCurrentNamespace(ns3_evictor);
        assertEquals(valNs3, cachingState.value(), "Value for ns3 should be fetched initially.");
        assertEquals(3, delegateValueCallCount.get(), "Delegate should be called for ns3 initial load, ns1 evicted.");

        cachingState.setCurrentNamespace(ns1);
        assertEquals(valNs1, cachingState.value(), "Value for ns1 should be re-fetched after namespace eviction.");
        assertEquals(4, delegateValueCallCount.get(), "Delegate should be called for ns1 re-fetch.");

        cachingState.setCurrentNamespace(ns2);
        assertEquals(valNs2, cachingState.value(), "Value for ns2 should be re-fetched after ns3_evictor caused its eviction.");
        if (policyType == CachingStateBackendFactory.CachePolicyType.TINYLFU) {
            assertEquals(4, delegateValueCallCount.get(), "TinyLFU: Delegate should NOT be called for ns2 re-fetch if it was kept due to frequency.");
        } else {
            assertEquals(5, delegateValueCallCount.get(), "LRU: Delegate should be called for ns2 re-fetch.");
        }

        cachingState.setCurrentNamespace(ns3_evictor);
        assertEquals(valNs3, cachingState.value(), "Value for ns3 should be re-fetched after ns1 caused its eviction.");
        if (policyType == CachingStateBackendFactory.CachePolicyType.TINYLFU) {
            assertEquals(5, delegateValueCallCount.get(), "TinyLFU: Delegate should be called for ns3 re-fetch as ns1 was likely evicted from Window.");
        } else {
            assertEquals(6, delegateValueCallCount.get(), "LRU: Delegate should be called for ns3 re-fetch.");
        }
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testFlush_writesDirtyEntriesToDelegate_marksClean(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        String ns1 = "flush_ns1";
        String ns2 = "flush_ns2";

        String key1Ns1Dirty = "f_key1_ns1_dirty";
        String val1Ns1Dirty = "dirty_val1_ns1";
        String key2Ns1Dirty = "f_key2_ns1_dirty";
        String val2Ns1Dirty = "dirty_val2_ns1";

        String key1Ns2Dirty = "f_key1_ns2_dirty";
        String val1Ns2Dirty = "dirty_val1_ns2";

        String keyCleanNs1 = "f_key_clean_ns1";
        String valCleanNs1 = "clean_val_ns1";
        String keyCleanNs2 = "f_key_clean_ns2";
        String valCleanNs2 = "clean_val_ns2";

        cachingState.setCurrentNamespace(ns1);

        cachingKeyedStateBackend.setCurrentKey(key1Ns1Dirty);
        cachingState.update(val1Ns1Dirty);

        cachingKeyedStateBackend.setCurrentKey(key2Ns1Dirty);
        cachingState.update(val2Ns1Dirty);

        cachingState.setCurrentNamespace(ns2);

        cachingKeyedStateBackend.setCurrentKey(key1Ns2Dirty);
        cachingState.update(val1Ns2Dirty);

        cachingKeyedStateBackend.setCurrentKey(keyCleanNs2);
        when(mockDelegateState.value()).thenReturn(valCleanNs2);
        cachingState.value();

        cachingState.setCurrentNamespace(ns1);
        cachingKeyedStateBackend.setCurrentKey(keyCleanNs1);
        when(mockDelegateState.value()).thenReturn(valCleanNs1);
        cachingState.value();

        cachingState.setCurrentNamespace(testNamespace);
        cachingKeyedStateBackend.setCurrentKey(testKey);

        cachingState.flushToUnderlyingState();

        verify(mockDelegateState, times(1)).update(val1Ns1Dirty);
        verify(mockDelegateState, times(1)).update(val2Ns1Dirty);
        verify(mockDelegateState, times(1)).update(val1Ns2Dirty);
        verify(mockDelegateState, times(0)).update(valCleanNs1);
        verify(mockDelegateState, times(0)).update(valCleanNs2);

        cachingState.setCurrentNamespace(ns1);
        cachingKeyedStateBackend.setCurrentKey(key1Ns1Dirty);

        cachingKeyedStateBackend.setCurrentKey("flushed_evictor1_ns1");
        when(mockDelegateState.value()).thenReturn("fe1");
        cachingState.value();

        cachingKeyedStateBackend.setCurrentKey("flushed_evictor2_ns1");
        when(mockDelegateState.value()).thenReturn("fe2");
        cachingState.value();

        verify(mockDelegateState, times(1)).update(val1Ns1Dirty);

        cachingKeyedStateBackend.setCurrentKey(key1Ns1Dirty);
        // If key1Ns1Dirty is reloaded from delegate, it should return the flushed value.
        lenient().when(mockDelegateState.value()).thenReturn(val1Ns1Dirty);
        assertEquals(
                val1Ns1Dirty,
                cachingState.value(),
                "Value should be available after flush and L1 eviction (from L2 or re-load)");
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testFlush_noDirtyEntries_doesNothing(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        String ns1 = "flush_clean_ns1";
        String key1Ns1 = "f_key1_ns1_clean";
        cachingState.setCurrentNamespace(ns1);
        cachingKeyedStateBackend.setCurrentKey(key1Ns1);
        when(mockDelegateState.value()).thenReturn("val_c1_n1");
        cachingState.value();

        verify(mockDelegateState, times(0)).update(org.mockito.ArgumentMatchers.anyString());
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testClear_removesFromL1L2AndDelegate(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.value()).thenReturn(testValue1);
        cachingState.value();
        verify(mockDelegateState, times(1)).value();

        String fillerKey1 = "clear_filler1";
        cachingKeyedStateBackend.setCurrentKey(fillerKey1);
        when(mockDelegateState.value()).thenReturn("fv1_clear");
        cachingState.value();
        verify(mockDelegateState, times(2)).value();

        String fillerKey2 = "clear_filler2";
        cachingKeyedStateBackend.setCurrentKey(fillerKey2);
        when(mockDelegateState.value()).thenReturn("fv2_clear");
        cachingState.value();
        verify(mockDelegateState, times(3)).value();

        cachingKeyedStateBackend.setCurrentKey(testKey);
        cachingState.clear();

        verify(mockDelegateState, times(1)).clear();

        when(mockDelegateState.value())
                .thenReturn(null);
        assertEquals(
                null,
                cachingState.value(),
                "Value should be null after clear (cache miss, from delegate)");
        verify(mockDelegateState, times(4)).value();
    }

    @Test
    void testSerializersAreDelegated() {
        setPolicyAndSetup(CachingStateBackendFactory.CachePolicyType.LRU);
        assertEquals(
                mockKeySerializer,
                cachingState.getKeySerializer(),
                "Key serializer should be delegated.");
        assertEquals(
                mockNamespaceSerializer,
                cachingState.getNamespaceSerializer(),
                "Namespace serializer should be delegated.");
        assertEquals(
                mockValueSerializer,
                cachingState.getValueSerializer(),
                "Value serializer should be delegated.");
    }

    @Test
    void testSetCurrentNamespaceIsDelegated() {
        setPolicyAndSetup(CachingStateBackendFactory.CachePolicyType.LRU);
        // After setUp, mockDelegateState.setCurrentNamespace(testNamespace) was called once.
        // We reset the mock to only focus on the behavior within this test method's main body.
        reset(mockDelegateState);

        String newNamespace = "newTestNamespace";
        cachingState.setCurrentNamespace(newNamespace);

        // Verify that setCurrentNamespace(newNamespace) was called on the delegate.
        verify(mockDelegateState, times(1)).setCurrentNamespace(newNamespace);
        // Verify that no other interactions happened with setCurrentNamespace on the delegate.
        verify(mockDelegateState, times(0)).setCurrentNamespace(testNamespace);
        verifyNoMoreInteractions(mockDelegateState);

        assertEquals(
                newNamespace,
                cachingState.getCurrentNamespace(),
                "Caching state should report the new namespace.");
    }

    @Test
    void testGetSerializedValueIsDelegated() throws Exception {
        setPolicyAndSetup(CachingStateBackendFactory.CachePolicyType.LRU);

        byte[] keyAndNamespace = new byte[] {1, 2, 3};
        byte[] expectedSerializedValue = new byte[] {4, 5, 6};

        when(mockDelegateState.getSerializedValue(
                        keyAndNamespace,
                        mockKeySerializer,
                        mockNamespaceSerializer,
                        mockValueSerializer
                        ))
                .thenReturn(expectedSerializedValue);

        byte[] actualSerializedValue =
                cachingState.getSerializedValue(
                        keyAndNamespace,
                        mockKeySerializer,
                        mockNamespaceSerializer,
                        mockValueSerializer);

        org.junit.jupiter.api.Assertions.assertArrayEquals(
                expectedSerializedValue,
                actualSerializedValue,
                "Serialized value should be delegated.");

        verify(mockDelegateState, times(1))
                .getSerializedValue(
                        keyAndNamespace,
                        mockKeySerializer,
                        mockNamespaceSerializer,
                        mockValueSerializer);
    }

    @Test
    void testGetStateIncrementalVisitorIsDelegated() {
        setPolicyAndSetup(CachingStateBackendFactory.CachePolicyType.LRU);

        int recommendedMaxNumberOfReturnedRecords = 100;
        @SuppressWarnings("unchecked")
        InternalKvState.StateIncrementalVisitor<String, String, String> mockVisitor =
                (InternalKvState.StateIncrementalVisitor<String, String, String>)
                        mock(InternalKvState.StateIncrementalVisitor.class);

        when(mockDelegateState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords))
                .thenReturn(mockVisitor);

        InternalKvState.StateIncrementalVisitor<String, String, String> actualVisitor =
                cachingState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);

        assertEquals(mockVisitor, actualVisitor, "StateIncrementalVisitor should be delegated.");

        verify(mockDelegateState, times(1))
                .getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }
}
