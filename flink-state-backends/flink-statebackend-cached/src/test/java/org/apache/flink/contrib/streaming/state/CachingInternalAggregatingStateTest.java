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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CachingInternalAggregatingStateTest {

    @Mock
    private InternalAggregatingState<String, String, String, String, String> mockDelegateState;
    private CachingKeyedStateBackend<String> cachingKeyedStateBackend;
    @Mock
    private AbstractKeyedStateBackend<String> mockAbstractKeyedStateBackendDelegate;
    @Mock
    private TypeSerializer<String> mockKeySerializer;
    @Mock
    private TypeSerializer<String> mockNamespaceSerializer;
    @Mock
    private TypeSerializer<String> mockValueSerializer;

    private CachingInternalAggregatingState<String, String, String, String, String> cachingState;
    private TestAggFunction testAggFunction;

    private final int l1CacheSize = 2;
    private final int l2CacheSize = 2;
    private final int maxActiveNamespaces = 2;
    private final String testKey = "testKey";
    private final String testNamespace = "testNamespace";

    private CachingStateBackendFactory.CachePolicyType currentCachePolicyType;

    static Stream<CachingStateBackendFactory.CachePolicyType> cachePolicies() {
        return Stream.of(CachingStateBackendFactory.CachePolicyType.LRU, CachingStateBackendFactory.CachePolicyType.TINYLFU);
    }

    private static class TestAggFunction implements AggregateFunction<String, String, String> {
        @Override
        public String createAccumulator() {
            return "";
        }

        @Override
        public String add(String value, String accumulator) {
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
        lenient().when(mockAbstractKeyedStateBackendDelegate.getKeySerializer()).thenReturn(mockKeySerializer);
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 15);
        lenient().when(mockAbstractKeyedStateBackendDelegate.getKeyContext()).thenReturn(new org.apache.flink.runtime.state.heap.InternalKeyContextImpl<>(keyGroupRange, 16));

        long mapL1KeyPresenceCacheSize = CachingStateBackendFactory.MAP_L1_KEY_PRESENCE_CACHE_SIZE_CONFIG.defaultValue();
        long mapL2KeyPresenceCacheSize = CachingStateBackendFactory.MAP_L2_KEY_PRESENCE_CACHE_SIZE_CONFIG.defaultValue();
        double mapCacheHitRateThreshold = CachingStateBackendFactory.MAP_CACHE_HIT_RATE_THRESHOLD_CONFIG.defaultValue();
        long mapCacheHitRateWindowSize = CachingStateBackendFactory.MAP_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG.defaultValue();
        long mapCacheMinAccessesForBypassCheck = CachingStateBackendFactory.MAP_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG.defaultValue();
        boolean mapKeyPresenceCacheEnabled = CachingStateBackendFactory.MAP_KEY_PRESENCE_CACHE_ENABLED_CONFIG.defaultValue();
        boolean mapBypassEnabled = CachingStateBackendFactory.MAP_BYPASS_ENABLED_CONFIG.defaultValue();

        cachingKeyedStateBackend = new CachingKeyedStateBackend<>(
                kvStateRegistry,
                mockKeySerializer,
                getClass().getClassLoader(),
                executionConfig,
                ttlTimeProvider,
                metricGroup,
                Collections.emptyList(),
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

        testAggFunction = new TestAggFunction();
        cachingState = new CachingInternalAggregatingState<>(
                mockDelegateState,
                cachingKeyedStateBackend,
                testAggFunction,
                l1CacheSize,
                l2CacheSize,
                currentCachePolicyType,
                new UnregisteredMetricsGroup());
        cachingState.setCurrentNamespace(testNamespace);
    }

    private void setPolicyAndSetup(CachingStateBackendFactory.CachePolicyType policyType) {
        this.currentCachePolicyType = policyType;
        setUp();
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testAddAndGet_cacheMiss_loadFromDelegate(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        when(mockDelegateState.getInternal()).thenReturn("a");
        cachingState.add("b");
        verify(mockDelegateState, times(1)).getInternal();
        verify(mockDelegateState, times(0)).updateInternal("ab");
        assertEquals("ab", cachingState.get());
        verify(mockDelegateState, times(1)).getInternal(); // should hit cache now

        // now flush and verify
        cachingState.flushToUnderlyingState();
        verify(mockDelegateState, times(1)).updateInternal("ab");
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMergeNamespaces_flushesAndClearsCache(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        String targetNs = "target";
        String sourceNs1 = "source1";
        String sourceNs2 = "source2";

        cachingState.setCurrentNamespace(sourceNs1);
        cachingKeyedStateBackend.setCurrentKey("key1");
        cachingState.add("a"); // dirty entry

        cachingState.mergeNamespaces(targetNs, Arrays.asList(sourceNs1, sourceNs2));

        verify(mockDelegateState, times(1)).updateInternal("a"); // flush
        verify(mockDelegateState, times(1)).mergeNamespaces(targetNs, Arrays.asList(sourceNs1, sourceNs2));
    }
} 
