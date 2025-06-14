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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.contrib.streaming.state.CachingStateBackendFactory.CachePolicyType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CachingInternalMapStateBypassTest {

    @Mock
    private InternalMapState<String, String, String, String> mockDelegateState;

    private CachingKeyedStateBackend<String> cachingKeyedStateBackend;
    @Mock
    private AbstractKeyedStateBackend<String> mockAbstractKeyedStateBackendDelegate;

    @Mock
    private TypeSerializer<String> mockKeySerializer;
    @Mock
    private TypeSerializer<String> mockNamespaceSerializer;
    @Mock
    private TypeSerializer<String> mockUserKeySerializer;
    @Mock
    private TypeSerializer<String> mockUserValueSerializer;
    @Mock
    private MapSerializer<String, String> mockMapValueSerializer;

    private CachingInternalMapState<String, String, String, String> cachingMapState;

    private final int l1CacheSizePerMap = 2;
    private final int l2CacheSizePerMap = 2;
    private final int maxActiveFlinkKeysWithActiveCachesPerNamespace = 2;
    private final long maxCacheMemoryMb = 10;
    private final int mapL1KeyPresenceCacheSize = 2;
    private final int mapL2KeyPresenceCacheSize = 2;

    private final String testFlinkKey = "testFlinkKey";
    private final String testNamespace = "testNamespace";

    private CachingStateBackendFactory.CachePolicyType currentCachePolicyType;

    static Stream<CachingStateBackendFactory.CachePolicyType> cachePolicies() {
        return Stream.of(CachingStateBackendFactory.CachePolicyType.LRU, CachingStateBackendFactory.CachePolicyType.TINYLFU);
    }

    @BeforeEach
    void setUp() {
        if (currentCachePolicyType == null) {
            currentCachePolicyType = CachingStateBackendFactory.CachePolicyType.LRU;
        }
        setPolicyAndSetup(currentCachePolicyType, 0.0, 1000, 10000, true, false);
    }

    private void setPolicyAndSetup(
        CachePolicyType policyType,
        double mapCacheHitRateThreshold,
        long mapCacheHitRateWindowSize,
        long mapCacheMinAccessesForBypassCheck,
        boolean keyPresenceCacheEnabled,
        boolean bypassEnabled) {

        this.currentCachePolicyType = policyType;

        TaskKvStateRegistry kvStateRegistry = mock(TaskKvStateRegistry.class);
        ExecutionConfig executionConfig = new ExecutionConfig();
        TtlTimeProvider ttlTimeProvider = TtlTimeProvider.DEFAULT;
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();

        when(mockKeySerializer.duplicate()).thenReturn(mockKeySerializer);
        when(mockAbstractKeyedStateBackendDelegate.getKeySerializer()).thenReturn(mockKeySerializer);
        when(mockAbstractKeyedStateBackendDelegate.getLatencyTrackingStateConfig()).thenReturn(mock(LatencyTrackingStateConfig.class));

        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 15);
        int numberOfKeyGroups = 16;
        InternalKeyContext<String> keyContext = new InternalKeyContextImpl<>(keyGroupRange, numberOfKeyGroups);

        when(mockAbstractKeyedStateBackendDelegate.getKeyContext()).thenReturn(keyContext);
        when(mockAbstractKeyedStateBackendDelegate.getKeyGroupCompressionDecorator()).thenReturn(UncompressedStreamCompressionDecorator.INSTANCE);
        
        cachingKeyedStateBackend = new CachingKeyedStateBackend<>(
            kvStateRegistry,
            mockKeySerializer,
            this.getClass().getClassLoader(),
            executionConfig,
            ttlTimeProvider,
            Collections.emptyList(),
            cancelStreamRegistry,
            mockAbstractKeyedStateBackendDelegate,
            l1CacheSizePerMap,
            l2CacheSizePerMap,
            maxActiveFlinkKeysWithActiveCachesPerNamespace,
            maxCacheMemoryMb,
            policyType,
            mapL1KeyPresenceCacheSize,
            mapL2KeyPresenceCacheSize,
            mapCacheHitRateThreshold,
            mapCacheHitRateWindowSize,
            mapCacheMinAccessesForBypassCheck,
            keyPresenceCacheEnabled,
            bypassEnabled,
            0.0,
            0,
            0,
            false
        );

        cachingKeyedStateBackend.setCurrentKey(testFlinkKey);

        when(mockDelegateState.getKeySerializer()).thenReturn(mockKeySerializer);
        when(mockDelegateState.getNamespaceSerializer()).thenReturn(mockNamespaceSerializer);
        when(mockDelegateState.getValueSerializer()).thenReturn(mockMapValueSerializer);
        when(mockMapValueSerializer.getKeySerializer()).thenReturn(mockUserKeySerializer);
        when(mockMapValueSerializer.getValueSerializer()).thenReturn(mockUserValueSerializer);

        cachingMapState =
            new CachingInternalMapState<>(
                mockDelegateState,
                cachingKeyedStateBackend,
                l1CacheSizePerMap,
                l2CacheSizePerMap,
                maxActiveFlinkKeysWithActiveCachesPerNamespace,
                maxCacheMemoryMb,
                policyType,
                mapL1KeyPresenceCacheSize,
                mapL2KeyPresenceCacheSize,
                mapCacheHitRateThreshold,
                mapCacheHitRateWindowSize,
                mapCacheMinAccessesForBypassCheck,
                keyPresenceCacheEnabled,
                bypassEnabled
            );

        cachingMapState.setCurrentNamespace(testNamespace);
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testBypassDisabled_ByDefaultOrZeroThreshold(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType, 0.0, 100, 1000, true, false);
        assertFalse(cachingMapState.isBypassCacheActive());

        when(mockDelegateState.get(anyString())).thenReturn(null);

        for (int i = 0; i < 2000; i++) {
            cachingMapState.get("key" + i);
        }

        assertFalse(cachingMapState.isBypassCacheActive());
    }
    
    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testCacheBypassWhenHitRateLow(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType, 0.5, 10, 10, true, true);

        for (int i = 0; i < 10; i++) {
            cachingMapState.get("miss" + i);
        }
        assertTrue(cachingMapState.isBypassCacheActive(), "Cache bypass should be active after 10 misses with a 50% threshold.");
    }
} 