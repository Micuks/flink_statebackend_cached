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

import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

/** Test suite for {@link CachingInternalAggregatingState}. */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CachingInternalAggregatingStateTest {

    @Mock
    private InternalAggregatingState<String, String, String, String, String> mockDelegateState;

    private CachingKeyedStateBackend<String> cachingKeyedStateBackend;
    @Mock private AbstractKeyedStateBackend<String> mockAbstractKeyedStateBackendDelegate;
    @Mock private TypeSerializer<String> mockKeySerializer;
    @Mock private TypeSerializer<String> mockNamespaceSerializer;
    @Mock private TypeSerializer<String> mockValueSerializer;

    private CachingInternalAggregatingState<String, String, String, String, String> cachingState;
    private TestAggFunction testAggFunction;

    private final int l1CacheSize = 2;
    private final int l2CacheSize = 2;
    private final int maxActiveNamespaces = 2;
    private final String testKey = "testKey";
    private final String testNamespace = "testNamespace";

    private CachingStateBackendFactory.CachePolicyType currentCachePolicyType;

    static Stream<CachingStateBackendFactory.CachePolicyType> cachePolicies() {
        return Stream.of(
                CachingStateBackendFactory.CachePolicyType.LRU,
                CachingStateBackendFactory.CachePolicyType.TINYLFU);
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
        // Default to LRU for tests not needing a specific policy
        setPolicyAndSetup(CachingStateBackendFactory.CachePolicyType.LRU);
    }

    private CachingKeyedStateBackend<String> createKeyedStateBackend(
            AbstractKeyedStateBackend<String> delegate) {
        Configuration config = new Configuration();
        config.set(CachingStateBackendFactory.L1_CACHE_SIZE_CONFIG, (long) l1CacheSize);
        config.set(CachingStateBackendFactory.L2_CACHE_SIZE_CONFIG, (long) l2CacheSize);
        config.set(
                CachingStateBackendFactory.MAX_ACTIVE_NAMESPACES_CONFIG,
                (long) maxActiveNamespaces);
        config.set(CachingStateBackendFactory.CACHE_POLICY_CONFIG, currentCachePolicyType);
        return new CachingKeyedStateBackendBuilder<String>(delegate, config).build();
    }

    private void setPolicyAndSetup(CachingStateBackendFactory.CachePolicyType policyType) {
        currentCachePolicyType = policyType;
        when(mockAbstractKeyedStateBackendDelegate.getKeySerializer()).thenReturn(mockKeySerializer);

        cachingKeyedStateBackend = createKeyedStateBackend(mockAbstractKeyedStateBackendDelegate);

        testAggFunction = new TestAggFunction();

        cachingState =
                new CachingInternalAggregatingState<>(
                        mockDelegateState,
                        cachingKeyedStateBackend,
                        testAggFunction,
                        l1CacheSize,
                        l2CacheSize,
                        currentCachePolicyType);

        when(mockDelegateState.getKeySerializer()).thenReturn(mockKeySerializer);
        when(mockDelegateState.getNamespaceSerializer()).thenReturn(mockNamespaceSerializer);
        when(mockDelegateState.getValueSerializer()).thenReturn(mockValueSerializer);
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testAddAndGet_cacheMiss_loadFromDelegate(
            CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
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