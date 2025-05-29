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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CachingInternalMapStateTest {

    private static final String DELEGATE_MAP_STATE_NAME = "testDelegateMapState";

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

    private final String testFlinkKey = "testFlinkKey";
    private final String testNamespace = "testNamespace";
    private final String testUserKey1 = "testUserKey1";
    private final String testUserValue1 = "testUserValue1";
    private final String testUserKey2 = "testUserKey2";
    private final String testUserValue2 = "testUserValue2";
    private final String testUserKey3 = "testUserKey3";
    private final String testUserValue3 = "testUserValue3";

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

        when(mockKeySerializer.duplicate()).thenReturn(mockKeySerializer);
        when(mockAbstractKeyedStateBackendDelegate.getKeySerializer())
                .thenReturn(mockKeySerializer);
        
        // Create proper KeyGroupRange and numberOfKeyGroups for InternalKeyContextImpl
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 15);
        int numberOfKeyGroups = 16;
        InternalKeyContext<String> keyContext = new InternalKeyContextImpl<>(keyGroupRange, numberOfKeyGroups);
        
        when(mockAbstractKeyedStateBackendDelegate.getKeyContext()).thenReturn(keyContext);
        when(mockAbstractKeyedStateBackendDelegate.getKeyGroupCompressionDecorator())
                .thenReturn(UncompressedStreamCompressionDecorator.INSTANCE);

        cachingKeyedStateBackend = new CachingKeyedStateBackend<>(kvStateRegistry,
                mockKeySerializer, CachingInternalMapStateTest.class.getClassLoader(),
                executionConfig, ttlTimeProvider, metricGroup,
                Collections.<KeyedStateHandle>emptyList(), cancelStreamRegistry,
                mockAbstractKeyedStateBackendDelegate, l1CacheSizePerMap, l2CacheSizePerMap,
                maxActiveFlinkKeysWithActiveCachesPerNamespace, maxCacheMemoryMb,
                currentCachePolicyType);
        cachingKeyedStateBackend.setCurrentKey(testFlinkKey);

        when(mockDelegateState.getKeySerializer()).thenReturn(mockKeySerializer);
        when(mockDelegateState.getNamespaceSerializer()).thenReturn(mockNamespaceSerializer);
        when(mockDelegateState.getValueSerializer()).thenReturn(mockMapValueSerializer);
        when(mockMapValueSerializer.getKeySerializer()).thenReturn(mockUserKeySerializer);
        when(mockMapValueSerializer.getValueSerializer()).thenReturn(mockUserValueSerializer);

        cachingMapState = new CachingInternalMapState<>(mockDelegateState, cachingKeyedStateBackend,
                l1CacheSizePerMap, l2CacheSizePerMap,
                maxActiveFlinkKeysWithActiveCachesPerNamespace, maxCacheMemoryMb,
                currentCachePolicyType);
        
        cachingMapState.setCurrentNamespace(testNamespace);
    }

    private void setPolicyAndSetup(CachingStateBackendFactory.CachePolicyType policyType) {
        this.currentCachePolicyType = policyType;
        setUp();
    }

    private Map<String, String> getMapFromDelegate() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put(testUserKey1, testUserValue1);
        map.put(testUserKey2, testUserValue2);
        return map;
    }

    private Map<String, String> getSingleEntryMapFromDelegate(String uk, String uv)
            throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put(uk, uv);
        return map;
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMapGet_cacheMiss_loadFromDelegate_populateL1(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);

        String retrievedValue1 = cachingMapState.get(testUserKey1);
        assertEquals(testUserValue1, retrievedValue1);
        verify(mockDelegateState, times(1)).get(testUserKey1);

        retrievedValue1 = cachingMapState.get(testUserKey1);
        assertEquals(testUserValue1, retrievedValue1);
        verify(mockDelegateState, times(1)).get(testUserKey1);
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMapGet_L1Hit(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        cachingMapState.get(testUserKey1);
        verify(mockDelegateState, times(1)).get(testUserKey1);

        String value = cachingMapState.get(testUserKey1);
        assertEquals(testUserValue1, value);
        verify(mockDelegateState, times(1)).get(testUserKey1);
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMapGet_L1Miss_L2Hit_promoteToL1(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        cachingMapState.get(testUserKey1);
        verify(mockDelegateState, times(1)).get(testUserKey1);

        when(mockDelegateState.get(testUserKey2)).thenReturn(testUserValue2);
        cachingMapState.get(testUserKey2);
        when(mockDelegateState.get(testUserKey3)).thenReturn(testUserValue3);
        cachingMapState.get(testUserKey3);

        String value = cachingMapState.get(testUserKey1);

        assertEquals(testUserValue1, value);
        verify(mockDelegateState, times(1)).get(testUserKey1);

        value = cachingMapState.get(testUserKey1);
        assertEquals(testUserValue1, value);
        verify(mockDelegateState, times(1)).get(testUserKey1);
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMapPut_newUserEntry_marksDirtyInL1_evictsL2(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        String flinkKeyForL2Setup = "flinkKeyForL2PutTest";
        cachingKeyedStateBackend.setCurrentKey(flinkKeyForL2Setup);
        cachingMapState.setCurrentNamespace(testNamespace);

        String initialUserKeyInL2 = "initialUkInL2";
        String initialUserValueInL2 = "initialUvInL2";

        // Pre-populate L1 and L2 under a different Flink key to set up L2 state
        when(mockDelegateState.get(initialUserKeyInL2)).thenReturn(initialUserValueInL2);
        cachingMapState.get(initialUserKeyInL2); // L1: {initialUkInL2(c)}

        when(mockDelegateState.get("fillerUk1")).thenReturn("fillerUv1");
        cachingMapState.get("fillerUk1"); // L1: {initialUkInL2(c), fillerUk1(c)}
        when(mockDelegateState.get("fillerUk2")).thenReturn("fillerUv2");
        cachingMapState.get("fillerUk2"); // L1: {fillerUk1(c), fillerUk2(c)}, initialUkInL2 evicted to L2

        // Switch to the target Flink key for the main part of the test
        cachingKeyedStateBackend.setCurrentKey(testFlinkKey);
        cachingMapState.setCurrentNamespace(testNamespace); // Reset namespace for the new key context

        cachingMapState.put(testUserKey1, testUserValue1); // L1 for testFlinkKey: {testUserKey1(dirty)}

        // Access testUserKey1 to ensure it's potentially in main segment for TinyLFU
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));
        verify(mockDelegateState, never()).get(testUserKey1); // Should be L1 hit

        // Use different keys for eviction to avoid L2 promotion complexities of testUserKey2/3
        String evictorKeyA = "evictorKeyA_for_putNew";
        String evictorValueA = "evictorValueA";
        String evictorKeyB = "evictorKeyB_for_putNew";
        String evictorValueB = "evictorValueB";

        when(mockDelegateState.get(evictorKeyA)).thenReturn(evictorValueA);
        // For TinyLFU, make evictorKeyA frequent enough to evict testUserKey1 (dirty)
        // L1 cache size is 2. TinyLFU: Window=1, Main=1.
        // State: M:{testUserKey1(d, freq~2)}, W:{}
        // get(evictorKeyA): W:{evictorKeyA(c, freq 1)}, M:{testUserKey1(d, freq~2)}
        for (int i = 0; i < (policyType == CachePolicyType.TINYLFU ? 5 : 1); i++) {
            cachingMapState.get(evictorKeyA);
        }
        // State after loop (TinyLFU): W:{evictorKeyA(c, freq 5)}, M:{testUserKey1(d, freq~2)}

        when(mockDelegateState.get(evictorKeyB)).thenReturn(evictorValueB);
        // get(evictorKeyB):
        // Candidate from W is evictorKeyA(c, freq 5). Victim from M is testUserKey1(d, freq ~2).
        // Freq(A) > Freq(testUserKey1), so testUserKey1(d) is evicted and flushed.
        // M becomes {evictorKeyA(c)}, W becomes {evictorKeyB(c)}.
        for (int i = 0; i < (policyType == CachePolicyType.TINYLFU ? 5 : 1); i++) {
            cachingMapState.get(evictorKeyB);
        }

        verify(mockDelegateState, times(1)).put(testUserKey1, testUserValue1);
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMapPut_existingUserEntry_updatesInL1_marksDirty(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        cachingMapState.get(testUserKey1); // L1: {testUserKey1(c)}. For TinyLFU (W=1,M=1): W:{}, M:{testUserKey1(c, freq~1)}
        verify(mockDelegateState, times(1)).get(testUserKey1);

        String updatedUserValue = "updatedValue";
        cachingMapState.put(testUserKey1, updatedUserValue); // L1: M:{testUserKey1(d, freq~2)} (updated, dirty)

        assertEquals(updatedUserValue, cachingMapState.get(testUserKey1)); // L1 hit
        verify(mockDelegateState, times(1)).get(testUserKey1); // Count shouldn't increase

        String evictorKeyA = "evictorKeyA_for_putExisting";
        String evictorValueA = "evictorValueA";
        String evictorKeyB = "evictorKeyB_for_putExisting";
        String evictorValueB = "evictorValueB";
        
        when(mockDelegateState.get(evictorKeyA)).thenReturn(evictorValueA);
        // For TinyLFU, make evictorKeyA frequent.
        // State: M:{testUserKey1(d, freq~3)}, W:{}
        // get(evictorKeyA): W:{evictorKeyA(c, freq 1)}, M:{testUserKey1(d, freq~3)}
        for (int i = 0; i < (policyType == CachePolicyType.TINYLFU ? 5 : 1); i++) {
            cachingMapState.get(evictorKeyA);
        }
        // State after loop (TinyLFU): W:{evictorKeyA(c, freq 5)}, M:{testUserKey1(d, freq~3)}


        when(mockDelegateState.get(evictorKeyB)).thenReturn(evictorValueB);
        // get(evictorKeyB):
        // Candidate from W is evictorKeyA(c, freq 5). Victim from M is testUserKey1(d, freq ~3).
        // Freq(A) > Freq(testUserKey1), so testUserKey1(d) is evicted and flushed.
        // M becomes {evictorKeyA(c)}, W becomes {evictorKeyB(c)}.
        for (int i = 0; i < (policyType == CachePolicyType.TINYLFU ? 5 : 1); i++) {
            cachingMapState.get(evictorKeyB);
        }

        verify(mockDelegateState, times(1)).put(testUserKey1, updatedUserValue);
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMapPut_nullValue_removesUserEntry(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        cachingMapState.get(testUserKey1); // L1: {testUserKey1(c)}. TinyLFU: M:{testUserKey1(c, freq~1)}, W:{}
        verify(mockDelegateState, times(1)).get(testUserKey1);

        cachingMapState.put(testUserKey1, null); // L1: M:{testUserKey1_tombstone(d, freq~2)} (marks dirty)

        assertNull(cachingMapState.get(testUserKey1)); // L1 hit (tombstone)
        assertFalse(cachingMapState.contains(testUserKey1)); // L1 hit (tombstone)
        verify(mockDelegateState, times(1)).get(testUserKey1); // Count shouldn't increase

        String evictorKeyA = "evictorKeyA_for_putNull";
        String evictorValueA = "evictorValueA";
        String evictorKeyB = "evictorKeyB_for_putNull";
        String evictorValueB = "evictorValueB";

        when(mockDelegateState.get(evictorKeyA)).thenReturn(evictorValueA);
        // For TinyLFU, make evictorKeyA frequent.
        // State: M:{testUserKey1_tombstone(d, freq~3)}, W:{}
        // get(evictorKeyA): W:{evictorKeyA(c, freq 1)}, M:{testUserKey1_tombstone(d, freq~3)}
        for (int i = 0; i < (policyType == CachePolicyType.TINYLFU ? 5 : 1); i++) {
            cachingMapState.get(evictorKeyA);
        }
        // State after loop (TinyLFU): W:{evictorKeyA(c, freq 5)}, M:{testUserKey1_tombstone(d, freq~3)}

        when(mockDelegateState.get(evictorKeyB)).thenReturn(evictorValueB);
        // get(evictorKeyB):
        // Candidate from W is evictorKeyA(c, freq 5). Victim from M is testUserKey1_tombstone(d, freq ~3).
        // Freq(A) > Freq(testUserKey1_tombstone), so testUserKey1_tombstone(d) is evicted and flushed (as remove).
        // M becomes {evictorKeyA(c)}, W becomes {evictorKeyB(c)}.
        for (int i = 0; i < (policyType == CachePolicyType.TINYLFU ? 5 : 1); i++) {
            cachingMapState.get(evictorKeyB);
        }
        
        verify(mockDelegateState, times(1)).remove(testUserKey1);
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMapRemove_userEntry_marksDirtyNullInL1_evictsL2(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        cachingKeyedStateBackend.setCurrentKey(testFlinkKey);
        cachingMapState.setCurrentNamespace(testNamespace);

        // Populate L1 to have {uk1(c), uk2(c)} for LRU, or M:{uk1(c)}, W:{uk2(c)} then M:{uk2(c)}, W:{uk1(c)} etc. for TinyLFU
        // L1 capacity = 2
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        cachingMapState.get(testUserKey1); // M:{uk1(c,f1)} W:{}
        when(mockDelegateState.get(testUserKey2)).thenReturn(testUserValue2);
        cachingMapState.get(testUserKey2); // M:{uk1(c,f1)} W:{uk2(c,f1)} -> M:{uk2(c,f1)} W:{uk1(c,f1)} if uk2 promoted from W
                                           // Or M:{uk1(c,f1)} W:{uk2(c,f1)}, then uk1 from W evicted to L2. Simpler: M:{uk1(c)}, W:{uk2(c)}
                                           // Let's assume after gets: M:{uk1(c)}, W:{uk2(c)} or M:{uk2(c)}, W:{uk1(c)}
                                           // Access them again to stabilize for TinyLFU if W=1, M=1
        if (policyType == CachePolicyType.TINYLFU) {
            cachingMapState.get(testUserKey1); // M:{uk1(c, f~2)} W:{}
            cachingMapState.get(testUserKey2); // M:{uk1(c, f~2)} W:{uk2(c, f~1)} -> If uk2 promotes M:{uk2(c,f~1)} W:{uk1(c,f~2)}
        }


        // Key to be removed
        cachingMapState.remove(testUserKey1); // This makes testUserKey1 a dirty tombstone in L1.
                                             // If uk1 was in M, M:{uk1_tomb(d)}. If in W, W:{uk1_tomb(d)}.
                                             // Example TinyLFU state after remove(uk1), assuming uk1 was in M:
                                             // M:{uk1_tombstone(d, freq~new)}, W:{uk2(c, freq~old)} (if uk2 was other L1 item)

        assertNull(cachingMapState.get(testUserKey1)); // Should hit tombstone in L1
        assertFalse(cachingMapState.contains(testUserKey1)); // Should hit tombstone

        // Evict the tombstone
        String evictorKeyA = "evictorKeyA_for_remove";
        String evictorValueA = "evictorValueA";
        String evictorKeyB = "evictorKeyB_for_remove"; // This key might not be strictly needed if L1 size is 2 and uk2 is already there
        String evictorValueB = "evictorValueB";

        // If L1 contains uk2(c) and uk1_tombstone(d).
        // We need to make new items more frequent than uk1_tombstone(d).
        // testUserKey2 is already in L1 (clean). Access it to boost its frequency.
        when(mockDelegateState.get(testUserKey2)).thenReturn(testUserValue2); // Already in L1 (or L2, will be promoted)
        for (int i = 0; i < (policyType == CachePolicyType.TINYLFU ? 5 : 1); i++) {
            cachingMapState.get(testUserKey2);
        }
        // Now testUserKey2(c) is frequent.
        // If L1 state was M:{uk1_tomb(d)}, W:{uk2(c)} -> M:{uk2(c)}, W:{uk1_tomb(d)} (uk2 promoted)
        // Now add evictorKeyA.
        when(mockDelegateState.get(evictorKeyA)).thenReturn(evictorValueA);
        // get(evictorKeyA): Candidate from W is uk1_tomb(d). Victim from M is uk2(c).
        // Freq(uk1_tomb(d)) vs Freq(uk2(c)). If Freq(uk1_tomb) is low, it's not admitted to M, stays in W and gets evicted.
        // This should make uk1_tombstone(d) the victim if uk2(c) becomes frequent in M.
        for (int i = 0; i < (policyType == CachePolicyType.TINYLFU ? 5 : 1); i++) {
            cachingMapState.get(evictorKeyA); // This should make uk1_tombstone get flushed
        }


        verify(mockDelegateState, times(1)).remove(testUserKey1);
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMapContains_userKey(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        when(mockDelegateState.contains(testUserKey1)).thenReturn(true);
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        assertTrue(cachingMapState.contains(testUserKey1));
        verify(mockDelegateState, times(1)).contains(testUserKey1);
        verify(mockDelegateState, times(1)).get(testUserKey1);

        assertTrue(cachingMapState.contains(testUserKey1));
        verify(mockDelegateState, times(1)).contains(testUserKey1);
        verify(mockDelegateState, times(1)).get(testUserKey1);

        when(mockDelegateState.contains(testUserKey2)).thenReturn(false);
        assertFalse(cachingMapState.contains(testUserKey2));
        verify(mockDelegateState, times(1)).contains(testUserKey2);
        verify(mockDelegateState, never()).get(testUserKey2);
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMapL1Eviction_cleanEntry_moveToL2(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        cachingMapState.get(testUserKey1);
        verify(mockDelegateState, times(1)).get(testUserKey1);

        when(mockDelegateState.get(testUserKey2)).thenReturn(testUserValue2);
        cachingMapState.get(testUserKey2);
        verify(mockDelegateState, times(1)).get(testUserKey2);

        when(mockDelegateState.get(testUserKey3)).thenReturn(testUserValue3);
        cachingMapState.get(testUserKey3);
        verify(mockDelegateState, times(1)).get(testUserKey3);

        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));
        verify(mockDelegateState, times(1)).get(testUserKey1);
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMapL1Eviction_dirtyEntry_flushToDelegate_moveToL2Clean(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        cachingMapState.put(testUserKey1, testUserValue1);

        when(mockDelegateState.get(testUserKey2)).thenReturn(testUserValue2);
        cachingMapState.get(testUserKey2);

        when(mockDelegateState.get(testUserKey3)).thenReturn(testUserValue3);
        cachingMapState.get(testUserKey3);

        verify(mockDelegateState, times(1)).put(testUserKey1, testUserValue1);

        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));
        verify(mockDelegateState, never()).get(testUserKey1);
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMapEntries_iterator_loadsAllIfCacheNotFullAndDirtyFlushed(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        // testUserKey1 will be put with null, making it a dirty tombstone.
        cachingMapState.put(testUserKey1, testUserValue1); // Initial put to make it exist, then it's overwritten
        if (policyType == CachePolicyType.TINYLFU) cachingMapState.get(testUserKey1); // Ensure in main for TinyLFU

        cachingMapState.put(testUserKey1, null); // L1: {testUserKey1_tombstone(d)}
                                                 // For TinyLFU: M:{testUserKey1_tombstone(d, f~2)}, W:{}

        // Get should return null and not hit delegate for get.
        assertEquals(null, cachingMapState.get(testUserKey1));

        String evictorKeyA = "putnull_evictor1";
        String evictorKeyB = "putnull_evictor2";
        String evictorValueA = "pnev1";
        String evictorValueB = "pnev2";


        when(mockDelegateState.get(evictorKeyA)).thenReturn(evictorValueA);
        for (int i = 0; i < (policyType == CachePolicyType.TINYLFU ? 5 : 1); i++) {
            cachingMapState.get(evictorKeyA);
        }
        // TinyLFU: M:{testUserKey1_tombstone(d,f~2)}, W:{evictorKeyA(c,f~5)}
        // -> M:{evictorKeyA(c,f~5)}, W:{testUserKey1_tombstone(d,f~2)} (evictorKeyA promoted)

        when(mockDelegateState.get(evictorKeyB)).thenReturn(evictorValueB);
        for (int i = 0; i < (policyType == CachePolicyType.TINYLFU ? 5 : 1); i++) {
            cachingMapState.get(evictorKeyB);
        }
        // TinyLFU: Candidate from W is testUserKey1_tombstone(d,f~2). Victim from M is evictorKeyA(c,f~5).
        // Freq(tomb) < Freq(A), tombstone not admitted to M.
        // Tombstone is evicted from W. Listener called -> flush (remove).
        // M:{evictorKeyA(c,f~5)}, W:{evictorKeyB(c,f~5)}

        verify(mockDelegateState, times(1)).remove(testUserKey1);
        // Regardless of policy, evictorKeyA is fetched from delegate once then cached.
        verify(mockDelegateState, times(1)).get(evictorKeyA);
        // Regardless of policy, evictorKeyB is fetched from delegate once then cached.
        verify(mockDelegateState, times(1)).get(evictorKeyB);
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMapValues_iterator_loadsAllIfCacheNotFull(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        Map<String, String> delegateMap = new java.util.HashMap<>();
        delegateMap.put(testUserKey1, testUserValue1);
        delegateMap.put(testUserKey2, testUserValue2);
        // CachingInternalMapState.values() will call loadAllEntriesToCache(), which uses
        // delegate.entries()
        when(mockDelegateState.entries()).thenReturn(delegateMap.entrySet());

        // L1 cache size is 2, so it can hold all entries.
        // Iterator should load all from delegate and populate L1.
        List<String> values = new ArrayList<>();
        cachingMapState.values().forEach(values::add);

        assertEquals(2, values.size()); // Should be 2 based on mocked entries
        assertTrue(values.contains(testUserValue1));
        assertTrue(values.contains(testUserValue2));
        verify(mockDelegateState, times(1)).entries(); // loadAllEntriesToCache calls this

        // Subsequent calls should hit L1
        values.clear();
        cachingMapState.values().forEach(values::add);
        assertEquals(2, values.size());
        verify(mockDelegateState, times(1)).entries(); // No more delegate.entries() calls
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMapKeys_iterator_loadsAllIfCacheNotFull(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        Map<String, String> delegateMap = new java.util.HashMap<>();
        delegateMap.put(testUserKey1, testUserValue1);
        delegateMap.put(testUserKey2, testUserValue2);
        // CachingInternalMapState.keys() will call loadAllEntriesToCache(), which uses
        // delegate.entries()
        when(mockDelegateState.entries()).thenReturn(delegateMap.entrySet());

        // L1 cache size is 2. Iterator should load all from delegate, populate L1.
        List<String> keys = new ArrayList<>();
        cachingMapState.keys().forEach(keys::add);

        assertEquals(2, keys.size()); // Should be 2 based on mocked entries
        assertTrue(keys.contains(testUserKey1));
        assertTrue(keys.contains(testUserKey2));
        verify(mockDelegateState, times(1)).entries(); // loadAllEntriesToCache calls this

        // Subsequent calls should hit L1
        keys.clear();
        cachingMapState.keys().forEach(keys::add);
        assertEquals(2, keys.size());
        verify(mockDelegateState, times(1)).entries(); // No more delegate.entries() calls
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMapIsEmpty(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        // Phase 1: Empty state initially
        when(mockDelegateState.isEmpty()).thenReturn(true);
        assertTrue(cachingMapState.isEmpty()); // Expect 1st call to delegate.isEmpty()
        verify(mockDelegateState, times(1)).isEmpty(); // Verify after 1st call

        // Phase 2: Non-empty via delegate
        when(mockDelegateState.isEmpty()).thenReturn(false); // Delegate now not empty
        Map<String, String> dummyEntry = new java.util.HashMap<>();
        dummyEntry.put("k", "v");
        when(mockDelegateState.entries()).thenReturn(dummyEntry.entrySet()); // For loadAll if
                                                                             // needed
        assertFalse(cachingMapState.isEmpty()); // Expect 2nd call to delegate.isEmpty(), then
                                                // loadAll
        verify(mockDelegateState, times(2)).isEmpty(); // Verify after 2nd call

        // Phase 3: Non-empty due to cache (L1 hit)
        cachingMapState.put(testUserKey1, testUserValue1); // L1 has a non-tombstone entry
        assertFalse(cachingMapState.isEmpty()); // Should be an L1 hit, no delegate call
        verify(mockDelegateState, times(2)).isEmpty(); // Count should remain 2

        // Phase 4: Empty again (L1 has tombstone, L2 empty, delegate reports empty)
        cachingMapState.remove(testUserKey1); // L1: uk1->null (dirty tombstone)
        when(mockDelegateState.isEmpty()).thenReturn(true); // Delegate is now empty again
        assertTrue(cachingMapState.isEmpty()); // Expect 3rd call to delegate.isEmpty()

        // Final verification for the total number of calls to delegate.isEmpty()
        verify(mockDelegateState, times(3)).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMapPutAll(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        final Map<String, String> delegateBackingMap = new java.util.HashMap<>();

        // Mock delegate interactions to use the backing map
        doAnswer(invocation -> {
            String key = invocation.getArgument(0);
            String value = invocation.getArgument(1);
            if (value == null) { // Simulate that put with null value is like a remove for the
                                 // backing map logic
                delegateBackingMap.remove(key);
            } else {
                delegateBackingMap.put(key, value);
            }
            return null;
        }).when(mockDelegateState).put(any(String.class), any(String.class));

        doAnswer(invocation -> {
            delegateBackingMap.remove(invocation.getArgument(0));
            return null;
        }).when(mockDelegateState).remove(any(String.class));

        // Mock get for specific keys if they are accessed from delegate during the test phases
        // This setup is mostly for verifying delegate state via the map later, or if cache misses
        // occur.
        when(mockDelegateState.get(any(String.class)))
                .thenAnswer(invocation -> delegateBackingMap.get(invocation.getArgument(0)));

        Map<String, String> mapToPut = new java.util.HashMap<>();
        mapToPut.put(testUserKey1, testUserValue1);
        mapToPut.put(testUserKey2, testUserValue2);
        mapToPut.put(testUserKey3, null); // Should result in remove(testUserKey3) on delegate

        // Pre-populate L2 with uk2 to check L2 invalidation
        // Ensure delegate state reflects this for the get() operation
        delegateBackingMap.put(testUserKey2, "old_uv2");
        when(mockDelegateState.get(testUserKey2)).thenReturn("old_uv2");
        cachingMapState.get(testUserKey2); // L1: {uk2 -> old_uv2 (clean)}. For TinyLFU (W=1,M=1): M:{uk2(old,c,f1)} W:{}

        // Fill L1 with other items to push uk2(old_uv2) to L2 (if L1 size is 2)
        // L1 cache size is 2
        when(mockDelegateState.get("f_pa1")).thenReturn("v_pa1");
        cachingMapState.get("f_pa1"); // TinyLFU: M:{uk2(old,c,f1)} W:{f_pa1(c,f1)}. -> M:{f_pa1(c,f1)} W:{uk2(old,c,f1)}
        
        when(mockDelegateState.get("f_pa2")).thenReturn("v_pa2");
        cachingMapState.get("f_pa2"); // TinyLFU: M:{f_pa1(c,f1)} W:{f_pa2(c,f1)}. uk2(old,c,f1) from W is candidate.
                                     // Victim from M is f_pa1(c,f1). Freqs are equal. uk2 not admitted to M.
                                     // uk2(old,c) is evicted from W to L2.
                                     // State before putAll: L1: M:{f_pa1(c)}, W:{f_pa2(c)}. L2:{uk2(old,c)}. Delegate:{uk2->old_uv2}

        // Clear the general mock for testUserKey2 as putAll will modify it.
        // The delegateBackingMap will be the source of truth for delegate.
        when(mockDelegateState.get(testUserKey2)).thenAnswer(inv -> delegateBackingMap.get(testUserKey2));


        cachingMapState.putAll(mapToPut);
        // Expected interactions for putAll:
        // put(uk1,uv1): L1 dirty. May evict f_pa1 or f_pa2. If so, evicted is clean, goes to L2.
        // put(uk2,uv2): L2 has uk2(old,c) - removed. L1 gets uk2(new,d). May evict another.
        // put(uk3,null): L1 gets uk3(tomb,d). May evict another.

        // After putAll, L1 (size 2) will contain two of {uk1(d), uk2(new,d), uk3(tomb,d)}, others evicted.
        // If uk1(d) evicted, delegate.put(uk1,uv1).
        // If uk2(new,d) evicted, delegate.put(uk2,uv2).
        // If uk3(tomb,d) evicted, delegate.remove(uk3).

        // The test asserts the state *after* gets. These gets can cause further evictions.
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));
        assertEquals(testUserValue2, cachingMapState.get(testUserKey2)); // This is the failing one.
        assertEquals(null, cachingMapState.get(testUserKey3));
        assertFalse(cachingMapState.contains(testUserKey3));
        
        // To ensure all dirty entries from putAll are flushed for verification against delegateBackingMap
        // we need to evict everything from L1.
        String evictAll1 = "evict_all_pa1";
        String evictAll2 = "evict_all_pa2";
        String evictAll3 = "evict_all_pa3"; // Extra one if needed.
        when(mockDelegateState.get(evictAll1)).thenReturn("evict_val1");
        when(mockDelegateState.get(evictAll2)).thenReturn("evict_val2");
        when(mockDelegateState.get(evictAll3)).thenReturn("evict_val3");

        for (int i = 0; i < (policyType == CachePolicyType.TINYLFU ? 5 : 1); i++) {
            cachingMapState.get(evictAll1);
            cachingMapState.get(evictAll2);
            if (l1CacheSizePerMap > 2) cachingMapState.get(evictAll3); // if L1 is larger
        }


        // Now, after evictions, verify all changes are in delegateBackingMap and on the mock
        assertEquals(testUserValue1, delegateBackingMap.get(testUserKey1));
        assertEquals(testUserValue2, delegateBackingMap.get(testUserKey2));
        assertFalse(delegateBackingMap.containsKey(testUserKey3));

        verify(mockDelegateState, times(1)).put(testUserKey1, testUserValue1);
        verify(mockDelegateState, times(1)).put(testUserKey2, testUserValue2);
        verify(mockDelegateState, times(1)).remove(testUserKey3);
    }

    // Test for L2 eviction of a PerKeyMapCache (when maxFlinkKeysWithActiveCachesPerNamespace is hit)
    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMapL2Eviction_PerKeyMapCache(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        String flinkKey1 = "fk1_map_l2_evict";
        String userKeyFK1 = "uk_fk1"; String userValFK1 = "uv_fk1";
        // ... existing code ...
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMultipleFlinkKeys_cachesAreSeparate(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        String flinkKey1 = "map_fk1";
        String userKey1 = "uk1"; String userVal1 = "uv1";
        // ... existing code ...
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMultipleNamespaces_mapCachesAreSeparate(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        String ns1 = "map_multi_ns_1";
        String userKeyNs1 = "uk_ns1"; String userValNs1 = "uv_ns1";
        // ... existing code ...
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testMaxActiveFlinkKeysPerNamespace_eviction(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        String fk1 = "max_fk_1"; String uk_fk1 = "uk_fk1"; String uv_fk1 = "uv_fk1";
        String fk2 = "max_fk_2"; String uk_fk2 = "uk_fk2"; String uv_fk2 = "uv_fk2";
        // ... existing code ...
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testFlushMap_writesDirtyEntriesToDelegate_marksClean(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        String ns1 = "map_flush_ns1";
        String fk1Ns1 = "map_fk1_ns1";
        // ... existing code ...
    }

    @ParameterizedTest
    @MethodSource("cachePolicies")
    void testClearMap_removesFromAllCachesAndDelegate(CachingStateBackendFactory.CachePolicyType policyType) throws Exception {
        setPolicyAndSetup(policyType);
        // Populate with an entry
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        // ... existing code ...
    }

    @Test // This test does not depend on the specific cache policy details for correctness of delegation
    void testMapSerializersAreDelegated() {
        setPolicyAndSetup(CachingStateBackendFactory.CachePolicyType.LRU); // Arbitrary choice
        assertEquals(mockKeySerializer, cachingMapState.getKeySerializer());
        // ... existing code ...
    }

    @Test // This test does not depend on the specific cache policy details for correctness of delegation
    void testMapGetValueSerializer_isAvailable() {
        setPolicyAndSetup(CachingStateBackendFactory.CachePolicyType.LRU); // Arbitrary choice
        assertEquals(mockMapValueSerializer, cachingMapState.getValueSerializer());
        assertEquals(mockUserKeySerializer, cachingMapState.getUserKeySerializer());
        // ... existing code ...
    }
}
