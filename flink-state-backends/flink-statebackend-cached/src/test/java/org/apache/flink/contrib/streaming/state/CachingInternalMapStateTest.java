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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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

    @BeforeEach
    void setUp() {
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
                CachePolicyType.LRU);
        cachingKeyedStateBackend.setCurrentKey(testFlinkKey);

        when(mockDelegateState.getKeySerializer()).thenReturn(mockKeySerializer);
        when(mockDelegateState.getNamespaceSerializer()).thenReturn(mockNamespaceSerializer);
        when(mockDelegateState.getValueSerializer()).thenReturn(mockMapValueSerializer);
        when(mockMapValueSerializer.getKeySerializer()).thenReturn(mockUserKeySerializer);
        when(mockMapValueSerializer.getValueSerializer()).thenReturn(mockUserValueSerializer);

        cachingMapState = new CachingInternalMapState<>(mockDelegateState, cachingKeyedStateBackend,
                l1CacheSizePerMap, l2CacheSizePerMap,
                maxActiveFlinkKeysWithActiveCachesPerNamespace, maxCacheMemoryMb,
                CachePolicyType.LRU);
        
        cachingMapState.setCurrentNamespace(testNamespace);
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

    @Test
    void testMapGet_cacheMiss_loadFromDelegate_populateL1() throws Exception {
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);

        String value = cachingMapState.get(testUserKey1);
        assertEquals(testUserValue1, value);
        verify(mockDelegateState, times(1)).get(testUserKey1);

        value = cachingMapState.get(testUserKey1);
        assertEquals(testUserValue1, value);
        verify(mockDelegateState, times(1)).get(testUserKey1);
    }

    @Test
    void testMapGet_L1Hit() throws Exception {
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        cachingMapState.get(testUserKey1);
        verify(mockDelegateState, times(1)).get(testUserKey1);

        String value = cachingMapState.get(testUserKey1);
        assertEquals(testUserValue1, value);
        verify(mockDelegateState, times(1)).get(testUserKey1);
    }

    @Test
    void testMapGet_L1Miss_L2Hit_promoteToL1() throws Exception {
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

    @Test
    void testMapPut_newUserEntry_marksDirtyInL1_evictsL2() throws Exception {
        String flinkKeyForL2Setup = "flinkKeyForL2PutTest";
        cachingKeyedStateBackend.setCurrentKey(flinkKeyForL2Setup);
        cachingMapState.setCurrentNamespace(testNamespace);

        String initialUserKeyInL2 = "initialUkInL2";
        String initialUserValueInL2 = "initialUvInL2";

        when(mockDelegateState.get(initialUserKeyInL2)).thenReturn(initialUserValueInL2);
        cachingMapState.get(initialUserKeyInL2);

        when(mockDelegateState.get("fillerUk1")).thenReturn("fillerUv1");
        cachingMapState.get("fillerUk1");
        when(mockDelegateState.get("fillerUk2")).thenReturn("fillerUv2");
        cachingMapState.get("fillerUk2");

        cachingKeyedStateBackend.setCurrentKey(testFlinkKey);
        cachingMapState.setCurrentNamespace(testNamespace);

        cachingMapState.put(testUserKey1, testUserValue1);

        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));
        verify(mockDelegateState, never()).get(testUserKey1);

        when(mockDelegateState.get(testUserKey2)).thenReturn(testUserValue2);
        cachingMapState.get(testUserKey2);
        when(mockDelegateState.get(testUserKey3)).thenReturn(testUserValue3);
        cachingMapState.get(testUserKey3);

        verify(mockDelegateState, times(1)).put(testUserKey1, testUserValue1);
    }

    @Test
    void testMapPut_existingUserEntry_updatesInL1_marksDirty() throws Exception {
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        cachingMapState.get(testUserKey1);
        verify(mockDelegateState, times(1)).get(testUserKey1);

        String updatedUserValue = "updatedValue";
        cachingMapState.put(testUserKey1, updatedUserValue);

        assertEquals(updatedUserValue, cachingMapState.get(testUserKey1));
        verify(mockDelegateState, times(1)).get(testUserKey1);

        when(mockDelegateState.get(testUserKey2)).thenReturn(testUserValue2);
        cachingMapState.get(testUserKey2);
        when(mockDelegateState.get(testUserKey3)).thenReturn(testUserValue3);
        cachingMapState.get(testUserKey3);

        verify(mockDelegateState, times(1)).put(testUserKey1, updatedUserValue);
    }

    @Test
    void testMapPut_nullValue_removesUserEntry() throws Exception {
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        cachingMapState.get(testUserKey1);
        verify(mockDelegateState, times(1)).get(testUserKey1);

        cachingMapState.put(testUserKey1, null);

        assertNull(cachingMapState.get(testUserKey1));
        assertFalse(cachingMapState.contains(testUserKey1));
        verify(mockDelegateState, times(1)).get(testUserKey1);

        when(mockDelegateState.get(testUserKey2)).thenReturn(testUserValue2);
        cachingMapState.get(testUserKey2);
        when(mockDelegateState.get(testUserKey3)).thenReturn(testUserValue3);
        cachingMapState.get(testUserKey3);

        verify(mockDelegateState, times(1)).remove(testUserKey1);
    }

    @Test
    void testMapRemove_userEntry_marksDirtyNullInL1_evictsL2() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testFlinkKey);
        cachingMapState.setCurrentNamespace(testNamespace);
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        cachingMapState.get(testUserKey1);
        when(mockDelegateState.get(testUserKey2)).thenReturn(testUserValue2);
        cachingMapState.get(testUserKey2);
        when(mockDelegateState.get(testUserKey3)).thenReturn(testUserValue3);
        cachingMapState.get(testUserKey3);

        cachingMapState.remove(testUserKey1);

        assertNull(cachingMapState.get(testUserKey1));
        assertFalse(cachingMapState.contains(testUserKey1));

        when(mockDelegateState.get(testUserKey2)).thenReturn(testUserValue2);
        cachingMapState.get(testUserKey2);
        when(mockDelegateState.get(testUserKey3)).thenReturn(testUserValue3);
        cachingMapState.get(testUserKey3);

        verify(mockDelegateState, times(1)).remove(testUserKey1);
    }

    @Test
    void testMapContains_userKey() throws Exception {
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

    @Test
    void testMapL1Eviction_cleanEntry_moveToL2() throws Exception {
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

    @Test
    void testMapL1Eviction_dirtyEntry_flushToDelegate_moveToL2Clean() throws Exception {
        cachingMapState.put(testUserKey1, testUserValue1);

        when(mockDelegateState.get(testUserKey2)).thenReturn(testUserValue2);
        cachingMapState.get(testUserKey2);

        when(mockDelegateState.get(testUserKey3)).thenReturn(testUserValue3);
        cachingMapState.get(testUserKey3);

        verify(mockDelegateState, times(1)).put(testUserKey1, testUserValue1);

        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));
        verify(mockDelegateState, never()).get(testUserKey1);
    }

    @Test
    void testMapL1Eviction_dirtyRemove_flushToDelegate_notInL2() throws Exception {
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        cachingMapState.get(testUserKey1);
        cachingMapState.remove(testUserKey1);

        when(mockDelegateState.get(testUserKey2)).thenReturn(testUserValue2);
        cachingMapState.get(testUserKey2);
        when(mockDelegateState.get(testUserKey3)).thenReturn(testUserValue3);
        cachingMapState.get(testUserKey3);

        verify(mockDelegateState, times(1)).remove(testUserKey1);

        when(mockDelegateState.get(testUserKey1)).thenReturn(null);
        assertNull(cachingMapState.get(testUserKey1));
        verify(mockDelegateState, times(1)).get(testUserKey1);
    }

    @Test
    void testMapL2Eviction() throws Exception {
        when(mockDelegateState.get("uk1")).thenReturn("uv1");
        cachingMapState.get("uk1");
        when(mockDelegateState.get("fillL1_1a")).thenReturn("fillL1_1a_v");
        cachingMapState.get("fillL1_1a");
        when(mockDelegateState.get("fillL1_1b")).thenReturn("fillL1_1b_v");
        cachingMapState.get("fillL1_1b");

        when(mockDelegateState.get("uk2")).thenReturn("uv2");
        cachingMapState.get("uk2");
        when(mockDelegateState.get("fillL1_2a")).thenReturn("fillL1_2a_v");
        cachingMapState.get("fillL1_2a");
        when(mockDelegateState.get("fillL1_2b")).thenReturn("fillL1_2b_v");
        cachingMapState.get("fillL1_2b");

        when(mockDelegateState.get("uk3")).thenReturn("uv3");
        cachingMapState.get("uk3");
        when(mockDelegateState.get("fillL1_3a")).thenReturn("fillL1_3a_v");
        cachingMapState.get("fillL1_3a");
        when(mockDelegateState.get("fillL1_3b")).thenReturn("fillL1_3b_v");
        cachingMapState.get("fillL1_3b");

        when(mockDelegateState.get("uk1")).thenReturn("uv1_reloaded");
        assertEquals("uv1_reloaded", cachingMapState.get("uk1"));
        verify(mockDelegateState, times(2)).get("uk1");
    }

    @Test
    void testMapEntries_iterator_loadsAllIfCacheNotFullAndDirtyFlushed() throws Exception {
        cachingMapState.put(testUserKey1, testUserValue1);

        when(mockDelegateState.get(testUserKey3)).thenReturn(testUserValue3);
        String tempFlinkKeyL2 = "tempFlinkKeyForL2";
        cachingKeyedStateBackend.setCurrentKey(tempFlinkKeyL2);
        cachingMapState.put(testUserKey1, null); // L1: uk1->null (dirty, tombstone).
                                                 // Internally,
                                                 // this should translate to a remove operation.

        // Get should return null and not hit delegate for get.
        assertEquals(null, cachingMapState.get(testUserKey1));
        verify(mockDelegateState, times(1)).get(testUserKey1); // Still only one GET call.

        // Evict to trigger flush (which should be a remove).
        when(mockDelegateState.get("putnull_evictor1")).thenReturn("pnev1");
        cachingMapState.get("putnull_evictor1");
        when(mockDelegateState.get("putnull_evictor2")).thenReturn("pnev2");
        cachingMapState.get("putnull_evictor2"); // uk1 (tombstone) is evicted and flushed.

        verify(mockDelegateState, times(1)).remove(testUserKey1); // remove(testUserKey1) should be
                                                                  // called on flush.
        // Verify total delegate.get calls
        verify(mockDelegateState, times(1)).get(testUserKey1);
        verify(mockDelegateState, times(1)).get("putnull_evictor1");
        verify(mockDelegateState, times(1)).get("putnull_evictor2");
    }

    @Test
    void testMapValues_iterator_loadsAllIfCacheNotFull() throws Exception {
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

    @Test
    void testMapKeys_iterator_loadsAllIfCacheNotFull() throws Exception {
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

    @Test
    void testMapIsEmpty() throws Exception {
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

    @Test
    void testMapPutAll() throws Exception {
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

        // Pre-populate L2 with uk2 to check L2 invalidation - this part needs care with the general
        // get mock
        // To avoid conflict, let actual pre-population use specific whens that might override the
        // general one if needed,
        // or ensure the general one returns what's expected for these keys if they were in
        // delegate.
        // For simplicity, let's assume delegate is initially empty for this test wrt these keys.
        // The pre-population of L2 means delegate *will* be called for those keys.
        // So, the general mock for get should be active *before* L2 pre-population calls.
        // Let delegateBackingMap reflect the state *before* L2 pre-population gets for these
        // specific calls.
        delegateBackingMap.put(testUserKey2, "old_uv2"); // Simulate this exists in delegate for L2
                                                         // pre-pop
        // delegateBackingMap.put("f_pa1", "v_pa1"); // If these are also from delegate
        // delegateBackingMap.put("f_pa2", "v_pa2");

        // Specific whens for L2 pre-population to ensure delegate responds as expected for these
        // specific calls.
        // These will take precedence over the general delegateBackingMap.get if called for these
        // keys.
        when(mockDelegateState.get(testUserKey2)).thenReturn("old_uv2"); // uk2 exists for L2
                                                                         // pre-pop
        cachingMapState.get(testUserKey2); // L1: {uk2 -> old_uv2 (clean)}, from delegate

        when(mockDelegateState.get("f_pa1")).thenReturn("v_pa1");
        cachingMapState.get("f_pa1"); // L1: {uk2, f_pa1}
        when(mockDelegateState.get("f_pa2")).thenReturn("v_pa2");
        cachingMapState.get("f_pa2"); // L1: {f_pa1, f_pa2}. uk2 evicted to L2: {uk2 -> old_uv2
                                      // (clean)}

        // Now, clear delegateBackingMap for main test phase if pre-pop values aren't meant to
        // persist in delegate
        // or ensure putAll correctly overwrites/removes them.
        // The test implies putAll operates on a state that might have old_uv2.
        // The .put(testUserKey2, testUserValue2) should overwrite it. The .remove(testUserKey3) is
        // separate.
        // delegateBackingMap.put(testUserKey1, testUserValue1) will be new.
        // So, delegateBackingMap should be {testUserKey2 -> "old_uv2"} before putAll.

        cachingMapState.putAll(mapToPut);

        // After putAll, delegateBackingMap should reflect the flushed states:
        // uk1 was put(uk1,uv1) -> L1 full -> flushed to delegate. So delegate has uk1.
        // uk2 was put(uk2,uv2), L2 entry for old_uv2 invalidated. uk2 stays dirty in L1 (or flushed
        // if L1 full).
        // uk3 was remove(uk3), tombstone in L1. delegate.remove(uk3) called upon flush.

        // Trace putAll with L1_size=2:
        // L1_before_putAll: {f_pa1, f_pa2} from pre-pop.
        // L2_before_putAll: {uk2->old_uv2}
        // mapToPut: {uk1->uv1, uk2->uv2, uk3->null}

        // 1. put(uk1, uv1):
        // L1: {f_pa2, uk1(d)}. f_pa1 evicted to L2 (if L2 has space, L2_size=2). L2:{uk2->old,
        // f_pa1}
        // 2. put(uk2, uv2):
        // L2 entry for uk2 (old_uv2) invalidated.
        // L1: {uk1(d), uk2(d)}. f_pa2 evicted to L2. L2:{f_pa1, f_pa2}
        // 3. remove(uk3):
        // L1: {uk2(d), uk3_tomb(d)}. uk1(d) evicted & flushed. delegate.put(uk1,uv1). uk1 to L2
        // (clean). L2:{f_pa2, uk1(c)}

        // State after putAll:
        // L1: {uk2(d), uk3_tomb(d)}
        // L2: {f_pa1(c), f_pa2(c), uk1(c)} -> L2 size is 2! So f_pa1 might be gone.
        // Let's assume LRU for L2: L2 becomes {f_pa2(c), uk1(c)} if f_pa1 was oldest from L2
        // pre-pop.
        // Delegate: {uk1->uv1} (from flush of uk1). And old_uv2 should be gone if put(uk2,uv2)
        // flushed. And uk3 removed.

        assertEquals(testUserValue1, cachingMapState.get(testUserKey1)); // Should be L2 hit for uk1
        assertEquals(testUserValue2, cachingMapState.get(testUserKey2)); // Should be L1 hit (or L2
                                                                         // if flushed by get(uk1))
        assertEquals(null, cachingMapState.get(testUserKey3));
        assertFalse(cachingMapState.contains(testUserKey3));

        // Verify delegate state via backing map AFTER all putAll operations and immediate gets.
        // By this point, uk1 was flushed by putAll. uk2 was in L1 dirty, then get(uk1) evicted it.
        // uk3 was remove, then tombstone flushed by an eviction.

        // To correctly verify flushes from putAll, we must evict L1 entries made by putAll.
        // L1 after putAll and get(uk1), get(uk2), get(uk3):
        // get(uk1): L1={uk3_tomb(d), uk1(c)}. uk2(d) flushed, to L2. delegate.put(uk2,uv2).
        // get(uk2): L1={uk1(c), uk2(c)}. uk3_tomb(d) flushed, to L2(tomb). delegate.remove(uk3).
        // get(uk3): L1={uk2(c), uk3_tomb(c)}. uk1(c) to L2.
        // This is getting complex. The original test's evictor pattern is simpler. We verify
        // *final* delegate state.

        // The verifications below are for flushes triggered by the *final* eviction phase.
        // We need to ensure the backing map reflects the state *after* putAll and *before* these
        // final evictions.
        // uk1 was flushed during putAll: delegateBackingMap.get(testUserKey1) == testUserValue1
        // uk2 was NOT necessarily flushed by putAll itself. It became dirty.
        // uk3 was NOT necessarily flushed by putAll itself. It became a dirty tombstone.

        // Let's verify delegate state *after* putAll completes and *before* the final eviction
        // loop.
        // At this point, uk1 should have been flushed. uk2, uk3 are dirty in cache.
        assertEquals(testUserValue1, delegateBackingMap.get(testUserKey1)); // uk1 flushed during
                                                                            // putAll
        // For uk2 and uk3, they are dirty in L1. Delegate map won't have their final state yet.

        // Evict all from L1 to check flushes of entries modified by putAll
        when(mockDelegateState.get("pa_ev1")).thenReturn("paev1"); // These gets should use this
                                                                   // mock, not backing map
        cachingMapState.get("pa_ev1");
        when(mockDelegateState.get("pa_ev2")).thenReturn("paev2");
        cachingMapState.get("pa_ev2");
        when(mockDelegateState.get("pa_ev3")).thenReturn("paev3");
        cachingMapState.get("pa_ev3");

        // Now, after evictions, verify all changes are in delegateBackingMap and on the mock
        assertEquals(testUserValue1, delegateBackingMap.get(testUserKey1)); // Flushed during putAll
        assertEquals(testUserValue2, delegateBackingMap.get(testUserKey2)); // Flushed by later
                                                                            // eviction
        assertFalse(delegateBackingMap.containsKey(testUserKey3)); // Removed by later eviction

        verify(mockDelegateState, times(1)).put(testUserKey1, testUserValue1); // This was flushed
                                                                               // during putAll's
                                                                               // internal L1
                                                                               // eviction
        verify(mockDelegateState, times(1)).put(testUserKey2, testUserValue2); // This was flushed
                                                                               // by the pa_evictors
        verify(mockDelegateState, times(1)).remove(testUserKey3); // This was flushed by the
                                                                  // pa_evictors
    }

    // --- Namespace and Flink Key Cache Management ---
    @Test
    void testMultipleFlinkKeys_cachesAreSeparate() throws Exception {
        String flinkKey1 = "mapFlinkKey1";
        String flinkKey2 = "mapFlinkKey2";

        // FK1: uk1->uv1
        cachingKeyedStateBackend.setCurrentKey(flinkKey1);
        cachingMapState.put(testUserKey1, testUserValue1);
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));

        // FK2: uk1->uv2 (same user key, different value for different Flink Key)
        cachingKeyedStateBackend.setCurrentKey(flinkKey2);
        cachingMapState.put(testUserKey1, testUserValue2);
        assertEquals(testUserValue2, cachingMapState.get(testUserKey1));

        // Check FK1 still has its value
        cachingKeyedStateBackend.setCurrentKey(flinkKey1);
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));
    }

    @Test
    void testMultipleNamespaces_mapCachesAreSeparate() throws Exception {
        String ns1 = "mapTestNs1";
        String ns2 = "mapTestNs2";

        // NS1: uk1->uv1
        cachingMapState.setCurrentNamespace(ns1);
        cachingMapState.put(testUserKey1, testUserValue1);
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));

        // NS2: uk1->uv2
        cachingMapState.setCurrentNamespace(ns2);
        cachingMapState.put(testUserKey1, testUserValue2);
        assertEquals(testUserValue2, cachingMapState.get(testUserKey1));

        // Check NS1 still has its value
        cachingMapState.setCurrentNamespace(ns1);
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));
    }

    @Test
    void testMaxActiveFlinkKeysPerNamespace_eviction() throws Exception {
        // maxActiveFlinkKeysPerNamespace is 2

        // Flink Key 1: put dirty entry
        cachingKeyedStateBackend.setCurrentKey("fk1");
        cachingMapState.put(testUserKey1, testUserValue1); // L1 for (fk1, testNamespace) has
                                                           // uk1->uv1 (dirty)

        // Flink Key 2: put dirty entry
        cachingKeyedStateBackend.setCurrentKey("fk2");
        cachingMapState.put(testUserKey2, testUserValue2); // L1 for (fk2, testNamespace) has
                                                           // uk2->uv2 (dirty)

        // Flink Key 3: access. This should evict cache for fk1 (LRU).
        // The dirty entry (uk1->uv1) for fk1 should be flushed.
        cachingKeyedStateBackend.setCurrentKey("fk3");
        when(mockDelegateState.get("someKeyForFk3")).thenReturn("someValueForFk3"); // For fk3
                                                                                    // access
        cachingMapState.get("someKeyForFk3");

        // Verify that the entry for fk1 was flushed (put to delegate)
        // The delegate's setCurrentNamespace would have been testNamespace, and key would be "fk1"
        // when the actual put happened internally during eviction.
        verify(mockDelegateState, times(1)).put(testUserKey1, testUserValue1);

        // Verify that entry for fk2 is still there (not flushed yet)
        verify(mockDelegateState, times(0)).put(testUserKey2, testUserValue2);

        // Access Flink Key 1 again. Its cache was evicted. Getting uk1 should go to delegate.
        cachingKeyedStateBackend.setCurrentKey("fk1");
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1); // Simulate it was
                                                                              // flushed and now in
                                                                              // delegate
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));
        // This get for fk1 (after its cache was evicted and the entry flushed) should call
        // delegate.get()
        verify(mockDelegateState, times(1)).get(testUserKey1); // Total for testUserKey1

        // Restore original context
        cachingKeyedStateBackend.setCurrentKey(testFlinkKey);
        cachingMapState.setCurrentNamespace(testNamespace);
    }

    // --- CachingInternalState Methods ---
    @Test
    void testFlushMap_writesDirtyEntriesToDelegate_marksClean() throws Exception {
        // Setup: current Flink key "fk1", namespace "flush_ns1"
        cachingKeyedStateBackend.setCurrentKey("fk1"); // Set Flink key context for the backend
        cachingMapState.setCurrentNamespace("flush_ns1");
        cachingMapState.put(testUserKey1, testUserValue1); // L1 dirty for (fk1, flush_ns1)
        cachingMapState.put(testUserKey2, testUserValue2); // L1 dirty for (fk1, flush_ns1)

        // Setup: current Flink key "fk2", namespace "flush_ns2"
        cachingKeyedStateBackend.setCurrentKey("fk2"); // Change Flink key context for the backend
        cachingMapState.setCurrentNamespace("flush_ns2");
        cachingMapState.put("anotherKey", "anotherValue"); // L1 dirty for (fk2, flush_ns2)

        // Call flush explicitly
        cachingMapState.flushToUnderlyingState(); // Corrected method name

        // Verify writes for "fk1", "flush_ns1"
        verify(mockDelegateState, times(1)).put(testUserKey1, testUserValue1);
        verify(mockDelegateState, times(1)).put(testUserKey2, testUserValue2);

        // Verify writes for "fk2", "flush_ns2"
        verify(mockDelegateState, times(1)).put("anotherKey", "anotherValue");

        // Reset Flink key/namespace to original for subsequent checks or other tests if needed.
        cachingKeyedStateBackend.setCurrentKey(testFlinkKey);
        cachingMapState.setCurrentNamespace(testNamespace);

        // Further checks for cleanliness:
        // Set context to fk1, flush_ns1
        cachingKeyedStateBackend.setCurrentKey("fk1");
        cachingMapState.setCurrentNamespace("flush_ns1");
        // After flush, getting them should not cause delegate.get if they are in cache (L1/L2)
        // We assume they are in L2 (or L1 if cache size allows and they were re-promoted)
        // To verify they are clean, we'd get them, then evict, then check no *more* puts.
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1); // Mock for L2 miss if
                                                                              // needed
        when(mockDelegateState.get(testUserKey2)).thenReturn(testUserValue2); // Mock for L2 miss if
                                                                              // needed
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));
        assertEquals(testUserValue2, cachingMapState.get(testUserKey2));

        // Evict them
        when(mockDelegateState.get("evictor_fk1_ns1_1")).thenReturn("v1");
        cachingMapState.get("evictor_fk1_ns1_1");
        when(mockDelegateState.get("evictor_fk1_ns1_2")).thenReturn("v2");
        cachingMapState.get("evictor_fk1_ns1_2");

        // Set context to fk2, flush_ns2
        cachingKeyedStateBackend.setCurrentKey("fk2");
        cachingMapState.setCurrentNamespace("flush_ns2");
        when(mockDelegateState.get("anotherKey")).thenReturn("anotherValue"); // Mock for L2 miss
        assertEquals("anotherValue", cachingMapState.get("anotherKey"));
        // Evict it
        when(mockDelegateState.get("evictor_fk2_ns2_1")).thenReturn("v3");
        cachingMapState.get("evictor_fk2_ns2_1");
        when(mockDelegateState.get("evictor_fk2_ns2_2")).thenReturn("v4");
        cachingMapState.get("evictor_fk2_ns2_2");

        // Verify no *additional* puts after the initial flush and subsequent evictions
        verify(mockDelegateState, times(1)).put(testUserKey1, testUserValue1);
        verify(mockDelegateState, times(1)).put(testUserKey2, testUserValue2);
        verify(mockDelegateState, times(1)).put("anotherKey", "anotherValue");

        // Restore original context again
        cachingKeyedStateBackend.setCurrentKey(testFlinkKey);
        cachingMapState.setCurrentNamespace(testNamespace);
    }

    @Test
    void testClearMap_removesFromAllCachesAndDelegate() throws Exception {
        // FK1, NS1: uk1 -> uv1 in L1, then L2
        cachingKeyedStateBackend.setCurrentKey("clear_fk");
        cachingMapState.setCurrentNamespace("clear_ns");
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        cachingMapState.get(testUserKey1); // L1
        cachingMapState.put("cf1", "v");
        cachingMapState.put("cf2", "v"); // uk1 to L2

        cachingMapState.clear();
        verify(mockDelegateState, times(1)).clear();

        when(mockDelegateState.get(testUserKey1)).thenReturn(null);
        when(mockDelegateState.contains(testUserKey1)).thenReturn(false);
        assertEquals(null, cachingMapState.get(testUserKey1));
        assertFalse(cachingMapState.contains(testUserKey1));
    }

    // --- Delegation of other InternalMapState/InternalKvState methods ---
    @Test
    void testMapSerializersAreDelegated() {
        // Delegate provides these directly
        assertEquals(mockKeySerializer, cachingMapState.getKeySerializer());
        assertEquals(mockNamespaceSerializer, cachingMapState.getNamespaceSerializer());

        // Caching layer uses the delegate's value serializer, which is a MapSerializer
        assertEquals(mockMapValueSerializer, cachingMapState.getValueSerializer());

        // UserKeySerializer and UserValueSerializer are derived from the mockMapValueSerializer
        when(mockMapValueSerializer.getKeySerializer()).thenReturn(mockUserKeySerializer);
        when(mockMapValueSerializer.getValueSerializer()).thenReturn(mockUserValueSerializer);

        assertEquals(mockUserKeySerializer, cachingMapState.getUserKeySerializer());
        assertEquals(mockUserValueSerializer, cachingMapState.getUserValueSerializer());

        // Verify that the caching state called the delegate for its own serializers
        verify(mockDelegateState, times(1)).getKeySerializer();
        verify(mockDelegateState, times(1)).getNamespaceSerializer();
        verify(mockDelegateState, times(2)).getValueSerializer(); // This gets the MapSerializer

        // Verify that the caching state called the MapSerializer for user key/value serializers
        verify(mockMapValueSerializer, times(1)).getKeySerializer();
        verify(mockMapValueSerializer, times(1)).getValueSerializer(); // CORRECTED TO 1
    }

    @Test
    void testMapGetValueSerializer_isAvailable() {
        // CachingInternalMapState.getValueSerializer() should return the MapSerializer from the
        // delegate.
        assertEquals(mockMapValueSerializer, cachingMapState.getValueSerializer());
        verify(mockDelegateState, times(2)).getValueSerializer(); // Verifies it was fetched from
                                                                  // delegate.

        // CachingInternalMapState.getUserValueSerializer() should get it from the MapSerializer.
        when(mockMapValueSerializer.getValueSerializer()).thenReturn(mockUserValueSerializer);
        assertEquals(mockUserValueSerializer, cachingMapState.getUserValueSerializer());
        verify(mockMapValueSerializer, times(1)).getValueSerializer(); // CORRECTED TO 1 (already
                                                                       // called in constructor,
                                                                       // this call in when() is the
                                                                       // second, but we verify
                                                                       // total after setup)
    }
}
