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
import org.apache.flink.contrib.streaming.state.CacheEntry;
import org.apache.flink.contrib.streaming.state.CachePolicy;
import org.apache.flink.contrib.streaming.state.LRUMap;
import org.apache.flink.contrib.streaming.state.TinyLFUMap;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.lenient;

import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.api.java.tuple.Tuple2;
import java.util.stream.Stream;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import static org.mockito.ArgumentMatchers.eq;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CachingInternalListStateTest {

    private static final String DELEGATE_LIST_STATE_NAME = "testDelegateListState";

    @Mock
    private TypeSerializer<String> mockKeySerializer; // K
    @Mock
    private TypeSerializer<String> mockNamespaceSerializer; // N
    @Mock
    private TypeSerializer<List<String>> mockValueSerializer; // V_SD (Value of State Descriptor, i.e. List<String>)

    @Mock
    private InternalListState<String, String, String> mockDelegateListState; // Direct delegate for SUT
    @Mock
    private AbstractKeyedStateBackend<String> mockAbstractKeyedStateBackendDelegate; // Delegate for CachingKeyedStateBackend

    private CachingKeyedStateBackend<String> cachingKeyedStateBackend;
    private CachingInternalListState<String, String, String> cachingListState; // System Under Test

    private final int l1CacheSize = 2;
    private final int l2CacheSize = 2;
    private final int maxActiveNamespaces = 2;

    private final String testKey = "testListKey";
    private final String testNamespace = "testListNamespace";
    private final String element1 = "element1";
    private final String element2 = "element2";
    private final String element3 = "element3";
    private List<String> delegateList; // Used for expected values

    private List<String> getAsList(CachingInternalListState<String, String, String> state)
            throws Exception {
        Iterable<String> iterable = state.get();
        if (iterable == null) {
            return null;
        }
        List<String> list = new ArrayList<>();
        iterable.forEach(list::add);
        return list;
    }

    @BeforeEach
    void setUp() {
        TaskKvStateRegistry kvStateRegistry = mock(TaskKvStateRegistry.class);
        ExecutionConfig executionConfig = new ExecutionConfig();
        TtlTimeProvider ttlTimeProvider = TtlTimeProvider.DEFAULT;
        MetricGroup metricGroup = new UnregisteredMetricsGroup();
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();

        // Common Serializer setup
        when(mockKeySerializer.duplicate()).thenReturn(mockKeySerializer);
        // mockValueSerializer is for List<String>
        // mockKeySerializer is also used as the element serializer for ListStateDescriptor

        // Setup for mockAbstractKeyedStateBackendDelegate (delegate of CachingKeyedStateBackend)
        when(mockAbstractKeyedStateBackendDelegate.getKeySerializer()).thenReturn(mockKeySerializer);
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 15);
        int numberOfKeyGroups = 16;
        InternalKeyContext<String> internalKeyContext
                = new org.apache.flink.runtime.state.heap.InternalKeyContextImpl<>(keyGroupRange, numberOfKeyGroups);
        when(mockAbstractKeyedStateBackendDelegate.getKeyContext()).thenReturn(internalKeyContext);
        when(mockAbstractKeyedStateBackendDelegate.getKeyGroupCompressionDecorator())
                .thenReturn(UncompressedStreamCompressionDecorator.INSTANCE);


        cachingKeyedStateBackend = new CachingKeyedStateBackend<>(
                kvStateRegistry,
                mockKeySerializer,
                Thread.currentThread().getContextClassLoader(),
                executionConfig,
                ttlTimeProvider,
                metricGroup,
                Collections.<KeyedStateHandle>emptyList(),
                cancelStreamRegistry,
                mockAbstractKeyedStateBackendDelegate, // Use the MOCK backend delegate
                l1CacheSize,
                l2CacheSize,
                maxActiveNamespaces,
                10, // maxCacheMemoryMb
                CachingStateBackendFactory.CachePolicyType.LRU
        );
        cachingKeyedStateBackend.setCurrentKey(testKey);

        // Setup for mockDelegateListState (direct delegate of CachingInternalListState SUT)
        when(mockDelegateListState.getKeySerializer()).thenReturn(mockKeySerializer);
        when(mockDelegateListState.getNamespaceSerializer()).thenReturn(mockNamespaceSerializer);
        when(mockDelegateListState.getValueSerializer()).thenReturn(mockValueSerializer);

        // Descriptor for the list state
        org.apache.flink.api.common.state.ListStateDescriptor<String> listStateDesc
                = new org.apache.flink.api.common.state.ListStateDescriptor<>(
                        DELEGATE_LIST_STATE_NAME,
                        mockKeySerializer); // Element serializer

        try {
            // Make CachingKeyedStateBackend's delegate (mockAbstractKeyedStateBackendDelegate)
            // return our mockDelegateListState when getOrCreateKeyedState is called.
            when(mockAbstractKeyedStateBackendDelegate.getOrCreateKeyedState(
                    eq(mockNamespaceSerializer),
                    eq(listStateDesc)))
                    .thenReturn(mockDelegateListState);

            // This call will now result in CachingInternalListState wrapping mockDelegateListState
            cachingListState = (CachingInternalListState<String, String, String>) cachingKeyedStateBackend.getOrCreateKeyedState(
                    mockNamespaceSerializer,
                    listStateDesc);
        } catch (Exception e) {
            throw new RuntimeException("Error creating CachingInternalListState in setUp", e);
        }
        cachingListState.setCurrentNamespace(testNamespace);

        // Initialize delegateList for tests that might use it for comparison or setup
        delegateList = new ArrayList<>(Arrays.asList(element1, element2));
    }

    @Test
    void testListGet_cacheMiss_loadFromDelegate_populateL1() throws Exception {
        when(mockDelegateListState.get()).thenReturn(new java.util.ArrayList<>(delegateList));

        List<String> retrievedList1 = getAsList(cachingListState);

        assertEquals(delegateList, retrievedList1, "List from first call should match delegate");
        verify(mockDelegateListState, times(1)).get();

        List<String> retrievedList2 = getAsList(cachingListState);

        assertEquals(delegateList, retrievedList2,
                "List from second call should match cached list");
        verify(mockDelegateListState, times(1)).get();

        retrievedList2.add("anotherElement");
        List<String> retrievedList3 = getAsList(cachingListState);
        assertEquals(delegateList, retrievedList3,
                "Modifying returned list should not affect cached list.");
        assertNotEquals(retrievedList2, retrievedList3);
    }

    @Test
    void testListGet_L1Hit_returnsCopy() throws Exception {
        when(mockDelegateListState.get()).thenReturn(new java.util.ArrayList<>(delegateList));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(1)).get();

        List<String> retrievedList1 = getAsList(cachingListState);

        assertEquals(delegateList, retrievedList1,
                "List from L1 hit should match initially cached list");
        verify(mockDelegateListState, times(1)).get();

        retrievedList1.add("anotherElementL1Hit");
        List<String> retrievedList2 = getAsList(cachingListState);

        assertEquals(delegateList, retrievedList2,
                "Modifying the first retrieved list should not affect the cached list for subsequent gets.");
        assertNotEquals(retrievedList1, retrievedList2,
                "Subsequent gets should return different list instances (copies).");
    }

    @Test
    void testListGet_L1Miss_L2Hit_promoteToL1_returnsCopy() throws Exception {
        int delegateGetCalls = 0;

        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState);
        delegateGetCalls++;
        verify(mockDelegateListState, times(delegateGetCalls)).get();

        String anotherKey1 = "listTestKey_L1MissL2Hit_Filler1";
        List<String> listForAnotherKey1 = Arrays.asList("ak1_e1");
        cachingKeyedStateBackend.setCurrentKey(anotherKey1);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(listForAnotherKey1));
        getAsList(cachingListState);
        delegateGetCalls++;
        verify(mockDelegateListState, times(delegateGetCalls)).get();

        String anotherKey2 = "listTestKey_L1MissL2Hit_Filler2";
        List<String> listForAnotherKey2 = Arrays.asList("ak2_e1");
        cachingKeyedStateBackend.setCurrentKey(anotherKey2);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(listForAnotherKey2));
        getAsList(cachingListState);
        delegateGetCalls++;
        verify(mockDelegateListState, times(delegateGetCalls)).get();

        cachingKeyedStateBackend.setCurrentKey(testKey);
        List<String> retrievedList1 = getAsList(cachingListState);

        assertEquals(delegateList, retrievedList1, "List should be retrieved from L2.");
        verify(mockDelegateListState, times(delegateGetCalls)).get();

        retrievedList1.add("modifiedAfterL2Hit");

        List<String> retrievedList2 = getAsList(cachingListState);
        assertEquals(delegateList, retrievedList2,
                "Second get after L2 hit should return original from L1.");
        assertNotEquals(retrievedList1, retrievedList2,
                "Returned lists should be different instances.");
    }

    @Test
    void testListUpdate_newList_marksDirtyInL1_evictsL2() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(1)).get();

        String fillerKey1 = "listUpdate_fillerKey1";
        cachingKeyedStateBackend.setCurrentKey(fillerKey1);
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(2)).get();

        String fillerKey2 = "listUpdate_fillerKey2";
        cachingKeyedStateBackend.setCurrentKey(fillerKey2);
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(3)).get();

        cachingKeyedStateBackend.setCurrentKey(testKey);
        List<String> newList = new ArrayList<>(Arrays.asList("new_el1", "new_el2"));

        cachingListState.update(newList);

        List<String> listFromCache1 = getAsList(cachingListState);
        assertEquals(newList, listFromCache1, "L1 should serve the new list after update.");
        assertNotEquals(System.identityHashCode(newList), System.identityHashCode(listFromCache1),
                "Get should return a copy from L1.");
        listFromCache1.add("modifiedCopy");
        List<String> listFromCache2 = getAsList(cachingListState);
        assertEquals(newList, listFromCache2,
                "L1 internal list not affected by copy modification.");
        verify(mockDelegateListState, times(3)).get();

        cachingKeyedStateBackend.setCurrentKey("listUpdate_evictor1");
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("evictorValue1"));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(4)).get();

        cachingKeyedStateBackend.setCurrentKey("listUpdate_evictor2");
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("evictorValue2"));
        getAsList(cachingListState);

        // Set key back to the one that was evicted and should be in L2
        cachingKeyedStateBackend.setCurrentKey(testKey); 
        // Add defensive stubbing for delegate in case L2 miss (though L2 hit is expected)
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(newList)); 

        List<String> listFromL2AfterEviction = getAsList(cachingListState);
        assertEquals(newList, listFromL2AfterEviction,
                "List should be served from L2 (clean) after dirty L1 eviction.");
        verify(mockDelegateListState, times(5)).get();
    }

    @Test
    void testListUpdate_null_marksDirtyInL1() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(1)).get();

        cachingListState.update(null);

        assertEquals(null, getAsList(cachingListState), "List after update(null) should be null.");
        verify(mockDelegateListState, times(1)).get();

        String fillerKey1 = "listUpdateNull_fillerKey1";
        cachingKeyedStateBackend.setCurrentKey(fillerKey1);
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(2)).get();

        String fillerKey2 = "listUpdateNull_fillerKey2";
        cachingKeyedStateBackend.setCurrentKey(fillerKey2);
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(3)).get();

        verify(mockDelegateListState, times(1)).update(null);
    }

    @Test
    void testListAdd_toNewList_marksDirtyInL1() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateListState.get()).thenReturn(null);

        String newElement = "addedElement1";
        cachingListState.add(newElement);

        List<String> listFromCache = getAsList(cachingListState);
        assertEquals(Arrays.asList(newElement), listFromCache,
                "List should contain the added element.");
        verify(mockDelegateListState, times(1)).get();

        String fillerKey1 = "listAdd_fillerKey1";
        cachingKeyedStateBackend.setCurrentKey(fillerKey1);
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(2)).get();

        String fillerKey2 = "listAdd_fillerKey2";
        cachingKeyedStateBackend.setCurrentKey(fillerKey2);
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(3)).get();

        verify(mockDelegateListState, times(1)).update(Arrays.asList(newElement));
    }

    @Test
    void testListAdd_toExistingCachedList_marksDirtyInL1() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(1)).get();

        String addedElement = "appendedToList";
        cachingListState.add(addedElement);

        List<String> expectedList = new ArrayList<>(delegateList);
        expectedList.add(addedElement);
        List<String> listFromCache = getAsList(cachingListState);
        assertEquals(expectedList, listFromCache,
                "List should contain the original and added elements.");
        verify(mockDelegateListState, times(1)).get();

        String fillerKey1 = "listAddExisting_fillerKey1";
        cachingKeyedStateBackend.setCurrentKey(fillerKey1);
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(2)).get();

        String fillerKey2 = "listAddExisting_fillerKey2";
        cachingKeyedStateBackend.setCurrentKey(fillerKey2);
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(3)).get();

        verify(mockDelegateListState, times(1)).update(expectedList);
    }

    @Test
    void testListAddAll_toNewList_marksDirtyInL1() throws Exception {
        cachingListState.setCurrentNamespace(testNamespace);
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateListState.get()).thenReturn(null);

        List<String> elementsToAdd = Arrays.asList(element1, element3);
        cachingListState.addAll(elementsToAdd);

        List<String> retrievedList = getAsList(cachingListState);
        assertEquals(elementsToAdd, retrievedList);
        verify(mockDelegateListState, times(1)).get();

        cachingKeyedStateBackend.setCurrentKey("fillerKey1_addAllNew");
        cachingListState.setCurrentNamespace(testNamespace);
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState);

        cachingKeyedStateBackend.setCurrentKey("fillerKey2_addAllNew");
        cachingListState.setCurrentNamespace(testNamespace);
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState);

        ArgumentCaptor<List<String>> listCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockDelegateListState, times(1)).update(listCaptor.capture());
        assertEquals(elementsToAdd, listCaptor.getValue());
    }

    @Test
    void testListAddAll_toExistingCachedList_marksDirtyInL1() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(1)).get();

        List<String> elementsToAdd = Arrays.asList(element3, "element4");
        cachingListState.addAll(elementsToAdd);

        List<String> expectedList = new ArrayList<>(delegateList);
        expectedList.addAll(elementsToAdd);

        assertEquals(expectedList, getAsList(cachingListState));
        verify(mockDelegateListState, times(1)).get();

        cachingKeyedStateBackend.setCurrentKey("fillerKey1_addAllExisting");
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState);
        cachingKeyedStateBackend.setCurrentKey("fillerKey2_addAllExisting");
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState);

        verify(mockDelegateListState, times(1)).update(expectedList);
    }

    @Test
    void testListGetList_sameAsGet() throws Exception {
    }

    @Test
    void testListL1Eviction_cleanEntry_moveToL2() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(1)).get();

        cachingKeyedStateBackend.setCurrentKey("evictor1_clean");
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("e1"));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(2)).get();

        cachingKeyedStateBackend.setCurrentKey("evictor2_clean");
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("e2"));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(3)).get();
        verify(mockDelegateListState, times(0)).update(org.mockito.ArgumentMatchers.anyList());

        cachingKeyedStateBackend.setCurrentKey(testKey);
        List<String> retrieved = getAsList(cachingListState);
        assertEquals(delegateList, retrieved);
        verify(mockDelegateListState, times(3)).get();
    }

    @Test
    void testListL1Eviction_dirtyEntry_flushToDelegate_moveToL2Clean() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testKey);
        List<String> updatedList = Arrays.asList(element1, element3, "dirtyElement");
        cachingListState.update(updatedList);

        cachingKeyedStateBackend.setCurrentKey("evictor1_dirty");
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("e1"));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(1)).get();

        cachingKeyedStateBackend.setCurrentKey("evictor2_dirty");
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("e2"));
        getAsList(cachingListState);

        verify(mockDelegateListState, times(1)).update(updatedList);

        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(updatedList));
        List<String> retrieved = getAsList(cachingListState);
        assertEquals(updatedList, retrieved);
        verify(mockDelegateListState, times(2)).get();
    }

    @Test
    void testListL2Eviction() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState);
        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler1_for_testKey");
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState);
        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler2_for_testKey");
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState);

        String keyL2_2 = "keyL2_2";
        List<String> listL2_2 = Arrays.asList("l2_e2_1");
        cachingKeyedStateBackend.setCurrentKey(keyL2_2);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(listL2_2));
        getAsList(cachingListState);
        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler1_for_keyL2_2");
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f3"));
        getAsList(cachingListState);
        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler2_for_keyL2_2");
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f4"));
        getAsList(cachingListState);

        String keyL2_3 = "keyL2_3";
        List<String> listL2_3 = Arrays.asList("l2_e3_1");
        cachingKeyedStateBackend.setCurrentKey(keyL2_3);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(listL2_3));
        getAsList(cachingListState);
        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler1_for_keyL2_3");
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f5"));
        getAsList(cachingListState);
        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler2_for_keyL2_3");
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f6"));
        getAsList(cachingListState);

        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(delegateList));
        List<String> retrieved = getAsList(cachingListState);
        assertEquals(delegateList, retrieved);
        verify(mockDelegateListState, times(10)).get();
    }

    @Test
    void testListMultipleKeys_cachesAreSeparate() throws Exception {
        String key1 = "mk1";
        List<String> list1 = Arrays.asList("mk1_e1", "mk1_e2");
        String key2 = "mk2";
        List<String> list2 = Arrays.asList("mk2_e1");

        cachingKeyedStateBackend.setCurrentKey(key1);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(list1));
        assertEquals(list1, getAsList(cachingListState));
        verify(mockDelegateListState, times(1)).get();

        cachingKeyedStateBackend.setCurrentKey(key2);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(list2));
        assertEquals(list2, getAsList(cachingListState));
        verify(mockDelegateListState, times(2)).get();

        cachingKeyedStateBackend.setCurrentKey(key1);
        assertEquals(list1, getAsList(cachingListState));
        verify(mockDelegateListState, times(2)).get();

        cachingKeyedStateBackend.setCurrentKey(key2);
        assertEquals(list2, getAsList(cachingListState));
        verify(mockDelegateListState, times(2)).get();
    }

    @Test
    void testListMultipleNamespaces_cachesAreSeparate() throws Exception {
        String ns1 = "multi_ns_1";
        List<String> listNs1 = Arrays.asList("L_mns1");
        String ns2 = "multi_ns_2";
        List<String> listNs2 = Arrays.asList("L_mns2");

        cachingKeyedStateBackend.setCurrentKey(testKey);

        cachingListState.setCurrentNamespace(ns1);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(listNs1));
        assertEquals(listNs1, getAsList(cachingListState));
        verify(mockDelegateListState, times(1)).get();
        verify(mockDelegateListState, times(1)).setCurrentNamespace(ns1);

        cachingListState.setCurrentNamespace(ns2);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(listNs2));
        assertEquals(listNs2, getAsList(cachingListState));
        verify(mockDelegateListState, times(2)).get();
        verify(mockDelegateListState, times(1)).setCurrentNamespace(ns2);

        cachingListState.setCurrentNamespace(ns1);
        assertEquals(listNs1, getAsList(cachingListState));
        verify(mockDelegateListState, times(2)).get();
        verify(mockDelegateListState, times(2)).setCurrentNamespace(ns1);

        cachingListState.setCurrentNamespace(ns2);
        assertEquals(listNs2, getAsList(cachingListState));
        verify(mockDelegateListState, times(2)).get();
        verify(mockDelegateListState, times(2)).setCurrentNamespace(ns2);
    }

    @Test
    void testListMaxActiveNamespaces_eviction() throws Exception {
        String ns1 = "max_ns_1";
        List<String> listNs1Data = Arrays.asList("L_mns1");
        String ns2 = "max_ns_2";
        List<String> listNs2Data = Arrays.asList("L_mns2");
        String ns3Evictor = "max_ns_3_evictor"; // This will be the 3rd namespace
        List<String> listNs3Data = Arrays.asList("L_mns3");

        cachingKeyedStateBackend.setCurrentKey(testKey);
        int delegateGetCount = 0;

        // Step 1: Access ns1. Cache miss, load from delegate. ns1's cache active.
        cachingListState.setCurrentNamespace(ns1);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(listNs1Data));
        assertEquals(listNs1Data, getAsList(cachingListState), "Get for ns1");
        delegateGetCount++;
        verify(mockDelegateListState, times(delegateGetCount)).get();

        // Step 2: Access ns2. Cache miss, load from delegate. ns1, ns2 caches active.
        cachingListState.setCurrentNamespace(ns2);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(listNs2Data));
        assertEquals(listNs2Data, getAsList(cachingListState), "Get for ns2");
        delegateGetCount++;
        verify(mockDelegateListState, times(delegateGetCount)).get();

        // maxActiveNamespaces is 2. Both ns1 and ns2 are now cached for testKey.
        // ns1 was accessed first, so it's LRU among active namespace caches.

        // Step 3: Access ns1 again. Should be a cache hit for testKey within ns1's cache.
        // This makes ns2 LRU.
        cachingListState.setCurrentNamespace(ns1);
        // No new when(mockDelegateListState.get()) needed as it should hit cache.
        assertEquals(listNs1Data, getAsList(cachingListState), "Cache hit for ns1");
        verify(mockDelegateListState, times(delegateGetCount)).get(); // Count should not increase

        // Step 4: Access ns3Evictor. This should evict ns2's namespace cache (LRU).
        // Then, for testKey in ns3Evictor: L1 miss, L2 miss (as whole namespace L1/L2 is new), delegate get.
        cachingListState.setCurrentNamespace(ns3Evictor);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(listNs3Data));
        assertEquals(listNs3Data, getAsList(cachingListState), "Get for ns3Evictor, ns2's cache evicted");
        delegateGetCount++;
        verify(mockDelegateListState, times(delegateGetCount)).get();
        // Active namespace caches: ns1, ns3Evictor. (ns3Evictor is MRU, ns1 is LRU)

        // Step 5: Access ns2 again. Its namespace cache was evicted.
        // This access should now evict ns1's namespace cache (current LRU).
        // Results in L1 miss, L2 miss, delegate get for testKey in ns2.
        cachingListState.setCurrentNamespace(ns2);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(listNs2Data)); // Expect to fetch this again
        assertEquals(listNs2Data, getAsList(cachingListState), "Get for ns2 after its cache eviction");
        delegateGetCount++;
        verify(mockDelegateListState, times(delegateGetCount)).get();
        // Active namespace caches: ns3Evictor, ns2. (ns2 is MRU, ns3Evictor is LRU)

        // Step 6: Access ns1 again. Its namespace cache was evicted by ns2's re-access.
        // This access should evict ns3Evictor's namespace cache.
        // Results in L1 miss, L2 miss, delegate get for testKey in ns1.
        cachingListState.setCurrentNamespace(ns1);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(listNs1Data));
        assertEquals(listNs1Data, getAsList(cachingListState), "Get for ns1 after its cache eviction");
        delegateGetCount++;
        verify(mockDelegateListState, times(delegateGetCount)).get();
        // Active namespace caches: ns2, ns1.

        // Final check: ns3Evictor's cache should be gone, requiring a delegate get.
        cachingListState.setCurrentNamespace(ns3Evictor);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(listNs3Data));
        assertEquals(listNs3Data, getAsList(cachingListState), "Get for ns3Evictor after its cache eviction");
        delegateGetCount++;
        verify(mockDelegateListState, times(delegateGetCount)).get();

        cachingListState.setCurrentNamespace(testNamespace); // Reset
    }

    @Test
    void testFlushList_writesDirtyEntriesToDelegate_marksClean() throws Exception {
        String ns1 = "flush_ns1";
        String key1Ns1 = "f_key1_L_ns1_dirty";
        List<String> val1Ns1Dirty = Arrays.asList("dirty_val1_ns1");

        String ns2 = "flush_ns2";
        String key1Ns2 = "f_key1_L_ns2_dirty";
        List<String> val1Ns2Dirty = Arrays.asList("dirty_val1_ns2");

        List<String> valCleanOriginal = Arrays.asList("cleanOriginal");

        cachingListState.setCurrentNamespace(ns1);
        cachingKeyedStateBackend.setCurrentKey(key1Ns1);
        cachingListState.update(val1Ns1Dirty);

        String key2Ns1Clean = "f_key2_L_ns1_clean";
        cachingKeyedStateBackend.setCurrentKey(key2Ns1Clean);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(valCleanOriginal));
        cachingListState.get();

        cachingListState.setCurrentNamespace(ns2);
        cachingKeyedStateBackend.setCurrentKey(key1Ns2);
        cachingListState.update(val1Ns2Dirty);

        cachingListState.setCurrentNamespace(testNamespace);
        cachingKeyedStateBackend.setCurrentKey(testKey);

        cachingListState.flushToUnderlyingState();

        verify(mockDelegateListState, times(1)).update(val1Ns1Dirty);
        verify(mockDelegateListState, times(1)).update(val1Ns2Dirty);
        verify(mockDelegateListState, times(0)).update(valCleanOriginal);

        cachingListState.setCurrentNamespace(ns1);
        cachingKeyedStateBackend.setCurrentKey(key1Ns1);
        cachingKeyedStateBackend.setCurrentKey("flushed_evictor1_L");
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("fe1"));
        cachingListState.get();
        cachingKeyedStateBackend.setCurrentKey("flushed_evictor2_L");
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("fe2"));
        cachingListState.get();

        verify(mockDelegateListState, times(1)).update(val1Ns1Dirty);
    }

    @Test
    void testClearList_removesFromAllCachesAndDelegate() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateListState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(1)).get();

        String fillerKey1 = "clear_filler1";
        cachingKeyedStateBackend.setCurrentKey(fillerKey1);
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f1_clear"));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(2)).get();

        String fillerKey2 = "clear_filler2";
        cachingKeyedStateBackend.setCurrentKey(fillerKey2);
        when(mockDelegateListState.get()).thenReturn(Arrays.asList("f2_clear"));
        getAsList(cachingListState);
        verify(mockDelegateListState, times(3)).get();

        cachingKeyedStateBackend.setCurrentKey(testKey);
        cachingListState.clear();

        verify(mockDelegateListState, times(1)).clear();

        when(mockDelegateListState.get()).thenReturn(null);
        List<String> listAfterClear = getAsList(cachingListState);
        assertNull(listAfterClear,
                "List should be null after clear (or empty if delegate returns that).");
        verify(mockDelegateListState, times(4)).get();
    }

    @Test
    void testListSerializersAreDelegated() {
        org.junit.jupiter.api.Assertions.assertEquals(mockKeySerializer,
                cachingListState.getKeySerializer());
        org.junit.jupiter.api.Assertions.assertEquals(mockNamespaceSerializer,
                cachingListState.getNamespaceSerializer());
        org.junit.jupiter.api.Assertions.assertEquals(mockValueSerializer,
                cachingListState.getValueSerializer());
    }
}

