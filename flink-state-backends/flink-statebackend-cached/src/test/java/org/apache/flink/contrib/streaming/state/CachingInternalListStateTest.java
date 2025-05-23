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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

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

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CachingInternalListStateTest {

    @Mock
    private InternalListState<String, String, String> mockDelegateState;

    private CachingKeyedStateBackend<String> cachingKeyedStateBackend;
    @Mock
    private AbstractKeyedStateBackend<String> mockAbstractKeyedStateBackendDelegate;

    @Mock
    private TypeSerializer<String> mockKeySerializer;
    @Mock
    private TypeSerializer<String> mockNamespaceSerializer;
    @Mock
    private TypeSerializer<List<String>> mockValueSerializer;

    private CachingInternalListState<String, String, String> cachingListState;

    private final int l1CacheSize = 2;
    private final int l2CacheSize = 2;
    private final int maxActiveNamespaces = 2;

    private final String testKey = "testListKey";
    private final String testNamespace = "testListNamespace";
    private final String element1 = "element1";
    private final String element2 = "element2";
    private final String element3 = "element3";
    private List<String> delegateList;

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

        when(mockKeySerializer.duplicate()).thenReturn(mockKeySerializer);
        when(mockAbstractKeyedStateBackendDelegate.getKeySerializer())
                .thenReturn(mockKeySerializer);
        when(mockAbstractKeyedStateBackendDelegate.getKeyContext()).thenReturn(
                new org.apache.flink.runtime.state.heap.InternalKeyContextImpl<>(null, 0));

        cachingKeyedStateBackend = new CachingKeyedStateBackend<>(kvStateRegistry,
                mockKeySerializer, CachingInternalListStateTest.class.getClassLoader(),
                executionConfig, ttlTimeProvider, metricGroup,
                Collections.<KeyedStateHandle>emptyList(), cancelStreamRegistry,
                mockAbstractKeyedStateBackendDelegate, l1CacheSize, l2CacheSize,
                maxActiveNamespaces, 10, CachingStateBackendFactory.CachePolicyType.LRU);
        cachingKeyedStateBackend.setCurrentKey(testKey);

        delegateList = new ArrayList<>(Arrays.asList(element1, element2));

        when(mockDelegateState.getKeySerializer()).thenReturn(mockKeySerializer);
        when(mockDelegateState.getNamespaceSerializer()).thenReturn(mockNamespaceSerializer);
        when(mockDelegateState.getValueSerializer()).thenReturn(mockValueSerializer);

        cachingListState = new CachingInternalListState<>(mockDelegateState,
                        cachingKeyedStateBackend, l1CacheSize, l2CacheSize, maxActiveNamespaces,
                CachingStateBackendFactory.CachePolicyType.LRU);
        cachingListState.setCurrentNamespace(testNamespace);
    }

    @Test
    void testListGet_cacheMiss_loadFromDelegate_populateL1() throws Exception {
        when(mockDelegateState.get()).thenReturn(new java.util.ArrayList<>(delegateList));

        List<String> retrievedList1 = getAsList(cachingListState);

        assertEquals(delegateList, retrievedList1, "List from first call should match delegate");
        verify(mockDelegateState, times(1)).get();

        List<String> retrievedList2 = getAsList(cachingListState);

        assertEquals(delegateList, retrievedList2,
                "List from second call should match cached list");
        verify(mockDelegateState, times(1)).get();

        retrievedList2.add("anotherElement");
        List<String> retrievedList3 = getAsList(cachingListState);
        assertEquals(delegateList, retrievedList3,
                "Modifying returned list should not affect cached list.");
        assertNotEquals(retrievedList2, retrievedList3);
    }

    @Test
    void testListGet_L1Hit_returnsCopy() throws Exception {
        when(mockDelegateState.get()).thenReturn(new java.util.ArrayList<>(delegateList));
        getAsList(cachingListState);
        org.mockito.Mockito.verify(mockDelegateState, org.mockito.Mockito.times(1)).get();

        List<String> retrievedList1 = getAsList(cachingListState);

        assertEquals(delegateList, retrievedList1,
                "List from L1 hit should match initially cached list");
        org.mockito.Mockito.verify(mockDelegateState, org.mockito.Mockito.times(1)).get();

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

        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState);
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).get();

        String anotherKey1 = "listTestKey_L1MissL2Hit_Filler1";
        List<String> listForAnotherKey1 = Arrays.asList("ak1_e1");
        cachingKeyedStateBackend.setCurrentKey(anotherKey1);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(listForAnotherKey1));
        getAsList(cachingListState);
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).get();

        String anotherKey2 = "listTestKey_L1MissL2Hit_Filler2";
        List<String> listForAnotherKey2 = Arrays.asList("ak2_e1");
        cachingKeyedStateBackend.setCurrentKey(anotherKey2);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(listForAnotherKey2));
        getAsList(cachingListState);
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).get();

        cachingKeyedStateBackend.setCurrentKey(testKey);
        List<String> retrievedList1 = getAsList(cachingListState);

        assertEquals(delegateList, retrievedList1, "List should be retrieved from L2.");
        verify(mockDelegateState, times(delegateGetCalls)).get();

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
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState);
        verify(mockDelegateState, times(1)).get();

        String fillerKey1 = "listUpdate_fillerKey1";
        cachingKeyedStateBackend.setCurrentKey(fillerKey1);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState);
        verify(mockDelegateState, times(2)).get();

        String fillerKey2 = "listUpdate_fillerKey2";
        cachingKeyedStateBackend.setCurrentKey(fillerKey2);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState);
        verify(mockDelegateState, times(3)).get();

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
        verify(mockDelegateState, times(3)).get();

        cachingKeyedStateBackend.setCurrentKey("listUpdate_evictor1");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("evictorValue1"));
        getAsList(cachingListState);
        verify(mockDelegateState, times(4)).get();

        cachingKeyedStateBackend.setCurrentKey("listUpdate_evictor2");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("evictorValue2"));
        getAsList(cachingListState);

        List<String> listFromL2AfterEviction = getAsList(cachingListState);
        assertEquals(newList, listFromL2AfterEviction,
                "List should be served from L2 (clean) after dirty L1 eviction.");
        verify(mockDelegateState, times(5)).get();
    }

    @Test
    void testListUpdate_null_marksDirtyInL1() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState);
        verify(mockDelegateState, times(1)).get();

        cachingListState.update(null);

        assertEquals(null, getAsList(cachingListState), "List after update(null) should be null.");
        verify(mockDelegateState, times(1)).get();

        String fillerKey1 = "listUpdateNull_fillerKey1";
        cachingKeyedStateBackend.setCurrentKey(fillerKey1);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState);
        verify(mockDelegateState, times(2)).get();

        String fillerKey2 = "listUpdateNull_fillerKey2";
        cachingKeyedStateBackend.setCurrentKey(fillerKey2);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState);
        verify(mockDelegateState, times(3)).get();

        verify(mockDelegateState, times(1)).update(null);
    }

    @Test
    void testListAdd_toNewList_marksDirtyInL1() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(null);

        String newElement = "addedElement1";
        cachingListState.add(newElement);

        List<String> listFromCache = getAsList(cachingListState);
        assertEquals(Arrays.asList(newElement), listFromCache,
                "List should contain the added element.");
        verify(mockDelegateState, times(1)).get();

        String fillerKey1 = "listAdd_fillerKey1";
        cachingKeyedStateBackend.setCurrentKey(fillerKey1);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState);
        verify(mockDelegateState, times(2)).get();

        String fillerKey2 = "listAdd_fillerKey2";
        cachingKeyedStateBackend.setCurrentKey(fillerKey2);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState);
        verify(mockDelegateState, times(3)).get();

        verify(mockDelegateState, times(1)).update(Arrays.asList(newElement));
    }

    @Test
    void testListAdd_toExistingCachedList_marksDirtyInL1() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState);
        verify(mockDelegateState, times(1)).get();

        String addedElement = "appendedToList";
        cachingListState.add(addedElement);

        List<String> expectedList = new ArrayList<>(delegateList);
        expectedList.add(addedElement);
        List<String> listFromCache = getAsList(cachingListState);
        assertEquals(expectedList, listFromCache,
                "List should contain the original and added elements.");
        verify(mockDelegateState, times(1)).get();

        String fillerKey1 = "listAddExisting_fillerKey1";
        cachingKeyedStateBackend.setCurrentKey(fillerKey1);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState);
        verify(mockDelegateState, times(2)).get();

        String fillerKey2 = "listAddExisting_fillerKey2";
        cachingKeyedStateBackend.setCurrentKey(fillerKey2);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState);
        verify(mockDelegateState, times(3)).get();

        verify(mockDelegateState, times(1)).update(expectedList);
    }

    @Test
    void testListAddAll_toNewList_marksDirtyInL1() throws Exception {
        cachingListState.setCurrentNamespace(testNamespace);
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(null);

        List<String> elementsToAdd = Arrays.asList(element1, element3);
        cachingListState.addAll(elementsToAdd);

        List<String> retrievedList = getAsList(cachingListState);
        assertEquals(elementsToAdd, retrievedList);
        verify(mockDelegateState, times(1)).get();

        cachingKeyedStateBackend.setCurrentKey("fillerKey1_addAllNew");
        cachingListState.setCurrentNamespace(testNamespace);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState);

        cachingKeyedStateBackend.setCurrentKey("fillerKey2_addAllNew");
        cachingListState.setCurrentNamespace(testNamespace);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState);

        ArgumentCaptor<List<String>> listCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockDelegateState, times(1)).update(listCaptor.capture());
        assertEquals(elementsToAdd, listCaptor.getValue());
    }

    @Test
    void testListAddAll_toExistingCachedList_marksDirtyInL1() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState);
        verify(mockDelegateState, times(1)).get();

        List<String> elementsToAdd = Arrays.asList(element3, "element4");
        cachingListState.addAll(elementsToAdd);

        List<String> expectedList = new ArrayList<>(delegateList);
        expectedList.addAll(elementsToAdd);

        assertEquals(expectedList, getAsList(cachingListState));
        verify(mockDelegateState, times(1)).get();

        cachingKeyedStateBackend.setCurrentKey("fillerKey1_addAllExisting");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState);
        cachingKeyedStateBackend.setCurrentKey("fillerKey2_addAllExisting");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState);

        verify(mockDelegateState, times(1)).update(expectedList);
    }

    @Test
    void testListGetList_sameAsGet() throws Exception {
    }

    @Test
    void testListL1Eviction_cleanEntry_moveToL2() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState);
        verify(mockDelegateState, times(1)).get();

        cachingKeyedStateBackend.setCurrentKey("evictor1_clean");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("e1"));
        getAsList(cachingListState);
        verify(mockDelegateState, times(2)).get();

        cachingKeyedStateBackend.setCurrentKey("evictor2_clean");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("e2"));
        getAsList(cachingListState);
        verify(mockDelegateState, times(3)).get();
        verify(mockDelegateState, times(0)).update(org.mockito.ArgumentMatchers.anyList());

        cachingKeyedStateBackend.setCurrentKey(testKey);
        List<String> retrieved = getAsList(cachingListState);
        assertEquals(delegateList, retrieved);
        verify(mockDelegateState, times(3)).get();
    }

    @Test
    void testListL1Eviction_dirtyEntry_flushToDelegate_moveToL2Clean() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testKey);
        List<String> updatedList = Arrays.asList(element1, element3, "dirtyElement");
        cachingListState.update(updatedList);

        cachingKeyedStateBackend.setCurrentKey("evictor1_dirty");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("e1"));
        getAsList(cachingListState);
        verify(mockDelegateState, times(1)).get();

        cachingKeyedStateBackend.setCurrentKey("evictor2_dirty");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("e2"));
        getAsList(cachingListState);

        verify(mockDelegateState, times(1)).update(updatedList);

        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(updatedList));
        List<String> retrieved = getAsList(cachingListState);
        assertEquals(updatedList, retrieved);
        verify(mockDelegateState, times(2)).get();
    }

    @Test
    void testListL2Eviction() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState);
        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler1_for_testKey");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState);
        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler2_for_testKey");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState);

        String keyL2_2 = "keyL2_2";
        List<String> listL2_2 = Arrays.asList("l2_e2_1");
        cachingKeyedStateBackend.setCurrentKey(keyL2_2);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(listL2_2));
        getAsList(cachingListState);
        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler1_for_keyL2_2");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f3"));
        getAsList(cachingListState);
        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler2_for_keyL2_2");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f4"));
        getAsList(cachingListState);

        String keyL2_3 = "keyL2_3";
        List<String> listL2_3 = Arrays.asList("l2_e3_1");
        cachingKeyedStateBackend.setCurrentKey(keyL2_3);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(listL2_3));
        getAsList(cachingListState);
        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler1_for_keyL2_3");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f5"));
        getAsList(cachingListState);
        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler2_for_keyL2_3");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f6"));
        getAsList(cachingListState);

        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList));
        List<String> retrieved = getAsList(cachingListState);
        assertEquals(delegateList, retrieved);
        verify(mockDelegateState, times(10)).get();
    }

    @Test
    void testListMultipleKeys_cachesAreSeparate() throws Exception {
        String key1 = "mk1";
        List<String> list1 = Arrays.asList("mk1_e1", "mk1_e2");
        String key2 = "mk2";
        List<String> list2 = Arrays.asList("mk2_e1");

        cachingKeyedStateBackend.setCurrentKey(key1);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(list1));
        assertEquals(list1, getAsList(cachingListState));
        verify(mockDelegateState, times(1)).get();

        cachingKeyedStateBackend.setCurrentKey(key2);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(list2));
        assertEquals(list2, getAsList(cachingListState));
        verify(mockDelegateState, times(2)).get();

        cachingKeyedStateBackend.setCurrentKey(key1);
        assertEquals(list1, getAsList(cachingListState));
        verify(mockDelegateState, times(2)).get();

        cachingKeyedStateBackend.setCurrentKey(key2);
        assertEquals(list2, getAsList(cachingListState));
        verify(mockDelegateState, times(2)).get();
    }

    @Test
    void testListMultipleNamespaces_cachesAreSeparate() throws Exception {
        String ns1 = "multi_ns_1";
        List<String> listNs1 = Arrays.asList("L_mns1");
        String ns2 = "multi_ns_2";
        List<String> listNs2 = Arrays.asList("L_mns2");

        cachingKeyedStateBackend.setCurrentKey(testKey);

        cachingListState.setCurrentNamespace(ns1);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(listNs1));
        assertEquals(listNs1, getAsList(cachingListState));
        verify(mockDelegateState, times(1)).get();
        verify(mockDelegateState, times(1)).setCurrentNamespace(ns1);

        cachingListState.setCurrentNamespace(ns2);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(listNs2));
        assertEquals(listNs2, getAsList(cachingListState));
        verify(mockDelegateState, times(2)).get();
        verify(mockDelegateState, times(1)).setCurrentNamespace(ns2);

        cachingListState.setCurrentNamespace(ns1);
        assertEquals(listNs1, getAsList(cachingListState));
        verify(mockDelegateState, times(2)).get();
        verify(mockDelegateState, times(2)).setCurrentNamespace(ns1);

        cachingListState.setCurrentNamespace(ns2);
        assertEquals(listNs2, getAsList(cachingListState));
        verify(mockDelegateState, times(2)).get();
        verify(mockDelegateState, times(2)).setCurrentNamespace(ns2);
    }

    @Test
    void testListMaxActiveNamespaces_eviction() throws Exception {
        String ns1 = "max_ns_1";
        String ns2 = "max_ns_2";
        String ns3 = "max_ns_3_evictor";

        cachingKeyedStateBackend.setCurrentKey(testKey);

        cachingListState.setCurrentNamespace(ns1);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(Arrays.asList("L_mns1")));
        getAsList(cachingListState);
        verify(mockDelegateState, times(1)).get();

        cachingListState.setCurrentNamespace(ns2);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(Arrays.asList("L_mns2")));
        getAsList(cachingListState);
        verify(mockDelegateState, times(2)).get();

        cachingListState.setCurrentNamespace(ns3);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(Arrays.asList("L_mns3")));
        getAsList(cachingListState);
        verify(mockDelegateState, times(3)).get();

        cachingListState.setCurrentNamespace(ns1);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(Arrays.asList("L_mns1")));
        assertEquals(Arrays.asList("L_mns1"), getAsList(cachingListState));
        verify(mockDelegateState, times(4)).get();

        cachingListState.setCurrentNamespace(ns2);
        assertEquals(Arrays.asList("L_mns2"), getAsList(cachingListState));
        verify(mockDelegateState, times(4)).get();

        cachingListState.setCurrentNamespace(ns3);
        assertEquals(Arrays.asList("L_mns3"), getAsList(cachingListState));
        verify(mockDelegateState, times(4)).get();

        cachingListState.setCurrentNamespace(testNamespace);
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
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(valCleanOriginal));
        cachingListState.get();

        cachingListState.setCurrentNamespace(ns2);
        cachingKeyedStateBackend.setCurrentKey(key1Ns2);
        cachingListState.update(val1Ns2Dirty);

        cachingListState.setCurrentNamespace(testNamespace);
        cachingKeyedStateBackend.setCurrentKey(testKey);

        cachingListState.flushToUnderlyingState();

        verify(mockDelegateState, times(1)).update(val1Ns1Dirty);
        verify(mockDelegateState, times(1)).update(val1Ns2Dirty);
        verify(mockDelegateState, times(0)).update(valCleanOriginal);

        cachingListState.setCurrentNamespace(ns1);
        cachingKeyedStateBackend.setCurrentKey(key1Ns1);
        cachingKeyedStateBackend.setCurrentKey("flushed_evictor1_L");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("fe1"));
        cachingListState.get();
        cachingKeyedStateBackend.setCurrentKey("flushed_evictor2_L");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("fe2"));
        cachingListState.get();

        verify(mockDelegateState, times(1)).update(val1Ns1Dirty);
    }

    @Test
    void testClearList_removesFromAllCachesAndDelegate() throws Exception {
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState);
        verify(mockDelegateState, times(1)).get();

        String fillerKey1 = "clear_filler1";
        cachingKeyedStateBackend.setCurrentKey(fillerKey1);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f1_clear"));
        getAsList(cachingListState);
        verify(mockDelegateState, times(2)).get();

        String fillerKey2 = "clear_filler2";
        cachingKeyedStateBackend.setCurrentKey(fillerKey2);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f2_clear"));
        getAsList(cachingListState);
        verify(mockDelegateState, times(3)).get();

        cachingKeyedStateBackend.setCurrentKey(testKey);
        cachingListState.clear();

        verify(mockDelegateState, times(1)).clear();

        when(mockDelegateState.get()).thenReturn(null);
        List<String> listAfterClear = getAsList(cachingListState);
        assertNull(listAfterClear,
                "List should be null after clear (or empty if delegate returns that).");
        verify(mockDelegateState, times(4)).get();
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
