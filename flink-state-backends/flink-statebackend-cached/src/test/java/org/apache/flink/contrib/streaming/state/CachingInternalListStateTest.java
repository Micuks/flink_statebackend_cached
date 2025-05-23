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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CachingInternalListStateTest {

    @Mock
    private InternalListState<String, String, String> mockDelegateState;
    @Mock
    private CachingKeyedStateBackend<String> mockBackend;
    @Mock
    private TypeSerializer<String> mockKeySerializer;
    @Mock
    private TypeSerializer<String> mockNamespaceSerializer;
    @Mock
    private TypeSerializer<List<String>> mockValueSerializer; // Serializer for List<V_ELE>

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

    // Field to control the key returned by mockBackend.getCurrentKey()
    private String currentKeyForMock;

    // Helper method to convert Iterable<String> from get() to List<String>
    private List<String> getAsList(CachingInternalListState<String, String, String> state)
            throws Exception {
        Iterable<String> iterable = state.get();
        if (iterable == null) {
            // Consistent with how ListState.get() can return null (e.g., if underlying state is
            // null/cleared)
            return null;
        }
        List<String> list = new ArrayList<>();
        iterable.forEach(list::add);
        return list;
    }

    @BeforeEach
    void setUp() {
        delegateList = new ArrayList<>(Arrays.asList(element1, element2));

        when(mockDelegateState.getKeySerializer()).thenReturn(mockKeySerializer);
        when(mockDelegateState.getNamespaceSerializer()).thenReturn(mockNamespaceSerializer);
        when(mockDelegateState.getValueSerializer()).thenReturn(mockValueSerializer);

        // Initialize currentKeyForMock and mock backend behavior
        currentKeyForMock = testKey; // Default key
        when(mockBackend.getCurrentKey()).thenAnswer(invocation -> currentKeyForMock);
        doAnswer(invocation -> {
            currentKeyForMock = invocation.getArgument(0);
            return null; // void method
        }).when(mockBackend).setCurrentKey(any(String.class)); // Assuming K is String

        cachingListState = new CachingInternalListState<>(mockDelegateState, mockBackend,
                l1CacheSize, l2CacheSize, maxActiveNamespaces,
                CachingStateBackendFactory.CachePolicyType.LRU);
        cachingListState.setCurrentNamespace(testNamespace); // Default namespace for tests
    }

    // --- Basic Get/Update/Add for List State ---
    @Test
    void testListGet_cacheMiss_loadFromDelegate_populateL1() throws Exception {
        // Setup: Delegate returns a specific list on first call
        when(mockDelegateState.get()).thenReturn(new java.util.ArrayList<>(delegateList));

        // Action 1: First call to get() - should be a cache miss
        List<String> retrievedList1 = getAsList(cachingListState);

        // Verification 1
        org.junit.jupiter.api.Assertions.assertEquals(delegateList, retrievedList1,
                "List from first call should match delegate");
        org.mockito.Mockito.verify(mockDelegateState, org.mockito.Mockito.times(1)).get(); // Delegate
        // should
        // be
        // called
        // once

        // Action 2: Second call to get() - should be an L1 cache hit
        List<String> retrievedList2 = getAsList(cachingListState);

        // Verification 2
        org.junit.jupiter.api.Assertions.assertEquals(delegateList, retrievedList2,
                "List from second call should match cached list");
        org.mockito.Mockito.verify(mockDelegateState, org.mockito.Mockito.times(1)).get(); // Delegate
        // should
        // still
        // only
        // be
        // called
        // once
        // (hit
        // L1)

        // Verify a copy is returned
        retrievedList2.add("anotherElement");
        List<String> retrievedList3 = getAsList(cachingListState);
        org.junit.jupiter.api.Assertions.assertEquals(delegateList, retrievedList3,
                "Modifying returned list should not affect cached list.");
        org.junit.jupiter.api.Assertions.assertNotEquals(retrievedList2, retrievedList3);
    }

    @Test
    void testListGet_L1Hit_returnsCopy() throws Exception {
        // Setup: Populate L1 cache by calling get() once
        when(mockDelegateState.get()).thenReturn(new java.util.ArrayList<>(delegateList));
        getAsList(cachingListState); // This call populates L1
        org.mockito.Mockito.verify(mockDelegateState, org.mockito.Mockito.times(1)).get(); // Verify
        // delegate
        // was
        // called
        // for
        // population

        // Action: Call get() again - should be an L1 cache hit
        List<String> retrievedList1 = getAsList(cachingListState);

        // Verification
        org.junit.jupiter.api.Assertions.assertEquals(delegateList, retrievedList1,
                "List from L1 hit should match initially cached list");
        // Delegate should still only have been called once from the initial population
        org.mockito.Mockito.verify(mockDelegateState, org.mockito.Mockito.times(1)).get();

        // Verify a copy is returned
        retrievedList1.add("anotherElementL1Hit");
        List<String> retrievedList2 = getAsList(cachingListState); // Get a fresh copy from cache

        org.junit.jupiter.api.Assertions.assertEquals(delegateList, retrievedList2,
                "Modifying the first retrieved list should not affect the cached list for subsequent gets.");
        org.junit.jupiter.api.Assertions.assertNotEquals(retrievedList1, retrievedList2,
                "Subsequent gets should return different list instances (copies).");
    }

    @Test
    void testListGet_L1Miss_L2Hit_promoteToL1_returnsCopy() throws Exception {
        // --- Setup: Get testKey (delegateList) into L2 ---
        // Current L1 cache size for a namespace is 2.
        int delegateGetCalls = 0;

        // 1. testKey -> delegateList (clean) in L1.
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState); // testKey is current key from setUp
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).get();

        // 2. anotherKey1 -> listForAnotherKey1 (clean) in L1.
        String anotherKey1 = "listTestKey_L1MissL2Hit_Filler1";
        List<String> listForAnotherKey1 = Arrays.asList("ak1_e1");
        mockBackend.setCurrentKey(anotherKey1);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(listForAnotherKey1));
        getAsList(cachingListState);
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).get();
        // L1 for testNamespace: (testKey, delegateList), (anotherKey1, listForAnotherKey1). testKey
        // is eldest.

        // 3. anotherKey2 -> listForAnotherKey2 (clean) in L1. This evicts testKey to L2.
        String anotherKey2 = "listTestKey_L1MissL2Hit_Filler2";
        List<String> listForAnotherKey2 = Arrays.asList("ak2_e1");
        mockBackend.setCurrentKey(anotherKey2);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(listForAnotherKey2));
        getAsList(cachingListState);
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).get();
        // L1 for testNamespace: (anotherKey1, listForAnotherKey1), (anotherKey2,
        // listForAnotherKey2).
        // L2 for testNamespace: (testKey, delegateList).

        // --- Action: Access testKey. Should be L1 miss, L2 hit, promote to L1. ---
        mockBackend.setCurrentKey(testKey);
        List<String> retrievedList1 = getAsList(cachingListState);

        // --- Verification ---
        assertEquals(delegateList, retrievedList1, "List should be retrieved from L2.");
        // Delegate.get() should NOT be called again (still 'delegateGetCalls' times).
        verify(mockDelegateState, times(delegateGetCalls)).get();

        // Verify a copy is returned and it was promoted to L1.
        retrievedList1.add("modifiedAfterL2Hit");

        List<String> retrievedList2 = getAsList(cachingListState); // Should be L1 hit now.
        assertEquals(delegateList, retrievedList2,
                "Second get after L2 hit should return original from L1.");
        assertNotEquals(retrievedList1, retrievedList2,
                "Returned lists should be different instances.");
        // Delegate.get() should still not have been called again.
        verify(mockDelegateState, times(delegateGetCalls)).get();
    }

    @Test
    void testListUpdate_newList_marksDirtyInL1_evictsL2() throws Exception {
        // --- Setup Phase 1: Get testKey (delegateList) into L2 ---
        // This ensures there's something in L2 for testKey that should be evicted upon update.
        mockBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState); // testKey -> delegateList (clean) in L1
        verify(mockDelegateState, times(1)).get();

        // Evict testKey to L2 by filling L1 for testNamespace with other keys.
        String fillerKey1 = "listUpdate_fillerKey1";
        mockBackend.setCurrentKey(fillerKey1);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState); // fillerKey1 in L1
        verify(mockDelegateState, times(2)).get();

        String fillerKey2 = "listUpdate_fillerKey2";
        mockBackend.setCurrentKey(fillerKey2);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState); // fillerKey2 in L1. testKey now in L2.
        verify(mockDelegateState, times(3)).get();

        // --- Setup Phase 2: Prepare for update ---
        mockBackend.setCurrentKey(testKey); // Target testKey again
        List<String> newList = new ArrayList<>(Arrays.asList("new_el1", "new_el2"));

        // --- Action: Update testKey with newList ---
        cachingListState.update(newList);
        // Expected: newList (dirty) in L1 for testKey. Previous testKey entry in L2 is invalidated.

        // --- Verification 1: L1 serves the new list (and a copy) ---
        List<String> listFromCache1 = getAsList(cachingListState);
        assertEquals(newList, listFromCache1, "L1 should serve the new list after update.");
        assertNotEquals(System.identityHashCode(newList), System.identityHashCode(listFromCache1),
                "Get should return a copy from L1.");
        listFromCache1.add("modifiedCopy");
        List<String> listFromCache2 = getAsList(cachingListState);
        assertEquals(newList, listFromCache2,
                "L1 internal list not affected by copy modification.");
        verify(mockDelegateState, times(3)).get(); // No new delegate GET calls for this.

        // --- Verification 2: If newList (dirty) is evicted from L1, it writes to delegate and
        // moves to L2 (clean) ---
        // Evict testKey (now with newList, dirty) by filling L1 with other keys.
        mockBackend.setCurrentKey("listUpdate_evictor1");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("ev1"));
        getAsList(cachingListState); // evictor1 in L1
        verify(mockDelegateState, times(4)).get(); // For evictor1

        mockBackend.setCurrentKey("listUpdate_evictor2");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("ev2"));
        getAsList(cachingListState); // evictor2 in L1. testKey (newList, dirty) should be evicted.
        // This eviction should trigger update(newList) on delegateState.
        verify(mockDelegateState, times(5)).get(); // For evictor2

        // Verify delegate was updated with newList during L1 eviction.
        // Note: The mockito `eq()` matcher might be needed if the list is copied internally before
        // update.
        // For ArrayList, `eq` works fine as it relies on .equals().
        verify(mockDelegateState, times(1)).update(newList);

        // --- Verification 3: testKey with newList (now clean) is in L2 ---
        // Accessing testKey again should be an L2 hit (no delegate.get()), promoting to L1.
        mockBackend.setCurrentKey(testKey);
        // If testKey was properly flushed and moved to L2, this get() should not call
        // delegate.get().
        // The previous when(mockDelegateState.get()) was for evictor2.
        // We need to ensure that if delegate.get() *were* called, it would return something
        // different.
        when(mockDelegateState.get())
                .thenReturn(Arrays.asList("unexpected_delegate_call_after_L2_promotion"));

        List<String> listAfterL1EvictionAndL2Promotion = getAsList(cachingListState);
        assertEquals(newList, listAfterL1EvictionAndL2Promotion,
                "newList (clean) should be retrieved from L2 and promoted to L1.");
        // Crucially, verify that mockDelegateState.get() was NOT called for testKey here.
        // Total get calls should be 5 (initial testKey, filler1, filler2, evictor1, evictor2).
        verify(mockDelegateState, times(5)).get();
    }

    @Test
    void testListUpdate_null_marksDirtyInL1() throws Exception {
        // --- Setup: Put an initial list into L1 for testKey ---
        mockBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList)); // delegateList =
        // [e1, e2]
        getAsList(cachingListState); // testKey -> delegateList (clean) in L1
        verify(mockDelegateState, times(1)).get();

        // --- Action: Update with null ---
        cachingListState.update(null);
        // Expected: testKey -> null (dirty) in L1.

        // --- Verification 1: Get returns null ---
        // The InternalListState.get() contract typically returns an Iterable.
        // If update(null) means "clear the list" or "set to null list", get() might return null or
        // empty.
        // For CachingInternalValueState, value() after update(null) was null.
        // Let's assume get() for list state will return null if it was updated to null.
        assertEquals(null, getAsList(cachingListState), "List after update(null) should be null.");
        verify(mockDelegateState, times(1)).get(); // No new delegate.get() call.

        // --- Verification 2: State is dirty and flush on L1 eviction updates delegate with null
        // ---
        // Evict testKey (null, dirty) by filling L1 with other keys.
        String fillerKey1 = "listUpdateNull_fillerKey1";
        mockBackend.setCurrentKey(fillerKey1);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState); // fillerKey1 in L1
        verify(mockDelegateState, times(2)).get();

        String fillerKey2 = "listUpdateNull_fillerKey2";
        mockBackend.setCurrentKey(fillerKey2);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState); // fillerKey2 in L1. testKey (null, dirty) is evicted.
        verify(mockDelegateState, times(3)).get();

        // Verify delegate was updated with null during L1 eviction.
        // This means the 'null' state was considered dirty and flushed.
        verify(mockDelegateState, times(1)).update(null);
    }

    @Test
    void testListAdd_toNewList_marksDirtyInL1() throws Exception {
        // --- Setup: Ensure testKey is not in cache and delegate would return null (new state) ---
        mockBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(null); // Simulate new state or cleared state

        // --- Action: Add an element ---
        String newElement = "addedElement1";
        cachingListState.add(newElement);
        // Expected: testKey -> [newElement] (dirty) in L1.

        // --- Verification 1: Get returns the list with the added element ---
        List<String> listFromCache = getAsList(cachingListState);
        assertEquals(Arrays.asList(newElement), listFromCache,
                "List should contain the added element.");
        verify(mockDelegateState, times(1)).get(); // Delegate.get() called once to check initial
        // state.

        // --- Verification 2: State is dirty and flush on L1 eviction updates delegate ---
        // Evict testKey ([newElement], dirty) by filling L1.
        String fillerKey1 = "listAdd_fillerKey1";
        mockBackend.setCurrentKey(fillerKey1);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState); // fillerKey1 in L1
        verify(mockDelegateState, times(2)).get(); // For fillerKey1

        String fillerKey2 = "listAdd_fillerKey2";
        mockBackend.setCurrentKey(fillerKey2);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState); // fillerKey2 in L1. testKey ([newElement], dirty) is evicted.
        verify(mockDelegateState, times(3)).get(); // For fillerKey2

        // Verify delegate was updated with the new list during L1 eviction.
        verify(mockDelegateState, times(1)).update(Arrays.asList(newElement));
    }

    @Test
    void testListAdd_toExistingCachedList_marksDirtyInL1() throws Exception {
        // --- Setup: Load an existing list into L1 for testKey ---
        mockBackend.setCurrentKey(testKey);
        // delegateList is [element1, element2] from setUp
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState); // testKey -> delegateList (clean) in L1
        verify(mockDelegateState, times(1)).get();

        // --- Action: Add an element to the cached list ---
        String addedElement = "appendedToList";
        cachingListState.add(addedElement);
        // Expected: testKey -> [element1, element2, appendedToList] (dirty) in L1.

        // --- Verification 1: Get returns the updated list ---
        List<String> expectedList = new ArrayList<>(delegateList);
        expectedList.add(addedElement);
        List<String> listFromCache = getAsList(cachingListState);
        assertEquals(expectedList, listFromCache,
                "List should contain the original and added elements.");
        verify(mockDelegateState, times(1)).get(); // No new delegate.get() call.

        // --- Verification 2: State is dirty and flush on L1 eviction updates delegate ---
        // Evict testKey (updatedList, dirty) by filling L1.
        String fillerKey1 = "listAddExisting_fillerKey1";
        mockBackend.setCurrentKey(fillerKey1);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState); // fillerKey1 in L1
        verify(mockDelegateState, times(2)).get(); // For fillerKey1

        String fillerKey2 = "listAddExisting_fillerKey2";
        mockBackend.setCurrentKey(fillerKey2);
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState); // fillerKey2 in L1. testKey (updatedList, dirty) is evicted.
        verify(mockDelegateState, times(3)).get(); // For fillerKey2

        // Verify delegate was updated with the new combined list during L1 eviction.
        verify(mockDelegateState, times(1)).update(expectedList);
    }

    @Test
    void testListAddAll_toNewList_marksDirtyInL1() throws Exception {
        cachingListState.setCurrentNamespace(testNamespace);
        mockBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(null); // Simulate new state

        List<String> elementsToAdd = Arrays.asList(element1, element3);
        cachingListState.addAll(elementsToAdd); // L1: testKey -> [e1, e3] (dirty)

        List<String> retrievedList = getAsList(cachingListState);
        assertEquals(elementsToAdd, retrievedList);
        verify(mockDelegateState, times(1)).get(); // For initial check

        // Evict to check flush
        mockBackend.setCurrentKey("fillerKey1_addAllNew");
        cachingListState.setCurrentNamespace(testNamespace); // Keep same namespace
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState);

        mockBackend.setCurrentKey("fillerKey2_addAllNew");
        cachingListState.setCurrentNamespace(testNamespace); // Keep same namespace
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState); // This should evict testKey

        ArgumentCaptor<List<String>> listCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockDelegateState, times(1)).update(listCaptor.capture());
        assertEquals(elementsToAdd, listCaptor.getValue());
    }

    @Test
    void testListAddAll_toExistingCachedList_marksDirtyInL1() throws Exception {
        mockBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList)); // [e1, e2]
        getAsList(cachingListState); // L1: testKey -> [e1, e2] (clean)
        verify(mockDelegateState, times(1)).get();

        List<String> elementsToAdd = Arrays.asList(element3, "element4");
        cachingListState.addAll(elementsToAdd); // L1: testKey -> [e1, e2, e3, e4] (dirty)

        List<String> expectedList = new ArrayList<>(delegateList);
        expectedList.addAll(elementsToAdd);

        assertEquals(expectedList, getAsList(cachingListState));
        verify(mockDelegateState, times(1)).get(); // No new delegate get

        // Evict to check flush
        mockBackend.setCurrentKey("fillerKey1_addAllExisting");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState);
        mockBackend.setCurrentKey("fillerKey2_addAllExisting");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState);

        verify(mockDelegateState, times(1)).update(expectedList);
    }

    @Test
    void testListGetList_sameAsGet() throws Exception {
        // This test seems to be a misinterpretation of available methods.
        // InternalListState has get(), not getList().
        // The existing get() tests cover its functionality.
        // If a specific scenario for get() was intended, it should be named explicitly.
        // Skipping implementation for "getList()".
    }

    // --- Eviction Logic for List State ---
    @Test
    void testListL1Eviction_cleanEntry_moveToL2() throws Exception {
        // Setup: testKey -> delegateList (clean) in L1
        mockBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState);
        verify(mockDelegateState, times(1)).get();

        // Evict testKey by loading two other keys into L1 (L1 size = 2)
        mockBackend.setCurrentKey("evictor1_clean");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("e1"));
        getAsList(cachingListState);
        verify(mockDelegateState, times(2)).get();

        mockBackend.setCurrentKey("evictor2_clean");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("e2"));
        getAsList(cachingListState); // testKey evicted to L2
        verify(mockDelegateState, times(3)).get();
        verify(mockDelegateState, times(0)).update(org.mockito.ArgumentMatchers.anyList()); // No
                                                                                            // updates
                                                                                            // for
                                                                                            // clean

        // Access testKey again - should be L2 hit
        mockBackend.setCurrentKey(testKey);
        List<String> retrieved = getAsList(cachingListState);
        assertEquals(delegateList, retrieved);
        verify(mockDelegateState, times(3)).get(); // No new delegate get
    }

    @Test
    void testListL1Eviction_dirtyEntry_flushToDelegate_moveToL2Clean() throws Exception {
        mockBackend.setCurrentKey(testKey);
        List<String> updatedList = Arrays.asList(element1, element3, "dirtyElement");
        cachingListState.update(updatedList); // L1: testKey -> updatedList (dirty)

        // Evict testKey
        mockBackend.setCurrentKey("evictor1_dirty");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("e1"));
        getAsList(cachingListState);
        mockBackend.setCurrentKey("evictor2_dirty");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("e2"));
        getAsList(cachingListState);
        // testKey (dirty) evicted

        verify(mockDelegateState, times(1)).update(updatedList); // Flushed to delegate

        // Access testKey again - should be L2 hit (now clean)
        mockBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(updatedList)); // If L2 miss,
                                                                                // delegate returns
                                                                                // this
        List<String> retrieved = getAsList(cachingListState);
        assertEquals(updatedList, retrieved);
        // If it was an L2 hit, delegate.get() count remains (2 from evictors).
        // If L2 population failed and it missed, it would be 3.
        // The CacheEntry should be clean in L2.
        verify(mockDelegateState, times(2)).get(); // Only for evictors
    }

    @Test
    void testListL2Eviction() throws Exception {
        // L2 Cache size is 2.
        // Put 3 items into L2 to evict the first one.
        // Item 1: testKey -> delegateList
        mockBackend.setCurrentKey(testKey);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList));
        getAsList(cachingListState); // -> L1
        mockBackend.setCurrentKey("l2_evict_filler1_for_testKey");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f1"));
        getAsList(cachingListState);
        mockBackend.setCurrentKey("l2_evict_filler2_for_testKey");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f2"));
        getAsList(cachingListState);
        // testKey now in L2. L2: {testKey=delegateList}

        // Item 2: "keyL2_2" -> listL2_2
        String keyL2_2 = "keyL2_2";
        List<String> listL2_2 = Arrays.asList("l2_e2_1");
        mockBackend.setCurrentKey(keyL2_2);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(listL2_2));
        getAsList(cachingListState); // -> L1
        mockBackend.setCurrentKey("l2_evict_filler1_for_keyL2_2");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f3"));
        getAsList(cachingListState);
        mockBackend.setCurrentKey("l2_evict_filler2_for_keyL2_2");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f4"));
        getAsList(cachingListState);
        // keyL2_2 now in L2. L2: {testKey=delegateList (eldest), keyL2_2=listL2_2}

        // Item 3: "keyL2_3" -> listL2_3 (this should evict testKey from L2)
        String keyL2_3 = "keyL2_3";
        List<String> listL2_3 = Arrays.asList("l2_e3_1");
        mockBackend.setCurrentKey(keyL2_3);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(listL2_3));
        getAsList(cachingListState); // -> L1
        mockBackend.setCurrentKey("l2_evict_filler1_for_keyL2_3");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f5"));
        getAsList(cachingListState);
        mockBackend.setCurrentKey("l2_evict_filler2_for_keyL2_3");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("f6"));
        getAsList(cachingListState);
        // keyL2_3 now in L2. L2: {keyL2_2=listL2_2, keyL2_3=listL2_3}. testKey is evicted from L2.

        // Access testKey again - should be a full cache miss, hitting delegate
        mockBackend.setCurrentKey(testKey);
        // Reset count for delegate.get(). It was called 2 (for delegateList) + 2 (for listL2_2) + 2
        // (for listL2_3) for initial loads.
        // Plus 2 for each set of fillers = 2*3 = 6 for fillers. Total 2+2+2 = 6 for main items.
        // Total delegate.get calls so far: 6 (main items) + 6 (fillers for L1->L2) = 12.
        // If we hit delegate for testKey now, it's 13.
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList)); // Simulate fresh
                                                                                 // load
        List<String> retrieved = getAsList(cachingListState);
        assertEquals(delegateList, retrieved);
        verify(mockDelegateState, times(10)).get();
    }

    // --- Namespace and Key Cache Management ---
    @Test
    void testListMultipleKeys_cachesAreSeparate() throws Exception {
        String key1 = "multiKey_1";
        List<String> list1 = Arrays.asList("mk1_e1");
        String key2 = "multiKey_2";
        List<String> list2 = Arrays.asList("mk2_e1");

        // Populate for key1
        mockBackend.setCurrentKey(key1);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(list1));
        assertEquals(list1, getAsList(cachingListState));
        verify(mockDelegateState, times(1)).get();

        // Populate for key2
        mockBackend.setCurrentKey(key2);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(list2));
        assertEquals(list2, getAsList(cachingListState));
        verify(mockDelegateState, times(2)).get();

        // Check key1 still in L1
        mockBackend.setCurrentKey(key1);
        assertEquals(list1, getAsList(cachingListState));
        verify(mockDelegateState, times(2)).get(); // Hit L1

        // Check key2 still in L1
        mockBackend.setCurrentKey(key2);
        assertEquals(list2, getAsList(cachingListState));
        verify(mockDelegateState, times(2)).get(); // Hit L1
    }

    @Test
    void testListMultipleNamespaces_cachesAreSeparate() throws Exception {
        String ns1 = "multiNs_1";
        List<String> listNs1 = Arrays.asList("mns1_e1");
        String ns2 = "multiNs_2";
        List<String> listNs2 = Arrays.asList("mns2_e1");

        mockBackend.setCurrentKey(testKey);

        // Populate for ns1
        cachingListState.setCurrentNamespace(ns1);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(listNs1));
        assertEquals(listNs1, getAsList(cachingListState));
        verify(mockDelegateState, times(1)).get();
        verify(mockDelegateState, times(1)).setCurrentNamespace(ns1);


        // Populate for ns2
        cachingListState.setCurrentNamespace(ns2);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(listNs2));
        assertEquals(listNs2, getAsList(cachingListState));
        verify(mockDelegateState, times(2)).get();
        verify(mockDelegateState, times(1)).setCurrentNamespace(ns2);

        // Check ns1 still in L1 of its namespace cache
        cachingListState.setCurrentNamespace(ns1);
        assertEquals(listNs1, getAsList(cachingListState));
        verify(mockDelegateState, times(2)).get(); // Hit L1 for ns1
        verify(mockDelegateState, times(2)).setCurrentNamespace(ns1);


        // Check ns2 still in L1 of its namespace cache
        cachingListState.setCurrentNamespace(ns2);
        assertEquals(listNs2, getAsList(cachingListState));
        verify(mockDelegateState, times(2)).get(); // Hit L1 for ns2
        verify(mockDelegateState, times(2)).setCurrentNamespace(ns2);
    }

    @Test
    void testListMaxActiveNamespaces_eviction() throws Exception {
        // maxActiveNamespaces = 2
        String key = "maxNsKey";
        mockBackend.setCurrentKey(key);

        String ns1 = "max_ns_1";
        List<String> listNs1 = Arrays.asList("L_mns1");
        String ns2 = "max_ns_2";
        List<String> listNs2 = Arrays.asList("L_mns2");
        String ns3 = "max_ns_3_evictor";
        List<String> listNs3 = Arrays.asList("L_mns3");

        // Populate ns1
        cachingListState.setCurrentNamespace(ns1);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(listNs1));
        getAsList(cachingListState); // ns1 cache active. NsCaches: {ns1}
        verify(mockDelegateState, times(1)).get();

        // Populate ns2
        cachingListState.setCurrentNamespace(ns2);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(listNs2));
        getAsList(cachingListState); // ns2 cache active. NsCaches: {ns1(LRU), ns2}
        verify(mockDelegateState, times(2)).get();

        // Populate ns3 (should evict ns1's entire cache container)
        cachingListState.setCurrentNamespace(ns3);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(listNs3));
        getAsList(cachingListState); // ns3 cache active. NsCaches: {ns2, ns3}. ns1 evicted.
        verify(mockDelegateState, times(3)).get();

        // Access ns1 again - should be a full miss (new namespace cache created)
        // This will also evict ns2's cache container from namespaceCachesL1/L2
        cachingListState.setCurrentNamespace(ns1);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(listNs1)); // Simulate reload for
                                                                            // ns1
        assertEquals(listNs1, getAsList(cachingListState));
        verify(mockDelegateState, times(4)).get(); // Delegate hit for ns1 reload

        // Access ns2 - its cache was evicted. This will be a delegate access.
        cachingListState.setCurrentNamespace(ns2);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(listNs2)); // ADDED THIS MOCKING
        assertEquals(listNs2, getAsList(cachingListState));
        verify(mockDelegateState, times(5)).get(); // Delegate hit for ns2 reload (was 4)
    }

    // --- CachingInternalState Methods ---
    @Test
    void testFlushList_writesDirtyEntriesToDelegate_marksClean() throws Exception {
        String ns1 = "flush_ns_L1";
        String key1Ns1 = "f_key1_L_ns1";
        List<String> val1Ns1Dirty = Arrays.asList("dirtyL1");
        String ns2 = "flush_ns_L2";
        String key1Ns2 = "f_key1_L_ns2";
        List<String> val1Ns2Dirty = Arrays.asList("dirtyL2");
        List<String> valCleanOriginal = Arrays.asList("cleanOriginal");


        // Dirty entry in ns1
        cachingListState.setCurrentNamespace(ns1);
        mockBackend.setCurrentKey(key1Ns1);
        cachingListState.update(val1Ns1Dirty);

        // Clean entry in ns1 (different key)
        String key2Ns1Clean = "f_key2_L_ns1_clean";
        mockBackend.setCurrentKey(key2Ns1Clean);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(valCleanOriginal));
        cachingListState.get(); // Loads valCleanOriginal into L1 for (ns1, key2Ns1Clean)

        // Dirty entry in ns2
        cachingListState.setCurrentNamespace(ns2);
        mockBackend.setCurrentKey(key1Ns2);
        cachingListState.update(val1Ns2Dirty);

        // Restore a default context before flush
        cachingListState.setCurrentNamespace(testNamespace);
        mockBackend.setCurrentKey(testKey);

        cachingListState.flushToUnderlyingState();

        verify(mockDelegateState, times(1)).update(val1Ns1Dirty); // For (ns1, key1Ns1)
        verify(mockDelegateState, times(1)).update(val1Ns2Dirty); // For (ns2, key1Ns2)
        verify(mockDelegateState, times(0)).update(valCleanOriginal); // Clean entry not updated

        // Verify entries are marked clean (e.g., by trying to evict them and not seeing another
        // update)
        // Check for (ns1, key1Ns1) - was dirty, now should be clean in L1 (or L2 if flush moves)
        cachingListState.setCurrentNamespace(ns1);
        mockBackend.setCurrentKey(key1Ns1);
        // Evict it by filling L1 for ns1
        mockBackend.setCurrentKey("flushed_evictor1_L");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("fe1"));
        cachingListState.get();
        mockBackend.setCurrentKey("flushed_evictor2_L");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("fe2"));
        cachingListState.get();

        // update for val1Ns1Dirty should still be 1 (not called again on clean eviction)
        verify(mockDelegateState, times(1)).update(val1Ns1Dirty);
    }

    @Test
    void testClearList_removesFromAllCachesAndDelegate() throws Exception {
        // --- Setup: testKey in L1 and L2 for testNamespace ---
        // 1. testKey -> delegateList (clean) in L1
        mockBackend.setCurrentKey(testKey);
        cachingListState.setCurrentNamespace(testNamespace);
        when(mockDelegateState.get()).thenReturn(new ArrayList<>(delegateList));
        cachingListState.get();

        // 2. Evict testKey to L2
        mockBackend.setCurrentKey("clear_filler_L1");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("cfL1"));
        cachingListState.get();
        mockBackend.setCurrentKey("clear_filler_L2");
        when(mockDelegateState.get()).thenReturn(Arrays.asList("cfL2"));
        cachingListState.get();
        // testKey is now in L2 for testNamespace.

        // --- Action: Clear for testKey in testNamespace ---
        mockBackend.setCurrentKey(testKey); // Set current key for clear
        cachingListState.setCurrentNamespace(testNamespace); // Set current namespace for clear
        cachingListState.clear();

        // --- Verification ---
        verify(mockDelegateState, times(1)).clear(); // Delegate's clear called

        // Get for testKey should now miss L1 & L2, and hit delegate (which should be clear)
        when(mockDelegateState.get()).thenReturn(null); // Simulate delegate is cleared
        assertEquals(null, cachingListState.get(), "List should be null after clear");

        int initialGets = 1 /* for delegateList */ + 1 /* cfL1 */ + 1 /* cfL2 */;
        verify(mockDelegateState, times(initialGets + 1)).get(); // +1 for the get after clear
    }

    // --- Delegation of other InternalListState/InternalKvState methods ---
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
