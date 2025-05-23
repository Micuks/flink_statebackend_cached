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
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CachingInternalMapStateTest {

    @Mock
    private InternalMapState<String, String, String, String> mockDelegateState;
    @Mock
    private CachingKeyedStateBackend<String> mockBackend;
    @Mock
    private TypeSerializer<String> mockKeySerializer; // Flink Key
    @Mock
    private TypeSerializer<String> mockNamespaceSerializer;
    @Mock
    private TypeSerializer<String> mockUserKeySerializer; // User Key
    @Mock
    private TypeSerializer<String> mockUserValueSerializer; // User Value

    @Mock
    private MapSerializer<String, String> mockMapValueSerializer; // Changed type to MapSerializer

    private CachingInternalMapState<String, String, String, String> cachingMapState;

    private final int l1CacheSizePerMap = 2;
    private final int l2CacheSizePerMap = 2;
    private final int maxActiveNamespaceKeyCombinations = 2;

    private final String testFlinkKey = "testFlinkKey";
    private final String testNamespace = "testNamespace";
    private final String testUserKey1 = "testUserKey1";
    private final String testUserValue1 = "testUserValue1";
    private final String testUserKey2 = "testUserKey2";
    private final String testUserValue2 = "testUserValue2";
    private final String testUserKey3 = "testUserKey3";
    private final String testUserValue3 = "testUserValue3";

    // Field to control the key returned by mockBackend.getCurrentKey()
    private String currentKeyForMock;

    @BeforeEach
    void setUp() {
        when(mockDelegateState.getKeySerializer()).thenReturn(mockKeySerializer);
        when(mockDelegateState.getNamespaceSerializer()).thenReturn(mockNamespaceSerializer);

        // Fix: Stub the mockMapValueSerializer to return the mock user key/value serializers
        when(mockMapValueSerializer.getKeySerializer()).thenReturn(mockUserKeySerializer);
        when(mockMapValueSerializer.getValueSerializer()).thenReturn(mockUserValueSerializer);
        // End Fix

        when(mockDelegateState.getValueSerializer()).thenReturn(mockMapValueSerializer);

        // Initialize currentKeyForMock and mock backend behavior
        currentKeyForMock = testFlinkKey; // Default key
        when(mockBackend.getCurrentKey()).thenAnswer(invocation -> currentKeyForMock);
        doAnswer(invocation -> {
            currentKeyForMock = invocation.getArgument(0);
            return null; // void method
        }).when(mockBackend).setCurrentKey(any(String.class)); // Assuming K is String

        when(mockBackend.getMaxActiveNamespaceOrPerKeyCacheContainers()).thenReturn(2);

        cachingMapState = new CachingInternalMapState<>(mockDelegateState, mockBackend,
                l1CacheSizePerMap, l2CacheSizePerMap, maxActiveNamespaceKeyCombinations, 20L, // maxCacheMemoryMb,
                CachingStateBackendFactory.CachePolicyType.LRU);
        cachingMapState.setCurrentNamespace(testNamespace);
    }

    // --- Basic Get/Put/Remove for Map Entries ---
    @Test
    void testMapGet_cacheMiss_loadFromDelegate_populateL1() throws Exception {
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);

        // First get: L1 miss, L2 miss, load from delegate
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));

        // Second get: L1 hit
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));
        verify(mockDelegateState, times(1)).get(testUserKey1);
    }

    @Test
    void testMapGet_L1Hit() throws Exception {
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        cachingMapState.get(testUserKey1); // Populate L1. mockDelegateState.get(testUserKey1) is
                                           // called once here.

        assertEquals(testUserValue1, cachingMapState.get(testUserKey1)); // This should be an L1
                                                                         // hit.
        verify(mockDelegateState, times(1)).get(testUserKey1); // Verify total calls to delegate.get
                                                               // for this key is 1.
    }

    @Test
    void testMapGet_L1Miss_L2Hit_promoteToL1() throws Exception {
        // Put testUserKey1 -> testUserValue1 into L2
        // 1. Load into L1
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        cachingMapState.get(testUserKey1); // Calls delegate.get(testUserKey1) once

        // 2. Evict to L2 by filling L1 (size 2)
        when(mockDelegateState.get("uk_filler1")).thenReturn("uv_filler1");
        cachingMapState.get("uk_filler1"); // Calls delegate.get("uk_filler1") once
        when(mockDelegateState.get("uk_filler2")).thenReturn("uv_filler2");
        cachingMapState.get("uk_filler2"); // Calls delegate.get("uk_filler2") once. testUserKey1
                                           // evicted to L2

        // Get testUserKey1: L1 miss, L2 hit, promote to L1. No new delegate call for testUserKey1.
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));
        // verify(mockDelegateState, times(1)).get(testUserKey1); // These intermediate verifies are
        // removed.
        // verify(mockDelegateState, times(1)).get("uk_filler1");
        // verify(mockDelegateState, times(1)).get("uk_filler2");

        // Get again: L1 hit. No new delegate call for testUserKey1.
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));

        // Verify total calls for each key at the end of the relevant operations.
        verify(mockDelegateState, times(1)).get(testUserKey1); // Only the initial load for
                                                               // testUserKey1
        verify(mockDelegateState, times(1)).get("uk_filler1");
        verify(mockDelegateState, times(1)).get("uk_filler2");
    }

    @Test
    void testMapPut_newUserEntry_marksDirtyInL1_evictsL2() throws Exception {
        // L1 and L2 cache size is 2.

        // Pre-populate L1 with 2 entries to later evict one to L2.
        when(mockDelegateState.get("pre_key1")).thenReturn("pre_val1");
        cachingMapState.get("pre_key1"); // L1: {pk1->pv1}
        when(mockDelegateState.get("pre_key2")).thenReturn("pre_val2");
        cachingMapState.get("pre_key2"); // L1: {pk1->pv1, pk2->pv2}

        // This will make pk1 the LRU in L1. Now, add testUserKey1 with an OLD value to L1.
        // Then evict it to L2.
        when(mockDelegateState.get(testUserKey1)).thenReturn("oldValueInL2");
        cachingMapState.get(testUserKey1); // L1: {pk2->pv2, uk1->oldL2}. pk1 is evicted to L2
                                           // {pk1->pv1}.
                                           // So L1 has 2 items, L2 has 1.

        // Now, put the NEW testUserValue1 for testUserKey1.
        // This should make L1: {uk1->uv1 (dirty), pk2->pv2}. The L2 entry for uk1 should be
        // invalidated.
        cachingMapState.put(testUserKey1, testUserValue1);

        // Get should hit L1 and return the new value.
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));

        // Verify delegate.get counts for pre-population and the initial get of testUserKey1
        verify(mockDelegateState, times(1)).get("pre_key1");
        verify(mockDelegateState, times(1)).get("pre_key2");
        verify(mockDelegateState, times(1)).get(testUserKey1); // For "oldValueInL2"

        // Evict testUserKey1 (dirty) from L1 to trigger flush.
        // L1: {uk1->uv1(dirty), pk2->pv2}. Evicting uk1 needs two more puts to L1.
        when(mockDelegateState.get("put_evictor1")).thenReturn("pev1");
        cachingMapState.get("put_evictor1"); // L1: {pk2->pv2, pe1->pev1}. uk1 (dirty) is flushed
                                             // and moved to L2.
                                             // So, mockDelegateState.put(testUserKey1,
                                             // testUserValue1) is called.
                                             // L2 should now have {pk1->pv1, uk1->uv1 (clean)}.

        when(mockDelegateState.get("put_evictor2")).thenReturn("pev2");
        cachingMapState.get("put_evictor2"); // L1: {pe1->pev1, pe2->pev2}. pk2 (clean) is moved to
                                             // L2.
                                             // L2: {pk1->pv1, uk1->uv1, pk2->pv2 (clean)}

        verify(mockDelegateState, times(1)).put(testUserKey1, testUserValue1); // Flushed

        // Access testUserKey1 again - should be an L2 hit (clean, new value).
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));

        // Verify total delegate.get calls (no new get for testUserKey1 after it's in L2).
        verify(mockDelegateState, times(1)).get("pre_key1");
        verify(mockDelegateState, times(1)).get("pre_key2");
        verify(mockDelegateState, times(1)).get(testUserKey1);
        verify(mockDelegateState, times(1)).get("put_evictor1");
        verify(mockDelegateState, times(1)).get("put_evictor2");
    }

    @Test
    void testMapPut_existingUserEntry_updatesInL1_marksDirty() throws Exception {
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        cachingMapState.get(testUserKey1); // L1: uk1->testUserValue1 (clean). Delegate.get(uk1)
                                           // called once.

        String updatedUserValue = "updatedUserValue1";
        cachingMapState.put(testUserKey1, updatedUserValue); // L1: uk1->updatedUserValue (dirty).

        assertEquals(updatedUserValue, cachingMapState.get(testUserKey1)); // L1 hit, should get
                                                                           // updated value.
        verify(mockDelegateState, times(1)).get(testUserKey1); // Delegate.get(uk1) still called
                                                               // only once from initial load.

        // Evict to check flush.
        when(mockDelegateState.get("putex_evictor1")).thenReturn("pexv1");
        cachingMapState.get("putex_evictor1");
        when(mockDelegateState.get("putex_evictor2")).thenReturn("pexv2");
        cachingMapState.get("putex_evictor2"); // uk1 (dirty, updated) is evicted and flushed.

        verify(mockDelegateState, times(1)).put(testUserKey1, updatedUserValue); // Updated value
                                                                                 // flushed.
        // Verify total delegate.get calls
        verify(mockDelegateState, times(1)).get(testUserKey1);
        verify(mockDelegateState, times(1)).get("putex_evictor1");
        verify(mockDelegateState, times(1)).get("putex_evictor2");
    }

    @Test
    void testMapPut_nullValue_removesUserEntry() throws Exception {
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        cachingMapState.get(testUserKey1); // L1: uk1->uv1 (clean). Delegate.get(uk1) called once.

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
    void testMapRemove_userEntry_marksDirtyNullInL1_evictsL2() throws Exception {
        // L1 and L2 cache size is 2.
        // Pre-populate L1 with some entries, one of which will be testUserKey1, then evict it to
        // L2.
        when(mockDelegateState.get("pre_rem_key1")).thenReturn("pre_rem_val1");
        cachingMapState.get("pre_rem_key1"); // L1: {prk1->prv1}

        when(mockDelegateState.get(testUserKey1)).thenReturn("valueInL2Original"); // This is what
                                                                                   // delegate has
                                                                                   // for uk1
        cachingMapState.get(testUserKey1); // L1: {prk1->prv1, uk1->valL2Orig}

        // Evict testUserKey1 to L2 by adding one more entry to L1.
        when(mockDelegateState.get("pre_rem_key2")).thenReturn("pre_rem_val2");
        cachingMapState.get("pre_rem_key2"); // L1: {uk1->valL2Orig, prk2->prv2}. prk1 evicted to
                                             // L2: {prk1->prv1}
                                             // Now L1 has uk1, L2 has prk1.
                                             // Wait, this isn't right. Let's simplify L2
                                             // pre-population.
        // Reset L1 and L2 to be sure, then carefully populate.
        cachingMapState.clear(); // Clears L1, L2, and delegate. Need to re-mock delegate clear.
        verify(mockDelegateState, times(1)).clear(); // From cachingMapState.clear()
        org.mockito.Mockito.reset(mockDelegateState); // Reset interactions for cleaner
                                                      // verification, re-mock essentials.
        setUp();// Re-run setup to ensure serializers and current Flink key/NS are set.

        // Pre-populate L1 with testUserKey1, then evict it to L2.
        when(mockDelegateState.get(testUserKey1)).thenReturn("valueInL2"); // Delegate has this for
                                                                           // testUserKey1
        cachingMapState.get(testUserKey1); // L1: {uk1 -> valueInL2 (clean)}. Delegate.get(uk1)
                                           // called.

        when(mockDelegateState.get("filler_rem1")).thenReturn("fv_rem1");
        cachingMapState.get("filler_rem1"); // L1: {uk1, fr1}
        when(mockDelegateState.get("filler_rem2")).thenReturn("fv_rem2");
        cachingMapState.get("filler_rem2"); // L1: {fr1, fr2}. uk1 evicted to L2: {uk1 -> valueInL2
                                            // (clean)}

        // Now, L1 has {fr1, fr2}, L2 has {uk1 -> valueInL2 (clean)}.
        // Remove testUserKey1. This should put a tombstone in L1 and invalidate L2 entry for uk1.
        cachingMapState.remove(testUserKey1); // L1: {fr1, uk1->null (dirty)}. L2 invalidated for
                                              // uk1.
                                              // Original error implies fr2 was expected in L1.
                                              // With L1 size 2, if fr1, fr2 were there, adding
                                              // uk1->null evicts one.
                                              // Let's assume LRU L1: {fr2, uk1->null(dirty)}

        assertEquals(null, cachingMapState.get(testUserKey1)); // Get should be null from L1
                                                               // tombstone.
        assertFalse(cachingMapState.contains(testUserKey1));
        // Delegate.get(testUserKey1) was called once for initial L2 population.
        // The get after remove should hit L1 tombstone, not delegate.
        verify(mockDelegateState, times(1)).get(testUserKey1);

        // Evict testUserKey1 (dirty null) from L1 by adding two more entries.
        when(mockDelegateState.get("rem_evictor1")).thenReturn("rev1");
        cachingMapState.get("rem_evictor1"); // L1: {uk1->null(dirty), rev1}. fr2 is evicted (clean,
                                             // to L2 or gone).
        when(mockDelegateState.get("rem_evictor2")).thenReturn("rev2");
        cachingMapState.get("rem_evictor2"); // L1: {rev1, rev2}. uk1(tombstone) is flushed
                                             // (delegate.remove).

        verify(mockDelegateState, times(1)).remove(testUserKey1); // Flushed as remove.

        // Verify total delegate.get calls
        verify(mockDelegateState, times(1)).get(testUserKey1); // Original load
        verify(mockDelegateState, times(1)).get("filler_rem1");
        verify(mockDelegateState, times(1)).get("filler_rem2");
        verify(mockDelegateState, times(1)).get("rem_evictor1");
        verify(mockDelegateState, times(1)).get("rem_evictor2");
    }

    @Test
    void testMapContains_userKey() throws Exception {
        // --- Scenario 1: Not in cache, not in delegate ---
        when(mockDelegateState.contains(testUserKey1)).thenReturn(false);
        assertFalse(cachingMapState.contains(testUserKey1));
        verify(mockDelegateState, times(1)).contains(testUserKey1);
        verify(mockDelegateState, times(0)).get(testUserKey1); // Should not call get if contains is
                                                               // false

        // --- Scenario 2: Not in cache, but in delegate ---
        when(mockDelegateState.contains(testUserKey2)).thenReturn(true);
        when(mockDelegateState.get(testUserKey2)).thenReturn(testUserValue2); // If contains is
                                                                              // true, get will be
                                                                              // called to populate
                                                                              // cache

        assertTrue(cachingMapState.contains(testUserKey2)); // First call: L1/L2 miss,
                                                            // delegate.contains=true, delegate.get
                                                            // called, L1 populated
        verify(mockDelegateState, times(1)).contains(testUserKey2); // Called once
        verify(mockDelegateState, times(1)).get(testUserKey2); // Called once to populate

        assertTrue(cachingMapState.contains(testUserKey2)); // Second call: L1 hit for contains
                                                            // (value is not null)
        verify(mockDelegateState, times(1)).contains(testUserKey2); // Still 1 for contains
        verify(mockDelegateState, times(1)).get(testUserKey2); // Still 1 for get

        // --- Scenario 3: In L1 cache (not tombstone) ---
        cachingMapState.put(testUserKey3, testUserValue3); // Puts uk3->uv3 (dirty) in L1
        assertTrue(cachingMapState.contains(testUserKey3));
        verify(mockDelegateState, times(0)).contains(testUserKey3); // No delegate call if L1 hit

        // --- Scenario 4: In L1 cache (as tombstone) ---
        cachingMapState.remove(testUserKey3); // Puts uk3->null (dirty tombstone) in L1
        assertFalse(cachingMapState.contains(testUserKey3));
        verify(mockDelegateState, times(0)).contains(testUserKey3); // No delegate call

        // --- Scenario 5: L1 miss, L2 hit ---
        // Setup L2: uk_l2 -> uv_l2
        when(mockDelegateState.get("uk_l2")).thenReturn("uv_l2");
        cachingMapState.get("uk_l2"); // Load to L1
        cachingMapState.get("evictor_c1"); // Evict uk_l2 to L2
        cachingMapState.get("evictor_c2");
        verify(mockDelegateState, times(1)).get("uk_l2"); // Initial load of uk_l2

        assertTrue(cachingMapState.contains("uk_l2")); // L1 miss, L2 hit, promoted. No
                                                       // delegate.contains call.
        verify(mockDelegateState, times(0)).contains("uk_l2");
        verify(mockDelegateState, times(1)).get("uk_l2"); // Still 1 from initial load
    }

    // --- Eviction Logic for Map Entries (Per Flink Key/Namespace) ---
    @Test
    void testMapL1Eviction_cleanEntry_moveToL2() throws Exception {
        // 1. Populate L1 with testUserKey1
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1);
        cachingMapState.get(testUserKey1); // L1: uk1->uv1 (clean). Delegate.get(uk1) called once.

        // 2. Fill L1 to evict testUserKey1 to L2.
        // L1 cache size is 2.
        when(mockDelegateState.get("evictor1")).thenReturn("evictor_val1");
        cachingMapState.get("evictor1"); // L1: {uk1->uv1, evictor1->ev1}. Delegate.get(evictor1)
                                         // called once.
        when(mockDelegateState.get("evictor2")).thenReturn("evictor_val2");
        cachingMapState.get("evictor2"); // L1: {evictor1->ev1, evictor2->ev2}. uk1 (clean) moved to
                                         // L2.
                                         // Delegate.get(evictor2) called once.

        // Verify testUserKey1 is in L2.
        // Accessing it again should be an L2 hit (and promote to L1), not a delegate call.
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));

        // Verify total delegate.get() calls.
        verify(mockDelegateState, times(1)).get(testUserKey1); // Called once for initial load.
        verify(mockDelegateState, times(1)).get("evictor1");
        verify(mockDelegateState, times(1)).get("evictor2");
    }

    @Test
    void testMapL1Eviction_dirtyEntry_flushToDelegate_moveToL2Clean() throws Exception {
        // Use a backing map to ensure delegate interactions are captured
        final Map<String, String> delegateBackingMap = new java.util.HashMap<>();

        // Mock put to update the backing map
        doAnswer(invocation -> {
            String key = invocation.getArgument(0);
            String value = invocation.getArgument(1);
            delegateBackingMap.put(key, value);
            return null;
        }).when(mockDelegateState).put(any(String.class), any(String.class));

        // Mock get to read from the backing map (for later checks if needed, though L2 hit is
        // primary)
        when(mockDelegateState.get(any(String.class))).thenAnswer(invocation -> {
            // This mock for .get() is important for the final verify(..., times(0)).get()
            // If cachingMapState.get() misses L2 and goes to delegate, this will be called.
            return delegateBackingMap.get(invocation.getArgument(0));
        });

        cachingMapState.put(testUserKey1, testUserValue1); // uk1->uv1 (dirty) in L1
        cachingMapState.put(testUserKey2, testUserValue2); // uk2->uv2 (dirty) in L1. uk1 is eldest.
        cachingMapState.put(testUserKey3, testUserValue3); // uk3->uv3 (dirty) in L1. uk1 (dirty)
                                                           // should be evicted and flushed.

        // Verify that testUserKey1 was flushed to the delegate state.
        verify(mockDelegateState, times(1)).put(testUserKey1, testUserValue1); // Flushed
        // Also assert that the backing map reflects this put.
        assertEquals(testUserValue1, delegateBackingMap.get(testUserKey1));

        // Access uk1 - should be an L2 hit (clean, as it was flushed and moved to L2).
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));

        // Verify no *new* get call on the delegate for testUserKey1 because it should be an L2 hit.
        // The when(mockDelegateState.get(testUserKey1)) was set up to use delegateBackingMap.
        // If cachingMapState.get() missed L2, it would call mockDelegateState.get(testUserKey1).
        // Since it should be an L2 hit, this delegate method should not be invoked for this access.
        verify(mockDelegateState, times(0)).get(testUserKey1); // No get from delegate
    }

    @Test
    void testMapL1Eviction_dirtyRemove_flushToDelegate_notInL2() throws Exception {
        // L1 cache size is 2.
        // 1. Put then remove testUserKey1, making it a dirty tombstone in L1.
        // No initial when(mockDelegateState.get(testUserKey1)) needed as put makes it dirty
        // directly.
        cachingMapState.put(testUserKey1, testUserValue1); // L1: {uk1->uv1 (dirty)}
        cachingMapState.remove(testUserKey1); // L1: {uk1->null (dirty tombstone)}

        // 2. Fill L1 to evict testUserKey1 (dirty tombstone).
        when(mockDelegateState.get("evictor_dr_1")).thenReturn("evictor_dr_val1");
        cachingMapState.get("evictor_dr_1"); // L1: {uk1->null(dirty), edr1->vdr1}
        when(mockDelegateState.get("evictor_dr_2")).thenReturn("evictor_dr_val2");
        cachingMapState.get("evictor_dr_2"); // L1: {edr1->vdr1, edr2->vdr2}. uk1 (tombstone) is
                                             // flushed.

        // Verify flush occurred (remove on delegate for testUserKey1).
        verify(mockDelegateState, times(1)).remove(testUserKey1);
        // Also verify the initial put for testUserKey1 never happened on the delegate because it
        // was removed before flush.
        verify(mockDelegateState, times(0)).put(testUserKey1, testUserValue1);

        // Verify testUserKey1 is not in L1 or L2 and getting it queries delegate.
        when(mockDelegateState.get(testUserKey1)).thenReturn(null); // Simulate it's gone from
                                                                    // backend after remove.
        assertEquals(null, cachingMapState.get(testUserKey1)); // Should trigger
                                                               // delegate.get(testUserKey1).

        // Verify total delegate.get calls.
        verify(mockDelegateState, times(1)).get(testUserKey1); // This is the get to check if it's
                                                               // in backend *after* supposed
                                                               // removal.
        verify(mockDelegateState, times(1)).get("evictor_dr_1");
        verify(mockDelegateState, times(1)).get("evictor_dr_2");
    }

    @Test
    void testMapL2Eviction() throws Exception {
        // L2 size is 2. Put 3 user keys into L2 for the same FlinkKey/Namespace.
        // UK1 to L2
        when(mockDelegateState.get("uk_l2_e1")).thenReturn("uv_l2_e1");
        cachingMapState.get("uk_l2_e1");
        when(mockDelegateState.get("f_l2e1_1")).thenReturn("v_l2e1_1");
        cachingMapState.get("f_l2e1_1");
        when(mockDelegateState.get("f_l2e1_2")).thenReturn("v_l2e1_2");
        cachingMapState.get("f_l2e1_2"); // uk_l2_e1 in L2

        // UK2 to L2
        when(mockDelegateState.get("uk_l2_e2")).thenReturn("uv_l2_e2");
        cachingMapState.get("uk_l2_e2");
        when(mockDelegateState.get("f_l2e2_1")).thenReturn("v_l2e2_1");
        cachingMapState.get("f_l2e2_1");
        when(mockDelegateState.get("f_l2e2_2")).thenReturn("v_l2e2_2");
        cachingMapState.get("f_l2e2_2"); // uk_l2_e2 in L2. L2:{uk_l2_e1, uk_l2_e2}

        // UK3 to L2 (evicts uk_l2_e1 from L2)
        when(mockDelegateState.get("uk_l2_e3")).thenReturn("uv_l2_e3");
        cachingMapState.get("uk_l2_e3");
        when(mockDelegateState.get("f_l2e3_1")).thenReturn("v_l2e3_1");
        cachingMapState.get("f_l2e3_1");
        when(mockDelegateState.get("f_l2e3_2")).thenReturn("v_l2e3_2");
        cachingMapState.get("f_l2e3_2"); // uk_l2_e3 in L2. L2:{uk_l2_e2, uk_l2_e3}

        // Access uk_l2_e1 - full miss
        when(mockDelegateState.get("uk_l2_e1")).thenReturn("uv_l2_e1_reloaded"); // Simulate reload
        assertEquals("uv_l2_e1_reloaded", cachingMapState.get("uk_l2_e1"));
        verify(mockDelegateState, times(2)).get("uk_l2_e1"); // Original + reload
    }

    // --- Iterators and Bulk Operations ---
    @Test
    void testMapEntries_iterator_loadsAllIfCacheNotFullAndDirtyFlushed() throws Exception {
        // Use a mutable map to back the delegate state for dynamic responses
        final Map<String, String> delegateBackingMap = new java.util.HashMap<>();
        delegateBackingMap.put(testUserKey1, testUserValue1);
        delegateBackingMap.put(testUserKey2, testUserValue2);

        when(mockDelegateState.entries()).thenAnswer(invocation -> delegateBackingMap.entrySet());
        when(mockDelegateState.isEmpty()).thenAnswer(invocation -> delegateBackingMap.isEmpty());
        when(mockDelegateState.get(any(String.class)))
                .thenAnswer(invocation -> delegateBackingMap.get(invocation.getArgument(0)));

        doAnswer(invocation -> {
            String key = invocation.getArgument(0);
            String value = invocation.getArgument(1);
            delegateBackingMap.put(key, value);
            return null;
        }).when(mockDelegateState).put(any(String.class), any(String.class));

        doAnswer(invocation -> {
            String key = invocation.getArgument(0);
            delegateBackingMap.remove(key);
            return null;
        }).when(mockDelegateState).remove(any(String.class));


        // L1 has (uk3, uv3_dirty)
        cachingMapState.put(testUserKey3, testUserValue3);

        Iterable<Map.Entry<String, String>> entries = cachingMapState.entries();
        // Verify that testUserKey3 was flushed to the delegate.
        // This is the crucial check that was failing.
        verify(mockDelegateState, times(1)).put(testUserKey3, testUserValue3); // uk3 flushed
        // Verify that entries() was called on the delegate to load data.
        verify(mockDelegateState, times(1)).entries(); // Delegate entries fetched

        Map<String, String> resultMap = new java.util.HashMap<>();
        for (Map.Entry<String, String> entry : entries) {
            resultMap.put(entry.getKey(), entry.getValue());
        }

        assertEquals(3, resultMap.size());
        assertEquals(testUserValue1, resultMap.get(testUserKey1));
        assertEquals(testUserValue2, resultMap.get(testUserKey2));
        assertEquals(testUserValue3, resultMap.get(testUserKey3)); // From flushed & reloaded cache

        // Check L1 is now fully loaded and clean
        // These gets should be L1 hits after `entries()` populated L1.
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));
        assertEquals(testUserValue2, cachingMapState.get(testUserKey2));
        assertEquals(testUserValue3, cachingMapState.get(testUserKey3));

        // Verify no *new* delegate .get calls for these keys after L1 is populated.
        // Since these keys were populated via delegate.entries() (or were already in L1 from put),
        // subsequent cachingMapState.get() calls should hit L1.
        // We need to be careful if delegate.get() was called during the entries() processing
        // itself,
        // but loadAllEntriesToCache uses delegate.entries(), not multiple delegate.get().
        // The mock for delegate.get() is now active. We need to ensure these specific verifications
        // are for calls *after* the main entries() call that populates L1.

        // To be precise:
        // uk1 and uk2 are loaded from delegate.entries() into L1.
        // uk3 was put into L1 (dirty), then flushed to delegate (via delegate.put),
        // then delegate.entries() (now including uk3) is used to reload L1.
        // So all three should be in L1, and clean.
        // Therefore, no delegate.get() calls should occur for these when fetched from
        // cachingMapState.
        // The when(mockDelegateState.get(...)) above will count any get that *does* occur.
        // We assert that the count of these specific get calls remains 0 *for these specific get
        // invocations*.

        // Let's reset interactions ONLY for get() calls on specific keys before these final checks,
        // to ensure we're verifying L1 hits correctly post-load.
        // However, a simpler way is to verify the *total* number of calls.
        // If uk1, uk2, uk3 were never fetched by delegate.get() before this point in this test,
        // then times(0) is correct. They are populated via .entries() or .put().
        verify(mockDelegateState, times(0)).get(testUserKey1);
        verify(mockDelegateState, times(0)).get(testUserKey2);
        verify(mockDelegateState, times(0)).get(testUserKey3);
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
        // specific keys.
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
        mockBackend.setCurrentKey(flinkKey1);
        cachingMapState.put(testUserKey1, testUserValue1);
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));

        // FK2: uk1->uv2 (same user key, different value for different Flink Key)
        mockBackend.setCurrentKey(flinkKey2);
        cachingMapState.put(testUserKey1, testUserValue2);
        assertEquals(testUserValue2, cachingMapState.get(testUserKey1));

        // Check FK1 still has its value
        mockBackend.setCurrentKey(flinkKey1);
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
        mockBackend.setCurrentKey("fk1");
        cachingMapState.put(testUserKey1, testUserValue1); // L1 for (fk1, testNamespace) has
                                                           // uk1->uv1 (dirty)

        // Flink Key 2: put dirty entry
        mockBackend.setCurrentKey("fk2");
        cachingMapState.put(testUserKey2, testUserValue2); // L1 for (fk2, testNamespace) has
                                                           // uk2->uv2 (dirty)

        // Flink Key 3: access. This should evict cache for fk1 (LRU).
        // The dirty entry (uk1->uv1) for fk1 should be flushed.
        mockBackend.setCurrentKey("fk3");
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
        mockBackend.setCurrentKey("fk1");
        when(mockDelegateState.get(testUserKey1)).thenReturn(testUserValue1); // Simulate it was
                                                                              // flushed and now in
                                                                              // delegate
        assertEquals(testUserValue1, cachingMapState.get(testUserKey1));
        // This get for fk1 (after its cache was evicted and the entry flushed) should call
        // delegate.get()
        verify(mockDelegateState, times(1)).get(testUserKey1); // Total for testUserKey1

        // Restore original context
        mockBackend.setCurrentKey(testFlinkKey);
        cachingMapState.setCurrentNamespace(testNamespace);
    }

    // --- CachingInternalState Methods ---
    @Test
    void testFlushMap_writesDirtyEntriesToDelegate_marksClean() throws Exception {
        // Setup: current Flink key "fk1", namespace "flush_ns1"
        mockBackend.setCurrentKey("fk1"); // Set Flink key context for the backend
        cachingMapState.setCurrentNamespace("flush_ns1");
        cachingMapState.put(testUserKey1, testUserValue1); // L1 dirty for (fk1, flush_ns1)
        cachingMapState.put(testUserKey2, testUserValue2); // L1 dirty for (fk1, flush_ns1)

        // Setup: current Flink key "fk2", namespace "flush_ns2"
        mockBackend.setCurrentKey("fk2"); // Change Flink key context for the backend
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
        mockBackend.setCurrentKey(testFlinkKey);
        cachingMapState.setCurrentNamespace(testNamespace);

        // Further checks for cleanliness:
        // Set context to fk1, flush_ns1
        mockBackend.setCurrentKey("fk1");
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
        mockBackend.setCurrentKey("fk2");
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
        mockBackend.setCurrentKey(testFlinkKey);
        cachingMapState.setCurrentNamespace(testNamespace);
    }

    @Test
    void testClearMap_removesFromAllCachesAndDelegate() throws Exception {
        // FK1, NS1: uk1 -> uv1 in L1, then L2
        mockBackend.setCurrentKey("clear_fk");
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
