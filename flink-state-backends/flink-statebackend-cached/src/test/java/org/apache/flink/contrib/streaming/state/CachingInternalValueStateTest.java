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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.mockito.Mockito.lenient;

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

    @BeforeEach
    void setUp() {
        TaskKvStateRegistry kvStateRegistry = mock(TaskKvStateRegistry.class);
        ExecutionConfig executionConfig = new ExecutionConfig();
        TtlTimeProvider ttlTimeProvider = TtlTimeProvider.DEFAULT;
        MetricGroup metricGroup = new UnregisteredMetricsGroup();
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();

        lenient().when(mockKeySerializer.duplicate()).thenReturn(mockKeySerializer);
        lenient()
                .when(mockAbstractKeyedStateBackendDelegate.getKeySerializer())
                .thenReturn(mockKeySerializer);

        // Create proper KeyGroupRange and numberOfKeyGroups for InternalKeyContextImpl
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 15);
        int numberOfKeyGroups = 16;
        lenient()
                .when(mockAbstractKeyedStateBackendDelegate.getKeyContext())
                .thenReturn(
                        new org.apache.flink.runtime.state.heap.InternalKeyContextImpl<>(
                                keyGroupRange, numberOfKeyGroups));

        cachingKeyedStateBackend =
                new CachingKeyedStateBackend<>(
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
                        0L,
                        CachingStateBackendFactory.CachePolicyType.LRU);
        cachingKeyedStateBackend.setCurrentKey(testKey);

        // Configure common mock behaviors
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
                        CachingStateBackendFactory.CachePolicyType.LRU);
        // Set current namespace for the caching state (and its delegate)
        cachingState.setCurrentNamespace(testNamespace);
    }

    // --- Basic Get/Update Tests ---

    @Test
    void testValueGet_cacheMiss_loadFromDelegate_populateL1() throws Exception {
        // Setup: Delegate returns testValue1 on first call
        when(mockDelegateState.value()).thenReturn(testValue1);

        // Action 1: First call to value() - should be a cache miss
        String retrievedValue1 = cachingState.value();

        // Verification 1
        assertEquals(testValue1, retrievedValue1, "Value from first call should match delegate");
        verify(mockDelegateState, times(1)).value(); // Delegate should be called once

        // Action 2: Second call to value() - should be an L1 cache hit
        String retrievedValue2 = cachingState.value();

        // Verification 2
        assertEquals(
                testValue1, retrievedValue2, "Value from second call should match cached value");
        verify(mockDelegateState, times(1))
                .value(); // Delegate should still only be called once (hit L1)
    }

    @Test
    void testValueGet_L1Hit() throws Exception {
        // Setup: Populate L1 cache by calling value() once
        when(mockDelegateState.value()).thenReturn(testValue1);
        cachingState.value(); // This call populates L1
        verify(mockDelegateState, times(1)).value(); // Verify delegate was called for population

        // Action: Call value() again - should be an L1 cache hit
        String retrievedValue = cachingState.value();

        // Verification
        assertEquals(
                testValue1,
                retrievedValue,
                "Value from L1 hit should match initially cached value");
        // Delegate should still only have been called once from the initial population
        verify(mockDelegateState, times(1)).value();
    }

    @Test
    void testValueGet_L1Miss_L2Hit_promoteToL1() throws Exception {
        // --- Setup: Populate L1 for 3 different keys in the same namespace to ensure one gets
        // evicted to L2 ---
        // Key 1 (testKey, our target key)
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.value()).thenReturn(testValue1); // For initial load of testKey
        cachingState.value(); // testKey -> testValue1 in L1
        verify(mockDelegateState, times(1)).value();

        // Key 2 (anotherKey1)
        String anotherKey1 = "anotherKey1";
        String anotherValue1 = "anotherValue1";
        cachingKeyedStateBackend.setCurrentKey(anotherKey1);
        when(mockDelegateState.value())
                .thenReturn(anotherValue1); // For initial load of anotherKey1
        cachingState.value(); // anotherKey1 -> anotherValue1 in L1
        verify(mockDelegateState, times(2)).value(); // Delegate called for anotherKey1

        // Key 3 (anotherKey2) - this should evict testKey (testValue1) from L1 to L2
        // because l1CacheSize is 2. Order of access: testKey, anotherKey1. Evicted: testKey.
        String anotherKey2 = "anotherKey2";
        String anotherValue2 = "anotherValue2";
        cachingKeyedStateBackend.setCurrentKey(anotherKey2);
        when(mockDelegateState.value())
                .thenReturn(anotherValue2); // For initial load of anotherKey2
        cachingState.value(); // anotherKey2 -> anotherValue2 in L1. testKey should now be in L2.
        verify(mockDelegateState, times(3)).value(); // Delegate called for anotherKey2

        // --- Action: Access the original key (testKey) ---
        // It should be an L1 miss, L2 hit, and then promoted to L1.
        cachingKeyedStateBackend.setCurrentKey(testKey); // Switch back to the original key
        // Ensure the delegate does not provide the value again for testKey
        // For an L2 hit, the delegate should NOT be called for testKey again.
        // (If we were to reset mock and set a new return value, that would test a full miss)

        String retrievedValue = cachingState.value();

        // --- Verification ---
        assertEquals(testValue1, retrievedValue, "Value from L2 hit should match original value");
        // Delegate was called 3 times (for testKey, anotherKey1, anotherKey2 initial loads).
        // It should NOT be called a 4th time for testKey's L2 hit.
        verify(mockDelegateState, times(3)).value();

        // Optional: Verify it's now in L1 again (by causing another eviction and seeing if it stays
        // or goes to delegate)
        // For simplicity, we'll assume the L2->L1 promotion logic inside CachingInternalValueState
        // is correct.
        // A more direct way to check L1 would be to inspect cache contents if possible, or verify
        // behavior on further ops.
    }

    @Test
    void testUpdate_newValue_marksDirtyInL1_evictsL2IfExists() throws Exception {
        // --- Setup Phase 1: Get testKey (testValue1) into L2 ---
        // Step 1.1: testKey -> testValue1 (clean) in L1.
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.value()).thenReturn(testValue1);
        cachingState.value(); // testKey is current key by default from setUp
        verify(mockDelegateState, times(1)).value();

        // Step 1.2: anotherKey1 -> anotherValue1 (clean) in L1.
        String anotherKey1 = "anotherKey1";
        String anotherValue1 = "anotherValue1";
        cachingKeyedStateBackend.setCurrentKey(anotherKey1);
        when(mockDelegateState.value()).thenReturn(anotherValue1);
        cachingState.value();
        verify(mockDelegateState, times(2)).value(); // Total 2 delegate.value() calls

        // Step 1.3: anotherKey2 -> anotherValue2 (clean) in L1. This evicts testKey to L2.
        // L1 (size 2) now: (anotherKey1, anotherValue1), (anotherKey2, anotherValue2)
        // L2 now: (testKey, testValue1)
        String anotherKey2 = "anotherKey2";
        String anotherValue2 = "anotherValue2";
        cachingKeyedStateBackend.setCurrentKey(anotherKey2);
        when(mockDelegateState.value()).thenReturn(anotherValue2);
        cachingState.value();
        verify(mockDelegateState, times(3)).value(); // Total 3 delegate.value() calls

        // --- Setup Phase 2: Current key is testKey. Update it. ---
        cachingKeyedStateBackend.setCurrentKey(testKey);
        String testValue2_updated = "testValue2_updated";
        cachingState.update(testValue2_updated);
        // Now: L1 should contain testKey -> testValue2_updated (dirty).
        // L2 should NOT have testKey anymore (entry for testValue1 removed).

        // --- Verification Phase 1: Accessing testKey gets new value from L1 ---
        assertEquals(
                testValue2_updated,
                cachingState.value(),
                "Value after update should be the new value from L1.");
        // delegateState.value() should still only have been called 3 times (no new call for this
        // access).
        verify(mockDelegateState, times(3)).value();

        // --- Verification Phase 2: Evict testKey (dirty) from L1, ensure it writes to delegate and
        // then L2 ---
        // To evict testKey (now with testValue2_updated, dirty) from L1:
        // L1 before this: (testKey, testValue2_updated_dirty), (anotherKey2, anotherValue2_clean) -
        // assuming order from recent access
        // Let's make testKey eldest by accessing anotherKey2
        cachingKeyedStateBackend.setCurrentKey(anotherKey2);
        cachingState.value(); // Access anotherKey2. It was already in L1. No new delegate call.
        verify(mockDelegateState, times(3)).value();

        // Now add a new key to evict testKey.
        String anotherKey3 = "anotherKey3";
        String anotherValue3 = "anotherValue3";
        cachingKeyedStateBackend.setCurrentKey(anotherKey3);
        when(mockDelegateState.value()).thenReturn(anotherValue3); // For loading anotherKey3
        cachingState.value(); // This should evict testKey(testValue2_updated_dirty)
        // Delegate should be called for anotherKey3's initial load.
        verify(mockDelegateState, times(4)).value();
        // Crucially, delegateState.update() should have been called for testKey with testValue2_updated.
        verify(mockDelegateState, times(1)).update(testValue2_updated);

        // --- Verification Phase 3: testKey (testValue2_updated) should now be clean in L2 ---
        // To verify, set key to testKey, then evict it from L1 (if it got there) and see if L2 hit
        // or delegate.
        // Simpler: If we access testKey now, it should be an L2 hit for testValue2_updated.
        cachingKeyedStateBackend.setCurrentKey(testKey);
        // The previous eviction of testKey (dirty) wrote testValue2_updated to delegate and put clean
        // testValue2_updated to L2.
        // So, current access to testKey should be an L2 hit.
        assertEquals(
                testValue2_updated,
                cachingState.value(),
                "Value for testKey after it was updated, evicted, and re-accessed should be from L2.");
        // Delegate value() count should still be 4. No new value() call for L2 hit.
        verify(mockDelegateState, times(4)).value();
    }

    @Test
    void testUpdate_nullValue_clearsStateAndCache() throws Exception {
        // Setup: Get testKey (testValue1) into L1
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.value()).thenReturn(testValue1);
        cachingState.value();
        verify(mockDelegateState, times(1)).value(); // Initial load

        // Action: Update with null
        cachingState.update(null);

        // Verification
        // 1. Delegate state should have been cleared.
        verify(mockDelegateState, times(1)).clear();

        // 2. Accessing the value now should return null (from delegate, as cache is cleared for
        // this key).
        //    Configure delegate to return null as it has been cleared for testKey.
        when(mockDelegateState.value()).thenReturn(null);
        assertEquals(null, cachingState.value(), "Value after update(null) should be null.");

        // 3. Delegate.value() should have been called again for this access.
        verify(mockDelegateState, times(2)).value();
    }

    // --- Eviction Logic Tests ---

    @Test
    void testL1Eviction_cleanEntry_moveToL2() throws Exception {
        // --- Setup: testKey -> testValue1 (clean) is in L1 --
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.value()).thenReturn(testValue1);
        cachingState.value();
        verify(mockDelegateState, times(1)).value(); // Initial load for testKey

        // --- Action 1: Populate L1 with two other entries to evict testKey to L2 ---
        // Key 2 (anotherKey1)
        String anotherKey1 = "anotherKey1_L1EvictClean";
        String anotherValue1 = "anotherValue1_L1EvictClean";
        cachingKeyedStateBackend.setCurrentKey(anotherKey1);
        when(mockDelegateState.value()).thenReturn(anotherValue1);
        cachingState.value();
        verify(mockDelegateState, times(2)).value(); // Delegate called for anotherKey1

        // Key 3 (anotherKey2)
        String anotherKey2 = "anotherKey2_L1EvictClean";
        String anotherValue2 = "anotherValue2_L1EvictClean";
        cachingKeyedStateBackend.setCurrentKey(anotherKey2);
        when(mockDelegateState.value()).thenReturn(anotherValue2);
        cachingState.value(); // This should evict testKey to L2
        verify(mockDelegateState, times(3)).value(); // Delegate called for anotherKey2

        // --- Verification 1: testKey (testValue1) is now in L2. Delegate should not be called.
        // ---
        cachingKeyedStateBackend.setCurrentKey(testKey); // Switch back to testKey
        assertEquals(
                testValue1, cachingState.value(), "Value should be retrieved from L2 (was clean)");
        // Delegate was called 3 times (for testKey, anotherKey1, anotherKey2 initial loads).
        // It should NOT be called a 4th time for testKey's L2 hit.
        verify(mockDelegateState, times(3)).value();
    }

    @Test
    void testL1Eviction_dirtyEntry_flushToDelegate_moveToL2Clean() throws Exception {
        // --- Setup: testKey -> testValue1 (dirty) is in L1 --
        cachingKeyedStateBackend.setCurrentKey(testKey);
        cachingState.update(testValue1); // testValue1 is now dirty in L1
        // No delegate.value() call yet, just an update that marks dirty.
        // Access it to make sure it's in L1 (and still dirty)
        assertEquals(testValue1, cachingState.value());
        verify(mockDelegateState, times(0))
                .value(); // Value was from update, not delegate.get()

        // --- Action 1: Populate L1 with two other entries to evict testKey to L2 ---
        // Key 2 (anotherKey1)
        String anotherKey1 = "anotherKey1_L1EvictDirty";
        String anotherValue1 = "anotherValue1_L1EvictDirty";
        cachingKeyedStateBackend.setCurrentKey(anotherKey1);
        when(mockDelegateState.value()).thenReturn(anotherValue1);
        cachingState.value();
        verify(mockDelegateState, times(1)).value(); // Delegate called for anotherKey1 (first get)

        // Key 3 (anotherKey2)
        String anotherKey2 = "anotherKey2_L1EvictDirty";
        String anotherValue2 = "anotherValue2_L1EvictDirty";
        cachingKeyedStateBackend.setCurrentKey(anotherKey2);
        when(mockDelegateState.value()).thenReturn(anotherValue2);
        cachingState.value(); // This should evict testKey (dirty) to L2
        verify(mockDelegateState, times(2)).value(); // Delegate called for anotherKey2

        // --- Verification 1: testKey (testValue1) should have been flushed to delegate ---
        verify(mockDelegateState, times(1)).update(testValue1);

        // --- Verification 2: testKey (testValue1) is now in L2 (clean).
        //    Accessing it should not call delegate.value()
        cachingKeyedStateBackend.setCurrentKey(testKey);
        // If it's in L2, the delegate shouldn't be called.
        // We need to make sure the delegate *would* return it if asked, to confirm it
        // was flushed.
        assertEquals(
                testValue1,
                cachingState.value(),
                "Value should be retrieved from L2 (was flushed and marked clean)");
        // Delegate.value() was called for anotherKey1, anotherKey2.
        // It should NOT be called again for testKey's L2 hit.
        verify(mockDelegateState, times(2)).value();
    }

    @Test
    void testL2Eviction() throws Exception {
        // --- Setup: Populate L1 and L2 such that testKey is in L2 ---
        // 1. testKey -> testValue1 (L1)
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.value()).thenReturn(testValue1);
        cachingState.value();
        int delegateGetCalls = 1;
        verify(mockDelegateState, times(delegateGetCalls)).value();

        // 2. fillerKey1 -> "fv1" (L1), testKey -> testValue1 (L2)
        String fillerKey1 = "l2_evict_filler1_for_testKey";
        cachingKeyedStateBackend.setCurrentKey(fillerKey1);
        when(mockDelegateState.value()).thenReturn("fv1");
        cachingState.value();
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).value();

        // 3. fillerKey2 -> "fv2" (L1), fillerKey1 -> "fv1" (L2), testKey -> testValue1 (evicted from L2)
        // No, L1 is size 2. So after fillerKey1, testKey goes to L2.
        // Then fillerKey2 -> "fv2" (L1), testKey -> testValue1 (L2), fillerKey1 -> "fv1" (L1)
        // L1: (fillerKey1, fv1), (testKey, testValue1)
        // Access fillerKey1 and fillerKey2 to make testKey the LRU in L1
        cachingKeyedStateBackend.setCurrentKey(fillerKey1);
        cachingState.value(); // Hit for fv1, no new delegate call
        verify(mockDelegateState, times(delegateGetCalls)).value();

        String fillerKey2_for_L1_evict_testKey = "l2_evict_filler2_for_testKey_L1_evict";
        cachingKeyedStateBackend.setCurrentKey(fillerKey2_for_L1_evict_testKey);
        when(mockDelegateState.value()).thenReturn("fv2_for_L1_evict");
        cachingState.value(); // This evicts testKey from L1 to L2
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).value();
        // L1: (fillerKey1, fv1), (fillerKey2_for_L1_evict_testKey, fv2_for_L1_evict)
        // L2: (testKey, testValue1)

        // --- Now, fill L2 with two *other* entries to evict testKey from L2 ---
        // Entry 1 for L2 (keyL2_2)
        // First, get it into L1, then evict it to L2
        String keyL2_2 = "keyL2_2";
        String valL2_2 = "valL2_2";
        cachingKeyedStateBackend.setCurrentKey(keyL2_2);
        when(mockDelegateState.value()).thenReturn(valL2_2);
        cachingState.value(); // keyL2_2 -> valL2_2 (L1)
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).value();

        // Evict keyL2_2 to L2 by adding two more to L1
        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler1_for_keyL2_2");
        when(mockDelegateState.value()).thenReturn("fv_l2_f1");
        cachingState.value();
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).value();

        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler2_for_keyL2_2");
        when(mockDelegateState.value()).thenReturn("fv_l2_f2");
        cachingState.value(); // keyL2_2 is now in L2
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).value();
        // L2 now contains (testKey, testValue1) and (keyL2_2, valL2_2), assuming LRU for L2

        // Entry 2 for L2 (keyL2_3) - this should evict testKey from L2
        String keyL2_3 = "keyL2_3";
        String valL2_3 = "valL2_3";
        cachingKeyedStateBackend.setCurrentKey(keyL2_3);
        when(mockDelegateState.value()).thenReturn(valL2_3);
        cachingState.value(); // keyL2_3 -> valL2_3 (L1)
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).value();

        // Evict keyL2_3 to L2
        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler1_for_keyL2_3");
        when(mockDelegateState.value()).thenReturn("fv_l2_f3");
        cachingState.value();
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).value();

        cachingKeyedStateBackend.setCurrentKey("l2_evict_filler2_for_keyL2_3");
        when(mockDelegateState.value()).thenReturn("fv_l2_f4");
        cachingState.value(); // keyL2_3 is now in L2
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).value();
        // L2 eviction order depends on CachePolicy (LRU for L2 by default)
        // Current L2 (capacity 2): (keyL2_2, valL2_2), (keyL2_3, valL2_3).
        // testKey (testValue1) should have been evicted.

        // --- Action: Access testKey. It should be a full cache miss (L1 & L2 miss). ---
        cachingKeyedStateBackend.setCurrentKey(testKey);
        // Delegate must provide the value again as it's not in cache
        when(mockDelegateState.value()).thenReturn(testValue1);
        assertEquals(
                testValue1,
                cachingState.value(),
                "Value should be retrieved from delegate after L2 eviction.");

        // --- Verification ---
        // Total delegate calls:
        // 1 for testKey (initial)
        // 1 for fillerKey1
        // 1 for fillerKey2_for_L1_evict_testKey
        // 1 for keyL2_2 (initial)
        // 1 for l2_evict_filler1_for_keyL2_2
        // 1 for l2_evict_filler2_for_keyL2_2
        // 1 for keyL2_3 (initial)
        // 1 for l2_evict_filler1_for_keyL2_3
        // 1 for l2_evict_filler2_for_keyL2_3
        // 1 for testKey (after L2 eviction)
        // Total = 10
        delegateGetCalls++;
        verify(mockDelegateState, times(delegateGetCalls)).value();
    }

    // --- Namespace Handling Tests ---

    @Test
    void testMultipleNamespaces_cachesAreSeparate() throws Exception {
        String ns1 = "multi_ns_1";
        String valNs1 = "val_mns1";
        String ns2 = "multi_ns_2";
        String valNs2 = "val_mns2";

        cachingKeyedStateBackend.setCurrentKey(testKey); // Keep key constant

        // Namespace 1
        cachingState.setCurrentNamespace(ns1);
        when(mockDelegateState.value()).thenReturn(valNs1);
        assertEquals(
                valNs1,
                cachingState.value(),
                "Value for key in ns1 should be from delegate initially.");
        verify(mockDelegateState, times(1)).value(); // First delegate call for (ns1, key)

        // --- Interact with ns2 ---
        cachingState.setCurrentNamespace(ns2);
        // Key remains the same (key)
        when(mockDelegateState.value()).thenReturn(valNs2);
        assertEquals(
                valNs2,
                cachingState.value(),
                "Value for key in ns2 should be from delegate initially.");
        verify(mockDelegateState, times(2)).value(); // Second delegate call for (ns2, key)

        // --- Verify ns1 is still intact and served from its L1 cache ---
        cachingState.setCurrentNamespace(ns1);
        // Key remains the same (key)
        assertEquals(
                valNs1,
                cachingState.value(),
                "Value for key in ns1 should be retrieved from its cache.");
        verify(mockDelegateState, times(2)).value(); // Count should remain 2

        // --- Verify ns2 is still intact and served from its L1 cache ---
        cachingState.setCurrentNamespace(ns2);
        // Key remains the same (key)
        assertEquals(
                valNs2,
                cachingState.value(),
                "Value for key in ns2 should be retrieved from its cache.");
        verify(mockDelegateState, times(2)).value(); // Count should remain 2
    }

    @Test
    void testMaxActiveNamespaces_eviction() throws Exception {
        String ns1 = "max_ns_1";
        String valNs1 = "val_max_ns1_DISTINCT"; // Make distinct
        String ns2 = "max_ns_2";
        String valNs2 = "val_max_ns2_DISTINCT"; // Make distinct
        String ns3_evictor = "max_ns_3_evictor"; // This will evict ns1
        String valNs3 = "val_max_ns3_DISTINCT"; // Make distinct

        // Keep key constant
        cachingKeyedStateBackend.setCurrentKey(testKey);
        AtomicInteger delegateValueCallCount = new AtomicInteger(0);

        // Use thenAnswer to count calls and ensure distinct returns are possible
        when(mockDelegateState.value())
                .thenAnswer(
                        invocation -> {
                                            delegateValueCallCount.incrementAndGet();
                                            String currentNs = cachingState.getCurrentNamespace(); // Get ns from
                                            // cachingState
                                            if (ns1.equals(currentNs))
                                                return valNs1;
                            if (ns2.equals(currentNs)) {
                                return valNs2;
                            }
                            if (ns3_evictor.equals(currentNs)) {
                                return valNs3;
                            }
                            // If an unexpected namespace is queried to the mock, fail loudly.
                            throw new AssertionError(
                                    "Unexpected namespace in mockDelegateState.value(): "
                                    + currentNs);
                        });

        // --- Populate with ns1 ---
        cachingState.setCurrentNamespace(ns1);
        assertEquals(valNs1, cachingState.value(), "Value for ns1 should be fetched initially.");
        assertEquals(1, delegateValueCallCount.get(), "Delegate should be called once for ns1 initial load.");

        // --- Populate with ns2 (maxActiveNamespaces=2, so ns1 and ns2 both fit) ---
        cachingState.setCurrentNamespace(ns2);
        assertEquals(valNs2, cachingState.value(), "Value for ns2 should be fetched initially.");
        assertEquals(2, delegateValueCallCount.get(), "Delegate should be called for ns2 initial load.");

        // --- Verify both ns1 and ns2 are still in cache ---
        cachingState.setCurrentNamespace(ns1);
        assertEquals(valNs1, cachingState.value(), "Value for ns1 should be from cache.");
        assertEquals(2, delegateValueCallCount.get(), "Delegate call count should not increase for ns1 cache hit.");

        cachingState.setCurrentNamespace(ns2);
        assertEquals(valNs2, cachingState.value(), "Value for ns2 should be from cache.");
        assertEquals(2, delegateValueCallCount.get(), "Delegate call count should not increase for ns2 cache hit.");

        // --- Populate with ns3 (should evict ns1 since maxActiveNamespaces=2) ---
        cachingState.setCurrentNamespace(ns3_evictor);
        assertEquals(valNs3, cachingState.value(), "Value for ns3 should be fetched initially.");
        assertEquals(3, delegateValueCallCount.get(), "Delegate should be called for ns3 initial load, ns1 evicted.");

        // --- Re-access ns1 after eviction (should require re-fetch from delegate) ---
        cachingState.setCurrentNamespace(ns1);
        assertEquals(valNs1, cachingState.value(), "Value for ns1 should be re-fetched after namespace eviction.");
        assertEquals(4, delegateValueCallCount.get(), "Delegate should be called for ns1 re-fetch.");

        // --- Verify ns2 and ns3 are still accessible from cache ---
        cachingState.setCurrentNamespace(ns2);
        assertEquals(valNs2, cachingState.value(), "Value for ns2 should be re-fetched after ns3_evictor caused its eviction.");
        assertEquals(5, delegateValueCallCount.get(), "Delegate should be called for ns2 re-fetch.");

        cachingState.setCurrentNamespace(ns3_evictor);
        assertEquals(valNs3, cachingState.value(), "Value for ns3 should be re-fetched after ns1 caused its eviction.");
        assertEquals(6, delegateValueCallCount.get(), "Delegate should be called for ns3 re-fetch.");
    }

    // --- flushToUnderlyingState() Tests ---

    @Test
    void testFlush_writesDirtyEntriesToDelegate_marksClean() throws Exception {
        // Namespaces
        String ns1 = "flush_ns1";
        String ns2 = "flush_ns2";

        // Key-Value pairs
        // Dirty entries
        String key1Ns1Dirty = "f_key1_ns1_dirty";
        String val1Ns1Dirty = "dirty_val1_ns1";
        String key2Ns1Dirty = "f_key2_ns1_dirty"; // New dirty entry for ns1
        String val2Ns1Dirty = "dirty_val2_ns1";

        String key1Ns2Dirty = "f_key1_ns2_dirty";
        String val1Ns2Dirty = "dirty_val1_ns2";

        // Clean entries (will be loaded but not modified)
        String keyCleanNs1 = "f_key_clean_ns1";
        String valCleanNs1 = "clean_val_ns1";
        String keyCleanNs2 = "f_key_clean_ns2";
        String valCleanNs2 = "clean_val_ns2";

        // --- Populate caches with some dirty and clean entries ---

        // Namespace 1: key1 (dirty), key2 (dirty), keyClean (clean)
        cachingState.setCurrentNamespace(ns1);

        cachingKeyedStateBackend.setCurrentKey(key1Ns1Dirty);
        cachingState.update(val1Ns1Dirty); // L1 dirty

        cachingKeyedStateBackend.setCurrentKey(key2Ns1Dirty);
        cachingState.update(val2Ns1Dirty); // L1 dirty
        // Note: L1 size is 2, so both dirty entries should fit in L1 for ns1

        // Namespace 2: key1 (dirty), keyClean (clean)
        cachingState.setCurrentNamespace(ns2);

        cachingKeyedStateBackend.setCurrentKey(key1Ns2Dirty);
        cachingState.update(val1Ns2Dirty); // L1 dirty

        cachingKeyedStateBackend.setCurrentKey(keyCleanNs2);
        when(mockDelegateState.value()).thenReturn(valCleanNs2);
        cachingState.value(); // L1 clean

        // Add clean entry to ns1 AFTER setting up ns2 to avoid evicting ns1 dirty
        // entries
        cachingState.setCurrentNamespace(ns1);
        cachingKeyedStateBackend.setCurrentKey(keyCleanNs1);
        when(mockDelegateState.value()).thenReturn(valCleanNs1);
        cachingState.value(); // This might evict key1Ns1Dirty to L2, but key2Ns1Dirty should stay in L1

        // Reset to test defaults for safety, though not strictly needed for flush
        cachingState.setCurrentNamespace(testNamespace);
        cachingKeyedStateBackend.setCurrentKey(testKey);

        // --- Action: Flush all states ---
        cachingState.flushToUnderlyingState();

        // --- Verification: Delegate's update should be called for all dirty entries ---
        verify(mockDelegateState, times(1)).update(val1Ns1Dirty);
        verify(mockDelegateState, times(1)).update(val2Ns1Dirty);
        verify(mockDelegateState, times(1)).update(val1Ns2Dirty);
        // Clean entries should not trigger an update
        verify(mockDelegateState, times(0)).update(valCleanNs1);
        verify(mockDelegateState, times(0)).update(valCleanNs2);

        // --- Verification: Dirty entries should now be clean in L1 (or L2 if evicted by flush logic)
        // ---
        // We'll test one dirty entry (key1Ns1Dirty). After flush, it should be clean.
        // If we evict it from L1, it should go to L2 without writing to delegate again.

        cachingState.setCurrentNamespace(ns1);
        cachingKeyedStateBackend.setCurrentKey(key1Ns1Dirty);

        // To verify it's clean, evict it from L1.
        // It's currently in L1 for (ns1, key1Ns1Dirty).
        // Fill L1 with 2 other entries *for the same namespace ns1*
        cachingKeyedStateBackend.setCurrentKey("flushed_evictor1_ns1");
        when(mockDelegateState.value()).thenReturn("fe1");
        cachingState.value();

        cachingKeyedStateBackend.setCurrentKey("flushed_evictor2_ns1");
        when(mockDelegateState.value()).thenReturn("fe2");
        cachingState.value();

        // Now, key1Ns1Dirty (val1Ns1Dirty) should have been evicted from L1 to L2.
        // Since it was marked clean by the flush, this L1->L2 transition should NOT call
        // delegate.update() again.
        // The total calls to update(val1Ns1Dirty) should remain 1 (from the flush).
        verify(mockDelegateState, times(1)).update(val1Ns1Dirty);

        // Accessing it again should be an L2 hit (or L1 if L2 is small and it got re-promoted,
        // but crucially, no new delegate.update)
        cachingKeyedStateBackend.setCurrentKey(key1Ns1Dirty);
        // If it's in L2, delegate.value() should NOT be called.
        // If it was flushed correctly, it should be in the underlying store.
        when(mockDelegateState.value()).thenReturn(val1Ns1Dirty);
        assertEquals(
                val1Ns1Dirty,
                cachingState.value(),
                "Value should be available after flush and L1 eviction (from L2 or re-load)");
        // The number of times val1Ns1Dirty was requested from delegate.value() should be low (e.g., 1
        // if L2 also evicted it, or 0 if L2 hit)
        // This part is tricky to assert perfectly without inspecting cache states.
        // The key is that update(val1Ns1Dirty) was not called more than once.
    }

    @Test
    void testFlush_noDirtyEntries_doesNothing() throws Exception {
        // --- Setup: Populate L1/L2 with only clean entries ---
        // Namespace 1
        cachingState.setCurrentNamespace("ns_clean1");
        cachingKeyedStateBackend.setCurrentKey("key_clean1_ns1");
        when(mockDelegateState.value()).thenReturn("val_c1_n1");
        cachingState.value(); // L1 clean

        cachingKeyedStateBackend.setCurrentKey("key_clean2_ns1");
        when(mockDelegateState.value()).thenReturn("val_c2_n1");
        cachingState.value(); // L1 clean

        // Namespace 2
        cachingState.setCurrentNamespace("ns_clean2");
        cachingKeyedStateBackend.setCurrentKey("key_clean1_ns2");
        when(mockDelegateState.value()).thenReturn("val_c1_n2");
        cachingState.value(); // L1 clean

        // Reset to test defaults
        cachingState.setCurrentNamespace(testNamespace);
        cachingKeyedStateBackend.setCurrentKey(testKey);

        // --- Action: Flush ---
        // cachingState.flush(); // CachingInternalValueState does not have a flush() method

        // --- Verification: No delegate update calls ---
        verify(mockDelegateState, times(0)).update(org.mockito.ArgumentMatchers.anyString());
    }

    // --- clear() Tests ---

    @Test
    void testClear_removesFromL1L2AndDelegate() throws Exception {
        // --- Setup: Get testKey into L1 (and L2 to test L2 removal) ---
        // 1. testKey -> testValue1 (L1)
        cachingKeyedStateBackend.setCurrentKey(testKey);
        when(mockDelegateState.value()).thenReturn(testValue1);
        cachingState.value();
        verify(mockDelegateState, times(1)).value(); // Initial load

        // 2. Evict testKey to L2 by adding two more keys to L1 (for the same namespace)
        String fillerKey1 = "clear_filler1";
        cachingKeyedStateBackend.setCurrentKey(fillerKey1);
        when(mockDelegateState.value()).thenReturn("fv1_clear");
        cachingState.value();
        verify(mockDelegateState, times(2)).value();

        String fillerKey2 = "clear_filler2";
        cachingKeyedStateBackend.setCurrentKey(fillerKey2);
        when(mockDelegateState.value()).thenReturn("fv2_clear");
        cachingState.value(); // testKey (testValue1) is now in L2
        verify(mockDelegateState, times(3)).value();
        // L1: (fillerKey1, fv1_clear), (fillerKey2, fv2_clear)
        // L2: (testKey, testValue1)

        // --- Action: Clear the state for testKey ---
        cachingKeyedStateBackend.setCurrentKey(testKey);
        cachingState.clear();

        // --- Verification ---
        // 1. Delegate's clear method should be called
        verify(mockDelegateState, times(1)).clear();

        // 2. Accessing the value for testKey should now return null (or whatever delegate returns
        // after clear)
        //    and it should be a cache miss (L1 and L2).
        when(mockDelegateState.value())
                .thenReturn(null); // Simulate delegate returns null after clear
        assertEquals(
                null,
                cachingState.value(),
                "Value should be null after clear (cache miss, from delegate)");
        // Delegate.value() called once for initial load, once for filler1, once for filler2,
        // and once now after clear. Total = 4.
        verify(mockDelegateState, times(4)).value();

        // 3. To be very sure L1/L2 are clear for testKey:
        //    If we try to evict other things from L1, testKey should not reappear from L2.
        //    This is implicitly tested by the fact that we had to mock delegateState.value() to
        // return null.
        //    If it were still in L2, the previous assertEquals would have gotten testValue1.
    }

    // --- Other InternalKvState methods ---
    @Test
    void testSerializersAreDelegated() {
        // getKeySerializer(), getNamespaceSerializer(), getValueSerializer() are already mocked in
        // setUp()
        // to return mockKeySerializer, mockNamespaceSerializer, mockValueSerializer respectively
        // from mockDelegateState.

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
        String newNamespace = "newTestNamespace";

        // Action
        cachingState.setCurrentNamespace(newNamespace);

        // Verification
        // 1. Delegate should have its namespace set.
        verify(mockDelegateState, times(1)).setCurrentNamespace(newNamespace);
        // Note: cachingState.setCurrentNamespace() in setUp also calls
        // delegate.setCurrentNamespace().
        // So, if we want to check for *this specific call only*, we might need to reset the mock or
        // verify(..., times(2))
        // if setUp is considered. For simplicity, we check it was called with newNamespace.
        // Let's refine: setUp calls it once. This test calls it again.
        // Total calls with newNamespace should be 1. Total calls with testNamespace (from setUp) is
        // 1.
        // More robust: verify it was called with newNamespace, and check cachingState's own
        // getCurrentNamespace.

        assertEquals(
                newNamespace,
                cachingState.getCurrentNamespace(),
                "Caching state should report the new namespace.");

        // To be very specific about the delegate call for *this* invocation:
        // We could use an ArgumentCaptor or ensure that the number of times it's called with
        // testNamespace (from setUp)
        // is 1, and with newNamespace is 1.
        // Or, more simply, assume that if cachingState.getCurrentNamespace() is correct, and we
        // verify *any* call to
        // delegate.setCurrentNamespace(newNamespace), it implies correct delegation for this call.
        // The current verify(mockDelegateState, times(1)).setCurrentNamespace(newNamespace) is fine
        // if we assume the mock object interactions are fresh or managed per test method by
        // MockitoExtension.
        // Let's make sure it's clear by verifying calls from setUp are separate.
        // In setUp: cachingState.setCurrentNamespace(testNamespace); ->
        // delegate.setCurrentNamespace(testNamespace)
        // In this test: cachingState.setCurrentNamespace(newNamespace); ->
        // delegate.setCurrentNamespace(newNamespace)
        verify(mockDelegateState, times(1)).setCurrentNamespace(testNamespace); // From setUp
        verify(mockDelegateState, times(1)).setCurrentNamespace(newNamespace); // From this test
    }

    @Test
    void testGetSerializedValueIsDelegated() throws Exception {
        byte[] keyAndNamespace = new byte[] {1, 2, 3};
        byte[] expectedSerializedValue = new byte[] {4, 5, 6};

        // Mock the delegate to return a specific serialized value
        when(mockDelegateState.getSerializedValue(
                        keyAndNamespace,
                        mockKeySerializer, // Already mocked in setUp
                        mockNamespaceSerializer, // Already mocked in setUp
                        mockValueSerializer // Already mocked in setUp
                        ))
                .thenReturn(expectedSerializedValue);

        // Action
        byte[] actualSerializedValue =
                cachingState.getSerializedValue(
                        keyAndNamespace,
                        mockKeySerializer,
                        mockNamespaceSerializer,
                        mockValueSerializer);

        // Verification
        // 1. The returned value should be what the delegate returned.
        org.junit.jupiter.api.Assertions.assertArrayEquals(
                expectedSerializedValue,
                actualSerializedValue,
                "Serialized value should be delegated.");

        // 2. Verify the delegate method was called exactly once with the correct parameters.
        verify(mockDelegateState, times(1))
                .getSerializedValue(
                        keyAndNamespace,
                        mockKeySerializer,
                        mockNamespaceSerializer,
                        mockValueSerializer);
    }

    @Test
    void testGetStateIncrementalVisitorIsDelegated() {
        int recommendedMaxNumberOfReturnedRecords = 100;
        @SuppressWarnings("unchecked")
        InternalKvState.StateIncrementalVisitor<String, String, String> mockVisitor =
                (InternalKvState.StateIncrementalVisitor<String, String, String>)
                        mock(InternalKvState.StateIncrementalVisitor.class);

        // Mock the delegate to return a specific visitor instance
        when(mockDelegateState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords))
                .thenReturn(mockVisitor);

        // Action
        InternalKvState.StateIncrementalVisitor<String, String, String> actualVisitor =
                cachingState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);

        // Verification
        // 1. The returned visitor should be the one from the delegate.
        assertEquals(mockVisitor, actualVisitor, "StateIncrementalVisitor should be delegated.");

        // 2. Verify the delegate method was called exactly once with the correct parameter.
        verify(mockDelegateState, times(1))
                .getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }
}
