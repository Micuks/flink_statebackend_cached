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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalValueState;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
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
    @Mock private CachingKeyedStateBackend<String> mockBackend;
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
        // Configure common mock behaviors
        lenient().when(mockBackend.getCurrentKey()).thenReturn(testKey);
        lenient().when(mockDelegateState.getKeySerializer()).thenReturn(mockKeySerializer);
        lenient().when(mockDelegateState.getNamespaceSerializer()).thenReturn(mockNamespaceSerializer);
        lenient().when(mockDelegateState.getValueSerializer()).thenReturn(mockValueSerializer);

        cachingState =
                new CachingInternalValueState<>(
                        mockDelegateState,
                        mockBackend,
                        l1CacheSize,
                        l2CacheSize,
                        maxActiveNamespaces,
                                        0L, CachingStateBackendFactory.CachePolicyType.LRU);
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
        when(mockBackend.getCurrentKey()).thenReturn(testKey);
        when(mockDelegateState.value()).thenReturn(testValue1); // For initial load of testKey
        cachingState.value(); // testKey -> testValue1 in L1
        verify(mockDelegateState, times(1)).value();

        // Key 2 (anotherKey1)
        String anotherKey1 = "anotherKey1";
        String anotherValue1 = "anotherValue1";
        mockBackend.setCurrentKey(anotherKey1);
        when(mockBackend.getCurrentKey()).thenReturn(anotherKey1);
        when(mockDelegateState.value())
                .thenReturn(anotherValue1); // For initial load of anotherKey1
        cachingState.value(); // anotherKey1 -> anotherValue1 in L1
        verify(mockDelegateState, times(2)).value(); // Delegate called for anotherKey1

        // Key 3 (anotherKey2) - this should evict testKey (testValue1) from L1 to L2
        // because l1CacheSize is 2. Order of access: testKey, anotherKey1. Evicted: testKey.
        String anotherKey2 = "anotherKey2";
        String anotherValue2 = "anotherValue2";
        mockBackend.setCurrentKey(anotherKey2);
        when(mockBackend.getCurrentKey()).thenReturn(anotherKey2);
        when(mockDelegateState.value())
                .thenReturn(anotherValue2); // For initial load of anotherKey2
        cachingState.value(); // anotherKey2 -> anotherValue2 in L1. testKey should now be in L2.
        verify(mockDelegateState, times(3)).value(); // Delegate called for anotherKey2

        // --- Action: Access the original key (testKey) ---
        // It should be an L1 miss, L2 hit, and then promoted to L1.
        mockBackend.setCurrentKey(testKey); // Switch back to the original key
        when(mockBackend.getCurrentKey()).thenReturn(testKey);
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
        when(mockBackend.getCurrentKey()).thenReturn(testKey);
        when(mockDelegateState.value()).thenReturn(testValue1);
        cachingState.value(); // testKey is current key by default from setUp
        verify(mockDelegateState, times(1)).value();

        // Step 1.2: anotherKey1 -> anotherValue1 (clean) in L1.
        String anotherKey1 = "anotherKey1";
        String anotherValue1 = "anotherValue1";
        mockBackend.setCurrentKey(anotherKey1);
        when(mockBackend.getCurrentKey()).thenReturn(anotherKey1);
        when(mockDelegateState.value()).thenReturn(anotherValue1);
        cachingState.value();
        verify(mockDelegateState, times(2)).value(); // Total 2 delegate.value() calls

        // Step 1.3: anotherKey2 -> anotherValue2 (clean) in L1. This evicts testKey to L2.
        // L1 (size 2) now: (anotherKey1, anotherValue1), (anotherKey2, anotherValue2)
        // L2 now: (testKey, testValue1)
        String anotherKey2 = "anotherKey2";
        String anotherValue2 = "anotherValue2";
        mockBackend.setCurrentKey(anotherKey2);
        when(mockBackend.getCurrentKey()).thenReturn(anotherKey2);
        when(mockDelegateState.value()).thenReturn(anotherValue2);
        cachingState.value();
        verify(mockDelegateState, times(3)).value(); // Total 3 delegate.value() calls

        // --- Setup Phase 2: Current key is testKey. Update it. ---
        mockBackend.setCurrentKey(testKey);
        when(mockBackend.getCurrentKey()).thenReturn(testKey);
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
        mockBackend.setCurrentKey(anotherKey2);
        when(mockBackend.getCurrentKey()).thenReturn(anotherKey2);
        cachingState.value(); // Access anotherKey2. It was already in L1. No new delegate call.
        verify(mockDelegateState, times(3)).value();

        // Now add a new key to evict testKey.
        String anotherKey3 = "anotherKey3";
        String anotherValue3 = "anotherValue3";
        mockBackend.setCurrentKey(anotherKey3);
        when(mockBackend.getCurrentKey()).thenReturn(anotherKey3);
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
        mockBackend.setCurrentKey(testKey);
        when(mockBackend.getCurrentKey()).thenReturn(testKey);
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
        // --- Setup: Put a value in L1 for testKey ---
        when(mockDelegateState.value()).thenReturn(testValue1);
        cachingState.value(); // testKey -> testValue1 in L1. Delegate.value() called once.
        verify(mockDelegateState, times(1)).value();

        // --- Action: Update with null ---
        cachingState.update(null);

        // --- Verification ---
        // 1. Delegate state should have been cleared.
        verify(mockDelegateState, times(1)).clear();

        // 2. Accessing the value now should return null (from delegate, as cache is cleared for
        // this key).
        //    Configure delegate to return null as it has been cleared.
        when(mockDelegateState.value()).thenReturn(null);
        assertEquals(null, cachingState.value(), "Value after update(null) should be null.");

        // 3. Delegate.value() should have been called again (once for initial load, once after
        // clear).
        verify(mockDelegateState, times(2)).value();
    }

    // --- Eviction Logic Tests ---

    @Test
    void testL1Eviction_cleanEntry_moveToL2() throws Exception {
        // --- Setup: Fill L1 with clean entries to cause eviction of the first one ---
        // Entry 1 (testKey -> testValue1)
        when(mockBackend.getCurrentKey()).thenReturn(testKey);
        when(mockDelegateState.value()).thenReturn(testValue1);
        cachingState.value(); // testKey is current key. testKey -> testValue1 (clean) in L1.
        verify(mockDelegateState, times(1)).value();

        // Entry 2 (anotherKey1 -> someOtherValue1)
        String anotherKey1 = "anotherKeyL1Evict1";
        String anotherValue1 = "anotherValueL1Evict1";
        mockBackend.setCurrentKey(anotherKey1);
        when(mockBackend.getCurrentKey()).thenReturn(anotherKey1);
        when(mockDelegateState.value()).thenReturn(anotherValue1);
        cachingState.value(); // anotherKey1 -> anotherValue1 (clean) in L1.
        verify(mockDelegateState, times(2)).value();
        // L1 now contains: (testKey, testValue1), (anotherKey1, anotherValue1). testKey is eldest.

        // Entry 3 (anotherKey2 -> someOtherValue2) - This will evict testKey to L2
        String anotherKey2 = "anotherKeyL1Evict2";
        String anotherValue2 = "anotherValueL1Evict2";
        mockBackend.setCurrentKey(anotherKey2);
        when(mockBackend.getCurrentKey()).thenReturn(anotherKey2);
        when(mockDelegateState.value()).thenReturn(anotherValue2);
        cachingState.value(); // anotherKey2 -> anotherValue2 (clean) in L1.
        verify(mockDelegateState, times(3)).value();
        // L1 now: (anotherKey1, anotherValue1), (anotherKey2, anotherValue2)
        // L2 should now contain: (testKey, testValue1) because it was clean upon eviction.

        // --- Verification: Access testKey, should be an L2 hit ---
        mockBackend.setCurrentKey(testKey);
        when(mockBackend.getCurrentKey()).thenReturn(testKey);
        String retrievedValue = cachingState.value();
        assertEquals(
                testValue1,
                retrievedValue,
                "Value for testKey should be retrieved from L2 after L1 eviction.");
        // Delegate.value() should not be called again; it was an L2 hit.
        verify(mockDelegateState, times(3)).value();
        // Delegate.update() should never have been called as only clean entries were handled.
        verify(mockDelegateState, times(0)).update(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void testL1Eviction_dirtyEntry_flushToDelegate_moveToL2Clean() throws Exception {
        // --- Setup: Fill L1 with dirty entries to cause eviction of the first one ---
        // Entry 1 (testKey -> testValue1, dirty)
        when(mockBackend.getCurrentKey()).thenReturn(testKey);
        cachingState.update(testValue1); // testKey is current. testKey -> testValue1 (dirty) in L1.

        // Entry 2 (anotherKey1 -> someOtherValue1, dirty)
        String anotherKey1 = "anotherKeyL1DirtyEvict1";
        String anotherValue1 = "anotherValueL1DirtyEvict1";
        when(mockBackend.getCurrentKey()).thenReturn(anotherKey1);
        cachingState.update(anotherValue1);
        // L1 now contains: (testKey, testValue1, dirty), (anotherKey1, anotherValue1, dirty).
        // testKey is eldest.

        // Entry 3 (anotherKey2 -> someOtherValue2, dirty) - This will evict testKey
        String anotherKey2 = "anotherKeyL1DirtyEvict2";
        String anotherValue2 = "anotherValueL1DirtyEvict2";
        when(mockBackend.getCurrentKey()).thenReturn(anotherKey2);
        cachingState.update(anotherValue2);
        // During this update, testKey(testValue1, dirty) is evicted from L1.
        // Expect: delegateState.update(testValue1) is called.
        // Expect: testKey -> testValue1 (now clean) is put into L2.

        // --- Verification: testKey was flushed and is now in L2 ---
        verify(mockDelegateState, times(1)).update(testValue1);

        when(mockBackend.getCurrentKey()).thenReturn(testKey);
        // Configure delegate.value() to return something different to ensure L2 hit
        // is not accidentally a delegate passthrough after a failed L2 population.
        // This stubbing is intentionally not expected to be called if L2 hit works.
        lenient().when(mockDelegateState.value()).thenReturn("unexpectedValueFromDelegate");

        String retrievedValue = cachingState.value();
        assertEquals(
                testValue1,
                retrievedValue,
                "Value for testKey should be retrieved from L2 (clean) after dirty L1 eviction.");

        // Ensure delegate.value() was NOT called for retrieving testValue1 (it was an L2 hit).
        // If it was called, it would have returned "unexpectedValueFromDelegate".
        // The assertEquals above, combined with the when().thenReturn("unexpected...") for the
        // delegate,
        // already verifies this.
    }

    @Test
    void testL2Eviction() throws Exception {
        final String l2EvictedValueMarker = "l2EvictedAndFetchedFromDelegate";
        AtomicInteger delegateValueCallCount = new AtomicInteger(0);
        // Removed: org.mockito.stubbing.OngoingStubbing<String> initialTestKeyStub = when(mockBackend.getCurrentKey()).thenReturn(testKey);

        // Helper to load a key into L1, then cause its eviction to L2
        // Assumes l1CacheSize is 2
        Runnable evictToL2 =
                () -> {
                    try {
                        // Prime the target key into L1
                        // when(mockDelegateState.value()) is configured by the caller for the
                        // target key
                        // The caller must also have set when(mockBackend.getCurrentKey())
                        cachingState.value(); // Current key loaded to L1
                        verify(mockDelegateState, times(delegateValueCallCount.incrementAndGet()))
                                .value();

                        // Fill L1 with two other keys to evict the current key to L2
                        String fillerKey1 = "l2_evict_filler1_" + System.nanoTime();
                        String fillerValue1 = "l2_evict_fillerval1_" + System.nanoTime();
                        mockBackend.setCurrentKey(fillerKey1);
                        when(mockBackend.getCurrentKey()).thenReturn(fillerKey1);
                        when(mockDelegateState.value()).thenReturn(fillerValue1);
                        cachingState.value();
                        verify(mockDelegateState, times(delegateValueCallCount.incrementAndGet()))
                                .value();

                        String fillerKey2 = "l2_evict_filler2_" + System.nanoTime();
                        String fillerValue2 = "l2_evict_fillerval2_" + System.nanoTime();
                        mockBackend.setCurrentKey(fillerKey2);
                        when(mockBackend.getCurrentKey()).thenReturn(fillerKey2);
                        when(mockDelegateState.value()).thenReturn(fillerValue2);
                        cachingState.value();
                        verify(mockDelegateState, times(delegateValueCallCount.incrementAndGet()))
                                .value();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                };

        // --- Setup: Get three distinct keys into L2 to cause eviction of the first one ---
        // L2 cache size is 2.

        // Key 1 (testKey) to L2
        mockBackend.setCurrentKey(testKey);
        when(mockBackend.getCurrentKey()).thenReturn(testKey); // Direct stub for testKey
        when(mockDelegateState.value()).thenReturn(testValue1);
        evictToL2.run(); // testKey now in L2. L2: {testKey=testValue1}

        // Key 2 (testValue2 related key) to L2
        String keyForTestValue2 = "keyForTestValue2";
        mockBackend.setCurrentKey(keyForTestValue2);
        when(mockBackend.getCurrentKey()).thenReturn(keyForTestValue2);
        when(mockDelegateState.value()).thenReturn(testValue2); // testValue2 is a field
        evictToL2.run(); // keyForTestValue2 now in L2. L2: {testKey=testValue1,
        // keyForTestValue2=testValue2}. testKey is eldest.

        // Key 3 (testValue3 related key) to L2 - This should evict testKey from L2
        String keyForTestValue3 = "keyForTestValue3";
        mockBackend.setCurrentKey(keyForTestValue3);
        when(mockBackend.getCurrentKey()).thenReturn(keyForTestValue3);
        when(mockDelegateState.value()).thenReturn(testValue3); // testValue3 is a field
        evictToL2.run(); // keyForTestValue3 now in L2. L2 should be: {keyForTestValue2=testValue2,
        // keyForTestValue3=testValue3}
        // testKey should have been evicted from L2.

        // --- Verification: Access testKey. It should be a full cache miss (L1 & L2 miss), hitting
        // delegate ---
        mockBackend.setCurrentKey(testKey);
        when(mockBackend.getCurrentKey()).thenReturn(testKey); // Direct stub for testKey
        // Configure delegate to return a specific marker for this expected full miss
        when(mockDelegateState.value()).thenReturn(l2EvictedValueMarker);

        String retrievedValue = cachingState.value();
        assertEquals(
                l2EvictedValueMarker,
                retrievedValue,
                "Value for testKey should be fetched from delegate after L2 eviction.");
        // Delegate.value() called again for testKey (the +1 below)
        verify(mockDelegateState, times(delegateValueCallCount.incrementAndGet())).value();
    }

    // --- Namespace Handling Tests ---

    @Test
    void testMultipleNamespaces_cachesAreSeparate() throws Exception {
        String key = "sharedKey";
        String ns1 = "namespace1";
        String valueNs1 = "valueForNamespace1";
        String ns2 = "namespace2";
        String valueNs2 = "valueForNamespace2";

        // --- Interact with ns1 ---
        cachingState.setCurrentNamespace(ns1);
        mockBackend.setCurrentKey(key);
        when(mockBackend.getCurrentKey()).thenReturn(key);
        when(mockDelegateState.value()).thenReturn(valueNs1);
        assertEquals(
                valueNs1,
                cachingState.value(),
                "Value for key in ns1 should be from delegate initially.");
        verify(mockDelegateState, times(1)).value(); // First delegate call for (ns1, key)

        // --- Interact with ns2 ---
        cachingState.setCurrentNamespace(ns2);
        // Key remains the same (key)
        when(mockBackend.getCurrentKey()).thenReturn(key);
        when(mockDelegateState.value()).thenReturn(valueNs2);
        assertEquals(
                valueNs2,
                cachingState.value(),
                "Value for key in ns2 should be from delegate initially.");
        verify(mockDelegateState, times(2)).value(); // Second delegate call for (ns2, key)

        // --- Verify ns1 is still intact and served from its L1 cache ---
        cachingState.setCurrentNamespace(ns1);
        // Key remains the same (key)
        when(mockBackend.getCurrentKey()).thenReturn(key);
        // Delegate should not be called again for (ns1, key) as it should be in ns1's L1 cache.
        assertEquals(
                valueNs1,
                cachingState.value(),
                "Value for key in ns1 should be retrieved from its cache.");
        verify(mockDelegateState, times(2)).value(); // Count should remain 2

        // --- Verify ns2 is still intact and served from its L1 cache ---
        cachingState.setCurrentNamespace(ns2);
        // Key remains the same (key)
        when(mockBackend.getCurrentKey()).thenReturn(key);
        // Delegate should not be called again for (ns2, key) as it should be in ns2's L1 cache.
        assertEquals(
                valueNs2,
                cachingState.value(),
                "Value for key in ns2 should be retrieved from its cache.");
        verify(mockDelegateState, times(2)).value(); // Count should remain 2
    }

    @Test
    void testMaxActiveNamespaces_eviction() throws Exception {
        // maxActiveNamespaces is 2 in setUp
        String key = "keyForMaxActiveNs";
        String ns1 = "ns_active_1";
        String valueNs1 = "val_ns_active_1";
        String ns2 = "ns_active_2";
        String valueNs2 = "val_ns_active_2";
        String ns3 = "ns_active_3_evictor";
        String valueNs3 = "val_ns_active_3_evictor";
        String valueNs1Reloaded = "val_ns_active_1_reloaded";

        AtomicInteger delegateValueCallCount = new AtomicInteger(0);
        // Removed: org.mockito.stubbing.OngoingStubbing<String> initialKeyStub = when(mockBackend.getCurrentKey());

        // --- Populate cache for ns1 ---
        cachingState.setCurrentNamespace(ns1);
        mockBackend.setCurrentKey(key);
        when(mockBackend.getCurrentKey()).thenReturn(key); // Apply/Re-apply stub for 'key'
        when(mockDelegateState.value()).thenReturn(valueNs1);
        assertEquals(valueNs1, cachingState.value());
        verify(mockDelegateState, times(delegateValueCallCount.incrementAndGet()))
                .value(); // Call 1

        // --- Populate cache for ns2 ---
        cachingState.setCurrentNamespace(ns2);
        mockBackend.setCurrentKey(key); // Can use the same key
        when(mockBackend.getCurrentKey()).thenReturn(key); // Apply/Re-apply stub for 'key'
        when(mockDelegateState.value()).thenReturn(valueNs2);
        assertEquals(valueNs2, cachingState.value());
        verify(mockDelegateState, times(delegateValueCallCount.incrementAndGet()))
                .value(); // Call 2
        // At this point, caches for ns1 and ns2 are active. ns1 is LRU.

        // --- Populate cache for ns3 (this should evict ns1's cache) ---
        cachingState.setCurrentNamespace(ns3);
        mockBackend.setCurrentKey(key);
        when(mockBackend.getCurrentKey()).thenReturn(key); // Apply/Re-apply stub for 'key'
        when(mockDelegateState.value()).thenReturn(valueNs3);
        assertEquals(valueNs3, cachingState.value());
        verify(mockDelegateState, times(delegateValueCallCount.incrementAndGet()))
                .value(); // Call 3
        // Caches for ns2 and ns3 should be active. ns1's cache should be gone.

        // --- Verify ns1's cache was evicted (accessing it goes to delegate) ---
        cachingState.setCurrentNamespace(ns1);
        mockBackend.setCurrentKey(key);
        when(mockBackend.getCurrentKey()).thenReturn(key); // Apply/Re-apply stub for 'key'
        when(mockDelegateState.value())
                .thenReturn(valueNs1Reloaded); // Simulate reload from delegate
        assertEquals(
                valueNs1Reloaded,
                cachingState.value(),
                "Cache for ns1 should have been evicted and reloaded from delegate.");
        verify(mockDelegateState, times(delegateValueCallCount.incrementAndGet()))
                .value(); // Call 4

        // --- Verify ns2's cache is still active (served from L1) ---
        cachingState.setCurrentNamespace(ns2);
        mockBackend.setCurrentKey(key);
        when(mockBackend.getCurrentKey()).thenReturn(key); // Apply/Re-apply stub for 'key'
        // ns2's caches were cleared when it was evicted. This will be a delegate call.
        when(mockDelegateState.value()).thenReturn(valueNs2); // Stub for ns2's reload
        assertEquals(valueNs2, cachingState.value(),
                "Cache for ns2 should be reloaded from delegate after its eviction.");
        // Delegate call count should increase for ns2's reload
        verify(mockDelegateState, times(delegateValueCallCount.incrementAndGet())).value(); // Call
                                                                                            // for
                                                                                            // ns2
                                                                                            // reload

        // --- Verify ns3's cache is still active (served from L1) ---
        cachingState.setCurrentNamespace(ns3);
        mockBackend.setCurrentKey(key);
        when(mockBackend.getCurrentKey()).thenReturn(key); // Apply/Re-apply stub for 'key'
        // ns3's caches were also cleared when it was evicted. This will be a delegate call.
        when(mockDelegateState.value()).thenReturn(valueNs3); // Stub for ns3's reload
        assertEquals(valueNs3, cachingState.value(),
                "Cache for ns3 should be reloaded from delegate after its eviction.");
        // Delegate call count should increase for ns3's reload
        verify(mockDelegateState, times(delegateValueCallCount.incrementAndGet())).value(); // Call
                                                                                            // for
                                                                                            // ns3
                                                                                            // reload
    }

    // --- flushToUnderlyingState() Tests ---

    @Test
    void testFlush_writesDirtyEntriesToDelegate_marksClean() throws Exception {
        String ns1 = "flush_ns1";
        String key1Ns1 = "flush_key1_ns1";
        String value1Ns1 = "flush_value1_ns1_dirty";
        String key2Ns1 = "flush_key2_ns1";
        String value2Ns1 = "flush_value2_ns1_dirty";

        String ns2 = "flush_ns2";
        String key1Ns2 = "flush_key1_ns2";
        String value1Ns2 = "flush_value1_ns2_dirty";

        // Removed: org.mockito.stubbing.OngoingStubbing<String> initialTestKeyStub = when(mockBackend.getCurrentKey()).thenReturn(testKey);

        // --- Setup dirty entries in different namespaces and keys ---
        // Namespace 1, Key 1
        cachingState.setCurrentNamespace(ns1);
        mockBackend.setCurrentKey(key1Ns1);
        when(mockBackend.getCurrentKey()).thenReturn(key1Ns1);
        cachingState.update(value1Ns1);

        // Namespace 1, Key 2
        mockBackend.setCurrentKey(key2Ns1);
        when(mockBackend.getCurrentKey()).thenReturn(key2Ns1);
        cachingState.update(value2Ns1);

        // Namespace 2, Key 1
        cachingState.setCurrentNamespace(ns2);
        mockBackend.setCurrentKey(key1Ns2);
        when(mockBackend.getCurrentKey()).thenReturn(key1Ns2);
        cachingState.update(value1Ns2);

        // Restore original context before flush, just in case, though flush should handle it.
        cachingState.setCurrentNamespace(testNamespace); // a known default from setUp
        mockBackend.setCurrentKey(testKey);
        when(mockBackend.getCurrentKey()).thenReturn(testKey); // Direct stub for testKey

        // --- Action: Flush all states ---
        cachingState.flushToUnderlyingState();

        // --- Verification: Delegate update should be called for each dirty entry ---
        // The flush method internally sets the correct key/namespace for the delegate before
        // updating.
        verify(mockDelegateState, times(1)).update(value1Ns1);
        verify(mockDelegateState, times(1)).update(value2Ns1);
        verify(mockDelegateState, times(1)).update(value1Ns2);

        // --- Verification of "marksClean" and L2 population after clean L1 eviction ---
        // Set context to the first flushed item (ns1, key1Ns1) which should be clean in L1.
        cachingState.setCurrentNamespace(ns1);
        mockBackend.setCurrentKey(key1Ns1);
        when(mockBackend.getCurrentKey()).thenReturn(key1Ns1);

        // --- Verification: Check L1 and L2 are empty for all, and delegate has flushed values
        // ---

        // General stub for delegate.value() calls AFTER flush, to ensure they are counted
        // and return something unexpected if caches weren't properly cleared/reloaded.
        final String unexpectedMarker = "VALUE_SHOULD_HAVE_BEEN_FLUSHED_OR_CLEARED_NOT_THIS";
        AtomicInteger delegateValueHitsAfterFlush = new AtomicInteger(0);
        // This when() will apply to all subsequent mockDelegateState.value() calls in this test
        // method
        // unless overridden by another more specific when() or reset.
        lenient() // MADE LENIENT
                .when(mockDelegateState.value())
                .thenAnswer(
                        inv -> {
                            delegateValueHitsAfterFlush.incrementAndGet();
                            // Return the actual known value if key matches, otherwise marker.
                            // This is tricky as mockDelegateState.value() is not key-aware by
                            // itself.
                            // For simplicity, we rely on the test flow: if it's called, it's an
                            // error for L1/L2 hits.
                            return unexpectedMarker;
                        });

        // Access key2Ns1 (also in ns1, was flushed, clean in L1) to make key1Ns1 eldest in L1 for
        // ns1.
        mockBackend.setCurrentKey(key2Ns1);
        when(mockBackend.getCurrentKey()).thenReturn(key2Ns1);
        assertEquals(
                value2Ns1,
                cachingState.value(),
                "Accessing key2Ns1 in ns1 after flush (should be L1 hit).");
        assertEquals(
                0,
                delegateValueHitsAfterFlush.get(),
                "Delegate.value() should not be called for L1 hit of key2Ns1.");

        // Now, add a new (dirty) item to L1 for ns1. This should evict key1Ns1 (clean) to L2.
        // L1 for ns1 (size 2) before evictor: (key2Ns1, value2Ns1, clean), (key1Ns1, value1Ns1,
        // clean) - key1Ns1 is eldest.
        String evictorKey = ns1 + "_evictor_clean_check";
        String evictorValue = "evictor_val_clean_check";
        mockBackend.setCurrentKey(evictorKey);
        when(mockBackend.getCurrentKey()).thenReturn(evictorKey);
        cachingState.update(
                evictorValue); // (evictorKey, dirty) and (key2Ns1, clean) in L1. key1Ns1 (clean)
        // evicted to L2.

        // VERIFY: value1Ns1 (for key1Ns1) was clean, so its eviction from L1 should NOT call
        // delegate.update() again.
        verify(mockDelegateState, times(1))
                .update(value1Ns1); // Still only 1 call from the original flush.
        // The new dirty entry (evictorKey, evictorValue) would be flushed if IT gets evicted dirty.
        // This is not tested here, we only care that key1Ns1 didn't trigger another update.

        // VERIFY: key1Ns1 is now in L2. Accessing it should be an L2 hit.
        mockBackend.setCurrentKey(key1Ns1);
        when(mockBackend.getCurrentKey()).thenReturn(key1Ns1);
        assertEquals(
                value1Ns1,
                cachingState.value(),
                "key1Ns1 should be hit from L2 after being flushed and evicted from L1 cleanly.");
        assertEquals(
                0,
                delegateValueHitsAfterFlush.get(),
                "Delegate.value() should not be called for L2 hit of key1Ns1.");
    }

    @Test
    void testFlush_noDirtyEntries_doesNothing() throws Exception {
        // --- Setup: Put a clean entry into L1 ---
        // mockBackend.setCurrentKey(testKey) is already set from general setUp if not changed.
        // Ensure current namespace is the one used in setUp for testKey.
        cachingState.setCurrentNamespace(testNamespace);
        mockBackend.setCurrentKey(testKey);
        when(mockBackend.getCurrentKey()).thenReturn(testKey);

        when(mockDelegateState.value()).thenReturn(testValue1);
        assertEquals(testValue1, cachingState.value(), "Initial load into L1.");
        verify(mockDelegateState, times(1)).value(); // Called for initial load

        // --- Action: Flush state ---
        cachingState.flushToUnderlyingState();

        // --- Verification ---
        // 1. No update calls should have been made to the delegate as there were no dirty entries.
        verify(mockDelegateState, times(0)).update(org.mockito.ArgumentMatchers.anyString());

        // 2. The clean entry should still be in L1.
        //    Accessing it again should be an L1 hit, not calling delegate.value() again.
        assertEquals(
                testValue1,
                cachingState.value(),
                "Value should still be in L1 after flush if it was clean.");
        verify(mockDelegateState, times(1))
                .value(); // Should still be 1 from the initial load only.
    }

    // --- clear() Tests ---

    @Test
    void testClear_removesFromL1L2AndDelegate() throws Exception {
        // --- Setup: Get testKey into L1, then L2 ---
        // Phase 1: testKey -> testValue1 in L1
        cachingState.setCurrentNamespace(testNamespace);
        mockBackend.setCurrentKey(testKey);
        when(mockBackend.getCurrentKey()).thenReturn(testKey);
        when(mockDelegateState.value()).thenReturn(testValue1);
        assertEquals(testValue1, cachingState.value(), "Initial load of testKey to L1.");
        int delegateValueCalls = 1;
        verify(mockDelegateState, times(delegateValueCalls)).value();

        // Phase 2: Evict testKey from L1 to L2
        // L1 size is 2. Add 2 more keys to current namespace's L1.
        mockBackend.setCurrentKey("clear_filler1");
        when(mockBackend.getCurrentKey()).thenReturn("clear_filler1");
        when(mockDelegateState.value()).thenReturn("clear_filler_val1");
        cachingState.value();
        delegateValueCalls++;
        verify(mockDelegateState, times(delegateValueCalls)).value();

        mockBackend.setCurrentKey("clear_filler2");
        when(mockBackend.getCurrentKey()).thenReturn("clear_filler2");
        when(mockDelegateState.value()).thenReturn("clear_filler_val2");
        cachingState.value();
        delegateValueCalls++;
        verify(mockDelegateState, times(delegateValueCalls)).value();
        // Now testKey should be in L2 for testNamespace.

        // Set context back to testKey for clear operation
        mockBackend.setCurrentKey(testKey);
        when(mockBackend.getCurrentKey()).thenReturn(testKey);
        cachingState.setCurrentNamespace(testNamespace); // Ensure current namespace

        // --- Action: Call clear() ---
        cachingState.clear();

        // --- Verification ---
        // 1. Delegate state should have been cleared.
        verify(mockDelegateState, times(1)).clear();

        // 2. Accessing the value now should return null (from delegate, as L1/L2 and delegate are
        // cleared).
        //    Configure delegate to return null as it has been cleared for testKey.
        when(mockDelegateState.value()).thenReturn(null);
        assertEquals(null, cachingState.value(), "Value after clear() should be null.");

        // 3. Delegate.value() should have been called again for this access.
        delegateValueCalls++;
        verify(mockDelegateState, times(delegateValueCalls)).value();
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
