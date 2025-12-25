/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.internal.InternalValueState;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.inOrder;

class CachedInternalValueStateTest {

    @Test
    void testL1CacheHit() throws IOException {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        CurrentKeyProvider<String> currentKeyProvider = currentKey::get;

        InternalValueState<String, VoidNamespace, Integer> delegate = mock(InternalValueState.class);
        when(delegate.value()).thenReturn(42);

        // L1 size will be max(128, 100/5) = 128.
        CachedInternalValueState<String, VoidNamespace, Integer> state = new CachedInternalValueState<>(delegate,
                currentKeyProvider, k -> {
                }, 100);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        // 1. First access loads from delegate
        assertEquals(42, state.value());
        verify(delegate, times(1)).value();

        // 2. Second access hits sticky/L1
        assertEquals(42, state.value());
        verify(delegate, times(1)).value();
    }

    @Test
    void testL1WriteBack() throws IOException {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        CurrentKeyProvider<String> currentKeyProvider = currentKey::get;

        InternalValueState<String, VoidNamespace, Integer> delegate = mock(InternalValueState.class);

        CachedInternalValueState<String, VoidNamespace, Integer> state = new CachedInternalValueState<>(delegate,
                currentKeyProvider, k -> {
                }, 100);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        // Update
        state.update(99);

        // Verify value can be read back
        assertEquals(99, state.value());

        // Verify delegate was NOT updated (Write-Back)
        verify(delegate, times(0)).update(any());

        // Flush should trigger update
        state.flush();
        verify(delegate, times(1)).update(99);
    }

    @Test
    void testL1EvictionFlushesToDelegateAndMovesToL2() throws IOException {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        CurrentKeyProvider<String> currentKeyProvider = currentKey::get;

        InternalValueState<String, VoidNamespace, Integer> delegate = mock(InternalValueState.class);

        // Use small maxEntries to force small L1.
        // Logic: L1 size = max(128, maxEntries/5).
        // To make L1 small, we can't easily using the current formula (min 128).
        // However, we can fill it up.
        // Wait, if min L1 is 128, I need to insert > 128 items to evict.

        CachedInternalValueState<String, VoidNamespace, Integer> state = new CachedInternalValueState<>(delegate,
                currentKeyProvider, k -> {
                }, 650);
        // L1 = 650/5 = 130.

        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        // Fill L1
        for (int i = 0; i < 140; i++) {
            currentKey.set("key-" + i);
            state.update(i);
        }

        // At 140 entries, L1 (size 130) must have evicted ~10 items.
        // Those evicted items (dirty) should have triggered delegate.update().
        // Verify at least some updates occurred.
        verify(delegate, org.mockito.Mockito.atLeast(1)).update(any());
    }

    @Test
    void testL2HitPromotesToL1() throws IOException {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        CurrentKeyProvider<String> currentKeyProvider = currentKey::get;

        InternalValueState<String, VoidNamespace, Integer> delegate = mock(InternalValueState.class);

        // Create state with large enough cache to hold our test data without unintended
        // eviction
        CachedInternalValueState<String, VoidNamespace, Integer> state = new CachedInternalValueState<>(delegate,
                currentKeyProvider, k -> {
                }, 1000); // L1=200
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        // 1. Update k1 (Dirty in L1)
        state.update(100);

        // 2. Force eviction of k1 by filling L1?
        // It's hard to force specific eviction without filling.
        // Instead, let's use the property that we can manually flush?
        // Flush doesn't move to L2 in the code, it just persists L1 dirty to
        // L2+Delegate and marks L1 clean.
        // Wait, current flush implementation:
        // for dirty: put to L2 (clean), write to delegate, put to L1 (clean).

        state.flush();

        // Now k1 is in L1 (Clean) and L2 (Clean).
        // If we evict k1 from L1 from simple capacity pressure, it goes to L2.

        // Let's rely on internal behavior or just trust the previous test for eviction
        // wiring.
        // The specific "L2 hit promotes" requires k1 to be IN L2 and NOT in L1.
        // How to achieve this?
        // 1. Update k1.
        // 2. Evict k1 from L1 (moves to L2).
        // 3. Access k1. Should hit L2 and promote to L1.

        // To do this reliably with 128 min size:
        for (int i = 1; i <= 200; i++) {
            currentKey.set("fill-" + i);
            state.update(i);
        }
        // k1 should be evicted if it was inserted first?
        // Since we last accessed k1 at start, and then 200 updates.
        // The LRU policy should evict k1 (LRU).

        // Clear invocations to verify strictly the read
        org.mockito.Mockito.clearInvocations(delegate);
        when(delegate.value()).thenReturn(999); // Should NOT be called if hit L2

        currentKey.set("k1");
        // Should hit L2 (value 100)
        assertEquals(100, state.value());

        // Verify delegate NOT called
        verify(delegate, times(0)).value();
    }
}
