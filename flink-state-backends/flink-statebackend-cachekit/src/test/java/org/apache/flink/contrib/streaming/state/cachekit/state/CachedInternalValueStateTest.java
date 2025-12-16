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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CachedInternalValueStateTest {

    @Test
    void cachesValuePerKeyWithLruEviction() throws IOException {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        CurrentKeyProvider<String> currentKeyProvider = currentKey::get;

        AtomicInteger delegateValueCalls = new AtomicInteger();
        @SuppressWarnings("unchecked")
        InternalValueState<String, VoidNamespace, Integer> delegate = mock(InternalValueState.class);

        when(delegate.value())
                .thenAnswer(
                        invocation -> {
                            delegateValueCalls.incrementAndGet();
                            return 42;
                        });
        doNothing().when(delegate).update(any());
        doNothing().when(delegate).setCurrentNamespace(any());

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(delegate, currentKeyProvider, 2);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertEquals(42, state.value());
        assertEquals(42, state.value());
        assertEquals(1, delegateValueCalls.get());

        currentKey.set("k2");
        assertEquals(42, state.value());
        assertEquals(2, delegateValueCalls.get());

        currentKey.set("k3");
        assertEquals(42, state.value());
        assertEquals(3, delegateValueCalls.get());

        currentKey.set("k1");
        assertEquals(42, state.value());
        assertEquals(4, delegateValueCalls.get());
    }

    @Test
    void updateWritesThroughAndUpdatesCache() throws IOException {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        CurrentKeyProvider<String> currentKeyProvider = currentKey::get;

        AtomicInteger delegateValueCalls = new AtomicInteger();
        @SuppressWarnings("unchecked")
        InternalValueState<String, VoidNamespace, Integer> delegate = mock(InternalValueState.class);

        when(delegate.value())
                .thenAnswer(
                        invocation -> {
                            delegateValueCalls.incrementAndGet();
                            return 1;
                        });
        doNothing().when(delegate).update(any());
        doNothing().when(delegate).setCurrentNamespace(any());

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(delegate, currentKeyProvider, 16);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        state.update(99);
        assertEquals(99, state.value());
        assertEquals(0, delegateValueCalls.get());
    }
}

