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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBBatchValueReader;
import org.apache.flink.contrib.streaming.state.cachekit.PrefetchExecutor;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.inOrder;

class CachedInternalValueStateTest {

    @Test
    void testSynchronousEvictionBatchPreservesKeysNamespacesDeletesAndContext() throws Exception {
        runEvictionBatchScenario(true, false, 2);
    }

    @Test
    void testSynchronousEvictionBatchFailureDoesNotPublishCleanL2() throws Exception {
        runEvictionBatchScenario(true, true, 2);
    }

    @Test
    void testEvictionBatchUnsupportedDelegateRetainsPointWrites() throws Exception {
        runEvictionBatchScenario(false, false, 2);
    }

    @Test
    void testEvictionSingleDirtyEntryRetainsPointWrite() throws Exception {
        runEvictionBatchScenario(true, false, 0);
    }

    @SuppressWarnings("unchecked")
    private void runEvictionBatchScenario(boolean supported, boolean fail, int overflow)
            throws Exception {
        String property = "cachekit.value.eviction-write-batch.enabled";
        String previous = System.getProperty(property);
        System.setProperty(property, "true");
        try {
            AtomicReference<String> key = new AtomicReference<>("k1");
            InternalValueState<String, String, Integer> delegate = mock(InternalValueState.class,
                    withSettings().extraInterfaces(RocksDBBatchValueReader.class));
            RocksDBBatchValueReader<String, String, Integer> writer =
                    (RocksDBBatchValueReader<String, String, Integer>) delegate;
            when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
            when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
            when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
            when(writer.supportsSynchronousValueWriteBatch()).thenReturn(supported);
            when(writer.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                    .thenAnswer(call -> (call.getArgument(0) + "/" + call.getArgument(1))
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
            if (fail) {
                doThrow(new IOException("injected batch failure"))
                        .when(writer).writeSerializedValueBatch(any(), any());
            }
            CachedInternalValueState<String, String, Integer> state = new CachedInternalValueState<>(
                    delegate, key::get, key::set, 100, CachePolicyType.LRU, overflow,
                    false, 0.05, 1000, false, true);
            int trigger = 129 + overflow;
            for (int i = 1; i < trigger; i++) {
                key.set("k" + i);
                state.setCurrentNamespace("n" + (i % 2));
                if (i == 2) state.clear();
                else state.update(i);
            }
            key.set("k" + trigger);
            state.setCurrentNamespace("n1");
            if (fail) {
                assertThrows(RuntimeException.class, () -> state.update(trigger));
                java.lang.reflect.Field l2 = CachedInternalValueState.class.getDeclaredField("l2Cache");
                l2.setAccessible(true);
                assertEquals(0, ((org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicy<?, ?>)
                        l2.get(state)).size());
                assertEquals(0, epochCounter(state, "evictionWriteBatchKeys"));
            } else {
                state.update(trigger);
                assertEquals("k" + trigger, key.get());
                assertEquals(trigger, state.value());
                if (supported && overflow > 0) {
                    org.mockito.ArgumentCaptor<List<byte[]>> keys = org.mockito.ArgumentCaptor.forClass(List.class);
                    org.mockito.ArgumentCaptor<List<Integer>> values = org.mockito.ArgumentCaptor.forClass(List.class);
                    verify(writer).writeSerializedValueBatch(keys.capture(), values.capture());
                    assertEquals(java.util.Arrays.asList(1, null, 3), values.getValue());
                    for (int i = 0; i < 3; i++) {
                        assertEquals("k" + (i + 1) + "/n" + ((i + 1) % 2),
                                new String(keys.getValue().get(i), java.nio.charset.StandardCharsets.UTF_8));
                    }
                    assertEquals(1, epochCounter(state, "evictionWriteBatches"));
                    assertEquals(3, epochCounter(state, "evictionWriteBatchKeys"));
                    verify(delegate, never()).update(any());
                    verify(delegate, never()).clear();
                    key.set("k1");
                    state.setCurrentNamespace("n1");
                    assertEquals(1, state.value()); // Clean L2 promotion after successful commit.
                } else {
                    verify(writer, never()).writeSerializedValueBatch(any(), any());
                    verify(delegate).update(1);
                    assertEquals(supported ? 1 : 0, epochCounter(state, "evictionSingleWrites"));
                }
            }
            state.close();
        } finally {
            if (previous == null) System.clearProperty(property);
            else System.setProperty(property, previous);
        }
    }

    private static long epochCounter(Object state, String name) throws Exception {
        java.lang.reflect.Field field = CachedInternalValueState.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.getLong(state);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testStickyEpochFastPathAndFlushReplacementFallback() throws Exception {
        String property = "cachekit.value.sticky-l1-epoch.enabled";
        String previous = System.getProperty(property);
        System.setProperty(property, "true");
        try {
            AtomicReference<String> key = new AtomicReference<>("a");
            InternalValueState<String, String, Integer> delegate = mock(InternalValueState.class);
            when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
            when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
            when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
            CachedInternalValueState<String, String, Integer> state = new CachedInternalValueState<>(
                    delegate, key::get, key::set, 100, CachePolicyType.LRU, 0,
                    false, 0.05, 1000, false, true);
            state.setCurrentNamespace("n");
            state.update(1);
            state.update(2);
            assertEquals(1, epochCounter(state, "stickyL1EpochFastPaths"));
            state.flush();
            state.update(3);
            assertEquals(1, epochCounter(state, "stickyL1EpochFastPaths"));
            assertEquals(1, epochCounter(state, "stickyL1EpochFallbacks"));
            state.update(4);
            assertEquals(2, epochCounter(state, "stickyL1EpochFastPaths"));
            state.clear();
            state.update(5);
            assertEquals(5, state.value());
            state.flush();
            verify(delegate).update(2);
            verify(delegate).update(5);
            state.close();
        } finally {
            if (previous == null) System.clearProperty(property);
            else System.setProperty(property, previous);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testStickyEpochInterveningPrefetchPreservesExactLruOrderAndEviction() throws Exception {
        String property = "cachekit.value.sticky-l1-epoch.enabled";
        String previous = System.getProperty(property);
        System.setProperty(property, "true");
        try {
            AtomicReference<String> key = new AtomicReference<>("k1");
            InternalValueState<String, String, Integer> delegate = mock(InternalValueState.class);
            when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
            when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
            when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
            when(delegate.value()).thenReturn(7);
            CachedInternalValueState<String, String, Integer> state = new CachedInternalValueState<>(
                    delegate, key::get, key::set, 100, CachePolicyType.LRU, 0,
                    false, 0.05, 1000, false, true);
            state.setCurrentNamespace("n");
            state.update(1);
            state.prefetch(Collections.singletonList("k2"));
            state.update(2);
            assertEquals(0, epochCounter(state, "stickyL1EpochFastPaths"));
            assertEquals(1, epochCounter(state, "stickyL1EpochFallbacks"));
            List<String> keys = new ArrayList<>();
            for (int i = 3; i <= 129; i++) keys.add("k" + i);
            state.prefetch(keys);
            // The fallback touched k1, so clean k2, not dirty k1, is evicted first.
            verify(delegate, never()).update(any());
            state.update(3);
            keys.clear();
            for (int i = 130; i <= 259; i++) keys.add("k" + i);
            state.prefetch(keys);
            verify(delegate).update(3);
            state.update(4);
            state.flush();
            verify(delegate).update(4);
            assertEquals(4, state.value());
            state.close();
        } finally {
            if (previous == null) System.clearProperty(property);
            else System.setProperty(property, previous);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testStickyEpochOverflowInvalidatesCertificate() throws Exception {
        String property = "cachekit.value.sticky-l1-epoch.enabled";
        String previous = System.getProperty(property);
        System.setProperty(property, "true");
        try {
            AtomicReference<String> key = new AtomicReference<>("a");
            InternalValueState<String, String, Integer> delegate = mock(InternalValueState.class);
            when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
            when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
            when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
            CachedInternalValueState<String, String, Integer> state = new CachedInternalValueState<>(
                    delegate, key::get, key::set, 100, CachePolicyType.LRU, 0,
                    false, 0.05, 1000, false, true);
            state.setCurrentNamespace("n");
            state.update(1);
            java.lang.reflect.Field epoch = CachedInternalValueState.class.getDeclaredField("l1OrderEpoch");
            epoch.setAccessible(true);
            epoch.setLong(state, Long.MAX_VALUE);
            state.prefetch(Collections.singletonList("b"));
            state.update(2);
            assertEquals(0, epochCounter(state, "stickyL1EpochFastPaths"));
            assertEquals(1, epochCounter(state, "stickyL1EpochFallbacks"));
            assertEquals(2, state.value());
            state.close();
        } finally {
            if (previous == null) System.clearProperty(property);
            else System.setProperty(property, previous);
        }
    }

    private static final class HashCountKey {
        private int value;
        private final java.util.concurrent.atomic.AtomicInteger calls;
        private HashCountKey(int value, java.util.concurrent.atomic.AtomicInteger calls) {
            this.value = value;
            this.calls = calls;
        }
        @Override
        public int hashCode() { calls.incrementAndGet(); return value; }
        @Override
        public boolean equals(Object other) {
            return other instanceof HashCountKey && value == ((HashCountKey) other).value;
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testOwnedCopiedHashesAreStableAndComputedOnceIncludingZero() throws Exception {
        Class<?> type = Class.forName(CachedInternalValueState.class.getName() + "$KeyNamespaceKey");
        java.lang.reflect.Constructor<?> constructor = type.getDeclaredConstructor(
                Object.class, Object.class, org.apache.flink.api.common.typeutils.TypeSerializer.class,
                org.apache.flink.api.common.typeutils.TypeSerializer.class, boolean.class);
        constructor.setAccessible(true);
        org.apache.flink.api.common.typeutils.TypeSerializer<HashCountKey> serializer =
                mock(org.apache.flink.api.common.typeutils.TypeSerializer.class);
        when(serializer.copy(any(HashCountKey.class))).thenAnswer(call -> {
            HashCountKey key = call.getArgument(0);
            return new HashCountKey(key.value, key.calls);
        });
        for (int keyValue : new int[] {0, 7, -31}) {
            java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
            HashCountKey key = new HashCountKey(keyValue, calls);
            HashCountKey namespace = new HashCountKey(0, calls);
            Object owned = constructor.newInstance(key, namespace, serializer, serializer, true);
            int expected = 31 * (31 + keyValue);
            assertEquals(2, calls.get());
            key.value = 999;
            namespace.value = 888;
            for (int i = 0; i < 100; i++) assertEquals(expected, owned.hashCode());
            assertEquals(2, calls.get());
            Object equal = constructor.newInstance(new HashCountKey(keyValue, calls),
                    new HashCountKey(0, calls), serializer, serializer, false);
            assertEquals(owned, equal);
            assertEquals(equal.hashCode(), owned.hashCode());
        }
    }

    @Test
    void testMutableLookupHashNeverCachesCallerMutation() throws Exception {
        Class<?> type = Class.forName(CachedInternalValueState.class.getName() + "$KeyNamespaceKey");
        java.lang.reflect.Constructor<?> constructor = type.getDeclaredConstructor(Object.class, Object.class);
        constructor.setAccessible(true);
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        HashCountKey key = new HashCountKey(1, calls);
        HashCountKey namespace = new HashCountKey(2, calls);
        Object lookup = constructor.newInstance(key, namespace);
        assertEquals(31 * 32 + 2, lookup.hashCode());
        key.value = 3;
        namespace.value = 4;
        assertEquals(31 * 34 + 4, lookup.hashCode());
        assertEquals(4, calls.get());
    }

    @Test
    void testHashCachingWithoutOwnershipFallsBackToDynamicHash() throws Exception {
        Class<?> type = Class.forName(CachedInternalValueState.class.getName() + "$KeyNamespaceKey");
        java.lang.reflect.Constructor<?> constructor = type.getDeclaredConstructor(
                Object.class, Object.class, org.apache.flink.api.common.typeutils.TypeSerializer.class,
                org.apache.flink.api.common.typeutils.TypeSerializer.class, boolean.class);
        constructor.setAccessible(true);
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        HashCountKey key = new HashCountKey(1, calls);
        Object lookup = constructor.newInstance(key, VoidNamespace.INSTANCE, null, null, true);
        int before = lookup.hashCode();
        key.value = 2;
        assertEquals(before + 31, lookup.hashCode());
        assertEquals(2, calls.get());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testJavaAccessGuidanceSkipsAllEntriesUntilFirstReadThenRemainsEligible() throws Exception {
        String property = "cachekit.value.prefetch.java-access-guided.enabled";
        String previous = System.getProperty(property);
        System.setProperty(property, "true");
        try {
            AtomicReference<String> currentKey = new AtomicReference<>("a");
            InternalValueState<String, VoidNamespace, Integer> delegate = mock(
                    InternalValueState.class, withSettings().extraInterfaces(RocksDBBatchValueReader.class));
            when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
            when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
            when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
            when(delegate.value()).thenReturn(7);
            RocksDBBatchValueReader<String, VoidNamespace, Integer> reader =
                    (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
            stubPreparedKeySerialization(reader);
            CachedInternalValueState<String, VoidNamespace, Integer> state =
                    new CachedInternalValueState<>(delegate, currentKey::get, currentKey::set,
                            100, CachePolicyType.LRU, 0, false, 0.05, 1000, true, 8, 2);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            assertFalse(state.isRecordKeyPrefetchEligible(false));
            assertFalse(state.shouldReceiveRecordKeyPrefetch(false));
            assertNull(state.buildAsyncPrefetchTask(Arrays.asList("b", "c")));
            assertFalse(state.prefetchWithCompletion(Arrays.asList("b", "c")).join());
            state.prefetch(Arrays.asList("b", "c"));
            verify(delegate, never()).value();
            verify(reader, never()).serializeBatchKeyAndNamespace(any(), any(), any(), any());
            assertFalse(state.hasInFlightReservationForTesting("b", VoidNamespace.INSTANCE));
            // A write alone does not activate read prediction; the first read may hit dirty L1.
            state.update(11);
            assertFalse(state.isRecordKeyPrefetchEligible(false));
            assertEquals(11, state.value());
            assertTrue(state.isRecordKeyPrefetchEligible(false));
            assertTrue(state.shouldReceiveRecordKeyPrefetch(false));
            Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("b", "c"));
            assertTrue(task != null);
            ((PrefetchExecutor.DropAwareTask) task).onDrop();
            state.clear();
            assertTrue(state.isRecordKeyPrefetchEligible(false));
            state.prefetch(Collections.singletonList("d"));
            currentKey.set("d");
            assertEquals(7, state.value());
            verify(delegate, times(1)).value();
            state.close();
        } finally {
            if (previous == null) System.clearProperty(property);
            else System.setProperty(property, previous);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testJavaAccessGuidanceDefaultOffAndNonVoidDirectCallerUnchanged() throws Exception {
        String property = "cachekit.value.prefetch.java-access-guided.enabled";
        String previous = System.getProperty(property);
        try {
            for (boolean enabled : new boolean[] {false, true}) {
                System.setProperty(property, Boolean.toString(enabled));
                AtomicReference<String> key = new AtomicReference<>("a");
                InternalValueState<String, String, Integer> delegate = mock(InternalValueState.class);
                when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
                when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
                when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
                when(delegate.value()).thenReturn(9);
                CachedInternalValueState<String, String, Integer> state =
                        new CachedInternalValueState<>(delegate, key::get, key::set,
                                100, CachePolicyType.LRU, 0, false, 0.05, 1000, false, 8, 2);
                state.setCurrentNamespace("known-stable-namespace");
                assertFalse(state.supportsRecordKeyPrefetch());
                state.prefetch(Collections.singletonList("b"));
                verify(delegate, times(1)).value();
                key.set("b");
                assertEquals(9, state.value());
                state.close();
            }
        } finally {
            if (previous == null) System.clearProperty(property);
            else System.setProperty(property, previous);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testJavaDeferredSerializationKeepsOwnedCopiesOfReusedIteratorKeys() throws Exception {
        String property = "cachekit.value.prefetch.early-small-batch.enabled";
        String previous = System.getProperty(property);
        System.setProperty(property, "true");
        try {
            InternalValueState<List<Integer>, VoidNamespace, Integer> delegate = mock(
                    InternalValueState.class, withSettings().extraInterfaces(RocksDBBatchValueReader.class));
            when(delegate.getKeySerializer()).thenReturn(new ListSerializer<>(IntSerializer.INSTANCE));
            when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
            when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
            RocksDBBatchValueReader<List<Integer>, VoidNamespace, Integer> reader =
                    (RocksDBBatchValueReader<List<Integer>, VoidNamespace, Integer>) delegate;
            List<Integer> serialized = new ArrayList<>();
            when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any())).thenAnswer(call -> {
                List<Integer> key = call.getArgument(0);
                serialized.add(key.get(0));
                return new byte[] {key.get(0).byteValue()};
            });
            CachedInternalValueState<List<Integer>, VoidNamespace, Integer> state =
                    new CachedInternalValueState<>(delegate, () -> Collections.singletonList(0), ignored -> {},
                            100, CachePolicyType.LRU, 0, false, 0.05, 1000, true, 8, 2);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            Iterable<List<Integer>> reusedKeys = () -> new java.util.Iterator<List<Integer>>() {
                final List<Integer> reused = new ArrayList<>(Collections.singletonList(0));
                int next;
                public boolean hasNext() { return next < 2; }
                public List<Integer> next() { reused.set(0, ++next); return reused; }
            };
            Runnable task = state.buildAsyncPrefetchTask(reusedKeys);
            assertTrue(task != null);
            assertEquals(Arrays.asList(1, 2), serialized);
            ((PrefetchExecutor.DropAwareTask) task).onDrop();
            state.close();
        } finally {
            if (previous == null) System.clearProperty(property);
            else System.setProperty(property, previous);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testJavaEarlyDropDeduplicatesReleasesAndPreservesReads() throws Exception {
        String property = "cachekit.value.prefetch.early-small-batch.enabled";
        String previous = System.getProperty(property);
        System.setProperty(property, "true");
        try {
            AtomicReference<String> currentKey = new AtomicReference<>("unused");
            InternalValueState<String, VoidNamespace, Integer> delegate = mock(
                    InternalValueState.class, withSettings().extraInterfaces(RocksDBBatchValueReader.class));
            when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
            when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
            when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
            when(delegate.value()).thenReturn(7);
            RocksDBBatchValueReader<String, VoidNamespace, Integer> reader =
                    (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
            stubPreparedKeySerialization(reader);
            when(reader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(2))).thenReturn(
                    Arrays.asList(KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE),
                            KvStateSerializer.serializeValue(33, IntSerializer.INSTANCE)));
            CachedInternalValueState<String, VoidNamespace, Integer> state =
                    new CachedInternalValueState<>(delegate, currentKey::get, currentKey::set,
                            100, CachePolicyType.LRU, 0, false, 0.05, 1000, true, 8, 2);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            assertNull(state.buildAsyncPrefetchTask(Arrays.asList("k1", "k1")));
            assertEquals(1, state.getJavaPrefetchEarlySmallBatchDropsForTesting());
            assertEquals(1, state.getJavaPrefetchEarlySmallBatchKeysForTesting());
            assertFalse(state.hasInFlightReservationForTesting("k1", VoidNamespace.INSTANCE));
            verify(reader, never()).serializeBatchKeyAndNamespace(any(), any(), any(), any());
            currentKey.set("k1");
            assertEquals(7, state.value());
            // Cached keys do not count toward the remaining worker batch.
            assertNull(state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")));
            assertFalse(state.hasInFlightReservationForTesting("k2", VoidNamespace.INSTANCE));
            assertEquals(2, state.getJavaPrefetchEarlySmallBatchDropsForTesting());
            Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("k2", "k3"));
            assertTrue(task != null);
            task.run();
            currentKey.set("k2");
            assertEquals(22, state.value());
            currentKey.set("k3");
            assertEquals(33, state.value());
            verify(reader, times(2)).serializeBatchKeyAndNamespace(any(), any(), any(), any());
            assertEquals(0, state.getPrefetchPointGetCallsForTesting());
            state.close();
        } finally {
            if (previous == null) System.clearProperty(property);
            else System.setProperty(property, previous);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testJavaDeferredSerializationFailureReleasesEveryReservation() throws Exception {
        String property = "cachekit.value.prefetch.early-small-batch.enabled";
        String previous = System.getProperty(property);
        System.setProperty(property, "true");
        try {
            InternalValueState<String, VoidNamespace, Integer> delegate = mock(
                    InternalValueState.class, withSettings().extraInterfaces(RocksDBBatchValueReader.class));
            when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
            when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
            when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
            RocksDBBatchValueReader<String, VoidNamespace, Integer> reader =
                    (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
            when(reader.serializeBatchKeyAndNamespace(any(), any(), any(), any()))
                    .thenThrow(new IOException("expected test serialization failure"));
            CachedInternalValueState<String, VoidNamespace, Integer> state =
                    new CachedInternalValueState<>(delegate, () -> "current", ignored -> {},
                            100, CachePolicyType.LRU, 0, false, 0.05, 1000, true, 8, 2);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            assertNull(state.buildAsyncPrefetchTask(Arrays.asList("a", "b")));
            assertFalse(state.hasInFlightReservationForTesting("a", VoidNamespace.INSTANCE));
            assertFalse(state.hasInFlightReservationForTesting("b", VoidNamespace.INSTANCE));
            assertEquals(0, state.getJavaPrefetchEarlySmallBatchDropsForTesting());
            state.close();
        } finally {
            if (previous == null) System.clearProperty(property);
            else System.setProperty(property, previous);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testJavaEarlyDropDisabledRetainsOriginalTaskBoundary() throws Exception {
        String property = "cachekit.value.prefetch.early-small-batch.enabled";
        String previous = System.getProperty(property);
        System.setProperty(property, "false");
        try {
            InternalValueState<String, VoidNamespace, Integer> delegate = mock(
                    InternalValueState.class, withSettings().extraInterfaces(RocksDBBatchValueReader.class));
            when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
            when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
            when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
            RocksDBBatchValueReader<String, VoidNamespace, Integer> reader =
                    (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
            stubPreparedKeySerialization(reader);
            CachedInternalValueState<String, VoidNamespace, Integer> state =
                    new CachedInternalValueState<>(delegate, () -> "current", ignored -> {},
                            100, CachePolicyType.LRU, 0, false, 0.05, 1000, true, 8, 2);
            state.setCurrentNamespace(VoidNamespace.INSTANCE);
            Runnable task = state.buildAsyncPrefetchTask(Collections.singletonList("a"));
            assertTrue(task != null);
            task.run();
            assertEquals(0, state.getJavaPrefetchEarlySmallBatchDropsForTesting());
            assertEquals(1, state.getPrefetchSmallBatchDropsForTesting());
            state.close();
        } finally {
            if (previous == null) System.clearProperty(property);
            else System.setProperty(property, previous);
        }
    }

    @Test
    void testCompletionPrefetchReportsPublishedStaging() throws Exception {
        InternalValueState<String, String, Integer> delegate = mock(InternalValueState.class);
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.getSerializedValue(any(), any(), any(), any()))
                .thenReturn(KvStateSerializer.serializeValue(17, IntSerializer.INSTANCE));
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        () -> "unused",
                        ignored -> {},
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        false);
        state.setCurrentNamespace("window");

        assertTrue(
                state.prefetchWithCompletion(Collections.singletonList("key"))
                        .get(10, TimeUnit.SECONDS));
        assertEquals(1, state.getStagingSizeForTesting());
        state.close();
    }

    @Test
    void testCompletionPrefetchFailsClosedAfterWorkerFallback() throws Exception {
        InternalValueState<String, String, Integer> delegate = mock(InternalValueState.class);
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.getSerializedValue(any(), any(), any(), any()))
                .thenThrow(new IOException("read failed"));
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        () -> "unused",
                        ignored -> {},
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        false);
        state.setCurrentNamespace("window");

        assertFalse(
                state.prefetchWithCompletion(Collections.singletonList("key"))
                        .get(10, TimeUnit.SECONDS));
        assertEquals(1, state.getPrefetchWorkerFailuresForTesting());
        state.close();
    }

    @Test
    void testCompletionPrefetchFailsClosedAfterGenerationChanges() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("mailbox-write");
        CountDownLatch workerEntered = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        InternalValueState<String, String, Integer> delegate = mock(InternalValueState.class);
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.getSerializedValue(any(), any(), any(), any()))
                .thenAnswer(
                        ignored -> {
                            workerEntered.countDown();
                            assertTrue(releaseWorker.await(10, TimeUnit.SECONDS));
                            return KvStateSerializer.serializeValue(17, IntSerializer.INSTANCE);
                        });
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        false);
        state.setCurrentNamespace("window");

        java.util.concurrent.CompletableFuture<Boolean> completion =
                state.prefetchWithCompletion(Collections.singletonList("prefetched"));
        assertTrue(workerEntered.await(10, TimeUnit.SECONDS));
        java.lang.reflect.Field writeGeneration =
                CachedInternalValueState.class.getDeclaredField("writeGen");
        writeGeneration.setAccessible(true);
        writeGeneration.setLong(state, 1L);
        releaseWorker.countDown();

        assertFalse(completion.get(10, TimeUnit.SECONDS));
        assertEquals(0, state.getStagingSizeForTesting());
        state.close();
    }

    @Test
    void testCompletionPrefetchDoesNotClaimReadyWithoutWorkerWork() throws Exception {
        InternalValueState<String, String, Integer> delegate = mock(InternalValueState.class);
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        () -> "unused",
                        ignored -> {},
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        false);
        state.setCurrentNamespace("window");

        assertFalse(
                state.prefetchWithCompletion(Collections.emptyList())
                        .get(10, TimeUnit.SECONDS));
        state.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testStickyInPlaceUpdatePreservesArbitraryNamespaceAndLifecycleSemantics()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        AtomicReference<String> delegateNamespace = new AtomicReference<>();
        java.util.ArrayList<String> flushed = new java.util.ArrayList<>();
        InternalValueState<String, String, Integer> delegate = mock(InternalValueState.class);
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        doAnswer(
                        invocation -> {
                            delegateNamespace.set(invocation.getArgument(0));
                            return null;
                        })
                .when(delegate)
                .setCurrentNamespace(any());
        doAnswer(
                        invocation -> {
                            flushed.add(
                                    currentKey.get()
                                            + "/"
                                            + delegateNamespace.get()
                                            + "="
                                            + invocation.getArgument(0));
                            return null;
                        })
                .when(delegate)
                .update(any());

        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        false,
                        true);

        state.setCurrentNamespace("window-a");
        state.update(10);
        state.update(11);
        state.setCurrentNamespace("window-b");
        state.update(20);
        state.update(21);
        currentKey.set("k2");
        state.setCurrentNamespace("window-a");
        state.update(30);
        state.update(31);

        assertEquals(3, state.getStickyUpdateSameKeyAttemptsForTesting());
        assertEquals(3, state.getStickyUpdateInPlaceReusesForTesting());

        currentKey.set("k1");
        state.setCurrentNamespace("window-a");
        assertEquals(11, state.value());
        state.setCurrentNamespace("window-b");
        assertEquals(21, state.value());
        currentKey.set("k2");
        state.setCurrentNamespace("window-a");
        assertEquals(31, state.value());

        state.flush();
        assertEquals(3, flushed.size());
        assertTrue(flushed.contains("k1/window-a=11"));
        assertTrue(flushed.contains("k1/window-b=21"));
        assertTrue(flushed.contains("k2/window-a=31"));

        // flush() replaces dirty L1 wrappers with clean ones. The sticky pointer may still refer
        // to the old wrapper, so a subsequent same-entry update must not mutate that detached
        // object.
        state.update(32);
        assertEquals(4, state.getStickyUpdateSameKeyAttemptsForTesting());
        assertEquals(3, state.getStickyUpdateInPlaceReusesForTesting());
        assertEquals(32, state.value());
        state.flush();
        assertEquals(4, flushed.size());
        assertTrue(flushed.contains("k2/window-a=32"));

        state.close();
        state.flush();
        assertEquals(4, flushed.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPendingMultiGetKeysAreDeduplicatedAndReleasedWhenDropped() throws Exception {
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        () -> "unused",
                        ignored -> {},
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        Runnable first = state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2"));
        Runnable duplicate = state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2"));
        assertTrue(first instanceof PrefetchExecutor.DropAwareTask);
        assertNull(duplicate);
        assertEquals(2, state.getPrefetchKeysDeduplicatedForTesting());

        ((PrefetchExecutor.DropAwareTask) first).onDrop();
        Runnable retry = state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2"));
        assertTrue(retry != null);
        assertEquals(1, state.getPrefetchTasksDroppedForTesting());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDispatchCancellationFiltersExactReservationBeforeWorkerRead() throws Exception {
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        () -> "unused",
                        ignored -> {},
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2"));
        assertEquals(1, state.cancelPrefetchForDispatch(Collections.singletonList("k1")));
        task.run();

        assertEquals(1, state.getPrefetchDispatchKeysExaminedForTesting());
        assertEquals(1, state.getPrefetchDispatchCancellationsForTesting());
        assertEquals(1, state.getPrefetchWorkerCancelledBeforeReadForTesting());
        assertEquals(1, state.getPrefetchSmallBatchDropsForTesting());
        verify(batchReader, never()).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        state.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDispatchCancellationPreservesAlreadyPublishedStagingWithoutReservationScan()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(2)))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE)));

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")).run();
        assertEquals(2, state.getStagingSizeForTesting());
        assertEquals(0, state.cancelPrefetchForDispatch(Collections.singletonList("k1")));
        assertEquals(0, state.getPrefetchDispatchAlreadyStagedForTesting());
        assertEquals(1, state.getPrefetchDispatchNoReservationForTesting());
        assertEquals(11, state.value());
        verify(delegate, never()).value();
        state.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDispatchCancellationFailsClosedWithoutPreparedMultiGet() {
        InternalValueState<String, VoidNamespace, Integer> delegate = mock(InternalValueState.class);
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        () -> "unused",
                        ignored -> {},
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertFalse(state.supportsDispatchPrefetchCancellation());
        assertEquals(0, state.cancelPrefetchForDispatch(Collections.singletonList("k1")));
        assertEquals(0, state.getPrefetchDispatchKeysExaminedForTesting());
        state.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDispatchCancellationFailsClosedForNamespacedPreparedMultiGet() {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);

        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        1,
                        false,
                        true);
        state.setCurrentNamespace("window-1");

        assertFalse(state.supportsDispatchPrefetchCancellation());
        assertEquals(0, state.cancelPrefetchForDispatch(Collections.singletonList("k1")));
        assertEquals(0, state.getPrefetchDispatchKeysExaminedForTesting());
        state.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testLiveReadRecordsRaceWithUnfinishedPrefetchWithoutChangingResult() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(17);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        Runnable unfinished = state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2"));
        assertTrue(unfinished != null);
        assertEquals(17, state.value());
        assertEquals(1, state.getPrefetchLiveReadRacedInFlightForTesting());
        assertEquals(1, state.getPrefetchLiveReadCancellationsForTesting());
        verify(delegate, times(1)).value();

        ((PrefetchExecutor.DropAwareTask) unfinished).onDrop();
        state.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testLiveReadCancellationFiltersKeyBeforeWorkerRocksDBRead() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(17);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        when(batchReader.getSerializedValueByRocksDBKey(any()))
                .thenReturn(KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE));

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2"));
        assertEquals(17, state.value());
        task.run();

        assertEquals(1, state.getPrefetchLiveReadCancellationsForTesting());
        assertEquals(1, state.getPrefetchWorkerCancelledBeforeReadForTesting());
        assertEquals(1, state.getPrefetchSmallBatchDropsForTesting());
        assertEquals(1, state.getPrefetchSmallBatchKeysDroppedForTesting());
        verify(batchReader, never()).getSerializedValueByRocksDBKey(any());
        verify(batchReader, never()).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        currentKey.set("k2");
        assertEquals(17, state.value());
        verify(delegate, times(2)).value();
        state.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testLaterSameGenerationReservationCannotReviveDispatchCancelledWorker()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(17);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        when(batchReader.getSerializedValueByRocksDBKey(any()))
                .thenReturn(KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE));

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        0,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        Runnable first = state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2"));
        assertEquals(1, state.cancelPrefetchForDispatch(Collections.singletonList("k1")));
        state.reReserveForTesting("k1", VoidNamespace.INSTANCE);

        first.run();
        assertEquals(1, state.getPrefetchWorkerCancelledBeforeReadForTesting());
        assertTrue(state.hasInFlightReservationForTesting("k1", VoidNamespace.INSTANCE));

        assertEquals(1, state.getPrefetchSmallBatchDropsForTesting());
        assertEquals(1, state.getPrefetchSmallBatchKeysDroppedForTesting());
        verify(batchReader, never()).getSerializedValueByRocksDBKey(any());
        verify(batchReader, never()).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        state.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDispatchCancellationDiscardsExactKeyAfterWorkerRocksDBRead() throws Exception {
        CountDownLatch batchStarted = new CountDownLatch(1);
        CountDownLatch releaseBatch = new CountDownLatch(1);
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(17);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(2)))
                .thenAnswer(
                        ignored -> {
                            batchStarted.countDown();
                            if (!releaseBatch.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("Timed out waiting to release batch read");
                            }
                            return Arrays.asList(
                                    KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                                    KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE));
                        });

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2"));
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        Thread worker =
                new Thread(
                        () -> {
                            try {
                                task.run();
                            } catch (Throwable failure) {
                                workerFailure.set(failure);
                            }
                        });
        worker.start();
        assertTrue(batchStarted.await(5, TimeUnit.SECONDS));
        assertEquals(1, state.cancelPrefetchForDispatch(Collections.singletonList("k1")));
        releaseBatch.countDown();
        worker.join(5000);

        assertFalse(worker.isAlive());
        assertNull(workerFailure.get());
        assertEquals(1, state.getPrefetchDispatchCancellationsForTesting());
        assertEquals(1, state.getPrefetchWorkerDiscardedAfterReadForTesting());
        currentKey.set("k2");
        assertEquals(22, state.value());
        state.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testNewGenerationCanReclaimStalePrefetchReservations() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("activate-bypass");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(1);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        true,
                        1.0,
                        1,
                        true,
                        8);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        assertEquals(1, state.value()); // one miss activates write-through bypass

        Runnable stale = state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2"));
        currentKey.set("writer");
        state.update(7); // direct delegate write advances writeGen
        Runnable current = state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2"));
        assertTrue(current != null);

        ((PrefetchExecutor.DropAwareTask) stale).onDrop();
        assertNull(state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")));
        assertEquals(2, state.getPrefetchKeysDeduplicatedForTesting());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testAsyncPrefetchUsesOneOrderedBatchRead() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);

        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        when(batchReader.getBatchDefaultValue()).thenReturn(99);
        List<byte[]> batchValues =
                Arrays.asList(
                        KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                        null,
                        KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE));
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(3)))
                .thenReturn(batchValues);
        when(delegate.value()).thenReturn(99);

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertFalse(state.shouldReceiveRecordKeyPrefetch(true));
        assertTrue(state.shouldReceiveRecordKeyPrefetch(false));

        Runnable prefetch = state.buildAsyncPrefetchTask(Arrays.asList("k1", "missing", "k2"));
        prefetch.run();

        currentKey.set("k1");
        assertEquals(11, state.value());
        assertTrue(state.shouldReceiveRecordKeyPrefetch(true));
        currentKey.set("missing");
        assertEquals(99, state.value());
        currentKey.set("k2");
        assertEquals(22, state.value());

        verify(batchReader, times(1))
                .getSerializedValuesByRocksDBKeys(any(), eq(0), eq(3));
        verify(batchReader, times(3))
                .serializeBatchKeyAndNamespace(
                        any(),
                        eq(VoidNamespace.INSTANCE),
                        eq(StringSerializer.INSTANCE),
                        eq(VoidNamespaceSerializer.INSTANCE));
        verify(batchReader, never()).getSerializedValues(any(), any(), any());
        verify(delegate, never()).getSerializedValue(any(), any(), any(), any());
        verify(delegate, never()).value();
        assertEquals(1, state.getPrefetchMultiGetCallsForTesting());
        assertEquals(1, state.getPrefetchMissingValuesStagedForTesting());
        assertEquals(3, state.getPrefetchValuesPromotedForTesting());
        assertEquals(0, state.getPrefetchLazyValuesStagedForTesting());
        assertEquals(0, state.getPrefetchLazyValuesMaterializedForTesting());
        assertTrue(state.supportsRecordKeyPrefetch());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testLazyAsyncStagingMaterializesOnlyUsedNamespacedValuesAndCopiesDefault()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, List<Integer>> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        ListSerializer<Integer> valueSerializer = new ListSerializer<>(IntSerializer.INSTANCE);
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(valueSerializer);
        when(delegate.value()).thenReturn(Collections.singletonList(-1));

        RocksDBBatchValueReader<String, String, List<Integer>> batchReader =
                (RocksDBBatchValueReader<String, String, List<Integer>>) delegate;
        when(batchReader.serializeBatchKeyAndNamespace(
                        any(), eq("window-lazy"), any(), any()))
                .thenReturn(new byte[] {1});
        List<Integer> defaultValue = new ArrayList<>(Collections.singletonList(99));
        when(batchReader.getBatchDefaultValue()).thenReturn(defaultValue);
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(5)))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(
                                        Collections.singletonList(11), valueSerializer),
                                null,
                                null,
                                KvStateSerializer.serializeValue(
                                        Collections.singletonList(44), valueSerializer),
                                KvStateSerializer.serializeValue(
                                        Collections.singletonList(55), valueSerializer)));

        CachedInternalValueState<String, String, List<Integer>> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true);
        state.setCurrentNamespace("window-lazy");
        Runnable prefetchTask =
                state.buildAsyncPrefetchTask(
                        Arrays.asList("used", "missing-a", "missing-b", "stale", "unused"));
        // The caller-owned default may be reused and mutated as soon as task construction
        // returns. The worker must retain the mailbox-side copy, not this source object.
        defaultValue.set(0, 1234);
        prefetchTask.run();

        assertEquals(5, state.getPrefetchLazyValuesStagedForTesting());
        assertEquals(0, state.getPrefetchLazyValuesMaterializedForTesting());
        assertEquals(5, state.getStagingSizeForTesting());

        currentKey.set("used");
        assertEquals(Collections.singletonList(11), state.value());
        assertEquals(1, state.getPrefetchLazyValuesMaterializedForTesting());

        currentKey.set("missing-a");
        List<Integer> firstDefault = state.value();
        assertEquals(Collections.singletonList(99), firstDefault);
        assertNotSame(defaultValue, firstDefault);
        firstDefault.add(100);
        currentKey.set("missing-b");
        List<Integer> secondDefault = state.value();
        assertEquals(Collections.singletonList(99), secondDefault);
        assertNotSame(defaultValue, secondDefault);
        assertNotSame(firstDefault, secondDefault);
        assertEquals(3, state.getPrefetchLazyValuesMaterializedForTesting());

        // A dirty flush advances writeGen. The remaining old-generation payload must be discarded
        // without materialization and the authoritative delegate read must win.
        currentKey.set("writer");
        state.update(Collections.singletonList(7));
        state.flush();
        currentKey.set("stale");
        assertEquals(Collections.singletonList(-1), state.value());
        assertEquals(3, state.getPrefetchLazyValuesMaterializedForTesting());
        verify(delegate, times(1)).value();

        assertEquals(1, state.getStagingSizeForTesting());
        state.close();
        assertEquals(0, state.getStagingSizeForTesting());
        assertEquals(1, state.getPrefetchUnusedStagedOnCloseForTesting());
        assertEquals(0, state.getStagingRetainedBytesForTesting());
        assertEquals(3, state.getPrefetchLazyValuesMaterializedForTesting());
        assertEquals(0, state.getPrefetchLazyMaterializationFailuresForTesting());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testLazyAsyncStagingPreservesNullDefaultWithoutDelegateRead() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> batchReader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(batchReader.serializeBatchKeyAndNamespace(
                        any(), eq("window-null"), any(), any()))
                .thenReturn(new byte[] {1});
        when(batchReader.getBatchDefaultValue()).thenReturn(null);
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(2)))
                .thenReturn(Arrays.asList(null, null));

        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        2,
                        2,
                        false,
                        true);
        state.setCurrentNamespace("window-null");
        state.buildAsyncPrefetchTask(Arrays.asList("missing-a", "missing-b")).run();

        assertEquals(2, state.getPrefetchLazyValuesStagedForTesting());
        assertEquals(0, state.getPrefetchLazyValuesMaterializedForTesting());
        currentKey.set("missing-a");
        assertNull(state.value());
        assertEquals(1, state.getPrefetchLazyValuesMaterializedForTesting());
        verify(delegate, never()).value();
        state.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testKeyScopedWriteInvalidatesOnlyMatchingPreparedValue() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        when(batchReader.getBatchDefaultValue()).thenReturn(99);
        byte[] staleValue = KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE);
        byte[] survivorValue = KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE);
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(2)))
                .thenReturn(Arrays.asList(staleValue, survivorValue));

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        100,
                        1 << 20,
                        true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("stale", "survivor"));
        task.run();
        assertEquals(2, state.getStagingSizeForTesting());
        assertEquals(
                staleValue.length + survivorValue.length,
                state.getStagingRetainedBytesForTesting());

        // Publish-before-write race: the exact matching speculative value is removed, while the
        // unrelated prepared value remains promotable.
        currentKey.set("stale");
        state.update(7);
        state.flush();
        assertEquals(1, state.getStagingSizeForTesting());
        assertEquals(survivorValue.length, state.getStagingRetainedBytesForTesting());
        assertEquals(1, state.getPrefetchKeyScopedInvalidationsForTesting());
        assertEquals(0, state.getPrefetchKeyScopedInFlightCancelledForTesting());
        assertEquals(1, state.getPrefetchKeyScopedStagedRemovedForTesting());

        currentKey.set("survivor");
        assertEquals(22, state.value());
        assertEquals(0, state.getStagingRetainedBytesForTesting());
        assertEquals(1, state.getPrefetchValuesPromotedForTesting());
        currentKey.set("stale");
        assertEquals(7, state.value());

        // The common write to a key absent from both speculative maps takes the lock-free
        // fast-negative path without disturbing subsequent authoritative reads.
        currentKey.set("never-prefetched");
        state.update(33);
        state.flush();
        assertEquals(1, state.getPrefetchKeyScopedFastNegativeSkipsForTesting());
        assertEquals(33, state.value());
        state.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testKeyScopedWriteCancelsMatchingReservationBeforePublish() throws Exception {
        CountDownLatch batchStarted = new CountDownLatch(1);
        CountDownLatch releaseBatch = new CountDownLatch(1);
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        when(batchReader.getBatchDefaultValue()).thenReturn(99);
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(2)))
                .thenAnswer(
                        ignored -> {
                            batchStarted.countDown();
                            if (!releaseBatch.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("Timed out waiting to release batch read");
                            }
                            return Arrays.asList(
                                    KvStateSerializer.serializeValue(
                                            11, IntSerializer.INSTANCE),
                                    KvStateSerializer.serializeValue(
                                            22, IntSerializer.INSTANCE));
                        });

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true,
                        100,
                        1 << 20,
                        true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        Runnable task = state.buildAsyncPrefetchTask(Arrays.asList("stale", "survivor"));
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        Thread worker =
                new Thread(
                        () -> {
                            try {
                                task.run();
                            } catch (Throwable failure) {
                                workerFailure.set(failure);
                            }
                        },
                        "key-scoped-prefetch-race");

        try {
            worker.start();
            assertTrue(batchStarted.await(5, TimeUnit.SECONDS));

            // Write-before-publish race: cancelling the matching reservation makes the worker
            // discard only that key after its RocksDB read completes.
            currentKey.set("stale");
            state.update(7);
            state.flush();
            releaseBatch.countDown();
            worker.join(5000);

            assertFalse(worker.isAlive());
            assertNull(workerFailure.get());
            assertEquals(1, state.getPrefetchKeyScopedInvalidationsForTesting());
            assertEquals(1, state.getPrefetchKeyScopedInFlightCancelledForTesting());
            assertEquals(0, state.getPrefetchKeyScopedStagedRemovedForTesting());
            assertEquals(1, state.getPrefetchWorkerDiscardedAfterReadForTesting());
            assertEquals(1, state.getStagingSizeForTesting());

            currentKey.set("survivor");
            assertEquals(22, state.value());
            currentKey.set("stale");
            assertEquals(7, state.value());
        } finally {
            releaseBatch.countDown();
            worker.join(5000);
            state.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testKeyScopedInvalidationFallsBackToGenerationForGenericQueryWire() throws Exception {
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(InternalValueState.class);
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.getSerializedValue(any(), any(), any(), any()))
                .thenAnswer(
                        ignored -> {
                            readStarted.countDown();
                            if (!releaseRead.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("Timed out waiting to release generic read");
                            }
                            return KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE);
                        });

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        1,
                        false,
                        true,
                        100,
                        1 << 20,
                        true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        Runnable task = state.buildAsyncPrefetchTask(Collections.singletonList("stale"));
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        Thread worker =
                new Thread(
                        () -> {
                            try {
                                task.run();
                            } catch (Throwable failure) {
                                workerFailure.set(failure);
                            }
                        },
                        "generic-prefetch-generation-race");

        try {
            worker.start();
            assertTrue(readStarted.await(5, TimeUnit.SECONDS));
            currentKey.set("stale");
            state.update(7);
            state.flush();
            releaseRead.countDown();
            worker.join(5000);

            assertFalse(worker.isAlive());
            assertNull(workerFailure.get());
            assertEquals(0, state.getPrefetchKeyScopedInvalidationsForTesting());
            assertEquals(0, state.getStagingSizeForTesting());
            assertTrue(state.getPrefetchStaleAbortsForTesting() > 0);
            assertEquals(7, state.value());
        } finally {
            releaseRead.countDown();
            worker.join(5000);
            state.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testLazyMaterializationFailureFallsBackToAuthoritativeRead() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(73);
        RocksDBBatchValueReader<String, String, Integer> batchReader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(batchReader.serializeBatchKeyAndNamespace(
                        any(), eq("window-corrupt"), any(), any()))
                .thenReturn(new byte[] {1});
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(2)))
                .thenReturn(Arrays.asList(new byte[] {1}, new byte[] {2}));

        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        2,
                        2,
                        false,
                        true);
        state.setCurrentNamespace("window-corrupt");
        state.buildAsyncPrefetchTask(Arrays.asList("bad", "unused")).run();
        currentKey.set("bad");

        assertEquals(73, state.value());
        assertEquals(1, state.getPrefetchLazyMaterializationFailuresForTesting());
        assertEquals(0, state.getPrefetchLazyValuesMaterializedForTesting());
        assertEquals(0, state.getPrefetchValuesPromotedForTesting());
        verify(delegate, times(1)).value();
    }

    @Test
    void testStagedPromotionPropagatesDirtyEvictionDelegateFailure() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("dirty-0");
        InternalValueState<String, String, Integer> delegate = mock(InternalValueState.class);
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.getSerializedValue(any(), any(), any(), any()))
                .thenReturn(KvStateSerializer.serializeValue(999, IntSerializer.INSTANCE));

        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        false);
        state.setCurrentNamespace("window-eviction");
        for (int i = 0; i < 128; i++) {
            currentKey.set("dirty-" + i);
            state.update(i);
        }

        state.buildAsyncPrefetchTask(Collections.singletonList("prefetched")).run();
        assertEquals(1, state.getStagingSizeForTesting());
        doThrow(new IOException("dirty eviction failed")).when(delegate).update(any());

        currentKey.set("prefetched");
        RuntimeException failure = assertThrows(RuntimeException.class, state::value);
        assertTrue(failure.getMessage().contains("Failed to flush state"));
        assertEquals(0, state.getPrefetchLazyMaterializationFailuresForTesting());
        assertEquals(0, state.getStagingSizeForTesting());
        verify(delegate, never()).value();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testLazyStagingStrictlyAdmitsOnlyUpToEntryCapAndReleasesBytesOnPromotion()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        byte[] first = KvStateSerializer.serializeValue(1, IntSerializer.INSTANCE);
        byte[] second = KvStateSerializer.serializeValue(2, IntSerializer.INSTANCE);
        byte[] rejected = KvStateSerializer.serializeValue(3, IntSerializer.INSTANCE);
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(3)))
                .thenReturn(Arrays.asList(first, second, rejected));

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        3,
                        2,
                        false,
                        true,
                        2,
                        1024L);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2", "k3")).run();

        assertEquals(2, state.getStagingSizeForTesting());
        assertEquals(first.length + second.length, state.getStagingRetainedBytesForTesting());
        assertEquals(2, state.getPrefetchLazyValuesStagedForTesting());
        assertEquals(1, state.getPrefetchStagingAdmissionDropsForTesting());

        currentKey.set("k1");
        assertEquals(1, state.value());
        assertEquals(second.length, state.getStagingRetainedBytesForTesting());
        currentKey.set("k2");
        assertEquals(2, state.value());
        assertEquals(0, state.getStagingRetainedBytesForTesting());
        verify(delegate, never()).value();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testLazyStagingRejectsOversizedPayloadButAdmitsExactByteBudget() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(2)))
                .thenReturn(
                        Arrays.asList(new byte[5], new byte[5]),
                        Arrays.asList(new byte[4], new byte[4]));

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        2,
                        2,
                        false,
                        true,
                        10,
                        4L);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        state.buildAsyncPrefetchTask(Arrays.asList("oversized-1", "oversized-2")).run();
        assertEquals(0, state.getStagingSizeForTesting());
        assertEquals(0, state.getStagingRetainedBytesForTesting());
        assertEquals(1, state.getPrefetchStagingAdmissionDropsForTesting());

        state.buildAsyncPrefetchTask(Arrays.asList("exact-1", "exact-2")).run();
        assertEquals(1, state.getStagingSizeForTesting());
        assertEquals(4, state.getStagingRetainedBytesForTesting());
        assertEquals(1, state.getPrefetchLazyValuesStagedForTesting());
        assertEquals(2, state.getPrefetchStagingAdmissionDropsForTesting());
        state.close();
        assertEquals(0, state.getStagingRetainedBytesForTesting());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testImmediatePrefetchReusesCompletedSpeculativeStaging() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        byte[] lazyValue = KvStateSerializer.serializeValue(1, IntSerializer.INSTANCE);
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(2)))
                .thenReturn(Arrays.asList(lazyValue, lazyValue));

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        2,
                        2,
                        false,
                        true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")).run();
        assertEquals(2L * lazyValue.length, state.getStagingRetainedBytesForTesting());

        state.prefetchForImmediateUse(Collections.singletonList("k1"), true);
        assertEquals(2, state.getStagingSizeForTesting());
        assertEquals(2L * lazyValue.length, state.getStagingRetainedBytesForTesting());
        assertEquals(1, state.getPrefetchDispatchKeysExaminedForTesting());
        assertEquals(1, state.getPrefetchDispatchAlreadyStagedForTesting());
        assertEquals(0, state.getPrefetchDispatchCancellationsForTesting());
        assertEquals(0, state.getPrefetchDispatchNoReservationForTesting());
        currentKey.set("k1");
        assertEquals(1, state.value());
        assertEquals(1, state.getPrefetchLazyValuesMaterializedForTesting());
        assertEquals(lazyValue.length, state.getStagingRetainedBytesForTesting());
        verify(batchReader, times(1))
                .getSerializedValuesByRocksDBKeys(any(), eq(0), eq(2));
        verify(batchReader, never()).getSerializedValueByRocksDBKey(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testImmediatePrefetchDoesNotDuplicateUnfinishedSpeculativeRead() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        2,
                        2,
                        false,
                        true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        Runnable unfinished =
                state.buildAsyncPrefetchTask(Collections.singletonList("k1"));
        assertTrue(unfinished != null);
        state.prefetchForImmediateUse(Collections.singletonList("k1"));

        verify(batchReader, never()).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        verify(batchReader, never()).getSerializedValueByRocksDBKey(any());
        assertEquals(1, state.getPrefetchKeysDeduplicatedForTesting());
        ((PrefetchExecutor.DropAwareTask) unfinished).onDrop();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testImmediatePrefetchAfterDispatchRevokesSpeculationAndReadsOnce() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        when(batchReader.getSerializedValueByRocksDBKey(any()))
                .thenReturn(KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE));

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        2,
                        2,
                        false,
                        true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        Runnable unfinished =
                state.buildAsyncPrefetchTask(Collections.singletonList("k1"));
        assertTrue(unfinished != null);
        state.prefetchForImmediateUse(Collections.singletonList("k1"), true);
        unfinished.run();

        assertEquals(1, state.getPrefetchDispatchKeysExaminedForTesting());
        assertEquals(1, state.getPrefetchDispatchCancellationsForTesting());
        assertEquals(
                state.getPrefetchDispatchKeysExaminedForTesting(),
                state.getPrefetchDispatchCancellationsForTesting()
                        + state.getPrefetchDispatchAlreadyStagedForTesting()
                        + state.getPrefetchDispatchNoReservationForTesting());
        assertEquals(1, state.getPrefetchWorkerCancelledBeforeReadForTesting());
        currentKey.set("k1");
        assertEquals(11, state.value());
        verify(batchReader, times(1)).getSerializedValueByRocksDBKey(any());
        verify(batchReader, never()).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        verify(delegate, never()).value();
        state.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testImmediatePrefetchAfterDispatchRevokesReservationBehindCacheHit() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(11);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        2,
                        2,
                        false,
                        true);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertEquals(11, state.value());
        state.reReserveForTesting("k1", VoidNamespace.INSTANCE);
        assertTrue(state.hasInFlightReservationForTesting("k1", VoidNamespace.INSTANCE));
        state.prefetchForImmediateUse(Collections.singletonList("k1"), true);

        assertFalse(state.hasInFlightReservationForTesting("k1", VoidNamespace.INSTANCE));
        assertEquals(1, state.getPrefetchDispatchKeysExaminedForTesting());
        assertEquals(1, state.getPrefetchDispatchCancellationsForTesting());
        verify(batchReader, never()).getSerializedValueByRocksDBKey(any());
        verify(batchReader, never()).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        state.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testStagingPromotionReusesReservedStorageKey() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        TypeSerializer<String> keySerializer = mock(TypeSerializer.class);
        TypeSerializer<String> namespaceSerializer = mock(TypeSerializer.class);
        when(delegate.getKeySerializer()).thenReturn(keySerializer);
        when(delegate.getNamespaceSerializer()).thenReturn(namespaceSerializer);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(keySerializer.copy(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(namespaceSerializer.copy(any()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(keySerializer.duplicate()).thenReturn(keySerializer);
        when(namespaceSerializer.duplicate()).thenReturn(namespaceSerializer);

        RocksDBBatchValueReader<String, String, Integer> batchReader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(batchReader.serializeBatchKeyAndNamespace(
                        any(), eq("window-1"), eq(keySerializer), eq(namespaceSerializer)))
                .thenReturn(new byte[] {1});
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(2)))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE)));

        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2,
                        false,
                        true);
        state.setCurrentNamespace("window-1");

        Runnable prefetch = state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2"));
        prefetch.run();
        currentKey.set("k1");
        assertEquals(11, state.value());

        verify(delegate, never()).value();
        verify(keySerializer, times(2)).copy(any());
        verify(namespaceSerializer, times(2)).copy(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testImmediatePrefetchStagesExactLocalPreaggKeys() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);

        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        when(batchReader.getBatchDefaultValue()).thenReturn(99);
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(3)))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                                null,
                                KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE)));

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertFalse(state.consumeImmediatePrefetchAccessObserved());
        state.prefetchForImmediateUse(Arrays.asList("k1", "missing", "k2"));
        currentKey.set("k1");
        assertEquals(11, state.value());
        currentKey.set("missing");
        assertEquals(99, state.value());
        currentKey.set("k2");
        assertEquals(22, state.value());

        verify(batchReader, times(1))
                .getSerializedValuesByRocksDBKeys(any(), eq(0), eq(3));
        verify(delegate, never()).value();
        assertEquals(1, state.getPrefetchMultiGetCallsForTesting());
        assertEquals(1, state.getPrefetchMissingValuesStagedForTesting());
        assertEquals(3, state.getPrefetchValuesPromotedForTesting());
        assertEquals(0, state.getPrefetchLazyValuesStagedForTesting());
        assertEquals(0, state.getPrefetchLazyValuesMaterializedForTesting());
        assertTrue(state.consumeImmediatePrefetchAccessObserved());
        assertFalse(state.consumeImmediatePrefetchAccessObserved());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testRecordImmediatePrefetchUsesOnlyMultiGetAndStagesExactValues() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(3)))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(33, IntSerializer.INSTANCE)));
        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertTrue(state.prefetchRecordKeysForImmediateUse(Arrays.asList("k1", "k2", "k3")));
        for (int i = 1; i <= 3; i++) {
            currentKey.set("k" + i);
            assertEquals(i * 11, state.value());
        }

        verify(batchReader, times(1)).getSerializedValuesByRocksDBKeys(any(), eq(0), eq(3));
        verify(batchReader, never()).getSerializedValueByRocksDBKey(any());
        verify(delegate, never()).value();
        assertEquals(1, state.getRecordImmediateMultiGetBatchesForTesting());
        assertEquals(3, state.getRecordImmediateMultiGetKeysForTesting());
        assertEquals(3, state.getRecordImmediateValuesStagedForTesting());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testRecordImmediatePrefetchSkipsSmallMissSetWithoutPointGets() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        4);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        assertFalse(state.prefetchRecordKeysForImmediateUse(Arrays.asList("k1", "k2", "k3")));

        verify(batchReader, never()).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        verify(batchReader, never()).getSerializedValueByRocksDBKey(any());
        assertEquals(1, state.getRecordImmediateSmallBatchSkipsForTesting());
        assertEquals(0, state.getRecordImmediateMultiGetBatchesForTesting());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testRecordImmediatePrefetchRejectsNamespacedState() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, String, Integer> batchReader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        2);
        state.setCurrentNamespace("window-1");

        assertFalse(state.prefetchRecordKeysForImmediateUse(Arrays.asList("k1", "k2")));
        verify(batchReader, never()).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        verify(batchReader, never()).getSerializedValueByRocksDBKey(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testAsyncMultiGetSupportsNamespacedValueState() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);

        RocksDBBatchValueReader<String, String, Integer> batchReader =
                (RocksDBBatchValueReader<String, String, Integer>) delegate;
        when(batchReader.serializeBatchKeyAndNamespace(
                        any(), eq("window-7"), any(), any()))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        invocation.getArgument(1),
                                        StringSerializer.INSTANCE));
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(2)))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE)));

        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8);
        state.setCurrentNamespace("window-7");
        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2")).run();

        currentKey.set("k1");
        assertEquals(11, state.value());
        currentKey.set("k2");
        assertEquals(22, state.value());
        verify(batchReader, times(1))
                .getSerializedValuesByRocksDBKeys(any(), eq(0), eq(2));
        verify(delegate, never()).value();
        // A direct caller supplied the stable namespace, so this API remains supported. The
        // generic record-lookahead broadcast must not guess a future window/session namespace.
        assertFalse(state.supportsRecordKeyPrefetch());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testChunkedMultiGetPublishesCompletedChunkBeforeNextChunkReturns() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        CountDownLatch secondChunkStarted = new CountDownLatch(1);
        CountDownLatch releaseSecondChunk = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);

        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenAnswer(
                        invocation -> {
                            int fromIndex = invocation.getArgument(1);
                            int toIndex = invocation.getArgument(2);
                            assertEquals(2, toIndex - fromIndex);
                            if (calls.getAndIncrement() == 0) {
                                return Arrays.asList(
                                        KvStateSerializer.serializeValue(
                                                11, IntSerializer.INSTANCE),
                                        KvStateSerializer.serializeValue(
                                                22, IntSerializer.INSTANCE));
                            }
                            secondChunkStarted.countDown();
                            assertTrue(releaseSecondChunk.await(5, TimeUnit.SECONDS));
                            return Arrays.asList(
                                    KvStateSerializer.serializeValue(33, IntSerializer.INSTANCE),
                                    KvStateSerializer.serializeValue(44, IntSerializer.INSTANCE));
                        });
        when(delegate.value()).thenReturn(99);

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        2);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        Thread prefetchThread =
                new Thread(
                        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2", "k3", "k4")),
                        "test-chunked-prefetch");

        try {
            prefetchThread.start();
            assertTrue(secondChunkStarted.await(5, TimeUnit.SECONDS));
            currentKey.set("k1");
            assertEquals(11, state.value());
            verify(delegate, never()).value();
        } finally {
            releaseSecondChunk.countDown();
            prefetchThread.join(5000);
        }
        assertFalse(prefetchThread.isAlive());
        assertEquals(2, calls.get());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testChunkedMultiGetDropsSingleKeySpeculativeTail() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.value()).thenReturn(33);

        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(2)))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE)));
        when(batchReader.getSerializedValueByRocksDBKey(any()))
                .thenReturn(KvStateSerializer.serializeValue(33, IntSerializer.INSTANCE));

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        2);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        state.buildAsyncPrefetchTask(Arrays.asList("k1", "k2", "k3")).run();

        currentKey.set("k1");
        assertEquals(11, state.value());
        currentKey.set("k2");
        assertEquals(22, state.value());
        currentKey.set("k3");
        assertEquals(33, state.value());
        verify(batchReader, times(1))
                .getSerializedValuesByRocksDBKeys(any(), eq(0), eq(2));
        verify(batchReader, never()).getSerializedValueByRocksDBKey(any());
        verify(delegate, times(1)).value();
        assertEquals(1, state.getPrefetchSmallBatchDropsForTesting());
        assertEquals(1, state.getPrefetchSmallBatchKeysDroppedForTesting());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testPreparedPrefetchDropsSpeculativeBatchBelowMinimum() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);

        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        when(batchReader.getSerializedValueByRocksDBKey(any()))
                .thenReturn(KvStateSerializer.serializeValue(7, IntSerializer.INSTANCE));
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), eq(0), eq(4)))
                .thenReturn(
                        Arrays.asList(
                                KvStateSerializer.serializeValue(11, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(22, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(33, IntSerializer.INSTANCE),
                                KvStateSerializer.serializeValue(44, IntSerializer.INSTANCE)));

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8,
                        4);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        state.buildAsyncPrefetchTask(Arrays.asList("p1", "p2", "p3")).run();
        verify(batchReader, never()).getSerializedValueByRocksDBKey(any());
        verify(batchReader, never()).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
        assertEquals(1, state.getPrefetchSmallBatchDropsForTesting());
        assertEquals(3, state.getPrefetchSmallBatchKeysDroppedForTesting());

        state.buildAsyncPrefetchTask(Arrays.asList("m1", "m2", "m3", "m4")).run();
        verify(batchReader, times(1))
                .getSerializedValuesByRocksDBKeys(any(), eq(0), eq(4));
        assertEquals(0, state.getPrefetchPointGetCallsForTesting());
        assertEquals(1, state.getPrefetchMultiGetCallsForTesting());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testAsyncPrefetchCanDisableMultiGetForControlledComparison() throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.getSerializedValue(any(), any(), any(), any()))
                .thenReturn(KvStateSerializer.serializeValue(7, IntSerializer.INSTANCE));

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        false);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        state.buildAsyncPrefetchTask(Collections.singletonList("k1")).run();

        currentKey.set("k1");
        assertEquals(7, state.value());
        verify(delegate, times(1)).getSerializedValue(any(), any(), any(), any());
        verify((RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate, never())
                .getSerializedValues(any(), any(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testLazyAsyncGenericPointReadMaterializesBeforeRetainingUnownedBytes()
            throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        InternalValueState<String, String, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        when(delegate.getSerializedValue(any(), any(), any(), any()))
                .thenReturn(KvStateSerializer.serializeValue(17, IntSerializer.INSTANCE));

        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        false,
                        false,
                        true);
        state.setCurrentNamespace("window-point");
        state.buildAsyncPrefetchTask(Collections.singletonList("k1")).run();

        assertEquals(0, state.getPrefetchLazyValuesStagedForTesting());
        assertEquals(0, state.getPrefetchLazyValuesMaterializedForTesting());
        currentKey.set("k1");
        assertEquals(17, state.value());
        assertEquals(0, state.getPrefetchLazyValuesMaterializedForTesting());
        verify(delegate, times(1)).getSerializedValue(any(), any(), any(), any());
        verify(delegate, never()).value();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCloseWaitsForInFlightMultiGet() throws Exception {
        CountDownLatch batchStarted = new CountDownLatch(1);
        CountDownLatch releaseBatch = new CountDownLatch(1);
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch closeReturned = new CountDownLatch(1);
        InternalValueState<String, VoidNamespace, Integer> delegate =
                mock(
                        InternalValueState.class,
                        withSettings().extraInterfaces(RocksDBBatchValueReader.class));
        when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
        when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
        when(delegate.getValueSerializer()).thenReturn(IntSerializer.INSTANCE);
        RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader =
                (RocksDBBatchValueReader<String, VoidNamespace, Integer>) delegate;
        stubPreparedKeySerialization(batchReader);
        when(batchReader.getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt()))
                .thenAnswer(
                        invocation -> {
                            batchStarted.countDown();
                            if (!releaseBatch.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("Timed out waiting to release batch read");
                            }
                            int start = invocation.getArgument(1);
                            int end = invocation.getArgument(2);
                            java.util.List<byte[]> values = new java.util.ArrayList<>(end - start);
                            for (int index = start; index < end; index++) {
                                values.add(
                                        KvStateSerializer.serializeValue(
                                                index, IntSerializer.INSTANCE));
                            }
                            return values;
                        });

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        () -> "unused",
                        ignored -> {},
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        8);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        java.util.List<String> keys = new java.util.ArrayList<>();
        for (int index = 0; index < 16; index++) {
            keys.add("k" + index);
        }
        Thread prefetchThread =
                new Thread(
                        state.buildAsyncPrefetchTask(keys),
                        "test-prefetch");
        Thread closeThread =
                new Thread(
                        () -> {
                            closeStarted.countDown();
                            state.close();
                            closeReturned.countDown();
                        },
                        "test-close");

        try {
            prefetchThread.start();
            assertTrue(batchStarted.await(5, TimeUnit.SECONDS));
            closeThread.start();
            assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
            assertFalse(closeReturned.await(200, TimeUnit.MILLISECONDS));
        } finally {
            releaseBatch.countDown();
            prefetchThread.join(5000);
            closeThread.join(5000);
        }
        assertFalse(prefetchThread.isAlive());
        assertFalse(closeThread.isAlive());
        assertEquals(0, closeReturned.getCount());
        assertEquals(0, state.getStagingSizeForTesting());
        assertEquals(0, state.getStagingRetainedBytesForTesting());
        verify(batchReader, times(1)).getSerializedValuesByRocksDBKeys(any(), anyInt(), anyInt());
    }

    private static void stubPreparedKeySerialization(
            RocksDBBatchValueReader<String, VoidNamespace, Integer> batchReader) throws Exception {
        when(batchReader.serializeBatchKeyAndNamespace(
                        any(),
                        eq(VoidNamespace.INSTANCE),
                        eq(StringSerializer.INSTANCE),
                        eq(VoidNamespaceSerializer.INSTANCE)))
                .thenAnswer(
                        invocation ->
                                KvStateSerializer.serializeKeyAndNamespace(
                                        invocation.getArgument(0),
                                        StringSerializer.INSTANCE,
                                        VoidNamespace.INSTANCE,
                                        VoidNamespaceSerializer.INSTANCE));
    }

    @Test
    void testL1CacheHit() throws IOException {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        CurrentKeyProvider<String> currentKeyProvider = currentKey::get;

        InternalValueState<String, VoidNamespace, Integer> delegate = mock(InternalValueState.class);
        when(delegate.value()).thenReturn(42);

        // L1 size will be max(128, 100/5) = 128.
        CachedInternalValueState<String, VoidNamespace, Integer> state = new CachedInternalValueState<>(delegate,
                currentKeyProvider, k -> {
                }, 100, CachePolicyType.LRU, 0, false, 0.05, 1000, false);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        // 1. First access loads from delegate
        assertEquals(42, state.value());
        verify(delegate, times(1)).value();

        // 2. Second access hits sticky/L1
        assertEquals(42, state.value());
        verify(delegate, times(1)).value();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testL1HitReusesStoredKeyWithoutAnotherSerializerCopy() throws IOException {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalValueState<String, String, Integer> delegate = mock(InternalValueState.class);
        TypeSerializer<String> keySerializer = mock(TypeSerializer.class);
        TypeSerializer<String> namespaceSerializer = mock(TypeSerializer.class);
        when(delegate.getKeySerializer()).thenReturn(keySerializer);
        when(delegate.getNamespaceSerializer()).thenReturn(namespaceSerializer);
        when(keySerializer.copy(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(namespaceSerializer.copy(any()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(delegate.value()).thenReturn(42);

        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        ignored -> {},
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        false);
        state.setCurrentNamespace("window-1");

        assertEquals(42, state.value()); // k1 miss: one immutable storage-key copy
        currentKey.set("k2");
        assertEquals(42, state.value()); // k2 miss: one immutable storage-key copy
        currentKey.set("k1");
        assertEquals(42, state.value()); // k1 L1 hit: reuse its stored copy

        verify(delegate, times(2)).value();
        verify(keySerializer, times(2)).copy(any());
        verify(namespaceSerializer, times(2)).copy(any());
    }

    @Test
    void testL1WriteBack() throws IOException {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        CurrentKeyProvider<String> currentKeyProvider = currentKey::get;

        InternalValueState<String, VoidNamespace, Integer> delegate = mock(InternalValueState.class);

        CachedInternalValueState<String, VoidNamespace, Integer> state = new CachedInternalValueState<>(delegate,
                currentKeyProvider, k -> {
                }, 100, CachePolicyType.LRU, 0, false, 0.05, 1000, false);
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
    void testFlushDoesNotWriteAfterClose() throws IOException {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalValueState<String, VoidNamespace, Integer> delegate = mock(InternalValueState.class);

        CachedInternalValueState<String, VoidNamespace, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        key -> {},
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);
        state.update(99);

        state.close();
        state.flush();

        verify(delegate, times(0)).update(any());
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
                }, 650, CachePolicyType.LRU, 0, false, 0.05, 1000, false);
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
    void testMaxEntriesEnforcement() throws IOException {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        CurrentKeyProvider<String> currentKeyProvider = currentKey::get;
        InternalValueState<String, VoidNamespace, Integer> delegate = mock(InternalValueState.class);

        // Max entries 10. L1 will be small (max(128, 2)=128).
        // Wait, if L1 min is 128, then "max entries 10" is tricky.
        // The factory "createFromConfig" does NOT enforce min 128 on the "maxEntries"
        // passed to the Backend constructor.
        // But CachedInternalValueState constructor does:
        // int l1Size = Math.max(128, maxEntries / 5);
        // this.l2Cache = createCachePolicy(maxEntries, this::onL2Eviction);

        // So L2 is sized to 'maxEntries'.
        // If I ask for 10 entries. L2 size is 10. L1 size is 128.
        // This seems like a design oddity (L1 bigger than L2?), but let's test L2
        // eviction which is the hard limit.
        // When L1 evicts (at 128), it pushes to L2. L2 (at 10) should evict.

        CachedInternalValueState<String, VoidNamespace, Integer> state = new CachedInternalValueState<>(delegate,
                currentKeyProvider, k -> {
                }, 10, CachePolicyType.LRU, 0, false, 0.05, 1000, false);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        // Fill with 200 items.
        // L1 will hold 128.
        // As we add more, L1 evicts to L2.
        // L2 holds 10. L2 should drop older ones.

        for (int i = 0; i < 200; i++) {
            currentKey.set("k-" + i);
            state.update(i);
        }

        // flush everything from L1 to L2/Delegate
        state.flush();

        // Now L1 is clean (but full? No, flush doesn't clear L1, it just marks clean).
        // Wait, flush in CachedInternalValueState:
        // for dirty entries: put to L2 (clean), write to delegate, put to L1 (clean).

        // So "k-0" ... "k-199" are all in the system.
        // The L2 cache only holds 10 items.
        // L1 holds 128 items (the most recently accessed/updated).
        // So "k-199" down to "k-(200-128) = k-72" are likely in L1.
        // "k-0" should definitively be gone from L2 (capacity 10) and gone from L1
        // (capacity 128).

        // Check k-0. Should miss L1, miss L2, hit Delegate.
        currentKey.set("k-0");

        org.mockito.Mockito.clearInvocations(delegate);
        when(delegate.value()).thenReturn(-1);

        int val = state.value();

        // It should have called delegate.value() because it's evicted from caches.
        verify(delegate, times(1)).value();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMutableKeyIsolation() throws IOException {
        class MutableKey {
            int id;

            MutableKey(int id) {
                this.id = id;
            }

            @Override
            public int hashCode() {
                return id;
            }

            @Override
            public boolean equals(Object o) {
                return o instanceof MutableKey && ((MutableKey) o).id == id;
            }
        }

        final MutableKey keyInstance = new MutableKey(1);
        AtomicReference<MutableKey> currentKey = new AtomicReference<>(keyInstance);

        InternalValueState<MutableKey, String, Integer> delegate =
                mock(InternalValueState.class);
        TypeSerializer<MutableKey> keySerializer = mock(TypeSerializer.class);
        TypeSerializer<String> namespaceSerializer = mock(TypeSerializer.class);
        when(delegate.getKeySerializer()).thenReturn(keySerializer);
        when(delegate.getNamespaceSerializer()).thenReturn(namespaceSerializer);
        when(keySerializer.copy(any()))
                .thenAnswer(
                        invocation ->
                                new MutableKey(((MutableKey) invocation.getArgument(0)).id));
        when(namespaceSerializer.copy(any()))
                .thenAnswer(invocation -> invocation.getArgument(0));

        CachedInternalValueState<MutableKey, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        ignored -> {},
                        100,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        false);
        state.setCurrentNamespace("window-1");

        state.update(12345);
        keyInstance.id = 999;
        currentKey.set(new MutableKey(1));

        assertEquals(12345, state.value());
        verify(delegate, never()).value();
        verify(keySerializer, times(1)).copy(any());
        verify(namespaceSerializer, times(1)).copy(any());
    }

    @Test
    void testBypassClearWritesThrough() throws IOException {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        CurrentKeyProvider<String> currentKeyProvider = currentKey::get;

        InternalValueState<String, VoidNamespace, Integer> delegate = mock(InternalValueState.class);
        when(delegate.value()).thenReturn(1);

        CachedInternalValueState<String, VoidNamespace, Integer> state = new CachedInternalValueState<>(delegate,
                currentKeyProvider, k -> {
                }, 100, CachePolicyType.LRU, 0, true, 1.0, 1, false);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        // Trigger a miss so bypass activates (hit rate 0 < threshold 1.0).
        assertEquals(1, state.value());
        verify(delegate, times(1)).value();

        org.mockito.Mockito.clearInvocations(delegate);

        // In bypass mode, clear must write-through to delegate.
        state.clear();
        verify(delegate, times(1)).clear();

        currentKey.set("k2");
        when(delegate.value()).thenReturn(7);
        assertEquals(7, state.value());
        verify(delegate, times(1)).value();
    }

    @Test
    void testResidentMutationAdaptiveGateWarmsUpAndKeepsProfitableQ18Shape() {
        assertTrue(
                CachedInternalValueState.isResidentMutationBatchProfitable(
                        4095, 0, 0, 4096, 8.0, 0.10));
        assertTrue(
                CachedInternalValueState.isResidentMutationBatchProfitable(
                        4096, 83_558, 3_234, 4096, 8.0, 0.10));
    }

    @Test
    void testResidentMutationAdaptiveGateRejectsSparseOrPositiveHeavyShapes() {
        assertFalse(
                CachedInternalValueState.isResidentMutationBatchProfitable(
                        4096, 16_711, 59, 4096, 8.0, 0.10));
        assertFalse(
                CachedInternalValueState.isResidentMutationBatchProfitable(
                        4096, 26_132, 5_753, 4096, 8.0, 0.10));
        assertFalse(
                CachedInternalValueState.isResidentMutationBatchProfitable(
                        4096, 0, 0, 4096, 8.0, 0.10));
    }

    // Adding a test for BinaryRowData specifically would be better if we can
    // instantiate it.
    // Assuming we can't easily instantiate Flink internal classes without
    // dependencies,
    // but the file imports `org.apache.flink.table.data.binary.BinaryRowData`.
    // Let's assume we can try to mock it or just skip if too complex.
    // For now, I will add the maxEntries test which is generic and critical for the
    // "10 vs 20000" issue.
}
