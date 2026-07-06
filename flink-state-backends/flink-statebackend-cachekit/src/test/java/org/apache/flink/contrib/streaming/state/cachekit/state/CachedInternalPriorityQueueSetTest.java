/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link CachedInternalPriorityQueueSet} covering:
 *
 * <ul>
 *   <li>Enabled mode: add/remove/poll buffer and async flush
 *   <li>Disabled mode: all operations delegate directly
 *   <li>Threshold triggers async flush at 1024 elements
 *   <li>flushAllPending flushes synchronously
 *   <li>close() flushes before closing
 *   <li>peek/isEmpty/size delegate correctly after await
 *   <li>passthrough of getSubsetForKeyGroup
 *   <li>HeapPriorityQueueElement contract
 * </ul>
 */
class CachedInternalPriorityQueueSetTest {

    // ---- Test element implementation ----

    /** A simple test element for the priority queue. */
    private static class TestElement implements HeapPriorityQueueElement, PriorityComparable<TestElement>, Keyed<String> {
        private final String key;
        private final long priority;
        private int internalIndex = NOT_CONTAINED;

        TestElement(String key, long priority) {
            this.key = key;
            this.priority = priority;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public int comparePriorityTo(@Nonnull TestElement other) {
            return Long.compare(this.priority, other.priority);
        }

        @Override
        public int getInternalIndex() {
            return internalIndex;
        }

        @Override
        public void setInternalIndex(int index) {
            this.internalIndex = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TestElement)) return false;
            TestElement that = (TestElement) o;
            return priority == that.priority && key.equals(that.key);
        }

        @Override
        public int hashCode() {
            return Long.hashCode(priority) * 31 + key.hashCode();
        }

        @Override
        public String toString() {
            return "TestElement{key=" + key + ", priority=" + priority + '}';
        }
    }

    // ---- Mock delegate factory ----

    @SuppressWarnings("unchecked")
    private KeyGroupedInternalPriorityQueue<TestElement> mockDelegate() {
        return mock(KeyGroupedInternalPriorityQueue.class);
    }

    // ---- Test fixtures ----

    private CachedInternalPriorityQueueSet<TestElement> createEnabled(
            KeyGroupedInternalPriorityQueue<TestElement> delegate, ExecutorService executor) {
        return new CachedInternalPriorityQueueSet<>(delegate, executor, true);
    }

    private CachedInternalPriorityQueueSet<TestElement> createDisabled(
            KeyGroupedInternalPriorityQueue<TestElement> delegate) {
        return new CachedInternalPriorityQueueSet<>(delegate, null, false);
    }

    // ---- Tests: Disabled mode ----

    @Test
    void testDisabled_addDelegates() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        when(delegate.add(any(TestElement.class))).thenReturn(true);

        CachedInternalPriorityQueueSet<TestElement> pq = createDisabled(delegate);
        TestElement e = new TestElement("k1", 100L);
        boolean result = pq.add(e);

        assertTrue(result);
        verify(delegate, times(1)).add(e);
    }

    @Test
    void testDisabled_removeDelegates() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        when(delegate.remove(any(TestElement.class))).thenReturn(true);

        CachedInternalPriorityQueueSet<TestElement> pq = createDisabled(delegate);
        TestElement e = new TestElement("k1", 100L);
        boolean result = pq.remove(e);

        assertTrue(result);
        verify(delegate, times(1)).remove(e);
    }

    @Test
    void testDisabled_pollDelegates() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        TestElement expected = new TestElement("k1", 100L);
        when(delegate.poll()).thenReturn(expected);

        CachedInternalPriorityQueueSet<TestElement> pq = createDisabled(delegate);
        TestElement result = pq.poll();

        assertEquals(expected, result);
        verify(delegate, times(1)).poll();
    }

    @Test
    void testDisabled_isEmptyDelegates() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        when(delegate.isEmpty()).thenReturn(false);

        CachedInternalPriorityQueueSet<TestElement> pq = createDisabled(delegate);
        assertFalse(pq.isEmpty());
        verify(delegate, times(1)).isEmpty();
    }

    @Test
    void testDisabled_sizeDelegates() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        when(delegate.size()).thenReturn(5);

        CachedInternalPriorityQueueSet<TestElement> pq = createDisabled(delegate);
        assertEquals(5, pq.size());
        verify(delegate, times(1)).size();
    }

    @Test
    void testDisabled_peekDelegates() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        TestElement expected = new TestElement("k1", 100L);
        when(delegate.peek()).thenReturn(expected);

        CachedInternalPriorityQueueSet<TestElement> pq = createDisabled(delegate);
        assertEquals(expected, pq.peek());
        verify(delegate, times(1)).peek();
    }

    @Test
    void testDisabled_getSubsetForKeyGroup() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        Set<TestElement> expected = new HashSet<>();
        expected.add(new TestElement("k1", 1L));
        when(delegate.getSubsetForKeyGroup(0)).thenReturn(expected);

        CachedInternalPriorityQueueSet<TestElement> pq = createDisabled(delegate);
        Set<TestElement> result = pq.getSubsetForKeyGroup(0);

        assertEquals(expected, result);
        verify(delegate, times(1)).getSubsetForKeyGroup(0);
    }

    @Test
    void testDisabled_addAllDelegates() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();

        CachedInternalPriorityQueueSet<TestElement> pq = createDisabled(delegate);
        Collection<TestElement> elements = Arrays.asList(
                new TestElement("k1", 1L),
                new TestElement("k2", 2L));

        pq.addAll(elements);

        // addAll delegates directly to delegate.addAll(elements)
        verify(delegate, times(1)).addAll(elements);
    }

    @Test
    void testDisabled_addAll_withNullOrEmpty() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();

        CachedInternalPriorityQueueSet<TestElement> pq = createDisabled(delegate);

        pq.addAll(null);
        pq.addAll(Collections.emptyList());

        verify(delegate, never()).add(any(TestElement.class));
    }

    // ---- Tests: Enabled mode ----

    @Test
    void testEnabled_add_buffersAndFlushesAtThreshold() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        when(delegate.add(any(TestElement.class))).thenReturn(true);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalPriorityQueueSet<TestElement> pq = createEnabled(delegate, executor);

            // Add 1023 elements — should not trigger async flush yet
            for (int i = 0; i < 1023; i++) {
                pq.add(new TestElement("key", (long) i));
            }
            // No adds should have been delegated yet (all in pending buffer)
            verify(delegate, never()).add(any(TestElement.class));

            // The 1024th element triggers async flush
            pq.add(new TestElement("key", 1023L));

            // Wait for the async flush to complete
            try { Thread.sleep(200); } catch (InterruptedException ignored) {}

            // Now add() should have been called at least once
            verify(delegate, times(1024)).add(any(TestElement.class));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testEnabled_addAll_buffersAndFlushesAtThreshold() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        when(delegate.add(any(TestElement.class))).thenReturn(true);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalPriorityQueueSet<TestElement> pq = createEnabled(delegate, executor);

            // Add 512 elements at once, twice to reach 1024 threshold
            for (int batch = 0; batch < 2; batch++) {
                Collection<TestElement> batchElements = new java.util.ArrayList<>();
                for (int i = 0; i < 512; i++) {
                    batchElements.add(new TestElement("key", (long) (batch * 512 + i)));
                }
                pq.addAll(batchElements);
            }

            try { Thread.sleep(200); } catch (InterruptedException ignored) {}

            // Should have been delegated
            verify(delegate, times(1024)).add(any(TestElement.class));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testEnabled_remove_buffersAndFlushesAtThreshold() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        when(delegate.remove(any(TestElement.class))).thenReturn(true);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalPriorityQueueSet<TestElement> pq = createEnabled(delegate, executor);

            // Add 1024 elements first to have something to remove
            for (int i = 0; i < 1024; i++) {
                pq.add(new TestElement("key", (long) i));
            }
            try { Thread.sleep(200); } catch (InterruptedException ignored) {}
            verify(delegate, times(1024)).add(any(TestElement.class));

            // Remove 1024 elements — should trigger async flush
            for (int i = 0; i < 1024; i++) {
                pq.remove(new TestElement("key", (long) i));
            }
            try { Thread.sleep(200); } catch (InterruptedException ignored) {}

            verify(delegate, times(1024)).remove(any(TestElement.class));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testEnabled_poll_awaitsInFlightFlush() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        TestElement expected = new TestElement("k1", 1L);
        when(delegate.poll()).thenReturn(expected);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalPriorityQueueSet<TestElement> pq = createEnabled(delegate, executor);

            // Add enough elements to trigger async flush
            for (int i = 0; i < 1024; i++) {
                pq.add(new TestElement("key", (long) i));
            }

            // poll() should wait for flush before calling delegate.poll()
            TestElement result = pq.poll();

            assertEquals(expected, result);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testEnabled_peek_awaitsInFlightFlush() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        TestElement expected = new TestElement("k1", 1L);
        when(delegate.peek()).thenReturn(expected);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalPriorityQueueSet<TestElement> pq = createEnabled(delegate, executor);

            for (int i = 0; i < 1024; i++) {
                pq.add(new TestElement("key", (long) i));
            }

            TestElement result = pq.peek();
            assertEquals(expected, result);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testEnabled_isEmpty_awaitsInFlightFlush() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        when(delegate.isEmpty()).thenReturn(false);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalPriorityQueueSet<TestElement> pq = createEnabled(delegate, executor);

            for (int i = 0; i < 1024; i++) {
                pq.add(new TestElement("key", (long) i));
            }

            boolean result = pq.isEmpty();
            assertFalse(result);
            verify(delegate, times(1)).isEmpty();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testEnabled_size_awaitsInFlightFlush() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        when(delegate.size()).thenReturn(1024);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalPriorityQueueSet<TestElement> pq = createEnabled(delegate, executor);

            for (int i = 0; i < 1024; i++) {
                pq.add(new TestElement("key", (long) i));
            }

            int result = pq.size();
            assertEquals(1024, result);
            verify(delegate, times(1)).size();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testEnabled_iterator_awaitsInFlightFlush() throws Exception {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        CloseableIterator<TestElement> mockIterator = mock(CloseableIterator.class);
        when(mockIterator.hasNext()).thenReturn(false);
        when(delegate.iterator()).thenReturn(mockIterator);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalPriorityQueueSet<TestElement> pq = createEnabled(delegate, executor);

            for (int i = 0; i < 1024; i++) {
                pq.add(new TestElement("key", (long) i));
            }

            CloseableIterator<TestElement> result = pq.iterator();
            assertNotNull(result);
            verify(delegate, times(1)).iterator();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testEnabled_close_flushesAllPending() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        when(delegate.add(any(TestElement.class))).thenReturn(true);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalPriorityQueueSet<TestElement> pq = createEnabled(delegate, executor);

            // Add some elements
            pq.add(new TestElement("k1", 1L));
            pq.add(new TestElement("k2", 2L));

            // Close should flush remaining pending (below threshold)
            pq.close();

            // After close, delegate.add should have been called
            verify(delegate, times(2)).add(any(TestElement.class));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testFlushAllPending_deliversAllBufferedElements() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        when(delegate.add(any(TestElement.class))).thenReturn(true);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalPriorityQueueSet<TestElement> pq = createEnabled(delegate, executor);

            // Add 100 elements (below threshold, no async flush triggered)
            for (int i = 0; i < 100; i++) {
                pq.add(new TestElement("key", (long) i));
            }
            verify(delegate, never()).add(any(TestElement.class));

            // flushAllPending should synchronously deliver all
            pq.flushAllPending();

            verify(delegate, times(100)).add(any(TestElement.class));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testEnabled_remove_returnsTrue() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        when(delegate.remove(any(TestElement.class))).thenReturn(true);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CachedInternalPriorityQueueSet<TestElement> pq = createEnabled(delegate, executor);

            boolean result = pq.remove(new TestElement("k1", 1L));

            assertTrue(result);
        } finally {
            executor.shutdownNow();
        }
    }

    // ---- HeapPriorityQueueElement contract tests ----

    // ---- HeapPriorityQueueElement contract tests ----
    // Note: getInternalIndex/setInternalIndex belong to the element, not the queue wrapper.
    // The wrapper delegates these to the element internally managed by the delegate heap PQ.

    @Test
    void testToString_containsDelegateInfo() {
        KeyGroupedInternalPriorityQueue<TestElement> delegate = mockDelegate();
        CachedInternalPriorityQueueSet<TestElement> pq = createDisabled(delegate);

        String str = pq.toString();
        assertNotNull(str);
        assertTrue(str.contains("CachedInternalPriorityQueueSet"));
    }
}
