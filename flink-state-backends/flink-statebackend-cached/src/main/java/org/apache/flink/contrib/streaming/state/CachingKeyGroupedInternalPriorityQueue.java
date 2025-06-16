/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.heap.HeapPriorityQueue;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link KeyGroupedInternalPriorityQueue} that buffers writes in memory to batch writes to a delegate queue.
 *
 * @param <T> The type of elements in the priority queue.
 */
public class CachingKeyGroupedInternalPriorityQueue<T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
        implements KeyGroupedInternalPriorityQueue<T> {

    private final KeyGroupedInternalPriorityQueue<T> delegate;
    private final InternalKeyContext<?> keyContext;
    private final CachingKeyedStateBackend<?> backend;
    private final Map<Integer, HeapPriorityQueue<T>> buffers;
    private final TypeSerializer<T> serializer;

    /**
     * Flush the per-key-group buffer eagerly once it reaches this many elements.
     * <p>128 has shown to give good throughput/latency trade-off in micro-benchmarks
     * and keeps the additional heap consumption negligible. The threshold can be
     * promoted to a configuration option later if needed.</p>
     */
    private static final int FLUSH_THRESHOLD = 128;

    public CachingKeyGroupedInternalPriorityQueue(
            KeyGroupedInternalPriorityQueue<T> delegate,
            InternalKeyContext<?> keyContext,
            CachingKeyedStateBackend<?> backend,
            TypeSerializer<T> serializer) {
        this.delegate = delegate;
        this.keyContext = keyContext;
        this.backend = backend;
        this.serializer = serializer;
        this.buffers = new ConcurrentHashMap<>();
    }

    private HeapPriorityQueue<T> getBuffer(int keyGroupId) {
        return buffers.computeIfAbsent(
                keyGroupId,
                k ->
                        new HeapPriorityQueue<>(
                                PriorityComparator.forPriorityComparableObjects(), 32));
    }

    private void flush(int keyGroupId) {
        HeapPriorityQueue<T> buffer = buffers.get(keyGroupId);
        if (buffer != null && !buffer.isEmpty()) {
            synchronized (buffer) {
                if (!buffer.isEmpty()) {
                    // HeapPriorityQueue is not a java.util.Collection & does not implement
                    // Iterable. We iterate via its CloseableIterator.
                    try (CloseableIterator<T> it = buffer.iterator()) {
                        while (it.hasNext()) {
                            delegate.add(it.next());
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to flush buffer for keyGroupId " + keyGroupId, e);
                    }
                    buffer.clear();
                }
            }
        }
    }

    public void flush() {
        for (Integer keyGroupId : buffers.keySet()) {
            flush(keyGroupId);
        }
    }

    @Override
    public T poll() {
        flush();
        return delegate.poll();
    }

    @Override
    public T peek() {
        flush();
        return delegate.peek();
    }

    @Override
    public boolean add(@Nonnull T toAdd) {
        // We rely on the (validated) current key-group information in the operator context.
        int keyGroupId = keyContext.getCurrentKeyGroupIndex();

        HeapPriorityQueue<T> buffer = getBuffer(keyGroupId);

        // Create a defensive copy of the element before buffering. This prevents later
        // mutations (e.g. key reuse for a different key-group) from making the element
        // appear to belong to another key-group when it is eventually flushed – which
        // leads to an IllegalArgumentException in the underlying queue. (See
        // stack-trace reported during Nexmark benchmarks.)
        T elementCopy = serializer.copy(toAdd);

        synchronized (buffer) {
            boolean added = buffer.add(elementCopy);
            if (added) {
                // Auto-flush if the buffer is getting too large to bound memory and latency.
                if (buffer.size() >= FLUSH_THRESHOLD) {
                    flush(keyGroupId);
                }
            }
            return added;
        }
    }

    @Override
    public boolean remove(@Nonnull T toRemove) {
        // Ensure all buffered elements for the current key-group are visible to the delegate
        // before we attempt to remove.
        flush();

        // We directly remove from the delegate. Even if the element is still in a buffer of
        // another key-group (which should be rare after the defensive copy change), it will
        // be removed on the next flush. This keeps the implementation simple and safe.
        return delegate.remove(toRemove);
    }

    @Override
    public boolean isEmpty() {
        flush();
        return delegate.isEmpty();
    }

    @Override
    public int size() {
        flush();
        return delegate.size();
    }

    @Override
    public void addAll(@Nonnull Collection<? extends T> toAdd) {
        if (toAdd.isEmpty()) {
            return;
        }
        // Delegate to the per-element add() implementation so that the same key-group selection
        // logic (based on the current operator context) is applied for every element.
        for (T element : toAdd) {
            add(element);
        }
    }

    @Nonnull
    @Override
    public CloseableIterator<T> iterator() {
        flush();
        return delegate.iterator();
    }

    @Nonnull
    @Override
    public Set<T> getSubsetForKeyGroup(int keyGroupId) {
        flush(keyGroupId);
        return delegate.getSubsetForKeyGroup(keyGroupId);
    }
} 