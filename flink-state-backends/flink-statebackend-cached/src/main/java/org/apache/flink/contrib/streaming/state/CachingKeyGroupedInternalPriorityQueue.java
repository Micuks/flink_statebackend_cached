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
        flush(keyContext.getCurrentKeyGroupIndex());
        return delegate.poll();
    }

    @Override
    public T peek() {
        flush(keyContext.getCurrentKeyGroupIndex());
        return delegate.peek();
    }

    @Override
    public boolean add(@Nonnull T toAdd) {
        Object key = toAdd.getKey();
        if (key == null) {
            // Use the current key from context if element's key is null
            key = keyContext.getCurrentKey();
        }
        int keyGroupId = backend.getKeyGroupIndexForKey(key);
        HeapPriorityQueue<T> buffer = getBuffer(keyGroupId);
        synchronized (buffer) {
            boolean added = buffer.add(toAdd);
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
        Object key = toRemove.getKey();
        if (key == null) {
            key = keyContext.getCurrentKey();
        }
        int keyGroupId = backend.getKeyGroupIndexForKey(key);
        HeapPriorityQueue<T> buffer = getBuffer(keyGroupId);
        boolean removedFromBuffer;
        synchronized (buffer) {
            removedFromBuffer = buffer.remove(toRemove);
        }
        // Element might be in delegate, so we try removing from there too.
        // Flush first to ensure delegate is up-to-date.
        flush(keyGroupId);
        boolean removedFromDelegate = delegate.remove(toRemove);
        return removedFromBuffer || removedFromDelegate;
    }

    @Override
    public boolean isEmpty() {
        flush(keyContext.getCurrentKeyGroupIndex());
        return delegate.isEmpty();
    }

    @Override
    public int size() {
        flush(keyContext.getCurrentKeyGroupIndex());
        return delegate.size();
    }

    @Override
    public void addAll(@Nonnull Collection<? extends T> toAdd) {
        if (toAdd.isEmpty()) {
            return;
        }
        // This is a bit inefficient as it will group by key group id,
        // but it's better than getting the key group for each element individually.
        Map<Integer, ? extends Collection<? extends T>> groupedByKg =
                backend.groupElementsbyKeyGroup(toAdd);
        for (Map.Entry<Integer, ? extends Collection<? extends T>> entry : groupedByKg.entrySet()) {
            HeapPriorityQueue<T> buffer = getBuffer(entry.getKey());
            synchronized (buffer) {
                buffer.addAll(entry.getValue());
            }

            if (buffer.size() >= FLUSH_THRESHOLD) {
                flush(entry.getKey());
            }
        }
    }

    @Nonnull
    @Override
    public CloseableIterator<T> iterator() {
        flush(keyContext.getCurrentKeyGroupIndex());
        return delegate.iterator();
    }

    @Nonnull
    @Override
    public Set<T> getSubsetForKeyGroup(int keyGroupId) {
        flush(keyGroupId);
        return delegate.getSubsetForKeyGroup(keyGroupId);
    }
} 