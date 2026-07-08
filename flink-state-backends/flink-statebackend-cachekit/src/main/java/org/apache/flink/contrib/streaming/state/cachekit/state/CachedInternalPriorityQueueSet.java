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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cachekit wrapper over delegate {@link KeyGroupedInternalPriorityQueue} that implements async pending
 * buffer optimization for PriorityQueue (used for timers).
 *
 * <p><b>Design &amp; Fixes Applied:</b>
 *
 * <ul>
 *   <li><b>Single ordered buffer</b> (PQ-2.2 fix): All add/remove operations are recorded in a
 *       single {@code List<Mutation<T>>} buffer, preserving insertion order. Flushing executes
 *       operations in order, preventing a remove-then-add from being reordered as add-then-remove.
 *   <li><b>Proper Future chaining</b> (PQ-2.1 fix): {@code CompletableFuture.runAsync()} return
 *       value is chained via a tail-reference, so {@code awaitPendingFlush()} actually waits for
 *       background flush to complete (instead of the previous bug where it always waited on a
 *       dummy completed future).
 *   <li><b>Complete visibility barrier</b> (PQ-2.4 fix): All observable methods ({@code peek()},
 *       {@code poll()}, {@code isEmpty()}, {@code size()}, {@code iterator()}, {@code
 *       getSubsetForKeyGroup()}) call {@code flushAndAwait()} before touching the delegate,
 *       ensuring pending operations are visible.
 *   <li><b>No double-delete on poll</b> (PQ-2.3 fix): {@code poll()} removes from the delegate
 *       and returns the result directly; the element is NOT re-added to the pending buffer.
 *   <li><b>add() always returns true</b> (PQ-2.6 fix): Consistent with the {@code
 *       PriorityQueue} contract that the element was successfully buffered.
 *   <li><b>Async exception propagation</b> (PQ-2.7 fix): Flush exceptions are captured in {@code
 *       flushError} and re-thrown on the next {@code awaitPendingFlush()} call, ensuring the Flink
 *       Task fails rather than silently losing data.
 *   <li><b>Only wraps Heap delegates</b>: Detected at construction time via {@code instanceof
 *       HeapPriorityQueueSet} in {@link
 *       org.apache.flink.contrib.streaming.state.cachekit.CacheKitKeyedStateBackend
 *       #wrapPriorityQueue}.
 * </ul>
 *
 * @param <T> The element type, constrained by Flink's timer interfaces.
 */
public class CachedInternalPriorityQueueSet<
                T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
        implements KeyGroupedInternalPriorityQueue<T> {

    private static final Logger LOG =
            LoggerFactory.getLogger(CachedInternalPriorityQueueSet.class);

    /** Threshold for triggering async flush. */
    private static final int MAX_PENDING_SIZE = 1024;

    /** The underlying delegate priority queue. */
    private final KeyGroupedInternalPriorityQueue<T> delegate;

    /** Whether the optimization is enabled. */
    private final boolean enabled;

    /** Element serializer for checkpoint snapshots. */
    private final TypeSerializer<T> elementSerializer;

    // ---- Single ordered pending buffer (PQ-2.2 fix: merged from two separate buffers) ----

    /** Pending mutations: each record is an ADD or REMOVE in insertion order. */
    @Nonnull private final List<Mutation<T>> pending = Collections.synchronizedList(new ArrayList<>());

    /** Chain of async flush futures using tail-reference pattern (PQ-2.1 fix). */
    private CompletableFuture<?> tail = CompletableFuture.completedFuture(null);

    /** Captured flush exception propagated to the next await (PQ-2.7 fix). */
    private final AtomicReference<Throwable> flushError = new AtomicReference<>(null);

    /** Shared flush executor (injected from CacheKitKeyedStateBackend). */
    private final ExecutorService flushExecutor;

    /**
     * Marker type for a pending mutation: either an ADD of an element or a REMOVE of an element.
     * (PQ-3.1 fix: replaced Java 16 record with a plain class for JDK 11 compatibility.)
     */
    private enum MutationType {
        ADD,
        REMOVE
    }

    private static final class Mutation<T> {
        private final MutationType type;
        private final T element;

        Mutation(MutationType type, T element) {
            this.type = type;
            this.element = element;
        }

        MutationType type() { return type; }
        T element() { return element; }
    }

    public CachedInternalPriorityQueueSet(
            KeyGroupedInternalPriorityQueue<T> delegate,
            TypeSerializer<T> elementSerializer,
            ExecutorService flushExecutor,
            boolean enabled) {
        this.delegate = delegate;
        this.elementSerializer = elementSerializer;
        this.flushExecutor = flushExecutor;
        this.enabled = enabled;

        LOG.info(
                "[CACHEKIT PQ] CachedInternalPriorityQueueSet created: enabled={}, delegate={}",
                enabled,
                delegate.getClass().getName());
    }

    // ------------------------------------------------------------------------
    //  Public API (InternalPriorityQueue contract)
    // ------------------------------------------------------------------------

    @Override
    public boolean add(@Nonnull T toAdd) {
        if (!enabled) {
            return delegate.add(toAdd);
        }

        synchronized (pending) {
            pending.add(new Mutation<>(MutationType.ADD, toAdd));
            if (pending.size() >= MAX_PENDING_SIZE) {
                flushAsyncInternal();
            }
        }
        // PQ-2.6 fix: always return true, matching PriorityQueue contract
        return true;
    }

    @Override
    public boolean remove(@Nonnull T toRemove) {
        if (!enabled) {
            return delegate.remove(toRemove);
        }

        synchronized (pending) {
            pending.add(new Mutation<>(MutationType.REMOVE, toRemove));
            if (pending.size() >= MAX_PENDING_SIZE) {
                flushAsyncInternal();
            }
        }
        return true;
    }

    @Override
    @Nullable
    public T poll() {
        if (!enabled) {
            return delegate.poll();
        }

        // PQ-2.4 fix: complete visibility barrier — flush all pending first
        flushAndAwait();

        // PQ-2.3 fix: poll from delegate and return directly; do NOT re-add to pending buffer
        return delegate.poll();
    }

    @Override
    @Nullable
    public T peek() {
        if (!enabled) {
            return delegate.peek();
        }
        // PQ-2.4 fix: complete visibility barrier
        flushAndAwait();
        return delegate.peek();
    }

    @Override
    public void addAll(@Nullable Collection<? extends T> elements) {
        if (elements == null || elements.isEmpty()) {
            return;
        }
        if (!enabled) {
            delegate.addAll(elements);
            return;
        }
        synchronized (pending) {
            for (T element : elements) {
                pending.add(new Mutation<>(MutationType.ADD, element));
            }
            if (pending.size() >= MAX_PENDING_SIZE) {
                flushAsyncInternal();
            }
        }
    }

    @Override
    public boolean isEmpty() {
        if (!enabled) {
            return delegate.isEmpty();
        }
        // PQ-2.4 fix: complete visibility barrier
        flushAndAwait();
        return delegate.isEmpty();
    }

    @Override
    public int size() {
        if (!enabled) {
            return delegate.size();
        }
        // PQ-2.4 fix: complete visibility barrier
        flushAndAwait();
        return delegate.size();
    }

    @Nonnull
    @Override
    public CloseableIterator<T> iterator() {
        if (!enabled) {
            return delegate.iterator();
        }
        // PQ-2.4 fix: complete visibility barrier
        flushAndAwait();
        return delegate.iterator();
    }

    @Nonnull
    @Override
    public Set<T> getSubsetForKeyGroup(int keyGroupId) {
        // PQ-2.4 fix: also flush before returning subset
        if (enabled) {
            flushAndAwait();
        }
        return delegate.getSubsetForKeyGroup(keyGroupId);
    }

    // ------------------------------------------------------------------------
    //  Internal helpers
    // ------------------------------------------------------------------------

    /**
     * Submits a new async flush task chained after the current tail future.
     *
     * <p>PQ-2.1 fix: uses {@code tail = tail.thenRunAsync(...)} so that subsequent flushes are
     * serialized in submission order, and {@code awaitPendingFlush()} (via {@code tail.join()})
     * actually waits for this flush to complete.
     */
    private void flushAsyncInternal() {
        if (pending.isEmpty()) {
            return;
        }
        if (flushExecutor == null) {
            flushSyncInternal();
            return;
        }

        @SuppressWarnings("unchecked")
        List<Mutation<T>> snapshot = new ArrayList<>((Collection<Mutation<T>>) pending);
        pending.clear();

        // PQ-2.1 fix: chain the new future onto the tail
        tail =
                tail.thenRunAsync(
                                () -> {
                                    for (Mutation<T> m : snapshot) {
                                        switch (m.type()) {
                                            case ADD:
                                                delegate.add(m.element());
                                                break;
                                            case REMOVE:
                                                delegate.remove(m.element());
                                                break;
                                        }
                                    }
                                },
                                flushExecutor)
                        .whenComplete(
                                (r, ex) -> {
                                    if (ex != null) {
                                        // PQ-2.7 / PQ-3.2 fix: capture exception instead of only logging
                                        LOG.error("[CACHEKIT PQ] Async flush failed", ex);
                                        flushError.set(ex);
                                    }
                                });
    }

    /**
     * Synchronous flush: applies all pending mutations in order directly on the caller thread.
     */
    private void flushSyncInternal() {
        if (pending.isEmpty()) {
            return;
        }
        @SuppressWarnings("unchecked")
        List<Mutation<T>> snapshot = new ArrayList<>((Collection<Mutation<T>>) pending);
        pending.clear();
        for (Mutation<T> m : snapshot) {
            switch (m.type()) {
                case ADD:
                    delegate.add(m.element());
                    break;
                case REMOVE:
                    delegate.remove(m.element());
                    break;
            }
        }
    }

    /**
     * PQ-2.4 fix: complete visibility barrier — submits remaining pending operations asynchronously
     * and blocks until the current tail future (and all previously submitted flushes) complete.
     *
     * <p>PQ-2.7 fix: re-throws any exception captured from a prior async flush.
     */
    private void flushAndAwait() {
        CompletableFuture<?> toAwait;
        synchronized (pending) {
            if (!pending.isEmpty()) {
                flushAsyncInternal();
            }
            toAwait = tail;
        }

        try {
            toAwait.join();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to await PQ flush", e);
        }

        // PQ-2.7 / PQ-3.2 fix: propagate async flush error to the caller
        Throwable ex = flushError.getAndSet(null);
        if (ex != null) {
            throw new FlinkRuntimeException("Prior async flush failed", ex);
        }
    }

    // ------------------------------------------------------------------------
    //  Checkpoint / lifecycle
    // ------------------------------------------------------------------------

    /**
     * Flushes all pending operations to the delegate synchronously. Called by the keyed state
     * backend during snapshots.
     */
    public void flushAllPending() {
        flushAndAwait();
    }

    /**
     * Closes the wrapper. Called by {@code CacheKitKeyedStateBackend.dispose()}. The executor is
     * shut down by the backend, not here.
     */
    public void close() {
        flushAndAwait();
        LOG.info("[CACHEKIT PQ] CachedInternalPriorityQueueSet closed");
    }

    // ------------------------------------------------------------------------
    //  Passthrough to delegate (priority queue metadata)
    // ------------------------------------------------------------------------

    public TypeSerializer<T> getElementSerializer() {
        return elementSerializer;
    }

    @Override
    public String toString() {
        return "CachedInternalPriorityQueueSet{"
                + "delegate="
                + delegate
                + ", enabled="
                + enabled
                + ", pendingSize="
                + pending.size()
                + '}';
    }
}
