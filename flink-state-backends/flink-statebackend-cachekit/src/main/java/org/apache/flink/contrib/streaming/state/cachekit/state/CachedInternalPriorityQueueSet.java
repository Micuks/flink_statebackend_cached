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
import org.apache.flink.runtime.state.InternalPriorityQueue;
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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cachekit wrapper over delegate {@link KeyGroupedInternalPriorityQueue} that implements async pending
 * buffer optimization for PriorityQueue (used for timers).
 *
 * <p><b>Design:</b>
 *
 * <ul>
 *   <li>When the optimization is enabled, {@code add()} / {@code remove()} / {@code poll()} do NOT
 *       immediately write to the delegate. Instead, they buffer the operation intent in
 *       pending-adds / pending-removes lists.
 *   <li>When the pending buffer reaches {@code MAX_PENDING_SIZE} (1024), the buffer is
 *       asynchronously flushed to the delegate.
 *   <li>On {@code poll()} / {@code peek()}, we wait for in-flight flushes so the delegate view is
 *       up-to-date.
 *   <li>The wrapper only wraps non-RocksDB delegates (detected by class name check in {@code
 *       CacheKitKeyedStateBackend}). For RocksDB delegates, the wrapper is not created.
 * </ul>
 *
 * @param <T> The element type, constrained by Flink's timer interfaces.
 */
public class CachedInternalPriorityQueueSet<
                T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
        implements KeyGroupedInternalPriorityQueue<T> {

    private static final Logger LOG =
            LoggerFactory.getLogger(CachedInternalPriorityQueueSet.class);

    /** Threshold for triggering async flush of the add buffer. */
    private static final int MAX_PENDING_ADD_SIZE = 1024;

    /** Threshold for triggering async flush of the remove buffer. */
    private static final int MAX_PENDING_REMOVE_SIZE = 1024;

    /** The underlying delegate priority queue. */
    private final KeyGroupedInternalPriorityQueue<T> delegate;

    /** Whether the optimization is enabled. */
    private final boolean enabled;

    // ---- Pending buffers ----
    /** Buffers pending add intents (elements to be added to delegate). */
    @Nonnull private final List<T> pendingAdds = new ArrayList<>();
    private final AtomicInteger pendingAddSize = new AtomicInteger(0);

    /** Buffers pending remove intents (elements to be removed from delegate). */
    @Nonnull private final List<T> pendingRemoves = new ArrayList<>();
    private final AtomicInteger pendingRemoveSize = new AtomicInteger(0);

    /** In-flight flush future for sequencing. */
    private final CompletableFuture<?>[] inFlightFlush = new CompletableFuture<?>[] {null};

    /** Shared flush executor (injected from CacheKitKeyedStateBackend). */
    private final ExecutorService flushExecutor;

    public CachedInternalPriorityQueueSet(
            KeyGroupedInternalPriorityQueue<T> delegate,
            ExecutorService flushExecutor,
            boolean enabled) {
        this.delegate = delegate;
        this.flushExecutor = flushExecutor;
        this.enabled = enabled;

        LOG.info(
                "[CACHEKIT PQ] CachedInternalPriorityQueueSet created: enabled={}",
                enabled);
    }

    // ------------------------------------------------------------------------
    //  Public API (InternalPriorityQueue contract)
    // ------------------------------------------------------------------------

    @Override
    public boolean add(@Nonnull T toAdd) {
        if (!enabled) {
            return delegate.add(toAdd);
        }

        boolean result;
        synchronized (pendingAdds) {
            pendingAdds.add(toAdd);
            pendingAddSize.incrementAndGet();
            result = pendingAdds.size() < MAX_PENDING_ADD_SIZE;
        }

        if (pendingAddSize.get() >= MAX_PENDING_ADD_SIZE) {
            flushAddsAsync();
        }
        return result;
    }

    @Override
    public boolean remove(@Nonnull T toRemove) {
        if (!enabled) {
            return delegate.remove(toRemove);
        }

        synchronized (pendingRemoves) {
            pendingRemoves.add(toRemove);
            pendingRemoveSize.incrementAndGet();
        }
        if (pendingRemoveSize.get() >= MAX_PENDING_REMOVE_SIZE) {
            flushRemovesAsync();
        }
        // Return true: the element is at least in the pending buffer; delegate will confirm on flush.
        return true;
    }

    @Override
    @Nullable
    public T poll() {
        if (!enabled) {
            return delegate.poll();
        }

        // Ensure any in-flight flush is done so delegate has the latest view
        awaitPendingFlush();

        T result = delegate.poll();
        if (result != null) {
            synchronized (pendingRemoves) {
                pendingRemoves.add(result);
                pendingRemoveSize.incrementAndGet();
            }
            if (pendingRemoveSize.get() >= MAX_PENDING_REMOVE_SIZE) {
                flushRemovesAsync();
            }
        }
        return result;
    }

    @Override
    @Nullable
    public T peek() {
        if (!enabled) {
            return delegate.peek();
        }
        awaitPendingFlush();
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
        for (T element : elements) {
            add(element);
        }
    }

    @Override
    public boolean isEmpty() {
        if (!enabled) {
            return delegate.isEmpty();
        }
        awaitPendingFlush();
        return delegate.isEmpty();
    }

    @Override
    public int size() {
        if (!enabled) {
            return delegate.size();
        }
        awaitPendingFlush();
        return delegate.size();
    }

    @Nonnull
    @Override
    public CloseableIterator<T> iterator() {
        if (!enabled) {
            return delegate.iterator();
        }
        awaitPendingFlush();
        return delegate.iterator();
    }

    @Nonnull
    @Override
    public Set<T> getSubsetForKeyGroup(int keyGroupId) {
        // Delegate handles key-group partitioning
        return delegate.getSubsetForKeyGroup(keyGroupId);
    }

    // ------------------------------------------------------------------------
    //  Internal helpers
    // ------------------------------------------------------------------------

    private void flushAddsAsync() {
        final List<T> snapshot;
        synchronized (pendingAdds) {
            if (pendingAdds.isEmpty()) return;
            snapshot = new ArrayList<>(pendingAdds);
            pendingAdds.clear();
            pendingAddSize.set(0);
        }

        if (flushExecutor == null) {
            // Fallback: flush synchronously
            for (T element : snapshot) {
                delegate.add(element);
            }
            return;
        }

        CompletableFuture.runAsync(
                        () -> {
                            for (T element : snapshot) {
                                delegate.add(element);
                            }
                        },
                        flushExecutor)
                .whenComplete(
                        (r, ex) -> {
                            if (ex != null) {
                                LOG.error("[CACHEKIT PQ] Async add flush failed", ex);
                            }
                        });
        trackInFlight();
    }

    private void flushRemovesAsync() {
        final List<T> snapshot;
        synchronized (pendingRemoves) {
            if (pendingRemoves.isEmpty()) return;
            snapshot = new ArrayList<>(pendingRemoves);
            pendingRemoves.clear();
            pendingRemoveSize.set(0);
        }

        if (flushExecutor == null) {
            // Fallback: flush synchronously
            for (T element : snapshot) {
                delegate.remove(element);
            }
            return;
        }

        CompletableFuture.runAsync(
                        () -> {
                            for (T element : snapshot) {
                                delegate.remove(element);
                            }
                        },
                        flushExecutor)
                .whenComplete(
                        (r, ex) -> {
                            if (ex != null) {
                                LOG.error("[CACHEKIT PQ] Async remove flush failed", ex);
                            }
                        });
        trackInFlight();
    }

    private void flushAddsSync() {
        synchronized (pendingAdds) {
            if (pendingAdds.isEmpty()) return;
            for (T element : pendingAdds) {
                delegate.add(element);
            }
            pendingAdds.clear();
            pendingAddSize.set(0);
        }
    }

    private void flushRemovesSync() {
        synchronized (pendingRemoves) {
            if (pendingRemoves.isEmpty()) return;
            for (T element : pendingRemoves) {
                delegate.remove(element);
            }
            pendingRemoves.clear();
            pendingRemoveSize.set(0);
        }
    }

    private void awaitPendingFlush() {
        CompletableFuture<?> f = inFlightFlush[0];
        if (f != null && !f.isDone()) {
            try {
                f.join();
            } catch (Exception e) {
                throw new FlinkRuntimeException("Failed to await pending PQ flush", e);
            }
        }
    }

    private void trackInFlight() {
        synchronized (inFlightFlush) {
            CompletableFuture<?> prev = inFlightFlush[0];
            if (prev == null || prev.isDone()) {
                inFlightFlush[0] = CompletableFuture.completedFuture(null);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Checkpoint / lifecycle
    // ------------------------------------------------------------------------

    /**
     * Flushes all pending operations to the delegate synchronously. Called by the keyed state backend
     * during snapshots.
     */
    public void flushAllPending() {
        awaitPendingFlush();
        flushAddsSync();
        flushRemovesSync();
    }

    /**
     * Closes the wrapper. Called by {@code CacheKitKeyedStateBackend.dispose()}. The executor is shut
     * down by the backend, not here.
     */
    public void close() {
        flushAllPending();
        LOG.info("[CACHEKIT PQ] CachedInternalPriorityQueueSet closed");
    }

    // ------------------------------------------------------------------------
    //  Passthrough to delegate (priority queue metadata)
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "CachedInternalPriorityQueueSet{delegate="
                + delegate
                + ", enabled="
                + enabled
                + ", pendingAddsSize="
                + pendingAddSize.get()
                + ", pendingRemovesSize="
                + pendingRemoveSize.get()
                + '}';
    }
}
