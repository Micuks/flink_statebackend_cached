/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared single-thread executor for backpressure-driven async state prefetch.
 *
 * <p>One daemon thread per TaskManager JVM, shared by all CacheKit backends, so the prefetch
 * side-work stays bounded no matter how many operators enable it (the benchmark's primary metric is
 * throughput <em>per core</em>). Submission never blocks the mailbox thread: when the queue is
 * full, the oldest pending prefetch is discarded — a fresher lookahead window is always worth more
 * than a stale one.
 */
public final class PrefetchExecutor {

    /** A queued prefetch that must release any key reservations if the executor drops it. */
    public interface DropAwareTask extends Runnable {
        void onDrop();
    }

    /**
     * Keeps the newest lookahead without leaking per-state in-flight reservations. The JDK's
     * {@link ThreadPoolExecutor.DiscardOldestPolicy} silently forgets the evicted task; CacheKit
     * needs a callback so a later chunk can prefetch those keys again.
     */
    private static final RejectedExecutionHandler DISCARD_OLDEST_WITH_NOTIFICATION =
            (incoming, executor) -> {
                if (executor.isShutdown()) {
                    notifyDropped(incoming);
                    return;
                }
                Runnable dropped = executor.getQueue().poll();
                notifyDropped(dropped);
                if (!executor.getQueue().offer(incoming)) {
                    // Another producer filled the single freed slot first. Dropping the incoming
                    // lookahead is safe, but its reservations still need releasing.
                    notifyDropped(incoming);
                }
            };

    private static final ThreadPoolExecutor EXECUTOR;

    static {
        EXECUTOR =
                new ThreadPoolExecutor(
                        1,
                        1,
                        60L,
                        TimeUnit.SECONDS,
                        new ArrayBlockingQueue<>(128),
                        runnable -> {
                            Thread t = new Thread(runnable, "cachekit-bp-prefetch");
                            t.setDaemon(true);
                            return t;
                        },
                        DISCARD_OLDEST_WITH_NOTIFICATION);
        EXECUTOR.allowCoreThreadTimeOut(true);
    }

    private PrefetchExecutor() {}

    /** Non-blocking, best-effort submission; failures never reach the mailbox thread. */
    public static void trySubmit(Runnable task) {
        try {
            EXECUTOR.execute(task);
        } catch (Throwable ignored) {
            notifyDropped(task);
            // Best-effort: dropping a prefetch is always safe once reservations are released.
        }
    }

    /**
     * Non-blocking submission whose future is completed only after the worker task has returned.
     *
     * <p>Unlike {@link #trySubmit(Runnable)}, this method makes executor rejection, queue eviction,
     * and worker failure observable to a ready gate. The wrapper forwards a drop notification to
     * the state-owned task first, so every reservation is released before the future wakes the
     * mailbox.
     */
    public static CompletableFuture<Void> submitWithCompletion(Runnable task) {
        CompletionTask completionTask = new CompletionTask(task);
        try {
            EXECUTOR.execute(completionTask);
        } catch (Throwable failure) {
            completionTask.onRejected(failure);
        }
        return completionTask.completion;
    }

    /**
     * Removes one exact task from the shared queue and runs its drop callback.
     *
     * <p>This is deliberately identity based: {@link ThreadPoolExecutor#remove(Runnable)} uses the
     * task object's {@code equals}, and CacheKit's tracked tasks retain object identity. A running
     * task cannot be removed and must instead observe its state-local cancellation flag and drain
     * normally. The method is used by ValueState close so an unrelated backend's queued work never
     * delays native-plane teardown.
     */
    public static boolean cancelIfQueued(Runnable task) {
        Runnable queuedTask = findQueuedTask(task);
        if (queuedTask == null || !EXECUTOR.remove(queuedTask)) {
            return false;
        }
        notifyDropped(queuedTask);
        return true;
    }

    private static Runnable findQueuedTask(Runnable task) {
        if (task == null) {
            return null;
        }
        for (Runnable queued : EXECUTOR.getQueue()) {
            if (queued == task
                    || (queued instanceof CompletionTask
                            && ((CompletionTask) queued).wraps(task))) {
                return queued;
            }
        }
        return null;
    }

    private static void notifyDropped(Runnable task) {
        if (task instanceof DropAwareTask) {
            try {
                ((DropAwareTask) task).onDrop();
            } catch (Throwable ignored) {
                // Diagnostics and reservation cleanup must never reach the mailbox thread.
            }
        }
    }

    /** Package-private so the executor's terminal races can be tested without filling its queue. */
    static final class CompletionTask implements DropAwareTask {
        private final Runnable delegate;
        private final CompletableFuture<Void> completion = new CompletableFuture<>();
        // 0=pending, 1=running, 2=terminal. A queue drop can only win from pending.
        private final AtomicInteger state = new AtomicInteger();

        CompletionTask(Runnable delegate) {
            if (delegate == null) {
                throw new NullPointerException("prefetch task");
            }
            this.delegate = delegate;
        }

        @Override
        public void run() {
            if (!state.compareAndSet(0, 1)) {
                return;
            }
            try {
                delegate.run();
                completion.complete(null);
            } catch (Throwable failure) {
                completion.completeExceptionally(failure);
            } finally {
                state.set(2);
            }
        }

        @Override
        public void onDrop() {
            if (!state.compareAndSet(0, 2)) {
                return;
            }
            notifyDropped(delegate);
            completion.completeExceptionally(
                    new RejectedExecutionException("prefetch task was dropped before execution"));
        }

        private void onRejected(Throwable failure) {
            if (!state.compareAndSet(0, 2)) {
                return;
            }
            notifyDropped(delegate);
            completion.completeExceptionally(failure);
        }

        CompletableFuture<Void> completionForTesting() {
            return completion;
        }

        private boolean wraps(Runnable task) {
            return delegate == task;
        }
    }
}
