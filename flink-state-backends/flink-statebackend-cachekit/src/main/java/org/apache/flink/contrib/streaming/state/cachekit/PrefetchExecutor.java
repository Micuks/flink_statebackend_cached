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
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared bounded executors for backpressure-driven async state prefetch.
 *
 * <p>The generic path retains one daemon thread per TaskManager JVM. A separately gated exact-map
 * deferred-wave path may use two daemon workers because those tasks contain only immutable encoded
 * RocksDB keys and publish an all-or-none raw value vector. Both paths are shared by all CacheKit
 * backends and remain bounded. Submission never blocks the mailbox thread: when a queue is full,
 * the oldest pending prefetch is discarded — a fresher lookahead window is always worth more than
 * a stale one. The explicit work-first path may instead execute one eligible direct-arena task on a
 * mailbox caller while the generic worker has backlog.
 */
public final class PrefetchExecutor {

    /** A queued prefetch that must release any key reservations if the executor drops it. */
    public interface DropAwareTask extends Runnable {
        void onDrop();
    }

    /** Marker for a task whose state and native buffers are safe on a mailbox caller. */
    public interface WorkFirstEligibleTask extends DropAwareTask {}

    /** Marker for an immutable exact-map wave that is safe on the isolated two-worker executor. */
    public interface DeferredWaveEligibleTask extends DropAwareTask {}

    public enum WorkFirstSubmission {
        CALLER_RUN,
        QUEUED_WORKER_IDLE,
        QUEUED_BACKLOG_EMPTY,
        QUEUED_PERMIT_BUSY
    }

    /**
     * Keeps the newest lookahead without leaking per-state in-flight reservations. The JDK's {@link
     * ThreadPoolExecutor.DiscardOldestPolicy} silently forgets the evicted task; CacheKit needs a
     * callback so a later chunk can prefetch those keys again.
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
    private static final ThreadPoolExecutor DEFERRED_WAVE_EXECUTOR;
    private static final Semaphore CALLER_RUN_PERMIT = new Semaphore(1);
    private static final AtomicInteger ACTIVE_EXECUTIONS = new AtomicInteger();
    private static final AtomicInteger MAX_ACTIVE_EXECUTIONS = new AtomicInteger();
    private static final AtomicInteger ACTIVE_DEFERRED_WAVE_EXECUTIONS = new AtomicInteger();
    private static final AtomicInteger MAX_ACTIVE_DEFERRED_WAVE_EXECUTIONS = new AtomicInteger();
    private static final AtomicInteger DEFERRED_WAVE_THREAD_ID = new AtomicInteger();

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
                        DISCARD_OLDEST_WITH_NOTIFICATION) {
                    @Override
                    protected void beforeExecute(Thread thread, Runnable task) {
                        super.beforeExecute(thread, task);
                        recordExecutionStarted();
                    }

                    @Override
                    protected void afterExecute(Runnable task, Throwable failure) {
                        try {
                            recordExecutionFinished();
                        } finally {
                            super.afterExecute(task, failure);
                        }
                    }
                };
        EXECUTOR.allowCoreThreadTimeOut(true);
        DEFERRED_WAVE_EXECUTOR =
                new ThreadPoolExecutor(
                        2,
                        2,
                        60L,
                        TimeUnit.SECONDS,
                        new ArrayBlockingQueue<>(128),
                        runnable -> {
                            Thread t =
                                    new Thread(
                                            runnable,
                                            "cachekit-map-wave-"
                                                    + DEFERRED_WAVE_THREAD_ID.incrementAndGet());
                            t.setDaemon(true);
                            return t;
                        },
                        DISCARD_OLDEST_WITH_NOTIFICATION) {
                    @Override
                    protected void beforeExecute(Thread thread, Runnable task) {
                        super.beforeExecute(thread, task);
                        int active = ACTIVE_DEFERRED_WAVE_EXECUTIONS.incrementAndGet();
                        MAX_ACTIVE_DEFERRED_WAVE_EXECUTIONS.accumulateAndGet(active, Math::max);
                    }

                    @Override
                    protected void afterExecute(Runnable task, Throwable failure) {
                        try {
                            ACTIVE_DEFERRED_WAVE_EXECUTIONS.decrementAndGet();
                        } finally {
                            super.afterExecute(task, failure);
                        }
                    }
                };
        DEFERRED_WAVE_EXECUTOR.allowCoreThreadTimeOut(true);
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
     * Non-blocking submission to the isolated two-worker exact-map wave executor.
     *
     * <p>Only callers that have already frozen every key and can fall back authoritatively may use
     * this path. Keeping it separate prevents unrelated ValueState prefetch from gaining an
     * unreviewed concurrency change.
     */
    public static void trySubmitDeferredWave(DeferredWaveEligibleTask task) {
        try {
            DEFERRED_WAVE_EXECUTOR.execute(task);
        } catch (Throwable ignored) {
            notifyDropped(task);
        }
    }

    /**
     * Runs a fresh lookahead on the caller when the shared worker already has both active and
     * queued work; otherwise submits it normally.
     *
     * <p>This work-first policy keeps one unit of queued lookahead while allowing the mailbox and
     * the prefetch worker to make progress concurrently. It is deliberately opt-in at the state
     * wrapper: generic prefetch users retain the non-blocking submission contract.
     *
     * @return whether the task ran on the caller or why it retained normal queue submission
     */
    public static WorkFirstSubmission trySubmitWorkFirst(WorkFirstEligibleTask task) {
        int activeCount = EXECUTOR.getActiveCount();
        int queuedCount = EXECUTOR.getQueue().size();
        if (activeCount <= 0) {
            trySubmit(task);
            return WorkFirstSubmission.QUEUED_WORKER_IDLE;
        }
        if (!shouldRunInline(activeCount, queuedCount)) {
            trySubmit(task);
            return WorkFirstSubmission.QUEUED_BACKLOG_EMPTY;
        }
        if (!CALLER_RUN_PERMIT.tryAcquire()) {
            trySubmit(task);
            return WorkFirstSubmission.QUEUED_PERMIT_BUSY;
        }
        recordExecutionStarted();
        try {
            try {
                task.run();
            } catch (Throwable ignored) {
                notifyDropped(task);
            }
        } finally {
            recordExecutionFinished();
            CALLER_RUN_PERMIT.release();
        }
        return WorkFirstSubmission.CALLER_RUN;
    }

    static boolean shouldRunInline(int activeCount, int queuedCount) {
        return activeCount > 0 && queuedCount > 0;
    }

    public static int maxActiveExecutions() {
        return MAX_ACTIVE_EXECUTIONS.get();
    }

    public static int maxActiveDeferredWaveExecutions() {
        return MAX_ACTIVE_DEFERRED_WAVE_EXECUTIONS.get();
    }

    private static void recordExecutionStarted() {
        int active = ACTIVE_EXECUTIONS.incrementAndGet();
        MAX_ACTIVE_EXECUTIONS.accumulateAndGet(active, Math::max);
    }

    private static void recordExecutionFinished() {
        ACTIVE_EXECUTIONS.decrementAndGet();
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
        if (task == null) {
            return false;
        }
        boolean removed = EXECUTOR.remove(task) || DEFERRED_WAVE_EXECUTOR.remove(task);
        if (!removed) {
            return false;
        }
        notifyDropped(task);
        return true;
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
}
