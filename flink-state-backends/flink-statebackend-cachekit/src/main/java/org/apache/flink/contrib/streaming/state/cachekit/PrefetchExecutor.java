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

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.GlobalConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

    private static final Logger LOG = LoggerFactory.getLogger(PrefetchExecutor.class);

    static final String WORKER_THREAD_NAME = "cachekit-bp-prefetch";
    static final String WORKER_NATIVE_THREAD_NAME = "cachekit-bp-pre";

    private static final String WORKER_CPU_LIST = loadWorkerCpuList();

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
                            Thread t =
                                    new Thread(
                                            () -> {
                                                LinuxThreadAffinity.BindingResult result =
                                                        LinuxThreadAffinity.bindCurrentThread(
                                                                WORKER_NATIVE_THREAD_NAME,
                                                                WORKER_CPU_LIST);
                                                if (result.isEnabled()) {
                                                    if (result.isSuccess()) {
                                                        LOG.info(
                                                                "CACHEKIT_BP_PREFETCH_AFFINITY "
                                                                        + "status=bound tid={} "
                                                                        + "requested={} observed={}",
                                                                result.getTid(),
                                                                result.getRequestedCpuList(),
                                                                result.getObservedCpuList());
                                                    } else {
                                                        LOG.error(
                                                                "CACHEKIT_BP_PREFETCH_AFFINITY "
                                                                        + "status=failed requested={} reason={}",
                                                                result.getRequestedCpuList(),
                                                                result.getReason());
                                                    }
                                                }
                                                runnable.run();
                                            },
                                            WORKER_THREAD_NAME);
                            t.setDaemon(true);
                            return t;
                        },
                        DISCARD_OLDEST_WITH_NOTIFICATION);
        EXECUTOR.allowCoreThreadTimeOut(true);
    }

    private PrefetchExecutor() {}

    private static String loadWorkerCpuList() {
        String perTaskManagerOverride = System.getenv("CACHEKIT_BP_PREFETCH_CPU_LIST");
        if (perTaskManagerOverride != null && !perTaskManagerOverride.trim().isEmpty()) {
            return perTaskManagerOverride.trim();
        }
        return GlobalConfiguration.loadConfiguration()
                .get(
                        ConfigOptions.key("state.backend.cachekit.bp-prefetch.affinity.cpu-list")
                                .stringType()
                                .defaultValue(""));
    }

    /** Non-blocking, best-effort submission; failures never reach the mailbox thread. */
    public static void trySubmit(Runnable task) {
        try {
            EXECUTOR.execute(task);
        } catch (Throwable ignored) {
            notifyDropped(task);
            // Best-effort: dropping a prefetch is always safe once reservations are released.
        }
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
