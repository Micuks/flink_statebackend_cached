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
                        new ThreadPoolExecutor.DiscardOldestPolicy());
        EXECUTOR.allowCoreThreadTimeOut(true);
    }

    private PrefetchExecutor() {}

    /** Non-blocking, best-effort submission; failures never reach the mailbox thread. */
    public static void trySubmit(Runnable task) {
        try {
            EXECUTOR.execute(task);
        } catch (Throwable ignored) {
            // Best-effort: dropping a prefetch is always safe.
        }
    }
}
