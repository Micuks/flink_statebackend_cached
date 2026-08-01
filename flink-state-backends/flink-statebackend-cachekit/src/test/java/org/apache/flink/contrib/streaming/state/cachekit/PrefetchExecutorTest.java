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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrefetchExecutorTest {

    @Test
    void initializeAffinityPrestartsExactlyOnePersistentWorker() throws Exception {
        int callers = 16;
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            Thread thread =
                    new Thread(
                            () -> {
                                try {
                                    start.await();
                                    PrefetchExecutor.initializeAffinity();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            },
                            "prefetch-init-" + i);
            thread.start();
            threads.add(thread);
        }

        start.countDown();
        for (Thread thread : threads) {
            thread.join(5_000L);
            assertFalse(thread.isAlive());
        }

        assertEquals(1, PrefetchExecutor.getWorkerCountForTesting());
        assertFalse(PrefetchExecutor.getCoreThreadTimeoutForTesting());
        if (LinuxThreadAffinity.isLinux()) {
            assertTrue(
                    LinuxThreadAffinity.findUniqueTid(
                                    LinuxThreadAffinity.PROC_SELF_TASK,
                                    PrefetchExecutor.WORKER_NATIVE_THREAD_NAME)
                            .isPresent());
        }

        PrefetchExecutor.initializeAffinity();
        assertEquals(1, PrefetchExecutor.getWorkerCountForTesting());
    }

    @Test
    void prestartedWorkerRetainsExactlyOnceSubmissionSemantics() throws Exception {
        PrefetchExecutor.initializeAffinity();
        AtomicInteger executions = new AtomicInteger();
        CountDownLatch executed = new CountDownLatch(1);

        PrefetchExecutor.trySubmit(
                () -> {
                    executions.incrementAndGet();
                    executed.countDown();
                });

        assertTrue(executed.await(5L, TimeUnit.SECONDS));
        assertEquals(1, executions.get());
        assertEquals(1, PrefetchExecutor.getWorkerCountForTesting());
    }
}
