/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class PrefetchExecutorTest {

    @Test
    void workFirstRequiresBothActiveWorkerAndQueuedBacklog() {
        assertFalse(PrefetchExecutor.shouldRunInline(0, 0));
        assertFalse(PrefetchExecutor.shouldRunInline(1, 0));
        assertFalse(PrefetchExecutor.shouldRunInline(0, 1));
        assertTrue(PrefetchExecutor.shouldRunInline(1, 1));
        assertTrue(PrefetchExecutor.shouldRunInline(2, 3));
    }

    @Test
    void workFirstRunsOneBoundedCallerAlongsideWorkerAndQueuesSecondCaller() throws Exception {
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        CountDownLatch queuedCompleted = new CountDownLatch(1);
        CountDownLatch callerStarted = new CountDownLatch(1);
        CountDownLatch releaseCaller = new CountDownLatch(1);
        CountDownLatch secondCompleted = new CountDownLatch(1);
        AtomicReference<PrefetchExecutor.WorkFirstSubmission> firstSubmission =
                new AtomicReference<>();

        PrefetchExecutor.trySubmit(
                () -> {
                    workerStarted.countDown();
                    await(releaseWorker);
                });
        assertTrue(workerStarted.await(5, TimeUnit.SECONDS));
        PrefetchExecutor.trySubmit(queuedCompleted::countDown);

        Thread caller =
                new Thread(
                        () ->
                                firstSubmission.set(
                                        PrefetchExecutor.trySubmitWorkFirst(
                                                eligible(
                                                        () -> {
                                                            callerStarted.countDown();
                                                            await(releaseCaller);
                                                        }))),
                        "mailbox-test-caller");
        try {
            caller.start();
            assertTrue(callerStarted.await(5, TimeUnit.SECONDS));

            PrefetchExecutor.WorkFirstSubmission secondSubmission =
                    PrefetchExecutor.trySubmitWorkFirst(eligible(secondCompleted::countDown));
            assertEquals(PrefetchExecutor.WorkFirstSubmission.QUEUED_PERMIT_BUSY, secondSubmission);
            assertEquals(2, PrefetchExecutor.maxActiveExecutions());

            releaseCaller.countDown();
            caller.join(5000L);
            assertFalse(caller.isAlive());
            assertEquals(PrefetchExecutor.WorkFirstSubmission.CALLER_RUN, firstSubmission.get());
            releaseWorker.countDown();
            assertTrue(queuedCompleted.await(5, TimeUnit.SECONDS));
            assertTrue(secondCompleted.await(5, TimeUnit.SECONDS));
        } finally {
            releaseCaller.countDown();
            releaseWorker.countDown();
            caller.join(5000L);
        }
    }

    @Test
    void workFirstReleasesCallerPermitAfterTaskFailure() throws Exception {
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        CountDownLatch queuedCompleted = new CountDownLatch(1);
        AtomicInteger dropped = new AtomicInteger();
        try {
            PrefetchExecutor.trySubmit(
                    () -> {
                        workerStarted.countDown();
                        await(releaseWorker);
                    });
            assertTrue(workerStarted.await(5, TimeUnit.SECONDS));
            PrefetchExecutor.trySubmit(queuedCompleted::countDown);

            PrefetchExecutor.WorkFirstSubmission failedSubmission =
                    PrefetchExecutor.trySubmitWorkFirst(
                            eligible(
                                    () -> {
                                        throw new IllegalStateException("expected test failure");
                                    },
                                    dropped::incrementAndGet));
            assertEquals(PrefetchExecutor.WorkFirstSubmission.CALLER_RUN, failedSubmission);
            assertEquals(1, dropped.get());

            AtomicInteger completed = new AtomicInteger();
            PrefetchExecutor.WorkFirstSubmission recoveredSubmission =
                    PrefetchExecutor.trySubmitWorkFirst(eligible(completed::incrementAndGet));
            assertEquals(PrefetchExecutor.WorkFirstSubmission.CALLER_RUN, recoveredSubmission);
            assertEquals(1, completed.get());
            assertEquals(2, PrefetchExecutor.maxActiveExecutions());
        } finally {
            releaseWorker.countDown();
            assertTrue(queuedCompleted.await(5, TimeUnit.SECONDS));
        }
    }

    private static PrefetchExecutor.WorkFirstEligibleTask eligible(Runnable action) {
        return eligible(action, () -> {});
    }

    private static PrefetchExecutor.WorkFirstEligibleTask eligible(
            Runnable action, Runnable onDrop) {
        return new PrefetchExecutor.WorkFirstEligibleTask() {
            @Override
            public void run() {
                action.run();
            }

            @Override
            public void onDrop() {
                onDrop.run();
            }
        };
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
