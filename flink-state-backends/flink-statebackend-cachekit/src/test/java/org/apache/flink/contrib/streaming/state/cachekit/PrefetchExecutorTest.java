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

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrefetchExecutorTest {

    @Test
    void cancelIfQueuedFindsCompletionWrapperByDelegateIdentity() throws Exception {
        CountDownLatch workerStarted = new CountDownLatch(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        PrefetchExecutor.trySubmit(
                () -> {
                    workerStarted.countDown();
                    try {
                        releaseWorker.await();
                    } catch (InterruptedException failure) {
                        Thread.currentThread().interrupt();
                    }
                });
        try {
            assertTrue(workerStarted.await(10, TimeUnit.SECONDS));
            PrefetchExecutor.DropAwareTask delegate =
                    new PrefetchExecutor.DropAwareTask() {
                        @Override
                        public void run() {}

                        @Override
                        public void onDrop() {}
                    };
            CompletableFuture<Void> completion = PrefetchExecutor.submitWithCompletion(delegate);
            assertTrue(PrefetchExecutor.cancelIfQueued(delegate));
            assertThrows(CompletionException.class, completion::join);
        } finally {
            releaseWorker.countDown();
        }
    }

    @Test
    void completionTaskCompletesOnlyAfterDelegateReturns() {
        AtomicBoolean ran = new AtomicBoolean();
        PrefetchExecutor.CompletionTask task =
                new PrefetchExecutor.CompletionTask(() -> ran.set(true));

        assertTrue(!task.completionForTesting().isDone());
        task.run();

        assertTrue(ran.get());
        assertDoesNotThrow(() -> task.completionForTesting().join());
    }

    @Test
    void completionTaskPropagatesWorkerFailure() {
        PrefetchExecutor.CompletionTask task =
                new PrefetchExecutor.CompletionTask(
                        () -> {
                            throw new IllegalStateException("worker failed");
                        });

        task.run();

        assertThrows(CompletionException.class, () -> task.completionForTesting().join());
    }

    @Test
    void droppedCompletionTaskReleasesDelegateBeforeWakingWaiter() {
        AtomicBoolean released = new AtomicBoolean();
        PrefetchExecutor.DropAwareTask delegate =
                new PrefetchExecutor.DropAwareTask() {
                    @Override
                    public void run() {}

                    @Override
                    public void onDrop() {
                        released.set(true);
                    }
                };
        PrefetchExecutor.CompletionTask task = new PrefetchExecutor.CompletionTask(delegate);

        task.onDrop();

        assertTrue(released.get());
        assertThrows(CompletionException.class, () -> task.completionForTesting().join());
    }
}
