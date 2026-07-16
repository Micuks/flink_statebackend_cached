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

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArmSpscStagingBufferTest {

    @Test
    void testSingleProducerSingleConsumerTransfersAllEntriesInOrder() throws Exception {
        final int entries = 100_000;
        ArmSpscStagingBuffer<Integer> buffer = new ArmSpscStagingBuffer<>(256);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread producer =
                new Thread(
                        () -> {
                            try {
                                start.await();
                                for (int i = 0; i < entries; i++) {
                                    while (!buffer.offer(i)) {
                                        Thread.yield();
                                    }
                                }
                            } catch (Throwable t) {
                                failure.compareAndSet(null, t);
                            }
                        },
                        "spsc-test-producer");
        Thread consumer =
                new Thread(
                        () -> {
                            try {
                                start.await();
                                for (int expected = 0; expected < entries; ) {
                                    Integer actual = buffer.poll();
                                    if (actual == null) {
                                        Thread.yield();
                                    } else if (actual != expected) {
                                        throw new AssertionError(
                                                "expected " + expected + " but got " + actual);
                                    } else {
                                        expected++;
                                    }
                                }
                            } catch (Throwable t) {
                                failure.compareAndSet(null, t);
                            }
                        },
                        "spsc-test-consumer");

        producer.start();
        consumer.start();
        start.countDown();
        producer.join(TimeUnit.SECONDS.toMillis(10));
        consumer.join(TimeUnit.SECONDS.toMillis(10));

        assertTrue(!producer.isAlive() && !consumer.isAlive(), "threads did not finish");
        assertNull(failure.get());
        assertNull(buffer.poll());
    }
}
