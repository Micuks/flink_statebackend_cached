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

package org.apache.flink.contrib.streaming.state.cachekit.state;

import java.lang.management.ManagementFactory;
import java.util.Objects;

/**
 * Explicit steady-state allocation probe for cache-key hashing.
 *
 * <p>This is deliberately not a unit test: run it after {@code mvn test-compile} with:
 *
 * <pre>
 * java -cp target/test-classes:target/classes \
 *   org.apache.flink.contrib.streaming.state.cachekit.state.CacheKeyHashAllocationProbe
 * </pre>
 */
public final class CacheKeyHashAllocationProbe {

    private static final int ITERATIONS = 5_000_000;
    private static volatile int sink;

    private CacheKeyHashAllocationProbe() {}

    public static void main(String[] args) {
        com.sun.management.ThreadMXBean bean =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!bean.isThreadAllocatedMemorySupported()) {
            throw new IllegalStateException("Thread allocated-memory accounting is unavailable");
        }
        bean.setThreadAllocatedMemoryEnabled(true);

        for (int i = 0; i < 5; i++) {
            runObjectsHash();
            runCacheKeyHash();
        }

        long objectsHashBytes = allocatedBytes(bean, CacheKeyHashAllocationProbe::runObjectsHash);
        long cacheKeyHashBytes =
                allocatedBytes(bean, CacheKeyHashAllocationProbe::runCacheKeyHash);
        System.out.printf(
                "iterations=%d objects_hash_bytes=%d cache_key_hash_bytes=%d sink=%d%n",
                ITERATIONS, objectsHashBytes, cacheKeyHashBytes, sink);
    }

    private static long allocatedBytes(
            com.sun.management.ThreadMXBean bean, Runnable operation) {
        long threadId = Thread.currentThread().getId();
        long before = bean.getThreadAllocatedBytes(threadId);
        operation.run();
        return bean.getThreadAllocatedBytes(threadId) - before;
    }

    private static void runObjectsHash() {
        int value = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            value += Objects.hash("key", "namespace");
        }
        sink = value;
    }

    private static void runCacheKeyHash() {
        int value = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            value += CacheKeyHash.hash("key", "namespace");
        }
        sink = value;
    }
}
