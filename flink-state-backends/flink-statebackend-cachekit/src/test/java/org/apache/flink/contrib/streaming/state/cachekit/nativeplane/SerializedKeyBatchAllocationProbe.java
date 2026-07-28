/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.nativeplane;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import com.sun.management.ThreadMXBean;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;

/** Manual allocation probe for append-clear reuse of a one-entry serialized-key batch. */
public final class SerializedKeyBatchAllocationProbe {

    private static final int WARMUP_ITERATIONS = 1_000_000;
    private static final int MEASURED_ITERATIONS = 5_000_000;
    private static final Integer KEY = 42;
    private static final String NAMESPACE = "tenant";

    private SerializedKeyBatchAllocationProbe() {}

    public static void main(String[] args) throws Exception {
        ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!bean.isThreadAllocatedMemorySupported()) {
            throw new IllegalStateException("Thread allocation measurement is not supported.");
        }
        bean.setThreadAllocatedMemoryEnabled(true);

        SerializedKeyBatch<Integer, String> batch =
                new SerializedKeyBatch<>(
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        ByteBuffer.allocateDirect(64),
                        ByteBuffer.allocateDirect(SerializedKeyBatch.METADATA_RECORD_BYTES));
        int sink = run(batch, WARMUP_ITERATIONS);

        long threadId = Thread.currentThread().getId();
        long before = bean.getThreadAllocatedBytes(threadId);
        sink += run(batch, MEASURED_ITERATIONS);
        long allocatedBytes = bean.getThreadAllocatedBytes(threadId) - before;

        System.out.printf(
                "iterations=%d allocated_bytes=%d bytes_per_append=%.3f sink=%d%n",
                MEASURED_ITERATIONS,
                allocatedBytes,
                (double) allocatedBytes / MEASURED_ITERATIONS,
                sink);
        if (allocatedBytes != 0) {
            throw new AssertionError("Batch append-clear allocated " + allocatedBytes + " B");
        }
    }

    private static int run(SerializedKeyBatch<Integer, String> batch, int iterations)
            throws IOException {
        int sink = 0;
        for (int iteration = 0; iteration < iterations; iteration++) {
            batch.clear();
            batch.append(iteration, iteration, KEY, NAMESPACE);
            sink += batch.entryCount() + batch.arenaBytesWritten() + batch.stateId(0);
        }
        return sink;
    }
}
