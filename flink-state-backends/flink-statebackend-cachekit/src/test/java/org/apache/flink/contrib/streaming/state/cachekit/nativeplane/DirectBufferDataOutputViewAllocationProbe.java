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

import com.sun.management.ThreadMXBean;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;

/** Manual allocation probe for the successful direct-buffer field-write path. */
public final class DirectBufferDataOutputViewAllocationProbe {

    private static final int WARMUP_ITERATIONS = 1_000_000;
    private static final int MEASURED_ITERATIONS = 5_000_000;
    private static final byte[] PAYLOAD = {1, 2, 3, 4};

    private DirectBufferDataOutputViewAllocationProbe() {}

    public static void main(String[] args) throws Exception {
        ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!bean.isThreadAllocatedMemorySupported()) {
            throw new IllegalStateException("Thread allocation measurement is not supported.");
        }
        bean.setThreadAllocatedMemoryEnabled(true);

        DirectBufferDataOutputView output =
                new DirectBufferDataOutputView(ByteBuffer.allocateDirect(64));
        int sink = run(output, WARMUP_ITERATIONS);

        long threadId = Thread.currentThread().getId();
        long before = bean.getThreadAllocatedBytes(threadId);
        sink += run(output, MEASURED_ITERATIONS);
        long allocatedBytes = bean.getThreadAllocatedBytes(threadId) - before;

        System.out.printf(
                "iterations=%d allocated_bytes=%d bytes_per_iteration=%.3f sink=%d%n",
                MEASURED_ITERATIONS,
                allocatedBytes,
                (double) allocatedBytes / MEASURED_ITERATIONS,
                sink);
        if (allocatedBytes != 0) {
            throw new AssertionError("Successful field writes allocated " + allocatedBytes + " B");
        }
    }

    private static int run(DirectBufferDataOutputView output, int iterations) throws IOException {
        int sink = 0;
        for (int iteration = 0; iteration < iterations; iteration++) {
            output.reset();
            output.writeInt(iteration);
            output.writeLong(0x0123456789abcdefL);
            output.writeBoolean((iteration & 1) == 0);
            output.write(PAYLOAD);
            output.writeUTF("native");
            sink += output.position();
        }
        return sink;
    }
}
