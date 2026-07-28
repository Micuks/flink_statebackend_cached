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

package org.apache.flink.contrib.streaming.state;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Opt-in direct-buffer read contract used by CacheKit's Direct State Transit Lane.
 *
 * <p>The caller owns both arenas and all descriptor arrays. Implementations must not retain any
 * buffer or array after a method returns. A batch is all-or-nothing from the caller's point of view:
 * implementations may write scratch bytes before failing, but the caller must not publish any value
 * until the complete method returns successfully.
 */
public interface RocksDBDirectValueAccess<K, N> {

    int NOT_FOUND = -1;

    /**
     * Serializes the exact RocksDB composite key for {@code (key, namespace)} at the target's
     * current position.
     *
     * @return number of bytes written
     */
    int writeKeyAndNamespace(K key, N namespace, ByteBuffer target) throws IOException;

    /**
     * Reads a batch of exact RocksDB keys from a direct key arena into fixed-size slots in a direct
     * value arena.
     *
     * <p>{@code valueLengths[i]} is set to {@link #NOT_FOUND} for an absent key and to a non-negative
     * serialized length for a present value. Empty values therefore remain distinguishable from
     * misses. If any value is larger than {@code valueStride}, the entire batch must be treated as a
     * fallback by the caller.
     */
    void readValueBatch(
            ByteBuffer keyArena,
            int[] keyOffsets,
            int[] keyLengths,
            int count,
            ByteBuffer valueArena,
            int valueStride,
            int[] valueLengths)
            throws IOException, ValueTooLargeException;

    /** Signals that a fixed direct-value slot cannot contain an authoritative RocksDB value. */
    final class ValueTooLargeException extends IOException {
        private static final long serialVersionUID = 1L;

        private final int index;
        private final int requiredBytes;
        private final int availableBytes;

        public ValueTooLargeException(int index, int requiredBytes, int availableBytes) {
            super(
                    "Direct value slot "
                            + index
                            + " requires "
                            + requiredBytes
                            + " bytes, but only "
                            + availableBytes
                            + " are available.");
            this.index = index;
            this.requiredBytes = requiredBytes;
            this.availableBytes = availableBytes;
        }

        public int getIndex() {
            return index;
        }

        public int getRequiredBytes() {
            return requiredBytes;
        }

        public int getAvailableBytes() {
            return availableBytes;
        }
    }
}
