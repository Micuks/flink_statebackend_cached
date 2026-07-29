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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Opt-in direct-buffer read contract used by CacheKit's Direct State Transit Lane.
 *
 * <p>The caller owns both arenas and all descriptor arrays. Implementations must not retain any
 * buffer or array after a method returns. A batch is all-or-nothing from the caller's point of
 * view: implementations may write scratch bytes before failing, but the caller must not publish any
 * value until the complete method returns successfully.
 */
@Internal
public interface RocksDBDirectValueAccess<K, N> {

    int NOT_FOUND = -1;
    int VALUE_OVERFLOW = -2;
    int MAX_BATCH_ENTRIES = 64;
    int DESCRIPTOR_BYTES = 32;
    int STATE_ID_OFFSET = 0;
    int ORIGINAL_INDEX_OFFSET = 4;
    int GENERATION_OFFSET = 8;
    int KEY_OFFSET_OFFSET = 16;
    int KEY_LENGTH_OFFSET = 20;
    int VALUE_OFFSET_OFFSET = 24;
    int VALUE_LENGTH_OR_STATUS_OFFSET = 28;

    /**
     * Serializes the exact RocksDB composite key for {@code (key, namespace)} into the caller-owned
     * output.
     */
    void writeKeyAndNamespace(K key, N namespace, DataOutputView target) throws IOException;

    /**
     * Reads a batch of exact RocksDB keys from a direct key arena into fixed-size slots in a direct
     * value arena.
     *
     * <p>Each native-order descriptor is 32 bytes: state id, original index, generation, key
     * offset, key length, value offset, and value length/status. A non-negative final field is an
     * exact present-value length, {@link #NOT_FOUND} is a miss, and {@link #VALUE_OVERFLOW} is a
     * slot overflow. Empty values therefore remain distinguishable from misses.
     *
     * <p>Offsets are relative to the supplied buffers' current positions. Implementations must not
     * mutate any caller position or limit. An overflow leaves the entire value arena unchanged. The
     * contract is mailbox-prepared and worker-consumed; it provides no database snapshot
     * consistency beyond one guarded native invocation.
     *
     * @return the number of RocksDB-present keys, including values reported as {@link
     *     #VALUE_OVERFLOW}
     */
    int readValueBatch(
            ByteBuffer keyArena,
            ByteBuffer descriptors,
            int count,
            ByteBuffer valueArena,
            int valueStride)
            throws IOException;

    /**
     * Whether a non-empty {@link #readValueBatch} crosses JNI exactly once.
     *
     * <p>Implementations must leave this false unless that property is guaranteed by the active
     * native dependency.
     */
    default boolean usesSingleJniBatchRead() {
        return false;
    }
}
