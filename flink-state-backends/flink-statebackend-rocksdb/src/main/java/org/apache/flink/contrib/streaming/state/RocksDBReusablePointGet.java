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

package org.apache.flink.contrib.streaming.state;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.Arrays;

/** Reusable value storage for RocksDB point gets. */
final class RocksDBReusablePointGet {

    static final int DEFAULT_INITIAL_CAPACITY = 128;
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private byte[] valueBuffer;

    RocksDBReusablePointGet() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    RocksDBReusablePointGet(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Initial capacity must not be negative.");
        }
        valueBuffer = new byte[initialCapacity];
    }

    /**
     * Reads {@code key} into the reusable buffer.
     *
     * <p>The preallocated RocksDB API returns the full value length even when the supplied buffer
     * is too small. In that case the buffer grows and the point get is retried. A concurrent value
     * growth is handled by repeating this process; a concurrent deletion is reported as {@link
     * RocksDB#NOT_FOUND}.
     */
    int get(RocksDB db, ColumnFamilyHandle columnFamily, byte[] key) throws RocksDBException {
        while (true) {
            final int valueLength = db.get(columnFamily, key, valueBuffer);
            if (valueLength == RocksDB.NOT_FOUND) {
                return RocksDB.NOT_FOUND;
            }
            validateFoundLength(valueLength);
            if (valueLength <= valueBuffer.length) {
                return valueLength;
            }
            grow(valueLength);
        }
    }

    /** Checks for a key without retrying when only part of an oversized value was copied. */
    boolean exists(RocksDB db, ColumnFamilyHandle columnFamily, byte[] key)
            throws RocksDBException {
        final int valueLength = db.get(columnFamily, key, valueBuffer);
        if (valueLength == RocksDB.NOT_FOUND) {
            return false;
        }
        validateFoundLength(valueLength);
        return true;
    }

    byte[] buffer() {
        return valueBuffer;
    }

    private static void validateFoundLength(int valueLength) {
        if (valueLength < 0) {
            throw new IllegalStateException(
                    "Unexpected RocksDB preallocated-get result: " + valueLength);
        }
    }

    private void grow(int requiredCapacity) {
        if (requiredCapacity < 0 || requiredCapacity > MAX_ARRAY_SIZE) {
            throw new OutOfMemoryError(
                    "Required RocksDB value length is too large: " + requiredCapacity);
        }

        final int currentCapacity = valueBuffer.length;
        final long geometricCapacity = currentCapacity + (currentCapacity >> 1) + 1L;
        final int newCapacity =
                (int)
                        Math.min(
                                MAX_ARRAY_SIZE,
                                Math.max((long) requiredCapacity, geometricCapacity));
        valueBuffer = Arrays.copyOf(valueBuffer, newCapacity);
    }
}
