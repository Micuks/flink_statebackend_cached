/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Fail-closed bridge for the experimental FrocksDB packed tiny-MapState prefix scan.
 *
 * <p>The native result format is deliberately self describing and fixed-width at its boundary:
 *
 * <pre>
 *   bytes 0..3   magic "CKMP"
 *   byte  4      version (1)
 *   byte  5      status (0 COMPLETE, 1 OVERFLOW, 2 ERROR)
 *   bytes 6..7   reserved (zero)
 *   bytes 8..11  entry count, unsigned big-endian (must fit signed int)
 *   bytes 12..15 total encoded length, big-endian int
 *   repeated COMPLETE records:
 *       key length, value length (signed big-endian int; key non-negative, value positive)
 *       raw RocksDB key bytes, raw RocksDB value bytes
 * </pre>
 *
 * <p>Only a canonical, completely decoded COMPLETE result may bypass the ordinary RocksIterator.
 * Every other condition returns a fallback outcome, so this optimization cannot omit state.
 */
final class RocksDBPackedTinyMapScan {

    static final int HEADER_BYTES = 16;
    static final byte FORMAT_VERSION = 1;
    static final byte STATUS_COMPLETE = 0;
    static final byte STATUS_OVERFLOW = 1;
    static final byte STATUS_ERROR = 2;

    private static final int MAGIC = 0x434b4d50; // "CKMP"

    private RocksDBPackedTinyMapScan() {}

    enum Outcome {
        COMPLETE,
        OVERFLOW,
        NATIVE_ERROR,
        MALFORMED
    }

    static final class EntryBytes {
        final byte[] rawKey;
        final byte[] rawValue;

        EntryBytes(byte[] rawKey, byte[] rawValue) {
            this.rawKey = rawKey;
            this.rawValue = rawValue;
        }
    }

    static final class Result {
        final Outcome outcome;
        final List<EntryBytes> entries;
        final int encodedBytes;

        private Result(Outcome outcome, List<EntryBytes> entries, int encodedBytes) {
            this.outcome = outcome;
            this.entries = entries;
            this.encodedBytes = encodedBytes;
        }

        static Result fallback(Outcome outcome, int encodedBytes) {
            return new Result(outcome, Collections.emptyList(), encodedBytes);
        }

        static Result complete(List<EntryBytes> entries, int encodedBytes) {
            return new Result(Outcome.COMPLETE, entries, encodedBytes);
        }
    }

    static Result tryScan(
            RocksDB db,
            ColumnFamilyHandle columnFamily,
            ReadOptions readOptions,
            byte[] seekPrefix,
            int prefixCompareOffset,
            int maxEntries,
            int maxBytes)
            throws RocksDBException {
        final byte[] encoded =
                db.scanPrefixTinyMapV1(
                        columnFamily,
                        readOptions,
                        seekPrefix,
                        prefixCompareOffset,
                        maxEntries,
                        maxBytes);
        return decode(encoded, seekPrefix, prefixCompareOffset, maxEntries, maxBytes);
    }

    static Result decode(
            @Nullable byte[] encoded,
            byte[] seekPrefix,
            int prefixCompareOffset,
            int maxEntries,
            int maxBytes) {
        if (encoded == null
                || maxEntries < 1
                || maxBytes < HEADER_BYTES
                || encoded.length < HEADER_BYTES
                || encoded.length > maxBytes
                || prefixCompareOffset < 0
                || prefixCompareOffset > seekPrefix.length
                || readInt(encoded, 0) != MAGIC
                || encoded[4] != FORMAT_VERSION
                || encoded[6] != 0
                || encoded[7] != 0
                || readInt(encoded, 12) != encoded.length) {
            return Result.fallback(Outcome.MALFORMED, encoded == null ? 0 : encoded.length);
        }

        final byte status = encoded[5];
        final int count = readInt(encoded, 8);
        if (status == STATUS_OVERFLOW || status == STATUS_ERROR) {
            if (count != 0 || encoded.length != HEADER_BYTES) {
                return Result.fallback(Outcome.MALFORMED, encoded.length);
            }
            return Result.fallback(
                    status == STATUS_OVERFLOW ? Outcome.OVERFLOW : Outcome.NATIVE_ERROR,
                    encoded.length);
        }
        if (status != STATUS_COMPLETE || count < 0 || count > maxEntries) {
            return Result.fallback(Outcome.MALFORMED, encoded.length);
        }

        int cursor = HEADER_BYTES;
        byte[] previousKey = null;
        final ArrayList<EntryBytes> entries = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            if ((long) cursor + 8L > encoded.length) {
                return Result.fallback(Outcome.MALFORMED, encoded.length);
            }
            final int keyLength = readInt(encoded, cursor);
            final int valueLength = readInt(encoded, cursor + 4);
            if (keyLength < 0 || valueLength <= 0) {
                return Result.fallback(Outcome.MALFORMED, encoded.length);
            }

            final long keyStart = (long) cursor + 8L;
            final long valueStart = keyStart + keyLength;
            final long recordEnd = valueStart + valueLength;
            if (valueStart > encoded.length || recordEnd > encoded.length) {
                return Result.fallback(Outcome.MALFORMED, encoded.length);
            }

            final byte[] rawKey = copyRange(encoded, (int) keyStart, (int) valueStart);
            final byte[] rawValue = copyRange(encoded, (int) valueStart, (int) recordEnd);
            if (!hasExpectedPrefix(rawKey, seekPrefix, prefixCompareOffset)
                    || (previousKey != null && compareUnsigned(previousKey, rawKey) >= 0)) {
                return Result.fallback(Outcome.MALFORMED, encoded.length);
            }

            entries.add(new EntryBytes(rawKey, rawValue));
            previousKey = rawKey;
            cursor = (int) recordEnd;
        }

        if (cursor != encoded.length) {
            return Result.fallback(Outcome.MALFORMED, encoded.length);
        }
        return Result.complete(Collections.unmodifiableList(entries), encoded.length);
    }

    private static boolean hasExpectedPrefix(
            byte[] rawKey, byte[] seekPrefix, int prefixCompareOffset) {
        if (rawKey.length < seekPrefix.length) {
            return false;
        }
        for (int index = prefixCompareOffset; index < seekPrefix.length; index++) {
            if (rawKey[index] != seekPrefix[index]) {
                return false;
            }
        }
        return true;
    }

    private static int compareUnsigned(byte[] left, byte[] right) {
        final int common = Math.min(left.length, right.length);
        for (int index = 0; index < common; index++) {
            final int comparison = Integer.compare(left[index] & 0xff, right[index] & 0xff);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private static byte[] copyRange(byte[] source, int start, int end) {
        final byte[] copy = new byte[end - start];
        System.arraycopy(source, start, copy, 0, copy.length);
        return copy;
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24)
                | ((bytes[offset + 1] & 0xff) << 16)
                | ((bytes[offset + 2] & 0xff) << 8)
                | (bytes[offset + 3] & 0xff);
    }
}
