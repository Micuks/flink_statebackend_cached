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
    private static final int[] EMPTY_ENTRY_SLICES = new int[0];

    private RocksDBPackedTinyMapScan() {}

    enum Outcome {
        COMPLETE,
        OVERFLOW,
        NATIVE_ERROR,
        MALFORMED
    }

    static final class Result {
        final Outcome outcome;
        @Nullable final byte[] encodedPage;
        /** Four ints per entry: key offset, key length, value offset, value length. */
        final int[] entrySlices;
        final int encodedBytes;

        private Result(
                Outcome outcome,
                @Nullable byte[] encodedPage,
                int[] entrySlices,
                int encodedBytes) {
            this.outcome = outcome;
            this.encodedPage = encodedPage;
            this.entrySlices = entrySlices;
            this.encodedBytes = encodedBytes;
        }

        static Result fallback(Outcome outcome, int encodedBytes) {
            return new Result(outcome, null, EMPTY_ENTRY_SLICES, encodedBytes);
        }

        static Result complete(byte[] encodedPage, int[] entrySlices, int encodedBytes) {
            return new Result(Outcome.COMPLETE, encodedPage, entrySlices, encodedBytes);
        }

        int entryCount() {
            return entrySlices.length / 4;
        }

        int keyOffset(int index) {
            return entrySlices[index * 4];
        }

        int keyLength(int index) {
            return entrySlices[index * 4 + 1];
        }

        int valueOffset(int index) {
            return entrySlices[index * 4 + 2];
        }

        int valueLength(int index) {
            return entrySlices[index * 4 + 3];
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
        // Every record needs two length fields and at least the null marker in its value. Bound
        // the primitive slice table before multiplying count by four, even if a direct unit-test
        // caller supplies a maxEntries value larger than the production configuration permits.
        final int maximumCountFromBytes = (encoded.length - HEADER_BYTES) / 9;
        if (status != STATUS_COMPLETE
                || count < 0
                || count > maxEntries
                || count > maximumCountFromBytes
                || count > Integer.MAX_VALUE / 4) {
            return Result.fallback(Outcome.MALFORMED, encoded.length);
        }

        int cursor = HEADER_BYTES;
        int previousKeyOffset = -1;
        int previousKeyLength = 0;
        final int[] entrySlices = count == 0 ? EMPTY_ENTRY_SLICES : new int[count * 4];
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

            if (!hasExpectedPrefix(
                            encoded,
                            (int) keyStart,
                            keyLength,
                            seekPrefix,
                            prefixCompareOffset)
                    || (previousKeyOffset >= 0
                            && compareUnsigned(
                                            encoded,
                                            previousKeyOffset,
                                            previousKeyLength,
                                            (int) keyStart,
                                            keyLength)
                                    >= 0)) {
                return Result.fallback(Outcome.MALFORMED, encoded.length);
            }

            final int sliceOffset = index * 4;
            entrySlices[sliceOffset] = (int) keyStart;
            entrySlices[sliceOffset + 1] = keyLength;
            entrySlices[sliceOffset + 2] = (int) valueStart;
            entrySlices[sliceOffset + 3] = valueLength;
            previousKeyOffset = (int) keyStart;
            previousKeyLength = keyLength;
            cursor = (int) recordEnd;
        }

        if (cursor != encoded.length) {
            return Result.fallback(Outcome.MALFORMED, encoded.length);
        }
        return Result.complete(encoded, entrySlices, encoded.length);
    }

    private static boolean hasExpectedPrefix(
            byte[] encoded,
            int rawKeyOffset,
            int rawKeyLength,
            byte[] seekPrefix,
            int prefixCompareOffset) {
        if (rawKeyLength < seekPrefix.length) {
            return false;
        }
        for (int index = prefixCompareOffset; index < seekPrefix.length; index++) {
            if (encoded[rawKeyOffset + index] != seekPrefix[index]) {
                return false;
            }
        }
        return true;
    }

    private static int compareUnsigned(
            byte[] encoded,
            int leftOffset,
            int leftLength,
            int rightOffset,
            int rightLength) {
        final int common = Math.min(leftLength, rightLength);
        for (int index = 0; index < common; index++) {
            final int comparison =
                    Integer.compare(
                            encoded[leftOffset + index] & 0xff,
                            encoded[rightOffset + index] & 0xff);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(leftLength, rightLength);
    }

    private static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24)
                | ((bytes[offset + 1] & 0xff) << 16)
                | ((bytes[offset + 2] & 0xff) << 8)
                | (bytes[offset + 3] & 0xff);
    }
}
