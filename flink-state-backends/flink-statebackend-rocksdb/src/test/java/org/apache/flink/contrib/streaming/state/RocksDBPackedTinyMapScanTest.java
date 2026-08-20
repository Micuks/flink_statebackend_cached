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

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests the fail-closed boundary of the packed tiny MapState scan. */
public class RocksDBPackedTinyMapScanTest {

    private static final byte[] BINARY_PREFIX = new byte[] {0x55, (byte) 0xff, 0x00, (byte) 0x80};

    @Test
    public void testCompleteEmptyAndMultipleEntriesWithBinaryPrefix() throws Exception {
        RocksDBPackedTinyMapScan.Result empty =
                decode(completePayload(new byte[0][], new byte[0][]), BINARY_PREFIX, 1, 8, 1024);
        assertEquals(RocksDBPackedTinyMapScan.Outcome.COMPLETE, empty.outcome);
        assertTrue(empty.entries.isEmpty());

        byte[][] keys =
                new byte[][] {
                    concat(BINARY_PREFIX, new byte[] {0x01}),
                    concat(BINARY_PREFIX, new byte[] {(byte) 0xfe})
                };
        byte[][] values = new byte[][] {{0x00, 0x11}, {0x00, 0x22, 0x33}};
        RocksDBPackedTinyMapScan.Result result =
                decode(completePayload(keys, values), BINARY_PREFIX, 1, 8, 1024);

        assertEquals(RocksDBPackedTinyMapScan.Outcome.COMPLETE, result.outcome);
        assertEquals(2, result.entries.size());
        assertArrayEquals(keys[0], result.entries.get(0).rawKey);
        assertArrayEquals(values[1], result.entries.get(1).rawValue);
    }

    @Test
    public void testOverflowAndNativeErrorRequestFallback() throws Exception {
        assertEquals(
                RocksDBPackedTinyMapScan.Outcome.OVERFLOW,
                decode(
                                statusPayload(RocksDBPackedTinyMapScan.STATUS_OVERFLOW),
                                BINARY_PREFIX,
                                0,
                                8,
                                1024)
                        .outcome);
        assertEquals(
                RocksDBPackedTinyMapScan.Outcome.NATIVE_ERROR,
                decode(
                                statusPayload(RocksDBPackedTinyMapScan.STATUS_ERROR),
                                BINARY_PREFIX,
                                0,
                                8,
                                1024)
                        .outcome);
    }

    @Test
    public void testMalformedPayloadsAlwaysFallback() throws Exception {
        byte[] valid =
                completePayload(
                        new byte[][] {concat(BINARY_PREFIX, new byte[] {0x01})},
                        new byte[][] {{0x00, 0x01}});

        assertMalformed(null, BINARY_PREFIX, 0, 8, 1024);
        assertMalformed(new byte[11], BINARY_PREFIX, 0, 8, 1024);

        byte[] badMagic = valid.clone();
        badMagic[0] = 0;
        assertMalformed(badMagic, BINARY_PREFIX, 0, 8, 1024);

        byte[] badVersion = valid.clone();
        badVersion[4] = 2;
        assertMalformed(badVersion, BINARY_PREFIX, 0, 8, 1024);

        byte[] badTotalLength = valid.clone();
        putInt(badTotalLength, 12, valid.length + 1);
        assertMalformed(badTotalLength, BINARY_PREFIX, 0, 8, 1024);

        byte[] negativeKeyLength = valid.clone();
        putInt(negativeKeyLength, RocksDBPackedTinyMapScan.HEADER_BYTES, -1);
        assertMalformed(negativeKeyLength, BINARY_PREFIX, 0, 8, 1024);

        byte[] overflowingLength = valid.clone();
        putInt(overflowingLength, RocksDBPackedTinyMapScan.HEADER_BYTES, Integer.MAX_VALUE);
        assertMalformed(overflowingLength, BINARY_PREFIX, 0, 8, 1024);

        byte[] crossPrefix = valid.clone();
        crossPrefix[RocksDBPackedTinyMapScan.HEADER_BYTES + 8 + 1] ^= 1;
        assertMalformed(crossPrefix, BINARY_PREFIX, 1, 8, 1024);

        byte[] trailingByte = new byte[valid.length + 1];
        System.arraycopy(valid, 0, trailingByte, 0, valid.length);
        assertMalformed(trailingByte, BINARY_PREFIX, 0, 8, 1024);

        assertMalformed(valid, BINARY_PREFIX, 0, 0, 1024);
        assertMalformed(valid, BINARY_PREFIX, 0, 8, valid.length - 1);
        assertMalformed(valid, BINARY_PREFIX, BINARY_PREFIX.length + 1, 8, 1024);
    }

    @Test
    public void testRejectsUnsortedAndDuplicateKeys() throws Exception {
        byte[] first = concat(BINARY_PREFIX, new byte[] {0x02});
        byte[] second = concat(BINARY_PREFIX, new byte[] {0x01});
        assertMalformed(
                completePayload(new byte[][] {first, second}, new byte[][] {{0x00}, {0x00}}),
                BINARY_PREFIX,
                0,
                8,
                1024);
        assertMalformed(
                completePayload(new byte[][] {first, first}, new byte[][] {{0x00}, {0x00}}),
                BINARY_PREFIX,
                0,
                8,
                1024);
    }

    @Test
    public void testDecodedRawKeyAndValueAreOwnedForExactMutation() throws Exception {
        byte[] key = concat(BINARY_PREFIX, new byte[] {0x01, 0x02});
        byte[] value = new byte[] {0x00, 0x11, 0x22};
        byte[] payload = completePayload(new byte[][] {key}, new byte[][] {value});
        RocksDBPackedTinyMapScan.Result result = decode(payload, BINARY_PREFIX, 0, 8, 1024);

        // Mutation of the JNI result buffer after decoding must not alter the exact key/value used
        // later by RocksDBMapEntry.remove() or setValue().
        for (int index = 0; index < payload.length; index++) {
            payload[index] = 0;
        }
        assertArrayEquals(key, result.entries.get(0).rawKey);
        assertArrayEquals(value, result.entries.get(0).rawValue);
    }

    private static RocksDBPackedTinyMapScan.Result decode(
            byte[] encoded, byte[] prefix, int offset, int maxEntries, int maxBytes) {
        return RocksDBPackedTinyMapScan.decode(encoded, prefix, offset, maxEntries, maxBytes);
    }

    private static void assertMalformed(
            byte[] encoded, byte[] prefix, int offset, int maxEntries, int maxBytes) {
        assertEquals(
                RocksDBPackedTinyMapScan.Outcome.MALFORMED,
                decode(encoded, prefix, offset, maxEntries, maxBytes).outcome);
    }

    private static byte[] completePayload(byte[][] keys, byte[][] values) throws IOException {
        if (keys.length != values.length) {
            throw new IllegalArgumentException();
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        long totalLength = RocksDBPackedTinyMapScan.HEADER_BYTES;
        for (int index = 0; index < keys.length; index++) {
            totalLength += 8L + keys[index].length + values[index].length;
        }
        if (totalLength > Integer.MAX_VALUE) {
            throw new IllegalArgumentException();
        }
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writeHeader(
                    out, RocksDBPackedTinyMapScan.STATUS_COMPLETE, keys.length, (int) totalLength);
            for (int index = 0; index < keys.length; index++) {
                out.writeInt(keys[index].length);
                out.writeInt(values[index].length);
                out.write(keys[index]);
                out.write(values[index]);
            }
        }
        return bytes.toByteArray();
    }

    private static byte[] statusPayload(byte status) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writeHeader(out, status, 0, RocksDBPackedTinyMapScan.HEADER_BYTES);
        }
        return bytes.toByteArray();
    }

    private static void writeHeader(DataOutputStream out, byte status, int count, int totalLength)
            throws IOException {
        out.writeInt(0x434b4d50);
        out.writeByte(RocksDBPackedTinyMapScan.FORMAT_VERSION);
        out.writeByte(status);
        out.writeShort(0);
        out.writeInt(count);
        out.writeInt(totalLength);
    }

    private static byte[] concat(byte[] left, byte[] right) {
        byte[] result = new byte[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }

    private static void putInt(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) (value >>> 24);
        bytes[offset + 1] = (byte) (value >>> 16);
        bytes[offset + 2] = (byte) (value >>> 8);
        bytes[offset + 3] = (byte) value;
    }
}
