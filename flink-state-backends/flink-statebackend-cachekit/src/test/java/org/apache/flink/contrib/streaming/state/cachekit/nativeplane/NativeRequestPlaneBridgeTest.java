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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.BatchKeyGroupingSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

class NativeRequestPlaneBridgeTest {

    @Test
    void testDefaultIsDisabledAndDoesNotLoadJni() {
        String previous = System.clearProperty(NativeRequestPlaneBridge.ENABLED_PROPERTY);
        try {
            assertFalse(NativeRequestPlaneBridge.isEnabledByDefault());
            IllegalStateException failure =
                    assertThrows(IllegalStateException.class, NativeRequestPlaneBridge::open);
            assertTrue(failure.getMessage().contains("disabled"));
        } finally {
            if (previous != null) {
                System.setProperty(NativeRequestPlaneBridge.ENABLED_PROPERTY, previous);
            }
        }
    }

    @Test
    @EnabledIfSystemProperty(named = NativeRequestPlaneBridge.LIBRARY_PATH_PROPERTY, matches = ".+")
    void testAutoKernelKeepsPlaneEnabledWithAuditableScalarX86Fallback() {
        try (NativeRequestPlaneBridge bridge =
                NativeRequestPlaneBridge.open(
                        true, 16, 1024, 1024, NativeRequestPlaneBridge.KERNEL_AUTO)) {
            String selectedKernel = bridge.selectedKernel();
            assertTrue(
                    selectedKernel.equals("scalar-crc32c")
                            || selectedKernel.equals("x86-sse4.2-crc32c")
                            || selectedKernel.equals("aarch64-neon-crc32c")
                            || selectedKernel.equals("aarch64-sve256-hybrid-crc32c"));
            String architecture =
                    System.getProperty("os.arch", "").toLowerCase(java.util.Locale.ROOT);
            if (architecture.equals("amd64") || architecture.equals("x86_64")) {
                assertTrue(
                        selectedKernel.equals("x86-sse4.2-crc32c")
                                || selectedKernel.equals("scalar-crc32c"));
            }
        }
    }

    @Test
    @EnabledIfSystemProperty(named = NativeRequestPlaneBridge.LIBRARY_PATH_PROPERTY, matches = ".+")
    void testGroupingBatchLimitIsIndependentFromCacheCapacity() throws Exception {
        try (NativeRequestPlaneBridge bridge =
                NativeRequestPlaneBridge.open(
                        true,
                        1,
                        4,
                        1024,
                        1024,
                        NativeRequestPlaneBridge.KERNEL_SCALAR,
                        System.getProperty(NativeRequestPlaneBridge.LIBRARY_PATH_PROPERTY))) {
            SerializedKeyBatch<Integer, String> keys = newBatch(4);
            keys.append(1, 1L, 1, "a");
            keys.append(1, 1L, 2, "b");
            keys.append(1, 1L, 1, "a");
            keys.append(1, 1L, 3, "c");
            ByteBuffer uniques = directNative(4 * Integer.BYTES);
            ByteBuffer groups = directNative(4 * Integer.BYTES);
            assertEquals(3, bridge.groupBatch(keys, uniques, groups));
            assertEquals(0, uniques.getInt(0));
            assertEquals(1, uniques.getInt(Integer.BYTES));
            assertEquals(3, uniques.getInt(2 * Integer.BYTES));
            assertEquals(0, groups.getInt(0));
            assertEquals(1, groups.getInt(Integer.BYTES));
            assertEquals(0, groups.getInt(2 * Integer.BYTES));
            assertEquals(2, groups.getInt(3 * Integer.BYTES));
        }
    }

    @Test
    @EnabledIfSystemProperty(named = NativeRequestPlaneBridge.LIBRARY_PATH_PROPERTY, matches = ".+")
    void testSingleJniFillAndProbeRoundTripIsAuditable() throws Exception {
        try (NativeRequestPlaneBridge bridge = openScalarBridge()) {
            SerializedKeyBatch<Integer, String> keys = newBatch(4);
            keys.append(7, 11L, 10, "ns-a");
            keys.append(7, 12L, 20, "ns-b");

            ByteBuffer valueArena = ByteBuffer.allocateDirect(5).order(ByteOrder.nativeOrder());
            valueArena.put(new byte[] {'v', 'a', 'l', 'u', 'e'}).flip();
            ByteBuffer valueMetadata =
                    directNative(2 * NativeRequestPlaneBridge.FILL_VALUE_RECORD_BYTES);
            putValueRecord(valueMetadata, 0, 0, 5, false);
            putValueRecord(valueMetadata, 1, 0, 0, true);
            ByteBuffer fillResults =
                    directNative(2 * NativeRequestPlaneBridge.FILL_RESULT_RECORD_BYTES);

            int valueArenaPosition = valueArena.position();
            assertEquals(2, bridge.fillBatch(keys, valueArena, valueMetadata, fillResults));
            assertEquals(valueArenaPosition, valueArena.position());
            assertEquals(0, fillStatus(fillResults, 0));
            assertEquals(0, fillError(fillResults, 0));
            assertEquals(0, fillStatus(fillResults, 1));
            assertEquals("scalar-crc32c", bridge.selectedKernel());
            assertTrue(bridge.detectedFeatureBits() >= 0);

            keys.clear();
            // Ordinary generation mismatch is a miss; the explicit sentinel asks for the latest
            // exact
            // key after Java has write-through versioned that key.
            keys.append(7, 111L, 10, "ns-a");
            keys.append(7, NativeRequestPlaneBridge.PROBE_LATEST_GENERATION, 10, "ns-a");
            keys.append(7, NativeRequestPlaneBridge.PROBE_LATEST_GENERATION, 20, "ns-b");
            keys.append(8, 13L, 30, "missing");
            ByteBuffer valueOutput = directNative(32);
            ByteBuffer probeResults =
                    directNative(4 * NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES);

            assertEquals(4, bridge.probeBatch(keys, valueOutput, probeResults));
            assertEquals(NativeRequestPlaneBridge.PROBE_MISS, probeStatus(probeResults, 0));
            assertEquals(0, probeError(probeResults, 0));
            assertEquals(0, probeValueLength(probeResults, 0));
            assertEquals(NativeRequestPlaneBridge.PROBE_HIT, probeStatus(probeResults, 1));
            assertEquals(0, probeError(probeResults, 1));
            assertEquals(0, probeValueOffset(probeResults, 1));
            assertEquals(5, probeValueLength(probeResults, 1));
            assertEquals(
                    "value",
                    readAscii(
                            valueOutput,
                            probeValueOffset(probeResults, 1),
                            probeValueLength(probeResults, 1)));
            assertEquals(NativeRequestPlaneBridge.PROBE_NEGATIVE, probeStatus(probeResults, 2));
            assertEquals(0, probeValueLength(probeResults, 2));
            assertEquals(NativeRequestPlaneBridge.PROBE_MISS, probeStatus(probeResults, 3));
            assertEquals(0, probeValueLength(probeResults, 3));
        }
    }

    @Test
    @EnabledIfSystemProperty(named = NativeRequestPlaneBridge.LIBRARY_PATH_PROPERTY, matches = ".+")
    void testJniCompactorKeepsFirstExactOccurrence() throws Exception {
        try (NativeRequestPlaneBridge bridge = openScalarBridge()) {
            SerializedKeyBatch<Integer, String> keys = newBatch(6);
            keys.append(7, 11L, 10, "same");
            keys.append(7, 11L, 20, "other");
            keys.append(7, 11L, 10, "same");
            keys.append(8, 11L, 10, "same");
            keys.append(7, 12L, 10, "same");
            keys.append(7, 11L, 20, "other");

            ByteBuffer indexes = directNative(6 * Integer.BYTES);
            assertEquals(4, bridge.compactBatch(keys, indexes));
            assertEquals(0, indexes.getInt(0));
            assertEquals(1, indexes.getInt(Integer.BYTES));
            assertEquals(3, indexes.getInt(2 * Integer.BYTES));
            assertEquals(4, indexes.getInt(3 * Integer.BYTES));
        }
    }

    @Test
    @EnabledIfSystemProperty(named = NativeRequestPlaneBridge.LIBRARY_PATH_PROPERTY, matches = ".+")
    void testJniGrouperMapsEverySourceToStableFirstSeenGroup() throws Exception {
        try (NativeRequestPlaneBridge bridge = openScalarBridge()) {
            SerializedKeyBatch<Integer, String> keys = newBatch(5);
            keys.append(7, 11L, 10, "same");
            keys.append(7, 11L, 20, "other");
            keys.append(7, 11L, 10, "same");
            keys.append(8, 11L, 10, "same");
            keys.append(7, 11L, 20, "other");

            ByteBuffer uniques = directNative(5 * Integer.BYTES);
            ByteBuffer groups = directNative(5 * Integer.BYTES);
            assertEquals(3, bridge.groupBatch(keys, uniques, groups));
            assertEquals(0, uniques.getInt(0));
            assertEquals(1, uniques.getInt(Integer.BYTES));
            assertEquals(3, uniques.getInt(2 * Integer.BYTES));
            assertEquals(0, groups.getInt(0));
            assertEquals(1, groups.getInt(Integer.BYTES));
            assertEquals(0, groups.getInt(2 * Integer.BYTES));
            assertEquals(2, groups.getInt(3 * Integer.BYTES));
            assertEquals(1, groups.getInt(4 * Integer.BYTES));
        }
    }

    @Test
    @EnabledIfSystemProperty(named = NativeRequestPlaneBridge.LIBRARY_PATH_PROPERTY, matches = ".+")
    void testTokenGrouperPublishesCommittedPackedPlan() {
        try (NativeRequestPlaneBridge bridge = openScalarBridge()) {
            ByteBuffer tokens = directNative(5 * Integer.BYTES);
            tokens.putInt(0, 7);
            tokens.putInt(Integer.BYTES, 8);
            tokens.putInt(2 * Integer.BYTES, 7);
            tokens.putInt(3 * Integer.BYTES, 9);
            tokens.putInt(4 * Integer.BYTES, 8);
            ByteBuffer plan = directNative(BatchKeyGroupingSupport.requiredPackedPlanBytes(5));

            assertEquals(3, bridge.groupHashTokens(tokens, 5, plan));
            assertEquals(BatchKeyGroupingSupport.PACKED_PLAN_MAGIC, plan.getInt(0));
            assertEquals(BatchKeyGroupingSupport.PACKED_PLAN_VERSION, plan.getInt(Integer.BYTES));
            assertEquals(5, plan.getInt(2 * Integer.BYTES));
            assertEquals(3, plan.getInt(3 * Integer.BYTES));

            int firstSourceBase = BatchKeyGroupingSupport.PACKED_PLAN_HEADER_BYTES;
            assertEquals(0, plan.getInt(firstSourceBase));
            assertEquals(1, plan.getInt(firstSourceBase + Integer.BYTES));
            assertEquals(3, plan.getInt(firstSourceBase + 2 * Integer.BYTES));
            int offsetsBase = firstSourceBase + 3 * Integer.BYTES;
            assertEquals(0, plan.getInt(offsetsBase));
            assertEquals(2, plan.getInt(offsetsBase + Integer.BYTES));
            assertEquals(4, plan.getInt(offsetsBase + 2 * Integer.BYTES));
            assertEquals(5, plan.getInt(offsetsBase + 3 * Integer.BYTES));
            int sourceGroupBase = offsetsBase + 4 * Integer.BYTES;
            assertEquals(0, plan.getInt(sourceGroupBase));
            assertEquals(1, plan.getInt(sourceGroupBase + Integer.BYTES));
            assertEquals(0, plan.getInt(sourceGroupBase + 2 * Integer.BYTES));
            assertEquals(2, plan.getInt(sourceGroupBase + 3 * Integer.BYTES));
            assertEquals(1, plan.getInt(sourceGroupBase + 4 * Integer.BYTES));
        }
    }

    @Test
    @EnabledIfSystemProperty(named = NativeRequestPlaneBridge.LIBRARY_PATH_PROPERTY, matches = ".+")
    void testStateGenerationWatermarkRejectsStaleFillAfterEviction() throws Exception {
        try (NativeRequestPlaneBridge bridge =
                NativeRequestPlaneBridge.open(
                        true, 1, 1024, 1024, NativeRequestPlaneBridge.KERNEL_SCALAR)) {
            SerializedKeyBatch<Integer, String> keys = newBatch(1);
            ByteBuffer valueArena = directNative(8);
            ByteBuffer valueMetadata =
                    directNative(NativeRequestPlaneBridge.FILL_VALUE_RECORD_BYTES);
            ByteBuffer fillResults =
                    directNative(NativeRequestPlaneBridge.FILL_RESULT_RECORD_BYTES);

            keys.append(41, 10L, 1, "k");
            valueArena.put(new byte[] {'v', '1', '0'}).flip();
            putValueRecord(valueMetadata, 0, 0, 3, false);
            assertEquals(1, bridge.fillBatch(keys, valueArena, valueMetadata, fillResults));
            assertEquals(NativeRequestPlaneBridge.FILL_INSERTED, fillStatus(fillResults, 0));

            keys.clear();
            keys.append(41, 11L, 2, "other");
            valueArena.clear();
            valueArena.put(new byte[] {'v', '1', '1'}).flip();
            assertEquals(1, bridge.fillBatch(keys, valueArena, valueMetadata, fillResults));
            assertEquals(NativeRequestPlaneBridge.FILL_INSERTED, fillStatus(fillResults, 0));

            keys.clear();
            keys.append(41, 5L, 1, "k");
            valueArena.clear();
            valueArena.put(new byte[] {'o', 'l', 'd'}).flip();
            assertEquals(1, bridge.fillBatch(keys, valueArena, valueMetadata, fillResults));
            assertEquals(
                    NativeRequestPlaneBridge.FILL_REJECTED_STALE_GENERATION,
                    fillStatus(fillResults, 0));
            assertEquals(NativeRequestPlaneBridge.ERROR_OK, fillError(fillResults, 0));

            keys.clear();
            keys.append(41, NativeRequestPlaneBridge.PROBE_LATEST_GENERATION, 1, "k");
            ByteBuffer probeResults =
                    directNative(NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES);
            assertEquals(1, bridge.probeBatch(keys, directNative(8), probeResults));
            assertEquals(NativeRequestPlaneBridge.PROBE_MISS, probeStatus(probeResults, 0));
        }
    }

    @Test
    @EnabledIfSystemProperty(named = NativeRequestPlaneBridge.LIBRARY_PATH_PROPERTY, matches = ".+")
    void testResidentOnlyMutationControlFlagsCrossJniBoundary() throws Exception {
        try (NativeRequestPlaneBridge bridge = openScalarBridge()) {
            SerializedKeyBatch<Integer, String> keys = newBatch(1);
            ByteBuffer valueArena = directNative(8);
            ByteBuffer valueMetadata =
                    directNative(NativeRequestPlaneBridge.FILL_VALUE_RECORD_BYTES);
            ByteBuffer fillResults =
                    directNative(NativeRequestPlaneBridge.FILL_RESULT_RECORD_BYTES);

            keys.append(73, 1L, 7, "resident");
            putValueRecord(
                    valueMetadata,
                    0,
                    0,
                    0,
                    true,
                    NativeRequestPlaneBridge.FILL_VALUE_CHECK_ONLY_FLAG);
            assertEquals(1, bridge.fillBatch(keys, valueArena, valueMetadata, fillResults));
            assertEquals(NativeRequestPlaneBridge.FILL_NOT_PRESENT, fillStatus(fillResults, 0));

            keys.clear();
            keys.append(73, 2L, 7, "resident");
            valueArena.clear();
            valueArena.put(new byte[] {'o', 'l', 'd'}).flip();
            putValueRecord(valueMetadata, 0, 0, 3, false);
            assertEquals(1, bridge.fillBatch(keys, valueArena, valueMetadata, fillResults));
            assertEquals(NativeRequestPlaneBridge.FILL_INSERTED, fillStatus(fillResults, 0));

            keys.clear();
            keys.append(73, 3L, 7, "resident");
            valueArena.clear();
            valueArena.put(new byte[] {'n', 'e', 'w'}).flip();
            putValueRecord(
                    valueMetadata,
                    0,
                    0,
                    3,
                    false,
                    NativeRequestPlaneBridge.FILL_VALUE_UPDATE_ONLY_FLAG);
            assertEquals(1, bridge.fillBatch(keys, valueArena, valueMetadata, fillResults));
            assertEquals(NativeRequestPlaneBridge.FILL_UPDATED, fillStatus(fillResults, 0));

            keys.clear();
            keys.append(
                    73,
                    NativeRequestPlaneBridge.PROBE_LATEST_GENERATION,
                    7,
                    "resident");
            ByteBuffer probeOutput = directNative(8);
            ByteBuffer probeResults =
                    directNative(NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES);
            assertEquals(1, bridge.probeBatch(keys, probeOutput, probeResults));
            assertEquals(NativeRequestPlaneBridge.PROBE_HIT, probeStatus(probeResults, 0));
            assertEquals(
                    "new",
                    readAscii(
                            probeOutput,
                            probeValueOffset(probeResults, 0),
                            probeValueLength(probeResults, 0)));

            keys.clear();
            keys.append(73, 4L, 7, "resident");
            putValueRecord(
                    valueMetadata,
                    0,
                    0,
                    0,
                    true,
                    NativeRequestPlaneBridge.FILL_VALUE_CHECK_ONLY_FLAG
                            | NativeRequestPlaneBridge.FILL_VALUE_UPDATE_ONLY_FLAG);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> bridge.fillBatch(keys, valueArena, valueMetadata, fillResults));
        }
    }

    @Test
    @EnabledIfSystemProperty(named = NativeRequestPlaneBridge.LIBRARY_PATH_PROPERTY, matches = ".+")
    void testMalformedBatchFailsClosedAndCloseIsTerminal() throws Exception {
        NativeRequestPlaneBridge bridge = openScalarBridge();
        SerializedKeyBatch<Integer, String> keys = newBatch(1);
        keys.append(1, 1L, 99, "bad");
        ByteBuffer valueArena = directNative(1);
        ByteBuffer invalidMetadata = directNative(NativeRequestPlaneBridge.FILL_VALUE_RECORD_BYTES);
        putValueRecord(invalidMetadata, 0, 2, 1, false);
        ByteBuffer fillResults = directNative(NativeRequestPlaneBridge.FILL_RESULT_RECORD_BYTES);

        assertThrows(
                IllegalArgumentException.class,
                () -> bridge.fillBatch(keys, valueArena, invalidMetadata, fillResults));

        ByteBuffer probeResults = directNative(NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES);
        assertEquals(1, bridge.probeBatch(keys, directNative(1), probeResults));
        assertEquals(NativeRequestPlaneBridge.PROBE_MISS, probeStatus(probeResults, 0));

        assertThrows(
                IllegalArgumentException.class,
                () -> bridge.probeBatch(keys, ByteBuffer.allocate(16), probeResults));
        bridge.close();
        bridge.close();
        assertThrows(IllegalStateException.class, bridge::selectedKernel);
    }

    private static NativeRequestPlaneBridge openScalarBridge() {
        return NativeRequestPlaneBridge.open(
                true, 16, 1024, 1024, NativeRequestPlaneBridge.KERNEL_SCALAR);
    }

    private static SerializedKeyBatch<Integer, String> newBatch(int entries) {
        return new SerializedKeyBatch<>(
                IntSerializer.INSTANCE,
                StringSerializer.INSTANCE,
                ByteBuffer.allocateDirect(512),
                ByteBuffer.allocateDirect(entries * SerializedKeyBatch.METADATA_RECORD_BYTES));
    }

    private static ByteBuffer directNative(int capacity) {
        return ByteBuffer.allocateDirect(capacity).order(ByteOrder.nativeOrder());
    }

    private static void putValueRecord(
            ByteBuffer metadata, int index, int arenaOffset, int length, boolean negative) {
        putValueRecord(metadata, index, arenaOffset, length, negative, 0);
    }

    private static void putValueRecord(
            ByteBuffer metadata,
            int index,
            int arenaOffset,
            int length,
            boolean negative,
            int controlFlags) {
        int base = index * NativeRequestPlaneBridge.FILL_VALUE_RECORD_BYTES;
        metadata.putInt(base + NativeRequestPlaneBridge.FILL_VALUE_ARENA_OFFSET, arenaOffset);
        metadata.putInt(base + NativeRequestPlaneBridge.FILL_VALUE_LENGTH_OFFSET, length);
        metadata.putInt(
                base + NativeRequestPlaneBridge.FILL_VALUE_FLAGS_OFFSET,
                negative ? NativeRequestPlaneBridge.FILL_VALUE_NEGATIVE_FLAG : 0);
        metadata.putInt(
                base + NativeRequestPlaneBridge.FILL_VALUE_RESERVED_OFFSET, controlFlags);
    }

    private static int fillStatus(ByteBuffer results, int index) {
        return nativeView(results)
                .getInt(
                        index * NativeRequestPlaneBridge.FILL_RESULT_RECORD_BYTES
                                + NativeRequestPlaneBridge.FILL_RESULT_STATUS_OFFSET);
    }

    private static int fillError(ByteBuffer results, int index) {
        return nativeView(results)
                .getInt(
                        index * NativeRequestPlaneBridge.FILL_RESULT_RECORD_BYTES
                                + NativeRequestPlaneBridge.FILL_RESULT_ERROR_OFFSET);
    }

    private static int probeStatus(ByteBuffer results, int index) {
        return nativeView(results)
                .getInt(
                        index * NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES
                                + NativeRequestPlaneBridge.PROBE_RESULT_STATUS_OFFSET);
    }

    private static int probeError(ByteBuffer results, int index) {
        return nativeView(results)
                .getInt(
                        index * NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES
                                + NativeRequestPlaneBridge.PROBE_RESULT_ERROR_OFFSET);
    }

    private static int probeValueOffset(ByteBuffer results, int index) {
        return nativeView(results)
                .getInt(
                        index * NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES
                                + NativeRequestPlaneBridge.PROBE_RESULT_ARENA_OFFSET);
    }

    private static int probeValueLength(ByteBuffer results, int index) {
        return nativeView(results)
                .getInt(
                        index * NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES
                                + NativeRequestPlaneBridge.PROBE_RESULT_LENGTH_OFFSET);
    }

    private static ByteBuffer nativeView(ByteBuffer buffer) {
        return buffer.duplicate().order(ByteOrder.nativeOrder());
    }

    private static String readAscii(ByteBuffer buffer, int offset, int length) {
        byte[] bytes = new byte[length];
        ByteBuffer view = buffer.duplicate();
        view.position(offset);
        view.get(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.US_ASCII);
    }
}
