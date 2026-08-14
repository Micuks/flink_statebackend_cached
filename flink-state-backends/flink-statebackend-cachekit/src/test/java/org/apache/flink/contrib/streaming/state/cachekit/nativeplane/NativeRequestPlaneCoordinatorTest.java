/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.nativeplane;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NativeRequestPlaneCoordinatorTest {

    @Test
    void testConstructionMetadataFailureClosesPlaneBeforeOwnershipEscapes() {
        FakePlane plane = new FakePlane();
        plane.failSelectedKernel = true;

        assertThrows(
                IllegalStateException.class,
                () -> NativeRequestPlaneCoordinator.forTesting(options(1), plane));

        assertEquals(1, plane.closeCalls);
    }

    @Test
    void testAarch64OnlyPolicyRejectsPortableScalarPlaneAndClosesIt() {
        FakePlane plane = new FakePlane();

        assertThrows(
                IllegalStateException.class,
                () ->
                        NativeRequestPlaneCoordinator.forTesting(
                                new NativeRequestPlaneOptions(
                                        true,
                                        "",
                                        "auto",
                                        16,
                                        1024,
                                        1024,
                                        4,
                                        1024,
                                        1024,
                                        1,
                                        1,
                                        true),
                                plane));

        assertEquals(1, plane.closeCalls);
    }

    @Test
    void testBatchSlotsAreBoundedReusableAndCloseIsIdempotent() {
        FakePlane plane = new FakePlane();
        plane.featureBits =
                NativeRequestPlaneBridge.FEATURE_AARCH64
                        | NativeRequestPlaneBridge.FEATURE_NEON
                        | NativeRequestPlaneBridge.FEATURE_CRC32
                        | NativeRequestPlaneBridge.FEATURE_SVE
                        | NativeRequestPlaneBridge.FEATURE_SVE_VL256;
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);

        assertEquals(0x1fL, coordinator.detectedFeatureBits());
        assertEquals("0x000000000000001f", coordinator.detectedFeatureBitsHex());
        assertEquals("aarch64|neon|crc32|sve|vl256", coordinator.detectedFeatures());
        assertEquals(4_480, coordinator.regularSlotDirectBytesForTesting());
        assertEquals(2_096, coordinator.mutationSlotDirectBytesForTesting());
        NativeRequestPlaneCoordinator.BatchSlot first = coordinator.tryAcquireBatchSlot();
        assertNotNull(first);
        assertNull(coordinator.tryAcquireBatchSlot());
        assertEquals(1, coordinator.leases());
        assertEquals(1, coordinator.leaseMisses());

        first.close();
        first.close();
        NativeRequestPlaneCoordinator.BatchSlot reused = coordinator.tryAcquireBatchSlot();
        assertNotNull(reused);
        reused.close();

        coordinator.disable(new IllegalStateException("injected"));
        assertFalse(coordinator.isActive());
        assertNull(coordinator.tryAcquireBatchSlot());
        coordinator.close();
        assertEquals(1, plane.closeCalls);
    }

    @Test
    void testExactProbeGenerationMatchesConditionalFillGeneration() throws Exception {
        FakePlane plane = new FakePlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);
        assertEquals("x86_64", coordinator.detectedFeatures());

        try (NativeRequestPlaneCoordinator.BatchSlot slot =
                coordinator.tryAcquireBatchSlot()) {
            assertNotNull(slot);
            slot.prepareLatest(3, 17L, Collections.singletonList(new byte[] {1, 2, 3}));
            assertEquals(17L, slot.preparedGeneration());
            assertEquals(1, coordinator.probe(slot));
            assertEquals(17L, plane.probeGeneration);
        }
        coordinator.close();
    }

    @Test
    void testDefaultCompactorRetainsArrivalOrder() throws Exception {
        FakePlane plane = new FakePlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);

        try (NativeRequestPlaneCoordinator.BatchSlot slot =
                coordinator.tryAcquireBatchSlot()) {
            slot.prepareLatest(
                    3,
                    17L,
                    java.util.Arrays.asList(new byte[] {1}, new byte[] {2}, new byte[] {1}));
            assertEquals(3, coordinator.compact(slot));
            assertEquals(0, slot.compactedSourceIndex(0));
            assertEquals(1, slot.compactedSourceIndex(1));
            assertEquals(2, slot.compactedSourceIndex(2));
        }
        coordinator.close();
    }

    @Test
    void testCompactorPublishesUniqueIndexesAndCounters() throws Exception {
        FakePlane plane = new FakePlane();
        plane.compactIndexes = new int[] {0, 2};
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);

        try (NativeRequestPlaneCoordinator.BatchSlot slot =
                coordinator.tryAcquireBatchSlot()) {
            slot.prepareLatest(
                    3,
                    17L,
                    java.util.Arrays.asList(new byte[] {1}, new byte[] {1}, new byte[] {2}));
            assertEquals(2, coordinator.compact(slot));
            assertEquals(0, slot.compactedSourceIndex(0));
            assertEquals(2, slot.compactedSourceIndex(1));
            assertEquals(1, coordinator.compactCalls());
        }
        coordinator.close();
    }

    @Test
    void testDirectBatchPreparationRetainsExactBytesAndRejectsEmptyKeys() throws Exception {
        FakePlane plane = new FakePlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);

        try (NativeRequestPlaneCoordinator.BatchSlot slot =
                coordinator.tryAcquireBatchSlot()) {
            slot.prepareLatestDirect(
                    3,
                    17L,
                    3,
                    (index, output) -> {
                        output.writeByte(index + 1);
                        output.writeShort(0x1011 + index);
                    });
            assertArrayEquals(new byte[] {1, 0x10, 0x11}, slot.copyPreparedKey(0));
            assertArrayEquals(new byte[] {2, 0x10, 0x12}, slot.copyPreparedKey(1));
            assertArrayEquals(new byte[] {3, 0x10, 0x13}, slot.copyPreparedKey(2));

            assertThrows(
                    java.io.IOException.class,
                    () -> slot.prepareLatestDirect(3, 17L, 1, (index, output) -> {}));
        }
        coordinator.close();
    }

    @Test
    void testGrouperPublishesOneStableGroupPerSource() throws Exception {
        FakePlane plane = new FakePlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);

        try (NativeRequestPlaneCoordinator.BatchSlot slot =
                coordinator.tryAcquireBatchSlot()) {
            slot.prepareLatest(
                    3,
                    17L,
                    java.util.Arrays.asList(
                            new byte[] {1}, new byte[] {2}, new byte[] {1}, new byte[] {3}));
            assertEquals(3, coordinator.group(slot));
            assertEquals(0, slot.sourceGroupIndex(0));
            assertEquals(1, slot.sourceGroupIndex(1));
            assertEquals(0, slot.sourceGroupIndex(2));
            assertEquals(2, slot.sourceGroupIndex(3));
            assertEquals(1, coordinator.groupCalls());
        }
        coordinator.close();
    }

    @Test
    void testDirectExactUpdatePublishesDirectKeyAndValue() throws Exception {
        FakePlane plane = new FakePlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);

        assertEquals(
                NativeRequestPlaneBridge.FILL_INSERTED,
                coordinator.updateExactKey(
                        9,
                        23L,
                        output -> {
                            output.writeByte(1);
                            output.writeInt(0x02030405);
                        },
                        output -> {
                            output.writeShort(0x0607);
                            output.writeByte(8);
                        }));

        assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, plane.fillKey);
        assertArrayEquals(new byte[] {6, 7, 8}, plane.fillValue);
        assertFalse(plane.fillNegative);
        assertEquals(23L, plane.fillGeneration);
        assertEquals(1, coordinator.fillCalls());
        coordinator.close();
    }

    private static NativeRequestPlaneOptions options(int slots) {
        return new NativeRequestPlaneOptions(
                true, "", "auto", 16, 1024, 1024, 4, 1024, 1024, 1, slots, false);
    }

    private static final class FakePlane implements NativeRequestPlane {
        private boolean failSelectedKernel;
        private int closeCalls;
        private long probeGeneration;
        private long featureBits;
        private int[] compactIndexes;
        private byte[] fillKey;
        private byte[] fillValue;
        private boolean fillNegative;
        private long fillGeneration;

        @Override
        public int fillBatch(
                SerializedKeyBatch<?, ?> keys,
                ByteBuffer valueArena,
                ByteBuffer valueMetadata,
                ByteBuffer fillResults) {
            ByteBuffer keyArena = keys.arenaSlice();
            fillKey = new byte[keys.serializedLength(0)];
            keyArena.position(keys.arenaOffset(0));
            keyArena.get(fillKey);
            fillGeneration = keys.generation(0);

            ByteBuffer metadata = valueMetadata.duplicate().order(ByteOrder.nativeOrder());
            int flags = metadata.getInt(NativeRequestPlaneBridge.FILL_VALUE_FLAGS_OFFSET);
            fillNegative =
                    (flags & NativeRequestPlaneBridge.FILL_VALUE_NEGATIVE_FLAG) != 0;
            int offset = metadata.getInt(NativeRequestPlaneBridge.FILL_VALUE_ARENA_OFFSET);
            int length = metadata.getInt(NativeRequestPlaneBridge.FILL_VALUE_LENGTH_OFFSET);
            fillValue = null;
            if (!fillNegative) {
                fillValue = new byte[length];
                ByteBuffer values = valueArena.duplicate();
                values.position(offset);
                values.get(fillValue);
            }
            ByteBuffer results = fillResults.duplicate().order(ByteOrder.nativeOrder());
            results.putInt(
                    NativeRequestPlaneBridge.FILL_RESULT_STATUS_OFFSET,
                    NativeRequestPlaneBridge.FILL_INSERTED);
            results.putInt(
                    NativeRequestPlaneBridge.FILL_RESULT_ERROR_OFFSET,
                    NativeRequestPlaneBridge.ERROR_OK);
            return keys.entryCount();
        }

        @Override
        public int probeBatch(
                SerializedKeyBatch<?, ?> keys,
                ByteBuffer valueOutput,
                ByteBuffer probeResults) {
            if (keys.entryCount() > 0) {
                probeGeneration = keys.generation(0);
            }
            return keys.entryCount();
        }

        @Override
        public int compactBatch(
                SerializedKeyBatch<?, ?> keys, ByteBuffer uniqueSourceIndexes) {
            if (compactIndexes == null) {
                return NativeRequestPlane.super.compactBatch(keys, uniqueSourceIndexes);
            }
            ByteBuffer output = uniqueSourceIndexes.duplicate().order(java.nio.ByteOrder.nativeOrder());
            for (int index = 0; index < compactIndexes.length; index++) {
                output.putInt(index * Integer.BYTES, compactIndexes[index]);
            }
            return compactIndexes.length;
        }

        @Override
        public String selectedKernel() {
            if (failSelectedKernel) {
                throw new IllegalStateException("injected selected-kernel failure");
            }
            return "scalar-crc32c";
        }

        @Override
        public long detectedFeatureBits() {
            return featureBits;
        }

        @Override
        public void close() {
            closeCalls++;
        }
    }
}
