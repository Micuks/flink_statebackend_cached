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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class NativeRequestPlaneCoordinatorTest {

    @Test
    void testNativeBatchCapacityCoversLargeCompactionScratch() {
        NativeRequestPlaneOptions largeScratch =
                optionsWithCompactionScratch(2, 4096, 2 << 20);
        assertEquals(4096, NativeRequestPlaneCoordinator.nativeMaxBatchEntries(largeScratch));
        assertEquals(
                4,
                NativeRequestPlaneCoordinator.nativeMaxBatchEntries(
                        optionsWithCompactionScratch(2)));
    }

    @Test
    void testCloseWaitsForOutstandingRegularSlot() throws Exception {
        FakePlane plane = new FakePlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);
        NativeRequestPlaneCoordinator.BatchSlot slot = coordinator.tryAcquireBatchSlot();
        assertNotNull(slot);

        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch closeReturned = new CountDownLatch(1);
        Thread closer =
                new Thread(
                        () -> {
                            closeStarted.countDown();
                            coordinator.close();
                            closeReturned.countDown();
                        },
                        "native-coordinator-close");
        try {
            closer.start();
            assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (coordinator.isActive() && System.nanoTime() < deadline) {
                Thread.yield();
            }
            assertFalse(coordinator.isActive());
            assertNull(coordinator.tryAcquireBatchSlot());
            assertFalse(closeReturned.await(200, TimeUnit.MILLISECONDS));
            assertEquals(0, plane.closeCalls);
        } finally {
            slot.close();
            closer.join(5000);
        }
        assertFalse(closer.isAlive());
        assertEquals(0, closeReturned.getCount());
        assertEquals(1, plane.closeCalls);
    }

    @Test
    void testCloseWaitsForOutstandingCompactionScratchSlot() throws Exception {
        FakePlane plane = new FakePlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(
                        optionsWithCompactionScratch(1), plane);
        NativeRequestPlaneCoordinator.BatchSlot scratch =
                coordinator.tryAcquireCompactionScratchSlot();
        assertNotNull(scratch);

        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch closeReturned = new CountDownLatch(1);
        Thread closer =
                new Thread(
                        () -> {
                            closeStarted.countDown();
                            coordinator.close();
                            closeReturned.countDown();
                        },
                        "native-coordinator-scratch-close");
        try {
            closer.start();
            assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (coordinator.isActive() && System.nanoTime() < deadline) {
                Thread.yield();
            }
            assertFalse(coordinator.isActive());
            assertNull(coordinator.tryAcquireCompactionScratchSlot());
            assertFalse(closeReturned.await(200, TimeUnit.MILLISECONDS));
            assertEquals(0, plane.closeCalls);
        } finally {
            scratch.close();
            closer.join(5000);
        }
        assertFalse(closer.isAlive());
        assertEquals(0, closeReturned.getCount());
        assertEquals(1, plane.closeCalls);
    }

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
                                        true, "", "auto", 16, 1024, 1024, 4, 1024, 1024, 1, 1,
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
    void testMapDistinctReadSlotIsIndependentFromRegularAsyncSlots() {
        FakePlane plane = new FakePlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(1), plane);

        NativeRequestPlaneCoordinator.BatchSlot regular = coordinator.tryAcquireBatchSlot();
        assertNotNull(regular);
        assertNull(coordinator.tryAcquireBatchSlot());

        NativeRequestPlaneCoordinator.BatchSlot distinct =
                coordinator.tryAcquireMapDistinctReadSlot();
        assertNotNull(distinct);
        assertNull(coordinator.tryAcquireMapDistinctReadSlot());
        assertEquals(1, coordinator.mapDistinctReadLeases());
        assertEquals(1, coordinator.mapDistinctReadLeaseMisses());

        distinct.close();
        NativeRequestPlaneCoordinator.BatchSlot reusedDistinct =
                coordinator.tryAcquireMapDistinctReadSlot();
        assertNotNull(reusedDistinct);
        reusedDistinct.close();
        regular.close();
        coordinator.close();
        assertEquals(1, plane.closeCalls);
    }

    @Test
    void testCompactionScratchSlotIsIndependentBoundedAndLightweight() throws Exception {
        FakePlane plane = new FakePlane();
        plane.compactIndexes = new int[] {0, 2};
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(
                        optionsWithCompactionScratch(1), plane);
        long regularBytes = coordinator.regularSlotDirectBytesForTesting();
        long scratchBytes = coordinator.compactionScratchSlotDirectBytesForTesting();
        assertTrue(scratchBytes < regularBytes);

        NativeRequestPlaneCoordinator.BatchSlot regular = coordinator.tryAcquireBatchSlot();
        assertNotNull(regular);
        assertNull(coordinator.tryAcquireBatchSlot());

        NativeRequestPlaneCoordinator.BatchSlot scratch =
                coordinator.tryAcquireCompactionScratchSlot();
        assertNotNull(scratch);
        assertTrue(scratch.isCompactionScratch());
        assertNull(coordinator.tryAcquireCompactionScratchSlot());
        assertEquals(1, coordinator.compactionScratchLeases());
        assertEquals(1, coordinator.compactionScratchLeaseMisses());

        scratch.prepareLatest(
                3,
                17L,
                Arrays.asList(new byte[] {1}, new byte[] {1}, new byte[] {2}));
        assertEquals(2, coordinator.compact(scratch));
        assertEquals(0, scratch.compactedSourceIndex(0));
        assertEquals(2, scratch.compactedSourceIndex(1));
        scratch.close();
        NativeRequestPlaneCoordinator.BatchSlot reused =
                coordinator.tryAcquireCompactionScratchSlot();
        assertNotNull(reused);
        reused.close();
        regular.close();
        coordinator.close();
        assertEquals(1, plane.closeCalls);
    }

    @Test
    void testExactProbeGenerationMatchesConditionalFillGeneration() throws Exception {
        FakePlane plane = new FakePlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);
        assertEquals("x86_64", coordinator.detectedFeatures());

        try (NativeRequestPlaneCoordinator.BatchSlot slot = coordinator.tryAcquireBatchSlot()) {
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

        try (NativeRequestPlaneCoordinator.BatchSlot slot = coordinator.tryAcquireBatchSlot()) {
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

        try (NativeRequestPlaneCoordinator.BatchSlot slot = coordinator.tryAcquireBatchSlot()) {
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
    void testMalformedCompactorOutputDisablesPlaneBeforePublishingSelection() throws Exception {
        FakePlane plane = new FakePlane();
        plane.compactIndexes = new int[] {1, 0};
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);

        try (NativeRequestPlaneCoordinator.BatchSlot slot = coordinator.tryAcquireBatchSlot()) {
            slot.prepareLatest(
                    3,
                    17L,
                    java.util.Arrays.asList(new byte[] {1}, new byte[] {2}, new byte[] {3}));

            assertThrows(IllegalStateException.class, () -> coordinator.compact(slot));
            assertFalse(coordinator.isActive());
            assertEquals(0, coordinator.compactCalls());
            assertThrows(IndexOutOfBoundsException.class, () -> slot.compactedSourceIndex(0));
        }
        coordinator.close();
        assertEquals(1, plane.closeCalls);
    }

    @Test
    void testNonEmptyCompactorCannotSilentlyDropEveryKey() throws Exception {
        FakePlane plane = new FakePlane();
        plane.compactIndexes = new int[0];
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);

        try (NativeRequestPlaneCoordinator.BatchSlot slot = coordinator.tryAcquireBatchSlot()) {
            slot.prepareLatest(3, 17L, Collections.singletonList(new byte[] {1}));

            assertThrows(IllegalStateException.class, () -> coordinator.compact(slot));
            assertFalse(coordinator.isActive());
            assertEquals(0, coordinator.compactCalls());
        }
        coordinator.close();
        assertEquals(1, plane.closeCalls);
    }

    @Test
    void testCompactedProjectionRetainsExactPreparedBytesWithoutArenaCopy() throws Exception {
        FakePlane plane = new FakePlane();
        plane.compactIndexes = new int[] {0, 2, 3};
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);

        try (NativeRequestPlaneCoordinator.BatchSlot slot = coordinator.tryAcquireBatchSlot()) {
            slot.prepareLatest(
                    3,
                    17L,
                    java.util.Arrays.asList(
                            new byte[] {10},
                            new byte[] {11},
                            new byte[] {20, 21},
                            new byte[] {30, 31, 32}));
            assertEquals(3, coordinator.compact(slot));

            // Simulate an in-flight reservation rejecting the middle compacted key.
            slot.retainCompactedSource(0, 0);
            slot.retainCompactedSource(2, 1);
            slot.projectRetainedCompactedSources(2);

            assertEquals(2, slot.preparedEntryCount());
            assertArrayEquals(new byte[] {10}, slot.copyPreparedKey(0));
            assertArrayEquals(new byte[] {30, 31, 32}, slot.copyPreparedKey(1));
            assertEquals(17L, slot.preparedGeneration());
            assertEquals(2, coordinator.probe(slot));
        }
        coordinator.close();
    }

    @Test
    void testCompactedProjectionRejectsReorderedOrOutOfRangeSelection() throws Exception {
        FakePlane plane = new FakePlane();
        plane.compactIndexes = new int[] {0, 2};
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);

        try (NativeRequestPlaneCoordinator.BatchSlot slot = coordinator.tryAcquireBatchSlot()) {
            slot.prepareLatest(
                    3,
                    17L,
                    java.util.Arrays.asList(new byte[] {1}, new byte[] {2}, new byte[] {3}));
            assertEquals(2, coordinator.compact(slot));
            assertThrows(IndexOutOfBoundsException.class, () -> slot.retainCompactedSource(0, 1));
            assertThrows(IllegalArgumentException.class, () -> slot.projectRetainedCompactedSources(3));
        }
        coordinator.close();
    }

    @Test
    void testDirectBatchPreparationRetainsExactBytesAndRejectsEmptyKeys() throws Exception {
        FakePlane plane = new FakePlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);

        try (NativeRequestPlaneCoordinator.BatchSlot slot = coordinator.tryAcquireBatchSlot()) {
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
    void testDirectArenaDescriptorsReferencePreparedKeysAndFillKeepsSelectionOrder()
            throws Exception {
        FakePlane plane = new FakePlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(directArenaOptions(1), plane);
        assertEquals(8_576, coordinator.regularSlotDirectBytesForTesting());

        try (NativeRequestPlaneCoordinator.BatchSlot slot = coordinator.tryAcquireBatchSlot()) {
            slot.prepareLatest(
                    7,
                    23L,
                    java.util.Arrays.asList(
                            new byte[] {10, 11}, new byte[] {20}, new byte[] {30, 31, 32}));
            int[] selection = new int[] {2, 0};
            slot.prepareDirectArenaMultiGet(selection, 0, 2);
            assertEquals(16, slot.directMultiGetValueStride());

            // The extended ABI can select 128-slot geometry while the legacy overload above
            // retains the original 64-slot value width. Tail count does not alter either width.
            slot.prepareDirectArenaMultiGet(selection, 0, 2, 128);
            assertEquals(8, slot.directMultiGetValueStride());

            ByteBuffer descriptors =
                    slot.directMultiGetDescriptors().order(ByteOrder.nativeOrder());
            assertEquals(7, descriptors.getInt(0));
            assertEquals(2, descriptors.getInt(4));
            assertEquals(23L, descriptors.getLong(8));
            assertEquals(3, descriptors.getInt(20));
            int second = org.apache.flink.contrib.streaming.state.RocksDBBatchValueReader
                    .DIRECT_ARENA_DESCRIPTOR_BYTES;
            assertEquals(0, descriptors.getInt(second + 4));
            assertEquals(2, descriptors.getInt(second + 20));

            slot.prepareFillFromPreparedIndices(
                    7,
                    23L,
                    selection,
                    2,
                    java.util.Arrays.asList(new byte[] {1, 2, 3, 4}, null));
            assertEquals(2, coordinator.fill(slot));
            assertArrayEquals(new byte[] {30, 31, 32}, plane.fillKey);
            assertArrayEquals(new byte[] {1, 2, 3, 4}, plane.fillValue);
            assertEquals(23L, plane.fillGeneration);
        }
        coordinator.close();
    }

    @Test
    void testGrouperPublishesOneStableGroupPerSource() throws Exception {
        FakePlane plane = new FakePlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);

        try (NativeRequestPlaneCoordinator.BatchSlot slot = coordinator.tryAcquireBatchSlot()) {
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
    void testTokenGrouperDelegatesCallerOwnedDirectBuffers() {
        FakePlane plane = new FakePlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);
        ByteBuffer tokens =
                ByteBuffer.allocateDirect(3 * Integer.BYTES).order(ByteOrder.nativeOrder());
        tokens.putInt(0, 1);
        tokens.putInt(Integer.BYTES, 2);
        tokens.putInt(2 * Integer.BYTES, 1);
        ByteBuffer plan = ByteBuffer.allocateDirect(56).order(ByteOrder.nativeOrder());

        assertEquals(2, coordinator.groupHashTokens(tokens, 3, plan));
        assertEquals(1, plane.tokenGroupCalls);
        assertEquals(1, coordinator.groupCalls());
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

    @Test
    void testResidentMutationVectorsUseOneCheckAndOneUpdateFill() throws Exception {
        FakePlane plane = new FakePlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);

        java.util.List<byte[]> keys =
                Arrays.asList(new byte[] {1}, new byte[] {2}, new byte[] {3});
        int[] checks = coordinator.tryCheckExactKeysPresent(17, 31L, keys);
        assertArrayEquals(
                new int[] {
                    NativeRequestPlaneBridge.FILL_UPDATED,
                    NativeRequestPlaneBridge.FILL_UPDATED,
                    NativeRequestPlaneBridge.FILL_UPDATED
                },
                checks);

        int[] updates =
                coordinator.updateExactKeysIfPresent(
                        17,
                        31L,
                        keys,
                        Arrays.asList(new byte[] {4}, null, new byte[] {5, 6}));
        assertArrayEquals(checks, updates);
        assertEquals(2, coordinator.fillCalls());
        coordinator.close();
    }

    @Test
    void testResidentHintLearnsEveryAcceptedSingleAndBatchFillWithoutCrossStateLeakage()
            throws Exception {
        FakePlane plane = new FakePlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(residentMutationBatchOptions(1), plane);
        byte[] singleKey = new byte[] {7, 8, 9};
        byte[] batchKey = new byte[] {10, 11, 12};

        assertTrue(coordinator.residentKeyHintEnabledForTesting());
        assertFalse(coordinator.mightContainResidentKey(71, singleKey));
        assertEquals(
                NativeRequestPlaneBridge.FILL_INSERTED,
                coordinator.updateExactKey(71, 1L, singleKey, new byte[] {1}));
        assertTrue(coordinator.mightContainResidentKey(71, singleKey));
        assertFalse(coordinator.mightContainResidentKey(72, singleKey));

        try (NativeRequestPlaneCoordinator.BatchSlot slot = coordinator.tryAcquireBatchSlot()) {
            assertNotNull(slot);
            slot.prepareFill(
                    71,
                    1L,
                    java.util.Collections.singletonList(batchKey),
                    java.util.Collections.singletonList(new byte[] {2}));
            assertEquals(1, coordinator.fill(slot));
        }
        assertTrue(coordinator.mightContainResidentKey(71, batchKey));
        coordinator.close();
    }

    @Test
    void testResidentHintIsNotAllocatedWhenResidentMutationBatchIsDisabled() throws Exception {
        FakePlane plane = new FakePlane();
        NativeRequestPlaneCoordinator coordinator =
                NativeRequestPlaneCoordinator.forTesting(options(1), plane);
        byte[] key = new byte[] {7, 8, 9};

        assertFalse(coordinator.residentKeyHintEnabledForTesting());
        assertTrue(coordinator.mightContainResidentKey(71, key));
        assertEquals(
                NativeRequestPlaneBridge.FILL_INSERTED,
                coordinator.updateExactKey(71, 1L, key, new byte[] {1}));
        assertTrue(coordinator.mightContainResidentKey(71, key));
        coordinator.close();
    }

    private static NativeRequestPlaneOptions options(int slots) {
        return new NativeRequestPlaneOptions(
                true, "", "auto", 16, 1024, 1024, 4, 1024, 1024, 1, slots, false);
    }

    private static NativeRequestPlaneOptions directArenaOptions(int slots) {
        return new NativeRequestPlaneOptions(
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
                slots,
                false,
                false,
                false,
                false,
                false,
                true,
                true,
                false,
                true,
                true,
                false,
                8192,
                0.02,
                262144);
    }

    private static NativeRequestPlaneOptions residentMutationBatchOptions(int slots) {
        return new NativeRequestPlaneOptions(
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
                slots,
                false,
                true,
                true,
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                8192,
                0.02,
                262144,
                false,
                true,
                true);
    }

    private static NativeRequestPlaneOptions optionsWithCompactionScratch(int slots) {
        return optionsWithCompactionScratch(slots, 4, 1024);
    }

    private static NativeRequestPlaneOptions optionsWithCompactionScratch(
            int slots, int scratchEntries, int scratchKeyArenaBytes) {
        return new NativeRequestPlaneOptions(
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
                slots,
                false,
                false,
                false,
                false,
                false,
                true,
                true,
                false,
                true,
                true,
                false,
                8192,
                0.02,
                262144,
                false,
                false,
                false,
                true,
                false,
                false,
                true,
                scratchEntries,
                scratchKeyArenaBytes);
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
        private int tokenGroupCalls;

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
            fillNegative = (flags & NativeRequestPlaneBridge.FILL_VALUE_NEGATIVE_FLAG) != 0;
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
            for (int index = 0; index < keys.entryCount(); index++) {
                int metadataBase =
                        index * NativeRequestPlaneBridge.FILL_VALUE_RECORD_BYTES;
                int controlFlags =
                        metadata.getInt(
                                metadataBase
                                        + NativeRequestPlaneBridge.FILL_VALUE_RESERVED_OFFSET);
                int resultBase = index * NativeRequestPlaneBridge.FILL_RESULT_RECORD_BYTES;
                results.putInt(
                        resultBase + NativeRequestPlaneBridge.FILL_RESULT_STATUS_OFFSET,
                        controlFlags == 0
                                ? NativeRequestPlaneBridge.FILL_INSERTED
                                : NativeRequestPlaneBridge.FILL_UPDATED);
                results.putInt(
                        resultBase + NativeRequestPlaneBridge.FILL_RESULT_ERROR_OFFSET,
                        NativeRequestPlaneBridge.ERROR_OK);
            }
            return keys.entryCount();
        }

        @Override
        public int probeBatch(
                SerializedKeyBatch<?, ?> keys, ByteBuffer valueOutput, ByteBuffer probeResults) {
            if (keys.entryCount() > 0) {
                probeGeneration = keys.generation(0);
            }
            return keys.entryCount();
        }

        @Override
        public int compactBatch(SerializedKeyBatch<?, ?> keys, ByteBuffer uniqueSourceIndexes) {
            if (compactIndexes == null) {
                return NativeRequestPlane.super.compactBatch(keys, uniqueSourceIndexes);
            }
            ByteBuffer output =
                    uniqueSourceIndexes.duplicate().order(java.nio.ByteOrder.nativeOrder());
            for (int index = 0; index < compactIndexes.length; index++) {
                output.putInt(index * Integer.BYTES, compactIndexes[index]);
            }
            return compactIndexes.length;
        }

        @Override
        public int groupHashTokens(ByteBuffer tokens, int count, ByteBuffer packedPlan) {
            tokenGroupCalls++;
            return 2;
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
