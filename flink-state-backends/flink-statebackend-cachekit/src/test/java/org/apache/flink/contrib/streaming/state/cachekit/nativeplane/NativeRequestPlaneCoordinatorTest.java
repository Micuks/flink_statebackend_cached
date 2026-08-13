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
import java.util.Collections;

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
        assertEquals(4_448, coordinator.regularSlotDirectBytesForTesting());
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
    void testLatestProbeSentinelDoesNotLeakIntoConditionalFillGeneration() throws Exception {
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
            assertEquals(NativeRequestPlaneBridge.PROBE_LATEST_GENERATION, plane.probeGeneration);
        }
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

        @Override
        public int fillBatch(
                SerializedKeyBatch<?, ?> keys,
                ByteBuffer valueArena,
                ByteBuffer valueMetadata,
                ByteBuffer fillResults) {
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
