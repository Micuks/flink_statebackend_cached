/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class AdaptiveNativeMailboxDensityControllerTest {

    @Test
    void remainsActiveForHighDensityCompaction() {
        AdaptiveNativeMailboxDensityController controller = controller();

        controller.recordCompaction(100, 30);
        controller.recordCompaction(100, 20);

        assertEquals(AdaptiveNativeMailboxDensityController.Mode.ACTIVE, controller.mode());
        assertTrue(controller.shouldUseNativeMailbox());
        assertEquals(1, controller.completedWindows());
    }

    @Test
    void bypassesAfterConsecutiveLowDensityWindows() {
        AdaptiveNativeMailboxDensityController controller = controller();

        controller.recordCompaction(100, 5);
        controller.recordCompaction(100, 5);
        controller.recordCompaction(100, 4);
        controller.recordCompaction(100, 6);

        assertEquals(AdaptiveNativeMailboxDensityController.Mode.BYPASS, controller.mode());
        assertFalse(controller.shouldUseNativeMailbox());
        controller.recordBypassedInputKeys(128);
        controller.recordDroppedSpeculativePrefetchTask();
        assertEquals(1, controller.bypassedBatches());
        assertEquals(128, controller.bypassedInputKeys());
        assertEquals(1, controller.droppedSpeculativePrefetchTasks());
    }

    @Test
    void recoveryWindowRestoresNativePath() {
        AdaptiveNativeMailboxDensityController controller = controller();
        for (int window = 0; window < 2; window++) {
            controller.recordCompaction(100, 5);
            controller.recordCompaction(100, 5);
        }

        assertFalse(controller.shouldUseNativeMailbox());
        assertFalse(controller.shouldUseNativeMailbox());
        assertTrue(controller.shouldUseNativeMailbox());
        assertEquals(AdaptiveNativeMailboxDensityController.Mode.RECOVERY, controller.mode());
        controller.recordCompaction(100, 30);
        controller.recordCompaction(100, 30);
        assertEquals(AdaptiveNativeMailboxDensityController.Mode.ACTIVE, controller.mode());
    }

    @Test
    void lowDensityRecoveryReturnsToBypass() {
        AdaptiveNativeMailboxDensityController controller = controller();
        for (int window = 0; window < 2; window++) {
            controller.recordCompaction(100, 5);
            controller.recordCompaction(100, 5);
        }
        controller.shouldUseNativeMailbox();
        controller.shouldUseNativeMailbox();
        assertTrue(controller.shouldUseNativeMailbox());
        controller.recordCompaction(100, 10);
        controller.recordCompaction(100, 10);
        assertEquals(AdaptiveNativeMailboxDensityController.Mode.BYPASS, controller.mode());
    }

    @Test
    void rejectsInvalidThresholds() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new AdaptiveNativeMailboxDensityController(0, 2, 2, 3, 0.10, 0.20));
        assertThrows(
                IllegalArgumentException.class,
                () -> new AdaptiveNativeMailboxDensityController(100, 2, 2, 3, 0.30, 0.20));
    }

    private static AdaptiveNativeMailboxDensityController controller() {
        return new AdaptiveNativeMailboxDensityController(200, 2, 2, 3, 0.10, 0.20);
    }
}
