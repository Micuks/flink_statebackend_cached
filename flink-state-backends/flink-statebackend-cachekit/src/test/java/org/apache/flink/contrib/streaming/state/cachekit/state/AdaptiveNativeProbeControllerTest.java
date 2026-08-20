/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class AdaptiveNativeProbeControllerTest {

    @Test
    void twoZeroWindowsBypassAndRecoveryHitRestoresActive() {
        AdaptiveNativeProbeController controller = controller();

        recordWindow(controller, 0);
        assertEquals(AdaptiveNativeProbeController.Mode.ACTIVE, controller.mode());
        recordWindow(controller, 0);
        assertEquals(AdaptiveNativeProbeController.Mode.BYPASS, controller.mode());

        assertFalse(controller.shouldProbe(8));
        assertFalse(controller.shouldProbe(8));
        assertTrue(controller.shouldProbe(8));
        assertEquals(AdaptiveNativeProbeController.Mode.RECOVERY, controller.mode());

        recordWindow(controller, 2);
        assertEquals(AdaptiveNativeProbeController.Mode.ACTIVE, controller.mode());
        assertEquals(2, controller.bypassedBatches());
        assertEquals(16, controller.bypassedKeys());
        assertEquals(3, controller.transitions());
    }

    @Test
    void negativeOrPositiveUsefulHitBreaksConsecutiveZeroWindows() {
        AdaptiveNativeProbeController controller = controller();
        recordWindow(controller, 0);
        recordWindow(controller, 1);
        recordWindow(controller, 0);
        assertEquals(AdaptiveNativeProbeController.Mode.ACTIVE, controller.mode());
        assertEquals(3, controller.completedWindows());
    }

    @Test
    void insufficientRecoveryReturnsToBypass() {
        AdaptiveNativeProbeController controller = controller();
        recordWindow(controller, 0);
        recordWindow(controller, 0);
        assertFalse(controller.shouldProbe(8));
        assertFalse(controller.shouldProbe(8));
        assertTrue(controller.shouldProbe(8));
        recordWindow(controller, 1);
        assertEquals(AdaptiveNativeProbeController.Mode.BYPASS, controller.mode());
    }

    private static AdaptiveNativeProbeController controller() {
        return new AdaptiveNativeProbeController(16, 2, 2, 3, 2, 0.10);
    }

    private static void recordWindow(AdaptiveNativeProbeController controller, int useful) {
        assertTrue(controller.shouldProbe(8));
        controller.recordProbe(8, useful);
        assertTrue(controller.shouldProbe(8));
        controller.recordProbe(8, 0);
    }
}
