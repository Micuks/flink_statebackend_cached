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

class PromotionYieldAdmissionControllerTest {

    @Test
    void admitsTrainingAndProductiveStates() {
        PromotionYieldAdmissionController controller = controller();

        assertTrue(controller.shouldAdmit(63, 0));
        assertTrue(controller.shouldAdmit(100, 10));
        assertEquals(0, controller.skippedTasks());
        assertEquals(0, controller.probeTasks());
    }

    @Test
    void suppressesLowYieldStateButPeriodicallyProbes() {
        PromotionYieldAdmissionController controller = controller();

        assertFalse(controller.shouldAdmit(100, 1));
        assertFalse(controller.shouldAdmit(100, 1));
        assertTrue(controller.shouldAdmit(100, 1));
        assertFalse(controller.shouldAdmit(100, 1));
        assertEquals(3, controller.skippedTasks());
        assertEquals(1, controller.probeTasks());
    }

    @Test
    void promotionRecoveryImmediatelyRestoresAdmission() {
        PromotionYieldAdmissionController controller = controller();

        assertFalse(controller.shouldAdmit(100, 0));
        assertTrue(controller.shouldAdmit(100, 5));
        assertTrue(controller.shouldAdmit(200, 20));
    }

    @Test
    void rejectsInvalidParameters() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new PromotionYieldAdmissionController(0, 0.05, 3));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PromotionYieldAdmissionController(64, 1.01, 3));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PromotionYieldAdmissionController(64, 0.05, 0));
    }

    private static PromotionYieldAdmissionController controller() {
        return new PromotionYieldAdmissionController(64, 0.05, 3);
    }
}
