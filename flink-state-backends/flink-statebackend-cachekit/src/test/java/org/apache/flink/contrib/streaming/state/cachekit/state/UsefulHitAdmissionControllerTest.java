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

class UsefulHitAdmissionControllerTest {

    @Test
    void admitsTrainingAndProductiveStreams() {
        UsefulHitAdmissionController controller = controller();

        controller.recordOutcomes(63, 0);
        assertTrue(controller.shouldAdmit());
        controller.recordOutcomes(37, 10);
        assertTrue(controller.shouldAdmit());
        assertEquals(100, controller.observedValues());
        assertEquals(10, controller.usefulValues());
    }

    @Test
    void suppressesMissOnlyStreamButPeriodicallyProbes() {
        UsefulHitAdmissionController controller = controller();
        controller.recordOutcomes(64, 0);

        assertFalse(controller.shouldAdmit());
        assertFalse(controller.shouldAdmit());
        assertTrue(controller.shouldAdmit());
        assertFalse(controller.shouldAdmit());
        assertEquals(3, controller.skippedTasks());
        assertEquals(1, controller.probeTasks());
    }

    @Test
    void usefulRecoveryImmediatelyRestoresAdmission() {
        UsefulHitAdmissionController controller = controller();
        controller.recordOutcomes(100, 0);
        assertFalse(controller.shouldAdmit());

        controller.recordOutcomes(100, 20);
        assertTrue(controller.shouldAdmit());
    }

    @Test
    void rejectsInvalidParametersAndOutcomes() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new UsefulHitAdmissionController(0, 0.01, 3));
        assertThrows(
                IllegalArgumentException.class,
                () -> new UsefulHitAdmissionController(64, 1.01, 3));
        assertThrows(
                IllegalArgumentException.class,
                () -> new UsefulHitAdmissionController(64, 0.01, 0));
        assertThrows(IllegalArgumentException.class, () -> controller().recordOutcomes(1, 2));
    }

    private static UsefulHitAdmissionController controller() {
        return new UsefulHitAdmissionController(64, 0.05, 3);
    }
}
