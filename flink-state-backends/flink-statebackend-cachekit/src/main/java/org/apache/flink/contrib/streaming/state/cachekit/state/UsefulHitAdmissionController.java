/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import java.util.concurrent.atomic.AtomicLong;

/**
 * State-local admission for speculative reads based on values found by the backing store.
 *
 * <p>Exact-namespace projections can be semantically precise while still targeting keys that do
 * not exist yet. Once enough outcomes have been observed, low-hit streams are suppressed and only
 * periodic probes are admitted so a phase change can recover. Rejection never bypasses the
 * authoritative mailbox read.
 */
final class UsefulHitAdmissionController {

    private final long minimumObservedValues;
    private final double minimumUsefulRate;
    private final long probeEveryDecisions;
    private final AtomicLong observedValues = new AtomicLong();
    private final AtomicLong usefulValues = new AtomicLong();
    private final AtomicLong lowYieldDecisions = new AtomicLong();
    private final AtomicLong skippedTasks = new AtomicLong();
    private final AtomicLong probeTasks = new AtomicLong();

    UsefulHitAdmissionController(
            long minimumObservedValues, double minimumUsefulRate, long probeEveryDecisions) {
        if (minimumObservedValues < 1) {
            throw new IllegalArgumentException("minimumObservedValues must be positive");
        }
        if (minimumUsefulRate < 0.0 || minimumUsefulRate > 1.0) {
            throw new IllegalArgumentException("minimumUsefulRate must be in [0, 1]");
        }
        if (probeEveryDecisions < 1) {
            throw new IllegalArgumentException("probeEveryDecisions must be positive");
        }
        this.minimumObservedValues = minimumObservedValues;
        this.minimumUsefulRate = minimumUsefulRate;
        this.probeEveryDecisions = probeEveryDecisions;
    }

    boolean shouldAdmit() {
        long observed = observedValues.get();
        long useful = usefulValues.get();
        if (observed < minimumObservedValues || useful >= observed * minimumUsefulRate) {
            return true;
        }
        long decision = lowYieldDecisions.incrementAndGet();
        if (decision % probeEveryDecisions == 0) {
            probeTasks.incrementAndGet();
            return true;
        }
        skippedTasks.incrementAndGet();
        return false;
    }

    void recordOutcomes(long observed, long useful) {
        if (observed < 0 || useful < 0 || useful > observed) {
            throw new IllegalArgumentException(
                    "Useful values must be within the observed value count.");
        }
        if (observed == 0) {
            return;
        }
        observedValues.addAndGet(observed);
        usefulValues.addAndGet(useful);
    }

    long observedValues() {
        return observedValues.get();
    }

    long usefulValues() {
        return usefulValues.get();
    }

    long skippedTasks() {
        return skippedTasks.get();
    }

    long probeTasks() {
        return probeTasks.get();
    }
}
