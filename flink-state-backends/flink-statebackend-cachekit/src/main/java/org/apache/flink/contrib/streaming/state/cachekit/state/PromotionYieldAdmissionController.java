/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import java.util.concurrent.atomic.AtomicLong;

/**
 * State-local admission for speculative prefetch based on values actually consumed by the mailbox.
 *
 * <p>A RocksDB hit is not evidence that lookahead was useful: a state can fill the staging map with
 * values that no following record consumes. This controller therefore compares promoted values
 * with staged values. Low-yield states stop occupying the TM-wide prefetch worker, while periodic
 * probes let a phase-changing state recover. Rejection only suppresses speculative work; the
 * authoritative point-read path is unchanged.
 */
final class PromotionYieldAdmissionController {

    private final long minimumStagedValues;
    private final double minimumPromotionRate;
    private final long probeEveryDecisions;
    private final AtomicLong lowYieldDecisions = new AtomicLong();
    private final AtomicLong skippedTasks = new AtomicLong();
    private final AtomicLong probeTasks = new AtomicLong();

    PromotionYieldAdmissionController(
            long minimumStagedValues,
            double minimumPromotionRate,
            long probeEveryDecisions) {
        if (minimumStagedValues < 1) {
            throw new IllegalArgumentException("minimumStagedValues must be positive");
        }
        if (minimumPromotionRate < 0.0 || minimumPromotionRate > 1.0) {
            throw new IllegalArgumentException("minimumPromotionRate must be in [0, 1]");
        }
        if (probeEveryDecisions < 1) {
            throw new IllegalArgumentException("probeEveryDecisions must be positive");
        }
        this.minimumStagedValues = minimumStagedValues;
        this.minimumPromotionRate = minimumPromotionRate;
        this.probeEveryDecisions = probeEveryDecisions;
    }

    boolean shouldAdmit(long stagedValues, long promotedValues) {
        if (stagedValues < minimumStagedValues
                || promotedValues >= stagedValues * minimumPromotionRate) {
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

    long skippedTasks() {
        return skippedTasks.get();
    }

    long probeTasks() {
        return probeTasks.get();
    }
}
