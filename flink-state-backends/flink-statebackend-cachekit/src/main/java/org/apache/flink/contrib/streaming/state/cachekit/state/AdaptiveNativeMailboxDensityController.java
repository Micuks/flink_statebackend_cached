/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

/**
 * State-local admission controller for native mailbox compaction.
 *
 * <p>Native compaction serializes every lookahead key before removing duplicates. That is useful
 * for high-entropy batches, but duplicate-heavy states can spend substantially more time crossing
 * the JNI boundary than the later state reads save. This controller samples the ratio of compacted
 * unique keys to input keys and temporarily routes a low-density state through the existing Java
 * prepared-MultiGet path. Periodic recovery windows preserve phase-change liveness. Rejection only
 * changes the speculative preparation implementation; reservations, RocksDB reads and state
 * semantics remain on the existing authoritative paths.
 */
final class AdaptiveNativeMailboxDensityController {

    enum Mode {
        ACTIVE,
        BYPASS,
        RECOVERY
    }

    private final long windowInputKeys;
    private final long windowBatches;
    private final int lowDensityWindowsBeforeBypass;
    private final long cooldownBatches;
    private final double minimumUniqueRate;
    private final double recoveryUniqueRate;

    private Mode mode = Mode.ACTIVE;
    private long currentInputKeys;
    private long currentUniqueKeys;
    private long currentBatches;
    private int consecutiveLowDensityWindows;
    private long cooldownProgress;
    private long transitions;
    private long completedWindows;
    private long bypassedBatches;
    private long bypassedInputKeys;
    private long droppedSpeculativePrefetchTasks;

    AdaptiveNativeMailboxDensityController(
            long windowInputKeys,
            long windowBatches,
            int lowDensityWindowsBeforeBypass,
            long cooldownBatches,
            double minimumUniqueRate,
            double recoveryUniqueRate) {
        if (windowInputKeys <= 0
                || windowBatches <= 0
                || lowDensityWindowsBeforeBypass <= 0
                || cooldownBatches <= 0
                || minimumUniqueRate < 0.0
                || minimumUniqueRate > 1.0
                || recoveryUniqueRate < minimumUniqueRate
                || recoveryUniqueRate > 1.0) {
            throw new IllegalArgumentException("Invalid adaptive native-mailbox density thresholds.");
        }
        this.windowInputKeys = windowInputKeys;
        this.windowBatches = windowBatches;
        this.lowDensityWindowsBeforeBypass = lowDensityWindowsBeforeBypass;
        this.cooldownBatches = cooldownBatches;
        this.minimumUniqueRate = minimumUniqueRate;
        this.recoveryUniqueRate = recoveryUniqueRate;
    }

    /** Returns whether the next mailbox batch should use native compaction. */
    synchronized boolean shouldUseNativeMailbox() {
        if (mode != Mode.BYPASS) {
            return true;
        }
        cooldownProgress++;
        if (cooldownProgress >= cooldownBatches) {
            transitionTo(Mode.RECOVERY);
            resetWindow();
            return true;
        }
        bypassedBatches++;
        return false;
    }

    /** Records the input size of a batch routed through the Java preparation path. */
    synchronized void recordBypassedInputKeys(int inputKeys) {
        if (mode == Mode.BYPASS && inputKeys > 0) {
            bypassedInputKeys += inputKeys;
        }
    }

    /** Records a speculative task rejected before key materialization or reservation. */
    synchronized void recordDroppedSpeculativePrefetchTask() {
        if (mode == Mode.BYPASS) {
            droppedSpeculativePrefetchTasks++;
        }
    }

    /** Records one successful native compaction sample. Failed attempts are not observations. */
    synchronized void recordCompaction(int inputKeys, int uniqueKeys) {
        if (mode == Mode.BYPASS
                || inputKeys <= 0
                || uniqueKeys < 0
                || uniqueKeys > inputKeys) {
            return;
        }
        currentBatches++;
        currentInputKeys += inputKeys;
        currentUniqueKeys += uniqueKeys;
        if (currentBatches < windowBatches || currentInputKeys < windowInputKeys) {
            return;
        }

        completedWindows++;
        double uniqueRate = currentUniqueKeys / (double) currentInputKeys;
        if (mode == Mode.ACTIVE) {
            consecutiveLowDensityWindows =
                    uniqueRate < minimumUniqueRate ? consecutiveLowDensityWindows + 1 : 0;
            if (consecutiveLowDensityWindows >= lowDensityWindowsBeforeBypass) {
                transitionTo(Mode.BYPASS);
                cooldownProgress = 0;
            }
        } else if (uniqueRate >= recoveryUniqueRate) {
            consecutiveLowDensityWindows = 0;
            transitionTo(Mode.ACTIVE);
        } else {
            transitionTo(Mode.BYPASS);
            cooldownProgress = 0;
        }
        resetWindow();
    }

    private void transitionTo(Mode target) {
        if (mode != target) {
            mode = target;
            transitions++;
        }
    }

    private void resetWindow() {
        currentInputKeys = 0;
        currentUniqueKeys = 0;
        currentBatches = 0;
    }

    synchronized Mode mode() {
        return mode;
    }

    synchronized long transitions() {
        return transitions;
    }

    synchronized long completedWindows() {
        return completedWindows;
    }

    synchronized long bypassedBatches() {
        return bypassedBatches;
    }

    synchronized long bypassedInputKeys() {
        return bypassedInputKeys;
    }

    synchronized long droppedSpeculativePrefetchTasks() {
        return droppedSpeculativePrefetchTasks;
    }
}
