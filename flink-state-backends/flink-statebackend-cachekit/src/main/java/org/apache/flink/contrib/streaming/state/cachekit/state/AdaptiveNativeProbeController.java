/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

/** Per-ValueState admission controller for speculative native prepared-key probe/fill. */
final class AdaptiveNativeProbeController {

    enum Mode {
        ACTIVE,
        BYPASS,
        RECOVERY
    }

    private final long windowKeys;
    private final long windowBatches;
    private final int zeroWindowsBeforeBypass;
    private final long cooldownBatches;
    private final long recoveryMinUseful;
    private final double recoveryMinUsefulRate;

    private Mode mode = Mode.ACTIVE;
    private long currentKeys;
    private long currentBatches;
    private long currentUseful;
    private int consecutiveZeroWindows;
    private long cooldownProgress;
    private long transitions;
    private long bypassedBatches;
    private long bypassedKeys;
    private long completedWindows;

    AdaptiveNativeProbeController(
            long windowKeys,
            long windowBatches,
            int zeroWindowsBeforeBypass,
            long cooldownBatches,
            long recoveryMinUseful,
            double recoveryMinUsefulRate) {
        if (windowKeys <= 0
                || windowBatches <= 0
                || zeroWindowsBeforeBypass <= 0
                || cooldownBatches <= 0
                || recoveryMinUseful < 0
                || recoveryMinUsefulRate < 0.0
                || recoveryMinUsefulRate > 1.0) {
            throw new IllegalArgumentException("Invalid adaptive native-probe thresholds.");
        }
        this.windowKeys = windowKeys;
        this.windowBatches = windowBatches;
        this.zeroWindowsBeforeBypass = zeroWindowsBeforeBypass;
        this.cooldownBatches = cooldownBatches;
        this.recoveryMinUseful = recoveryMinUseful;
        this.recoveryMinUsefulRate = recoveryMinUsefulRate;
    }

    /** Returns whether this batch should execute native probe/fill. */
    synchronized boolean shouldProbe(int keyCount) {
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
        bypassedKeys += Math.max(0, keyCount);
        return false;
    }

    /** Records one protocol-valid native probe. Failures and dropped/cancelled-before-probe tasks do not call this. */
    synchronized void recordProbe(int keyCount, int usefulCount) {
        if (mode == Mode.BYPASS || keyCount <= 0 || usefulCount < 0 || usefulCount > keyCount) {
            return;
        }
        currentBatches++;
        currentKeys += keyCount;
        currentUseful += usefulCount;
        if (currentBatches < windowBatches || currentKeys < windowKeys) {
            return;
        }

        completedWindows++;
        if (mode == Mode.ACTIVE) {
            consecutiveZeroWindows = currentUseful == 0 ? consecutiveZeroWindows + 1 : 0;
            if (consecutiveZeroWindows >= zeroWindowsBeforeBypass) {
                transitionTo(Mode.BYPASS);
                cooldownProgress = 0;
            }
        } else {
            double usefulRate = currentUseful / (double) currentKeys;
            if (currentUseful >= recoveryMinUseful
                    && usefulRate >= recoveryMinUsefulRate) {
                consecutiveZeroWindows = 0;
                transitionTo(Mode.ACTIVE);
            } else {
                transitionTo(Mode.BYPASS);
                cooldownProgress = 0;
            }
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
        currentKeys = 0;
        currentBatches = 0;
        currentUseful = 0;
    }

    synchronized Mode mode() {
        return mode;
    }

    synchronized long transitions() {
        return transitions;
    }

    synchronized long bypassedBatches() {
        return bypassedBatches;
    }

    synchronized long bypassedKeys() {
        return bypassedKeys;
    }

    synchronized long completedWindows() {
        return completedWindows;
    }
}
