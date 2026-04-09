/*
 * Licensed under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state.cachekit.window;

/**
 * Detects the current system phase (accumulation-heavy vs trigger-heavy) based on
 * the ratio of writes to reads. Used by the memory allocator to dynamically shift
 * memory between write buffer, prefetch buffer, CacheKit, and RocksDB block cache.
 */
public class PhaseDetector {

    private long recentWrites;
    private long recentReads;
    private long lastResetTime;
    private final long windowMs;

    public PhaseDetector(long windowMs) {
        this.windowMs = windowMs;
        this.lastResetTime = System.currentTimeMillis();
    }

    public void recordWrite() {
        maybeReset();
        recentWrites++;
    }

    public void recordRead() {
        maybeReset();
        recentReads++;
    }

    public SystemPhase detect() {
        long total = recentWrites + recentReads;
        if (total == 0) return SystemPhase.BALANCED;

        double writeRatio = (double) recentWrites / total;

        if (writeRatio > 0.8) return SystemPhase.ACCUMULATING;
        if (writeRatio < 0.3) return SystemPhase.TRIGGERING;
        return SystemPhase.BALANCED;
    }

    public double getWriteRatio() {
        long total = recentWrites + recentReads;
        if (total == 0) return 0.5;
        return (double) recentWrites / total;
    }

    private void maybeReset() {
        long now = System.currentTimeMillis();
        if (now - lastResetTime > windowMs) {
            recentWrites = 0;
            recentReads = 0;
            lastResetTime = now;
        }
    }

    public enum SystemPhase {
        ACCUMULATING,   // mostly writes
        TRIGGERING,     // mostly reads
        BALANCED        // mixed
    }
}
