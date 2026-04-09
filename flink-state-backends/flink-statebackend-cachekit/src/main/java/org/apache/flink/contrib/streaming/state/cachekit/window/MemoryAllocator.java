/*
 * Licensed under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state.cachekit.window;

/**
 * Dynamically allocates memory among window write buffer, prefetch buffer,
 * CacheKit cache, and RocksDB block cache based on the detected system phase.
 *
 * <p>Call {@link #rebalance()} periodically (e.g., every 5 seconds). The allocator
 * computes new budgets and returns them. The caller is responsible for applying
 * the budgets to each component.
 */
public class MemoryAllocator {

    private final long totalBudget;
    private final PhaseDetector phaseDetector;

    private long writeBufferBudget;
    private long prefetchBufferBudget;
    private long cacheKitBudget;
    private long blockCacheBudget;

    public MemoryAllocator(long totalBudget, PhaseDetector phaseDetector) {
        this.totalBudget = totalBudget;
        this.phaseDetector = phaseDetector;
        // Default balanced allocation
        rebalance();
    }

    /** Recomputes memory budgets based on current phase. Returns true if budgets changed. */
    public boolean rebalance() {
        PhaseDetector.SystemPhase phase = phaseDetector.detect();

        long oldWb = writeBufferBudget;
        long oldPf = prefetchBufferBudget;
        long oldCk = cacheKitBudget;
        long oldBc = blockCacheBudget;

        switch (phase) {
            case ACCUMULATING:
                writeBufferBudget   = (long) (totalBudget * 0.40);
                prefetchBufferBudget = (long) (totalBudget * 0.05);
                cacheKitBudget      = (long) (totalBudget * 0.35);
                blockCacheBudget    = (long) (totalBudget * 0.20);
                break;
            case TRIGGERING:
                writeBufferBudget   = (long) (totalBudget * 0.15);
                prefetchBufferBudget = (long) (totalBudget * 0.25);
                cacheKitBudget      = (long) (totalBudget * 0.30);
                blockCacheBudget    = (long) (totalBudget * 0.30);
                break;
            case BALANCED:
            default:
                writeBufferBudget   = (long) (totalBudget * 0.25);
                prefetchBufferBudget = (long) (totalBudget * 0.15);
                cacheKitBudget      = (long) (totalBudget * 0.30);
                blockCacheBudget    = (long) (totalBudget * 0.30);
                break;
        }

        return writeBufferBudget != oldWb || prefetchBufferBudget != oldPf
                || cacheKitBudget != oldCk || blockCacheBudget != oldBc;
    }

    public long getWriteBufferBudget() {
        return writeBufferBudget;
    }

    public long getPrefetchBufferBudget() {
        return prefetchBufferBudget;
    }

    public long getCacheKitBudget() {
        return cacheKitBudget;
    }

    public long getBlockCacheBudget() {
        return blockCacheBudget;
    }

    public long getTotalBudget() {
        return totalBudget;
    }

    public PhaseDetector.SystemPhase getCurrentPhase() {
        return phaseDetector.detect();
    }
}
