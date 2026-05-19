/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.metrics;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Lightweight metrics collector for MapSnapshot cache (entries() short-circuit).
 *
 * <p>Tracks per-state counters for snapshot cache operations and periodically
 * flushes them to a CSV file for offline analysis. Uses {@link LongAdder} for
 * lock-free, high-throughput counting.
 *
 * <p>Metrics are organized per {@code stateName} so that different MapState
 * instances (e.g., different Nexmark operators) are tracked separately.
 *
 * <p>CSV columns:
 * {@code timestamp, state_name, hit_empty, hit_single, miss, fallback,
 *        fill_empty, fill_single, invalidate, entries_total,
 *        hit_rate, fallback_rate}
 */
public final class SnapshotCacheMetrics {

    /** Per-metric counters for a single MapState instance. */
    public static final class StateMetrics {
        private final String stateName;
        public final LongAdder hitEmpty = new LongAdder();
        public final LongAdder hitSingle = new LongAdder();
        public final LongAdder miss = new LongAdder();
        public final LongAdder fallback = new LongAdder();
        public final LongAdder fillEmpty = new LongAdder();
        public final LongAdder fillSingle = new LongAdder();
        public final LongAdder invalidate = new LongAdder();
        /** Total entries() / iterator() / keys() / values() calls. */
        public final LongAdder entriesTotal = new LongAdder();

        StateMetrics(String stateName) {
            this.stateName = stateName;
        }

        public String getStateName() {
            return stateName;
        }

        // ---- derived metrics (computed on demand) ----

        /** Total calls that hit the snapshot cache. */
        public long totalHits() {
            return hitEmpty.sum() + hitSingle.sum();
        }

        /** Total calls that penetrated the snapshot cache. */
        public long totalPenetrations() {
            return miss.sum() + fallback.sum();
        }

        /** Hit rate in [0, 1]; returns 0.0 if no calls recorded. */
        public double hitRate() {
            long hits = totalHits();
            long total = hits + totalPenetrations();
            return total == 0 ? 0.0 : (double) hits / total;
        }

        /** Fallback (stale data) rate among SINGLE hits. */
        public double fallbackRate() {
            long single = hitSingle.sum();
            long fb = fallback.sum();
            long total = single + fb;
            return total == 0 ? 0.0 : (double) fb / total;
        }

        /**
         * Returns a snapshot of all counters as a long array for CSV serialization.
         * Order: hitEmpty, hitSingle, miss, fallback, fillEmpty, fillSingle, invalidate, entriesTotal
         */
        long[] snapshot() {
            return new long[] {
                    hitEmpty.sum(),
                    hitSingle.sum(),
                    miss.sum(),
                    fallback.sum(),
                    fillEmpty.sum(),
                    fillSingle.sum(),
                    invalidate.sum(),
                    entriesTotal.sum()
            };
        }

        /**
         * Reset all counters (used after a CSV flush for incremental reporting).
         */
        void reset() {
            hitEmpty.reset();
            hitSingle.reset();
            miss.reset();
            fallback.reset();
            fillEmpty.reset();
            fillSingle.reset();
            invalidate.reset();
            entriesTotal.reset();
        }

        @Override
        public String toString() {
            return String.format(
                    "StateMetrics[%s]{hitEmpty=%d, hitSingle=%d, miss=%d, fallback=%d, "
                            + "fillEmpty=%d, fillSingle=%d, invalidate=%d, entriesTotal=%d, "
                            + "hitRate=%.4f, fallbackRate=%.4f}",
                    stateName,
                    hitEmpty.sum(), hitSingle.sum(), miss.sum(), fallback.sum(),
                    fillEmpty.sum(), fillSingle.sum(), invalidate.sum(), entriesTotal.sum(),
                    hitRate(), fallbackRate());
        }
    }

    // ---- singleton per-JVM registry ----

    private static final Map<String, StateMetrics> REGISTRY = new ConcurrentHashMap<>();
    private static volatile ScheduledExecutorService scheduler;
    private static volatile ScheduledFuture<?> scheduledTask;
    private static volatile String csvFilePath;
    private static volatile boolean incrementalMode = false;
    private static final Object INIT_LOCK = new Object();

    /** Get or create a metrics instance for the given state name. */
    public static StateMetrics forState(String stateName) {
        return REGISTRY.computeIfAbsent(
                stateName != null ? stateName : "unknown",
                StateMetrics::new);
    }

    /**
     * Initialize periodic CSV dumping.
     *
     * @param outputDir       directory for CSV files; null to disable file output.
     * @param intervalSec     interval in seconds between dumps; 0 to disable periodic dumps.
     * @param incremental     if true, counters are reset after each dump (incremental mode);
     *                        if false, counters are cumulative.
     * @param jobIdentifier   a human-readable identifier for the current job (e.g. job name or
     *                        short JobID). Used in the CSV filename to separate different queries.
     */
    public static void init(String outputDir, int intervalSec, boolean incremental,
                            String jobIdentifier) {
        synchronized (INIT_LOCK) {
            incrementalMode = incremental;

            // --- Per-job: switch CSV file if job changed ---
            if (outputDir != null && !outputDir.isBlank()) {
                File dir = new File(outputDir);
                if (!dir.exists()) {
                    dir.mkdirs();
                }
                String safeName = (jobIdentifier != null ? jobIdentifier : "unknown")
                        .replaceAll("[^a-zA-Z0-9_\\-]", "_");
                File csvFile = new File(dir, "snapshot_cache_metrics_" + safeName + ".csv");
                String newPath = csvFile.getAbsolutePath();

                // If switching to a new job file, dump remaining data from previous job first
                if (csvFilePath != null && !csvFilePath.equals(newPath)) {
                    dumpAll();
                    // Reset all counters for the new job
                    for (StateMetrics m : REGISTRY.values()) {
                        m.reset();
                    }
                }

                csvFilePath = newPath;
                // Write CSV header only if file is new
                if (!csvFile.exists() || csvFile.length() == 0) {
                    try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(csvFilePath, false)))) {
                        pw.println("timestamp,state_name,hit_empty,hit_single,miss,fallback,"
                                + "fill_empty,fill_single,invalidate,entries_total,"
                                + "hit_rate,fallback_rate");
                    } catch (IOException e) {
                        System.err.println("[SnapshotCacheMetrics] Failed to create CSV file: " + e.getMessage());
                        csvFilePath = null;
                    }
                }
                System.out.printf("[SnapshotCacheMetrics] CSV output: %s (job=%s), interval=%ds, incremental=%s%n",
                        csvFilePath, jobIdentifier, intervalSec, incremental);
            }

            // --- Per-JVM: scheduler + shutdown hook only once ---
            if (scheduler == null && intervalSec > 0) {
                scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "snapshot-cache-metrics-dumper");
                    t.setDaemon(true);
                    return t;
                });
                scheduledTask = scheduler.scheduleAtFixedRate(
                        SnapshotCacheMetrics::dumpAll,
                        intervalSec, intervalSec, TimeUnit.SECONDS);

                // Shutdown hook to dump final metrics
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    dumpAll();
                    printSummary();
                }, "snapshot-cache-metrics-shutdown"));
            }
        }
    }

    /**
     * Dump all metrics to CSV and optionally to stdout.
     */
    public static void dumpAll() {
        if (REGISTRY.isEmpty()) {
            return;
        }
        String ts = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date());

        if (csvFilePath != null) {
            try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(csvFilePath, true)))) {
                for (StateMetrics m : REGISTRY.values()) {
                    long[] s = m.snapshot();
                    pw.printf("%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%.6f,%.6f%n",
                            ts, m.stateName,
                            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
                            m.hitRate(), m.fallbackRate()
                    );
                }
            } catch (IOException e) {
                System.err.println("[SnapshotCacheMetrics] CSV write failed: " + e.getMessage());
            }
        }

        if (incrementalMode) {
            for (StateMetrics m : REGISTRY.values()) {
                m.reset();
            }
        }
    }

    /**
     * Print a human-readable summary to stdout.
     */
    public static void printSummary() {
        if (REGISTRY.isEmpty()) {
            return;
        }
        System.out.println("========================================");
        System.out.println("  SnapshotCache Metrics Summary");
        System.out.println("========================================");
        for (StateMetrics m : REGISTRY.values()) {
            System.out.println(m);
        }
        System.out.println("========================================");
    }

    /**
     * Shutdown the periodic dumper (if active).
     */
    public static void shutdown() {
        synchronized (INIT_LOCK) {
            if (scheduledTask != null) {
                scheduledTask.cancel(false);
                scheduledTask = null;
            }
            if (scheduler != null) {
                scheduler.shutdown();
                try {
                    scheduler.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                scheduler = null;
            }
        }
    }

    /** Clear all metrics (for testing). */
    public static void clearAll() {
        REGISTRY.clear();
    }

    private SnapshotCacheMetrics() {
        // utility class
    }
}
