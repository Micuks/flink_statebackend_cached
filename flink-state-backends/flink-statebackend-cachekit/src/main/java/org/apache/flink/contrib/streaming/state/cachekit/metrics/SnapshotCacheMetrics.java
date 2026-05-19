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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Counters for MapSnapshot (entries() 0/1 fast-path) instrumentation.
 *
 * <p>Each {@link org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalMapState}
 * instance holds one of these. On backend close/dispose the counters are flushed to
 * {@code /home/data/snapshot_cache_metrics.csv}.
 *
 * <p>Tracked metrics:
 * <ul>
 *   <li><b>entries_total</b> – total calls to entries() / keys() / values() / iterator()</li>
 *   <li><b>snapshot_hit_empty</b> – short-circuit hits where snapshot == EMPTY (0 elements)</li>
 *   <li><b>snapshot_hit_single</b> – short-circuit hits where snapshot == SINGLE (1 element)</li>
 *   <li><b>snapshot_miss</b> – snapshot cache misses (fallthrough to delegate)</li>
 *   <li><b>backfill_empty</b> – backfill produced EMPTY snapshot</li>
 *   <li><b>backfill_single</b> – backfill produced SINGLE snapshot</li>
 *   <li><b>backfill_multi</b> – backfill skipped (>1 element, no snapshot stored)</li>
 *   <li><b>snapshot_invalidated</b> – snapshot invalidated by put/remove/putAll/clear</li>
 * </ul>
 */
public final class SnapshotCacheMetrics {

    // --- entries()/keys()/values()/iterator() call counts ---
    private final AtomicLong entriesTotal = new AtomicLong();

    // --- short-circuit hit breakdown ---
    private final AtomicLong snapshotHitEmpty = new AtomicLong();
    private final AtomicLong snapshotHitSingle = new AtomicLong();

    // --- cache miss (fallthrough to delegate) ---
    private final AtomicLong snapshotMiss = new AtomicLong();

    // --- backfill breakdown ---
    private final AtomicLong backfillEmpty = new AtomicLong();
    private final AtomicLong backfillSingle = new AtomicLong();
    private final AtomicLong backfillMulti = new AtomicLong();

    // --- invalidation count ---
    private final AtomicLong snapshotInvalidated = new AtomicLong();

    /** An identifier so we can distinguish multiple MapState instances in the output. */
    private final String stateId;

    public SnapshotCacheMetrics(String stateId) {
        this.stateId = stateId != null ? stateId : "unknown";
    }

    // ---- Recording methods ----

    public void recordEntriesCall() {
        entriesTotal.incrementAndGet();
    }

    public void recordSnapshotHitEmpty() {
        snapshotHitEmpty.incrementAndGet();
    }

    public void recordSnapshotHitSingle() {
        snapshotHitSingle.incrementAndGet();
    }

    public void recordSnapshotMiss() {
        snapshotMiss.incrementAndGet();
    }

    public void recordBackfillEmpty() {
        backfillEmpty.incrementAndGet();
    }

    public void recordBackfillSingle() {
        backfillSingle.incrementAndGet();
    }

    public void recordBackfillMulti() {
        backfillMulti.incrementAndGet();
    }

    public void recordSnapshotInvalidated() {
        snapshotInvalidated.incrementAndGet();
    }

    // ---- Getters (for testing / inspection) ----

    public long getEntriesTotal() { return entriesTotal.get(); }
    public long getSnapshotHitEmpty() { return snapshotHitEmpty.get(); }
    public long getSnapshotHitSingle() { return snapshotHitSingle.get(); }
    public long getSnapshotMiss() { return snapshotMiss.get(); }
    public long getBackfillEmpty() { return backfillEmpty.get(); }
    public long getBackfillSingle() { return backfillSingle.get(); }
    public long getBackfillMulti() { return backfillMulti.get(); }
    public long getSnapshotInvalidated() { return snapshotInvalidated.get(); }

    /**
     * Flush all counters as a single CSV row to {@code /home/data/snapshot_cache_metrics.csv}.
     * Appends to the file if it already exists, creates it (and parent dirs) otherwise.
     * The first call will also write a CSV header if the file is new.
     */
    public void flushToFile() {
        File dir = new File("/home/data");
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File file = new File(dir, "snapshot_cache_metrics.csv");
        boolean needsHeader = !file.exists() || file.length() == 0;

        try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file, true)))) {
            if (needsHeader) {
                pw.println("state_id,entries_total,"
                        + "snapshot_hit_empty,snapshot_hit_single,"
                        + "snapshot_miss,"
                        + "backfill_empty,backfill_single,backfill_multi,"
                        + "snapshot_invalidated,"
                        + "hit_rate");
            }
            long total = entriesTotal.get();
            long hitEmpty = snapshotHitEmpty.get();
            long hitSingle = snapshotHitSingle.get();
            long hits = hitEmpty + hitSingle;
            double hitRate = total > 0 ? (double) hits / total : 0.0;

            pw.printf("%s,%d,%d,%d,%d,%d,%d,%d,%d,%.6f%n",
                    stateId,
                    total,
                    hitEmpty,
                    hitSingle,
                    snapshotMiss.get(),
                    backfillEmpty.get(),
                    backfillSingle.get(),
                    backfillMulti.get(),
                    snapshotInvalidated.get(),
                    hitRate);
        } catch (IOException e) {
            // Best-effort; log to stderr so it shows in TM logs.
            System.err.println("[SnapshotCacheMetrics] Failed to flush metrics to "
                    + file.getAbsolutePath() + ": " + e.getMessage());
        }
    }

    @Override
    public String toString() {
        long total = entriesTotal.get();
        long hits = snapshotHitEmpty.get() + snapshotHitSingle.get();
        double hitRate = total > 0 ? (double) hits / total : 0.0;
        return String.format(
                "SnapshotCacheMetrics[state=%s, total=%d, hitEmpty=%d, hitSingle=%d, miss=%d, "
                        + "backfillEmpty=%d, backfillSingle=%d, backfillMulti=%d, invalidated=%d, hitRate=%.4f]",
                stateId, total, snapshotHitEmpty.get(), snapshotHitSingle.get(),
                snapshotMiss.get(), backfillEmpty.get(), backfillSingle.get(),
                backfillMulti.get(), snapshotInvalidated.get(), hitRate);
    }
}
