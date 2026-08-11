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

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Task-level diagnostics for the MapState EMPTY/SINGLE snapshot cache.
 *
 * <p>One instance is shared by all MapState wrappers in a keyed backend so that the REST metrics
 * remain unique within the task metric group. Diagnostics are intentionally opt-in because the
 * counters are on MapState hot paths.
 */
public final class MapSnapshotCacheMetrics {

    private static final MapSnapshotCacheMetrics DISABLED = new MapSnapshotCacheMetrics(false);

    private final boolean enabled;
    private final AtomicLong probes = new AtomicLong();
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong emptyShortCircuits = new AtomicLong();
    private final AtomicLong singleShortCircuits = new AtomicLong();
    private final AtomicLong storesEmpty = new AtomicLong();
    private final AtomicLong storesSingle = new AtomicLong();
    private final AtomicLong multiEntrySkips = new AtomicLong();
    private final AtomicLong invalidations = new AtomicLong();
    private final AtomicLong staleInvalidations = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();

    private MapSnapshotCacheMetrics(boolean enabled) {
        this.enabled = enabled;
    }

    public static MapSnapshotCacheMetrics create(MetricGroup metricGroup, boolean enabled) {
        if (!enabled || metricGroup == null) {
            return DISABLED;
        }

        MapSnapshotCacheMetrics metrics = new MapSnapshotCacheMetrics(true);
        MetricGroup diagnostics = metricGroup.addGroup("cachekit").addGroup("diagnostics");
        registerGauge(diagnostics, "map_snapshot_cache_probes", metrics.probes);
        registerGauge(diagnostics, "map_snapshot_cache_hits", metrics.hits);
        registerGauge(diagnostics, "map_snapshot_cache_misses", metrics.misses);
        registerGauge(
                diagnostics,
                "map_snapshot_cache_empty_short_circuits",
                metrics.emptyShortCircuits);
        registerGauge(
                diagnostics,
                "map_snapshot_cache_single_short_circuits",
                metrics.singleShortCircuits);
        registerGauge(diagnostics, "map_snapshot_cache_stores_empty", metrics.storesEmpty);
        registerGauge(diagnostics, "map_snapshot_cache_stores_single", metrics.storesSingle);
        registerGauge(diagnostics, "map_snapshot_cache_multi_entry_skips", metrics.multiEntrySkips);
        registerGauge(diagnostics, "map_snapshot_cache_invalidations", metrics.invalidations);
        registerGauge(
                diagnostics,
                "map_snapshot_cache_stale_invalidations",
                metrics.staleInvalidations);
        registerGauge(diagnostics, "map_snapshot_cache_evictions", metrics.evictions);
        return metrics;
    }

    public static MapSnapshotCacheMetrics disabled() {
        return DISABLED;
    }

    static MapSnapshotCacheMetrics forTesting() {
        return new MapSnapshotCacheMetrics(true);
    }

    void recordProbe() {
        increment(probes);
    }

    void recordHit() {
        increment(hits);
    }

    void recordMiss() {
        increment(misses);
    }

    void recordEmptyShortCircuit() {
        increment(emptyShortCircuits);
    }

    void recordSingleShortCircuit() {
        increment(singleShortCircuits);
    }

    void recordStoreEmpty() {
        increment(storesEmpty);
    }

    void recordStoreSingle() {
        increment(storesSingle);
    }

    void recordMultiEntrySkip() {
        increment(multiEntrySkips);
    }

    void recordInvalidation() {
        increment(invalidations);
    }

    void recordStaleInvalidation() {
        increment(staleInvalidations);
    }

    void recordEviction() {
        increment(evictions);
    }

    long probes() {
        return probes.get();
    }

    long hits() {
        return hits.get();
    }

    long misses() {
        return misses.get();
    }

    long emptyShortCircuits() {
        return emptyShortCircuits.get();
    }

    long singleShortCircuits() {
        return singleShortCircuits.get();
    }

    long storesEmpty() {
        return storesEmpty.get();
    }

    long storesSingle() {
        return storesSingle.get();
    }

    long multiEntrySkips() {
        return multiEntrySkips.get();
    }

    long invalidations() {
        return invalidations.get();
    }

    private void increment(AtomicLong counter) {
        if (enabled) {
            counter.incrementAndGet();
        }
    }

    private static void registerGauge(MetricGroup metricGroup, String name, AtomicLong value) {
        metricGroup.gauge(name, (Gauge<Long>) value::get);
    }
}
