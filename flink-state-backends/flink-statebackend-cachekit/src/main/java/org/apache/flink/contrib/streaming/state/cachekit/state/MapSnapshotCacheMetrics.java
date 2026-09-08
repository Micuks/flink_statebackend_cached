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
 * Task-level diagnostics for the MapState EMPTY/SINGLE/SMALL snapshot cache.
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
    private final AtomicLong smallShortCircuits = new AtomicLong();
    private final AtomicLong storesEmpty = new AtomicLong();
    private final AtomicLong storesSingle = new AtomicLong();
    private final AtomicLong storesSmall = new AtomicLong();
    private final AtomicLong multiEntrySkips = new AtomicLong();
    private final AtomicLong invalidations = new AtomicLong();
    private final AtomicLong staleInvalidations = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();
    private final AtomicLong ownedKeyReuseActiveStates = new AtomicLong();
    private final AtomicLong ownedInternalKeysReused = new AtomicLong();
    private final AtomicLong internalKeyCopiesAvoided = new AtomicLong();
    private final AtomicLong exposedKeyCopiesDeferred = new AtomicLong();
    private final AtomicLong exposedKeyCopiesMaterialized = new AtomicLong();
    private final AtomicLong nativeCostOperations = new AtomicLong();
    private final AtomicLong nativeLookupOperations = new AtomicLong();
    private final AtomicLong nativePutOperations = new AtomicLong();
    private final AtomicLong nativeRemoveOperations = new AtomicLong();
    private final AtomicLong nativeCostRawKeySamples = new AtomicLong();
    private final AtomicLong nativeCostSerializedKeySamples = new AtomicLong();
    private final AtomicLong nativeCostInputBytes = new AtomicLong();
    private final AtomicLong nativeCostKeyEncodeNs = new AtomicLong();
    private final AtomicLong nativeRemoveRequests = new AtomicLong();
    private final AtomicLong nativeRemoveHintSkips = new AtomicLong();
    private final AtomicLong nativeRemoveJniCalls = new AtomicLong();
    private final AtomicLong nativeRemoveHits = new AtomicLong();
    private final AtomicLong nativeRemoveHintFalsePositives = new AtomicLong();
    private final NativeOperationCost nativeLookupCost = new NativeOperationCost();
    private final NativeOperationCost nativePutCost = new NativeOperationCost();
    private final NativeOperationCost nativeRemoveCost = new NativeOperationCost();

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
        registerGauge(
                diagnostics,
                "map_snapshot_cache_small_short_circuits",
                metrics.smallShortCircuits);
        registerGauge(diagnostics, "map_snapshot_cache_stores_empty", metrics.storesEmpty);
        registerGauge(diagnostics, "map_snapshot_cache_stores_single", metrics.storesSingle);
        registerGauge(diagnostics, "map_snapshot_cache_stores_small", metrics.storesSmall);
        registerGauge(diagnostics, "map_snapshot_cache_multi_entry_skips", metrics.multiEntrySkips);
        registerGauge(diagnostics, "map_snapshot_cache_invalidations", metrics.invalidations);
        registerGauge(
                diagnostics,
                "map_snapshot_cache_stale_invalidations",
                metrics.staleInvalidations);
        registerGauge(diagnostics, "map_snapshot_cache_evictions", metrics.evictions);
        registerGauge(
                diagnostics,
                "map_snapshot_owned_key_reuse_active_states",
                metrics.ownedKeyReuseActiveStates);
        registerGauge(
                diagnostics,
                "map_snapshot_owned_internal_keys_reused",
                metrics.ownedInternalKeysReused);
        registerGauge(
                diagnostics,
                "map_snapshot_internal_key_copies_avoided",
                metrics.internalKeyCopiesAvoided);
        registerGauge(
                diagnostics,
                "map_snapshot_exposed_key_copies_deferred",
                metrics.exposedKeyCopiesDeferred);
        registerGauge(
                diagnostics,
                "map_snapshot_exposed_key_copies_materialized",
                metrics.exposedKeyCopiesMaterialized);
        registerGauge(diagnostics, "native_snapshot_cost_operations", metrics.nativeCostOperations);
        registerGauge(
                diagnostics,
                "native_snapshot_cost_lookup_operations",
                metrics.nativeLookupOperations);
        registerGauge(
                diagnostics, "native_snapshot_cost_put_operations", metrics.nativePutOperations);
        registerGauge(
                diagnostics,
                "native_snapshot_cost_remove_operations",
                metrics.nativeRemoveOperations);
        registerGauge(
                diagnostics,
                "native_snapshot_cost_raw_key_samples",
                metrics.nativeCostRawKeySamples);
        registerGauge(
                diagnostics,
                "native_snapshot_cost_serialized_key_samples",
                metrics.nativeCostSerializedKeySamples);
        registerGauge(
                diagnostics, "native_snapshot_cost_input_bytes", metrics.nativeCostInputBytes);
        registerGauge(
                diagnostics,
                "native_snapshot_cost_key_encode_ns",
                metrics.nativeCostKeyEncodeNs);
        registerGauge(diagnostics, "native_snapshot_remove_requests", metrics.nativeRemoveRequests);
        registerGauge(
                diagnostics,
                "native_snapshot_remove_hint_skips",
                metrics.nativeRemoveHintSkips);
        registerGauge(
                diagnostics,
                "native_snapshot_remove_jni_calls",
                metrics.nativeRemoveJniCalls);
        registerGauge(diagnostics, "native_snapshot_remove_hits", metrics.nativeRemoveHits);
        registerGauge(
                diagnostics,
                "native_snapshot_remove_hint_false_positives",
                metrics.nativeRemoveHintFalsePositives);
        registerNativeCostGauges(diagnostics, "lookup", metrics.nativeLookupCost);
        registerNativeCostGauges(diagnostics, "put", metrics.nativePutCost);
        registerNativeCostGauges(diagnostics, "remove", metrics.nativeRemoveCost);
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

    void recordSmallShortCircuit() {
        increment(smallShortCircuits);
    }

    void recordStoreEmpty() {
        increment(storesEmpty);
    }

    void recordStoreSingle() {
        increment(storesSingle);
    }

    void recordStoreSmall() {
        increment(storesSmall);
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

    void recordOwnedKeyReuseActiveState() {
        increment(ownedKeyReuseActiveStates);
    }

    void recordOwnedInternalKeyReused() {
        increment(ownedInternalKeysReused);
    }

    void recordInternalKeyCopyAvoided() {
        increment(internalKeyCopiesAvoided);
    }

    void recordExposedKeyCopyDeferred() {
        increment(exposedKeyCopiesDeferred);
    }

    void recordExposedKeyCopyMaterialized() {
        increment(exposedKeyCopiesMaterialized);
    }

    boolean shouldSampleNativeLookupCost() {
        return shouldSampleNativeCost(nativeLookupOperations);
    }

    boolean shouldSampleNativePutCost() {
        return shouldSampleNativeCost(nativePutOperations);
    }

    boolean shouldSampleNativeRemoveCost() {
        return shouldSampleNativeCost(nativeRemoveOperations);
    }

    private boolean shouldSampleNativeCost(AtomicLong operationCounter) {
        if (!enabled) {
            return false;
        }
        nativeCostOperations.incrementAndGet();
        return (operationCounter.incrementAndGet() & 1023L) == 0;
    }

    void recordNativeKeySample(boolean rawKey, int inputBytes, long keyEncodeNs) {
        increment(rawKey ? nativeCostRawKeySamples : nativeCostSerializedKeySamples);
        nativeCostInputBytes.addAndGet(inputBytes);
        nativeCostKeyEncodeNs.addAndGet(keyEncodeNs);
    }

    void recordNativeLookupCost(long jniNs, long nativeCoreNs, long materializeNs) {
        nativeLookupCost.record(jniNs, nativeCoreNs, materializeNs);
    }

    void recordNativePutCost(long jniNs, long nativeCoreNs) {
        nativePutCost.record(jniNs, nativeCoreNs, 0);
    }

    void recordNativeRemoveCost(long jniNs, long nativeCoreNs) {
        nativeRemoveCost.record(jniNs, nativeCoreNs, 0);
    }

    void recordNativeRemoveRequest() {
        increment(nativeRemoveRequests);
    }

    void recordNativeRemoveHintSkip() {
        increment(nativeRemoveHintSkips);
    }

    void recordNativeRemoveJniCall() {
        increment(nativeRemoveJniCalls);
    }

    void recordNativeRemoveHit() {
        increment(nativeRemoveHits);
    }

    void recordNativeRemoveHintFalsePositive() {
        increment(nativeRemoveHintFalsePositives);
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

    long smallShortCircuits() {
        return smallShortCircuits.get();
    }

    long storesEmpty() {
        return storesEmpty.get();
    }

    long storesSingle() {
        return storesSingle.get();
    }

    long storesSmall() {
        return storesSmall.get();
    }

    long invalidations() {
        return invalidations.get();
    }

    long ownedKeyReuseActiveStates() {
        return ownedKeyReuseActiveStates.get();
    }

    long ownedInternalKeysReused() {
        return ownedInternalKeysReused.get();
    }

    long internalKeyCopiesAvoided() {
        return internalKeyCopiesAvoided.get();
    }

    long exposedKeyCopiesDeferred() {
        return exposedKeyCopiesDeferred.get();
    }

    long exposedKeyCopiesMaterialized() {
        return exposedKeyCopiesMaterialized.get();
    }

    long nativeCostRawKeySamples() {
        return nativeCostRawKeySamples.get();
    }

    long nativeCostSerializedKeySamples() {
        return nativeCostSerializedKeySamples.get();
    }

    long nativeLookupCostSamples() {
        return nativeLookupCost.samples.get();
    }

    long nativePutCostSamples() {
        return nativePutCost.samples.get();
    }

    long nativeRemoveCostSamples() {
        return nativeRemoveCost.samples.get();
    }

    long nativeRemoveRequests() {
        return nativeRemoveRequests.get();
    }

    long nativeRemoveHintSkips() {
        return nativeRemoveHintSkips.get();
    }

    long nativeRemoveJniCalls() {
        return nativeRemoveJniCalls.get();
    }

    long nativeRemoveHits() {
        return nativeRemoveHits.get();
    }

    long nativeRemoveHintFalsePositives() {
        return nativeRemoveHintFalsePositives.get();
    }

    /** Returns one stable, machine-readable snapshot for end-of-task audit logs. */
    public String diagnosticSummary() {
        return String.format(
                "enabled=%s probes=%d hits=%d misses=%d emptyShortCircuits=%d "
                        + "singleShortCircuits=%d smallShortCircuits=%d storesEmpty=%d "
                        + "storesSingle=%d storesSmall=%d multiEntrySkips=%d invalidations=%d "
                        + "staleInvalidations=%d evictions=%d ownedKeyReuseActiveStates=%d "
                        + "ownedInternalKeysReused=%d internalKeyCopiesAvoided=%d "
                        + "exposedKeyCopiesDeferred=%d exposedKeyCopiesMaterialized=%d",
                enabled,
                probes.get(),
                hits.get(),
                misses.get(),
                emptyShortCircuits.get(),
                singleShortCircuits.get(),
                smallShortCircuits.get(),
                storesEmpty.get(),
                storesSingle.get(),
                storesSmall.get(),
                multiEntrySkips.get(),
                invalidations.get(),
                staleInvalidations.get(),
                evictions.get(),
                ownedKeyReuseActiveStates.get(),
                ownedInternalKeysReused.get(),
                internalKeyCopiesAvoided.get(),
                exposedKeyCopiesDeferred.get(),
                exposedKeyCopiesMaterialized.get());
    }

    private void increment(AtomicLong counter) {
        if (enabled) {
            counter.incrementAndGet();
        }
    }

    private static void registerGauge(MetricGroup metricGroup, String name, AtomicLong value) {
        metricGroup.gauge(name, (Gauge<Long>) value::get);
    }

    private static void registerNativeCostGauges(
            MetricGroup metricGroup, String operation, NativeOperationCost cost) {
        String prefix = "native_snapshot_cost_" + operation + "_";
        registerGauge(metricGroup, prefix + "samples", cost.samples);
        registerGauge(metricGroup, prefix + "jni_ns", cost.jniNs);
        registerGauge(metricGroup, prefix + "native_core_ns", cost.nativeCoreNs);
        registerGauge(metricGroup, prefix + "jni_transport_ns", cost.jniTransportNs);
        registerGauge(metricGroup, prefix + "materialize_ns", cost.materializeNs);
    }

    private static final class NativeOperationCost {
        private final AtomicLong samples = new AtomicLong();
        private final AtomicLong jniNs = new AtomicLong();
        private final AtomicLong nativeCoreNs = new AtomicLong();
        private final AtomicLong jniTransportNs = new AtomicLong();
        private final AtomicLong materializeNs = new AtomicLong();

        private void record(
                long jniDurationNs, long nativeCoreDurationNs, long materializeDurationNs) {
            samples.incrementAndGet();
            jniNs.addAndGet(jniDurationNs);
            nativeCoreNs.addAndGet(nativeCoreDurationNs);
            jniTransportNs.addAndGet(Math.max(0, jniDurationNs - nativeCoreDurationNs));
            materializeNs.addAndGet(materializeDurationNs);
        }
    }
}
