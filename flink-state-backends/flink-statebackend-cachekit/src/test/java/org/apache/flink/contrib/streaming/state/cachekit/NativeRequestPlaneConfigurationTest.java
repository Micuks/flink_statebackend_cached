/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneBridge;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneOptions;
import org.junit.jupiter.api.Test;

class NativeRequestPlaneConfigurationTest {

    @Test
    void testKeyScopedInvalidationIsCarriedByConfiguredBackendInstance() throws Exception {
        Configuration disabled = new Configuration();
        disabled.set(
                CacheKitStateBackendFactory.DELEGATE_BACKEND,
                "org.apache.flink.runtime.state.hashmap.HashMapStateBackend");
        CacheKitStateBackend disabledBackend =
                new CacheKitStateBackendFactory()
                        .createFromConfig(disabled, getClass().getClassLoader());
        assertFalse(disabledBackend.keyScopedPrefetchInvalidationEnabledForTesting());

        Configuration enabled = new Configuration();
        enabled.set(
                CacheKitStateBackendFactory.DELEGATE_BACKEND,
                "org.apache.flink.runtime.state.hashmap.HashMapStateBackend");
        enabled.set(CacheKitStateBackendFactory.BP_PREFETCH_KEY_SCOPED_INVALIDATION_ENABLED, true);
        CacheKitStateBackend enabledBackend =
                new CacheKitStateBackendFactory()
                        .createFromConfig(enabled, getClass().getClassLoader());
        assertTrue(enabledBackend.keyScopedPrefetchInvalidationEnabledForTesting());
    }

    @Test
    void testAccessGuidedPrefetchIsExplicitAndCarriedByConfiguredBackendInstance()
            throws Exception {
        Configuration disabled = new Configuration();
        disabled.set(
                CacheKitStateBackendFactory.DELEGATE_BACKEND,
                "org.apache.flink.runtime.state.hashmap.HashMapStateBackend");
        CacheKitStateBackend disabledBackend =
                new CacheKitStateBackendFactory()
                        .createFromConfig(disabled, getClass().getClassLoader());
        assertFalse(disabledBackend.nativePrefetchAccessGuidedStateEnabledForTesting());

        Configuration enabled = new Configuration(disabled);
        enabled.set(CacheKitStateBackendFactory.NATIVE_PREFETCH_ACCESS_GUIDED_STATE_ENABLED, true);
        CacheKitStateBackend enabledBackend =
                new CacheKitStateBackendFactory()
                        .createFromConfig(enabled, getClass().getClassLoader());
        assertTrue(enabledBackend.nativePrefetchAccessGuidedStateEnabledForTesting());
    }

    @Test
    void testMapDistinctBatchPrefetchIsExplicitAndCarriedByConfiguredBackendInstance()
            throws Exception {
        Configuration disabled = new Configuration();
        disabled.set(
                CacheKitStateBackendFactory.DELEGATE_BACKEND,
                "org.apache.flink.runtime.state.hashmap.HashMapStateBackend");
        CacheKitStateBackend disabledBackend =
                new CacheKitStateBackendFactory()
                        .createFromConfig(disabled, getClass().getClassLoader());
        assertFalse(disabledBackend.nativeMapDistinctBatchPrefetchEnabledForTesting());
        assertFalse(
                disabledBackend.nativeMapDistinctBatchPrefetchDirectArenaEnabledForTesting());
        assertEquals(8, disabledBackend.nativeMapDistinctBatchPrefetchAsyncMinUniqueKeysForTesting());
        assertEquals(
                8,
                CacheKitStateBackend.normalizeNativeMapDistinctBatchPrefetchAsyncMinUniqueKeys(0));
        assertFalse(
                disabled.get(
                        CacheKitStateBackendFactory
                                .NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_ENABLED));

        Configuration enabled = new Configuration(disabled);
        enabled.set(CacheKitStateBackendFactory.NATIVE_MAP_DISTINCT_BATCH_PREFETCH_ENABLED, true);
        enabled.set(
                CacheKitStateBackendFactory
                        .NATIVE_MAP_DISTINCT_BATCH_PREFETCH_DIRECT_ARENA_ENABLED,
                true);
        enabled.set(
                CacheKitStateBackendFactory
                        .NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_ENABLED,
                true);
        enabled.set(
                CacheKitStateBackendFactory
                        .NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_ASYNC_MIN_UNIQUE_KEYS,
                16);
        CacheKitStateBackend enabledBackend =
                new CacheKitStateBackendFactory()
                        .createFromConfig(enabled, getClass().getClassLoader());
        assertTrue(enabledBackend.nativeMapDistinctBatchPrefetchEnabledForTesting());
        assertTrue(enabledBackend.nativeMapDistinctBatchPrefetchDirectArenaEnabledForTesting());
        assertEquals(
                16,
                enabledBackend.nativeMapDistinctBatchPrefetchAsyncMinUniqueKeysForTesting());
        assertTrue(
                enabled.get(
                        CacheKitStateBackendFactory
                                .NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_ENABLED));
    }

    @Test
    void testNativeRequestPlaneIsOffByDefault() {
        NativeRequestPlaneOptions options =
                CacheKitStateBackendFactory.nativeRequestPlaneOptions(new Configuration());

        assertFalse(options.enabled());
        assertFalse(options.indexedFoldEnabled());
        assertEquals("auto", options.kernel());
        assertEquals(NativeRequestPlaneBridge.KERNEL_AUTO, options.kernelPreference());
        assertEquals(64, options.minBatchSize());
        assertTrue(options.aarch64Only());
        assertFalse(options.writeThroughMutations());
        assertFalse(options.readActivatedWriteThrough());
        assertFalse(options.valueCacheEnabled());
        assertFalse(options.mapCacheEnabled());
        assertFalse(options.mapSnapshotEnabled());
        assertFalse(options.mapSnapshotAdaptiveBypassEnabled());
        assertEquals(8192, options.mapSnapshotAdaptiveWindowProbes());
        assertEquals(0.02, options.mapSnapshotAdaptiveMinUsefulHitRate());
        assertEquals(262144, options.mapSnapshotAdaptiveResampleIntervalProbes());
        assertFalse(options.mailboxBatchEnabled());
        assertFalse(options.prefetchEnabled());
        assertFalse(options.preaggEnabled());
        assertFalse(options.compactSelectedProbeEnabled());
        assertFalse(options.directArenaMultiGetEnabled());
        assertEquals(64, options.directArenaBatchSize());
        assertFalse(options.directArenaReadOnlyEnabled());
        assertFalse(options.directArenaEagerMaterializationEnabled());
        assertFalse(options.negativeHandoffEnabled());
        assertFalse(options.compactionScratchSlotEnabled());
        assertEquals(4096, options.compactionScratchEntries());
        assertEquals(2 << 20, options.compactionScratchKeyArenaBytes());
        assertFalse(options.requiresValueCache());
    }

    @Test
    void testFlinkConfigurationCarriesExplicitSve256AndCapacities() {
        Configuration config = new Configuration();
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_ENABLED, true);
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_LIBRARY, "/tmp/native.so");
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_KERNEL, "sve256");
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_CAPACITY_ENTRIES, 333);
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_BATCH_ENTRIES, 17);
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_MIN_BATCH_SIZE, 5);
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_BATCH_SLOTS, 3);
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_AARCH64_ONLY, false);
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_WRITE_THROUGH_MUTATIONS, true);
        config.set(
                CacheKitStateBackendFactory.NATIVE_VALUE_CACHE_READ_ACTIVATED_WRITE_THROUGH, true);
        config.set(CacheKitStateBackendFactory.NATIVE_VALUE_CACHE_ENABLED, true);
        config.set(CacheKitStateBackendFactory.NATIVE_MAP_CACHE_ENABLED, true);
        config.set(CacheKitStateBackendFactory.NATIVE_MAP_SNAPSHOT_ENABLED, true);
        config.set(CacheKitStateBackendFactory.NATIVE_MAP_SNAPSHOT_ADAPTIVE_BYPASS_ENABLED, true);
        config.set(CacheKitStateBackendFactory.NATIVE_MAP_SNAPSHOT_ADAPTIVE_WINDOW_PROBES, 1234);
        config.set(
                CacheKitStateBackendFactory.NATIVE_MAP_SNAPSHOT_ADAPTIVE_MIN_USEFUL_HIT_RATE,
                0.125);
        config.set(
                CacheKitStateBackendFactory.NATIVE_MAP_SNAPSHOT_ADAPTIVE_RESAMPLE_INTERVAL_PROBES,
                5678);
        config.set(CacheKitStateBackendFactory.NATIVE_PREFETCH_ENABLED, true);
        config.set(CacheKitStateBackendFactory.NATIVE_MAILBOX_BATCH_ENABLED, true);
        config.set(CacheKitStateBackendFactory.NATIVE_LOCAL_PREAGG_ENABLED, true);
        config.set(CacheKitStateBackendFactory.NATIVE_COMPACT_SELECTED_PROBE_ENABLED, true);
        config.set(CacheKitStateBackendFactory.NATIVE_DIRECT_ARENA_MULTIGET_ENABLED, true);
        config.set(CacheKitStateBackendFactory.NATIVE_DIRECT_ARENA_BATCH_SIZE, 128);
        config.set(CacheKitStateBackendFactory.NATIVE_DIRECT_ARENA_READ_ONLY_ENABLED, true);
        config.set(
                CacheKitStateBackendFactory.NATIVE_DIRECT_ARENA_EAGER_MATERIALIZATION_ENABLED,
                true);
        config.set(CacheKitStateBackendFactory.NATIVE_PREFETCH_NEGATIVE_HANDOFF_ENABLED, true);
        config.set(
                CacheKitStateBackendFactory.NATIVE_MAILBOX_COMPACTION_SCRATCH_SLOT_ENABLED, true);
        config.set(CacheKitStateBackendFactory.NATIVE_MAILBOX_COMPACTION_SCRATCH_ENTRIES, 777);
        config.set(
                CacheKitStateBackendFactory.NATIVE_MAILBOX_COMPACTION_SCRATCH_KEY_ARENA_BYTES,
                123456);

        NativeRequestPlaneOptions options =
                CacheKitStateBackendFactory.nativeRequestPlaneOptions(config);

        assertTrue(options.enabled());
        assertEquals("/tmp/native.so", options.libraryPath());
        assertEquals("sve256", options.kernel());
        assertEquals(NativeRequestPlaneBridge.KERNEL_SVE256, options.kernelPreference());
        assertEquals(333, options.capacityEntries());
        assertEquals(17, options.batchEntries());
        assertEquals(5, options.minBatchSize());
        assertEquals(3, options.batchSlots());
        assertFalse(options.aarch64Only());
        assertTrue(options.writeThroughMutations());
        assertTrue(options.readActivatedWriteThrough());
        assertTrue(options.valueCacheEnabled());
        assertTrue(options.mapCacheEnabled());
        assertTrue(options.mapSnapshotEnabled());
        assertTrue(options.mapSnapshotAdaptiveBypassEnabled());
        assertEquals(1234, options.mapSnapshotAdaptiveWindowProbes());
        assertEquals(0.125, options.mapSnapshotAdaptiveMinUsefulHitRate());
        assertEquals(5678, options.mapSnapshotAdaptiveResampleIntervalProbes());
        assertTrue(options.mailboxBatchEnabled());
        assertTrue(options.prefetchEnabled());
        assertTrue(options.preaggEnabled());
        assertTrue(options.compactSelectedProbeEnabled());
        assertTrue(options.directArenaMultiGetEnabled());
        assertEquals(128, options.directArenaBatchSize());
        assertTrue(options.directArenaReadOnlyEnabled());
        assertTrue(options.directArenaEagerMaterializationEnabled());
        assertTrue(options.negativeHandoffEnabled());
        assertTrue(options.compactionScratchSlotEnabled());
        assertEquals(777, options.compactionScratchEntries());
        assertEquals(123456, options.compactionScratchKeyArenaBytes());
        assertTrue(options.requiresValueCache());
    }

    @Test
    void testSVE2AndRelativeLibraryAreRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new NativeRequestPlaneOptions(true, "", "sve2", 1, 1, 1, 1, 1, 1, 1, 1));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new NativeRequestPlaneOptions(
                                true, "relative.so", "auto", 1, 1, 1, 1, 1, 1, 1, 1));
    }

    @Test
    void testDirectArenaRequiresCompactSelectedProbe() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new NativeRequestPlaneOptions(
                                true, "", "auto", 128, 4096, 4096, 16, 4096, 4096, 1, 2, false,
                                false, false, false, false, true, true, false, false, true, false,
                                8192, 0.02, 262144));
    }

    @Test
    void testDirectArenaRequiresAtLeastOneBytePerMaximumBatchEntry() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new NativeRequestPlaneOptions(
                                true, "", "auto", 128, 4096, 4096, 16, 4096, 63, 1, 2, false, false,
                                false, false, false, true, true, false, true, true, false, 8192,
                                0.02, 262144));
    }

    @Test
    void testDirectArenaEagerMaterializationRequiresDirectReadOnly() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new NativeRequestPlaneOptions(
                                true, "", "auto", 128, 4096, 4096, 16, 4096, 4096, 1, 2, false,
                                false, false, false, false, true, true, false, true, true, false,
                                8192, 0.02, 262144, false, false, false, false, false, false, false,
                                16, 4096, true));
    }

    @Test
    void testNegativeHandoffRequiresDirectReadOnlyPrefetch() {
        Configuration config = new Configuration();
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_ENABLED, true);
        config.set(CacheKitStateBackendFactory.NATIVE_PREFETCH_NEGATIVE_HANDOFF_ENABLED, true);

        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> CacheKitStateBackendFactory.nativeRequestPlaneOptions(config));
        assertTrue(failure.getMessage().contains("direct-read-only"));
    }

    @Test
    void testMissingFieldFromOlderSerializedBackendNormalizesToDisabled() {
        NativeRequestPlaneOptions options =
                CacheKitStateBackend.normalizeNativeRequestPlaneOptions(null);

        assertFalse(options.enabled());
        assertEquals("auto", options.kernel());
    }

    @Test
    void testNativeMapCacheFailsClosedWithoutNativeRuntime() {
        Configuration config = new Configuration();
        config.set(CacheKitStateBackendFactory.NATIVE_MAP_CACHE_ENABLED, true);

        assertThrows(
                IllegalArgumentException.class,
                () -> CacheKitStateBackendFactory.nativeRequestPlaneOptions(config));
    }

    @Test
    void testNativeValueCacheFailsClosedWithoutNativeRuntime() {
        Configuration config = new Configuration();
        config.set(CacheKitStateBackendFactory.NATIVE_VALUE_CACHE_ENABLED, true);

        assertThrows(
                IllegalArgumentException.class,
                () -> CacheKitStateBackendFactory.nativeRequestPlaneOptions(config));
    }

    @Test
    void testNativeMapSnapshotFailsClosedWithoutNativeRuntime() {
        Configuration config = new Configuration();
        config.set(CacheKitStateBackendFactory.NATIVE_MAP_SNAPSHOT_ENABLED, true);

        assertThrows(
                IllegalArgumentException.class,
                () -> CacheKitStateBackendFactory.nativeRequestPlaneOptions(config));
    }

    @Test
    void testNativeMapSnapshotAdaptiveBypassRequiresSnapshotAndValidBounds() {
        Configuration missingSnapshot = new Configuration();
        missingSnapshot.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_ENABLED, true);
        missingSnapshot.set(
                CacheKitStateBackendFactory.NATIVE_MAP_SNAPSHOT_ADAPTIVE_BYPASS_ENABLED, true);
        assertThrows(
                IllegalArgumentException.class,
                () -> CacheKitStateBackendFactory.nativeRequestPlaneOptions(missingSnapshot));

        Configuration invalidWindow = new Configuration();
        invalidWindow.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_ENABLED, true);
        invalidWindow.set(CacheKitStateBackendFactory.NATIVE_MAP_SNAPSHOT_ENABLED, true);
        invalidWindow.set(
                CacheKitStateBackendFactory.NATIVE_MAP_SNAPSHOT_ADAPTIVE_WINDOW_PROBES, 1);
        assertThrows(
                IllegalArgumentException.class,
                () -> CacheKitStateBackendFactory.nativeRequestPlaneOptions(invalidWindow));

        Configuration invalidRate = new Configuration();
        invalidRate.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_ENABLED, true);
        invalidRate.set(CacheKitStateBackendFactory.NATIVE_MAP_SNAPSHOT_ENABLED, true);
        invalidRate.set(
                CacheKitStateBackendFactory.NATIVE_MAP_SNAPSHOT_ADAPTIVE_MIN_USEFUL_HIT_RATE, 1.01);
        assertThrows(
                IllegalArgumentException.class,
                () -> CacheKitStateBackendFactory.nativeRequestPlaneOptions(invalidRate));

        Configuration invalidNaNRate = new Configuration();
        invalidNaNRate.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_ENABLED, true);
        invalidNaNRate.set(CacheKitStateBackendFactory.NATIVE_MAP_SNAPSHOT_ENABLED, true);
        invalidNaNRate.set(
                CacheKitStateBackendFactory.NATIVE_MAP_SNAPSHOT_ADAPTIVE_MIN_USEFUL_HIT_RATE,
                Double.NaN);
        assertThrows(
                IllegalArgumentException.class,
                () -> CacheKitStateBackendFactory.nativeRequestPlaneOptions(invalidNaNRate));

        Configuration invalidResample = new Configuration();
        invalidResample.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_ENABLED, true);
        invalidResample.set(CacheKitStateBackendFactory.NATIVE_MAP_SNAPSHOT_ENABLED, true);
        invalidResample.set(
                CacheKitStateBackendFactory.NATIVE_MAP_SNAPSHOT_ADAPTIVE_RESAMPLE_INTERVAL_PROBES,
                0);
        assertThrows(
                IllegalArgumentException.class,
                () -> CacheKitStateBackendFactory.nativeRequestPlaneOptions(invalidResample));
    }

    @Test
    void testOnlyValueCacheAndPrefetchRequireJavaValueCache() {
        Configuration mailbox = new Configuration();
        mailbox.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_ENABLED, true);
        mailbox.set(CacheKitStateBackendFactory.NATIVE_MAILBOX_BATCH_ENABLED, true);
        assertFalse(
                CacheKitStateBackendFactory.nativeRequestPlaneOptions(mailbox)
                        .requiresValueCache());

        Configuration preagg = new Configuration();
        preagg.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_ENABLED, true);
        preagg.set(CacheKitStateBackendFactory.NATIVE_LOCAL_PREAGG_ENABLED, true);
        assertFalse(
                CacheKitStateBackendFactory.nativeRequestPlaneOptions(preagg).requiresValueCache());

        Configuration prefetch = new Configuration();
        prefetch.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_ENABLED, true);
        prefetch.set(CacheKitStateBackendFactory.NATIVE_PREFETCH_ENABLED, true);
        assertTrue(
                CacheKitStateBackendFactory.nativeRequestPlaneOptions(prefetch)
                        .requiresValueCache());
    }

    @Test
    void testIndexedFoldRequiresNativeLocalPreagg() {
        Configuration invalid = new Configuration();
        invalid.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_ENABLED, true);
        invalid.set(CacheKitStateBackendFactory.NATIVE_LOCAL_PREAGG_INDEXED_FOLD_ENABLED, true);
        assertThrows(
                IllegalArgumentException.class,
                () -> CacheKitStateBackendFactory.nativeRequestPlaneOptions(invalid));

        invalid.set(CacheKitStateBackendFactory.NATIVE_LOCAL_PREAGG_ENABLED, true);
        NativeRequestPlaneOptions options =
                CacheKitStateBackendFactory.nativeRequestPlaneOptions(invalid);
        assertTrue(options.preaggEnabled());
        assertTrue(options.indexedFoldEnabled());
    }

    @Test
    void testReadActivatedWriteThroughRequiresMutationWriteThrough() {
        Configuration invalid = new Configuration();
        invalid.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_ENABLED, true);
        invalid.set(CacheKitStateBackendFactory.NATIVE_VALUE_CACHE_ENABLED, true);
        invalid.set(
                CacheKitStateBackendFactory.NATIVE_VALUE_CACHE_READ_ACTIVATED_WRITE_THROUGH, true);
        assertThrows(
                IllegalArgumentException.class,
                () -> CacheKitStateBackendFactory.nativeRequestPlaneOptions(invalid));

        invalid.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_WRITE_THROUGH_MUTATIONS, true);
        invalid.set(CacheKitStateBackendFactory.NATIVE_VALUE_CACHE_ENABLED, false);
        assertThrows(
                IllegalArgumentException.class,
                () -> CacheKitStateBackendFactory.nativeRequestPlaneOptions(invalid));
    }

    @Test
    void testCompactSelectedProbeRequiresMailboxAndPrefetch() {
        Configuration missingMailbox = new Configuration();
        missingMailbox.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_ENABLED, true);
        missingMailbox.set(CacheKitStateBackendFactory.NATIVE_PREFETCH_ENABLED, true);
        missingMailbox.set(CacheKitStateBackendFactory.NATIVE_COMPACT_SELECTED_PROBE_ENABLED, true);
        assertThrows(
                IllegalArgumentException.class,
                () -> CacheKitStateBackendFactory.nativeRequestPlaneOptions(missingMailbox));

        Configuration missingPrefetch = new Configuration();
        missingPrefetch.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_ENABLED, true);
        missingPrefetch.set(CacheKitStateBackendFactory.NATIVE_MAILBOX_BATCH_ENABLED, true);
        missingPrefetch.set(
                CacheKitStateBackendFactory.NATIVE_COMPACT_SELECTED_PROBE_ENABLED, true);
        assertThrows(
                IllegalArgumentException.class,
                () -> CacheKitStateBackendFactory.nativeRequestPlaneOptions(missingPrefetch));
    }

    @Test
    void testDeferredReservationMaterializationRequiresCompactSelectedPrefetch() {
        Configuration invalid = new Configuration();
        invalid.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_ENABLED, true);
        invalid.set(
                CacheKitStateBackendFactory
                        .NATIVE_PREFETCH_DEFERRED_RESERVATION_MATERIALIZATION_ENABLED,
                true);
        assertThrows(
                IllegalArgumentException.class,
                () -> CacheKitStateBackendFactory.nativeRequestPlaneOptions(invalid));

        invalid.set(CacheKitStateBackendFactory.NATIVE_PREFETCH_ENABLED, true);
        invalid.set(CacheKitStateBackendFactory.NATIVE_MAILBOX_BATCH_ENABLED, true);
        invalid.set(CacheKitStateBackendFactory.NATIVE_COMPACT_SELECTED_PROBE_ENABLED, true);
        NativeRequestPlaneOptions options =
                CacheKitStateBackendFactory.nativeRequestPlaneOptions(invalid);
        assertTrue(options.deferredReservationMaterializationEnabled());
    }

    @Test
    void testCompactionScratchRequiresMailboxPrefetch() {
        Configuration invalid = new Configuration();
        invalid.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_ENABLED, true);
        invalid.set(
                CacheKitStateBackendFactory.NATIVE_MAILBOX_COMPACTION_SCRATCH_SLOT_ENABLED, true);
        assertThrows(
                IllegalArgumentException.class,
                () -> CacheKitStateBackendFactory.nativeRequestPlaneOptions(invalid));

        invalid.set(CacheKitStateBackendFactory.NATIVE_PREFETCH_ENABLED, true);
        invalid.set(CacheKitStateBackendFactory.NATIVE_MAILBOX_BATCH_ENABLED, true);
        assertTrue(
                CacheKitStateBackendFactory.nativeRequestPlaneOptions(invalid)
                        .compactionScratchSlotEnabled());
    }
}
