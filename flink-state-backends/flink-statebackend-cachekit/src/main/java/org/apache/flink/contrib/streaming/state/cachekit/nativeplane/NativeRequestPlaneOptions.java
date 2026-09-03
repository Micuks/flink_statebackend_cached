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

package org.apache.flink.contrib.streaming.state.cachekit.nativeplane;

import org.apache.flink.annotation.Internal;
import org.apache.flink.contrib.streaming.state.RocksDBBatchValueReader;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Objects;

/** Serializable, explicitly opt-in configuration for one keyed-backend native request plane. */
@Internal
public final class NativeRequestPlaneOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final NativeRequestPlaneOptions DISABLED =
            new NativeRequestPlaneOptions(
                    false, "", "auto", 16_384, 4L << 20, 16L << 20, 1024, 256 << 10, 4 << 20, 64,
                    2);

    private final boolean enabled;
    private final String libraryPath;
    private final String kernel;
    private final int capacityEntries;
    private final long keyArenaBytes;
    private final long valueArenaBytes;
    private final int batchEntries;
    private final int batchKeyArenaBytes;
    private final int batchValueArenaBytes;
    private final int minBatchSize;
    private final int batchSlots;
    private final boolean aarch64Only;
    private final boolean writeThroughMutations;
    private final boolean readActivatedWriteThrough;
    private final boolean residentMutationBatchEnabled;
    private final boolean valueCacheEnabled;
    private final boolean valuePointAdaptiveBypassEnabled;
    private final int valuePointAdaptiveWindowProbes;
    private final int valuePointAdaptiveZeroWindows;
    private final int valuePointAdaptiveResampleIntervalProbes;
    private final int valuePointAdaptiveSampleSlots;
    private final boolean mapCacheEnabled;
    private final boolean mapSnapshotEnabled;
    private final boolean prefetchEnabled;
    private final boolean mailboxBatchEnabled;
    private final boolean preaggEnabled;
    private final boolean indexedFoldEnabled;
    private final boolean compactSelectedProbeEnabled;
    private final boolean directArenaMultiGetEnabled;
    private final int directArenaBatchSize;
    private final boolean directArenaReadOnlyEnabled;
    private final boolean directArenaEagerMaterializationEnabled;
    private final boolean negativeHandoffEnabled;
    private final boolean deferredReservationMaterializationEnabled;
    private final boolean compactionScratchSlotEnabled;
    private final int compactionScratchEntries;
    private final int compactionScratchKeyArenaBytes;
    private final boolean mapSnapshotAdaptiveBypassEnabled;
    private final int mapSnapshotAdaptiveWindowProbes;
    private final double mapSnapshotAdaptiveMinUsefulHitRate;
    private final int mapSnapshotAdaptiveResampleIntervalProbes;

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                true,
                false,
                false);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                false,
                false);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                false);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean mapCacheEnabled) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                mapCacheEnabled,
                false);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean mapCacheEnabled,
            boolean mapSnapshotEnabled) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                mapCacheEnabled,
                mapSnapshotEnabled,
                enabled,
                false,
                false);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean mapCacheEnabled,
            boolean mapSnapshotEnabled,
            boolean mailboxBatchEnabled) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                mapCacheEnabled,
                mapSnapshotEnabled,
                enabled,
                mailboxBatchEnabled,
                false);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean mapCacheEnabled,
            boolean mapSnapshotEnabled,
            boolean prefetchEnabled,
            boolean mailboxBatchEnabled) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                mapCacheEnabled,
                mapSnapshotEnabled,
                prefetchEnabled,
                mailboxBatchEnabled,
                false);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean mapCacheEnabled,
            boolean mapSnapshotEnabled,
            boolean prefetchEnabled,
            boolean mailboxBatchEnabled,
            boolean preaggEnabled) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                enabled,
                mapCacheEnabled,
                mapSnapshotEnabled,
                prefetchEnabled,
                mailboxBatchEnabled,
                preaggEnabled,
                false);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean valueCacheEnabled,
            boolean mapCacheEnabled,
            boolean mapSnapshotEnabled,
            boolean prefetchEnabled,
            boolean mailboxBatchEnabled,
            boolean preaggEnabled) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                valueCacheEnabled,
                mapCacheEnabled,
                mapSnapshotEnabled,
                prefetchEnabled,
                mailboxBatchEnabled,
                preaggEnabled,
                false);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean valueCacheEnabled,
            boolean mapCacheEnabled,
            boolean mapSnapshotEnabled,
            boolean prefetchEnabled,
            boolean mailboxBatchEnabled,
            boolean preaggEnabled,
            boolean compactSelectedProbeEnabled) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                valueCacheEnabled,
                mapCacheEnabled,
                mapSnapshotEnabled,
                prefetchEnabled,
                mailboxBatchEnabled,
                preaggEnabled,
                compactSelectedProbeEnabled,
                false,
                8192,
                0.02,
                262144);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean valueCacheEnabled,
            boolean mapCacheEnabled,
            boolean mapSnapshotEnabled,
            boolean prefetchEnabled,
            boolean mailboxBatchEnabled,
            boolean preaggEnabled,
            boolean compactSelectedProbeEnabled,
            boolean mapSnapshotAdaptiveBypassEnabled,
            int mapSnapshotAdaptiveWindowProbes,
            double mapSnapshotAdaptiveMinUsefulHitRate,
            int mapSnapshotAdaptiveResampleIntervalProbes) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                valueCacheEnabled,
                mapCacheEnabled,
                mapSnapshotEnabled,
                prefetchEnabled,
                mailboxBatchEnabled,
                preaggEnabled,
                compactSelectedProbeEnabled,
                false,
                mapSnapshotAdaptiveBypassEnabled,
                mapSnapshotAdaptiveWindowProbes,
                mapSnapshotAdaptiveMinUsefulHitRate,
                mapSnapshotAdaptiveResampleIntervalProbes);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean valueCacheEnabled,
            boolean mapCacheEnabled,
            boolean mapSnapshotEnabled,
            boolean prefetchEnabled,
            boolean mailboxBatchEnabled,
            boolean preaggEnabled,
            boolean compactSelectedProbeEnabled,
            boolean directArenaMultiGetEnabled,
            boolean mapSnapshotAdaptiveBypassEnabled,
            int mapSnapshotAdaptiveWindowProbes,
            double mapSnapshotAdaptiveMinUsefulHitRate,
            int mapSnapshotAdaptiveResampleIntervalProbes) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                valueCacheEnabled,
                mapCacheEnabled,
                mapSnapshotEnabled,
                prefetchEnabled,
                mailboxBatchEnabled,
                preaggEnabled,
                compactSelectedProbeEnabled,
                directArenaMultiGetEnabled,
                mapSnapshotAdaptiveBypassEnabled,
                mapSnapshotAdaptiveWindowProbes,
                mapSnapshotAdaptiveMinUsefulHitRate,
                mapSnapshotAdaptiveResampleIntervalProbes,
                false);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean valueCacheEnabled,
            boolean mapCacheEnabled,
            boolean mapSnapshotEnabled,
            boolean prefetchEnabled,
            boolean mailboxBatchEnabled,
            boolean preaggEnabled,
            boolean compactSelectedProbeEnabled,
            boolean directArenaMultiGetEnabled,
            boolean mapSnapshotAdaptiveBypassEnabled,
            int mapSnapshotAdaptiveWindowProbes,
            double mapSnapshotAdaptiveMinUsefulHitRate,
            int mapSnapshotAdaptiveResampleIntervalProbes,
            boolean indexedFoldEnabled) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                valueCacheEnabled,
                mapCacheEnabled,
                mapSnapshotEnabled,
                prefetchEnabled,
                mailboxBatchEnabled,
                preaggEnabled,
                compactSelectedProbeEnabled,
                directArenaMultiGetEnabled,
                mapSnapshotAdaptiveBypassEnabled,
                mapSnapshotAdaptiveWindowProbes,
                mapSnapshotAdaptiveMinUsefulHitRate,
                mapSnapshotAdaptiveResampleIntervalProbes,
                indexedFoldEnabled,
                false);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean valueCacheEnabled,
            boolean mapCacheEnabled,
            boolean mapSnapshotEnabled,
            boolean prefetchEnabled,
            boolean mailboxBatchEnabled,
            boolean preaggEnabled,
            boolean compactSelectedProbeEnabled,
            boolean directArenaMultiGetEnabled,
            boolean mapSnapshotAdaptiveBypassEnabled,
            int mapSnapshotAdaptiveWindowProbes,
            double mapSnapshotAdaptiveMinUsefulHitRate,
            int mapSnapshotAdaptiveResampleIntervalProbes,
            boolean indexedFoldEnabled,
            boolean readActivatedWriteThrough) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                valueCacheEnabled,
                mapCacheEnabled,
                mapSnapshotEnabled,
                prefetchEnabled,
                mailboxBatchEnabled,
                preaggEnabled,
                compactSelectedProbeEnabled,
                directArenaMultiGetEnabled,
                mapSnapshotAdaptiveBypassEnabled,
                mapSnapshotAdaptiveWindowProbes,
                mapSnapshotAdaptiveMinUsefulHitRate,
                mapSnapshotAdaptiveResampleIntervalProbes,
                indexedFoldEnabled,
                readActivatedWriteThrough,
                false);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean valueCacheEnabled,
            boolean mapCacheEnabled,
            boolean mapSnapshotEnabled,
            boolean prefetchEnabled,
            boolean mailboxBatchEnabled,
            boolean preaggEnabled,
            boolean compactSelectedProbeEnabled,
            boolean directArenaMultiGetEnabled,
            boolean mapSnapshotAdaptiveBypassEnabled,
            int mapSnapshotAdaptiveWindowProbes,
            double mapSnapshotAdaptiveMinUsefulHitRate,
            int mapSnapshotAdaptiveResampleIntervalProbes,
            boolean indexedFoldEnabled,
            boolean readActivatedWriteThrough,
            boolean residentMutationBatchEnabled) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                valueCacheEnabled,
                mapCacheEnabled,
                mapSnapshotEnabled,
                prefetchEnabled,
                mailboxBatchEnabled,
                preaggEnabled,
                compactSelectedProbeEnabled,
                directArenaMultiGetEnabled,
                mapSnapshotAdaptiveBypassEnabled,
                mapSnapshotAdaptiveWindowProbes,
                mapSnapshotAdaptiveMinUsefulHitRate,
                mapSnapshotAdaptiveResampleIntervalProbes,
                indexedFoldEnabled,
                readActivatedWriteThrough,
                residentMutationBatchEnabled,
                false);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean valueCacheEnabled,
            boolean mapCacheEnabled,
            boolean mapSnapshotEnabled,
            boolean prefetchEnabled,
            boolean mailboxBatchEnabled,
            boolean preaggEnabled,
            boolean compactSelectedProbeEnabled,
            boolean directArenaMultiGetEnabled,
            boolean mapSnapshotAdaptiveBypassEnabled,
            int mapSnapshotAdaptiveWindowProbes,
            double mapSnapshotAdaptiveMinUsefulHitRate,
            int mapSnapshotAdaptiveResampleIntervalProbes,
            boolean indexedFoldEnabled,
            boolean readActivatedWriteThrough,
            boolean residentMutationBatchEnabled,
            boolean directArenaReadOnlyEnabled) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                valueCacheEnabled,
                mapCacheEnabled,
                mapSnapshotEnabled,
                prefetchEnabled,
                mailboxBatchEnabled,
                preaggEnabled,
                compactSelectedProbeEnabled,
                directArenaMultiGetEnabled,
                mapSnapshotAdaptiveBypassEnabled,
                mapSnapshotAdaptiveWindowProbes,
                mapSnapshotAdaptiveMinUsefulHitRate,
                mapSnapshotAdaptiveResampleIntervalProbes,
                indexedFoldEnabled,
                readActivatedWriteThrough,
                residentMutationBatchEnabled,
                directArenaReadOnlyEnabled,
                false);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean valueCacheEnabled,
            boolean mapCacheEnabled,
            boolean mapSnapshotEnabled,
            boolean prefetchEnabled,
            boolean mailboxBatchEnabled,
            boolean preaggEnabled,
            boolean compactSelectedProbeEnabled,
            boolean directArenaMultiGetEnabled,
            boolean mapSnapshotAdaptiveBypassEnabled,
            int mapSnapshotAdaptiveWindowProbes,
            double mapSnapshotAdaptiveMinUsefulHitRate,
            int mapSnapshotAdaptiveResampleIntervalProbes,
            boolean indexedFoldEnabled,
            boolean readActivatedWriteThrough,
            boolean residentMutationBatchEnabled,
            boolean directArenaReadOnlyEnabled,
            boolean negativeHandoffEnabled) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                valueCacheEnabled,
                mapCacheEnabled,
                mapSnapshotEnabled,
                prefetchEnabled,
                mailboxBatchEnabled,
                preaggEnabled,
                compactSelectedProbeEnabled,
                directArenaMultiGetEnabled,
                mapSnapshotAdaptiveBypassEnabled,
                mapSnapshotAdaptiveWindowProbes,
                mapSnapshotAdaptiveMinUsefulHitRate,
                mapSnapshotAdaptiveResampleIntervalProbes,
                indexedFoldEnabled,
                readActivatedWriteThrough,
                residentMutationBatchEnabled,
                directArenaReadOnlyEnabled,
                negativeHandoffEnabled,
                false);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean valueCacheEnabled,
            boolean mapCacheEnabled,
            boolean mapSnapshotEnabled,
            boolean prefetchEnabled,
            boolean mailboxBatchEnabled,
            boolean preaggEnabled,
            boolean compactSelectedProbeEnabled,
            boolean directArenaMultiGetEnabled,
            boolean mapSnapshotAdaptiveBypassEnabled,
            int mapSnapshotAdaptiveWindowProbes,
            double mapSnapshotAdaptiveMinUsefulHitRate,
            int mapSnapshotAdaptiveResampleIntervalProbes,
            boolean indexedFoldEnabled,
            boolean readActivatedWriteThrough,
            boolean residentMutationBatchEnabled,
            boolean directArenaReadOnlyEnabled,
            boolean negativeHandoffEnabled,
            boolean deferredReservationMaterializationEnabled) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                valueCacheEnabled,
                mapCacheEnabled,
                mapSnapshotEnabled,
                prefetchEnabled,
                mailboxBatchEnabled,
                preaggEnabled,
                compactSelectedProbeEnabled,
                directArenaMultiGetEnabled,
                mapSnapshotAdaptiveBypassEnabled,
                mapSnapshotAdaptiveWindowProbes,
                mapSnapshotAdaptiveMinUsefulHitRate,
                mapSnapshotAdaptiveResampleIntervalProbes,
                indexedFoldEnabled,
                readActivatedWriteThrough,
                residentMutationBatchEnabled,
                directArenaReadOnlyEnabled,
                negativeHandoffEnabled,
                deferredReservationMaterializationEnabled,
                false);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean valueCacheEnabled,
            boolean mapCacheEnabled,
            boolean mapSnapshotEnabled,
            boolean prefetchEnabled,
            boolean mailboxBatchEnabled,
            boolean preaggEnabled,
            boolean compactSelectedProbeEnabled,
            boolean directArenaMultiGetEnabled,
            boolean mapSnapshotAdaptiveBypassEnabled,
            int mapSnapshotAdaptiveWindowProbes,
            double mapSnapshotAdaptiveMinUsefulHitRate,
            int mapSnapshotAdaptiveResampleIntervalProbes,
            boolean indexedFoldEnabled,
            boolean readActivatedWriteThrough,
            boolean residentMutationBatchEnabled,
            boolean directArenaReadOnlyEnabled,
            boolean negativeHandoffEnabled,
            boolean deferredReservationMaterializationEnabled,
            boolean compactionScratchSlotEnabled) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                valueCacheEnabled,
                mapCacheEnabled,
                mapSnapshotEnabled,
                prefetchEnabled,
                mailboxBatchEnabled,
                preaggEnabled,
                compactSelectedProbeEnabled,
                directArenaMultiGetEnabled,
                mapSnapshotAdaptiveBypassEnabled,
                mapSnapshotAdaptiveWindowProbes,
                mapSnapshotAdaptiveMinUsefulHitRate,
                mapSnapshotAdaptiveResampleIntervalProbes,
                indexedFoldEnabled,
                readActivatedWriteThrough,
                residentMutationBatchEnabled,
                directArenaReadOnlyEnabled,
                negativeHandoffEnabled,
                deferredReservationMaterializationEnabled,
                compactionScratchSlotEnabled,
                batchEntries,
                batchKeyArenaBytes);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean valueCacheEnabled,
            boolean mapCacheEnabled,
            boolean mapSnapshotEnabled,
            boolean prefetchEnabled,
            boolean mailboxBatchEnabled,
            boolean preaggEnabled,
            boolean compactSelectedProbeEnabled,
            boolean directArenaMultiGetEnabled,
            boolean mapSnapshotAdaptiveBypassEnabled,
            int mapSnapshotAdaptiveWindowProbes,
            double mapSnapshotAdaptiveMinUsefulHitRate,
            int mapSnapshotAdaptiveResampleIntervalProbes,
            boolean indexedFoldEnabled,
            boolean readActivatedWriteThrough,
            boolean residentMutationBatchEnabled,
            boolean directArenaReadOnlyEnabled,
            boolean negativeHandoffEnabled,
            boolean deferredReservationMaterializationEnabled,
            boolean compactionScratchSlotEnabled,
            int compactionScratchEntries,
            int compactionScratchKeyArenaBytes) {
        this(
                enabled,
                libraryPath,
                kernel,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                batchEntries,
                batchKeyArenaBytes,
                batchValueArenaBytes,
                minBatchSize,
                batchSlots,
                aarch64Only,
                writeThroughMutations,
                valueCacheEnabled,
                mapCacheEnabled,
                mapSnapshotEnabled,
                prefetchEnabled,
                mailboxBatchEnabled,
                preaggEnabled,
                compactSelectedProbeEnabled,
                directArenaMultiGetEnabled,
                mapSnapshotAdaptiveBypassEnabled,
                mapSnapshotAdaptiveWindowProbes,
                mapSnapshotAdaptiveMinUsefulHitRate,
                mapSnapshotAdaptiveResampleIntervalProbes,
                indexedFoldEnabled,
                readActivatedWriteThrough,
                residentMutationBatchEnabled,
                directArenaReadOnlyEnabled,
                negativeHandoffEnabled,
                deferredReservationMaterializationEnabled,
                compactionScratchSlotEnabled,
                compactionScratchEntries,
                compactionScratchKeyArenaBytes,
                false);
    }

    public NativeRequestPlaneOptions(
            boolean enabled,
            String libraryPath,
            String kernel,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int batchEntries,
            int batchKeyArenaBytes,
            int batchValueArenaBytes,
            int minBatchSize,
            int batchSlots,
            boolean aarch64Only,
            boolean writeThroughMutations,
            boolean valueCacheEnabled,
            boolean mapCacheEnabled,
            boolean mapSnapshotEnabled,
            boolean prefetchEnabled,
            boolean mailboxBatchEnabled,
            boolean preaggEnabled,
            boolean compactSelectedProbeEnabled,
            boolean directArenaMultiGetEnabled,
            boolean mapSnapshotAdaptiveBypassEnabled,
            int mapSnapshotAdaptiveWindowProbes,
            double mapSnapshotAdaptiveMinUsefulHitRate,
            int mapSnapshotAdaptiveResampleIntervalProbes,
            boolean indexedFoldEnabled,
            boolean readActivatedWriteThrough,
            boolean residentMutationBatchEnabled,
            boolean directArenaReadOnlyEnabled,
            boolean negativeHandoffEnabled,
            boolean deferredReservationMaterializationEnabled,
            boolean compactionScratchSlotEnabled,
            int compactionScratchEntries,
            int compactionScratchKeyArenaBytes,
            boolean directArenaEagerMaterializationEnabled) {
        this.enabled = enabled;
        this.libraryPath = Objects.requireNonNull(libraryPath, "libraryPath").trim();
        this.kernel = normalizeKernel(kernel);
        if (capacityEntries <= 0
                || keyArenaBytes <= 0
                || valueArenaBytes <= 0
                || batchEntries <= 0
                || batchKeyArenaBytes <= 0
                || batchValueArenaBytes <= 0
                || minBatchSize <= 0
                || minBatchSize > batchEntries
                || batchSlots <= 0
                || compactionScratchEntries <= 0
                || compactionScratchKeyArenaBytes <= 0) {
            throw new IllegalArgumentException(
                    "Native request-plane capacities, batch sizes, and slot count must be positive; "
                            + "min-batch-size must not exceed batch-entries.");
        }
        if (!this.libraryPath.isEmpty()) {
            Path path = Paths.get(this.libraryPath);
            if (!path.isAbsolute()) {
                throw new IllegalArgumentException(
                        "Native request-plane library path must be absolute: " + libraryPath);
            }
        }
        this.capacityEntries = capacityEntries;
        this.keyArenaBytes = keyArenaBytes;
        this.valueArenaBytes = valueArenaBytes;
        this.batchEntries = batchEntries;
        this.batchKeyArenaBytes = batchKeyArenaBytes;
        this.batchValueArenaBytes = batchValueArenaBytes;
        this.minBatchSize = minBatchSize;
        this.batchSlots = batchSlots;
        this.aarch64Only = aarch64Only;
        this.writeThroughMutations = writeThroughMutations;
        if (readActivatedWriteThrough && (!writeThroughMutations || !valueCacheEnabled)) {
            throw new IllegalArgumentException(
                    "Resident-only native mutation write-through requires both write-through-mutations and native ValueState cache.");
        }
        this.readActivatedWriteThrough = readActivatedWriteThrough;
        if (residentMutationBatchEnabled && !readActivatedWriteThrough) {
            throw new IllegalArgumentException(
                    "Resident mutation batching requires resident-only native mutation write-through.");
        }
        this.residentMutationBatchEnabled = residentMutationBatchEnabled;
        if (valueCacheEnabled && !enabled) {
            throw new IllegalArgumentException(
                    "Native ValueState cache requires the native request plane to be enabled.");
        }
        this.valueCacheEnabled = valueCacheEnabled;
        if (mapCacheEnabled && !enabled) {
            throw new IllegalArgumentException(
                    "Native MapState cache requires the native request plane to be enabled.");
        }
        this.mapCacheEnabled = mapCacheEnabled;
        if (mapSnapshotEnabled && !enabled) {
            throw new IllegalArgumentException(
                    "Native MapState snapshot requires the native request plane to be enabled.");
        }
        this.mapSnapshotEnabled = mapSnapshotEnabled;
        if (prefetchEnabled && !enabled) {
            throw new IllegalArgumentException(
                    "Native prepared-key prefetch requires the native request plane to be enabled.");
        }
        this.prefetchEnabled = prefetchEnabled;
        if (mailboxBatchEnabled && !enabled) {
            throw new IllegalArgumentException(
                    "Native mailbox batch compaction requires the native request plane to be enabled.");
        }
        this.mailboxBatchEnabled = mailboxBatchEnabled;
        if (preaggEnabled && !enabled) {
            throw new IllegalArgumentException(
                    "Native LocalPreAgg grouping requires the native request plane to be enabled.");
        }
        this.preaggEnabled = preaggEnabled;
        if (indexedFoldEnabled && !preaggEnabled) {
            throw new IllegalArgumentException(
                    "Indexed LocalPreAgg fold requires native LocalPreAgg grouping.");
        }
        this.indexedFoldEnabled = indexedFoldEnabled;
        if (compactSelectedProbeEnabled && (!enabled || !prefetchEnabled || !mailboxBatchEnabled)) {
            throw new IllegalArgumentException(
                    "Compact-selected probe requires native request plane, prefetch, and mailbox batch.");
        }
        this.compactSelectedProbeEnabled = compactSelectedProbeEnabled;
        if (directArenaMultiGetEnabled && !compactSelectedProbeEnabled) {
            throw new IllegalArgumentException(
                    "Direct-arena MultiGet requires compact-selected prepared-key probe.");
        }
        if (directArenaMultiGetEnabled
                && batchValueArenaBytes < RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH) {
            throw new IllegalArgumentException(
                    "Direct-arena MultiGet requires batch-value-arena-bytes to be at least "
                            + RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH
                            + ".");
        }
        this.directArenaMultiGetEnabled = directArenaMultiGetEnabled;
        this.directArenaBatchSize = RocksDBBatchValueReader.DIRECT_ARENA_DEFAULT_BATCH;
        if (directArenaReadOnlyEnabled && !directArenaMultiGetEnabled) {
            throw new IllegalArgumentException(
                    "Direct-read-only prefetch requires direct-arena MultiGet.");
        }
        this.directArenaReadOnlyEnabled = directArenaReadOnlyEnabled;
        if (directArenaEagerMaterializationEnabled && !directArenaReadOnlyEnabled) {
            throw new IllegalArgumentException(
                    "Direct-arena eager materialization requires direct-read-only prefetch.");
        }
        this.directArenaEagerMaterializationEnabled =
                directArenaEagerMaterializationEnabled;
        if (negativeHandoffEnabled && !directArenaReadOnlyEnabled) {
            throw new IllegalArgumentException(
                    "Native negative handoff requires direct-read-only prefetch.");
        }
        this.negativeHandoffEnabled = negativeHandoffEnabled;
        if (deferredReservationMaterializationEnabled
                && (!enabled
                        || !prefetchEnabled
                        || !mailboxBatchEnabled
                        || !compactSelectedProbeEnabled)) {
            throw new IllegalArgumentException(
                    "Deferred reservation materialization requires native request plane, prefetch, mailbox batching, and compact-selected probe.");
        }
        this.deferredReservationMaterializationEnabled =
                deferredReservationMaterializationEnabled;
        if (compactionScratchSlotEnabled
                && (!enabled || !prefetchEnabled || !mailboxBatchEnabled)) {
            throw new IllegalArgumentException(
                    "Native mailbox compaction scratch slot requires native request plane, prefetch, and mailbox batching.");
        }
        this.compactionScratchSlotEnabled = compactionScratchSlotEnabled;
        this.compactionScratchEntries = compactionScratchEntries;
        this.compactionScratchKeyArenaBytes = compactionScratchKeyArenaBytes;
        if (mapSnapshotAdaptiveBypassEnabled && !mapSnapshotEnabled) {
            throw new IllegalArgumentException(
                    "Native MapSnapshot adaptive bypass requires native MapSnapshot to be enabled.");
        }
        if (mapSnapshotAdaptiveWindowProbes < 2
                || Double.isNaN(mapSnapshotAdaptiveMinUsefulHitRate)
                || Double.isInfinite(mapSnapshotAdaptiveMinUsefulHitRate)
                || mapSnapshotAdaptiveMinUsefulHitRate < 0.0
                || mapSnapshotAdaptiveMinUsefulHitRate > 1.0
                || mapSnapshotAdaptiveResampleIntervalProbes <= 0) {
            throw new IllegalArgumentException(
                    "Native MapSnapshot adaptive window must be at least 2, resample must be positive, and useful-hit rate must be in [0, 1].");
        }
        this.mapSnapshotAdaptiveBypassEnabled = mapSnapshotAdaptiveBypassEnabled;
        this.mapSnapshotAdaptiveWindowProbes = mapSnapshotAdaptiveWindowProbes;
        this.mapSnapshotAdaptiveMinUsefulHitRate = mapSnapshotAdaptiveMinUsefulHitRate;
        this.mapSnapshotAdaptiveResampleIntervalProbes = mapSnapshotAdaptiveResampleIntervalProbes;
        this.valuePointAdaptiveBypassEnabled = false;
        this.valuePointAdaptiveWindowProbes = 4096;
        this.valuePointAdaptiveZeroWindows = 2;
        this.valuePointAdaptiveResampleIntervalProbes = 4096;
        this.valuePointAdaptiveSampleSlots = 64;
    }

    public static NativeRequestPlaneOptions disabled() {
        return DISABLED;
    }

    public boolean enabled() {
        return enabled;
    }

    public String libraryPath() {
        return libraryPath;
    }

    public String kernel() {
        return kernel;
    }

    public int kernelPreference() {
        switch (kernel) {
            case "auto":
                return NativeRequestPlaneBridge.KERNEL_AUTO;
            case "neon":
                return NativeRequestPlaneBridge.KERNEL_NEON_CRC;
            case "sve256":
                return NativeRequestPlaneBridge.KERNEL_SVE256;
            default:
                throw new IllegalStateException("Unexpected native kernel: " + kernel);
        }
    }

    public int capacityEntries() {
        return capacityEntries;
    }

    public long keyArenaBytes() {
        return keyArenaBytes;
    }

    public long valueArenaBytes() {
        return valueArenaBytes;
    }

    public int batchEntries() {
        return batchEntries;
    }

    public int batchKeyArenaBytes() {
        return batchKeyArenaBytes;
    }

    public int batchValueArenaBytes() {
        return batchValueArenaBytes;
    }

    public int minBatchSize() {
        return minBatchSize;
    }

    public int batchSlots() {
        return batchSlots;
    }

    /**
     * Whether an explicitly enabled request plane must fail closed on a non-AArch64 host.
     *
     * <p>This defaults to true so the production ARM-native path cannot silently turn into the
     * portable scalar kernel on x86. Cross-platform experiments may set it to false explicitly.
     */
    public boolean aarch64Only() {
        return aarch64Only;
    }

    /**
     * Whether authoritative RocksDB mutations are also serialized and written through JNI.
     *
     * <p>The default is false. Every mutation already advances the ValueState generation; exact
     * generation probes therefore reject all older native entries without duplicating value
     * serialization and JNI work on the write path.
     */
    public boolean writeThroughMutations() {
        return writeThroughMutations;
    }

    /**
     * Whether native mutation publication is dormant until a state read activates it, then updates
     * only exact keys already resident in the native ValueState cache.
     */
    public boolean readActivatedWriteThrough() {
        return readActivatedWriteThrough;
    }

    /** Whether resident-only mutation checks and updates are coalesced per mailbox dispatch. */
    public boolean residentMutationBatchEnabled() {
        return residentMutationBatchEnabled;
    }

    /** Whether the separately gated native ValueState point-cache path is enabled. */
    public boolean valueCacheEnabled() {
        return valueCacheEnabled;
    }

    public boolean valuePointAdaptiveBypassEnabled() {
        return valuePointAdaptiveBypassEnabled;
    }

    public int valuePointAdaptiveWindowProbes() {
        return valuePointAdaptiveWindowProbes;
    }

    public int valuePointAdaptiveZeroWindows() {
        return valuePointAdaptiveZeroWindows;
    }

    public int valuePointAdaptiveResampleIntervalProbes() {
        return valuePointAdaptiveResampleIntervalProbes;
    }

    public int valuePointAdaptiveSampleSlots() {
        return valuePointAdaptiveSampleSlots;
    }

    /** Whether the separately gated native MapState point-cache path is enabled. */
    public boolean mapCacheEnabled() {
        return mapCacheEnabled;
    }

    public boolean mapSnapshotEnabled() {
        return mapSnapshotEnabled;
    }

    public boolean mapSnapshotAdaptiveBypassEnabled() {
        return mapSnapshotAdaptiveBypassEnabled;
    }

    public int mapSnapshotAdaptiveWindowProbes() {
        return mapSnapshotAdaptiveWindowProbes;
    }

    public double mapSnapshotAdaptiveMinUsefulHitRate() {
        return mapSnapshotAdaptiveMinUsefulHitRate;
    }

    public int mapSnapshotAdaptiveResampleIntervalProbes() {
        return mapSnapshotAdaptiveResampleIntervalProbes;
    }

    public boolean prefetchEnabled() {
        return prefetchEnabled;
    }

    public boolean mailboxBatchEnabled() {
        return mailboxBatchEnabled;
    }

    public boolean preaggEnabled() {
        return preaggEnabled;
    }

    public boolean indexedFoldEnabled() {
        return indexedFoldEnabled;
    }

    /** Whether compacted mailbox keys are probed directly from their original prepared arena. */
    public boolean compactSelectedProbeEnabled() {
        return compactSelectedProbeEnabled;
    }

    /** Whether compacted RocksDB misses are read from the original prepared-key direct arena. */
    public boolean directArenaMultiGetEnabled() {
        return directArenaMultiGetEnabled;
    }

    /** Maximum prepared-key count requested by one direct-arena JNI call. */
    public int directArenaBatchSize() {
        return directArenaBatchSize;
    }

    /** Returns an otherwise identical immutable option set with a different direct batch size. */
    public NativeRequestPlaneOptions withDirectArenaBatchSize(int batchSize) {
        if (batchSize < 2 || batchSize > RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH) {
            throw new IllegalArgumentException(
                    "Direct-arena batch size must be between 2 and "
                            + RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH
                            + ".");
        }
        return batchSize == directArenaBatchSize
                ? this
                : new NativeRequestPlaneOptions(this, batchSize);
    }

    /** Returns an otherwise identical option set with adaptive ValueState point bypass settings. */
    public NativeRequestPlaneOptions withValuePointAdaptiveBypass(
            boolean enabled,
            int windowProbes,
            int zeroWindows,
            int resampleIntervalProbes,
            int sampleSlots) {
        if (enabled && !valueCacheEnabled) {
            throw new IllegalArgumentException(
                    "Adaptive ValueState point bypass requires native ValueState cache.");
        }
        if (windowProbes <= 0
                || zeroWindows <= 0
                || resampleIntervalProbes <= 0
                || sampleSlots <= 0) {
            throw new IllegalArgumentException(
                    "Adaptive ValueState point window, zero-window threshold, resample interval, and sample slots must be positive.");
        }
        if ((sampleSlots & (sampleSlots - 1)) != 0) {
            throw new IllegalArgumentException(
                    "Adaptive ValueState point sample slots must be a power of two.");
        }
        if (enabled == valuePointAdaptiveBypassEnabled
                && windowProbes == valuePointAdaptiveWindowProbes
                && zeroWindows == valuePointAdaptiveZeroWindows
                && resampleIntervalProbes == valuePointAdaptiveResampleIntervalProbes
                && sampleSlots == valuePointAdaptiveSampleSlots) {
            return this;
        }
        return new NativeRequestPlaneOptions(
                this,
                directArenaBatchSize,
                enabled,
                windowProbes,
                zeroWindows,
                resampleIntervalProbes,
                sampleSlots);
    }

    private NativeRequestPlaneOptions(NativeRequestPlaneOptions source, int batchSize) {
        this(
                source,
                batchSize,
                source.valuePointAdaptiveBypassEnabled,
                source.valuePointAdaptiveWindowProbes,
                source.valuePointAdaptiveZeroWindows,
                source.valuePointAdaptiveResampleIntervalProbes,
                source.valuePointAdaptiveSampleSlots);
    }

    private NativeRequestPlaneOptions(
            NativeRequestPlaneOptions source,
            int batchSize,
            boolean valuePointAdaptiveBypassEnabled,
            int valuePointAdaptiveWindowProbes,
            int valuePointAdaptiveZeroWindows,
            int valuePointAdaptiveResampleIntervalProbes,
            int valuePointAdaptiveSampleSlots) {
        this.enabled = source.enabled;
        this.libraryPath = source.libraryPath;
        this.kernel = source.kernel;
        this.capacityEntries = source.capacityEntries;
        this.keyArenaBytes = source.keyArenaBytes;
        this.valueArenaBytes = source.valueArenaBytes;
        this.batchEntries = source.batchEntries;
        this.batchKeyArenaBytes = source.batchKeyArenaBytes;
        this.batchValueArenaBytes = source.batchValueArenaBytes;
        this.minBatchSize = source.minBatchSize;
        this.batchSlots = source.batchSlots;
        this.aarch64Only = source.aarch64Only;
        this.writeThroughMutations = source.writeThroughMutations;
        this.readActivatedWriteThrough = source.readActivatedWriteThrough;
        this.residentMutationBatchEnabled = source.residentMutationBatchEnabled;
        this.valueCacheEnabled = source.valueCacheEnabled;
        this.mapCacheEnabled = source.mapCacheEnabled;
        this.mapSnapshotEnabled = source.mapSnapshotEnabled;
        this.prefetchEnabled = source.prefetchEnabled;
        this.mailboxBatchEnabled = source.mailboxBatchEnabled;
        this.preaggEnabled = source.preaggEnabled;
        this.indexedFoldEnabled = source.indexedFoldEnabled;
        this.compactSelectedProbeEnabled = source.compactSelectedProbeEnabled;
        this.directArenaMultiGetEnabled = source.directArenaMultiGetEnabled;
        this.directArenaBatchSize = batchSize;
        this.directArenaReadOnlyEnabled = source.directArenaReadOnlyEnabled;
        this.directArenaEagerMaterializationEnabled = source.directArenaEagerMaterializationEnabled;
        this.negativeHandoffEnabled = source.negativeHandoffEnabled;
        this.deferredReservationMaterializationEnabled =
                source.deferredReservationMaterializationEnabled;
        this.compactionScratchSlotEnabled = source.compactionScratchSlotEnabled;
        this.compactionScratchEntries = source.compactionScratchEntries;
        this.compactionScratchKeyArenaBytes = source.compactionScratchKeyArenaBytes;
        this.mapSnapshotAdaptiveBypassEnabled = source.mapSnapshotAdaptiveBypassEnabled;
        this.mapSnapshotAdaptiveWindowProbes = source.mapSnapshotAdaptiveWindowProbes;
        this.mapSnapshotAdaptiveMinUsefulHitRate = source.mapSnapshotAdaptiveMinUsefulHitRate;
        this.mapSnapshotAdaptiveResampleIntervalProbes =
                source.mapSnapshotAdaptiveResampleIntervalProbes;
        this.valuePointAdaptiveBypassEnabled = valuePointAdaptiveBypassEnabled;
        this.valuePointAdaptiveWindowProbes = valuePointAdaptiveWindowProbes;
        this.valuePointAdaptiveZeroWindows = valuePointAdaptiveZeroWindows;
        this.valuePointAdaptiveResampleIntervalProbes =
                valuePointAdaptiveResampleIntervalProbes;
        this.valuePointAdaptiveSampleSlots = valuePointAdaptiveSampleSlots;
    }

    /** Whether prepared keys bypass native cache probe/fill and go directly to RocksDB MultiGet. */
    public boolean directArenaReadOnlyEnabled() {
        return directArenaReadOnlyEnabled;
    }

    /** Whether direct-read values are deserialized from the native arena before slot release. */
    public boolean directArenaEagerMaterializationEnabled() {
        return directArenaEagerMaterializationEnabled;
    }

    /** Whether speculative native NOT_FOUND results reuse their exact staged key as payload. */
    public boolean negativeHandoffEnabled() {
        return negativeHandoffEnabled;
    }

    /** Whether Java reservation objects are allocated only after native duplicate compaction. */
    public boolean deferredReservationMaterializationEnabled() {
        return deferredReservationMaterializationEnabled;
    }

    /** Whether slot exhaustion may fall back to one compaction-only, short-lived scratch lease. */
    public boolean compactionScratchSlotEnabled() {
        return compactionScratchSlotEnabled;
    }

    /** Maximum prepared entries in the key-only compaction scratch workspace. */
    public int compactionScratchEntries() {
        return compactionScratchEntries;
    }

    /** Prepared-key arena bytes in the key-only compaction scratch workspace. */
    public int compactionScratchKeyArenaBytes() {
        return compactionScratchKeyArenaBytes;
    }

    /** Whether this treatment consumes the bounded Java ValueState cache. */
    public boolean requiresValueCache() {
        return valueCacheEnabled || prefetchEnabled;
    }

    private static String normalizeKernel(String kernel) {
        String normalized =
                Objects.requireNonNull(kernel, "kernel").trim().toLowerCase(Locale.ROOT);
        if (!normalized.equals("auto")
                && !normalized.equals("neon")
                && !normalized.equals("sve256")) {
            throw new IllegalArgumentException(
                    "Native request-plane kernel must be auto, neon, or sve256: " + kernel);
        }
        return normalized;
    }
}
