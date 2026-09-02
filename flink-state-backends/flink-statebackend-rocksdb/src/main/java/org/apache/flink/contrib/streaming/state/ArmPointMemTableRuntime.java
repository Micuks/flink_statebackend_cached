/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;

import org.rocksdb.ArmPointMemTableConfig;

import javax.annotation.Nullable;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Same-binary experiment selection and process-terminal audit for ArmPoint memtables. */
final class ArmPointMemTableRuntime {
    static final String ENVIRONMENT = "CACHEKIT_ROCKSDB_ARM_POINT_MEMTABLE";

    private static final AtomicBoolean INITIALIZED = new AtomicBoolean();
    private static final AtomicReference<String> REQUESTED_MODE =
            new AtomicReference<>("off");
    private static final AtomicReference<String> REQUESTED_SCOPE =
            new AtomicReference<>("none");

    private ArmPointMemTableRuntime() {}

    static Selection currentSelection(boolean configured, String configuredMode) {
        return selection(System.getenv(ENVIRONMENT), configured, configuredMode);
    }

    static Selection selection(
            @Nullable String environment, boolean configured, String configuredMode) {
        if (environment == null || environment.trim().isEmpty()) {
            return configured
                    ? new Selection(true, normalizeProbeMode(configuredMode), false)
                    : new Selection(false, "off", false);
        }

        final String value = environment.trim().toLowerCase(Locale.ROOT);
        if (value.equals("0") || value.equals("false") || value.equals("off")) {
            return new Selection(false, "off", true);
        }
        if (value.equals("scalar") || value.equals("sve") || value.equals("auto")) {
            // The explicit experiment gate covers every KV state. Range and iterator operations
            // remain on ArmPoint's authoritative SkipList representation.
            return new Selection(true, value, true);
        }
        throw new IllegalArgumentException(
                ENVIRONMENT + " must be off, scalar, sve, or auto, but was " + environment);
    }

    static void initializeAndRecord(Selection selection) {
        if (INITIALIZED.compareAndSet(false, true)) {
            ArmPointMemTableConfig.resetStats();
            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(
                                    ArmPointMemTableRuntime::emitTerminalAudit,
                                    "cachekit-arm-point-memtable-audit"));
        }
        if (selection.enabled) {
            recordConsistent(REQUESTED_MODE, selection.probeMode);
            recordConsistent(
                    REQUESTED_SCOPE, selection.allKeyValueStates ? "all-kv" : "value-only");
        }
    }

    private static void recordConsistent(AtomicReference<String> target, String value) {
        for (; ; ) {
            final String current = target.get();
            if (current.equals(value) || current.equals("mixed")) {
                return;
            }
            final String next = current.equals("off") || current.equals("none") ? value : "mixed";
            if (target.compareAndSet(current, next)) {
                return;
            }
        }
    }

    private static String normalizeProbeMode(String mode) {
        final String normalized = mode.toLowerCase(Locale.ROOT);
        if (normalized.equals("scalar")
                || normalized.equals("sve")
                || normalized.equals("auto")) {
            return normalized;
        }
        throw new IllegalArgumentException("invalid ArmPoint probe mode " + mode);
    }

    private static void emitTerminalAudit() {
        final ArmPointMemTableConfig.Stats stats = ArmPointMemTableConfig.stats();
        System.err.printf(
                Locale.ROOT,
                "CacheKitArmPointMemTable configured=%s mode=%s scope=%s sveSupported=%s "
                        + "pointLookups=%d tagRejects=%d bucketScans=%d "
                        + "internalKeyCandidates=%d scalarTagProbes=%d sveTagProbes=%d "
                        + "tagDirectoryOverflows=%d orderedFallbackLookups=%d "
                        + "hashIndexedReps=%d incompatibleComparatorReps=%d "
                        + "positiveIndexHits=%d tagMissFallbacks=%d mutableFallbackLookups=%d%n",
                !REQUESTED_MODE.get().equals("off"),
                REQUESTED_MODE.get(),
                REQUESTED_SCOPE.get(),
                ArmPointMemTableConfig.isSveSupported(),
                stats.pointLookups(),
                stats.tagRejects(),
                stats.bucketScans(),
                stats.internalKeyCandidates(),
                stats.scalarTagProbes(),
                stats.sveTagProbes(),
                stats.tagDirectoryOverflows(),
                stats.orderedFallbackLookups(),
                stats.hashIndexedReps(),
                stats.incompatibleComparatorReps(),
                stats.positiveIndexHits(),
                stats.tagMissFallbacks(),
                stats.mutableFallbackLookups());
    }

    static final class Selection {
        final boolean enabled;
        final String probeMode;
        final boolean allKeyValueStates;

        private Selection(boolean enabled, String probeMode, boolean allKeyValueStates) {
            this.enabled = enabled;
            this.probeMode = probeMode;
            this.allKeyValueStates = allKeyValueStates;
        }

        boolean appliesTo(@Nullable RegisteredStateMetaInfoBase stateMetaInfo) {
            if (!enabled || !(stateMetaInfo instanceof RegisteredKeyValueStateBackendMetaInfo)) {
                return false;
            }
            return allKeyValueStates
                    || ((RegisteredKeyValueStateBackendMetaInfo<?, ?>) stateMetaInfo)
                                    .getStateType()
                            == StateDescriptor.Type.VALUE;
        }
    }
}
