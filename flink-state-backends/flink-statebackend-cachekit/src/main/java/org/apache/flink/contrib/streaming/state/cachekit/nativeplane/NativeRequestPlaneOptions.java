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
                    false, "", "auto", 16_384, 4L << 20, 16L << 20, 1024, 256 << 10, 4 << 20, 64, 2);

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
                true);
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
                || batchSlots <= 0) {
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
