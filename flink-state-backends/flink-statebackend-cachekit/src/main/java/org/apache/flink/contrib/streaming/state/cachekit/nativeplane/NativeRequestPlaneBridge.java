/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.nativeplane;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.BatchKeyGroupingSupport;

/**
 * Opt-in, fail-closed JNI owner for one native CacheKit request plane.
 *
 * <p>{@link #open()} is disabled unless {@link #ENABLED_PROPERTY} is explicitly true. Production
 * ValueState integration passes its Flink configuration through the explicit {@link #open(boolean,
 * int, long, long, int, String)} overload. An enabled bridge either loads and uses the native
 * implementation or throws during keyed-backend creation; runtime recovery is owned by the
 * coordinator above this low-level bridge.
 *
 * <p>Each {@link #fillBatch} or {@link #probeBatch} method makes one JNI call for the entire {@link
 * SerializedKeyBatch}. All buffers are interpreted from their current position to limit; caller
 * positions and limits are not changed. Input/output metadata uses native byte order.
 *
 * <p>The bridge and its native request plane are not thread-safe. The keyed-backend coordinator
 * serializes all calls.
 */
@Internal
public final class NativeRequestPlaneBridge implements NativeRequestPlane {

    public static final String ENABLED_PROPERTY =
            "state.backend.cachekit.native.request-plane.enabled";
    public static final String LIBRARY_PATH_PROPERTY =
            "state.backend.cachekit.native.request-plane.library";

    public static final int KERNEL_AUTO = 0;
    public static final int KERNEL_SCALAR = 1;
    public static final int KERNEL_NEON_CRC = 2;
    public static final int KERNEL_SVE256 = 3;
    /**
     * Unsigned UINT64_MAX on the native side; valid only for latest-version probes, never fills.
     */
    public static final long PROBE_LATEST_GENERATION = -1L;

    public static final int FILL_VALUE_ARENA_OFFSET = 0;
    public static final int FILL_VALUE_LENGTH_OFFSET = 4;
    public static final int FILL_VALUE_FLAGS_OFFSET = 8;
    public static final int FILL_VALUE_RESERVED_OFFSET = 12;
    public static final int FILL_VALUE_RECORD_BYTES = 16;
    public static final int FILL_VALUE_NEGATIVE_FLAG = 1;
    public static final int FILL_VALUE_UPDATE_ONLY_FLAG = 1;
    public static final int FILL_VALUE_CHECK_ONLY_FLAG = 1 << 1;

    public static final int FILL_RESULT_STATUS_OFFSET = 0;
    public static final int FILL_RESULT_ERROR_OFFSET = 4;
    public static final int FILL_RESULT_RECORD_BYTES = 8;
    public static final int FILL_INSERTED = 0;
    public static final int FILL_UPDATED = 1;
    public static final int FILL_REJECTED_STALE_GENERATION = 2;
    public static final int FILL_REJECTED_CAPACITY = 3;
    public static final int FILL_INVALID_ARGUMENT = 4;
    public static final int FILL_INTERNAL_ERROR = 5;
    public static final int FILL_NOT_PRESENT = 6;

    public static final int ERROR_OK = 0;
    public static final int ERROR_INVALID_ARGUMENT = 1;
    public static final int ERROR_CAPACITY_EXCEEDED = 2;
    public static final int ERROR_INTERNAL = 6;

    public static final int PROBE_RESULT_STATUS_OFFSET = 0;
    public static final int PROBE_RESULT_ERROR_OFFSET = 4;
    public static final int PROBE_RESULT_ARENA_OFFSET = 8;
    public static final int PROBE_RESULT_LENGTH_OFFSET = 12;
    public static final int PROBE_RESULT_RECORD_BYTES = 16;

    public static final int PROBE_MISS = 0;
    public static final int PROBE_HIT = 1;
    public static final int PROBE_NEGATIVE = 2;

    public static final long FEATURE_AARCH64 = 1L;
    public static final long FEATURE_NEON = 1L << 1;
    public static final long FEATURE_CRC32 = 1L << 2;
    public static final long FEATURE_SVE = 1L << 3;
    public static final long FEATURE_SVE_VL256 = 1L << 4;
    // Deliberately no SVE2 bit: the native runtime currently proves SVE and vector length only.

    private static final String LIBRARY_NAME = "cachekit_native_request_plane_jni";
    private static final int EXPECTED_JNI_ABI_VERSION = 4;
    private static final Object LIBRARY_LOAD_LOCK = new Object();

    private static volatile boolean libraryLoaded;
    private static volatile Throwable libraryLoadFailure;

    private long nativeHandle;

    private NativeRequestPlaneBridge(long nativeHandle) {
        if (nativeHandle == 0) {
            throw new IllegalStateException("Native request plane returned a zero handle.");
        }
        this.nativeHandle = nativeHandle;
    }

    /** Opens the default-sized request plane only when {@link #ENABLED_PROPERTY} is true. */
    public static NativeRequestPlaneBridge open() {
        return open(Boolean.getBoolean(ENABLED_PROPERTY), 1024, 1 << 20, 4 << 20, KERNEL_AUTO);
    }

    /**
     * Opens one native request plane.
     *
     * @param enabled must be explicitly true; false fails before attempting to load JNI
     */
    public static NativeRequestPlaneBridge open(
            boolean enabled,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int kernelPreference) {
        return open(
                enabled,
                capacityEntries,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                kernelPreference,
                System.getProperty(LIBRARY_PATH_PROPERTY, ""));
    }

    /** Opens one native request plane using an explicit Flink-configured library path. */
    public static NativeRequestPlaneBridge open(
            boolean enabled,
            int capacityEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int kernelPreference,
            String libraryPath) {
        return open(
                enabled,
                capacityEntries,
                capacityEntries,
                keyArenaBytes,
                valueArenaBytes,
                kernelPreference,
                libraryPath);
    }

    /** Opens one native request plane with an independent grouping-batch limit. */
    public static NativeRequestPlaneBridge open(
            boolean enabled,
            int capacityEntries,
            int maxBatchEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int kernelPreference,
            String libraryPath) {
        if (!enabled) {
            throw new IllegalStateException(
                    "Native request plane is disabled; explicitly enable "
                            + ENABLED_PROPERTY
                            + ".");
        }
        if (capacityEntries <= 0
                || maxBatchEntries <= 0
                || keyArenaBytes < 0
                || valueArenaBytes < 0) {
            throw new IllegalArgumentException(
                    "capacityEntries and maxBatchEntries must be positive and arena sizes must be "
                            + "non-negative.");
        }
        if (kernelPreference < KERNEL_AUTO || kernelPreference > KERNEL_SVE256) {
            throw new IllegalArgumentException("Unknown native kernel preference.");
        }
        ensureLibraryLoaded(libraryPath);
        verifyNativeAbiCompatibility();
        return new NativeRequestPlaneBridge(
                nativeCreateV3(
                        capacityEntries,
                        maxBatchEntries,
                        keyArenaBytes,
                        valueArenaBytes,
                        kernelPreference));
    }

    public static boolean isEnabledByDefault() {
        return Boolean.getBoolean(ENABLED_PROPERTY);
    }

    /**
     * Fills clean positive/negative entries from direct buffers in one JNI call.
     *
     * <p>{@code valueMetadata} has one 16-byte record per key: arena offset, length, value flags,
     * and ABI4 mutation-control flags. A negative entry uses flag {@link
     * #FILL_VALUE_NEGATIVE_FLAG} and must have zero offset and length. The control field accepts
     * either {@link #FILL_VALUE_CHECK_ONLY_FLAG} or {@link #FILL_VALUE_UPDATE_ONLY_FLAG}; ordinary
     * fills use zero. {@code fillResults} receives status and error integers.
     */
    public int fillBatch(
            SerializedKeyBatch<?, ?> keys,
            ByteBuffer valueArena,
            ByteBuffer valueMetadata,
            ByteBuffer fillResults) {
        Objects.requireNonNull(keys, "keys");
        int count = keys.entryCount();
        ByteBuffer nativeValueArena = directInputSlice(valueArena, "valueArena");
        ByteBuffer nativeValueMetadata = directInputSlice(valueMetadata, "valueMetadata");
        ByteBuffer nativeFillResults = directOutputSlice(fillResults, "fillResults");
        requireCapacity(nativeValueMetadata, count, FILL_VALUE_RECORD_BYTES, "valueMetadata");
        requireCapacity(nativeFillResults, count, FILL_RESULT_RECORD_BYTES, "fillResults");
        return nativeFill(
                requireOpenHandle(),
                keys.arenaSlice(),
                keys.metadataSlice(),
                count,
                nativeValueArena,
                nativeValueMetadata,
                nativeFillResults);
    }

    /**
     * Probes a complete serialized-key batch in one JNI call.
     *
     * <p>Positive hit bytes are packed contiguously into {@code valueOutput}. Each 16-byte result
     * record contains status, error, value offset, and value length. Misses and negative hits have
     * zero offset/length.
     */
    public int probeBatch(
            SerializedKeyBatch<?, ?> keys, ByteBuffer valueOutput, ByteBuffer probeResults) {
        Objects.requireNonNull(keys, "keys");
        int count = keys.entryCount();
        ByteBuffer nativeValueOutput = directOutputSlice(valueOutput, "valueOutput");
        ByteBuffer nativeProbeResults = directOutputSlice(probeResults, "probeResults");
        requireCapacity(nativeProbeResults, count, PROBE_RESULT_RECORD_BYTES, "probeResults");
        return nativeProbe(
                requireOpenHandle(),
                keys.arenaSlice(),
                keys.metadataSlice(),
                count,
                nativeValueOutput,
                nativeProbeResults);
    }

    @Override
    public int compactBatch(SerializedKeyBatch<?, ?> keys, ByteBuffer uniqueSourceIndexes) {
        Objects.requireNonNull(keys, "keys");
        ByteBuffer nativeIndexes = directOutputSlice(uniqueSourceIndexes, "uniqueSourceIndexes");
        requireCapacity(nativeIndexes, keys.entryCount(), Integer.BYTES, "uniqueSourceIndexes");
        return nativeCompact(
                requireOpenHandle(),
                keys.arenaSlice(),
                keys.metadataSlice(),
                keys.entryCount(),
                nativeIndexes);
    }

    @Override
    public int groupBatch(
            SerializedKeyBatch<?, ?> keys,
            ByteBuffer uniqueSourceIndexes,
            ByteBuffer sourceGroupIndexes) {
        Objects.requireNonNull(keys, "keys");
        ByteBuffer nativeUniques = directOutputSlice(uniqueSourceIndexes, "uniqueSourceIndexes");
        ByteBuffer nativeGroups = directOutputSlice(sourceGroupIndexes, "sourceGroupIndexes");
        requireCapacity(nativeUniques, keys.entryCount(), Integer.BYTES, "uniqueSourceIndexes");
        requireCapacity(nativeGroups, keys.entryCount(), Integer.BYTES, "sourceGroupIndexes");
        return nativeGroup(
                requireOpenHandle(),
                keys.arenaSlice(),
                keys.metadataSlice(),
                keys.entryCount(),
                nativeUniques,
                nativeGroups);
    }

    @Override
    public int groupHashTokens(ByteBuffer tokens, int count, ByteBuffer packedPlan) {
        if (count <= 0) {
            throw new IllegalArgumentException("count must be positive.");
        }
        ByteBuffer nativeTokens = directInputSlice(tokens, "tokens");
        ByteBuffer nativePlan = directOutputSlice(packedPlan, "packedPlan");
        requireCapacity(nativeTokens, count, Integer.BYTES, "tokens");
        int requiredPlanBytes = BatchKeyGroupingSupport.requiredPackedPlanBytes(count);
        if (nativePlan.capacity() < requiredPlanBytes) {
            throw new IllegalArgumentException(
                    "packedPlan requires "
                            + requiredPlanBytes
                            + " bytes but has "
                            + nativePlan.capacity()
                            + ".");
        }
        nativePlan.putInt(0, 0);
        return nativeGroupTokensV3(requireOpenHandle(), nativeTokens, count, nativePlan);
    }

    public String selectedKernel() {
        return nativeKernelName(requireOpenHandle());
    }

    public long detectedFeatureBits() {
        return nativeFeatureBits(requireOpenHandle());
    }

    @Override
    public void close() {
        long handle = nativeHandle;
        if (handle != 0) {
            nativeHandle = 0;
            nativeDestroy(handle);
        }
    }

    private long requireOpenHandle() {
        long handle = nativeHandle;
        if (handle == 0) {
            throw new IllegalStateException("Native request plane is closed.");
        }
        return handle;
    }

    private static ByteBuffer directInputSlice(ByteBuffer buffer, String name) {
        Objects.requireNonNull(buffer, name);
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException(name + " must be a direct ByteBuffer.");
        }
        return buffer.slice().order(ByteOrder.nativeOrder());
    }

    private static ByteBuffer directOutputSlice(ByteBuffer buffer, String name) {
        Objects.requireNonNull(buffer, name);
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException(name + " must be a direct ByteBuffer.");
        }
        if (buffer.isReadOnly()) {
            throw new IllegalArgumentException(name + " must be writable.");
        }
        return buffer.slice().order(ByteOrder.nativeOrder());
    }

    private static void requireCapacity(
            ByteBuffer buffer, int count, int recordBytes, String name) {
        long required = (long) count * recordBytes;
        if (required > buffer.capacity()) {
            throw new IllegalArgumentException(
                    name + " requires " + required + " bytes but has " + buffer.capacity() + ".");
        }
    }

    private static void ensureLibraryLoaded(String configuredPath) {
        if (libraryLoaded) {
            return;
        }
        synchronized (LIBRARY_LOAD_LOCK) {
            if (libraryLoaded) {
                return;
            }
            if (libraryLoadFailure != null) {
                throw new IllegalStateException(
                        "Native request-plane JNI previously failed to load.", libraryLoadFailure);
            }
            try {
                if (configuredPath == null || configuredPath.trim().isEmpty()) {
                    System.loadLibrary(LIBRARY_NAME);
                } else {
                    Path path = Paths.get(configuredPath);
                    if (!path.isAbsolute()) {
                        throw new IllegalArgumentException(
                                LIBRARY_PATH_PROPERTY + " must be an absolute path.");
                    }
                    System.load(path.toString());
                }
                libraryLoaded = true;
            } catch (LinkageError | RuntimeException failure) {
                libraryLoadFailure = failure;
                throw new IllegalStateException(
                        "Native request-plane JNI failed to load; no fallback was used.", failure);
            }
        }
    }

    private static void verifyNativeAbiCompatibility() {
        final int actualVersion;
        try {
            actualVersion = nativeAbiVersion();
        } catch (LinkageError failure) {
            throw new IllegalStateException(
                    "Native request-plane JNI does not expose the required ABI handshake.",
                    failure);
        }
        if (actualVersion != EXPECTED_JNI_ABI_VERSION) {
            throw new IllegalStateException(
                    "Native request-plane JNI ABI mismatch: expected "
                            + EXPECTED_JNI_ABI_VERSION
                            + " but loaded "
                            + actualVersion
                            + ".");
        }
    }

    private static native int nativeAbiVersion();

    private static native long nativeCreateV3(
            int capacityEntries,
            int maxBatchEntries,
            long keyArenaBytes,
            long valueArenaBytes,
            int kernelPreference);

    private static native void nativeDestroy(long handle);

    private static native int nativeFill(
            long handle,
            ByteBuffer keyArena,
            ByteBuffer keyMetadata,
            int count,
            ByteBuffer valueArena,
            ByteBuffer valueMetadata,
            ByteBuffer fillResults);

    private static native int nativeProbe(
            long handle,
            ByteBuffer keyArena,
            ByteBuffer keyMetadata,
            int count,
            ByteBuffer valueOutput,
            ByteBuffer probeResults);

    private static native int nativeCompact(
            long handle,
            ByteBuffer keyArena,
            ByteBuffer keyMetadata,
            int count,
            ByteBuffer uniqueSourceIndexes);

    private static native int nativeGroup(
            long handle,
            ByteBuffer keyArena,
            ByteBuffer keyMetadata,
            int count,
            ByteBuffer uniqueSourceIndexes,
            ByteBuffer sourceGroupIndexes);

    private static native int nativeGroupTokensV3(
            long handle, ByteBuffer tokens, int count, ByteBuffer packedPlan);

    private static native String nativeKernelName(long handle);

    private static native long nativeFeatureBits(long handle);
}
