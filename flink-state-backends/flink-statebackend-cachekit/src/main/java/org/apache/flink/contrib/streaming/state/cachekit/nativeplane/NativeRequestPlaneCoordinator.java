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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Keyed-backend owner for one native request plane and a bounded set of direct batch slots.
 *
 * <p>Probe, fill, disable, and close are serialized because the native plane is single-owner.
 * A slot is leased before an async task is queued; inability to lease is an explicit Java-path
 * fallback rather than unbounded direct-memory allocation.
 */
@Internal
public final class NativeRequestPlaneCoordinator implements AutoCloseable {

    private final Object planeLock = new Object();
    private final NativeRequestPlaneOptions options;
    private final NativeRequestPlane plane;
    private final ArrayDeque<BatchSlot> availableSlots;
    private final BatchSlot mutationSlot;
    private final String selectedKernel;
    private final long detectedFeatureBits;

    private volatile boolean active = true;
    private volatile Throwable disableCause;
    private boolean planeClosed;
    private long leases;
    private long leaseMisses;
    private long probeCalls;
    private long fillCalls;

    public static NativeRequestPlaneCoordinator open(NativeRequestPlaneOptions options) {
        Objects.requireNonNull(options, "options");
        if (!options.enabled()) {
            return null;
        }
        NativeRequestPlaneBridge bridge =
                NativeRequestPlaneBridge.open(
                        true,
                        options.capacityEntries(),
                        options.keyArenaBytes(),
                        options.valueArenaBytes(),
                        options.kernelPreference(),
                        options.libraryPath());
        try {
            return new NativeRequestPlaneCoordinator(options, bridge);
        } catch (RuntimeException | Error failure) {
            try {
                bridge.close();
            } catch (RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    /** Test seam for deterministic probe/fill/failure and lifecycle tests without JNI. */
    public static NativeRequestPlaneCoordinator forTesting(
            NativeRequestPlaneOptions options, NativeRequestPlane plane) {
        if (!options.enabled()) {
            throw new IllegalArgumentException("Testing coordinator options must be enabled.");
        }
        Objects.requireNonNull(plane, "plane");
        try {
            return new NativeRequestPlaneCoordinator(options, plane);
        } catch (RuntimeException | Error failure) {
            try {
                plane.close();
            } catch (RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private NativeRequestPlaneCoordinator(
            NativeRequestPlaneOptions options, NativeRequestPlane plane) {
        this.options = Objects.requireNonNull(options, "options");
        this.plane = Objects.requireNonNull(plane, "plane");
        this.availableSlots = new ArrayDeque<>(options.batchSlots());
        for (int i = 0; i < options.batchSlots(); i++) {
            availableSlots.addLast(new BatchSlot(this, options));
        }
        this.mutationSlot = new BatchSlot(this, options);
        this.mutationSlot.markLeased();
        // Capture audit metadata during construction. If either JNI query fails, open() closes the
        // bridge before ownership can escape. These getters are thereafter non-JNI and cannot make
        // CacheKitKeyedStateBackend construction leak an already-open plane.
        this.selectedKernel =
                Objects.requireNonNull(plane.selectedKernel(), "plane.selectedKernel()");
        this.detectedFeatureBits = plane.detectedFeatureBits();
    }

    public NativeRequestPlaneOptions options() {
        return options;
    }

    public BatchSlot tryAcquireBatchSlot() {
        synchronized (availableSlots) {
            if (!active) {
                return null;
            }
            BatchSlot slot = availableSlots.pollFirst();
            if (slot == null) {
                leaseMisses++;
                return null;
            }
            slot.markLeased();
            leases++;
            return slot;
        }
    }

    public int probe(BatchSlot slot) {
        requireOwnedSlot(slot);
        synchronized (planeLock) {
            requireActive();
            try {
                int processed =
                        plane.probeBatch(
                                slot.preparedKeys, slot.probeValueOutput(), slot.probeResults());
                probeCalls++;
                return processed;
            } catch (RuntimeException | LinkageError failure) {
                disableLocked(failure);
                throw failure;
            }
        }
    }

    public int fill(BatchSlot slot) {
        requireOwnedSlot(slot);
        synchronized (planeLock) {
            requireActive();
            try {
                int processed =
                        plane.fillBatch(
                                slot.missKeys,
                                slot.fillValueArena(),
                                slot.fillValueMetadata(),
                                slot.fillResults());
                fillCalls++;
                return processed;
            } catch (RuntimeException | LinkageError failure) {
                disableLocked(failure);
                throw failure;
            }
        }
    }

    /**
     * Writes through one exact prepared key after its authoritative RocksDB mutation.
     *
     * <p>The dedicated slot cannot be exhausted by queued speculative batches. Insert/update and
     * stale-generation rejection are correctness-safe; every other result disables the whole plane
     * so a later latest-key probe can never observe an older value.
     */
    public int updateExactKey(
            int stateId, long generation, byte[] preparedRocksDBKey, byte[] serializedValue)
            throws IOException {
        synchronized (planeLock) {
            requireActive();
            try {
                mutationSlot.prepareSingleFill(
                        stateId, generation, preparedRocksDBKey, serializedValue);
                int processed =
                        plane.fillBatch(
                                mutationSlot.missKeys,
                                mutationSlot.fillValueArena(),
                                mutationSlot.fillValueMetadata(),
                                mutationSlot.fillResults());
                fillCalls++;
                if (processed != 1) {
                    throw new IllegalStateException(
                            "Native exact-key update processed " + processed + " of 1 entry.");
                }
                int status = mutationSlot.fillStatus(0);
                int error = mutationSlot.fillError(0);
                boolean applied =
                        error == NativeRequestPlaneBridge.ERROR_OK
                                && (status == NativeRequestPlaneBridge.FILL_INSERTED
                                        || status == NativeRequestPlaneBridge.FILL_UPDATED);
                boolean superseded =
                        error == NativeRequestPlaneBridge.ERROR_OK
                                && status
                                        == NativeRequestPlaneBridge
                                                .FILL_REJECTED_STALE_GENERATION;
                if (!applied && !superseded) {
                    throw new IllegalStateException(
                            "Native exact-key update returned status="
                                    + status
                                    + ", error="
                                    + error
                                    + ".");
                }
                return status;
            } catch (IOException failure) {
                disableLocked(failure);
                throw failure;
            } catch (RuntimeException | LinkageError failure) {
                disableLocked(failure);
                throw failure;
            }
        }
    }

    public boolean isActive() {
        return active;
    }

    public Throwable disableCause() {
        return disableCause;
    }

    public String selectedKernel() {
        return selectedKernel;
    }

    public long detectedFeatureBits() {
        return detectedFeatureBits;
    }

    /** Fixed-width audit form used by TaskManager startup and per-state close records. */
    public String detectedFeatureBitsHex() {
        return String.format(Locale.ROOT, "0x%016x", detectedFeatureBits);
    }

    /** Stable human-readable decode paired with {@link #detectedFeatureBitsHex()}. */
    public String detectedFeatures() {
        StringBuilder decoded = new StringBuilder();
        appendFeature(
                decoded,
                (detectedFeatureBits & NativeRequestPlaneBridge.FEATURE_AARCH64) != 0
                        ? "aarch64"
                        : "x86_64");
        if ((detectedFeatureBits & NativeRequestPlaneBridge.FEATURE_NEON) != 0) {
            appendFeature(decoded, "neon");
        }
        if ((detectedFeatureBits & NativeRequestPlaneBridge.FEATURE_CRC32) != 0) {
            appendFeature(decoded, "crc32");
        }
        if ((detectedFeatureBits & NativeRequestPlaneBridge.FEATURE_SVE) != 0) {
            appendFeature(decoded, "sve");
        }
        if ((detectedFeatureBits & NativeRequestPlaneBridge.FEATURE_SVE_VL256) != 0) {
            appendFeature(decoded, "vl256");
        }
        return decoded.toString();
    }

    private static void appendFeature(StringBuilder decoded, String feature) {
        if (decoded.length() != 0) {
            decoded.append('|');
        }
        decoded.append(feature);
    }

    public long leases() {
        return leases;
    }

    public long leaseMisses() {
        return leaseMisses;
    }

    public long probeCalls() {
        return probeCalls;
    }

    public long fillCalls() {
        return fillCalls;
    }

    public void disable(Throwable cause) {
        synchronized (planeLock) {
            disableLocked(Objects.requireNonNull(cause, "cause"));
        }
    }

    @Override
    public void close() {
        synchronized (planeLock) {
            if (planeClosed) {
                return;
            }
            active = false;
            disableCause = new IllegalStateException("Native request plane was closed.");
            planeClosed = true;
            plane.close();
        }
    }

    private void disableLocked(Throwable cause) {
        if (!active) {
            return;
        }
        active = false;
        disableCause = cause;
        planeClosed = true;
        plane.close();
    }

    private void requireActive() {
        if (!active) {
            throw new IllegalStateException("Native request plane is inactive.", disableCause);
        }
    }

    private void requireOwnedSlot(BatchSlot slot) {
        if (slot == null || slot.owner != this || !slot.leased) {
            throw new IllegalArgumentException("Batch slot is not leased from this coordinator.");
        }
    }

    private void release(BatchSlot slot) {
        requireOwnedSlot(slot);
        slot.reset();
        synchronized (availableSlots) {
            slot.leased = false;
            availableSlots.addLast(slot);
        }
    }

    /** One bounded direct-memory lease retained by a queued or executing prepared batch. */
    @Internal
    public static final class BatchSlot implements AutoCloseable {

        private final NativeRequestPlaneCoordinator owner;
        private final SerializedKeyBatch<byte[], byte[]> preparedKeys;
        private final SerializedKeyBatch<byte[], byte[]> missKeys;
        private final ByteBuffer probeValueOutput;
        private final ByteBuffer probeResults;
        private final ByteBuffer valueArena;
        private final ByteBuffer valueMetadata;
        private final ByteBuffer fillResults;

        private boolean leased;
        private int fillValueBytes;
        private long preparedFillGeneration;

        private BatchSlot(
                NativeRequestPlaneCoordinator owner, NativeRequestPlaneOptions options) {
            this.owner = owner;
            ByteBuffer preparedArena = ByteBuffer.allocateDirect(options.batchKeyArenaBytes());
            ByteBuffer preparedMetadata =
                    ByteBuffer.allocateDirect(
                            Math.multiplyExact(
                                    options.batchEntries(),
                                    SerializedKeyBatch.METADATA_RECORD_BYTES));
            ByteBuffer missArena = ByteBuffer.allocateDirect(options.batchKeyArenaBytes());
            ByteBuffer missMetadata =
                    ByteBuffer.allocateDirect(
                            Math.multiplyExact(
                                    options.batchEntries(),
                                    SerializedKeyBatch.METADATA_RECORD_BYTES));
            this.preparedKeys =
                    SerializedKeyBatch.forSerializedBytes(preparedArena, preparedMetadata);
            this.missKeys = SerializedKeyBatch.forSerializedBytes(missArena, missMetadata);
            this.probeValueOutput = ByteBuffer.allocateDirect(options.batchValueArenaBytes());
            this.probeResults =
                    ByteBuffer.allocateDirect(
                                    Math.multiplyExact(
                                            options.batchEntries(),
                                            NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES))
                            .order(ByteOrder.nativeOrder());
            this.valueArena = ByteBuffer.allocateDirect(options.batchValueArenaBytes());
            this.valueMetadata =
                    ByteBuffer.allocateDirect(
                                    Math.multiplyExact(
                                            options.batchEntries(),
                                            NativeRequestPlaneBridge.FILL_VALUE_RECORD_BYTES))
                            .order(ByteOrder.nativeOrder());
            this.fillResults =
                    ByteBuffer.allocateDirect(
                                    Math.multiplyExact(
                                            options.batchEntries(),
                                            NativeRequestPlaneBridge.FILL_RESULT_RECORD_BYTES))
                            .order(ByteOrder.nativeOrder());
        }

        public void prepareLatest(
                int stateId, long fillGeneration, List<byte[]> preparedRocksDBKeys)
                throws IOException {
            requireLeased();
            preparedKeys.clear();
            preparedFillGeneration = fillGeneration;
            if (preparedRocksDBKeys.size() > preparedKeys.maxEntries()) {
                throw new IOException(
                        "Prepared native batch has "
                                + preparedRocksDBKeys.size()
                                + " entries but slot capacity is "
                                + preparedKeys.maxEntries()
                                + ".");
            }
            for (byte[] key : preparedRocksDBKeys) {
                preparedKeys.appendSerialized(
                        stateId, NativeRequestPlaneBridge.PROBE_LATEST_GENERATION, key);
            }
        }

        public void prepareFill(
                int stateId,
                long generation,
                List<byte[]> compactMissKeys,
                List<byte[]> compactMissValues)
                throws IOException {
            requireLeased();
            if (compactMissKeys.size() != compactMissValues.size()) {
                throw new IllegalArgumentException("Miss key/value counts differ.");
            }
            missKeys.clear();
            valueArena.clear();
            fillValueBytes = 0;
            for (int i = 0; i < compactMissKeys.size(); i++) {
                missKeys.appendSerialized(stateId, generation, compactMissKeys.get(i));
                byte[] value = compactMissValues.get(i);
                int metadataBase = i * NativeRequestPlaneBridge.FILL_VALUE_RECORD_BYTES;
                if (value == null) {
                    valueMetadata.putInt(
                            metadataBase + NativeRequestPlaneBridge.FILL_VALUE_ARENA_OFFSET, 0);
                    valueMetadata.putInt(
                            metadataBase + NativeRequestPlaneBridge.FILL_VALUE_LENGTH_OFFSET, 0);
                    valueMetadata.putInt(
                            metadataBase + NativeRequestPlaneBridge.FILL_VALUE_FLAGS_OFFSET,
                            NativeRequestPlaneBridge.FILL_VALUE_NEGATIVE_FLAG);
                } else {
                    if (value.length > valueArena.remaining()) {
                        throw new IOException(
                                "Native fill value arena exhausted at "
                                        + fillValueBytes
                                        + " bytes.");
                    }
                    valueMetadata.putInt(
                            metadataBase + NativeRequestPlaneBridge.FILL_VALUE_ARENA_OFFSET,
                            fillValueBytes);
                    valueMetadata.putInt(
                            metadataBase + NativeRequestPlaneBridge.FILL_VALUE_LENGTH_OFFSET,
                            value.length);
                    valueMetadata.putInt(
                            metadataBase + NativeRequestPlaneBridge.FILL_VALUE_FLAGS_OFFSET, 0);
                    valueArena.put(value);
                    fillValueBytes += value.length;
                }
                valueMetadata.putInt(
                        metadataBase + NativeRequestPlaneBridge.FILL_VALUE_RESERVED_OFFSET, 0);
            }
        }

        public int preparedEntryCount() {
            return preparedKeys.entryCount();
        }

        public int missEntryCount() {
            return missKeys.entryCount();
        }

        public long preparedGeneration() {
            requireLeased();
            if (preparedKeys.entryCount() == 0) {
                throw new IllegalStateException("Prepared batch is empty.");
            }
            return preparedFillGeneration;
        }

        public int probeStatus(int index) {
            checkPreparedIndex(index);
            return probeResults
                    .duplicate()
                    .order(ByteOrder.nativeOrder())
                    .getInt(
                            index * NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES
                                    + NativeRequestPlaneBridge.PROBE_RESULT_STATUS_OFFSET);
        }

        public int probeError(int index) {
            checkPreparedIndex(index);
            return probeResults
                    .duplicate()
                    .order(ByteOrder.nativeOrder())
                    .getInt(
                            index * NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES
                                    + NativeRequestPlaneBridge.PROBE_RESULT_ERROR_OFFSET);
        }

        public byte[] copyProbeValue(int index) {
            checkPreparedIndex(index);
            ByteBuffer results = probeResults.duplicate().order(ByteOrder.nativeOrder());
            int base = index * NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES;
            int offset =
                    results.getInt(base + NativeRequestPlaneBridge.PROBE_RESULT_ARENA_OFFSET);
            int length = results.getInt(base + NativeRequestPlaneBridge.PROBE_RESULT_LENGTH_OFFSET);
            if (offset < 0 || length < 0 || offset > probeValueOutput.capacity() - length) {
                throw new IllegalStateException("Native probe returned an invalid value slice.");
            }
            byte[] copy = new byte[length];
            ByteBuffer source = probeValueOutput.duplicate();
            source.position(offset);
            source.get(copy);
            return copy;
        }

        public int fillStatus(int index) {
            checkMissIndex(index);
            return fillResults
                    .duplicate()
                    .order(ByteOrder.nativeOrder())
                    .getInt(
                            index * NativeRequestPlaneBridge.FILL_RESULT_RECORD_BYTES
                                    + NativeRequestPlaneBridge.FILL_RESULT_STATUS_OFFSET);
        }

        public int fillError(int index) {
            checkMissIndex(index);
            return fillResults
                    .duplicate()
                    .order(ByteOrder.nativeOrder())
                    .getInt(
                            index * NativeRequestPlaneBridge.FILL_RESULT_RECORD_BYTES
                                    + NativeRequestPlaneBridge.FILL_RESULT_ERROR_OFFSET);
        }

        @Override
        public synchronized void close() {
            if (!leased) {
                return;
            }
            owner.release(this);
        }

        private ByteBuffer probeValueOutput() {
            ByteBuffer output = probeValueOutput.duplicate();
            output.clear();
            return output;
        }

        private ByteBuffer probeResults() {
            ByteBuffer results = probeResults.duplicate().order(ByteOrder.nativeOrder());
            results.position(0);
            results.limit(
                    preparedKeys.entryCount()
                            * NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES);
            return results.slice().order(ByteOrder.nativeOrder());
        }

        private ByteBuffer fillValueArena() {
            ByteBuffer arena = valueArena.duplicate();
            arena.position(0);
            arena.limit(fillValueBytes);
            return arena.slice();
        }

        private ByteBuffer fillValueMetadata() {
            ByteBuffer metadata = valueMetadata.duplicate().order(ByteOrder.nativeOrder());
            metadata.position(0);
            metadata.limit(
                    missKeys.entryCount() * NativeRequestPlaneBridge.FILL_VALUE_RECORD_BYTES);
            return metadata.slice().order(ByteOrder.nativeOrder());
        }

        private ByteBuffer fillResults() {
            ByteBuffer results = fillResults.duplicate().order(ByteOrder.nativeOrder());
            results.position(0);
            results.limit(
                    missKeys.entryCount() * NativeRequestPlaneBridge.FILL_RESULT_RECORD_BYTES);
            return results.slice().order(ByteOrder.nativeOrder());
        }

        private void markLeased() {
            if (leased) {
                throw new IllegalStateException("Native batch slot is already leased.");
            }
            leased = true;
        }

        private void prepareSingleFill(
                int stateId, long generation, byte[] preparedRocksDBKey, byte[] serializedValue)
                throws IOException {
            requireLeased();
            missKeys.clear();
            valueArena.clear();
            fillValueBytes = 0;
            missKeys.appendSerialized(stateId, generation, preparedRocksDBKey);
            putFillValueMetadata(0, serializedValue);
        }

        private void putFillValueMetadata(int index, byte[] value) throws IOException {
            int metadataBase = index * NativeRequestPlaneBridge.FILL_VALUE_RECORD_BYTES;
            if (value == null) {
                valueMetadata.putInt(
                        metadataBase + NativeRequestPlaneBridge.FILL_VALUE_ARENA_OFFSET, 0);
                valueMetadata.putInt(
                        metadataBase + NativeRequestPlaneBridge.FILL_VALUE_LENGTH_OFFSET, 0);
                valueMetadata.putInt(
                        metadataBase + NativeRequestPlaneBridge.FILL_VALUE_FLAGS_OFFSET,
                        NativeRequestPlaneBridge.FILL_VALUE_NEGATIVE_FLAG);
            } else {
                if (value.length > valueArena.remaining()) {
                    throw new IOException(
                            "Native fill value arena exhausted at " + fillValueBytes + " bytes.");
                }
                valueMetadata.putInt(
                        metadataBase + NativeRequestPlaneBridge.FILL_VALUE_ARENA_OFFSET,
                        fillValueBytes);
                valueMetadata.putInt(
                        metadataBase + NativeRequestPlaneBridge.FILL_VALUE_LENGTH_OFFSET,
                        value.length);
                valueMetadata.putInt(
                        metadataBase + NativeRequestPlaneBridge.FILL_VALUE_FLAGS_OFFSET, 0);
                valueArena.put(value);
                fillValueBytes += value.length;
            }
            valueMetadata.putInt(
                    metadataBase + NativeRequestPlaneBridge.FILL_VALUE_RESERVED_OFFSET, 0);
        }

        private void reset() {
            preparedKeys.clear();
            missKeys.clear();
            fillValueBytes = 0;
            preparedFillGeneration = 0;
        }

        private void requireLeased() {
            if (!leased) {
                throw new IllegalStateException("Native batch slot is not leased.");
            }
        }

        private void checkPreparedIndex(int index) {
            requireLeased();
            if (index < 0 || index >= preparedKeys.entryCount()) {
                throw new IndexOutOfBoundsException("Prepared entry index: " + index);
            }
        }

        private void checkMissIndex(int index) {
            requireLeased();
            if (index < 0 || index >= missKeys.entryCount()) {
                throw new IndexOutOfBoundsException("Miss entry index: " + index);
            }
        }
    }
}
