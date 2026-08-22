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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.annotation.Internal;
import org.apache.flink.contrib.streaming.state.RocksDBBatchValueReader;

/**
 * Keyed-backend owner for one native request plane and a bounded set of direct batch slots.
 *
 * <p>Probe, fill, disable, and close are serialized because the native plane is single-owner. A
 * slot is leased before an async task is queued; inability to lease is an explicit Java-path
 * fallback rather than unbounded direct-memory allocation.
 */
@Internal
public final class NativeRequestPlaneCoordinator implements AutoCloseable {

    /** Serializes one non-negative fill value directly into the mutation slot's value arena. */
    @FunctionalInterface
    public interface DirectValueWriter {
        void write(DirectBufferDataOutputView output) throws IOException;
    }

    private final Object planeLock = new Object();
    private final NativeRequestPlaneOptions options;
    private final NativeRequestPlane plane;
    private final ArrayDeque<BatchSlot> availableSlots;
    private final BatchSlot mutationSlot;
    private final ConcurrentMap<Integer, ValueReadActivation> valueReadActivations =
            new ConcurrentHashMap<>();
    private final String selectedKernel;
    private final long detectedFeatureBits;

    private volatile boolean active = true;
    private volatile Throwable disableCause;
    private boolean planeClosed;
    private long leases;
    private long leaseMisses;
    private long probeCalls;
    private long fillCalls;
    private long compactCalls;
    private long groupCalls;

    public static NativeRequestPlaneCoordinator open(NativeRequestPlaneOptions options) {
        Objects.requireNonNull(options, "options");
        if (!options.enabled()) {
            return null;
        }
        NativeRequestPlaneBridge bridge =
                NativeRequestPlaneBridge.open(
                        true,
                        options.capacityEntries(),
                        options.batchEntries(),
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
        this.mutationSlot = new BatchSlot(this, options, true);
        this.mutationSlot.markLeased();
        // Capture audit metadata during construction. If either JNI query fails, open() closes the
        // bridge before ownership can escape. These getters are thereafter non-JNI and cannot make
        // CacheKitKeyedStateBackend construction leak an already-open plane.
        this.selectedKernel =
                Objects.requireNonNull(plane.selectedKernel(), "plane.selectedKernel()");
        this.detectedFeatureBits = plane.detectedFeatureBits();
        if (options.aarch64Only()
                && (detectedFeatureBits & NativeRequestPlaneBridge.FEATURE_AARCH64) == 0) {
            throw new IllegalStateException(
                    "Native request plane is AArch64-only by policy, but the loaded JNI library "
                            + "reported host features "
                            + detectedFeatureBitsHex()
                            + ". Set state.backend.cachekit.native.request-plane.aarch64-only=false "
                            + "only for an explicit portable x86 comparison.");
        }
    }

    public NativeRequestPlaneOptions options() {
        return options;
    }

    /**
     * Returns the backend-lifetime activation token for one native state id.
     *
     * <p>The production backend assigns each writable wrapper an exclusive state id. Tests may
     * attach read-only observers to the same token, but multiple writable wrappers would also need
     * to share their generation clock and are deliberately outside this contract.
     */
    public ValueReadActivation valueReadActivation(int stateId) {
        if (stateId <= 0) {
            throw new IllegalArgumentException("Native state id must be positive: " + stateId);
        }
        return valueReadActivations.computeIfAbsent(stateId, ignored -> new ValueReadActivation());
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

    public int compact(BatchSlot slot) {
        requireOwnedSlot(slot);
        synchronized (planeLock) {
            requireActive();
            try {
                int unique = plane.compactBatch(slot.preparedKeys, slot.uniqueSourceIndexes());
                validateCompactedSources(slot, unique);
                compactCalls++;
                slot.compactedEntryCount = unique;
                return unique;
            } catch (RuntimeException | LinkageError failure) {
                disableLocked(failure);
                throw failure;
            }
        }
    }

    private static void validateCompactedSources(BatchSlot slot, int unique) {
        int inputCount = slot.preparedKeys.entryCount();
        if (unique < (inputCount == 0 ? 0 : 1) || unique > inputCount) {
            throw new IllegalStateException(
                    "Native compact returned invalid unique count "
                            + unique
                            + " for "
                            + inputCount
                            + " entries.");
        }
        ByteBuffer indexes = slot.uniqueSourceIndexes.duplicate().order(ByteOrder.nativeOrder());
        int previous = -1;
        for (int target = 0; target < unique; target++) {
            int source = indexes.getInt(target * Integer.BYTES);
            if (source <= previous || source < target || source >= inputCount) {
                throw new IllegalStateException(
                        "Native compact returned invalid source index "
                                + source
                                + " at compacted index "
                                + target
                                + ".");
            }
            previous = source;
        }
    }

    public int group(BatchSlot slot) {
        requireOwnedSlot(slot);
        synchronized (planeLock) {
            requireActive();
            try {
                int unique =
                        plane.groupBatch(
                                slot.preparedKeys,
                                slot.uniqueSourceIndexes(),
                                slot.sourceGroupIndexes());
                groupCalls++;
                slot.compactedEntryCount = unique;
                return unique;
            } catch (RuntimeException | LinkageError failure) {
                disableLocked(failure);
                throw failure;
            }
        }
    }

    /** Groups caller-owned 32-bit hash tokens without constructing generic key metadata. */
    public int groupHashTokens(ByteBuffer tokens, int count, ByteBuffer packedPlan) {
        synchronized (planeLock) {
            requireActive();
            try {
                int unique = plane.groupHashTokens(tokens, count, packedPlan);
                if (unique <= 0 || unique > count) {
                    throw new IllegalStateException(
                            "Native token grouping returned invalid group count " + unique);
                }
                groupCalls++;
                return unique;
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
                return fillPreparedMutation();
            } catch (IOException failure) {
                disableLocked(failure);
                throw failure;
            } catch (RuntimeException | LinkageError failure) {
                disableLocked(failure);
                throw failure;
            }
        }
    }

    /**
     * Writes one exact key/value pair directly into the reusable mutation slot.
     *
     * <p>A {@code null} value writer denotes a negative entry. Both writers are invoked while the
     * plane lock is held, and a serialization failure publishes no fill to the native plane.
     */
    public int updateExactKey(
            int stateId,
            long generation,
            SerializedKeyBatch.DirectKeyWriter directKeyWriter,
            DirectValueWriter directValueWriter)
            throws IOException {
        synchronized (planeLock) {
            requireActive();
            try {
                mutationSlot.prepareSingleFill(
                        stateId, generation, directKeyWriter, directValueWriter);
                return fillPreparedMutation();
            } catch (IOException failure) {
                disableLocked(failure);
                throw failure;
            } catch (RuntimeException | LinkageError failure) {
                disableLocked(failure);
                throw failure;
            }
        }
    }

    /**
     * Advances the state watermark and updates one exact key only when it is already resident.
     *
     * <p>The membership check, conditional value serialization, and update are serialized under
     * the plane lock, so no native probe can interleave between the check and update. The earlier
     * interval from the authoritative RocksDB mutation to this method is protected by mailbox
     * serialization and by the generation revalidation on asynchronous reads.
     */
    public int updateExactKeyIfPresent(
            int stateId,
            long generation,
            SerializedKeyBatch.DirectKeyWriter directKeyWriter,
            DirectValueWriter directValueWriter)
            throws IOException {
        synchronized (planeLock) {
            requireActive();
            try {
                mutationSlot.prepareSingleMutationCheck(
                        stateId, generation, directKeyWriter);
                int checkStatus = fillPreparedMutationControl();
                if (checkStatus != NativeRequestPlaneBridge.FILL_UPDATED) {
                    return checkStatus;
                }
                mutationSlot.prepareConditionalUpdateForPreparedKey(directValueWriter);
                return fillPreparedConditionalMutation();
            } catch (IOException failure) {
                disableLocked(failure);
                throw failure;
            } catch (RuntimeException | LinkageError failure) {
                disableLocked(failure);
                throw failure;
            }
        }
    }

    /**
     * Advances one state's generation fence and checks a bounded exact-key vector in JNI batches.
     *
     * <p>The returned status vector is aligned with {@code preparedRocksDBKeys}. A {@code null}
     * return means no bounded slot was available before any native call; callers may safely fall
     * back to the established per-mutation path. Once a slot is acquired, every native error fails
     * closed and disables the request plane.
     */
    public int[] tryCheckExactKeysPresent(
            int stateId, long generation, List<byte[]> preparedRocksDBKeys) throws IOException {
        Objects.requireNonNull(preparedRocksDBKeys, "preparedRocksDBKeys");
        if (preparedRocksDBKeys.isEmpty()) {
            return new int[0];
        }
        BatchSlot slot = tryAcquireBatchSlot();
        if (slot == null) {
            return null;
        }
        try (BatchSlot ignored = slot) {
            int[] statuses = new int[preparedRocksDBKeys.size()];
            int from = 0;
            while (from < preparedRocksDBKeys.size()) {
                int to = boundedMutationChunkEnd(preparedRocksDBKeys, null, from);
                slot.prepareMutationChecks(
                        stateId, generation, preparedRocksDBKeys, from, to);
                fillAndCopyMutationStatuses(slot, statuses, from, to);
                from = to;
            }
            return statuses;
        } catch (IOException failure) {
            disable(failure);
            throw failure;
        } catch (RuntimeException | LinkageError failure) {
            disable(failure);
            throw failure;
        }
    }

    /**
     * Publishes an aligned exact-key/value vector with update-only semantics.
     *
     * <p>Absent or concurrently evicted entries remain absent; this method never inserts or
     * resurrects a cache entry. The generation fence must already have been advanced by {@link
     * #tryCheckExactKeysPresent(int, long, List)}.
     */
    public int[] updateExactKeysIfPresent(
            int stateId,
            long generation,
            List<byte[]> preparedRocksDBKeys,
            List<byte[]> serializedValues)
            throws IOException {
        Objects.requireNonNull(preparedRocksDBKeys, "preparedRocksDBKeys");
        Objects.requireNonNull(serializedValues, "serializedValues");
        if (preparedRocksDBKeys.size() != serializedValues.size()) {
            throw new IllegalArgumentException("Native mutation key/value vectors must align.");
        }
        if (preparedRocksDBKeys.isEmpty()) {
            return new int[0];
        }
        BatchSlot slot = tryAcquireBatchSlot();
        if (slot == null) {
            int[] statuses = new int[preparedRocksDBKeys.size()];
            for (int index = 0; index < statuses.length; index++) {
                statuses[index] =
                        updateExactKeyKnownPresent(
                                stateId,
                                generation,
                                preparedRocksDBKeys.get(index),
                                serializedValues.get(index));
            }
            return statuses;
        }
        try (BatchSlot ignored = slot) {
            int[] statuses = new int[preparedRocksDBKeys.size()];
            int from = 0;
            while (from < preparedRocksDBKeys.size()) {
                int to = boundedMutationChunkEnd(preparedRocksDBKeys, serializedValues, from);
                slot.prepareConditionalMutationUpdates(
                        stateId,
                        generation,
                        preparedRocksDBKeys,
                        serializedValues,
                        from,
                        to);
                fillAndCopyMutationStatuses(slot, statuses, from, to);
                from = to;
            }
            return statuses;
        } catch (IOException failure) {
            disable(failure);
            throw failure;
        } catch (RuntimeException | LinkageError failure) {
            disable(failure);
            throw failure;
        }
    }

    private int updateExactKeyKnownPresent(
            int stateId, long generation, byte[] preparedRocksDBKey, byte[] serializedValue)
            throws IOException {
        synchronized (planeLock) {
            requireActive();
            try {
                mutationSlot.prepareSingleConditionalUpdate(
                        stateId, generation, preparedRocksDBKey, serializedValue);
                return fillPreparedConditionalMutation();
            } catch (IOException failure) {
                disableLocked(failure);
                throw failure;
            } catch (RuntimeException | LinkageError failure) {
                disableLocked(failure);
                throw failure;
            }
        }
    }

    private int boundedMutationChunkEnd(
            List<byte[]> keys, List<byte[]> values, int fromIndex) throws IOException {
        int maxEntries = Math.min(options.batchEntries(), keys.size() - fromIndex);
        long keyBytes = 0;
        long valueBytes = 0;
        int count = 0;
        while (count < maxEntries) {
            byte[] key = Objects.requireNonNull(keys.get(fromIndex + count), "mutation key");
            byte[] value = values == null ? null : values.get(fromIndex + count);
            long nextKeyBytes = keyBytes + key.length;
            long nextValueBytes = valueBytes + (value == null ? 0 : value.length);
            if (count > 0
                    && (nextKeyBytes > options.batchKeyArenaBytes()
                            || nextValueBytes > options.batchValueArenaBytes())) {
                break;
            }
            if (nextKeyBytes > options.batchKeyArenaBytes()
                    || nextValueBytes > options.batchValueArenaBytes()) {
                throw new IOException("One native mutation exceeds the bounded batch arena.");
            }
            keyBytes = nextKeyBytes;
            valueBytes = nextValueBytes;
            count++;
        }
        return fromIndex + count;
    }

    private void fillAndCopyMutationStatuses(
            BatchSlot slot, int[] statuses, int fromIndex, int toIndex) {
        int expected = toIndex - fromIndex;
        int processed = fill(slot);
        if (processed != expected) {
            throw new IllegalStateException(
                    "Native resident mutation batch processed "
                            + processed
                            + " of "
                            + expected
                            + " entries.");
        }
        for (int local = 0; local < expected; local++) {
            int status = slot.fillStatus(local);
            int error = slot.fillError(local);
            boolean valid =
                    error == NativeRequestPlaneBridge.ERROR_OK
                            && (status == NativeRequestPlaneBridge.FILL_UPDATED
                                    || status == NativeRequestPlaneBridge.FILL_NOT_PRESENT
                                    || status
                                            == NativeRequestPlaneBridge
                                                    .FILL_REJECTED_STALE_GENERATION);
            if (!valid) {
                throw new IllegalStateException(
                        "Native resident mutation batch returned status="
                                + status
                                + ", error="
                                + error
                                + ".");
            }
            statuses[fromIndex + local] = status;
        }
    }

    private int fillPreparedMutation() {
        int status = fillPreparedMutationRaw();
        int error = mutationSlot.fillError(0);
        boolean applied =
                error == NativeRequestPlaneBridge.ERROR_OK
                        && (status == NativeRequestPlaneBridge.FILL_INSERTED
                                || status == NativeRequestPlaneBridge.FILL_UPDATED);
        boolean superseded =
                error == NativeRequestPlaneBridge.ERROR_OK
                        && status == NativeRequestPlaneBridge.FILL_REJECTED_STALE_GENERATION;
        if (!applied && !superseded) {
            throw new IllegalStateException(
                    "Native exact-key update returned status=" + status + ", error=" + error + ".");
        }
        return status;
    }

    private int fillPreparedMutationControl() {
        int status = fillPreparedMutationRaw();
        int error = mutationSlot.fillError(0);
        boolean valid =
                error == NativeRequestPlaneBridge.ERROR_OK
                        && (status == NativeRequestPlaneBridge.FILL_UPDATED
                                || status == NativeRequestPlaneBridge.FILL_NOT_PRESENT
                                || status
                                        == NativeRequestPlaneBridge
                                                .FILL_REJECTED_STALE_GENERATION);
        if (!valid) {
            throw new IllegalStateException(
                    "Native resident check returned status=" + status + ", error=" + error + ".");
        }
        return status;
    }

    private int fillPreparedConditionalMutation() {
        int status = fillPreparedMutationRaw();
        int error = mutationSlot.fillError(0);
        boolean valid =
                error == NativeRequestPlaneBridge.ERROR_OK
                        && (status == NativeRequestPlaneBridge.FILL_UPDATED
                                || status == NativeRequestPlaneBridge.FILL_NOT_PRESENT
                                || status
                                        == NativeRequestPlaneBridge
                                                .FILL_REJECTED_STALE_GENERATION);
        if (!valid) {
            throw new IllegalStateException(
                    "Native resident update returned status=" + status + ", error=" + error + ".");
        }
        return status;
    }

    private int fillPreparedMutationRaw() {
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
        return mutationSlot.fillStatus(0);
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

    public long compactCalls() {
        return compactCalls;
    }

    public long groupCalls() {
        return groupCalls;
    }

    long mutationSlotDirectBytesForTesting() {
        return mutationSlot.allocatedDirectBytes;
    }

    long regularSlotDirectBytesForTesting() {
        synchronized (availableSlots) {
            BatchSlot slot = availableSlots.peekFirst();
            if (slot == null) {
                throw new IllegalStateException("No regular native batch slot is available.");
            }
            return slot.allocatedDirectBytes;
        }
    }

    public void disable(Throwable cause) {
        synchronized (planeLock) {
            disableLocked(Objects.requireNonNull(cause, "cause"));
        }
    }

    @Override
    public void close() {
        // First serialize with any JNI caller and stop new leases/calls. Existing batch slots can
        // still be owned by queued or completing state tasks; destroying the plane before those
        // leases are returned leaves teardown correctness dependent on task timing.
        synchronized (planeLock) {
            if (planeClosed) {
                return;
            }
            active = false;
            disableCause = new IllegalStateException("Native request plane was closed.");
        }

        boolean interrupted = false;
        synchronized (availableSlots) {
            while (availableSlots.size() != options.batchSlots()) {
                try {
                    availableSlots.wait();
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
        }

        synchronized (planeLock) {
            if (!planeClosed) {
                planeClosed = true;
                plane.close();
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
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
            availableSlots.notifyAll();
        }
    }

    /** Backend-lifetime one-way gate used to preserve coherent read-activated write-through. */
    @Internal
    public static final class ValueReadActivation {
        private final AtomicBoolean active = new AtomicBoolean();

        public boolean activate() {
            return active.compareAndSet(false, true);
        }

        public boolean isActive() {
            return active.get();
        }
    }

    /** One bounded direct-memory lease retained by a queued or executing prepared batch. */
    @Internal
    public static final class BatchSlot implements AutoCloseable {

        private final NativeRequestPlaneCoordinator owner;
        private final SerializedKeyBatch<byte[], byte[]> preparedKeys;
        private final SerializedKeyBatch<byte[], byte[]> missKeys;
        private final ByteBuffer probeValueOutput;
        private final DirectBufferDataInputView probeValueInput;
        private final ByteBuffer probeResults;
        private final ByteBuffer valueArena;
        private final DirectBufferDataOutputView valueArenaOutput;
        private final ByteBuffer valueMetadata;
        private final ByteBuffer fillResults;
        private final ByteBuffer directMultiGetDescriptors;
        private final ByteBuffer uniqueSourceIndexes;
        private final ByteBuffer sourceGroupIndexes;
        private final long allocatedDirectBytes;

        private boolean leased;
        private int fillValueBytes;
        private long preparedFillGeneration;
        private int compactedEntryCount;
        private int directMultiGetCount;
        private int directMultiGetValueStride;

        private BatchSlot(NativeRequestPlaneCoordinator owner, NativeRequestPlaneOptions options) {
            this(owner, options, false);
        }

        private BatchSlot(
                NativeRequestPlaneCoordinator owner,
                NativeRequestPlaneOptions options,
                boolean mutationOnly) {
            this.owner = owner;
            int entries = mutationOnly ? 1 : options.batchEntries();
            int preparedArenaBytes = mutationOnly ? 0 : options.batchKeyArenaBytes();
            int preparedMetadataBytes =
                    mutationOnly
                            ? 0
                            : Math.multiplyExact(entries, SerializedKeyBatch.METADATA_RECORD_BYTES);
            int missArenaBytes = options.batchKeyArenaBytes();
            int missMetadataBytes =
                    Math.multiplyExact(entries, SerializedKeyBatch.METADATA_RECORD_BYTES);
            int probeValueBytes = mutationOnly ? 0 : options.batchValueArenaBytes();
            int probeResultBytes =
                    mutationOnly
                            ? 0
                            : Math.multiplyExact(
                                    entries, NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES);
            int valueArenaBytes = options.batchValueArenaBytes();
            int valueMetadataBytes =
                    Math.multiplyExact(entries, NativeRequestPlaneBridge.FILL_VALUE_RECORD_BYTES);
            int fillResultBytes =
                    Math.multiplyExact(entries, NativeRequestPlaneBridge.FILL_RESULT_RECORD_BYTES);
            int directMultiGetDescriptorBytes =
                    mutationOnly || !options.directArenaMultiGetEnabled()
                            ? 0
                            : Math.multiplyExact(
                                    RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH,
                                    RocksDBBatchValueReader.DIRECT_ARENA_DESCRIPTOR_BYTES);
            int uniqueIndexBytes = mutationOnly ? 0 : Math.multiplyExact(entries, Integer.BYTES);
            int groupIndexBytes = uniqueIndexBytes;

            ByteBuffer preparedArena = ByteBuffer.allocateDirect(preparedArenaBytes);
            ByteBuffer preparedMetadata = ByteBuffer.allocateDirect(preparedMetadataBytes);
            ByteBuffer missArena = ByteBuffer.allocateDirect(missArenaBytes);
            ByteBuffer missMetadata = ByteBuffer.allocateDirect(missMetadataBytes);
            this.preparedKeys =
                    SerializedKeyBatch.forSerializedBytes(preparedArena, preparedMetadata);
            this.missKeys = SerializedKeyBatch.forSerializedBytes(missArena, missMetadata);
            this.probeValueOutput = ByteBuffer.allocateDirect(probeValueBytes);
            this.probeValueInput = new DirectBufferDataInputView(probeValueOutput);
            this.probeResults =
                    ByteBuffer.allocateDirect(probeResultBytes).order(ByteOrder.nativeOrder());
            this.valueArena = ByteBuffer.allocateDirect(valueArenaBytes);
            this.valueArenaOutput = new DirectBufferDataOutputView(this.valueArena);
            this.valueMetadata =
                    ByteBuffer.allocateDirect(valueMetadataBytes).order(ByteOrder.nativeOrder());
            this.fillResults =
                    ByteBuffer.allocateDirect(fillResultBytes).order(ByteOrder.nativeOrder());
            this.directMultiGetDescriptors =
                    ByteBuffer.allocateDirect(directMultiGetDescriptorBytes)
                            .order(ByteOrder.nativeOrder());
            this.uniqueSourceIndexes =
                    ByteBuffer.allocateDirect(uniqueIndexBytes).order(ByteOrder.nativeOrder());
            this.sourceGroupIndexes =
                    ByteBuffer.allocateDirect(groupIndexBytes).order(ByteOrder.nativeOrder());
            this.allocatedDirectBytes =
                    (long) preparedArenaBytes
                            + preparedMetadataBytes
                            + missArenaBytes
                            + missMetadataBytes
                            + probeValueBytes
                            + probeResultBytes
                            + valueArenaBytes
                            + valueMetadataBytes
                            + fillResultBytes
                            + directMultiGetDescriptorBytes
                            + uniqueIndexBytes
                            + groupIndexBytes;
        }

        public void prepareLatest(
                int stateId, long fillGeneration, List<byte[]> preparedRocksDBKeys)
                throws IOException {
            requireLeased();
            preparedKeys.clear();
            compactedEntryCount = 0;
            preparedFillGeneration = fillGeneration;
            if (preparedRocksDBKeys.size() > preparedKeys.maxEntries()) {
                throw new IOException(
                        "Prepared native batch has "
                                + preparedRocksDBKeys.size()
                                + " entries but slot capacity is "
                                + preparedKeys.maxEntries()
                                + ".");
            }
            long probeGeneration =
                    owner.options.writeThroughMutations()
                            ? NativeRequestPlaneBridge.PROBE_LATEST_GENERATION
                            : fillGeneration;
            for (byte[] key : preparedRocksDBKeys) {
                preparedKeys.appendSerialized(stateId, probeGeneration, key);
            }
        }

        /** Prepares one exact key by serializing directly into this slot's direct arena. */
        public void prepareLatest(
                int stateId,
                long fillGeneration,
                SerializedKeyBatch.DirectKeyWriter directKeyWriter)
                throws IOException {
            requireLeased();
            preparedKeys.clear();
            compactedEntryCount = 0;
            preparedFillGeneration = fillGeneration;
            long probeGeneration =
                    owner.options.writeThroughMutations()
                            ? NativeRequestPlaneBridge.PROBE_LATEST_GENERATION
                            : fillGeneration;
            preparedKeys.appendSerialized(stateId, probeGeneration, directKeyWriter);
        }

        /**
         * Prepares one exact-generation key even when value-cache mutations use latest-key probes.
         *
         * <p>MapSnapshot invalidation relies on a generation mismatch producing MISS. It must not
         * observe an older EMPTY/SINGLE entry through the latest-generation sentinel.
         */
        public void prepareExact(
                int stateId,
                long generation,
                SerializedKeyBatch.DirectKeyWriter directKeyWriter)
                throws IOException {
            requireLeased();
            preparedKeys.clear();
            compactedEntryCount = 0;
            preparedFillGeneration = generation;
            preparedKeys.appendSerialized(stateId, generation, directKeyWriter);
        }

        /** Prepares an indexed exact-key batch without materializing per-key heap arrays. */
        public void prepareLatestDirect(
                int stateId,
                long fillGeneration,
                int count,
                SerializedKeyBatch.IndexedDirectKeyWriter directKeyWriter)
                throws IOException {
            requireLeased();
            if (count < 0 || count > preparedKeys.maxEntries()) {
                throw new IOException(
                        "Prepared native direct batch has "
                                + count
                                + " entries but slot capacity is "
                                + preparedKeys.maxEntries()
                                + ".");
            }
            preparedKeys.clear();
            compactedEntryCount = 0;
            preparedFillGeneration = fillGeneration;
            long probeGeneration =
                    owner.options.writeThroughMutations()
                            ? NativeRequestPlaneBridge.PROBE_LATEST_GENERATION
                            : fillGeneration;
            for (int index = 0; index < count; index++) {
                final int sourceIndex = index;
                int appended =
                        preparedKeys.appendSerialized(
                                stateId,
                                probeGeneration,
                                output -> directKeyWriter.write(sourceIndex, output));
                if (preparedKeys.serializedLength(appended) == 0) {
                    throw new IOException(
                            "Prepared native direct key at index " + index + " is empty.");
                }
            }
        }

        /** Copies one prepared exact key for an API that still requires a heap byte array. */
        public byte[] copyPreparedKey(int index) {
            requireLeased();
            int offset = preparedKeys.arenaOffset(index);
            int length = preparedKeys.serializedLength(index);
            byte[] copy = new byte[length];
            ByteBuffer source = preparedKeys.arenaSlice();
            source.position(offset);
            source.get(copy);
            return copy;
        }

        /**
         * Builds one bounded direct-arena MultiGet descriptor chunk over prepared keys.
         *
         * <p>The prepared key arena remains immutable. The value arena is divided into 64 fixed
         * slots regardless of the current chunk size, so changing the final chunk length cannot
         * change the overflow boundary.
         */
        public void prepareDirectArenaMultiGet(
                int[] preparedIndices, int fromIndex, int count) {
            requireLeased();
            Objects.requireNonNull(preparedIndices, "preparedIndices");
            if (fromIndex < 0
                    || count <= 0
                    || count > RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH
                    || fromIndex > preparedIndices.length - count) {
                throw new IllegalArgumentException("Invalid direct-arena MultiGet chunk.");
            }
            int stride =
                    valueArena.capacity() / RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH;
            if (stride <= 0) {
                throw new IllegalStateException(
                        "Native batch value arena is too small for direct MultiGet slots.");
            }
            directMultiGetDescriptors.clear();
            valueArena.clear();
            valueArenaOutput.reset();
            for (int target = 0; target < count; target++) {
                int source = preparedIndices[fromIndex + target];
                checkPreparedIndex(source);
                int base = target * RocksDBBatchValueReader.DIRECT_ARENA_DESCRIPTOR_BYTES;
                directMultiGetDescriptors.putInt(
                        base + RocksDBBatchValueReader.DIRECT_ARENA_STATE_ID_OFFSET,
                        preparedKeys.stateId(source));
                directMultiGetDescriptors.putInt(
                        base + RocksDBBatchValueReader.DIRECT_ARENA_ORIGINAL_INDEX_OFFSET, source);
                directMultiGetDescriptors.putLong(
                        base + RocksDBBatchValueReader.DIRECT_ARENA_GENERATION_OFFSET,
                        preparedKeys.generation(source));
                directMultiGetDescriptors.putInt(
                        base + RocksDBBatchValueReader.DIRECT_ARENA_KEY_OFFSET,
                        preparedKeys.arenaOffset(source));
                directMultiGetDescriptors.putInt(
                        base + RocksDBBatchValueReader.DIRECT_ARENA_KEY_LENGTH_OFFSET,
                        preparedKeys.serializedLength(source));
                directMultiGetDescriptors.putInt(
                        base + RocksDBBatchValueReader.DIRECT_ARENA_VALUE_OFFSET,
                        target * stride);
                directMultiGetDescriptors.putInt(
                        base + RocksDBBatchValueReader.DIRECT_ARENA_RESULT_OFFSET,
                        Integer.MIN_VALUE);
            }
            directMultiGetCount = count;
            directMultiGetValueStride = stride;
        }

        public ByteBuffer directMultiGetKeyArena() {
            requireDirectMultiGetPrepared();
            return preparedKeys.arenaSlice();
        }

        public ByteBuffer directMultiGetDescriptors() {
            requireDirectMultiGetPrepared();
            ByteBuffer descriptors =
                    directMultiGetDescriptors.duplicate().order(ByteOrder.nativeOrder());
            descriptors.position(0);
            descriptors.limit(
                    directMultiGetCount
                            * RocksDBBatchValueReader.DIRECT_ARENA_DESCRIPTOR_BYTES);
            return descriptors.slice().order(ByteOrder.nativeOrder());
        }

        public ByteBuffer directMultiGetValueArena() {
            requireDirectMultiGetPrepared();
            ByteBuffer values = valueArena.duplicate();
            values.position(0);
            values.limit(directMultiGetCount * directMultiGetValueStride);
            return values.slice();
        }

        public int directMultiGetValueStride() {
            requireDirectMultiGetPrepared();
            return directMultiGetValueStride;
        }

        public int directMultiGetResult(int index) {
            requireDirectMultiGetIndex(index);
            return directMultiGetDescriptors.getInt(
                    index * RocksDBBatchValueReader.DIRECT_ARENA_DESCRIPTOR_BYTES
                            + RocksDBBatchValueReader.DIRECT_ARENA_RESULT_OFFSET);
        }

        public byte[] copyDirectMultiGetValue(int index) {
            int length = directMultiGetResult(index);
            if (length < 0 || length > directMultiGetValueStride) {
                throw new IllegalStateException(
                        "Direct-arena result at " + index + " is not a present in-slot value.");
            }
            byte[] copy = new byte[length];
            if (length != 0) {
                ByteBuffer source = valueArena.duplicate();
                source.position(index * directMultiGetValueStride);
                source.get(copy);
            }
            return copy;
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

        /** Prepares native fill while copying miss keys direct-to-direct from the prepared arena. */
        public void prepareFillFromPreparedIndices(
                int stateId,
                long generation,
                int[] preparedIndices,
                int count,
                List<byte[]> compactMissValues)
                throws IOException {
            requireLeased();
            Objects.requireNonNull(preparedIndices, "preparedIndices");
            Objects.requireNonNull(compactMissValues, "compactMissValues");
            if (count < 0
                    || count > preparedIndices.length
                    || count != compactMissValues.size()) {
                throw new IllegalArgumentException("Prepared miss key/value counts differ.");
            }
            missKeys.clear();
            valueArena.clear();
            valueArenaOutput.reset();
            fillValueBytes = 0;
            ByteBuffer preparedArena = preparedKeys.arenaSlice();
            for (int index = 0; index < count; index++) {
                int source = preparedIndices[index];
                checkPreparedIndex(source);
                missKeys.appendSerialized(
                        stateId,
                        generation,
                        preparedArena,
                        preparedKeys.arenaOffset(source),
                        preparedKeys.serializedLength(source));
                putFillValueMetadata(index, compactMissValues.get(index));
            }
        }

        public int preparedEntryCount() {
            return preparedKeys.entryCount();
        }

        public int compactedSourceIndex(int compactedIndex) {
            requireLeased();
            if (compactedIndex < 0 || compactedIndex >= compactedEntryCount) {
                throw new IndexOutOfBoundsException(
                        "Compacted index "
                                + compactedIndex
                                + " outside [0, "
                                + compactedEntryCount
                                + ").");
            }
            return uniqueSourceIndexes.getInt(compactedIndex * Integer.BYTES);
        }

        /** Retains one compacted source index after Java reservation filtering. */
        public void retainCompactedSource(int compactedIndex, int retainedIndex) {
            requireLeased();
            if (compactedIndex < 0
                    || compactedIndex >= compactedEntryCount
                    || retainedIndex < 0
                    || retainedIndex > compactedIndex) {
                throw new IndexOutOfBoundsException(
                        "Cannot retain compacted index "
                                + compactedIndex
                                + " at "
                                + retainedIndex
                                + ".");
            }
            int source = uniqueSourceIndexes.getInt(compactedIndex * Integer.BYTES);
            uniqueSourceIndexes.putInt(retainedIndex * Integer.BYTES, source);
        }

        /**
         * Projects the prepared-key metadata onto the retained compacted sources.
         *
         * <p>The direct key arena is not copied. The selected metadata remains in stable first-seen
         * order and becomes the exact batch consumed by the following native probe.
         */
        public void projectRetainedCompactedSources(int retainedCount) {
            requireLeased();
            if (retainedCount < 0 || retainedCount > compactedEntryCount) {
                throw new IllegalArgumentException(
                        "Retained compacted count "
                                + retainedCount
                                + " outside [0, "
                                + compactedEntryCount
                                + "].");
            }
            preparedKeys.retainSerializedEntries(uniqueSourceIndexes, retainedCount);
            compactedEntryCount = retainedCount;
        }

        public int sourceGroupIndex(int sourceIndex) {
            requireLeased();
            if (sourceIndex < 0 || sourceIndex >= preparedKeys.entryCount()) {
                throw new IndexOutOfBoundsException(
                        "Source index "
                                + sourceIndex
                                + " outside [0, "
                                + preparedKeys.entryCount()
                                + ").");
            }
            return sourceGroupIndexes.getInt(sourceIndex * Integer.BYTES);
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
            int offset = results.getInt(base + NativeRequestPlaneBridge.PROBE_RESULT_ARENA_OFFSET);
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

        /**
         * Returns a reusable zero-copy input view over one positive probe value.
         *
         * <p>The view belongs to this slot and is invalidated by the next call or slot reuse. The
         * caller must deserialize it before releasing the slot.
         */
        public DirectBufferDataInputView probeValueInput(int index) {
            checkPreparedIndex(index);
            int base = index * NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES;
            int offset =
                    probeResults.getInt(base + NativeRequestPlaneBridge.PROBE_RESULT_ARENA_OFFSET);
            int length =
                    probeResults.getInt(base + NativeRequestPlaneBridge.PROBE_RESULT_LENGTH_OFFSET);
            if (offset < 0 || length < 0 || offset > probeValueOutput.capacity() - length) {
                throw new IllegalStateException("Native probe returned an invalid value slice.");
            }
            probeValueInput.reset(offset, length);
            return probeValueInput;
        }

        public int probeValueLength(int index) {
            checkPreparedIndex(index);
            int base = index * NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES;
            return probeResults.getInt(base + NativeRequestPlaneBridge.PROBE_RESULT_LENGTH_OFFSET);
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
                    preparedKeys.entryCount() * NativeRequestPlaneBridge.PROBE_RESULT_RECORD_BYTES);
            return results.slice().order(ByteOrder.nativeOrder());
        }

        private ByteBuffer uniqueSourceIndexes() {
            requireLeased();
            uniqueSourceIndexes.clear();
            return uniqueSourceIndexes;
        }

        private ByteBuffer sourceGroupIndexes() {
            sourceGroupIndexes.clear();
            return sourceGroupIndexes;
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
            valueArenaOutput.reset();
            fillValueBytes = 0;
            missKeys.appendSerialized(stateId, generation, preparedRocksDBKey);
            putFillValueMetadata(0, serializedValue);
        }

        private void prepareSingleFill(
                int stateId,
                long generation,
                SerializedKeyBatch.DirectKeyWriter directKeyWriter,
                DirectValueWriter directValueWriter)
                throws IOException {
            requireLeased();
            missKeys.clear();
            valueArena.clear();
            valueArenaOutput.reset();
            fillValueBytes = 0;
            missKeys.appendSerialized(stateId, generation, directKeyWriter);
            putFillValueMetadata(0, directValueWriter);
        }

        private void prepareSingleMutationCheck(
                int stateId,
                long generation,
                SerializedKeyBatch.DirectKeyWriter directKeyWriter)
                throws IOException {
            requireLeased();
            missKeys.clear();
            valueArena.clear();
            valueArenaOutput.reset();
            fillValueBytes = 0;
            missKeys.appendSerialized(stateId, generation, directKeyWriter);
            putFillValueMetadata(
                    0,
                    (DirectValueWriter) null,
                    NativeRequestPlaneBridge.FILL_VALUE_CHECK_ONLY_FLAG);
        }

        private void prepareMutationChecks(
                int stateId,
                long generation,
                List<byte[]> preparedRocksDBKeys,
                int fromIndex,
                int toIndex)
                throws IOException {
            requireLeased();
            missKeys.clear();
            valueArena.clear();
            valueArenaOutput.reset();
            fillValueBytes = 0;
            for (int index = fromIndex; index < toIndex; index++) {
                int local = index - fromIndex;
                missKeys.appendSerialized(
                        stateId, generation, preparedRocksDBKeys.get(index));
                putFillValueMetadata(
                        local,
                        (DirectValueWriter) null,
                        NativeRequestPlaneBridge.FILL_VALUE_CHECK_ONLY_FLAG);
            }
        }

        private void prepareConditionalMutationUpdates(
                int stateId,
                long generation,
                List<byte[]> preparedRocksDBKeys,
                List<byte[]> serializedValues,
                int fromIndex,
                int toIndex)
                throws IOException {
            requireLeased();
            missKeys.clear();
            valueArena.clear();
            valueArenaOutput.reset();
            fillValueBytes = 0;
            for (int index = fromIndex; index < toIndex; index++) {
                int local = index - fromIndex;
                missKeys.appendSerialized(
                        stateId, generation, preparedRocksDBKeys.get(index));
                byte[] value = serializedValues.get(index);
                putFillValueMetadata(
                        local,
                        value == null
                                ? null
                                : output -> output.write(value),
                        NativeRequestPlaneBridge.FILL_VALUE_UPDATE_ONLY_FLAG);
            }
        }

        private void prepareConditionalUpdateForPreparedKey(DirectValueWriter directValueWriter)
                throws IOException {
            requireLeased();
            if (missKeys.entryCount() != 1) {
                throw new IllegalStateException(
                        "Conditional native mutation requires exactly one prepared key.");
            }
            valueArena.clear();
            valueArenaOutput.reset();
            fillValueBytes = 0;
            putFillValueMetadata(
                    0,
                    directValueWriter,
                    NativeRequestPlaneBridge.FILL_VALUE_UPDATE_ONLY_FLAG);
        }

        private void prepareSingleConditionalUpdate(
                int stateId,
                long generation,
                byte[] preparedRocksDBKey,
                byte[] serializedValue)
                throws IOException {
            requireLeased();
            missKeys.clear();
            valueArena.clear();
            valueArenaOutput.reset();
            fillValueBytes = 0;
            missKeys.appendSerialized(stateId, generation, preparedRocksDBKey);
            putFillValueMetadata(
                    0,
                    serializedValue == null
                            ? null
                            : output -> output.write(serializedValue),
                    NativeRequestPlaneBridge.FILL_VALUE_UPDATE_ONLY_FLAG);
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

        private void putFillValueMetadata(int index, DirectValueWriter writer) throws IOException {
            putFillValueMetadata(index, writer, 0);
        }

        private void putFillValueMetadata(
                int index, DirectValueWriter writer, int controlFlags) throws IOException {
            int metadataBase = index * NativeRequestPlaneBridge.FILL_VALUE_RECORD_BYTES;
            if (writer == null) {
                valueMetadata.putInt(
                        metadataBase + NativeRequestPlaneBridge.FILL_VALUE_ARENA_OFFSET, 0);
                valueMetadata.putInt(
                        metadataBase + NativeRequestPlaneBridge.FILL_VALUE_LENGTH_OFFSET, 0);
                valueMetadata.putInt(
                        metadataBase + NativeRequestPlaneBridge.FILL_VALUE_FLAGS_OFFSET,
                        NativeRequestPlaneBridge.FILL_VALUE_NEGATIVE_FLAG);
            } else {
                int checkpoint = valueArenaOutput.checkpoint();
                try {
                    writer.write(valueArenaOutput);
                } catch (IOException | RuntimeException failure) {
                    valueArenaOutput.truncateTo(checkpoint);
                    throw failure;
                }
                int length = valueArenaOutput.position() - checkpoint;
                valueMetadata.putInt(
                        metadataBase + NativeRequestPlaneBridge.FILL_VALUE_ARENA_OFFSET,
                        checkpoint);
                valueMetadata.putInt(
                        metadataBase + NativeRequestPlaneBridge.FILL_VALUE_LENGTH_OFFSET, length);
                valueMetadata.putInt(
                        metadataBase + NativeRequestPlaneBridge.FILL_VALUE_FLAGS_OFFSET, 0);
                fillValueBytes = valueArenaOutput.position();
            }
            valueMetadata.putInt(
                    metadataBase + NativeRequestPlaneBridge.FILL_VALUE_RESERVED_OFFSET,
                    controlFlags);
        }

        private void reset() {
            preparedKeys.clear();
            missKeys.clear();
            fillValueBytes = 0;
            preparedFillGeneration = 0;
            compactedEntryCount = 0;
            directMultiGetCount = 0;
            directMultiGetValueStride = 0;
        }

        private void requireDirectMultiGetPrepared() {
            requireLeased();
            if (directMultiGetCount <= 0 || directMultiGetValueStride <= 0) {
                throw new IllegalStateException("Direct-arena MultiGet chunk is not prepared.");
            }
        }

        private void requireDirectMultiGetIndex(int index) {
            requireDirectMultiGetPrepared();
            if (index < 0 || index >= directMultiGetCount) {
                throw new IndexOutOfBoundsException("Direct-arena result index: " + index);
            }
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
