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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBDirectValueAccess;
import org.apache.flink.core.memory.MemorySegmentFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

/**
 * Reusable owner of one prepared-key to materialized-value transit batch.
 *
 * <p>Key, 32-byte descriptor, and fixed-stride value arenas are direct and 128-byte aligned. Four
 * descriptors therefore occupy exactly one Kunpeng L3 cache line. The envelope keeps logical key
 * copies only so a successfully fenced result can be installed into CacheKit; it never creates a
 * per-key byte array or collection wrapper.
 */
@Internal
public final class DirectStateEnvelope<K, N> {

    public static final int CACHE_LINE_BYTES = 128;

    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final AlignedDirectBuffer keyArena;
    private final AlignedDirectBuffer descriptors;
    private final AlignedDirectBuffer valueArena;
    private final SerializedKeyBatch<K, N> preparedKeys;
    private final Object[] logicalKeys;
    private final DirectBufferDataInputView valueInput;
    private final int maxEntries;
    private final int valueStride;

    private N namespaceSnapshot;
    private long generation;

    public DirectStateEnvelope(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            int maxEntries,
            int keyArenaBytes,
            int requestedValueStride) {
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be positive.");
        }
        if (maxEntries > RocksDBDirectValueAccess.MAX_BATCH_ENTRIES) {
            throw new IllegalArgumentException(
                    "maxEntries exceeds the first-version direct JNI limit of "
                            + RocksDBDirectValueAccess.MAX_BATCH_ENTRIES
                            + '.');
        }
        if (keyArenaBytes <= 0 || requestedValueStride <= 0) {
            throw new IllegalArgumentException("Arena bytes and value stride must be positive.");
        }
        this.keySerializer = Objects.requireNonNull(keySerializer, "keySerializer").duplicate();
        this.namespaceSerializer =
                Objects.requireNonNull(namespaceSerializer, "namespaceSerializer").duplicate();
        this.maxEntries = maxEntries;
        this.valueStride = roundUpCacheLine(requestedValueStride);
        this.keyArena = AlignedDirectBuffer.allocate(roundUpCacheLine(keyArenaBytes));
        this.descriptors =
                AlignedDirectBuffer.allocate(
                        roundUpCacheLine(
                                Math.multiplyExact(
                                        maxEntries, SerializedKeyBatch.METADATA_RECORD_BYTES)));
        this.valueArena =
                AlignedDirectBuffer.allocate(Math.multiplyExact(maxEntries, this.valueStride));
        this.preparedKeys =
                new SerializedKeyBatch<>(
                        this.keySerializer,
                        this.namespaceSerializer,
                        this.keyArena.view,
                        this.descriptors.view);
        this.logicalKeys = new Object[maxEntries];
        this.valueInput = new DirectBufferDataInputView(this.valueArena.view);
    }

    /** Starts a new generation and snapshots the common namespace. */
    public void begin(N namespace, long generation) {
        clear();
        this.namespaceSnapshot =
                namespaceSerializer.copy(Objects.requireNonNull(namespace, "namespace"));
        this.generation = generation;
    }

    /** Appends one exact RocksDB key and a serializer-safe logical key copy. */
    public int append(
            int stateId, K key, SerializedKeyBatch.PreparedKeyWriter<K, N> preparedKeyWriter)
            throws IOException {
        if (namespaceSnapshot == null) {
            throw new IllegalStateException("begin(namespace, generation) must be called first.");
        }
        if (entryCount() >= maxEntries) {
            throw new IllegalStateException("Direct state envelope is full.");
        }
        K keyCopy = keySerializer.copy(Objects.requireNonNull(key, "key"));
        int index =
                preparedKeys.appendPrepared(
                        stateId,
                        entryCount(),
                        generation,
                        key,
                        namespaceSnapshot,
                        preparedKeyWriter);
        preparedKeys.setValueOffset(index, Math.multiplyExact(index, valueStride));
        logicalKeys[index] = keyCopy;
        return index;
    }

    /** Executes the backend batch without exposing partially published values. */
    public int read(RocksDBDirectValueAccess<K, N> access) throws IOException {
        Objects.requireNonNull(access, "access");
        if (entryCount() == 0) {
            return 0;
        }
        return access.readValueBatch(
                preparedKeys.arenaSlice(),
                preparedKeys.writableMetadataSlice(),
                entryCount(),
                writableValueSlice(),
                valueStride);
    }

    public boolean hasOverflow() {
        for (int index = 0; index < entryCount(); index++) {
            if (preparedKeys.valueLengthOrStatus(index) == SerializedKeyBatch.VALUE_OVERFLOW) {
                return true;
            }
        }
        return false;
    }

    /** Selects one present value for direct TypeSerializer materialization. */
    public DirectBufferDataInputView valueInput(int index) {
        int length = preparedKeys.valueLengthOrStatus(index);
        if (length < 0) {
            throw new IllegalStateException(
                    "Entry " + index + " has no materializable value: " + length);
        }
        valueInput.setRegion(preparedKeys.valueOffset(index), length);
        return valueInput;
    }

    @SuppressWarnings("unchecked")
    public K logicalKey(int index) {
        checkIndex(index);
        return (K) logicalKeys[index];
    }

    public N namespaceSnapshot() {
        if (namespaceSnapshot == null) {
            throw new IllegalStateException("Envelope is not active.");
        }
        return namespaceSnapshot;
    }

    public long generation() {
        return generation;
    }

    public int entryCount() {
        return preparedKeys.entryCount();
    }

    public int maxEntries() {
        return maxEntries;
    }

    public int valueStride() {
        return valueStride;
    }

    public int valueLengthOrStatus(int index) {
        return preparedKeys.valueLengthOrStatus(index);
    }

    public int originalIndex(int index) {
        return preparedKeys.originalIndex(index);
    }

    public boolean usesOnlyAlignedArenas() {
        return keyArena.isAligned() && descriptors.isAligned() && valueArena.isAligned();
    }

    public ByteBuffer keyArenaView() {
        return preparedKeys.arenaSlice();
    }

    public ByteBuffer descriptorView() {
        return preparedKeys.metadataSlice();
    }

    public ByteBuffer writableDescriptorView() {
        return preparedKeys.writableMetadataSlice();
    }

    public ByteBuffer writableValueSlice() {
        ByteBuffer visible = valueArena.view.duplicate().order(ByteOrder.nativeOrder());
        visible.position(0);
        visible.limit(Math.multiplyExact(entryCount(), valueStride));
        return visible.slice().order(ByteOrder.nativeOrder());
    }

    /** Clears logical ownership while retaining all three native allocations. */
    public void clear() {
        Arrays.fill(logicalKeys, 0, preparedKeys.entryCount(), null);
        preparedKeys.clear();
        namespaceSnapshot = null;
        generation = 0L;
    }

    private void checkIndex(int index) {
        if (index < 0 || index >= entryCount()) {
            throw new IndexOutOfBoundsException(
                    "Entry index " + index + " outside [0, " + entryCount() + ").");
        }
    }

    private static int roundUpCacheLine(int bytes) {
        return Math.multiplyExact(
                Math.floorDiv(Math.addExact(bytes, CACHE_LINE_BYTES - 1), CACHE_LINE_BYTES),
                CACHE_LINE_BYTES);
    }

    private static final class AlignedDirectBuffer {
        private final ByteBuffer owner;
        private final ByteBuffer view;
        private final long address;

        private AlignedDirectBuffer(ByteBuffer owner, ByteBuffer view, long address) {
            this.owner = owner;
            this.view = view;
            this.address = address;
        }

        private static AlignedDirectBuffer allocate(int capacity) {
            ByteBuffer owner =
                    ByteBuffer.allocateDirect(Math.addExact(capacity, CACHE_LINE_BYTES - 1));
            long ownerAddress = MemorySegmentFactory.wrapOffHeapMemory(owner).getAddress();
            int padding =
                    (int)
                            ((CACHE_LINE_BYTES - (ownerAddress & (CACHE_LINE_BYTES - 1)))
                                    & (CACHE_LINE_BYTES - 1));
            ByteBuffer aligned = owner.duplicate();
            aligned.position(padding);
            aligned.limit(padding + capacity);
            ByteBuffer view = aligned.slice().order(ByteOrder.nativeOrder());
            long address = MemorySegmentFactory.wrapOffHeapMemory(view).getAddress();
            if ((address & (CACHE_LINE_BYTES - 1)) != 0) {
                throw new IllegalStateException("Failed to align direct arena.");
            }
            return new AlignedDirectBuffer(owner, view, address);
        }

        private boolean isAligned() {
            // Touch owner so its Cleaner cannot run while a sliced view is in use.
            return owner.isDirect() && (address & (CACHE_LINE_BYTES - 1)) == 0;
        }
    }
}
