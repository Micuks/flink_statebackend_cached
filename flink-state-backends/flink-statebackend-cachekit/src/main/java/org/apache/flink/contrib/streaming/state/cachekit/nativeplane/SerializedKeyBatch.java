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
import org.apache.flink.core.memory.DataOutputView;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Reusable direct-memory batch of serialized key/namespace pairs.
 *
 * <p>Each append writes key bytes immediately followed by namespace bytes into a direct arena. A
 * separate direct metadata region contains fixed-width records for one single-JNI direct arena
 * request. This class prepares input and publishes descriptor results; it does not own native
 * database handles.
 *
 * <p>The caller-owned buffers' positions and limits are never changed. The batch is not
 * thread-safe, stores no key/namespace references, and creates no per-entry wrapper, list, or byte
 * array.
 */
@Internal
public final class SerializedKeyBatch<K, N> {

    public static final int STATE_ID_OFFSET = 0;
    public static final int ORIGINAL_INDEX_OFFSET = 4;
    public static final int GENERATION_OFFSET = 8;
    public static final int ARENA_OFFSET_OFFSET = 16;
    public static final int LENGTH_OFFSET = 20;
    public static final int VALUE_OFFSET_OFFSET = 24;
    public static final int VALUE_LENGTH_OR_STATUS_OFFSET = 28;
    public static final int METADATA_RECORD_BYTES = 32;
    public static final int VALUE_NOT_FOUND = -1;
    public static final int VALUE_OVERFLOW = -2;
    public static final int VALUE_UNSET = Integer.MIN_VALUE;
    public static final ByteOrder METADATA_BYTE_ORDER = ByteOrder.nativeOrder();

    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final DirectBufferDataOutputView arenaOutput;
    private final ByteBuffer metadata;
    private final int maxEntries;

    private int entryCount;

    /**
     * Creates a batch over the remaining regions of two writable direct buffers.
     *
     * <p>Metadata uses native byte order because it is a transient JNI control plane, not persisted
     * Flink serialization. The region may contain trailing bytes smaller than one record; they
     * remain unused.
     */
    public SerializedKeyBatch(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            ByteBuffer arena,
            ByteBuffer metadata) {
        this.keySerializer = Objects.requireNonNull(keySerializer, "keySerializer").duplicate();
        this.namespaceSerializer =
                Objects.requireNonNull(namespaceSerializer, "namespaceSerializer").duplicate();
        this.arenaOutput = new DirectBufferDataOutputView(arena);
        this.metadata = directWritableSlice(metadata, "metadata");
        this.maxEntries = this.metadata.capacity() / METADATA_RECORD_BYTES;
    }

    /**
     * Appends one key/namespace pair and returns its zero-based metadata index.
     *
     * <p>If either serializer, arena capacity, or metadata capacity fails, arena position, metadata
     * visibility, and entry count remain at their pre-call values.
     */
    public int append(int stateId, long generation, K key, N namespace) throws IOException {
        return append(stateId, entryCount, generation, key, namespace);
    }

    /** Appends one serializer-defined key/namespace pair with an explicit stable result index. */
    public int append(int stateId, int originalIndex, long generation, K key, N namespace)
            throws IOException {
        if (entryCount >= maxEntries) {
            throw new EOFException(
                    "Direct metadata capacity is exhausted at " + entryCount + " entries.");
        }

        int arenaCheckpoint = arenaOutput.checkpoint();
        try {
            keySerializer.serialize(key, arenaOutput);
            namespaceSerializer.serialize(namespace, arenaOutput);

            int serializedLength = arenaOutput.position() - arenaCheckpoint;
            int metadataBase = entryCount * METADATA_RECORD_BYTES;
            metadata.putInt(metadataBase + STATE_ID_OFFSET, stateId);
            metadata.putInt(metadataBase + ORIGINAL_INDEX_OFFSET, originalIndex);
            metadata.putLong(metadataBase + GENERATION_OFFSET, generation);
            metadata.putInt(metadataBase + ARENA_OFFSET_OFFSET, arenaCheckpoint);
            metadata.putInt(metadataBase + LENGTH_OFFSET, serializedLength);
            metadata.putInt(metadataBase + VALUE_OFFSET_OFFSET, 0);
            metadata.putInt(metadataBase + VALUE_LENGTH_OR_STATUS_OFFSET, VALUE_UNSET);

            return entryCount++;
        } catch (IOException | RuntimeException failure) {
            arenaOutput.truncateTo(arenaCheckpoint);
            throw failure;
        }
    }

    /**
     * Appends an exact prepared key directly into the arena without a temporary byte array.
     *
     * <p>The writer receives a {@link DataOutputView} backed by the remaining direct arena.
     * Metadata and arena position are published only after the writer returns successfully.
     */
    public int appendPrepared(
            int stateId,
            int originalIndex,
            long generation,
            K key,
            N namespace,
            PreparedKeyWriter<K, N> writer)
            throws IOException {
        Objects.requireNonNull(writer, "writer");
        if (entryCount >= maxEntries) {
            throw new EOFException(
                    "Direct metadata capacity is exhausted at " + entryCount + " entries.");
        }

        int arenaCheckpoint = arenaOutput.checkpoint();
        try {
            writer.write(key, namespace, arenaOutput);
            int serializedLength = arenaOutput.position() - arenaCheckpoint;

            int metadataBase = entryCount * METADATA_RECORD_BYTES;
            metadata.putInt(metadataBase + STATE_ID_OFFSET, stateId);
            metadata.putInt(metadataBase + ORIGINAL_INDEX_OFFSET, originalIndex);
            metadata.putLong(metadataBase + GENERATION_OFFSET, generation);
            metadata.putInt(metadataBase + ARENA_OFFSET_OFFSET, arenaCheckpoint);
            metadata.putInt(metadataBase + LENGTH_OFFSET, serializedLength);
            metadata.putInt(metadataBase + VALUE_OFFSET_OFFSET, 0);
            metadata.putInt(metadataBase + VALUE_LENGTH_OR_STATUS_OFFSET, VALUE_UNSET);
            return entryCount++;
        } catch (IOException | RuntimeException failure) {
            arenaOutput.truncateTo(arenaCheckpoint);
            throw failure;
        }
    }

    /** Clears logical contents while retaining both direct buffers and serializer instances. */
    public void clear() {
        arenaOutput.reset();
        entryCount = 0;
    }

    public int entryCount() {
        return entryCount;
    }

    public int maxEntries() {
        return maxEntries;
    }

    public int arenaBytesWritten() {
        return arenaOutput.position();
    }

    public int arenaCapacityBytes() {
        return arenaOutput.capacity();
    }

    public int stateId(int entryIndex) {
        return metadata.getInt(metadataBase(entryIndex) + STATE_ID_OFFSET);
    }

    public long generation(int entryIndex) {
        return metadata.getLong(metadataBase(entryIndex) + GENERATION_OFFSET);
    }

    public int originalIndex(int entryIndex) {
        return metadata.getInt(metadataBase(entryIndex) + ORIGINAL_INDEX_OFFSET);
    }

    public int arenaOffset(int entryIndex) {
        return metadata.getInt(metadataBase(entryIndex) + ARENA_OFFSET_OFFSET);
    }

    public int serializedLength(int entryIndex) {
        return metadata.getInt(metadataBase(entryIndex) + LENGTH_OFFSET);
    }

    public int valueOffset(int entryIndex) {
        return metadata.getInt(metadataBase(entryIndex) + VALUE_OFFSET_OFFSET);
    }

    public void setValueOffset(int entryIndex, int valueOffset) {
        if (valueOffset < 0) {
            throw new IllegalArgumentException("Value offset must not be negative.");
        }
        metadata.putInt(metadataBase(entryIndex) + VALUE_OFFSET_OFFSET, valueOffset);
    }

    public int valueLengthOrStatus(int entryIndex) {
        return metadata.getInt(metadataBase(entryIndex) + VALUE_LENGTH_OR_STATUS_OFFSET);
    }

    public void setValueLengthOrStatus(int entryIndex, int valueLengthOrStatus) {
        if (valueLengthOrStatus < 0
                && valueLengthOrStatus != VALUE_NOT_FOUND
                && valueLengthOrStatus != VALUE_OVERFLOW
                && valueLengthOrStatus != VALUE_UNSET) {
            throw new IllegalArgumentException("Unknown value status: " + valueLengthOrStatus);
        }
        metadata.putInt(
                metadataBase(entryIndex) + VALUE_LENGTH_OR_STATUS_OFFSET, valueLengthOrStatus);
    }

    /** Returns a read-only, zero-position direct view of the serialized arena without copying. */
    public ByteBuffer arenaSlice() {
        return arenaOutput.writtenSlice();
    }

    /** Returns a read-only, zero-position direct view of visible metadata without copying. */
    public ByteBuffer metadataSlice() {
        ByteBuffer visible = metadata.asReadOnlyBuffer().order(METADATA_BYTE_ORDER);
        visible.position(0);
        visible.limit(entryCount * METADATA_RECORD_BYTES);
        return visible.slice().asReadOnlyBuffer().order(METADATA_BYTE_ORDER);
    }

    /** Returns a writable, zero-position direct descriptor view for the JNI result publication. */
    public ByteBuffer writableMetadataSlice() {
        ByteBuffer visible = metadata.duplicate().order(METADATA_BYTE_ORDER);
        visible.position(0);
        visible.limit(entryCount * METADATA_RECORD_BYTES);
        return visible.slice().order(METADATA_BYTE_ORDER);
    }

    private int metadataBase(int entryIndex) {
        if (entryIndex < 0 || entryIndex >= entryCount) {
            throw new IndexOutOfBoundsException(
                    "Entry index " + entryIndex + " outside [0, " + entryCount + ").");
        }
        return entryIndex * METADATA_RECORD_BYTES;
    }

    private static ByteBuffer directWritableSlice(ByteBuffer buffer, String name) {
        Objects.requireNonNull(buffer, name);
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException(name + " must be a direct ByteBuffer.");
        }
        if (buffer.isReadOnly()) {
            throw new IllegalArgumentException(name + " must be writable.");
        }
        return buffer.slice().order(METADATA_BYTE_ORDER);
    }

    @FunctionalInterface
    public interface PreparedKeyWriter<K, N> {
        void write(K key, N namespace, DataOutputView target) throws IOException;
    }
}
