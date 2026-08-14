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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Reusable direct-memory batch of serialized key/namespace pairs.
 *
 * <p>Each append writes either serialized key/namespace bytes or an exact already-prepared
 * RocksDB composite key into a direct arena. A separate direct metadata region contains
 * fixed-width records suitable for one JNI request.
 *
 * <p>The caller-owned buffers' positions and limits are never changed. The batch is not
 * thread-safe, stores no key/namespace references, and creates no per-entry wrapper, list, or byte
 * array.
 */
@Internal
public final class SerializedKeyBatch<K, N> {

    /** Serializes one exact key directly into the reusable direct arena. */
    @FunctionalInterface
    public interface DirectKeyWriter {
        void write(DirectBufferDataOutputView output) throws IOException;
    }

    /** Serializes one indexed exact key directly into the reusable arena. */
    @FunctionalInterface
    public interface IndexedDirectKeyWriter {
        void write(int index, DirectBufferDataOutputView output) throws IOException;
    }

    public static final int STATE_ID_OFFSET = 0;
    public static final int RESERVED_OFFSET = 4;
    public static final int GENERATION_OFFSET = 8;
    public static final int ARENA_OFFSET_OFFSET = 16;
    public static final int LENGTH_OFFSET = 20;
    public static final int METADATA_RECORD_BYTES = 24;
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
        this.keySerializer =
                Objects.requireNonNull(keySerializer, "keySerializer").duplicate();
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
            metadata.putInt(metadataBase + RESERVED_OFFSET, 0);
            metadata.putLong(metadataBase + GENERATION_OFFSET, generation);
            metadata.putInt(metadataBase + ARENA_OFFSET_OFFSET, arenaCheckpoint);
            metadata.putInt(metadataBase + LENGTH_OFFSET, serializedLength);

            return entryCount++;
        } catch (IOException | RuntimeException failure) {
            arenaOutput.truncateTo(arenaCheckpoint);
            throw failure;
        }
    }

    /**
     * Appends one already-prepared RocksDB composite key.
     *
     * <p>The exact prepared bytes include key-group and namespace encoding and are therefore the
     * authoritative identity used by both native lookup and the RocksDB fallback. This avoids a
     * second serializer pass and avoids ambiguous naked {@code key || namespace} boundaries.
     */
    public int appendSerialized(int stateId, long generation, byte[] serializedKey)
            throws IOException {
        Objects.requireNonNull(serializedKey, "serializedKey");
        if (entryCount >= maxEntries) {
            throw new EOFException(
                    "Direct metadata capacity is exhausted at " + entryCount + " entries.");
        }
        int arenaCheckpoint = arenaOutput.checkpoint();
        try {
            arenaOutput.write(serializedKey);
            return commitMetadata(
                    stateId, generation, arenaCheckpoint, serializedKey.length);
        } catch (IOException | RuntimeException failure) {
            arenaOutput.truncateTo(arenaCheckpoint);
            throw failure;
        }
    }

    /**
     * Appends one exact key without first materializing a heap {@code byte[]}.
     *
     * <p>The writer must emit the complete authoritative identity, including any key-group,
     * namespace, or state-specific separators required by its caller. A failed writer is
     * transactional: no metadata becomes visible and the arena position is restored.
     */
    public int appendSerialized(int stateId, long generation, DirectKeyWriter writer)
            throws IOException {
        Objects.requireNonNull(writer, "writer");
        if (entryCount >= maxEntries) {
            throw new EOFException(
                    "Direct metadata capacity is exhausted at " + entryCount + " entries.");
        }
        int arenaCheckpoint = arenaOutput.checkpoint();
        try {
            writer.write(arenaOutput);
            return commitMetadata(
                    stateId,
                    generation,
                    arenaCheckpoint,
                    arenaOutput.position() - arenaCheckpoint);
        } catch (IOException | RuntimeException failure) {
            arenaOutput.truncateTo(arenaCheckpoint);
            throw failure;
        }
    }

    /** Creates a raw-prepared-key batch whose object serializers are never consulted. */
    public static SerializedKeyBatch<byte[], byte[]> forSerializedBytes(
            ByteBuffer arena, ByteBuffer metadata) {
        return new SerializedKeyBatch<>(
                org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer
                        .INSTANCE,
                org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer
                        .INSTANCE,
                arena,
                metadata);
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

    public int arenaOffset(int entryIndex) {
        return metadata.getInt(metadataBase(entryIndex) + ARENA_OFFSET_OFFSET);
    }

    public int serializedLength(int entryIndex) {
        return metadata.getInt(metadataBase(entryIndex) + LENGTH_OFFSET);
    }

    /**
     * Returns a read-only, zero-position direct view of the serialized arena without copying.
     */
    public ByteBuffer arenaSlice() {
        return arenaOutput.writtenSlice();
    }

    /**
     * Returns a read-only, zero-position direct view of visible metadata without copying.
     */
    public ByteBuffer metadataSlice() {
        ByteBuffer visible = metadata.asReadOnlyBuffer().order(METADATA_BYTE_ORDER);
        visible.position(0);
        visible.limit(entryCount * METADATA_RECORD_BYTES);
        return visible.slice().asReadOnlyBuffer().order(METADATA_BYTE_ORDER);
    }

    private int metadataBase(int entryIndex) {
        if (entryIndex < 0 || entryIndex >= entryCount) {
            throw new IndexOutOfBoundsException(
                    "Entry index " + entryIndex + " outside [0, " + entryCount + ").");
        }
        return entryIndex * METADATA_RECORD_BYTES;
    }

    private int commitMetadata(
            int stateId, long generation, int arenaOffset, int serializedLength) {
        int metadataBase = entryCount * METADATA_RECORD_BYTES;
        metadata.putInt(metadataBase + STATE_ID_OFFSET, stateId);
        metadata.putInt(metadataBase + RESERVED_OFFSET, 0);
        metadata.putLong(metadataBase + GENERATION_OFFSET, generation);
        metadata.putInt(metadataBase + ARENA_OFFSET_OFFSET, arenaOffset);
        metadata.putInt(metadataBase + LENGTH_OFFSET, serializedLength);
        return entryCount++;
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
}
