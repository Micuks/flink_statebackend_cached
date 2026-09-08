/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Internal capability exposed by RocksDB {@code ValueState} for ordered batch reads.
 *
 * <p>Each result is the raw serialized value for the key and namespace at the same index. Missing
 * entries are represented by {@code null}. Implementations must preserve input order and list size.
 * Each non-null returned byte array is a caller-owned immutable snapshot: the implementation must
 * not reuse or mutate it after return, and callers that retain it must not mutate it.
 */
@Internal
public interface RocksDBBatchValueReader<K, N, V> {

    int DIRECT_ARENA_DESCRIPTOR_BYTES = 32;
    /** Legacy/default call geometry retained for backward-compatible experiment controls. */
    int DIRECT_ARENA_DEFAULT_BATCH = 64;
    /** Maximum count accepted by the extended FrocksDB direct-arena JNI ABI. */
    int DIRECT_ARENA_MAX_BATCH = 128;
    int DIRECT_ARENA_STATE_ID_OFFSET = 0;
    int DIRECT_ARENA_ORIGINAL_INDEX_OFFSET = 4;
    int DIRECT_ARENA_GENERATION_OFFSET = 8;
    int DIRECT_ARENA_KEY_OFFSET = 16;
    int DIRECT_ARENA_KEY_LENGTH_OFFSET = 20;
    int DIRECT_ARENA_VALUE_OFFSET = 24;
    int DIRECT_ARENA_RESULT_OFFSET = 28;
    int DIRECT_ARENA_NOT_FOUND = -1;
    int DIRECT_ARENA_OVERFLOW = -2;

    /**
     * Returns the state default value. Batch readers use this when RocksDB reports a missing key so
     * prefetch can cache the same value that {@code ValueState.value()} would return instead of
     * issuing a second point lookup. Callers must copy a non-null value before publishing it.
     */
    V getBatchDefaultValue();

    /** Optional synchronous mailbox-only write capability; null values mean deletes. */
    default boolean supportsSynchronousValueWriteBatch() {
        return false;
    }

    /**
     * Apply the complete ordered batch before returning, using the same value encoding and write
     * options as ordinary ValueState writes. No ownership is retained after return. Callers must
     * not publish clean cache entries until this operation succeeds.
     */
    default void writeSerializedValueBatch(List<byte[]> rocksDBKeys, List<V> values)
            throws Exception {
        throw new UnsupportedOperationException("Synchronous value write batch is unavailable.");
    }

    byte[] serializeBatchKeyAndNamespace(
            K key,
            N namespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer)
            throws Exception;

    /**
     * Serializes one exact RocksDB key directly into a caller-owned reusable output region.
     *
     * <p>The compatibility fallback copies the existing byte-array result. Implementations should
     * override this method when they can emit the composite key without an intermediate array.
     */
    default void serializeBatchKeyAndNamespace(
            K key,
            N namespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            PositionedDataOutputView output)
            throws Exception {
        output.write(
                serializeBatchKeyAndNamespace(
                        key, namespace, safeKeySerializer, safeNamespaceSerializer));
    }

    byte[] getSerializedValueByRocksDBKey(byte[] rocksDBKey) throws Exception;

    List<byte[]> getSerializedValuesByRocksDBKeys(
            List<byte[]> rocksDBKeys, int fromIndex, int toIndex) throws Exception;

    /**
     * Whether this reader can execute one RocksDB MultiGet directly from caller-owned arenas.
     *
     * <p>A true return only advertises the Java API. A missing native symbol may still surface as a
     * {@link LinkageError}; callers must then disable only this optional path and use the ordinary
     * authoritative reader.
     */
    default boolean supportsDirectArenaMultiGet() {
        return false;
    }

    /**
     * Maximum direct-arena batch supported by the loaded native library.
     *
     * <p>The legacy fail-closed value is 64. Implementations backed by an extended JNI library
     * should query its runtime capability instead of inferring it from Java constants.
     */
    default int directArenaMultiGetMaxBatch() {
        return DIRECT_ARENA_DEFAULT_BATCH;
    }

    /**
     * Executes one ordered RocksDB MultiGet using the 32-byte native-order descriptor ABI.
     *
     * <p>The first three descriptor fields are caller-owned. The final field is overwritten with
     * an exact value length, {@link #DIRECT_ARENA_NOT_FOUND}, or {@link #DIRECT_ARENA_OVERFLOW}.
     * If any entry overflows its fixed-size value slot, the implementation must leave the complete
     * value arena unchanged. Callers must therefore discard the entire direct result and fall back
     * for that chunk; consuming only non-overflow slots is forbidden.
     *
     * @return the number of present keys, including overflowed present values
     */
    default int getSerializedValuesByRocksDBKeyArena(
            ByteBuffer keyArena,
            ByteBuffer descriptors,
            int count,
            ByteBuffer valueArena,
            int valueStride)
            throws Exception {
        throw new UnsupportedOperationException("Direct-arena MultiGet is unavailable.");
    }

    List<byte[]> getSerializedValues(
            List<byte[]> serializedKeyAndNamespaces,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer)
            throws Exception;
}
