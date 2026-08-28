/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.annotation.Internal;

/** Internal capability exposed by RocksDB {@code MapState} for exact user-key batch reads. */
@Internal
public interface RocksDBBatchMapReader<UK> {

    /**
     * One fully validated, serialized mutation column that has not yet touched RocksDB.
     *
     * <p>Keys and values are owned by the preparing state token. The mutation is intentionally
     * opaque outside this backend capability so callers cannot bypass the single-write commit
     * boundary.
     */
    final class PreparedMutation {
        private final RocksDBBatchMapReader<?> reader;
        private final List<byte[]> rocksDBKeys;
        private final byte[][] serializedValues;
        private final boolean[] dirty;
        private final boolean[] removed;

        public PreparedMutation(
                RocksDBBatchMapReader<?> reader,
                List<byte[]> rocksDBKeys,
                byte[][] serializedValues,
                boolean[] dirty,
                boolean[] removed) {
            this.reader = reader;
            this.rocksDBKeys =
                    Collections.unmodifiableList(new ArrayList<byte[]>(rocksDBKeys));
            this.serializedValues = serializedValues.clone();
            this.dirty = dirty.clone();
            this.removed = removed.clone();
        }

        public RocksDBBatchMapReader<?> reader() {
            return reader;
        }

        public List<byte[]> rocksDBKeys() {
            return rocksDBKeys;
        }

        public byte[][] serializedValues() {
            return serializedValues;
        }

        public boolean[] dirty() {
            return dirty;
        }

        public boolean[] removed() {
            return removed;
        }
    }

    /**
     * Returns raw null-sensitive MapState values for the current outer key and namespace.
     *
     * <p>The returned list must preserve input order and size. A missing entry is represented by
     * {@code null}; a present value is the exact serialized RocksDB value, including MapState's
     * leading null marker.
     */
    List<byte[]> getSerializedValuesByUserKeys(List<UK> userKeys) throws Exception;

    /**
     * Serializes the exact RocksDB keys for the current outer key/namespace and ordered user-key
     * list. The returned arrays are caller-owned immutable snapshots.
     *
     * <p>Separating mailbox-side key preparation from the RocksDB read lets CacheKit copy the keys
     * into its bounded native arena and avoid one heap value array per hit.
     */
    List<byte[]> serializeRocksDBKeysByUserKeys(List<UK> userKeys) throws Exception;

    /** Reads an ordered sub-range of already prepared exact RocksDB keys. */
    List<byte[]> getSerializedValuesByRocksDBKeys(
            List<byte[]> rocksDBKeys, int fromIndex, int toIndex) throws Exception;

    /** Stable identity of the RocksDB instance used for an optional multi-column read. */
    default Object multiColumnReadOwner() {
        return null;
    }

    /** Stable identity of the RocksDB instance used for prepared mutation commits. */
    default Object preparedWriteOwner() {
        return null;
    }

    /** Whether this reader can prepare and atomically commit already-serialized exact keys. */
    default boolean supportsPreparedMutations() {
        return false;
    }

    /**
     * Serializes dirty values without writing RocksDB.
     *
     * <p>A {@code null} result means the capability is unavailable before any write. Exceptions
     * are fail-loud serializer errors and must not be converted into an authoritative replay.
     */
    default PreparedMutation prepareSerializedMutations(
            List<byte[]> rocksDBKeys, Object[] values, boolean[] dirty, boolean[] removed)
            throws Exception {
        return null;
    }

    /**
     * Commits one or more prepared columns with one RocksDB WriteBatch.
     *
     * <p>Implementations must validate every reader/database before invoking {@code db.write}. Any
     * exception after that invocation is fail-loud and must never trigger replay.
     */
    default void commitPreparedMutations(List<? extends PreparedMutation> mutations)
            throws Exception {
        throw new UnsupportedOperationException("Prepared MapState mutations are unavailable.");
    }

    /**
     * Reads already-serialized keys from several column families through one ordered MultiGet.
     *
     * <p>The outer list is ordered by state column and the returned flat list follows the same
     * order. Implementations must reject readers backed by another database. The default keeps the
     * capability unavailable so ordinary RocksDB state semantics are unchanged.
     */
    default List<byte[]> getSerializedValuesAcrossColumns(
            List<? extends RocksDBBatchMapReader<?>> readers,
            List<? extends List<byte[]>> keysByReader)
            throws Exception {
        throw new UnsupportedOperationException("Cross-column MapState MultiGet is unavailable.");
    }

    /** Whether this reader can execute the shared direct-arena MultiGet ABI. */
    default boolean supportsDirectArenaMultiGet() {
        return false;
    }

    /** Maximum count accepted by the loaded direct-arena JNI implementation. */
    default int directArenaMultiGetMaxBatch() {
        return RocksDBBatchValueReader.DIRECT_ARENA_DEFAULT_BATCH;
    }

    /**
     * Executes one ordered RocksDB MultiGet using the same descriptor ABI as ValueState.
     * Missing and overflow statuses follow {@link RocksDBBatchValueReader}.
     */
    default int getSerializedValuesByRocksDBKeyArena(
            ByteBuffer keyArena,
            ByteBuffer descriptors,
            int count,
            ByteBuffer valueArena,
            int valueStride)
            throws Exception {
        throw new UnsupportedOperationException("Direct-arena MapState MultiGet is unavailable.");
    }
}
