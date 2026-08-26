/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.flink.annotation.Internal;

/** Internal capability exposed by RocksDB {@code MapState} for exact user-key batch reads. */
@Internal
public interface RocksDBBatchMapReader<UK> {

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
