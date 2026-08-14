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

    /**
     * Returns the state default value. Batch readers use this when RocksDB reports a missing key so
     * prefetch can cache the same value that {@code ValueState.value()} would return instead of
     * issuing a second point lookup. Callers must copy a non-null value before publishing it.
     */
    V getBatchDefaultValue();

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

    List<byte[]> getSerializedValues(
            List<byte[]> serializedKeyAndNamespaces,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer)
            throws Exception;
}
