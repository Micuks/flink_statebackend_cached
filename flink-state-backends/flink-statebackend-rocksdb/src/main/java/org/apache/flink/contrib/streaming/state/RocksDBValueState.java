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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.util.FlinkRuntimeException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * {@link ValueState} implementation that stores state in RocksDB.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 */
class RocksDBValueState<K, N, V> extends AbstractRocksDBState<K, N, V>
        implements InternalValueState<K, N, V>, RocksDBBatchValueReader<K, N, V> {

    /**
     * Creates a new {@code RocksDBValueState}.
     *
     * @param columnFamily The RocksDB column family that this state is associated to.
     * @param namespaceSerializer The serializer for the namespace.
     * @param valueSerializer The serializer for the state.
     * @param defaultValue The default value for the state.
     * @param backend The backend for which this state is bind to.
     */
    private RocksDBValueState(
            ColumnFamilyHandle columnFamily,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue,
            RocksDBKeyedStateBackend<K> backend) {

        super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return backend.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public V getBatchDefaultValue() {
        return getDefaultValue();
    }

    @Override
    public V value() {
        try {
            byte[] valueBytes =
                    backend.db.get(columnFamily, serializeCurrentKeyWithGroupAndNamespace());

            if (valueBytes == null) {
                return getDefaultValue();
            }
            dataInputView.setBuffer(valueBytes);
            return valueSerializer.deserialize(dataInputView);
        } catch (IOException | RocksDBException e) {
            throw new FlinkRuntimeException("Error while retrieving data from RocksDB.", e);
        }
    }

    @Override
    public byte[] serializeBatchKeyAndNamespace(
            K key,
            N namespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer)
            throws Exception {
        return serializeKeyAndNamespace(key, namespace, safeKeySerializer, safeNamespaceSerializer);
    }

    @Override
    public void serializeBatchKeyAndNamespace(
            K key,
            N namespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            PositionedDataOutputView output)
            throws Exception {
        int keyGroup =
                KeyGroupRangeAssignment.assignToKeyGroup(key, backend.getNumberOfKeyGroups());
        CompositeKeySerializationUtils.writeKeyGroup(
                keyGroup, backend.getKeyGroupPrefixBytes(), output);
        boolean ambiguous =
                CompositeKeySerializationUtils.isAmbiguousKeyPossible(
                        safeKeySerializer, safeNamespaceSerializer);
        int keyStart = output.position();
        safeKeySerializer.serialize(key, output);
        if (ambiguous) {
            CompositeKeySerializationUtils.writeVariableIntBytes(
                    output.position() - keyStart, output);
        }
        int namespaceStart = output.position();
        safeNamespaceSerializer.serialize(namespace, output);
        if (ambiguous) {
            CompositeKeySerializationUtils.writeVariableIntBytes(
                    output.position() - namespaceStart, output);
        }
    }

    @Override
    public byte[] getSerializedValueByRocksDBKey(byte[] rocksDBKey) throws Exception {
        return backend.db.get(columnFamily, rocksDBKey);
    }

    @Override
    public List<byte[]> getSerializedValuesByRocksDBKeys(
            List<byte[]> rocksDBKeys, int fromIndex, int toIndex) throws Exception {
        if (fromIndex < 0 || toIndex < fromIndex || toIndex > rocksDBKeys.size()) {
            throw new IndexOutOfBoundsException(
                    "Invalid RocksDB key range ["
                            + fromIndex
                            + ", "
                            + toIndex
                            + ") for size "
                            + rocksDBKeys.size());
        }
        if (fromIndex == toIndex) {
            return Collections.emptyList();
        }
        List<byte[]> keyRange = rocksDBKeys.subList(fromIndex, toIndex);
        return backend.db.multiGetAsList(
                Collections.nCopies(keyRange.size(), columnFamily), keyRange);
    }

    @Override
    public boolean supportsDirectArenaMultiGet() {
        return true;
    }

    @Override
    public int directArenaMultiGetMaxBatch() {
        try {
            Object advertised =
                    backend.db
                            .getClass()
                            .getMethod("directMultiGetMaxBatch")
                            .invoke(backend.db);
            if (!(advertised instanceof Number)) {
                return RocksDBBatchValueReader.DIRECT_ARENA_DEFAULT_BATCH;
            }
            return Math.max(
                    1,
                    Math.min(
                            RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH,
                            ((Number) advertised).intValue()));
        } catch (ReflectiveOperationException
                | LinkageError
                | SecurityException incompatibleWrapperOrNativeLibrary) {
            return RocksDBBatchValueReader.DIRECT_ARENA_DEFAULT_BATCH;
        }
    }

    @Override
    public int getSerializedValuesByRocksDBKeyArena(
            ByteBuffer keyArena,
            ByteBuffer descriptors,
            int count,
            ByteBuffer valueArena,
            int valueStride)
            throws RocksDBException {
        return backend.db.multiGetDirectArena(
                columnFamily,
                backend.getReadOptions(),
                keyArena,
                descriptors,
                count,
                valueArena,
                valueStride);
    }

    @Override
    public List<byte[]> getSerializedValues(
            List<byte[]> serializedKeyAndNamespaces,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer)
            throws Exception {
        if (serializedKeyAndNamespaces.isEmpty()) {
            return Collections.emptyList();
        }

        List<byte[]> rocksDBKeys = new ArrayList<>(serializedKeyAndNamespaces.size());
        for (byte[] serializedKeyAndNamespace : serializedKeyAndNamespaces) {
            rocksDBKeys.add(
                    serializeQueryKeyAndNamespace(
                            serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer));
        }
        return getSerializedValuesByRocksDBKeys(rocksDBKeys, 0, rocksDBKeys.size());
    }

    @Override
    public void update(V value) {
        if (value == null) {
            clear();
            return;
        }

        try {
            backend.db.put(
                    columnFamily,
                    writeOptions,
                    serializeCurrentKeyWithGroupAndNamespace(),
                    serializeValue(value));
        } catch (Exception e) {
            throw new FlinkRuntimeException("Error while adding data to RocksDB", e);
        }
    }

    @Override
    public boolean supportsSynchronousValueWriteBatch() {
        return true;
    }

    @Override
    public void writeSerializedValueBatch(List<byte[]> rocksDBKeys, List<V> values)
            throws Exception {
        if (rocksDBKeys.size() != values.size()) {
            throw new IllegalArgumentException("Mismatched value write batch sizes");
        }
        if (rocksDBKeys.isEmpty()) {
            return;
        }
        try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
            for (int i = 0; i < rocksDBKeys.size(); i++) {
                V value = values.get(i);
                if (value == null) {
                    batch.delete(columnFamily, rocksDBKeys.get(i));
                } else {
                    batch.put(columnFamily, rocksDBKeys.get(i), serializeValue(value));
                }
            }
            backend.db.write(writeOptions, batch);
        }
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            RocksDBKeyedStateBackend<K> backend) {
        return (IS)
                new RocksDBValueState<>(
                        registerResult.f0,
                        registerResult.f1.getNamespaceSerializer(),
                        registerResult.f1.getStateSerializer(),
                        stateDesc.getDefaultValue(),
                        backend);
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            IS existingState) {
        return (IS)
                ((RocksDBValueState<K, N, SV>) existingState)
                        .setNamespaceSerializer(registerResult.f1.getNamespaceSerializer())
                        .setValueSerializer(registerResult.f1.getStateSerializer())
                        .setDefaultValue(stateDesc.getDefaultValue());
    }
}
