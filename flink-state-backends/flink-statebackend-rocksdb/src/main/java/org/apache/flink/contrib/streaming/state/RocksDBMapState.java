/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.ReadOptions;
import org.rocksdb.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * {@link MapState} implementation that stores state in RocksDB.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <UK> The type of the keys in the map state.
 * @param <UV> The type of the values in the map state.
 */
class RocksDBMapState<K, N, UK, UV> extends AbstractRocksDBState<K, N, Map<UK, UV>>
        implements InternalMapState<K, N, UK, UV> {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBMapState.class);

    /** Serializer for the keys and values. */
    private TypeSerializer<UK> userKeySerializer;

    private TypeSerializer<UV> userValueSerializer;

    /**
     * Creates a new {@code RocksDBMapState}.
     *
     * @param columnFamily The RocksDB column family that this state is associated to.
     * @param namespaceSerializer The serializer for the namespace.
     * @param valueSerializer The serializer for the state.
     * @param defaultValue The default value for the state.
     * @param backend The backend for which this state is bind to.
     */
    private RocksDBMapState(
            ColumnFamilyHandle columnFamily,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<Map<UK, UV>> valueSerializer,
            Map<UK, UV> defaultValue,
            RocksDBKeyedStateBackend<K> backend) {

        super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);

        Preconditions.checkState(
                valueSerializer instanceof MapSerializer, "Unexpected serializer type.");

        MapSerializer<UK, UV> castedMapSerializer = (MapSerializer<UK, UV>) valueSerializer;
        this.userKeySerializer = castedMapSerializer.getKeySerializer();
        this.userValueSerializer = castedMapSerializer.getValueSerializer();
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
    public TypeSerializer<Map<UK, UV>> getValueSerializer() {
        return valueSerializer;
    }

    // ------------------------------------------------------------------------
    //  MapState Implementation
    // ------------------------------------------------------------------------

    @Override
    public UV get(UK userKey) throws IOException, RocksDBException {
        byte[] rawKeyBytes =
                serializeCurrentKeyWithGroupAndNamespacePlusUserKey(userKey, userKeySerializer);
        byte[] rawValueBytes = backend.db.get(columnFamily, rawKeyBytes);

        return (rawValueBytes == null
                ? null
                : deserializeUserValue(dataInputView, rawValueBytes, userValueSerializer));
    }

    @Override
    public void put(UK userKey, UV userValue) throws IOException, RocksDBException {

        byte[] rawKeyBytes =
                serializeCurrentKeyWithGroupAndNamespacePlusUserKey(userKey, userKeySerializer);
        byte[] rawValueBytes = serializeValueNullSensitive(userValue, userValueSerializer);

        backend.db.put(columnFamily, writeOptions, rawKeyBytes, rawValueBytes);
    }

    @Override
    public void putAll(Map<UK, UV> map) throws IOException, RocksDBException {
        if (map == null) {
            return;
        }

        try (RocksDBWriteBatchWrapper writeBatchWrapper =
                new RocksDBWriteBatchWrapper(
                        backend.db, writeOptions, backend.getWriteBatchSize())) {
            for (Map.Entry<UK, UV> entry : map.entrySet()) {
                byte[] rawKeyBytes =
                        serializeCurrentKeyWithGroupAndNamespacePlusUserKey(
                                entry.getKey(), userKeySerializer);
                byte[] rawValueBytes =
                        serializeValueNullSensitive(entry.getValue(), userValueSerializer);
                writeBatchWrapper.put(columnFamily, rawKeyBytes, rawValueBytes);
            }
        }
    }

    @Override
    public void remove(UK userKey) throws IOException, RocksDBException {
        byte[] rawKeyBytes =
                serializeCurrentKeyWithGroupAndNamespacePlusUserKey(userKey, userKeySerializer);

        backend.db.delete(columnFamily, writeOptions, rawKeyBytes);
    }

    @Override
    public boolean contains(UK userKey) throws IOException, RocksDBException {
        byte[] rawKeyBytes =
                serializeCurrentKeyWithGroupAndNamespacePlusUserKey(userKey, userKeySerializer);
        byte[] rawValueBytes = backend.db.get(columnFamily, rawKeyBytes);

        return (rawValueBytes != null);
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() {
        return this::iterator;
    }

    @Override
    public Iterable<UK> keys() {
        final byte[] prefixBytes = serializeCurrentKeyWithGroupAndNamespace();

        return () ->
                new RocksDBMapIterator<UK>(
                        backend.db,
                        prefixBytes,
                        userKeySerializer,
                        userValueSerializer,
                        dataInputView) {
                    @Nullable
                    @Override
                    public UK next() {
                        RocksDBMapEntry entry = nextEntry();
                        return (entry == null ? null : entry.getKey());
                    }
                };
    }

    @Override
    public Iterable<UV> values() {
        final byte[] prefixBytes = serializeCurrentKeyWithGroupAndNamespace();

        return () ->
                new RocksDBMapIterator<UV>(
                        backend.db,
                        prefixBytes,
                        userKeySerializer,
                        userValueSerializer,
                        dataInputView) {
                    @Override
                    public UV next() {
                        RocksDBMapEntry entry = nextEntry();
                        return (entry == null ? null : entry.getValue());
                    }
                };
    }

    @Override
    public void migrateSerializedValue(
            DataInputDeserializer serializedOldValueInput,
            DataOutputSerializer serializedMigratedValueOutput,
            TypeSerializer<Map<UK, UV>> priorSerializer,
            TypeSerializer<Map<UK, UV>> newSerializer)
            throws StateMigrationException {

        checkArgument(priorSerializer instanceof MapSerializer);
        checkArgument(newSerializer instanceof MapSerializer);

        TypeSerializer<UV> priorMapValueSerializer =
                ((MapSerializer<UK, UV>) priorSerializer).getValueSerializer();
        TypeSerializer<UV> newMapValueSerializer =
                ((MapSerializer<UK, UV>) newSerializer).getValueSerializer();

        try {
            boolean isNull = serializedOldValueInput.readBoolean();
            UV mapUserValue = null;
            if (!isNull) {
                mapUserValue = priorMapValueSerializer.deserialize(serializedOldValueInput);
            }
            serializedMigratedValueOutput.writeBoolean(mapUserValue == null);
            newMapValueSerializer.serialize(mapUserValue, serializedMigratedValueOutput);
        } catch (Exception e) {
            throw new StateMigrationException(
                    "Error while trying to migrate RocksDB map state.", e);
        }
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() {
        final byte[] prefixBytes = serializeCurrentKeyWithGroupAndNamespace();

        return new RocksDBMapIterator<Map.Entry<UK, UV>>(
                backend.db, prefixBytes, userKeySerializer, userValueSerializer, dataInputView) {
            @Override
            public Map.Entry<UK, UV> next() {
                return nextEntry();
            }
        };
    }

    @Override
    public boolean isEmpty() {
        final byte[] prefixBytes = serializeCurrentKeyWithGroupAndNamespace();

        try (RocksIteratorWrapper iterator =
                RocksDBOperationUtils.getRocksIterator(
                        backend.db, columnFamily, backend.getReadOptions())) {

            iterator.seek(prefixBytes);

            return !iterator.isValid() || !startWithKeyPrefix(prefixBytes, iterator.key());
        }
    }

    @Override
    public void clear() {
        try (RocksIteratorWrapper iterator =
                        RocksDBOperationUtils.getRocksIterator(
                                backend.db, columnFamily, backend.getReadOptions());
                RocksDBWriteBatchWrapper rocksDBWriteBatchWrapper =
                        new RocksDBWriteBatchWrapper(
                                backend.db,
                                backend.getWriteOptions(),
                                backend.getWriteBatchSize())) {

            final byte[] keyPrefixBytes = serializeCurrentKeyWithGroupAndNamespace();
            iterator.seek(keyPrefixBytes);

            while (iterator.isValid()) {
                byte[] keyBytes = iterator.key();
                if (startWithKeyPrefix(keyPrefixBytes, keyBytes)) {
                    rocksDBWriteBatchWrapper.remove(columnFamily, keyBytes);
                } else {
                    break;
                }
                iterator.next();
            }
        } catch (RocksDBException e) {
            throw new FlinkRuntimeException("Error while cleaning the state in RocksDB.", e);
        }
    }

    @Override
    protected RocksDBMapState<K, N, UK, UV> setValueSerializer(
            TypeSerializer<Map<UK, UV>> valueSerializer) {
        super.setValueSerializer(valueSerializer);
        MapSerializer<UK, UV> castedMapSerializer = (MapSerializer<UK, UV>) valueSerializer;
        this.userKeySerializer = castedMapSerializer.getKeySerializer();
        this.userValueSerializer = castedMapSerializer.getValueSerializer();
        return this;
    }

    @Override
    public byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<Map<UK, UV>> safeValueSerializer)
            throws Exception {

        Preconditions.checkNotNull(serializedKeyAndNamespace);
        Preconditions.checkNotNull(safeKeySerializer);
        Preconditions.checkNotNull(safeNamespaceSerializer);
        Preconditions.checkNotNull(safeValueSerializer);

        // TODO make KvStateSerializer key-group aware to save this round trip and key-group
        // computation
        Tuple2<K, N> keyAndNamespace =
                KvStateSerializer.deserializeKeyAndNamespace(
                        serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

        int keyGroup =
                KeyGroupRangeAssignment.assignToKeyGroup(
                        keyAndNamespace.f0, backend.getNumberOfKeyGroups());

        SerializedCompositeKeyBuilder<K> keyBuilder =
                new SerializedCompositeKeyBuilder<>(
                        safeKeySerializer, backend.getKeyGroupPrefixBytes(), 32);

        keyBuilder.setKeyAndKeyGroup(keyAndNamespace.f0, keyGroup);

        final byte[] keyPrefixBytes =
                keyBuilder.buildCompositeKeyNamespace(keyAndNamespace.f1, namespaceSerializer);

        final MapSerializer<UK, UV> serializer = (MapSerializer<UK, UV>) safeValueSerializer;

        final TypeSerializer<UK> dupUserKeySerializer = serializer.getKeySerializer();
        final TypeSerializer<UV> dupUserValueSerializer = serializer.getValueSerializer();
        final DataInputDeserializer inputView = new DataInputDeserializer();

        final Iterator<Map.Entry<UK, UV>> iterator =
                new RocksDBMapIterator<Map.Entry<UK, UV>>(
                        backend.db,
                        keyPrefixBytes,
                        dupUserKeySerializer,
                        dupUserValueSerializer,
                        inputView) {

                    @Override
                    public Map.Entry<UK, UV> next() {
                        return nextEntry();
                    }
                };

        // Return null to make the behavior consistent with other backends
        if (!iterator.hasNext()) {
            return null;
        }

        return KvStateSerializer.serializeMap(
                () -> iterator, dupUserKeySerializer, dupUserValueSerializer);
    }

    // ------------------------------------------------------------------------
    //  Serialization Methods
    // ------------------------------------------------------------------------

    private static <UK> UK deserializeUserKey(
            DataInputDeserializer dataInputView,
            int userKeyOffset,
            byte[] rawKeyBytes,
            TypeSerializer<UK> keySerializer)
            throws IOException {
        dataInputView.setBuffer(rawKeyBytes, userKeyOffset, rawKeyBytes.length - userKeyOffset);
        return keySerializer.deserialize(dataInputView);
    }

    private static <UV> UV deserializeUserValue(
            DataInputDeserializer dataInputView,
            byte[] rawValueBytes,
            TypeSerializer<UV> valueSerializer)
            throws IOException {

        dataInputView.setBuffer(rawValueBytes);

        boolean isNull = dataInputView.readBoolean();

        return isNull ? null : valueSerializer.deserialize(dataInputView);
    }

    private static <UK> UK deserializeUserKey(
            DataInputDeserializer dataInputView,
            int userKeyOffset,
            byte[] packedPage,
            int rawKeyOffset,
            int rawKeyLength,
            TypeSerializer<UK> keySerializer)
            throws IOException {
        dataInputView.setBuffer(
                packedPage,
                rawKeyOffset + userKeyOffset,
                rawKeyLength - userKeyOffset);
        return keySerializer.deserialize(dataInputView);
    }

    private static <UV> UV deserializeUserValue(
            DataInputDeserializer dataInputView,
            byte[] packedPage,
            int rawValueOffset,
            int rawValueLength,
            TypeSerializer<UV> valueSerializer)
            throws IOException {
        dataInputView.setBuffer(packedPage, rawValueOffset, rawValueLength);
        boolean isNull = dataInputView.readBoolean();
        return isNull ? null : valueSerializer.deserialize(dataInputView);
    }

    private boolean startWithKeyPrefix(byte[] keyPrefixBytes, byte[] rawKeyBytes) {
        if (rawKeyBytes.length < keyPrefixBytes.length) {
            return false;
        }

        for (int i = keyPrefixBytes.length; --i >= backend.getKeyGroupPrefixBytes(); ) {
            if (rawKeyBytes[i] != keyPrefixBytes[i]) {
                return false;
            }
        }

        return true;
    }

    // ------------------------------------------------------------------------
    //  Internal Classes
    // ------------------------------------------------------------------------

    /** A map entry in RocksDBMapState. */
    private class RocksDBMapEntry implements Map.Entry<UK, UV> {
        private final RocksDB db;

        /**
         * The raw bytes of the key stored in RocksDB. Each user key is stored in RocksDB with the
         * format #KeyGroup#Key#Namespace#UserKey.
         */
        @Nullable private byte[] rawKeyBytes;

        /** The raw bytes of the value stored in RocksDB. */
        @Nullable private byte[] rawValueBytes;

        /** Immutable packed page shared by all entries from one complete tiny scan. */
        @Nullable private final byte[] packedPage;
        private final int packedRawKeyOffset;
        private final int packedRawKeyLength;
        private final int packedRawValueOffset;
        private final int packedRawValueLength;

        /** True if the entry has been deleted. */
        private boolean deleted;

        /**
         * The user key and value. The deserialization is performed lazily, i.e. the key and the
         * value is deserialized only when they are accessed.
         */
        private UK userKey;

        private UV userValue;

        /** The offset of User Key offset in raw key bytes. */
        private final int userKeyOffset;

        private final TypeSerializer<UK> keySerializer;

        private final TypeSerializer<UV> valueSerializer;

        private final DataInputDeserializer dataInputView;

        RocksDBMapEntry(
                @Nonnull final RocksDB db,
                @Nonnegative final int userKeyOffset,
                @Nonnull final byte[] rawKeyBytes,
                @Nonnull final byte[] rawValueBytes,
                @Nonnull final TypeSerializer<UK> keySerializer,
                @Nonnull final TypeSerializer<UV> valueSerializer,
                @Nonnull DataInputDeserializer dataInputView) {
            this.db = db;

            this.userKeyOffset = userKeyOffset;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;

            this.rawKeyBytes = rawKeyBytes;
            this.rawValueBytes = rawValueBytes;
            this.packedPage = null;
            this.packedRawKeyOffset = 0;
            this.packedRawKeyLength = 0;
            this.packedRawValueOffset = 0;
            this.packedRawValueLength = 0;
            this.deleted = false;
            this.dataInputView = dataInputView;
        }

        RocksDBMapEntry(
                @Nonnull final RocksDB db,
                @Nonnegative final int userKeyOffset,
                @Nonnull final byte[] packedPage,
                @Nonnegative final int rawKeyOffset,
                @Nonnegative final int rawKeyLength,
                @Nonnegative final int rawValueOffset,
                @Nonnegative final int rawValueLength,
                @Nonnull final TypeSerializer<UK> keySerializer,
                @Nonnull final TypeSerializer<UV> valueSerializer,
                @Nonnull DataInputDeserializer dataInputView) {
            this.db = db;
            this.userKeyOffset = userKeyOffset;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.rawKeyBytes = null;
            this.rawValueBytes = null;
            this.packedPage = packedPage;
            this.packedRawKeyOffset = rawKeyOffset;
            this.packedRawKeyLength = rawKeyLength;
            this.packedRawValueOffset = rawValueOffset;
            this.packedRawValueLength = rawValueLength;
            this.deleted = false;
            this.dataInputView = dataInputView;
        }

        private byte[] materializeRawKey() {
            if (rawKeyBytes == null) {
                rawKeyBytes =
                        Arrays.copyOfRange(
                                packedPage,
                                packedRawKeyOffset,
                                packedRawKeyOffset + packedRawKeyLength);
            }
            return rawKeyBytes;
        }

        public void remove() {
            deleted = true;
            rawValueBytes = null;

            try {
                db.delete(columnFamily, writeOptions, materializeRawKey());
            } catch (RocksDBException e) {
                throw new FlinkRuntimeException("Error while removing data from RocksDB.", e);
            }
        }

        @Override
        public UK getKey() {
            if (userKey == null) {
                try {
                    userKey =
                            packedPage == null
                                    ? deserializeUserKey(
                                            dataInputView,
                                            userKeyOffset,
                                            rawKeyBytes,
                                            keySerializer)
                                    : deserializeUserKey(
                                            dataInputView,
                                            userKeyOffset,
                                            packedPage,
                                            packedRawKeyOffset,
                                            packedRawKeyLength,
                                            keySerializer);
                } catch (IOException e) {
                    throw new FlinkRuntimeException("Error while deserializing the user key.", e);
                }
            }

            return userKey;
        }

        @Override
        public UV getValue() {
            if (deleted) {
                return null;
            } else {
                if (userValue == null) {
                    try {
                        userValue =
                                packedPage == null || rawValueBytes != null
                                        ? deserializeUserValue(
                                                dataInputView, rawValueBytes, valueSerializer)
                                        : deserializeUserValue(
                                                dataInputView,
                                                packedPage,
                                                packedRawValueOffset,
                                                packedRawValueLength,
                                                valueSerializer);
                    } catch (IOException e) {
                        throw new FlinkRuntimeException(
                                "Error while deserializing the user value.", e);
                    }
                }

                return userValue;
            }
        }

        @Override
        public UV setValue(UV value) {
            if (deleted) {
                throw new IllegalStateException("The value has already been deleted.");
            }

            UV oldValue = getValue();

            try {
                userValue = value;
                rawValueBytes = serializeValueNullSensitive(value, valueSerializer);

                db.put(columnFamily, writeOptions, materializeRawKey(), rawValueBytes);
            } catch (IOException | RocksDBException e) {
                throw new FlinkRuntimeException("Error while putting data into RocksDB.", e);
            }

            return oldValue;
        }
    }

    /** An auxiliary utility to scan all entries under the given key. */
    private abstract class RocksDBMapIterator<T> implements Iterator<T> {

        private static final int CACHE_SIZE_LIMIT = 128;

        /** The db where data resides. */
        private final RocksDB db;

        /**
         * The prefix bytes of the key being accessed. All entries under the same key have the same
         * prefix, hence we can stop iterating once coming across an entry with a different prefix.
         */
        @Nonnull private final byte[] keyPrefixBytes;

        /**
         * True if all entries have been accessed or the iterator has come across an entry with a
         * different prefix.
         */
        private boolean expired = false;

        /** A in-memory cache for the entries in the rocksdb. */
        private ArrayList<RocksDBMapEntry> cacheEntries = new ArrayList<>();

        /**
         * The entry pointing to the current position which is last returned by calling {@link
         * #nextEntry()}.
         */
        private RocksDBMapEntry currentEntry;

        private int cacheIndex = 0;

        private final TypeSerializer<UK> keySerializer;
        private final TypeSerializer<UV> valueSerializer;
        private final DataInputDeserializer dataInputView;

        RocksDBMapIterator(
                final RocksDB db,
                final byte[] keyPrefixBytes,
                final TypeSerializer<UK> keySerializer,
                final TypeSerializer<UV> valueSerializer,
                DataInputDeserializer dataInputView) {

            this.db = db;
            this.keyPrefixBytes = keyPrefixBytes;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.dataInputView = dataInputView;
        }

        @Override
        public boolean hasNext() {
            loadCache();

            return (cacheIndex < cacheEntries.size());
        }

        @Override
        public void remove() {
            if (currentEntry == null || currentEntry.deleted) {
                throw new IllegalStateException(
                        "The remove operation must be called after a valid next operation.");
            }

            currentEntry.remove();
        }

        final RocksDBMapEntry nextEntry() {
            loadCache();

            if (cacheIndex == cacheEntries.size()) {
                if (!expired) {
                    throw new IllegalStateException();
                }

                return null;
            }

            this.currentEntry = cacheEntries.get(cacheIndex);
            cacheIndex++;

            return currentEntry;
        }

        private void loadCache() {
            if (cacheIndex > cacheEntries.size()) {
                throw new IllegalStateException();
            }

            // Load cache entries only when the cache is empty and there still exist unread entries
            if (cacheIndex < cacheEntries.size() || expired) {
                return;
            }

            // The packed native API is only safe to use for the initial, complete tiny-map scan.
            // Any non-COMPLETE outcome falls through to the ordinary iterator below.
            if (currentEntry == null
                    && backend.isMapIteratorPackedTinyScanEnabled()
                    && tryLoadPackedTinyMap()) {
                return;
            }

            // use try-with-resources to ensure RocksIterator can be release even some runtime
            // exception
            // occurred in the below code block.
            long entriesLoaded = 0;
            long keyJniCalls = 0;
            long duplicateKeyFetchesAvoided = 0;
            long valueJniCalls = 0;
            long seeks = 0;
            long nativeIterators = 0;
            long boundedIterators = 0;
            long upperBoundFallbacks = 0;
            final byte[] prefixUpperBound =
                    backend.isMapIteratorPrefixUpperBoundEnabled()
                            ? unsignedBytewisePrefixSuccessor(keyPrefixBytes)
                            : null;
            if (backend.isMapIteratorPrefixUpperBoundEnabled() && prefixUpperBound == null) {
                upperBoundFallbacks = 1;
            }
            final Slice upperBoundSlice =
                    prefixUpperBound == null ? null : new Slice(prefixUpperBound);
            final ReadOptions boundedReadOptions =
                    upperBoundSlice == null
                            ? null
                            : new ReadOptions(backend.getReadOptions())
                                    .setIterateUpperBound(upperBoundSlice);
            try (Slice ignoredUpperBoundSlice = upperBoundSlice;
                    ReadOptions ignoredBoundedReadOptions = boundedReadOptions;
                    RocksIteratorWrapper iterator =
                    RocksDBOperationUtils.getRocksIterator(
                            db,
                            columnFamily,
                            boundedReadOptions == null
                                    ? backend.getReadOptions()
                                    : boundedReadOptions)) {
                nativeIterators = 1;
                boundedIterators = boundedReadOptions == null ? 0 : 1;
                /*
                 * The iteration starts from the prefix bytes at the first loading. After #nextEntry() is called,
                 * the currentEntry points to the last returned entry, and at that time, we will start
                 * the iterating from currentEntry if reloading cache is needed.
                 */
                byte[] startBytes =
                        (currentEntry == null ? keyPrefixBytes : currentEntry.materializeRawKey());

                cacheEntries.clear();
                cacheIndex = 0;

                iterator.seek(startBytes);
                seeks = 1;

                /*
                 * If the entry pointing to the current position is not removed, it will be the first entry in the
                 * new iterating. Skip it to avoid redundant access in such cases.
                 */
                if (currentEntry != null && !currentEntry.deleted) {
                    iterator.next();
                }

                while (true) {
                    if (!iterator.isValid()) {
                        expired = true;
                        break;
                    }

                    if (cacheEntries.size() >= CACHE_SIZE_LIMIT) {
                        break;
                    }

                    byte[] rawKeyBytes = iterator.key();
                    keyJniCalls++;
                    if (boundedReadOptions == null
                            && !startWithKeyPrefix(keyPrefixBytes, rawKeyBytes)) {
                        expired = true;
                        break;
                    }

                    byte[] entryRawKeyBytes;
                    if (backend.isMapIteratorSingleKeyFetchEnabled()) {
                        entryRawKeyBytes = rawKeyBytes;
                        duplicateKeyFetchesAvoided++;
                    } else {
                        entryRawKeyBytes = iterator.key();
                        keyJniCalls++;
                    }
                    byte[] rawValueBytes = iterator.value();
                    valueJniCalls++;

                    RocksDBMapEntry entry =
                            new RocksDBMapEntry(
                                    db,
                                    keyPrefixBytes.length,
                                    entryRawKeyBytes,
                                    rawValueBytes,
                                    keySerializer,
                                    valueSerializer,
                                    dataInputView);

                    cacheEntries.add(entry);
                    entriesLoaded++;

                    iterator.next();
                }
            } finally {
                backend.recordMapIteratorPageStats(
                        entriesLoaded,
                        keyJniCalls,
                        duplicateKeyFetchesAvoided,
                        valueJniCalls,
                        seeks,
                        nativeIterators,
                        boundedIterators,
                        upperBoundFallbacks);
            }
        }

        private boolean tryLoadPackedTinyMap() {
            final byte[] prefixUpperBound =
                    backend.isMapIteratorPrefixUpperBoundEnabled()
                            ? unsignedBytewisePrefixSuccessor(keyPrefixBytes)
                            : null;
            final Slice upperBoundSlice =
                    prefixUpperBound == null ? null : new Slice(prefixUpperBound);
            final ReadOptions boundedReadOptions =
                    upperBoundSlice == null
                            ? null
                            : new ReadOptions(backend.getReadOptions())
                                    .setIterateUpperBound(upperBoundSlice);
            final long boundedIterator = boundedReadOptions == null ? 0 : 1;
            final long upperBoundFallback =
                    backend.isMapIteratorPrefixUpperBoundEnabled() && prefixUpperBound == null
                            ? 1
                            : 0;
            final RocksDBPackedTinyMapScan.Result packedResult;
            try (Slice ignoredUpperBoundSlice = upperBoundSlice;
                    ReadOptions ignoredBoundedReadOptions = boundedReadOptions) {
                try {
                    packedResult =
                            RocksDBPackedTinyMapScan.tryScan(
                                    db,
                                    columnFamily,
                                    boundedReadOptions == null
                                            ? backend.getReadOptions()
                                            : boundedReadOptions,
                                    keyPrefixBytes,
                                    backend.getKeyGroupPrefixBytes(),
                                    backend.getMapIteratorPackedTinyScanMaxEntries(),
                                    backend.getMapIteratorPackedTinyScanMaxBytes());
                } catch (RocksDBException e) {
                    backend.recordMapIteratorPackedScan(
                            RocksDBPackedTinyMapScan.Result.fallback(
                                    RocksDBPackedTinyMapScan.Outcome.NATIVE_ERROR, 0));
                    throw new FlinkRuntimeException(
                            "Error while scanning a packed tiny MapState prefix.", e);
                }
            }
            backend.recordMapIteratorPackedScan(packedResult);
            if (packedResult.outcome == RocksDBPackedTinyMapScan.Outcome.NATIVE_ERROR) {
                throw new FlinkRuntimeException(
                        "FrocksDB reported an error while scanning a packed tiny MapState prefix.");
            }
            if (packedResult.outcome != RocksDBPackedTinyMapScan.Outcome.COMPLETE) {
                backend.recordMapIteratorPackedScanIteratorFallback();
                return false;
            }

            cacheEntries.clear();
            cacheIndex = 0;
            final byte[] packedPage = packedResult.encodedPage;
            for (int index = 0; index < packedResult.entryCount(); index++) {
                cacheEntries.add(
                        new RocksDBMapEntry(
                                db,
                                keyPrefixBytes.length,
                                packedPage,
                                packedResult.keyOffset(index),
                                packedResult.keyLength(index),
                                packedResult.valueOffset(index),
                                packedResult.valueLength(index),
                                keySerializer,
                                valueSerializer,
                                dataInputView));
            }
            // COMPLETE is an explicit native proof that the entire prefix scan fit the limits.
            expired = true;
            backend.recordMapIteratorPageStats(
                    cacheEntries.size(), 0, 0, 0, 1, 1, boundedIterator, upperBoundFallback);
            return true;
        }
    }

    /** Returns the shortest unsigned-byte lexicographic successor, or null for all-0xff. */
    @Nullable
    static byte[] unsignedBytewisePrefixSuccessor(byte[] prefix) {
        byte[] successor = Arrays.copyOf(prefix, prefix.length);
        for (int index = successor.length - 1; index >= 0; index--) {
            int unsigned = successor[index] & 0xff;
            if (unsigned != 0xff) {
                successor[index] = (byte) (unsigned + 1);
                return Arrays.copyOf(successor, index + 1);
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    static <UK, UV, K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            RocksDBKeyedStateBackend<K> backend) {
        return (IS)
                new RocksDBMapState<>(
                        registerResult.f0,
                        registerResult.f1.getNamespaceSerializer(),
                        (TypeSerializer<Map<UK, UV>>) registerResult.f1.getStateSerializer(),
                        (Map<UK, UV>) stateDesc.getDefaultValue(),
                        backend);
    }

    @SuppressWarnings("unchecked")
    static <UK, UV, K, N, SV, S extends State, IS extends S> IS update(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<ColumnFamilyHandle, RegisteredKeyValueStateBackendMetaInfo<N, SV>>
                    registerResult,
            IS existingState) {
        return (IS)
                ((RocksDBMapState<K, N, UK, UV>) existingState)
                        .setNamespaceSerializer(registerResult.f1.getNamespaceSerializer())
                        .setValueSerializer(
                                (TypeSerializer<Map<UK, UV>>)
                                        registerResult.f1.getStateSerializer())
                        .setDefaultValue((Map<UK, UV>) stateDesc.getDefaultValue());
    }

    /**
     * RocksDB map state specific byte value transformer wrapper.
     *
     * <p>This specific transformer wrapper checks the first byte to detect null user value entries
     * and if not null forward the rest of byte array to the original byte value transformer.
     */
    static class StateSnapshotTransformerWrapper implements StateSnapshotTransformer<byte[]> {
        private static final byte[] NULL_VALUE;
        private static final byte NON_NULL_VALUE_PREFIX;

        static {
            DataOutputSerializer dov = new DataOutputSerializer(1);
            try {
                dov.writeBoolean(true);
                NULL_VALUE = dov.getCopyOfBuffer();
                dov.clear();
                dov.writeBoolean(false);
                NON_NULL_VALUE_PREFIX = dov.getSharedBuffer()[0];
            } catch (IOException e) {
                throw new FlinkRuntimeException(
                        "Failed to serialize boolean flag of map user null value", e);
            }
        }

        private final StateSnapshotTransformer<byte[]> elementTransformer;
        private final DataInputDeserializer div;

        StateSnapshotTransformerWrapper(StateSnapshotTransformer<byte[]> originalTransformer) {
            this.elementTransformer = originalTransformer;
            this.div = new DataInputDeserializer();
        }

        @Override
        @Nullable
        public byte[] filterOrTransform(@Nullable byte[] value) {
            if (value == null || isNull(value)) {
                return NULL_VALUE;
            } else {
                // we have to skip the first byte indicating null user value
                // TODO: optimization here could be to work with slices and not byte arrays
                // and copy slice sub-array only when needed
                byte[] woNullByte = Arrays.copyOfRange(value, 1, value.length);
                byte[] filteredValue = elementTransformer.filterOrTransform(woNullByte);
                if (filteredValue == null) {
                    filteredValue = NULL_VALUE;
                } else if (filteredValue != woNullByte) {
                    filteredValue = prependWithNonNullByte(filteredValue, value);
                } else {
                    filteredValue = value;
                }
                return filteredValue;
            }
        }

        private boolean isNull(byte[] value) {
            try {
                div.setBuffer(value, 0, 1);
                return div.readBoolean();
            } catch (IOException e) {
                throw new FlinkRuntimeException(
                        "Failed to deserialize boolean flag of map user null value", e);
            }
        }

        private static byte[] prependWithNonNullByte(byte[] value, byte[] reuse) {
            int len = 1 + value.length;
            byte[] result = reuse.length == len ? reuse : new byte[len];
            result[0] = NON_NULL_VALUE_PREFIX;
            System.arraycopy(value, 0, result, 1, value.length);
            return result;
        }
    }
}
