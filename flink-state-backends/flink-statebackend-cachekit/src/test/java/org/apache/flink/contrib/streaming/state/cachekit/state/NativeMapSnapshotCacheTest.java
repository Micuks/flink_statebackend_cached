/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBMapStateNativeSnapshotAccess;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PresenceCacheImplementation;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class NativeMapSnapshotCacheTest {

    @TempDir Path temporaryDirectory;

    @Test
    void testNativeClassifierReturnsEmptySingleAndMulti() throws Exception {
        String library = nativeLibrary();
        RocksDB.loadLibrary();
        byte[] prefix = new byte[] {7, 11, 13};

        try (Options options = new Options().setCreateIfMissing(true);
                RocksDB db = RocksDB.open(options, temporaryDirectory.resolve("db").toString());
                ColumnFamilyHandle columnFamily = db.getDefaultColumnFamily();
                ReadOptions readOptions = new ReadOptions();
                NativeMapSnapshotCache<Integer, Integer, String> cache =
                        new NativeMapSnapshotCache<>(
                                8,
                                "SCALAR",
                                library,
                                true,
                                IntSerializer.INSTANCE,
                                IntSerializer.INSTANCE,
                                StringSerializer.INSTANCE)) {
            RocksDBMapStateNativeSnapshotAccess access =
                    new TestNativeSnapshotAccess(db, columnFamily, readOptions, prefix);

            NativeMapSnapshotCache.Classification<String> empty = cache.classify(access);
            assertTrue(empty.isEmpty());

            db.put(compositeKey(prefix, "first"), new byte[] {1});
            NativeMapSnapshotCache.Classification<String> single = cache.classify(access);
            assertTrue(single.isSingle());
            assertEquals("first", single.userKey());

            db.put(compositeKey(prefix, "second"), new byte[] {2});
            NativeMapSnapshotCache.Classification<String> multi = cache.classify(access);
            assertFalse(multi.isEmpty());
            assertFalse(multi.isSingle());
        }
    }

    @Test
    void testPrefixBatchFeedsNativeSnapshotCacheWithoutJavaRescan() throws Exception {
        assertPrefixBatchFeedsSnapshotCache(true, "native-flow-db");
    }

    @Test
    void testPrefixBatchFeedsJavaSnapshotCacheWithoutJavaRescan() throws Exception {
        assertPrefixBatchFeedsSnapshotCache(false, "java-flow-db");
    }

    private void assertPrefixBatchFeedsSnapshotCache(
            boolean nativeSnapshotCacheEnabled, String databaseDirectory) throws Exception {
        String library = nativeLibrary();
        RocksDB.loadLibrary();
        AtomicReference<String> currentKey = new AtomicReference<>("empty-key");
        AtomicReference<byte[]> currentPrefix = new AtomicReference<>(new byte[] {21, 0});

        try (Options options = new Options().setCreateIfMissing(true);
                RocksDB db =
                        RocksDB.open(
                                options,
                                temporaryDirectory.resolve(databaseDirectory).toString());
                ColumnFamilyHandle columnFamily = db.getDefaultColumnFamily();
                ReadOptions readOptions = new ReadOptions()) {
            @SuppressWarnings("unchecked")
            InternalMapState<String, VoidNamespace, String, Integer> delegate =
                    mock(
                            InternalMapState.class,
                            withSettings().extraInterfaces(RocksDBMapStateNativeSnapshotAccess.class));
            RocksDBMapStateNativeSnapshotAccess access =
                    (RocksDBMapStateNativeSnapshotAccess) delegate;
            when(delegate.getKeySerializer()).thenReturn(StringSerializer.INSTANCE);
            when(delegate.getNamespaceSerializer()).thenReturn(VoidNamespaceSerializer.INSTANCE);
            when(delegate.getValueSerializer())
                    .thenReturn(
                            new MapSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE));
            when(access.serializeCurrentKeyNamespacePrefix()).thenAnswer(ignored -> currentPrefix.get());
            when(access.getDbNativeHandle()).thenReturn(db.getNativeHandle());
            when(access.getColumnFamilyNativeHandle()).thenReturn(columnFamily.getNativeHandle());
            when(access.getReadOptionsNativeHandle()).thenReturn(readOptions.getNativeHandle());
            when(access.getKeyGroupPrefixBytes()).thenReturn(0);
            when(delegate.get("first")).thenReturn(1);

            Map<String, Integer> multiEntries = new LinkedHashMap<>();
            multiEntries.put("first", 1);
            multiEntries.put("second", 2);
            when(delegate.entries()).thenReturn(multiEntries.entrySet());

            MapSnapshotCacheMetrics metrics = MapSnapshotCacheMetrics.forTesting();
            CachedInternalMapState<String, VoidNamespace, String, Integer> state =
                    new CachedInternalMapState<>(
                            delegate,
                            currentKey::get,
                            currentKey::set,
                            0,
                            CachePolicyType.LRU,
                            0,
                            PresenceCacheImplementation.PRIMITIVE,
                            0,
                            CachePolicyType.LRU,
                            0,
                            false,
                            0.0,
                            1,
                            false,
                            8,
                            metrics,
                            nativeSnapshotCacheEnabled,
                            true,
                            "SCALAR",
                            library);
            try {
                state.setCurrentNamespace(VoidNamespace.INSTANCE);
                assertTrue(state.isEmpty());
                verify(delegate, times(0)).isEmpty();
                assertEquals(1, metrics.storesEmpty());

                currentKey.set("single-key");
                currentPrefix.set(new byte[] {21, 1});
                db.put(compositeKey(currentPrefix.get(), "first"), serializedIntValue(1));

                assertEquals("first", state.entries().iterator().next().getKey());
                verify(delegate, times(0)).entries();
                verify(delegate, times(0)).get("first");
                assertEquals(1, metrics.storesSingle());

                clearInvocations(delegate);
                assertEquals("first", state.entries().iterator().next().getKey());
                verify(delegate, times(0)).entries();
                verify(delegate, times(1)).get("first");
                assertEquals(1, metrics.hits());

                currentKey.set("multi-key");
                currentPrefix.set(new byte[] {21, 2});
                db.put(compositeKey(currentPrefix.get(), "first"), serializedIntValue(1));
                db.put(compositeKey(currentPrefix.get(), "second"), serializedIntValue(2));
                clearInvocations(delegate);

                int firstPass = 0;
                for (Map.Entry<String, Integer> entry : state.entries()) {
                    assertEquals(++firstPass, entry.getValue());
                }
                int secondPass = 0;
                for (Map.Entry<String, Integer> entry : state.entries()) {
                    assertEquals(++secondPass, entry.getValue());
                }
                assertEquals(2, firstPass);
                assertEquals(2, secondPass);
                verify(delegate, times(0)).entries();
                assertEquals(2, metrics.multiEntrySkips());

                currentKey.set("paged-key");
                currentPrefix.set(new byte[] {21, 3});
                for (int value = 0; value < 130; value++) {
                    String userKey = String.format("key-%03d", value);
                    db.put(
                            compositeKey(currentPrefix.get(), userKey),
                            serializedIntValue(value));
                }
                clearInvocations(delegate);

                Map<String, Integer> pagedEntries = new LinkedHashMap<>();
                for (Map.Entry<String, Integer> entry : state.entries()) {
                    pagedEntries.put(entry.getKey(), entry.getValue());
                }
                assertEquals(130, pagedEntries.size());
                for (int value = 0; value < 130; value++) {
                    assertEquals(value, pagedEntries.get(String.format("key-%03d", value)));
                }
                verify(delegate, times(0)).entries();
                assertEquals(3, metrics.multiEntrySkips());

                currentKey.set("null-value-key");
                currentPrefix.set(new byte[] {21, 4});
                db.put(compositeKey(currentPrefix.get(), "nullable"), new byte[] {1});
                clearInvocations(delegate);

                Map.Entry<String, Integer> nullValueEntry =
                        state.entries().iterator().next();
                assertEquals("nullable", nullValueEntry.getKey());
                assertNull(nullValueEntry.getValue());
                verify(delegate, times(0)).entries();
                verify(delegate, times(0)).get("nullable");
                assertEquals(2, metrics.storesSingle());
            } finally {
                state.close();
            }
        }
    }

    private static byte[] serializedIntValue(int value) {
        return new byte[] {
            0,
            (byte) (value >>> 24),
            (byte) (value >>> 16),
            (byte) (value >>> 8),
            (byte) value
        };
    }

    @Test
    void testNexmarkRowDataReturnsRetainedUserKey() throws Exception {
        String library = nativeLibrary();
        RowDataSerializer serializer =
                new RowDataSerializer(new IntType(), VarCharType.STRING_TYPE);
        NativeMapSnapshotCache<RowData, VoidNamespace, RowData> cache =
                new NativeMapSnapshotCache<>(
                        8,
                        "SCALAR",
                        library,
                        serializer,
                        VoidNamespaceSerializer.INSTANCE,
                        serializer.duplicate());
        try {
            BinaryRowData encodedKey =
                    serializer.toBinaryRow(
                            GenericRowData.of(7, StringData.fromString("auction")));
            byte[] paddedKey = new byte[encodedKey.getSizeInBytes() + 16];
            encodedKey
                    .getSegments()[0]
                    .get(encodedKey.getOffset(), paddedKey, 8, encodedKey.getSizeInBytes());
            BinaryRowData key = new BinaryRowData(2);
            key.pointTo(MemorySegmentFactory.wrap(paddedKey), 8, encodedKey.getSizeInBytes());
            RowData userKey = GenericRowData.of(11, StringData.fromString("bidder"));
            assertFalse(cache.putSingle(key, VoidNamespace.INSTANCE, userKey));

            RowData retained = cache.get(key, VoidNamespace.INSTANCE).userKey();
            assertSame(userKey, retained);
            assertEquals(11, retained.getInt(0));
            assertEquals("bidder", retained.getString(1).toString());
        } finally {
            cache.close();
        }
    }

    @Test
    void testEmptySingleEvictionRemoveAndClose() throws Exception {
        String library = nativeLibrary();

        NativeMapSnapshotCache<Integer, Integer, String> cache =
                new NativeMapSnapshotCache<>(
                        2,
                        "SCALAR",
                        library,
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE);
        assertEquals("scalar", cache.kernelName());
        assertFalse(cache.putEmpty(1, 0));
        String largeUserKey = "x".repeat(1024);
        assertFalse(cache.putSingle(2, 0, largeUserKey));

        assertTrue(cache.get(1, 0).isEmpty());
        assertSame(largeUserKey, cache.get(2, 0).userKey());
        String replacement = new String("replacement");
        assertFalse(cache.putSingle(2, 0, replacement));
        assertSame(replacement, cache.get(2, 0).userKey());
        assertTrue(cache.putEmpty(3, 0));
        assertNull(cache.get(1, 0));
        assertEquals(2, cache.size());

        assertTrue(cache.remove(2, 0));
        assertFalse(cache.remove(2, 0));
        cache.clear();
        assertEquals(0, cache.size());
        cache.close();
        assertThrows(IllegalStateException.class, cache::size);
        cache.close();
    }

    private static String nativeLibrary() {
        String library = System.getProperty("cachekit.native.snapshot.library");
        Assumptions.assumeTrue(library != null && Files.isRegularFile(Path.of(library)));
        return library;
    }

    private static byte[] compositeKey(byte[] prefix, String userKey) throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(32);
        StringSerializer.INSTANCE.serialize(userKey, output);
        byte[] suffix = output.getCopyOfBuffer();
        byte[] key = new byte[prefix.length + suffix.length];
        System.arraycopy(prefix, 0, key, 0, prefix.length);
        System.arraycopy(suffix, 0, key, prefix.length, suffix.length);
        return key;
    }

    private static final class TestNativeSnapshotAccess
            implements RocksDBMapStateNativeSnapshotAccess {
        private final RocksDB db;
        private final ColumnFamilyHandle columnFamily;
        private final ReadOptions readOptions;
        private final byte[] prefix;

        private TestNativeSnapshotAccess(
                RocksDB db,
                ColumnFamilyHandle columnFamily,
                ReadOptions readOptions,
                byte[] prefix) {
            this.db = db;
            this.columnFamily = columnFamily;
            this.readOptions = readOptions;
            this.prefix = prefix;
        }

        @Override
        public byte[] serializeCurrentKeyNamespacePrefix() {
            return prefix;
        }

        @Override
        public long getDbNativeHandle() {
            return db.getNativeHandle();
        }

        @Override
        public long getColumnFamilyNativeHandle() {
            return columnFamily.getNativeHandle();
        }

        @Override
        public long getReadOptionsNativeHandle() {
            return readOptions.getNativeHandle();
        }

        @Override
        public int getKeyGroupPrefixBytes() {
            return 0;
        }
    }
}
