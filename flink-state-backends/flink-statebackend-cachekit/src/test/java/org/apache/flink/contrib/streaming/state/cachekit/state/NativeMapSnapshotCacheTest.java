/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeMapSnapshotCacheTest {

    @Test
    void testWithdrawnClassifierModeIsRejected() {
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new NativeMapSnapshotCache<>(
                                        8,
                                        "SCALAR",
                                        "",
                                        true,
                                        IntSerializer.INSTANCE,
                                        IntSerializer.INSTANCE,
                                        StringSerializer.INSTANCE));
        assertTrue(error.getMessage().contains("without RocksDB backend changes"));
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
    void testSixteenByteRowUsesPrimitiveLookup() throws Exception {
        RowDataSerializer serializer = new RowDataSerializer(new BigIntType());
        try (NativeMapSnapshotCache<RowData, VoidNamespace, String> cache =
                new NativeMapSnapshotCache<>(
                        8,
                        "SCALAR",
                        nativeLibrary(),
                        serializer,
                        VoidNamespaceSerializer.INSTANCE,
                        StringSerializer.INSTANCE)) {
            BinaryRowData key = serializer.toBinaryRow(GenericRowData.of(123456789L));
            assertEquals(16, key.getSizeInBytes());
            assertFalse(cache.putSingle(key, VoidNamespace.INSTANCE, "retained"));
            assertEquals("retained", cache.get(key, VoidNamespace.INSTANCE).userKey());
            assertNull(
                    cache.get(
                            serializer.toBinaryRow(GenericRowData.of(987654321L)),
                            VoidNamespace.INSTANCE));
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

    @Test
    void testRemoveHintSkipsOnlyProvenMissesAndTracksEvictions() throws Exception {
        MapSnapshotCacheMetrics metrics = MapSnapshotCacheMetrics.forTesting();
        try (NativeMapSnapshotCache<Integer, Integer, String> cache =
                new NativeMapSnapshotCache<>(
                        2,
                        "SCALAR",
                        nativeLibrary(),
                        false,
                        true,
                        metrics,
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE)) {
            assertFalse(cache.remove(99, 0));
            assertFalse(cache.putEmpty(1, 0));
            assertFalse(cache.putSingle(2, 0, "two"));
            assertFalse(cache.putSingle(2, 0, "two-updated"));
            assertTrue(cache.putEmpty(3, 0));

            assertFalse(cache.remove(1, 0));
            assertTrue(cache.remove(2, 0));
            assertFalse(cache.remove(2, 0));
            cache.clear();
            assertFalse(cache.remove(3, 0));
        }

        assertEquals(5, metrics.nativeRemoveRequests());
        assertEquals(4, metrics.nativeRemoveHintSkips());
        assertEquals(1, metrics.nativeRemoveJniCalls());
        assertEquals(1, metrics.nativeRemoveHits());
        assertEquals(0, metrics.nativeRemoveHintFalsePositives());
    }

    @Test
    void testJavaHashMatchesNativeHashForRandomSlices() throws Exception {
        try (NativeMapSnapshotCache<Integer, Integer, String> ignored =
                new NativeMapSnapshotCache<>(
                        2,
                        "SCALAR",
                        nativeLibrary(),
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE)) {
            Random random = new Random(0x5eedL);
            for (int iteration = 0; iteration < 4096; iteration++) {
                byte[] bytes = new byte[1 + random.nextInt(256)];
                random.nextBytes(bytes);
                int offset = random.nextInt(bytes.length + 1);
                int length = random.nextInt(bytes.length - offset + 1);
                assertEquals(
                        NativeMapSnapshotCache.nativeHashForTesting(bytes, offset, length),
                        NativeMapSnapshotCache.hashKeyBytes(bytes, offset, length));
            }
        }
    }

    @Test
    void testAutoUsesScalarOnKunpengSnapshotTarget() throws Exception {
        String cpuinfo = Files.readString(Path.of("/proc/cpuinfo"));
        Assumptions.assumeTrue(
                cpuinfo.contains("CPU implementer\t: 0x48")
                        && (cpuinfo.contains("CPU part\t: 0xd01")
                                || cpuinfo.contains("CPU part\t: 0xd02")));
        try (NativeMapSnapshotCache<Integer, Integer, String> cache =
                new NativeMapSnapshotCache<>(
                        2,
                        "AUTO",
                        nativeLibrary(),
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE)) {
            assertEquals("scalar", cache.kernelName());
            assertEquals("crc32c-16", cache.hashName());
        }
    }

    @Test
    void testSampledNativeCostPathsMatchNormalOperations() throws Exception {
        MapSnapshotCacheMetrics metrics = MapSnapshotCacheMetrics.forTesting();
        try (NativeMapSnapshotCache<Integer, Integer, String> cache =
                new NativeMapSnapshotCache<>(
                        2,
                        "SCALAR",
                        nativeLibrary(),
                        false,
                        metrics,
                        IntSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE)) {
            for (int index = 0; index < 1024; index++) {
                assertFalse(cache.putSingle(1, 0, "value"));
            }
            for (int index = 0; index < 1024; index++) {
                assertEquals("value", cache.get(1, 0).userKey());
            }
            for (int index = 0; index < 1024; index++) {
                assertEquals(index == 0, cache.remove(1, 0));
            }
        }

        assertEquals(1, metrics.nativePutCostSamples());
        assertEquals(1, metrics.nativeLookupCostSamples());
        assertEquals(1, metrics.nativeRemoveCostSamples());
        assertEquals(0, metrics.nativeCostRawKeySamples());
        assertEquals(3, metrics.nativeCostSerializedKeySamples());
    }

    @Test
    void testSampledSixteenByteRowUsesProfiledPrimitiveLookup() throws Exception {
        MapSnapshotCacheMetrics metrics = MapSnapshotCacheMetrics.forTesting();
        RowDataSerializer serializer = new RowDataSerializer(new BigIntType());
        try (NativeMapSnapshotCache<RowData, VoidNamespace, String> cache =
                new NativeMapSnapshotCache<>(
                        8,
                        "SCALAR",
                        nativeLibrary(),
                        false,
                        metrics,
                        serializer,
                        VoidNamespaceSerializer.INSTANCE,
                        StringSerializer.INSTANCE)) {
            BinaryRowData key = serializer.toBinaryRow(GenericRowData.of(123456789L));
            assertEquals(16, key.getSizeInBytes());
            assertFalse(cache.putSingle(key, VoidNamespace.INSTANCE, "retained"));
            for (int index = 0; index < 1024; index++) {
                assertEquals("retained", cache.get(key, VoidNamespace.INSTANCE).userKey());
            }
        }

        assertEquals(1, metrics.nativeLookupCostSamples());
        assertEquals(1, metrics.nativeCostRawKeySamples());
        assertEquals(0, metrics.nativeCostSerializedKeySamples());
    }

    private static String nativeLibrary() {
        String library = System.getProperty("cachekit.native.snapshot.library", "");
        Assumptions.assumeTrue(library.isEmpty() || Files.isRegularFile(Path.of(library)));
        return library;
    }

}
