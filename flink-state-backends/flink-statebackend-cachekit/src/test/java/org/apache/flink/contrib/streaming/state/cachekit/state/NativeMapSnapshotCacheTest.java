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
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeMapSnapshotCacheTest {

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
}
