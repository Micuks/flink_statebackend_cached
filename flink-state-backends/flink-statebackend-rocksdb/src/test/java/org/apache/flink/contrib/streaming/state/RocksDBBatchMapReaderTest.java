/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for prepared exact-key reads exposed by {@link RocksDBBatchMapReader}. */
public class RocksDBBatchMapReaderTest {

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    @Test
    @SuppressWarnings("unchecked")
    public void testPreparedKeysPreserveOrderDuplicatesAndMissingEntries() throws Exception {
        try (RocksDBKeyedStateBackendTestFactory factory =
                new RocksDBKeyedStateBackendTestFactory()) {
            RocksDBKeyedStateBackend<Integer> backend =
                    factory.create(tmp, IntSerializer.INSTANCE, 128);
            InternalMapState<Integer, VoidNamespace, String, Integer> state =
                    (InternalMapState<Integer, VoidNamespace, String, Integer>)
                            backend.getPartitionedState(
                                    VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    new MapStateDescriptor<>(
                                            "batch-map",
                                            StringSerializer.INSTANCE,
                                            IntSerializer.INSTANCE));

            backend.setCurrentKey(7);
            state.put("u1", 11);
            state.put("u2", 22);

            assertTrue(state instanceof RocksDBBatchMapReader<?>);
            RocksDBBatchMapReader<String> reader = (RocksDBBatchMapReader<String>) state;
            List<String> userKeys = Arrays.asList("u2", "missing", "u1", "u2");
            List<byte[]> rocksDBKeys = reader.serializeRocksDBKeysByUserKeys(userKeys);
            List<byte[]> values =
                    reader.getSerializedValuesByRocksDBKeys(
                            rocksDBKeys, 0, rocksDBKeys.size());

            assertEquals(userKeys.size(), rocksDBKeys.size());
            assertEquals(userKeys.size(), values.size());
            assertEquals(Integer.valueOf(22), deserializeMapValue(values.get(0)));
            assertNull(values.get(1));
            assertEquals(Integer.valueOf(11), deserializeMapValue(values.get(2)));
            assertEquals(Integer.valueOf(22), deserializeMapValue(values.get(3)));
            assertTrue(
                    reader.getSerializedValuesByRocksDBKeys(rocksDBKeys, 2, 2).isEmpty());
            assertTrue(reader.supportsDirectArenaMultiGet());
            assertTrue(reader.directArenaMultiGetMaxBatch() >= 1);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testIndexedBatchPreservesMapStateReadYourWritesAndBulkReadFence() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setBoolean(RocksDBConfigurableOptions.WRITE_BATCH_WITH_INDEX_ENABLED, true);
        try (RocksDBKeyedStateBackendTestFactory factory =
                new RocksDBKeyedStateBackendTestFactory()) {
            RocksDBKeyedStateBackend<Integer> backend =
                    factory.create(tmp, IntSerializer.INSTANCE, 128, configuration);
            InternalMapState<Integer, VoidNamespace, String, Integer> state =
                    (InternalMapState<Integer, VoidNamespace, String, Integer>)
                            backend.getPartitionedState(
                                    VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    new MapStateDescriptor<>(
                                            "indexed-map",
                                            StringSerializer.INSTANCE,
                                            IntSerializer.INSTANCE));

            backend.setCurrentKey(7);
            state.put("u1", 11);
            state.put("u2", 22);
            assertTrue(backend.hasPendingMapStateIndexedWrites());
            assertEquals(Integer.valueOf(11), state.get("u1"));

            int entryCount = 0;
            for (Map.Entry<String, Integer> entry : state.entries()) {
                assertTrue(entry.getKey().equals("u1") || entry.getKey().equals("u2"));
                entryCount++;
            }
            assertEquals(2, entryCount);

            RocksDBBatchMapReader<String> reader = (RocksDBBatchMapReader<String>) state;
            List<byte[]> keys = reader.serializeRocksDBKeysByUserKeys(Arrays.asList("u1", "u2"));
            assertNull(backend.db.get(backend.getColumnFamilyHandle("indexed-map"), keys.get(0)));
            List<byte[]> values = reader.getSerializedValuesByRocksDBKeys(keys, 0, keys.size());
            assertEquals(Integer.valueOf(11), deserializeMapValue(values.get(0)));
            assertEquals(Integer.valueOf(22), deserializeMapValue(values.get(1)));
            assertTrue(!backend.hasPendingMapStateIndexedWrites());
            assertTrue(
                    backend.db.get(backend.getColumnFamilyHandle("indexed-map"), keys.get(0))
                            != null);

            state.remove("u1");
            assertNull(state.get("u1"));
            state.clear();
            assertTrue(state.isEmpty());
        }
    }

    private static Integer deserializeMapValue(byte[] value) throws Exception {
        if (value == null) {
            return null;
        }
        DataInputDeserializer input = new DataInputDeserializer(value);
        return input.readBoolean() ? null : IntSerializer.INSTANCE.deserialize(input);
    }
}
