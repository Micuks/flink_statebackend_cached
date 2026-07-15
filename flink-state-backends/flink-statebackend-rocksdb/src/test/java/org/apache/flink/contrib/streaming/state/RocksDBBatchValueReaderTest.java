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

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for the ordered RocksDB {@link RocksDBBatchValueReader} capability. */
public class RocksDBBatchValueReaderTest {

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    @Test
    @SuppressWarnings("unchecked")
    public void testBatchReadPreservesOrderDuplicatesAndMissingEntries() throws Exception {
        try (RocksDBKeyedStateBackendTestFactory factory =
                new RocksDBKeyedStateBackendTestFactory()) {
            RocksDBKeyedStateBackend<Integer> backend =
                    factory.create(tmp, IntSerializer.INSTANCE, 128);
            InternalValueState<Integer, VoidNamespace, String> state =
                    (InternalValueState<Integer, VoidNamespace, String>)
                            backend.getPartitionedState(
                                    VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    new ValueStateDescriptor<>(
                                            "batch-value", StringSerializer.INSTANCE));

            backend.setCurrentKey(1);
            state.update("one");
            backend.setCurrentKey(2);
            state.update("two");

            assertTrue(state instanceof RocksDBBatchValueReader<?, ?>);
            RocksDBBatchValueReader<Integer, VoidNamespace> reader =
                    (RocksDBBatchValueReader<Integer, VoidNamespace>) state;
            List<byte[]> values =
                    reader.getSerializedValues(
                            Arrays.asList(
                                    serializeKey(2),
                                    serializeKey(99),
                                    serializeKey(1),
                                    serializeKey(2)),
                            IntSerializer.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE);

            assertEquals(4, values.size());
            assertEquals("two", deserializeValue(values.get(0)));
            assertNull(values.get(1));
            assertEquals("one", deserializeValue(values.get(2)));
            assertEquals("two", deserializeValue(values.get(3)));
            assertTrue(
                    reader.getSerializedValues(
                                    java.util.Collections.emptyList(),
                                    IntSerializer.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE)
                            .isEmpty());

            List<byte[]> rocksDBKeys =
                    Arrays.asList(
                            reader.serializeBatchKeyAndNamespace(
                                    99,
                                    VoidNamespace.INSTANCE,
                                    IntSerializer.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE),
                            reader.serializeBatchKeyAndNamespace(
                                    2,
                                    VoidNamespace.INSTANCE,
                                    IntSerializer.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE),
                            reader.serializeBatchKeyAndNamespace(
                                    1,
                                    VoidNamespace.INSTANCE,
                                    IntSerializer.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE),
                            reader.serializeBatchKeyAndNamespace(
                                    99,
                                    VoidNamespace.INSTANCE,
                                    IntSerializer.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE));
            List<byte[]> directValues =
                    reader.getSerializedValuesByRocksDBKeys(rocksDBKeys, 1, 3);
            assertEquals(2, directValues.size());
            assertEquals("two", deserializeValue(directValues.get(0)));
            assertEquals("one", deserializeValue(directValues.get(1)));
            assertEquals(
                    "two",
                    deserializeValue(reader.getSerializedValueByRocksDBKey(rocksDBKeys.get(1))));
        }
    }

    private static byte[] serializeKey(int key) throws Exception {
        return KvStateSerializer.serializeKeyAndNamespace(
                key,
                IntSerializer.INSTANCE,
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE);
    }

    private static String deserializeValue(byte[] value) throws Exception {
        return KvStateSerializer.deserializeValue(value, StringSerializer.INSTANCE);
    }
}
