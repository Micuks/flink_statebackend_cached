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
import org.apache.flink.core.memory.DataOutputSerializer;
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
                                            "batch-value", StringSerializer.INSTANCE, "fallback"));

            backend.setCurrentKey(1);
            state.update("one");
            backend.setCurrentKey(2);
            state.update("two");

            assertTrue(state instanceof RocksDBBatchValueReader<?, ?, ?>);
            RocksDBBatchValueReader<Integer, VoidNamespace, String> reader =
                    (RocksDBBatchValueReader<Integer, VoidNamespace, String>) state;
            assertEquals("fallback", reader.getBatchDefaultValue());
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
            for (int key : new int[] {99, 2, 1}) {
                byte[] expected =
                        reader.serializeBatchKeyAndNamespace(
                                key,
                                VoidNamespace.INSTANCE,
                                IntSerializer.INSTANCE,
                                VoidNamespaceSerializer.INSTANCE);
                PositionedSerializer direct = new PositionedSerializer(32);
                reader.serializeBatchKeyAndNamespace(
                        key,
                        VoidNamespace.INSTANCE,
                        IntSerializer.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        direct);
                org.junit.Assert.assertArrayEquals(expected, direct.getCopyOfBuffer());
            }
            List<byte[]> directValues = reader.getSerializedValuesByRocksDBKeys(rocksDBKeys, 1, 3);
            assertEquals(2, directValues.size());
            assertEquals("two", deserializeValue(directValues.get(0)));
            assertEquals("one", deserializeValue(directValues.get(1)));
            assertEquals(
                    "two",
                    deserializeValue(reader.getSerializedValueByRocksDBKey(rocksDBKeys.get(1))));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPreparedMutationBatchCommitsPutsAndDeletes() throws Exception {
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
                                            "batch-write", StringSerializer.INSTANCE, "fallback"));
            backend.setCurrentKey(2);
            state.update("old");

            RocksDBBatchValueReader<Integer, VoidNamespace, String> reader =
                    (RocksDBBatchValueReader<Integer, VoidNamespace, String>) state;
            assertTrue(reader.supportsPreparedValueMutationBatch());
            List<byte[]> keys =
                    Arrays.asList(
                            reader.serializeBatchKeyAndNamespace(
                                    1,
                                    VoidNamespace.INSTANCE,
                                    IntSerializer.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE),
                            reader.serializeBatchKeyAndNamespace(
                                    2,
                                    VoidNamespace.INSTANCE,
                                    IntSerializer.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE),
                            reader.serializeBatchKeyAndNamespace(
                                    3,
                                    VoidNamespace.INSTANCE,
                                    IntSerializer.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE));
            reader.writePreparedValues(
                    keys,
                    Arrays.asList(
                            reader.serializeBatchValue("one", StringSerializer.INSTANCE),
                            null,
                            reader.serializeBatchValue("three", StringSerializer.INSTANCE)));

            backend.setCurrentKey(1);
            assertEquals("one", state.value());
            backend.setCurrentKey(2);
            assertEquals("fallback", state.value());
            backend.setCurrentKey(3);
            assertEquals("three", state.value());

            try {
                reader.writePreparedValues(
                        keys, java.util.Collections.singletonList(new byte[] {1}));
                org.junit.Assert.fail("mismatched batch sizes must be rejected");
            } catch (IllegalArgumentException expected) {
                // expected
            }
        }
    }

    private static byte[] serializeKey(int key) throws Exception {
        return KvStateSerializer.serializeKeyAndNamespace(
                key,
                IntSerializer.INSTANCE,
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE);
    }

    private static final class PositionedSerializer extends DataOutputSerializer
            implements PositionedDataOutputView {

        private PositionedSerializer(int startSize) {
            super(startSize);
        }

        @Override
        public int position() {
            return length();
        }
    }

    private static String deserializeValue(byte[] value) throws Exception {
        return KvStateSerializer.deserializeValue(value, StringSerializer.INSTANCE);
    }
}
