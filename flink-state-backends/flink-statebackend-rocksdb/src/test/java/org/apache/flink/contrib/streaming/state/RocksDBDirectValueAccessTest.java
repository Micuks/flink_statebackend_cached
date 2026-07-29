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

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Integration tests for the direct descriptor ABI on a real RocksDB state. */
public class RocksDBDirectValueAccessTest {

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    @Test
    @SuppressWarnings("unchecked")
    public void testMissingEmptyAndExactValuePreserveBufferLifecycle() throws Exception {
        try (RocksDBKeyedStateBackendTestFactory factory =
                new RocksDBKeyedStateBackendTestFactory()) {
            RocksDBKeyedStateBackend<Integer> backend =
                    factory.create(tmp, IntSerializer.INSTANCE, 128);
            InternalValueState<Integer, VoidNamespace, Integer> internal =
                    (InternalValueState<Integer, VoidNamespace, Integer>)
                            backend.getPartitionedState(
                                    VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    new ValueStateDescriptor<>(
                                            "direct-value", IntSerializer.INSTANCE));
            RocksDBValueState<Integer, VoidNamespace, Integer> state =
                    (RocksDBValueState<Integer, VoidNamespace, Integer>) internal;
            RocksDBDirectValueAccess<Integer, VoidNamespace> access = state;

            backend.setCurrentKey(1);
            state.update(123456);
            byte[] emptyKey = exactKey(access, 2);
            backend.db.put(state.columnFamily, emptyKey, new byte[0]);

            PreparedBatch batch = prepare(access, new int[] {99, 2, 1}, Integer.BYTES);
            fill(batch.values, (byte) 0x5a);
            int keyPosition = batch.keys.position();
            int keyLimit = batch.keys.limit();
            int descriptorPosition = batch.descriptors.position();
            int descriptorLimit = batch.descriptors.limit();
            int valuePosition = batch.values.position();
            int valueLimit = batch.values.limit();

            assertEquals(
                    2,
                    access.readValueBatch(
                            batch.keys, batch.descriptors, 3, batch.values, Integer.BYTES));

            ByteBuffer descriptors = batch.descriptors.duplicate().order(ByteOrder.nativeOrder());
            int base = batch.descriptors.position();
            assertEquals(
                    RocksDBDirectValueAccess.NOT_FOUND,
                    descriptors.getInt(
                            base + RocksDBDirectValueAccess.VALUE_LENGTH_OR_STATUS_OFFSET));
            assertEquals(
                    0,
                    descriptors.getInt(
                            base
                                    + RocksDBDirectValueAccess.DESCRIPTOR_BYTES
                                    + RocksDBDirectValueAccess.VALUE_LENGTH_OR_STATUS_OFFSET));
            assertEquals(
                    Integer.BYTES,
                    descriptors.getInt(
                            base
                                    + 2 * RocksDBDirectValueAccess.DESCRIPTOR_BYTES
                                    + RocksDBDirectValueAccess.VALUE_LENGTH_OR_STATUS_OFFSET));

            DataOutputSerializer expectedValue = new DataOutputSerializer(Integer.BYTES);
            IntSerializer.INSTANCE.serialize(123456, expectedValue);
            byte[] exact = new byte[Integer.BYTES];
            ByteBuffer valueView = batch.values.duplicate();
            valueView.position(batch.values.position() + 2 * Integer.BYTES);
            valueView.get(exact);
            assertArrayEquals(expectedValue.getCopyOfBuffer(), exact);

            assertEquals(keyPosition, batch.keys.position());
            assertEquals(keyLimit, batch.keys.limit());
            assertEquals(descriptorPosition, batch.descriptors.position());
            assertEquals(descriptorLimit, batch.descriptors.limit());
            assertEquals(valuePosition, batch.values.position());
            assertEquals(valueLimit, batch.values.limit());
            assertTrue(access.usesSingleJniBatchRead());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOverflowPublishesStatusButNotAnyValueBytes() throws Exception {
        try (RocksDBKeyedStateBackendTestFactory factory =
                new RocksDBKeyedStateBackendTestFactory()) {
            RocksDBKeyedStateBackend<Integer> backend =
                    factory.create(tmp, IntSerializer.INSTANCE, 128);
            InternalValueState<Integer, VoidNamespace, String> internal =
                    (InternalValueState<Integer, VoidNamespace, String>)
                            backend.getPartitionedState(
                                    VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    new ValueStateDescriptor<>(
                                            "direct-overflow", StringSerializer.INSTANCE));
            RocksDBValueState<Integer, VoidNamespace, String> state =
                    (RocksDBValueState<Integer, VoidNamespace, String>) internal;
            RocksDBDirectValueAccess<Integer, VoidNamespace> access = state;

            backend.setCurrentKey(1);
            state.update("a-value-larger-than-four-bytes");
            PreparedBatch batch = prepare(access, new int[] {1}, Integer.BYTES);
            fill(batch.values, (byte) 0x33);
            byte[] before = copy(batch.values);

            assertEquals(
                    1,
                    access.readValueBatch(
                            batch.keys, batch.descriptors, 1, batch.values, Integer.BYTES));

            assertEquals(
                    RocksDBDirectValueAccess.VALUE_OVERFLOW,
                    batch.descriptors
                            .duplicate()
                            .order(ByteOrder.nativeOrder())
                            .getInt(
                                    batch.descriptors.position()
                                            + RocksDBDirectValueAccess
                                                    .VALUE_LENGTH_OR_STATUS_OFFSET));
            assertArrayEquals(before, copy(batch.values));
        }
    }

    private static <V> PreparedBatch prepare(
            RocksDBDirectValueAccess<Integer, VoidNamespace> access, int[] keys, int valueStride)
            throws Exception {
        byte[][] serializedKeys = new byte[keys.length][];
        int totalKeyBytes = 0;
        for (int index = 0; index < keys.length; index++) {
            serializedKeys[index] = exactKey(access, keys[index]);
            totalKeyBytes += serializedKeys[index].length;
        }

        int keyPrefix = 7;
        ByteBuffer keyArena = ByteBuffer.allocateDirect(keyPrefix + totalKeyBytes + 5);
        keyArena.position(keyPrefix);
        int relativeOffset = 0;

        int descriptorPrefix = 8;
        ByteBuffer descriptors =
                ByteBuffer.allocateDirect(
                                descriptorPrefix
                                        + keys.length * RocksDBDirectValueAccess.DESCRIPTOR_BYTES
                                        + 8)
                        .order(ByteOrder.nativeOrder());
        descriptors.position(descriptorPrefix);
        descriptors.limit(
                descriptorPrefix + keys.length * RocksDBDirectValueAccess.DESCRIPTOR_BYTES);
        ByteBuffer descriptorView = descriptors.duplicate().order(ByteOrder.nativeOrder());
        for (int index = 0; index < serializedKeys.length; index++) {
            keyArena.put(serializedKeys[index]);
            int record = descriptorPrefix + index * RocksDBDirectValueAccess.DESCRIPTOR_BYTES;
            descriptorView.putInt(record + RocksDBDirectValueAccess.STATE_ID_OFFSET, 17);
            descriptorView.putInt(record + RocksDBDirectValueAccess.ORIGINAL_INDEX_OFFSET, index);
            descriptorView.putLong(record + RocksDBDirectValueAccess.GENERATION_OFFSET, 23L);
            descriptorView.putInt(
                    record + RocksDBDirectValueAccess.KEY_OFFSET_OFFSET, relativeOffset);
            descriptorView.putInt(
                    record + RocksDBDirectValueAccess.KEY_LENGTH_OFFSET,
                    serializedKeys[index].length);
            descriptorView.putInt(
                    record + RocksDBDirectValueAccess.VALUE_OFFSET_OFFSET, index * valueStride);
            descriptorView.putInt(
                    record + RocksDBDirectValueAccess.VALUE_LENGTH_OR_STATUS_OFFSET,
                    Integer.MIN_VALUE);
            relativeOffset += serializedKeys[index].length;
        }
        keyArena.limit(keyPrefix + totalKeyBytes);
        keyArena.position(keyPrefix);

        int valuePrefix = 5;
        ByteBuffer values = ByteBuffer.allocateDirect(valuePrefix + keys.length * valueStride + 3);
        values.position(valuePrefix);
        values.limit(valuePrefix + keys.length * valueStride);
        return new PreparedBatch(keyArena, descriptors, values);
    }

    private static byte[] exactKey(RocksDBDirectValueAccess<Integer, VoidNamespace> access, int key)
            throws Exception {
        DataOutputSerializer output = new DataOutputSerializer(32);
        access.writeKeyAndNamespace(key, VoidNamespace.INSTANCE, output);
        return output.getCopyOfBuffer();
    }

    private static void fill(ByteBuffer target, byte value) {
        ByteBuffer copy = target.duplicate();
        while (copy.hasRemaining()) {
            copy.put(value);
        }
    }

    private static byte[] copy(ByteBuffer source) {
        ByteBuffer view = source.duplicate();
        byte[] bytes = new byte[view.remaining()];
        view.get(bytes);
        return bytes;
    }

    private static final class PreparedBatch {
        private final ByteBuffer keys;
        private final ByteBuffer descriptors;
        private final ByteBuffer values;

        private PreparedBatch(ByteBuffer keys, ByteBuffer descriptors, ByteBuffer values) {
            this.keys = keys;
            this.descriptors = descriptors;
            this.values = values;
        }
    }
}
