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

package org.apache.flink.contrib.streaming.state.cachekit.nativeplane;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBDirectValueAccess;
import org.apache.flink.core.memory.DataOutputView;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DirectStateEnvelopeTest {

    private static final SerializedKeyBatch.PreparedKeyWriter<Integer, String> KEY_WRITER =
            (key, namespace, target) -> {
                target.writeInt(key);
                target.writeUTF(namespace);
            };

    @Test
    void ownsAlignedArenasAndPublishesExact32ByteDescriptors() throws Exception {
        DirectStateEnvelope<Integer, String> envelope = newEnvelope();
        envelope.begin("ns", 41L);
        envelope.append(7, 100, KEY_WRITER);
        envelope.append(7, 200, KEY_WRITER);
        envelope.append(7, 300, KEY_WRITER);
        envelope.append(7, 400, KEY_WRITER);

        assertTrue(envelope.usesOnlyAlignedArenas());
        assertEquals(128, envelope.descriptorView().remaining());
        assertEquals(128, envelope.valueStride());

        ByteBuffer descriptors = envelope.descriptorView().order(ByteOrder.nativeOrder());
        assertEquals(7, descriptors.getInt(SerializedKeyBatch.STATE_ID_OFFSET));
        assertEquals(0, descriptors.getInt(SerializedKeyBatch.ORIGINAL_INDEX_OFFSET));
        assertEquals(41L, descriptors.getLong(SerializedKeyBatch.GENERATION_OFFSET));
        assertEquals(0, descriptors.getInt(SerializedKeyBatch.ARENA_OFFSET_OFFSET));
        assertEquals(0, descriptors.getInt(SerializedKeyBatch.VALUE_OFFSET_OFFSET));
        assertEquals(
                SerializedKeyBatch.VALUE_UNSET,
                descriptors.getInt(SerializedKeyBatch.VALUE_LENGTH_OR_STATUS_OFFSET));
    }

    @Test
    void distinguishesMissingEmptyAndExactThenMaterializesDirectly() throws Exception {
        DirectStateEnvelope<Integer, String> envelope = newEnvelope();
        envelope.begin("ns", 99L);
        envelope.append(3, 10, KEY_WRITER);
        envelope.append(3, 20, KEY_WRITER);
        envelope.append(3, 30, KEY_WRITER);

        RocksDBDirectValueAccess<Integer, String> access =
                new FakeAccess(RocksDBDirectValueAccess.NOT_FOUND, 0, Integer.BYTES);
        assertEquals(2, envelope.read(access));

        assertEquals(RocksDBDirectValueAccess.NOT_FOUND, envelope.valueLengthOrStatus(0));
        assertEquals(0, envelope.valueLengthOrStatus(1));
        assertEquals(Integer.BYTES, envelope.valueLengthOrStatus(2));
        assertEquals(123456, IntSerializer.INSTANCE.deserialize(envelope.valueInput(2)));
        assertThrows(IllegalStateException.class, () -> envelope.valueInput(0));
        assertSame(envelope.valueInput(1), envelope.valueInput(2));
        assertEquals(10, envelope.logicalKey(0));
        assertEquals("ns", envelope.namespaceSnapshot());
        assertEquals(99L, envelope.generation());
        assertFalse(envelope.hasOverflow());
    }

    @Test
    void overflowPublishesStatusButLeavesWholeValueArenaUntouched() throws Exception {
        DirectStateEnvelope<Integer, String> envelope = newEnvelope();
        envelope.begin("ns", 5L);
        envelope.append(1, 1, KEY_WRITER);
        envelope.append(1, 2, KEY_WRITER);

        ByteBuffer valueArena = envelope.writableValueSlice();
        while (valueArena.hasRemaining()) {
            valueArena.put((byte) 0x5a);
        }
        byte[] before = copy(envelope.writableValueSlice());

        envelope.read(new FakeAccess(Integer.BYTES, RocksDBDirectValueAccess.VALUE_OVERFLOW));

        assertTrue(envelope.hasOverflow());
        assertEquals(RocksDBDirectValueAccess.VALUE_OVERFLOW, envelope.valueLengthOrStatus(1));
        assertTrue(Arrays.equals(before, copy(envelope.writableValueSlice())));
    }

    @Test
    void clearReusesNativeOwnershipAndDropsLogicalReferences() throws Exception {
        DirectStateEnvelope<Integer, String> envelope = newEnvelope();
        envelope.begin("old", 1L);
        envelope.append(1, 11, KEY_WRITER);
        ByteBuffer descriptorBefore = envelope.writableDescriptorView();

        envelope.begin("new", 2L);
        envelope.append(2, 22, KEY_WRITER);

        assertEquals(1, envelope.entryCount());
        assertEquals(22, envelope.logicalKey(0));
        assertEquals("new", envelope.namespaceSnapshot());
        assertEquals(2L, envelope.generation());
        assertEquals(descriptorBefore.capacity(), envelope.writableDescriptorView().capacity());
    }

    @Test
    void rejectsBatchesBeyondFrozenSingleJniLimit() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new DirectStateEnvelope<>(
                                IntSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                RocksDBDirectValueAccess.MAX_BATCH_ENTRIES + 1,
                                512,
                                128));
    }

    private static DirectStateEnvelope<Integer, String> newEnvelope() {
        return new DirectStateEnvelope<>(
                IntSerializer.INSTANCE, StringSerializer.INSTANCE, 4, 512, 17);
    }

    private static byte[] copy(ByteBuffer source) {
        byte[] bytes = new byte[source.remaining()];
        source.get(bytes);
        return bytes;
    }

    private static final class FakeAccess implements RocksDBDirectValueAccess<Integer, String> {
        private final int[] statuses;

        private FakeAccess(int... statuses) {
            this.statuses = statuses;
        }

        @Override
        public void writeKeyAndNamespace(Integer key, String namespace, DataOutputView target)
                throws IOException {
            KEY_WRITER.write(key, namespace, target);
        }

        @Override
        public int readValueBatch(
                ByteBuffer keyArena,
                ByteBuffer descriptors,
                int count,
                ByteBuffer valueArena,
                int valueStride) {
            int keyPosition = keyArena.position();
            int descriptorPosition = descriptors.position();
            int valuePosition = valueArena.position();
            ByteBuffer descriptorView = descriptors.duplicate().order(ByteOrder.nativeOrder());
            boolean overflow = false;
            int found = 0;
            for (int index = 0; index < count; index++) {
                int status = statuses[index];
                descriptorView.putInt(
                        index * RocksDBDirectValueAccess.DESCRIPTOR_BYTES
                                + RocksDBDirectValueAccess.VALUE_LENGTH_OR_STATUS_OFFSET,
                        status);
                overflow |= status == RocksDBDirectValueAccess.VALUE_OVERFLOW;
                found += status != RocksDBDirectValueAccess.NOT_FOUND ? 1 : 0;
            }
            if (!overflow) {
                for (int index = 0; index < count; index++) {
                    if (statuses[index] == Integer.BYTES) {
                        int valueOffset =
                                descriptorView.getInt(
                                        index * RocksDBDirectValueAccess.DESCRIPTOR_BYTES
                                                + RocksDBDirectValueAccess.VALUE_OFFSET_OFFSET);
                        valueArena
                                .duplicate()
                                .order(ByteOrder.BIG_ENDIAN)
                                .putInt(valueOffset, 123456);
                    }
                }
            }
            assertEquals(keyPosition, keyArena.position());
            assertEquals(descriptorPosition, descriptors.position());
            assertEquals(valuePosition, valueArena.position());
            return found;
        }
    }
}
