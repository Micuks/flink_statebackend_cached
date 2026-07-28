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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SerializedKeyBatchTest {

    @Test
    void testSingleNonVoidNamespaceMatchesDataOutputSerializer() throws Exception {
        SerializedKeyBatch<Integer, String> batch =
                newBatch(IntSerializer.INSTANCE, StringSerializer.INSTANCE, 128, 4);
        int stateId = 17;
        long generation = 0x0102030405060708L;
        int key = -123456;
        String namespace = "tenant-\u20ac";

        int entryIndex = batch.append(stateId, generation, key, namespace);

        DataOutputSerializer expected = serializePair(IntSerializer.INSTANCE, key, namespace);
        assertEquals(0, entryIndex);
        assertEquals(1, batch.entryCount());
        assertEquals(expected.length(), batch.arenaBytesWritten());
        assertArrayEquals(expected.getCopyOfBuffer(), copyBytes(batch.arenaSlice()));
        assertEquals(stateId, batch.stateId(0));
        assertEquals(generation, batch.generation(0));
        assertEquals(0, batch.arenaOffset(0));
        assertEquals(expected.length(), batch.serializedLength(0));

        ByteBuffer metadata = batch.metadataSlice();
        assertTrue(metadata.isDirect());
        assertTrue(metadata.isReadOnly());
        assertEquals(ByteOrder.nativeOrder(), metadata.order());
        assertEquals(SerializedKeyBatch.METADATA_RECORD_BYTES, metadata.remaining());
        assertEquals(stateId, metadata.getInt(SerializedKeyBatch.STATE_ID_OFFSET));
        assertEquals(0, metadata.getInt(SerializedKeyBatch.RESERVED_OFFSET));
        assertEquals(generation, metadata.getLong(SerializedKeyBatch.GENERATION_OFFSET));
        assertEquals(0, metadata.getInt(SerializedKeyBatch.ARENA_OFFSET_OFFSET));
        assertEquals(expected.length(), metadata.getInt(SerializedKeyBatch.LENGTH_OFFSET));
    }

    @Test
    void testPreparedRocksDbBytesAreCopiedExactlyAndRemainStable() throws Exception {
        SerializedKeyBatch<byte[], byte[]> batch =
                SerializedKeyBatch.forSerializedBytes(
                        ByteBuffer.allocateDirect(64),
                        ByteBuffer.allocateDirect(2 * SerializedKeyBatch.METADATA_RECORD_BYTES));
        byte[] prepared = {9, 0, 4, 7, 1};

        batch.appendSerialized(31, 77L, prepared);
        prepared[0] = 99;

        assertArrayEquals(new byte[] {9, 0, 4, 7, 1}, copyBytes(batch.arenaSlice()));
        assertEquals(31, batch.stateId(0));
        assertEquals(77L, batch.generation(0));
        assertEquals(0, batch.arenaOffset(0));
        assertEquals(5, batch.serializedLength(0));
    }

    @Test
    void testPreparedRocksDbByteOverflowRollsBackAndBatchRemainsReusable() throws Exception {
        SerializedKeyBatch<byte[], byte[]> batch =
                SerializedKeyBatch.forSerializedBytes(
                        ByteBuffer.allocateDirect(4),
                        ByteBuffer.allocateDirect(SerializedKeyBatch.METADATA_RECORD_BYTES));

        assertThrows(
                EOFException.class,
                () -> batch.appendSerialized(1, 1L, new byte[] {1, 2, 3, 4, 5}));
        assertEquals(0, batch.entryCount());
        assertEquals(0, batch.arenaBytesWritten());

        batch.appendSerialized(2, 2L, new byte[] {6, 7, 8, 9});
        assertArrayEquals(new byte[] {6, 7, 8, 9}, copyBytes(batch.arenaSlice()));
        assertEquals(2, batch.stateId(0));
    }

    @Test
    void testMultipleMutableKeysAreSerializedContiguouslyWithoutRetainingReferences()
            throws Exception {
        SerializedKeyBatch<byte[], String> batch =
                newBatch(BytePrimitiveArraySerializer.INSTANCE, StringSerializer.INSTANCE, 256, 3);
        DataOutputSerializer expected = new DataOutputSerializer(256);
        byte[] firstKey = {1, 2, 3};
        byte[] secondKey = {4, 5};

        int firstOffset = expected.length();
        BytePrimitiveArraySerializer.INSTANCE.serialize(firstKey, expected);
        StringSerializer.INSTANCE.serialize("ns-a", expected);
        int firstLength = expected.length() - firstOffset;
        batch.append(1, 11L, firstKey, "ns-a");

        firstKey[0] = 99;

        int secondOffset = expected.length();
        BytePrimitiveArraySerializer.INSTANCE.serialize(secondKey, expected);
        StringSerializer.INSTANCE.serialize("ns-b", expected);
        int secondLength = expected.length() - secondOffset;
        batch.append(2, 22L, secondKey, "ns-b");

        secondKey[1] = 88;

        assertEquals(2, batch.entryCount());
        assertEquals(firstOffset, batch.arenaOffset(0));
        assertEquals(firstLength, batch.serializedLength(0));
        assertEquals(secondOffset, batch.arenaOffset(1));
        assertEquals(secondLength, batch.serializedLength(1));
        assertArrayEquals(expected.getCopyOfBuffer(), copyBytes(batch.arenaSlice()));
        assertEquals(
                2 * SerializedKeyBatch.METADATA_RECORD_BYTES, batch.metadataSlice().remaining());
    }

    @Test
    void testCallerBufferPositionsAndLimitsArePreservedAcrossClearAndReuse() throws Exception {
        ByteBuffer callerArena = ByteBuffer.allocateDirect(48);
        callerArena.position(4);
        callerArena.limit(36);
        ByteBuffer callerMetadata = ByteBuffer.allocateDirect(80);
        callerMetadata.position(8);
        callerMetadata.limit(56);

        SerializedKeyBatch<Integer, String> batch =
                new SerializedKeyBatch<>(
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        callerArena,
                        callerMetadata);
        assertEquals(32, batch.arenaCapacityBytes());
        assertEquals(2, batch.maxEntries());
        batch.append(1, 1L, 10, "old");

        assertEquals(4, callerArena.position());
        assertEquals(36, callerArena.limit());
        assertEquals(8, callerMetadata.position());
        assertEquals(56, callerMetadata.limit());

        batch.clear();
        assertEquals(0, batch.entryCount());
        assertEquals(0, batch.arenaBytesWritten());
        assertEquals(0, batch.arenaSlice().remaining());
        assertEquals(0, batch.metadataSlice().remaining());

        batch.append(9, 99L, 20, "new");
        DataOutputSerializer expected = serializePair(IntSerializer.INSTANCE, 20, "new");
        assertEquals(1, batch.entryCount());
        assertEquals(0, batch.arenaOffset(0));
        assertEquals(9, batch.stateId(0));
        assertEquals(99L, batch.generation(0));
        assertArrayEquals(expected.getCopyOfBuffer(), copyBytes(batch.arenaSlice()));
        assertEquals(4, callerArena.position());
        assertEquals(36, callerArena.limit());
        assertEquals(8, callerMetadata.position());
        assertEquals(56, callerMetadata.limit());
    }

    @Test
    void testExactArenaCapacityThenOverflowRollsBackAllLogicalState() throws Exception {
        SerializedKeyBatch<Integer, Long> batch =
                newBatch(IntSerializer.INSTANCE, LongSerializer.INSTANCE, 24, 3);
        batch.append(1, 1L, 10, 100L);
        batch.append(2, 2L, 20, 200L);
        byte[] arenaBeforeFailure = copyBytes(batch.arenaSlice());
        byte[] metadataBeforeFailure = copyBytes(batch.metadataSlice());

        assertEquals(24, batch.arenaBytesWritten());
        assertThrows(EOFException.class, () -> batch.append(3, 3L, 30, 300L));

        assertEquals(2, batch.entryCount());
        assertEquals(24, batch.arenaBytesWritten());
        assertArrayEquals(arenaBeforeFailure, copyBytes(batch.arenaSlice()));
        assertArrayEquals(metadataBeforeFailure, copyBytes(batch.metadataSlice()));
    }

    @Test
    void testPartialNamespaceOverflowRollsArenaBackToEntryCheckpoint() {
        SerializedKeyBatch<Integer, String> batch =
                newBatch(IntSerializer.INSTANCE, StringSerializer.INSTANCE, 5, 1);

        assertThrows(EOFException.class, () -> batch.append(1, 1L, 10, "namespace"));

        assertEquals(0, batch.entryCount());
        assertEquals(0, batch.arenaBytesWritten());
        assertEquals(0, batch.arenaSlice().remaining());
        assertEquals(0, batch.metadataSlice().remaining());
    }

    @Test
    void testMetadataOverflowIsCheckedBeforeSerializersOrArena() throws Exception {
        SerializedKeyBatch<Integer, String> batch =
                newBatch(IntSerializer.INSTANCE, StringSerializer.INSTANCE, 128, 1);
        batch.append(1, 1L, 10, "first");
        byte[] arenaBeforeFailure = copyBytes(batch.arenaSlice());
        byte[] metadataBeforeFailure = copyBytes(batch.metadataSlice());

        assertThrows(EOFException.class, () -> batch.append(2, 2L, 20, "second"));

        assertEquals(1, batch.entryCount());
        assertArrayEquals(arenaBeforeFailure, copyBytes(batch.arenaSlice()));
        assertArrayEquals(metadataBeforeFailure, copyBytes(batch.metadataSlice()));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSerializerFailuresRollbackAndBatchRemainsReusable() throws Exception {
        TypeSerializer<String> namespaceSerializer = mock(TypeSerializer.class);
        when(namespaceSerializer.duplicate()).thenReturn(namespaceSerializer);
        doAnswer(
                        invocation -> {
                            String value = invocation.getArgument(0);
                            DataOutputView output = invocation.getArgument(1);
                            if ("bad-io".equals(value)) {
                                output.writeInt(0x11223344);
                                throw new IOException("expected serializer failure");
                            }
                            if ("bad-runtime".equals(value)) {
                                output.writeByte(0x55);
                                throw new IllegalStateException("expected runtime failure");
                            }
                            StringSerializer.INSTANCE.serialize(value, output);
                            return null;
                        })
                .when(namespaceSerializer)
                .serialize(anyString(), any(DataOutputView.class));

        SerializedKeyBatch<Integer, String> batch =
                newBatch(IntSerializer.INSTANCE, namespaceSerializer, 128, 3);
        batch.append(1, 1L, 10, "good");
        byte[] arenaBeforeFailure = copyBytes(batch.arenaSlice());
        byte[] metadataBeforeFailure = copyBytes(batch.metadataSlice());

        assertThrows(IOException.class, () -> batch.append(2, 2L, 20, "bad-io"));
        assertEquals(1, batch.entryCount());
        assertArrayEquals(arenaBeforeFailure, copyBytes(batch.arenaSlice()));
        assertArrayEquals(metadataBeforeFailure, copyBytes(batch.metadataSlice()));

        assertThrows(
                IllegalStateException.class, () -> batch.append(3, 3L, 30, "bad-runtime"));
        assertEquals(1, batch.entryCount());
        assertArrayEquals(arenaBeforeFailure, copyBytes(batch.arenaSlice()));
        assertArrayEquals(metadataBeforeFailure, copyBytes(batch.metadataSlice()));

        assertEquals(1, batch.append(4, 4L, 40, "recovered"));
        assertEquals(2, batch.entryCount());
        assertEquals(arenaBeforeFailure.length, batch.arenaOffset(1));
    }

    @Test
    void testRejectsNonDirectOrReadOnlyBuffersAndZeroMetadataCapacity() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SerializedKeyBatch<>(
                                IntSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                ByteBuffer.allocate(32),
                                ByteBuffer.allocateDirect(24)));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SerializedKeyBatch<>(
                                IntSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                ByteBuffer.allocateDirect(32),
                                ByteBuffer.allocate(24)));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SerializedKeyBatch<>(
                                IntSerializer.INSTANCE,
                                StringSerializer.INSTANCE,
                                ByteBuffer.allocateDirect(32),
                                ByteBuffer.allocateDirect(24).asReadOnlyBuffer()));

        SerializedKeyBatch<Integer, String> noMetadata =
                new SerializedKeyBatch<>(
                        IntSerializer.INSTANCE,
                        StringSerializer.INSTANCE,
                        ByteBuffer.allocateDirect(32),
                        ByteBuffer.allocateDirect(23));
        assertEquals(0, noMetadata.maxEntries());
        assertThrows(EOFException.class, () -> noMetadata.append(1, 1L, 1, "ns"));
        assertEquals(0, noMetadata.entryCount());
        assertEquals(0, noMetadata.arenaBytesWritten());
    }

    @Test
    void testMetadataAccessorsRejectInvisibleEntries() throws Exception {
        SerializedKeyBatch<Integer, String> batch =
                newBatch(IntSerializer.INSTANCE, StringSerializer.INSTANCE, 32, 1);

        assertThrows(IndexOutOfBoundsException.class, () -> batch.stateId(0));
        batch.append(1, 1L, 1, "ns");
        assertThrows(IndexOutOfBoundsException.class, () -> batch.generation(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> batch.serializedLength(1));
    }

    private static <K, N> SerializedKeyBatch<K, N> newBatch(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            int arenaBytes,
            int entries) {
        return new SerializedKeyBatch<>(
                keySerializer,
                namespaceSerializer,
                ByteBuffer.allocateDirect(arenaBytes),
                ByteBuffer.allocateDirect(entries * SerializedKeyBatch.METADATA_RECORD_BYTES));
    }

    private static <K> DataOutputSerializer serializePair(
            TypeSerializer<K> keySerializer, K key, String namespace) throws IOException {
        DataOutputSerializer expected = new DataOutputSerializer(128);
        keySerializer.serialize(key, expected);
        StringSerializer.INSTANCE.serialize(namespace, expected);
        return expected;
    }

    private static byte[] copyBytes(ByteBuffer source) {
        assertEquals(0, source.position());
        byte[] copy = new byte[source.remaining()];
        source.get(copy);
        return copy;
    }
}
