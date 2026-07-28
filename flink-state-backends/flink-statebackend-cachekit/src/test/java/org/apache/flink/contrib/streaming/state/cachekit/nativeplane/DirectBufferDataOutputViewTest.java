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
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DirectBufferDataOutputViewTest {

    @Test
    void testPrimitiveEncodingMatchesDataOutputSerializer() throws Exception {
        DataOutputSerializer expected = new DataOutputSerializer(256);
        DirectBufferDataOutputView actual =
                new DirectBufferDataOutputView(ByteBuffer.allocateDirect(256));

        writePrimitiveSequence(expected);
        writePrimitiveSequence(actual);

        assertArrayEquals(expected.getCopyOfBuffer(), copyWrittenBytes(actual));
    }

    @Test
    void testExistingTypeSerializersWriteIdenticalBytes() throws Exception {
        assertSerializerMatches(IntSerializer.INSTANCE, Integer.MIN_VALUE);
        assertSerializerMatches(LongSerializer.INSTANCE, Long.MAX_VALUE);
        assertSerializerMatches(StringSerializer.INSTANCE, "ascii-\u0000-\u20ac-\ud83d\ude80");
        assertSerializerMatches(
                BytePrimitiveArraySerializer.INSTANCE,
                new byte[] {Byte.MIN_VALUE, -1, 0, 1, Byte.MAX_VALUE});
    }

    @Test
    void testTypeSerializerCopyCanTargetDirectBuffer() throws Exception {
        byte[] value = {5, 4, 3, 2, 1};
        DataOutputSerializer serialized = new DataOutputSerializer(32);
        BytePrimitiveArraySerializer.INSTANCE.serialize(value, serialized);

        DataOutputSerializer expected = new DataOutputSerializer(32);
        DirectBufferDataOutputView actual =
                new DirectBufferDataOutputView(ByteBuffer.allocateDirect(32));
        BytePrimitiveArraySerializer.INSTANCE.copy(
                new DataInputDeserializer(serialized.getCopyOfBuffer()), expected);
        BytePrimitiveArraySerializer.INSTANCE.copy(
                new DataInputDeserializer(serialized.getCopyOfBuffer()), actual);

        assertArrayEquals(expected.getCopyOfBuffer(), copyWrittenBytes(actual));
    }

    @Test
    void testModifiedUtfMatchesDataOutputSerializer() throws Exception {
        String[] values = {
            "",
            "plain-ascii",
            "\u0000",
            "\u0001\u007f\u0080\u07ff\u0800\uffff",
            "surrogate-\ud83d\ude80"
        };
        DataOutputSerializer expected = new DataOutputSerializer(128);
        DirectBufferDataOutputView actual =
                new DirectBufferDataOutputView(ByteBuffer.allocateDirect(128));

        for (String value : values) {
            expected.clear();
            actual.reset();
            expected.writeUTF(value);
            actual.writeUTF(value);
            assertArrayEquals(
                    expected.getCopyOfBuffer(),
                    copyWrittenBytes(actual),
                    "modified UTF mismatch for " + value);
        }
    }

    @Test
    void testInputCopyAndSkipMatchDataOutputSerializer() throws Exception {
        byte[] payload = {9, 8, 7, 6, 5};
        DataOutputSerializer expected = new DataOutputSerializer(64);
        Arrays.fill(expected.getSharedBuffer(), (byte) 0x5a);

        ByteBuffer directBuffer = ByteBuffer.allocateDirect(64);
        while (directBuffer.hasRemaining()) {
            directBuffer.put((byte) 0x5a);
        }
        directBuffer.clear();
        DirectBufferDataOutputView actual = new DirectBufferDataOutputView(directBuffer);

        expected.writeInt(0x01020304);
        actual.writeInt(0x01020304);
        expected.skipBytesToWrite(3);
        actual.skipBytesToWrite(3);
        expected.write(new DataInputDeserializer(payload), payload.length);
        actual.write(new DataInputDeserializer(payload), payload.length);
        expected.writeByte(0xee);
        actual.writeByte(0xee);

        assertEquals(expected.length(), actual.position());
        assertArrayEquals(expected.getCopyOfBuffer(), copyWrittenBytes(actual));
    }

    @Test
    void testWriteBytesUsesLowEightBitsWithoutTemporaryEncoding() throws Exception {
        DirectBufferDataOutputView output =
                new DirectBufferDataOutputView(ByteBuffer.allocateDirect(4));

        output.writeBytes("\u0000\u007f\u0080\u20ac");

        assertArrayEquals(new byte[] {0, 0x7f, (byte) 0x80, (byte) 0xac}, copyWrittenBytes(output));
    }

    @Test
    void testRelativeRegionResetAndWrittenSliceSemantics() throws Exception {
        ByteBuffer caller = ByteBuffer.allocateDirect(16).order(ByteOrder.LITTLE_ENDIAN);
        caller.position(4);
        caller.limit(12);
        DirectBufferDataOutputView output = new DirectBufferDataOutputView(caller);

        assertEquals(0, output.position());
        assertEquals(8, output.capacity());
        assertEquals(8, output.remaining());
        output.writeInt(0x01020304);

        assertEquals(4, caller.position());
        assertEquals(12, caller.limit());
        assertEquals(4, output.position());
        assertArrayEquals(new byte[] {1, 2, 3, 4}, copyWrittenBytes(output));

        output.reset();
        assertEquals(0, output.position());
        assertEquals(8, output.remaining());
        output.writeShort(0xa1b2);
        assertArrayEquals(new byte[] {(byte) 0xa1, (byte) 0xb2}, copyWrittenBytes(output));

        assertEquals((byte) 0xa1, caller.get(4));
        assertEquals((byte) 0xb2, caller.get(5));
    }

    @Test
    void testExactCapacityAndOverflowAreFailClosed() throws Exception {
        DirectBufferDataOutputView output =
                new DirectBufferDataOutputView(ByteBuffer.allocateDirect(Long.BYTES));

        output.writeLong(0x0102030405060708L);
        assertEquals(Long.BYTES, output.position());
        assertEquals(0, output.remaining());

        assertThrows(EOFException.class, () -> output.writeByte(1));
        assertEquals(Long.BYTES, output.position());
        assertArrayEquals(
                new byte[] {1, 2, 3, 4, 5, 6, 7, 8}, copyWrittenBytes(output));
    }

    @Test
    void testBulkSkipAndUtfOverflowDoNotAdvancePositionOrConsumeInput() throws Exception {
        DirectBufferDataOutputView output =
                new DirectBufferDataOutputView(ByteBuffer.allocateDirect(4));
        output.writeByte(0x5a);

        assertThrows(EOFException.class, () -> output.write(new byte[4]));
        assertEquals(1, output.position());
        assertThrows(EOFException.class, () -> output.skipBytesToWrite(4));
        assertEquals(1, output.position());
        assertThrows(EOFException.class, () -> output.writeUTF("abcd"));
        assertEquals(1, output.position());

        DataInputDeserializer source = new DataInputDeserializer(new byte[] {9, 8, 7, 6});
        assertThrows(EOFException.class, () -> output.write(source, 4));
        assertEquals(1, output.position());
        assertEquals(9, source.readUnsignedByte(), "overflow must be checked before source reads");
        assertArrayEquals(new byte[] {0x5a}, copyWrittenBytes(output));
    }

    @Test
    void testSourceFailureRollsBackLogicalDestinationPosition() {
        DirectBufferDataOutputView output =
                new DirectBufferDataOutputView(ByteBuffer.allocateDirect(4));
        DataInputDeserializer shortSource = new DataInputDeserializer(new byte[] {1, 2});

        assertThrows(EOFException.class, () -> output.write(shortSource, 3));
        assertEquals(0, output.position());
        assertEquals(4, output.remaining());
    }

    @Test
    void testUtfExactBoundaryAndMaximumLength() throws Exception {
        DirectBufferDataOutputView exact =
                new DirectBufferDataOutputView(ByteBuffer.allocateDirect(7));
        exact.writeUTF("\u0000abc");
        assertEquals(7, exact.position());

        char[] maximumChars = new char[65535];
        Arrays.fill(maximumChars, 'a');
        String maximum = new String(maximumChars);
        DirectBufferDataOutputView maximumOutput =
                new DirectBufferDataOutputView(ByteBuffer.allocateDirect(65537));
        DataOutputSerializer expected = new DataOutputSerializer(65537);
        maximumOutput.writeUTF(maximum);
        expected.writeUTF(maximum);
        assertArrayEquals(expected.getCopyOfBuffer(), copyWrittenBytes(maximumOutput));

        DirectBufferDataOutputView tooLongOutput =
                new DirectBufferDataOutputView(ByteBuffer.allocateDirect(8));
        assertThrows(UTFDataFormatException.class, () -> tooLongOutput.writeUTF(maximum + "b"));
        assertEquals(0, tooLongOutput.position());
    }

    @Test
    void testRejectsHeapAndReadOnlyBuffers() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new DirectBufferDataOutputView(ByteBuffer.allocate(8)));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new DirectBufferDataOutputView(
                                ByteBuffer.allocateDirect(8).asReadOnlyBuffer()));
        assertThrows(NullPointerException.class, () -> new DirectBufferDataOutputView(null));
    }

    @Test
    void testRejectsInvalidLengthsWithoutChangingPosition() throws Exception {
        DirectBufferDataOutputView output =
                new DirectBufferDataOutputView(ByteBuffer.allocateDirect(8));
        output.writeByte(7);

        assertThrows(IllegalArgumentException.class, () -> output.skipBytesToWrite(-1));
        assertThrows(
                IllegalArgumentException.class,
                () -> output.write(new DataInputDeserializer(new byte[] {1}), -1));
        assertThrows(IndexOutOfBoundsException.class, () -> output.write(new byte[2], 1, 2));

        assertEquals(1, output.position());
        assertFalse(output.writtenSlice().hasArray());
    }

    private static void writePrimitiveSequence(DataOutputView output) throws IOException {
        output.write(0x123);
        output.write(new byte[] {1, 2, 3});
        output.write(new byte[] {4, 5, 6, 7}, 1, 2);
        output.writeBoolean(true);
        output.writeBoolean(false);
        output.writeByte(0xfe);
        output.writeShort(0x89ab);
        output.writeChar('\u20ac');
        output.writeInt(0x89abcdef);
        output.writeLong(0x0123456789abcdefL);
        output.writeFloat(Float.intBitsToFloat(0x7fa12345));
        output.writeDouble(Double.longBitsToDouble(0x7ff123456789abcdL));
        output.writeChars("A\u20ac");
    }

    private static <T> void assertSerializerMatches(TypeSerializer<T> serializer, T value)
            throws IOException {
        DataOutputSerializer expected = new DataOutputSerializer(128);
        DirectBufferDataOutputView actual =
                new DirectBufferDataOutputView(ByteBuffer.allocateDirect(128));

        serializer.serialize(value, expected);
        serializer.serialize(value, actual);

        assertArrayEquals(expected.getCopyOfBuffer(), copyWrittenBytes(actual));
    }

    private static byte[] copyWrittenBytes(DirectBufferDataOutputView output) {
        ByteBuffer written = output.writtenSlice();
        assertTrue(written.isDirect());
        assertTrue(written.isReadOnly());
        assertEquals(ByteOrder.BIG_ENDIAN, written.order());
        assertEquals(0, written.position());
        byte[] result = new byte[written.remaining()];
        written.get(result);
        return result;
    }
}
