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

import org.apache.flink.annotation.Internal;
import org.apache.flink.contrib.streaming.state.PositionedDataOutputView;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * A fixed-capacity {@link DataOutputView} over direct memory.
 *
 * <p>The constructor takes the caller buffer's remaining region and creates a relative view over
 * it. Writes start at relative position zero and never change the caller buffer's position or
 * limit. All multi-byte values use the big-endian encoding required by {@link java.io.DataOutput}.
 *
 * <p>This class is not thread-safe. A successful write performs no per-field heap allocation.
 * Methods that return a {@link ByteBuffer} create a view object but do not copy payload bytes.
 */
@Internal
public final class DirectBufferDataOutputView implements PositionedDataOutputView {

    private final ByteBuffer buffer;

    /**
     * Creates an output view over {@code directBuffer.position()..directBuffer.limit()}.
     *
     * @throws IllegalArgumentException if the buffer is not direct or is read-only
     */
    public DirectBufferDataOutputView(ByteBuffer directBuffer) {
        Objects.requireNonNull(directBuffer, "directBuffer");
        if (!directBuffer.isDirect()) {
            throw new IllegalArgumentException("A direct ByteBuffer is required.");
        }
        if (directBuffer.isReadOnly()) {
            throw new IllegalArgumentException("The direct ByteBuffer must be writable.");
        }
        this.buffer = directBuffer.slice().order(ByteOrder.BIG_ENDIAN);
    }

    /** Returns the number of bytes written since construction or the last {@link #reset()}. */
    @Override
    public int position() {
        return buffer.position();
    }

    /** Returns the fixed capacity of this relative view. */
    public int capacity() {
        return buffer.capacity();
    }

    /** Returns the number of bytes that can still be written. */
    public int remaining() {
        return buffer.remaining();
    }

    /**
     * Rewinds this view to relative position zero without zeroing or otherwise changing memory.
     */
    public void reset() {
        buffer.clear();
    }

    int checkpoint() {
        return buffer.position();
    }

    void truncateTo(int checkpoint) {
        if (checkpoint < 0 || checkpoint > buffer.position()) {
            throw new IllegalArgumentException(
                    "Checkpoint "
                            + checkpoint
                            + " is outside written range [0, "
                            + buffer.position()
                            + "].");
        }
        buffer.position(checkpoint);
    }

    /**
     * Returns a read-only, big-endian, zero-position slice of the bytes written so far.
     *
     * <p>The returned view shares direct memory with this output. Subsequent writes or reset may
     * overwrite its contents. Calling this method does not change this output's position.
     */
    public ByteBuffer writtenSlice() {
        ByteBuffer written = buffer.asReadOnlyBuffer().order(ByteOrder.BIG_ENDIAN);
        written.position(0);
        written.limit(buffer.position());
        return written.slice().asReadOnlyBuffer().order(ByteOrder.BIG_ENDIAN);
    }

    @Override
    public void write(int value) throws IOException {
        ensureWritable(1);
        buffer.put((byte) value);
    }

    @Override
    public void write(byte[] source) throws IOException {
        write(source, 0, source.length);
    }

    @Override
    public void write(byte[] source, int offset, int length) throws IOException {
        Objects.requireNonNull(source, "source");
        if ((offset | length) < 0 || offset > source.length - length) {
            throw new IndexOutOfBoundsException(
                    "offset=" + offset + ", length=" + length + ", size=" + source.length);
        }
        ensureWritable(length);
        buffer.put(source, offset, length);
    }

    @Override
    public void writeBoolean(boolean value) throws IOException {
        write(value ? 1 : 0);
    }

    @Override
    public void writeByte(int value) throws IOException {
        write(value);
    }

    @Override
    public void writeShort(int value) throws IOException {
        ensureWritable(Short.BYTES);
        buffer.putShort((short) value);
    }

    @Override
    public void writeChar(int value) throws IOException {
        ensureWritable(Character.BYTES);
        buffer.putChar((char) value);
    }

    @Override
    public void writeInt(int value) throws IOException {
        ensureWritable(Integer.BYTES);
        buffer.putInt(value);
    }

    @Override
    public void writeLong(long value) throws IOException {
        ensureWritable(Long.BYTES);
        buffer.putLong(value);
    }

    @Override
    public void writeFloat(float value) throws IOException {
        writeInt(Float.floatToIntBits(value));
    }

    @Override
    public void writeDouble(double value) throws IOException {
        writeLong(Double.doubleToLongBits(value));
    }

    @Override
    public void writeBytes(String value) throws IOException {
        int length = value.length();
        ensureWritable(length);
        for (int index = 0; index < length; index++) {
            buffer.put((byte) value.charAt(index));
        }
    }

    @Override
    public void writeChars(String value) throws IOException {
        int length = value.length();
        if (length > Integer.MAX_VALUE / Character.BYTES) {
            throw new EOFException("Character sequence is too large.");
        }
        ensureWritable(length * Character.BYTES);
        for (int index = 0; index < length; index++) {
            buffer.putChar(value.charAt(index));
        }
    }

    @Override
    public void writeUTF(String value) throws IOException {
        int characterCount = value.length();
        int utfLength = 0;
        for (int index = 0; index < characterCount; index++) {
            int character = value.charAt(index);
            if (character >= 0x0001 && character <= 0x007f) {
                utfLength++;
            } else if (character > 0x07ff) {
                utfLength += 3;
            } else {
                utfLength += 2;
            }
            if (utfLength > 65535) {
                throw new UTFDataFormatException("Encoded string is too long: " + utfLength);
            }
        }

        ensureWritable(utfLength + Short.BYTES);
        buffer.putShort((short) utfLength);
        for (int index = 0; index < characterCount; index++) {
            int character = value.charAt(index);
            if (character >= 0x0001 && character <= 0x007f) {
                buffer.put((byte) character);
            } else if (character > 0x07ff) {
                buffer.put((byte) (0xe0 | ((character >> 12) & 0x0f)));
                buffer.put((byte) (0x80 | ((character >> 6) & 0x3f)));
                buffer.put((byte) (0x80 | (character & 0x3f)));
            } else {
                buffer.put((byte) (0xc0 | ((character >> 6) & 0x1f)));
                buffer.put((byte) (0x80 | (character & 0x3f)));
            }
        }
    }

    @Override
    public void skipBytesToWrite(int numBytes) throws IOException {
        ensureWritable(numBytes);
        buffer.position(buffer.position() + numBytes);
    }

    @Override
    public void write(DataInputView source, int numBytes) throws IOException {
        Objects.requireNonNull(source, "source");
        ensureWritable(numBytes);

        int startPosition = buffer.position();
        try {
            for (int index = 0; index < numBytes; index++) {
                buffer.put(source.readByte());
            }
        } catch (IOException | RuntimeException failure) {
            buffer.position(startPosition);
            throw failure;
        }
    }

    private void ensureWritable(int numBytes) throws EOFException {
        if (numBytes < 0) {
            throw new IllegalArgumentException("Number of bytes must not be negative.");
        }
        if (buffer.remaining() < numBytes) {
            throw new EOFException(
                    "Direct buffer overflow: requested "
                            + numBytes
                            + " bytes with "
                            + buffer.remaining()
                            + " remaining.");
        }
    }
}
