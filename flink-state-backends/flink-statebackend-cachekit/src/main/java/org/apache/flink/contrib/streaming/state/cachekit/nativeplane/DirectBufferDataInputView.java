/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.nativeplane;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Reusable {@link DataInputView} over a bounded region of direct memory.
 *
 * <p>The backing buffer is duplicated once by the constructor. {@link #reset(int, int)} then
 * selects native-arena slices without allocating a heap byte array or a new {@link ByteBuffer}.
 * Multi-byte values use Flink's big-endian serializer wire format.
 */
@Internal
public final class DirectBufferDataInputView implements DataInputView {

    private final ByteBuffer buffer;

    public DirectBufferDataInputView(ByteBuffer directBuffer) {
        Objects.requireNonNull(directBuffer, "directBuffer");
        if (!directBuffer.isDirect()) {
            throw new IllegalArgumentException("A direct ByteBuffer is required.");
        }
        this.buffer = directBuffer.duplicate().order(ByteOrder.BIG_ENDIAN);
        this.buffer.position(0);
        this.buffer.limit(0);
    }

    /** Selects {@code offset..offset+length} in the fixed backing arena. */
    public void reset(int offset, int length) {
        if ((offset | length) < 0 || offset > buffer.capacity() - length) {
            throw new IndexOutOfBoundsException(
                    "offset=" + offset + ", length=" + length + ", capacity=" + buffer.capacity());
        }
        buffer.limit(buffer.capacity());
        buffer.position(offset);
        buffer.limit(offset + length);
    }

    public int remaining() {
        return buffer.remaining();
    }

    @Override
    public void readFully(byte[] target) throws IOException {
        readFully(target, 0, target.length);
    }

    @Override
    public void readFully(byte[] target, int offset, int length) throws IOException {
        Objects.requireNonNull(target, "target");
        if ((offset | length) < 0 || offset > target.length - length) {
            throw new IndexOutOfBoundsException();
        }
        require(length);
        buffer.get(target, offset, length);
    }

    @Override
    public int skipBytes(int count) {
        int skipped = Math.min(Math.max(count, 0), buffer.remaining());
        buffer.position(buffer.position() + skipped);
        return skipped;
    }

    @Override
    public void skipBytesToRead(int count) throws IOException {
        require(count);
        buffer.position(buffer.position() + count);
    }

    @Override
    public int read(byte[] target, int offset, int length) {
        Objects.requireNonNull(target, "target");
        if ((offset | length) < 0 || offset > target.length - length) {
            throw new IndexOutOfBoundsException();
        }
        if (length == 0) {
            return 0;
        }
        if (!buffer.hasRemaining()) {
            return -1;
        }
        int actual = Math.min(length, buffer.remaining());
        buffer.get(target, offset, actual);
        return actual;
    }

    @Override
    public int read(byte[] target) {
        return read(target, 0, target.length);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return readUnsignedByte() != 0;
    }

    @Override
    public byte readByte() throws IOException {
        require(Byte.BYTES);
        return buffer.get();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return readByte() & 0xff;
    }

    @Override
    public short readShort() throws IOException {
        require(Short.BYTES);
        return buffer.getShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return readShort() & 0xffff;
    }

    @Override
    public char readChar() throws IOException {
        require(Character.BYTES);
        return buffer.getChar();
    }

    @Override
    public int readInt() throws IOException {
        require(Integer.BYTES);
        return buffer.getInt();
    }

    @Override
    public long readLong() throws IOException {
        require(Long.BYTES);
        return buffer.getLong();
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    @SuppressWarnings("deprecation")
    public String readLine() throws IOException {
        if (!buffer.hasRemaining()) {
            return null;
        }
        StringBuilder line = new StringBuilder();
        while (buffer.hasRemaining()) {
            int value = readUnsignedByte();
            if (value == '\n') {
                break;
            }
            if (value == '\r') {
                if (buffer.hasRemaining()) {
                    int next = readUnsignedByte();
                    if (next != '\n') {
                        buffer.position(buffer.position() - 1);
                    }
                }
                break;
            }
            line.append((char) value);
        }
        return line.toString();
    }

    @Override
    public String readUTF() throws IOException {
        return DataInputStream.readUTF(this);
    }

    private void require(int bytes) throws EOFException {
        if (bytes < 0) {
            throw new IllegalArgumentException("Number of bytes must not be negative.");
        }
        if (buffer.remaining() < bytes) {
            throw new EOFException(
                    "Requested " + bytes + " bytes with " + buffer.remaining() + " remaining.");
        }
    }
}
