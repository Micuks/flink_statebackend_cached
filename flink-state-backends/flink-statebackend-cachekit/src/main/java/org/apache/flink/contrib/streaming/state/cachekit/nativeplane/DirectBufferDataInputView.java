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
import org.apache.flink.core.memory.DataInputView;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Reusable {@link DataInputView} over regions of one direct buffer.
 *
 * <p>Changing the visible region and reading values never changes the caller buffer's position or
 * limit. Multi-byte values use Flink's big-endian serialization order. The view is not thread-safe.
 */
@Internal
public final class DirectBufferDataInputView implements DataInputView {

    private final ByteBuffer buffer;
    private int position;
    private int end;

    public DirectBufferDataInputView(ByteBuffer directBuffer) {
        Objects.requireNonNull(directBuffer, "directBuffer");
        if (!directBuffer.isDirect()) {
            throw new IllegalArgumentException("A direct ByteBuffer is required.");
        }
        this.buffer = directBuffer.duplicate().order(ByteOrder.BIG_ENDIAN);
        setRegion(0, directBuffer.capacity());
    }

    /** Selects an absolute region without creating a buffer view object. */
    public void setRegion(int offset, int length) {
        if (offset < 0 || length < 0 || offset > buffer.capacity() - length) {
            throw new IndexOutOfBoundsException(
                    "Region offset="
                            + offset
                            + ", length="
                            + length
                            + ", capacity="
                            + buffer.capacity());
        }
        position = offset;
        end = offset + length;
    }

    public int available() {
        return end - position;
    }

    public int position() {
        return position;
    }

    @Override
    public void readFully(byte[] destination) throws IOException {
        readFully(destination, 0, destination.length);
    }

    @Override
    public void readFully(byte[] destination, int offset, int length) throws IOException {
        Objects.requireNonNull(destination, "destination");
        if ((offset | length) < 0 || offset > destination.length - length) {
            throw new IndexOutOfBoundsException();
        }
        require(length);
        ByteBuffer source = buffer.duplicate();
        source.position(position);
        source.get(destination, offset, length);
        position += length;
    }

    @Override
    public int skipBytes(int numBytes) {
        if (numBytes <= 0) {
            return 0;
        }
        int skipped = Math.min(numBytes, available());
        position += skipped;
        return skipped;
    }

    @Override
    public boolean readBoolean() throws IOException {
        return readByte() != 0;
    }

    @Override
    public byte readByte() throws IOException {
        require(Byte.BYTES);
        return buffer.get(position++);
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return readByte() & 0xff;
    }

    @Override
    public short readShort() throws IOException {
        require(Short.BYTES);
        short value = buffer.getShort(position);
        position += Short.BYTES;
        return value;
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return readShort() & 0xffff;
    }

    @Override
    public char readChar() throws IOException {
        require(Character.BYTES);
        char value = buffer.getChar(position);
        position += Character.BYTES;
        return value;
    }

    @Override
    public int readInt() throws IOException {
        require(Integer.BYTES);
        int value = buffer.getInt(position);
        position += Integer.BYTES;
        return value;
    }

    @Override
    public long readLong() throws IOException {
        require(Long.BYTES);
        long value = buffer.getLong(position);
        position += Long.BYTES;
        return value;
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    @Nullable
    @Override
    public String readLine() throws IOException {
        if (available() == 0) {
            return null;
        }
        StringBuilder line = new StringBuilder();
        while (available() > 0) {
            char next = (char) readUnsignedByte();
            if (next == '\n') {
                break;
            }
            if (next == '\r') {
                if (available() > 0 && (buffer.get(position) & 0xff) == '\n') {
                    position++;
                }
                break;
            }
            line.append(next);
        }
        return line.toString();
    }

    @Override
    public String readUTF() throws IOException {
        return DataInputStream.readUTF(this);
    }

    @Override
    public void skipBytesToRead(int numBytes) throws IOException {
        if (numBytes < 0) {
            throw new IllegalArgumentException("Number of bytes must not be negative.");
        }
        if (skipBytes(numBytes) != numBytes) {
            throw new EOFException("Could not skip " + numBytes + " bytes.");
        }
    }

    @Override
    public int read(byte[] destination, int offset, int length) {
        Objects.requireNonNull(destination, "destination");
        if ((offset | length) < 0 || offset > destination.length - length) {
            throw new IndexOutOfBoundsException();
        }
        if (length == 0) {
            return 0;
        }
        if (available() == 0) {
            return -1;
        }
        int copied = Math.min(length, available());
        ByteBuffer source = buffer.duplicate();
        source.position(position);
        source.get(destination, offset, copied);
        position += copied;
        return copied;
    }

    @Override
    public int read(byte[] destination) {
        return read(destination, 0, destination.length);
    }

    private void require(int numBytes) throws EOFException {
        if (numBytes < 0 || available() < numBytes) {
            throw new EOFException(
                    "Direct input underflow: requested "
                            + numBytes
                            + " bytes with "
                            + available()
                            + " remaining.");
        }
    }
}
