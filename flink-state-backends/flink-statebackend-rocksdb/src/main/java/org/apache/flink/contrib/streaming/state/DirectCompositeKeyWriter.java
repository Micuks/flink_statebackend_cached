/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;

import java.io.IOException;
import java.util.Objects;

/** Reusable exact-key serializer that writes into a caller-owned output. */
final class DirectCompositeKeyWriter<K> {

    private final TypeSerializer<K> keySerializer;
    private final int keyGroupPrefixBytes;
    private final CountingDataOutputView countingOutput = new CountingDataOutputView();

    DirectCompositeKeyWriter(TypeSerializer<K> keySerializer, int keyGroupPrefixBytes) {
        this.keySerializer = keySerializer.duplicate();
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
    }

    <N> void write(
            K key,
            int keyGroup,
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            DataOutputView target)
            throws IOException {
        countingOutput.reset(target);
        CompositeKeySerializationUtils.writeKeyGroup(keyGroup, keyGroupPrefixBytes, countingOutput);
        boolean ambiguous =
                CompositeKeySerializationUtils.isAmbiguousKeyPossible(
                        keySerializer, namespaceSerializer);
        int keyStart = countingOutput.bytesWritten();
        keySerializer.serialize(key, countingOutput);
        if (ambiguous) {
            CompositeKeySerializationUtils.writeVariableIntBytes(
                    countingOutput.bytesWritten() - keyStart, countingOutput);
        }
        int namespaceStart = countingOutput.bytesWritten();
        namespaceSerializer.serialize(namespace, countingOutput);
        if (ambiguous) {
            CompositeKeySerializationUtils.writeVariableIntBytes(
                    countingOutput.bytesWritten() - namespaceStart, countingOutput);
        }
    }

    /** Reusable byte-counting adapter; it never owns or retains serialized payload. */
    private static final class CountingDataOutputView implements DataOutputView {
        private DataOutputView target;
        private int bytesWritten;

        private void reset(DataOutputView target) {
            this.target = Objects.requireNonNull(target, "target");
            bytesWritten = 0;
        }

        private int bytesWritten() {
            return bytesWritten;
        }

        @Override
        public void write(int value) throws IOException {
            target.write(value);
            bytesWritten++;
        }

        @Override
        public void write(byte[] source) throws IOException {
            target.write(source);
            bytesWritten = Math.addExact(bytesWritten, source.length);
        }

        @Override
        public void write(byte[] source, int offset, int length) throws IOException {
            target.write(source, offset, length);
            bytesWritten = Math.addExact(bytesWritten, length);
        }

        @Override
        public void writeBoolean(boolean value) throws IOException {
            target.writeBoolean(value);
            bytesWritten++;
        }

        @Override
        public void writeByte(int value) throws IOException {
            target.writeByte(value);
            bytesWritten++;
        }

        @Override
        public void writeShort(int value) throws IOException {
            target.writeShort(value);
            bytesWritten = Math.addExact(bytesWritten, Short.BYTES);
        }

        @Override
        public void writeChar(int value) throws IOException {
            target.writeChar(value);
            bytesWritten = Math.addExact(bytesWritten, Character.BYTES);
        }

        @Override
        public void writeInt(int value) throws IOException {
            target.writeInt(value);
            bytesWritten = Math.addExact(bytesWritten, Integer.BYTES);
        }

        @Override
        public void writeLong(long value) throws IOException {
            target.writeLong(value);
            bytesWritten = Math.addExact(bytesWritten, Long.BYTES);
        }

        @Override
        public void writeFloat(float value) throws IOException {
            target.writeFloat(value);
            bytesWritten = Math.addExact(bytesWritten, Float.BYTES);
        }

        @Override
        public void writeDouble(double value) throws IOException {
            target.writeDouble(value);
            bytesWritten = Math.addExact(bytesWritten, Double.BYTES);
        }

        @Override
        public void writeBytes(String value) throws IOException {
            target.writeBytes(value);
            bytesWritten = Math.addExact(bytesWritten, value.length());
        }

        @Override
        public void writeChars(String value) throws IOException {
            target.writeChars(value);
            bytesWritten =
                    Math.addExact(
                            bytesWritten, Math.multiplyExact(value.length(), Character.BYTES));
        }

        @Override
        public void writeUTF(String value) throws IOException {
            target.writeUTF(value);
            int encodedBytes = 0;
            for (int index = 0; index < value.length(); index++) {
                int character = value.charAt(index);
                encodedBytes +=
                        character >= 0x0001 && character <= 0x007f ? 1 : character > 0x07ff ? 3 : 2;
            }
            bytesWritten = Math.addExact(bytesWritten, Math.addExact(Short.BYTES, encodedBytes));
        }

        @Override
        public void skipBytesToWrite(int numBytes) throws IOException {
            target.skipBytesToWrite(numBytes);
            bytesWritten = Math.addExact(bytesWritten, numBytes);
        }

        @Override
        public void write(DataInputView source, int numBytes) throws IOException {
            target.write(source, numBytes);
            bytesWritten = Math.addExact(bytesWritten, numBytes);
        }
    }
}
