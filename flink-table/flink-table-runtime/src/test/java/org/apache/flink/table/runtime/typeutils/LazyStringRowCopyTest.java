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
package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration coverage for the experimental string copy through real row serializers. */
class LazyStringRowCopyTest {
    private final RowDataSerializer serializer =
            new RowDataSerializer(new LogicalType[] {new VarCharType(), new IntType()});

    @Test
    void repeatedRowCopiesRetainTheExpectedRepresentation() {
        BinaryStringData string = BinaryStringData.fromString("汉字🙂");
        GenericRowData original = GenericRowData.of(string, 42);
        original.setRowKind(RowKind.UPDATE_BEFORE);
        RowData first = serializer.copy(original);
        RowData second = serializer.copy(first);
        boolean enabled = Boolean.parseBoolean(System.getProperty(
                "flink.table.binary-string.lazy-copy.enabled",
                System.getenv("FLINK_TABLE_BINARY_STRING_LAZY_COPY_ENABLED")));
        assertThat(((BinaryStringData) second.getString(0)).getBinarySection() == null)
                .isEqualTo(enabled);
        original.setField(0, BinaryStringData.fromString("different"));
        string.setJavaObject("changed");
        assertThat(first.getString(0).toString()).isEqualTo("汉字🙂");
        assertThat(second.getString(0).toString()).isEqualTo("汉字🙂");
        assertThat(second.getInt(1)).isEqualTo(42);
        assertThat(second.getRowKind()).isEqualTo(RowKind.UPDATE_BEFORE);
    }

    @Test
    void serializedBytesAndRoundTripAreUnchanged() throws Exception {
        for (String value : new String[] {"", "abc", "汉字🙂", "\u0000\u0080", "\ud800"}) {
            BinaryStringData lazy = BinaryStringData.fromString(value);
            BinaryStringData eager = BinaryStringData.fromString(value);
            eager.ensureMaterialized();
            RowData copy = serializer.copy(GenericRowData.of(lazy, 42));
            DataOutputSerializer actual = new DataOutputSerializer(64);
            DataOutputSerializer reference = new DataOutputSerializer(64);
            serializer.serialize(copy, actual);
            serializer.serialize(GenericRowData.of(eager, 42), reference);
            assertThat(actual.getCopyOfBuffer()).containsExactly(reference.getCopyOfBuffer());
            RowData restored = serializer.deserialize(new DataInputDeserializer(actual.getCopyOfBuffer()));
            assertThat(restored.getString(0).toBytes()).containsExactly(eager.toBytes());
            assertThat(restored.getInt(1)).isEqualTo(42);
        }
    }

    @Test
    void binaryBackedRowCopyDoesNotAliasInputBytes() {
        byte[] bytes = new byte[] {'a', 'b', 'c'};
        GenericRowData original = GenericRowData.of(BinaryStringData.fromBytes(bytes), 42);
        RowData copy = serializer.copy(original);
        bytes[0] = 'x';
        assertThat(copy.getString(0).toBytes()).containsExactly((byte) 'a', (byte) 'b', (byte) 'c');
    }

    @Test
    void nullFieldsRemainNull() throws Exception {
        RowData copy = serializer.copy(GenericRowData.of(null, 42));
        assertThat(copy.isNullAt(0)).isTrue();
        DataOutputSerializer out = new DataOutputSerializer(32);
        serializer.serialize(copy, out);
        assertThat(serializer.deserialize(new DataInputDeserializer(out.getCopyOfBuffer())).isNullAt(0)).isTrue();
    }
}
