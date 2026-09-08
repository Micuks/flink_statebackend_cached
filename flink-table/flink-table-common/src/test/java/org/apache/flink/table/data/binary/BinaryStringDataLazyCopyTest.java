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
package org.apache.flink.table.data.binary;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Run in separate JVMs with the experimental property false and true. */
class BinaryStringDataLazyCopyTest {
    private static final boolean ENABLED =
            Boolean.parseBoolean(
                    System.getProperty(
                            "flink.table.binary-string.lazy-copy.enabled",
                            System.getenv("FLINK_TABLE_BINARY_STRING_LAZY_COPY_ENABLED")));

    @Test
    void preservesLazyRepresentationOnlyWhenEnabled() {
        BinaryStringData source = BinaryStringData.fromString("hello");
        BinaryStringData copy = source.copy();
        assertThat(copy).isNotSameAs(source);
        assertThat(copy.getJavaObject()).isSameAs(source.getJavaObject());
        assertThat(source.getBinarySection() == null).isEqualTo(ENABLED);
        assertThat(copy.getBinarySection() == null).isEqualTo(ENABLED);
    }

    @Test
    void javaWrappersAreIndependent() {
        BinaryStringData source = BinaryStringData.fromString("original");
        BinaryStringData copy = source.copy();
        source.setJavaObject("changed");
        assertThat(copy.getJavaObject()).isEqualTo("original");
        copy.setJavaObject("copy changed");
        assertThat(source.getJavaObject()).isEqualTo("changed");
    }

    @Test
    void laterMaterializationDoesNotShareMutableBytes() {
        BinaryStringData source = BinaryStringData.fromString("abcd");
        BinaryStringData copy = source.copy();
        source.ensureMaterialized();
        copy.ensureMaterialized();
        source.getSegments()[0].put(0, (byte) 'x');
        assertThat(copy.toBytes()).containsExactly((byte) 'a', (byte) 'b', (byte) 'c', (byte) 'd');
        copy.getSegments()[0].put(1, (byte) 'y');
        assertThat(source.toBytes()).containsExactly((byte) 'x', (byte) 'b', (byte) 'c', (byte) 'd');
    }

    @Test
    void binaryBackedCopyRemainsDeepAndRespectsOffset() {
        byte[] bytes = new byte[] {0, 'a', 'b', 'c', 0};
        BinaryStringData source = BinaryStringData.fromBytes(bytes, 1, 3);
        BinaryStringData copy = source.copy();
        bytes[1] = 'x';
        assertThat(copy.toBytes()).containsExactly((byte) 'a', (byte) 'b', (byte) 'c');
        assertThat(copy.getOffset()).isZero();
    }

    @Test
    void materializedWithJavaObjectStillCopiesBinaryBacking() {
        BinaryStringData source = BinaryStringData.fromString("abc");
        source.ensureMaterialized();
        source.getSegments()[0].put(0, (byte) 'x');
        BinaryStringData copy = source.copy();
        assertThat(copy.toBytes()).containsExactly((byte) 'x', (byte) 'b', (byte) 'c');
        assertThat(copy.getJavaObject()).isEqualTo("abc");
        source.getSegments()[0].put(0, (byte) 'y');
        assertThat(copy.toBytes()[0]).isEqualTo((byte) 'x');
    }

    @Test
    void segmentedBackingRemainsDeep() {
        byte[] first = new byte[] {0, 'a', 'b'};
        byte[] second = new byte[] {'c', 'd', 0};
        BinaryStringData source =
                BinaryStringData.fromAddress(
                        new MemorySegment[] {
                            MemorySegmentFactory.wrap(first), MemorySegmentFactory.wrap(second)
                        },
                        1,
                        4);
        BinaryStringData copy = source.copy();
        first[1] = 'x';
        second[0] = 'y';
        assertThat(copy.toBytes()).containsExactly((byte) 'a', (byte) 'b', (byte) 'c', (byte) 'd');
    }

    @Test
    void encodingAndHashMatchOriginalMaterializingCopy() {
        for (String value : new String[] {"", "abc", "汉字🙂", "\u0000\u007f\u0080", "\ud800", "\udc00", "x\ud800y"}) {
            BinaryStringData source = BinaryStringData.fromString(value);
            BinaryStringData first = source.copy();
            BinaryStringData second = first.copy();
            BinaryStringData materialized = BinaryStringData.fromString(value);
            materialized.ensureMaterialized();
            BinaryStringData reference = materialized.copy();
            assertThat(first.toBytes()).containsExactly(reference.toBytes());
            assertThat(second.toBytes()).containsExactly(reference.toBytes());
            assertThat(first.hashCode()).isEqualTo(reference.hashCode());
            assertThat(first.equals(reference)).isTrue();
            assertThat(first.compareTo(reference)).isZero();
            assertThat(first.toString()).isEqualTo(value);
        }
        assertThat(BinaryStringData.fromString(null)).isNull();
    }
}
