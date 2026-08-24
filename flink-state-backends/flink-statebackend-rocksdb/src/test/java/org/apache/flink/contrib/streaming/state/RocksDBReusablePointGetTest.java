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

import org.junit.Rule;
import org.junit.Test;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RocksDBReusablePointGet}. */
public class RocksDBReusablePointGetTest {

    @Rule public final RocksDBResource rocksDBResource = new RocksDBResource();

    @Test
    public void returnsNotFoundWithoutReplacingTheBuffer() throws Exception {
        final RocksDBReusablePointGet pointGet = new RocksDBReusablePointGet(8);
        final byte[] initialBuffer = pointGet.buffer();

        assertThat(read(pointGet, bytes("missing"))).isEqualTo(RocksDB.NOT_FOUND);
        assertThat(pointGet.buffer()).isSameAs(initialBuffer);
    }

    @Test
    public void acceptsZeroLengthAndExactCapacityValues() throws Exception {
        final RocksDBReusablePointGet pointGet = new RocksDBReusablePointGet(8);
        final byte[] initialBuffer = pointGet.buffer();

        write(bytes("empty"), new byte[0]);
        assertThat(read(pointGet, bytes("empty"))).isZero();
        assertThat(pointGet.buffer()).isSameAs(initialBuffer);

        final byte[] exact = sequence(8);
        write(bytes("exact"), exact);
        assertThat(read(pointGet, bytes("exact"))).isEqualTo(exact.length);
        assertThat(pointGet.buffer()).isSameAs(initialBuffer);
        assertThat(Arrays.copyOf(pointGet.buffer(), exact.length)).isEqualTo(exact);
    }

    @Test
    public void growsRetriesAndReusesTheOversizeBuffer() throws Exception {
        final RocksDBReusablePointGet pointGet = new RocksDBReusablePointGet(4);
        final byte[] initialBuffer = pointGet.buffer();
        final byte[] oversized = sequence(257);

        write(bytes("large"), oversized);
        assertThat(read(pointGet, bytes("large"))).isEqualTo(oversized.length);
        assertThat(pointGet.buffer()).isNotSameAs(initialBuffer);
        assertThat(pointGet.buffer().length).isGreaterThanOrEqualTo(oversized.length);
        assertThat(Arrays.copyOf(pointGet.buffer(), oversized.length)).isEqualTo(oversized);

        final byte[] grownBuffer = pointGet.buffer();
        final byte[] smaller = sequence(31);
        write(bytes("small"), smaller);
        assertThat(read(pointGet, bytes("small"))).isEqualTo(smaller.length);
        assertThat(pointGet.buffer()).isSameAs(grownBuffer);
        assertThat(Arrays.copyOf(pointGet.buffer(), smaller.length)).isEqualTo(smaller);
    }

    @Test
    public void oversizedExistenceCheckDoesNotGrowOrRetry() throws Exception {
        final RocksDBReusablePointGet pointGet = new RocksDBReusablePointGet(4);
        final byte[] initialBuffer = pointGet.buffer();
        write(bytes("large"), sequence(257));

        assertThat(
                        pointGet.exists(
                                rocksDBResource.getRocksDB(),
                                rocksDBResource.getDefaultColumnFamily(),
                                bytes("large")))
                .isTrue();
        assertThat(pointGet.buffer()).isSameAs(initialBuffer);
        assertThat(
                        pointGet.exists(
                                rocksDBResource.getRocksDB(),
                                rocksDBResource.getDefaultColumnFamily(),
                                bytes("missing")))
                .isFalse();
    }

    @Test
    public void rejectsNegativeInitialCapacity() {
        assertThatThrownBy(() -> new RocksDBReusablePointGet(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private int read(RocksDBReusablePointGet pointGet, byte[] key) throws Exception {
        return pointGet.get(
                rocksDBResource.getRocksDB(), rocksDBResource.getDefaultColumnFamily(), key);
    }

    private void write(byte[] key, byte[] value) throws Exception {
        final ColumnFamilyHandle columnFamily = rocksDBResource.getDefaultColumnFamily();
        rocksDBResource.getRocksDB().put(columnFamily, key, value);
    }

    private static byte[] sequence(int length) {
        final byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) (i * 31 + 7);
        }
        return bytes;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
