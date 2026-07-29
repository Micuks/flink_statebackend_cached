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

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Deterministic correctness/per-call timing probe for the three candidate RocksDB read paths.
 *
 * <p>The timings are an early local smoke signal, not benchmark evidence. Nexmark on an isolated
 * target remains the performance gate.
 */
public class RocksDBDirectArenaProbeTest {

    private static final int[] BATCH_SIZES = {4, 8, 16, 64};
    private static final int[] HIT_RATIOS = {0, 50, 100};
    private static final int VALUE_STRIDE = 128;
    private static final int SAMPLES = 7;
    private static final int ITERATIONS_PER_SAMPLE = 10;

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    @Test
    @SuppressWarnings("unchecked")
    public void compareDirectGetListMultiGetAndDirectArena() throws Exception {
        StringBuilder csv =
                new StringBuilder(
                        "batch_size,hit_ratio,path,median_ns_per_batch,median_ns_per_key,"
                                + "present,checksum\n");
        try (RocksDBKeyedStateBackendTestFactory factory =
                new RocksDBKeyedStateBackendTestFactory()) {
            RocksDBKeyedStateBackend<Integer> backend =
                    factory.create(tmp, IntSerializer.INSTANCE, 128);
            InternalValueState<Integer, VoidNamespace, Integer> internal =
                    (InternalValueState<Integer, VoidNamespace, Integer>)
                            backend.getPartitionedState(
                                    VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    new ValueStateDescriptor<>(
                                            "direct-arena-probe", IntSerializer.INSTANCE));
            RocksDBValueState<Integer, VoidNamespace, Integer> state =
                    (RocksDBValueState<Integer, VoidNamespace, Integer>) internal;

            int caseIndex = 0;
            for (int batchSize : BATCH_SIZES) {
                for (int hitRatio : HIT_RATIOS) {
                    int baseKey = 100_000 + caseIndex++ * 1_000;
                    int present = batchSize * hitRatio / 100;
                    int[] logicalKeys = new int[batchSize];
                    for (int index = 0; index < batchSize; index++) {
                        logicalKeys[index] = baseKey + index;
                        if (index < present) {
                            backend.setCurrentKey(logicalKeys[index]);
                            state.update(expectedValue(logicalKeys[index]));
                        }
                    }
                    PreparedCase prepared = prepare(state, logicalKeys);

                    ProbeResult directGet =
                            runDirectGet(backend, state.columnFamily, prepared.directKeys);
                    ProbeResult listMultiGet =
                            runListMultiGet(backend, state.columnFamily, prepared.heapKeys);
                    ProbeResult directArena = runDirectArena(backend, state.columnFamily, prepared);
                    assertEquals(directGet, listMultiGet);
                    assertEquals(directGet, directArena);
                    assertEquals(present, directArena.present);

                    record(
                            csv,
                            batchSize,
                            hitRatio,
                            "direct_get_n",
                            measure(
                                    () ->
                                            runDirectGet(
                                                    backend,
                                                    state.columnFamily,
                                                    prepared.directKeys),
                                    directGet),
                            directGet);
                    record(
                            csv,
                            batchSize,
                            hitRatio,
                            "multi_get_as_list",
                            measure(
                                    () ->
                                            runListMultiGet(
                                                    backend, state.columnFamily, prepared.heapKeys),
                                    directGet),
                            directGet);
                    record(
                            csv,
                            batchSize,
                            hitRatio,
                            "direct_arena",
                            measure(
                                    () -> runDirectArena(backend, state.columnFamily, prepared),
                                    directGet),
                            directGet);
                }
            }
        }

        Path output = Paths.get("target", "direct-arena-probe.csv");
        Files.createDirectories(output.getParent());
        Files.write(
                output,
                csv.toString().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("DSTL_PROBE_CSV=" + output.toAbsolutePath());
        System.out.print(csv);
    }

    private static PreparedCase prepare(
            RocksDBDirectValueAccess<Integer, VoidNamespace> access, int[] logicalKeys)
            throws Exception {
        List<byte[]> heapKeys = new ArrayList<>(logicalKeys.length);
        List<ByteBuffer> directKeys = new ArrayList<>(logicalKeys.length);
        int keyBytes = 0;
        for (int logicalKey : logicalKeys) {
            DataOutputSerializer output = new DataOutputSerializer(32);
            access.writeKeyAndNamespace(logicalKey, VoidNamespace.INSTANCE, output);
            byte[] key = output.getCopyOfBuffer();
            heapKeys.add(key);
            ByteBuffer direct = ByteBuffer.allocateDirect(key.length);
            direct.put(key).flip();
            directKeys.add(direct);
            keyBytes += key.length;
        }

        ByteBuffer keyArena = ByteBuffer.allocateDirect(keyBytes);
        ByteBuffer descriptors =
                ByteBuffer.allocateDirect(
                                logicalKeys.length * RocksDBDirectValueAccess.DESCRIPTOR_BYTES)
                        .order(ByteOrder.nativeOrder());
        int keyOffset = 0;
        for (int index = 0; index < heapKeys.size(); index++) {
            byte[] key = heapKeys.get(index);
            keyArena.put(key);
            int record = index * RocksDBDirectValueAccess.DESCRIPTOR_BYTES;
            descriptors.putInt(record + RocksDBDirectValueAccess.STATE_ID_OFFSET, 1);
            descriptors.putInt(record + RocksDBDirectValueAccess.ORIGINAL_INDEX_OFFSET, index);
            descriptors.putLong(record + RocksDBDirectValueAccess.GENERATION_OFFSET, 1L);
            descriptors.putInt(record + RocksDBDirectValueAccess.KEY_OFFSET_OFFSET, keyOffset);
            descriptors.putInt(record + RocksDBDirectValueAccess.KEY_LENGTH_OFFSET, key.length);
            descriptors.putInt(
                    record + RocksDBDirectValueAccess.VALUE_OFFSET_OFFSET, index * VALUE_STRIDE);
            descriptors.putInt(
                    record + RocksDBDirectValueAccess.VALUE_LENGTH_OR_STATUS_OFFSET,
                    Integer.MIN_VALUE);
            keyOffset += key.length;
        }
        keyArena.flip();
        return new PreparedCase(
                Collections.unmodifiableList(heapKeys),
                Collections.unmodifiableList(directKeys),
                keyArena,
                descriptors,
                logicalKeys.length);
    }

    private static ProbeResult runDirectGet(
            RocksDBKeyedStateBackend<Integer> backend,
            ColumnFamilyHandle columnFamily,
            List<ByteBuffer> keys)
            throws Exception {
        long checksum = 1L;
        int present = 0;
        ByteBuffer value = ByteBuffer.allocateDirect(VALUE_STRIDE);
        for (ByteBuffer preparedKey : keys) {
            value.clear();
            int length =
                    backend.db.get(
                            columnFamily, backend.getReadOptions(), preparedKey.duplicate(), value);
            if (length == RocksDB.NOT_FOUND) {
                checksum = mix(checksum, RocksDBDirectValueAccess.NOT_FOUND);
            } else {
                present++;
                checksum = mix(checksum, decodeInt(value, length));
            }
        }
        return new ProbeResult(present, checksum);
    }

    private static ProbeResult runListMultiGet(
            RocksDBKeyedStateBackend<Integer> backend,
            ColumnFamilyHandle columnFamily,
            List<byte[]> keys)
            throws Exception {
        List<ColumnFamilyHandle> columnFamilies = Collections.nCopies(keys.size(), columnFamily);
        List<byte[]> values =
                backend.db.multiGetAsList(backend.getReadOptions(), columnFamilies, keys);
        long checksum = 1L;
        int present = 0;
        for (byte[] value : values) {
            if (value == null) {
                checksum = mix(checksum, RocksDBDirectValueAccess.NOT_FOUND);
            } else {
                present++;
                checksum = mix(checksum, decodeInt(ByteBuffer.wrap(value), value.length));
            }
        }
        return new ProbeResult(present, checksum);
    }

    private static ProbeResult runDirectArena(
            RocksDBKeyedStateBackend<Integer> backend,
            ColumnFamilyHandle columnFamily,
            PreparedCase prepared)
            throws Exception {
        ByteBuffer descriptors = copyDirect(prepared.descriptors, ByteOrder.nativeOrder());
        ByteBuffer values =
                ByteBuffer.allocateDirect(prepared.count * VALUE_STRIDE)
                        .order(ByteOrder.BIG_ENDIAN);
        int present =
                backend.db.multiGetDirectArena(
                        columnFamily,
                        backend.getReadOptions(),
                        prepared.keyArena.duplicate(),
                        descriptors,
                        prepared.count,
                        values,
                        VALUE_STRIDE);
        long checksum = 1L;
        for (int index = 0; index < prepared.count; index++) {
            int status =
                    descriptors.getInt(
                            index * RocksDBDirectValueAccess.DESCRIPTOR_BYTES
                                    + RocksDBDirectValueAccess.VALUE_LENGTH_OR_STATUS_OFFSET);
            if (status == RocksDBDirectValueAccess.NOT_FOUND) {
                checksum = mix(checksum, status);
            } else {
                checksum =
                        mix(
                                checksum,
                                decodeInt(
                                        values.duplicate()
                                                .position(index * VALUE_STRIDE)
                                                .slice()
                                                .order(ByteOrder.BIG_ENDIAN),
                                        status));
            }
        }
        return new ProbeResult(present, checksum);
    }

    private static long measure(CheckedProbe probe, ProbeResult expected) throws Exception {
        for (int warmup = 0; warmup < 3; warmup++) {
            assertEquals(expected, probe.run());
        }
        long[] samples = new long[SAMPLES];
        for (int sample = 0; sample < samples.length; sample++) {
            long start = System.nanoTime();
            ProbeResult actual = null;
            for (int iteration = 0; iteration < ITERATIONS_PER_SAMPLE; iteration++) {
                actual = probe.run();
            }
            samples[sample] = (System.nanoTime() - start) / ITERATIONS_PER_SAMPLE;
            assertEquals(expected, actual);
        }
        Arrays.sort(samples);
        return samples[samples.length / 2];
    }

    private static int decodeInt(ByteBuffer value, int length) {
        assertEquals(Integer.BYTES, length);
        return value.duplicate().order(ByteOrder.BIG_ENDIAN).getInt(0);
    }

    private static ByteBuffer copyDirect(ByteBuffer source, ByteOrder order) {
        ByteBuffer copy = ByteBuffer.allocateDirect(source.remaining()).order(order);
        copy.put(source.duplicate()).flip();
        return copy.order(order);
    }

    private static int expectedValue(int key) {
        return key * 31 + 7;
    }

    private static long mix(long checksum, int value) {
        return checksum * 0x9e3779b97f4a7c15L + value;
    }

    private static void record(
            StringBuilder csv,
            int batchSize,
            int hitRatio,
            String path,
            long medianNanos,
            ProbeResult result) {
        csv.append(batchSize)
                .append(',')
                .append(hitRatio)
                .append(',')
                .append(path)
                .append(',')
                .append(medianNanos)
                .append(',')
                .append((double) medianNanos / batchSize)
                .append(',')
                .append(result.present)
                .append(',')
                .append(result.checksum)
                .append('\n');
    }

    @FunctionalInterface
    private interface CheckedProbe {
        ProbeResult run() throws Exception;
    }

    private static final class PreparedCase {
        private final List<byte[]> heapKeys;
        private final List<ByteBuffer> directKeys;
        private final ByteBuffer keyArena;
        private final ByteBuffer descriptors;
        private final int count;

        private PreparedCase(
                List<byte[]> heapKeys,
                List<ByteBuffer> directKeys,
                ByteBuffer keyArena,
                ByteBuffer descriptors,
                int count) {
            this.heapKeys = heapKeys;
            this.directKeys = directKeys;
            this.keyArena = keyArena;
            this.descriptors = descriptors;
            this.count = count;
        }
    }

    private static final class ProbeResult {
        private final int present;
        private final long checksum;

        private ProbeResult(int present, long checksum) {
            this.present = present;
            this.checksum = checksum;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ProbeResult)) {
                return false;
            }
            ProbeResult that = (ProbeResult) other;
            return present == that.present && checksum == that.checksum;
        }

        @Override
        public int hashCode() {
            return 31 * present + Long.hashCode(checksum);
        }

        @Override
        public String toString() {
            return "ProbeResult{present=" + present + ", checksum=" + checksum + '}';
        }
    }
}
