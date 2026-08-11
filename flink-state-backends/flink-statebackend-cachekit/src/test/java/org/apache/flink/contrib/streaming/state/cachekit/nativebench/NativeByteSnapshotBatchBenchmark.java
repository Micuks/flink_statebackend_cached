/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state.cachekit.nativebench;

import org.apache.flink.contrib.streaming.state.cachekit.cache.LruCachePolicy;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Explicitly invoked P4 benchmark for the production-shaped byte-key snapshot table.
 *
 * <p>The class deliberately does not end in {@code Test}; normal unit-test runs must not execute a
 * timing experiment. JNI entry points are attached only to this test class.
 */
class NativeByteSnapshotBatchBenchmark {

    private static final int EMPTY_KIND = 1;
    private static final int SINGLE_KIND = 2;
    private static final int SCALAR_KERNEL = 0;
    private static final int MAX_ENTRIES = 2000;
    private static final int QUERY_COUNT = 1 << 16;
    private static final int KEY_COUNT = 4096;
    private static final int[] BATCH_SIZES = {8, 32, 64};
    private static final int WARMUP_ROUNDS = 3;
    private static final int MEASURED_ROUNDS = 7;

    private static final Object NATIVE_MISS = new Object();
    private static final Object NATIVE_EMPTY = new Object();
    private static final Object NATIVE_MULTI = new Object();

    private static String loadedLibrary;
    private static volatile long blackhole;

    @Test
    void testRandomizedDifferentialBatch() {
        loadNativeLibrary();
        final int capacity = 17;
        long handle =
                byteCreate(
                        capacity,
                        SCALAR_KERNEL,
                        NATIVE_MISS,
                        NATIVE_EMPTY,
                        NATIVE_MULTI);
        assertTrue(handle != 0);
        NativeModel model = new NativeModel(capacity);
        try {
            byte[][] keys = makeRawKeys(96);
            int[] collision = findNativeBucketCollision(keys);
            Payload collisionPayload = new Payload(10001);

            assertPut(handle, model, keys[collision[0]], NATIVE_EMPTY);
            assertPut(handle, model, keys[collision[1]], new Payload(10000));
            assertSame(NATIVE_EMPTY, byteLookup(handle, keys[collision[0]], 0, 8));
            assertSame(model.get(keys[collision[0]]), NATIVE_EMPTY);
            assertPut(handle, model, keys[collision[0]], collisionPayload);
            assertSame(collisionPayload, byteLookup(handle, keys[collision[0]], 0, 8));
            assertSame(model.get(keys[collision[0]]), collisionPayload);

            model.clear();
            byteClear(handle);
            for (int index = 0; index < capacity; index++) {
                assertPut(handle, model, keys[index], new Payload(11000 + index));
            }
            assertPut(handle, model, keys[capacity], new Payload(12000));
            assertSame(NATIVE_MISS, byteLookup(handle, keys[0], 0, keys[0].length));
            assertSame(model.get(keys[0]), NATIVE_MISS);

            Random random = new Random(0x5eedc0deL);
            for (int operation = 0; operation < 12000; operation++) {
                byte[] key = keys[random.nextInt(keys.length)];
                int choice = random.nextInt(100);
                if (choice < 42) {
                    Object value =
                            random.nextInt(5) == 0
                                    ? NATIVE_EMPTY
                                    : new Payload((operation << 8) | random.nextInt(256));
                    assertPut(handle, model, key, value);
                } else if (choice < 78) {
                    assertSame(model.get(key), byteLookup(handle, key, 0, key.length));
                } else if (choice < 93) {
                    assertEquals(model.remove(key), byteRemove(handle, key, 0, key.length));
                } else {
                    model.clear();
                    byteClear(handle);
                }
                assertEquals(model.size(), byteSize(handle));
            }

            for (int index = 0; index < capacity; index++) {
                Object value = index % 4 == 0 ? NATIVE_EMPTY : new Payload(20000 + index);
                assertPut(handle, model, keys[index], value);
            }
            ByteBuffer arena = ByteBuffer.allocateDirect(keys.length * 8);
            ByteBuffer ranges =
                    ByteBuffer.allocateDirect(keys.length * 8).order(ByteOrder.nativeOrder());
            for (byte[] key : keys) {
                int offset = arena.position();
                arena.put(key);
                ranges.putInt(offset).putInt(key.length);
            }
            Object[] output = new Object[keys.length];
            byteLookupBatch(handle, arena, ranges, output, keys.length);
            for (int index = 0; index < keys.length; index++) {
                assertSame(model.get(keys[index]), output[index]);
            }

            Payload retained = new Payload(30000);
            byteClear(handle);
            model.clear();
            assertPut(handle, model, keys[0], retained);
            arena.clear();
            ranges.clear();
            arena.put(keys[0]);
            ranges.putInt(0).putInt(keys[0].length);
            byteLookupBatch(handle, arena, ranges, output, 1);
            assertSame(retained, output[0]);
            byteDestroy(handle);
            handle = 0;
            assertSame(retained, output[0]);
            assertEquals(30000, ((Payload) output[0]).id);
        } finally {
            if (handle != 0) {
                byteDestroy(handle);
            }
        }
    }

    @Test
    void benchmarkProductionBytePath() {
        loadNativeLibrary();
        BinaryRowData[] keys = makeBinaryRows(KEY_COUNT);
        int keyBytes = keys[0].getSizeInBytes();
        for (BinaryRowData key : keys) {
            assertEquals(keyBytes, key.getSizeInBytes());
            assertEquals(1, key.getSegments().length);
            assertFalse(key.getSegments()[0].isOffHeap());
        }

        int[] queries = makeQueries();
        JavaSnapshotCache javaCache = new JavaSnapshotCache(MAX_ENTRIES);
        long handle =
                byteCreate(
                        MAX_ENTRIES,
                        SCALAR_KERNEL,
                        NATIVE_MISS,
                        NATIVE_EMPTY,
                        NATIVE_MULTI);
        assertTrue(handle != 0);
        try {
            for (int index = 0; index < MAX_ENTRIES; index++) {
                Payload payload = index % 16 == 0 ? null : new Payload(index);
                javaCache.put(keys[index], payload);
                BinaryRowData key = keys[index];
                byte[] bytes = key.getSegments()[0].getArray();
                int result =
                        bytePut(
                                handle,
                                bytes,
                                key.getOffset(),
                                key.getSizeInBytes(),
                                payload == null ? EMPTY_KIND : SINGLE_KIND,
                                payload);
                assertEquals(1, result);
            }

            BatchBuffers[] batchBuffers = new BatchBuffers[BATCH_SIZES.length];
            for (int index = 0; index < BATCH_SIZES.length; index++) {
                batchBuffers[index] = new BatchBuffers(BATCH_SIZES[index], keyBytes);
            }
            for (int round = 0; round < WARMUP_ROUNDS; round++) {
                blackhole ^= runJava(javaCache, keys, queries);
                blackhole ^= runNativeScalar(handle, keys, queries);
                for (BatchBuffers buffers : batchBuffers) {
                    blackhole ^= runNativeBatch(handle, keys, queries, buffers);
                }
            }

            double javaNanos = median(measureJava(javaCache, keys, queries));
            double scalarNanos = median(measureNativeScalar(handle, keys, queries));
            double[] batchNanos = new double[BATCH_SIZES.length];
            for (int index = 0; index < BATCH_SIZES.length; index++) {
                batchNanos[index] =
                        median(
                                measureNativeBatch(
                                        handle, keys, queries, batchBuffers[index]));
            }

            printResult("java", 1, keyBytes, javaNanos);
            printResult("native-scalar", 1, keyBytes, scalarNanos);
            for (int index = 0; index < BATCH_SIZES.length; index++) {
                printResult(
                        "native-batch-" + BATCH_SIZES[index],
                        BATCH_SIZES[index],
                        keyBytes,
                        batchNanos[index]);
            }
            double bestBatch = Math.min(batchNanos[1], batchNanos[2]);
            System.out.printf(
                    "P4_BYTE_GATE java_ns_per_lookup=%.3f batch32_ns_per_lookup=%.3f "
                            + "batch64_ns_per_lookup=%.3f best_pass=%s%n",
                    javaNanos,
                    batchNanos[1],
                    batchNanos[2],
                    bestBatch < javaNanos);
        } finally {
            byteDestroy(handle);
        }
    }

    private static double[] measureJava(
            JavaSnapshotCache cache, BinaryRowData[] keys, int[] queries) {
        double[] measurements = new double[MEASURED_ROUNDS];
        for (int round = 0; round < measurements.length; round++) {
            long start = System.nanoTime();
            blackhole ^= runJava(cache, keys, queries);
            measurements[round] = (double) (System.nanoTime() - start) / queries.length;
        }
        return measurements;
    }

    private static double[] measureNativeScalar(long handle, BinaryRowData[] keys, int[] queries) {
        double[] measurements = new double[MEASURED_ROUNDS];
        for (int round = 0; round < measurements.length; round++) {
            long start = System.nanoTime();
            blackhole ^= runNativeScalar(handle, keys, queries);
            measurements[round] = (double) (System.nanoTime() - start) / queries.length;
        }
        return measurements;
    }

    private static double[] measureNativeBatch(
            long handle, BinaryRowData[] keys, int[] queries, BatchBuffers buffers) {
        double[] measurements = new double[MEASURED_ROUNDS];
        for (int round = 0; round < measurements.length; round++) {
            long start = System.nanoTime();
            blackhole ^= runNativeBatch(handle, keys, queries, buffers);
            measurements[round] = (double) (System.nanoTime() - start) / queries.length;
        }
        return measurements;
    }

    private static long runJava(JavaSnapshotCache cache, BinaryRowData[] keys, int[] queries) {
        long consumed = 0;
        for (int query : queries) {
            JavaSnapshot result = cache.get(keys[query]);
            if (result == null) {
                consumed += 1;
            } else if (result.payload == null) {
                consumed += 3;
            } else {
                consumed += result.payload.id + 7L;
            }
        }
        return consumed;
    }

    private static long runNativeScalar(long handle, BinaryRowData[] keys, int[] queries) {
        long consumed = 0;
        for (int query : queries) {
            BinaryRowData key = keys[query];
            Object result =
                    byteLookup(
                            handle,
                            key.getSegments()[0].getArray(),
                            key.getOffset(),
                            key.getSizeInBytes());
            consumed += consumeNative(result);
        }
        return consumed;
    }

    private static long runNativeBatch(
            long handle, BinaryRowData[] keys, int[] queries, BatchBuffers buffers) {
        ByteBuffer arena = buffers.arena;
        ByteBuffer ranges = buffers.ranges;
        Object[] output = buffers.output;
        int batchSize = output.length;
        long consumed = 0;
        for (int base = 0; base < queries.length; base += batchSize) {
            int count = Math.min(batchSize, queries.length - base);
            arena.clear();
            ranges.clear();
            for (int index = 0; index < count; index++) {
                BinaryRowData key = keys[queries[base + index]];
                int offset = arena.position();
                arena.put(
                        key.getSegments()[0].getArray(),
                        key.getOffset(),
                        key.getSizeInBytes());
                ranges.putInt(offset).putInt(key.getSizeInBytes());
            }
            byteLookupBatch(handle, arena, ranges, output, count);
            for (int index = 0; index < count; index++) {
                consumed += consumeNative(output[index]);
            }
        }
        return consumed;
    }

    private static long consumeNative(Object result) {
        if (result == NATIVE_MISS) {
            return 1;
        }
        if (result == NATIVE_EMPTY) {
            return 3;
        }
        return ((Payload) result).id + 7L;
    }

    private static void assertPut(long handle, NativeModel model, byte[] key, Object value) {
        boolean evicted = model.put(key, value);
        int kind = value == NATIVE_EMPTY ? EMPTY_KIND : SINGLE_KIND;
        int result = bytePut(handle, key, 0, key.length, kind, value == NATIVE_EMPTY ? null : value);
        assertEquals(evicted ? 2 : 1, result);
    }

    private static BinaryRowData[] makeBinaryRows(int count) {
        BinaryRowData[] keys = new BinaryRowData[count];
        for (int index = 0; index < count; index++) {
            BinaryRowData row = new BinaryRowData(1);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            writer.writeLong(0, index);
            writer.complete();
            keys[index] = row;
        }
        return keys;
    }

    private static int[] makeQueries() {
        int[] queries = new int[QUERY_COUNT];
        Random random = new Random(0x920L);
        for (int index = 0; index < queries.length; index++) {
            queries[index] =
                    index % 8 == 0
                            ? MAX_ENTRIES + random.nextInt(KEY_COUNT - MAX_ENTRIES)
                            : random.nextInt(MAX_ENTRIES);
        }
        return queries;
    }

    private static byte[][] makeRawKeys(int count) {
        byte[][] keys = new byte[count][8];
        for (int index = 0; index < count; index++) {
            long value = index * 0x9e3779b97f4a7c15L;
            for (int offset = 0; offset < 8; offset++) {
                keys[index][offset] = (byte) (value >>> (offset * 8));
            }
        }
        return keys;
    }

    private static int[] findNativeBucketCollision(byte[][] keys) {
        int[] first = new int[256];
        Arrays.fill(first, -1);
        for (int index = 0; index < keys.length; index++) {
            int bucket = (int) nativeHash(keys[index]) & 0xff;
            if (first[bucket] >= 0) {
                return new int[] {first[bucket], index};
            }
            first[bucket] = index;
        }
        throw new AssertionError("test data did not contain a native table bucket collision");
    }

    private static long nativeHash(byte[] bytes) {
        long hash = 1469598103934665603L;
        for (byte value : bytes) {
            hash ^= value & 0xffL;
            hash *= 1099511628211L;
        }
        hash ^= hash >>> 30;
        hash *= 0xbf58476d1ce4e5b9L;
        hash ^= hash >>> 27;
        hash *= 0x94d049bb133111ebL;
        return hash ^ (hash >>> 31);
    }

    private static double median(double[] values) {
        Arrays.sort(values);
        return values[values.length / 2];
    }

    private static void printResult(String mode, int batch, int keyBytes, double nanos) {
        System.out.printf(
                "P4_BYTE_BENCH mode=%s batch=%d key_bytes=%d ns_per_lookup=%.3f%n",
                mode, batch, keyBytes, nanos);
    }

    private static synchronized void loadNativeLibrary() {
        String library = System.getProperty("cachekit.native.snapshot.library", "").trim();
        Assumptions.assumeTrue(!library.isEmpty(), "native snapshot library path is not configured");
        Assumptions.assumeTrue(Files.isRegularFile(Paths.get(library)), "native library is missing");
        if (loadedLibrary == null) {
            System.load(library);
            loadedLibrary = library;
        } else {
            assertEquals(loadedLibrary, library);
        }
    }

    private static final class JavaSnapshotCache {
        private final LruCachePolicy<KeyNamespace, JavaSnapshot> cache;
        private final KeyNamespace probe = new KeyNamespace(null, VoidNamespace.INSTANCE);

        private JavaSnapshotCache(int maxEntries) {
            cache = new LruCachePolicy<>(maxEntries);
        }

        private void put(BinaryRowData key, Payload payload) {
            cache.put(
                    new KeyNamespace(key.copy(), VoidNamespace.INSTANCE),
                    new JavaSnapshot(payload));
        }

        private JavaSnapshot get(BinaryRowData key) {
            probe.key = key;
            return cache.get(probe);
        }
    }

    private static final class KeyNamespace {
        private BinaryRowData key;
        private final VoidNamespace namespace;

        private KeyNamespace(BinaryRowData key, VoidNamespace namespace) {
            this.key = key;
            this.namespace = namespace;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof KeyNamespace)) {
                return false;
            }
            KeyNamespace that = (KeyNamespace) other;
            return Objects.equals(key, that.key) && Objects.equals(namespace, that.namespace);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, namespace);
        }
    }

    private static final class JavaSnapshot {
        private final Payload payload;

        private JavaSnapshot(Payload payload) {
            this.payload = payload;
        }
    }

    private static final class NativeModel {
        private final int capacity;
        private final LinkedHashMap<ByteKey, Object> entries =
                new LinkedHashMap<ByteKey, Object>(16, 0.75f, true);

        private NativeModel(int capacity) {
            this.capacity = capacity;
        }

        private boolean put(byte[] key, Object value) {
            ByteKey stored = new ByteKey(key);
            boolean existed = entries.containsKey(stored);
            entries.put(stored, value);
            if (!existed && entries.size() > capacity) {
                ByteKey eldest = entries.entrySet().iterator().next().getKey();
                entries.remove(eldest);
                return true;
            }
            return false;
        }

        private Object get(byte[] key) {
            Object value = entries.get(new ByteKey(key));
            return value == null ? NATIVE_MISS : value;
        }

        private boolean remove(byte[] key) {
            return entries.remove(new ByteKey(key)) != null;
        }

        private void clear() {
            entries.clear();
        }

        private int size() {
            return entries.size();
        }
    }

    private static final class ByteKey {
        private final byte[] bytes;

        private ByteKey(byte[] bytes) {
            this.bytes = Arrays.copyOf(bytes, bytes.length);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ByteKey && Arrays.equals(bytes, ((ByteKey) other).bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    private static final class Payload {
        private final int id;

        private Payload(int id) {
            this.id = id;
        }
    }

    private static final class BatchBuffers {
        private final ByteBuffer arena;
        private final ByteBuffer ranges;
        private final Object[] output;

        private BatchBuffers(int batchSize, int keyBytes) {
            arena = ByteBuffer.allocateDirect(batchSize * keyBytes);
            ranges = ByteBuffer.allocateDirect(batchSize * 8).order(ByteOrder.nativeOrder());
            output = new Object[batchSize];
        }
    }

    private static native long byteCreate(
            int maxEntries,
            int kernel,
            Object missSentinel,
            Object emptySentinel,
            Object multiSentinel);

    private static native void byteDestroy(long handle);

    private static native int bytePut(
            long handle, byte[] key, int offset, int length, int kind, Object userKey);

    private static native Object byteLookup(long handle, byte[] key, int offset, int length);

    private static native void byteLookupBatch(
            long handle, ByteBuffer keyArena, ByteBuffer ranges, Object[] output, int count);

    private static native boolean byteRemove(long handle, byte[] key, int offset, int length);

    private static native void byteClear(long handle);

    private static native int byteSize(long handle);
}
