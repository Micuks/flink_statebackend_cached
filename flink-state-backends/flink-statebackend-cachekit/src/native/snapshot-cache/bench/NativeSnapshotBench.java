package org.apache.flink.contrib.streaming.state.cachekit.nativebench;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** JNI boundary cost probe. This is not a Flink integration or a Nexmark result. */
public final class NativeSnapshotBench {

    private static final int CAPACITY = 4096;
    private static final int ENTRIES = 2048;
    private static final int QUERY_COUNT = 1 << 18;
    private static final int ROUNDS = 8;

    private static native boolean kernelSupported(int kernel);

    private static native long create(int capacity, int kernel);

    private static native void destroy(long handle);

    private static native boolean put(
            long handle, long key, long namespace, int kind, int entryId);

    private static native long lookup(long handle, long key, long namespace);

    private static native void lookupBatch(
            long handle, ByteBuffer input, ByteBuffer output, int start, int count);

    private static native String kernelName(long handle);

    private static native int vectorBytes(long handle);

    public static void main(String[] args) {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: NativeSnapshotBench /absolute/path/lib.so");
        }
        System.load(args[0]);

        long[] keys = new long[QUERY_COUNT];
        long[] namespaces = new long[QUERY_COUNT];
        ByteBuffer input = ByteBuffer.allocateDirect(QUERY_COUNT * 16).order(ByteOrder.nativeOrder());
        ByteBuffer output = ByteBuffer.allocateDirect(QUERY_COUNT * 8).order(ByteOrder.nativeOrder());
        long random = 0x243f6a8885a308d3L;
        for (int index = 0; index < QUERY_COUNT; index++) {
            random ^= random << 13;
            random ^= random >>> 7;
            random ^= random << 17;
            boolean hit = (index & 3) != 0;
            long key = hit ? random & 2047 : 4096 + (random & 2047);
            keys[index] = key;
            namespaces[index] = key % 7;
            input.putLong(index * 16, key);
            input.putLong(index * 16 + 8, namespaces[index]);
        }

        runJavaBaseline(keys, namespaces);
        for (int kernel = 0; kernel <= 2; kernel++) {
            if (kernelSupported(kernel)) {
                runKernel(kernel, keys, namespaces, input, output);
            }
        }
    }

    private static void runJavaBaseline(long[] keys, long[] namespaces) {
        JavaScalarTable table = new JavaScalarTable(CAPACITY);
        for (int index = 0; index < ENTRIES; index++) {
            if (!table.put(index, index % 7, 2, index)) {
                throw new AssertionError("Java baseline put failed");
            }
        }
        for (int warmup = 0; warmup < 5; warmup++) {
            runJavaLookups(table, keys, namespaces, 1);
        }
        Measurement result = runJavaLookups(table, keys, namespaces, ROUNDS);
        report("java", 1, "single", result);
    }

    private static Measurement runJavaLookups(
            JavaScalarTable table, long[] keys, long[] namespaces, int rounds) {
        long checksum = 0;
        long start = System.nanoTime();
        for (int round = 0; round < rounds; round++) {
            for (int index = 0; index < keys.length; index++) {
                checksum += table.lookup(keys[index], namespaces[index]);
            }
        }
        return new Measurement(System.nanoTime() - start, (long) keys.length * rounds, checksum);
    }

    private static void runKernel(
            int kernel,
            long[] keys,
            long[] namespaces,
            ByteBuffer input,
            ByteBuffer output) {
        long handle = create(CAPACITY, kernel);
        try {
            for (int index = 0; index < ENTRIES; index++) {
                if (!put(handle, index, index % 7, 2, index)) {
                    throw new AssertionError("native put failed");
                }
            }
            for (int warmup = 0; warmup < 3; warmup++) {
                runSingle(handle, keys, namespaces, 1);
                runBatch(handle, input, output, 32, 1);
            }
            report(kernelName(handle), vectorBytes(handle), "single", runSingle(handle, keys, namespaces, ROUNDS));
            for (int batch : new int[] {8, 32, 64}) {
                report(
                        kernelName(handle),
                        vectorBytes(handle),
                        "batch-" + batch,
                        runBatch(handle, input, output, batch, ROUNDS));
            }
        } finally {
            destroy(handle);
        }
    }

    private static Measurement runSingle(
            long handle, long[] keys, long[] namespaces, int rounds) {
        long checksum = 0;
        long start = System.nanoTime();
        for (int round = 0; round < rounds; round++) {
            for (int index = 0; index < keys.length; index++) {
                checksum += lookup(handle, keys[index], namespaces[index]);
            }
        }
        return new Measurement(System.nanoTime() - start, (long) keys.length * rounds, checksum);
    }

    private static Measurement runBatch(
            long handle, ByteBuffer input, ByteBuffer output, int batchSize, int rounds) {
        long checksum = 0;
        long start = System.nanoTime();
        for (int round = 0; round < rounds; round++) {
            for (int index = 0; index < QUERY_COUNT; index += batchSize) {
                lookupBatch(handle, input, output, index, Math.min(batchSize, QUERY_COUNT - index));
            }
            for (int index = 0; index < QUERY_COUNT; index++) {
                checksum += output.getLong(index * 8);
            }
        }
        return new Measurement(System.nanoTime() - start, (long) QUERY_COUNT * rounds, checksum);
    }

    private static void report(String kernel, int bytes, String mode, Measurement result) {
        double nanosPerOperation = (double) result.nanoseconds / result.operations;
        System.out.printf(
                "%-6s vector_bytes=%-3d %-8s ns/op=%7.2f Mops/s=%6.2f checksum=%d%n",
                kernel,
                bytes,
                mode,
                nanosPerOperation,
                1000.0 / nanosPerOperation,
                result.checksum);
    }

    private static final class Measurement {
        private final long nanoseconds;
        private final long operations;
        private final long checksum;

        private Measurement(long nanoseconds, long operations, long checksum) {
            this.nanoseconds = nanoseconds;
            this.operations = operations;
            this.checksum = checksum;
        }
    }

    /** Same primitive SoA algorithm as the Native scalar table, including lookup-time LRU touch. */
    private static final class JavaScalarTable {
        private final int mask;
        private final byte[] control;
        private final long[] hashes;
        private final long[] keys;
        private final long[] namespaces;
        private final byte[] kinds;
        private final int[] entryIds;
        private final int[] previous;
        private final int[] next;
        private int tail = -1;
        private int size;

        private JavaScalarTable(int capacity) {
            this.mask = capacity - 1;
            this.control = new byte[capacity];
            this.hashes = new long[capacity];
            this.keys = new long[capacity];
            this.namespaces = new long[capacity];
            this.kinds = new byte[capacity];
            this.entryIds = new int[capacity];
            this.previous = new int[capacity];
            this.next = new int[capacity];
            java.util.Arrays.fill(previous, -1);
            java.util.Arrays.fill(next, -1);
        }

        private boolean put(long key, long namespace, int kind, int entryId) {
            long hash = compositeHash(key, namespace);
            int slot = find(key, namespace, hash);
            if (slot >= 0) {
                kinds[slot] = (byte) kind;
                entryIds[slot] = entryId;
                touch(slot);
                return true;
            }
            if ((size + 1) * 4 > control.length * 3) {
                return false;
            }
            slot = (int) hash & mask;
            while (control[slot] != 0) {
                slot = (slot + 1) & mask;
            }
            control[slot] = fingerprint(hash);
            hashes[slot] = hash;
            keys[slot] = key;
            namespaces[slot] = namespace;
            kinds[slot] = (byte) kind;
            entryIds[slot] = entryId;
            append(slot);
            size++;
            return true;
        }

        private long lookup(long key, long namespace) {
            long hash = compositeHash(key, namespace);
            int slot = find(key, namespace, hash);
            if (slot < 0) {
                return 0;
            }
            touch(slot);
            return ((long) entryIds[slot] << 8) | kinds[slot];
        }

        private int find(long key, long namespace, long hash) {
            int slot = (int) hash & mask;
            byte fingerprint = fingerprint(hash);
            for (int probes = 0; probes < control.length; probes++) {
                byte marker = control[slot];
                if (marker == 0) {
                    return -1;
                }
                if (marker == fingerprint
                        && hashes[slot] == hash
                        && keys[slot] == key
                        && namespaces[slot] == namespace) {
                    return slot;
                }
                slot = (slot + 1) & mask;
            }
            return -1;
        }

        private void touch(int slot) {
            if (slot == tail) {
                return;
            }
            int before = previous[slot];
            int after = next[slot];
            if (before >= 0) {
                next[before] = after;
            }
            if (after >= 0) {
                previous[after] = before;
            }
            previous[slot] = tail;
            next[slot] = -1;
            if (tail >= 0) {
                next[tail] = slot;
            }
            tail = slot;
        }

        private void append(int slot) {
            previous[slot] = tail;
            next[slot] = -1;
            if (tail >= 0) {
                next[tail] = slot;
            }
            tail = slot;
        }

        private static long compositeHash(long key, long namespace) {
            return mix64(key ^ (mix64(namespace) + 0x9e3779b97f4a7c15L));
        }

        private static long mix64(long value) {
            value ^= value >>> 30;
            value *= 0xbf58476d1ce4e5b9L;
            value ^= value >>> 27;
            value *= 0x94d049bb133111ebL;
            return value ^ (value >>> 31);
        }

        private static byte fingerprint(long hash) {
            return (byte) (2 + ((hash >>> 57) & 0x7f));
        }
    }

    private NativeSnapshotBench() {}
}
