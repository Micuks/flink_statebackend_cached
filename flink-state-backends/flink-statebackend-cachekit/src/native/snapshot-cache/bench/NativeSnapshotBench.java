package org.apache.flink.contrib.streaming.state.cachekit.nativebench;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

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

    private static native long createBytes(
            int capacity, int kernel, Object missSentinel, Object emptySentinel);

    private static native void destroyBytes(long handle);

    private static native boolean putBytes(long handle, byte[] key, Object value);

    private static native Object lookupBytes(long handle, byte[] key);

    private static native Object lookupWords(long handle, long first, long second);

    private static native int echoBytes(byte[] key);

    private static native int byteVectorBytes(long handle);

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
        runByteKeyBench(16);
        runByteKeyBench(32);
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

    private static void runByteKeyBench(int keyBytes) {
        byte[][] stored = new byte[ENTRIES][];
        for (int index = 0; index < stored.length; index++) {
            stored[index] = encodeKey(index, keyBytes);
        }
        byte[][] queries = new byte[QUERY_COUNT][];
        ByteKey[] wrappedQueries = new ByteKey[QUERY_COUNT];
        long[] firstWords = keyBytes == 16 ? new long[QUERY_COUNT] : null;
        long[] secondWords = keyBytes == 16 ? new long[QUERY_COUNT] : null;
        long random = 0x243f6a8885a308d3L;
        for (int index = 0; index < queries.length; index++) {
            random ^= random << 13;
            random ^= random >>> 7;
            random ^= random << 17;
            int key = (index & 3) != 0 ? (int) (random & 2047) : 4096 + (int) (random & 2047);
            queries[index] = key < ENTRIES ? stored[key] : encodeKey(key, keyBytes);
            wrappedQueries[index] = new ByteKey(queries[index]);
            if (keyBytes == 16) {
                ByteBuffer words = ByteBuffer.wrap(queries[index]).order(ByteOrder.nativeOrder());
                firstWords[index] = words.getLong(0);
                secondWords[index] = words.getLong(8);
            }
        }

        Map<ByteKey, Integer> javaLru =
                new LinkedHashMap<ByteKey, Integer>(CAPACITY, 0.75f, true);
        for (int index = 0; index < stored.length; index++) {
            javaLru.put(new ByteKey(stored[index]), index);
        }
        for (int warmup = 0; warmup < 5; warmup++) {
            runJavaByteLru(javaLru, wrappedQueries, 1);
        }
        report("java", 1, "byte-lru", runJavaByteLru(javaLru, wrappedQueries, ROUNDS));
        report("java", 1, "byte-copy", runByteCopy(queries, ROUNDS));
        report("jni", 1, "byte-echo", runByteEcho(queries, ROUNDS));

        Object miss = new Object();
        Object empty = new Object();
        for (int kernel = 0; kernel <= 2; kernel++) {
            if (!kernelSupported(kernel)) {
                continue;
            }
            long handle = createBytes(CAPACITY, kernel, miss, empty);
            try {
                for (int index = 0; index < stored.length; index++) {
                    if (!putBytes(handle, stored[index], Integer.valueOf(index))) {
                        throw new AssertionError("native byte put failed");
                    }
                }
                for (int warmup = 0; warmup < 3; warmup++) {
                    runNativeBytes(handle, queries, miss, 1);
                }
                report(
                        kernel == 0 ? "scalar" : kernel == 1 ? "neon" : "sve",
                        byteVectorBytes(handle),
                        "byte-full-" + keyBytes,
                        runNativeBytes(handle, queries, miss, ROUNDS));
                if (keyBytes == 16) {
                    report(
                            kernel == 0 ? "scalar" : kernel == 1 ? "neon" : "sve",
                            byteVectorBytes(handle),
                            "word-full-16",
                            runNativeWords(handle, firstWords, secondWords, miss, ROUNDS));
                }
            } finally {
                destroyBytes(handle);
            }
        }
    }

    private static Measurement runJavaByteLru(
            Map<ByteKey, Integer> table, ByteKey[] queries, int rounds) {
        long checksum = 0;
        long start = System.nanoTime();
        for (int round = 0; round < rounds; round++) {
            for (ByteKey key : queries) {
                Integer value = table.get(key);
                checksum += value == null ? 0 : value + 1;
            }
        }
        return new Measurement(System.nanoTime() - start, (long) queries.length * rounds, checksum);
    }

    private static Measurement runByteCopy(byte[][] queries, int rounds) {
        byte[] target = new byte[queries[0].length];
        long checksum = 0;
        long start = System.nanoTime();
        for (int round = 0; round < rounds; round++) {
            for (byte[] key : queries) {
                System.arraycopy(key, 0, target, 0, key.length);
                checksum += target[0] & 0xff;
            }
        }
        return new Measurement(System.nanoTime() - start, (long) queries.length * rounds, checksum);
    }

    private static Measurement runByteEcho(byte[][] queries, int rounds) {
        long checksum = 0;
        long start = System.nanoTime();
        for (int round = 0; round < rounds; round++) {
            for (byte[] key : queries) {
                checksum += echoBytes(key);
            }
        }
        return new Measurement(System.nanoTime() - start, (long) queries.length * rounds, checksum);
    }

    private static Measurement runNativeBytes(
            long handle, byte[][] queries, Object miss, int rounds) {
        long checksum = 0;
        long start = System.nanoTime();
        for (int round = 0; round < rounds; round++) {
            for (byte[] key : queries) {
                Object value = lookupBytes(handle, key);
                checksum += value == miss ? 0 : ((Integer) value) + 1;
            }
        }
        return new Measurement(System.nanoTime() - start, (long) queries.length * rounds, checksum);
    }

    private static Measurement runNativeWords(
            long handle, long[] firstWords, long[] secondWords, Object miss, int rounds) {
        long checksum = 0;
        long start = System.nanoTime();
        for (int round = 0; round < rounds; round++) {
            for (int index = 0; index < firstWords.length; index++) {
                Object value = lookupWords(handle, firstWords[index], secondWords[index]);
                checksum += value == miss ? 0 : ((Integer) value) + 1;
            }
        }
        return new Measurement(System.nanoTime() - start, (long) firstWords.length * rounds, checksum);
    }

    private static byte[] encodeKey(long value, int size) {
        byte[] key = new byte[size];
        for (int index = 0; index < key.length; index++) {
            value ^= value << 13;
            value ^= value >>> 7;
            value ^= value << 17;
            key[index] = (byte) value;
        }
        return key;
    }

    private static final class ByteKey {
        private final byte[] bytes;
        private final int hash;

        private ByteKey(byte[] bytes) {
            this.bytes = bytes;
            this.hash = Arrays.hashCode(bytes);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ByteKey && Arrays.equals(bytes, ((ByteKey) other).bytes);
        }
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
