/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.table.data.binary.BinaryRowData;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.Objects;

/** JNI-backed (key, namespace) to EMPTY/SINGLE(userKey) snapshot cache. */
final class NativeMapSnapshotCache<K, N, UK> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NativeMapSnapshotCache.class);

    static final int EMPTY = 1;
    static final int SINGLE = 2;

    private static final int PUT_REJECTED = 0;
    private static final int PUT_UPDATED = 1;
    private static final int PUT_INSERTED = 2;
    private static final int PUT_INSERTED_WITH_EVICTION = 3;

    private static final Object NATIVE_MISS = new Object();
    private static final Object NATIVE_EMPTY = new Object();
    private static final String EMBEDDED_LIBRARY =
            "/META-INF/native/libcachekit_snapshot_jni.so";
    private static String loadedLibrary;

    static boolean snapshotFeatureAvailable(String libraryPath, boolean nativeRequested) {
        if (!isCandidatePlatform(
                System.getProperty("os.name", ""), System.getProperty("os.arch", ""))) {
            return false;
        }
        if (!kunpengCrc32Available()) {
            return false;
        }
        if (!nativeRequested) {
            return true;
        }
        loadLibrary(libraryPath, false);
        return nativeSnapshotFeatureAvailable();
    }

    static boolean isCandidatePlatform(String osName, String osArch) {
        String normalizedOs = osName == null ? "" : osName.trim().toLowerCase(Locale.ROOT);
        String normalizedArch = osArch == null ? "" : osArch.trim().toLowerCase(Locale.ROOT);
        return normalizedOs.contains("linux")
                && (normalizedArch.equals("aarch64") || normalizedArch.equals("arm64"));
    }

    private static boolean kunpengCrc32Available() {
        try {
            return isKunpengCrc32CpuInfo(Files.readString(Path.of("/proc/cpuinfo")));
        } catch (IOException error) {
            return false;
        }
    }

    static boolean isKunpengCrc32CpuInfo(String cpuInfo) {
        String normalized = cpuInfo == null ? "" : cpuInfo.toLowerCase(Locale.ROOT);
        return normalized.contains("cpu implementer")
                && normalized.contains("0x48")
                && (normalized.contains("cpu part\t: 0xd01")
                        || normalized.contains("cpu part\t: 0xd02"))
                && normalized.matches("(?s).*\\bcrc32\\b.*");
    }

    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<UK> userKeySerializer;
    private final MapSnapshotCacheMetrics metrics;
    private final ThreadLocal<Buffers> buffers = ThreadLocal.withInitial(Buffers::new);
    private final boolean removeHintEnabled;
    private final Long2IntOpenHashMap membershipCounts;
    private long handle;

    NativeMapSnapshotCache(
            int maxEntries,
            String libraryPath,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<UK> userKeySerializer) {
        this(
                maxEntries,
                libraryPath,
                false,
                false,
                MapSnapshotCacheMetrics.disabled(),
                keySerializer,
                namespaceSerializer,
                userKeySerializer);
    }

    NativeMapSnapshotCache(
            int maxEntries,
            String libraryPath,
            boolean classifierEnabled,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<UK> userKeySerializer) {
        this(
                maxEntries,
                libraryPath,
                classifierEnabled,
                false,
                MapSnapshotCacheMetrics.disabled(),
                keySerializer,
                namespaceSerializer,
                userKeySerializer);
    }

    NativeMapSnapshotCache(
            int maxEntries,
            String libraryPath,
            boolean classifierEnabled,
            MapSnapshotCacheMetrics metrics,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<UK> userKeySerializer) {
        this(
                maxEntries,
                libraryPath,
                classifierEnabled,
                false,
                metrics,
                keySerializer,
                namespaceSerializer,
                userKeySerializer);
    }

    NativeMapSnapshotCache(
            int maxEntries,
            String libraryPath,
            boolean classifierEnabled,
            boolean removeHintEnabled,
            MapSnapshotCacheMetrics metrics,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<UK> userKeySerializer) {
        this.keySerializer = Objects.requireNonNull(keySerializer, "keySerializer");
        this.namespaceSerializer = Objects.requireNonNull(namespaceSerializer, "namespaceSerializer");
        this.userKeySerializer = Objects.requireNonNull(userKeySerializer, "userKeySerializer");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        if (classifierEnabled) {
            throw new IllegalArgumentException(
                    "Native snapshot classifier is unavailable without RocksDB backend changes");
        }
        this.removeHintEnabled = removeHintEnabled;
        this.membershipCounts = removeHintEnabled ? new Long2IntOpenHashMap() : null;
        loadLibrary(libraryPath);
        this.handle = nativeCreate(maxEntries, NATIVE_MISS, NATIVE_EMPTY);
        if (handle == 0) {
            throw new IllegalStateException("Native snapshot cache creation returned a null handle");
        }
        LOG.info(
                "CacheKit Native snapshot JNI initialized: maxEntries={}, hash={}, removeHint={}",
                maxEntries,
                nativeHashName(liveHandle()),
                removeHintEnabled);
    }

    synchronized Lookup<UK> get(K key, N namespace) throws IOException {
        Buffers local = buffers.get();
        if (metrics.shouldSampleNativeLookupCost()) {
            return getProfiled(local, key, namespace);
        }

        KeyBytes serialized = keyBytes(local, key, namespace);
        return materializeLookup(
                serialized.words
                        ? nativeLookup16(
                                liveHandle(), serialized.firstWord, serialized.secondWord)
                        : nativeLookup(
                                liveHandle(),
                                serialized.bytes,
                                serialized.offset,
                                serialized.length));
    }

    private Lookup<UK> getProfiled(Buffers local, K key, N namespace) throws IOException {
        long encodeStart = System.nanoTime();
        KeyBytes serialized = keyBytes(local, key, namespace);
        long encodeNs = System.nanoTime() - encodeStart;
        long jniStart = System.nanoTime();
        Object result =
                serialized.words
                        ? nativeLookup16Profiled(
                                liveHandle(),
                                serialized.firstWord,
                                serialized.secondWord,
                                local.nativeCoreNs)
                        : nativeLookupProfiled(
                                liveHandle(),
                                serialized.bytes,
                                serialized.offset,
                                serialized.length,
                                local.nativeCoreNs);
        long jniNs = System.nanoTime() - jniStart;
        metrics.recordNativeKeySample(serialized.raw, serialized.length, encodeNs);
        long materializeStart = System.nanoTime();
        Lookup<UK> lookup = materializeLookup(result);
        metrics.recordNativeLookupCost(
                jniNs, local.nativeCoreNs[0], System.nanoTime() - materializeStart);
        return lookup;
    }

    private Lookup<UK> materializeLookup(Object result) {
        if (result == NATIVE_MISS) {
            return null;
        }
        if (result == NATIVE_EMPTY) {
            return Lookup.empty();
        }
        @SuppressWarnings("unchecked")
        UK userKey = (UK) result;
        return Lookup.single(userKey);
    }

    synchronized boolean putEmpty(K key, N namespace) throws IOException {
        return put(key, namespace, EMPTY, null);
    }

    synchronized boolean putSingle(K key, N namespace, UK userKey) throws IOException {
        return put(key, namespace, SINGLE, Objects.requireNonNull(userKey, "userKey"));
    }

    synchronized boolean remove(K key, N namespace) throws IOException {
        Buffers local = buffers.get();
        boolean sampled = metrics.shouldSampleNativeRemoveCost();
        long encodeStart = sampled ? System.nanoTime() : 0;
        KeyBytes serialized = keyBytes(local, key, namespace);
        metrics.recordNativeRemoveRequest();
        long hash = 0;
        if (removeHintEnabled) {
            hash = hashKeyBytes(serialized.bytes, serialized.offset, serialized.length);
            if (membershipCounts.get(hash) == 0) {
                metrics.recordNativeRemoveHintSkip();
                if (sampled) {
                    metrics.recordNativeKeySample(
                            serialized.raw,
                            serialized.length,
                            System.nanoTime() - encodeStart);
                }
                return false;
            }
        }
        metrics.recordNativeRemoveJniCall();
        boolean removed;
        if (!sampled) {
            removed = nativeRemove(
                    liveHandle(), serialized.bytes, serialized.offset, serialized.length);
        } else {
            long encodeNs = System.nanoTime() - encodeStart;
            long jniStart = System.nanoTime();
            removed = nativeRemoveProfiled(
                    liveHandle(),
                    serialized.bytes,
                    serialized.offset,
                    serialized.length,
                    local.nativeCoreNs);
            metrics.recordNativeRemoveCost(
                    System.nanoTime() - jniStart, local.nativeCoreNs[0]);
            metrics.recordNativeKeySample(serialized.raw, serialized.length, encodeNs);
        }
        if (removed) {
            metrics.recordNativeRemoveHit();
            if (removeHintEnabled) {
                decrementMembership(hash);
            }
        } else if (removeHintEnabled) {
            metrics.recordNativeRemoveHintFalsePositive();
        }
        return removed;
    }

    synchronized void clear() {
        nativeClear(liveHandle());
        if (removeHintEnabled) {
            membershipCounts.clear();
        }
    }

    synchronized int size() {
        return nativeSize(liveHandle());
    }

    synchronized String hashName() {
        return nativeHashName(liveHandle());
    }

    @Override
    public synchronized void close() {
        if (handle != 0) {
            nativeDestroy(handle);
            handle = 0;
        }
        if (removeHintEnabled) {
            membershipCounts.clear();
        }
        buffers.remove();
    }

    private boolean put(K key, N namespace, int kind, UK userKey) throws IOException {
        Buffers local = buffers.get();
        boolean sampled = metrics.shouldSampleNativePutCost();
        long encodeStart = sampled ? System.nanoTime() : 0;
        KeyBytes serialized = keyBytes(local, key, namespace);
        int result;
        if (sampled) {
            long encodeNs = System.nanoTime() - encodeStart;
            long jniStart = System.nanoTime();
            result = nativePutProfiled(
                    liveHandle(),
                    serialized.bytes,
                    serialized.offset,
                    serialized.length,
                    kind,
                    userKey,
                    local.nativeCoreNs,
                    removeHintEnabled ? local.evictedHash : null);
            metrics.recordNativePutCost(
                    System.nanoTime() - jniStart, local.nativeCoreNs[0]);
            metrics.recordNativeKeySample(serialized.raw, serialized.length, encodeNs);
        } else {
            result = nativePut(
                    liveHandle(),
                    serialized.bytes,
                    serialized.offset,
                    serialized.length,
                    kind,
                    userKey,
                    removeHintEnabled ? local.evictedHash : null);
        }
        if (result == PUT_REJECTED) {
            throw new IllegalStateException("Native snapshot cache rejected a valid put");
        }
        if (removeHintEnabled) {
            long hash = hashKeyBytes(serialized.bytes, serialized.offset, serialized.length);
            if (result == PUT_INSERTED_WITH_EVICTION) {
                decrementMembership(local.evictedHash[0]);
                membershipCounts.addTo(hash, 1);
            } else if (result == PUT_INSERTED) {
                membershipCounts.addTo(hash, 1);
            } else if (result != PUT_UPDATED) {
                throw new IllegalStateException("Native snapshot cache returned unknown put result: " + result);
            }
        }
        return result == PUT_INSERTED_WITH_EVICTION;
    }

    private void decrementMembership(long hash) {
        int count = membershipCounts.get(hash);
        if (count <= 1) {
            membershipCounts.remove(hash);
        } else {
            membershipCounts.put(hash, count - 1);
        }
    }

    static long hashKeyBytes(byte[] bytes, int offset, int length) {
        long hash = 1469598103934665603L;
        for (int index = offset; index < offset + length; index++) {
            hash ^= bytes[index] & 0xffL;
            hash *= 1099511628211L;
        }
        hash ^= hash >>> 30;
        hash *= 0xbf58476d1ce4e5b9L;
        hash ^= hash >>> 27;
        hash *= 0x94d049bb133111ebL;
        return hash ^ (hash >>> 31);
    }

    static long nativeHashForTesting(byte[] bytes, int offset, int length) {
        return nativeHashForTesting0(bytes, offset, length);
    }

    private void serializeKey(DataOutputSerializer output, K key, N namespace)
            throws IOException {
        output.clear();
        keySerializer.serialize(Objects.requireNonNull(key, "key"), output);
        namespaceSerializer.serialize(Objects.requireNonNull(namespace, "namespace"), output);
    }

    private KeyBytes keyBytes(Buffers local, K key, N namespace) throws IOException {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(namespace, "namespace");
        if (key instanceof BinaryRowData && namespace instanceof VoidNamespace) {
            BinaryRowData row = (BinaryRowData) key;
            MemorySegment[] segments = row.getSegments();
            if (segments.length == 1 && !segments[0].isOffHeap()) {
                if (row.getSizeInBytes() == 16) {
                    return local.keyBytes.resetWords(
                            segments[0].getArray(),
                            row.getOffset(),
                            segments[0].getLong(row.getOffset()),
                            segments[0].getLong(row.getOffset() + Long.BYTES));
                }
                return local.keyBytes.reset(
                        segments[0].getArray(), row.getOffset(), row.getSizeInBytes(), true);
            }
        }
        serializeKey(local.keyOutput, key, namespace);
        return local.keyBytes.reset(
                local.keyOutput.getSharedBuffer(), 0, local.keyOutput.length(), false);
    }

    private long liveHandle() {
        if (handle == 0) {
            throw new IllegalStateException("Native snapshot cache is closed");
        }
        return handle;
    }

    private static synchronized void loadLibrary(String configuredPath) {
        loadLibrary(configuredPath, true);
    }

    private static synchronized void loadLibrary(String configuredPath, boolean logEmbeddedLoad) {
        String requested = configuredPath == null ? "" : configuredPath.trim();
        String identity = requested.isEmpty() ? EMBEDDED_LIBRARY : requested;
        if (loadedLibrary != null) {
            if (!loadedLibrary.equals(identity)) {
                throw new IllegalStateException(
                        "Native snapshot library already loaded from " + loadedLibrary
                                + ", cannot switch to " + identity);
            }
            return;
        }
        if (!requested.isEmpty()) {
            System.load(requested);
        } else {
            loadEmbeddedLibrary(logEmbeddedLoad);
        }
        loadedLibrary = identity;
    }

    private static void loadEmbeddedLibrary(boolean logEmbeddedLoad) {
        try (InputStream input = NativeMapSnapshotCache.class.getResourceAsStream(EMBEDDED_LIBRARY)) {
            if (input == null) {
                System.loadLibrary("cachekit_snapshot_jni");
                return;
            }
            Path extracted = Files.createTempFile("cachekit_snapshot_jni-", ".so");
            Files.copy(input, extracted, StandardCopyOption.REPLACE_EXISTING);
            extracted.toFile().deleteOnExit();
            System.load(extracted.toAbsolutePath().toString());
            if (logEmbeddedLoad) {
                LOG.info(
                        "Loaded embedded CacheKit Native snapshot JNI library from {}",
                        EMBEDDED_LIBRARY);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to extract embedded Native snapshot library", e);
        }
    }

    static final class Lookup<T> {
        private final T userKey;

        private Lookup(T userKey) {
            this.userKey = userKey;
        }

        boolean isEmpty() {
            return userKey == null;
        }

        T userKey() {
            return userKey;
        }

        static <T> Lookup<T> empty() {
            return new Lookup<>(null);
        }

        static <T> Lookup<T> single(T userKey) {
            return new Lookup<>(userKey);
        }
    }

    private static final class Buffers {
        private final DataOutputSerializer keyOutput = new DataOutputSerializer(128);
        private final KeyBytes keyBytes = new KeyBytes();
        private final long[] nativeCoreNs = new long[1];
        private final long[] evictedHash = new long[1];
    }

    private static final class KeyBytes {
        private byte[] bytes;
        private int offset;
        private int length;
        private boolean raw;
        private boolean words;
        private long firstWord;
        private long secondWord;

        private KeyBytes reset(byte[] bytes, int offset, int length, boolean raw) {
            this.bytes = bytes;
            this.offset = offset;
            this.length = length;
            this.raw = raw;
            this.words = false;
            return this;
        }

        private KeyBytes resetWords(
                byte[] bytes, int offset, long firstWord, long secondWord) {
            this.bytes = bytes;
            this.offset = offset;
            this.length = 16;
            this.raw = true;
            this.words = true;
            this.firstWord = firstWord;
            this.secondWord = secondWord;
            return this;
        }
    }

    private static native long nativeCreate(
            int maxEntries,
            Object missSentinel,
            Object emptySentinel);

    private static native boolean nativeSnapshotFeatureAvailable();

    private static native void nativeDestroy(long handle);

    private static native int nativePut(
            long handle,
            byte[] key,
            int keyOffset,
            int keySize,
            int kind,
            Object userKey,
            long[] evictedHash);

    private static native int nativePutProfiled(
            long handle,
            byte[] key,
            int keyOffset,
            int keySize,
            int kind,
            Object userKey,
            long[] nativeCoreNs,
            long[] evictedHash);

    private static native long nativeHashForTesting0(
            byte[] key, int keyOffset, int keySize);

    private static native Object nativeLookup(
            long handle, byte[] key, int keyOffset, int keySize);

    private static native Object nativeLookup16(long handle, long firstWord, long secondWord);

    private static native Object nativeLookup16Profiled(
            long handle, long firstWord, long secondWord, long[] nativeCoreNs);

    private static native Object nativeLookupProfiled(
            long handle, byte[] key, int keyOffset, int keySize, long[] nativeCoreNs);

    private static native boolean nativeRemove(
            long handle, byte[] key, int keyOffset, int keySize);

    private static native boolean nativeRemoveProfiled(
            long handle, byte[] key, int keyOffset, int keySize, long[] nativeCoreNs);

    private static native void nativeClear(long handle);

    private static native int nativeSize(long handle);

    private static native String nativeHashName(long handle);

}
