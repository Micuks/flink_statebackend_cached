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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/** JNI-backed (key, namespace) to EMPTY/SINGLE(userKey) snapshot cache. */
final class NativeMapSnapshotCache<K, N, UK> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NativeMapSnapshotCache.class);

    static final int EMPTY = 1;
    static final int SINGLE = 2;

    private static final Object NATIVE_MISS = new Object();
    private static final Object NATIVE_EMPTY = new Object();
    private static String loadedLibrary;

    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final ThreadLocal<Buffers> buffers = ThreadLocal.withInitial(Buffers::new);
    private long handle;

    NativeMapSnapshotCache(
            int maxEntries,
            String kernel,
            String libraryPath,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<UK> userKeySerializer) {
        this.keySerializer = Objects.requireNonNull(keySerializer, "keySerializer");
        this.namespaceSerializer = Objects.requireNonNull(namespaceSerializer, "namespaceSerializer");
        Objects.requireNonNull(userKeySerializer, "userKeySerializer");
        loadLibrary(libraryPath);
        this.handle =
                nativeCreate(maxEntries, kernelId(kernel), NATIVE_MISS, NATIVE_EMPTY);
        if (handle == 0) {
            throw new IllegalStateException("Native snapshot cache creation returned a null handle");
        }
        LOG.info(
                "CacheKit Native MapSnapshot cache initialized: maxEntries={}, kernel={}",
                maxEntries,
                kernelName());
    }

    synchronized Lookup<UK> get(K key, N namespace) throws IOException {
        Buffers local = buffers.get();
        KeyBytes serialized = keyBytes(local, key, namespace);
        Object result =
                nativeLookup(
                        liveHandle(),
                        serialized.bytes,
                        serialized.offset,
                        serialized.length);
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
        KeyBytes serialized = keyBytes(local, key, namespace);
        return nativeRemove(
                liveHandle(), serialized.bytes, serialized.offset, serialized.length);
    }

    synchronized void clear() {
        nativeClear(liveHandle());
    }

    synchronized int size() {
        return nativeSize(liveHandle());
    }

    synchronized String kernelName() {
        return nativeKernelName(liveHandle());
    }

    @Override
    public synchronized void close() {
        if (handle != 0) {
            nativeDestroy(handle);
            handle = 0;
        }
        buffers.remove();
    }

    private boolean put(K key, N namespace, int kind, UK userKey) throws IOException {
        Buffers local = buffers.get();
        KeyBytes serialized = keyBytes(local, key, namespace);
        int result = nativePut(
                liveHandle(),
                serialized.bytes,
                serialized.offset,
                serialized.length,
                kind,
                userKey);
        if (result == 0) {
            throw new IllegalStateException("Native snapshot cache rejected a valid put");
        }
        return result == 2;
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
                return local.keyBytes.reset(
                        segments[0].getArray(), row.getOffset(), row.getSizeInBytes());
            }
        }
        serializeKey(local.keyOutput, key, namespace);
        return local.keyBytes.reset(local.keyOutput.getSharedBuffer(), 0, local.keyOutput.length());
    }

    private long liveHandle() {
        if (handle == 0) {
            throw new IllegalStateException("Native snapshot cache is closed");
        }
        return handle;
    }

    private static synchronized void loadLibrary(String configuredPath) {
        String requested = configuredPath == null ? "" : configuredPath.trim();
        String identity = requested.isEmpty() ? "cachekit_snapshot_jni" : requested;
        if (loadedLibrary != null) {
            if (!loadedLibrary.equals(identity)) {
                throw new IllegalStateException(
                        "Native snapshot library already loaded from " + loadedLibrary
                                + ", cannot switch to " + identity);
            }
            return;
        }
        if (requested.isEmpty()) {
            System.loadLibrary(identity);
        } else {
            System.load(requested);
        }
        loadedLibrary = identity;
    }

    private static int kernelId(String configuredKernel) {
        String kernel = configuredKernel == null
                ? "AUTO"
                : configuredKernel.trim().toUpperCase(Locale.ROOT);
        switch (kernel) {
            case "SCALAR":
                return 0;
            case "NEON":
                return 1;
            case "SVE":
                return 2;
            case "AUTO":
                return 3;
            default:
                throw new IllegalArgumentException("Unknown Native snapshot kernel: " + configuredKernel);
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
    }

    private static final class KeyBytes {
        private byte[] bytes;
        private int offset;
        private int length;

        private KeyBytes reset(byte[] bytes, int offset, int length) {
            this.bytes = bytes;
            this.offset = offset;
            this.length = length;
            return this;
        }
    }

    private static native long nativeCreate(
            int maxEntries, int kernel, Object missSentinel, Object emptySentinel);

    private static native void nativeDestroy(long handle);

    private static native int nativePut(
            long handle,
            byte[] key,
            int keyOffset,
            int keySize,
            int kind,
            Object userKey);

    private static native Object nativeLookup(
            long handle, byte[] key, int keyOffset, int keySize);

    private static native boolean nativeRemove(
            long handle, byte[] key, int keyOffset, int keySize);

    private static native void nativeClear(long handle);

    private static native int nativeSize(long handle);

    private static native String nativeKernelName(long handle);
}
