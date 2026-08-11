/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBMapStateNativeSnapshotAccess;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.table.data.binary.BinaryRowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/** JNI-backed (key, namespace) to EMPTY/SINGLE(userKey) snapshot cache. */
final class NativeMapSnapshotCache<K, N, UK> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NativeMapSnapshotCache.class);

    static final int EMPTY = 1;
    static final int SINGLE = 2;

    private static final Object NATIVE_MISS = new Object();
    private static final Object NATIVE_EMPTY = new Object();
    private static final Object NATIVE_MULTI = new Object();
    private static String loadedLibrary;

    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<UK> userKeySerializer;
    private final ThreadLocal<Buffers> buffers = ThreadLocal.withInitial(Buffers::new);
    private final DataInputDeserializer userKeyInput = new DataInputDeserializer();
    private final boolean classifierEnabled;
    private long handle;

    NativeMapSnapshotCache(
            int maxEntries,
            String kernel,
            String libraryPath,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<UK> userKeySerializer) {
        this(
                maxEntries,
                kernel,
                libraryPath,
                false,
                keySerializer,
                namespaceSerializer,
                userKeySerializer);
    }

    NativeMapSnapshotCache(
            int maxEntries,
            String kernel,
            String libraryPath,
            boolean classifierEnabled,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<UK> userKeySerializer) {
        this.keySerializer = Objects.requireNonNull(keySerializer, "keySerializer");
        this.namespaceSerializer = Objects.requireNonNull(namespaceSerializer, "namespaceSerializer");
        this.userKeySerializer = Objects.requireNonNull(userKeySerializer, "userKeySerializer");
        this.classifierEnabled = classifierEnabled;
        loadLibrary(libraryPath);
        this.handle =
                nativeCreate(
                        maxEntries,
                        kernelId(kernel),
                        NATIVE_MISS,
                        NATIVE_EMPTY,
                        NATIVE_MULTI,
                        classifierEnabled);
        if (handle == 0) {
            throw new IllegalStateException("Native snapshot cache creation returned a null handle");
        }
        LOG.info(
                "CacheKit Native snapshot JNI initialized: maxEntries={}, kernel={}, classifier={}",
                maxEntries,
                kernelName(),
                classifierEnabled ? nativeBridgeDescription(liveHandle()) : "disabled");
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

    synchronized Classification<UK> classify(
            RocksDBMapStateNativeSnapshotAccess access) throws IOException {
        if (!classifierEnabled) {
            throw new IllegalStateException("Native snapshot classifier is disabled");
        }
        byte[] prefix = access.serializeCurrentKeyNamespacePrefix();
        Object result = nativeClassifyPrefix(
                liveHandle(),
                access.getDbNativeHandle(),
                access.getColumnFamilyNativeHandle(),
                access.getReadOptionsNativeHandle(),
                prefix,
                access.getKeyGroupPrefixBytes());
        if (result == NATIVE_EMPTY) {
            return Classification.empty();
        }
        if (result == NATIVE_MULTI) {
            return Classification.multi();
        }
        if (!(result instanceof byte[])) {
            throw new IllegalStateException("Native classifier returned an invalid result");
        }
        return Classification.single(decodeUserKey((byte[]) result, prefix.length));
    }

    synchronized PrefixBatch<UK> readPrefixBatch(
            RocksDBMapStateNativeSnapshotAccess access,
            byte[] prefix,
            byte[] startAfter,
            int maxEntries)
            throws IOException {
        if (!classifierEnabled) {
            throw new IllegalStateException("Native snapshot classifier is disabled");
        }
        Object[] result = nativeReadPrefixBatch(
                liveHandle(),
                access.getDbNativeHandle(),
                access.getColumnFamilyNativeHandle(),
                access.getReadOptionsNativeHandle(),
                prefix,
                access.getKeyGroupPrefixBytes(),
                startAfter,
                maxEntries);
        List<PrefixEntry<UK>> entries = new ArrayList<>(maxEntries);
        for (int index = 0; index < result.length; index += 2) {
            Object keyOrMarker = result[index];
            if (keyOrMarker == NATIVE_EMPTY) {
                return new PrefixBatch<>(prefix, entries, false);
            }
            if (keyOrMarker == NATIVE_MULTI) {
                return new PrefixBatch<>(prefix, entries, true);
            }
            if (!(keyOrMarker instanceof byte[])
                    || index + 1 >= result.length
                    || !(result[index + 1] instanceof byte[])) {
                throw new IllegalStateException("Native prefix batch returned an invalid result");
            }
            byte[] rawKey = (byte[]) keyOrMarker;
            entries.add(
                    new PrefixEntry<>(
                            rawKey,
                            decodeUserKey(rawKey, prefix.length),
                            (byte[]) result[index + 1]));
        }
        throw new IllegalStateException("Native prefix batch omitted its continuation marker");
    }

    private UK decodeUserKey(byte[] rawKey, int userKeyOffset) throws IOException {
        if (rawKey.length < userKeyOffset) {
            throw new IllegalStateException("Native classifier returned a truncated RocksDB key");
        }
        userKeyInput.setBuffer(rawKey, userKeyOffset, rawKey.length - userKeyOffset);
        UK userKey = userKeySerializer.deserialize(userKeyInput);
        if (userKeyInput.available() != 0) {
            throw new IllegalStateException("Native classifier user-key suffix was not fully decoded");
        }
        return userKey;
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

    static final class Classification<T> {
        private static final int EMPTY_KIND = 1;
        private static final int SINGLE_KIND = 2;
        private static final int MULTI_KIND = 3;

        private final int kind;
        private final T userKey;

        private Classification(int kind, T userKey) {
            this.kind = kind;
            this.userKey = userKey;
        }

        boolean isEmpty() {
            return kind == EMPTY_KIND;
        }

        boolean isSingle() {
            return kind == SINGLE_KIND;
        }

        T userKey() {
            return userKey;
        }

        static <T> Classification<T> empty() {
            return new Classification<>(EMPTY_KIND, null);
        }

        static <T> Classification<T> single(T userKey) {
            return new Classification<>(SINGLE_KIND, userKey);
        }

        static <T> Classification<T> multi() {
            return new Classification<>(MULTI_KIND, null);
        }
    }

    static final class PrefixBatch<T> {
        private final byte[] prefix;
        private final List<PrefixEntry<T>> entries;
        private final boolean hasMore;

        private PrefixBatch(
                byte[] prefix, List<PrefixEntry<T>> entries, boolean hasMore) {
            this.prefix = prefix;
            this.entries = entries;
            this.hasMore = hasMore;
        }

        byte[] prefix() {
            return prefix;
        }

        List<PrefixEntry<T>> entries() {
            return entries;
        }

        boolean hasMore() {
            return hasMore;
        }
    }

    static final class PrefixEntry<T> {
        private final byte[] rawKey;
        private final T userKey;
        private final byte[] rawValue;

        private PrefixEntry(byte[] rawKey, T userKey, byte[] rawValue) {
            this.rawKey = rawKey;
            this.userKey = userKey;
            this.rawValue = rawValue;
        }

        byte[] rawKey() {
            return rawKey;
        }

        T userKey() {
            return userKey;
        }

        byte[] rawValue() {
            return rawValue;
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
            int maxEntries,
            int kernel,
            Object missSentinel,
            Object emptySentinel,
            Object multiSentinel,
            boolean classifierEnabled);

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

    private static native String nativeBridgeDescription(long handle);

    private static native Object nativeClassifyPrefix(
            long handle,
            long dbHandle,
            long columnFamilyHandle,
            long readOptionsHandle,
            byte[] prefix,
            int compareOffset);

    private static native Object[] nativeReadPrefixBatch(
            long handle,
            long dbHandle,
            long columnFamilyHandle,
            long readOptionsHandle,
            byte[] prefix,
            int compareOffset,
            byte[] startAfter,
            int maxEntries);
}
