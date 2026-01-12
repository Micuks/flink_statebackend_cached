/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicy;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CaffeineCachePolicy;
import org.apache.flink.contrib.streaming.state.cachekit.cache.LruCachePolicy;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.table.data.binary.BinaryRowData;

import javax.annotation.Nonnull;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * Minimal {@link InternalValueState} wrapper that adds a per-state LRU cache.
 *
 * <p>
 * Keying: (currentKey, namespace).
 */
public final class CachedInternalValueState<K, N, V> implements InternalValueState<K, N, V> {

    private final InternalValueState<K, N, V> delegate;
    private final CurrentKeyProvider<K> currentKeyProvider;
    private final CachePolicy<KeyNamespaceKey<K, N>, CachedValue<V>> l1Cache;
    private final CachePolicy<KeyNamespaceKey<K, N>, CachedValue<V>> l2Cache;
    private final CachePolicyType cachePolicyType;
    private final int lruOverflow;

    private N currentNamespace;

    // Sticky Cache (L1)
    private KeyNamespaceKey<K, N> lastAccessKey;
    private CachedValue<V> lastAccessValue;

    private final java.util.function.Consumer<K> keyContextSetter;

    // Access logging
    private final String logFilePath;
    private final BufferedWriter logWriter;
    private final Object logLock = new Object();

    /** Access event type enumeration. */
    public enum AccessEventType {
        VALUE_READ,
        VALUE_UPDATE,
        VALUE_CLEAR,
        VALUE_READ_STICKY_HIT,
        VALUE_READ_L1_HIT,
        VALUE_READ_L2_HIT,
        VALUE_READ_DELEGATE_LOAD
    }

    public CachedInternalValueState(
            InternalValueState<K, N, V> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
            int maxEntries,
            CachePolicyType cachePolicyType,
            int lruOverflow) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.currentKeyProvider = Objects.requireNonNull(currentKeyProvider, "currentKeyProvider");
        this.keyContextSetter = Objects.requireNonNull(keyContextSetter, "keyContextSetter");
        this.cachePolicyType = Objects.requireNonNull(cachePolicyType, "cachePolicyType");
        this.lruOverflow = Math.max(0, lruOverflow);
        this.logFilePath = "./value_state_access_log.txt";

        // L1 Cache: ~20% of maxEntries or at least 128
        int l1Size = Math.max(128, maxEntries / 5);
        this.l1Cache = createCachePolicy(l1Size, this::onL1Eviction);

        // L2 Cache: Remaining size (or full maxEntries if we treat L2 as the main
        // capacity)
        // Plan said: "use existing maxEntries for L2".
        this.l2Cache = createCachePolicy(maxEntries, this::onL2Eviction);

        // Initialize log writer if log file path is provided
        if (logFilePath != null && !logFilePath.isEmpty()) {
            try {
                Path path = Paths.get(logFilePath);
                // Create parent directories if they don't exist
                if (path.getParent() != null) {
                    Files.createDirectories(path.getParent());
                }
                this.logWriter = new BufferedWriter(new FileWriter(logFilePath, true));
                // Write header
                synchronized (logLock) {
                    logWriter.write("# timestamp\tkey\tnamespace\tevent_type\tcache_level");
                    logWriter.newLine();
                    logWriter.flush();
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to initialize log file: " + logFilePath, e);
            }
        } else {
            this.logWriter = null;
        }
    }

    @Override
    public V value() throws IOException {
        K currentKey = currentKeyProvider.getCurrentKey();

        // 1. Check Sticky Cache (Fast Path)
        if (lastAccessKey != null && lastAccessKey.isSame(currentKey, currentNamespace)) {
            recordAccess(currentKey, currentNamespace, AccessEventType.VALUE_READ_STICKY_HIT, "STICKY");
            return lastAccessValue.valueOrNull();
        }

        KeyNamespaceKey<K, N> probeKey = new KeyNamespaceKey<>(currentKey, currentNamespace, false);

        // 2. Check L1 Cache
        CachedValue<V> l1Cached = l1Cache.get(probeKey);
        if (l1Cached != null) {
            recordAccess(currentKey, currentNamespace, AccessEventType.VALUE_READ_L1_HIT, "L1");
            updateSticky(probeKey, l1Cached);
            return l1Cached.valueOrNull();
        }

        // 3. Check L2 Cache
        CachedValue<V> l2Cached = l2Cache.get(probeKey);
        if (l2Cached != null) {
            recordAccess(currentKey, currentNamespace, AccessEventType.VALUE_READ_L2_HIT, "L2");
            // Promote to L1 (Clean)
            // Note: This puts a clean entry in L1.
            // If sticky is updated, next access hits sticky.
            // If L1 eviction happens, clean entry might be demoted back to L2.
            KeyNamespaceKey<K, N> storageKey = new KeyNamespaceKey<>(currentKey, currentNamespace, true);
            CachedValue<V> newValue = CachedValue.of(l2Cached.valueOrNull(), false);
            l1Cache.put(storageKey, newValue);

            updateSticky(storageKey, newValue);
            return l2Cached.valueOrNull();
        }

        // 4. Load from Delegate
        recordAccess(currentKey, currentNamespace, AccessEventType.VALUE_READ_DELEGATE_LOAD, "DELEGATE");
        V loaded = delegate.value();

        // 5. Update L1 (Clean) - commonly new hot data goes to L1
        KeyNamespaceKey<K, N> storageKey = new KeyNamespaceKey<>(currentKey, currentNamespace, true);
        CachedValue<V> newValue = CachedValue.of(loaded, false);
        l1Cache.put(storageKey, newValue);

        // Also ensure it's in L2?
        // Standard multi-level: Load -> L1. Evict L1 -> L2.
        // So we don't put in L2 here.

        updateSticky(storageKey, newValue);
        return loaded;
    }

    @Override
    public void update(V value) throws IOException {
        K currentKey = currentKeyProvider.getCurrentKey();
        recordAccess(currentKey, currentNamespace, AccessEventType.VALUE_UPDATE, "L1");
        KeyNamespaceKey<K, N> cacheKey = new KeyNamespaceKey<>(currentKey, currentNamespace, true);
        CachedValue<V> newValue = CachedValue.of(value, true);

        // Write-Back: Update L1 only (marked dirty)
        l1Cache.put(cacheKey, newValue);

        // Invalidate L2 to avoid stale data if L1 evicts later and L2 has older
        // version?
        // OR rely on L1 eviction overwriting L2.
        // If L2 has it, it's now stale.
        // Optimally: l2Cache.remove(cacheKey);
        // But LruCachePolicy might not have efficient remove without key object match.
        // Assuming put to L1 eventually flushes to L2.
        // If L1 evicts, it will overwrite L2.
        // However, if we possess a valid entry in L2, we should probably remove it or
        // update it?
        // Simpler: Just update L1. If L1 evicts, it pushes to L2.

        updateSticky(cacheKey, newValue);
    }

    @Override
    public void clear() {
        K currentKey = currentKeyProvider.getCurrentKey();
        recordAccess(currentKey, currentNamespace, AccessEventType.VALUE_CLEAR, "L1");
        KeyNamespaceKey<K, N> cacheKey = new KeyNamespaceKey<>(currentKey, currentNamespace, true);
        CachedValue<V> newValue = CachedValue.of(null, true);

        l1Cache.put(cacheKey, newValue);

        updateSticky(cacheKey, newValue);
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return delegate.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return delegate.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return delegate.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(@Nonnull N namespace) {
        this.currentNamespace = namespace;
        delegate.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<V> safeValueSerializer)
            throws Exception {
        // Must flush to ensure delegate has latest state before serialization
        flush();
        return delegate.getSerializedValue(
                serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer, safeValueSerializer);
    }

    @Override
    public InternalKvState.StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        // Visitor bypasses cache, so flush first
        flush();
        return delegate.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    public void flush() {
        // Flush L1 dirty entries to L2 (which writes through)
        java.util.List<java.util.Map.Entry<KeyNamespaceKey<K, N>, CachedValue<V>>> dirtyEntries = new java.util.ArrayList<>();
        for (java.util.Map.Entry<KeyNamespaceKey<K, N>, CachedValue<V>> entry : l1Cache.entries()) {
            if (entry.getValue().dirty) {
                dirtyEntries.add(entry);
            }
        }
        for (java.util.Map.Entry<KeyNamespaceKey<K, N>, CachedValue<V>> entry : dirtyEntries) {
            CachedValue<V> val = entry.getValue();
            if (val.dirty) {
                // Push to L2 (Write-Through)
                // We simulate eviction to L2
                l2Cache.put(entry.getKey(), CachedValue.of(val.value, false)); // L2 holds clean
                flushEntryToDelegate(entry.getKey(), val); // Write-through to delegate

                // Mark L1 clean
                l1Cache.put(entry.getKey(), CachedValue.of(val.value, false));
            }
        }
    }

    private void updateSticky(KeyNamespaceKey<K, N> key, CachedValue<V> value) {
        lastAccessKey = key;
        lastAccessValue = value;
    }

    /**
     * Records an access event for the given key and namespace.
     *
     * @param key the key being accessed
     * @param namespace the namespace being accessed
     * @param eventType the type of access event
     * @param cacheLevel the cache level where the access occurred
     */
    private void recordAccess(K key, N namespace, AccessEventType eventType, String cacheLevel) {
        if (logWriter == null) {
            return;
        }

        long timestamp = System.nanoTime();
        String keyStr = key != null ? key.toString() : "null";
        String namespaceStr = namespace != null ? namespace.toString() : "null";

        synchronized (logLock) {
            try {
                logWriter.write(String.format("%d\t%s\t%s\t%s\t%s%n",
                        timestamp, keyStr, namespaceStr, eventType, cacheLevel));
                logWriter.flush();
            } catch (IOException e) {
                // Log error but don't throw to avoid breaking cache operations
                System.err.println("Failed to write to access log file: " + e.getMessage());
            }
        }
    }

    /**
     * Closes the log file writer if it was opened.
     * Should be called when the state is no longer needed to ensure all data is flushed.
     */
    public void close() {
        if (logWriter != null) {
            synchronized (logLock) {
                try {
                    logWriter.flush();
                    logWriter.close();
                } catch (IOException e) {
                    System.err.println("Failed to close access log file: " + e.getMessage());
                }
            }
        }
    }

    private CachePolicy<KeyNamespaceKey<K, N>, CachedValue<V>> createCachePolicy(
            int maxEntries,
            java.util.function.BiConsumer<KeyNamespaceKey<K, N>, CachedValue<V>> evictionListener) {
        if (cachePolicyType == CachePolicyType.CAFFEINE) {
            return new CaffeineCachePolicy<>(maxEntries, evictionListener);
        }
        return new LruCachePolicy<>(maxEntries, lruOverflow, evictionListener);
    }

    // L1 Eviction Listener
    private void onL1Eviction(KeyNamespaceKey<K, N> key, CachedValue<V> value) {
        // Demote to L2

        if (value.dirty) {
            // Write-Back: Flush to Delegate first (because L2 is Write-Through / Clean)
            // Or put to L2 and let L2 write-through?
            // "L2 Write-Through" implies: Putting to L2 triggers write to delegate.

            // 1. Write to Delegate
            flushEntryToDelegate(key, value);

            // 2. Put to L2 (Clean)
            l2Cache.put(key, CachedValue.of(value.valueOrNull(), false));

        } else {
            // Clean L1 eviction: Just move to L2
            l2Cache.put(key, value);
        }
    }

    // L2 Eviction Listener
    private void onL2Eviction(KeyNamespaceKey<K, N> key, CachedValue<V> value) {
        // L2 is clean (backed by delegate). Just drop.
    }

    private void flushEntryToDelegate(KeyNamespaceKey<K, N> key, CachedValue<V> value) {
        // Save current context
        K previousKey = currentKeyProvider.getCurrentKey();
        // We rely on 'currentNamespace' field in this class but it might have changed.
        // We must use the namespace from the key.

        keyContextSetter.accept(key.key);
        delegate.setCurrentNamespace(key.namespace);
        try {
            if (value.isNull) {
                delegate.clear();
            } else {
                delegate.update(value.value);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush state to delegate interaction", e);
        } finally {
            // Restore context
            keyContextSetter.accept(previousKey);
            if (currentNamespace != null) {
                delegate.setCurrentNamespace(currentNamespace);
            }
        }
    }

    private static final class KeyNamespaceKey<K, N> {
        private final K key;
        private final N namespace;

        private KeyNamespaceKey(K key, N namespace, boolean deepCopy) {
            if (deepCopy && key instanceof BinaryRowData) {
                this.key = (K) ((BinaryRowData) key).copy();
            } else {
                this.key = key;
            }
            if (deepCopy && namespace instanceof BinaryRowData) {
                this.namespace = (N) ((BinaryRowData) namespace).copy();
            } else {
                this.namespace = namespace;
            }
        }

        boolean isSame(K otherKey, N otherNamespace) {
            return Objects.equals(this.key, otherKey) && Objects.equals(this.namespace, otherNamespace);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof KeyNamespaceKey)) {
                return false;
            }
            KeyNamespaceKey<?, ?> that = (KeyNamespaceKey<?, ?>) other;
            return Objects.equals(key, that.key) && Objects.equals(namespace, that.namespace);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, namespace);
        }
    }

    private static final class CachedValue<V> {
        private final V value;
        private final boolean isNull;
        private final boolean dirty;

        private CachedValue(V value, boolean isNull, boolean dirty) {
            this.value = value;
            this.isNull = isNull;
            this.dirty = dirty;
        }

        static <V> CachedValue<V> of(V value, boolean dirty) {
            return new CachedValue<>(value, value == null, dirty);
        }

        V valueOrNull() {
            return isNull ? null : value;
        }
    }
}
