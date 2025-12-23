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
import org.apache.flink.contrib.streaming.state.cachekit.cache.LruCachePolicy;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.table.data.binary.BinaryRowData;

import javax.annotation.Nonnull;

import java.io.IOException;
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
    private final CachePolicy<KeyNamespaceKey<K, N>, CachedValue<V>> cache;

    private N currentNamespace;

    private final java.util.function.Consumer<K> keyContextSetter;

    public CachedInternalValueState(
            InternalValueState<K, N, V> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
            int maxEntries) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.currentKeyProvider = Objects.requireNonNull(currentKeyProvider, "currentKeyProvider");
        this.keyContextSetter = Objects.requireNonNull(keyContextSetter, "keyContextSetter");
        this.cache = new LruCachePolicy<>(maxEntries, this::onEviction);
    }

    @Override
    public V value() throws IOException {
        KeyNamespaceKey<K, N> cacheKey = new KeyNamespaceKey<>(currentKeyProvider.getCurrentKey(), currentNamespace);
        CachedValue<V> cached = cache.get(cacheKey);
        if (cached != null) {
            return cached.valueOrNull();
        }
        V loaded = delegate.value();
        // Load callback: put clean value
        cache.put(cacheKey, CachedValue.of(loaded, false));
        return loaded;
    }

    @Override
    public void update(V value) throws IOException {
        KeyNamespaceKey<K, N> cacheKey = new KeyNamespaceKey<>(currentKeyProvider.getCurrentKey(), currentNamespace);
        cache.put(cacheKey, CachedValue.of(value, true));
    }

    @Override
    public void clear() {
        KeyNamespaceKey<K, N> cacheKey = new KeyNamespaceKey<>(currentKeyProvider.getCurrentKey(), currentNamespace);
        cache.put(cacheKey, CachedValue.of(null, true));
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
        return delegate.getSerializedValue(
                serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer, safeValueSerializer);
    }

    @Override
    public InternalKvState.StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return delegate.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    public void flush() {
        java.util.List<java.util.Map.Entry<KeyNamespaceKey<K, N>, CachedValue<V>>> dirtyEntries = new java.util.ArrayList<>();
        for (java.util.Map.Entry<KeyNamespaceKey<K, N>, CachedValue<V>> entry : cache.entries()) {
            if (entry.getValue().dirty) {
                dirtyEntries.add(entry);
            }
        }
        for (java.util.Map.Entry<KeyNamespaceKey<K, N>, CachedValue<V>> entry : dirtyEntries) {
            CachedValue<V> val = entry.getValue();
            // Double check if still dirty (though likely unchanged)
            if (val.dirty) {
                flushEntry(entry.getKey(), val);
                cache.put(entry.getKey(), CachedValue.of(val.value, false));
            }
        }
    }

    private void onEviction(KeyNamespaceKey<K, N> key, CachedValue<V> value) {
        if (value.dirty) {
            // Save current context
            K previousKey = currentKeyProvider.getCurrentKey();
            // N previousNamespace = currentNamespace; // currentNamespace might track
            // thread local, but delegate has its own
            // We can't easily get 'currentNamespace' from provided N if it's not exposed.
            // But we have 'currentNamespace' field in this class which tracks what we set.
            // Be careful: 'currentNamespace' field tracks what user sets.
            // When we switch key, we must also switch namespace to what the key expects?
            // The KeyNamespaceKey has the namespace.

            try {
                flushEntry(key, value);
            } finally {
                // Restore context
                keyContextSetter.accept(previousKey);
                // delegate.setCurrentNamespace(previousNamespace);
                // We must restore the namespace that was active before eviction!
                // 'currentNamespace' field holds the arguably 'active' namespace for the user.
                if (currentNamespace != null) {
                    delegate.setCurrentNamespace(currentNamespace);
                }
            }
        }
    }

    private void flushEntry(KeyNamespaceKey<K, N> key, CachedValue<V> value) {
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
        }
    }

    private static final class KeyNamespaceKey<K, N> {
        private final K key;
        private final N namespace;

        private KeyNamespaceKey(K key, N namespace) {
            if (key instanceof BinaryRowData) {
                this.key = (K) ((BinaryRowData) key).copy();
            } else {
                this.key = key;
            }
            if (namespace instanceof BinaryRowData) {
                this.namespace = (N) ((BinaryRowData) namespace).copy();
            } else {
                this.namespace = namespace;
            }
        }

        // TODO: Binary RowData Deep Copy

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
