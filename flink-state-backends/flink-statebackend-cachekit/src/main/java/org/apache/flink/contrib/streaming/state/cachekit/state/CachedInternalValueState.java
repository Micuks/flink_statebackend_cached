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

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Objects;

/**
 * Minimal {@link InternalValueState} wrapper that adds a per-state LRU cache.
 *
 * <p>Keying: (currentKey, namespace).
 */
public final class CachedInternalValueState<K, N, V> implements InternalValueState<K, N, V> {

    private final InternalValueState<K, N, V> delegate;
    private final CurrentKeyProvider<K> currentKeyProvider;
    private final CachePolicy<KeyNamespaceKey<K, N>, CachedValue<V>> cache;

    private N currentNamespace;

    public CachedInternalValueState(
            InternalValueState<K, N, V> delegate, CurrentKeyProvider<K> currentKeyProvider, int maxEntries) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.currentKeyProvider = Objects.requireNonNull(currentKeyProvider, "currentKeyProvider");
        this.cache = new LruCachePolicy<>(maxEntries);
    }

    @Override
    public V value() throws IOException {
        KeyNamespaceKey<K, N> cacheKey =
                new KeyNamespaceKey<>(currentKeyProvider.getCurrentKey(), currentNamespace);
        CachedValue<V> cached = cache.get(cacheKey);
        if (cached != null) {
            return cached.valueOrNull();
        }
        V loaded = delegate.value();
        cache.put(cacheKey, CachedValue.of(loaded));
        return loaded;
    }

    @Override
    public void update(V value) throws IOException {
        delegate.update(value);
        KeyNamespaceKey<K, N> cacheKey =
                new KeyNamespaceKey<>(currentKeyProvider.getCurrentKey(), currentNamespace);
        cache.put(cacheKey, CachedValue.of(value));
    }

    @Override
    public void clear() {
        delegate.clear();
        KeyNamespaceKey<K, N> cacheKey =
                new KeyNamespaceKey<>(currentKeyProvider.getCurrentKey(), currentNamespace);
        cache.remove(cacheKey);
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

    private static final class KeyNamespaceKey<K, N> {
        private final K key;
        private final N namespace;

        private KeyNamespaceKey(K key, N namespace) {
            this.key = key;
            this.namespace = namespace;
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
        private static final CachedValue<?> NULL = new CachedValue<>(null, true);

        private final V value;
        private final boolean isNull;

        private CachedValue(V value, boolean isNull) {
            this.value = value;
            this.isNull = isNull;
        }

        static <V> CachedValue<V> of(V value) {
            if (value == null) {
                @SuppressWarnings("unchecked")
                CachedValue<V> casted = (CachedValue<V>) NULL;
                return casted;
            }
            return new CachedValue<>(value, false);
        }

        V valueOrNull() {
            return isNull ? null : value;
        }
    }
}

