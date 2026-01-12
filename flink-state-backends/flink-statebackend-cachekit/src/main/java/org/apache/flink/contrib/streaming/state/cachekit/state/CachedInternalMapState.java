/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicy;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CaffeineCachePolicy;
import org.apache.flink.contrib.streaming.state.cachekit.cache.LruCachePolicy;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PresenceCacheImplementation;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PrimitivePresenceCache;
import org.apache.flink.contrib.streaming.state.cachekit.util.MurmurHash3;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.table.data.binary.BinaryRowData;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * Minimal {@link InternalMapState} wrapper that adds a per-state key presence cache.
 *
 * <p>
 * Keying: (currentKey, namespace, userKey).
 */
public final class CachedInternalMapState<K, N, UK, UV> implements InternalMapState<K, N, UK, UV> {

    private final InternalMapState<K, N, UK, UV> delegate;
    private final CurrentKeyProvider<K> currentKeyProvider;
    private final CachePolicy<KeyNamespaceUserKey<K, N, UK>, Boolean> l1PresenceCache;
    private final CachePolicy<KeyNamespaceUserKey<K, N, UK>, Boolean> l2PresenceCache;
    private final CachePolicy<Long, Byte> l1PrimitivePresenceCache;
    private final CachePolicy<Long, Byte> l2PrimitivePresenceCache;
    private final CachePolicyType cachePolicyType;
    private final int lruOverflow;
    private final boolean presenceCacheEnabled;
    private final PresenceCacheImplementation presenceCacheImplementation;
    private final boolean usePrimitivePresenceCache;
    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<UK> userKeySerializer;
    private final ThreadLocal<DataOutputSerializer> serializerView;

    private N currentNamespace;

    public CachedInternalMapState(
            InternalMapState<K, N, UK, UV> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            int maxEntries,
            CachePolicyType cachePolicyType,
            int lruOverflow,
            PresenceCacheImplementation presenceCacheImplementation) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.currentKeyProvider = Objects.requireNonNull(currentKeyProvider, "currentKeyProvider");
        this.cachePolicyType = Objects.requireNonNull(cachePolicyType, "cachePolicyType");
        this.lruOverflow = Math.max(0, lruOverflow);
        this.presenceCacheEnabled = maxEntries > 0;
        this.presenceCacheImplementation = Objects.requireNonNull(
                presenceCacheImplementation, "presenceCacheImplementation");
        this.keySerializer = delegate.getKeySerializer();
        this.namespaceSerializer = delegate.getNamespaceSerializer();
        TypeSerializer<UK> resolvedUserKeySerializer = null;
        TypeSerializer<Map<UK, UV>> valueSerializer = delegate.getValueSerializer();
        if (valueSerializer instanceof MapSerializer) {
            resolvedUserKeySerializer = ((MapSerializer<UK, UV>) valueSerializer).getKeySerializer();
        }
        this.userKeySerializer = resolvedUserKeySerializer;
        this.usePrimitivePresenceCache =
                presenceCacheImplementation == PresenceCacheImplementation.PRIMITIVE
                        && keySerializer != null
                        && namespaceSerializer != null
                        && userKeySerializer != null;
        if (usePrimitivePresenceCache) {
            this.serializerView = ThreadLocal.withInitial(() -> new DataOutputSerializer(128));
        } else {
            this.serializerView = null;
        }

        if (presenceCacheEnabled) {
            int l1Size = Math.max(128, maxEntries / 5);
            if (usePrimitivePresenceCache) {
                this.l1PrimitivePresenceCache = new PrimitivePresenceCache(
                        l1Size, this::onL1PrimitiveEviction);
                this.l2PrimitivePresenceCache = new PrimitivePresenceCache(
                        maxEntries, this::onL2PrimitiveEviction);
                this.l1PresenceCache = new NoOpCachePolicy<>();
                this.l2PresenceCache = new NoOpCachePolicy<>();
            } else {
                this.l1PresenceCache = createCachePolicy(l1Size, this::onL1Eviction);
                this.l2PresenceCache = createCachePolicy(maxEntries, this::onL2Eviction);
                this.l1PrimitivePresenceCache = new NoOpCachePolicy<>();
                this.l2PrimitivePresenceCache = new NoOpCachePolicy<>();
            }
        } else {
            this.l1PresenceCache = new NoOpCachePolicy<>();
            this.l2PresenceCache = new NoOpCachePolicy<>();
            this.l1PrimitivePresenceCache = new NoOpCachePolicy<>();
            this.l2PrimitivePresenceCache = new NoOpCachePolicy<>();
        }
    }

    @Override
    public UV get(UK userKey) throws Exception {
        if (userKey == null) {
            return null;
        }
        ensureDelegateNamespace();
        if (presenceCacheEnabled) {
            Boolean present = getPresence(userKey);
            if (present != null && !present) {
                return null;
            }
        }
        UV value = delegate.get(userKey);
        if (presenceCacheEnabled) {
            updatePresence(userKey, value != null);
        }
        return value;
    }

    @Override
    public void put(UK userKey, UV userValue) throws Exception {
        if (userKey == null) {
            return;
        }
        ensureDelegateNamespace();
        if (userValue == null) {
            remove(userKey);
            return;
        }
        delegate.put(userKey, userValue);
        if (presenceCacheEnabled) {
            updatePresence(userKey, true);
        }
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        if (map == null || map.isEmpty()) {
            return;
        }
        ensureDelegateNamespace();
        delegate.putAll(map);
        if (presenceCacheEnabled) {
            for (Map.Entry<UK, UV> entry : map.entrySet()) {
                if (entry.getKey() == null) {
                    continue;
                }
                updatePresence(entry.getKey(), entry.getValue() != null);
            }
        }
    }

    @Override
    public void remove(UK userKey) throws Exception {
        if (userKey == null) {
            return;
        }
        ensureDelegateNamespace();
        delegate.remove(userKey);
        if (presenceCacheEnabled) {
            updatePresence(userKey, false);
        }
    }

    @Override
    public boolean contains(UK userKey) throws Exception {
        if (userKey == null) {
            return false;
        }
        ensureDelegateNamespace();
        if (presenceCacheEnabled) {
            Boolean present = getPresence(userKey);
            if (present != null) {
                return present;
            }
        }
        boolean exists = delegate.contains(userKey);
        if (presenceCacheEnabled) {
            updatePresence(userKey, exists);
        }
        return exists;
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        ensureDelegateNamespace();
        clearPresenceCaches();
        return delegate.entries();
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        ensureDelegateNamespace();
        clearPresenceCaches();
        return delegate.keys();
    }

    @Override
    public Iterable<UV> values() throws Exception {
        ensureDelegateNamespace();
        clearPresenceCaches();
        return delegate.values();
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        ensureDelegateNamespace();
        clearPresenceCaches();
        return delegate.iterator();
    }

    @Override
    public boolean isEmpty() throws Exception {
        ensureDelegateNamespace();
        return delegate.isEmpty();
    }

    @Override
    public void clear() {
        ensureDelegateNamespace();
        delegate.clear();
        clearPresenceCaches();
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
    public TypeSerializer<Map<UK, UV>> getValueSerializer() {
        return delegate.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(@Nonnull N namespace) {
        this.currentNamespace = namespace;
        if (namespace != null) {
            delegate.setCurrentNamespace(namespace);
        }
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<Map<UK, UV>> safeValueSerializer)
            throws Exception {
        ensureDelegateNamespace();
        return delegate.getSerializedValue(
                serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer, safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, Map<UK, UV>> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        ensureDelegateNamespace();
        return delegate.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    private void ensureDelegateNamespace() {
        if (currentNamespace != null) {
            delegate.setCurrentNamespace(currentNamespace);
        }
    }

    private Boolean getPresence(UK userKey) {
        K currentKey = currentKeyProvider.getCurrentKey();
        if (currentKey == null || currentNamespace == null) {
            return null;
        }
        if (usePrimitivePresenceCache) {
            long fp = fingerprint(currentKey, currentNamespace, userKey);
            Byte l1 = l1PrimitivePresenceCache.get(fp);
            if (l1 != null) {
                return l1 == PrimitivePresenceCache.PRESENT;
            }
            Byte l2 = l2PrimitivePresenceCache.get(fp);
            if (l2 != null) {
                l1PrimitivePresenceCache.put(fp, l2);
                return l2 == PrimitivePresenceCache.PRESENT;
            }
            return null;
        }

        KeyNamespaceUserKey<K, N, UK> probe =
                new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey, false);
        Boolean present = l1PresenceCache.get(probe);
        if (present != null) {
            return present;
        }
        present = l2PresenceCache.get(probe);
        if (present != null) {
            KeyNamespaceUserKey<K, N, UK> storage =
                    new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey, true);
            l1PresenceCache.put(storage, present);
        }
        return present;
    }

    private void updatePresence(UK userKey, boolean present) {
        K currentKey = currentKeyProvider.getCurrentKey();
        if (currentKey == null || currentNamespace == null) {
            return;
        }
        if (usePrimitivePresenceCache) {
            long fp = fingerprint(currentKey, currentNamespace, userKey);
            l1PrimitivePresenceCache.put(fp, present
                    ? PrimitivePresenceCache.PRESENT
                    : PrimitivePresenceCache.ABSENT);
            return;
        }
        KeyNamespaceUserKey<K, N, UK> storage =
                new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey, true);
        l1PresenceCache.put(storage, present);
    }

    private void clearPresenceCaches() {
        if (!presenceCacheEnabled) {
            return;
        }
        l1PresenceCache.clear();
        l2PresenceCache.clear();
        l1PrimitivePresenceCache.clear();
        l2PrimitivePresenceCache.clear();
    }

    private CachePolicy<KeyNamespaceUserKey<K, N, UK>, Boolean> createCachePolicy(
            int maxEntries,
            java.util.function.BiConsumer<KeyNamespaceUserKey<K, N, UK>, Boolean> evictionListener) {
        if (cachePolicyType == CachePolicyType.CAFFEINE) {
            return new CaffeineCachePolicy<>(maxEntries, evictionListener);
        }
        return new LruCachePolicy<>(maxEntries, lruOverflow, evictionListener);
    }

    private long fingerprint(K key, N namespace, UK userKey) {
        DataOutputSerializer dos = serializerView.get();
        dos.clear();
        try {
            keySerializer.serialize(key, dos);
            namespaceSerializer.serialize(namespace, dos);
            userKeySerializer.serialize(userKey, dos);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize presence cache key", e);
        }
        byte[] bytes = dos.getSharedBuffer();
        int len = dos.length();
        MurmurHash3.LongPair out = new MurmurHash3.LongPair();
        MurmurHash3.murmurhash3_x64_128(bytes, 0, len, 0, out);
        return out.val1;
    }

    private void onL1Eviction(KeyNamespaceUserKey<K, N, UK> key, Boolean value) {
        if (key == null || value == null) {
            return;
        }
        l2PresenceCache.put(key, value);
    }

    private void onL2Eviction(KeyNamespaceUserKey<K, N, UK> key, Boolean value) {
        // Presence cache doesn't need flush on L2 eviction.
    }

    private void onL1PrimitiveEviction(Long key, Byte value) {
        if (key == null || value == null) {
            return;
        }
        l2PrimitivePresenceCache.put(key, value);
    }

    private void onL2PrimitiveEviction(Long key, Byte value) {
        // Presence cache doesn't need flush on L2 eviction.
    }

    private static final class KeyNamespaceUserKey<K, N, UK> {
        private final K key;
        private final N namespace;
        private final UK userKey;

        private KeyNamespaceUserKey(K key, N namespace, UK userKey, boolean deepCopy) {
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
            if (deepCopy && userKey instanceof BinaryRowData) {
                this.userKey = (UK) ((BinaryRowData) userKey).copy();
            } else {
                this.userKey = userKey;
            }
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof KeyNamespaceUserKey)) {
                return false;
            }
            KeyNamespaceUserKey<?, ?, ?> that = (KeyNamespaceUserKey<?, ?, ?>) other;
            return Objects.equals(key, that.key)
                    && Objects.equals(namespace, that.namespace)
                    && Objects.equals(userKey, that.userKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, namespace, userKey);
        }
    }

    private static final class NoOpCachePolicy<K, V> implements CachePolicy<K, V> {
        @Override
        public V get(K key) {
            return null;
        }

        @Override
        public V put(K key, V value) {
            return null;
        }

        @Override
        public V remove(K key) {
            return null;
        }

        @Override
        public void clear() {
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Iterable<Map.Entry<K, V>> entries() {
            return Collections.emptyList();
        }
    }
}
