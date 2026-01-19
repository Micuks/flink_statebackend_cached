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
 * Minimal {@link InternalMapState} wrapper that adds a per-state cache for entries and presence.
 *
 * <p>
 * Keying: (currentKey, namespace, userKey).
 */
public final class CachedInternalMapState<K, N, UK, UV> implements InternalMapState<K, N, UK, UV> {

    private final InternalMapState<K, N, UK, UV> delegate;
    private final CurrentKeyProvider<K> currentKeyProvider;
    private final CachePolicy<KeyNamespaceUserKey<K, N, UK>, CachedMapValue<UV>> l1ValueCache;
    private final CachePolicy<KeyNamespaceUserKey<K, N, UK>, CachedMapValue<UV>> l2ValueCache;
    private final CachePolicy<KeyNamespaceUserKey<K, N, UK>, Boolean> l1PresenceCache;
    private final CachePolicy<KeyNamespaceUserKey<K, N, UK>, Boolean> l2PresenceCache;
    private final CachePolicy<Long, Byte> l1PrimitivePresenceCache;
    private final CachePolicy<Long, Byte> l2PrimitivePresenceCache;
    private final CachePolicyType presenceCachePolicyType;
    private final int presenceCacheLruOverflow;
    private final boolean presenceCacheEnabled;
    private final PresenceCacheImplementation presenceCacheImplementation;
    private final boolean mapCacheEnabled;
    private final CachePolicyType mapCachePolicyType;
    private final int mapCacheLruOverflow;
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
            PresenceCacheImplementation presenceCacheImplementation,
            int mapCacheMaxEntries,
            CachePolicyType mapCachePolicyType,
            int mapCacheLruOverflow) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.currentKeyProvider = Objects.requireNonNull(currentKeyProvider, "currentKeyProvider");
        this.presenceCachePolicyType = Objects.requireNonNull(cachePolicyType, "cachePolicyType");
        this.presenceCacheLruOverflow = Math.max(0, lruOverflow);
        this.presenceCacheEnabled = maxEntries > 0;
        this.presenceCacheImplementation = Objects.requireNonNull(
                presenceCacheImplementation, "presenceCacheImplementation");
        this.mapCacheEnabled = mapCacheMaxEntries > 0;
        this.mapCachePolicyType = Objects.requireNonNull(mapCachePolicyType, "mapCachePolicyType");
        this.mapCacheLruOverflow = Math.max(0, mapCacheLruOverflow);
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
                this.l1PresenceCache = createCachePolicy(
                        l1Size, presenceCachePolicyType, presenceCacheLruOverflow, this::onL1Eviction);
                this.l2PresenceCache = createCachePolicy(
                        maxEntries, presenceCachePolicyType, presenceCacheLruOverflow, this::onL2Eviction);
                this.l1PrimitivePresenceCache = new NoOpCachePolicy<>();
                this.l2PrimitivePresenceCache = new NoOpCachePolicy<>();
            }
        } else {
            this.l1PresenceCache = new NoOpCachePolicy<>();
            this.l2PresenceCache = new NoOpCachePolicy<>();
            this.l1PrimitivePresenceCache = new NoOpCachePolicy<>();
            this.l2PrimitivePresenceCache = new NoOpCachePolicy<>();
        }

        if (mapCacheEnabled) {
            int l1Size = Math.max(128, mapCacheMaxEntries / 5);
            this.l1ValueCache = createCachePolicy(
                    l1Size, mapCachePolicyType, mapCacheLruOverflow, this::onValueL1Eviction);
            this.l2ValueCache = createCachePolicy(
                    mapCacheMaxEntries, mapCachePolicyType, mapCacheLruOverflow, this::onValueL2Eviction);
        } else {
            this.l1ValueCache = new NoOpCachePolicy<>();
            this.l2ValueCache = new NoOpCachePolicy<>();
        }
    }

    @Override
    public UV get(UK userKey) throws Exception {
        if (userKey == null) {
            return null;
        }
        ensureDelegateNamespace();
        if (mapCacheEnabled) {
            CachedMapValue<UV> cached = getCachedValue(userKey);
            if (cached != null) {
                return cached.valueOrNull();
            }
        }
        if (presenceCacheEnabled) {
            Boolean present = getPresence(userKey);
            if (present != null && !present) {
                return null;
            }
        }
        UV value = delegate.get(userKey);
        if (mapCacheEnabled) {
            updateValueCache(userKey, value);
        }
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
        if (mapCacheEnabled) {
            updateValueCache(userKey, userValue);
        }
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
        for (Map.Entry<UK, UV> entry : map.entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }
            if (mapCacheEnabled) {
                updateValueCache(entry.getKey(), entry.getValue());
            }
            if (presenceCacheEnabled) {
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
        if (mapCacheEnabled) {
            updateValueCache(userKey, null);
        }
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
        if (mapCacheEnabled) {
            CachedMapValue<UV> cached = getCachedValue(userKey);
            if (cached != null) {
                return !cached.isNull();
            }
        }
        if (presenceCacheEnabled) {
            Boolean present = getPresence(userKey);
            if (present != null) {
                return present;
            }
        }
        boolean exists = delegate.contains(userKey);
        if (mapCacheEnabled && !exists) {
            updateValueCache(userKey, null);
        }
        if (presenceCacheEnabled) {
            updatePresence(userKey, exists);
        }
        return exists;
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        ensureDelegateNamespace();
        Iterable<Map.Entry<UK, UV>> entries = delegate.entries();
        if (!mapCacheEnabled && !presenceCacheEnabled) {
            return entries;
        }
        return cacheEntries(entries);
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        ensureDelegateNamespace();
        if (!mapCacheEnabled && !presenceCacheEnabled) {
            return delegate.keys();
        }
        Iterable<Map.Entry<UK, UV>> entries = delegate.entries();
        return cacheKeys(entries);
    }

    @Override
    public Iterable<UV> values() throws Exception {
        ensureDelegateNamespace();
        if (!mapCacheEnabled && !presenceCacheEnabled) {
            return delegate.values();
        }
        Iterable<Map.Entry<UK, UV>> entries = delegate.entries();
        return cacheValues(entries);
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        ensureDelegateNamespace();
        Iterator<Map.Entry<UK, UV>> iterator = delegate.iterator();
        if (!mapCacheEnabled && !presenceCacheEnabled) {
            return iterator;
        }
        return new CachingEntryIterator(iterator);
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
        clearValueCaches();
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
                if (l1 == PrimitivePresenceCache.ABSENT) {
                    return false;
                }
                l1PrimitivePresenceCache.remove(fp);
            }
            Byte l2 = l2PrimitivePresenceCache.get(fp);
            if (l2 != null) {
                if (l2 == PrimitivePresenceCache.ABSENT) {
                    l1PrimitivePresenceCache.put(fp, l2);
                    return false;
                }
                l2PrimitivePresenceCache.remove(fp);
            }
            return null;
        }

        KeyNamespaceUserKey<K, N, UK> probe =
                new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey, false);
        Boolean present = l1PresenceCache.get(probe);
        if (present != null) {
            if (!present) {
                return false;
            }
            KeyNamespaceUserKey<K, N, UK> storage =
                    new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey, true);
            l1PresenceCache.remove(storage);
            return null;
        }
        present = l2PresenceCache.get(probe);
        if (present != null) {
            KeyNamespaceUserKey<K, N, UK> storage =
                    new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey, true);
            if (!present) {
                l1PresenceCache.put(storage, false);
                return false;
            }
            l2PresenceCache.remove(storage);
        }
        return null;
    }

    private void updatePresence(UK userKey, boolean present) {
        K currentKey = currentKeyProvider.getCurrentKey();
        if (currentKey == null || currentNamespace == null) {
            return;
        }
        if (usePrimitivePresenceCache) {
            long fp = fingerprint(currentKey, currentNamespace, userKey);
            if (present) {
                l1PrimitivePresenceCache.remove(fp);
                l2PrimitivePresenceCache.remove(fp);
                return;
            }
            l2PrimitivePresenceCache.remove(fp);
            l1PrimitivePresenceCache.put(fp, PrimitivePresenceCache.ABSENT);
            return;
        }
        KeyNamespaceUserKey<K, N, UK> storage =
                new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey, true);
        if (present) {
            l1PresenceCache.remove(storage);
            l2PresenceCache.remove(storage);
            return;
        }
        l2PresenceCache.remove(storage);
        l1PresenceCache.put(storage, false);
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

    private void clearValueCaches() {
        if (!mapCacheEnabled) {
            return;
        }
        l1ValueCache.clear();
        l2ValueCache.clear();
    }

    private CachedMapValue<UV> getCachedValue(UK userKey) {
        K currentKey = currentKeyProvider.getCurrentKey();
        if (currentKey == null || currentNamespace == null) {
            return null;
        }
        KeyNamespaceUserKey<K, N, UK> probe =
                new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey, false);
        CachedMapValue<UV> cached = l1ValueCache.get(probe);
        if (cached != null) {
            return cached;
        }
        cached = l2ValueCache.get(probe);
        if (cached != null) {
            KeyNamespaceUserKey<K, N, UK> storage =
                    new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey, true);
            l1ValueCache.put(storage, cached);
            l2ValueCache.remove(storage);
        }
        return cached;
    }

    private void updateValueCache(UK userKey, UV userValue) {
        K currentKey = currentKeyProvider.getCurrentKey();
        if (currentKey == null || currentNamespace == null) {
            return;
        }
        KeyNamespaceUserKey<K, N, UK> storage =
                new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey, true);
        CachedMapValue<UV> cached = CachedMapValue.of(userValue);
        l1ValueCache.put(storage, cached);
        l2ValueCache.remove(storage);
    }

    private Iterable<Map.Entry<UK, UV>> cacheEntries(Iterable<Map.Entry<UK, UV>> entries) {
        return () -> new CachingEntryIterator(entries.iterator());
    }

    private Iterable<UK> cacheKeys(Iterable<Map.Entry<UK, UV>> entries) {
        return () -> new Iterator<UK>() {
            private final Iterator<Map.Entry<UK, UV>> delegateIterator = entries.iterator();

            @Override
            public boolean hasNext() {
                return delegateIterator.hasNext();
            }

            @Override
            public UK next() {
                Map.Entry<UK, UV> entry = delegateIterator.next();
                cacheEntry(entry);
                return entry.getKey();
            }
        };
    }

    private Iterable<UV> cacheValues(Iterable<Map.Entry<UK, UV>> entries) {
        return () -> new Iterator<UV>() {
            private final Iterator<Map.Entry<UK, UV>> delegateIterator = entries.iterator();

            @Override
            public boolean hasNext() {
                return delegateIterator.hasNext();
            }

            @Override
            public UV next() {
                Map.Entry<UK, UV> entry = delegateIterator.next();
                cacheEntry(entry);
                return entry.getValue();
            }
        };
    }

    private void cacheEntry(Map.Entry<UK, UV> entry) {
        if (entry == null) {
            return;
        }
        UK userKey = entry.getKey();
        if (userKey == null) {
            return;
        }
        if (mapCacheEnabled) {
            updateValueCache(userKey, entry.getValue());
        }
        if (presenceCacheEnabled) {
            updatePresence(userKey, entry.getValue() != null);
        }
    }

    private <V> CachePolicy<KeyNamespaceUserKey<K, N, UK>, V> createCachePolicy(
            int maxEntries,
            CachePolicyType policyType,
            int lruOverflow,
            java.util.function.BiConsumer<KeyNamespaceUserKey<K, N, UK>, V> evictionListener) {
        if (policyType == CachePolicyType.CAFFEINE) {
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
        if (key == null || value == null || value) {
            return;
        }
        l2PresenceCache.put(key, value);
    }

    private void onL2Eviction(KeyNamespaceUserKey<K, N, UK> key, Boolean value) {
        // Presence cache doesn't need flush on L2 eviction.
    }

    private void onL1PrimitiveEviction(Long key, Byte value) {
        if (key == null || value == null || value.byteValue() != PrimitivePresenceCache.ABSENT) {
            return;
        }
        l2PrimitivePresenceCache.put(key, value);
    }

    private void onL2PrimitiveEviction(Long key, Byte value) {
        // Presence cache doesn't need flush on L2 eviction.
    }

    private void onValueL1Eviction(KeyNamespaceUserKey<K, N, UK> key, CachedMapValue<UV> value) {
        if (key == null || value == null) {
            return;
        }
        l2ValueCache.put(key, value);
    }

    private void onValueL2Eviction(KeyNamespaceUserKey<K, N, UK> key, CachedMapValue<UV> value) {
        // MapState cache doesn't need flush on L2 eviction.
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

    private final class CachingEntryIterator implements Iterator<Map.Entry<UK, UV>> {
        private final Iterator<Map.Entry<UK, UV>> delegateIterator;

        private CachingEntryIterator(Iterator<Map.Entry<UK, UV>> delegateIterator) {
            this.delegateIterator = delegateIterator;
        }

        @Override
        public boolean hasNext() {
            return delegateIterator.hasNext();
        }

        @Override
        public Map.Entry<UK, UV> next() {
            Map.Entry<UK, UV> entry = delegateIterator.next();
            cacheEntry(entry);
            return entry;
        }
    }

    private static final class CachedMapValue<V> {
        private final V value;
        private final boolean isNull;

        private CachedMapValue(V value, boolean isNull) {
            this.value = value;
            this.isNull = isNull;
        }

        static <V> CachedMapValue<V> of(V value) {
            return new CachedMapValue<>(value, value == null);
        }

        V valueOrNull() {
            return isNull ? null : value;
        }

        boolean isNull() {
            return isNull;
        }
    }
}
