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

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * Minimal {@link InternalMapState} wrapper that adds a per-state cache for
 * entries and presence.
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
    private final boolean bypassEnabled;
    private final double hitRateThreshold;
    private final int hitRateWindow;
    private final boolean iterationCacheFillEnabled;
    private final boolean usePrimitivePresenceCache;
    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<UK> userKeySerializer;
    private final ThreadLocal<DataOutputSerializer> serializerView;

    private N currentNamespace;
    private final KeyNamespaceUserKey<K, N, UK> lookupKey = new KeyNamespaceUserKey<>(null, null, null);

    private volatile boolean isBypassing = false;
    private long currentWindowAccesses = 0;
    private long currentWindowHits = 0;
    private int opsSinceLastSample = 0;

    public CachedInternalMapState(
            InternalMapState<K, N, UK, UV> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            int maxEntries,
            CachePolicyType cachePolicyType,
            int lruOverflow,
            PresenceCacheImplementation presenceCacheImplementation,
            int mapCacheMaxEntries,
            CachePolicyType mapCachePolicyType,
            int mapCacheLruOverflow,
            boolean bypassEnabled,
            double hitRateThreshold,
            int hitRateWindow,
            boolean iterationCacheFillEnabled) {
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
        this.bypassEnabled = bypassEnabled;
        this.hitRateThreshold = hitRateThreshold;
        this.hitRateWindow = hitRateWindow;
        this.iterationCacheFillEnabled = iterationCacheFillEnabled;
        this.keySerializer = delegate.getKeySerializer();
        this.namespaceSerializer = delegate.getNamespaceSerializer();
        TypeSerializer<UK> resolvedUserKeySerializer = null;
        TypeSerializer<Map<UK, UV>> valueSerializer = delegate.getValueSerializer();
        if (valueSerializer instanceof MapSerializer) {
            resolvedUserKeySerializer = ((MapSerializer<UK, UV>) valueSerializer).getKeySerializer();
        }
        this.userKeySerializer = resolvedUserKeySerializer;
        this.usePrimitivePresenceCache = presenceCacheImplementation == PresenceCacheImplementation.PRIMITIVE
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

    // Helper to update lookup key safely without allocation
    private void setLookupKey(K key, N namespace, UK userKey) {
        lookupKey.key = key;
        lookupKey.namespace = namespace;
        lookupKey.userKey = userKey;
    }

    @Override
    public UV get(UK userKey) throws Exception {
        if (userKey == null) {
            return null;
        }
        K currentKey = currentKeyProvider.getCurrentKey();
        ensureDelegateNamespace(currentKey); // Pass key to avoid re-fetch if needed (though ensureDelegateNamespace
                                             // uses currentNamespace)

        // Use reusable key for lookups
        setLookupKey(currentKey, currentNamespace, userKey);

        if (bypassEnabled && isBypassing && shouldBypassRead()) {
            return delegate.get(userKey);
        }

        if (mapCacheEnabled) {
            CachedMapValue<UV> cached = getCachedValue(currentKey); // Optimize getCachedValue to use lookupKey
            if (cached != null) {
                recordAccess(true);
                return cached.valueOrNull();
            }
        }
        if (presenceCacheEnabled) {
            Boolean present = getPresence(currentKey, userKey); // Optimize getPresence
            if (present != null && !present) {
                recordAccess(true);
                return null;
            }
        }
        UV value = delegate.get(userKey);
        recordAccess(false);
        if (mapCacheEnabled) {
            updateValueCache(currentKey, userKey, value);
        }
        if (presenceCacheEnabled) {
            updatePresence(currentKey, userKey, value != null);
        }
        return value;
    }

    @Override
    public void put(UK userKey, UV userValue) throws Exception {
        if (userKey == null) {
            return;
        }
        K currentKey = currentKeyProvider.getCurrentKey();
        ensureDelegateNamespace(currentKey);

        if (userValue == null) {
            remove(userKey);
            return;
        }
        delegate.put(userKey, userValue);

        // Cache Update need deep copy for storage
        if (mapCacheEnabled) {
            updateValueCache(currentKey, userKey, userValue);
        }
        if (presenceCacheEnabled) {
            updatePresence(currentKey, userKey, true);
        }
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        if (map == null || map.isEmpty()) {
            return;
        }
        K currentKey = currentKeyProvider.getCurrentKey();
        ensureDelegateNamespace(currentKey);

        delegate.putAll(map);
        for (Map.Entry<UK, UV> entry : map.entrySet()) {
            UK uKey = entry.getKey();
            if (uKey == null) {
                continue;
            }
            if (mapCacheEnabled) {
                updateValueCache(currentKey, uKey, entry.getValue());
            }
            if (presenceCacheEnabled) {
                updatePresence(currentKey, uKey, entry.getValue() != null);
            }
        }
    }

    @Override
    public void remove(UK userKey) throws Exception {
        if (userKey == null) {
            return;
        }
        K currentKey = currentKeyProvider.getCurrentKey();
        ensureDelegateNamespace(currentKey);

        delegate.remove(userKey);
        if (mapCacheEnabled) {
            updateValueCache(currentKey, userKey, null);
        }
        if (presenceCacheEnabled) {
            updatePresence(currentKey, userKey, false);
        }
    }

    @Override
    public boolean contains(UK userKey) throws Exception {
        if (userKey == null) {
            return false;
        }
        K currentKey = currentKeyProvider.getCurrentKey();
        ensureDelegateNamespace(currentKey);

        // Use reusable key for lookups
        setLookupKey(currentKey, currentNamespace, userKey);

        if (bypassEnabled && isBypassing && shouldBypassRead()) {
            return delegate.contains(userKey);
        }

        if (mapCacheEnabled) {
            CachedMapValue<UV> cached = getCachedValue(currentKey);
            if (cached != null) {
                recordAccess(true);
                return !cached.isNull();
            }
        }
        if (presenceCacheEnabled) {
            Boolean present = getPresence(currentKey, userKey);
            if (present != null) {
                recordAccess(true);
                return present;
            }
        }
        boolean exists = delegate.contains(userKey);
        recordAccess(false);
        if (mapCacheEnabled && !exists) {
            updateValueCache(currentKey, userKey, null);
        }
        if (presenceCacheEnabled) {
            updatePresence(currentKey, userKey, exists);
        }
        return exists;
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        ensureDelegateNamespace(null); // Key not needed for this check
        Iterable<Map.Entry<UK, UV>> entries = delegate.entries();
        if (!mapCacheEnabled && !presenceCacheEnabled) {
            return entries;
        }
        if (!iterationCacheFillEnabled) {
            return entries;
        }
        return cacheEntries(entries);
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        ensureDelegateNamespace(null);
        if (!mapCacheEnabled && !presenceCacheEnabled) {
            return delegate.keys();
        }
        if (!iterationCacheFillEnabled) {
            return delegate.keys();
        }
        Iterable<Map.Entry<UK, UV>> entries = delegate.entries();
        return cacheKeys(entries);
    }

    @Override
    public Iterable<UV> values() throws Exception {
        ensureDelegateNamespace(null);
        if (!mapCacheEnabled && !presenceCacheEnabled) {
            return delegate.values();
        }
        if (!iterationCacheFillEnabled) {
            return delegate.values();
        }
        Iterable<Map.Entry<UK, UV>> entries = delegate.entries();
        return cacheValues(entries);
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        ensureDelegateNamespace(null);
        Iterator<Map.Entry<UK, UV>> iterator = delegate.iterator();
        if (!mapCacheEnabled && !presenceCacheEnabled) {
            return iterator;
        }
        if (!iterationCacheFillEnabled) {
            return iterator;
        }
        return new CachingEntryIterator(iterator);
    }

    @Override
    public boolean isEmpty() throws Exception {
        ensureDelegateNamespace(null);
        return delegate.isEmpty();
    }

    @Override
    public void clear() {
        ensureDelegateNamespace(null);
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
        ensureDelegateNamespace(null);
        return delegate.getSerializedValue(
                serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer, safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, Map<UK, UV>> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        ensureDelegateNamespace(null);
        return delegate.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    private void ensureDelegateNamespace(K currentKey) {
        if (currentNamespace != null) {
            delegate.setCurrentNamespace(currentNamespace);
        }
    }

    private Boolean getPresence(K currentKey, UK userKey) {
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
                if (l1 == PrimitivePresenceCache.PRESENT) {
                    return true;
                }
            }
            Byte l2 = l2PrimitivePresenceCache.get(fp);
            if (l2 != null) {
                if (l2 == PrimitivePresenceCache.ABSENT || l2 == PrimitivePresenceCache.PRESENT) {
                    l2PrimitivePresenceCache.remove(fp);
                    l1PrimitivePresenceCache.put(fp, l2);
                    return l2 == PrimitivePresenceCache.PRESENT;
                }
            }
            return null;
        }

        // Use lookupKey which has been set by calling method
        Boolean present = l1PresenceCache.get(lookupKey);
        if (present != null) {
            return present;
        }
        present = l2PresenceCache.get(lookupKey);
        if (present != null) {
            l2PresenceCache.remove(lookupKey);
            KeyNamespaceUserKey<K, N, UK> storage = new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey,
                    keySerializer, namespaceSerializer, userKeySerializer);
            l1PresenceCache.put(storage, present);
            return present;
        }
        return null;
    }

    private void updatePresence(K currentKey, UK userKey, boolean present) {
        if (currentKey == null || currentNamespace == null) {
            return;
        }
        if (usePrimitivePresenceCache) {
            long fp = fingerprint(currentKey, currentNamespace, userKey);
            if (present) {
                l2PrimitivePresenceCache.remove(fp);
                l1PrimitivePresenceCache.put(fp, PrimitivePresenceCache.PRESENT);
                return;
            }
            l2PrimitivePresenceCache.remove(fp);
            l1PrimitivePresenceCache.put(fp, PrimitivePresenceCache.ABSENT);
            return;
        }
        // Need reusable key for removal? Yes.
        // Need immutable key for storage? Yes.
        setLookupKey(currentKey, currentNamespace, userKey);

        l2PresenceCache.remove(lookupKey);

        // Storage requries deep copy
        KeyNamespaceUserKey<K, N, UK> storage = new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey,
                keySerializer, namespaceSerializer, userKeySerializer);
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

    private void clearValueCaches() {
        if (!mapCacheEnabled) {
            return;
        }
        l1ValueCache.clear();
        l2ValueCache.clear();
    }

    private CachedMapValue<UV> getCachedValue(K currentKey) {
        if (currentKey == null || currentNamespace == null) {
            return null;
        }
        // Assume lookupKey is already set by caller (get/contains)
        CachedMapValue<UV> cached = l1ValueCache.get(lookupKey);
        if (cached != null) {
            return cached;
        }
        cached = l2ValueCache.get(lookupKey);
        if (cached != null) {
            // Must create immutable key for storage because we are PUTTING to L1
            KeyNamespaceUserKey<K, N, UK> storage = new KeyNamespaceUserKey<>(currentKey, currentNamespace,
                    lookupKey.userKey, keySerializer, namespaceSerializer, userKeySerializer);
            l1ValueCache.put(storage, cached);
            l2ValueCache.remove(lookupKey);
        }
        return cached;
    }

    private void updateValueCache(K currentKey, UK userKey, UV userValue) {
        if (currentKey == null || currentNamespace == null) {
            return;
        }
        KeyNamespaceUserKey<K, N, UK> storage = new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey,
                keySerializer, namespaceSerializer, userKeySerializer);
        CachedMapValue<UV> cached = CachedMapValue.of(userValue);
        l1ValueCache.put(storage, cached);
        // remove allows probe key
        setLookupKey(currentKey, currentNamespace, userKey);
        l2ValueCache.remove(lookupKey);
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
        K currentKey = currentKeyProvider.getCurrentKey();
        if (mapCacheEnabled) {
            updateValueCache(currentKey, userKey, entry.getValue());
        }
        if (presenceCacheEnabled) {
            updatePresence(currentKey, userKey, entry.getValue() != null);
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

    private void onValueL1Eviction(KeyNamespaceUserKey<K, N, UK> key, CachedMapValue<UV> value) {
        if (key == null || value == null) {
            return;
        }
        l2ValueCache.put(key, value);
    }

    private void onValueL2Eviction(KeyNamespaceUserKey<K, N, UK> key, CachedMapValue<UV> value) {
        // MapState cache doesn't need flush on L2 eviction.
    }

    private boolean shouldBypassRead() {
        opsSinceLastSample++;
        if (opsSinceLastSample < 100) {
            return true;
        }
        opsSinceLastSample = 0;
        return false;
    }

    private void recordAccess(boolean isHit) {
        if (!bypassEnabled) {
            return;
        }
        currentWindowAccesses++;
        if (isHit) {
            currentWindowHits++;
        }
        if (currentWindowAccesses >= hitRateWindow) {
            double hitRate = (double) currentWindowHits / currentWindowAccesses;
            boolean shouldBypass = hitRate < hitRateThreshold;

            if (isBypassing) {
                if (!shouldBypass) {
                    isBypassing = false;
                    opsSinceLastSample = 0;
                }
            } else if (shouldBypass) {
                isBypassing = true;
                opsSinceLastSample = 0;
            }

            currentWindowAccesses = 0;
            currentWindowHits = 0;
        }
    }

    private static final class KeyNamespaceUserKey<K, N, UK> {
        private K key;
        private N namespace;
        private UK userKey;

        // Constructor for mutable probe (no copy)
        private KeyNamespaceUserKey(K key, N namespace, UK userKey) {
            this.key = key;
            this.namespace = namespace;
            this.userKey = userKey;
        }

        // Constructor for immutable storage (deep copy)
        private KeyNamespaceUserKey(K key, N namespace, UK userKey,
                TypeSerializer<K> keySerializer,
                TypeSerializer<N> namespaceSerializer,
                TypeSerializer<UK> userKeySerializer) {
            this.key = keySerializer != null ? keySerializer.copy(key) : key;
            this.namespace = namespaceSerializer != null ? namespaceSerializer.copy(namespace) : namespace;
            this.userKey = userKeySerializer != null ? userKeySerializer.copy(userKey) : userKey;
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
