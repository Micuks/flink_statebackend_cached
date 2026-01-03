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
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalValueState;

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
    private final CachePolicy<SerializedKeyNamespaceKey, CachedValue<V>> l1Cache;
    private final CachePolicy<SerializedKeyNamespaceKey, CachedValue<V>> l2Cache;
    private final CachePolicyType cachePolicyType;
    private final int lruOverflow;
    private final double l1Ratio;

    private final boolean bypassEnabled;
    private final long bypassMinAccesses;
    private final int bypassSampleEvery;
    private final double bypassHysteresis;
    private final int bypassCooldownWindows;
    private final double hitRateThreshold;
    private final double hitRateLowThreshold;
    private final double hitRateHighThreshold;
    private final int hitRateWindow;
    private final int enterConsecutiveLowWindows;
    private final int exitConsecutiveHighWindows;

    private N currentNamespace;

    // Sticky Cache (L1)
    private SerializedKeyNamespaceKey lastAccessKey;
    private CachedValue<V> lastAccessValue;

    private final java.util.function.Consumer<K> keyContextSetter;

    // Bypass State
    private volatile boolean isBypassing = false;
    private long currentWindowAccesses = 0;
    private long currentWindowHits = 0;
    private int opsSinceLastSample = 0;
    private long totalAccesses = 0;
    private int consecutiveLow = 0;
    private int consecutiveHigh = 0;
    private int windowsSinceToggle = 0;
    private byte[] currentNamespaceBytes;

    public CachedInternalValueState(
            InternalValueState<K, N, V> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
            int maxEntries,
            CachePolicyType cachePolicyType,
            int lruOverflow,
            double l1Ratio,
            boolean bypassEnabled,
            long bypassMinAccesses,
            int bypassSampleEvery,
            double bypassHysteresis,
            int bypassCooldownWindows,
            double hitRateThreshold,
            int hitRateWindow) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.currentKeyProvider = Objects.requireNonNull(currentKeyProvider, "currentKeyProvider");
        this.keyContextSetter = Objects.requireNonNull(keyContextSetter, "keyContextSetter");
        this.cachePolicyType = Objects.requireNonNull(cachePolicyType, "cachePolicyType");
        this.lruOverflow = Math.max(0, lruOverflow);
        this.l1Ratio = Math.max(0.0, Math.min(1.0, l1Ratio));
        this.bypassEnabled = bypassEnabled;
        this.bypassMinAccesses = Math.max(0L, bypassMinAccesses);
        this.bypassSampleEvery = Math.max(1, bypassSampleEvery);
        this.bypassHysteresis = Math.max(0.0, Math.min(1.0, bypassHysteresis));
        this.bypassCooldownWindows = Math.max(0, bypassCooldownWindows);
        this.hitRateThreshold = hitRateThreshold;
        this.hitRateLowThreshold = Math.max(0.0, this.hitRateThreshold - this.bypassHysteresis);
        this.hitRateHighThreshold = Math.min(1.0, this.hitRateThreshold + this.bypassHysteresis);
        this.hitRateWindow = hitRateWindow;
        this.enterConsecutiveLowWindows = 1;
        this.exitConsecutiveHighWindows = 2;

        int l1Size;
        int l2Size;
        if (maxEntries <= 0) {
            l1Size = 0;
            l2Size = 0;
        } else {
            int minL1 = Math.min(128, maxEntries);
            l1Size = Math.max(minL1, (int) Math.floor(maxEntries * this.l1Ratio));
            l1Size = Math.min(l1Size, maxEntries);
            l2Size = Math.max(0, maxEntries - l1Size);
        }
        this.l1Cache = createCachePolicy(l1Size, this::onL1Eviction);
        this.l2Cache = createCachePolicy(l2Size, this::onL2Eviction);
    }

    @Override
    public V value() throws IOException {
        K currentKey = currentKeyProvider.getCurrentKey();
        SerializedKeyNamespaceKey serializedKey = createSerializedKey(currentKey, currentNamespace);

        // 1. Check Sticky Cache (Always Check L0 - Fast Path)
        if (lastAccessKey != null && lastAccessKey.equals(serializedKey)) {
            recordAccessWeighted(true, 1); // Hit
            return lastAccessValue.valueOrNull();
        }

        int recordWeight = 1;
        CachedValue<V> l1Cached = null;
        boolean l1Checked = false;

        // 2. Bypass Logic with L1 probe and weighted sampling
        if (bypassEnabled && isBypassing) {
            l1Cached = l1Cache.get(serializedKey);
            l1Checked = true;
            if (l1Cached != null) {
                updateSticky(serializedKey, l1Cached);
                recordAccessWeighted(true, 1); // L1 hit in bypass
                return l1Cached.valueOrNull();
            }

            opsSinceLastSample++;
            if (opsSinceLastSample < bypassSampleEvery) {
                return delegate.value();
            }
            opsSinceLastSample = 0;
            recordWeight = bypassSampleEvery;
        }

        // 3. Check L1 Cache
        if (!l1Checked) {
            l1Cached = l1Cache.get(serializedKey);
            if (l1Cached != null) {
                updateSticky(serializedKey, l1Cached);
                recordAccessWeighted(true, 1); // Hit
                return l1Cached.valueOrNull();
            }
        }

        // 4. Check L2 Cache
        CachedValue<V> l2Cached = l2Cache.get(serializedKey);
        if (l2Cached != null) {
            // Promote to L1 (Clean)
            CachedValue<V> newValue = CachedValue.of(l2Cached.valueOrNull(), false);
            l1Cache.put(serializedKey, newValue);

            updateSticky(serializedKey, newValue);
            recordAccessWeighted(true, recordWeight); // Hit
            return l2Cached.valueOrNull();
        }

        // 5. Miss -> Load from Delegate
        V loaded = delegate.value();
        recordAccessWeighted(false, recordWeight); // Miss

        // 6. Update L1 (Clean)
        // Even if bypassing (sampled), we populate L1 to allow hit rate recovery
        CachedValue<V> newValue = CachedValue.of(loaded, false);
        l1Cache.put(serializedKey, newValue);

        updateSticky(serializedKey, newValue);
        return loaded;
    }

    @Override
    public void update(V value) throws IOException {
        K currentKey = currentKeyProvider.getCurrentKey();
        SerializedKeyNamespaceKey serializedKey = createSerializedKey(currentKey, currentNamespace);

        if (bypassEnabled && isBypassing) {
            // Write-Through (Bypass Mode)
            delegate.update(value);

            // Update L1 as Clean so subsequent reads (if sampled or re-enabled) find it.
            // This also ensures cache coherence.
            CachedValue<V> newValue = CachedValue.of(value, false); // Clean
            l1Cache.put(serializedKey, newValue);
            updateSticky(serializedKey, newValue);
            return;
        }

        CachedValue<V> newValue = CachedValue.of(value, true);

        // Write-Back: Update L1 only (marked dirty)
        l1Cache.put(serializedKey, newValue);

        // Optimistically update sticky
        updateSticky(serializedKey, newValue);
    }

    @Override
    public void clear() {
        K currentKey = currentKeyProvider.getCurrentKey();
        SerializedKeyNamespaceKey cacheKey = createSerializedKey(currentKey, currentNamespace);
        CachedValue<V> newValue;

        if (bypassEnabled && isBypassing) {
            delegate.clear();
            newValue = CachedValue.of(null, false);
        } else {
            newValue = CachedValue.of(null, true);
        }

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
        this.currentNamespaceBytes = serializeNamespaceBytes(namespace);
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
        java.util.List<java.util.Map.Entry<SerializedKeyNamespaceKey, CachedValue<V>>> dirtyEntries =
                new java.util.ArrayList<>();
        for (java.util.Map.Entry<SerializedKeyNamespaceKey, CachedValue<V>> entry : l1Cache.entries()) {
            if (entry.getValue().dirty) {
                dirtyEntries.add(entry);
            }
        }
        for (java.util.Map.Entry<SerializedKeyNamespaceKey, CachedValue<V>> entry : dirtyEntries) {
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

    private SerializedKeyNamespaceKey createSerializedKey(K key, N namespace) {
        return new SerializedKeyNamespaceKey(serializeKeyAndNamespace(key, namespace));
    }

    private byte[] serializeKeyAndNamespace(K key, N namespace) {
        try {
            DataOutputSerializer out = new DataOutputSerializer(128);
            byte[] namespaceBytes =
                    (namespace == currentNamespace && currentNamespaceBytes != null)
                            ? currentNamespaceBytes
                            : serializeNamespaceBytes(namespace);
            out.writeInt(namespaceBytes.length);
            out.write(namespaceBytes);
            getKeySerializer().serialize(key, out);
            return out.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize cache key", e);
        }
    }

    private byte[] serializeNamespaceBytes(N namespace) {
        try {
            DataOutputSerializer out = new DataOutputSerializer(64);
            getNamespaceSerializer().serialize(namespace, out);
            return out.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize namespace for cache key", e);
        }
    }

    private void updateSticky(SerializedKeyNamespaceKey key, CachedValue<V> value) {
        lastAccessKey = key;
        lastAccessValue = value;
    }

    private CachePolicy<SerializedKeyNamespaceKey, CachedValue<V>> createCachePolicy(
            int maxEntries,
            java.util.function.BiConsumer<SerializedKeyNamespaceKey, CachedValue<V>> evictionListener) {
        if (cachePolicyType == CachePolicyType.CAFFEINE) {
            return new CaffeineCachePolicy<>(maxEntries, evictionListener);
        }
        return new LruCachePolicy<>(maxEntries, lruOverflow, evictionListener);
    }

    // L1 Eviction Listener
    private void onL1Eviction(SerializedKeyNamespaceKey key, CachedValue<V> value) {
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
    private void onL2Eviction(SerializedKeyNamespaceKey key, CachedValue<V> value) {
        // L2 is clean (backed by delegate). Just drop.
    }

    private void recordAccess(boolean isHit) {
        recordAccessWeighted(isHit, 1);
    }

    private void recordAccessWeighted(boolean isHit, int weight) {
        if (!bypassEnabled) {
            return;
        }
        if (weight <= 0) {
            return;
        }
        totalAccesses += weight;
        currentWindowAccesses += weight;
        if (isHit) {
            currentWindowHits += weight;
        }

        if (currentWindowAccesses >= hitRateWindow) {
            double hitRate = (double) currentWindowHits / currentWindowAccesses;
            if (totalAccesses >= bypassMinAccesses) {
                boolean lowHitRate = hitRate < hitRateLowThreshold;
                boolean highHitRate = hitRate > hitRateHighThreshold;
                boolean inCooldown = windowsSinceToggle < bypassCooldownWindows;
                boolean toggled = false;

                if (isBypassing) {
                    if (!inCooldown) {
                        if (highHitRate) {
                            consecutiveHigh++;
                            consecutiveLow = 0;
                        } else if (lowHitRate) {
                            consecutiveHigh = 0;
                        }
                        if (consecutiveHigh >= exitConsecutiveHighWindows) {
                            isBypassing = false;
                            toggled = true;
                        }
                    }
                } else {
                    if (!inCooldown) {
                        if (lowHitRate) {
                            consecutiveLow++;
                            consecutiveHigh = 0;
                        } else if (highHitRate) {
                            consecutiveLow = 0;
                        }
                        if (consecutiveLow >= enterConsecutiveLowWindows) {
                            isBypassing = true;
                            toggled = true;
                            flush(); // Flush dirty entries before bypassing cache
                        }
                    }
                }

                if (toggled) {
                    windowsSinceToggle = 0;
                    consecutiveLow = 0;
                    consecutiveHigh = 0;
                    opsSinceLastSample = 0;
                }

                if (windowsSinceToggle < bypassCooldownWindows) {
                    windowsSinceToggle++;
                }
            }

            currentWindowAccesses = 0;
            currentWindowHits = 0;
        }
    }

    private void flushEntryToDelegate(SerializedKeyNamespaceKey key, CachedValue<V> value) {
        // Save current context
        K previousKey = currentKeyProvider.getCurrentKey();
        K deserializedKey = key.deserializeKey(getKeySerializer());
        N deserializedNamespace = key.deserializeNamespace(getNamespaceSerializer());

        keyContextSetter.accept(deserializedKey);
        delegate.setCurrentNamespace(deserializedNamespace);
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
    private static final class SerializedKeyNamespaceKey {
        private final byte[] bytes;

        private SerializedKeyNamespaceKey(byte[] bytes) {
            this.bytes = Objects.requireNonNull(bytes, "bytes");
        }

        <K> K deserializeKey(TypeSerializer<K> keySerializer) {
            try {
                DataInputDeserializer in = new DataInputDeserializer(bytes);
                int namespaceLength = in.readInt();
                in.skipBytesToRead(namespaceLength);
                return keySerializer.deserialize(in);
            } catch (IOException e) {
                throw new RuntimeException("Failed to deserialize key from cache key", e);
            }
        }

        <N> N deserializeNamespace(TypeSerializer<N> namespaceSerializer) {
            try {
                DataInputDeserializer in = new DataInputDeserializer(bytes);
                int namespaceLength = in.readInt();
                byte[] namespaceBytes = new byte[namespaceLength];
                in.read(namespaceBytes);
                return namespaceSerializer.deserialize(new DataInputDeserializer(namespaceBytes));
            } catch (IOException e) {
                throw new RuntimeException("Failed to deserialize namespace from cache key", e);
            }
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof SerializedKeyNamespaceKey)) {
                return false;
            }
            SerializedKeyNamespaceKey that = (SerializedKeyNamespaceKey) other;
            if (this.bytes.length != that.bytes.length) {
                return false;
            }
            for (int i = 0; i < this.bytes.length; i++) {
                if (this.bytes[i] != that.bytes[i]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = 1;
            for (byte element : bytes) {
                result = 31 * result + element;
            }
            return result;
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
