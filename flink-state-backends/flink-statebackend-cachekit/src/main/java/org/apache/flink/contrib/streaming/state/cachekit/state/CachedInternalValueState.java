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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

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
    private final CachePolicy<KeyNamespaceKey<K, N>, CachedValue<V>> l1Cache;
    private final CachePolicy<KeyNamespaceKey<K, N>, CachedValue<V>> l2Cache;
    private final CachePolicyType cachePolicyType;
    private final int lruOverflow;

    private final boolean bypassEnabled;
    private final double hitRateThreshold;
    private final int hitRateWindow;

    private N currentNamespace;

    // Sticky Cache (L1)
    private KeyNamespaceKey<K, N> lastAccessKey;
    private CachedValue<V> lastAccessValue;

    private final java.util.function.Consumer<K> keyContextSetter;

    private final Counter valueCalls;
    private final Counter updateCalls;
    private final Counter clearCalls;
    private final Counter getSerializedValueCalls;
    private final Counter delegateValueCalls;
    private final Counter delegateUpdateCalls;
    private final Counter delegateClearCalls;
    private final Counter cacheHits;
    private final Counter cacheMisses;
    private final Counter bypassValueCalls;
    private final Counter l1Evictions;
    private final Counter l2Evictions;

    private final KeyAccessStats<KeyNamespaceKey<K, N>> keyAccessStats;
    private final KeyAccessStats<K> globalKeyAccessStats;

    // Bypass State
    private volatile boolean isBypassing = false;
    private long currentWindowAccesses = 0;
    private long currentWindowHits = 0;
    private int opsSinceLastSample = 0;

    public CachedInternalValueState(
            InternalValueState<K, N, V> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
            int maxEntries,
            CachePolicyType cachePolicyType,
            int lruOverflow,
            boolean bypassEnabled,
            double hitRateThreshold,
            int hitRateWindow) {
        this(
                delegate,
                currentKeyProvider,
                keyContextSetter,
                maxEntries,
                cachePolicyType,
                lruOverflow,
                bypassEnabled,
                hitRateThreshold,
                hitRateWindow,
                null,
                null,
                null,
                hitRateWindow);
    }

    public CachedInternalValueState(
            InternalValueState<K, N, V> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
            int maxEntries,
            CachePolicyType cachePolicyType,
            int lruOverflow,
            boolean bypassEnabled,
            double hitRateThreshold,
            int hitRateWindow,
            MetricGroup metricGroup,
            String stateName,
            KeyAccessStats<K> globalKeyAccessStats,
            int keyStatsWindow) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.currentKeyProvider = Objects.requireNonNull(currentKeyProvider, "currentKeyProvider");
        this.keyContextSetter = Objects.requireNonNull(keyContextSetter, "keyContextSetter");
        this.cachePolicyType = Objects.requireNonNull(cachePolicyType, "cachePolicyType");
        this.lruOverflow = Math.max(0, lruOverflow);
        this.bypassEnabled = bypassEnabled;
        this.hitRateThreshold = hitRateThreshold;
        this.hitRateWindow = hitRateWindow;

        MetricGroup stateMetrics = null;
        if (metricGroup != null && stateName != null) {
            stateMetrics = metricGroup.addGroup("value_state").addGroup(stateName);
        }

        if (stateMetrics != null) {
            MetricGroup callGroup = stateMetrics.addGroup("calls");
            valueCalls = callGroup.counter("value");
            updateCalls = callGroup.counter("update");
            clearCalls = callGroup.counter("clear");
            getSerializedValueCalls = callGroup.counter("get_serialized_value");

            MetricGroup delegateGroup = stateMetrics.addGroup("delegate");
            delegateValueCalls = delegateGroup.counter("value");
            delegateUpdateCalls = delegateGroup.counter("update");
            delegateClearCalls = delegateGroup.counter("clear");

            MetricGroup cacheGroup = stateMetrics.addGroup("cache");
            cacheHits = cacheGroup.counter("hits");
            cacheMisses = cacheGroup.counter("misses");
            bypassValueCalls = cacheGroup.counter("bypass");
            l1Evictions = cacheGroup.counter("l1_evictions");
            l2Evictions = cacheGroup.counter("l2_evictions");
            cacheGroup.gauge("l1_size", () -> l1Cache.size());
            cacheGroup.gauge("l2_size", () -> l2Cache.size());
            cacheGroup.gauge(
                    "value_delegate_ratio",
                    () -> ratio(delegateValueCalls, valueCalls));
            cacheGroup.gauge(
                    "value_delegate_savings_ratio",
                    () -> savingsRatio(delegateValueCalls, valueCalls));

            MetricGroup keyGroup = stateMetrics.addGroup("keys");
            keyAccessStats = new KeyAccessStats<>(keyStatsWindow);
            keyGroup.gauge("total_accesses", () -> keyAccessStats.getWindowedAccesses());
            keyGroup.gauge("total_unique_keys", () -> keyAccessStats.getWindowedUniqueKeys());
            keyGroup.gauge("total_repeat_ratio", () -> keyAccessStats.getWindowedRepeatRatio());
            keyGroup.gauge("total_unique_ratio", () -> keyAccessStats.getWindowedUniqueRatio());
        } else {
            valueCalls = null;
            updateCalls = null;
            clearCalls = null;
            getSerializedValueCalls = null;
            delegateValueCalls = null;
            delegateUpdateCalls = null;
            delegateClearCalls = null;
            cacheHits = null;
            cacheMisses = null;
            bypassValueCalls = null;
            l1Evictions = null;
            l2Evictions = null;
            keyAccessStats = null;
        }
        this.globalKeyAccessStats = globalKeyAccessStats;

        // L1 Cache: ~20% of maxEntries or at least 128
        int l1Size = Math.max(128, maxEntries / 5);
        this.l1Cache = createCachePolicy(l1Size, this::onL1Eviction);

        // L2 Cache: Remaining size (or full maxEntries)
        this.l2Cache = createCachePolicy(maxEntries, this::onL2Eviction);
    }

    @Override
    public V value() throws IOException {
        if (valueCalls != null) {
            valueCalls.inc();
        }

        K currentKey = currentKeyProvider.getCurrentKey();
        KeyNamespaceKey<K, N> statsKey = new KeyNamespaceKey<>(currentKey, currentNamespace, true);
        recordKeyAccess(statsKey, currentKey);
        KeyNamespaceKey<K, N> probeKey = new KeyNamespaceKey<>(currentKey, currentNamespace, false);

        // 1. Check Sticky Cache (Always Check L0 - Fast Path)
        if (lastAccessKey != null && lastAccessKey.isSame(currentKey, currentNamespace)) {
            recordAccess(true); // Hit
            return lastAccessValue.valueOrNull();
        }

        // 2. Bypass Logic
        if (bypassEnabled && isBypassing) {
            // Sampling: Check cache every ~100 requests to see if we should re-enable
            opsSinceLastSample++;
            if (opsSinceLastSample < 100) {
                if (bypassValueCalls != null) {
                    bypassValueCalls.inc();
                }
                if (delegateValueCalls != null) {
                    delegateValueCalls.inc();
                }
                // Bypass mode: Direct to Delegate
                // Don't record access here to avoid skewing stats with 100% hits/misses?
                // Actually, if we bypass, we assume it's a "Miss" for the cache utility?
                // Or we just don't count it.
                // If we don't count it, we never exit bypass?
                // We MUST count it.
                // In bypass, we assume we SAVED a cache lookup overhead.
                // But to calculate "Hit Rate", we need to know if it WOULD have been a hit.
                // We don't know.
                // So we rely on the sample (below) to estimate hit rate.
                // We behave as if we are not looking.
                return delegate.value();
            }
            // Sample this request
            opsSinceLastSample = 0;
        }

        // 3. Check L1 Cache
        CachedValue<V> l1Cached = l1Cache.get(probeKey);
        if (l1Cached != null) {
            updateSticky(probeKey, l1Cached);
            recordAccess(true); // Hit
            return l1Cached.valueOrNull();
        }

        // 4. Check L2 Cache
        CachedValue<V> l2Cached = l2Cache.get(probeKey);
        if (l2Cached != null) {
            // Promote to L1 (Clean)
            KeyNamespaceKey<K, N> storageKey = new KeyNamespaceKey<>(currentKey, currentNamespace, true);
            CachedValue<V> newValue = CachedValue.of(l2Cached.valueOrNull(), false);
            l1Cache.put(storageKey, newValue);

            updateSticky(storageKey, newValue);
            recordAccess(true); // Hit
            return l2Cached.valueOrNull();
        }

        // 5. Miss -> Load from Delegate
        V loaded = delegate.value();
        if (delegateValueCalls != null) {
            delegateValueCalls.inc();
        }
        recordAccess(false); // Miss

        // 6. Update L1 (Clean)
        // Even if bypassing (sampled), we populate L1 to allow hit rate recovery
        KeyNamespaceKey<K, N> storageKey = new KeyNamespaceKey<>(currentKey, currentNamespace, true);
        CachedValue<V> newValue = CachedValue.of(loaded, false);
        l1Cache.put(storageKey, newValue);

        updateSticky(storageKey, newValue);
        return loaded;
    }

    @Override
    public void update(V value) throws IOException {
        if (updateCalls != null) {
            updateCalls.inc();
        }
        K currentKey = currentKeyProvider.getCurrentKey();

        if (bypassEnabled && isBypassing) {
            // Write-Through (Bypass Mode)
            delegate.update(value);
            if (delegateUpdateCalls != null) {
                delegateUpdateCalls.inc();
            }

            // Update L1 as Clean so subsequent reads (if sampled or re-enabled) find it.
            // This also ensures cache coherence.
            KeyNamespaceKey<K, N> cacheKey = new KeyNamespaceKey<>(currentKey, currentNamespace, true);
            CachedValue<V> newValue = CachedValue.of(value, false); // Clean
            l1Cache.put(cacheKey, newValue);
            updateSticky(cacheKey, newValue);
            return;
        }

        KeyNamespaceKey<K, N> cacheKey = new KeyNamespaceKey<>(currentKey, currentNamespace, true);
        CachedValue<V> newValue = CachedValue.of(value, true);

        // Write-Back: Update L1 only (marked dirty)
        l1Cache.put(cacheKey, newValue);

        // Optimistically update sticky
        updateSticky(cacheKey, newValue);
    }

    @Override
    public void clear() {
        if (clearCalls != null) {
            clearCalls.inc();
        }
        K currentKey = currentKeyProvider.getCurrentKey();
        KeyNamespaceKey<K, N> cacheKey = new KeyNamespaceKey<>(currentKey, currentNamespace, true);
        CachedValue<V> newValue;

        if (bypassEnabled && isBypassing) {
            delegate.clear();
            if (delegateClearCalls != null) {
                delegateClearCalls.inc();
            }
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
        delegate.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<V> safeValueSerializer)
            throws Exception {
        if (getSerializedValueCalls != null) {
            getSerializedValueCalls.inc();
        }
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
        if (l1Evictions != null) {
            l1Evictions.inc();
        }
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
        if (l2Evictions != null) {
            l2Evictions.inc();
        }
        // L2 is clean (backed by delegate). Just drop.
    }

    private void recordAccess(boolean isHit) {
        if (isHit) {
            if (cacheHits != null) {
                cacheHits.inc();
            }
        } else {
            if (cacheMisses != null) {
                cacheMisses.inc();
            }
        }

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
                // If currently bypassing, we only switch BACK if hit rate > threshold
                if (!shouldBypass) {
                    isBypassing = false;
                    // Reset sticky cache to avoid stale hits? No, sticky is updated on update()
                    // Ops since last sample reset automatically
                }
            } else {
                // If currently NOT bypassing, switch TO bypass if hit rate < threshold
                if (shouldBypass) {
                    isBypassing = true;
                    flush(); // Essential: Flush dirty value to delegate before entering bypass
                             // (Write-Through) mode
                }
            }

            // Reset window
            currentWindowAccesses = 0;
            currentWindowHits = 0;
        }
    }

    private void recordKeyAccess(KeyNamespaceKey<K, N> key, K currentKey) {
        if (keyAccessStats != null) {
            keyAccessStats.record(key);
        }
        if (globalKeyAccessStats != null) {
            globalKeyAccessStats.record(currentKey);
        }
    }

    private static double ratio(Counter numerator, Counter denominator) {
        if (numerator == null || denominator == null) {
            return 0.0d;
        }
        long total = denominator.getCount();
        if (total <= 0) {
            return 0.0d;
        }
        return (double) numerator.getCount() / total;
    }

    private static double savingsRatio(Counter delegateCounter, Counter totalCounter) {
        if (delegateCounter == null || totalCounter == null) {
            return 0.0d;
        }
        long total = totalCounter.getCount();
        if (total <= 0) {
            return 0.0d;
        }
        return 1.0d - ((double) delegateCounter.getCount() / total);
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
                delegateClearCalls.inc();
            } else {
                delegate.update(value.value);
                delegateUpdateCalls.inc();
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
