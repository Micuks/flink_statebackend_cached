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

package org.apache.flink.contrib.streaming.state.cacheplus;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.CachingPlusKeyedStateBackend;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalKvState.StateIncrementalVisitor;
import org.apache.flink.runtime.state.internal.InternalValueState;

import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * An {@link InternalValueState} that uses an L1/L2 cache for its values.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class CachingInternalValueState<K, N, V>
        implements InternalValueState<K, N, V>,
        CachingInternalState<K, N, V, InternalValueState<K, N, V>> {

    private static final Logger LOG = NOPLogger.NOP_LOGGER;
    private final InternalValueState<K, N, V> delegateState;
    private final CachingPlusKeyedStateBackend<K> backend; // For accessing current key
    // Single-layer global caches keyed by composite key (keyGroup + namespace + key)
    private final CachePolicy<CompositeKey, CacheEntry<V>> l1Cache;
    private final CachePolicy<CompositeKey, CacheEntry<V>> l2Cache;

    private final int l1CacheSizePerKeyPerNamespace;
    private final int l2CacheSizePerKeyPerNamespace;
    private final int maxActiveNamespacesInCache;
    private final long maxCacheMemoryMb;
    private N currentNamespace;
    // Cached serialized form of current namespace to avoid re-serialization on hot paths
    private StableNamespaceKey currentNamespaceStableKey;
    private final CachingStateBackendFactory.CachePolicyType cachePolicyType;

    // Configuration for cache bypass
    private final double cacheHitRateThreshold;
    private final long cacheHitRateWindowSize;
    private final long cacheMinAccessesForBypassCheck;

    // State for cache bypass logic
    private transient AtomicLong accessesForHitRateWindow;
    private transient AtomicLong hitsInHitRateWindow;
    private transient AtomicLong totalAccessesForBypassEligibility;
    private transient AtomicLong accessSampler;
    private static final int SAMPLING_RATE = 100; // 1%
    // Hysteresis parameters for adaptive bypass
    private final double lowHitRateThreshold; // enter bypass below this
    private final double highHitRateThreshold; // exit bypass above this
    private final int enterConsecutiveLowWindows = 1; // react quickly to poor locality
    private final int exitConsecutiveHighWindows = 2; // require stability to re-enable cache
    private final int cooldownWindowsAfterToggle = 2; // avoid thrashing after a decision
    private transient long completedWindows = 0L;
    private transient int consecutiveLow = 0;
    private transient int consecutiveHigh = 0;
    private transient int windowsSinceToggle = 0;
    // Metrics
    private final transient MetricGroup metrics;
    private final Counter cacheHits;                 // generic compatibility
    private final Counter cacheMisses;               // generic compatibility
    private final Counter cacheBypassActivations;    // generic

    // Detailed counters aligned with CachingInternalMapState naming
    private final Counter l1ValueCacheHitCount;
    private final Counter l1ValueCacheMissCount;
    private final Counter l2ValueCacheHitCount;
    private final Counter l2ValueCacheMissCount;
    private final Counter delegateLookups;
    // ValueState-specific aliases to enable separate reporting in benchmarks
    private final Counter valueStateL1CacheHitCount;
    private final Counter valueStateL1CacheMissCount;
    private final Counter valueStateL2CacheHitCount;
    private final Counter valueStateL2CacheMissCount;

    private volatile boolean bypassCache = false;
    private final boolean bypassEnabled;
    // When false, perform write-through on update() to ensure correctness
    private final boolean writeBehindEnabled;

    // Lightweight profiling controls (align names with MapState for Prometheus queries)
    private final boolean profileEnabled;
    private final int profileSampleRate;
    private final transient AtomicLong profileCounter = new AtomicLong(0L);
    private final transient AtomicLong profGetTotalNanos = new AtomicLong(0L);    // value() ~ get
    private final transient AtomicLong profPutTotalNanos = new AtomicLong(0L);    // update() ~ put
    private final transient AtomicLong profRemoveTotalNanos = new AtomicLong(0L); // clear() ~ remove
    private transient Counter profGetCalls;
    private transient Counter profPutCalls;
    private transient Counter profRemoveCalls;

    public CachingInternalValueState(
            InternalValueState<K, N, V> delegateState,
            CachingPlusKeyedStateBackend<K> backend,
            int l1CacheSize,
            int l2CacheSize,
            int maxActiveNamespacesInCache,
            long maxCacheMemoryMb,
            CachingStateBackendFactory.CachePolicyType cachePolicyType,
            double cacheHitRateThreshold,
            long cacheHitRateWindowSize,
            long cacheMinAccessesForBypassCheck,
            boolean bypassEnabled,
            boolean writeBehindEnabled,
            MetricGroup metricsGroup) {
        this.delegateState = delegateState;
        this.backend = backend;
        this.l1CacheSizePerKeyPerNamespace = l1CacheSize;
        this.l2CacheSizePerKeyPerNamespace = l2CacheSize;
        this.maxActiveNamespacesInCache = maxActiveNamespacesInCache;
        this.maxCacheMemoryMb = maxCacheMemoryMb;
        this.cachePolicyType = cachePolicyType;

        this.cacheHitRateThreshold = cacheHitRateThreshold;
        this.cacheHitRateWindowSize = cacheHitRateWindowSize;
        this.cacheMinAccessesForBypassCheck = cacheMinAccessesForBypassCheck;
        this.bypassEnabled = bypassEnabled;
        this.writeBehindEnabled = writeBehindEnabled;

        // Initialize hysteresis thresholds based on provided threshold
        this.lowHitRateThreshold = Math.max(0.0, this.cacheHitRateThreshold - 0.10);
        this.highHitRateThreshold = Math.min(1.0, this.cacheHitRateThreshold);

        // Initialize lightweight profiling configuration (must assign final fields)
        this.profileEnabled = backend != null && backend.isProfileEnabled();
        this.profileSampleRate = backend != null ? backend.getProfileSampleRate() : 1024;

        if (this.bypassEnabled && this.cacheHitRateThreshold > 0.0) {
            this.accessesForHitRateWindow = new AtomicLong(0);
            this.hitsInHitRateWindow = new AtomicLong(0);
            this.totalAccessesForBypassEligibility = new AtomicLong(0);
            this.accessSampler = new AtomicLong(0);
        } else {
            this.accessesForHitRateWindow = null;
            this.hitsInHitRateWindow = null;
            this.totalAccessesForBypassEligibility = null;
            this.accessSampler = null;
        }

        // Initialize metrics
        this.metrics = metricsGroup;
        if (metricsGroup != null) {
            MetricGroup cacheMetrics = metricsGroup.addGroup("cache");
            // Detailed counters (match map-state names: l1ValueCacheHit/Miss, l2ValueCacheHit/Miss)
            this.l1ValueCacheHitCount = cacheMetrics.counter("l1ValueCacheHit");
            this.l1ValueCacheMissCount = cacheMetrics.counter("l1ValueCacheMiss");
            this.l2ValueCacheHitCount = cacheMetrics.counter("l2ValueCacheHit");
            this.l2ValueCacheMissCount = cacheMetrics.counter("l2ValueCacheMiss");
            this.delegateLookups = cacheMetrics.counter("delegateLookups");

            // ValueState-specific counters for separate reporting in Nexmark
            this.valueStateL1CacheHitCount = cacheMetrics.counter("valueStateL1CacheHit");
            this.valueStateL1CacheMissCount = cacheMetrics.counter("valueStateL1CacheMiss");
            this.valueStateL2CacheHitCount = cacheMetrics.counter("valueStateL2CacheHit");
            this.valueStateL2CacheMissCount = cacheMetrics.counter("valueStateL2CacheMiss");

            // Generic counters for backward-compat dashboards
            this.cacheHits = cacheMetrics.counter("hits");
            this.cacheMisses = cacheMetrics.counter("misses");
            this.cacheBypassActivations = cacheMetrics.counter("bypassActivations");

            // Optional aggregate gauges for total entries across namespaces (low-cardinality)
            try {
                MetricGroup agg = cacheMetrics.addGroup("aggregate");
                agg.gauge("l1EntriesTotal", (Gauge<Long>) this::sumL1Entries);
                agg.gauge("l2EntriesTotal", (Gauge<Long>) this::sumL2Entries);
            } catch (Throwable t) {
                // best-effort only
            }
        } else {
            // No metrics group: use no-op counters
            Counter no = new NoOpCounter();
            this.cacheHits = no;
            this.cacheMisses = no;
            this.cacheBypassActivations = no;
            this.l1ValueCacheHitCount = no;
            this.l1ValueCacheMissCount = no;
            this.l2ValueCacheHitCount = no;
            this.l2ValueCacheMissCount = no;
            this.delegateLookups = no;
            this.valueStateL1CacheHitCount = no;
            this.valueStateL1CacheMissCount = no;
            this.valueStateL2CacheHitCount = no;
            this.valueStateL2CacheMissCount = no;
        }

        // Create single-layer global caches with eviction listeners
        int l1GlobalCapacity = Math.max(1, this.l1CacheSizePerKeyPerNamespace * this.maxActiveNamespacesInCache);
        int l2GlobalCapacity = Math.max(1, this.l2CacheSizePerKeyPerNamespace * this.maxActiveNamespacesInCache);

        this.l2Cache = createCachePolicyWithEvictionListener(l2GlobalCapacity, evicted -> {
            CacheEntry<V> entry = evicted.getValue();
            if (entry != null) {
                backend.reportCacheMemoryReleased(entry.getEstimatedSizeBytes());
            }
        });

        this.l1Cache = createCachePolicyWithEvictionListener(l1GlobalCapacity, evicted -> {
            CompositeKey ck = evicted.getKey();
            CacheEntry<V> entry = evicted.getValue();
            if (entry == null) {
                return;
            }
            long estimatedSize = entry.getEstimatedSizeBytes();
            backend.reportCacheMemoryReleased(estimatedSize); // release L1 memory

            if (entry.isDirty()) {
                // Flush dirty entry to delegate using composite key context
                K originalKey = backend.getCurrentKey();
                N originalNs = this.currentNamespace;
                try {
                    K k = ck.deserializeKey(getKeySerializer());
                    N n = ck.deserializeNamespace(getNamespaceSerializer());
                    backend.setCurrentKey(k);
                    delegateState.setCurrentNamespace(n);
                    delegateState.update(entry.getValue());
                    entry.setDirty(false);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to flush dirty entry on L1 eviction", e);
                } finally {
                    backend.setCurrentKey(originalKey);
                    if (originalNs != null) {
                        delegateState.setCurrentNamespace(originalNs);
                    }
                }
            }

            CacheEntry<V> old = l2Cache.put(ck, entry);
            if (old != null) {
                backend.reportCacheMemoryReleased(old.getEstimatedSizeBytes());
            }
            backend.reportCacheMemoryAdded(entry.getEstimatedSizeBytes());
        });
    }

    private void registerProfileMetricsIfNeeded() {
        if (profGetCalls != null) return; // already registered
        LOG.info("Attempting to register profile metrics. profileEnabled={}, metricsIsNull={}", profileEnabled, metrics == null);
        if (!profileEnabled || metrics == null) {
            return;
        }
        try {
            MetricGroup pg = metrics.addGroup("profile");
            profGetCalls = pg.counter("get_calls");
            profPutCalls = pg.counter("put_calls");
            profRemoveCalls = pg.counter("remove_calls");
            pg.gauge("get_totalNanos", () -> profGetTotalNanos.get());
            pg.gauge("put_totalNanos", () -> profPutTotalNanos.get());
            pg.gauge("remove_totalNanos", () -> profRemoveTotalNanos.get());
            pg.gauge("get_avgMicros", () -> {
                long c = profGetCalls.getCount();
                return c > 0 ? (profGetTotalNanos.get() / (double) c) / 1000.0 : 0.0;
            });
            pg.gauge("put_avgMicros", () -> {
                long c = profPutCalls.getCount();
                return c > 0 ? (profPutTotalNanos.get() / (double) c) / 1000.0 : 0.0;
            });
            pg.gauge("remove_avgMicros", () -> {
                long c = profRemoveCalls.getCount();
                return c > 0 ? (profRemoveTotalNanos.get() / (double) c) / 1000.0 : 0.0;
            });
            LOG.info("Successfully registered profile metrics.");
        } catch (Exception e) {
            LOG.error("Failed to register profile metrics", e);
        }
    }

    private long maybeStartTimer() {
        if (!profileEnabled) return 0L;
        long c = profileCounter.incrementAndGet();
        if (profileSampleRate <= 1 || (c % profileSampleRate) == 0L) {
            return System.nanoTime();
        }
        return 0L;
    }
    private <CK, CV> CachePolicy<CK, CV> createCachePolicy(int capacity) {
        switch (cachePolicyType) {
            case TINYLFU:
                return new TinyLFUMap<>(capacity);
            case LRU:
            default:
                return new LRUMap<>(capacity);
        }
    }

    private <CK, CV> CachePolicy<CK, CV> createCachePolicyWithEvictionListener(int capacity,
            Consumer<Map.Entry<CK, CV>> evictionListener) {
        switch (cachePolicyType) {
            case TINYLFU:
                return new TinyLFUMap<>(capacity, evictionListener);
            case LRU:
            default:
                return new LRUMap<>(capacity, evictionListener);
        }
    }

    private void flushCacheForNamespaceKey(StableNamespaceKey namespaceKey, CachePolicy<K, CacheEntry<V>> cache) {
        // Flush without mutating this wrapper's currentNamespace to avoid interfering with
        // concurrent operator logic that relies on wrapper context.
        N originalWrapperNs = this.currentNamespace; // do not modify
        K originalKey = backend.getCurrentKey();
        N nsForFlush = namespaceKey.deserialize(getNamespaceSerializer());
        // Capture delegate's original namespace (equal to wrapper's in normal cases)
        N originalDelegateNs = originalWrapperNs;
        try {
            if (nsForFlush != null) {
                delegateState.setCurrentNamespace(nsForFlush);
            }

            // Create a copy of entries to avoid ConcurrentModificationException
            List<Map.Entry<K, CacheEntry<V>>> entries = new ArrayList<>();
            for (Map.Entry<K, CacheEntry<V>> e : cache.entrySet()) {
                entries.add(e);
            }

            for (Map.Entry<K, CacheEntry<V>> entry : entries) {
                K key = entry.getKey();
                CacheEntry<V> cacheEntry = entry.getValue();
                if (cacheEntry != null && cacheEntry.isDirty()) {
                    V value = cacheEntry.getValue();
                    K innerOriginalKey = backend.getCurrentKey();
                    try {
                        backend.setCurrentKey(key);
                        delegateState.update(value);
                        cacheEntry.setDirty(false); // Mark clean after flushing
                    } finally {
                        backend.setCurrentKey(innerOriginalKey);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush dirty entries for evicted namespace: " + namespaceKey, e);
        } finally {
            // Restore delegate namespace to original without touching wrapper's field
            if (originalDelegateNs != null) {
                delegateState.setCurrentNamespace(originalDelegateNs);
            }
            backend.setCurrentKey(originalKey);
        }
    }

    // Aggregate helpers for gauges
    private long sumL1Entries() {
        try {
            return l1Cache.size();
        } catch (Throwable ignored) {
            return 0L;
        }
    }

    private long sumL2Entries() {
        try {
            return l2Cache.size();
        } catch (Throwable ignored) {
            return 0L;
        }
    }

    private void updateCacheBypassCondition(boolean resolvedByCache) {
        updateCacheBypassConditionWeighted(resolvedByCache, 1);
    }

    // Weighted variant for adaptive bypass with hysteresis and cooldown.
    private void updateCacheBypassConditionWeighted(boolean resolvedByCache, int weight) {
        if (weight <= 0) {
            return; // ignore non-positive weights
        }
        if (
                !bypassEnabled ||
                cacheHitRateThreshold <= 0.0 ||
                accessesForHitRateWindow == null ||
                hitsInHitRateWindow == null ||
                totalAccessesForBypassEligibility == null
        ) {
            this.bypassCache = false;
            return;
        }

        if (resolvedByCache) {
            hitsInHitRateWindow.addAndGet(weight);
        }
        long currentWindowAccesses = accessesForHitRateWindow.addAndGet(weight);

        if (currentWindowAccesses >= this.cacheHitRateWindowSize) {
            long totalAccesses = totalAccessesForBypassEligibility.addAndGet(currentWindowAccesses);

            if (totalAccesses < this.cacheMinAccessesForBypassCheck) {
                accessesForHitRateWindow.set(0);
                hitsInHitRateWindow.set(0);
                this.bypassCache = false;
                return;
            }

            double currentHitRate = (double) hitsInHitRateWindow.get() / currentWindowAccesses;

            completedWindows++;
            boolean inCooldown = windowsSinceToggle < cooldownWindowsAfterToggle;

            if (currentHitRate < lowHitRateThreshold) {
                consecutiveLow++;
                consecutiveHigh = 0;
                if (!inCooldown && !bypassCache && consecutiveLow >= enterConsecutiveLowWindows) {
                    bypassCache = true; // enter bypass
                    windowsSinceToggle = 0;
                    consecutiveLow = 0;
                    consecutiveHigh = 0;
                    cacheBypassActivations.inc();
                    LOG.info(
                        "ValueState cache bypass ENTER. hitRate={}%, window={} hits/{} acc, ns={}.",
                        String.format("%.2f", currentHitRate * 100),
                        hitsInHitRateWindow.get(),
                        currentWindowAccesses,
                        getCurrentNamespace()
                    );
                }
            } else if (currentHitRate > highHitRateThreshold) {
                consecutiveHigh++;
                consecutiveLow = 0;
                if (!inCooldown && bypassCache && consecutiveHigh >= exitConsecutiveHighWindows) {
                    bypassCache = false; // exit bypass
                    windowsSinceToggle = 0;
                    consecutiveLow = 0;
                    consecutiveHigh = 0;
                    LOG.info(
                        "ValueState cache bypass EXIT. hitRate={}%, window={} hits/{} acc, ns={}.",
                        String.format("%.2f", currentHitRate * 100),
                        hitsInHitRateWindow.get(),
                        currentWindowAccesses,
                        getCurrentNamespace()
                    );
                }
            } else {
                // Between thresholds: drift toward stability
                consecutiveLow = 0;
                consecutiveHigh = 0;
            }

            if (windowsSinceToggle < cooldownWindowsAfterToggle) {
                windowsSinceToggle++;
            }

            // Reset for next window
            accessesForHitRateWindow.set(0);
            hitsInHitRateWindow.set(0);
        }
    }

    // Method for CachingPlusKeyedStateBackend to access the delegate for registration checks
    @Override
    public InternalValueState<K, N, V> getDelegateState() {
        return delegateState;
    }

    private CachePolicy<CompositeKey, CacheEntry<V>> getL1CacheForNamespace(N namespace) {
        return l1Cache;
    }

    private CachePolicy<CompositeKey, CacheEntry<V>> getL2CacheForNamespace(N namespace) {
        return l2Cache;
    }

    @Override
    public V value() throws IOException {
        registerProfileMetricsIfNeeded();
        final long t0 = maybeStartTimer();
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        try {
            int decisionWeight = 1;
            if (bypassEnabled && bypassCache) {
                boolean isSample = accessSampler != null && (accessSampler.incrementAndGet() % SAMPLING_RATE == 0);
                if (!isSample) {
                    // In bypass mode, do not count unsampled accesses toward the hit-rate window.
                    delegateLookups.inc();
                    return delegateState.value();
                }
                decisionWeight = SAMPLING_RATE; // sample represents multiple accesses
            }

        CachePolicy<CompositeKey, CacheEntry<V>> l1Cache = getL1CacheForNamespace(currentNamespace);
        int kg = backend.getKeyContext().getCurrentKeyGroupIndex();
        CompositeKey ck = CompositeKey.from(currentKey, kg, currentNamespace, getKeySerializer(), getNamespaceSerializer());
        CacheEntry<V> l1Entry = l1Cache.get(ck);

        if (l1Entry != null) {
            updateCacheBypassConditionWeighted(true, decisionWeight);
            cacheHits.inc();
            l1ValueCacheHitCount.inc();
            valueStateL1CacheHitCount.inc();
            return l1Entry.getValue();
        }

        // L1 miss
        l1ValueCacheMissCount.inc();
        valueStateL1CacheMissCount.inc();

        CachePolicy<CompositeKey, CacheEntry<V>> l2Cache = getL2CacheForNamespace(currentNamespace);
        CacheEntry<V> l2Entry = l2Cache.get(ck);

        if (l2Entry != null) {
            updateCacheBypassConditionWeighted(true, decisionWeight);
            cacheHits.inc();
            l2ValueCacheHitCount.inc();
            valueStateL2CacheHitCount.inc();
            // L2 entry is always clean. Remove from L2, put into L1.
            // No memory change reported here as it's a move between caches of the same backend instance.
            // However, if L1.put causes an eviction, that eviction will report a release.
            // And this put will report an add.
            l2Cache.remove(ck); // This remove itself doesn't release memory from the CachingPlusKeyedStateBackend's perspective yet
            // The entry is still "in play" until it's re-added to L1 or discarded.
            // We don't report release for l2Entry removal & add for l1Cache.put here to avoid double counting
            // if l1Cache.put evicts something (which would report release) and then adds this (reporting add).
            // Instead, the L1 put will handle the accounting if it replaces something or just adds.
            CacheEntry<V> entryToL1 = CacheEntry.clean(l2Entry.getValue()); // Create a new entry for L1 if needed, or use l2Entry if suitable
            CacheEntry<V> oldL1Entry = l1Cache.put(ck, entryToL1);
            if (oldL1Entry != null) {
                backend.reportCacheMemoryReleased(oldL1Entry.getEstimatedSizeBytes());
            }
            backend.reportCacheMemoryAdded(entryToL1.getEstimatedSizeBytes());
            return entryToL1.getValue();
        }

        updateCacheBypassConditionWeighted(false, decisionWeight);
        cacheMisses.inc();
        l2ValueCacheMissCount.inc();
        valueStateL2CacheMissCount.inc();
        delegateLookups.inc();
        V valueFromDelegate = delegateState.value();
        if (valueFromDelegate != null) { // Only cache non-null
            CacheEntry<V> newEntry = CacheEntry.clean(valueFromDelegate);
            CacheEntry<V> oldL1Entry = l1Cache.put(ck, newEntry);
            if (oldL1Entry != null) {
                backend.reportCacheMemoryReleased(oldL1Entry.getEstimatedSizeBytes());
            }
            backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
        }
            return valueFromDelegate;
        } finally {
            if (t0 != 0L) {
                long dur = System.nanoTime() - t0;
                long weight = Math.max(1, this.profileSampleRate);
                profGetTotalNanos.addAndGet(dur * weight);
                if (profGetCalls != null) profGetCalls.inc(weight);
            }
        }
    }

    @Override
    public void update(V value) throws IOException {
        registerProfileMetricsIfNeeded();
        final long t0 = maybeStartTimer();
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();
        try {
            if (value == null) { // As per Flink ValueState contract
                clear(); // clear() will handle memory reporting
                return;
            }

            if (bypassEnabled && bypassCache) {
                delegateState.update(value);
                // Invalidate caches to avoid stale flush later
                CachePolicy<CompositeKey, CacheEntry<V>> l1Bypass = getL1CacheForNamespace(currentNamespace);
                int kgBypass = backend.getKeyContext().getCurrentKeyGroupIndex();
                CompositeKey ckBypass = CompositeKey.from(currentKey, kgBypass, currentNamespace, getKeySerializer(), getNamespaceSerializer());
                CacheEntry<V> old1 = l1Bypass.remove(ckBypass);
                if (old1 != null) {
                    backend.reportCacheMemoryReleased(old1.getEstimatedSizeBytes());
                }
                CachePolicy<CompositeKey, CacheEntry<V>> l2Bypass = getL2CacheForNamespace(currentNamespace);
                CacheEntry<V> old2 = l2Bypass.remove(ckBypass);
                if (old2 != null) {
                    backend.reportCacheMemoryReleased(old2.getEstimatedSizeBytes());
                }
                // Optionally cache a clean value in L1 for locality
                CacheEntry<V> cleanEntry = CacheEntry.clean(value);
                CacheEntry<V> prev = l1Bypass.put(ckBypass, cleanEntry);
                if (prev != null) {
                    backend.reportCacheMemoryReleased(prev.getEstimatedSizeBytes());
                }
                backend.reportCacheMemoryAdded(cleanEntry.getEstimatedSizeBytes());
                return;
            }

            // Honor write-behind toggle: when disabled, perform write-through and keep L1 clean
            if (!writeBehindEnabled) {
                delegateState.update(value);
                CachePolicy<CompositeKey, CacheEntry<V>> l1Cache = getL1CacheForNamespace(currentNamespace);
                CacheEntry<V> clean = CacheEntry.clean(value);
                int kgWt = backend.getKeyContext().getCurrentKeyGroupIndex();
                CompositeKey ckWt = CompositeKey.from(currentKey, kgWt, currentNamespace, getKeySerializer(), getNamespaceSerializer());
                CacheEntry<V> oldL1 = l1Cache.put(ckWt, clean);
                if (oldL1 != null) {
                    backend.reportCacheMemoryReleased(oldL1.getEstimatedSizeBytes());
                }
                backend.reportCacheMemoryAdded(clean.getEstimatedSizeBytes());
                CachePolicy<CompositeKey, CacheEntry<V>> l2Cache = getL2CacheForNamespace(currentNamespace);
                CacheEntry<V> oldL2 = l2Cache.remove(ckWt);
                if (oldL2 != null) {
                    backend.reportCacheMemoryReleased(oldL2.getEstimatedSizeBytes());
                }
                return;
            }

            CachePolicy<CompositeKey, CacheEntry<V>> l1Cache = getL1CacheForNamespace(currentNamespace);
            CacheEntry<V> newEntry = CacheEntry.dirty(value);
            int kgWb = backend.getKeyContext().getCurrentKeyGroupIndex();
            CompositeKey ckWb = CompositeKey.from(currentKey, kgWb, currentNamespace, getKeySerializer(), getNamespaceSerializer());
            CacheEntry<V> oldL1Entry = l1Cache.put(ckWb, newEntry);
            if (oldL1Entry != null) {
                backend.reportCacheMemoryReleased(oldL1Entry.getEstimatedSizeBytes());
            }
            backend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());

            // L1 remains write-back tolerant even if write-behind is disabled; L2 will only contain clean entries via eviction path

            // If L2 had this key, it's now stale, remove it.
            CachePolicy<CompositeKey, CacheEntry<V>> l2Cache = getL2CacheForNamespace(currentNamespace);
            CacheEntry<V> oldL2Entry = l2Cache.remove(ckWb);
            if (oldL2Entry != null) {
                // L2 entries are implicitly managed by L1 evictions or direct stale removal like here.
                // Their memory was accounted for when they moved from L1 to L2 (L1 released, L2 added - though we simplified this)
                // Or when loaded to L2 directly. When removing from L2 here because L1 got an update,
                // we should report its memory as released if it wasn't already part of L1's old entry.
                // Simplified: Assume L2 entries are clean and their removal directly translates to released memory
                // if they weren't the source for the L1 update that just happened.
                // However, simpler just to let their L1 eviction listener handle the release when they were put there.
                // The current logic in L1 eviction listener (getL1CacheForNamespace) moves to L2 and L2 doesn't have
                // an aggressive release reporting on its own removals. This explicit remove should report.
                backend.reportCacheMemoryReleased(oldL2Entry.getEstimatedSizeBytes());
            }
        } finally {
            if (t0 != 0L) {
                long dur = System.nanoTime() - t0;
                long weight = Math.max(1, this.profileSampleRate);
                profPutTotalNanos.addAndGet(dur * weight);
                if (profPutCalls != null) profPutCalls.inc(weight);
            }
        }
    }

    @Override
    public void clear() {
        registerProfileMetricsIfNeeded();
        final long t0 = maybeStartTimer();
        K currentKey = backend.getCurrentKey();
        N currentNamespace = getCurrentNamespace();

        CachePolicy<CompositeKey, CacheEntry<V>> l1Cache = getL1CacheForNamespace(currentNamespace);
        int kgCl = backend.getKeyContext().getCurrentKeyGroupIndex();
        CompositeKey ckCl = CompositeKey.from(currentKey, kgCl, currentNamespace, getKeySerializer(), getNamespaceSerializer());
        CacheEntry<V> oldL1Entry = l1Cache.remove(ckCl);
        if (oldL1Entry != null) {
            backend.reportCacheMemoryReleased(oldL1Entry.getEstimatedSizeBytes());
        }

        CachePolicy<CompositeKey, CacheEntry<V>> l2Cache = getL2CacheForNamespace(currentNamespace);
        CacheEntry<V> oldL2Entry = l2Cache.remove(ckCl);
        if (oldL2Entry != null) {
            backend.reportCacheMemoryReleased(oldL2Entry.getEstimatedSizeBytes());
        }

        delegateState.clear(); // Clear the underlying state
        if (t0 != 0L) {
            long dur = System.nanoTime() - t0;
            long weight = Math.max(1, this.profileSampleRate);
            profRemoveTotalNanos.addAndGet(dur * weight);
            if (profRemoveCalls != null) profRemoveCalls.inc(weight);
        }
    }

    @Override
    public void flushToUnderlyingState() throws IOException {
        K originalKey = backend.getCurrentKey();
        N originalWrapperNs = this.currentNamespace; // keep wrapper stable
        N originalDelegateNs = originalWrapperNs;
        try {
            // Flush dirty entries across L1 without mutating wrapper context
            List<Map.Entry<CompositeKey, CacheEntry<V>>> snapshot = new ArrayList<>();
            for (Map.Entry<CompositeKey, CacheEntry<V>> e : l1Cache.entrySet()) {
                snapshot.add(e);
            }
            for (Map.Entry<CompositeKey, CacheEntry<V>> e : snapshot) {
                CacheEntry<V> entry = e.getValue();
                if (entry != null && entry.isDirty()) {
                    CompositeKey ck = e.getKey();
                    V value = entry.getValue();
                    K prevKey = backend.getCurrentKey();
                    N prevNs = this.currentNamespace;
                    try {
                        K key = ck.deserializeKey(getKeySerializer());
                        N ns = ck.deserializeNamespace(getNamespaceSerializer());
                        backend.setCurrentKey(key);
                        delegateState.setCurrentNamespace(ns);
                        delegateState.update(value);
                        entry.setDirty(false);
                    } finally {
                        backend.setCurrentKey(prevKey);
                        if (prevNs != null) {
                            delegateState.setCurrentNamespace(prevNs);
                        }
                    }
                }
            }

            // L2 should be clean; no-op

        } finally {
            backend.setCurrentKey(originalKey);
            if (originalDelegateNs != null) {
                delegateState.setCurrentNamespace(originalDelegateNs);
            }
        }
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return delegateState.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return delegateState.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return delegateState.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(@Nonnull N namespace) {
        // Set for the delegate, the cache keying already uses the namespace.
        this.currentNamespace = namespace;
        if (namespace != null) {
            delegateState.setCurrentNamespace(namespace);
        }
        // Cache serialized form for hot-path cache lookups
        try {
            if (namespace != null) {
                this.currentNamespaceStableKey = StableNamespaceKey.fromNamespace(namespace, getNamespaceSerializer());
            } else {
                this.currentNamespaceStableKey = null;
            }
        } catch (Throwable t) {
            // Best-effort; if serialization fails here, fall back to on-demand serialization later
            this.currentNamespaceStableKey = null;
        }
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<V> safeValueSerializer)
            throws Exception {
        // To guarantee correctness for delegate reads that bypass the wrapper, flush
        // pending updates first. This is a relatively cold path and safe to flush.
        flushToUnderlyingState();
        return delegateState.getSerializedValue(
                serializedKeyAndNamespace,
                safeKeySerializer,
                safeNamespaceSerializer,
                safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        try {
            // Ensure delegate reflects all pending updates before exposing a visitor
            flushToUnderlyingState();
        } catch (IOException e) {
            throw new RuntimeException("Error flushing value state before creating visitor.", e);
        }
        return delegateState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    @Nonnull
    public N getCurrentNamespace() {
        return this.currentNamespace;
    }

    @Override
    public long evictEntriesToFreeMemory(long targetBytesToFreeThisState) {
        long bytesFreed = 0;
        if (targetBytesToFreeThisState <= 0) return 0;

        // Phase 1: free from L2 (all clean)
        Iterator<Map.Entry<CompositeKey, CacheEntry<V>>> l2Iter = l2Cache.entrySet().iterator();
        while (l2Iter.hasNext() && bytesFreed < targetBytesToFreeThisState) {
            Map.Entry<CompositeKey, CacheEntry<V>> e = l2Iter.next();
            CacheEntry<V> cacheValue = e.getValue();
            long estimatedSize = cacheValue.getEstimatedSizeBytes();
            l2Iter.remove();
            backend.reportCacheMemoryReleased(estimatedSize);
            bytesFreed += estimatedSize;
        }
        if (bytesFreed >= targetBytesToFreeThisState) return bytesFreed;

        // Phase 2: free clean from L1, collect dirty
        List<Map.Entry<CompositeKey, CacheEntry<V>>> dirtyL1 = new ArrayList<>();
        Iterator<Map.Entry<CompositeKey, CacheEntry<V>>> l1Iter = l1Cache.entrySet().iterator();
        while (l1Iter.hasNext() && bytesFreed < targetBytesToFreeThisState) {
            Map.Entry<CompositeKey, CacheEntry<V>> e = l1Iter.next();
            CacheEntry<V> val = e.getValue();
            if (!val.isDirty()) {
                long est = val.getEstimatedSizeBytes();
                l1Iter.remove();
                backend.reportCacheMemoryReleased(est);
                bytesFreed += est;
            } else {
                dirtyL1.add(e);
            }
        }
        if (bytesFreed >= targetBytesToFreeThisState) return bytesFreed;

        // Phase 3: flush-and-remove dirty from L1
        for (Map.Entry<CompositeKey, CacheEntry<V>> e : dirtyL1) {
            if (bytesFreed >= targetBytesToFreeThisState) break;
            CompositeKey ck = e.getKey();
            CacheEntry<V> val = e.getValue();
            long est = val.getEstimatedSizeBytes();
            K prevKey = backend.getCurrentKey();
            N prevNs = this.currentNamespace;
            try {
                K key = ck.deserializeKey(getKeySerializer());
                N ns = ck.deserializeNamespace(getNamespaceSerializer());
                backend.setCurrentKey(key);
                delegateState.setCurrentNamespace(ns);
                delegateState.update(val.getValue());
                val.setDirty(false);
                l1Cache.remove(ck);
                backend.reportCacheMemoryReleased(est);
                bytesFreed += est;
            } catch (Exception ex) {
                // best-effort
            } finally {
                backend.setCurrentKey(prevKey);
                if (prevNs != null) {
                    delegateState.setCurrentNamespace(prevNs);
                }
            }
        }
        return bytesFreed;
    }

    // Debug method to get cache statistics
    public String getCacheStats() {
        long hits = cacheHits.getCount();
        long misses = cacheMisses.getCount();
        long total = hits + misses;
        double hitRate = total > 0 ? (double) hits / total * 100 : 0.0;

        return String.format("CacheStats{hits=%d, misses=%d, hitRate=%.2f%%, bypassActivations=%d}",
                hits, misses, hitRate, cacheBypassActivations.getCount());
    }

    /**
     * A stable, serialized namespace key to avoid relying on object identity or mutable namespaces
     * for cache keying. Two logically equal namespaces map to the same StableNamespaceKey via
     * their serialized bytes.
     */
    private static final class StableNamespaceKey {
        private final byte[] serialized;

        private StableNamespaceKey(byte[] serialized) {
            this.serialized = serialized;
        }

        static <N> StableNamespaceKey fromNamespace(N namespace, TypeSerializer<N> namespaceSerializer) {
            try {
                DataOutputSerializer out = new DataOutputSerializer(64);
                namespaceSerializer.serialize(namespace, out);
                return new StableNamespaceKey(out.getCopyOfBuffer());
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize namespace for cache key", e);
            }
        }

        <N> N deserialize(TypeSerializer<N> namespaceSerializer) {
            try {
                DataInputDeserializer in = new DataInputDeserializer(serialized);
                return namespaceSerializer.deserialize(in);
            } catch (IOException e) {
                throw new RuntimeException("Failed to deserialize namespace from cache key", e);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || this.getClass() != o.getClass()) return false;
            StableNamespaceKey that = (StableNamespaceKey) o;
            if (this.serialized.length != that.serialized.length) return false;
            for (int i = 0; i < this.serialized.length; i++) {
                if (this.serialized[i] != that.serialized[i]) return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = 1;
            for (byte element : serialized) {
                result = 31 * result + element;
            }
            return result;
        }

        @Override
        public String toString() {
            return "StableNamespaceKey{" + serialized.length + "b}";
        }
    }

    private N namespaceKeyToNamespace(StableNamespaceKey key) {
        return key.deserialize(getNamespaceSerializer());
    }

    /**
     * Composite key for global caches: [ keyGroup:int32 | nsLen:int32 | nsBytes | keyBytes ].
     */
    private static final class CompositeKey {
        private final byte[] bytes;

        private CompositeKey(byte[] bytes) { this.bytes = bytes; }

        static <K, N> CompositeKey from(K key, int keyGroup, N ns,
                                        TypeSerializer<K> keySer,
                                        TypeSerializer<N> nsSer) {
            try {
                DataOutputSerializer out = new DataOutputSerializer(256);
                out.writeInt(keyGroup);
                DataOutputSerializer nsOut = new DataOutputSerializer(128);
                nsSer.serialize(ns, nsOut);
                byte[] nsBytes = nsOut.getCopyOfBuffer();
                out.writeInt(nsBytes.length);
                out.write(nsBytes);
                keySer.serialize(key, out);
                return new CompositeKey(out.getCopyOfBuffer());
            } catch (IOException e) {
                throw new RuntimeException("CompositeKey serialization failed", e);
            }
        }

        <K> K deserializeKey(TypeSerializer<K> keySer) {
            try {
                DataInputDeserializer in = new DataInputDeserializer(bytes);
                in.readInt(); // keyGroup
                int nsLen = in.readInt();
                in.skipBytesToRead(nsLen);
                return keySer.deserialize(in);
            } catch (IOException e) {
                throw new RuntimeException("CompositeKey key deserialization failed", e);
            }
        }

        <N> N deserializeNamespace(TypeSerializer<N> nsSer) {
            try {
                DataInputDeserializer in = new DataInputDeserializer(bytes);
                in.readInt(); // keyGroup
                int nsLen = in.readInt();
                byte[] nsBytes = new byte[nsLen];
                in.read(nsBytes);
                return nsSer.deserialize(new DataInputDeserializer(nsBytes));
            } catch (IOException e) {
                throw new RuntimeException("CompositeKey namespace deserialization failed", e);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || this.getClass() != o.getClass()) return false;
            CompositeKey that = (CompositeKey) o;
            if (this.bytes.length != that.bytes.length) return false;
            for (int i = 0; i < this.bytes.length; i++) {
                if (this.bytes[i] != that.bytes[i]) return false;
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

        @Override
        public String toString() { return "CompositeKey{" + bytes.length + "b}"; }
    }

    // No-op counter for when metrics group is absent
    private static class NoOpCounter implements Counter {
        @Override public void inc() {}
        @Override public void inc(long n) {}
        @Override public void dec() {}
        @Override public void dec(long n) {}
        @Override public long getCount() { return 0; }
    }
}
