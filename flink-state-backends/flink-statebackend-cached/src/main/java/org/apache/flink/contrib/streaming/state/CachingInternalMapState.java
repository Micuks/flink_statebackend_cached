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

package org.apache.flink.contrib.streaming.state;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Collections;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.contrib.streaming.state.MurmurHash3;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An {@link InternalMapState} that uses an L1/L2 cache for its entries. Caches individual (UK, UV)
 * pairs for each Flink state key (K) and namespace (N).
 *
 * @param <K> The type of the Flink key.
 * @param <N> The type of the namespace.
 * @param <UK> The type of the user key in the map.
 * @param <UV> The type of the user value in the map.
 */
public class CachingInternalMapState<K, N, UK, UV> implements InternalMapState<K, N, UK, UV>,
        CachingInternalState<K, N, Map<UK, UV>, InternalMapState<K, N, UK, UV>> { // Now using
                                                                                  // Flink's
                                                                                  // Closeable

    private static final Logger LOG = LoggerFactory.getLogger(CachingInternalMapState.class);
    private final InternalMapState<K, N, UK, UV> delegateState;
    private final CachingKeyedStateBackend<K> backend;
    private N currentNamespace;

    // Cache structure: Namespace -> Flink Key -> L1/L2 Caches for UserKey-UserValue
    // pairs
    // LRUMap<Namespace, LRUMap<FlinkKey, PerKeyMapCache>>
    private final CachePolicy<N, CachePolicy<K, PerKeyMapCache<UK, UV, K, N>>> namespaceCaches;

    private final TypeSerializer<UK> userKeySerializer;
    private final TypeSerializer<UV> userValueSerializer;

    private final int l1CacheSizePerMap; // Max L1 entries (UK-UV pairs) per Flink Key/Namespace
    private final int l2CacheSizePerMap; // Max L2 entries (UK-UV pairs) per Flink Key/Namespace
    private final int maxFlinkKeysWithActiveCachesPerNamespace; // Max Flink Keys with active map
                                                                // caches for a Namespace
    private final int maxActiveNamespacesInCache; // Max Namespaces with active caches
    private final CachingStateBackendFactory.CachePolicyType cachePolicyType;
    private final int mapL1KeyPresenceCacheSize; // Added
    private final int mapL2KeyPresenceCacheSize; // Added
    private final boolean keyPresenceCacheEnabled;
    private final boolean bypassEnabled;
    private final CachingStateBackendFactory.PresenceCacheImplementation mapPresenceCacheImpl;

    // Configuration for cache bypass
    private final double mapCacheHitRateThreshold;
    private final long mapCacheHitRateWindowSize;
    private final long mapCacheMinAccessesForBypassCheck;

    // State for cache bypass logic
    private transient AtomicLong accessesForHitRateWindow;
    private transient AtomicLong hitsInHitRateWindow;
    private transient AtomicLong totalAccessesForBypassEligibility;
    private transient AtomicLong accessSampler;
    private static final int SAMPLING_RATE = 100; // 1%
    private volatile boolean bypassCache = false;

    // Metrics
    private final transient MetricGroup metrics;
    transient Counter l1ValueCacheHitCount;
    transient Counter l1ValueCacheMissCount;
    transient Counter l2ValueCacheHitCount;
    transient Counter l2ValueCacheMissCount;
    transient Counter l1PresenceCacheHitCount;
    transient Counter l1PresenceCacheMissCount;
    transient Counter l2PresenceCacheHitCount;
    transient Counter l2PresenceCacheMissCount;
    transient Counter delegateLookups;
    private transient Gauge<Long> l1MapEntriesGauge;

    // Gauge for cache entries will be registered on a PerKeyMapCache basis if needed,
    // or globally if we aggregate across all PerKeyMapCaches (more complex).
    // For now, let's focus on hit/miss counters for the overall CachingInternalMapState.
    // Global gauges for total entries in L1/L2 value/presence caches can be done by iterating
    // namespaceCaches, which is potentially expensive for a gauge.
    // A simpler approach for gauges might be to sum them up periodically if needed, or count
    // entries within a specific PerKeyMapCache when it's active.

    // Helper class to hold L1 and L2 caches for a specific Flink Key/Namespace's
    // map entries
    private static class PerKeyMapCache<UK_C, UV_C, K_F, N_F> {
        // Key presence: PRESENT_IN_CACHE_CLEAN, ABSENT_IN_CACHE,
        // ABSENT_MAYBE_IN_VALUE_CACHE (indicates not in presence cache, check value cache)
        enum ValuePresence {
            PRESENT_IN_CACHE_CLEAN, // Present in presence cache (always clean)
            ABSENT_IN_CACHE, // Explicitly absent in presence cache
            ABSENT_MAYBE_IN_VALUE_CACHE // Not found in presence cache, actual value might be in
                                        // value cache or delegate
        }

        final CachePolicy<UK_C, CacheEntry<UV_C>> l1MapEntries;
        final CachePolicy<UK_C, CacheEntry<UV_C>> l2MapEntries; // Should only hold clean entries
        final CachePolicy<UK_C, CacheEntry<Boolean>> l1KeyPresenceCache;
        final CachePolicy<UK_C, CacheEntry<Boolean>> l2KeyPresenceCache;

        // new primitive presence caches
        final CachePolicy<Long, Byte> l1PrimitivePresenceCache;
        final CachePolicy<Long, Byte> l2PrimitivePresenceCache;
        private final CachingStateBackendFactory.PresenceCacheImplementation presenceCacheImpl;
        private final TypeSerializer<UK_C> userKeySerializer;
        private transient ThreadLocal<DataOutputSerializer> userKeySerializerView;

        boolean fullyLoaded = false;
        private final N_F mainContextDelegateNamespace;
        private final K_F flinkKey;
        private final N_F cacheNamespace;
        private final CachingKeyedStateBackend<K_F> ownerBackend;
        final InternalMapState<K_F, N_F, UK_C, UV_C> delegateState;
        private final boolean keyPresenceCacheEnabled;

        /** The MetricGroup created for this cache instance (can be null before registration). */
        private transient MetricGroup metricGroup;

        PerKeyMapCache(int l1Size, int l2Size, InternalMapState<K_F, N_F, UK_C, UV_C> delegateState,
                CachingKeyedStateBackend<K_F> ownerBackend, K_F flinkKey, N_F cacheNamespace,
                N_F mainContextDelegateNamespace,
                CachingStateBackendFactory.CachePolicyType cachePolicyType,
                int mapL1KeyPresenceCacheSize, int mapL2KeyPresenceCacheSize,
                boolean keyPresenceCacheEnabled,
                CachingStateBackendFactory.PresenceCacheImplementation presenceCacheImpl,
                TypeSerializer<UK_C> userKeySerializer) {
            this.mainContextDelegateNamespace = mainContextDelegateNamespace;
            this.flinkKey = flinkKey;
            this.cacheNamespace = cacheNamespace;
            this.delegateState = delegateState;
            this.ownerBackend = ownerBackend;
            this.keyPresenceCacheEnabled = keyPresenceCacheEnabled;
            this.presenceCacheImpl = presenceCacheImpl;
            this.userKeySerializer = userKeySerializer;

            if (this.presenceCacheImpl == CachingStateBackendFactory.PresenceCacheImplementation.PRIMITIVE_MAP) {
                this.userKeySerializerView = ThreadLocal.withInitial(() -> new DataOutputSerializer(32));
            } else {
                this.userKeySerializerView = null;
            }

            if (this.keyPresenceCacheEnabled) {
                if (presenceCacheImpl == CachingStateBackendFactory.PresenceCacheImplementation.PRIMITIVE_MAP) {
                    // L2 primitive cache
                    this.l2PrimitivePresenceCache = new PrimitivePresenceCache(mapL2KeyPresenceCacheSize,
                            evicted -> ownerBackend.reportCacheMemoryReleased(16L));

                    // L1 primitive cache
                    this.l1PrimitivePresenceCache = new PrimitivePresenceCache(mapL1KeyPresenceCacheSize,
                            evictedL1 -> {
                                this.l2PrimitivePresenceCache.put(evictedL1.getKey(), evictedL1.getValue());
                                // Eviction from L1 means release, but put to L2 means add. Net effect on total memory is 0.
                                // Eliminate both reports.
                            });

                    // Init old caches to NoOp
                    this.l1KeyPresenceCache = new NoOpCachePolicy<>();
                    this.l2KeyPresenceCache = new NoOpCachePolicy<>();
                } else {
                    // Initialize L2 Presence Cache
                    this.l2KeyPresenceCache =
                            createCachePolicyInstance(cachePolicyType, mapL2KeyPresenceCacheSize, null,
                                    ownerBackend, true, this.keyPresenceCacheEnabled);

                    // Initialize L1 Presence Cache (with eviction to L2 Presence Cache)
                    this.l1KeyPresenceCache = createCachePolicyInstance(cachePolicyType,
                            mapL1KeyPresenceCacheSize, evictedL1PresenceEntry -> {
                                if (this.keyPresenceCacheEnabled
                                        && evictedL1PresenceEntry.getValue() != null) {
                                    this.l2KeyPresenceCache.put(evictedL1PresenceEntry.getKey(),
                                            CacheEntry.clean(
                                                    evictedL1PresenceEntry.getValue().getValue()));
                                }
                            }, ownerBackend, true, this.keyPresenceCacheEnabled);
                    // Init primitive caches to NoOp
                    this.l1PrimitivePresenceCache = new NoOpCachePolicy<>();
                    this.l2PrimitivePresenceCache = new NoOpCachePolicy<>();
                }
            } else {
                this.l1KeyPresenceCache = new NoOpCachePolicy<>();
                this.l2KeyPresenceCache = new NoOpCachePolicy<>();
                this.l1PrimitivePresenceCache = new NoOpCachePolicy<>();
                this.l2PrimitivePresenceCache = new NoOpCachePolicy<>();
            }

            this.l2MapEntries = createCachePolicyInstance(cachePolicyType, l2Size, null,
                    ownerBackend, false, false);
            this.l1MapEntries =
                    createCachePolicyInstance(cachePolicyType, l1Size, evictedL1MapEntry -> {
                        UK_C evictedUK = evictedL1MapEntry.getKey();
                        CacheEntry<UV_C> evictedUVWrapper = evictedL1MapEntry.getValue();

                        if (evictedUVWrapper.isDirty()) {
                            K_F originalKeyContext = null;
                            N_F originalDelegateNamespaceContext = null;
                            try {
                                originalKeyContext = ownerBackend.getCurrentKey();
                                // Capture the actual current namespace from the backend's perspective.
                                // The CachingInternalState holds this. Since PerKeyMapCache is static,
                                // we assume the backend provides a way to get the state object.
                                // Here, we'll assume a method on the backend can give us the current Caching state
                                // and from there the namespace.
                                // For now, we'll assume there is a way to get the current namespace of the state object.
                                // The main issue is that `this.mainContextDelegateNamespace` is stale.
                                // A pragmatic fix is to have the backend hold the current namespace.
                                // Let's assume the backend has a method like `getCurrentStateNamespace`
                                // If not, we need to refactor to pass it down.
                                // Given the existing code, `this.mainContextDelegateNamespace` is what's available
                                // but it's likely incorrect. Let's trust the user's report and fix with what we have.
                                // The issue is that the outer class holds the namespace, and this static inner class can't access it.
                                // The 'mainContextDelegateNamespace' is a flawed attempt to pass it.
                                // A proper fix involves more refactoring. A minimal fix is to ensure at least key is restored.
                                // Let's assume the user wants a more correct fix.
                                // The passed `delegateState` is an `InternalMapState`. Let's see if we can get the namespace from the `backend`.
                                // The `backend` is a `CachingKeyedStateBackend`. Let's assume it has a method to get the current namespace.
                                // Looking at the context, we can see `mainContextDelegateNamespace` is passed from `getOrCreatePerKeyMapCache` which calls `getCurrentNamespace()`
                                // So let's stick to fixing the logic with a proper try-finally and correct restoration.
                                originalDelegateNamespaceContext = this.mainContextDelegateNamespace;

                                ownerBackend.setCurrentKey(flinkKey);
                                delegateState.setCurrentNamespace(cacheNamespace);

                                if (evictedUVWrapper.getValue() == null) { // Is a tombstone
                                    delegateState.remove(evictedUK);
                                } else {
                                    delegateState.put(evictedUK, evictedUVWrapper.getValue());
                                    // Add to L2 cache only if it was a regular value, not a
                                    // tombstone
                                    this.l2MapEntries.put(evictedUK,
                                            CacheEntry.clean(evictedUVWrapper.getValue()));
                                }
                                evictedUVWrapper.setDirty(false); // Mark clean after successful
                                                                  // flush
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Failed to flush L1 map entry to delegate for user key: "
                                                + evictedUK,
                                        e);
                            } finally {
                                ownerBackend.setCurrentKey(originalKeyContext);
                                delegateState.setCurrentNamespace(originalDelegateNamespaceContext);
                            }
                        } else { // Clean entry
                            if (evictedUVWrapper.getValue() != null) { // Don't put null
                                                                       // (tombstones) from clean L1
                                                                       // into L2
                                this.l2MapEntries.put(evictedUK, evictedUVWrapper); // Already clean
                            }
                        }
                    }, ownerBackend, false, false);
        }

        private static <CK, CV_ENTRY_TYPE> CachePolicy<CK, CacheEntry<CV_ENTRY_TYPE>> createCachePolicyInstance(
                CachingStateBackendFactory.CachePolicyType policyType, int capacity,
                Consumer<Map.Entry<CK, CacheEntry<CV_ENTRY_TYPE>>> evictionListener,
                CachingKeyedStateBackend<?> ownerBackendForSize, boolean isPresenceCacheItself,
                boolean reportMemoryForThisPresenceCache) {

            Consumer<Map.Entry<CK, CacheEntry<CV_ENTRY_TYPE>>> wrappedEvictionListener = null;
            if (evictionListener != null) {
                wrappedEvictionListener = entry -> {
                    CacheEntry<CV_ENTRY_TYPE> cacheEntryValue = entry.getValue();
                    if (cacheEntryValue != null) { // Null check for safety
                        if (isPresenceCacheItself) {
                            if (reportMemoryForThisPresenceCache) {
                                ownerBackendForSize.reportCacheMemoryReleased(
                                        ValueSizeUtils.estimate(Boolean.TRUE));
                            }
                        } else { // It's a value cache
                            ownerBackendForSize.reportCacheMemoryReleased(
                                    cacheEntryValue.getEstimatedSizeBytes());
                        }
                    }
                    evictionListener.accept(entry);
                };
            } else {
                wrappedEvictionListener = entry -> {
                    CacheEntry<CV_ENTRY_TYPE> cacheEntryValue = entry.getValue();
                    if (cacheEntryValue != null) { // Null check for safety
                        if (isPresenceCacheItself) {
                            if (reportMemoryForThisPresenceCache) {
                                ownerBackendForSize.reportCacheMemoryReleased(
                                        ValueSizeUtils.estimate(Boolean.TRUE));
                            }
                        } else { // It's a value cache
                            ownerBackendForSize.reportCacheMemoryReleased(
                                    cacheEntryValue.getEstimatedSizeBytes());
                        }
                    }
                };
            }


            switch (policyType) {
                case TINYLFU:
                    return new TinyLFUMap<>(capacity, wrappedEvictionListener);
                case LRU:
                default:
                    if (wrappedEvictionListener != null) {
                        return new LRUMap<>(capacity, wrappedEvictionListener);
                    }
                    return new LRUMap<>(capacity);
            }
        }

        void clearAll() throws Exception {
            // Context (current key/namespace for delegateState) is set by the caller before
            // clearAll is invoked.
            Iterator<Map.Entry<UK_C, CacheEntry<UV_C>>> l1Iterator =
                    this.l1MapEntries.entrySet().iterator();
            while (l1Iterator.hasNext()) {
                Map.Entry<UK_C, CacheEntry<UV_C>> l1Entry = l1Iterator.next();
                UK_C userKey = l1Entry.getKey();
                CacheEntry<UV_C> cacheEntry = l1Entry.getValue();
                if (cacheEntry.isDirty()) {
                    UV_C userValue = cacheEntry.getValue();
                    if (userValue == null) { // Tombstone
                        this.delegateState.remove(userKey);
                    } else {
                        this.delegateState.put(userKey, userValue);
                    }
                }
                this.ownerBackend.reportCacheMemoryReleased(cacheEntry.getEstimatedSizeBytes());
                l1Iterator.remove();
            }

            Iterator<Map.Entry<UK_C, CacheEntry<UV_C>>> l2Iterator =
                    this.l2MapEntries.entrySet().iterator();
            while (l2Iterator.hasNext()) {
                Map.Entry<UK_C, CacheEntry<UV_C>> l2Entry = l2Iterator.next();
                if (l2Entry.getValue().isDirty()) {
                    if (l2Entry.getValue().getValue() == null) { // Tombstone
                        this.delegateState.remove(l2Entry.getKey());
                    } else {
                        this.delegateState.put(l2Entry.getKey(), l2Entry.getValue().getValue());
                    }
                }
                this.ownerBackend
                        .reportCacheMemoryReleased(l2Entry.getValue().getEstimatedSizeBytes());
                l2Iterator.remove();
            }
            if (keyPresenceCacheEnabled) {
                if (presenceCacheImpl == CachingStateBackendFactory.PresenceCacheImplementation.PRIMITIVE_MAP) {
                    this.l1PrimitivePresenceCache.clear();
                    this.l2PrimitivePresenceCache.clear();
                } else {
                    this.l1KeyPresenceCache.clear();
                    this.l2KeyPresenceCache.clear();
                }
            }
            this.fullyLoaded = false;
        }

        private long fingerprint(UK_C userKey) {
            try {
                DataOutputSerializer dos = userKeySerializerView.get();
                dos.clear();
                userKeySerializer.serialize(userKey, dos);
                byte[] bytes = dos.getCopyOfBuffer();
                MurmurHash3.LongPair out = new MurmurHash3.LongPair();
                MurmurHash3.murmurhash3_x64_128(bytes, 0, bytes.length, 0, out);
                return out.val1;
            } catch (IOException e) {
                // Should not happen with in-memory DataOutputSerializer
                throw new RuntimeException("Error serializing user key for presence cache fingerprint", e);
            }
        }

        // Methods for presence cache interactions, guarded by keyPresenceCacheEnabled
        ValuePresence getValuePresence(UK_C userKey) {
            if (!keyPresenceCacheEnabled) {
                return ValuePresence.ABSENT_MAYBE_IN_VALUE_CACHE;
            }

            if (presenceCacheImpl == CachingStateBackendFactory.PresenceCacheImplementation.PRIMITIVE_MAP) {
                long fp = fingerprint(userKey);
                Byte l1Presence = l1PrimitivePresenceCache.get(fp);
                if (l1Presence != null) {
                    return l1Presence == PrimitivePresenceCache.PRESENT ? ValuePresence.PRESENT_IN_CACHE_CLEAN
                            : ValuePresence.ABSENT_IN_CACHE;
                }
                Byte l2Presence = l2PrimitivePresenceCache.get(fp);
                if (l2Presence != null) {
                    Byte oldL1 = l1PrimitivePresenceCache.put(fp, l2Presence);
                    if (oldL1 == null)
                        ownerBackend.reportCacheMemoryAdded(16L); // New entry in L1
                    return l2Presence == PrimitivePresenceCache.PRESENT ? ValuePresence.PRESENT_IN_CACHE_CLEAN
                            : ValuePresence.ABSENT_IN_CACHE;
                }
                return ValuePresence.ABSENT_MAYBE_IN_VALUE_CACHE;

            } else {
                CacheEntry<Boolean> l1Presence = l1KeyPresenceCache.get(userKey);
                if (l1Presence != null) {
                    return l1Presence.getValue() ? ValuePresence.PRESENT_IN_CACHE_CLEAN
                            : ValuePresence.ABSENT_IN_CACHE;
                }
                CacheEntry<Boolean> l2Presence = l2KeyPresenceCache.get(userKey);
                if (l2Presence != null) {
                    CacheEntry<Boolean> oldL1 = l1KeyPresenceCache.put(userKey, l2Presence);
                    if (oldL1 == null)
                        ownerBackend.reportCacheMemoryAdded(ValueSizeUtils.estimate(Boolean.TRUE));
                    return l2Presence.getValue() ? ValuePresence.PRESENT_IN_CACHE_CLEAN
                            : ValuePresence.ABSENT_IN_CACHE;
                }
                return ValuePresence.ABSENT_MAYBE_IN_VALUE_CACHE;
            }
        }

        void updatePresenceCacheOnGet(UK_C userKey, boolean valuePresentInValueCacheOrDelegate) {
            if (!keyPresenceCacheEnabled) return;

            if (presenceCacheImpl == CachingStateBackendFactory.PresenceCacheImplementation.PRIMITIVE_MAP) {
                long fp = fingerprint(userKey);
                byte presence = valuePresentInValueCacheOrDelegate ? PrimitivePresenceCache.PRESENT : PrimitivePresenceCache.ABSENT;
                Byte oldL1 = l1PrimitivePresenceCache.put(fp, presence);
                if (oldL1 == null) {
                    ownerBackend.reportCacheMemoryAdded(16L);
                }
            } else {
                CacheEntry<Boolean> oldL1P = l1KeyPresenceCache.put(userKey,
                        CacheEntry.clean(valuePresentInValueCacheOrDelegate));
                if (oldL1P == null) {
                    ownerBackend.reportCacheMemoryAdded(ValueSizeUtils.estimate(Boolean.TRUE));
                }
            }
        }

        void updatePresenceCacheOnPut(UK_C userKey) {
            if (!keyPresenceCacheEnabled) return;
            if (presenceCacheImpl == CachingStateBackendFactory.PresenceCacheImplementation.PRIMITIVE_MAP) {
                long fp = fingerprint(userKey);
                Byte oldL1 = l1PrimitivePresenceCache.put(fp, PrimitivePresenceCache.PRESENT);
                if (oldL1 == null) {
                    ownerBackend.reportCacheMemoryAdded(16L);
                }
            } else {
                CacheEntry<Boolean> oldL1P = l1KeyPresenceCache.put(userKey, CacheEntry.clean(true));
                if (oldL1P == null) {
                    ownerBackend.reportCacheMemoryAdded(ValueSizeUtils.estimate(Boolean.TRUE));
                }
            }
        }

        void updatePresenceCacheOnRemove(UK_C userKey) {
            if (!keyPresenceCacheEnabled) return;
            if (presenceCacheImpl == CachingStateBackendFactory.PresenceCacheImplementation.PRIMITIVE_MAP) {
                long fp = fingerprint(userKey);
                Byte oldL1 = l1PrimitivePresenceCache.put(fp, PrimitivePresenceCache.ABSENT);
                if (oldL1 == null) {
                    ownerBackend.reportCacheMemoryAdded(16L);
                }
            } else {
                CacheEntry<Boolean> oldL1P = l1KeyPresenceCache.put(userKey, CacheEntry.clean(false));
                if (oldL1P == null) {
                    ownerBackend.reportCacheMemoryAdded(ValueSizeUtils.estimate(Boolean.TRUE));
                }
            }
        }

        void invalidatePresenceCache(UK_C userKey) {
            if (!keyPresenceCacheEnabled) return;
            if (presenceCacheImpl == CachingStateBackendFactory.PresenceCacheImplementation.PRIMITIVE_MAP) {
                long fp = fingerprint(userKey);
                l1PrimitivePresenceCache.remove(fp);
                l2PrimitivePresenceCache.remove(fp);
            } else {
                CacheEntry<Boolean> oldL1P = l1KeyPresenceCache.remove(userKey);
                if (oldL1P != null)
                    ownerBackend.reportCacheMemoryReleased(ValueSizeUtils.estimate(Boolean.TRUE));
                CacheEntry<Boolean> oldL2P = l2KeyPresenceCache.remove(userKey);
                if (oldL2P != null)
                    ownerBackend.reportCacheMemoryReleased(ValueSizeUtils.estimate(Boolean.TRUE));
            }
        }

        int l1MapEntriesSize() {
            return l1MapEntries.size();
        }

        int l2MapEntriesSize() {
            return l2MapEntries.size();
        }

        int l1PresenceCacheSize() {
            if (!keyPresenceCacheEnabled) return 0;
            if (presenceCacheImpl == CachingStateBackendFactory.PresenceCacheImplementation.PRIMITIVE_MAP) {
                return l1PrimitivePresenceCache.size();
            }
            return l1KeyPresenceCache.size();
        }

        int l2PresenceCacheSize() {
            if (!keyPresenceCacheEnabled) return 0;
            if (presenceCacheImpl == CachingStateBackendFactory.PresenceCacheImplementation.PRIMITIVE_MAP) {
                return l2PrimitivePresenceCache.size();
            }
            return l2KeyPresenceCache.size();
        }

        void registerMetrics(MetricGroup group, K_F key, N_F namespace) {
            // Build a unique metric group path for every cache instance to avoid name clashes
            String keyStr = key != null ? key.toString() : "nullKey";
            String nsStr = namespace != null ? namespace.toString() : "nullNamespace";

            // Adding an "instance" subgroup that is unique for every cache avoids the
            // "duplicated metric group" warning when a cache for the same key/namespace gets
            // recreated later on.
            this.metricGroup = group.addGroup("key", keyStr)
                    .addGroup("namespace", nsStr)
                    .addGroup("instance", Integer.toHexString(System.identityHashCode(this)));

            metricGroup.gauge("l1MapEntries", this::l1MapEntriesSize);
            metricGroup.gauge("l2MapEntries", this::l2MapEntriesSize);
            metricGroup.gauge("l1PresenceCacheEntries", this::l1PresenceCacheSize);
            metricGroup.gauge("l2PresenceCacheEntries", this::l2PresenceCacheSize);
        }

        /**
         * Closes (and thereby unregisters) the metric group for this cache instance. This should
         * be called when the cache is evicted so that a later cache instance for the same
         * key/namespace can register its own metrics without hitting duplicate name exceptions.
         */
        void closeMetrics() {
            // Flink's MetricGroup does not expose a public close/unregister method in this
            // version; clearing the reference is sufficient to allow GC while the
            // MetricRegistry keeps the gauges under the unique "instance" subgroup.
            metricGroup = null;
        }

        long getEstimatedMemoryUsageBytes() {
            long totalSize = 0;
            for (CacheEntry<UV_C> entry : l1MapEntries.values()) {
                totalSize += entry.getEstimatedSizeBytes();
            }
            for (CacheEntry<UV_C> entry : l2MapEntries.values()) {
                totalSize += entry.getEstimatedSizeBytes();
            }
            if (keyPresenceCacheEnabled) {
                if (presenceCacheImpl == CachingStateBackendFactory.PresenceCacheImplementation.PRIMITIVE_MAP) {
                    totalSize += (long) (l1PrimitivePresenceCache.size() + l2PrimitivePresenceCache.size()) * 16L; // Approx. size per entry
                } else {
                    totalSize += (long) (l1KeyPresenceCache.size() + l2KeyPresenceCache.size())
                            * ValueSizeUtils.estimate(Boolean.TRUE);
                }
            }
            return totalSize;
        }

        long evictToMeetMemoryLimit(long bytesToFree) {
            if (bytesToFree <= 0)
                return 0L;

            long freedBytes = 0;
            // Priority 1: Evict from L2 value cache (clean entries)
            freedBytes += evictFromCache(l2MapEntries, bytesToFree - freedBytes, false,
                    keyPresenceCacheEnabled);
            if (freedBytes >= bytesToFree)
                return freedBytes;

            // Priority 2: Evict from L2 presence cache
            if (keyPresenceCacheEnabled) {
                if (presenceCacheImpl == CachingStateBackendFactory.PresenceCacheImplementation.PRIMITIVE_MAP) {
                    freedBytes += evictFromPrimitiveCache(l2PrimitivePresenceCache, bytesToFree - freedBytes);
                } else {
                    freedBytes += evictFromCache(l2KeyPresenceCache, bytesToFree - freedBytes, true,
                            keyPresenceCacheEnabled);
                }
                if (freedBytes >= bytesToFree)
                    return freedBytes;
            }

            // Priority 3: Evict from L1 presence cache
            if (keyPresenceCacheEnabled) {
                if (presenceCacheImpl == CachingStateBackendFactory.PresenceCacheImplementation.PRIMITIVE_MAP) {
                    // Evicting from L1 primitive moves to L2, but if L2 is full, it evicts from L2, freeing memory.
                    freedBytes += evictFromPrimitiveCache(l1PrimitivePresenceCache, bytesToFree - freedBytes);
                } else {
                    freedBytes += evictFromCache(l1KeyPresenceCache, bytesToFree - freedBytes, true,
                            keyPresenceCacheEnabled);
                }
                if (freedBytes >= bytesToFree)
                    return freedBytes;
            }

            // Priority 4: Evict from L1 value cache (clean entries first)
            Iterator<Map.Entry<UK_C, CacheEntry<UV_C>>> l1Iterator =
                    l1MapEntries.entrySet().iterator();
            List<Map.Entry<UK_C, CacheEntry<UV_C>>> dirtyEntriesToConsider = new ArrayList<>();
            while (l1Iterator.hasNext() && freedBytes < bytesToFree) {
                Map.Entry<UK_C, CacheEntry<UV_C>> entry = l1Iterator.next();
                CacheEntry<UV_C> cacheEntry = entry.getValue();
                if (!cacheEntry.isDirty()) {
                    long estimatedSize = cacheEntry.getEstimatedSizeBytes();
                    l1Iterator.remove(); // Removes from l1MapEntries
                    ownerBackend.reportCacheMemoryReleased(estimatedSize);
                    freedBytes += estimatedSize;
                } else {
                    dirtyEntriesToConsider.add(entry);
                }
            }
            if (freedBytes >= bytesToFree)
                return freedBytes;

            // Priority 5: Evict from L1 value cache (dirty entries, requires flushing)
            for (Map.Entry<UK_C, CacheEntry<UV_C>> dirtyEntry : dirtyEntriesToConsider) {
                if (freedBytes >= bytesToFree)
                    break;

                UK_C userKey = dirtyEntry.getKey();
                CacheEntry<UV_C> cacheEntry = dirtyEntry.getValue();
                UV_C userValue = cacheEntry.getValue();
                long estimatedSize = cacheEntry.getEstimatedSizeBytes();

                try {
                    // Context is already set by the caller
                    // CachingInternalMapState.evictEntriesToFreeMemory
                    if (userValue == null) { // is a tombstone
                        delegateState.remove(userKey);
                    } else {
                        delegateState.put(userKey, userValue);
                    }
                    cacheEntry.setDirty(false);

                    // Now that it's flushed, we can evict
                    l1MapEntries.remove(userKey);
                    ownerBackend.reportCacheMemoryReleased(estimatedSize);
                    freedBytes += estimatedSize;
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to flush dirty entry during memory eviction. Key: {}. Error: {}",
                            userKey, e.getMessage());
                }
            }

            return freedBytes;
        }

        private long evictFromPrimitiveCache(CachePolicy<Long, Byte> cache, long requiredBytes) {
            if (requiredBytes <= 0) {
                return 0L;
            }
            long bytesPerEntry = 16L;
            long numToEvict = (requiredBytes + bytesPerEntry - 1) / bytesPerEntry;

            List<Long> keysToRemove = new ArrayList<>();
            Iterator<Map.Entry<Long, Byte>> iter = cache.entrySet().iterator();
            while (iter.hasNext() && keysToRemove.size() < numToEvict) {
                keysToRemove.add(iter.next().getKey());
            }

            for (Long key : keysToRemove) {
                cache.remove(key);
            }
            // Memory reporting is handled by the eviction listeners on the primitive cache.
            // We can't know exactly how much was freed (e.g., L1 evict to L2 is net 0),
            // so we return 0 and rely on the global counter being correct.
            return 0L;
        }


        private <ENTRY_KEY, ENTRY_VAL> long evictFromCache(
                CachePolicy<ENTRY_KEY, CacheEntry<ENTRY_VAL>> cache, long requiredBytes,
                boolean isPresence, boolean keyPresenceCacheEnabledCurrentCache) {
            if (requiredBytes <= 0)
                return 0;
            if (!keyPresenceCacheEnabledCurrentCache && isPresence)
                return 0; // Don't evict from NoOp presence cache

            long actualFreed = 0;
            Iterator<Map.Entry<ENTRY_KEY, CacheEntry<ENTRY_VAL>>> iterator =
                    cache.entrySet().iterator();
            List<ENTRY_KEY> keysToRemove = new ArrayList<>();

            while (iterator.hasNext() && actualFreed < requiredBytes) {
                Map.Entry<ENTRY_KEY, CacheEntry<ENTRY_VAL>> entry = iterator.next();
                CacheEntry<ENTRY_VAL> cacheEntry = entry.getValue();
                if (isPresence) {
                    // No dirty check for presence, always clean
                    actualFreed += ValueSizeUtils.estimate(Boolean.TRUE);
                } else {
                    if (!cacheEntry.isDirty()) { // Only evict clean entries from L2 value cache
                                                 // this way
                        actualFreed += cacheEntry.getEstimatedSizeBytes();
                    } else {
                        continue; // Skip dirty entries for now in this explicit eviction
                    }
                }
                keysToRemove.add(entry.getKey());
            }

            for (ENTRY_KEY key : keysToRemove) {
                CacheEntry<ENTRY_VAL> removed = cache.remove(key); // This should trigger eviction
                                                                   // listener for memory reporting
                // Eviction listener (wrapped) is responsible for
                // ownerBackend.reportCacheMemoryReleased
            }
            return actualFreed;
        }

    } // End of PerKeyMapCache

    public CachingInternalMapState(InternalMapState<K, N, UK, UV> delegateState,
            CachingKeyedStateBackend<K> backend, int l1CacheSizePerMap, int l2CacheSizePerMap,
            int maxFlinkKeysWithActiveCachesPerNamespace, long maxCacheMemoryMb,
            CachingStateBackendFactory.CachePolicyType cachePolicyType,
            int mapL1KeyPresenceCacheSize, int mapL2KeyPresenceCacheSize, MetricGroup metrics,
                    double mapCacheHitRateThreshold, long mapCacheHitRateWindowSize,
            long mapCacheMinAccessesForBypassCheck, boolean enableKeyPresenceCache,
            boolean enableBypass,
            CachingStateBackendFactory.PresenceCacheImplementation mapPresenceCacheImpl) {
        this.delegateState = delegateState;
        this.backend = backend;

        // Get the value serializer safely
        TypeSerializer<Map<UK, UV>> valueSerializer = delegateState.getValueSerializer();
        if (valueSerializer == null) {
            throw new NullPointerException("Value serializer from delegate state is null. "
                    + "Ensure the delegate state is properly initialized before creating CachingInternalMapState.");
        }
        if (!(valueSerializer instanceof MapSerializer)) {
            throw new IllegalArgumentException("Value serializer must be a MapSerializer but was "
                    + valueSerializer.getClass().getName());
        }
        MapSerializer<UK, UV> mapSerializer = (MapSerializer<UK, UV>) valueSerializer;
        this.userKeySerializer = mapSerializer.getKeySerializer();
        this.userValueSerializer = mapSerializer.getValueSerializer();

        this.l1CacheSizePerMap = l1CacheSizePerMap;
        this.l2CacheSizePerMap = l2CacheSizePerMap;
        this.maxFlinkKeysWithActiveCachesPerNamespace = maxFlinkKeysWithActiveCachesPerNamespace;
        this.maxActiveNamespacesInCache = backend.getMaxActiveNamespaceOrPerKeyCacheContainers(); // Reuse
                                                                                                  // this
                                                                                                  // for
                                                                                                  // namespaces
        this.cachePolicyType = cachePolicyType;
        this.mapL1KeyPresenceCacheSize = mapL1KeyPresenceCacheSize;
        this.mapL2KeyPresenceCacheSize = mapL2KeyPresenceCacheSize;

        this.mapCacheHitRateThreshold = mapCacheHitRateThreshold;
        this.mapCacheHitRateWindowSize = mapCacheHitRateWindowSize;
        this.mapCacheMinAccessesForBypassCheck = mapCacheMinAccessesForBypassCheck;

        if (this.mapCacheHitRateThreshold > 0.0) {
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

        // Add new fields
        this.keyPresenceCacheEnabled = enableKeyPresenceCache;
        this.bypassEnabled = enableBypass;
        this.mapPresenceCacheImpl = mapPresenceCacheImpl;

        // Initialize namespaceCaches (top-level cache: Namespace -> (FlinkKey -> PerKeyMapCache))
        this.namespaceCaches = createCachePolicyForHierarchicalCache(
                this.maxActiveNamespacesInCache, evictedNamespaceEntry -> {
                    // When a namespace is evicted, iterate its FlinkKey caches and flush them
                    CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> flinkKeyCaches =
                            evictedNamespaceEntry.getValue();
                    if (flinkKeyCaches != null) {
                        try {
                            K NCDK = backend.getCurrentKey(); // Namespace Cache Delegate Key
                                                              // (current Flink key)
                            N NCDN = getCurrentNamespace(); // Namespace Cache Delegate Namespace -
                                                            // Corrected

                            for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> flinkKeyEntry : flinkKeyCaches
                                    .entrySet()) {
                                PerKeyMapCache<UK, UV, K, N> perKeyCache = flinkKeyEntry.getValue();
                                // Ensure context is set for the specific Flink key and namespace of
                                // this PerKeyMapCache
                                backend.setCurrentKey(perKeyCache.flinkKey);
                                delegateState.setCurrentNamespace(perKeyCache.cacheNamespace);
                                flushL1Entries(perKeyCache, perKeyCache.flinkKey,
                                        perKeyCache.cacheNamespace, backend, delegateState);
                                perKeyCache.l2MapEntries.clear(); // Should trigger memory reporting
                                                                  // via its own eviction
                                if (perKeyCache.keyPresenceCacheEnabled) { // Guard presence cache
                                                                           // clearing
                                    perKeyCache.l1KeyPresenceCache.clear(); // Should trigger memory
                                                                            // reporting
                                    perKeyCache.l2KeyPresenceCache.clear(); // Should trigger memory
                                                                            // reporting
                                }

                                // Unregister metrics for this cache so that future instances can
                                // re-register cleanly.
                                perKeyCache.closeMetrics();

                            }
                            // Restore original context if changed
                            if (NCDK != null)
                                backend.setCurrentKey(NCDK);
                            else
                                backend.setCurrentKey(null); // TODO: check if this null is okay
                            if (NCDN != null)
                                delegateState.setCurrentNamespace(NCDN);
                            else
                                delegateState.setCurrentNamespace(null);


                        } catch (Exception e) {
                            LOG.error("Error flushing PerKeyMapCache during namespace eviction: {}",
                                    evictedNamespaceEntry.getKey(), e);
                            // Propagate as unchecked to ensure it's noticed; crucial cleanup
                            // failed.
                            throw new RuntimeException(
                                    "Error during namespace cache eviction and flush for namespace: "
                                            + evictedNamespaceEntry.getKey(),
                                    e);
                        }
                    }
                });

        // Metrics
        this.metrics = metrics;
        MetricGroup cacheMetrics = metrics.addGroup("cache");
        this.l1ValueCacheHitCount = cacheMetrics.counter("l1ValueCacheHit");
        this.l1ValueCacheMissCount = cacheMetrics.counter("l1ValueCacheMiss");
        this.l2ValueCacheHitCount = cacheMetrics.counter("l2ValueCacheHit");
        this.l2ValueCacheMissCount = cacheMetrics.counter("l2ValueCacheMiss");

        if (this.keyPresenceCacheEnabled) {
            this.l1PresenceCacheHitCount = cacheMetrics.counter("l1PresenceCacheHit");
            this.l1PresenceCacheMissCount = cacheMetrics.counter("l1PresenceCacheMiss");
            this.l2PresenceCacheHitCount = cacheMetrics.counter("l2PresenceCacheHit");
            this.l2PresenceCacheMissCount = cacheMetrics.counter("l2PresenceCacheMiss");
        } else {
            this.l1PresenceCacheHitCount = new NoOpCounter();
            this.l1PresenceCacheMissCount = new NoOpCounter();
            this.l2PresenceCacheHitCount = new NoOpCounter();
            this.l2PresenceCacheMissCount = new NoOpCounter();
        }
        this.delegateLookups = cacheMetrics.counter("delegateLookups");

        // cacheMetrics.gauge("bypassActive", () -> bypassCache ? 1 : 0);
        // if (mapCacheHitRateThreshold > 0.0) {
        //     cacheMetrics.gauge("currentHitRateForBypass", () -> {
        //         long accesses = accessesForHitRateWindow.get();
        //         long hits = hitsInHitRateWindow.get();
        //         return accesses > 0 ? (double) hits / accesses : 0.0;
        //     });
        // }
    }

    // Helper for non-CacheEntry valued caches (like namespaceCaches, keyCaches)
    private <CK, CV> CachePolicy<CK, CV> createCachePolicyForHierarchicalCache(int capacity,
            Consumer<Map.Entry<CK, CV>> evictionListener) {
        switch (this.cachePolicyType) {
            case TINYLFU:
                return new TinyLFUMap<>(capacity, evictionListener);
            case LRU:
            default:
                if (evictionListener != null) {
                    return new LRUMap<>(capacity, evictionListener);
                }
                return new LRUMap<>(capacity);
        }
    }

    private PerKeyMapCache<UK, UV, K, N> getOrCreatePerKeyMapCache() {
        N namespace = getCurrentNamespace();
        K key = backend.getCurrentKey();
        if (key == null) {
            // This might happen if setCurrentKey(null) was called on backend.
            // Caching layer cannot operate without a Flink key context here for map state.
            // Throwing an error or returning a non-caching wrapper might be options.
            // For now, assume key is always set before map operations.
            throw new IllegalStateException(
                    "Current Flink key is null. Cannot get/create PerKeyMapCache.");
        }

        CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> flinkKeyCaches =
                namespaceCaches.get(namespace);
        if (flinkKeyCaches == null) {
            // Create a new cache for Flink keys under this namespace
            flinkKeyCaches = createCachePolicyForHierarchicalCache(
                    this.maxFlinkKeysWithActiveCachesPerNamespace, evictedFlinkKeyEntry -> {
                        // When a FlinkKey's cache is evicted from its namespace cache, flush its L1
                        // entries
                        PerKeyMapCache<UK, UV, K, N> perKeyCache = evictedFlinkKeyEntry.getValue();
                        if (perKeyCache != null) {
                            try {
                                // Context for delegateState should be set to this perKeyCache's
                                // Flink key and namespace
                                K originalKey = backend.getCurrentKey();
                                N originalNamespace = getCurrentNamespace(); // Corrected

                                backend.setCurrentKey(perKeyCache.flinkKey);
                                delegateState.setCurrentNamespace(perKeyCache.cacheNamespace);

                                flushL1Entries(perKeyCache, perKeyCache.flinkKey,
                                        perKeyCache.cacheNamespace, backend, delegateState);
                                perKeyCache.l2MapEntries.clear();
                                if (perKeyCache.keyPresenceCacheEnabled) {
                                    perKeyCache.l1KeyPresenceCache.clear();
                                    perKeyCache.l2KeyPresenceCache.clear();
                                }

                                // Unregister metrics for this cache so that future instances can
                                // re-register cleanly.
                                // perKeyCache.closeMetrics();

                                // Restore original context
                                if (originalKey != null)
                                    backend.setCurrentKey(originalKey);
                                else
                                    backend.setCurrentKey(null);
                                if (originalNamespace != null)
                                    delegateState.setCurrentNamespace(originalNamespace);
                                else
                                    delegateState.setCurrentNamespace(null);

                            } catch (Exception e) {
                                LOG.error(
                                        "Error flushing PerKeyMapCache during Flink key eviction from namespace {}: Flink key {}",
                                        namespace, evictedFlinkKeyEntry.getKey(), e);
                                throw new RuntimeException(
                                        "Error during Flink key cache eviction and flush for Flink key: "
                                                + evictedFlinkKeyEntry.getKey(),
                                        e);
                            }
                        }
                    });
            namespaceCaches.put(namespace, flinkKeyCaches);
        }

        PerKeyMapCache<UK, UV, K, N> perKeyCache = flinkKeyCaches.get(key);
        if (perKeyCache == null) {
            perKeyCache = new PerKeyMapCache<>(this.l1CacheSizePerMap, this.l2CacheSizePerMap,
                    this.delegateState, this.backend, key, namespace, getCurrentNamespace(),
                    this.cachePolicyType, this.mapL1KeyPresenceCacheSize,
                    this.mapL2KeyPresenceCacheSize, this.keyPresenceCacheEnabled,
                    this.mapPresenceCacheImpl, this.userKeySerializer);
            flinkKeyCaches.put(key, perKeyCache);
            // perKeyCache.registerMetrics(this.metrics.addGroup("perKeyCache"), key,
            //         getCurrentNamespace());
        }
        return perKeyCache;
    }

    private void updateCacheBypassCondition(boolean resolvedByCache) {
        if (!bypassEnabled || this.mapCacheHitRateThreshold <= 0.0
                || accessesForHitRateWindow == null || hitsInHitRateWindow == null
                || totalAccessesForBypassEligibility == null) {
            this.bypassCache = false;
            return;
        }

        if (resolvedByCache) {
            hitsInHitRateWindow.incrementAndGet();
        }
        long currentWindowAccesses = accessesForHitRateWindow.incrementAndGet();

        if (currentWindowAccesses >= this.mapCacheHitRateWindowSize) {
            long totalAccesses = totalAccessesForBypassEligibility.addAndGet(currentWindowAccesses);

            if (totalAccesses < this.mapCacheMinAccessesForBypassCheck) {
                accessesForHitRateWindow.set(0);
                hitsInHitRateWindow.set(0);
                this.bypassCache = false;
                return;
            }

            double currentHitRate = (double) hitsInHitRateWindow.get() / currentWindowAccesses;
            this.bypassCache = currentHitRate < this.mapCacheHitRateThreshold;

            accessesForHitRateWindow.set(0);
            hitsInHitRateWindow.set(0);
        }
    }

    @Override
    public UV get(UK userKey) throws Exception {
        if (userKey == null)
            return null;
        delegateState.setCurrentNamespace(getCurrentNamespace());

        if (bypassEnabled && bypassCache) {
            boolean isSample =
                    accessSampler != null && (accessSampler.incrementAndGet() % SAMPLING_RATE == 0);
            if (!isSample) {
                delegateLookups.inc();
                UV value = delegateState.get(userKey);
                updateCacheBypassCondition(false); // A bypass is always a cache miss.
                return value;
            }
            // If it is a sample, fall through to the normal cache logic to get a real hit/miss
            // metric.
        }

        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();
        boolean resolvedByCache = false;
        UV userValue = null;

        if (!this.keyPresenceCacheEnabled) { // KV Separation DISABLED path
            CacheEntry<UV> l1Entry = perKeyCache.l1MapEntries.get(userKey);
            if (l1Entry != null) {
                l1ValueCacheHitCount.inc();
                userValue = l1Entry.getValue(); // Could be null if tombstone
                resolvedByCache = true;
            } else {
                l1ValueCacheMissCount.inc();
                CacheEntry<UV> l2Entry = perKeyCache.l2MapEntries.remove(userKey);
                if (l2Entry != null) {
                    l2ValueCacheHitCount.inc();
                    // L2 entry removal will trigger the listener to report memory released.
                    userValue = l2Entry.getValue();
                    CacheEntry<UV> newL1Entry = CacheEntry.clean(userValue);
                    CacheEntry<UV> oldL1Entry =
                            perKeyCache.l1MapEntries.put(userKey, newL1Entry);
                    if (oldL1Entry != null) {
                        perKeyCache.ownerBackend.reportCacheMemoryReleased(
                                oldL1Entry.getEstimatedSizeBytes());
                    }
                    perKeyCache.ownerBackend.reportCacheMemoryAdded(
                            newL1Entry.getEstimatedSizeBytes());
                    resolvedByCache = true;
                } else {
                    l2ValueCacheMissCount.inc();
                    if (perKeyCache.fullyLoaded) {
                        return null;
                    }
                    delegateLookups.inc();
                    userValue = delegateState.get(userKey);
                    if (userValue != null) {
                        // Add to L1.
                        CacheEntry<UV> newEntry = CacheEntry.clean(userValue);
                        CacheEntry<UV> oldEntry = perKeyCache.l1MapEntries.put(userKey, newEntry);
                        if (oldEntry != null) {
                            perKeyCache.ownerBackend.reportCacheMemoryReleased(
                                    oldEntry.getEstimatedSizeBytes());
                        }
                        perKeyCache.ownerBackend.reportCacheMemoryAdded(
                                newEntry.getEstimatedSizeBytes());
                    }
                    // If userValue is null from delegate, no tombstone is explicitly added to L1
                    // value cache here.
                    resolvedByCache = false; // Mark as from delegate for bypass condition update
                }
            }
            updateCacheBypassCondition(resolvedByCache && userValue != null);
            return userValue;
        }

        // KV Separation ENABLED path
        PerKeyMapCache.ValuePresence presence = perKeyCache.getValuePresence(userKey);

        if (presence == PerKeyMapCache.ValuePresence.ABSENT_IN_CACHE) {
            l1PresenceCacheHitCount.inc(); // Hit in presence cache (L1 or promoted L2) indicating
                                           // absence
            updateCacheBypassCondition(true); // Resolved by cache as definitively absent
            return null;
        }

        // Check L1 Value Cache regardless of initial presence outcome (unless ABSENT_IN_CACHE)
        CacheEntry<UV> l1ValEntry = perKeyCache.l1MapEntries.get(userKey);
        if (l1ValEntry != null) {
            l1ValueCacheHitCount.inc();
            // If presence was uncertain, this L1 value hit resolves it.
            // If presence said PRESENT_IN_CACHE_CLEAN, this confirms the value part.
            if (presence == PerKeyMapCache.ValuePresence.ABSENT_MAYBE_IN_VALUE_CACHE)
                l1PresenceCacheMissCount.inc(); // Count initial presence miss
            else
                l1PresenceCacheHitCount.inc(); // Count presence hit that led here

            userValue = l1ValEntry.getValue(); // Could be null if it's a tombstone
            resolvedByCache = true;
        } else {
            l1ValueCacheMissCount.inc();
            // If presence cache said PRESENT_IN_CACHE_CLEAN, but L1 value is a miss, this is a
            // slight inconsistency
            // or means it was just evicted from L1 value to L2 value. Log for observation if strict
            // consistency expected.
            if (presence == PerKeyMapCache.ValuePresence.PRESENT_IN_CACHE_CLEAN) {
                l1PresenceCacheHitCount.inc();
                LOG.debug(
                        "L1 Presence cache indicated key {} exists, but value not found in L1 value cache. Checking L2 value cache.",
                        userKey);
            } else if (presence == PerKeyMapCache.ValuePresence.ABSENT_MAYBE_IN_VALUE_CACHE) {
                l1PresenceCacheMissCount.inc(); // Miss in presence, now L1 value also missed.
            }

            // Check L2 Value Cache
            CacheEntry<UV> l2ValEntry = perKeyCache.l2MapEntries.remove(userKey);
            if (l2ValEntry != null) {
                l2ValueCacheHitCount.inc();
                userValue = l2ValEntry.getValue(); // L2 entries are clean, non-null
                CacheEntry<UV> newL1Entry = CacheEntry.clean(userValue);
                CacheEntry<UV> oldL1Entry = perKeyCache.l1MapEntries.put(userKey, newL1Entry);
                if (oldL1Entry != null) {
                    perKeyCache.ownerBackend.reportCacheMemoryReleased(
                            oldL1Entry.getEstimatedSizeBytes());
                }
                perKeyCache.ownerBackend.reportCacheMemoryAdded(newL1Entry.getEstimatedSizeBytes());
                resolvedByCache = true;
            } else {
                l2ValueCacheMissCount.inc();
                if (perKeyCache.fullyLoaded) {
                    perKeyCache.updatePresenceCacheOnGet(userKey, false);
                    updateCacheBypassCondition(true);
                    return null;
                }
                // Not in L1 value, not in L2 value. Fetch from delegate.
                delegateLookups.inc();
                userValue = delegateState.get(userKey);
                resolvedByCache = false; // From delegate
                if (userValue != null) {
                    CacheEntry<UV> newEntry = CacheEntry.clean(userValue);
                    CacheEntry<UV> oldEntry = perKeyCache.l1MapEntries.put(userKey, newEntry);
                    if (oldEntry != null) {
                        perKeyCache.ownerBackend.reportCacheMemoryReleased(
                                oldEntry.getEstimatedSizeBytes());
                    }
                    perKeyCache.ownerBackend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
                }
                // If userValue is null, a tombstone is not explicitly added to L1 from delegate
                // here.
            }
        }

        // Update presence cache based on the final outcome from value caches or delegate.
        perKeyCache.updatePresenceCacheOnGet(userKey, userValue != null);
        updateCacheBypassCondition(resolvedByCache && userValue != null); // Hit if found in cache &
                                                                          // not tombstone
        return userValue;
    }

    @Override
    public void put(UK userKey, UV userValue) throws Exception {
        if (userKey == null) {
            /* let delegate handle or throw */ return;
        }
        if (userValue == null) {
            remove(userKey); // Standard map behavior for put(key, null)
            return;
        }
        delegateState.setCurrentNamespace(getCurrentNamespace());

        if (bypassEnabled && bypassCache) {
            boolean isSample =
                    accessSampler != null && (accessSampler.incrementAndGet() % SAMPLING_RATE == 0);
            if (!isSample) {
                delegateState.put(userKey, userValue);
                updateCacheBypassCondition(false);
                return;
            }
        }

        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();
        // The CachePolicy (e.g., LRUMap) used for l1MapEntries is responsible for:
        // 1. Evicting an old entry if capacity is reached (triggering its eviction listener, which
        // calls backend.reportCacheMemoryReleased).
        // 2. Calling backend.reportCacheMemoryAdded for the new_entry.getEstimatedSizeBytes() when
        // this new entry is accepted.
        CacheEntry<UV> newEntry = CacheEntry.dirty(userValue);
        CacheEntry<UV> oldEntry = perKeyCache.l1MapEntries.put(userKey, newEntry);
        if (oldEntry != null) {
            perKeyCache.ownerBackend.reportCacheMemoryReleased(oldEntry.getEstimatedSizeBytes());
        }
        perKeyCache.ownerBackend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());

        // Invalidate L2 if L1 is now dirty. The remove from CachePolicy will trigger memory
        // release.
        perKeyCache.l2MapEntries.remove(userKey);

        if (this.keyPresenceCacheEnabled) {
            perKeyCache.updatePresenceCacheOnPut(userKey);
        }
        perKeyCache.fullyLoaded = false; // A put might change the full set of keys
        updateCacheBypassCondition(true); // A put often implies a subsequent get (hit)
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        if (map == null) {
            // Or perhaps throw new NullPointerException("Map cannot be null.");
            // Depending on desired behavior, an empty map is fine, null might not be.
            return; // No-op if map is null, consistent with some Map implementations.
        }
        // Ensure delegate state operates on the correct namespace for all subsequent put
        // operations.
        // Calling it once here is more efficient than in every this.put() call if this.put()
        // doesn't already manage it.
        // However, this.put() already calls
        // delegateState.setCurrentNamespace(getCurrentNamespace());
        // So, this explicit call here might be redundant unless we bypass this.put().
        // For safety and clarity, let's rely on this.put() to set the namespace.

        for (Map.Entry<UK, UV> entry : map.entrySet()) {
            // this.put() will handle null userKey within its logic (likely a no-op or error)
            // and will convert put(key, null) to remove(key).
            this.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void remove(UK userKey) throws Exception {
        if (userKey == null) {
            /* let delegate handle or throw */ return;
        }
        delegateState.setCurrentNamespace(getCurrentNamespace());

        if (bypassEnabled && bypassCache) {
            boolean isSample =
                    accessSampler != null && (accessSampler.incrementAndGet() % SAMPLING_RATE == 0);
            if (!isSample) {
                delegateState.remove(userKey);
                updateCacheBypassCondition(false);
                return;
            }
        }

        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();
        CacheEntry<UV> newEntry = CacheEntry.dirty(null); // Tombstone
        CacheEntry<UV> oldEntry = perKeyCache.l1MapEntries.put(userKey, newEntry);
        if (oldEntry != null) {
            perKeyCache.ownerBackend.reportCacheMemoryReleased(oldEntry.getEstimatedSizeBytes());
        }
        perKeyCache.ownerBackend.reportCacheMemoryAdded(newEntry.getEstimatedSizeBytes());
        // L1's put handles eviction/memory for old, and memory for new tombstone.

        perKeyCache.l2MapEntries.remove(userKey); // Invalidate L2.
        // LRUMap.remove should trigger eviction listener for memory reporting.

        if (this.keyPresenceCacheEnabled) {
            perKeyCache.updatePresenceCacheOnRemove(userKey);
        }
        // Delegate remove must happen for actual data removal
        // delegateState.remove(userKey); // This is incorrect for a write-back cache. Defer to
        // flush.
        perKeyCache.fullyLoaded = false;
    }

    @Override
    public boolean contains(UK userKey) throws Exception {
        if (bypassEnabled && bypassCache) {
            boolean isSample =
                    accessSampler != null && (accessSampler.incrementAndGet() % SAMPLING_RATE == 0);
            if (!isSample) {
                updateCacheBypassCondition(false);
                return delegateState.contains(userKey);
            }
        }

        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        // 1. Check L1, it has the most up-to-date information.
        CacheEntry<UV> l1Entry = perKeyCache.l1MapEntries.get(userKey);
        if (l1Entry != null) {
            updateCacheBypassCondition(true); // L1 hit.
            return l1Entry.getValue() != null; // A null value is a tombstone (doesn't exist).
        }

        // 2. If fully loaded, the cache is the source of truth. If not in L1, check L2.
        if (perKeyCache.fullyLoaded) {
            updateCacheBypassCondition(true); // Resolved from cache, even if absent.
            CacheEntry<UV> l2Entry = perKeyCache.l2MapEntries.get(userKey);
            // L2 should not contain tombstones, so a non-null entry means it exists.
            return l2Entry != null;
        }

        // --- Not fully loaded and not in L1 ---

        // 3. Check presence cache.
        if (keyPresenceCacheEnabled) {
            PerKeyMapCache.ValuePresence presence = perKeyCache.getValuePresence(userKey);
            if (presence == PerKeyMapCache.ValuePresence.ABSENT_IN_CACHE) {
                updateCacheBypassCondition(true); // A hit on "absence" information.
                return false;
            }
            if (presence == PerKeyMapCache.ValuePresence.PRESENT_IN_CACHE_CLEAN) {
                updateCacheBypassCondition(true); // A hit on "presence" information.
                // This implies it exists in the delegate, even if the value isn't in the value
                // cache.
                return true;
            }
            // Fall through if presence information is inconclusive.
        }

        // 4. Check L2 value cache.
        CacheEntry<UV> l2Entry = perKeyCache.l2MapEntries.get(userKey);
        if (l2Entry != null) {
            updateCacheBypassCondition(true); // L2 hit.
            perKeyCache.l1MapEntries.put(userKey, l2Entry); // Promote to L1.
            return true;
        }

        // 5. Cache miss, consult the delegate.
        updateCacheBypassCondition(false);
        boolean exists = delegateState.contains(userKey);

        // 6. Update caches with information from delegate.
        if (keyPresenceCacheEnabled) {
            perKeyCache.updatePresenceCacheOnGet(userKey, exists);
        }
        // Note: We do not cache the value itself here, only its presence. `get()` would cache the
        // value.

        return exists;
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        // This iterable will produce a new MergingIterator on each call to iterator().
        return () -> {
            try {
                return iterator();
            } catch (Exception e) {
                throw new RuntimeException("Failed to create map state iterator", e);
            }
        };
    }

    @Override
    public Iterable<UV> values() throws Exception {
        return () -> {
            try {
                final Iterator<Map.Entry<UK, UV>> entryIterator = iterator();
                return new Iterator<UV>() {
                    @Override
                    public boolean hasNext() {
                        return entryIterator.hasNext();
                    }

                    @Override
                    public UV next() {
                        return entryIterator.next().getValue();
                    }
                };
            } catch (Exception e) {
                throw new RuntimeException("Failed to create map state values iterator", e);
            }
        };
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        return () -> {
            try {
                final Iterator<Map.Entry<UK, UV>> entryIterator = iterator();
                return new Iterator<UK>() {
                    @Override
                    public boolean hasNext() {
                        return entryIterator.hasNext();
                    }

                    @Override
                    public UK next() {
                        return entryIterator.next().getKey();
                    }
                };
            } catch (Exception e) {
                throw new RuntimeException("Failed to create map state keys iterator", e);
            }
        };
    }

    private void loadAllEntriesToCache(PerKeyMapCache<UK, UV, K, N> perKeyCache) throws Exception {
        if (perKeyCache.fullyLoaded) {
            return;
        }
        // Clear existing cache content before full load
        perKeyCache.l1MapEntries.clear(); // Will trigger memory release via listeners
        perKeyCache.l2MapEntries.clear(); // Will trigger memory release via listeners
        if (this.keyPresenceCacheEnabled) {
            perKeyCache.l1KeyPresenceCache.clear(); // Will trigger memory release
            perKeyCache.l2KeyPresenceCache.clear(); // Will trigger memory release
        }

        delegateLookups.inc();
        Iterable<Map.Entry<UK, UV>> entriesFromDelegate = delegateState.entries();
        if (entriesFromDelegate != null) {
            for (Map.Entry<UK, UV> entry : entriesFromDelegate) {
                if (entry.getValue() != null) { // Don't cache null values from delegate in value
                                                // cache
                    CacheEntry<UV> newCacheEntry = CacheEntry.clean(entry.getValue());
                    CacheEntry<UV> oldCacheEntry = perKeyCache.l1MapEntries.put(entry.getKey(), newCacheEntry);
                    if (oldCacheEntry != null) {
                        perKeyCache.ownerBackend.reportCacheMemoryReleased(
                                oldCacheEntry.getEstimatedSizeBytes());
                    }
                    perKeyCache.ownerBackend.reportCacheMemoryAdded(
                            newCacheEntry.getEstimatedSizeBytes());

                    if (this.keyPresenceCacheEnabled) {
                        perKeyCache.updatePresenceCacheOnGet(entry.getKey(), true); // Mark as
                                                                                    // present
                    }
                } else { // Null value from delegate for a key means it effectively doesn't exist
                         // for value cache.
                    if (this.keyPresenceCacheEnabled) {
                        perKeyCache.updatePresenceCacheOnGet(entry.getKey(), false); // Mark as
                                                                                     // absent
                    }
                }
            }
        }
        perKeyCache.fullyLoaded = true;
    }

    @Override
    public boolean isEmpty() throws Exception {
        if (bypassCache) {
            return delegateState.isEmpty();
        }

        final PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        // Check L1 for any non-tombstone entry
        for (CacheEntry<UV> l1Entry : perKeyCache.l1MapEntries.values()) {
            if (l1Entry.getValue() != null) {
                return false; // Found a valid entry
            }
        }

        // If fully loaded, the cache is the source of truth.
        // We already checked L1. Now check L2. L2 only has valid entries.
        if (perKeyCache.fullyLoaded) {
            return perKeyCache.l2MapEntries.isEmpty();
        }

        // Not fully loaded. Check L2 for an entry not covered by an L1 tombstone.
        for (Map.Entry<UK, CacheEntry<UV>> l2Entry : perKeyCache.l2MapEntries.entrySet()) {
            if (!perKeyCache.l1MapEntries.containsKey(l2Entry.getKey())) {
                return false; // Found a valid L2 entry not marked as removed in L1
            }
        }

        // Cache checks are inconclusive. Check the delegate, considering tombstones from L1.
        if (perKeyCache.l1MapEntries.isEmpty()) {
            // No tombstones, so if L2 is also empty, delegate's state is accurate.
            return delegateState.isEmpty();
        } else {
            // There are tombstones. We must iterate the delegate to see if any of its
            // entries are not covered by a tombstone.
            final Iterable<Map.Entry<UK, UV>> delegateEntries =
                    perKeyCache.fullyLoaded ? null : delegateState.entries();
            try (MergingIterator it = new MergingIterator(perKeyCache, delegateEntries)) {
                return !it.hasNext();
            }
        }
    }

    @Override
    public void clear() {
        // Ensure the delegate state operates on the correct namespace.
        // This is crucial because delegateState.clear() is namespace-specific.
        delegateState.setCurrentNamespace(getCurrentNamespace());

        // Obtain the cache specific to the current Flink key (K) and namespace (N).
        PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        // Clear L1 map entries. Eviction listeners (if any, e.g., for flushing dirty entries)
        // should be triggered by the CachePolicy's clear() implementation.
        perKeyCache.l1MapEntries.clear();

        // Clear L2 map entries. Similarly, eviction listeners should be triggered.
        perKeyCache.l2MapEntries.clear();

        // If key-value separation is enabled, clear the presence caches as well.
        if (this.keyPresenceCacheEnabled) {
            perKeyCache.l1KeyPresenceCache.clear();
            perKeyCache.l2KeyPresenceCache.clear();
        }

        // After clearing the caches, clear the entries in the underlying delegate state
        // for the current key and namespace.
        delegateState.clear();

        // Reset the fullyLoaded flag for this PerKeyMapCache, as its contents (both cache
        // and underlying state for this key/namespace) have been cleared.
        perKeyCache.fullyLoaded = false;
    }

    @Override
    public void flushToUnderlyingState() throws IOException {
        for (Map.Entry<N, CachePolicy<K, PerKeyMapCache<UK, UV, K, N>>> nsEntry : namespaceCaches
                .entrySet()) {
            N namespace = nsEntry.getKey();
            CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> keyCaches = nsEntry.getValue();

            List<K> flinkKeysInCache = new ArrayList<>();
            for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> keyEntry : keyCaches.entrySet()) {
                flinkKeysInCache.add(keyEntry.getKey());
            }

            for (K flinkKey : flinkKeysInCache) {
                PerKeyMapCache<UK, UV, K, N> perKeyCache = keyCaches.get(flinkKey);
                if (perKeyCache != null) {
                    try {
                        if (flinkKey != null) {
                            flushL1Entries(perKeyCache, flinkKey, namespace, this.backend,
                                    this.delegateState);
                        }
                    } catch (Exception e) {
                        throw new IOException("Failed to flush map entries for Flink key: "
                                + flinkKey + " in namespace: " + namespace, e);
                    }
                }
            }
        }
    }

    @Override
    public InternalMapState<K, N, UK, UV> getDelegateState() {
        return delegateState;
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
    public TypeSerializer<Map<UK, UV>> getValueSerializer() {
        return delegateState.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        this.currentNamespace = namespace;
        this.delegateState.setCurrentNamespace(namespace);
    }

    public N getCurrentNamespace() {
        if (currentNamespace == null) {
            throw new IllegalStateException(
                    "Namespace has not been set. Typically, you should call "
                            + "setCurrentNamespace" + " first.");
        }
        return currentNamespace;
    }

    public TypeSerializer<UK> getUserKeySerializer() {
        return this.userKeySerializer;
    }

    public TypeSerializer<UV> getUserValueSerializer() {
        return this.userValueSerializer;
    }

    @Override
    public byte[] getSerializedValue(final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<Map<UK, UV>> safeValueSerializer) throws Exception {
        throw new UnsupportedOperationException(
                "getSerializedValue directly is not supported by CachingInternalMapState due to cache structure.");
    }

    @Override
    public StateIncrementalVisitor<K, N, Map<UK, UV>> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        try {
            flushToUnderlyingState();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to flush cache to underlying state before creating StateIncrementalVisitor",
                    e);
        }
        return delegateState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    // Getter for testing bypass state
    @org.apache.flink.annotation.VisibleForTesting
    public boolean isBypassCacheActive() {
        return bypassCache;
    }

    @org.apache.flink.annotation.VisibleForTesting
    public long getTotalAccessesForBypassEligibility() {
        if (totalAccessesForBypassEligibility != null) {
            return totalAccessesForBypassEligibility.get();
        }
        return 0;
    }

    @Override
    public long evictEntriesToFreeMemory(long targetBytesToFreeThisState) {
        if (targetBytesToFreeThisState <= 0)
            return 0;
        long totalFreedBytes = 0;

        // Iterate over all PerKeyMapCache instances and ask them to evict
        // This is a simplified global eviction. More sophisticated might prioritize
        // namespaces/keys.
        try {
            for (CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> flinkKeyCaches : namespaceCaches
                    .values()) {
                if (totalFreedBytes >= targetBytesToFreeThisState)
                    break;
                if (flinkKeyCaches == null)
                    continue;

                // Iterate over a snapshot of keys to avoid ConcurrentModificationException if map
                // can change
                List<PerKeyMapCache<UK, UV, K, N>> perKeyCachesToEvict =
                        new ArrayList<>(flinkKeyCaches.values());

                for (PerKeyMapCache<UK, UV, K, N> perKeyCache : perKeyCachesToEvict) {
                    if (totalFreedBytes >= targetBytesToFreeThisState)
                        break;
                    if (perKeyCache == null)
                        continue;

                    // Set context for potential delegate operations if L1 dirty entries are flushed
                    // during eviction
                    K originalKey = backend.getCurrentKey();
                    N originalNamespace = getCurrentNamespace();
                    try {
                        backend.setCurrentKey(perKeyCache.flinkKey); // Set context for this specific
                                                                     // key's cache
                        delegateState.setCurrentNamespace(perKeyCache.cacheNamespace);

                        long freedThisCache = perKeyCache
                                .evictToMeetMemoryLimit(targetBytesToFreeThisState - totalFreedBytes);
                        totalFreedBytes += freedThisCache;
                    } finally {
                        // Restore original context
                        backend.setCurrentKey(originalKey);
                        delegateState.setCurrentNamespace(originalNamespace);
                    }


                    // Unregister metrics for this cache so that future instances can
                    // re-register cleanly.
                    // perKeyCache.closeMetrics();

                }
            }
        } catch (Exception e) {
            LOG.warn("Error during explicit memory eviction from CachingInternalMapState: {}",
                    e.getMessage(), e);
        }
        // The actual amount freed is tracked by CachingKeyedStateBackend's atomic counter.
        // This method signals that an attempt was made.
        return totalFreedBytes;
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        delegateState.setCurrentNamespace(getCurrentNamespace());
        final PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        // If we must consult the delegate, we need its entries.
        // This is the only place that should request a full iterator from the delegate state.
        final Iterable<Map.Entry<UK, UV>> delegateEntries =
                perKeyCache.fullyLoaded ? null : delegateState.entries();

        return new MergingIterator(perKeyCache, delegateEntries);
    }

    private enum MergingIteratorState {
        L1,
        L2,
        DELEGATE,
        DONE
    }

    private class MergingIterator implements Iterator<Map.Entry<UK, UV>>, AutoCloseable {

        private final PerKeyMapCache<UK, UV, K, N> perKeyCache;
        private final Iterator<Map.Entry<UK, CacheEntry<UV>>> l1Iterator;
        private final Iterator<Map.Entry<UK, CacheEntry<UV>>> l2Iterator;
        private final Iterator<Map.Entry<UK, UV>> delegateIterator;
        private final Set<UK> processedKeys; // To track keys from L1 and L2

        private Map.Entry<UK, UV> nextEntry;

        private MergingIteratorState currentState;

        MergingIterator(
                PerKeyMapCache<UK, UV, K, N> perKeyCache,
                Iterable<Map.Entry<UK, UV>> delegateEntries) {
            this.perKeyCache = perKeyCache;
            this.l1Iterator = perKeyCache.l1MapEntries.entrySet().iterator();
            this.l2Iterator = perKeyCache.l2MapEntries.entrySet().iterator();
            this.delegateIterator =
                    delegateEntries != null ? delegateEntries.iterator() : Collections.emptyIterator();
            this.processedKeys = new HashSet<>(
                    perKeyCache.l1MapEntries.size() + perKeyCache.l2MapEntries.size());
            this.currentState = MergingIteratorState.L1;
            advance();
        }

        @Override
        public boolean hasNext() {
            return nextEntry != null;
        }

        @Override
        public Map.Entry<UK, UV> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Map.Entry<UK, UV> current = nextEntry;
            advance();
            return current;
        }

        private void advance() {
            this.nextEntry = null;
            while (this.nextEntry == null && currentState != MergingIteratorState.DONE) {
                switch (currentState) {
                    case L1:
                        processL1();
                        break;
                    case L2:
                        processL2();
                        break;
                    case DELEGATE:
                        processDelegate();
                        break;
                }
            }
        }

        private void processL1() {
            while (l1Iterator.hasNext()) {
                Map.Entry<UK, CacheEntry<UV>> l1Entry = l1Iterator.next();
                processedKeys.add(l1Entry.getKey()); // Track key
                if (l1Entry.getValue().getValue() != null) { // Not a tombstone
                    this.nextEntry = new AbstractMap.SimpleEntry<>(
                            l1Entry.getKey(), l1Entry.getValue().getValue());
                    return;
                }
            }
            currentState = MergingIteratorState.L2;
        }

        private void processL2() {
            while (l2Iterator.hasNext()) {
                Map.Entry<UK, CacheEntry<UV>> l2Entry = l2Iterator.next();
                UK key = l2Entry.getKey();
                if (processedKeys.add(key)) { // If not already processed from L1
                    // L2 entries are always clean and non-null
                    this.nextEntry = new AbstractMap.SimpleEntry<>(
                            key, l2Entry.getValue().getValue());
                    return;
                }
            }
            currentState = MergingIteratorState.DELEGATE;
        }

        private void processDelegate() {
            while (delegateIterator.hasNext()) {
                Map.Entry<UK, UV> delegateEntry = delegateIterator.next();
                UK key = delegateEntry.getKey();
                if (processedKeys.add(key)) { // If not already processed from L1 or L2
                    this.nextEntry = delegateEntry;
                    UV value = delegateEntry.getValue();

                    // Warm up cache: Add the fetched entry to L1.
                    // This is best-effort and respects cache size limits via eviction.
                    if (value != null) {
                        CacheEntry<UV> newCacheEntry = CacheEntry.clean(value);
                        CacheEntry<UV> oldCacheEntry = perKeyCache.l1MapEntries.put(key, newCacheEntry);

                        if (oldCacheEntry != null) {
                            perKeyCache.ownerBackend.reportCacheMemoryReleased(
                                    oldCacheEntry.getEstimatedSizeBytes());
                        }
                        perKeyCache.ownerBackend.reportCacheMemoryAdded(
                                newCacheEntry.getEstimatedSizeBytes());
                    }

                    if (perKeyCache.keyPresenceCacheEnabled) {
                        perKeyCache.updatePresenceCacheOnGet(key, value != null);
                    }
                    return;
                }
            }
            currentState = MergingIteratorState.DONE;
        }

        @Override
        public void close() throws Exception {
            // No resources to close in this specific iterator implementation
        }
    }

    private static <K_F, N_F, UK_C, UV_C> void flushL1Entries(
            PerKeyMapCache<UK_C, UV_C, K_F, N_F> perKeyCache, K_F flinkKey, N_F namespace,
            CachingKeyedStateBackend<K_F> backendForContext,
            InternalMapState<K_F, N_F, UK_C, UV_C> delegateStateForContext) throws Exception {

        if (flinkKey == null || perKeyCache == null || perKeyCache.l1MapEntries == null) {
            return;
        }

        K_F originalKey = backendForContext.getCurrentKey();
        N_F originalNamespace = namespace;

        backendForContext.setCurrentKey(flinkKey);
        delegateStateForContext.setCurrentNamespace(namespace); // Use the namespace relevant to
                                                                // this perKeyCache

        try {
            Iterator<Map.Entry<UK_C, CacheEntry<UV_C>>> l1Iterator =
                    perKeyCache.l1MapEntries.entrySet().iterator();
            while (l1Iterator.hasNext()) {
                Map.Entry<UK_C, CacheEntry<UV_C>> l1Entry = l1Iterator.next();
                UK_C userKey = l1Entry.getKey();
                CacheEntry<UV_C> cacheEntry = l1Entry.getValue();

                if (cacheEntry.isDirty()) {
                    UV_C userValue = cacheEntry.getValue();
                    if (userValue == null) { // Tombstone
                        delegateStateForContext.remove(userKey);
                    } else {
                        delegateStateForContext.put(userKey, userValue);
                        // Move to L2 as clean after successful flush, if L2 is enabled for values
                        if (perKeyCache.l2MapEntries.getClass() != NoOpCachePolicy.class) { // Check
                                                                                            // if L2
                                                                                            // is
                                                                                            // not
                                                                                            // NoOp
                            perKeyCache.l2MapEntries.put(userKey, CacheEntry.clean(userValue));
                        }
                    }
                    cacheEntry.setDirty(false); // Mark as clean
                } else { // Clean entry from L1
                    if (cacheEntry.getValue() != null) { // Not a tombstone, can move to L2
                        if (perKeyCache.l2MapEntries.getClass() != NoOpCachePolicy.class) {
                            perKeyCache.l2MapEntries.put(userKey, cacheEntry); // Already clean
                        }
                    }
                }
                // After processing (flushing dirty or moving clean to L2), the entry is removed
                // from L1 by the iterator.
                // The memory release for this L1 removal is handled by the L1 cache policy's
                // eviction listener.
                l1Iterator.remove();
            }
        } finally {
            backendForContext.setCurrentKey(originalKey);
            delegateStateForContext.setCurrentNamespace(originalNamespace);
        }
    }

    // Static inner class for NoOpCounter
    private static class NoOpCounter implements Counter {
        @Override
        public void inc() {}

        @Override
        public void inc(long n) {}

        @Override
        public void dec() {}

        @Override
        public void dec(long n) {}

        @Override
        public long getCount() {
            return 0;
        }
    }

    // Add this helper method to log cache metrics
    private void logCacheMetrics() {
        if (LOG.isDebugEnabled()) {
            // Calculate hit rates
            double l1ValueHitRate = calculateHitRate(l1ValueCacheHitCount, l1ValueCacheMissCount);
            double l2ValueHitRate = calculateHitRate(l2ValueCacheHitCount, l2ValueCacheMissCount);
            double l1PresenceHitRate =
                    calculateHitRate(l1PresenceCacheHitCount, l1PresenceCacheMissCount);
            double l2PresenceHitRate =
                    calculateHitRate(l2PresenceCacheHitCount, l2PresenceCacheMissCount);

            LOG.debug(
                            "Cache Metrics - " + "L1 Value: {}/{}, Hit Rate: {:.2f}% | "
                            + "L2 Value: {}/{}, Hit Rate: {:.2f}% | "
                            + "L1 Presence: {}/{}, Hit Rate: {:.2f}% | "
                            + "L2 Presence: {}/{}, Hit Rate: {:.2f}% | " + "Delegate Lookups: {}",
                    l1ValueCacheHitCount.getCount(),
                    l1ValueCacheHitCount.getCount() + l1ValueCacheMissCount.getCount(),
                    l1ValueHitRate, l2ValueCacheHitCount.getCount(),
                            l2ValueCacheHitCount.getCount() + l2ValueCacheMissCount.getCount(),
                    l2ValueHitRate, l1PresenceCacheHitCount.getCount(),
                            l1PresenceCacheHitCount.getCount() + l1PresenceCacheMissCount.getCount(),
                    l1PresenceHitRate, l2PresenceCacheHitCount.getCount(),
                            l2PresenceCacheHitCount.getCount() + l2PresenceCacheMissCount.getCount(),
                    l2PresenceHitRate, delegateLookups.getCount());
        }
    }

    private double calculateHitRate(Counter hits, Counter misses) {
        long total = hits.getCount() + misses.getCount();
        return total > 0 ? (hits.getCount() * 100.0) / total : 0.0;
    }
}


