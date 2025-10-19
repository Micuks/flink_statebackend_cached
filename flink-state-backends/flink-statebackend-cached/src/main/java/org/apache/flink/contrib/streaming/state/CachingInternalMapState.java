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
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Collections;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.contrib.streaming.state.MurmurHash3;
import org.apache.flink.contrib.streaming.state.CachePolicy;
import org.apache.flink.contrib.streaming.state.CacheEntry;
import org.apache.flink.contrib.streaming.state.NoOpCachePolicy;
import org.apache.flink.contrib.streaming.state.OffHeapKVStore;
import org.apache.flink.contrib.streaming.state.CachingKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.CachingStateBackendFactory;
import org.apache.flink.contrib.streaming.state.CachingInternalState;
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
    private final boolean l2ManagedMemoryEnabled;
    private final boolean perKeyMetricsEnabled;
    private final boolean forceBypassAlways;

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

    // ----------------------------------------------------------------------
    // Left-stream bypass implementation (restored from milestone_40_pct)
    // ----------------------------------------------------------------------
    // Thread-local access hints to support asymmetric cache usage per-call.
    // BYPASS: fully bypass cache and go directly to the delegate (for writes or reads).
    // NO_TOUCH: avoid touching/creating cache structures during iteration; use delegate iteration.
    private static final ThreadLocal<Boolean> THREAD_LOCAL_BYPASS =
            ThreadLocal.withInitial(() -> Boolean.FALSE);
    private static final ThreadLocal<Boolean> THREAD_LOCAL_NO_TOUCH =
            ThreadLocal.withInitial(() -> Boolean.FALSE);

    // Global defaults controlled by configuration; OR'ed with thread-local hints.
    private static volatile boolean GLOBAL_BYPASS = false;
    private static volatile boolean GLOBAL_NO_TOUCH = false;

    // Auto-detect join side based on stack frames (no operator changes required).
    // If enabled, calls coming from processElement1() are treated as LEFT and will bypass/no-touch.
    private static volatile boolean AUTO_LEFT_BYPASS = true;

    public static void setAutoLeftBypass(boolean enable) {
        AUTO_LEFT_BYPASS = enable;
    }

    private enum JoinSide { LEFT, RIGHT, UNKNOWN }

    // Note: older versions attempted per-thread memoization of detected side.
    // In Flink task threads that process both inputs, that led to repeated
    // stack walks and incorrect reuse. We now detect per-state instance once
    // and reuse the result to avoid stack walking on hot paths.

    // Best-effort single log for unknown detection to avoid log flooding
    private static volatile boolean LOGGED_UNKNOWN_ONCE = false;

    private static JoinSide detectJoinSideFromStack() {
        // Always inspect a small portion of the stack to determine side for this call.
        // Do NOT cache the result per-thread: the same task thread processes both inputs.
        try {
            StackTraceElement[] stack = Thread.currentThread().getStackTrace();
            // Limit scan depth aggressively to reduce overhead; 16 is sufficient
            // to catch operator processing frames in typical Flink stacks.
            int maxDepth = Math.min(stack.length, 16);
            for (int i = 2; i < maxDepth; i++) {
                String method = stack[i].getMethodName();
                if ("processElement1".equals(method) || method.contains("processElement1")
                        || "processRecord1".equals(method) || method.contains("processRecord1")) {
                    return JoinSide.LEFT;
                }
                if ("processElement2".equals(method) || method.contains("processElement2")
                        || "processRecord2".equals(method) || method.contains("processRecord2")) {
                    return JoinSide.RIGHT;
                }
            }
        } catch (Throwable t) {
            // ignore and fall through
        }
        if (!LOGGED_UNKNOWN_ONCE) {
            LOGGED_UNKNOWN_ONCE = true;
            LOG.debug("Join side could not be detected from stack; using conservative mode (no auto-bypass).");
        }
        return JoinSide.UNKNOWN;
    }

    private boolean isAutoBypassActiveForThisCall() {
        if (!AUTO_LEFT_BYPASS) {
            return false;
        }
        JoinSide side = detectJoinSideForThisState();
        return side == JoinSide.LEFT;
    }

    // ------------------------------------------------------------------
    // Per-state side detection (avoids per-thread memoization bugs)
    // ------------------------------------------------------------------
    // Each CachingInternalMapState instance typically belongs to one logical
    // side in a two-input operator. Detect once for this instance and reuse.
    private volatile JoinSide perStateDetectedSide = JoinSide.UNKNOWN;
    private volatile boolean perStateDetectionAttempted = false;

    private JoinSide detectJoinSideForThisState() {
        if (perStateDetectionAttempted) {
            return perStateDetectedSide;
        }
        // Perform a single, shallow scan to infer the side. Even if UNKNOWN, we
        // mark as attempted to avoid scanning on every call.
        JoinSide s = detectJoinSideFromStack();
        perStateDetectedSide = s;
        perStateDetectionAttempted = true;
        return s;
    }

    // Metrics
    private final transient MetricGroup metrics;
    transient Counter l1MapValueCacheHitCount;
    transient Counter l1MapValueCacheMissCount;
    transient Counter l2MapValueCacheHitCount;
    transient Counter l2MapValueCacheMissCount;
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
    private static enum ValuePresence {
        PRESENT_IN_CACHE_CLEAN, // Present in presence cache (always clean)
        ABSENT_IN_CACHE, // Explicitly absent in presence cache
        ABSENT_MAYBE_IN_VALUE_CACHE // Not found in presence cache, actual value might be in
                                    // value cache or delegate
    }

    private class PerKeyMapCache<UK_C, UV_C, K_F, N_F> {
        final CachePolicy<UK_C, CacheEntry<UV_C>> l1MapEntries;
        final CachePolicy<UK_C, CacheEntry<UV_C>> l2MapEntries;
        final OffHeapKVStore l2MapEntriesOffHeap;
        final boolean l2ManagedMemoryEnabled;
        final CachePolicy<UK_C, CacheEntry<Boolean>> l1KeyPresenceCache;
        final CachePolicy<UK_C, CacheEntry<Boolean>> l2KeyPresenceCache;

        // new primitive presence caches
        final CachePolicy<Long, Byte> l1PrimitivePresenceCache;
        final CachePolicy<Long, Byte> l2PrimitivePresenceCache;
        private final CachingStateBackendFactory.PresenceCacheImplementation presenceCacheImpl;
        private final TypeSerializer<UK_C> userKeySerializer;
        private transient ThreadLocal<DataOutputSerializer> userKeySerializerView;
        private transient ThreadLocal<DataInputDeserializer> userKeyDeserializerView;
        private final TypeSerializer<UV_C> userValueSerializer;
        private transient ThreadLocal<DataOutputSerializer> userValueSerializerView;
        private transient ThreadLocal<DataInputDeserializer> userValueDeserializerView;

        boolean fullyLoaded = false;
        private final K_F flinkKey;
        private final N_F cacheNamespace;
        private final CachingKeyedStateBackend<K_F> ownerBackend;
        final InternalMapState<K_F, N_F, UK_C, UV_C> delegateState;
        private final boolean keyPresenceCacheEnabled;

        /** The MetricGroup created for this cache instance (can be null before registration). */
        private transient MetricGroup metricGroup;
        private transient boolean metricsRegistered = false;

        PerKeyMapCache(int l1Size, int l2Size, InternalMapState<K_F, N_F, UK_C, UV_C> delegateState,
                CachingKeyedStateBackend<K_F> ownerBackend, K_F flinkKey, N_F cacheNamespace,
                CachingStateBackendFactory.CachePolicyType cachePolicyType,
                int mapL1KeyPresenceCacheSize, int mapL2KeyPresenceCacheSize,
                boolean keyPresenceCacheEnabled,
                CachingStateBackendFactory.PresenceCacheImplementation presenceCacheImpl,
                TypeSerializer<UK_C> userKeySerializer,
                TypeSerializer<UV_C> userValueSerializer,
                boolean l2ManagedMemoryEnabled) {
            this.flinkKey = flinkKey;
            this.cacheNamespace = cacheNamespace;
            this.delegateState = delegateState;
            this.ownerBackend = ownerBackend;
            this.keyPresenceCacheEnabled = keyPresenceCacheEnabled;
            this.presenceCacheImpl = presenceCacheImpl;
            this.userKeySerializer = userKeySerializer;
            this.userValueSerializer = userValueSerializer;
            this.l2ManagedMemoryEnabled = l2ManagedMemoryEnabled;

            this.userKeySerializerView =
                    ThreadLocal.withInitial(
                            () -> {
                                if (this.userKeySerializer != null) {
                                    return new DataOutputSerializer(128);
                                } else {
                                    return new DataOutputSerializer(0);
                                }
                            });

            this.userKeyDeserializerView =
                    ThreadLocal.withInitial(
                            () -> {
                                if (this.userKeySerializer != null) {
                                    return new DataInputDeserializer();
                                } else {
                                    return new DataInputDeserializer(new byte[0]);
                                }
                            });

            this.userValueSerializerView =
                    ThreadLocal.withInitial(
                            () -> {
                                if (this.userValueSerializer != null) {
                                    return new DataOutputSerializer(128);
                                } else {
                                    return new DataOutputSerializer(0);
                                }
                            });
            this.userValueDeserializerView =
                    ThreadLocal.withInitial(
                            () -> {
                                if (this.userValueSerializer != null) {
                                    return new DataInputDeserializer();
                                } else {
                                    return new DataInputDeserializer(new byte[0]);
                                }
                            });

            if (this.keyPresenceCacheEnabled) {
                if (presenceCacheImpl
                        == CachingStateBackendFactory.PresenceCacheImplementation.PRIMITIVE_MAP) {
                    this.l1PrimitivePresenceCache = new PrimitivePresenceCache(
                            mapL1KeyPresenceCacheSize,
                            entry -> {
                                if (entry.getValue() != null) {
                                    ownerBackend.reportCacheMemoryAdded(16L);
                                }
                            });
                    this.l2PrimitivePresenceCache = new PrimitivePresenceCache(
                            mapL2KeyPresenceCacheSize,
                            entry -> {
                                if (entry.getValue() != null) {
                                    ownerBackend.reportCacheMemoryAdded(16L);
                                }
                            });
                    // Init old caches to NoOp
                    this.l1KeyPresenceCache = new NoOpCachePolicy<>();
                    this.l2KeyPresenceCache = new NoOpCachePolicy<>();
                } else {
                    this.l1KeyPresenceCache = createCachePolicyInstance(
                            cachePolicyType,
                            mapL1KeyPresenceCacheSize,
                            entry -> {
                                if (entry.getValue() != null) {
                                    ownerBackend.reportCacheMemoryAdded(ValueSizeUtils.estimate(Boolean.TRUE));
                                }
                            },
                            ownerBackend,
                            true,
                            true);
                    this.l2KeyPresenceCache = createCachePolicyInstance(
                            cachePolicyType,
                            mapL2KeyPresenceCacheSize,
                            entry -> {
                                if (entry.getValue() != null) {
                                    ownerBackend.reportCacheMemoryAdded(ValueSizeUtils.estimate(Boolean.TRUE));
                                }
                            },
                            ownerBackend,
                            true,
                            true);
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

            if (l2ManagedMemoryEnabled && l2Size > 0 && ownerBackend.getManagedPagePool() != null
                    && ownerBackend.getManagedPagePool().isUsable()) {
                this.l2MapEntriesOffHeap = new OffHeapKVStore(
                        ownerBackend.getManagedPagePool(), ownerBackend, ownerBackend.getL2TimeBucketSizeMillis());
                this.l2MapEntries = new NoOpCachePolicy<>();
            } else {
                if (l2ManagedMemoryEnabled && l2Size > 0) {
                    LOG.info("Falling back to on-heap L2 cache for this PerKeyMapCache because managed memory is not available.");
                }
                this.l2MapEntriesOffHeap = null;
                this.l2MapEntries = createCachePolicyInstance(cachePolicyType, l2Size, null,
                    ownerBackend, false, false);
            }

            this.l1MapEntries =
                    createCachePolicyInstance(cachePolicyType, l1Size, evictedL1MapEntry -> {
                        UK_C evictedUK = evictedL1MapEntry.getKey();
                        CacheEntry<UV_C> evictedUVWrapper = evictedL1MapEntry.getValue();

                        // --- START OF FIX ---
                        // Treat all L1 evicted entries as needing a flush to the delegate state.
                        // This changes the behavior from a pure write-back on dirty entries to a
                        // write-through on L1 eviction, which is safer and prevents data loss
                        // when clean entries are evicted from L2 later without being persisted.
                        K_F originalKeyContext = null;
                        N_F originalDelegateNamespaceContext = null;
                        try {
                            originalKeyContext = ownerBackend.getCurrentKey();
                            originalDelegateNamespaceContext = (N_F) CachingInternalMapState.this.getCurrentNamespace();
                            LOG.debug("L1-Evict-Flush: CONTEXT SWITCH. Original(key={}, ns={}). Setting to(key={}, ns={}) for UK {}",
                                    originalKeyContext, originalDelegateNamespaceContext,
                                    flinkKey, cacheNamespace, evictedL1MapEntry.getKey());

                            ownerBackend.setCurrentKey(flinkKey);
                            delegateState.setCurrentNamespace(cacheNamespace);

                            if (evictedUVWrapper.getValue() == null) { // Is a tombstone
                                delegateState.remove(evictedUK);
                            } else {
                                delegateState.put(evictedUK, evictedUVWrapper.getValue());
                                // Add to L2 cache only if it was a regular value, not a
                                // tombstone
                                try {
                                    if (l2ManagedMemoryEnabled && l2MapEntriesOffHeap != null) {
                                        byte[] serializedKey = serializeKey(evictedUK);
                                        byte[] serializedValue = serializeValue(evictedUVWrapper.getValue());
                                        try {
                                            this.l2MapEntriesOffHeap.put(serializedKey, serializedValue);
                                        } catch (Exception e) {
                                            Throwable cause = (e instanceof java.io.IOException) ? e.getCause() : e;
                                            if (cause instanceof org.apache.flink.runtime.memory.MemoryAllocationException) {
                                                LOG.info("L2 off-heap cache is full. Attempting to evict to make space for key '{}'.", evictedUK);
                                                long spaceNeeded = serializedKey.length + serializedValue.length + 128; // 128 bytes buffer
                                                long freed = this.l2MapEntriesOffHeap.evict(spaceNeeded);
                                                if (freed > 0) {
                                                    try {
                                                        this.l2MapEntriesOffHeap.put(serializedKey, serializedValue);
                                                    } catch (Exception e2) {
                                                        LOG.warn("Failed to add entry for key '{}' to L2 cache even after evicting {} bytes. This may impact performance.", evictedUK, freed, e2);
                                                    }
                                                } else {
                                                    LOG.warn("Could not evict any data from L2 cache for key '{}'. Entry will not be cached in L2. This may impact performance.", evictedUK);
                                                }
                                            } else {
                                                throw e;
                                            }
                                        }
                                    } else {
                                        this.l2MapEntries.put(evictedUK,
                                            CacheEntry.clean(evictedUVWrapper.getValue()));
                                    }
                                } catch (IOException e) {
                                    throw new RuntimeException("Failed to write to L2 cache during L1 eviction", e);
                                }
                            }
                            evictedUVWrapper.setDirty(false); // Mark clean after successful flush
                        } catch (Exception e) {
                            throw new RuntimeException(
                                    "Failed to flush L1 map entry to delegate for user key: "
                                            + evictedUK,
                                    e);
                        } finally {
                            K_F keyAfter = ownerBackend.getCurrentKey();
                            // We cannot get the namespace from the delegate, so we log what it was set to.
                            LOG.debug("L1-Evict-Flush: CONTEXT RESTORE. Before restore(key={}, ns={}). Restored to(key={}, ns={}). For UK {}",
                                    keyAfter, cacheNamespace,
                                    originalKeyContext, originalDelegateNamespaceContext, evictedL1MapEntry.getKey());
                            ownerBackend.setCurrentKey(originalKeyContext);
                            delegateState.setCurrentNamespace(originalDelegateNamespaceContext);
                        }
                        // --- END OF FIX ---
                    }, ownerBackend, false, false);
        }

        private void initializeViews() {
            this.userKeySerializerView =
                    ThreadLocal.withInitial(
                            () -> {
                                if (this.userKeySerializer != null) {
                                    return new DataOutputSerializer(128);
                                } else {
                                    return new DataOutputSerializer(0);
                                }
                            });

            this.userValueSerializerView =
                    ThreadLocal.withInitial(
                            () -> {
                                if (this.userValueSerializer != null) {
                                    return new DataOutputSerializer(128);
                                } else {
                                    return new DataOutputSerializer(0);
                                }
                            });
            this.userValueDeserializerView =
                    ThreadLocal.withInitial(
                            () -> {
                                if (this.userValueSerializer != null) {
                                    return new DataInputDeserializer();
                                } else {
                                    return new DataInputDeserializer(new byte[0]);
                                }
                            });
        }

        private <CK, CV_ENTRY_TYPE> CachePolicy<CK, CacheEntry<CV_ENTRY_TYPE>> createCachePolicyInstance(
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

            if (l2ManagedMemoryEnabled && l2MapEntriesOffHeap != null) {
                this.l2MapEntriesOffHeap.clear();
            } else {
                this.l2MapEntries.clear();
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

        private byte[] serializeKey(UK_C key) throws IOException {
            DataOutputSerializer dos = userKeySerializerView.get();
            dos.clear();
            userKeySerializer.serialize(key, dos);
            return dos.getCopyOfBuffer();
        }

        private UK_C deserializeKey(byte[] serializedKey) throws IOException {
            if (serializedKey == null) {
                return null;
            }
            DataInputDeserializer did = userKeyDeserializerView.get();
            did.setBuffer(serializedKey);
            return userKeySerializer.deserialize(did);
        }

        private byte[] serializeValue(UV_C value) throws IOException {
            DataOutputSerializer dos = userValueSerializerView.get();
            dos.clear();
            userValueSerializer.serialize(value, dos);
            return dos.getCopyOfBuffer();
        }

        private UV_C deserializeValue(byte[] serializedValue) throws IOException {
            if (serializedValue == null) {
                return null;
            }
            DataInputDeserializer did = userValueDeserializerView.get();
            did.setBuffer(serializedValue);
            return userValueSerializer.deserialize(did);
        }

        private long fingerprint(UK_C userKey) {
            try {
                DataOutputSerializer dos = userKeySerializerView.get();
                dos.clear();
                userKeySerializer.serialize(userKey, dos);
                byte[] bytes = dos.getSharedBuffer();
                int len = dos.length();
                MurmurHash3.LongPair out = new MurmurHash3.LongPair();
                MurmurHash3.murmurhash3_x64_128(bytes, 0, len, 0, out);
                return out.val1;
            } catch (IOException e) {
                // Should not happen with in-memory DataOutputSerializer
                throw new RuntimeException("Error serializing user key for presence cache fingerprint", e);
            }
        }

        private long fingerprint(byte[] serializedKey) {
            MurmurHash3.LongPair out = new MurmurHash3.LongPair();
            MurmurHash3.murmurhash3_x64_128(serializedKey, 0, serializedKey.length, 0, out);
            return out.val1;
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
            if (l2ManagedMemoryEnabled && l2MapEntriesOffHeap != null) {
                return l2MapEntriesOffHeap.size();
            } else {
                return l2MapEntries.size();
            }
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
            if (metricsRegistered) {
                return; // avoid duplicate registration with Prometheus reporters
            }
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

            // Off-heap L2 specific gauges
            if (l2ManagedMemoryEnabled && l2MapEntriesOffHeap != null) {
                metricGroup.gauge("l2OffHeapPages", () -> l2MapEntriesOffHeap.getPageCount());
                metricGroup.gauge("l2OffHeapBytes", () -> l2MapEntriesOffHeap.getEstimatedMemoryUsageBytes());
                metricGroup.gauge("l2OffHeapEntries", () -> l2MapEntriesOffHeap.size());
                metricGroup.gauge("l2OffHeapPagesFreedCapacity", () -> l2MapEntriesOffHeap.getPagesFreedByCapacity());
                metricGroup.gauge("l2OffHeapPagesFreedWatermark", () -> l2MapEntriesOffHeap.getPagesFreedByWatermark());
                metricGroup.gauge("l2OffHeapPagesFreedPerSec", () -> l2MapEntriesOffHeap.getApproxPagesFreedPerSecond());
            }

            metricsRegistered = true;
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
            if (l2ManagedMemoryEnabled && l2MapEntriesOffHeap != null) {
                totalSize += l2MapEntriesOffHeap.getEstimatedMemoryUsageBytes();
            } else {
                for (CacheEntry<UV_C> entry : l2MapEntries.values()) {
                    totalSize += entry.getEstimatedSizeBytes();
                }
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
            if (l2ManagedMemoryEnabled && l2MapEntriesOffHeap != null) {
                freedBytes += l2MapEntriesOffHeap.evict(bytesToFree);
            } else {
                freedBytes += evictFromCache(l2MapEntries, bytesToFree - freedBytes, false,
                        keyPresenceCacheEnabled);
            }
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
            long freedBytes = 0;

            List<Map.Entry<Long, Byte>> keysToRemove = new ArrayList<>();
            Iterator<Map.Entry<Long, Byte>> iter = cache.entrySet().iterator();
            while (iter.hasNext() && keysToRemove.size() < numToEvict) {
                keysToRemove.add(iter.next());
            }

            for (Map.Entry<Long, Byte> entry : keysToRemove) {
                Long key = entry.getKey();
                Byte value = entry.getValue();

                if (cache.remove(key) != null) {
                    if (cache == l1PrimitivePresenceCache) {
                        // Manually trigger the L1 eviction logic: demote to L2
                        l2PrimitivePresenceCache.put(key, value);
                        // The net memory change is 0, as an L1 entry is removed and an L2 entry is added.
                        // The ownerBackend's memory counter is correctly unchanged.
                        // We don't count this as "freed" bytes from the perspective of the caller,
                        // as the goal is to reduce total memory, which this action alone doesn't do.
                        // However, the `put` to L2 might evict an entry from L2, which *will* free memory
                        // and be reported by L2's eviction listener.
                    } else { // It is the l2PrimitivePresenceCache
                        // Manually trigger the L2 eviction logic
                        ownerBackend.reportCacheMemoryReleased(bytesPerEntry);
                        freedBytes += bytesPerEntry;
                    }
                }
            }
            return freedBytes;
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
            CachingStateBackendFactory.PresenceCacheImplementation mapPresenceCacheImpl,
            boolean l2ManagedMemoryEnabled,
            boolean perKeyMetricsEnabled,
            boolean forceBypassAlways) {
        this.delegateState = delegateState;
        this.backend = backend;

        // Get the value serializer safely
        TypeSerializer<Map<UK, UV>> valueSerializer = delegateState.getValueSerializer();
        if (valueSerializer == null) {
            throw new NullPointerException(
                    "Value serializer from delegate state is null. "
                            + "Ensure the delegate state is properly initialized before creating CachingInternalMapState.");
        }
        if (!(valueSerializer instanceof MapSerializer)) {
            throw new IllegalArgumentException(
                    "Value serializer must be a MapSerializer but was "
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
        // Initialize hysteresis thresholds after threshold is set
        this.lowHitRateThreshold = Math.max(0.0, this.mapCacheHitRateThreshold - 0.10);
        this.highHitRateThreshold = Math.min(1.0, this.mapCacheHitRateThreshold);
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
        this.l2ManagedMemoryEnabled = l2ManagedMemoryEnabled;
        this.perKeyMetricsEnabled = perKeyMetricsEnabled;
        this.forceBypassAlways = forceBypassAlways;

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
                                        perKeyCache.cacheNamespace, backend, delegateState, NCDN);
                                if (perKeyCache.l2ManagedMemoryEnabled && perKeyCache.l2MapEntriesOffHeap != null) {
                                    perKeyCache.l2MapEntriesOffHeap.clear();
                                } else {
                                    perKeyCache.l2MapEntries.clear(); // Should trigger memory reporting
                                }
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
        this.l1MapValueCacheHitCount = cacheMetrics.counter("l1ValueCacheHit");
        this.l1MapValueCacheMissCount = cacheMetrics.counter("l1ValueCacheMiss");
        this.l2MapValueCacheHitCount = cacheMetrics.counter("l2ValueCacheHit");
        this.l2MapValueCacheMissCount = cacheMetrics.counter("l2ValueCacheMiss");

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
        try {
            cacheMetrics.gauge("bypassActive", (Gauge<Integer>) () -> (bypassEnabled && bypassCache) ? 1 : 0);
            cacheMetrics.gauge("currentHitRateForBypass",
                    (Gauge<Double>) () -> {
                        if (accessesForHitRateWindow == null || hitsInHitRateWindow == null) return 0.0;
                        long acc = Math.max(1L, accessesForHitRateWindow.get());
                        return Math.min(1.0, Math.max(0.0, ((double) hitsInHitRateWindow.get()) / acc));
                    });
        } catch (Throwable t) {
            // best-effort only
        }

        // Register aggregate (low-cardinality) gauges on the state-level metrics group
        try {
            MetricGroup agg = cacheMetrics.addGroup("aggregate");
            agg.gauge("perKeyCaches", () -> countPerKeyCaches());
            agg.gauge("l1MapEntriesTotal", () -> sumL1Entries());
            agg.gauge("l2MapEntriesTotal", () -> sumL2Entries());
            agg.gauge("l2OffHeapPagesTotal", () -> sumOffHeapPages());
            agg.gauge("l2OffHeapBytesTotal", () -> sumOffHeapBytes());
            agg.gauge("l2OffHeapEntriesTotal", () -> sumOffHeapEntries());
            agg.gauge("l2OffHeapPagesFreedCapacityTotal", () -> sumOffHeapPagesFreedCapacity());
            agg.gauge("l2OffHeapPagesFreedWatermarkTotal", () -> sumOffHeapPagesFreedWatermark());
        } catch (Throwable t) {
            // best-effort only
        }

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
                namespaceCaches.computeIfAbsent(
                        namespace,
                        n -> {
                            // Create a new cache for Flink keys under this namespace
                            return createCachePolicyForHierarchicalCache(
                                    this.maxFlinkKeysWithActiveCachesPerNamespace,
                                    evictedFlinkKeyEntry -> {
                                        // When a FlinkKey's cache is evicted from its namespace
                                        // cache, flush its L1
                                        // entries
                                        PerKeyMapCache<UK, UV, K, N> perKeyCache =
                                                evictedFlinkKeyEntry.getValue();
                                        if (perKeyCache != null) {
                                            try {
                                                // Context for delegateState should be set to this
                                                // perKeyCache's
                                                // Flink key and namespace
                                                K originalKey = backend.getCurrentKey();
                                                N originalNamespace =
                                                        getCurrentNamespace(); // Corrected

                                                backend.setCurrentKey(perKeyCache.flinkKey);
                                                delegateState.setCurrentNamespace(
                                                        perKeyCache.cacheNamespace);

                                                flushL1Entries(
                                                        perKeyCache,
                                                        perKeyCache.flinkKey,
                                                        perKeyCache.cacheNamespace,
                                                        backend,
                                                        delegateState,
                                                        originalNamespace);
                                                if (perKeyCache.l2ManagedMemoryEnabled && perKeyCache.l2MapEntriesOffHeap != null) {
                                                    perKeyCache.l2MapEntriesOffHeap.clear();
                                                } else {
                                                    perKeyCache.l2MapEntries.clear();
                                                }
                                                if (perKeyCache.keyPresenceCacheEnabled) {
                                                    perKeyCache.l1KeyPresenceCache.clear();
                                                    perKeyCache.l2KeyPresenceCache.clear();
                                                }

                                                // Unregister metrics for this cache so that future
                                                // instances can
                                                // re-register cleanly.
                                                // perKeyCache.closeMetrics();

                                                // Restore original context
                                                if (originalKey != null)
                                                    backend.setCurrentKey(originalKey);
                                                else backend.setCurrentKey(null);
                                                if (originalNamespace != null)
                                                    delegateState.setCurrentNamespace(
                                                            originalNamespace);
                                                else delegateState.setCurrentNamespace(null);

                                            } catch (Exception e) {
                                                LOG.error(
                                                        "Error flushing PerKeyMapCache during Flink key eviction from namespace {}: Flink key {}",
                                                        n,
                                                        evictedFlinkKeyEntry.getKey(),
                                                        e);
                                                throw new RuntimeException(
                                                        "Error during Flink key cache eviction and flush for Flink key: "
                                                                + evictedFlinkKeyEntry.getKey(),
                                                        e);
                                            }
                                        }
                                    });
                        });

        return flinkKeyCaches.computeIfAbsent(
                key,
                k -> {
                    PerKeyMapCache<UK, UV, K, N> perKeyCache =
                            new PerKeyMapCache<>(
                                    this.l1CacheSizePerMap,
                                    this.l2CacheSizePerMap,
                                    this.delegateState,
                                    this.backend,
                                    k,
                                    namespace,
                                    this.cachePolicyType,
                                    this.mapL1KeyPresenceCacheSize,
                                    this.mapL2KeyPresenceCacheSize,
                                    this.keyPresenceCacheEnabled,
                                    this.mapPresenceCacheImpl,
                                    this.userKeySerializer,
                                    this.userValueSerializer,
                                    l2ManagedMemoryEnabled);
                    if (CachingInternalMapState.this.metrics != null && CachingInternalMapState.this.perKeyMetricsEnabled) {
                        perKeyCache.registerMetrics(CachingInternalMapState.this.metrics.addGroup("perKeyCache"), k,
                                getCurrentNamespace());
                    }
                    return perKeyCache;
                });

    }

    // Overloaded constructor with explicit maxActiveNamespacesInCache override for MapState
    public CachingInternalMapState(InternalMapState<K, N, UK, UV> delegateState,
            CachingKeyedStateBackend<K> backend, int l1CacheSizePerMap, int l2CacheSizePerMap,
            int maxFlinkKeysWithActiveCachesPerNamespace, long maxCacheMemoryMb,
            CachingStateBackendFactory.CachePolicyType cachePolicyType,
            int mapL1KeyPresenceCacheSize, int mapL2KeyPresenceCacheSize, MetricGroup metrics,
            double mapCacheHitRateThreshold, long mapCacheHitRateWindowSize,
            long mapCacheMinAccessesForBypassCheck, boolean enableKeyPresenceCache,
            boolean enableBypass,
            CachingStateBackendFactory.PresenceCacheImplementation mapPresenceCacheImpl,
            boolean l2ManagedMemoryEnabled,
            boolean perKeyMetricsEnabled,
            boolean forceBypassAlways,
            int maxActiveNamespacesInCacheOverride) {
        this.delegateState = delegateState;
        this.backend = backend;

        TypeSerializer<Map<UK, UV>> valueSerializer = delegateState.getValueSerializer();
        if (valueSerializer == null) {
            throw new NullPointerException(
                    "Value serializer from delegate state is null. "
                            + "Ensure the delegate state is properly initialized before creating CachingInternalMapState.");
        }
        if (!(valueSerializer instanceof MapSerializer)) {
            throw new IllegalArgumentException(
                    "Value serializer must be a MapSerializer but was "
                            + valueSerializer.getClass().getName());
        }
        MapSerializer<UK, UV> mapSerializer = (MapSerializer<UK, UV>) valueSerializer;
        this.userKeySerializer = mapSerializer.getKeySerializer();
        this.userValueSerializer = mapSerializer.getValueSerializer();

        this.l1CacheSizePerMap = l1CacheSizePerMap;
        this.l2CacheSizePerMap = l2CacheSizePerMap;
        this.maxFlinkKeysWithActiveCachesPerNamespace = maxFlinkKeysWithActiveCachesPerNamespace;
        this.maxActiveNamespacesInCache = maxActiveNamespacesInCacheOverride;
        this.cachePolicyType = cachePolicyType;
        this.mapL1KeyPresenceCacheSize = mapL1KeyPresenceCacheSize;
        this.mapL2KeyPresenceCacheSize = mapL2KeyPresenceCacheSize;

        this.mapCacheHitRateThreshold = mapCacheHitRateThreshold;
        this.lowHitRateThreshold = Math.max(0.0, this.mapCacheHitRateThreshold - 0.10);
        this.highHitRateThreshold = Math.min(1.0, this.mapCacheHitRateThreshold);
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

        this.keyPresenceCacheEnabled = enableKeyPresenceCache;
        this.bypassEnabled = enableBypass;
        this.mapPresenceCacheImpl = mapPresenceCacheImpl;
        this.l2ManagedMemoryEnabled = l2ManagedMemoryEnabled;
        this.perKeyMetricsEnabled = perKeyMetricsEnabled;
        this.forceBypassAlways = forceBypassAlways;

        this.namespaceCaches = createCachePolicyForHierarchicalCache(
                this.maxActiveNamespacesInCache, evictedNamespaceEntry -> {
                    CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> flinkKeyCaches =
                            evictedNamespaceEntry.getValue();
                    if (flinkKeyCaches != null) {
                        try {
                            K NCDK = backend.getCurrentKey();
                            N NCDN = getCurrentNamespace();

                            for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> flinkKeyEntry : flinkKeyCaches.entrySet()) {
                                PerKeyMapCache<UK, UV, K, N> perKeyCache = flinkKeyEntry.getValue();
                                backend.setCurrentKey(perKeyCache.flinkKey);
                                delegateState.setCurrentNamespace(perKeyCache.cacheNamespace);
                                flushL1Entries(perKeyCache, perKeyCache.flinkKey,
                                        perKeyCache.cacheNamespace, backend, delegateState, NCDN);
                                if (perKeyCache.l2ManagedMemoryEnabled && perKeyCache.l2MapEntriesOffHeap != null) {
                                    perKeyCache.l2MapEntriesOffHeap.clear();
                                } else {
                                    perKeyCache.l2MapEntries.clear();
                                }
                                if (perKeyCache.keyPresenceCacheEnabled) {
                                    perKeyCache.l1KeyPresenceCache.clear();
                                    perKeyCache.l2KeyPresenceCache.clear();
                                }
                                perKeyCache.closeMetrics();
                            }
                            if (NCDK != null) backend.setCurrentKey(NCDK); else backend.setCurrentKey(null);
                            if (NCDN != null) delegateState.setCurrentNamespace(NCDN); else delegateState.setCurrentNamespace(null);
                        } catch (Exception e) {
                            LOG.error("Error flushing PerKeyMapCache during namespace eviction: {}",
                                    evictedNamespaceEntry.getKey(), e);
                            throw new RuntimeException(
                                    "Error during namespace cache eviction and flush for namespace: "
                                            + evictedNamespaceEntry.getKey(), e);
                        }
                    }
                });
        // Initialize metrics just like the primary constructor
        this.metrics = metrics;
        MetricGroup cacheMetrics = metrics.addGroup("cache");
        this.l1MapValueCacheHitCount = cacheMetrics.counter("l1ValueCacheHit");
        this.l1MapValueCacheMissCount = cacheMetrics.counter("l1ValueCacheMiss");
        this.l2MapValueCacheHitCount = cacheMetrics.counter("l2ValueCacheHit");
        this.l2MapValueCacheMissCount = cacheMetrics.counter("l2ValueCacheMiss");

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
        try {
            cacheMetrics.gauge("bypassActive", (Gauge<Integer>) () -> (bypassEnabled && bypassCache) ? 1 : 0);
            cacheMetrics.gauge("currentHitRateForBypass",
                    (Gauge<Double>) () -> {
                        if (accessesForHitRateWindow == null || hitsInHitRateWindow == null) return 0.0;
                        long acc = Math.max(1L, accessesForHitRateWindow.get());
                        return Math.min(1.0, Math.max(0.0, ((double) hitsInHitRateWindow.get()) / acc));
                    });
        } catch (Throwable t) {
            // best-effort only
        }

        try {
            MetricGroup agg = cacheMetrics.addGroup("aggregate");
            agg.gauge("perKeyCaches", () -> countPerKeyCaches());
            agg.gauge("l1MapEntriesTotal", () -> sumL1Entries());
            agg.gauge("l2MapEntriesTotal", () -> sumL2Entries());
            agg.gauge("l2OffHeapPagesTotal", () -> sumOffHeapPages());
            agg.gauge("l2OffHeapBytesTotal", () -> sumOffHeapBytes());
            agg.gauge("l2OffHeapEntriesTotal", () -> sumOffHeapEntries());
            agg.gauge("l2OffHeapPagesFreedCapacityTotal", () -> sumOffHeapPagesFreedCapacity());
            agg.gauge("l2OffHeapPagesFreedWatermarkTotal", () -> sumOffHeapPagesFreedWatermark());
        } catch (Throwable t) {
            // best-effort only
        }
    }

    private long countPerKeyCaches() {
        long c = 0;
        for (CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> m : namespaceCaches.values()) {
            if (m == null) continue;
            for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> e : m.entrySet()) {
                if (e.getValue() != null) c++;
            }
        }
        return c;
    }

    private long sumL1Entries() {
        long s = 0;
        for (CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> m : namespaceCaches.values()) {
            if (m == null) continue;
            for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> e : m.entrySet()) {
                PerKeyMapCache<UK, UV, K, N> pc = e.getValue();
                if (pc != null) s += pc.l1MapEntries.size();
            }
        }
        return s;
    }

    private long sumL2Entries() {
        long s = 0;
        for (CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> m : namespaceCaches.values()) {
            if (m == null) continue;
            for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> e : m.entrySet()) {
                PerKeyMapCache<UK, UV, K, N> pc = e.getValue();
                if (pc == null) continue;
                if (pc.l2ManagedMemoryEnabled && pc.l2MapEntriesOffHeap != null) s += pc.l2MapEntriesOffHeap.size();
                else s += pc.l2MapEntries.size();
            }
        }
        return s;
    }

    private long sumOffHeapPages() {
        long s = 0;
        for (CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> m : namespaceCaches.values()) {
            if (m == null) continue;
            for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> e : m.entrySet()) {
                PerKeyMapCache<UK, UV, K, N> pc = e.getValue();
                if (pc != null && pc.l2ManagedMemoryEnabled && pc.l2MapEntriesOffHeap != null) s += pc.l2MapEntriesOffHeap.getPageCount();
            }
        }
        return s;
    }

    private long sumOffHeapBytes() {
        long s = 0;
        for (CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> m : namespaceCaches.values()) {
            if (m == null) continue;
            for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> e : m.entrySet()) {
                PerKeyMapCache<UK, UV, K, N> pc = e.getValue();
                if (pc != null && pc.l2ManagedMemoryEnabled && pc.l2MapEntriesOffHeap != null) s += pc.l2MapEntriesOffHeap.getEstimatedMemoryUsageBytes();
            }
        }
        return s;
    }

    private long sumOffHeapEntries() {
        long s = 0;
        for (CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> m : namespaceCaches.values()) {
            if (m == null) continue;
            for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> e : m.entrySet()) {
                PerKeyMapCache<UK, UV, K, N> pc = e.getValue();
                if (pc != null && pc.l2ManagedMemoryEnabled && pc.l2MapEntriesOffHeap != null) s += pc.l2MapEntriesOffHeap.size();
            }
        }
        return s;
    }

    private long sumOffHeapPagesFreedCapacity() {
        long s = 0;
        for (CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> m : namespaceCaches.values()) {
            if (m == null) continue;
            for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> e : m.entrySet()) {
                PerKeyMapCache<UK, UV, K, N> pc = e.getValue();
                if (pc != null && pc.l2ManagedMemoryEnabled && pc.l2MapEntriesOffHeap != null) s += pc.l2MapEntriesOffHeap.getPagesFreedByCapacity();
            }
        }
        return s;
    }

    private long sumOffHeapPagesFreedWatermark() {
        long s = 0;
        for (CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> m : namespaceCaches.values()) {
            if (m == null) continue;
            for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> e : m.entrySet()) {
                PerKeyMapCache<UK, UV, K, N> pc = e.getValue();
                if (pc != null && pc.l2ManagedMemoryEnabled && pc.l2MapEntriesOffHeap != null) s += pc.l2MapEntriesOffHeap.getPagesFreedByWatermark();
            }
        }
        return s;
    }

    // Hysteresis parameters for adaptive bypass
    private final double lowHitRateThreshold; // enter bypass below this
    private final double highHitRateThreshold; // exit bypass above this
    private final int enterConsecutiveLowWindows = 1;  // react quickly to poor locality
    private final int exitConsecutiveHighWindows = 2;  // require stability to re-enable cache
    private final int cooldownWindowsAfterToggle = 2;  // avoid thrashing after a decision

    private transient long completedWindows = 0L;
    private transient int consecutiveLow = 0;
    private transient int consecutiveHigh = 0;
    private transient int windowsSinceToggle = 0;

    private void updateCacheBypassCondition(boolean resolvedByCache) {
        updateCacheBypassConditionWeighted(resolvedByCache, 1);
    }

    // Weighted variant used during bypass sampling. When in bypass mode we sample only a fraction
    // of accesses; this method lets a single sample represent multiple accesses to avoid biasing
    // the hit-rate estimate toward zero and to let windows progress.
    private void updateCacheBypassConditionWeighted(boolean resolvedByCache, int weight) {
        if (weight <= 0) {
            return; // ignore zero/negative weights
        }
        if (!bypassEnabled || this.mapCacheHitRateThreshold <= 0.0
                || accessesForHitRateWindow == null || hitsInHitRateWindow == null
                || totalAccessesForBypassEligibility == null) {
            this.bypassCache = false;
            return;
        }

        if (resolvedByCache) {
            hitsInHitRateWindow.addAndGet(weight);
        }
        long currentWindowAccesses = accessesForHitRateWindow.addAndGet(weight);

        if (currentWindowAccesses >= this.mapCacheHitRateWindowSize) {
            long totalAccesses = totalAccessesForBypassEligibility.addAndGet(currentWindowAccesses);

            if (totalAccesses < this.mapCacheMinAccessesForBypassCheck) {
                accessesForHitRateWindow.set(0);
                hitsInHitRateWindow.set(0);
                this.bypassCache = false;
                return;
            }

            double currentHitRate = (double) hitsInHitRateWindow.get() / currentWindowAccesses;

            // Hysteresis window accounting
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
                }
            } else if (currentHitRate > highHitRateThreshold) {
                consecutiveHigh++;
                consecutiveLow = 0;
                if (!inCooldown && bypassCache && consecutiveHigh >= exitConsecutiveHighWindows) {
                    bypassCache = false; // exit bypass
                    windowsSinceToggle = 0;
                    consecutiveLow = 0;
                    consecutiveHigh = 0;
                }
            } else {
                // Between thresholds: drift toward stability
                consecutiveLow = 0;
                consecutiveHigh = 0;
            }

            // Advance cooldown window counter if we have recently toggled
            if (windowsSinceToggle < cooldownWindowsAfterToggle) {
                windowsSinceToggle++;
            }

            accessesForHitRateWindow.set(0);
            hitsInHitRateWindow.set(0);
        }
    }

    // Helper methods for serialization in the main class
    private byte[] serializeKey(UK key) throws IOException {
        DataOutputSerializer dos = new DataOutputSerializer(64);
        userKeySerializer.serialize(key, dos);
        return dos.getCopyOfBuffer();
    }

    private byte[] serializeValue(UV value) throws IOException {
        DataOutputSerializer dos = new DataOutputSerializer(128);
        userValueSerializer.serialize(value, dos);
        return dos.getCopyOfBuffer();
    }

    private UV deserializeValue(byte[] serializedValue) throws IOException {
        if (serializedValue == null) {
            return null;
        }
        DataInputDeserializer did = new DataInputDeserializer(serializedValue);
        return userValueSerializer.deserialize(did);
    }

    @Override
    public UV get(UK userKey) throws Exception {
        if (userKey == null)
            return null;
        delegateState.setCurrentNamespace(getCurrentNamespace());

        if (forceBypassAlways) {
            delegateLookups.inc();
            return delegateState.get(userKey);
        }

        // Prefer explicit hints; otherwise use auto-bypass if this is likely the left input.
        if (forceBypassAlways
                || Boolean.TRUE.equals(THREAD_LOCAL_BYPASS.get())
                || GLOBAL_BYPASS
                || isAutoBypassActiveForThisCall()) {
            delegateLookups.inc();
            return delegateState.get(userKey);
        }

        int decisionWeight = 1;
        if (bypassEnabled && bypassCache) {
            boolean isSample =
                    accessSampler != null && (accessSampler.incrementAndGet() % SAMPLING_RATE == 0);
            if (!isSample) {
                delegateLookups.inc();
                // In bypass mode, do not count unsampled accesses toward the hit-rate window to
                // avoid biasing hit rate to zero.
                return delegateState.get(userKey);
            }
            // Sampled access: account with weight so windows progress and estimate is unbiased.
            decisionWeight = SAMPLING_RATE;
        }

        try {
            PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();
            boolean resolvedByCache = false;
            UV userValue = null;

            if (!this.keyPresenceCacheEnabled) { // KV Separation DISABLED path
                CacheEntry<UV> l1Entry = perKeyCache.l1MapEntries.get(userKey);
                if (l1Entry != null) {
                    l1MapValueCacheHitCount.inc();
                    userValue = l1Entry.getValue(); // Could be null if tombstone
                    resolvedByCache = true;
                } else {
                    l1MapValueCacheMissCount.inc();
                    try {
                        CacheEntry<UV> l2Entry = null;
                        if (perKeyCache.l2ManagedMemoryEnabled && perKeyCache.l2MapEntriesOffHeap != null) {
                            byte[] serializedKey = serializeKey(userKey);
                            byte[] serializedL2Value = perKeyCache.l2MapEntriesOffHeap.get(serializedKey);
                            if (serializedL2Value != null) {
                                perKeyCache.l2MapEntriesOffHeap.remove(serializedKey);
                                userValue = deserializeValue(serializedL2Value);
                                l2Entry = CacheEntry.clean(userValue);
                            }
                        } else {
                            l2Entry = perKeyCache.l2MapEntries.remove(userKey);
                            if (l2Entry != null) {
                                userValue = l2Entry.getValue();
                            }
                        }
                        
                        if (l2Entry != null || userValue != null) {
                            l2MapValueCacheHitCount.inc();
                            // L2 entry removal will trigger the listener to report memory released.
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
                            l2MapValueCacheMissCount.inc();
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
                    } catch (IOException e) {
                        throw new RuntimeException("Error during L2 cache access", e);
                    }
                }
                updateCacheBypassConditionWeighted(resolvedByCache && userValue != null, decisionWeight);
                return userValue;
            }

            // KV Separation ENABLED path
            ValuePresence presence = perKeyCache.getValuePresence(userKey);

            if (presence == ValuePresence.ABSENT_IN_CACHE) {
                l1PresenceCacheHitCount.inc(); // Hit in presence cache (L1 or promoted L2) indicating
                                               // absence
                updateCacheBypassConditionWeighted(true, decisionWeight); // Resolved by cache as definitively absent
                return null;
            }

            // Check L1 Value Cache regardless of initial presence outcome (unless ABSENT_IN_CACHE)
            CacheEntry<UV> l1ValEntry = perKeyCache.l1MapEntries.get(userKey);
            if (l1ValEntry != null) {
                l1MapValueCacheHitCount.inc();
                // If presence was uncertain, this L1 value hit resolves it.
                // If presence said PRESENT_IN_CACHE_CLEAN, this confirms the value part.
                if (presence == ValuePresence.ABSENT_MAYBE_IN_VALUE_CACHE)
                    l1PresenceCacheMissCount.inc(); // Count initial presence miss
                else
                    l1PresenceCacheHitCount.inc(); // Count presence hit that led here

                userValue = l1ValEntry.getValue(); // Could be null if it's a tombstone
                resolvedByCache = true;
            } else {
                l1MapValueCacheMissCount.inc();
                // If presence cache said PRESENT_IN_CACHE_CLEAN, but L1 value is a miss, this is a
                // slight inconsistency
                // or means it was just evicted from L1 value to L2 value. Log for observation if strict
                // consistency expected.
                if (presence == ValuePresence.PRESENT_IN_CACHE_CLEAN) {
                    l1PresenceCacheHitCount.inc();
                    LOG.debug(
                            "L1 Presence cache indicated key {} exists, but value not found in L1 value cache. Checking L2 value cache.",
                            userKey);
                } else if (presence == ValuePresence.ABSENT_MAYBE_IN_VALUE_CACHE) {
                    l1PresenceCacheMissCount.inc(); // Miss in presence, now L1 value also missed.
                }

                // Check L2 Value Cache
                try {
                    boolean foundInL2 = false;
                    if (perKeyCache.l2ManagedMemoryEnabled && perKeyCache.l2MapEntriesOffHeap != null) {
                        byte[] serializedKey = serializeKey(userKey);
                        byte[] serializedL2Value = perKeyCache.l2MapEntriesOffHeap.get(serializedKey);
                        if (serializedL2Value != null) {
                            perKeyCache.l2MapEntriesOffHeap.remove(serializedKey);
                            userValue = deserializeValue(serializedL2Value);
                            foundInL2 = true;
                        }
                    } else {
                        CacheEntry<UV> l2Entry = perKeyCache.l2MapEntries.remove(userKey);
                        if (l2Entry != null) {
                            userValue = l2Entry.getValue();
                            foundInL2 = true;
                        }
                    }
                    
                    if (foundInL2) {
                        l2MapValueCacheHitCount.inc();
                        CacheEntry<UV> newL1Entry = CacheEntry.clean(userValue);
                        CacheEntry<UV> oldL1Entry = perKeyCache.l1MapEntries.put(userKey, newL1Entry);
                        if (oldL1Entry != null) {
                            perKeyCache.ownerBackend.reportCacheMemoryReleased(
                                    oldL1Entry.getEstimatedSizeBytes());
                        }
                        perKeyCache.ownerBackend.reportCacheMemoryAdded(newL1Entry.getEstimatedSizeBytes());
                        resolvedByCache = true;
                    } else {
                        l2MapValueCacheMissCount.inc();
                        if (perKeyCache.fullyLoaded) {
                            perKeyCache.updatePresenceCacheOnGet(userKey, false);
                            updateCacheBypassConditionWeighted(true, decisionWeight);
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
                } catch (IOException e) {
                    throw new RuntimeException("Error during L2 cache access", e);
                }
            }

            // Update presence cache based on the final outcome from value caches or delegate.
            perKeyCache.updatePresenceCacheOnGet(userKey, userValue != null);
            updateCacheBypassConditionWeighted(resolvedByCache && userValue != null, decisionWeight); // Hit if found in cache &
                                                                              // not tombstone
            return userValue;
        } catch (IOException e) {
            throw new RuntimeException("Error during cache access", e);
        }
    }

    @Override
    public void put(UK userKey, UV userValue) throws Exception {
        LOG.info("Putting value for key {}", userKey);
        if (userKey == null) {
            /* let delegate handle or throw */ return;
        }
        if (userValue == null) {
            remove(userKey); // Standard map behavior for put(key, null)
            return;
        }
        delegateState.setCurrentNamespace(getCurrentNamespace());

        if (forceBypassAlways
                || Boolean.TRUE.equals(THREAD_LOCAL_BYPASS.get())
                || GLOBAL_BYPASS
                || isAutoBypassActiveForThisCall()) {
            delegateState.put(userKey, userValue);
            return;
        }

        if (bypassEnabled && bypassCache) {
            boolean isSample =
                    accessSampler != null && (accessSampler.incrementAndGet() % SAMPLING_RATE == 0);
            if (!isSample) {
                delegateState.put(userKey, userValue);
                // Do not update hit-rate window for unsampled bypass writes.
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
        try {
            if (perKeyCache.l2ManagedMemoryEnabled && perKeyCache.l2MapEntriesOffHeap != null) {
                byte[] serializedKey = serializeKey(userKey);
                perKeyCache.l2MapEntriesOffHeap.remove(serializedKey);
            } else {
                perKeyCache.l2MapEntries.remove(userKey);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize key for L2 remove", e);
        }

        if (this.keyPresenceCacheEnabled) {
            perKeyCache.updatePresenceCacheOnPut(userKey);
        }
        perKeyCache.fullyLoaded = false; // A put might change the full set of keys
        updateCacheBypassCondition(true); // Leave as-is: primes toward enabling cache after writes
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

        // If left side or explicit bypass, write-through directly.
        if (forceBypassAlways
                || Boolean.TRUE.equals(THREAD_LOCAL_BYPASS.get())
                || GLOBAL_BYPASS
                || isAutoBypassActiveForThisCall()) {
            delegateState.setCurrentNamespace(getCurrentNamespace());
            for (Map.Entry<UK, UV> entry : map.entrySet()) {
                if (entry.getKey() == null) continue;
                if (entry.getValue() == null) {
                    delegateState.remove(entry.getKey());
                } else {
                    delegateState.put(entry.getKey(), entry.getValue());
                }
            }
            return;
        }

        for (Map.Entry<UK, UV> entry : map.entrySet()) {
            this.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void remove(UK userKey) throws Exception {
        if (userKey == null) {
            /* let delegate handle or throw */ return;
        }
        delegateState.setCurrentNamespace(getCurrentNamespace());

        if (forceBypassAlways
                || Boolean.TRUE.equals(THREAD_LOCAL_BYPASS.get())
                || GLOBAL_BYPASS
                || isAutoBypassActiveForThisCall()) {
            delegateState.remove(userKey);
            return;
        }

        if (bypassEnabled && bypassCache) {
            boolean isSample =
                    accessSampler != null && (accessSampler.incrementAndGet() % SAMPLING_RATE == 0);
            if (!isSample) {
                delegateState.remove(userKey);
                // Do not update hit-rate window for unsampled bypass removes.
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

        try {
            if (perKeyCache.l2ManagedMemoryEnabled && perKeyCache.l2MapEntriesOffHeap != null) {
                byte[] serializedKey = serializeKey(userKey);
                perKeyCache.l2MapEntriesOffHeap.remove(serializedKey);
            } else {
                perKeyCache.l2MapEntries.remove(userKey);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize key for L2 remove", e);
        }
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
        if (userKey == null) {
            return false;
        }
        delegateState.setCurrentNamespace(getCurrentNamespace());

        if (forceBypassAlways
                || Boolean.TRUE.equals(THREAD_LOCAL_BYPASS.get())
                || GLOBAL_BYPASS
                || isAutoBypassActiveForThisCall()) {
            return delegateState.contains(userKey);
        }

        int containsWeight = 1;
        if (bypassEnabled && bypassCache) {
            boolean isSample =
                    accessSampler != null && (accessSampler.incrementAndGet() % SAMPLING_RATE == 0);
            if (!isSample) {
                // Do not skew hit rate with unsampled bypass reads
                return delegateState.contains(userKey);
            }
            containsWeight = SAMPLING_RATE;
        }

        try {
            PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

            // 1. Check L1, it has the most up-to-date information.
            CacheEntry<UV> l1Entry = perKeyCache.l1MapEntries.get(userKey);
            if (l1Entry != null) {
                updateCacheBypassConditionWeighted(true, containsWeight); // L1 hit.
                return l1Entry.getValue() != null; // A null value is a tombstone (doesn't exist).
            }

                    // 2. If fully loaded, the cache is the source of truth. If not in L1, check L2.
        if (perKeyCache.fullyLoaded) {
            updateCacheBypassConditionWeighted(true, containsWeight); // Resolved from cache, even if absent.
            try {
                if (perKeyCache.l2ManagedMemoryEnabled && perKeyCache.l2MapEntriesOffHeap != null) {
                    byte[] serializedKey = serializeKey(userKey);
                    return perKeyCache.l2MapEntriesOffHeap.get(serializedKey) != null;
                } else {
                    return perKeyCache.l2MapEntries.get(userKey) != null;
                }
            } catch (IOException e) {
                throw new RuntimeException("Error during L2 cache access for contains", e);
            }
        }

            // --- Not fully loaded and not in L1 ---

            // 3. Check presence cache.
            if (keyPresenceCacheEnabled) {
                ValuePresence presence = perKeyCache.getValuePresence(userKey);
                if (presence == ValuePresence.ABSENT_IN_CACHE) {
                    updateCacheBypassConditionWeighted(true, containsWeight); // A hit on "absence" information.
                    return false;
                }
                if (presence == ValuePresence.PRESENT_IN_CACHE_CLEAN) {
                    updateCacheBypassConditionWeighted(true, containsWeight); // A hit on "presence" information.
                    // This implies it exists in the delegate, even if the value isn't in the value
                    // cache.
                    return true;
                }
                // Fall through if presence information is inconclusive.
            }

            // 4. Check L2 value cache.
            try {
                boolean foundInL2 = false;
                UV l2Value = null;
                if (perKeyCache.l2ManagedMemoryEnabled && perKeyCache.l2MapEntriesOffHeap != null) {
                    byte[] serializedKey = serializeKey(userKey);
                    byte[] serializedL2Value = perKeyCache.l2MapEntriesOffHeap.get(serializedKey);
                    if (serializedL2Value != null) {
                        l2Value = deserializeValue(serializedL2Value);
                        foundInL2 = true;
                    }
                } else {
                    CacheEntry<UV> l2Entry = perKeyCache.l2MapEntries.get(userKey);
                    if (l2Entry != null) {
                        l2Value = l2Entry.getValue();
                        foundInL2 = true;
                    }
                }
                
                if (foundInL2) {
                    updateCacheBypassConditionWeighted(true, containsWeight); // L2 hit.
                    // Promote to L1.
                    perKeyCache.l1MapEntries.put(userKey, CacheEntry.clean(l2Value));
                    return true;
                }
            } catch (IOException e) {
                throw new RuntimeException("Error during L2 cache access for contains", e);
            }

            // 5. Cache miss, consult the delegate.
            updateCacheBypassConditionWeighted(false, containsWeight);
            boolean exists = delegateState.contains(userKey);

            // 6. Update caches with information from delegate.
            if (keyPresenceCacheEnabled) {
                perKeyCache.updatePresenceCacheOnGet(userKey, exists);
            }
            // Note: We do not cache the value itself here, only its presence. `get()` would cache the
            // value.

            return exists;
        } catch (IOException e) {
            throw new RuntimeException("Error during cache access for contains", e);
        }
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        // This iterable will produce a new UnionIterator on each call to iterator().
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
        if (perKeyCache.l2ManagedMemoryEnabled && perKeyCache.l2MapEntriesOffHeap != null) {
            perKeyCache.l2MapEntriesOffHeap.clear();
        } else {
            perKeyCache.l2MapEntries.clear(); // Will trigger memory release via listeners
        }
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
            if (perKeyCache.l2ManagedMemoryEnabled && perKeyCache.l2MapEntriesOffHeap != null) {
                return perKeyCache.l2MapEntriesOffHeap.isEmpty();
            } else {
                return perKeyCache.l2MapEntries.isEmpty();
            }
        }

        // Not fully loaded. Check L2 for an entry not covered by an L1 tombstone.
        boolean l2HasEntries;
        if (perKeyCache.l2ManagedMemoryEnabled && perKeyCache.l2MapEntriesOffHeap != null) {
            l2HasEntries = !perKeyCache.l2MapEntriesOffHeap.isEmpty();
        } else {
            l2HasEntries = !perKeyCache.l2MapEntries.isEmpty();
        }
        
        if (l2HasEntries) {
            // This is an approximation. A more correct implementation would need to iterate L2
            // and check against L1 tombstones. For performance, we accept this simplification.
            // If L2 has entries, we assume the map is not empty, even if some might be covered by L1 tombstones.
            return false;
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
            try (UnionIterator it = new UnionIterator(perKeyCache, delegateEntries)) {
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
        if (perKeyCache.l2ManagedMemoryEnabled && perKeyCache.l2MapEntriesOffHeap != null) {
            perKeyCache.l2MapEntriesOffHeap.clear();
        } else {
            perKeyCache.l2MapEntries.clear();
        }

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
        K originalKey = backend.getCurrentKey();
        // Do not call getCurrentNamespace() here because it may be unset during snapshots
        N originalNamespace = this.currentNamespace;
        try {
            for (Map.Entry<N, CachePolicy<K, PerKeyMapCache<UK, UV, K, N>>> nsEntry : namespaceCaches
                    .entrySet()) {
                N namespace = nsEntry.getKey();
                CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> keyCaches = nsEntry.getValue();

                // To avoid ConcurrentModificationException, we iterate over a copy of the entries.
                // This is safer and more efficient than iterating over keys and then looking up
                // the values, which might change cache order (e.g., in an LRU cache).
                List<Map.Entry<K, PerKeyMapCache<UK, UV, K, N>>> keyEntries = new ArrayList<>();
                for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> entry : keyCaches.entrySet()) {
                    keyEntries.add(entry);
                }
                for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> keyEntry : keyEntries) {
                    K flinkKey = keyEntry.getKey();
                    PerKeyMapCache<UK, UV, K, N> perKeyCache = keyEntry.getValue();
                    if (perKeyCache != null) {
                        try {
                            if (flinkKey != null) {
                                flushL1Entries(
                                        perKeyCache,
                                        flinkKey,
                                        namespace,
                                        this.backend,
                                        this.delegateState,
                                        originalNamespace);
                            }
                        } catch (Exception e) {
                            throw new IOException(
                                    "Failed to flush map entries for Flink key: "
                                            + flinkKey
                                            + " in namespace: "
                                            + namespace,
                                    e);
                        }
                    }
                }
            }
        } finally {
            backend.setCurrentKey(originalKey);
            // Restore namespace context only if it was previously set
            if (originalNamespace != null) {
                setCurrentNamespace(originalNamespace);
            } else {
                this.currentNamespace = null;
            }
        }
    }

    @Override
    public InternalMapState<K, N, UK, UV> getDelegateState() {
        return delegateState;
    }

    /**
     * Trigger time-bucketed eviction on all off-heap L2 caches based on a watermark.
     * This does nothing if off-heap L2 is disabled.
     */
    public void onWatermarkEvict(long watermarkMillis) {
        try {
            for (Map.Entry<N, CachePolicy<K, PerKeyMapCache<UK, UV, K, N>>> nsEntry : namespaceCaches.entrySet()) {
                CachePolicy<K, PerKeyMapCache<UK, UV, K, N>> keyCaches = nsEntry.getValue();
                if (keyCaches == null) continue;
                // iterate over a snapshot to avoid CME
                java.util.List<PerKeyMapCache<UK, UV, K, N>> caches = new java.util.ArrayList<>();
                for (Map.Entry<K, PerKeyMapCache<UK, UV, K, N>> e : keyCaches.entrySet()) {
                    if (e.getValue() != null) caches.add(e.getValue());
                }
                for (PerKeyMapCache<UK, UV, K, N> perKey : caches) {
                    if (perKey.l2ManagedMemoryEnabled && perKey.l2MapEntriesOffHeap != null) {
                        perKey.l2MapEntriesOffHeap.evictBucketsUpTo(watermarkMillis);
                    }
                }
            }
        } catch (Throwable t) {
            LOG.debug("onWatermarkEvict failed: {}", t.getMessage());
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
    public TypeSerializer<Map<UK, UV>> getValueSerializer() {
        return delegateState.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        this.currentNamespace = namespace;
        // Avoid setting a null namespace on the delegate; it may not accept null.
        if (namespace != null) {
            this.delegateState.setCurrentNamespace(namespace);
        }
    }

    public N getCurrentNamespace() {
        // May be null during snapshot/initialization; callers must handle null.
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
                        LOG.debug("evictEntriesToFreeMemory: CONTEXT SWITCH for per-key eviction. Original(key={}, ns={}). Setting to(key={}, ns={}).",
                                originalKey, originalNamespace,
                                perKeyCache.flinkKey, perKeyCache.cacheNamespace);
                        backend.setCurrentKey(perKeyCache.flinkKey); // Set context for this specific
                                                                     // key's cache
                        delegateState.setCurrentNamespace(perKeyCache.cacheNamespace);

                        long freedThisCache = perKeyCache
                                .evictToMeetMemoryLimit(targetBytesToFreeThisState - totalFreedBytes);
                        totalFreedBytes += freedThisCache;
                    } finally {
                        K keyAfter = backend.getCurrentKey();
                        N nsAfter = getCurrentNamespace();
                        // Restore original context
                        backend.setCurrentKey(originalKey);
                        delegateState.setCurrentNamespace(originalNamespace);
                        LOG.debug("evictEntriesToFreeMemory: CONTEXT RESTORE. Before restore(key={}, ns={}). Restored to(key={}, ns={}).",
                                keyAfter, nsAfter,
                                originalKey, originalNamespace);
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
        // Respect explicit/global/auto bypass for iteration to avoid touching caches
        if (forceBypassAlways
                || Boolean.TRUE.equals(THREAD_LOCAL_NO_TOUCH.get())
                || Boolean.TRUE.equals(THREAD_LOCAL_BYPASS.get())
                || GLOBAL_BYPASS
                || GLOBAL_NO_TOUCH
                || isAutoBypassActiveForThisCall()) {
            delegateState.setCurrentNamespace(getCurrentNamespace());
            final Iterable<Map.Entry<UK, UV>> delegateEntries = delegateState.entries();
            return delegateEntries == null
                    ? java.util.Collections.<Map.Entry<UK, UV>>emptyList().iterator()
                    : delegateEntries.iterator();
        }

        delegateState.setCurrentNamespace(getCurrentNamespace());
        final PerKeyMapCache<UK, UV, K, N> perKeyCache = getOrCreatePerKeyMapCache();

        // If we must consult the delegate, we need its entries.
        // This is the only place that should request a full iterator from the delegate state.
        final Iterable<Map.Entry<UK, UV>> delegateEntries =
                perKeyCache.fullyLoaded ? null : delegateState.entries();

        return new UnionIterator(perKeyCache, delegateEntries);
    }

    private static final class VisitedKeySet {
        private static final float LOAD_FACTOR = 0.6f;
        private long[] keys;
        private boolean[] occupied;
        private int mask;
        private int size;

        VisitedKeySet(int expected) {
            int capacity = 1;
            int target = expected <= 0 ? 4 : (int) Math.ceil(expected / LOAD_FACTOR);
            while (capacity < target) {
                capacity <<= 1;
            }
            keys = new long[capacity];
            occupied = new boolean[capacity];
            mask = capacity - 1;
        }

        boolean add(long key) {
            ensureCapacity();
            int idx = mix(key) & mask;
            while (occupied[idx]) {
                if (keys[idx] == key) {
                    return false;
                }
                idx = (idx + 1) & mask;
            }
            occupied[idx] = true;
            keys[idx] = key;
            size++;
            return true;
        }

        boolean contains(long key) {
            int idx = mix(key) & mask;
            while (occupied[idx]) {
                if (keys[idx] == key) {
                    return true;
                }
                idx = (idx + 1) & mask;
            }
            return false;
        }

        private void ensureCapacity() {
            if ((float) (size + 1) / keys.length <= LOAD_FACTOR) {
                return;
            }
            int newCapacity = keys.length << 1;
            long[] newKeys = new long[newCapacity];
            boolean[] newOccupied = new boolean[newCapacity];
            int newMask = newCapacity - 1;
            for (int i = 0; i < keys.length; i++) {
                if (!occupied[i]) {
                    continue;
                }
                long key = keys[i];
                int idx = mix(key) & newMask;
                while (newOccupied[idx]) {
                    idx = (idx + 1) & newMask;
                }
                newOccupied[idx] = true;
                newKeys[idx] = key;
            }
            keys = newKeys;
            occupied = newOccupied;
            mask = newMask;
        }

        private int mix(long key) {
            key ^= (key >>> 33);
            key *= 0xff51afd7ed558ccdL;
            key ^= (key >>> 33);
            key *= 0xc4ceb9fe1a85ec53L;
            key ^= (key >>> 33);
            return (int) key;
        }
    }

    private class UnionIterator implements Iterator<Map.Entry<UK, UV>>, AutoCloseable {

        private final PerKeyMapCache<UK, UV, K, N> perKeyCache;
        private final Iterator<Map.Entry<UK, CacheEntry<UV>>> l1Iterator;
        private final Iterator<?> l2Iterator;
        private final Iterator<Map.Entry<UK, UV>> delegateIterator;
        private final boolean usingOffHeapL2;
        private final VisitedKeySet visited;
        private Map.Entry<UK, UV> nextEntry;

        UnionIterator(
                PerKeyMapCache<UK, UV, K, N> perKeyCache,
                Iterable<Map.Entry<UK, UV>> delegateEntries) {
            this.perKeyCache = perKeyCache;
            this.l1Iterator = perKeyCache.l1MapEntries.entrySet().iterator();
            this.usingOffHeapL2 =
                    perKeyCache.l2ManagedMemoryEnabled && perKeyCache.l2MapEntriesOffHeap != null;
            this.l2Iterator =
                    usingOffHeapL2
                            ? perKeyCache.l2MapEntriesOffHeap.keyIterator()
                            : perKeyCache.l2MapEntries.entrySet().iterator();
            this.delegateIterator = delegateEntries != null ? delegateEntries.iterator() : null;
            int l1Size = perKeyCache.l1MapEntries.size();
            int l2Size =
                    usingOffHeapL2
                            ? perKeyCache.l2MapEntriesOffHeap.size()
                            : perKeyCache.l2MapEntries.size();
            this.visited = new VisitedKeySet(l1Size + l2Size + 4);
            advance();
        }

        @Override
        public boolean hasNext() {
            if (nextEntry != null) {
                return true;
            }
            advance();
            return nextEntry != null;
        }

        @Override
        public Map.Entry<UK, UV> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Map.Entry<UK, UV> current = nextEntry;
            nextEntry = null;
            return current;
        }

        private void advance() {
            if (nextEntry != null) {
                return;
            }
            nextEntry = null;
            if (emitFromL1()) {
                return;
            }
            if (emitFromL2()) {
                return;
            }
            emitFromDelegate();
        }

        private boolean emitFromL1() {
            while (l1Iterator.hasNext()) {
                Map.Entry<UK, CacheEntry<UV>> entry = l1Iterator.next();
                CacheEntry<UV> cacheEntry = entry.getValue();
                if (cacheEntry == null) {
                    continue;
                }
                long fingerprint = perKeyCache.fingerprint(entry.getKey());
                if (!visited.add(fingerprint)) {
                    continue;
                }
                UV value = cacheEntry.getValue();
                if (value == null) {
                    continue;
                }
                nextEntry = new AbstractMap.SimpleEntry<>(entry.getKey(), value);
                return true;
            }
            return false;
        }

        private boolean emitFromL2() {
            return usingOffHeapL2 ? emitFromL2OffHeap() : emitFromL2OnHeap();
        }

        @SuppressWarnings("unchecked")
        private boolean emitFromL2OnHeap() {
            Iterator<Map.Entry<UK, CacheEntry<UV>>> iterator =
                    (Iterator<Map.Entry<UK, CacheEntry<UV>>>) l2Iterator;
            while (iterator.hasNext()) {
                Map.Entry<UK, CacheEntry<UV>> entry = iterator.next();
                CacheEntry<UV> cacheEntry = entry.getValue();
                if (cacheEntry == null) {
                    continue;
                }
                long fingerprint = perKeyCache.fingerprint(entry.getKey());
                if (!visited.add(fingerprint)) {
                    continue;
                }
                UV value = cacheEntry.getValue();
                if (value == null) {
                    continue;
                }
                nextEntry = new AbstractMap.SimpleEntry<>(entry.getKey(), value);
                return true;
            }
            return false;
        }

        @SuppressWarnings("unchecked")
        private boolean emitFromL2OffHeap() {
            Iterator<byte[]> iterator = (Iterator<byte[]>) l2Iterator;
            while (iterator.hasNext()) {
                byte[] keyBytes = iterator.next();
                long fingerprint = perKeyCache.fingerprint(keyBytes);
                if (!visited.add(fingerprint)) {
                    continue;
                }
                byte[] valueBytes = perKeyCache.l2MapEntriesOffHeap.get(keyBytes);
                if (valueBytes == null) {
                    continue;
                }
                try {
                    UK key = perKeyCache.deserializeKey(keyBytes);
                    UV value = perKeyCache.deserializeValue(valueBytes);
                    if (value == null) {
                        continue;
                    }
                    nextEntry = new AbstractMap.SimpleEntry<>(key, value);
                    return true;
                } catch (IOException e) {
                    throw new RuntimeException("Failed to deserialize L2 off-heap map entry", e);
                }
            }
            return false;
        }

        private boolean emitFromDelegate() {
            if (delegateIterator == null) {
                return false;
            }
            while (delegateIterator.hasNext()) {
                Map.Entry<UK, UV> entry = delegateIterator.next();
                long fingerprint = perKeyCache.fingerprint(entry.getKey());
                if (!visited.add(fingerprint)) {
                    continue;
                }
                UV value = entry.getValue();
                if (value == null) {
                    continue;
                }
                nextEntry = entry;
                return true;
            }
            return false;
        }

        @Override
        public void close() {
            // Nothing to close
        }
    }

    private <K_F, N_F, UK_C, UV_C> void flushL1Entries(
            PerKeyMapCache<UK_C, UV_C, K_F, N_F> perKeyCache, K_F flinkKey, N_F namespace,
            CachingKeyedStateBackend<K_F> backendForContext,
            InternalMapState<K_F, N_F, UK_C, UV_C> delegateStateForContext,
            N_F originalNamespaceToRestore) throws Exception {

        if (perKeyCache.l1MapEntries.isEmpty()) {
            return;
        }

        // Set context for the duration of this flush
        backendForContext.setCurrentKey(flinkKey);
        delegateStateForContext.setCurrentNamespace(namespace);

        try {
            for (Map.Entry<UK_C, CacheEntry<UV_C>> l1Entry : perKeyCache.l1MapEntries.entrySet()) {
                UK_C userKey = l1Entry.getKey();
                CacheEntry<UV_C> cacheEntry = l1Entry.getValue();
                if (cacheEntry.isDirty()) {
                    UV_C userValue = cacheEntry.getValue();
                    if (userValue == null) { // Tombstone
                        delegateStateForContext.remove(userKey);
                    } else {
                        delegateStateForContext.put(userKey, userValue);
                        // After flushing a dirty entry from L1, it's clean and can be moved to L2
                        if (perKeyCache.l2ManagedMemoryEnabled && perKeyCache.l2MapEntriesOffHeap != null) {
                            byte[] serializedKey = perKeyCache.serializeKey(userKey);
                            byte[] serializedValue = perKeyCache.serializeValue(userValue);
                            try {
                                perKeyCache.l2MapEntriesOffHeap.put(serializedKey, serializedValue);
                            } catch (IOException ioe) {
                                Throwable cause = ioe.getCause();
                                if (cause instanceof org.apache.flink.runtime.memory.MemoryAllocationException) {
                                    long freed = perKeyCache.l2MapEntriesOffHeap.evict(serializedKey.length + serializedValue.length + 128L);
                                    if (freed > 0) {
                                        try {
                                            perKeyCache.l2MapEntriesOffHeap.put(serializedKey, serializedValue);
                                        } catch (IOException retry) {
                                            LOG.warn("L2 off-heap put still failed after evicting {} bytes.", freed, retry);
                                        }
                                    } else {
                                        LOG.warn("L2 off-heap eviction freed 0 bytes; skipping L2 insert.");
                                    }
                                } else {
                                    throw ioe;
                                }
                            }
                        } else if (perKeyCache.l2MapEntries.getClass() != NoOpCachePolicy.class) {
                            perKeyCache.l2MapEntries.put(userKey, CacheEntry.clean(userValue));
                        }
                    }
                    cacheEntry.setDirty(false); // Mark as clean
                } else if (cacheEntry.getValue() != null) { // Clean entry, move to L2
                    if (perKeyCache.l2ManagedMemoryEnabled && perKeyCache.l2MapEntriesOffHeap != null) {
                        byte[] serializedKey = perKeyCache.serializeKey(userKey);
                        byte[] serializedValue = perKeyCache.serializeValue(cacheEntry.getValue());
                        try {
                            perKeyCache.l2MapEntriesOffHeap.put(serializedKey, serializedValue);
                        } catch (IOException ioe) {
                            Throwable cause = ioe.getCause();
                            if (cause instanceof org.apache.flink.runtime.memory.MemoryAllocationException) {
                                long freed = perKeyCache.l2MapEntriesOffHeap.evict(serializedKey.length + serializedValue.length + 128L);
                                if (freed > 0) {
                                    try {
                                        perKeyCache.l2MapEntriesOffHeap.put(serializedKey, serializedValue);
                                    } catch (IOException retry) {
                                        LOG.warn("L2 off-heap put still failed after evicting {} bytes.", freed, retry);
                                    }
                                } else {
                                    LOG.warn("L2 off-heap eviction freed 0 bytes; skipping L2 insert.");
                                }
                            } else {
                                throw ioe;
                            }
                        }
                    } else if (perKeyCache.l2MapEntries.getClass() != NoOpCachePolicy.class) {
                        perKeyCache.l2MapEntries.put(userKey, CacheEntry.clean(cacheEntry.getValue()));
                    }
                }
            }
        } finally {
            // Restore context
            backendForContext.setCurrentKey(null);
            if (originalNamespaceToRestore != null) {
                delegateStateForContext.setCurrentNamespace(originalNamespaceToRestore);
            }
            // Clear L1 after flushing its contents
            perKeyCache.l1MapEntries.clear();
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
            double l1ValueHitRate = calculateHitRate(l1MapValueCacheHitCount, l1MapValueCacheMissCount);
            double l2ValueHitRate = calculateHitRate(l2MapValueCacheHitCount, l2MapValueCacheMissCount);
            double l1PresenceHitRate =
                    calculateHitRate(l1PresenceCacheHitCount, l1PresenceCacheMissCount);
            double l2PresenceHitRate =
                    calculateHitRate(l2PresenceCacheHitCount, l2PresenceCacheMissCount);

            LOG.debug(
                            "Cache Metrics - " + "L1 Value: {}/{}, Hit Rate: {:.2f}% | "
                            + "L2 Value: {}/{}, Hit Rate: {:.2f}% | "
                            + "L1 Presence: {}/{}, Hit Rate: {:.2f}% | "
                            + "L2 Presence: {}/{}, Hit Rate: {:.2f}% | " + "Delegate Lookups: {}",
                    l1MapValueCacheHitCount.getCount(),
                    l1MapValueCacheHitCount.getCount() + l1MapValueCacheMissCount.getCount(),
                    l1ValueHitRate, l2MapValueCacheHitCount.getCount(),
                            l2MapValueCacheHitCount.getCount() + l2MapValueCacheMissCount.getCount(),
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
