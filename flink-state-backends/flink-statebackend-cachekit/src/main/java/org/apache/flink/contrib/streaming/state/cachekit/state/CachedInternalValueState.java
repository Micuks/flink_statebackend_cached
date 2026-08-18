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
import org.apache.flink.contrib.streaming.state.RocksDBBatchValueReader;
import org.apache.flink.contrib.streaming.state.cachekit.PrefetchExecutor;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicy;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CaffeineCachePolicy;
import org.apache.flink.contrib.streaming.state.cachekit.cache.LruCachePolicy;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneBridge;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneCoordinator;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalValueState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(CachedInternalValueState.class);

    private final InternalValueState<K, N, V> delegate;
    private final CurrentKeyProvider<K> currentKeyProvider;
    private final CachePolicy<KeyNamespaceKey<K, N>, CachedValue<V>> l1Cache;
    private final CachePolicy<KeyNamespaceKey<K, N>, CachedValue<V>> l2Cache;
    private final CachePolicyType cachePolicyType;
    private final int lruOverflow;

    private final boolean bypassEnabled;
    private final double hitRateThreshold;
    private final int hitRateWindow;
    private final boolean multiGetPrefetchEnabled;
    private final int multiGetChunkSize;
    private final int multiGetMinBatchSize;
    private final boolean stickyUpdateInPlaceEnabled;
    private final boolean lazyStagingEnabled;
    private final NativeRequestPlaneCoordinator nativeRequestPlaneCoordinator;
    private final int nativeStateId;

    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    // Mailbox-thread-only serializers for delegate-visible native write-through. Creating these
    // once avoids serializer duplicate allocation on every update/clear/dirty flush.
    private final TypeSerializer<K> nativeMutationKeySerializer;
    private final TypeSerializer<N> nativeMutationNamespaceSerializer;
    private final TypeSerializer<V> nativeMutationValueSerializer;

    private N currentNamespace;

    // Sticky Cache (L1)
    private KeyNamespaceKey<K, N> lastAccessKey;
    private CachedValue<V> lastAccessValue;

    // Reusable lookup key
    private final KeyNamespaceKey<K, N> lookupKey = new KeyNamespaceKey<>(null, null);

    private final java.util.function.Consumer<K> keyContextSetter;

    /**
     * Lifecycle guard for delegate accesses that can overlap backend teardown. The worker reads
     * RocksDB through {@code delegate.getSerializedValue()} on the SHARED, TM-JVM-level {@code
     * PrefetchExecutor} — whose lifetime is longer than this backend's. Without a guard, a worker
     * still holding a native ColumnFamilyHandle can race {@code delegate.dispose()} ({@code
     * closeQuietly(db)}) and SIGSEGV in librocksdbjni (a native crash the worker's {@code catch
     * (Throwable)} cannot catch). The worker and dirty-cache flushes take the read lock around each
     * delegate access and bail if {@link #closed}; {@link #close()} takes the write lock as a
     * barrier and then marks the wrapper closed, guaranteeing no guarded access is in flight before
     * the backend disposes the RocksDB delegate.
     */
    private volatile boolean closed = false;
    private final java.util.concurrent.locks.ReadWriteLock lifecycleLock =
            new java.util.concurrent.locks.ReentrantReadWriteLock();

    // ---- Backpressure-driven async prefetch (off-mailbox worker -> staging -> L1) ----

    /** Hard admission cap on staged entries. */
    private static final int ASYNC_STAGING_MAX_ENTRIES = loadStagingMaxEntries();
    /** Hard cap on RocksDB-owned serialized value bytes retained by lazy staging. */
    private static final long ASYNC_STAGING_MAX_RETAINED_BYTES =
            loadStagingMaxRetainedBytes();
    private static final int MULTIGET_CHUNK_SIZE = loadMultiGetChunkSize();
    private static final int MULTIGET_MIN_BATCH_SIZE = loadMultiGetMinBatchSize();
    private final int asyncStagingMaxEntries;
    private final long asyncStagingMaxRetainedBytes;

    /**
     * Values fetched by the shared prefetch worker, waiting to be promoted into L1 by the mailbox
     * thread. The worker publishes, the mailbox removes/promotes, and close clears retained
     * entries. A staged entry may only be promoted while {@code writeGen} still equals the
     * generation captured at submission — any delegate-visible write or dirty flush on this state
     * in between makes the RocksDB read potentially stale.
     */
    private final java.util.concurrent.ConcurrentHashMap<KeyNamespaceKey<K, N>, StagedValue<V>>
            staging = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong stagingRetainedBytes =
            new java.util.concurrent.atomic.AtomicLong();

    /**
     * Keys reserved by submitted-but-not-yet-finished prefetch tasks. Without this set, adjacent
     * early-lookahead chunks can enqueue the same key repeatedly while the first task is still
     * waiting behind the shared worker. Values are write generations so stale reservations can be
     * reclaimed without waiting for their old task.
     */
    private final java.util.concurrent.ConcurrentHashMap<KeyNamespaceKey<K, N>, Long> inFlight =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Write generation: bumped on every delegate-visible update/clear and dirty flush-through.
     * Plain write-back entries remain authoritative in L1 and shield an older staged value until
     * their flush bumps this generation. Single writer (mailbox thread); the worker only reads it.
     */
    private volatile long writeGen;

    // Best-effort diagnostic counters. Mailbox and worker paths can both contribute, so these are
    // volatile for visibility rather than synchronization. Unlike these summaries, retained-byte
    // accounting is exact and serialized with every staging-map mutation.
    private volatile long prefetchTasksBuilt;
    private volatile long prefetchTasksExecuted;
    private volatile long prefetchTasksDropped;
    private volatile long prefetchKeysPrepared;
    private volatile long prefetchKeysDeduplicated;
    private volatile long prefetchMultiGetCalls;
    private volatile long prefetchMultiGetKeys;
    private volatile long prefetchPointGetCalls;
    private volatile long prefetchValuesStaged;
    private volatile long prefetchMissingValuesStaged;
    private volatile long prefetchValuesPromoted;
    private volatile long prefetchLazyValuesStaged;
    private volatile long prefetchLazyValuesMaterialized;
    private volatile long prefetchLazyMaterializationFailures;
    private volatile long prefetchStagingAdmissionDrops;
    private volatile long prefetchStaleAborts;
    private volatile long prefetchLiveReadRacedInFlight;
    private volatile long prefetchUnusedStagedOnClose;
    private volatile long prefetchBuildFailures;
    private volatile long prefetchWorkerFailures;
    private volatile long stickyUpdateSameKeyAttempts;
    private volatile long stickyUpdateInPlaceReuses;
    private volatile long nativeBatchesActivated;
    private volatile long nativeProbeKeys;
    private volatile long nativeHits;
    private volatile long nativeHitBytesCopied;
    private volatile long nativeHitBytesDirect;
    private volatile long nativeNegativeHits;
    private volatile long nativeMisses;
    private volatile long nativeFillBatches;
    private volatile long nativeFillKeys;
    private volatile long nativeFillRejected;
    private volatile long nativeFallbackBatches;
    private volatile long nativeMailboxCompactBatches;
    private volatile long nativeMailboxCompactInputKeys;
    private volatile long nativeMailboxCompactUniqueKeys;
    private volatile long nativeMailboxCompactFallbacks;
    private volatile long nativeMailboxCompactThresholdFallbacks;
    private volatile long nativeDirectPreparedBatches;
    private volatile long nativeDirectPreparedKeys;
    private volatile long nativeDirectPreparedFallbacks;
    private volatile long nativeDirectPreparedThresholdFallbacks;
    private volatile long nativeRuntimeFailures;
    private volatile long nativeGenerationAdvances;
    private volatile long nativeMutationAttempts;
    private volatile long nativeMutationWriteThroughSkipped;
    private volatile long nativeMutationApplied;
    private volatile long nativeMutationSuperseded;
    private volatile long nativeMutationFailures;
    private volatile long nativeMutationTombstonesApplied;
    private final java.util.concurrent.atomic.AtomicLong nativeWriteEpoch =
            new java.util.concurrent.atomic.AtomicLong();

    // Worker-only serializers and scratch inputs. PrefetchExecutor serializes all tasks on its
    // single shared worker; mailbox paths use separate fields below.
    private TypeSerializer<K> workerKeySerializer;
    private TypeSerializer<N> workerNamespaceSerializer;
    private TypeSerializer<V> workerValueSerializer;
    private org.apache.flink.core.memory.DataInputDeserializer workerKeyNamespaceInput;
    private org.apache.flink.core.memory.DataInputDeserializer workerValueInput;

    // Mailbox-thread-only scratch buffer. Captured prefetch tasks retain copied byte arrays only.
    private org.apache.flink.core.memory.DataOutputSerializer mailboxKeyOutput;

    // Mailbox-thread-confined deserializer for local-preagg's synchronous MultiGet path. Keep it
    // separate from the worker fields because the shared prefetch worker may still be active for
    // a different batch.
    private TypeSerializer<V> immediateValueSerializer;
    private org.apache.flink.core.memory.DataInputDeserializer immediateValueInput;

    // Mailbox-thread-confined materializer for serialized speculative worker results.
    private TypeSerializer<V> stagedValueSerializer;
    private org.apache.flink.core.memory.DataInputDeserializer stagedValueInput;
    private TypeSerializer<V> mailboxDefaultValueSerializer;

    // Mailbox-thread-confined signal used by local-preagg's immediate MultiGet hook. The backend
    // cannot know which of an operator's ValueState wrappers the user function will actually
    // consult, so each wrapper reports whether it was read in the preceding dispatch. This avoids
    // broadcasting every grouped key to every registered ValueState (which is particularly
    // expensive for operators with several conditional states).
    private boolean immediatePrefetchAccessObserved;

    private static int loadStagingMaxEntries() {
        try {
            return Math.max(
                    128,
                    org.apache.flink.configuration.GlobalConfiguration.loadConfiguration()
                            .get(
                                    org.apache.flink.configuration.ConfigOptions.key(
                                                    "state.backend.cachekit.bp-prefetch.staging.max-entries")
                                            .intType()
                                            .defaultValue(8192)));
        } catch (Throwable t) {
            return 8192;
        }
    }

    private static long loadStagingMaxRetainedBytes() {
        try {
            return Math.max(
                    0L,
                    org.apache.flink.configuration.GlobalConfiguration.loadConfiguration()
                            .get(
                                    org.apache.flink.configuration.ConfigOptions.key(
                                                    "state.backend.cachekit.value.lazy-staging.max-retained-bytes")
                                            .longType()
                                            .defaultValue(64L * 1024L * 1024L)));
        } catch (Throwable t) {
            return 64L * 1024L * 1024L;
        }
    }

    private static int loadMultiGetChunkSize() {
        try {
            return Math.max(
                    2,
                    Math.min(
                            4096,
                            org.apache.flink.configuration.GlobalConfiguration.loadConfiguration()
                                    .get(
                                            org.apache.flink.configuration.ConfigOptions.key(
                                                            "state.backend.cachekit.bp-prefetch.multiget.chunk-size")
                                                    .intType()
                                                    .defaultValue(8))));
        } catch (Throwable t) {
            return 8;
        }
    }

    private static int loadMultiGetMinBatchSize() {
        try {
            return Math.max(
                    2,
                    Math.min(
                            MULTIGET_CHUNK_SIZE,
                            org.apache.flink.configuration.GlobalConfiguration.loadConfiguration()
                                    .get(
                                            org.apache.flink.configuration.ConfigOptions.key(
                                                            "state.backend.cachekit.bp-prefetch.multiget.min-batch-size")
                                                    .intType()
                                                    .defaultValue(4))));
        } catch (Throwable t) {
            return Math.min(4, MULTIGET_CHUNK_SIZE);
        }
    }

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
                false);
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
            boolean multiGetPrefetchEnabled) {
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
                multiGetPrefetchEnabled,
                MULTIGET_CHUNK_SIZE,
                MULTIGET_MIN_BATCH_SIZE,
                false,
                false);
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
            boolean multiGetPrefetchEnabled,
            boolean stickyUpdateInPlaceEnabled) {
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
                multiGetPrefetchEnabled,
                stickyUpdateInPlaceEnabled,
                false);
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
            boolean multiGetPrefetchEnabled,
            boolean stickyUpdateInPlaceEnabled,
            boolean lazyStagingEnabled) {
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
                multiGetPrefetchEnabled,
                MULTIGET_CHUNK_SIZE,
                MULTIGET_MIN_BATCH_SIZE,
                stickyUpdateInPlaceEnabled,
                lazyStagingEnabled);
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
            boolean multiGetPrefetchEnabled,
            boolean stickyUpdateInPlaceEnabled,
            boolean lazyStagingEnabled,
            NativeRequestPlaneCoordinator nativeRequestPlaneCoordinator,
            int nativeStateId) {
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
                multiGetPrefetchEnabled,
                MULTIGET_CHUNK_SIZE,
                MULTIGET_MIN_BATCH_SIZE,
                stickyUpdateInPlaceEnabled,
                lazyStagingEnabled,
                ASYNC_STAGING_MAX_ENTRIES,
                ASYNC_STAGING_MAX_RETAINED_BYTES,
                nativeRequestPlaneCoordinator,
                nativeStateId);
    }

    CachedInternalValueState(
            InternalValueState<K, N, V> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
            int maxEntries,
            CachePolicyType cachePolicyType,
            int lruOverflow,
            boolean bypassEnabled,
            double hitRateThreshold,
            int hitRateWindow,
            boolean multiGetPrefetchEnabled,
            int multiGetChunkSize) {
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
                multiGetPrefetchEnabled,
                multiGetChunkSize,
                2,
                false,
                false);
    }

    CachedInternalValueState(
            InternalValueState<K, N, V> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
            int maxEntries,
            CachePolicyType cachePolicyType,
            int lruOverflow,
            boolean bypassEnabled,
            double hitRateThreshold,
            int hitRateWindow,
            boolean multiGetPrefetchEnabled,
            int multiGetChunkSize,
            int multiGetMinBatchSize) {
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
                multiGetPrefetchEnabled,
                multiGetChunkSize,
                multiGetMinBatchSize,
                false,
                false);
    }

    CachedInternalValueState(
            InternalValueState<K, N, V> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
            int maxEntries,
            CachePolicyType cachePolicyType,
            int lruOverflow,
            boolean bypassEnabled,
            double hitRateThreshold,
            int hitRateWindow,
            boolean multiGetPrefetchEnabled,
            int multiGetChunkSize,
            int multiGetMinBatchSize,
            boolean stickyUpdateInPlaceEnabled) {
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
                multiGetPrefetchEnabled,
                multiGetChunkSize,
                multiGetMinBatchSize,
                stickyUpdateInPlaceEnabled,
                false);
    }

    CachedInternalValueState(
            InternalValueState<K, N, V> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
            int maxEntries,
            CachePolicyType cachePolicyType,
            int lruOverflow,
            boolean bypassEnabled,
            double hitRateThreshold,
            int hitRateWindow,
            boolean multiGetPrefetchEnabled,
            int multiGetChunkSize,
            int multiGetMinBatchSize,
            boolean stickyUpdateInPlaceEnabled,
            boolean lazyStagingEnabled) {
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
                multiGetPrefetchEnabled,
                multiGetChunkSize,
                multiGetMinBatchSize,
                stickyUpdateInPlaceEnabled,
                lazyStagingEnabled,
                ASYNC_STAGING_MAX_ENTRIES,
                ASYNC_STAGING_MAX_RETAINED_BYTES);
    }

    CachedInternalValueState(
            InternalValueState<K, N, V> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
            int maxEntries,
            CachePolicyType cachePolicyType,
            int lruOverflow,
            boolean bypassEnabled,
            double hitRateThreshold,
            int hitRateWindow,
            boolean multiGetPrefetchEnabled,
            int multiGetChunkSize,
            int multiGetMinBatchSize,
            boolean stickyUpdateInPlaceEnabled,
            boolean lazyStagingEnabled,
            int asyncStagingMaxEntries,
            long asyncStagingMaxRetainedBytes) {
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
                multiGetPrefetchEnabled,
                multiGetChunkSize,
                multiGetMinBatchSize,
                stickyUpdateInPlaceEnabled,
                lazyStagingEnabled,
                asyncStagingMaxEntries,
                asyncStagingMaxRetainedBytes,
                null,
                0);
    }

    CachedInternalValueState(
            InternalValueState<K, N, V> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
            int maxEntries,
            CachePolicyType cachePolicyType,
            int lruOverflow,
            boolean bypassEnabled,
            double hitRateThreshold,
            int hitRateWindow,
            boolean multiGetPrefetchEnabled,
            int multiGetChunkSize,
            int multiGetMinBatchSize,
            boolean stickyUpdateInPlaceEnabled,
            boolean lazyStagingEnabled,
            int asyncStagingMaxEntries,
            long asyncStagingMaxRetainedBytes,
            NativeRequestPlaneCoordinator nativeRequestPlaneCoordinator,
            int nativeStateId) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.currentKeyProvider = Objects.requireNonNull(currentKeyProvider, "currentKeyProvider");
        this.keyContextSetter = Objects.requireNonNull(keyContextSetter, "keyContextSetter");
        this.cachePolicyType = Objects.requireNonNull(cachePolicyType, "cachePolicyType");
        this.lruOverflow = Math.max(0, lruOverflow);
        this.bypassEnabled = bypassEnabled;
        this.hitRateThreshold = hitRateThreshold;
        this.hitRateWindow = hitRateWindow;
        // Native feature flags are independently attributable. Map-only native modes must not
        // silently enable ValueState MultiGet. Prefetch and mailbox compaction both require the
        // prepared-key path; all other modes retain the explicit Java bp-prefetch setting.
        this.multiGetPrefetchEnabled =
                multiGetPrefetchEnabled
                        || (nativeRequestPlaneCoordinator != null
                                && (nativeRequestPlaneCoordinator.options().prefetchEnabled()
                                        || nativeRequestPlaneCoordinator
                                                .options()
                                                .mailboxBatchEnabled()));
        this.multiGetChunkSize = Math.max(2, multiGetChunkSize);
        this.multiGetMinBatchSize =
                Math.max(2, Math.min(this.multiGetChunkSize, multiGetMinBatchSize));
        this.stickyUpdateInPlaceEnabled = stickyUpdateInPlaceEnabled;
        this.lazyStagingEnabled = lazyStagingEnabled;
        this.asyncStagingMaxEntries = Math.max(1, asyncStagingMaxEntries);
        this.asyncStagingMaxRetainedBytes = Math.max(0L, asyncStagingMaxRetainedBytes);
        if ((nativeRequestPlaneCoordinator == null && nativeStateId != 0)
                || (nativeRequestPlaneCoordinator != null && nativeStateId <= 0)) {
            throw new IllegalArgumentException(
                    "Native request-plane coordinator and positive state id must be supplied together.");
        }
        this.nativeRequestPlaneCoordinator = nativeRequestPlaneCoordinator;
        this.nativeStateId = nativeStateId;

        this.keySerializer = delegate.getKeySerializer();
        this.namespaceSerializer = delegate.getNamespaceSerializer();
        this.nativeMutationKeySerializer =
                nativeRequestPlaneCoordinator == null ? null : keySerializer.duplicate();
        this.nativeMutationNamespaceSerializer =
                nativeRequestPlaneCoordinator == null ? null : namespaceSerializer.duplicate();
        this.nativeMutationValueSerializer =
                nativeRequestPlaneCoordinator == null
                        ? null
                        : delegate.getValueSerializer().duplicate();

        // L1 Cache: ~20% of maxEntries or at least 128 when caching is enabled. A zero
        // capacity is used by cacheless native mailbox experiments and must not silently
        // instantiate a 128-entry Java cache.
        int l1Size = maxEntries > 0 ? Math.max(128, maxEntries / 5) : 0;
        this.l1Cache = createCachePolicy(l1Size, this::onL1Eviction);

        // L2 Cache: Remaining size (or full maxEntries)
        this.l2Cache = createCachePolicy(maxEntries, this::onL2Eviction);
    }

    private void setLookupKey(K key, N namespace) {
        lookupKey.key = key;
        lookupKey.namespace = namespace;
    }

    @Override
    public V value() throws IOException {
        immediatePrefetchAccessObserved = true;
        K currentKey = currentKeyProvider.getCurrentKey();

        // 1. Check Sticky Cache (Always Check L0 - Fast Path)
        // Use direct comparison if possible or rely on isSame with current objects (no
        // allocation)
        if (lastAccessKey != null && lastAccessKey.isSame(currentKey, currentNamespace)) {
            recordAccess(true); // Hit
            return lastAccessValue.valueOrNull();
        }

        // 2. Bypass Logic
        if (bypassEnabled && isBypassing) {
            // Sampling: Check cache every ~100 requests to see if we should re-enable
            opsSinceLastSample++;
            if (opsSinceLastSample < 100) {
                return delegate.value();
            }
            // Sample this request
            opsSinceLastSample = 0;
        }

        // 3. Check L1 Cache
        // Use reusable key for lookup
        setLookupKey(currentKey, currentNamespace);

        CachedValue<V> l1Cached = l1Cache.get(lookupKey);
        if (l1Cached != null) {
            // Cached values retain the immutable key used at insertion, so switching back to an
            // L1-resident key does not deep-copy the current key and namespace just for L0.
            KeyNamespaceKey<K, N> storageKey = l1Cached.storageKey();
            updateSticky(storageKey, l1Cached);

            recordAccess(true); // Hit
            return l1Cached.valueOrNull();
        }

        // 4. Check L2 Cache
        CachedValue<V> l2Cached = l2Cache.get(lookupKey);
        if (l2Cached != null) {
            // Promote to L1 (Clean)
            KeyNamespaceKey<K, N> storageKey = l2Cached.storageKey();
            CachedValue<V> newValue = CachedValue.of(storageKey, l2Cached.valueOrNull(), false);
            l1Cache.put(storageKey, newValue);

            updateSticky(storageKey, newValue);
            recordAccess(true); // Hit
            return l2Cached.valueOrNull();
        }

        // 4b. Check async-prefetch staging. Sound only when no write/dirty-flush happened on
        // this state since the fetch was submitted (writeGen match); otherwise fall through to
        // the authoritative delegate read.
        Long reservedGeneration = inFlight.get(lookupKey);
        if (reservedGeneration != null && reservedGeneration == writeGen) {
            // The mailbox reached this key before the worker published its speculative result.
            // The authoritative read below remains correct, but this is duplicate I/O and direct
            // evidence that the attempted overlap was too short for this key.
            prefetchLiveReadRacedInFlight++;
        }
        if (!staging.isEmpty()) {
            StagedValue<V> staged = removeStagedValue(lookupKey);
            if (staged != null && staged.gen == writeGen) {
                V stagedValue;
                boolean materializationFailed = false;
                try {
                    stagedValue = materializeStagedValue(staged);
                } catch (IOException | RuntimeException ignored) {
                    prefetchLazyMaterializationFailures++;
                    // Speculative cache fill is best-effort. Fall through to the authoritative
                    // delegate read if a retained payload cannot be materialized.
                    stagedValue = null;
                    materializationFailed = true;
                }
                if (!materializationFailed) {
                    // Cache insertion and eviction are part of the authoritative write-back path.
                    // Never swallow their failures as if speculative materialization had failed.
                    KeyNamespaceKey<K, N> storageKey = staged.storageKey();
                    CachedValue<V> newValue = CachedValue.of(storageKey, stagedValue, false);
                    l1Cache.put(storageKey, newValue);
                    updateSticky(storageKey, newValue);
                    recordAccess(true); // Hit
                    prefetchValuesPromoted++;
                    return newValue.valueOrNull();
                }
            } else if (staged != null) {
                prefetchStaleAborts++;
            }
        }

        // 4c. Independently gated native ValueState point cache. This is deliberately separate
        // from native Prefetch/MultiGet so Mailbox, Prefetch, and PreAgg treatments can be
        // measured without implicitly enabling VCache.
        NativePointCacheResult<V> nativePoint =
                probeNativeValueCache(currentKey, currentNamespace);
        if (nativePoint.hit) {
            KeyNamespaceKey<K, N> storageKey =
                    new KeyNamespaceKey<>(
                            currentKey, currentNamespace, keySerializer, namespaceSerializer);
            CachedValue<V> newValue = CachedValue.of(storageKey, nativePoint.value, false);
            l1Cache.put(storageKey, newValue);
            updateSticky(storageKey, newValue);
            recordAccess(true);
            return newValue.valueOrNull();
        }

        // 5. Miss -> Load from Delegate
        V loaded = delegate.value();
        recordAccess(false); // Miss
        fillNativeValueCache(currentKey, currentNamespace, loaded);

        // 6. Update L1 (Clean)
        // Even if bypassing (sampled), we populate L1 to allow hit rate recovery
        KeyNamespaceKey<K, N> storageKey = new KeyNamespaceKey<>(currentKey, currentNamespace, keySerializer,
                namespaceSerializer);
        CachedValue<V> newValue = CachedValue.of(storageKey, loaded, false);
        l1Cache.put(storageKey, newValue);

        updateSticky(storageKey, newValue);
        return loaded;
    }

    @SuppressWarnings("unchecked")
    private NativePointCacheResult<V> probeNativeValueCache(K key, N namespace) {
        if (nativeRequestPlaneCoordinator == null
                || !nativeRequestPlaneCoordinator.isActive()
                || !nativeRequestPlaneCoordinator.options().valueCacheEnabled()
                || key == null
                || namespace == null
                || !(delegate instanceof RocksDBBatchValueReader<?, ?, ?>)) {
            return NativePointCacheResult.miss();
        }
        NativeRequestPlaneCoordinator.BatchSlot slot =
                nativeRequestPlaneCoordinator.tryAcquireBatchSlot();
        if (slot == null) {
            nativeFallbackBatches++;
            return NativePointCacheResult.miss();
        }
        try (NativeRequestPlaneCoordinator.BatchSlot ignored = slot) {
            RocksDBBatchValueReader<K, N, V> batchReader =
                    (RocksDBBatchValueReader<K, N, V>) delegate;
            byte[] preparedKey =
                    batchReader.serializeBatchKeyAndNamespace(
                            key,
                            namespace,
                            nativeMutationKeySerializer,
                            nativeMutationNamespaceSerializer);
            slot.prepareLatest(
                    nativeStateId,
                    nativeWriteEpoch.get(),
                    java.util.Collections.singletonList(preparedKey));
            int processed = nativeRequestPlaneCoordinator.probe(slot);
            if (processed != 1 || slot.probeError(0) != NativeRequestPlaneBridge.ERROR_OK) {
                throw new IllegalStateException(
                        "Native ValueState point probe returned an invalid result.");
            }
            nativeBatchesActivated++;
            nativeProbeKeys++;
            int status = slot.probeStatus(0);
            if (status == NativeRequestPlaneBridge.PROBE_HIT) {
                V value = deserializeNativeProbeValue(slot, 0, true);
                nativeHits++;
                nativeHitBytesDirect += slot.probeValueLength(0);
                return NativePointCacheResult.hit(value);
            }
            if (status == NativeRequestPlaneBridge.PROBE_NEGATIVE) {
                nativeNegativeHits++;
                V defaultValue = batchReader.getBatchDefaultValue();
                return NativePointCacheResult.hit(
                        deserializeImmediateValueOrCopyDefault(null, defaultValue));
            }
            if (status == NativeRequestPlaneBridge.PROBE_MISS) {
                nativeMisses++;
                return NativePointCacheResult.miss();
            }
            throw new IllegalStateException(
                    "Native ValueState point probe returned status=" + status + ".");
        } catch (Exception | LinkageError failure) {
            nativeRuntimeFailures++;
            nativeFallbackBatches++;
            nativeRequestPlaneCoordinator.disable(failure);
            return NativePointCacheResult.miss();
        }
    }

    @SuppressWarnings("unchecked")
    private void fillNativeValueCache(K key, N namespace, V loaded) {
        if (nativeRequestPlaneCoordinator == null
                || !nativeRequestPlaneCoordinator.isActive()
                || !nativeRequestPlaneCoordinator.options().valueCacheEnabled()
                || key == null
                || namespace == null
                || !(delegate instanceof RocksDBBatchValueReader<?, ?, ?>)) {
            return;
        }
        try {
            RocksDBBatchValueReader<K, N, V> batchReader =
                    (RocksDBBatchValueReader<K, N, V>) delegate;
            byte[] preparedKey =
                    batchReader.serializeBatchKeyAndNamespace(
                            key,
                            namespace,
                            nativeMutationKeySerializer,
                            nativeMutationNamespaceSerializer);
            byte[] serializedValue =
                    loaded == null
                            ? null
                            : KvStateSerializer.serializeValue(
                                    loaded, nativeMutationValueSerializer);
            int status =
                    nativeRequestPlaneCoordinator.updateExactKey(
                            nativeStateId,
                            nativeWriteEpoch.get(),
                            preparedKey,
                            serializedValue);
            if (status == NativeRequestPlaneBridge.FILL_INSERTED
                    || status == NativeRequestPlaneBridge.FILL_UPDATED) {
                nativeFillBatches++;
                nativeFillKeys++;
            } else {
                nativeFillRejected++;
            }
        } catch (Exception | LinkageError failure) {
            nativeRuntimeFailures++;
            nativeFillRejected++;
            nativeRequestPlaneCoordinator.disable(failure);
        }
    }

    @Override
    public void update(V value) throws IOException {
        if (value == null) {
            clear();
            return;
        }
        K currentKey = currentKeyProvider.getCurrentKey();

        // Note on staging soundness: a plain write-back update lands dirty in L1, and the
        // staging promote path is only reachable after an L1+L2 miss — the dirty entry shields
        // the stale staged value until eviction, and eviction flush-through bumps writeGen.
        // Only writes that reach the delegate (bypass mode, flush) need to bump writeGen here.

        // Optimistic Sticky Update (Check L0 first)
        if (lastAccessKey != null && lastAccessKey.isSame(currentKey, currentNamespace)) {
            if (stickyUpdateInPlaceEnabled) {
                stickyUpdateSameKeyAttempts++;
            }
            // A clean L1 value may be moved to L2 by eviction. Only mutate when the sticky object
            // is still the exact value owned by L1; otherwise a shared L2 alias could become dirty.
            CachedValue<V> reusableValue =
                    stickyUpdateInPlaceEnabled
                                    && l1Cache.get(lastAccessKey) == lastAccessValue
                            ? lastAccessValue
                            : null;
            CachedValue<V> newValue;
            if (bypassEnabled && isBypassing) {
                long nativeEpoch =
                        advanceWriteGeneration(); // staged/native RocksDB reads may now be stale
                delegate.update(value);
                publishNativeMutation(currentKey, currentNamespace, value, nativeEpoch);
                if (reusableValue != null) {
                    stickyUpdateInPlaceReuses++;
                    reusableValue.replace(value, false);
                    return;
                }
                newValue =
                        CachedValue.of(
                                lastAccessKey, value, false); // Clean because written to delegate
            } else {
                if (reusableValue != null) {
                    stickyUpdateInPlaceReuses++;
                    reusableValue.replace(value, true);
                    return;
                }
                newValue = CachedValue.of(lastAccessKey, value, true); // Dirty
            }
            // Update L1
            l1Cache.put(lastAccessKey, newValue);
            updateSticky(lastAccessKey, newValue);
            return;
        }

        if (bypassEnabled && isBypassing) {
            // Write-Through (Bypass Mode)
            long nativeEpoch =
                    advanceWriteGeneration(); // staged/native RocksDB reads may now be stale
            delegate.update(value);
            publishNativeMutation(currentKey, currentNamespace, value, nativeEpoch);

            // Update L1 as Clean so subsequent reads (if sampled or re-enabled) find it.
            KeyNamespaceKey<K, N> cacheKey = new KeyNamespaceKey<>(currentKey, currentNamespace, keySerializer,
                    namespaceSerializer);
            CachedValue<V> newValue = CachedValue.of(cacheKey, value, false); // Clean
            l1Cache.put(cacheKey, newValue);
            updateSticky(cacheKey, newValue);
            return;
        }

        KeyNamespaceKey<K, N> cacheKey = new KeyNamespaceKey<>(currentKey, currentNamespace, keySerializer,
                namespaceSerializer);
        CachedValue<V> newValue = CachedValue.of(cacheKey, value, true);

        // Write-Back: Update L1 only (marked dirty)
        l1Cache.put(cacheKey, newValue);

        // Optimistically update sticky
        updateSticky(cacheKey, newValue);
    }

    @Override
    public void clear() {
        K currentKey = currentKeyProvider.getCurrentKey();
        KeyNamespaceKey<K, N> cacheKey;
        // Reuse sticky key if possible
        if (lastAccessKey != null && lastAccessKey.isSame(currentKey, currentNamespace)) {
            cacheKey = lastAccessKey;
        } else {
            cacheKey = new KeyNamespaceKey<>(currentKey, currentNamespace, keySerializer, namespaceSerializer);
        }

        CachedValue<V> existing = findCachedValue(currentKey);
        CachedValue<V> newValue;

        if (bypassEnabled && isBypassing) {
            long nativeEpoch =
                    advanceWriteGeneration(); // staged/native RocksDB reads may now be stale
            delegate.clear();
            publishNativeMutation(currentKey, currentNamespace, null, nativeEpoch);
            newValue = CachedValue.of(cacheKey, null, false);
        } else if (existing != null && existing.isNull && !existing.dirty) {
            // Known clean null: avoid scheduling an extra delete.
            newValue = existing;
        } else {
            newValue = CachedValue.of(cacheKey, null, true);
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

    /**
     * Whether future keys extracted from input records are sufficient to identify this state's
     * entries.
     *
     * <p>The generic record-lookahead hook knows future keys but not their future window/session
     * namespaces. Reusing this wrapper's current non-void namespace therefore warms unrelated
     * entries (and can issue millions of negative reads). Direct callers that know a stable
     * namespace may still use {@link #buildAsyncPrefetchTask}; this gate applies only to the
     * backend's record-key broadcast.
     */
    public boolean supportsRecordKeyPrefetch() {
        return namespaceSerializer instanceof VoidNamespaceSerializer;
    }

    /**
     * Returns and clears whether this wrapper was read since the previous immediate-prefetch
     * decision.
     *
     * <p>Both this method and {@link #value()} run on the task mailbox thread. The first
     * local-preagg dispatch therefore performs no speculative state broadcast; its real accesses
     * teach the next dispatch which wrappers are worth warming. A conditionally unused wrapper is
     * removed from the active set after at most one batch and always retains the normal point-read
     * fallback.
     */
    public boolean consumeImmediatePrefetchAccessObserved() {
        boolean observed = immediatePrefetchAccessObserved;
        immediatePrefetchAccessObserved = false;
        return observed;
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
        lifecycleLock.readLock().lock();
        try {
            if (closed) {
                return;
            }

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
                    l2Cache.put(
                            entry.getKey(),
                            CachedValue.of(
                                    entry.getKey(), val.value, false)); // L2 holds clean
                    flushEntryToDelegate(entry.getKey(), val); // Write-through to delegate

                    // Mark L1 clean
                    l1Cache.put(
                            entry.getKey(), CachedValue.of(entry.getKey(), val.value, false));
                }
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    /**
     * Quiesce the async prefetch for this state before the backend disposes its RocksDB delegate.
     * Takes the write lock as a barrier so any in-flight guarded delegate access has drained, then
     * sets {@link #closed} so new work becomes a no-op. After this returns it is safe for the
     * backend to call {@code delegate.dispose()} / {@code close()}.
     */
    public void close() {
        lifecycleLock.writeLock().lock();
        try {
            closed = true;
            prefetchUnusedStagedOnClose += staging.size();
            clearStaging();
            inFlight.clear();
        } finally {
            lifecycleLock.writeLock().unlock();
        }
        if (prefetchTasksBuilt > 0
                || multiGetPrefetchEnabled
                || nativeRequestPlaneCoordinator != null) {
            LOG.info(
                    "[CACHEKIT VALUE PREFETCH] delegate={} namespaceSerializer={} "
                            + "recordKeyPrefetch={} multiGet={} chunkSize={} minBatchSize={} "
                            + "tasksBuilt={} tasksExecuted={} tasksDropped={} keysPrepared={} "
                            + "keysDeduplicated={} multiGetCalls={} "
                            + "multiGetKeys={} pointGetCalls={} staged={} missingStaged={} "
                            + "promoted={} lazyStaging={} lazyStaged={} lazyMaterialized={} "
                            + "lazyMaterializationFailures={} stagingEntries={} retainedBytes={} "
                            + "maxRetainedBytes={} admissionDrops={} staleAborts={} "
                            + "liveReadRacedInFlight={} unusedStagedOnClose={} "
                            + "buildFailures={} workerFailures={} stickyUpdateInPlace={} "
                            + "stickySameKeyAttempts={} stickyInPlaceReuses={} "
                            + "nativeEnabled={} nativeStateId={} nativeActivated={} "
                            + "nativeProbeKeys={} nativeHits={} nativeHitBytesCopied={} "
                            + "nativeHitBytesDirect={} "
                            + "nativeNegativeHits={} "
                            + "nativeMisses={} nativeFillBatches={} nativeFillKeys={} "
                            + "nativeFillRejected={} nativeFallbackBatches={} "
                            + "nativeMailboxCompactBatches={} "
                            + "nativeMailboxCompactInputKeys={} "
                            + "nativeMailboxCompactUniqueKeys={} "
                            + "nativeMailboxCompactFallbacks={} "
                            + "nativeMailboxCompactThresholdFallbacks={} "
                            + "nativeDirectPreparedBatches={} "
                            + "nativeDirectPreparedKeys={} "
                            + "nativeDirectPreparedFallbacks={} "
                            + "nativeDirectPreparedThresholdFallbacks={} "
                            + "nativeRuntimeFailures={} nativeGenerationAdvances={} "
                            + "nativeMutationAttempts={} nativeMutationApplied={} "
                            + "nativeMutationWriteThroughSkipped={} "
                            + "nativeMutationSuperseded={} nativeMutationFailures={} "
                            + "nativeMutationTombstonesApplied={} nativeActive={} "
                            + "nativeKernel={} nativeFeatureBits={} nativeFeatures={} "
                            + "nativeDisableCause={} coordinatorProbeCalls={} "
                            + "coordinatorFillCalls={} coordinatorLeaseMisses={}",
                    delegate.getClass().getSimpleName(),
                    namespaceSerializer.getClass().getSimpleName(),
                    supportsRecordKeyPrefetch(),
                    multiGetPrefetchEnabled,
                    multiGetChunkSize,
                    multiGetMinBatchSize,
                    prefetchTasksBuilt,
                    prefetchTasksExecuted,
                    prefetchTasksDropped,
                    prefetchKeysPrepared,
                    prefetchKeysDeduplicated,
                    prefetchMultiGetCalls,
                    prefetchMultiGetKeys,
                    prefetchPointGetCalls,
                    prefetchValuesStaged,
                    prefetchMissingValuesStaged,
                    prefetchValuesPromoted,
                    lazyStagingEnabled,
                    prefetchLazyValuesStaged,
                    prefetchLazyValuesMaterialized,
                    prefetchLazyMaterializationFailures,
                    staging.size(),
                    stagingRetainedBytes.get(),
                    asyncStagingMaxRetainedBytes,
                    prefetchStagingAdmissionDrops,
                    prefetchStaleAborts,
                    prefetchLiveReadRacedInFlight,
                    prefetchUnusedStagedOnClose,
                    prefetchBuildFailures,
                    prefetchWorkerFailures,
                    stickyUpdateInPlaceEnabled,
                    stickyUpdateSameKeyAttempts,
                    stickyUpdateInPlaceReuses,
                    nativeRequestPlaneCoordinator != null,
                    nativeStateId,
                    nativeBatchesActivated,
                    nativeProbeKeys,
                    nativeHits,
                    nativeHitBytesCopied,
                    nativeHitBytesDirect,
                    nativeNegativeHits,
                    nativeMisses,
                    nativeFillBatches,
                    nativeFillKeys,
                    nativeFillRejected,
                    nativeFallbackBatches,
                    nativeMailboxCompactBatches,
                    nativeMailboxCompactInputKeys,
                    nativeMailboxCompactUniqueKeys,
                    nativeMailboxCompactFallbacks,
                    nativeMailboxCompactThresholdFallbacks,
                    nativeDirectPreparedBatches,
                    nativeDirectPreparedKeys,
                    nativeDirectPreparedFallbacks,
                    nativeDirectPreparedThresholdFallbacks,
                    nativeRuntimeFailures,
                    nativeGenerationAdvances,
                    nativeMutationAttempts,
                    nativeMutationApplied,
                    nativeMutationWriteThroughSkipped,
                    nativeMutationSuperseded,
                    nativeMutationFailures,
                    nativeMutationTombstonesApplied,
                    nativeRequestPlaneCoordinator != null
                            && nativeRequestPlaneCoordinator.isActive(),
                    nativeRequestPlaneCoordinator == null
                            ? "disabled"
                            : nativeRequestPlaneCoordinator.selectedKernel(),
                    nativeRequestPlaneCoordinator == null
                            ? "0x0000000000000000"
                            : nativeRequestPlaneCoordinator.detectedFeatureBitsHex(),
                    nativeRequestPlaneCoordinator == null
                            ? "disabled"
                            : nativeRequestPlaneCoordinator.detectedFeatures(),
                    nativeDisableCauseForAudit(),
                    nativeRequestPlaneCoordinator == null
                            ? 0
                            : nativeRequestPlaneCoordinator.probeCalls(),
                    nativeRequestPlaneCoordinator == null
                            ? 0
                            : nativeRequestPlaneCoordinator.fillCalls(),
                    nativeRequestPlaneCoordinator == null
                            ? 0
                            : nativeRequestPlaneCoordinator.leaseMisses());
        }
    }

    long getPrefetchMultiGetCallsForTesting() {
        return prefetchMultiGetCalls;
    }

    long getPrefetchPointGetCallsForTesting() {
        return prefetchPointGetCalls;
    }

    long getPrefetchMissingValuesStagedForTesting() {
        return prefetchMissingValuesStaged;
    }

    long getPrefetchValuesPromotedForTesting() {
        return prefetchValuesPromoted;
    }

    long getPrefetchLazyValuesStagedForTesting() {
        return prefetchLazyValuesStaged;
    }

    long getPrefetchLazyValuesMaterializedForTesting() {
        return prefetchLazyValuesMaterialized;
    }

    long getPrefetchLazyMaterializationFailuresForTesting() {
        return prefetchLazyMaterializationFailures;
    }

    int getStagingSizeForTesting() {
        synchronized (staging) {
            return staging.size();
        }
    }

    long getStagingRetainedBytesForTesting() {
        return stagingRetainedBytes.get();
    }

    long getPrefetchStagingAdmissionDropsForTesting() {
        return prefetchStagingAdmissionDrops;
    }

    long getPrefetchLiveReadRacedInFlightForTesting() {
        return prefetchLiveReadRacedInFlight;
    }

    long getPrefetchUnusedStagedOnCloseForTesting() {
        return prefetchUnusedStagedOnClose;
    }

    long getStickyUpdateSameKeyAttemptsForTesting() {
        return stickyUpdateSameKeyAttempts;
    }

    long getStickyUpdateInPlaceReusesForTesting() {
        return stickyUpdateInPlaceReuses;
    }

    long getPrefetchKeysDeduplicatedForTesting() {
        return prefetchKeysDeduplicated;
    }

    long getNativeBatchesActivatedForTesting() {
        return nativeBatchesActivated;
    }

    long getNativeProbeKeysForTesting() {
        return nativeProbeKeys;
    }

    long getNativeHitsForTesting() {
        return nativeHits;
    }

    long getNativeHitBytesCopiedForTesting() {
        return nativeHitBytesCopied;
    }

    long getNativeHitBytesDirectForTesting() {
        return nativeHitBytesDirect;
    }

    long getNativeNegativeHitsForTesting() {
        return nativeNegativeHits;
    }

    long getNativeMissesForTesting() {
        return nativeMisses;
    }

    long getNativeFillBatchesForTesting() {
        return nativeFillBatches;
    }

    long getNativeFillKeysForTesting() {
        return nativeFillKeys;
    }

    long getNativeFillRejectedForTesting() {
        return nativeFillRejected;
    }

    long getNativeFallbackBatchesForTesting() {
        return nativeFallbackBatches;
    }

    long getNativeMailboxCompactBatchesForTesting() {
        return nativeMailboxCompactBatches;
    }

    long getNativeMailboxCompactInputKeysForTesting() {
        return nativeMailboxCompactInputKeys;
    }

    long getNativeMailboxCompactUniqueKeysForTesting() {
        return nativeMailboxCompactUniqueKeys;
    }

    long getNativeMailboxCompactFallbacksForTesting() {
        return nativeMailboxCompactFallbacks;
    }

    long getNativeMailboxCompactThresholdFallbacksForTesting() {
        return nativeMailboxCompactThresholdFallbacks;
    }

    long getNativeDirectPreparedBatchesForTesting() {
        return nativeDirectPreparedBatches;
    }

    long getNativeDirectPreparedKeysForTesting() {
        return nativeDirectPreparedKeys;
    }

    long getNativeDirectPreparedFallbacksForTesting() {
        return nativeDirectPreparedFallbacks;
    }

    long getNativeDirectPreparedThresholdFallbacksForTesting() {
        return nativeDirectPreparedThresholdFallbacks;
    }

    long getNativeRuntimeFailuresForTesting() {
        return nativeRuntimeFailures;
    }

    long getNativeGenerationAdvancesForTesting() {
        return nativeGenerationAdvances;
    }

    long getNativeMutationAttemptsForTesting() {
        return nativeMutationAttempts;
    }

    long getNativeMutationAppliedForTesting() {
        return nativeMutationApplied;
    }

    long getNativeMutationWriteThroughSkippedForTesting() {
        return nativeMutationWriteThroughSkipped;
    }

    long getNativeMutationSupersededForTesting() {
        return nativeMutationSuperseded;
    }

    long getNativeMutationFailuresForTesting() {
        return nativeMutationFailures;
    }

    long getNativeMutationTombstonesAppliedForTesting() {
        return nativeMutationTombstonesApplied;
    }

    long getPrefetchTasksDroppedForTesting() {
        return prefetchTasksDropped;
    }

    /**
     * Mailbox-side half of the async prefetch: serialize (key, namespace) for every key that is
     * not already cached or staged, then hand the byte[] batch to the shared worker thread. The
     * only work on the mailbox thread is key serialization; the RocksDB reads and value
     * eager-path deserialization happen off-thread and overlap with record dispatch / backpressure
     * waits. Lazy RocksDB results are materialized only if the mailbox promotes them.
     *
     * @return a worker task to run via PrefetchExecutor, or null if there is nothing to fetch.
     */
    public Runnable buildAsyncPrefetchTask(Iterable<? extends K> keys) {
        if (closed || keys == null || currentNamespace == null) {
            return null;
        }
        if (multiGetPrefetchEnabled && delegate instanceof RocksDBBatchValueReader<?, ?, ?>) {
            return buildPreparedMultiGetTask(keys, currentNamespace);
        }
        java.util.ArrayList<byte[]> serialized = new java.util.ArrayList<>();
        java.util.ArrayList<KeyNamespaceKey<K, N>> reservations = new java.util.ArrayList<>();
        final long gen = writeGen;
        final N namespace = currentNamespace;
        try {
            if (mailboxKeyOutput == null) {
                mailboxKeyOutput = new org.apache.flink.core.memory.DataOutputSerializer(64);
            }
            for (K key : keys) {
                if (key == null || findCachedValueFor(key, namespace) != null) {
                    continue;
                }
                if (hasStagedOrInFlightValue(key, namespace, gen)) {
                    continue;
                }
                KeyNamespaceKey<K, N> storageKey =
                        new KeyNamespaceKey<>(key, namespace, keySerializer, namespaceSerializer);
                if (inFlight.putIfAbsent(storageKey, gen) != null) {
                    prefetchKeysDeduplicated++;
                    continue;
                }
                reservations.add(storageKey);
                mailboxKeyOutput.clear();
                keySerializer.serialize(storageKey.key, mailboxKeyOutput);
                mailboxKeyOutput.writeByte(42); // KvStateSerializer.MAGIC_NUMBER wire format
                namespaceSerializer.serialize(storageKey.namespace, mailboxKeyOutput);
                serialized.add(mailboxKeyOutput.getCopyOfBuffer());
            }
        } catch (Throwable t) {
            prefetchBuildFailures++;
            releaseReservations(reservations, gen);
            return null; // Best-effort: an unserializable key aborts this batch only.
        }
        if (serialized.isEmpty()) {
            return null;
        }
        final V defaultValue;
        try {
            defaultValue = copyBatchDefaultValueForAsyncTask();
        } catch (Throwable t) {
            prefetchBuildFailures++;
            releaseReservations(reservations, gen);
            return null;
        }
        prefetchTasksBuilt++;
        prefetchKeysPrepared += serialized.size();
        return trackedTask(
                reservations, gen, () -> fetchIntoStaging(serialized, defaultValue, gen));
    }

    /**
     * Mailbox-side half of the RocksDB MultiGet path. It prepares the exact composite RocksDB key
     * once and keeps the corresponding immutable cache key beside it. The worker can therefore
     * issue each chunk directly, without deserializing query-wire keys and serializing them again
     * inside RocksDBValueState.
     */
    @SuppressWarnings("unchecked")
    private Runnable buildPreparedMultiGetTask(Iterable<? extends K> keys, N namespace) {
        java.util.ArrayList<byte[]> rocksDBKeys = new java.util.ArrayList<>();
        java.util.ArrayList<KeyNamespaceKey<K, N>> storageKeys = new java.util.ArrayList<>();
        final long gen = writeGen;
        final boolean nativeMailboxBatch =
                nativeRequestPlaneCoordinator != null
                        && nativeRequestPlaneCoordinator.isActive()
                        && nativeRequestPlaneCoordinator.options().mailboxBatchEnabled();
        final boolean nativeDirectPrefetch =
                !nativeMailboxBatch
                        && nativeRequestPlaneCoordinator != null
                        && nativeRequestPlaneCoordinator.isActive()
                        && nativeRequestPlaneCoordinator.options().prefetchEnabled();
        RocksDBBatchValueReader<K, N, V> batchReader =
                (RocksDBBatchValueReader<K, N, V>) delegate;
        try {
            for (K key : keys) {
                if (key == null || findCachedValueFor(key, namespace) != null) {
                    continue;
                }
                if (hasStagedOrInFlightValue(key, namespace, gen)) {
                    continue;
                }
                KeyNamespaceKey<K, N> storageKey =
                        new KeyNamespaceKey<>(
                                key, namespace, keySerializer, namespaceSerializer);
                if (!nativeMailboxBatch) {
                    if (inFlight.putIfAbsent(storageKey, gen) != null) {
                        prefetchKeysDeduplicated++;
                        continue;
                    }
                }
                storageKeys.add(storageKey);
                if (!nativeMailboxBatch && !nativeDirectPrefetch) {
                    rocksDBKeys.add(
                            batchReader.serializeBatchKeyAndNamespace(
                                    storageKey.key,
                                    storageKey.namespace,
                                    keySerializer,
                                    namespaceSerializer));
                }
            }
        } catch (Throwable t) {
            prefetchBuildFailures++;
            if (!nativeMailboxBatch) {
                releaseReservations(storageKeys, gen);
            }
            return null; // Best-effort: an unserializable key aborts this batch only.
        }
        if (storageKeys.isEmpty()) {
            return null;
        }
        NativeRequestPlaneCoordinator.BatchSlot nativeBatchSlot = null;
        if (nativeMailboxBatch) {
            nativeBatchSlot =
                    compactNativeMailboxBatch(batchReader, rocksDBKeys, storageKeys);
            reservePreparedKeys(rocksDBKeys, storageKeys, gen);
            if (rocksDBKeys.isEmpty()) {
                if (nativeBatchSlot != null) {
                    nativeBatchSlot.close();
                }
                return null;
            }
            if (nativeBatchSlot != null) {
                if (!nativeRequestPlaneCoordinator.options().prefetchEnabled()) {
                    nativeBatchSlot.close();
                    nativeBatchSlot = null;
                } else {
                    try {
                        nativeBatchSlot.prepareLatest(
                                nativeStateId, nativeWriteEpoch.get(), rocksDBKeys);
                    } catch (IOException | RuntimeException failure) {
                        nativeBatchSlot.close();
                        nativeBatchSlot = null;
                        nativeFallbackBatches++;
                    }
                }
            }
        }
        java.util.List<byte[]> preparedRocksDBKeys = rocksDBKeys;
        if (nativeDirectPrefetch) {
            nativeBatchSlot = prepareNativeBatchSlotDirect(batchReader, storageKeys);
            if (nativeBatchSlot != null) {
                preparedRocksDBKeys = preparedKeyView(nativeBatchSlot, storageKeys.size());
            } else {
                try {
                    for (KeyNamespaceKey<K, N> storageKey : storageKeys) {
                        rocksDBKeys.add(
                                batchReader.serializeBatchKeyAndNamespace(
                                        storageKey.key,
                                        storageKey.namespace,
                                        keySerializer,
                                        namespaceSerializer));
                    }
                } catch (Throwable failure) {
                    prefetchBuildFailures++;
                    releaseReservations(storageKeys, gen);
                    return null;
                }
            }
        }
        if (preparedRocksDBKeys.isEmpty()) {
            if (nativeBatchSlot != null) {
                nativeBatchSlot.close();
            }
            releaseReservations(storageKeys, gen);
            return null;
        }
        final V defaultValue;
        try {
            defaultValue = copyBatchDefaultValueForAsyncTask();
        } catch (Throwable t) {
            prefetchBuildFailures++;
            releaseReservations(storageKeys, gen);
            if (nativeBatchSlot != null) {
                nativeBatchSlot.close();
            }
            return null;
        }
        prefetchTasksBuilt++;
        prefetchKeysPrepared += preparedRocksDBKeys.size();
        if (nativeBatchSlot == null) {
            nativeBatchSlot = prepareNativeBatchSlot(preparedRocksDBKeys);
        }
        final java.util.List<byte[]> taskRocksDBKeys = preparedRocksDBKeys;
        final NativeRequestPlaneCoordinator.BatchSlot preparedNativeBatchSlot = nativeBatchSlot;
        return trackedTask(
                storageKeys,
                gen,
                () ->
                        fetchPreparedChunksIntoStaging(
                                taskRocksDBKeys,
                                storageKeys,
                                defaultValue,
                                gen,
                                preparedNativeBatchSlot),
                preparedNativeBatchSlot == null ? null : preparedNativeBatchSlot::close);
    }

    private NativeRequestPlaneCoordinator.BatchSlot prepareNativeBatchSlotDirect(
            RocksDBBatchValueReader<K, N, V> batchReader,
            java.util.List<KeyNamespaceKey<K, N>> storageKeys) {
        if (storageKeys.size() < nativeRequestPlaneCoordinator.options().minBatchSize()) {
            nativeDirectPreparedFallbacks++;
            nativeDirectPreparedThresholdFallbacks++;
            return null;
        }
        NativeRequestPlaneCoordinator.BatchSlot slot =
                nativeRequestPlaneCoordinator.tryAcquireBatchSlot();
        if (slot == null) {
            nativeDirectPreparedFallbacks++;
            return null;
        }
        try {
            slot.prepareLatestDirect(
                    nativeStateId,
                    nativeWriteEpoch.get(),
                    storageKeys.size(),
                    (index, output) -> {
                        KeyNamespaceKey<K, N> storageKey = storageKeys.get(index);
                        try {
                            batchReader.serializeBatchKeyAndNamespace(
                                    storageKey.key,
                                    storageKey.namespace,
                                    keySerializer,
                                    namespaceSerializer,
                                    output);
                        } catch (IOException failure) {
                            throw failure;
                        } catch (Exception failure) {
                            throw new IOException(
                                    "Failed to serialize a native prefetch key.", failure);
                        }
                    });
            nativeDirectPreparedBatches++;
            nativeDirectPreparedKeys += storageKeys.size();
            return slot;
        } catch (IOException | RuntimeException | LinkageError failure) {
            nativeDirectPreparedFallbacks++;
            slot.close();
            return null;
        }
    }

    private java.util.List<byte[]> preparedKeyView(
            NativeRequestPlaneCoordinator.BatchSlot slot, int size) {
        return new java.util.AbstractList<byte[]>() {
            @Override
            public byte[] get(int index) {
                return slot.copyPreparedKey(index);
            }

            @Override
            public int size() {
                return size;
            }
        };
    }

    private NativeRequestPlaneCoordinator.BatchSlot compactNativeMailboxBatch(
            RocksDBBatchValueReader<K, N, V> batchReader,
            java.util.ArrayList<byte[]> rocksDBKeys,
            java.util.ArrayList<KeyNamespaceKey<K, N>> storageKeys) {
        nativeMailboxCompactInputKeys += storageKeys.size();
        if (storageKeys.size() < nativeRequestPlaneCoordinator.options().minBatchSize()) {
            nativeFallbackBatches++;
            nativeMailboxCompactFallbacks++;
            nativeMailboxCompactThresholdFallbacks++;
            materializeMailboxFallbackKeys(batchReader, storageKeys, rocksDBKeys);
            return null;
        }
        NativeRequestPlaneCoordinator.BatchSlot slot =
                nativeRequestPlaneCoordinator.tryAcquireBatchSlot();
        if (slot == null) {
            nativeFallbackBatches++;
            nativeMailboxCompactFallbacks++;
            materializeMailboxFallbackKeys(batchReader, storageKeys, rocksDBKeys);
            return null;
        }
        try {
            try {
                slot.prepareLatestDirect(
                        nativeStateId,
                        nativeWriteEpoch.get(),
                        storageKeys.size(),
                        (index, output) -> {
                            KeyNamespaceKey<K, N> storageKey = storageKeys.get(index);
                            try {
                                batchReader.serializeBatchKeyAndNamespace(
                                        storageKey.key,
                                        storageKey.namespace,
                                        keySerializer,
                                        namespaceSerializer,
                                        output);
                            } catch (IOException failure) {
                                throw failure;
                            } catch (Exception failure) {
                                throw new IOException(
                                        "Failed to serialize a native mailbox key.", failure);
                            }
                        });
            } catch (IOException directFailure) {
                rocksDBKeys.clear();
                for (KeyNamespaceKey<K, N> storageKey : storageKeys) {
                    rocksDBKeys.add(
                            batchReader.serializeBatchKeyAndNamespace(
                                    storageKey.key,
                                    storageKey.namespace,
                                    keySerializer,
                                    namespaceSerializer));
                }
                slot.prepareLatest(nativeStateId, nativeWriteEpoch.get(), rocksDBKeys);
            }
            int uniqueCount = nativeRequestPlaneCoordinator.compact(slot);
            nativeMailboxCompactBatches++;
            nativeMailboxCompactUniqueKeys += uniqueCount;
            rocksDBKeys.clear();
            for (int target = 0; target < uniqueCount; target++) {
                int source = slot.compactedSourceIndex(target);
                storageKeys.set(target, storageKeys.get(source));
                rocksDBKeys.add(slot.copyPreparedKey(source));
            }
            int duplicates = storageKeys.size() - uniqueCount;
            if (duplicates > 0) {
                prefetchKeysDeduplicated += duplicates;
                storageKeys.subList(uniqueCount, storageKeys.size()).clear();
            }
            return slot;
        } catch (Exception | LinkageError failure) {
            nativeFallbackBatches++;
            nativeMailboxCompactFallbacks++;
            slot.close();
            materializeMailboxFallbackKeys(batchReader, storageKeys, rocksDBKeys);
            return null;
        }
    }

    private void materializeMailboxFallbackKeys(
            RocksDBBatchValueReader<K, N, V> batchReader,
            java.util.ArrayList<KeyNamespaceKey<K, N>> storageKeys,
            java.util.ArrayList<byte[]> rocksDBKeys) {
        rocksDBKeys.clear();
        try {
            for (KeyNamespaceKey<K, N> storageKey : storageKeys) {
                rocksDBKeys.add(
                        batchReader.serializeBatchKeyAndNamespace(
                                storageKey.key,
                                storageKey.namespace,
                                keySerializer,
                                namespaceSerializer));
            }
        } catch (Throwable failure) {
            rocksDBKeys.clear();
            storageKeys.clear();
            prefetchBuildFailures++;
        }
    }

    private void reservePreparedKeys(
            java.util.ArrayList<byte[]> rocksDBKeys,
            java.util.ArrayList<KeyNamespaceKey<K, N>> storageKeys,
            long generation) {
        int writeIndex = 0;
        for (int readIndex = 0; readIndex < storageKeys.size(); readIndex++) {
            KeyNamespaceKey<K, N> storageKey = storageKeys.get(readIndex);
            if (inFlight.putIfAbsent(storageKey, generation) != null) {
                prefetchKeysDeduplicated++;
                continue;
            }
            if (writeIndex != readIndex) {
                storageKeys.set(writeIndex, storageKey);
                rocksDBKeys.set(writeIndex, rocksDBKeys.get(readIndex));
            }
            writeIndex++;
        }
        if (writeIndex < storageKeys.size()) {
            storageKeys.subList(writeIndex, storageKeys.size()).clear();
            rocksDBKeys.subList(writeIndex, rocksDBKeys.size()).clear();
        }
    }

    /**
     * Blocking MultiGet for the exact, deduplicated key set produced by local pre-aggregation.
     * Results are staged and then promoted by the immediately following {@link #value()} calls.
     * Existing clean/dirty cache entries are skipped, so a speculative RocksDB value can never
     * overwrite a newer write-back value.
     */
    @SuppressWarnings("unchecked")
    public void prefetchForImmediateUse(Iterable<? extends K> keys) {
        if (closed
                || keys == null
                || currentNamespace == null
                || !multiGetPrefetchEnabled
                || !(delegate instanceof RocksDBBatchValueReader<?, ?, ?>)) {
            return;
        }
        final N namespace = currentNamespace;
        final long gen = writeGen;
        final RocksDBBatchValueReader<K, N, V> batchReader =
                (RocksDBBatchValueReader<K, N, V>) delegate;
        final java.util.ArrayList<byte[]> rocksDBKeys = new java.util.ArrayList<>();
        final java.util.ArrayList<KeyNamespaceKey<K, N>> storageKeys =
                new java.util.ArrayList<>();
        try {
            for (K key : keys) {
                if (key == null || findCachedValueFor(key, namespace) != null) {
                    continue;
                }
                KeyNamespaceKey<K, N> storageKey =
                        new KeyNamespaceKey<>(
                                key, namespace, keySerializer, namespaceSerializer);
                storageKeys.add(storageKey);
                rocksDBKeys.add(
                        batchReader.serializeBatchKeyAndNamespace(
                                storageKey.key,
                                storageKey.namespace,
                                keySerializer,
                                namespaceSerializer));
            }
        } catch (Throwable t) {
            prefetchBuildFailures++;
            return;
        }
        if (rocksDBKeys.isEmpty()) {
            return;
        }
        prefetchKeysPrepared += rocksDBKeys.size();
        if (immediateValueSerializer == null) {
            immediateValueSerializer = delegate.getValueSerializer().duplicate();
            immediateValueInput = new org.apache.flink.core.memory.DataInputDeserializer();
        }
        final V defaultValue = batchReader.getBatchDefaultValue();
        NativeRequestPlaneCoordinator.BatchSlot nativeBatchSlot =
                prepareNativeBatchSlot(rocksDBKeys);
        if (nativeBatchSlot != null) {
            try {
                if (executeNativePreparedBatch(
                        rocksDBKeys,
                        storageKeys,
                        defaultValue,
                        gen,
                        nativeBatchSlot,
                        true)) {
                    return;
                }
            } catch (Exception t) {
                prefetchWorkerFailures++;
                return;
            } finally {
                nativeBatchSlot.close();
            }
        }
        try {
            for (int start = 0; start < rocksDBKeys.size(); start += multiGetChunkSize) {
                if (closed || gen != writeGen) {
                    prefetchStaleAborts++;
                    return;
                }
                int end = Math.min(start + multiGetChunkSize, rocksDBKeys.size());
                java.util.List<byte[]> valueBytes = new java.util.ArrayList<>(end - start);
                lifecycleLock.readLock().lock();
                try {
                    if (end - start < multiGetMinBatchSize) {
                        for (int i = start; i < end; i++) {
                            prefetchPointGetCalls++;
                            valueBytes.add(
                                    batchReader.getSerializedValueByRocksDBKey(
                                            rocksDBKeys.get(i)));
                        }
                    } else {
                        prefetchMultiGetCalls++;
                        prefetchMultiGetKeys += end - start;
                        valueBytes =
                                batchReader.getSerializedValuesByRocksDBKeys(
                                        rocksDBKeys, start, end);
                    }
                } finally {
                    lifecycleLock.readLock().unlock();
                }
                if (closed || gen != writeGen || valueBytes.size() != end - start) {
                    prefetchStaleAborts++;
                    return;
                }
                for (int i = 0; i < valueBytes.size(); i++) {
                    byte[] serializedValue = valueBytes.get(i);
                    V value =
                            deserializeImmediateValueOrCopyDefault(
                                    serializedValue, defaultValue);
                    if (!publishStagedValue(
                            StagedValue.materialized(
                                    storageKeys.get(start + i), value, gen),
                            serializedValue == null)) {
                        return;
                    }
                }
            }
        } catch (Throwable t) {
            prefetchWorkerFailures++;
        }
    }

    private V deserializeImmediateValueOrCopyDefault(byte[] valueBytes, V defaultValue)
            throws IOException {
        if (valueBytes == null) {
            return defaultValue == null ? null : immediateValueSerializer.copy(defaultValue);
        }
        immediateValueInput.setBuffer(valueBytes, 0, valueBytes.length);
        return immediateValueSerializer.deserialize(immediateValueInput);
    }

    private boolean hasStagedOrInFlightValue(K key, N namespace, long gen) {
        setLookupKey(key, namespace);
        StagedValue<V> staged = staging.get(lookupKey);
        if (staged != null) {
            if (staged.gen == gen) {
                prefetchKeysDeduplicated++;
                return true;
            }
            removeStagedValue(lookupKey, staged);
        }
        Long reservedGen = inFlight.get(lookupKey);
        if (reservedGen != null) {
            if (reservedGen == gen) {
                prefetchKeysDeduplicated++;
                return true;
            }
            inFlight.remove(lookupKey, reservedGen);
        }
        return false;
    }

    private Runnable trackedTask(
            java.util.List<KeyNamespaceKey<K, N>> reservations, long gen, Runnable task) {
        return trackedTask(reservations, gen, task, null);
    }

    private Runnable trackedTask(
            java.util.List<KeyNamespaceKey<K, N>> reservations,
            long gen,
            Runnable task,
            Runnable completion) {
        return new PrefetchExecutor.DropAwareTask() {
            @Override
            public void run() {
                try {
                    task.run();
                } finally {
                    releaseReservations(reservations, gen);
                    if (completion != null) {
                        completion.run();
                    }
                }
            }

            @Override
            public void onDrop() {
                prefetchTasksDropped++;
                releaseReservations(reservations, gen);
                if (completion != null) {
                    completion.run();
                }
            }
        };
    }

    private void releaseReservations(
            java.util.List<KeyNamespaceKey<K, N>> reservations, long gen) {
        for (KeyNamespaceKey<K, N> key : reservations) {
            inFlight.remove(key, gen);
        }
    }

    /**
     * Worker-side half: runs on the single shared prefetch thread. Reads RocksDB through the
     * delegate's {@code getSerializedValue} — the same thread-safe path Flink's queryable state
     * uses concurrently with the task thread — and parks serialized or materialized values in
     * {@link #staging}.
     */
    private void fetchIntoStaging(
            java.util.List<byte[]> serializedKeyAndNamespaces, V defaultValue, long gen) {
        prefetchTasksExecuted++;
        try {
            prepareWorkerKeyState();
            prepareWorkerValueState();
            if (multiGetPrefetchEnabled && delegate instanceof RocksDBBatchValueReader<?, ?, ?>) {
                fetchChunksIntoStaging(serializedKeyAndNamespaces, defaultValue, gen);
                return;
            }
            for (byte[] skn : serializedKeyAndNamespaces) {
                if (gen != writeGen) {
                    prefetchStaleAborts++;
                    return; // a write already invalidated this batch; stop wasting reads
                }
                if (!fetchSingleIntoStaging(skn, defaultValue, gen)) {
                    return;
                }
            }
        } catch (Throwable ignored) {
            prefetchWorkerFailures++;
            // Best-effort cache warmup; the authoritative read path is untouched.
        }
    }

    private void prepareWorkerKeyState() {
        if (workerKeySerializer == null) {
            workerKeySerializer = keySerializer.duplicate();
            workerNamespaceSerializer = namespaceSerializer.duplicate();
            workerKeyNamespaceInput = new org.apache.flink.core.memory.DataInputDeserializer();
        }
    }

    private void prepareWorkerValueState() {
        if (workerValueSerializer == null) {
            workerValueSerializer = delegate.getValueSerializer().duplicate();
            workerValueInput = new org.apache.flink.core.memory.DataInputDeserializer();
        }
    }

    private void fetchPreparedChunksIntoStaging(
            java.util.List<byte[]> rocksDBKeys,
            java.util.List<KeyNamespaceKey<K, N>> storageKeys,
            V defaultValue,
            long gen,
            NativeRequestPlaneCoordinator.BatchSlot nativeBatchSlot) {
        prefetchTasksExecuted++;
        try {
            if (nativeBatchSlot != null
                    && executeNativePreparedBatch(
                            rocksDBKeys,
                            storageKeys,
                            defaultValue,
                            gen,
                            nativeBatchSlot,
                            false)) {
                return;
            }
            if (!lazyStagingEnabled) {
                prepareWorkerValueState();
            }
            for (int start = 0; start < rocksDBKeys.size(); start += multiGetChunkSize) {
                if (closed || gen != writeGen) {
                    prefetchStaleAborts++;
                    return;
                }
                int end = Math.min(start + multiGetChunkSize, rocksDBKeys.size());
                fetchPreparedChunkIntoStaging(
                        rocksDBKeys, storageKeys, start, end, defaultValue, gen);
            }
        } catch (Throwable ignored) {
            prefetchWorkerFailures++;
            // Best-effort cache warmup; the authoritative read path is untouched.
        }
    }

    private NativeRequestPlaneCoordinator.BatchSlot prepareNativeBatchSlot(
            java.util.List<byte[]> rocksDBKeys) {
        if (nativeRequestPlaneCoordinator == null
                || !nativeRequestPlaneCoordinator.isActive()
                || !nativeRequestPlaneCoordinator.options().prefetchEnabled()
                || rocksDBKeys.size()
                        < nativeRequestPlaneCoordinator.options().minBatchSize()) {
            if (nativeRequestPlaneCoordinator != null) {
                nativeFallbackBatches++;
            }
            return null;
        }
        NativeRequestPlaneCoordinator.BatchSlot slot =
                nativeRequestPlaneCoordinator.tryAcquireBatchSlot();
        if (slot == null) {
            nativeFallbackBatches++;
            return null;
        }
        try {
            slot.prepareLatest(nativeStateId, nativeWriteEpoch.get(), rocksDBKeys);
            return slot;
        } catch (IOException | RuntimeException failure) {
            nativeFallbackBatches++;
            slot.close();
            return null;
        }
    }

    /**
     * Executes one native probe over the complete prepared batch, compacts misses for the existing
     * RocksDB reader, fills only those misses, and publishes all results through the existing
     * generation-checked staging path.
     *
     * @return true when this batch was handled (including a non-fatal fill rejection); false when
     *     probe failed before any result was published and the caller must use the Java path
     */
    @SuppressWarnings("unchecked")
    private boolean executeNativePreparedBatch(
            java.util.List<byte[]> rocksDBKeys,
            java.util.List<KeyNamespaceKey<K, N>> storageKeys,
            V defaultValue,
            long gen,
            NativeRequestPlaneCoordinator.BatchSlot slot,
            boolean immediate)
            throws Exception {
        if (closed || gen != writeGen || !nativeRequestPlaneCoordinator.isActive()) {
            nativeFallbackBatches++;
            return false;
        }

        final int processed;
        try {
            processed = nativeRequestPlaneCoordinator.probe(slot);
        } catch (RuntimeException | LinkageError failure) {
            nativeRuntimeFailures++;
            nativeFallbackBatches++;
            return false;
        }
        if (processed != rocksDBKeys.size()) {
            IllegalStateException failure =
                    new IllegalStateException(
                            "Native probe processed "
                                    + processed
                                    + " of "
                                    + rocksDBKeys.size()
                                    + " prepared keys.");
            nativeRequestPlaneCoordinator.disable(failure);
            nativeRuntimeFailures++;
            nativeFallbackBatches++;
            return false;
        }
        if (closed || gen != writeGen) {
            prefetchStaleAborts++;
            return true;
        }

        Object[] valuesByOriginalIndex = new Object[processed];
        java.util.ArrayList<byte[]> missKeys = new java.util.ArrayList<>(processed);
        int[] missOriginalIndices = new int[processed];
        int missCount = 0;
        int batchHits = 0;
        long batchHitBytesDirect = 0;
        int batchNegativeHits = 0;
        int batchMisses = 0;
        try {
            for (int i = 0; i < processed; i++) {
                int error = slot.probeError(i);
                int status = slot.probeStatus(i);
                if (error != NativeRequestPlaneBridge.ERROR_OK
                        || (status != NativeRequestPlaneBridge.PROBE_MISS
                                && status != NativeRequestPlaneBridge.PROBE_HIT
                                && status != NativeRequestPlaneBridge.PROBE_NEGATIVE)) {
                    throw new IllegalStateException(
                            "Native probe returned status="
                                    + status
                                    + ", error="
                                    + error
                                    + " at index "
                                    + i
                                    + ".");
                }
                if (status == NativeRequestPlaneBridge.PROBE_HIT) {
                    // Deserialize from the slot-owned direct value arena while the slot is leased.
                    // The materialized value can safely outlive the slot; no per-hit byte[] is
                    // allocated and lazy staging never retains mutable native memory.
                    valuesByOriginalIndex[i] =
                            deserializeNativeProbeValue(slot, i, immediate);
                    batchHitBytesDirect += slot.probeValueLength(i);
                    batchHits++;
                } else if (status == NativeRequestPlaneBridge.PROBE_NEGATIVE) {
                    batchNegativeHits++;
                } else {
                    batchMisses++;
                    missKeys.add(rocksDBKeys.get(i));
                    missOriginalIndices[missCount++] = i;
                }
            }
        } catch (IOException | RuntimeException protocolFailure) {
            nativeRequestPlaneCoordinator.disable(protocolFailure);
            nativeRuntimeFailures++;
            nativeFallbackBatches++;
            return false;
        }
        nativeBatchesActivated++;
        nativeProbeKeys += processed;
        nativeHits += batchHits;
        nativeHitBytesDirect += batchHitBytesDirect;
        nativeNegativeHits += batchNegativeHits;
        nativeMisses += batchMisses;

        java.util.List<byte[]> missValues =
                fetchCompactPreparedMissValues(missKeys, gen);
        if (missValues == null) {
            return true;
        }
        for (int i = 0; i < missValues.size(); i++) {
            valuesByOriginalIndex[missOriginalIndices[i]] = missValues.get(i);
        }

        if (!missKeys.isEmpty() && !closed && gen == writeGen) {
            try {
                slot.prepareFill(
                        nativeStateId, slot.preparedGeneration(), missKeys, missValues);
                int filled = nativeRequestPlaneCoordinator.fill(slot);
                if (filled != missKeys.size()) {
                    IllegalStateException failure =
                            new IllegalStateException(
                                    "Native fill processed "
                                            + filled
                                            + " of "
                                            + missKeys.size()
                                            + " compacted misses.");
                    nativeRequestPlaneCoordinator.disable(failure);
                    nativeRuntimeFailures++;
                    nativeFillRejected += missKeys.size();
                } else {
                    nativeFillBatches++;
                    nativeFillKeys += filled;
                    for (int i = 0; i < filled; i++) {
                        int status = slot.fillStatus(i);
                        int error = slot.fillError(i);
                        boolean accepted =
                                error == NativeRequestPlaneBridge.ERROR_OK
                                        && (status == NativeRequestPlaneBridge.FILL_INSERTED
                                                || status
                                                        == NativeRequestPlaneBridge.FILL_UPDATED);
                        boolean nonFatalRejection =
                                isNonFatalNativeFillRejection(status, error);
                        if (!accepted) {
                            nativeFillRejected++;
                        }
                        if (!accepted && !nonFatalRejection) {
                            IllegalStateException failure =
                                    new IllegalStateException(
                                            "Native fill returned status="
                                                    + status
                                                    + ", error="
                                                    + error
                                                    + " at index "
                                                    + i
                                                    + ".");
                            nativeRequestPlaneCoordinator.disable(failure);
                            nativeRuntimeFailures++;
                            break;
                        }
                    }
                }
            } catch (IOException capacityFailure) {
                // This batch's RocksDB results remain authoritative and are still staged.
                nativeFillRejected += missKeys.size();
            } catch (RuntimeException | LinkageError nativeFailure) {
                nativeRequestPlaneCoordinator.disable(nativeFailure);
                nativeRuntimeFailures++;
                // Do not re-read RocksDB after a fill-side runtime/protocol failure.
            }
        }

        if (closed || gen != writeGen) {
            prefetchStaleAborts++;
            return true;
        }
        if (!immediate && !lazyStagingEnabled) {
            prepareWorkerValueState();
        }
        for (int i = 0; i < processed; i++) {
            if (closed || gen != writeGen) {
                prefetchStaleAborts++;
                return true;
            }
            boolean published;
            if (slot.probeStatus(i) == NativeRequestPlaneBridge.PROBE_HIT) {
                @SuppressWarnings("unchecked")
                V value = (V) valuesByOriginalIndex[i];
                published =
                        publishStagedValue(
                                StagedValue.materialized(storageKeys.get(i), value, gen), false);
            } else if (immediate) {
                byte[] serializedValue = (byte[]) valuesByOriginalIndex[i];
                V value =
                        deserializeImmediateValueOrCopyDefault(serializedValue, defaultValue);
                published =
                        publishStagedValue(
                                StagedValue.materialized(storageKeys.get(i), value, gen),
                                serializedValue == null);
            } else {
                byte[] serializedValue = (byte[]) valuesByOriginalIndex[i];
                published =
                        stagePreparedValue(
                                storageKeys.get(i), serializedValue, defaultValue, gen);
            }
            if (!published) {
                return true;
            }
        }
        return true;
    }

    private V deserializeNativeProbeValue(
            NativeRequestPlaneCoordinator.BatchSlot slot, int index, boolean immediate)
            throws IOException {
        if (immediate) {
            if (immediateValueSerializer == null) {
                immediateValueSerializer = delegate.getValueSerializer().duplicate();
                immediateValueInput = new org.apache.flink.core.memory.DataInputDeserializer();
            }
            return immediateValueSerializer.deserialize(slot.probeValueInput(index));
        }
        prepareWorkerValueState();
        return workerValueSerializer.deserialize(slot.probeValueInput(index));
    }

    private static boolean isNonFatalNativeFillRejection(int status, int error) {
        return (status == NativeRequestPlaneBridge.FILL_REJECTED_STALE_GENERATION
                        && error == NativeRequestPlaneBridge.ERROR_OK)
                || (status == NativeRequestPlaneBridge.FILL_REJECTED_CAPACITY
                        && error == NativeRequestPlaneBridge.ERROR_CAPACITY_EXCEEDED);
    }

    @SuppressWarnings("unchecked")
    private java.util.List<byte[]> fetchCompactPreparedMissValues(
            java.util.List<byte[]> missKeys, long gen) throws Exception {
        if (missKeys.isEmpty()) {
            return java.util.Collections.emptyList();
        }
        RocksDBBatchValueReader<K, N, V> batchReader =
                (RocksDBBatchValueReader<K, N, V>) delegate;
        java.util.ArrayList<byte[]> values = new java.util.ArrayList<>(missKeys.size());
        for (int start = 0; start < missKeys.size(); start += multiGetChunkSize) {
            if (closed || gen != writeGen) {
                prefetchStaleAborts++;
                return null;
            }
            int end = Math.min(start + multiGetChunkSize, missKeys.size());
            java.util.List<byte[]> chunkValues;
            lifecycleLock.readLock().lock();
            try {
                if (closed || gen != writeGen) {
                    prefetchStaleAborts++;
                    return null;
                }
                if (end - start < multiGetMinBatchSize) {
                    chunkValues = new java.util.ArrayList<>(end - start);
                    for (int i = start; i < end; i++) {
                        prefetchPointGetCalls++;
                        chunkValues.add(
                                batchReader.getSerializedValueByRocksDBKey(missKeys.get(i)));
                    }
                } else {
                    prefetchMultiGetCalls++;
                    prefetchMultiGetKeys += end - start;
                    chunkValues =
                            batchReader.getSerializedValuesByRocksDBKeys(
                                    missKeys, start, end);
                }
            } finally {
                lifecycleLock.readLock().unlock();
            }
            if (chunkValues.size() != end - start) {
                throw new IllegalStateException(
                        "RocksDB compacted MultiGet result count does not match miss count.");
            }
            values.addAll(chunkValues);
        }
        return values;
    }

    @SuppressWarnings("unchecked")
    private void fetchPreparedChunkIntoStaging(
            java.util.List<byte[]> rocksDBKeys,
            java.util.List<KeyNamespaceKey<K, N>> storageKeys,
            int start,
            int end,
            V defaultValue,
            long gen)
            throws Exception {
        RocksDBBatchValueReader<K, N, V> batchReader =
                (RocksDBBatchValueReader<K, N, V>) delegate;
        java.util.List<byte[]> valueBytes;
        lifecycleLock.readLock().lock();
        try {
            if (closed || gen != writeGen) {
                prefetchStaleAborts++;
                return;
            }
            if (end - start < multiGetMinBatchSize) {
                valueBytes = new java.util.ArrayList<>(end - start);
                for (int i = start; i < end; i++) {
                    prefetchPointGetCalls++;
                    valueBytes.add(
                            batchReader.getSerializedValueByRocksDBKey(rocksDBKeys.get(i)));
                }
            } else {
                prefetchMultiGetCalls++;
                prefetchMultiGetKeys += end - start;
                valueBytes =
                        batchReader.getSerializedValuesByRocksDBKeys(
                                rocksDBKeys, start, end);
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }

        if (closed || gen != writeGen || valueBytes.size() != end - start) {
            prefetchStaleAborts++;
            return;
        }
        for (int i = 0; i < valueBytes.size(); i++) {
            if (gen != writeGen) {
                prefetchStaleAborts++;
                return;
            }
            byte[] serializedValue = valueBytes.get(i);
            if (!stagePreparedValue(
                    storageKeys.get(start + i), serializedValue, defaultValue, gen)) {
                return;
            }
        }
    }

    private boolean stagePreparedValue(
            KeyNamespaceKey<K, N> storageKey, byte[] valueBytes, V defaultValue, long gen)
            throws IOException {
        StagedValue<V> staged;
        if (lazyStagingEnabled) {
            staged = StagedValue.serialized(storageKey, valueBytes, defaultValue, gen);
        } else {
            V value = deserializeValueOrCopyDefault(valueBytes, defaultValue);
            staged = StagedValue.materialized(storageKey, value, gen);
        }
        return publishStagedValue(staged, valueBytes == null);
    }

    private boolean publishStagedValue(StagedValue<V> staged, boolean missing) {
        lifecycleLock.readLock().lock();
        try {
            if (closed || staged.gen != writeGen) {
                prefetchStaleAborts++;
                return false;
            }
            synchronized (staging) {
                StagedValue<V> previous = staging.get(staged.storageKey());
                if (previous == null && staging.size() >= asyncStagingMaxEntries) {
                    prefetchStagingAdmissionDrops++;
                    return false;
                }
                long retainedBytes =
                        stagingRetainedBytes.get()
                                - (previous == null ? 0L : previous.retainedBytes())
                                + staged.retainedBytes();
                if (retainedBytes > asyncStagingMaxRetainedBytes) {
                    prefetchStagingAdmissionDrops++;
                    return false;
                }
                staging.put(staged.storageKey(), staged);
                stagingRetainedBytes.set(retainedBytes);
            }
            prefetchValuesStaged++;
            if (missing) {
                prefetchMissingValuesStaged++;
            }
            if (staged instanceof SerializedStagedValue<?>) {
                prefetchLazyValuesStaged++;
            }
            return true;
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    private StagedValue<V> removeStagedValue(Object key) {
        synchronized (staging) {
            StagedValue<V> removed = staging.remove(key);
            if (removed != null) {
                stagingRetainedBytes.addAndGet(-removed.retainedBytes());
            }
            return removed;
        }
    }

    private boolean removeStagedValue(Object key, StagedValue<V> expected) {
        synchronized (staging) {
            if (!staging.remove(key, expected)) {
                return false;
            }
            stagingRetainedBytes.addAndGet(-expected.retainedBytes());
            return true;
        }
    }

    private void clearStaging() {
        synchronized (staging) {
            staging.clear();
            stagingRetainedBytes.set(0L);
        }
    }

    @SuppressWarnings("unchecked")
    private void fetchChunksIntoStaging(
            java.util.List<byte[]> serializedKeyAndNamespaces, V defaultValue, long gen)
            throws Exception {
        for (int start = 0; start < serializedKeyAndNamespaces.size(); start += multiGetChunkSize) {
            if (closed || gen != writeGen) {
                prefetchStaleAborts++;
                return;
            }
            int end = Math.min(start + multiGetChunkSize, serializedKeyAndNamespaces.size());
            if (end - start < multiGetMinBatchSize) {
                for (int i = start; i < end; i++) {
                    if (!fetchSingleIntoStaging(
                            serializedKeyAndNamespaces.get(i), defaultValue, gen)) {
                        return;
                    }
                }
                continue;
            }
            fetchChunkIntoStaging(
                    serializedKeyAndNamespaces.subList(start, end), defaultValue, gen);
        }
    }

    @SuppressWarnings("unchecked")
    private void fetchChunkIntoStaging(
            java.util.List<byte[]> serializedKeyAndNamespaces, V defaultValue, long gen)
            throws Exception {
        java.util.List<byte[]> valueBytes;
        lifecycleLock.readLock().lock();
        try {
            if (closed || gen != writeGen) {
                prefetchStaleAborts++;
                return;
            }
            prefetchMultiGetCalls++;
            prefetchMultiGetKeys += serializedKeyAndNamespaces.size();
            valueBytes =
                    ((RocksDBBatchValueReader<K, N, V>) delegate)
                            .getSerializedValues(
                                    serializedKeyAndNamespaces,
                                    workerKeySerializer,
                                    workerNamespaceSerializer);
        } finally {
            lifecycleLock.readLock().unlock();
        }

        if (closed
                || gen != writeGen
                || valueBytes.size() != serializedKeyAndNamespaces.size()) {
            prefetchStaleAborts++;
            return;
        }
        for (int i = 0; i < valueBytes.size(); i++) {
            if (gen != writeGen) {
                prefetchStaleAborts++;
                return;
            }
            byte[] serializedValue = valueBytes.get(i);
            if (!stageSerializedValue(
                    serializedKeyAndNamespaces.get(i),
                    serializedValue,
                    defaultValue,
                    gen,
                    true)) {
                return;
            }
        }
    }

    private boolean fetchSingleIntoStaging(byte[] skn, V defaultValue, long gen) throws Exception {
        byte[] valueBytes;
        lifecycleLock.readLock().lock();
        try {
            if (closed || gen != writeGen) {
                prefetchStaleAborts++;
                return false;
            }
            prefetchPointGetCalls++;
            valueBytes =
                    ((InternalKvState<K, N, V>) delegate)
                            .getSerializedValue(
                                    skn,
                                    workerKeySerializer,
                                    workerNamespaceSerializer,
                                    workerValueSerializer);
        } finally {
            lifecycleLock.readLock().unlock();
        }
        if (valueBytes == null && !(delegate instanceof RocksDBBatchValueReader<?, ?, ?>)) {
            return true;
        }
        if (!closed && gen == writeGen) {
            // InternalKvState does not grant ownership of getSerializedValue()'s byte array.
            // Materialize it on the worker instead of retaining an unowned reference.
            return stageSerializedValue(skn, valueBytes, defaultValue, gen, false);
        } else {
            prefetchStaleAborts++;
            return false;
        }
    }

    private boolean stageSerializedValue(
            byte[] skn,
            byte[] valueBytes,
            V defaultValue,
            long gen,
            boolean callerOwnsValueBytes)
            throws IOException {
        workerKeyNamespaceInput.setBuffer(skn, 0, skn.length);
        K key = workerKeySerializer.deserialize(workerKeyNamespaceInput);
        workerKeyNamespaceInput.readByte(); // magic number
        N namespace = workerNamespaceSerializer.deserialize(workerKeyNamespaceInput);
        KeyNamespaceKey<K, N> storageKey = new KeyNamespaceKey<>(key, namespace);
        StagedValue<V> staged;
        if (lazyStagingEnabled
                && callerOwnsValueBytes
                && delegate instanceof RocksDBBatchValueReader<?, ?, ?>) {
            staged = StagedValue.serialized(storageKey, valueBytes, defaultValue, gen);
        } else {
            V value = deserializeValueOrCopyDefault(valueBytes, defaultValue);
            staged = StagedValue.materialized(storageKey, value, gen);
        }
        return publishStagedValue(staged, valueBytes == null);
    }

    @SuppressWarnings("unchecked")
    private V copyBatchDefaultValueForAsyncTask() {
        if (delegate instanceof RocksDBBatchValueReader<?, ?, ?>) {
            V defaultValue =
                    ((RocksDBBatchValueReader<K, N, V>) delegate).getBatchDefaultValue();
            if (defaultValue != null) {
                if (mailboxDefaultValueSerializer == null) {
                    mailboxDefaultValueSerializer = delegate.getValueSerializer().duplicate();
                }
                return mailboxDefaultValueSerializer.copy(defaultValue);
            }
        }
        return null;
    }

    private V deserializeValueOrCopyDefault(byte[] valueBytes, V defaultValue) throws IOException {
        if (valueBytes == null) {
            return defaultValue == null ? null : workerValueSerializer.copy(defaultValue);
        }
        workerValueInput.setBuffer(valueBytes, 0, valueBytes.length);
        return workerValueSerializer.deserialize(workerValueInput);
    }

    private V materializeStagedValue(StagedValue<V> staged) throws IOException {
        if (!(staged instanceof SerializedStagedValue<?>)) {
            return staged.value;
        }
        SerializedStagedValue<V> serialized = (SerializedStagedValue<V>) staged;
        if (stagedValueSerializer == null) {
            stagedValueSerializer = delegate.getValueSerializer().duplicate();
            stagedValueInput = new org.apache.flink.core.memory.DataInputDeserializer();
        }
        V value;
        if (serialized.serializedValue == null) {
            value =
                    serialized.defaultValue == null
                            ? null
                            : stagedValueSerializer.copy(serialized.defaultValue);
        } else {
            stagedValueInput.setBuffer(
                    serialized.serializedValue, 0, serialized.serializedValue.length);
            value = stagedValueSerializer.deserialize(stagedValueInput);
        }
        prefetchLazyValuesMaterialized++;
        return value;
    }

    public void prefetch(Iterable<? extends K> keys) {
        if (keys == null || currentNamespace == null) {
            return;
        }
        K previousKey = currentKeyProvider.getCurrentKey();
        try {
            for (K key : keys) {
                if (key == null || findCachedValueFor(key, currentNamespace) != null) {
                    continue;
                }
                keyContextSetter.accept(key);
                delegate.setCurrentNamespace(currentNamespace);
                V loaded = delegate.value();
                KeyNamespaceKey<K, N> storageKey =
                        new KeyNamespaceKey<>(
                                key, currentNamespace, keySerializer, namespaceSerializer);
                l1Cache.put(storageKey, CachedValue.of(storageKey, loaded, false));
            }
        } catch (Throwable ignored) {
            // Best-effort cache warmup. Authoritative reads still go through value().
        } finally {
            keyContextSetter.accept(previousKey);
            if (currentNamespace != null) {
                delegate.setCurrentNamespace(currentNamespace);
            }
        }
    }

    private void updateSticky(KeyNamespaceKey<K, N> key, CachedValue<V> value) {
        lastAccessKey = key;
        lastAccessValue = value;
    }

    private CachedValue<V> findCachedValue(K currentKey) {
        if (lastAccessKey != null && lastAccessKey.isSame(currentKey, currentNamespace)) {
            return lastAccessValue;
        }
        return findCachedValueFor(currentKey, currentNamespace);
    }

    private CachedValue<V> findCachedValueFor(K key, N namespace) {
        setLookupKey(key, namespace);
        CachedValue<V> l1Value = l1Cache.get(lookupKey);
        if (l1Value != null) {
            return l1Value;
        }
        return l2Cache.get(lookupKey);
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
        // Demote to L2

        if (value.dirty) {
            // Write-Back: Flush to Delegate first (because L2 is Write-Through / Clean)
            // Or put to L2 and let L2 write-through?
            // "L2 Write-Through" implies: Putting to L2 triggers write to delegate.

            // 1. Write to Delegate
            flushEntryToDelegate(key, value);

            // 2. Put to L2 (Clean)
            l2Cache.put(key, CachedValue.of(key, value.valueOrNull(), false));

        } else {
            // Clean L1 eviction: Just move to L2
            l2Cache.put(key, value);
        }
    }

    // L2 Eviction Listener
    private void onL2Eviction(KeyNamespaceKey<K, N> key, CachedValue<V> value) {
        // L2 is clean (backed by delegate). Just drop.
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

    private void flushEntryToDelegate(KeyNamespaceKey<K, N> key, CachedValue<V> value) {
        lifecycleLock.readLock().lock();
        try {
            if (closed) {
                return;
            }

            // A dirty flush changes RocksDB content: a concurrent prefetch read may now be stale.
            long nativeEpoch = advanceWriteGeneration();
            // Save current context
            K previousKey = currentKeyProvider.getCurrentKey();
            // We rely on 'currentNamespace' field in this class but it might have changed.
            // We must use the namespace from the key.

            keyContextSetter.accept(key.key);
            // delegate.setCurrentNamespace(key.namespace); // delegate namespace must be
            // set before update
            // The delegate might look at its own currentNamespace.
            // However, 'key.namespace' is the correct one for this entry.
            // We need to ensure we restore the *previous* namespace of the delegate if we
            // change it.
            // Actually, we don't have access to delegate's internal 'currentNamespace'
            // easily to restore it?
            // But 'setCurrentNamespace' updates 'delegate's currentNamespace.
            // We can just rely on 'this.currentNamespace' being the "logic" current
            // namespace,
            // but 'flushEntryToDelegate' is called for arbitrary keys (eviction).
            // So we must change it.
            // And then restore it to 'this.currentNamespace' (which is what the user
            // expects).

            delegate.setCurrentNamespace(key.namespace);
            try {
                if (value.isNull) {
                    delegate.clear();
                } else {
                    delegate.update(value.value);
                }
                publishNativeMutation(
                        key.key, key.namespace, value.isNull ? null : value.value, nativeEpoch);
            } catch (IOException e) {
                throw new RuntimeException("Failed to flush state to delegate interaction", e);
            } finally {
                // Restore context
                keyContextSetter.accept(previousKey);
                if (currentNamespace != null) {
                    delegate.setCurrentNamespace(currentNamespace);
                }
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    private long advanceWriteGeneration() {
        writeGen++;
        if (nativeRequestPlaneCoordinator != null) {
            nativeGenerationAdvances++;
            return nativeWriteEpoch.incrementAndGet();
        }
        return 0L;
    }

    @SuppressWarnings("unchecked")
    private void publishNativeMutation(
            K key, N namespace, V value, long nativeEpoch) {
        if (nativeRequestPlaneCoordinator == null
                || !nativeRequestPlaneCoordinator.isActive()
                || !nativeRequestPlaneCoordinator.options().valueCacheEnabled()
                || !(delegate instanceof RocksDBBatchValueReader<?, ?, ?>)) {
            return;
        }
        nativeMutationAttempts++;
        if (!nativeRequestPlaneCoordinator.options().writeThroughMutations()) {
            nativeMutationWriteThroughSkipped++;
            return;
        }
        RocksDBBatchValueReader<K, N, V> batchReader =
                (RocksDBBatchValueReader<K, N, V>) delegate;
        try {
            byte[] preparedKey =
                    batchReader.serializeBatchKeyAndNamespace(
                            key,
                            namespace,
                            nativeMutationKeySerializer,
                            nativeMutationNamespaceSerializer);
            byte[] serializedValue =
                    KvStateSerializer.serializeValue(
                            value, nativeMutationValueSerializer);
            int status =
                    nativeRequestPlaneCoordinator.updateExactKey(
                            nativeStateId, nativeEpoch, preparedKey, serializedValue);
            if (status == NativeRequestPlaneBridge.FILL_REJECTED_STALE_GENERATION) {
                nativeMutationSuperseded++;
            } else {
                nativeMutationApplied++;
                if (value == null) {
                    nativeMutationTombstonesApplied++;
                }
            }
        } catch (Exception | LinkageError failure) {
            nativeRequestPlaneCoordinator.disable(failure);
            nativeMutationFailures++;
            nativeRuntimeFailures++;
        }
    }

    private String nativeDisableCauseForAudit() {
        if (nativeRequestPlaneCoordinator == null
                || nativeRequestPlaneCoordinator.disableCause() == null) {
            return "none";
        }
        Throwable cause = nativeRequestPlaneCoordinator.disableCause();
        return cause.getClass().getSimpleName() + ":" + String.valueOf(cause.getMessage());
    }

    private static final class NativePointCacheResult<V> {
        private static final NativePointCacheResult<?> MISS =
                new NativePointCacheResult<>(false, null);

        private final boolean hit;
        private final V value;

        private NativePointCacheResult(boolean hit, V value) {
            this.hit = hit;
            this.value = value;
        }

        @SuppressWarnings("unchecked")
        private static <V> NativePointCacheResult<V> miss() {
            return (NativePointCacheResult<V>) MISS;
        }

        private static <V> NativePointCacheResult<V> hit(V value) {
            return new NativePointCacheResult<>(true, value);
        }
    }

    private static final class KeyNamespaceKey<K, N> {
        private K key;
        private N namespace;

        // Mutable constructor
        private KeyNamespaceKey(K key, N namespace) {
            this.key = key;
            this.namespace = namespace;
        }

        // Storage constructor (Deep Copy)
        private KeyNamespaceKey(K key, N namespace, TypeSerializer<K> keySerializer,
                TypeSerializer<N> namespaceSerializer) {
            this.key = keySerializer != null ? keySerializer.copy(key) : key;
            this.namespace = namespaceSerializer != null ? namespaceSerializer.copy(namespace) : namespace;
        }

        boolean isSame(K otherKey, N otherNamespace) {
            return Objects.equals(this.key, otherKey) && Objects.equals(this.namespace, otherNamespace);
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
            return CacheKeyHash.hash(key, namespace);
        }
    }

    /**
     * A value fetched by prefetch and tagged with its write generation.
     *
     * <p>Serialized payloads take exclusive ownership of the RocksDB-returned byte array and never
     * mutate it. The optional default value is copied with a mailbox-confined serializer before
     * the task is published; mailbox promotion makes one further copy per missing entry.
     */
    private static class StagedValue<V> {
        private final KeyNamespaceKey<?, ?> storageKey;
        private final V value;
        private final long gen;

        private StagedValue(KeyNamespaceKey<?, ?> storageKey, V value, long gen) {
            this.storageKey = storageKey;
            this.value = value;
            this.gen = gen;
        }

        private static <V> StagedValue<V> materialized(
                KeyNamespaceKey<?, ?> storageKey, V value, long gen) {
            return new StagedValue<>(storageKey, value, gen);
        }

        private static <V> StagedValue<V> serialized(
                KeyNamespaceKey<?, ?> storageKey,
                byte[] serializedValue,
                V defaultValue,
                long gen) {
            return new SerializedStagedValue<>(storageKey, serializedValue, defaultValue, gen);
        }

        @SuppressWarnings("unchecked")
        private <K, N> KeyNamespaceKey<K, N> storageKey() {
            return (KeyNamespaceKey<K, N>) storageKey;
        }

        long retainedBytes() {
            return 0L;
        }
    }

    private static final class SerializedStagedValue<V> extends StagedValue<V> {
        private final byte[] serializedValue;
        private final V defaultValue;

        private SerializedStagedValue(
                KeyNamespaceKey<?, ?> storageKey,
                byte[] serializedValue,
                V defaultValue,
                long gen) {
            super(storageKey, null, gen);
            this.serializedValue = serializedValue;
            this.defaultValue = defaultValue;
        }

        @Override
        long retainedBytes() {
            return serializedValue == null ? 0L : serializedValue.length;
        }
    }

    private static final class CachedValue<V> {
        private final KeyNamespaceKey<?, ?> storageKey;
        private V value;
        private boolean isNull;
        private boolean dirty;

        private CachedValue(
                KeyNamespaceKey<?, ?> storageKey, V value, boolean isNull, boolean dirty) {
            this.storageKey = storageKey;
            this.value = value;
            this.isNull = isNull;
            this.dirty = dirty;
        }

        static <V> CachedValue<V> of(
                KeyNamespaceKey<?, ?> storageKey, V value, boolean dirty) {
            return new CachedValue<>(storageKey, value, value == null, dirty);
        }

        @SuppressWarnings("unchecked")
        private <K, N> KeyNamespaceKey<K, N> storageKey() {
            return (KeyNamespaceKey<K, N>) storageKey;
        }

        private void replace(V value, boolean dirty) {
            this.value = value;
            this.isNull = value == null;
            this.dirty = dirty;
        }

        V valueOrNull() {
            return isNull ? null : value;
        }
    }
}
