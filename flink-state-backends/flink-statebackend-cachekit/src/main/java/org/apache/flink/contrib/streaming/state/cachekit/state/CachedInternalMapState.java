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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBBatchMapReader;
import org.apache.flink.contrib.streaming.state.RocksDBBatchValueReader;
import org.apache.flink.contrib.streaming.state.cachekit.PrefetchExecutor;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicy;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CaffeineCachePolicy;
import org.apache.flink.contrib.streaming.state.cachekit.cache.LruCachePolicy;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PresenceCacheImplementation;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PrimitivePresenceCache;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneBridge;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneCoordinator;
import org.apache.flink.contrib.streaming.state.cachekit.util.MurmurHash3;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.internal.BatchPrefetchableMapState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal {@link InternalMapState} wrapper that adds a per-state cache for entries and presence.
 *
 * <p>Keying: (currentKey, namespace, userKey).
 */
public final class CachedInternalMapState<K, N, UK, UV>
        implements InternalMapState<K, N, UK, UV>, BatchPrefetchableMapState<UK> {

    private static final Logger LOG = LoggerFactory.getLogger(CachedInternalMapState.class);

    private enum NativeSnapshotAdaptiveMode {
        EVALUATE,
        BYPASS
    }

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
    private final java.util.function.Consumer<K> keyContextSetter;
    private final boolean bypassEnabled;
    private final double hitRateThreshold;
    private final int hitRateWindow;
    private final boolean iterationCacheFillEnabled;
    private final boolean usePrimitivePresenceCache;
    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<UK> userKeySerializer;
    private final TypeSerializer<UV> userValueSerializer;
    private final ThreadLocal<DataOutputSerializer> serializerView;
    private final NativeRequestPlaneCoordinator nativeRequestPlaneCoordinator;
    private final boolean nativeMapCacheEnabled;
    private final int nativeStateId;
    private final boolean nativeMapSnapshotEnabled;
    private final int nativeSnapshotStateId;
    private final DataOutputSerializer nativeComponentOutput;
    private long nativeGeneration;
    private long nativeProbeAttempts;
    private long nativeHits;
    private long nativeNegativeHits;
    private long nativeMisses;
    private long nativeFills;
    private long nativeFallbacks;
    private long nativeFailures;
    private long nativeSnapshotProbes;
    private long nativeSnapshotHits;
    private long nativeSnapshotNegativeHits;
    private long nativeSnapshotMisses;
    private long nativeSnapshotFills;
    private long nativeSnapshotFallbacks;
    private final boolean nativeSnapshotAdaptiveBypassEnabled;
    private final int nativeSnapshotAdaptiveWindowProbes;
    private final double nativeSnapshotAdaptiveMinUsefulHitRate;
    private final int nativeSnapshotAdaptiveResampleIntervalProbes;
    private NativeSnapshotAdaptiveMode nativeSnapshotAdaptiveMode =
            NativeSnapshotAdaptiveMode.EVALUATE;
    private long nativeSnapshotAdaptiveWindowProbeCount;
    private long nativeSnapshotAdaptiveWindowPositiveHits;
    private long nativeSnapshotAdaptiveWindowNegativeHits;
    private long nativeSnapshotAdaptiveWindowMisses;
    private long nativeSnapshotAdaptiveWindowFillAttempts;
    private long nativeSnapshotAdaptiveBypassClock;
    private long nativeSnapshotAdaptiveEvaluatedWindows;
    private long nativeSnapshotAdaptiveBypassTransitions;
    private long nativeSnapshotAdaptiveTrialTransitions;
    private long nativeSnapshotAdaptiveBypassedProbes;
    private long nativeSnapshotAdaptiveBypassedFills;
    private final RocksDBBatchMapReader<UK> rocksDBBatchMapReader;
    private final DataInputDeserializer batchPrefetchInput = new DataInputDeserializer();
    private final Map<UK, PrefetchedMapValue<UV>> batchPrefetchStaging = new HashMap<>();
    private boolean nativeDistinctBatchPrefetchEnabled;
    private boolean nativeDistinctBatchDirectArenaEnabled;
    private boolean nativeDistinctBatchDirectArenaDisabled;
    private int nativeDistinctBatchAsyncMinUniqueKeys = 2;
    private boolean nativeDistinctBatchWorkFirstEnabled;
    private boolean nativeDistinctBatchDeferredWaveEnabled;
    private boolean batchPrefetchActive;
    private K batchPrefetchOuterKey;
    private N batchPrefetchNamespace;
    private long batchPrefetchAttempts;
    private long batchPrefetchBatches;
    private long batchPrefetchInputKeys;
    private long batchPrefetchUniqueKeys;
    private long batchPrefetchFound;
    private long batchPrefetchMissing;
    private long batchPrefetchHits;
    private long batchPrefetchDirectOverlayValues;
    private long batchPrefetchFallbacks;
    private long batchPrefetchFailures;
    private long batchPrefetchDirectArenaAttempts;
    private long batchPrefetchDirectArenaBatches;
    private long batchPrefetchDirectArenaKeys;
    private long batchPrefetchDirectArenaCompletedBatches;
    private long batchPrefetchDirectArenaCompletedKeys;
    private long batchPrefetchDirectArenaFound;
    private long batchPrefetchDirectArenaMissing;
    private long batchPrefetchDirectArenaOverflows;
    private long batchPrefetchDirectArenaFallbacks;
    private long batchPrefetchDirectArenaFailures;
    private long batchPrefetchDirectArenaProtocolFallbacks;
    private final Object asyncBatchPrefetchMonitor = new Object();
    private final Set<PrefetchExecutor.DropAwareTask> outstandingAsyncBatchPrefetchTasks =
            Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchAttempts =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchSubmitted =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchCompleted =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchDropped =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchFailures =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchKeys =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchAwaits =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchReadyBeforeAwait =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchWaitNanos =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchQueueNanos =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchServiceNanos =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchCallerRuns =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchWorkFirstEligible =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchCallerRunKeys =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchCallerRunNanos =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchWorkFirstWorkerIdle =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchWorkFirstBacklogEmpty =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchWorkFirstPermitBusy =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchQueuedSubmissions =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncBatchPrefetchReadySlackNanos =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncDirectArenaBatches =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncDirectArenaKeys =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncDirectArenaCompletedBatches =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncDirectArenaCompletedKeys =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncDirectArenaFound =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncDirectArenaMissing =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncDirectArenaLeaseMisses =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncDirectArenaOverflows =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncDirectArenaProtocolFailures =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong asyncDirectArenaPreparationFallbacks =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredSyncBatchPrefetchBatches =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredSyncBatchPrefetchKeys =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredSyncBatchPrefetchCompleted =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredSyncBatchPrefetchFallbacks =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong
            deferredSyncBatchPrefetchContextMismatches =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveAttempts =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveWindows =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveSubmitted =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveDropped =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveGroups =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveKeys =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveJniCalls =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveCompletedGroups =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveCancelledGroups =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveFallbackWindows =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveFailures =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveOwnerRejects =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveOverflows =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveProtocolFailures =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveAwaits =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveReadyBeforeAwait =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveWaitNanos =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveQueueNanos =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveServiceNanos =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveReadySlackNanos =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveFound =
            new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong deferredWaveMissing =
            new java.util.concurrent.atomic.AtomicLong();

    // --- MapSnapshot cache (entries() fast path) ---
    private final CachePolicy<KeyNamespace<K, N>, MapSnapshot<UK>> mapSnapshotCache;
    private final boolean mapSnapshotCacheEnabled;
    private final int mapSnapshotSmallMaxEntries;
    private final MapSnapshotCacheMetrics mapSnapshotCacheMetrics;
    /** Reusable probe key for snapshot cache lookups (avoids allocation per lookup). */
    private final KeyNamespace<K, N> snapshotProbe = new KeyNamespace<>(null, null);
    /** Dirty MapState write-back entries, partitioned by key and namespace for scoped flushes. */
    private final Map<KeyNamespace<K, N>, Set<KeyNamespaceUserKey<K, N, UK>>>
            dirtyValueEntriesByNamespace = new HashMap<>();
    /** Reusable lookup key for {@link #dirtyValueEntriesByNamespace}. */
    private final KeyNamespace<K, N> dirtyNamespaceProbe = new KeyNamespace<>(null, null);

    private N currentNamespace;
    private final KeyNamespaceUserKey<K, N, UK> lookupKey =
            new KeyNamespaceUserKey<>(null, null, null);

    private volatile boolean isBypassing = false;
    private long currentWindowAccesses = 0;
    private long currentWindowHits = 0;
    private int opsSinceLastSample = 0;

    /**
     * Prevents dirty-cache write-back from racing backend teardown. Cache eviction can call {@link
     * #flushEntryToDelegate(KeyNamespaceUserKey, CachedMapValue)} on the task thread while {@code
     * CacheKitKeyedStateBackend.dispose()} releases RocksDB column-family handles.
     */
    private volatile boolean closed;

    private final java.util.concurrent.locks.ReadWriteLock lifecycleLock =
            new java.util.concurrent.locks.ReentrantReadWriteLock();

    public CachedInternalMapState(
            InternalMapState<K, N, UK, UV> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
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
            boolean iterationCacheFillEnabled,
            int mapSnapshotCacheMaxEntries) {
        this(
                delegate,
                currentKeyProvider,
                keyContextSetter,
                maxEntries,
                cachePolicyType,
                lruOverflow,
                presenceCacheImplementation,
                mapCacheMaxEntries,
                mapCachePolicyType,
                mapCacheLruOverflow,
                bypassEnabled,
                hitRateThreshold,
                hitRateWindow,
                iterationCacheFillEnabled,
                mapSnapshotCacheMaxEntries,
                MapSnapshotCacheMetrics.disabled());
    }

    public CachedInternalMapState(
            InternalMapState<K, N, UK, UV> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
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
            boolean iterationCacheFillEnabled,
            int mapSnapshotCacheMaxEntries,
            MapSnapshotCacheMetrics mapSnapshotCacheMetrics) {
        this(
                delegate,
                currentKeyProvider,
                keyContextSetter,
                maxEntries,
                cachePolicyType,
                lruOverflow,
                presenceCacheImplementation,
                mapCacheMaxEntries,
                mapCachePolicyType,
                mapCacheLruOverflow,
                bypassEnabled,
                hitRateThreshold,
                hitRateWindow,
                iterationCacheFillEnabled,
                mapSnapshotCacheMaxEntries,
                mapSnapshotCacheMetrics,
                null,
                0,
                false,
                0,
                false);
    }

    public CachedInternalMapState(
            InternalMapState<K, N, UK, UV> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
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
            boolean iterationCacheFillEnabled,
            int mapSnapshotCacheMaxEntries,
            MapSnapshotCacheMetrics mapSnapshotCacheMetrics,
            NativeRequestPlaneCoordinator nativeRequestPlaneCoordinator,
            int nativeStateId,
            boolean nativeMapCacheEnabled,
            int nativeSnapshotStateId,
            boolean nativeMapSnapshotEnabled) {
        this(
                delegate,
                currentKeyProvider,
                keyContextSetter,
                maxEntries,
                cachePolicyType,
                lruOverflow,
                presenceCacheImplementation,
                mapCacheMaxEntries,
                mapCachePolicyType,
                mapCacheLruOverflow,
                bypassEnabled,
                hitRateThreshold,
                hitRateWindow,
                iterationCacheFillEnabled,
                mapSnapshotCacheMaxEntries,
                mapSnapshotCacheMetrics,
                nativeRequestPlaneCoordinator,
                nativeStateId,
                nativeMapCacheEnabled,
                nativeSnapshotStateId,
                nativeMapSnapshotEnabled,
                1);
    }

    public CachedInternalMapState(
            InternalMapState<K, N, UK, UV> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
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
            boolean iterationCacheFillEnabled,
            int mapSnapshotCacheMaxEntries,
            MapSnapshotCacheMetrics mapSnapshotCacheMetrics,
            NativeRequestPlaneCoordinator nativeRequestPlaneCoordinator,
            int nativeStateId,
            boolean nativeMapCacheEnabled,
            int nativeSnapshotStateId,
            boolean nativeMapSnapshotEnabled,
            int mapSnapshotSmallMaxEntries) {
        this(
                delegate,
                currentKeyProvider,
                keyContextSetter,
                maxEntries,
                cachePolicyType,
                lruOverflow,
                presenceCacheImplementation,
                mapCacheMaxEntries,
                mapCachePolicyType,
                mapCacheLruOverflow,
                bypassEnabled,
                hitRateThreshold,
                hitRateWindow,
                iterationCacheFillEnabled,
                mapSnapshotCacheMaxEntries,
                mapSnapshotCacheMetrics,
                nativeRequestPlaneCoordinator,
                nativeStateId,
                nativeMapCacheEnabled,
                nativeSnapshotStateId,
                nativeMapSnapshotEnabled,
                mapSnapshotSmallMaxEntries,
                false,
                8192,
                0.02,
                262144);
    }

    public CachedInternalMapState(
            InternalMapState<K, N, UK, UV> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
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
            boolean iterationCacheFillEnabled,
            int mapSnapshotCacheMaxEntries,
            MapSnapshotCacheMetrics mapSnapshotCacheMetrics,
            NativeRequestPlaneCoordinator nativeRequestPlaneCoordinator,
            int nativeStateId,
            boolean nativeMapCacheEnabled,
            int nativeSnapshotStateId,
            boolean nativeMapSnapshotEnabled,
            int mapSnapshotSmallMaxEntries,
            boolean nativeSnapshotAdaptiveBypassEnabled,
            int nativeSnapshotAdaptiveWindowProbes,
            double nativeSnapshotAdaptiveMinUsefulHitRate,
            int nativeSnapshotAdaptiveResampleIntervalProbes) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.rocksDBBatchMapReader =
                delegate instanceof RocksDBBatchMapReader
                        ? (RocksDBBatchMapReader<UK>) delegate
                        : null;
        this.currentKeyProvider = Objects.requireNonNull(currentKeyProvider, "currentKeyProvider");
        this.keyContextSetter = Objects.requireNonNull(keyContextSetter, "keyContextSetter");
        this.mapSnapshotCacheMetrics =
                Objects.requireNonNull(mapSnapshotCacheMetrics, "mapSnapshotCacheMetrics");
        this.presenceCachePolicyType = Objects.requireNonNull(cachePolicyType, "cachePolicyType");
        this.presenceCacheLruOverflow = Math.max(0, lruOverflow);
        this.presenceCacheEnabled = maxEntries > 0;
        this.presenceCacheImplementation =
                Objects.requireNonNull(presenceCacheImplementation, "presenceCacheImplementation");
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
        TypeSerializer<UV> resolvedUserValueSerializer = null;
        TypeSerializer<Map<UK, UV>> valueSerializer = delegate.getValueSerializer();
        if (valueSerializer instanceof MapSerializer) {
            resolvedUserKeySerializer =
                    ((MapSerializer<UK, UV>) valueSerializer).getKeySerializer();
            resolvedUserValueSerializer =
                    ((MapSerializer<UK, UV>) valueSerializer).getValueSerializer();
        }
        this.userKeySerializer = resolvedUserKeySerializer;
        this.userValueSerializer = resolvedUserValueSerializer;
        if (nativeMapCacheEnabled
                && (nativeRequestPlaneCoordinator == null
                        || resolvedUserKeySerializer == null
                        || resolvedUserValueSerializer == null)) {
            throw new IllegalArgumentException(
                    "Native MapState cache requires an active coordinator and MapSerializer key/value serializers.");
        }
        if (nativeMapSnapshotEnabled
                && (nativeRequestPlaneCoordinator == null || resolvedUserKeySerializer == null)) {
            throw new IllegalArgumentException(
                    "Native MapState snapshot requires an active coordinator and MapSerializer key serializer.");
        }
        this.nativeRequestPlaneCoordinator = nativeRequestPlaneCoordinator;
        this.nativeMapCacheEnabled = nativeMapCacheEnabled;
        this.nativeStateId = nativeStateId;
        this.nativeMapSnapshotEnabled = nativeMapSnapshotEnabled;
        this.nativeSnapshotStateId = nativeSnapshotStateId;
        if (nativeSnapshotAdaptiveBypassEnabled && !nativeMapSnapshotEnabled) {
            throw new IllegalArgumentException(
                    "Native MapSnapshot adaptive bypass requires native MapSnapshot.");
        }
        if (nativeSnapshotAdaptiveBypassEnabled
                && (nativeSnapshotAdaptiveWindowProbes < 2
                        || Double.isNaN(nativeSnapshotAdaptiveMinUsefulHitRate)
                        || Double.isInfinite(nativeSnapshotAdaptiveMinUsefulHitRate)
                        || nativeSnapshotAdaptiveMinUsefulHitRate < 0.0
                        || nativeSnapshotAdaptiveMinUsefulHitRate > 1.0
                        || nativeSnapshotAdaptiveResampleIntervalProbes <= 0)) {
            throw new IllegalArgumentException(
                    "Native MapSnapshot adaptive window must be at least 2, resample must be positive, and useful-hit rate must be in [0, 1].");
        }
        this.nativeSnapshotAdaptiveBypassEnabled = nativeSnapshotAdaptiveBypassEnabled;
        this.nativeSnapshotAdaptiveWindowProbes = nativeSnapshotAdaptiveWindowProbes;
        this.nativeSnapshotAdaptiveMinUsefulHitRate = nativeSnapshotAdaptiveMinUsefulHitRate;
        this.nativeSnapshotAdaptiveResampleIntervalProbes =
                nativeSnapshotAdaptiveResampleIntervalProbes;
        // The native snapshot codec currently represents only EMPTY/SINGLE. Keep bounded
        // multi-key snapshots on the Java cache until that codec gains an explicit list format.
        this.mapSnapshotSmallMaxEntries = Math.max(1, Math.min(16, mapSnapshotSmallMaxEntries));
        this.nativeComponentOutput =
                nativeMapCacheEnabled || nativeMapSnapshotEnabled
                        ? new DataOutputSerializer(128)
                        : null;
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
                this.l1PrimitivePresenceCache =
                        new PrimitivePresenceCache(l1Size, this::onL1PrimitiveEviction);
                this.l2PrimitivePresenceCache =
                        new PrimitivePresenceCache(maxEntries, this::onL2PrimitiveEviction);
                this.l1PresenceCache = new NoOpCachePolicy<>();
                this.l2PresenceCache = new NoOpCachePolicy<>();
            } else {
                this.l1PresenceCache =
                        createCachePolicy(
                                l1Size,
                                presenceCachePolicyType,
                                presenceCacheLruOverflow,
                                this::onL1Eviction);
                this.l2PresenceCache =
                        createCachePolicy(
                                maxEntries,
                                presenceCachePolicyType,
                                presenceCacheLruOverflow,
                                this::onL2Eviction);
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
            this.l1ValueCache =
                    createCachePolicy(
                            l1Size,
                            mapCachePolicyType,
                            mapCacheLruOverflow,
                            this::onValueL1Eviction);
            this.l2ValueCache =
                    createCachePolicy(
                            mapCacheMaxEntries,
                            mapCachePolicyType,
                            mapCacheLruOverflow,
                            this::onValueL2Eviction);
        } else {
            this.l1ValueCache = new NoOpCachePolicy<>();
            this.l2ValueCache = new NoOpCachePolicy<>();
        }

        // MapSnapshot cache initialization
        this.mapSnapshotCacheEnabled = mapSnapshotCacheMaxEntries > 0;
        if (mapSnapshotCacheEnabled) {
            this.mapSnapshotCache =
                    new LruCachePolicy<>(
                            mapSnapshotCacheMaxEntries,
                            64,
                            (key, value) -> mapSnapshotCacheMetrics.recordEviction());
        } else {
            this.mapSnapshotCache = new NoOpCachePolicy<>();
        }
    }

    // Helper to update lookup key safely without allocation
    private void setLookupKey(K key, N namespace, UK userKey) {
        lookupKey.key = key;
        lookupKey.namespace = namespace;
        lookupKey.userKey = userKey;
    }

    /** Enables the separately gated exact-DISTINCT native RocksDB MultiGet path. */
    public void enableNativeDistinctBatchPrefetch(boolean enabled) {
        enableNativeDistinctBatchPrefetch(enabled, false);
    }

    /** Enables the exact-DISTINCT path and its independently gated direct-arena value transport. */
    public void enableNativeDistinctBatchPrefetch(boolean enabled, boolean directArenaEnabled) {
        enableNativeDistinctBatchPrefetch(enabled, directArenaEnabled, 2);
    }

    /** Enables async preparation only above a separately tunable exact-key threshold. */
    public void enableNativeDistinctBatchPrefetch(
            boolean enabled, boolean directArenaEnabled, int asyncMinUniqueKeys) {
        enableNativeDistinctBatchPrefetch(enabled, directArenaEnabled, asyncMinUniqueKeys, false);
    }

    /** Enables the backlog-triggered work-first policy for direct-arena async preparation. */
    public void enableNativeDistinctBatchPrefetch(
            boolean enabled,
            boolean directArenaEnabled,
            int asyncMinUniqueKeys,
            boolean workFirstEnabled) {
        enableNativeDistinctBatchPrefetch(
                enabled, directArenaEnabled, asyncMinUniqueKeys, workFirstEnabled, false);
    }

    /** Enables a single-owner, mailbox-owned direct MultiGet wave across prepared outer keys. */
    public void enableNativeDistinctBatchPrefetch(
            boolean enabled,
            boolean directArenaEnabled,
            int asyncMinUniqueKeys,
            boolean workFirstEnabled,
            boolean deferredWaveEnabled) {
        this.nativeDistinctBatchPrefetchEnabled = enabled;
        this.nativeDistinctBatchDirectArenaEnabled = enabled && directArenaEnabled;
        this.nativeDistinctBatchAsyncMinUniqueKeys = Math.max(2, asyncMinUniqueKeys);
        this.nativeDistinctBatchWorkFirstEnabled =
                enabled && directArenaEnabled && workFirstEnabled;
        this.nativeDistinctBatchDeferredWaveEnabled =
                enabled && directArenaEnabled && deferredWaveEnabled;
    }

    @Override
    public boolean supportsDirectPrefetchedValues() {
        return nativeDistinctBatchPrefetchEnabled && rocksDBBatchMapReader != null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<UV> prefetchCurrentUniqueKeyValues(List<? extends UK> uniqueUserKeys)
            throws Exception {
        endPrefetchCurrentKeys();
        batchPrefetchAttempts++;
        if (!supportsDirectPrefetchedValues()) {
            batchPrefetchFallbacks++;
            return null;
        }
        K currentKey = currentKeyProvider.getCurrentKey();
        N namespace = currentNamespace;
        if (currentKey == null || namespace == null || uniqueUserKeys.size() < 2) {
            batchPrefetchFallbacks++;
            return null;
        }

        // The generated MapView layer already copied, de-duplicated and insertion-ordered this
        // list. RocksDBBatchMapReader is read-only with respect to it, so retain the same list and
        // avoid a second LinkedHashSet plus ArrayList allocation on every tiny outer-key batch.
        List<UK> orderedKeys = (List<UK>) (List<?>) uniqueUserKeys;
        batchPrefetchInputKeys += orderedKeys.size();
        batchPrefetchUniqueKeys += orderedKeys.size();
        try {
            flushCurrentKey(currentKey);
            ensureDelegateNamespace(currentKey);
            if (nativeDistinctBatchDirectArenaEnabled) {
                List<UV> directValues = tryDirectArenaPrefetch(orderedKeys);
                if (directValues != null) {
                    batchPrefetchBatches++;
                    batchPrefetchDirectOverlayValues += directValues.size();
                    return directValues;
                }
                batchPrefetchDirectArenaFallbacks++;
            }
            List<byte[]> rawValues =
                    rocksDBBatchMapReader.getSerializedValuesByUserKeys(orderedKeys);
            if (rawValues.size() != orderedKeys.size()) {
                throw new IllegalStateException(
                        "MapState MultiGet returned "
                                + rawValues.size()
                                + " values for "
                                + orderedKeys.size()
                                + " keys.");
            }
            ArrayList<UV> values = new ArrayList<>(rawValues.size());
            for (byte[] rawValue : rawValues) {
                if (rawValue == null) {
                    values.add(null);
                    batchPrefetchMissing++;
                } else {
                    batchPrefetchInput.setBuffer(rawValue);
                    boolean isNull = batchPrefetchInput.readBoolean();
                    values.add(isNull ? null : userValueSerializer.deserialize(batchPrefetchInput));
                    batchPrefetchFound++;
                }
            }
            batchPrefetchBatches++;
            batchPrefetchDirectOverlayValues += values.size();
            return values;
        } catch (Exception | LinkageError failure) {
            batchPrefetchFailures++;
            batchPrefetchFallbacks++;
            return null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public PreparedValues prepareCurrentUniqueKeyValues(List<? extends UK> uniqueUserKeys)
            throws Exception {
        asyncBatchPrefetchAttempts.incrementAndGet();
        if (!supportsDirectPrefetchedValues() || closed) {
            return null;
        }
        K currentKey = currentKeyProvider.getCurrentKey();
        N namespace = currentNamespace;
        if (currentKey == null || namespace == null || uniqueUserKeys.size() < 2) {
            return null;
        }

        // Generated code already supplies stable, de-duplicated keys. Serialize the complete
        // RocksDB keys while this outer key/namespace is current; the worker never reads mutable
        // Flink key context or serializers.
        List<UK> orderedKeys = (List<UK>) (List<?>) uniqueUserKeys;
        if (nativeDistinctBatchDeferredWaveEnabled
                && orderedKeys.size() < nativeDistinctBatchAsyncMinUniqueKeys
                && orderedKeys.size() <= RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH) {
            KeyNamespace<K, N> stableContext = newStoredKeyNamespace(currentKey, namespace);
            flushCurrentKey(currentKey);
            ensureDelegateNamespace(currentKey);
            List<byte[]> rocksDBKeys =
                    rocksDBBatchMapReader.serializeRocksDBKeysByUserKeys(orderedKeys);
            if (rocksDBKeys.size() != orderedKeys.size()) {
                return null;
            }
            deferredSyncBatchPrefetchBatches.incrementAndGet();
            deferredSyncBatchPrefetchKeys.addAndGet(orderedKeys.size());
            return new DeferredDirectMapValues(
                    stableContext.key,
                    stableContext.namespace,
                    new ArrayList<>(orderedKeys),
                    rocksDBKeys);
        }
        if (nativeDistinctBatchDirectArenaEnabled
                && orderedKeys.size() < nativeDistinctBatchAsyncMinUniqueKeys) {
            deferredSyncBatchPrefetchBatches.incrementAndGet();
            deferredSyncBatchPrefetchKeys.addAndGet(orderedKeys.size());
            return new DeferredDirectMapValues(
                    currentKey, namespace, new ArrayList<>(orderedKeys), null);
        }
        flushCurrentKey(currentKey);
        ensureDelegateNamespace(currentKey);
        List<byte[]> rocksDBKeys =
                rocksDBBatchMapReader.serializeRocksDBKeysByUserKeys(orderedKeys);
        if (rocksDBKeys.size() != orderedKeys.size()) {
            return null;
        }
        NativeRequestPlaneCoordinator.BatchSlot directSlot = null;
        if (nativeDistinctBatchDirectArenaEnabled
                && nativeRequestPlaneCoordinator != null
                && nativeRequestPlaneCoordinator.isActive()
                && orderedKeys.size() <= RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH) {
            directSlot = nativeRequestPlaneCoordinator.tryAcquireMapDistinctAsyncReadSlot();
            if (directSlot == null) {
                asyncDirectArenaLeaseMisses.incrementAndGet();
            } else {
                try {
                    directSlot.prepareLatest(nativeStateId, nativeGeneration, rocksDBKeys);
                    directSlot.prepareContiguousDirectArenaMultiGet(
                            orderedKeys.size(), RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH);
                    asyncDirectArenaBatches.incrementAndGet();
                    asyncDirectArenaKeys.addAndGet(orderedKeys.size());
                } catch (Exception | LinkageError directPreparationFailure) {
                    directSlot.close();
                    directSlot = null;
                    asyncDirectArenaPreparationFallbacks.incrementAndGet();
                }
            }
        }
        PreparedMapValues prepared =
                directSlot == null
                        ? new PreparedMapValues(rocksDBKeys, orderedKeys.size())
                        : new PreparedMapValues(directSlot, orderedKeys.size());
        synchronized (asyncBatchPrefetchMonitor) {
            if (closed) {
                prepared.releaseDirectSlotOnce();
                return null;
            }
            outstandingAsyncBatchPrefetchTasks.add(prepared);
        }
        asyncBatchPrefetchSubmitted.incrementAndGet();
        asyncBatchPrefetchKeys.addAndGet(orderedKeys.size());
        if (nativeDistinctBatchWorkFirstEnabled && directSlot != null) {
            asyncBatchPrefetchWorkFirstEligible.incrementAndGet();
            long submitStartedNanos = System.nanoTime();
            PrefetchExecutor.WorkFirstSubmission submission =
                    PrefetchExecutor.trySubmitWorkFirst(prepared);
            switch (submission) {
                case CALLER_RUN:
                    asyncBatchPrefetchCallerRuns.incrementAndGet();
                    asyncBatchPrefetchCallerRunKeys.addAndGet(orderedKeys.size());
                    asyncBatchPrefetchCallerRunNanos.addAndGet(
                            Math.max(0L, System.nanoTime() - submitStartedNanos));
                    break;
                case QUEUED_WORKER_IDLE:
                    asyncBatchPrefetchWorkFirstWorkerIdle.incrementAndGet();
                    asyncBatchPrefetchQueuedSubmissions.incrementAndGet();
                    break;
                case QUEUED_BACKLOG_EMPTY:
                    asyncBatchPrefetchWorkFirstBacklogEmpty.incrementAndGet();
                    asyncBatchPrefetchQueuedSubmissions.incrementAndGet();
                    break;
                case QUEUED_PERMIT_BUSY:
                    asyncBatchPrefetchWorkFirstPermitBusy.incrementAndGet();
                    asyncBatchPrefetchQueuedSubmissions.incrementAndGet();
                    break;
                default:
                    throw new IllegalStateException("Unknown work-first submission " + submission);
            }
        } else {
            PrefetchExecutor.trySubmit(prepared);
            asyncBatchPrefetchQueuedSubmissions.incrementAndGet();
        }
        return prepared;
    }

    private final class PreparedMapValues
            implements PreparedValues, PrefetchExecutor.WorkFirstEligibleTask {

        private final List<byte[]> rocksDBKeys;
        private final NativeRequestPlaneCoordinator.BatchSlot directSlot;
        private final int expectedValues;
        private final java.util.concurrent.CountDownLatch completed =
                new java.util.concurrent.CountDownLatch(1);
        private final java.util.concurrent.atomic.AtomicInteger state =
                new java.util.concurrent.atomic.AtomicInteger();
        private volatile List<byte[]> rawValues;
        private volatile boolean directValuesReady;
        private volatile Throwable failure;
        private volatile boolean dropped;
        private volatile boolean cancelled;
        private volatile long completedNanos;
        private final long submittedNanos = System.nanoTime();
        private final java.util.concurrent.atomic.AtomicBoolean directSlotReleased =
                new java.util.concurrent.atomic.AtomicBoolean();

        private PreparedMapValues(List<byte[]> rocksDBKeys, int expectedValues) {
            this.rocksDBKeys = rocksDBKeys;
            this.directSlot = null;
            this.expectedValues = expectedValues;
        }

        private PreparedMapValues(
                NativeRequestPlaneCoordinator.BatchSlot directSlot, int expectedValues) {
            this.rocksDBKeys = null;
            this.directSlot = directSlot;
            this.expectedValues = expectedValues;
        }

        @Override
        public WaveParticipation waveParticipation() {
            return nativeDistinctBatchDeferredWaveEnabled
                    ? WaveParticipation.INELIGIBLE
                    : WaveParticipation.DISABLED;
        }

        @Override
        public void run() {
            if (!state.compareAndSet(0, 1)) {
                return;
            }
            long workerStartedNanos = System.nanoTime();
            asyncBatchPrefetchQueueNanos.addAndGet(
                    Math.max(0L, workerStartedNanos - submittedNanos));
            try {
                lifecycleLock.readLock().lock();
                try {
                    if (!closed) {
                        if (directSlot == null) {
                            List<byte[]> values =
                                    rocksDBBatchMapReader.getSerializedValuesByRocksDBKeys(
                                            rocksDBKeys, 0, rocksDBKeys.size());
                            if (values.size() != expectedValues) {
                                throw new IllegalStateException(
                                        "Async MapState MultiGet returned "
                                                + values.size()
                                                + " values for "
                                                + expectedValues
                                                + " keys.");
                            }
                            rawValues = values;
                        } else {
                            directValuesReady = executeDirectArenaRead();
                        }
                    }
                } finally {
                    lifecycleLock.readLock().unlock();
                }
            } catch (Throwable currentFailure) {
                failure = currentFailure;
                asyncBatchPrefetchFailures.incrementAndGet();
            } finally {
                asyncBatchPrefetchServiceNanos.addAndGet(
                        Math.max(0L, System.nanoTime() - workerStartedNanos));
                finish(false);
                if (cancelled) {
                    releaseDirectSlotOnce();
                }
            }
        }

        private boolean executeDirectArenaRead() throws Exception {
            int presentCount =
                    rocksDBBatchMapReader.getSerializedValuesByRocksDBKeyArena(
                            directSlot.directMultiGetKeyArena(),
                            directSlot.directMultiGetDescriptors(),
                            expectedValues,
                            directSlot.directMultiGetValueArena(),
                            directSlot.directMultiGetValueStride());
            int observedPresent = 0;
            int observedMissing = 0;
            boolean overflow = false;
            boolean invalidProtocol = presentCount < 0 || presentCount > expectedValues;
            for (int index = 0; index < expectedValues && !invalidProtocol; index++) {
                int result = directSlot.directMultiGetResult(index);
                if (result >= 0 && result <= directSlot.directMultiGetValueStride()) {
                    observedPresent++;
                } else if (result == RocksDBBatchValueReader.DIRECT_ARENA_NOT_FOUND) {
                    observedMissing++;
                } else if (result == RocksDBBatchValueReader.DIRECT_ARENA_OVERFLOW) {
                    observedPresent++;
                    overflow = true;
                } else {
                    invalidProtocol = true;
                }
            }
            if (observedPresent != presentCount) {
                invalidProtocol = true;
            }
            if (invalidProtocol) {
                asyncDirectArenaProtocolFailures.incrementAndGet();
                return false;
            }
            if (overflow) {
                asyncDirectArenaOverflows.incrementAndGet();
                return false;
            }
            asyncDirectArenaCompletedBatches.incrementAndGet();
            asyncDirectArenaCompletedKeys.addAndGet(expectedValues);
            asyncDirectArenaFound.addAndGet(observedPresent);
            asyncDirectArenaMissing.addAndGet(observedMissing);
            return true;
        }

        @Override
        public void onDrop() {
            if (state.compareAndSet(0, 1)) {
                dropped = true;
                asyncBatchPrefetchDropped.incrementAndGet();
                finish(true);
                releaseDirectSlotOnce();
            }
        }

        private void finish(boolean wasDropped) {
            completedNanos = System.nanoTime();
            state.set(2);
            if (!wasDropped && (rawValues != null || directValuesReady)) {
                asyncBatchPrefetchCompleted.incrementAndGet();
            }
            completed.countDown();
            synchronized (asyncBatchPrefetchMonitor) {
                outstandingAsyncBatchPrefetchTasks.remove(this);
                asyncBatchPrefetchMonitor.notifyAll();
            }
        }

        @Override
        public List<?> awaitValues() throws Exception {
            asyncBatchPrefetchAwaits.incrementAndGet();
            long awaitStarted = System.nanoTime();
            long observedCompletedNanos = completedNanos;
            if (observedCompletedNanos > 0L && observedCompletedNanos <= awaitStarted) {
                asyncBatchPrefetchReadyBeforeAwait.incrementAndGet();
                asyncBatchPrefetchReadySlackNanos.addAndGet(awaitStarted - observedCompletedNanos);
            }
            try {
                completed.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            } finally {
                asyncBatchPrefetchWaitNanos.addAndGet(
                        Math.max(0L, System.nanoTime() - awaitStarted));
            }
            try {
                if (dropped
                        || failure != null
                        || directSlot == null && rawValues == null
                        || directSlot != null && !directValuesReady) {
                    return null;
                }
                ArrayList<UV> values = new ArrayList<>(expectedValues);
                if (directSlot != null) {
                    for (int index = 0; index < expectedValues; index++) {
                        int result = directSlot.directMultiGetResult(index);
                        if (result == RocksDBBatchValueReader.DIRECT_ARENA_NOT_FOUND) {
                            values.add(null);
                        } else {
                            DataInputView input = directSlot.directMultiGetValueInput(index);
                            boolean isNull = input.readBoolean();
                            values.add(isNull ? null : userValueSerializer.deserialize(input));
                        }
                    }
                } else {
                    for (byte[] rawValue : rawValues) {
                        if (rawValue == null) {
                            values.add(null);
                        } else {
                            batchPrefetchInput.setBuffer(rawValue);
                            boolean isNull = batchPrefetchInput.readBoolean();
                            values.add(
                                    isNull
                                            ? null
                                            : userValueSerializer.deserialize(batchPrefetchInput));
                        }
                    }
                }
                return values;
            } finally {
                releaseDirectSlotOnce();
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
            PrefetchExecutor.cancelIfQueued(this);
            if (state.get() == 2) {
                releaseDirectSlotOnce();
            }
        }

        private void releaseDirectSlotOnce() {
            if (directSlot != null && directSlotReleased.compareAndSet(false, true)) {
                directSlot.close();
            }
        }
    }

    /**
     * Keeps one exact-key batch and optionally attaches it to a future-only asynchronous wave.
     *
     * <p>The generated DISTINCT view already copied and de-duplicated the key list. Wave submission
     * serializes immutable RocksDB keys on the mailbox, while the worker performs only the raw
     * batch read. Consumption validates the original outer-key context and deserializes on the
     * mailbox. Any drop, failure, incomplete vector, or context mismatch returns {@code null} so
     * the existing caller takes its authoritative state-read fallback.
     */
    private final class DeferredDirectMapValues implements PreparedValues {

        private final K preparedOuterKey;
        private final N preparedNamespace;
        private final List<UK> orderedKeys;
        private final List<byte[]> preparedRocksDBKeys;
        private DeferredWaveTask waveTask;
        private int waveOffset;
        private boolean consumed;
        private boolean waveTokenReleased;

        private DeferredDirectMapValues(
                K preparedOuterKey,
                N preparedNamespace,
                List<UK> orderedKeys,
                List<byte[]> preparedRocksDBKeys) {
            this.preparedOuterKey = preparedOuterKey;
            this.preparedNamespace = preparedNamespace;
            this.orderedKeys = orderedKeys;
            this.preparedRocksDBKeys = preparedRocksDBKeys;
        }

        @Override
        public Object waveOwner() {
            return nativeDistinctBatchDeferredWaveEnabled && preparedRocksDBKeys != null
                    ? CachedInternalMapState.this
                    : null;
        }

        @Override
        public WaveParticipation waveParticipation() {
            if (!nativeDistinctBatchDeferredWaveEnabled) {
                return WaveParticipation.DISABLED;
            }
            return preparedRocksDBKeys == null
                    ? WaveParticipation.INELIGIBLE
                    : WaveParticipation.ELIGIBLE;
        }

        @Override
        public boolean executeWave(List<? extends PreparedValues> tokens) throws Exception {
            deferredWaveAttempts.incrementAndGet();
            return submitDeferredWave(tokens);
        }

        private boolean submitDeferredWave(List<? extends PreparedValues> tokens) {
            if (!nativeDistinctBatchDeferredWaveEnabled
                    || closed
                    || tokens == null
                    || tokens.size() < 2
                    || rocksDBBatchMapReader == null) {
                deferredWaveFallbackWindows.incrementAndGet();
                return false;
            }
            ArrayList<DeferredDirectMapValues> waveTokens = new ArrayList<>(tokens.size());
            ArrayList<byte[]> rocksDBKeys = new ArrayList<>();
            int totalKeys = 0;
            for (PreparedValues token : tokens) {
                if (token == null || token.waveOwner() != CachedInternalMapState.this) {
                    deferredWaveOwnerRejects.incrementAndGet();
                    deferredWaveFallbackWindows.incrementAndGet();
                    return false;
                }
                @SuppressWarnings("unchecked")
                DeferredDirectMapValues current = (DeferredDirectMapValues) token;
                if (current.consumed
                        || current.waveTask != null
                        || current.preparedRocksDBKeys == null
                        || current.preparedRocksDBKeys.size() != current.orderedKeys.size()) {
                    deferredWaveOwnerRejects.incrementAndGet();
                    deferredWaveFallbackWindows.incrementAndGet();
                    return false;
                }
                for (DeferredDirectMapValues prior : waveTokens) {
                    if (Objects.equals(prior.preparedOuterKey, current.preparedOuterKey)
                            && Objects.equals(prior.preparedNamespace, current.preparedNamespace)) {
                        deferredWaveOwnerRejects.incrementAndGet();
                deferredWaveFallbackWindows.incrementAndGet();
                return false;
            }
            }
                totalKeys += current.preparedRocksDBKeys.size();
                waveTokens.add(current);
                    rocksDBKeys.addAll(current.preparedRocksDBKeys);
                }
            if (totalKeys <= 0 || totalKeys > RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH) {
                    deferredWaveOverflows.incrementAndGet();
                    deferredWaveFallbackWindows.incrementAndGet();
                    return false;
                }
            DeferredWaveTask task =
                    new DeferredWaveTask(
                            Collections.unmodifiableList(rocksDBKeys),
                            totalKeys,
                            waveTokens.size());
                int offset = 0;
                for (DeferredDirectMapValues current : waveTokens) {
                current.waveTask = task;
                current.waveOffset = offset;
                offset += current.orderedKeys.size();
                        }
            synchronized (asyncBatchPrefetchMonitor) {
                if (closed) {
                    for (DeferredDirectMapValues current : waveTokens) {
                        current.waveTask = null;
                        current.waveOffset = 0;
                    }
                    deferredWaveFallbackWindows.incrementAndGet();
                    return false;
                }
                outstandingAsyncBatchPrefetchTasks.add(task);
                }
            deferredWaveSubmitted.incrementAndGet();
                deferredWaveGroups.addAndGet(waveTokens.size());
                deferredWaveKeys.addAndGet(totalKeys);
            PrefetchExecutor.trySubmit(task);
                return true;
        }

        @Override
        public List<?> awaitValues() throws Exception {
            if (consumed) {
                return null;
            }
            consumed = true;
            if (closed
                    || !Objects.equals(preparedOuterKey, currentKeyProvider.getCurrentKey())
                    || !Objects.equals(preparedNamespace, currentNamespace)) {
                deferredSyncBatchPrefetchFallbacks.incrementAndGet();
                deferredSyncBatchPrefetchContextMismatches.incrementAndGet();
                if (waveTask != null) {
                    releaseWaveToken(waveTask);
                }
                return null;
            }
            if (waveTask != null) {
                DeferredWaveTask task = waveTask;
                try {
                    List<byte[]> rawValues = task.awaitRawValues();
                    if (rawValues == null
                            || waveOffset < 0
                            || waveOffset + orderedKeys.size() > rawValues.size()) {
                    deferredSyncBatchPrefetchFallbacks.incrementAndGet();
                    return null;
                }
                    ArrayList<UV> values = new ArrayList<>(orderedKeys.size());
                    int waveEnd = waveOffset + orderedKeys.size();
                    for (int index = waveOffset; index < waveEnd; index++) {
                        byte[] rawValue = rawValues.get(index);
                        if (rawValue == null) {
                            values.add(null);
                        } else {
                            batchPrefetchInput.setBuffer(rawValue);
                            boolean isNull = batchPrefetchInput.readBoolean();
                            values.add(
                                    isNull
                                            ? null
                                            : userValueSerializer.deserialize(batchPrefetchInput));
                        }
                    }
                deferredSyncBatchPrefetchCompleted.incrementAndGet();
                deferredWaveCompletedGroups.incrementAndGet();
                return values;
                } catch (Exception | LinkageError failure) {
                    deferredWaveProtocolFailures.incrementAndGet();
                    deferredSyncBatchPrefetchFallbacks.incrementAndGet();
                    return null;
                } finally {
                    releaseWaveToken(task);
                }
            }
            List<UV> values = prefetchCurrentUniqueKeyValues(orderedKeys);
            if (values == null || values.size() != orderedKeys.size()) {
                deferredSyncBatchPrefetchFallbacks.incrementAndGet();
                return null;
            }
            deferredSyncBatchPrefetchCompleted.incrementAndGet();
            return values;
        }

        @Override
        public void cancel() {
            if (!consumed && waveTask != null) {
                deferredWaveCancelledGroups.incrementAndGet();
            }
            consumed = true;
            DeferredWaveTask task = waveTask;
            if (task != null) {
                releaseWaveToken(task);
            }
        }

        private void releaseWaveToken(DeferredWaveTask task) {
            if (!waveTokenReleased) {
                waveTokenReleased = true;
                task.releaseToken();
            }
        }
    }

    /** One future-only raw RocksDB read. Worker code never touches key context or serializers. */
    private final class DeferredWaveTask implements PrefetchExecutor.DropAwareTask {

        private final List<byte[]> rocksDBKeys;
        private final int expectedValues;
        private final java.util.concurrent.CountDownLatch completed =
                new java.util.concurrent.CountDownLatch(1);
        private final java.util.concurrent.atomic.AtomicInteger state =
                new java.util.concurrent.atomic.AtomicInteger();
        private final java.util.concurrent.atomic.AtomicInteger remainingTokens;
        private final long submittedNanos = System.nanoTime();
        private volatile List<byte[]> rawValues;
        private volatile Throwable failure;
        private volatile boolean dropped;
        private volatile long completedNanos;

        private DeferredWaveTask(List<byte[]> rocksDBKeys, int expectedValues, int tokenCount) {
            this.rocksDBKeys = rocksDBKeys;
            this.expectedValues = expectedValues;
            this.remainingTokens = new java.util.concurrent.atomic.AtomicInteger(tokenCount);
        }

        @Override
        public void run() {
            if (!state.compareAndSet(0, 1)) {
                return;
            }
            long startedNanos = System.nanoTime();
            deferredWaveQueueNanos.addAndGet(Math.max(0L, startedNanos - submittedNanos));
            try {
                lifecycleLock.readLock().lock();
                try {
                    if (closed) {
                        throw new IllegalStateException("MapState closed before deferred wave");
                    }
                    deferredWaveJniCalls.incrementAndGet();
                    List<byte[]> completedValues =
                            rocksDBBatchMapReader.getSerializedValuesByRocksDBKeys(
                                    rocksDBKeys, 0, expectedValues);
                    if (completedValues == null || completedValues.size() != expectedValues) {
                        deferredWaveProtocolFailures.incrementAndGet();
                        throw new IllegalStateException("Incomplete deferred wave result");
                    }
                    rawValues = completedValues;
                } finally {
                    lifecycleLock.readLock().unlock();
                }
                int present = 0;
                int missing = 0;
                for (byte[] rawValue : rawValues) {
                    if (rawValue == null) {
                        missing++;
                    } else {
                        present++;
                    }
                }
                deferredWaveFound.addAndGet(present);
                deferredWaveMissing.addAndGet(missing);
                deferredWaveWindows.incrementAndGet();
            } catch (Exception | LinkageError currentFailure) {
                failure = currentFailure;
                deferredWaveFailures.incrementAndGet();
                deferredWaveFallbackWindows.incrementAndGet();
            } finally {
                deferredWaveServiceNanos.addAndGet(Math.max(0L, System.nanoTime() - startedNanos));
                finish();
            }
        }

        @Override
        public void onDrop() {
            if (state.compareAndSet(0, 1)) {
                dropped = true;
                deferredWaveDropped.incrementAndGet();
                deferredWaveFallbackWindows.incrementAndGet();
                finish();
            }
        }

        private void finish() {
            completedNanos = System.nanoTime();
            state.set(2);
            if (remainingTokens.get() == 0) {
                rawValues = null;
            }
            completed.countDown();
            synchronized (asyncBatchPrefetchMonitor) {
                outstandingAsyncBatchPrefetchTasks.remove(this);
                asyncBatchPrefetchMonitor.notifyAll();
            }
        }

        private List<byte[]> awaitRawValues() throws InterruptedException {
            deferredWaveAwaits.incrementAndGet();
            long awaitStarted = System.nanoTime();
            long observedCompleted = completedNanos;
            if (observedCompleted > 0L && observedCompleted <= awaitStarted) {
                deferredWaveReadyBeforeAwait.incrementAndGet();
                deferredWaveReadySlackNanos.addAndGet(awaitStarted - observedCompleted);
            }
            try {
                completed.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            } finally {
                deferredWaveWaitNanos.addAndGet(Math.max(0L, System.nanoTime() - awaitStarted));
            }
            List<byte[]> completedValues = rawValues;
            if (dropped
                    || failure != null
                    || completedValues == null
                    || completedValues.size() != expectedValues) {
                return null;
            }
            return completedValues;
        }

        private void releaseToken() {
            int remaining = remainingTokens.decrementAndGet();
            if (remaining < 0) {
                throw new IllegalStateException("Deferred wave token released more than once");
            }
            if (remaining == 0) {
                if (!PrefetchExecutor.cancelIfQueued(this) && state.get() == 2) {
                    rawValues = null;
                }
            }
        }
    }

    private List<UV> tryDirectArenaPrefetch(List<UK> orderedKeys) {
        batchPrefetchDirectArenaAttempts++;
        if (nativeDistinctBatchDirectArenaDisabled
                || nativeRequestPlaneCoordinator == null
                || !nativeRequestPlaneCoordinator.isActive()
                || !nativeRequestPlaneCoordinator.options().directArenaMultiGetEnabled()
                || !rocksDBBatchMapReader.supportsDirectArenaMultiGet()
                || nativeStateId <= 0) {
            return null;
        }

        NativeRequestPlaneCoordinator.BatchSlot slot =
                nativeRequestPlaneCoordinator.tryAcquireMapDistinctReadSlot();
        if (slot == null) {
            return null;
        }
        try (NativeRequestPlaneCoordinator.BatchSlot ignored = slot) {
            List<byte[]> rocksDBKeys =
                    rocksDBBatchMapReader.serializeRocksDBKeysByUserKeys(orderedKeys);
            if (rocksDBKeys.size() != orderedKeys.size()) {
                throw new IllegalStateException(
                        "MapState prepared "
                                + rocksDBKeys.size()
                                + " RocksDB keys for "
                                + orderedKeys.size()
                                + " user keys.");
            }
            slot.prepareLatest(nativeStateId, nativeGeneration, rocksDBKeys);
            int[] preparedIndices = new int[rocksDBKeys.size()];
            for (int index = 0; index < preparedIndices.length; index++) {
                preparedIndices[index] = index;
            }

            int configuredChunk = nativeRequestPlaneCoordinator.options().directArenaBatchSize();
            int chunkSize =
                    Math.max(
                            1,
                            Math.min(
                                    RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH,
                                    Math.min(
                                            configuredChunk,
                                            rocksDBBatchMapReader.directArenaMultiGetMaxBatch())));
            ArrayList<UV> values = new ArrayList<>(orderedKeys.size());
            int totalFound = 0;
            int totalMissing = 0;
            for (int start = 0; start < orderedKeys.size(); start += chunkSize) {
                int count = Math.min(chunkSize, orderedKeys.size() - start);
                slot.prepareDirectArenaMultiGet(
                        preparedIndices,
                        start,
                        count,
                        RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH);
                batchPrefetchDirectArenaBatches++;
                batchPrefetchDirectArenaKeys += count;
                int presentCount =
                        rocksDBBatchMapReader.getSerializedValuesByRocksDBKeyArena(
                                slot.directMultiGetKeyArena(),
                                slot.directMultiGetDescriptors(),
                                count,
                                slot.directMultiGetValueArena(),
                                slot.directMultiGetValueStride());
                int observedPresent = 0;
                int observedMissing = 0;
                boolean overflow = false;
                boolean invalidProtocol = presentCount < 0 || presentCount > count;
                for (int index = 0; index < count && !invalidProtocol; index++) {
                    int result = slot.directMultiGetResult(index);
                    if (result >= 0 && result <= slot.directMultiGetValueStride()) {
                        observedPresent++;
                    } else if (result == RocksDBBatchValueReader.DIRECT_ARENA_NOT_FOUND) {
                        observedMissing++;
                    } else if (result == RocksDBBatchValueReader.DIRECT_ARENA_OVERFLOW) {
                        observedPresent++;
                        overflow = true;
                    } else {
                        invalidProtocol = true;
                    }
                }
                if (observedPresent != presentCount) {
                    invalidProtocol = true;
                }
                if (invalidProtocol) {
                    batchPrefetchDirectArenaProtocolFallbacks++;
                    nativeDistinctBatchDirectArenaDisabled = true;
                    return null;
                }
                if (overflow) {
                    batchPrefetchDirectArenaOverflows++;
                    return null;
                }
                for (int index = 0; index < count; index++) {
                    int result = slot.directMultiGetResult(index);
                    if (result == RocksDBBatchValueReader.DIRECT_ARENA_NOT_FOUND) {
                        values.add(null);
                    } else {
                        DataInputView input = slot.directMultiGetValueInput(index);
                        boolean isNull = input.readBoolean();
                        values.add(isNull ? null : userValueSerializer.deserialize(input));
                    }
                }
                batchPrefetchDirectArenaCompletedBatches++;
                batchPrefetchDirectArenaCompletedKeys += count;
                batchPrefetchDirectArenaFound += observedPresent;
                batchPrefetchDirectArenaMissing += observedMissing;
                totalFound += observedPresent;
                totalMissing += observedMissing;
            }
            batchPrefetchFound += totalFound;
            batchPrefetchMissing += totalMissing;
            return values;
        } catch (LinkageError
                | UnsupportedOperationException
                | IllegalArgumentException invalidDirectArenaAbi) {
            batchPrefetchDirectArenaFailures++;
            nativeDistinctBatchDirectArenaDisabled = true;
            return null;
        } catch (Exception failure) {
            batchPrefetchDirectArenaFailures++;
            return null;
        }
    }

    @Override
    public boolean beginPrefetchCurrentKeys(Iterable<? extends UK> userKeys) throws Exception {
        endPrefetchCurrentKeys();
        batchPrefetchAttempts++;
        if (!nativeDistinctBatchPrefetchEnabled || rocksDBBatchMapReader == null) {
            batchPrefetchFallbacks++;
            return false;
        }
        K currentKey = currentKeyProvider.getCurrentKey();
        N namespace = currentNamespace;
        if (currentKey == null || namespace == null) {
            batchPrefetchFallbacks++;
            return false;
        }

        LinkedHashSet<UK> unique = new LinkedHashSet<>();
        for (UK userKey : userKeys) {
            if (userKey != null) {
                batchPrefetchInputKeys++;
                unique.add(userKey);
            }
        }
        if (unique.size() < 2) {
            batchPrefetchFallbacks++;
            return false;
        }

        List<UK> orderedKeys = new ArrayList<>(unique);
        batchPrefetchUniqueKeys += orderedKeys.size();
        try {
            // A write-back MapState cache can otherwise make RocksDB older than the logical state.
            flushCurrentKey(currentKey);
            ensureDelegateNamespace(currentKey);
            List<byte[]> rawValues =
                    rocksDBBatchMapReader.getSerializedValuesByUserKeys(orderedKeys);
            if (rawValues.size() != orderedKeys.size()) {
                throw new IllegalStateException(
                        "MapState MultiGet returned "
                                + rawValues.size()
                                + " values for "
                                + orderedKeys.size()
                                + " keys.");
            }
            for (int i = 0; i < orderedKeys.size(); i++) {
                byte[] rawValue = rawValues.get(i);
                PrefetchedMapValue<UV> staged;
                if (rawValue == null) {
                    staged = PrefetchedMapValue.missing();
                    batchPrefetchMissing++;
                } else {
                    batchPrefetchInput.setBuffer(rawValue);
                    boolean isNull = batchPrefetchInput.readBoolean();
                    UV value = isNull ? null : userValueSerializer.deserialize(batchPrefetchInput);
                    staged = PrefetchedMapValue.present(value);
                    batchPrefetchFound++;
                }
                batchPrefetchStaging.put(orderedKeys.get(i), staged);
            }
            batchPrefetchOuterKey = currentKey;
            batchPrefetchNamespace = namespace;
            batchPrefetchActive = true;
            batchPrefetchBatches++;
            return true;
        } catch (Exception | LinkageError failure) {
            batchPrefetchFailures++;
            batchPrefetchFallbacks++;
            endPrefetchCurrentKeys();
            return false;
        }
    }

    @Override
    public void endPrefetchCurrentKeys() {
        batchPrefetchActive = false;
        batchPrefetchOuterKey = null;
        batchPrefetchNamespace = null;
        batchPrefetchStaging.clear();
    }

    private PrefetchedMapValue<UV> getBatchPrefetchedValue(K currentKey, UK userKey) {
        if (!batchPrefetchActive
                || !Objects.equals(batchPrefetchOuterKey, currentKey)
                || !Objects.equals(batchPrefetchNamespace, currentNamespace)) {
            if (batchPrefetchActive) {
                endPrefetchCurrentKeys();
            }
            return null;
        }
        PrefetchedMapValue<UV> value = batchPrefetchStaging.get(userKey);
        if (value != null) {
            batchPrefetchHits++;
        }
        return value;
    }

    @Override
    public UV get(UK userKey) throws Exception {
        if (userKey == null) {
            return null;
        }
        K currentKey = currentKeyProvider.getCurrentKey();
        ensureDelegateNamespace(
                currentKey); // Pass key to avoid re-fetch if needed (though ensureDelegateNamespace
                                             // uses currentNamespace)

        // Use reusable key for lookups
        setLookupKey(currentKey, currentNamespace, userKey);

        PrefetchedMapValue<UV> prefetched = getBatchPrefetchedValue(currentKey, userKey);
        if (prefetched != null) {
            return prefetched.value;
        }

        if (bypassEnabled && isBypassing && shouldBypassRead()) {
            NativeMapRead<UV> nativeRead = readThroughNative(currentKey, userKey);
            UV value = nativeRead == null ? delegate.get(userKey) : nativeRead.value;
            if (mapCacheEnabled) {
                updateValueCache(currentKey, userKey, value, false);
            }
            if (presenceCacheEnabled) {
                updatePresence(currentKey, userKey, value != null);
            }
            return value;
        }

        if (mapCacheEnabled) {
            CachedMapValue<UV> cached =
                    getCachedValue(currentKey); // Optimize getCachedValue to use lookupKey
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
        NativeMapRead<UV> nativeRead = readThroughNative(currentKey, userKey);
        UV value = nativeRead == null ? delegate.get(userKey) : nativeRead.value;
        recordAccess(nativeRead != null && nativeRead.cacheHit);
        if (mapCacheEnabled) {
            updateValueCache(currentKey, userKey, value, false);
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
        advanceNativeGeneration();
        updateBatchPrefetchStaging(currentKey, userKey, true, userValue);
        boolean writeThrough = !mapCacheEnabled || (bypassEnabled && isBypassing);
        if (writeThrough) {
            delegate.put(userKey, userValue);
        }

        if (mapCacheEnabled) {
            updateValueCache(currentKey, userKey, userValue, !writeThrough);
        }
        if (presenceCacheEnabled) {
            updatePresence(currentKey, userKey, true);
        }
        invalidateSnapshot(currentKey);
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        if (map == null || map.isEmpty()) {
            return;
        }
        K currentKey = currentKeyProvider.getCurrentKey();
        ensureDelegateNamespace(currentKey);
        advanceNativeGeneration();

        boolean writeThrough = !mapCacheEnabled || (bypassEnabled && isBypassing);
        if (writeThrough) {
            delegate.putAll(map);
        }
        for (Map.Entry<UK, UV> entry : map.entrySet()) {
            UK uKey = entry.getKey();
            if (uKey == null) {
                continue;
            }
            updateBatchPrefetchStaging(currentKey, uKey, true, entry.getValue());
            if (mapCacheEnabled) {
                updateValueCache(currentKey, uKey, entry.getValue(), !writeThrough);
            }
            if (presenceCacheEnabled) {
                updatePresence(currentKey, uKey, entry.getValue() != null);
            }
        }
        invalidateSnapshot(currentKey);
    }

    @Override
    public void remove(UK userKey) throws Exception {
        if (userKey == null) {
            return;
        }
        K currentKey = currentKeyProvider.getCurrentKey();
        ensureDelegateNamespace(currentKey);
        advanceNativeGeneration();
        updateBatchPrefetchStaging(currentKey, userKey, false, null);

        boolean writeThrough = !mapCacheEnabled || (bypassEnabled && isBypassing);
        if (writeThrough) {
            delegate.remove(userKey);
        }
        if (mapCacheEnabled) {
            updateValueCache(currentKey, userKey, null, !writeThrough);
        }
        if (presenceCacheEnabled) {
            updatePresence(currentKey, userKey, false);
        }
        invalidateSnapshot(currentKey);
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

        PrefetchedMapValue<UV> prefetched = getBatchPrefetchedValue(currentKey, userKey);
        if (prefetched != null) {
            return prefetched.present;
        }

        if (bypassEnabled && isBypassing && shouldBypassRead()) {
            NativeMapRead<UV> nativeRead = readThroughNative(currentKey, userKey);
            boolean exists =
                    nativeRead == null ? delegate.contains(userKey) : nativeRead.value != null;
            if (mapCacheEnabled && !exists) {
                updateValueCache(currentKey, userKey, null, false);
            }
            if (presenceCacheEnabled) {
                updatePresence(currentKey, userKey, exists);
            }
            return exists;
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
        NativeMapRead<UV> nativeRead = readThroughNative(currentKey, userKey);
        boolean exists = nativeRead == null ? delegate.contains(userKey) : nativeRead.value != null;
        recordAccess(nativeRead != null && nativeRead.cacheHit);
        if (mapCacheEnabled && !exists) {
            updateValueCache(currentKey, userKey, null, false);
        }
        if (presenceCacheEnabled) {
            updatePresence(currentKey, userKey, exists);
        }
        return exists;
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        K currentKey = currentKeyProvider.getCurrentKey();
        ensureDelegateNamespace(currentKey);

        // --- MapSnapshot short-circuit ---
        if (snapshotOptimizationEnabled()) {
            Iterable<Map.Entry<UK, UV>> shortCircuit = trySnapshotShortCircuit(currentKey);
            if (shortCircuit != null) {
                return shortCircuit;
            }
        }

        flushCurrentKey(currentKey);
        Iterable<Map.Entry<UK, UV>> entries = delegate.entries();
        if (!mapCacheEnabled && !presenceCacheEnabled && !snapshotOptimizationEnabled()) {
            return entries;
        }
        return wrapWithSnapshotAwareIterator(
                entries, currentKey, currentNamespace, iterationCacheFillEnabled);
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        K currentKey = currentKeyProvider.getCurrentKey();
        ensureDelegateNamespace(currentKey);

        if (snapshotOptimizationEnabled()) {
            Iterable<Map.Entry<UK, UV>> shortCircuit = trySnapshotShortCircuit(currentKey);
            if (shortCircuit != null) {
                return entryKeys(shortCircuit);
            }
        }

        flushCurrentKey(currentKey);
        if (!mapCacheEnabled && !presenceCacheEnabled) {
            return delegate.keys();
        }
        if (!iterationCacheFillEnabled) {
            return delegate.keys();
        }
        Iterable<Map.Entry<UK, UV>> entries = delegate.entries();
        return cacheKeys(entries, currentKey, currentNamespace);
    }

    @Override
    public Iterable<UV> values() throws Exception {
        K currentKey = currentKeyProvider.getCurrentKey();
        ensureDelegateNamespace(currentKey);

        if (snapshotOptimizationEnabled()) {
            Iterable<Map.Entry<UK, UV>> shortCircuit = trySnapshotShortCircuit(currentKey);
            if (shortCircuit != null) {
                return entryValues(shortCircuit);
            }
        }

        flushCurrentKey(currentKey);
        if (!mapCacheEnabled && !presenceCacheEnabled) {
            return delegate.values();
        }
        if (!iterationCacheFillEnabled) {
            return delegate.values();
        }
        Iterable<Map.Entry<UK, UV>> entries = delegate.entries();
        return cacheValues(entries, currentKey, currentNamespace);
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        K currentKey = currentKeyProvider.getCurrentKey();
        ensureDelegateNamespace(currentKey);

        if (snapshotOptimizationEnabled()) {
            Iterable<Map.Entry<UK, UV>> shortCircuit = trySnapshotShortCircuit(currentKey);
            if (shortCircuit != null) {
                return shortCircuit.iterator();
            }
        }

        flushCurrentKey(currentKey);
        Iterator<Map.Entry<UK, UV>> iterator = delegate.iterator();
        if (!mapCacheEnabled && !presenceCacheEnabled && !snapshotOptimizationEnabled()) {
            return iterator;
        }
        return new SnapshotAwareIterator(
                iterator, currentKey, currentNamespace, iterationCacheFillEnabled);
    }

    @Override
    public boolean isEmpty() throws Exception {
        K currentKey = currentKeyProvider.getCurrentKey();
        ensureDelegateNamespace(currentKey);

        if (snapshotOptimizationEnabled()) {
            MapSnapshot<UK> snapshot = lookupSnapshot(currentKey);
            if (snapshot != null) {
                if (snapshot.isEmpty()) {
                    mapSnapshotCacheMetrics.recordEmptyShortCircuit();
                    return true;
                } else {
                    if (snapshot.isSingle()) {
                        mapSnapshotCacheMetrics.recordSingleShortCircuit();
                    } else {
                        mapSnapshotCacheMetrics.recordSmallShortCircuit();
                    }
                    return false;
                }
            }
        }

        flushCurrentKey(currentKey);
        return delegate.isEmpty();
    }

    @Override
    public void clear() {
        K currentKey = currentKeyProvider.getCurrentKey();
        ensureDelegateNamespace(currentKey);
        delegate.clear();
        endPrefetchCurrentKeys();
        advanceNativeGeneration();
        clearPresenceCaches();
        clearValueCaches();
        resetBypassState();

        if (snapshotOptimizationEnabled()) {
            if (currentKey != null && currentNamespace != null) {
                KeyNamespace<K, N> stored = newStoredKeyNamespace(currentKey, currentNamespace);
                storeSnapshot(stored, MapSnapshot.empty());
            }
        }
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
        if (!Objects.equals(this.currentNamespace, namespace)) {
            endPrefetchCurrentKeys();
        }
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
        flush();
        return delegate.getSerializedValue(
                serializedKeyAndNamespace,
                safeKeySerializer,
                safeNamespaceSerializer,
                safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, Map<UK, UV>> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        ensureDelegateNamespace(null);
        flush();
        return delegate.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    private void ensureDelegateNamespace(K currentKey) {
        if (currentNamespace != null) {
            delegate.setCurrentNamespace(currentNamespace);
        }
    }

    private void updateBatchPrefetchStaging(K currentKey, UK userKey, boolean present, UV value) {
        if (!batchPrefetchActive
                || !Objects.equals(batchPrefetchOuterKey, currentKey)
                || !Objects.equals(batchPrefetchNamespace, currentNamespace)) {
            return;
        }
        batchPrefetchStaging.put(
                userKey,
                present ? PrefetchedMapValue.present(value) : PrefetchedMapValue.missing());
    }

    private NativeMapRead<UV> readThroughNative(K currentKey, UK userKey) throws Exception {
        if (!nativeMapCacheEnabled
                || currentKey == null
                || currentNamespace == null
                || !nativeRequestPlaneCoordinator.isActive()) {
            return null;
        }
        NativeRequestPlaneCoordinator.BatchSlot slot =
                nativeRequestPlaneCoordinator.tryAcquireBatchSlot();
        if (slot == null) {
            nativeFallbacks++;
            return null;
        }
        try (NativeRequestPlaneCoordinator.BatchSlot ignored = slot) {
            slot.prepareLatest(
                    nativeStateId,
                    nativeGeneration,
                    output -> writeNativeMapKey(currentKey, currentNamespace, userKey, output));
            nativeProbeAttempts++;
            int processed = nativeRequestPlaneCoordinator.probe(slot);
            if (processed != 1 || slot.probeError(0) != NativeRequestPlaneBridge.ERROR_OK) {
                throw new IllegalStateException(
                        "Native MapState probe returned an invalid result.");
            }
            int status = slot.probeStatus(0);
            if (status == NativeRequestPlaneBridge.PROBE_HIT) {
                nativeHits++;
                return new NativeMapRead<>(
                        userValueSerializer.deserialize(slot.probeValueInput(0)), true);
            }
            if (status == NativeRequestPlaneBridge.PROBE_NEGATIVE) {
                nativeNegativeHits++;
                return new NativeMapRead<>(null, true);
            }
            if (status != NativeRequestPlaneBridge.PROBE_MISS) {
                throw new IllegalStateException(
                        "Native MapState probe returned status=" + status + ".");
            }
            nativeMisses++;
        } catch (Exception | LinkageError failure) {
            nativeFailures++;
            nativeFallbacks++;
            nativeRequestPlaneCoordinator.disable(failure);
            return null;
        }

        UV value = delegate.get(userKey);
        try {
            nativeRequestPlaneCoordinator.updateExactKey(
                    nativeStateId,
                    nativeGeneration,
                    output -> writeNativeMapKey(currentKey, currentNamespace, userKey, output),
                    value == null ? null : output -> userValueSerializer.serialize(value, output));
            nativeFills++;
        } catch (Exception | LinkageError fillFailure) {
            nativeFailures++;
            // The delegate result is authoritative. Native fill failure must not re-read or alter
            // the MapState result; the coordinator already fails closed where appropriate.
        }
        return new NativeMapRead<>(value, false);
    }

    private void writeNativeMapKey(K key, N namespace, UK userKey, DataOutputView destination)
            throws IOException {
        writeLengthPrefixed(keySerializer, key, destination);
        writeLengthPrefixed(namespaceSerializer, namespace, destination);
        writeLengthPrefixed(userKeySerializer, userKey, destination);
    }

    private <T> void writeLengthPrefixed(
            TypeSerializer<T> serializer, T value, DataOutputView destination) throws IOException {
        nativeComponentOutput.clear();
        serializer.serialize(value, nativeComponentOutput);
        int length = nativeComponentOutput.length();
        destination.writeInt(length);
        destination.write(nativeComponentOutput.getSharedBuffer(), 0, length);
    }

    private boolean shouldProbeNativeSnapshot() {
        if (!nativeSnapshotAdaptiveBypassEnabled) {
            return true;
        }
        if (nativeSnapshotAdaptiveMode == NativeSnapshotAdaptiveMode.BYPASS) {
            nativeSnapshotAdaptiveBypassClock++;
            if (nativeSnapshotAdaptiveBypassClock < nativeSnapshotAdaptiveResampleIntervalProbes) {
                nativeSnapshotAdaptiveBypassedProbes++;
                return false;
            }
            nativeSnapshotAdaptiveMode = NativeSnapshotAdaptiveMode.EVALUATE;
            nativeSnapshotAdaptiveTrialTransitions++;
            resetNativeSnapshotAdaptiveWindow();
        }
        if (nativeSnapshotAdaptiveWindowProbeCount >= nativeSnapshotAdaptiveWindowProbes) {
            nativeSnapshotAdaptiveEvaluatedWindows++;
            long usefulHits =
                    nativeSnapshotAdaptiveWindowPositiveHits
                            + nativeSnapshotAdaptiveWindowNegativeHits;
            double usefulHitRate = usefulHits / (double) nativeSnapshotAdaptiveWindowProbeCount;
            if (usefulHitRate < nativeSnapshotAdaptiveMinUsefulHitRate) {
                nativeSnapshotAdaptiveMode = NativeSnapshotAdaptiveMode.BYPASS;
                nativeSnapshotAdaptiveBypassTransitions++;
                nativeSnapshotAdaptiveBypassClock = 0;
                nativeSnapshotAdaptiveBypassedProbes++;
                return false;
            }
            resetNativeSnapshotAdaptiveWindow();
        }
        return true;
    }

    private void recordNativeSnapshotAdaptiveProbe(int status) {
        if (!nativeSnapshotAdaptiveBypassEnabled) {
            return;
        }
        nativeSnapshotAdaptiveWindowProbeCount++;
        if (status == NativeRequestPlaneBridge.PROBE_NEGATIVE) {
            nativeSnapshotAdaptiveWindowNegativeHits++;
        } else if (status == NativeRequestPlaneBridge.PROBE_HIT) {
            nativeSnapshotAdaptiveWindowPositiveHits++;
        } else if (status == NativeRequestPlaneBridge.PROBE_MISS) {
            nativeSnapshotAdaptiveWindowMisses++;
        }
    }

    private boolean shouldStoreNativeSnapshot() {
        if (!nativeSnapshotAdaptiveBypassEnabled) {
            return true;
        }
        if (nativeSnapshotAdaptiveMode == NativeSnapshotAdaptiveMode.BYPASS) {
            nativeSnapshotAdaptiveBypassedFills++;
            return false;
        }
        nativeSnapshotAdaptiveWindowFillAttempts++;
        return true;
    }

    private void resetNativeSnapshotAdaptiveWindow() {
        nativeSnapshotAdaptiveWindowProbeCount = 0;
        nativeSnapshotAdaptiveWindowPositiveHits = 0;
        nativeSnapshotAdaptiveWindowNegativeHits = 0;
        nativeSnapshotAdaptiveWindowMisses = 0;
        nativeSnapshotAdaptiveWindowFillAttempts = 0;
        nativeSnapshotAdaptiveBypassClock = 0;
    }

    private MapSnapshot<UK> lookupNativeSnapshot(K currentKey) {
        if (!nativeMapSnapshotEnabled
                || currentKey == null
                || currentNamespace == null
                || !nativeRequestPlaneCoordinator.isActive()) {
            return null;
        }
        if (!shouldProbeNativeSnapshot()) {
            return null;
        }
        try {
            NativeRequestPlaneCoordinator.BatchSlot slot =
                    nativeRequestPlaneCoordinator.tryAcquireBatchSlot();
            if (slot == null) {
                nativeSnapshotFallbacks++;
                return null;
            }
            try (NativeRequestPlaneCoordinator.BatchSlot ignored = slot) {
                slot.prepareExact(
                        nativeSnapshotStateId,
                        nativeGeneration,
                        output -> {
                            writeLengthPrefixed(keySerializer, currentKey, output);
                            writeLengthPrefixed(namespaceSerializer, currentNamespace, output);
                        });
                nativeSnapshotProbes++;
                int processed = nativeRequestPlaneCoordinator.probe(slot);
                if (processed != 1 || slot.probeError(0) != NativeRequestPlaneBridge.ERROR_OK) {
                    throw new IllegalStateException(
                            "Native MapState snapshot probe returned an invalid result.");
                }
                int status = slot.probeStatus(0);
                recordNativeSnapshotAdaptiveProbe(status);
                if (status == NativeRequestPlaneBridge.PROBE_NEGATIVE) {
                    nativeSnapshotNegativeHits++;
                    return MapSnapshot.empty();
                }
                if (status == NativeRequestPlaneBridge.PROBE_HIT) {
                    nativeSnapshotHits++;
                    return MapSnapshot.single(
                            userKeySerializer.deserialize(slot.probeValueInput(0)));
                }
                if (status == NativeRequestPlaneBridge.PROBE_MISS) {
                    nativeSnapshotMisses++;
                    return null;
                }
                throw new IllegalStateException(
                        "Native MapState snapshot probe returned status=" + status + ".");
            }
        } catch (Exception | LinkageError failure) {
            nativeSnapshotFallbacks++;
            nativeRequestPlaneCoordinator.disable(failure);
            return null;
        }
    }

    private void storeNativeSnapshot(K key, N namespace, MapSnapshot<UK> snapshot) {
        if (!nativeMapSnapshotEnabled
                || key == null
                || namespace == null
                || !nativeRequestPlaneCoordinator.isActive()) {
            return;
        }
        if (!shouldStoreNativeSnapshot()) {
            return;
        }
        try {
            nativeRequestPlaneCoordinator.updateExactKey(
                    nativeSnapshotStateId,
                    nativeGeneration,
                    output -> {
                        writeLengthPrefixed(keySerializer, key, output);
                        writeLengthPrefixed(namespaceSerializer, namespace, output);
                    },
                    snapshot.isEmpty()
                            ? null
                            : output ->
                                    userKeySerializer.serialize(snapshot.singleUserKey(), output));
            nativeSnapshotFills++;
        } catch (Exception | LinkageError failure) {
            nativeSnapshotFallbacks++;
            nativeRequestPlaneCoordinator.disable(failure);
        }
    }

    private boolean snapshotOptimizationEnabled() {
        return mapSnapshotCacheEnabled || nativeMapSnapshotEnabled;
    }

    private void advanceNativeGeneration() {
        if (nativeMapCacheEnabled || nativeMapSnapshotEnabled) {
            nativeGeneration++;
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
            KeyNamespaceUserKey<K, N, UK> storage =
                    new KeyNamespaceUserKey<>(
                            currentKey,
                            currentNamespace,
                            userKey,
                            keySerializer,
                            namespaceSerializer,
                            userKeySerializer);
            l1PresenceCache.put(storage, present);
            return present;
        }
        return null;
    }

    private void updatePresence(K currentKey, UK userKey, boolean present) {
        updatePresence(currentKey, currentNamespace, userKey, present);
    }

    private void updatePresence(K currentKey, N namespace, UK userKey, boolean present) {
        if (currentKey == null || namespace == null) {
            return;
        }
        if (usePrimitivePresenceCache) {
            long fp = fingerprint(currentKey, namespace, userKey);
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
        setLookupKey(currentKey, namespace, userKey);

        l2PresenceCache.remove(lookupKey);

        // Storage requries deep copy
        KeyNamespaceUserKey<K, N, UK> storage =
                new KeyNamespaceUserKey<>(
                        currentKey,
                        namespace,
                        userKey,
                        keySerializer,
                        namespaceSerializer,
                        userKeySerializer);
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
        K currentKey = currentKeyProvider.getCurrentKey();
        if (currentKey == null || currentNamespace == null) {
            dirtyValueEntriesByNamespace.clear();
            l1ValueCache.clear();
            l2ValueCache.clear();
            return;
        }
        removeDirtyEntriesForNamespace(currentKey, currentNamespace);
        removeValueEntriesForNamespace(l1ValueCache, currentKey, currentNamespace);
        removeValueEntriesForNamespace(l2ValueCache, currentKey, currentNamespace);
    }

    private void removeValueEntriesForNamespace(
            CachePolicy<KeyNamespaceUserKey<K, N, UK>, CachedMapValue<UV>> cache,
            K currentKey,
            N namespace) {
        java.util.List<KeyNamespaceUserKey<K, N, UK>> toRemove = new java.util.ArrayList<>();
        for (Map.Entry<KeyNamespaceUserKey<K, N, UK>, CachedMapValue<UV>> entry : cache.entries()) {
            KeyNamespaceUserKey<K, N, UK> key = entry.getKey();
            if (key != null
                    && Objects.equals(currentKey, key.key)
                    && Objects.equals(namespace, key.namespace)) {
                toRemove.add(key);
            }
        }
        for (KeyNamespaceUserKey<K, N, UK> key : toRemove) {
            cache.remove(key);
        }
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
            KeyNamespaceUserKey<K, N, UK> storage =
                    new KeyNamespaceUserKey<>(
                            currentKey,
                            currentNamespace,
                            lookupKey.userKey,
                            keySerializer,
                            namespaceSerializer,
                            userKeySerializer);
            l1ValueCache.put(storage, cached);
            l2ValueCache.remove(lookupKey);
        }
        return cached;
    }

    private void updateValueCache(K currentKey, UK userKey, UV userValue, boolean dirty) {
        updateValueCache(currentKey, currentNamespace, userKey, userValue, dirty);
    }

    private void updateValueCache(
            K currentKey, N namespace, UK userKey, UV userValue, boolean dirty) {
        if (currentKey == null || namespace == null) {
            return;
        }
        UV cachedValue = copyUserValue(userValue);
        KeyNamespaceUserKey<K, N, UK> storage =
                new KeyNamespaceUserKey<>(
                        currentKey,
                        namespace,
                        userKey,
                        keySerializer,
                        namespaceSerializer,
                        userKeySerializer);
        CachedMapValue<UV> cached = CachedMapValue.of(cachedValue, dirty);
        if (dirty) {
            trackDirtyEntry(storage);
        } else {
            untrackDirtyEntry(storage);
        }
        l1ValueCache.put(storage, cached);
        // remove allows probe key
        setLookupKey(currentKey, namespace, userKey);
        l2ValueCache.remove(lookupKey);
    }

    private UV copyUserValue(UV value) {
        if (value == null || userValueSerializer == null) {
            return value;
        }
        return userValueSerializer.copy(value);
    }

    private Iterable<UK> cacheKeys(Iterable<Map.Entry<UK, UV>> entries, K key, N namespace) {
        KeyNamespace<K, N> captured = newStoredKeyNamespace(key, namespace);
        return () ->
                new Iterator<UK>() {
            private final Iterator<Map.Entry<UK, UV>> delegateIterator =
                    iteratorInContext(entries, captured.key, captured.namespace);
            private UK lastUserKey;

            @Override
            public boolean hasNext() {
                return delegateIterator.hasNext();
            }

            @Override
            public UK next() {
                Map.Entry<UK, UV> entry = delegateIterator.next();
                lastUserKey = copyUserKey(entry.getKey());
                cacheEntry(captured.key, captured.namespace, entry);
                return entry.getKey();
            }

            @Override
            public void remove() {
                delegateIterator.remove();
                recordIteratorRemoval(captured.key, captured.namespace, lastUserKey);
            }
        };
    }

    private Iterable<UV> cacheValues(Iterable<Map.Entry<UK, UV>> entries, K key, N namespace) {
        KeyNamespace<K, N> captured = newStoredKeyNamespace(key, namespace);
        return () ->
                new Iterator<UV>() {
            private final Iterator<Map.Entry<UK, UV>> delegateIterator =
                    iteratorInContext(entries, captured.key, captured.namespace);
            private UK lastUserKey;

            @Override
            public boolean hasNext() {
                return delegateIterator.hasNext();
            }

            @Override
            public UV next() {
                Map.Entry<UK, UV> entry = delegateIterator.next();
                lastUserKey = copyUserKey(entry.getKey());
                cacheEntry(captured.key, captured.namespace, entry);
                return entry.getValue();
            }

            @Override
            public void remove() {
                delegateIterator.remove();
                recordIteratorRemoval(captured.key, captured.namespace, lastUserKey);
            }
        };
    }

    private void recordIteratorRemoval(K key, N namespace, UK userKey) {
        advanceNativeGeneration();
        if (userKey != null) {
            if (mapCacheEnabled) {
                updateValueCache(key, namespace, userKey, null, false);
            }
            if (presenceCacheEnabled) {
                updatePresence(key, namespace, userKey, false);
            }
        }
        invalidateSnapshot(key, namespace);
    }

    private Iterable<UK> entryKeys(Iterable<Map.Entry<UK, UV>> entries) {
        return () ->
                new Iterator<UK>() {
            private final Iterator<Map.Entry<UK, UV>> delegateIterator = entries.iterator();

            @Override
            public boolean hasNext() {
                return delegateIterator.hasNext();
            }

            @Override
            public UK next() {
                return delegateIterator.next().getKey();
            }

            @Override
            public void remove() {
                delegateIterator.remove();
            }
        };
    }

    private Iterable<UV> entryValues(Iterable<Map.Entry<UK, UV>> entries) {
        return () ->
                new Iterator<UV>() {
            private final Iterator<Map.Entry<UK, UV>> delegateIterator = entries.iterator();

            @Override
            public boolean hasNext() {
                return delegateIterator.hasNext();
            }

            @Override
            public UV next() {
                return delegateIterator.next().getValue();
            }

            @Override
            public void remove() {
                delegateIterator.remove();
            }
        };
    }

    private void cacheEntry(Map.Entry<UK, UV> entry) {
        cacheEntry(currentKeyProvider.getCurrentKey(), currentNamespace, entry);
    }

    private void cacheEntry(K currentKey, N namespace, Map.Entry<UK, UV> entry) {
        if (entry == null) {
            return;
        }
        UK userKey = entry.getKey();
        if (userKey == null) {
            return;
        }
        if (mapCacheEnabled) {
            updateValueCache(currentKey, namespace, userKey, entry.getValue(), false);
        }
        if (presenceCacheEnabled) {
            updatePresence(currentKey, namespace, userKey, entry.getValue() != null);
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
        if (value.dirty) {
            flushEntryToDelegate(key, value);
            untrackDirtyEntry(key);
            l2ValueCache.put(key, CachedMapValue.of(value.valueOrNull(), false));
            return;
        }
        untrackDirtyEntry(key);
        l2ValueCache.put(key, value);
    }

    private void onValueL2Eviction(KeyNamespaceUserKey<K, N, UK> key, CachedMapValue<UV> value) {
        if (key == null || value == null || !value.dirty) {
            return;
        }
        flushEntryToDelegate(key, value);
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
                flush();
                isBypassing = true;
                opsSinceLastSample = 0;
            }

            currentWindowAccesses = 0;
            currentWindowHits = 0;
        }
    }

    private void resetBypassState() {
        isBypassing = false;
        currentWindowAccesses = 0;
        currentWindowHits = 0;
        opsSinceLastSample = 0;
    }

    public void flush() {
        flush(null, null);
    }

    private void flushCurrentKey(K currentKey) {
        N namespace = currentNamespace;
        if (currentKey == null || namespace == null) {
            flush();
            return;
        }
        if (!hasDirtyEntries(currentKey, namespace)) {
            return;
        }
        flush(currentKey, namespace);
    }

    private void flush(K scopeKey, N scopeNamespace) {
        boolean scoped = scopeKey != null && scopeNamespace != null;
        lifecycleLock.readLock().lock();
        try {
            if (closed || !mapCacheEnabled) {
                return;
            }

            for (KeyNamespaceUserKey<K, N, UK> key :
                    snapshotDirtyKeys(scopeKey, scopeNamespace, scoped)) {
                flushDirtyEntry(key);
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    private List<KeyNamespaceUserKey<K, N, UK>> snapshotDirtyKeys(
            K scopeKey, N scopeNamespace, boolean scoped) {
        if (scoped) {
            dirtyNamespaceProbe.key = scopeKey;
            dirtyNamespaceProbe.namespace = scopeNamespace;
            Set<KeyNamespaceUserKey<K, N, UK>> scopedEntries =
                    dirtyValueEntriesByNamespace.get(dirtyNamespaceProbe);
            if (scopedEntries == null || scopedEntries.isEmpty()) {
                return Collections.emptyList();
            }
            return new ArrayList<>(scopedEntries);
        }

        if (dirtyValueEntriesByNamespace.isEmpty()) {
            return Collections.emptyList();
        }

        List<KeyNamespaceUserKey<K, N, UK>> dirtyKeys = new ArrayList<>();
        for (Set<KeyNamespaceUserKey<K, N, UK>> entries : dirtyValueEntriesByNamespace.values()) {
            dirtyKeys.addAll(entries);
        }
        return dirtyKeys;
    }

    private void flushDirtyEntry(KeyNamespaceUserKey<K, N, UK> key) {
        CachedMapValue<UV> value = l1ValueCache.get(key);
        if (value == null || !value.dirty) {
            untrackDirtyEntry(key);
            return;
        }

        flushEntryToDelegate(key, value);
        CachedMapValue<UV> clean = CachedMapValue.of(value.valueOrNull(), false);
        l2ValueCache.put(key, clean);
        l1ValueCache.put(key, clean);
        untrackDirtyEntry(key);
    }

    private boolean hasDirtyEntries(K key, N namespace) {
        dirtyNamespaceProbe.key = key;
        dirtyNamespaceProbe.namespace = namespace;
        Set<KeyNamespaceUserKey<K, N, UK>> entries =
                dirtyValueEntriesByNamespace.get(dirtyNamespaceProbe);
        return entries != null && !entries.isEmpty();
    }

    private void trackDirtyEntry(KeyNamespaceUserKey<K, N, UK> key) {
        dirtyNamespaceProbe.key = key.key;
        dirtyNamespaceProbe.namespace = key.namespace;
        Set<KeyNamespaceUserKey<K, N, UK>> entries =
                dirtyValueEntriesByNamespace.get(dirtyNamespaceProbe);
        if (entries == null) {
            entries = new HashSet<>();
            dirtyValueEntriesByNamespace.put(new KeyNamespace<>(key.key, key.namespace), entries);
        }
        entries.add(key);
    }

    private void untrackDirtyEntry(KeyNamespaceUserKey<K, N, UK> key) {
        if (key == null) {
            return;
        }
        dirtyNamespaceProbe.key = key.key;
        dirtyNamespaceProbe.namespace = key.namespace;
        Set<KeyNamespaceUserKey<K, N, UK>> entries =
                dirtyValueEntriesByNamespace.get(dirtyNamespaceProbe);
        if (entries == null) {
            return;
        }
        entries.remove(key);
        if (entries.isEmpty()) {
            dirtyValueEntriesByNamespace.remove(dirtyNamespaceProbe);
        }
    }

    private void removeDirtyEntriesForNamespace(K key, N namespace) {
        dirtyNamespaceProbe.key = key;
        dirtyNamespaceProbe.namespace = namespace;
        dirtyValueEntriesByNamespace.remove(dirtyNamespaceProbe);
    }

    private void flushEntryToDelegate(KeyNamespaceUserKey<K, N, UK> key, CachedMapValue<UV> value) {
        lifecycleLock.readLock().lock();
        try {
            if (closed || key == null || value == null) {
                return;
            }
            K previousKey = currentKeyProvider.getCurrentKey();
            N previousNamespace = currentNamespace;
            keyContextSetter.accept(key.key);
            delegate.setCurrentNamespace(key.namespace);
            try {
                if (value.isNull) {
                    delegate.remove(key.userKey);
                } else {
                    delegate.put(key.userKey, value.value);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to flush MapState entry to delegate", e);
            } finally {
                keyContextSetter.accept(previousKey);
                if (previousNamespace != null) {
                    delegate.setCurrentNamespace(previousNamespace);
                }
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    private void cancelQueuedAndAwaitAsyncBatchPrefetchTasks() {
        List<PrefetchExecutor.DropAwareTask> snapshot;
        synchronized (asyncBatchPrefetchMonitor) {
            snapshot = new ArrayList<>(outstandingAsyncBatchPrefetchTasks);
        }
        for (PrefetchExecutor.DropAwareTask task : snapshot) {
            PrefetchExecutor.cancelIfQueued(task);
        }

        boolean interrupted = false;
        synchronized (asyncBatchPrefetchMonitor) {
            while (!outstandingAsyncBatchPrefetchTasks.isEmpty()) {
                try {
                    asyncBatchPrefetchMonitor.wait();
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Quiesces dirty write-back and async reads before releasing the RocksDB delegate. */
    public void close() {
        closed = true;
        cancelQueuedAndAwaitAsyncBatchPrefetchTasks();
        lifecycleLock.writeLock().lock();
        try {
            endPrefetchCurrentKeys();
        } finally {
            lifecycleLock.writeLock().unlock();
        }
        if (nativeMapCacheEnabled) {
            LOG.info(
                    "[CACHEKIT NATIVE MAP CACHE] stateId={} generation={} probes={} hits={} "
                            + "negativeHits={} misses={} fills={} fallbacks={} failures={} "
                            + "nativeActive={} kernel={}",
                    nativeStateId,
                    nativeGeneration,
                    nativeProbeAttempts,
                    nativeHits,
                    nativeNegativeHits,
                    nativeMisses,
                    nativeFills,
                    nativeFallbacks,
                    nativeFailures,
                    nativeRequestPlaneCoordinator.isActive(),
                    nativeRequestPlaneCoordinator.selectedKernel());
        }
        if (nativeMapSnapshotEnabled) {
            LOG.info(
                    "[CACHEKIT NATIVE MAP SNAPSHOT] stateId={} generation={} probes={} hits={} "
                            + "negativeHits={} misses={} fills={} fallbacks={} nativeActive={} kernel={}",
                    nativeSnapshotStateId,
                    nativeGeneration,
                    nativeSnapshotProbes,
                    nativeSnapshotHits,
                    nativeSnapshotNegativeHits,
                    nativeSnapshotMisses,
                    nativeSnapshotFills,
                    nativeSnapshotFallbacks,
                    nativeRequestPlaneCoordinator.isActive(),
                    nativeRequestPlaneCoordinator.selectedKernel());
            LOG.info(
                    "[CACHEKIT NATIVE MAP SNAPSHOT ADAPTIVE] stateId={} enabled={} mode={} "
                            + "windowProbes={} windowPositiveHits={} windowNegativeHits={} "
                            + "windowMisses={} windowFillAttempts={} evaluatedWindows={} "
                            + "bypassTransitions={} trialTransitions={} bypassedProbes={} "
                            + "bypassedFills={} windowLimit={} minUsefulHitRate={} resampleInterval={}",
                    nativeSnapshotStateId,
                    nativeSnapshotAdaptiveBypassEnabled,
                    nativeSnapshotAdaptiveMode,
                    nativeSnapshotAdaptiveWindowProbeCount,
                    nativeSnapshotAdaptiveWindowPositiveHits,
                    nativeSnapshotAdaptiveWindowNegativeHits,
                    nativeSnapshotAdaptiveWindowMisses,
                    nativeSnapshotAdaptiveWindowFillAttempts,
                    nativeSnapshotAdaptiveEvaluatedWindows,
                    nativeSnapshotAdaptiveBypassTransitions,
                    nativeSnapshotAdaptiveTrialTransitions,
                    nativeSnapshotAdaptiveBypassedProbes,
                    nativeSnapshotAdaptiveBypassedFills,
                    nativeSnapshotAdaptiveWindowProbes,
                    nativeSnapshotAdaptiveMinUsefulHitRate,
                    nativeSnapshotAdaptiveResampleIntervalProbes);
        }
        if (nativeDistinctBatchPrefetchEnabled) {
            LOG.info(
                    "[CACHEKIT NATIVE MAP DISTINCT PREFETCH] attempts={} batches={} inputKeys={} "
                            + "uniqueKeys={} found={} missing={} stagingHits={} directOverlayValues={} "
                            + "fallbacks={} failures={} directArenaEnabled={} directArenaDisabled={} "
                            + "directArenaAttempts={} directArenaBatches={} directArenaKeys={} "
                            + "directArenaCompletedBatches={} directArenaCompletedKeys={} "
                            + "directArenaFound={} directArenaMissing={} directArenaOverflows={} "
                            + "directArenaFallbacks={} directArenaFailures={} "
                            + "directArenaProtocolFallbacks={} "
                            + "batchReaderAvailable={}",
                    batchPrefetchAttempts,
                    batchPrefetchBatches,
                    batchPrefetchInputKeys,
                    batchPrefetchUniqueKeys,
                    batchPrefetchFound,
                    batchPrefetchMissing,
                    batchPrefetchHits,
                    batchPrefetchDirectOverlayValues,
                    batchPrefetchFallbacks,
                    batchPrefetchFailures,
                    nativeDistinctBatchDirectArenaEnabled,
                    nativeDistinctBatchDirectArenaDisabled,
                    batchPrefetchDirectArenaAttempts,
                    batchPrefetchDirectArenaBatches,
                    batchPrefetchDirectArenaKeys,
                    batchPrefetchDirectArenaCompletedBatches,
                    batchPrefetchDirectArenaCompletedKeys,
                    batchPrefetchDirectArenaFound,
                    batchPrefetchDirectArenaMissing,
                    batchPrefetchDirectArenaOverflows,
                    batchPrefetchDirectArenaFallbacks,
                    batchPrefetchDirectArenaFailures,
                    batchPrefetchDirectArenaProtocolFallbacks,
                    rocksDBBatchMapReader != null);
            LOG.info(
                    "[CACHEKIT NATIVE MAP DISTINCT ASYNC] attempts={} submitted={} completed={} "
                            + "dropped={} failures={} keys={} awaits={} readyBeforeAwait={} "
                            + "waitNanos={} queueNanos={} serviceNanos={} readySlackNanos={} "
                            + "workFirstEnabled={} workFirstEligible={} callerRuns={} "
                            + "callerRunKeys={} callerRunNanos={} workerIdle={} backlogEmpty={} "
                            + "permitBusy={} queuedSubmissions={} executorMaxActive={} "
                            + "asyncMinUniqueKeys={} deferredSyncBatches={} "
                            + "deferredSyncKeys={} deferredSyncCompleted={} "
                            + "deferredSyncFallbacks={} deferredSyncContextMismatches={} "
                            + "asyncDirectArenaBatches={} asyncDirectArenaKeys={} "
                            + "asyncDirectArenaCompletedBatches={} asyncDirectArenaCompletedKeys={} "
                            + "asyncDirectArenaFound={} asyncDirectArenaMissing={} "
                            + "asyncDirectArenaLeaseMisses={} asyncDirectArenaOverflows={} "
                            + "asyncDirectArenaProtocolFailures={} "
                            + "asyncDirectArenaPreparationFallbacks={} "
                            + "coordinatorAsyncSlotLeases={} coordinatorAsyncSlotLeaseMisses={}",
                    asyncBatchPrefetchAttempts.get(),
                    asyncBatchPrefetchSubmitted.get(),
                    asyncBatchPrefetchCompleted.get(),
                    asyncBatchPrefetchDropped.get(),
                    asyncBatchPrefetchFailures.get(),
                    asyncBatchPrefetchKeys.get(),
                    asyncBatchPrefetchAwaits.get(),
                    asyncBatchPrefetchReadyBeforeAwait.get(),
                    asyncBatchPrefetchWaitNanos.get(),
                    asyncBatchPrefetchQueueNanos.get(),
                    asyncBatchPrefetchServiceNanos.get(),
                    asyncBatchPrefetchReadySlackNanos.get(),
                    nativeDistinctBatchWorkFirstEnabled,
                    asyncBatchPrefetchWorkFirstEligible.get(),
                    asyncBatchPrefetchCallerRuns.get(),
                    asyncBatchPrefetchCallerRunKeys.get(),
                    asyncBatchPrefetchCallerRunNanos.get(),
                    asyncBatchPrefetchWorkFirstWorkerIdle.get(),
                    asyncBatchPrefetchWorkFirstBacklogEmpty.get(),
                    asyncBatchPrefetchWorkFirstPermitBusy.get(),
                    asyncBatchPrefetchQueuedSubmissions.get(),
                    PrefetchExecutor.maxActiveExecutions(),
                    nativeDistinctBatchAsyncMinUniqueKeys,
                    deferredSyncBatchPrefetchBatches.get(),
                    deferredSyncBatchPrefetchKeys.get(),
                    deferredSyncBatchPrefetchCompleted.get(),
                    deferredSyncBatchPrefetchFallbacks.get(),
                    deferredSyncBatchPrefetchContextMismatches.get(),
                    asyncDirectArenaBatches.get(),
                    asyncDirectArenaKeys.get(),
                    asyncDirectArenaCompletedBatches.get(),
                    asyncDirectArenaCompletedKeys.get(),
                    asyncDirectArenaFound.get(),
                    asyncDirectArenaMissing.get(),
                    asyncDirectArenaLeaseMisses.get(),
                    asyncDirectArenaOverflows.get(),
                    asyncDirectArenaProtocolFailures.get(),
                    asyncDirectArenaPreparationFallbacks.get(),
                    nativeRequestPlaneCoordinator == null
                            ? 0L
                            : nativeRequestPlaneCoordinator.mapDistinctAsyncReadLeases(),
                    nativeRequestPlaneCoordinator == null
                            ? 0L
                            : nativeRequestPlaneCoordinator.mapDistinctAsyncReadLeaseMisses());
            LOG.info(
                    "[CACHEKIT NATIVE MAP DISTINCT WAVE] enabled={} attempts={} submitted={} "
                            + "windows={} dropped={} groups={} keys={} jniCalls={} found={} "
                            + "missing={} completedGroups={} cancelledGroups={} awaits={} "
                            + "readyBeforeAwait={} waitNanos={} queueNanos={} serviceNanos={} "
                            + "readySlackNanos={} "
                            + "fallbackWindows={} failures={} ownerRejects={} overflows={} "
                            + "protocolFailures={}",
                    nativeDistinctBatchDeferredWaveEnabled,
                    deferredWaveAttempts.get(),
                    deferredWaveSubmitted.get(),
                    deferredWaveWindows.get(),
                    deferredWaveDropped.get(),
                    deferredWaveGroups.get(),
                    deferredWaveKeys.get(),
                    deferredWaveJniCalls.get(),
                    deferredWaveFound.get(),
                    deferredWaveMissing.get(),
                    deferredWaveCompletedGroups.get(),
                    deferredWaveCancelledGroups.get(),
                    deferredWaveAwaits.get(),
                    deferredWaveReadyBeforeAwait.get(),
                    deferredWaveWaitNanos.get(),
                    deferredWaveQueueNanos.get(),
                    deferredWaveServiceNanos.get(),
                    deferredWaveReadySlackNanos.get(),
                    deferredWaveFallbackWindows.get(),
                    deferredWaveFailures.get(),
                    deferredWaveOwnerRejects.get(),
                    deferredWaveOverflows.get(),
                    deferredWaveProtocolFailures.get());
        }
    }

    long getNativeProbeAttemptsForTesting() {
        return nativeProbeAttempts;
    }

    long getNativeHitsForTesting() {
        return nativeHits;
    }

    long getNativeNegativeHitsForTesting() {
        return nativeNegativeHits;
    }

    long getNativeMissesForTesting() {
        return nativeMisses;
    }

    long getNativeFillsForTesting() {
        return nativeFills;
    }

    long getNativeSnapshotHitsForTesting() {
        return nativeSnapshotHits;
    }

    long getNativeSnapshotNegativeHitsForTesting() {
        return nativeSnapshotNegativeHits;
    }

    long getNativeSnapshotFillsForTesting() {
        return nativeSnapshotFills;
    }

    long getNativeSnapshotProbesForTesting() {
        return nativeSnapshotProbes;
    }

    long getNativeSnapshotAdaptiveEvaluatedWindowsForTesting() {
        return nativeSnapshotAdaptiveEvaluatedWindows;
    }

    long getNativeSnapshotAdaptiveBypassTransitionsForTesting() {
        return nativeSnapshotAdaptiveBypassTransitions;
    }

    long getNativeSnapshotAdaptiveTrialTransitionsForTesting() {
        return nativeSnapshotAdaptiveTrialTransitions;
    }

    long getNativeSnapshotAdaptiveBypassedProbesForTesting() {
        return nativeSnapshotAdaptiveBypassedProbes;
    }

    long getNativeSnapshotAdaptiveBypassedFillsForTesting() {
        return nativeSnapshotAdaptiveBypassedFills;
    }

    boolean isNativeSnapshotAdaptiveBypassingForTesting() {
        return nativeSnapshotAdaptiveMode == NativeSnapshotAdaptiveMode.BYPASS;
    }

    long getBatchPrefetchBatchesForTesting() {
        return batchPrefetchBatches;
    }

    long getBatchPrefetchUniqueKeysForTesting() {
        return batchPrefetchUniqueKeys;
    }

    long getBatchPrefetchHitsForTesting() {
        return batchPrefetchHits;
    }

    long getBatchPrefetchDirectOverlayValuesForTesting() {
        return batchPrefetchDirectOverlayValues;
    }

    long getBatchPrefetchFallbacksForTesting() {
        return batchPrefetchFallbacks;
    }

    long getBatchPrefetchDirectArenaCompletedKeysForTesting() {
        return batchPrefetchDirectArenaCompletedKeys;
    }

    long getBatchPrefetchDirectArenaFallbacksForTesting() {
        return batchPrefetchDirectArenaFallbacks;
    }

    long getDeferredSyncBatchPrefetchBatchesForTesting() {
        return deferredSyncBatchPrefetchBatches.get();
    }

    long getDeferredSyncBatchPrefetchCompletedForTesting() {
        return deferredSyncBatchPrefetchCompleted.get();
    }

    long getDeferredSyncBatchPrefetchFallbacksForTesting() {
        return deferredSyncBatchPrefetchFallbacks.get();
    }

    long getDeferredSyncBatchPrefetchContextMismatchesForTesting() {
        return deferredSyncBatchPrefetchContextMismatches.get();
    }

    long getAsyncBatchPrefetchSubmittedForTesting() {
        return asyncBatchPrefetchSubmitted.get();
    }

    long getAsyncBatchPrefetchCallerRunsForTesting() {
        return asyncBatchPrefetchCallerRuns.get();
    }

    long getAsyncBatchPrefetchQueuedSubmissionsForTesting() {
        return asyncBatchPrefetchQueuedSubmissions.get();
    }

    long getDeferredWaveWindowsForTesting() {
        return deferredWaveWindows.get();
    }

    long getDeferredWaveSubmittedForTesting() {
        return deferredWaveSubmitted.get();
    }

    long getDeferredWaveDroppedForTesting() {
        return deferredWaveDropped.get();
    }

    long getDeferredWaveGroupsForTesting() {
        return deferredWaveGroups.get();
    }

    long getDeferredWaveKeysForTesting() {
        return deferredWaveKeys.get();
    }

    long getDeferredWaveJniCallsForTesting() {
        return deferredWaveJniCalls.get();
    }

    long getDeferredWaveCompletedGroupsForTesting() {
        return deferredWaveCompletedGroups.get();
    }

    long getDeferredWaveCancelledGroupsForTesting() {
        return deferredWaveCancelledGroups.get();
    }

    long getDeferredWaveFallbackWindowsForTesting() {
        return deferredWaveFallbackWindows.get();
    }

    long getDeferredWaveOwnerRejectsForTesting() {
        return deferredWaveOwnerRejects.get();
    }

    long getDeferredWaveProtocolFailuresForTesting() {
        return deferredWaveProtocolFailures.get();
    }

    long getDeferredWaveAwaitsForTesting() {
        return deferredWaveAwaits.get();
    }

    long getDeferredWaveReadyBeforeAwaitForTesting() {
        return deferredWaveReadyBeforeAwait.get();
    }

    private static final class PrefetchedMapValue<V> {
        private final boolean present;
        private final V value;

        private PrefetchedMapValue(boolean present, V value) {
            this.present = present;
            this.value = value;
        }

        private static <V> PrefetchedMapValue<V> present(V value) {
            return new PrefetchedMapValue<>(true, value);
        }

        private static <V> PrefetchedMapValue<V> missing() {
            return new PrefetchedMapValue<>(false, null);
        }
    }

    private static final class NativeMapRead<V> {
        private final V value;
        private final boolean cacheHit;

        private NativeMapRead(V value, boolean cacheHit) {
            this.value = value;
            this.cacheHit = cacheHit;
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
        private KeyNamespaceUserKey(
                K key,
                N namespace,
                UK userKey,
                TypeSerializer<K> keySerializer,
                TypeSerializer<N> namespaceSerializer,
                TypeSerializer<UK> userKeySerializer) {
            this.key = keySerializer != null ? keySerializer.copy(key) : key;
            this.namespace =
                    namespaceSerializer != null ? namespaceSerializer.copy(namespace) : namespace;
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
            return CacheKeyHash.hash(key, namespace, userKey);
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
        public void clear() {}

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Iterable<Map.Entry<K, V>> entries() {
            return Collections.emptyList();
        }
    }

    private static final class CachedMapValue<V> {
        private final V value;
        private final boolean isNull;
        private final boolean dirty;

        private CachedMapValue(V value, boolean isNull, boolean dirty) {
            this.value = value;
            this.isNull = isNull;
            this.dirty = dirty;
        }

        static <V> CachedMapValue<V> of(V value, boolean dirty) {
            return new CachedMapValue<>(value, value == null, dirty);
        }

        V valueOrNull() {
            return isNull ? null : value;
        }

        boolean isNull() {
            return isNull;
        }
    }

    // ====================================================================
    // MapSnapshot cache: entries() short-circuit logic
    // ====================================================================

    private Iterable<Map.Entry<UK, UV>> trySnapshotShortCircuit(K currentKey) throws Exception {
        MapSnapshot<UK> snapshot = lookupSnapshot(currentKey);
        if (snapshot == null) {
            return null; // UNKNOWN → fallthrough to delegate
        }
        if (snapshot.isEmpty()) {
            // EMPTY → return empty list, zero JNI
            mapSnapshotCacheMetrics.recordEmptyShortCircuit();
            return Collections.emptyList();
        }
        // SINGLE/SMALL → downgrade the short range scan to bounded point gets. Resolve every
        // value before publishing the iterable so a stale key can fall back without exposing a
        // partial result.
        List<UV> values = new ArrayList<>(snapshot.size());
        for (UK userKey : snapshot.cachedUserKeys) {
            UV value = this.get(userKey);
            if (value == null) {
                // A stale native snapshot must become a generation miss. Publishing a null
                // exact-key fill would mean a real EMPTY snapshot, not a tombstone.
                advanceNativeGeneration();
                removeSnapshot(snapshotProbe);
                mapSnapshotCacheMetrics.recordStaleInvalidation();
                return null;
            }
            values.add(value);
        }
        if (snapshot.isSingle()) {
            mapSnapshotCacheMetrics.recordSingleShortCircuit();
        } else {
            mapSnapshotCacheMetrics.recordSmallShortCircuit();
        }
        KeyNamespace<K, N> captured = newStoredKeyNamespace(currentKey, currentNamespace);
        return snapshotEntries(captured.key, captured.namespace, snapshot.cachedUserKeys, values);
    }

    private MapSnapshot<UK> lookupSnapshot(K currentKey) {
        if (!snapshotOptimizationEnabled() || currentKey == null || currentNamespace == null) {
            return null;
        }
        snapshotProbe.key = currentKey;
        snapshotProbe.namespace = currentNamespace;
        mapSnapshotCacheMetrics.recordProbe();
        MapSnapshot<UK> snapshot =
                mapSnapshotCacheEnabled ? mapSnapshotCache.get(snapshotProbe) : null;
        if (snapshot == null) {
            snapshot = lookupNativeSnapshot(currentKey);
        }
        if (snapshot == null) {
            mapSnapshotCacheMetrics.recordMiss();
        } else {
            mapSnapshotCacheMetrics.recordHit();
        }
        return snapshot;
    }

    private void storeSnapshot(KeyNamespace<K, N> key, MapSnapshot<UK> snapshot) {
        if (mapSnapshotCacheEnabled) {
            mapSnapshotCache.put(key, snapshot);
        }
        if (snapshot.isEmpty() || snapshot.isSingle()) {
            storeNativeSnapshot(key.key, key.namespace, snapshot);
        }
        if (snapshot.isEmpty()) {
            mapSnapshotCacheMetrics.recordStoreEmpty();
        } else if (snapshot.isSingle()) {
            mapSnapshotCacheMetrics.recordStoreSingle();
        } else {
            mapSnapshotCacheMetrics.recordStoreSmall();
        }
    }

    private boolean removeSnapshot(KeyNamespace<K, N> key) {
        boolean removed = mapSnapshotCacheEnabled && mapSnapshotCache.remove(key) != null;
        if (removed) {
            mapSnapshotCacheMetrics.recordInvalidation();
        }
        return removed;
    }

    private void invalidateSnapshot(K currentKey) {
        invalidateSnapshot(currentKey, currentNamespace);
    }

    private void invalidateSnapshot(K currentKey, N namespace) {
        if (!snapshotOptimizationEnabled() || currentKey == null || namespace == null) {
            return;
        }
        snapshotProbe.key = currentKey;
        snapshotProbe.namespace = namespace;
        removeSnapshot(snapshotProbe);
    }

    private Iterable<Map.Entry<UK, UV>> wrapWithSnapshotAwareIterator(
            Iterable<Map.Entry<UK, UV>> delegateEntries,
            K currentKey,
            N namespace,
            boolean cacheEntries) {
        KeyNamespace<K, N> captured = newStoredKeyNamespace(currentKey, namespace);
        return () ->
                new SnapshotAwareIterator(
                        iteratorInContext(delegateEntries, captured.key, captured.namespace),
                        captured.key,
                        captured.namespace,
                        cacheEntries);
    }

    private Iterator<Map.Entry<UK, UV>> iteratorInContext(
            Iterable<Map.Entry<UK, UV>> entries, K stateKey, N namespace) {
        K previousKey = currentKeyProvider.getCurrentKey();
        N previousNamespace = currentNamespace;
        keyContextSetter.accept(stateKey);
        currentNamespace = namespace;
        delegate.setCurrentNamespace(namespace);
        try {
            return entries.iterator();
        } finally {
            keyContextSetter.accept(previousKey);
            currentNamespace = previousNamespace;
            if (previousNamespace != null) {
                delegate.setCurrentNamespace(previousNamespace);
            }
        }
    }

    private Iterable<Map.Entry<UK, UV>> snapshotEntries(
            K stateKey, N namespace, List<UK> userKeys, List<UV> values) {
        final List<UK> internalUserKeys = new ArrayList<>(userKeys.size());
        for (UK userKey : userKeys) {
            internalUserKeys.add(copyUserKey(userKey));
        }
        final List<UV> currentValues = new ArrayList<>(values);
        final boolean[] removed = new boolean[internalUserKeys.size()];
        return () ->
                new Iterator<Map.Entry<UK, UV>>() {
            private int index;
            private int lastSlot = -1;
            private boolean removable;

            private void skipRemoved() {
                while (index < removed.length && removed[index]) {
                    index++;
                }
            }

            @Override
            public boolean hasNext() {
                skipRemoved();
                return index < internalUserKeys.size();
            }

            @Override
            public Map.Entry<UK, UV> next() {
                if (!hasNext()) {
                    throw new java.util.NoSuchElementException();
                }
                int slot = index;
                UK userKey = internalUserKeys.get(slot);
                UV value = currentValues.get(slot);
                index++;
                lastSlot = slot;
                removable = true;
                return new SnapshotMapEntry(
                        stateKey, namespace, userKey, value, currentValues, slot);
            }

            @Override
            public void remove() {
                if (!removable) {
                    throw new IllegalStateException("remove() requires a preceding next()");
                }
                try {
                    mutateSnapshotEntry(
                                    stateKey,
                                    namespace,
                                    internalUserKeys.get(lastSlot),
                                    null,
                                    true);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to remove MapState entry", e);
                }
                removed[lastSlot] = true;
                removable = false;
            }
        };
    }

    private final class SnapshotMapEntry implements Map.Entry<UK, UV> {
        private final K stateKey;
        private final N namespace;
        private final UK internalUserKey;
        private final UK exposedUserKey;
        private final List<UV> sharedValues;
        private final int sharedSlot;
        private UV value;

        private SnapshotMapEntry(K stateKey, N namespace, UK userKey, UV value) {
            this(stateKey, namespace, userKey, value, null, -1);
        }

        private SnapshotMapEntry(
                K stateKey,
                N namespace,
                UK userKey,
                UV value,
                List<UV> sharedValues,
                int sharedSlot) {
            this.stateKey = stateKey;
            this.namespace = namespace;
            this.internalUserKey = copyUserKey(userKey);
            this.exposedUserKey = copyUserKey(internalUserKey);
            this.sharedValues = sharedValues;
            this.sharedSlot = sharedSlot;
            this.value = value;
        }

        @Override
        public UK getKey() {
            return exposedUserKey;
        }

        @Override
        public UV getValue() {
            return value;
        }

        @Override
        public UV setValue(UV newValue) {
            if (newValue == null) {
                throw new NullPointerException("MapState entries do not accept null values");
            }
            UV previous = value;
            try {
                mutateSnapshotEntry(stateKey, namespace, internalUserKey, newValue, false);
            } catch (Exception e) {
                throw new RuntimeException("Failed to update MapState entry", e);
            }
            value = newValue;
            if (sharedValues != null) {
                sharedValues.set(sharedSlot, newValue);
            }
            return previous;
        }
    }

    /** Applies an entry mutation to the key and namespace captured by the iterator. */
    private void mutateSnapshotEntry(K stateKey, N namespace, UK userKey, UV value, boolean remove)
            throws Exception {
        K previousKey = currentKeyProvider.getCurrentKey();
        N previousNamespace = currentNamespace;
        keyContextSetter.accept(stateKey);
        currentNamespace = namespace;
        delegate.setCurrentNamespace(namespace);
        try {
            if (remove) {
                CachedInternalMapState.this.remove(userKey);
            } else {
                CachedInternalMapState.this.put(userKey, value);
            }
        } finally {
            keyContextSetter.accept(previousKey);
            currentNamespace = previousNamespace;
            if (previousNamespace != null) {
                delegate.setCurrentNamespace(previousNamespace);
            }
        }
    }

    private KeyNamespace<K, N> newStoredKeyNamespace(K key, N namespace) {
        K keyCopy = keySerializer == null ? key : keySerializer.copy(key);
        N nsCopy = namespaceSerializer == null ? namespace : namespaceSerializer.copy(namespace);
        if (keySerializer == null
                && key instanceof org.apache.flink.table.data.binary.BinaryRowData) {
            keyCopy = (K) ((org.apache.flink.table.data.binary.BinaryRowData) key).copy();
        }
        if (namespaceSerializer == null
                && namespace instanceof org.apache.flink.table.data.binary.BinaryRowData) {
            nsCopy = (N) ((org.apache.flink.table.data.binary.BinaryRowData) namespace).copy();
        }
        return new KeyNamespace<>(keyCopy, nsCopy);
    }

    private static final class KeyNamespace<K, N> {
        K key;
        N namespace;

        KeyNamespace(K key, N namespace) {
            this.key = key;
            this.namespace = namespace;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof KeyNamespace)) return false;
            KeyNamespace<?, ?> that = (KeyNamespace<?, ?>) other;
            return Objects.equals(key, that.key) && Objects.equals(namespace, that.namespace);
        }

        @Override
        public int hashCode() {
            return CacheKeyHash.hash(key, namespace);
        }
    }

    private static final class MapSnapshot<UK> {
        final List<UK> cachedUserKeys;

        MapSnapshot(List<UK> cachedUserKeys) {
            this.cachedUserKeys = Collections.unmodifiableList(new ArrayList<>(cachedUserKeys));
        }

        boolean isEmpty() {
            return cachedUserKeys.isEmpty();
        }

        boolean isSingle() {
            return cachedUserKeys.size() == 1;
        }

        int size() {
            return cachedUserKeys.size();
        }

        UK singleUserKey() {
            return cachedUserKeys.get(0);
        }

        static <UK> MapSnapshot<UK> single(UK userKey) {
            return new MapSnapshot<>(Collections.singletonList(userKey));
        }

        static <UK> MapSnapshot<UK> of(List<UK> userKeys) {
            return new MapSnapshot<>(userKeys);
        }

        static <UK> MapSnapshot<UK> empty() {
            return new MapSnapshot<>(Collections.emptyList());
        }
    }

    private final class SnapshotAwareIterator implements Iterator<Map.Entry<UK, UV>> {
        private final Iterator<Map.Entry<UK, UV>> delegateIterator;
        private final K currentKey;
        private final N namespace;
        private final boolean cacheEntries;
        private int iteratedCount = 0;
        private final List<UK> snapshotUserKeys = new ArrayList<>();
        private UK lastUserKey = null;
        private boolean backfilled = false;

        SnapshotAwareIterator(
                Iterator<Map.Entry<UK, UV>> delegateIterator,
                K currentKey,
                N namespace,
                boolean cacheEntries) {
            this.delegateIterator = delegateIterator;
            KeyNamespace<K, N> captured = newStoredKeyNamespace(currentKey, namespace);
            this.currentKey = captured.key;
            this.namespace = captured.namespace;
            this.cacheEntries = cacheEntries;
        }

        @Override
        public boolean hasNext() {
            boolean has = delegateIterator.hasNext();
            if (!has && !backfilled && snapshotOptimizationEnabled()) {
                backfilled = true;
                backfillSnapshotCache();
            }
            return has;
        }

        @Override
        public Map.Entry<UK, UV> next() {
            Map.Entry<UK, UV> entry = delegateIterator.next();
            if (cacheEntries) {
                cacheEntry(currentKey, namespace, entry);
            }
            iteratedCount++;
            if (iteratedCount <= mapSnapshotSmallMaxEntries) {
                snapshotUserKeys.add(copyUserKey(entry.getKey()));
            } else if (!snapshotUserKeys.isEmpty()) {
                snapshotUserKeys.clear();
            }
            lastUserKey = entry.getKey();
            return new SnapshotMapEntry(currentKey, namespace, entry.getKey(), entry.getValue());
        }

        @Override
        public void remove() {
            delegateIterator.remove();
            advanceNativeGeneration();
            if (lastUserKey != null) {
                if (mapCacheEnabled) {
                    updateValueCache(currentKey, namespace, lastUserKey, null, false);
                }
                if (presenceCacheEnabled) {
                    updatePresence(currentKey, namespace, lastUserKey, false);
                }
            }
            invalidateSnapshot(currentKey, namespace);
            backfilled = true;
        }

        private void backfillSnapshotCache() {
            if (currentKey == null || namespace == null) {
                return;
            }
            KeyNamespace<K, N> stored = newStoredKeyNamespace(currentKey, namespace);
            if (iteratedCount == 0) {
                storeSnapshot(stored, MapSnapshot.empty());
            } else if (iteratedCount <= mapSnapshotSmallMaxEntries
                    && snapshotUserKeys.size() == iteratedCount) {
                storeSnapshot(stored, MapSnapshot.of(snapshotUserKeys));
            } else {
                mapSnapshotCacheMetrics.recordMultiEntrySkip();
                removeSnapshot(stored);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private UK copyUserKey(UK userKey) {
        if (userKey instanceof org.apache.flink.table.data.binary.BinaryRowData) {
            return (UK) ((org.apache.flink.table.data.binary.BinaryRowData) userKey).copy();
        }
        return userKeySerializer == null ? userKey : userKeySerializer.copy(userKey);
    }
}
