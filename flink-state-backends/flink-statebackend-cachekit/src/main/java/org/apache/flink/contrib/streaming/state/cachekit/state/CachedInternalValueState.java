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
import org.apache.flink.contrib.streaming.state.PositionedDataOutputView;
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
    private final NativeRequestPlaneCoordinator.ValueReadActivation nativeValueReadActivation;
    private final boolean nativePointAdaptiveBypassEnabled;
    private final int nativePointAdaptiveWindowProbes;
    private final int nativePointAdaptiveZeroWindows;
    private final int nativePointAdaptiveResampleIntervalProbes;
    private final int nativePointAdaptiveSampleSlots;
    private final long[] nativePointAdaptiveFingerprintSamples;

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
     * barrier after publishing {@link #closed}, guaranteeing no new guarded access starts and no
     * existing access remains in flight before the backend disposes the RocksDB delegate.
     */
    private volatile boolean closed = false;
    private final java.util.concurrent.locks.ReadWriteLock lifecycleLock =
            new java.util.concurrent.locks.ReentrantReadWriteLock();

    /**
     * State-local ownership of tasks submitted to the TM-wide prefetch executor.
     *
     * <p>The lifecycle read/write lock only drains RocksDB calls. A task also owns reservations,
     * direct buffers, and possibly a native batch slot before and after that call. Backend teardown
     * must therefore wait for the complete task (including its completion callback) before closing
     * the shared native request plane.
     */
    private final Object prefetchTaskMonitor = new Object();
    private final java.util.Set<PrefetchExecutor.DropAwareTask> outstandingPrefetchTasks =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());

    // ---- Backpressure-driven async prefetch (off-mailbox worker -> staging -> L1) ----

    /** Hard admission cap on staged entries. */
    private static final int ASYNC_STAGING_MAX_ENTRIES = loadStagingMaxEntries();
    /** Hard cap on RocksDB-owned serialized value bytes retained by lazy staging. */
    private static final long ASYNC_STAGING_MAX_RETAINED_BYTES =
            loadStagingMaxRetainedBytes();
    private static final int MULTIGET_CHUNK_SIZE = loadMultiGetChunkSize();
    private static final int MULTIGET_MIN_BATCH_SIZE = loadMultiGetMinBatchSize();
    private static final boolean ASYNC_ADAPTIVE_ADMISSION_ENABLED =
            loadBooleanConfig(
                    "state.backend.cachekit.bp-prefetch.adaptive-admission.enabled", false);
    private static final int ASYNC_ADAPTIVE_MIN_SAMPLES =
            loadIntConfig(
                    "state.backend.cachekit.bp-prefetch.adaptive-admission.min-samples",
                    1024,
                    64,
                    1_000_000);
    private static final double ASYNC_ADAPTIVE_MIN_USEFUL_RATE =
            loadDoubleConfig(
                    "state.backend.cachekit.bp-prefetch.adaptive-admission.min-useful-rate",
                    0.02,
                    0.0,
                    1.0);
    private static final int ASYNC_ADAPTIVE_PROBE_EVERY_TASKS =
            loadIntConfig(
                    "state.backend.cachekit.bp-prefetch.adaptive-admission.probe-every-tasks",
                    1024,
                    1,
                    1_000_000);
    private static final boolean PROMOTION_YIELD_ADMISSION_ENABLED =
            loadBooleanConfig(
                    "state.backend.cachekit.native.prefetch.promotion-yield-admission.enabled",
                    false);
    private static final int PROMOTION_YIELD_MIN_STAGED_VALUES =
            loadIntConfig(
                    "state.backend.cachekit.native.prefetch.promotion-yield-admission.min-staged-values",
                    4096,
                    64,
                    1_000_000);
    private static final double PROMOTION_YIELD_MIN_PROMOTION_RATE =
            loadDoubleConfig(
                    "state.backend.cachekit.native.prefetch.promotion-yield-admission.min-promotion-rate",
                    0.02,
                    0.0,
                    1.0);
    private static final int PROMOTION_YIELD_PROBE_EVERY_TASKS =
            loadIntConfig(
                    "state.backend.cachekit.native.prefetch.promotion-yield-admission.probe-every-tasks",
                    256,
                    1,
                    1_000_000);
    private static final boolean NATIVE_MAILBOX_ADAPTIVE_DENSITY_ENABLED =
            loadBooleanConfig(
                    "state.backend.cachekit.native.mailbox-batch.adaptive-density.enabled", false);
    private static final boolean
            NATIVE_MAILBOX_ADAPTIVE_DENSITY_DROP_SPECULATIVE_PREFETCH_ENABLED =
                    loadBooleanConfig(
                            "state.backend.cachekit.native.mailbox-batch.adaptive-density.drop-speculative-prefetch.enabled",
                            false);
    private static final int NATIVE_MAILBOX_ADAPTIVE_DENSITY_WINDOW_INPUT_KEYS =
            loadIntConfig(
                    "state.backend.cachekit.native.mailbox-batch.adaptive-density.window-input-keys",
                    8192,
                    1,
                    1_000_000);
    private static final int NATIVE_MAILBOX_ADAPTIVE_DENSITY_WINDOW_BATCHES =
            loadIntConfig(
                    "state.backend.cachekit.native.mailbox-batch.adaptive-density.window-batches",
                    64,
                    1,
                    1_000_000);
    private static final int NATIVE_MAILBOX_ADAPTIVE_DENSITY_LOW_WINDOWS =
            loadIntConfig(
                    "state.backend.cachekit.native.mailbox-batch.adaptive-density.low-density-windows",
                    2,
                    1,
                    1000);
    private static final int NATIVE_MAILBOX_ADAPTIVE_DENSITY_COOLDOWN_BATCHES =
            loadIntConfig(
                    "state.backend.cachekit.native.mailbox-batch.adaptive-density.cooldown-batches",
                    4096,
                    1,
                    1_000_000);
    private static final double NATIVE_MAILBOX_ADAPTIVE_DENSITY_MIN_UNIQUE_RATE =
            loadDoubleConfig(
                    "state.backend.cachekit.native.mailbox-batch.adaptive-density.min-unique-rate",
                    0.10,
                    0.0,
                    1.0);
    private static final double NATIVE_MAILBOX_ADAPTIVE_DENSITY_RECOVERY_UNIQUE_RATE =
            loadDoubleConfig(
                    "state.backend.cachekit.native.mailbox-batch.adaptive-density.recovery-unique-rate",
                    0.15,
                    0.0,
                    1.0);
    private static final boolean NATIVE_ADAPTIVE_PROBE_BYPASS_ENABLED =
            loadBooleanConfig(
                    "state.backend.cachekit.native.compact-selected-probe.adaptive-bypass.enabled",
                    false);
    private static final boolean NATIVE_RESIDENT_MUTATION_ADAPTIVE_ENABLED =
            loadBooleanConfig(
                    "state.backend.cachekit.native.value-cache.resident-mutation-batch.adaptive.enabled",
                    false);
    private static final int NATIVE_RESIDENT_MUTATION_ADAPTIVE_MIN_SCOPES =
            loadIntConfig(
                    "state.backend.cachekit.native.value-cache.resident-mutation-batch.adaptive.min-scopes",
                    4096,
                    1,
                    1_000_000);
    private static final double NATIVE_RESIDENT_MUTATION_ADAPTIVE_MIN_MUTATIONS_PER_SCOPE =
            loadDoubleConfig(
                    "state.backend.cachekit.native.value-cache.resident-mutation-batch.adaptive.min-mutations-per-scope",
                    8.0,
                    0.0,
                    1_000_000.0);
    private static final double NATIVE_RESIDENT_MUTATION_ADAPTIVE_MAX_HINT_POSITIVE_RATE =
            loadDoubleConfig(
                    "state.backend.cachekit.native.value-cache.resident-mutation-batch.adaptive.max-hint-positive-rate",
                    0.10,
                    0.0,
                    1.0);
    private static final int NATIVE_ADAPTIVE_PROBE_WINDOW_KEYS =
            loadIntConfig(
                    "state.backend.cachekit.native.compact-selected-probe.adaptive-bypass.window-keys",
                    4096,
                    1,
                    1_000_000);
    private static final int NATIVE_ADAPTIVE_PROBE_WINDOW_BATCHES =
            loadIntConfig(
                    "state.backend.cachekit.native.compact-selected-probe.adaptive-bypass.window-batches",
                    64,
                    1,
                    1_000_000);
    private static final int NATIVE_ADAPTIVE_PROBE_ZERO_WINDOWS =
            loadIntConfig(
                    "state.backend.cachekit.native.compact-selected-probe.adaptive-bypass.zero-windows",
                    2,
                    1,
                    1000);
    private static final int NATIVE_ADAPTIVE_PROBE_COOLDOWN_BATCHES =
            loadIntConfig(
                    "state.backend.cachekit.native.compact-selected-probe.adaptive-bypass.cooldown-batches",
                    256,
                    1,
                    1_000_000);
    private static final int NATIVE_ADAPTIVE_PROBE_RECOVERY_MIN_USEFUL =
            loadIntConfig(
                    "state.backend.cachekit.native.compact-selected-probe.adaptive-bypass.recovery-min-useful",
                    16,
                    0,
                    1_000_000);
    private static final double NATIVE_ADAPTIVE_PROBE_RECOVERY_USEFUL_RATE =
            loadDoubleConfig(
                    "state.backend.cachekit.native.compact-selected-probe.adaptive-bypass.recovery-useful-rate",
                    0.02,
                    0.0,
                    1.0);
    private final int asyncStagingMaxEntries;
    private final long asyncStagingMaxRetainedBytes;
    /**
     * Whether delegate-visible writes revoke only the matching prepared-key reservation/staging
     * entry instead of invalidating unrelated speculative reads for the whole state.
     */
    private final boolean keyScopedPrefetchInvalidationEnabled;

    /**
     * Values fetched by the shared prefetch worker, waiting to be promoted into L1 by the mailbox
     * thread. The worker publishes, the mailbox removes/promotes, and close clears retained
     * entries. A staged entry may only be promoted while {@code writeGen} still equals the
     * generation captured at submission — any delegate-visible write or dirty flush on this state
     * in between makes the RocksDB read potentially stale.
     */
    private final java.util.concurrent.ConcurrentHashMap<KeyNamespaceKey<K, N>, Object>
            staging = new java.util.concurrent.ConcurrentHashMap<>();
    /**
     * Native direct-read-only NOT_FOUND results need no value payload. When negative handoff is
     * enabled, {@link #staging} stores the already-owned exact storage key as both key and value;
     * positive results retain their {@link StagedValue}. This tagged union avoids both a
     * per-negative {@link SerializedStagedValue} allocation and the second ConcurrentHashMap that
     * the first implementation used. Access is serialized with {@code staging} whenever retained
     * byte accounting or exact write invalidation is involved, and the key sentinel is legal only
     * with key-scoped reservation invalidation.
     */
    private final java.util.concurrent.atomic.AtomicLong stagingRetainedBytes =
            new java.util.concurrent.atomic.AtomicLong();

    /**
     * Keys reserved by submitted-but-not-yet-finished prefetch tasks. Without this set, adjacent
     * early-lookahead chunks can enqueue the same key repeatedly while the first task is still
     * waiting behind the shared worker. Values are write generations so stale reservations can be
     * reclaimed without waiting for their old task.
     */
    private final java.util.concurrent.ConcurrentHashMap<
                    KeyNamespaceKey<K, N>, PrefetchReservation>
            inFlight = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong prefetchReservationSequence =
            new java.util.concurrent.atomic.AtomicLong();

    /**
     * State-wide write generation used by the conservative invalidation mode. When key-scoped
     * invalidation is enabled, a delegate-visible write instead revokes only its exact prepared-key
     * reservation and staging entry. Plain write-back entries remain authoritative in L1 until
     * flush-through. Single writer (mailbox thread); workers only read this generation.
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
    private volatile long prefetchImmediatePointGetCalls;
    /** Speculative prepared batches abandoned after cancellations made them too small to batch. */
    private volatile long prefetchSmallBatchDrops;
    private volatile long prefetchSmallBatchKeysDropped;
    private volatile long prefetchValuesStaged;
    private volatile long prefetchMissingValuesStaged;
    private volatile long prefetchValuesPromoted;
    private volatile long prefetchLazyValuesStaged;
    private volatile long prefetchLazyValuesMaterialized;
    private volatile long prefetchLazyMaterializationFailures;
    private volatile long prefetchStagingAdmissionDrops;
    private volatile long prefetchStaleAborts;
    private volatile long prefetchKeyScopedInvalidations;
    private volatile long prefetchKeyScopedFastNegativeSkips;
    private volatile long prefetchKeyScopedInFlightCancelled;
    private volatile long prefetchKeyScopedStagedRemoved;
    private volatile long prefetchLiveReadRacedInFlight;
    private volatile long prefetchLiveReadCancellations;
    private volatile long prefetchDispatchKeysExamined;
    private volatile long prefetchDispatchCancellations;
    private volatile long prefetchDispatchAlreadyStaged;
    private volatile long prefetchDispatchNoReservation;
    private volatile long prefetchWorkerCancelledBeforeRead;
    private volatile long prefetchWorkerDiscardedAfterRead;
    /** Queue delay from mailbox reservation to worker start, for overlap sizing diagnostics. */
    private volatile long prefetchWorkerQueueNanos;
    private volatile long prefetchWorkerQueueNanosMax;
    /** Worker execution time, excluding the shared-executor queue delay. */
    private volatile long prefetchWorkerRunNanos;
    private volatile long prefetchWorkerRunNanosMax;
    /** Age of a reservation when the mailbox overtakes and cancels it. */
    private volatile long prefetchLiveReadRaceNanos;
    private volatile long prefetchLiveReadRaceNanosMax;
    private volatile long prefetchAsyncValuesRead;
    private volatile long prefetchAsyncUsefulValues;
    private volatile long prefetchAdaptiveAdmissionSkips;
    private volatile long prefetchAdaptiveProbeTasks;
    private final java.util.concurrent.atomic.AtomicLong prefetchAdaptiveSkipSequence =
            new java.util.concurrent.atomic.AtomicLong();
    private volatile long prefetchUnusedStagedOnClose;
    private volatile long prefetchBuildFailures;
    private volatile long prefetchWorkerFailures;
    private static final java.util.concurrent.atomic.AtomicBoolean
            FIRST_PREFETCH_WORKER_FAILURE_LOGGED = new java.util.concurrent.atomic.AtomicBoolean();
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
    // Mailbox-thread-confined adaptive point-probe state and audit counters. Async prefetch tasks
    // never read or update these fields.
    private NativePointAdaptiveMode nativePointAdaptiveMode = NativePointAdaptiveMode.ACTIVE;
    private long nativePointAdaptiveWindowProbeCount;
    private long nativePointAdaptiveWindowPositiveHits;
    private long nativePointAdaptiveWindowNegativeHits;
    private int nativePointAdaptiveConsecutiveZeroWindows;
    private long nativePointAdaptiveBypassProgress;
    private long nativePointAdaptiveEligibleLookups;
    private long nativePointAdaptiveEvaluatedWindows;
    private long nativePointAdaptiveZeroHitWindows;
    private long nativePointAdaptiveActiveToBypassTransitions;
    private long nativePointAdaptiveTargetedRecoveryTransitions;
    private long nativePointAdaptiveBypassedProbes;
    private long nativePointAdaptiveBypassedFills;
    private long nativePointAdaptiveTrialProbes;
    private long nativePointAdaptiveTrialPositiveHits;
    private long nativePointAdaptiveTrialNegativeHits;
    private long nativePointAdaptiveTrialMisses;
    private long nativePointAdaptiveTrialSlotDeferrals;
    private long nativePointAdaptiveSamplesRecorded;
    private long nativePointAdaptiveSampleReplacements;
    private long nativePointAdaptiveTargetedProbes;
    private long nativePointAdaptiveTargetedPositiveHits;
    private long nativePointAdaptiveTargetedNegativeHits;
    private long nativePointAdaptiveTargetedMisses;
    private long nativePointAdaptiveTargetedSlotDeferrals;
    private volatile long nativeMailboxCompactBatches;
    private volatile long nativeMailboxCompactScratchBatches;
    private volatile long nativeMailboxCompactInputKeys;
    private volatile long nativeMailboxCompactUniqueKeys;
    private volatile long nativeMailboxCompactFallbacks;
    private volatile long nativeMailboxCompactThresholdFallbacks;
    private volatile long nativeMailboxCompactSlotMissFallbacks;
    private volatile long nativeMailboxCompactCapacityFallbacks;
    private volatile long nativeMailboxCompactOperationFallbacks;
    private volatile long nativeCompactSelectedProbeBatches;
    private volatile long nativeCompactSelectedProbeKeys;
    private volatile long nativeCompactSelectedLazyHeapKeyCopies;
    private volatile long nativeCompactPostCompactBytesRecopied;
    private volatile long nativeDeferredReservationInputKeys;
    private volatile long nativeDeferredReservationObjectsMaterialized;
    private volatile long nativeDeferredReservationObjectsAvoided;
    private volatile long nativeDirectArenaMultiGetBatches;
    private volatile long nativeDirectArenaMultiGetKeys;
    private volatile long nativeDirectArenaMultiGetCompletedBatches;
    private volatile long nativeDirectArenaMultiGetCompletedKeys;
    private volatile long nativeDirectArenaMultiGetValueBytesCopied;
    private volatile long nativeDirectArenaEagerMaterializedValues;
    private volatile long nativeDirectArenaEagerMaterializedValueBytes;
    private volatile long nativeDirectArenaEagerMissingValues;
    private volatile long nativeDirectArenaEagerFallbackValues;
    private volatile long nativeDirectArenaMultiGetFound;
    private volatile long nativeDirectArenaMultiGetNotFound;
    private volatile long nativeDirectArenaMultiGetOverflowStatuses;
    private volatile long nativeDirectArenaMultiGetBatch1;
    private volatile long nativeDirectArenaMultiGetBatch2To3;
    private volatile long nativeDirectArenaMultiGetBatch4To7;
    private volatile long nativeDirectArenaMultiGetBatch8To15;
    private volatile long nativeDirectArenaMultiGetBatch16To31;
    private volatile long nativeDirectArenaMultiGetBatch32To63;
    private volatile long nativeDirectArenaMultiGetBatch64;
    private volatile long nativeDirectArenaMultiGetBatch65To127;
    private volatile long nativeDirectArenaMultiGetBatch128;
    private volatile long nativeDirectArenaMultiGetOverflows;
    private volatile long nativeDirectArenaMultiGetFallbackBatches;
    private volatile long nativeDirectArenaMultiGetFallbackKeys;
    private volatile long nativeDirectArenaMultiGetThresholdFallbacks;
    private volatile long nativeDirectArenaMultiGetCapabilityFallbacks;
    private volatile long nativeDirectArenaMultiGetLinkageFallbacks;
    private volatile long nativeDirectArenaMultiGetProtocolFallbacks;
    private volatile long nativeDirectArenaMultiGetHeapKeyCopies;
    private volatile boolean nativeDirectArenaMultiGetDisabled;
    private volatile long nativeDirectArenaReadOnlyBatches;
    private volatile long nativeDirectArenaReadOnlyKeys;
    private volatile long nativeDirectArenaReadOnlyCancelledKeys;
    private volatile long nativeDirectArenaSpeculativePreCompactDrops;
    private volatile long nativeDirectArenaSpeculativePreCompactKeys;
    private volatile long nativeDirectArenaSpeculativePostCompactDrops;
    private volatile long nativeDirectArenaSpeculativePostCompactKeys;
    private volatile long nativeDirectArenaSpeculativeTailDrops;
    private volatile long nativeDirectArenaSpeculativeTailKeys;
    private volatile long nativeDirectArenaNegativeHandoffStaged;
    private volatile long nativeDirectArenaNegativeHandoffPromoted;
    private volatile long nativeDirectArenaNegativeHandoffInvalidated;
    private volatile long nativeMailboxDirectSerializationFallbackKeys;
    private volatile long nativeMailboxDirectSerializationFallbackBytes;
    private volatile long nativeDirectPreparedBatches;
    private volatile long nativeDirectPreparedKeys;
    private volatile long nativeDirectPreparedFallbacks;
    private volatile long nativeDirectPreparedThresholdFallbacks;
    private volatile long nativeRuntimeFailures;
    private volatile long nativeGenerationAdvances;
    private volatile long nativeMutationAttempts;
    private volatile long nativeMutationWriteThroughSkipped;
    private volatile long nativeMutationReadInactiveSkipped;
    private volatile long nativeMutationResidentMissSkipped;
    private volatile long nativeValueReadActivations;
    private volatile long nativeMutationApplied;
    private volatile long nativeMutationSuperseded;
    private volatile long nativeMutationFailures;
    private volatile long nativeMutationTombstonesApplied;
    private volatile long nativeMutationBatchScopes;
    private volatile long nativeMutationBatchScopeKeys;
    private volatile long nativeMutationBatchFlushes;
    private volatile long nativeMutationBatchFlushKeys;
    private volatile long nativeMutationBatchCoalesced;
    private volatile long nativeMutationResidentHintChecks;
    private volatile long nativeMutationResidentHintPositives;
    private volatile long nativeMutationResidentHintNegatives;
    private volatile long nativeMutationFenceOnlyFlushes;
    private volatile long nativeMutationAdaptiveBatchObservedScopes;
    private volatile long nativeMutationAdaptiveBatchBypassedScopes;
    private volatile long nativeMutationAdaptiveBatchAttempts;
    private long nativeMutationAdaptiveBatchStartAttempts;
    private boolean nativeResidentMutationBatchActive;
    private long nativeResidentMutationBatchEpoch;
    private byte[] nativeResidentMutationBatchFenceKey;
    private final java.util.HashSet<KeyNamespaceKey<K, N>> nativeResidentBatchDirtyKeys =
            new java.util.HashSet<>();
    private final java.util.LinkedHashMap<KeyNamespaceKey<K, N>, PendingNativeMutation<V>>
            pendingNativeResidentMutations = new java.util.LinkedHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong nativeWriteEpoch =
            new java.util.concurrent.atomic.AtomicLong();
    private AdaptiveNativeProbeController adaptiveNativeProbeController;
    private AdaptiveNativeMailboxDensityController adaptiveNativeMailboxDensityController;
    private boolean adaptiveNativeMailboxDropSpeculativePrefetchEnabled;
    private final PromotionYieldAdmissionController promotionYieldAdmissionController;

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

    // Sticky mailbox-thread-confined signal for speculative record lookahead. A state becomes
    // eligible after its first authoritative value() access and remains eligible for the job.
    // Skipping before that point is always safe because the normal point-read path is unchanged.
    private boolean recordPrefetchAccessObserved;
    private long recordPrefetchAccessGuidedSkips;

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

    private static boolean loadBooleanConfig(String key, boolean defaultValue) {
        try {
            return org.apache.flink.configuration.GlobalConfiguration.loadConfiguration()
                    .get(
                            org.apache.flink.configuration.ConfigOptions.key(key)
                                    .booleanType()
                                    .defaultValue(defaultValue));
        } catch (Throwable ignored) {
            return defaultValue;
        }
    }

    private static int loadIntConfig(
            String key, int defaultValue, int minimum, int maximum) {
        try {
            int configured =
                    org.apache.flink.configuration.GlobalConfiguration.loadConfiguration()
                            .get(
                                    org.apache.flink.configuration.ConfigOptions.key(key)
                                            .intType()
                                            .defaultValue(defaultValue));
            return Math.max(minimum, Math.min(maximum, configured));
        } catch (Throwable ignored) {
            return defaultValue;
        }
    }

    private static double loadDoubleConfig(
            String key, double defaultValue, double minimum, double maximum) {
        try {
            double configured =
                    org.apache.flink.configuration.GlobalConfiguration.loadConfiguration()
                            .get(
                                    org.apache.flink.configuration.ConfigOptions.key(key)
                                            .doubleType()
                                            .defaultValue(defaultValue));
            return Math.max(minimum, Math.min(maximum, configured));
        } catch (Throwable ignored) {
            return defaultValue;
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
                stickyUpdateInPlaceEnabled,
                lazyStagingEnabled,
                false,
                nativeRequestPlaneCoordinator,
                nativeStateId);
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
            boolean keyScopedPrefetchInvalidationEnabled,
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
                keyScopedPrefetchInvalidationEnabled,
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
                false,
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
            boolean keyScopedPrefetchInvalidationEnabled) {
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
                keyScopedPrefetchInvalidationEnabled,
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
            boolean keyScopedPrefetchInvalidationEnabled,
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
        this.nativeValueReadActivation =
                nativeRequestPlaneCoordinator != null
                                && nativeRequestPlaneCoordinator
                                        .options()
                                        .readActivatedWriteThrough()
                        ? nativeRequestPlaneCoordinator.valueReadActivation(nativeStateId)
                        : null;
        this.nativePointAdaptiveBypassEnabled =
                nativeRequestPlaneCoordinator != null
                        && nativeRequestPlaneCoordinator
                                .options()
                                .valuePointAdaptiveBypassEnabled();
        this.nativePointAdaptiveWindowProbes =
                nativeRequestPlaneCoordinator == null
                        ? 4096
                        : nativeRequestPlaneCoordinator.options().valuePointAdaptiveWindowProbes();
        this.nativePointAdaptiveZeroWindows =
                nativeRequestPlaneCoordinator == null
                        ? 2
                        : nativeRequestPlaneCoordinator.options().valuePointAdaptiveZeroWindows();
        this.nativePointAdaptiveResampleIntervalProbes =
                nativeRequestPlaneCoordinator == null
                        ? 4096
                        : nativeRequestPlaneCoordinator
                                .options()
                                .valuePointAdaptiveResampleIntervalProbes();
        this.nativePointAdaptiveSampleSlots =
                nativeRequestPlaneCoordinator == null
                        ? 64
                        : nativeRequestPlaneCoordinator
                                .options()
                                .valuePointAdaptiveSampleSlots();
        this.nativePointAdaptiveFingerprintSamples =
                nativePointAdaptiveBypassEnabled
                        ? new long[nativePointAdaptiveSampleSlots]
                        : null;
        this.adaptiveNativeProbeController =
                NATIVE_ADAPTIVE_PROBE_BYPASS_ENABLED
                                && nativeRequestPlaneCoordinator != null
                                && nativeRequestPlaneCoordinator
                                        .options()
                                        .compactSelectedProbeEnabled()
                        ? new AdaptiveNativeProbeController(
                                NATIVE_ADAPTIVE_PROBE_WINDOW_KEYS,
                                NATIVE_ADAPTIVE_PROBE_WINDOW_BATCHES,
                                NATIVE_ADAPTIVE_PROBE_ZERO_WINDOWS,
                                NATIVE_ADAPTIVE_PROBE_COOLDOWN_BATCHES,
                                NATIVE_ADAPTIVE_PROBE_RECOVERY_MIN_USEFUL,
                                NATIVE_ADAPTIVE_PROBE_RECOVERY_USEFUL_RATE)
                        : null;
        this.adaptiveNativeMailboxDensityController =
                NATIVE_MAILBOX_ADAPTIVE_DENSITY_ENABLED
                                && nativeRequestPlaneCoordinator != null
                                && nativeRequestPlaneCoordinator.options().mailboxBatchEnabled()
                        ? new AdaptiveNativeMailboxDensityController(
                                NATIVE_MAILBOX_ADAPTIVE_DENSITY_WINDOW_INPUT_KEYS,
                                NATIVE_MAILBOX_ADAPTIVE_DENSITY_WINDOW_BATCHES,
                                NATIVE_MAILBOX_ADAPTIVE_DENSITY_LOW_WINDOWS,
                                NATIVE_MAILBOX_ADAPTIVE_DENSITY_COOLDOWN_BATCHES,
                                NATIVE_MAILBOX_ADAPTIVE_DENSITY_MIN_UNIQUE_RATE,
                                NATIVE_MAILBOX_ADAPTIVE_DENSITY_RECOVERY_UNIQUE_RATE)
                        : null;
        this.adaptiveNativeMailboxDropSpeculativePrefetchEnabled =
                NATIVE_MAILBOX_ADAPTIVE_DENSITY_DROP_SPECULATIVE_PREFETCH_ENABLED;
        this.promotionYieldAdmissionController =
                PROMOTION_YIELD_ADMISSION_ENABLED
                                && nativeRequestPlaneCoordinator != null
                                && nativeRequestPlaneCoordinator.isActive()
                                && nativeRequestPlaneCoordinator.options().prefetchEnabled()
                                && nativeRequestPlaneCoordinator.options().mailboxBatchEnabled()
                        ? new PromotionYieldAdmissionController(
                                PROMOTION_YIELD_MIN_STAGED_VALUES,
                                PROMOTION_YIELD_MIN_PROMOTION_RATE,
                                PROMOTION_YIELD_PROBE_EVERY_TASKS)
                        : null;
        // The narrow invalidation proof relies on the prepared-key reservation identity checked
        // before and after RocksDB I/O. Native direct-read-only uses that exact same reservation
        // on both sides of its authoritative RocksDB MultiGet, so it can safely share the proof.
        // Native point-cache modes and generic query-wire prefetch retain the conservative
        // state-wide generation barrier.
        boolean nativeDirectReadOnly =
                nativeRequestPlaneCoordinator != null
                        && nativeRequestPlaneCoordinator.options().directArenaReadOnlyEnabled();
        this.keyScopedPrefetchInvalidationEnabled =
                keyScopedPrefetchInvalidationEnabled
                        && this.multiGetPrefetchEnabled
                        && delegate instanceof RocksDBBatchValueReader<?, ?, ?>
                        && (nativeRequestPlaneCoordinator == null || nativeDirectReadOnly);

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
        recordPrefetchAccessObserved = true;
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
        PrefetchReservation reservation = inFlight.get(lookupKey);
        if (reservation != null && reservation.generation == writeGen) {
            // The mailbox reached this key before the worker published its speculative result.
            // The authoritative read below remains correct, but this is duplicate I/O and direct
            // evidence that the attempted overlap was too short for this key.
            prefetchLiveReadRacedInFlight++;
            long raceNanos = Math.max(0L, System.nanoTime() - reservation.submittedNanos);
            prefetchLiveReadRaceNanos += raceNanos;
            prefetchLiveReadRaceNanosMax =
                    Math.max(prefetchLiveReadRaceNanosMax, raceNanos);
            boolean cancelled;
            synchronized (staging) {
                cancelled = inFlight.remove(lookupKey, reservation);
            }
            if (cancelled) {
                // The mailbox is now the authoritative consumer for this key. Revoking the
                // generation token lets the worker filter it before I/O or discard it before
                // staging, without interrupting unrelated keys in the same chunk.
                prefetchLiveReadCancellations++;
            }
        }
        if (!staging.isEmpty()) {
            Object stagedEntry = removeStagedEntry(lookupKey);
            if (isNativeNegativeStagedEntry(stagedEntry)) {
                KeyNamespaceKey<K, N> negativeKey = nativeNegativeStorageKey(stagedEntry);
                V stagedValue;
                try {
                    stagedValue = copyBatchDefaultValueForMailbox();
                } catch (RuntimeException ignored) {
                    prefetchLazyMaterializationFailures++;
                    stagedValue = null;
                    negativeKey = null;
                }
                if (negativeKey != null) {
                    CachedValue<V> newValue = CachedValue.of(negativeKey, stagedValue, false);
                    l1Cache.put(negativeKey, newValue);
                    updateSticky(negativeKey, newValue);
                    recordAccess(true);
                    prefetchValuesPromoted++;
                    nativeDirectArenaNegativeHandoffPromoted++;
                    return newValue.valueOrNull();
                }
            } else if (stagedEntry instanceof StagedValue<?>) {
                @SuppressWarnings("unchecked")
                StagedValue<V> staged = (StagedValue<V>) stagedEntry;
                if (staged.gen != writeGen) {
                    prefetchStaleAborts++;
                    staged = null;
                }
                if (staged == null) {
                    // Continue to the authoritative delegate read below.
                } else {
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
                        // Cache insertion and eviction are part of the authoritative write-back
                        // path. Never swallow their failures as if speculative materialization had
                        // failed.
                        KeyNamespaceKey<K, N> storageKey = staged.storageKey();
                        CachedValue<V> newValue = CachedValue.of(storageKey, stagedValue, false);
                        l1Cache.put(storageKey, newValue);
                        updateSticky(storageKey, newValue);
                        recordAccess(true); // Hit
                        prefetchValuesPromoted++;
                        return newValue.valueOrNull();
                    }
                }
            }
        }

        // 4c. Independently gated native ValueState point cache. This is deliberately separate
        // from native Prefetch/MultiGet so Mailbox, Prefetch, and PreAgg treatments can be
        // measured without implicitly enabling VCache.
        NativePointCacheResult<V> nativePoint =
                probeNativeValueCache(currentKey, currentNamespace);
        if (nativePoint.isHit()) {
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
        if (nativePoint.shouldFillAfterDelegateRead()) {
            fillNativeValueCache(currentKey, currentNamespace, loaded);
        } else if (nativePoint.adaptiveFillSuppressed()) {
            nativePointAdaptiveBypassedFills++;
        }

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
            return NativePointCacheResult.delegateAndFill();
        }
        if (nativeResidentMutationBatchActive) {
            setLookupKey(key, namespace);
            if (nativeResidentBatchDirtyKeys.contains(lookupKey)) {
                return NativePointCacheResult.delegateAndFill();
            }
        }
        // A fallback delegate read can still fill the native cache. Activate before acquiring a
        // bounded slot so temporary slot exhaustion cannot create an entry while mutation
        // write-through remains dormant.
        activateNativeValueRead();
        int adaptiveFingerprint =
                nativePointAdaptiveBypassEnabled
                                && nativePointAdaptiveMode == NativePointAdaptiveMode.BYPASS
                        ? CacheKeyHash.hash(key, namespace)
                        : 0;
        NativePointProbePermit adaptivePermit =
                selectNativePointProbePermit(adaptiveFingerprint);
        if (adaptivePermit == NativePointProbePermit.SKIP) {
            return NativePointCacheResult.adaptiveBypass();
        }
        NativeRequestPlaneCoordinator.BatchSlot slot =
                nativeRequestPlaneCoordinator.tryAcquireBatchSlot();
        if (slot == null) {
            nativeFallbackBatches++;
            if (adaptivePermit == NativePointProbePermit.TRIAL) {
                nativePointAdaptiveTrialSlotDeferrals++;
                return NativePointCacheResult.adaptiveBypass();
            }
            if (adaptivePermit == NativePointProbePermit.TARGETED) {
                nativePointAdaptiveTargetedSlotDeferrals++;
                return NativePointCacheResult.adaptiveBypass();
            }
            return NativePointCacheResult.delegateAndFill();
        }
        try (NativeRequestPlaneCoordinator.BatchSlot ignored = slot) {
            RocksDBBatchValueReader<K, N, V> batchReader =
                    (RocksDBBatchValueReader<K, N, V>) delegate;
            slot.prepareLatest(
                    nativeStateId,
                    nativeWriteEpoch.get(),
                    output -> serializeNativeKey(batchReader, key, namespace, output));
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
                recordNativePointAdaptiveProbe(status, adaptivePermit, adaptiveFingerprint);
                return NativePointCacheResult.hit(value);
            }
            if (status == NativeRequestPlaneBridge.PROBE_NEGATIVE) {
                V defaultValue = batchReader.getBatchDefaultValue();
                if (immediateValueSerializer == null) {
                    immediateValueSerializer = delegate.getValueSerializer().duplicate();
                }
                V value = deserializeImmediateValueOrCopyDefault(null, defaultValue);
                nativeNegativeHits++;
                recordNativePointAdaptiveProbe(status, adaptivePermit, adaptiveFingerprint);
                return NativePointCacheResult.hit(value);
            }
            if (status == NativeRequestPlaneBridge.PROBE_MISS) {
                nativeMisses++;
                boolean suppressFill =
                        recordNativePointAdaptiveProbe(
                                status, adaptivePermit, adaptiveFingerprint);
                return suppressFill
                        ? NativePointCacheResult.adaptiveBypass()
                        : NativePointCacheResult.delegateAndFill();
            }
            throw new IllegalStateException(
                    "Native ValueState point probe returned status=" + status + ".");
        } catch (Exception | LinkageError failure) {
            nativeRuntimeFailures++;
            nativeFallbackBatches++;
            nativeRequestPlaneCoordinator.disable(failure);
            return adaptivePermit == NativePointProbePermit.TRIAL
                            || adaptivePermit == NativePointProbePermit.TARGETED
                    ? NativePointCacheResult.adaptiveBypass()
                    : NativePointCacheResult.delegateAndFill();
        }
    }

    private NativePointProbePermit selectNativePointProbePermit(int fingerprint) {
        if (!nativePointAdaptiveBypassEnabled) {
            return NativePointProbePermit.NORMAL;
        }
        nativePointAdaptiveEligibleLookups++;
        if (nativePointAdaptiveMode == NativePointAdaptiveMode.ACTIVE) {
            return NativePointProbePermit.NORMAL;
        }
        if (consumeNativePointAdaptiveFingerprintSample(fingerprint)) {
            // A targeted attempt also restarts the periodic backoff. This prevents an immediately
            // adjacent periodic trial when a sampled key happens to return at the interval edge.
            nativePointAdaptiveBypassProgress = 0L;
            return NativePointProbePermit.TARGETED;
        }
        if (nativePointAdaptiveBypassProgress < nativePointAdaptiveResampleIntervalProbes) {
            nativePointAdaptiveBypassProgress++;
        }
        if (nativePointAdaptiveBypassProgress < nativePointAdaptiveResampleIntervalProbes) {
            nativePointAdaptiveBypassedProbes++;
            return NativePointProbePermit.SKIP;
        }
        // Consume the periodic attempt before acquiring a slot. A slot deferral therefore starts
        // a complete new backoff interval instead of retrying on every following lookup.
        nativePointAdaptiveBypassProgress = 0L;
        return NativePointProbePermit.TRIAL;
    }

    private boolean recordNativePointAdaptiveProbe(
            int status, NativePointProbePermit adaptivePermit, int fingerprint) {
        if (!nativePointAdaptiveBypassEnabled) {
            return false;
        }
        if (adaptivePermit == NativePointProbePermit.TRIAL) {
            nativePointAdaptiveTrialProbes++;
            if (status == NativeRequestPlaneBridge.PROBE_HIT) {
                nativePointAdaptiveTrialPositiveHits++;
                recoverNativePointAdaptiveProbe(false);
            } else if (status == NativeRequestPlaneBridge.PROBE_NEGATIVE) {
                nativePointAdaptiveTrialNegativeHits++;
                recoverNativePointAdaptiveProbe(false);
            } else {
                nativePointAdaptiveTrialMisses++;
                recordNativePointAdaptiveFingerprintSample(fingerprint);
            }
            return false;
        }
        if (adaptivePermit == NativePointProbePermit.TARGETED) {
            nativePointAdaptiveTargetedProbes++;
            if (status == NativeRequestPlaneBridge.PROBE_HIT) {
                nativePointAdaptiveTargetedPositiveHits++;
                recoverNativePointAdaptiveProbe(true);
                return false;
            }
            if (status == NativeRequestPlaneBridge.PROBE_NEGATIVE) {
                nativePointAdaptiveTargetedNegativeHits++;
                recoverNativePointAdaptiveProbe(true);
                return false;
            }
            nativePointAdaptiveTargetedMisses++;
            return true;
        }

        nativePointAdaptiveWindowProbeCount++;
        if (status == NativeRequestPlaneBridge.PROBE_HIT) {
            nativePointAdaptiveWindowPositiveHits++;
        } else if (status == NativeRequestPlaneBridge.PROBE_NEGATIVE) {
            nativePointAdaptiveWindowNegativeHits++;
        }
        if (nativePointAdaptiveWindowProbeCount < nativePointAdaptiveWindowProbes) {
            return false;
        }

        nativePointAdaptiveEvaluatedWindows++;
        boolean zeroUsefulHits =
                nativePointAdaptiveWindowPositiveHits == 0L
                        && nativePointAdaptiveWindowNegativeHits == 0L;
        if (zeroUsefulHits) {
            nativePointAdaptiveZeroHitWindows++;
            nativePointAdaptiveConsecutiveZeroWindows++;
        } else {
            nativePointAdaptiveConsecutiveZeroWindows = 0;
        }
        resetNativePointAdaptiveWindow();
        if (zeroUsefulHits
                && nativePointAdaptiveConsecutiveZeroWindows >= nativePointAdaptiveZeroWindows) {
            nativePointAdaptiveMode = NativePointAdaptiveMode.BYPASS;
            nativePointAdaptiveBypassProgress = 0L;
            clearNativePointAdaptiveFingerprintSamples();
            nativePointAdaptiveActiveToBypassTransitions++;
            return true;
        }
        return false;
    }

    private void recoverNativePointAdaptiveProbe(boolean targeted) {
        nativePointAdaptiveMode = NativePointAdaptiveMode.ACTIVE;
        nativePointAdaptiveConsecutiveZeroWindows = 0;
        resetNativePointAdaptiveWindow();
        nativePointAdaptiveBypassProgress = 0L;
        clearNativePointAdaptiveFingerprintSamples();
        if (targeted) {
            nativePointAdaptiveTargetedRecoveryTransitions++;
        }
    }

    private void recordNativePointAdaptiveFingerprintSample(int fingerprint) {
        long encoded = encodeNativePointAdaptiveFingerprint(fingerprint);
        int slot = nativePointAdaptiveFingerprintSlot(fingerprint);
        long previous = nativePointAdaptiveFingerprintSamples[slot];
        if (previous != 0L && previous != encoded) {
            nativePointAdaptiveSampleReplacements++;
        }
        nativePointAdaptiveFingerprintSamples[slot] = encoded;
        nativePointAdaptiveSamplesRecorded++;
    }

    private boolean consumeNativePointAdaptiveFingerprintSample(int fingerprint) {
        int slot = nativePointAdaptiveFingerprintSlot(fingerprint);
        long encoded = encodeNativePointAdaptiveFingerprint(fingerprint);
        if (nativePointAdaptiveFingerprintSamples[slot] != encoded) {
            return false;
        }
        // Consume before acquiring a native slot. A slot deferral or miss cannot hot-retry this
        // sampled fingerprint on the next lookup.
        nativePointAdaptiveFingerprintSamples[slot] = 0L;
        return true;
    }

    private int nativePointAdaptiveFingerprintSlot(int fingerprint) {
        int spread = fingerprint ^ (fingerprint >>> 16);
        return spread & (nativePointAdaptiveSampleSlots - 1);
    }

    private static long encodeNativePointAdaptiveFingerprint(int fingerprint) {
        return (fingerprint & 0xffff_ffffL) | (1L << 32);
    }

    private void clearNativePointAdaptiveFingerprintSamples() {
        java.util.Arrays.fill(nativePointAdaptiveFingerprintSamples, 0L);
    }

    private void resetNativePointAdaptiveWindow() {
        nativePointAdaptiveWindowProbeCount = 0L;
        nativePointAdaptiveWindowPositiveHits = 0L;
        nativePointAdaptiveWindowNegativeHits = 0L;
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
        // Publication through this method always follows an authoritative read. Keep activation
        // at the publication boundary as a defensive invariant if the caller is refactored.
        activateNativeValueRead();
        try {
            RocksDBBatchValueReader<K, N, V> batchReader =
                    (RocksDBBatchValueReader<K, N, V>) delegate;
            int status =
                    nativeRequestPlaneCoordinator.updateExactKey(
                            nativeStateId,
                            nativeWriteEpoch.get(),
                            output -> serializeNativeKey(batchReader, key, namespace, output),
                            loaded == null
                                    ? null
                                    : output ->
                                            nativeMutationValueSerializer.serialize(
                                                    loaded, output));
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
                        prepareDelegateWrite(currentKey, currentNamespace);
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
                    prepareDelegateWrite(currentKey, currentNamespace);
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
                    prepareDelegateWrite(currentKey, currentNamespace);
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
     * Returns whether this wrapper should receive the next record-lookahead broadcast.
     *
     * <p>When access guidance is enabled, a wrapper is activated by its first real mailbox read.
     * Until then prefetch is skipped and the unmodified authoritative read learns the state on
     * demand. The signal is deliberately sticky: later phase changes cannot lose correctness or
     * permanently starve a state that has once proved relevant.
     */
    public boolean shouldReceiveRecordKeyPrefetch(boolean accessGuided) {
        if (isRecordKeyPrefetchEligible(accessGuided)) {
            return promotionYieldAdmissionController == null
                    || promotionYieldAdmissionController.shouldAdmit(
                            prefetchValuesStaged, prefetchValuesPromoted);
        }
        if (supportsRecordKeyPrefetch() && accessGuided) {
            recordPrefetchAccessGuidedSkips++;
        }
        return false;
    }

    /** Non-counting capability probe used before the streaming runtime extracts a key batch. */
    public boolean isRecordKeyPrefetchEligible(boolean accessGuided) {
        return supportsRecordKeyPrefetch() && (!accessGuided || recordPrefetchAccessObserved);
    }

    /**
     * Whether mailbox record keys can identify exact prepared-MultiGet reservations.
     *
     * <p>The generic serialized query-wire path is intentionally excluded: it does not expose the
     * same prepared-key ownership contract. Non-void namespaces are also excluded because the
     * future record key alone cannot identify an entry.
     */
    public boolean supportsDispatchPrefetchCancellation() {
        return supportsRecordKeyPrefetch()
                && multiGetPrefetchEnabled
                && delegate instanceof RocksDBBatchValueReader<?, ?, ?>;
    }

    /** Cheap mailbox-side gate that avoids scanning keys for wrappers with no cancellable work. */
    public boolean hasInFlightDispatchPrefetchReservations() {
        return !closed && supportsDispatchPrefetchCancellation() && !inFlight.isEmpty();
    }

    /**
     * Revoke exact still-in-flight reservations for selected mailbox records.
     *
     * <p>Cancellation is serialized with worker publication. If publication won the race, its
     * staging value remains available. If cancellation won, the worker's pre-read or pre-publish
     * identity check rejects the old task. The worker itself is never interrupted.
     */
    public int cancelPrefetchForDispatch(Iterable<? extends K> keys) {
        if (closed
                || keys == null
                || currentNamespace == null
                || !supportsDispatchPrefetchCancellation()) {
            return 0;
        }
        final N namespace = currentNamespace;
        int cancelled = 0;
        for (K key : keys) {
            if (key == null) {
                continue;
            }
            prefetchDispatchKeysExamined++;
            setLookupKey(key, namespace);
            PrefetchReservation reservation = inFlight.get(lookupKey);
            if (reservation == null) {
                prefetchDispatchNoReservation++;
                continue;
            }
            synchronized (staging) {
                // A completed speculative read is useful to the record now being dispatched.
                // Never turn a staging hit back into an authoritative RocksDB point read.
                if (staging.containsKey(lookupKey)) {
                    prefetchDispatchAlreadyStaged++;
                    continue;
                }
                if (inFlight.remove(lookupKey, reservation)) {
                    prefetchDispatchCancellations++;
                    cancelled++;
                } else {
                    prefetchDispatchNoReservation++;
                }
            }
        }
        return cancelled;
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
        // Publish cancellation before waiting for the write barrier. A worker processes a large
        // lookahead as several short read-locked chunks; setting this only after acquiring the
        // write lock lets that worker repeatedly reacquire the read lock and can starve task
        // cancellation. Volatile publication makes it stop before its next chunk, while the write
        // lock below still drains the one native delegate access that may already be in flight.
        closed = true;
        cancelQueuedAndAwaitPrefetchTasks();
        lifecycleLock.writeLock().lock();
        try {
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
                            + "keyScopedInvalidation={} accessObserved={} accessGuidedSkips={} "
                            + "tasksBuilt={} tasksExecuted={} tasksDropped={} keysPrepared={} "
                            + "keysDeduplicated={} multiGetCalls={} "
                            + "multiGetKeys={} pointGetCalls={} immediatePointGetCalls={} "
                            + "smallBatchDrops={} "
                            + "smallBatchKeysDropped={} staged={} missingStaged={} "
                            + "promoted={} lazyStaging={} lazyStaged={} lazyMaterialized={} "
                            + "lazyMaterializationFailures={} stagingEntries={} retainedBytes={} "
                            + "maxRetainedBytes={} admissionDrops={} staleAborts={} "
                            + "keyScopedInvalidations={} keyScopedFastNegativeSkips={} "
                            + "keyScopedInFlightCancelled={} "
                            + "keyScopedStagedRemoved={} "
                            + "liveReadRacedInFlight={} liveReadCancellations={} "
                            + "dispatchKeysExamined={} dispatchCancellations={} "
                            + "dispatchAlreadyStaged={} dispatchNoReservation={} "
                            + "workerCancelledBeforeRead={} workerDiscardedAfterRead={} "
                            + "workerQueueAvgUs={} workerQueueMaxUs={} "
                            + "workerRunAvgUs={} workerRunMaxUs={} "
                            + "liveReadRaceAvgUs={} liveReadRaceMaxUs={} "
                            + "asyncValuesRead={} asyncUsefulValues={} "
                            + "adaptiveAdmissionSkips={} adaptiveProbeTasks={} "
                            + "promotionYieldAdmissionActive={} "
                            + "promotionYieldAdmissionSkips={} promotionYieldProbeTasks={} "
                            + "unusedStagedOnClose={} "
                            + "buildFailures={} workerFailures={} stickyUpdateInPlace={} "
                            + "stickySameKeyAttempts={} stickyInPlaceReuses={} "
                            + "nativeEnabled={} nativeStateId={} nativeActivated={} "
                            + "nativeProbeKeys={} nativeHits={} nativeHitBytesCopied={} "
                            + "nativeHitBytesDirect={} "
                            + "nativeNegativeHits={} "
                            + "nativeMisses={} nativeFillBatches={} nativeFillKeys={} "
                            + "nativeFillRejected={} nativeFallbackBatches={} "
                            + "nativeMailboxCompactBatches={} "
                            + "nativeMailboxCompactScratchBatches={} "
                            + "nativeMailboxCompactInputKeys={} "
                            + "nativeMailboxCompactUniqueKeys={} "
                            + "nativeMailboxCompactFallbacks={} "
                            + "nativeMailboxCompactThresholdFallbacks={} "
                            + "nativeMailboxCompactSlotMissFallbacks={} "
                            + "nativeMailboxCompactCapacityFallbacks={} "
                            + "nativeMailboxCompactOperationFallbacks={} "
                            + "nativeCompactSelectedProbeBatches={} "
                            + "nativeCompactSelectedProbeKeys={} "
                            + "nativeCompactSelectedLazyHeapKeyCopies={} "
                            + "nativeCompactPostCompactBytesRecopied={} "
                            + "nativeDeferredReservationInputKeys={} "
                            + "nativeDeferredReservationObjectsMaterialized={} "
                            + "nativeDeferredReservationObjectsAvoided={} "
                            + "nativeMailboxDirectSerializationFallbackKeys={} "
                            + "nativeMailboxDirectSerializationFallbackBytes={} "
                            + "nativeDirectPreparedBatches={} "
                            + "nativeDirectPreparedKeys={} "
                            + "nativeDirectPreparedFallbacks={} "
                            + "nativeDirectPreparedThresholdFallbacks={} "
                            + "nativeRuntimeFailures={} nativeGenerationAdvances={} "
                            + "nativeMutationAttempts={} nativeMutationApplied={} "
                            + "nativeMutationWriteThroughSkipped={} "
                            + "nativeMutationReadInactiveSkipped={} "
                            + "nativeMutationResidentMissSkipped={} nativeValueReadActivations={} "
                            + "nativeMutationSuperseded={} nativeMutationFailures={} "
                            + "nativeMutationTombstonesApplied={} "
                            + "nativeMutationBatchScopes={} nativeMutationBatchScopeKeys={} "
                            + "nativeMutationBatchFlushes={} nativeMutationBatchFlushKeys={} "
                            + "nativeMutationBatchCoalesced={} "
                            + "nativeMutationResidentHintChecks={} "
                            + "nativeMutationResidentHintPositives={} "
                            + "nativeMutationResidentHintNegatives={} "
                            + "nativeMutationFenceOnlyFlushes={} "
                            + "nativeMutationAdaptiveBatchObservedScopes={} "
                            + "nativeMutationAdaptiveBatchBypassedScopes={} "
                            + "nativeMutationAdaptiveBatchAttempts={} "
                            + "nativeMutationAdaptiveBatchEnabled={} nativeActive={} "
                            + "nativeKernel={} nativeFeatureBits={} nativeFeatures={} "
                            + "nativeDisableCause={} coordinatorProbeCalls={} "
                            + "coordinatorFillCalls={} coordinatorLeaseMisses={}",
                    delegate.getClass().getSimpleName(),
                    namespaceSerializer.getClass().getSimpleName(),
                    supportsRecordKeyPrefetch(),
                    multiGetPrefetchEnabled,
                    multiGetChunkSize,
                    multiGetMinBatchSize,
                    keyScopedPrefetchInvalidationEnabled,
                    recordPrefetchAccessObserved,
                    recordPrefetchAccessGuidedSkips,
                    prefetchTasksBuilt,
                    prefetchTasksExecuted,
                    prefetchTasksDropped,
                    prefetchKeysPrepared,
                    prefetchKeysDeduplicated,
                    prefetchMultiGetCalls,
                    prefetchMultiGetKeys,
                    prefetchPointGetCalls,
                    prefetchImmediatePointGetCalls,
                    prefetchSmallBatchDrops,
                    prefetchSmallBatchKeysDropped,
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
                    prefetchKeyScopedInvalidations,
                    prefetchKeyScopedFastNegativeSkips,
                    prefetchKeyScopedInFlightCancelled,
                    prefetchKeyScopedStagedRemoved,
                    prefetchLiveReadRacedInFlight,
                    prefetchLiveReadCancellations,
                    prefetchDispatchKeysExamined,
                    prefetchDispatchCancellations,
                    prefetchDispatchAlreadyStaged,
                    prefetchDispatchNoReservation,
                    prefetchWorkerCancelledBeforeRead,
                    prefetchWorkerDiscardedAfterRead,
                    nanosAverageMicros(prefetchWorkerQueueNanos, prefetchTasksExecuted),
                    nanosToMicros(prefetchWorkerQueueNanosMax),
                    nanosAverageMicros(prefetchWorkerRunNanos, prefetchTasksExecuted),
                    nanosToMicros(prefetchWorkerRunNanosMax),
                    nanosAverageMicros(
                            prefetchLiveReadRaceNanos, prefetchLiveReadRacedInFlight),
                    nanosToMicros(prefetchLiveReadRaceNanosMax),
                    prefetchAsyncValuesRead,
                    prefetchAsyncUsefulValues,
                    prefetchAdaptiveAdmissionSkips,
                    prefetchAdaptiveProbeTasks,
                    promotionYieldAdmissionController != null,
                    promotionYieldAdmissionController == null
                            ? 0L
                            : promotionYieldAdmissionController.skippedTasks(),
                    promotionYieldAdmissionController == null
                            ? 0L
                            : promotionYieldAdmissionController.probeTasks(),
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
                    nativeMailboxCompactScratchBatches,
                    nativeMailboxCompactInputKeys,
                    nativeMailboxCompactUniqueKeys,
                    nativeMailboxCompactFallbacks,
                    nativeMailboxCompactThresholdFallbacks,
                    nativeMailboxCompactSlotMissFallbacks,
                    nativeMailboxCompactCapacityFallbacks,
                    nativeMailboxCompactOperationFallbacks,
                    nativeCompactSelectedProbeBatches,
                    nativeCompactSelectedProbeKeys,
                    nativeCompactSelectedLazyHeapKeyCopies,
                    nativeCompactPostCompactBytesRecopied,
                    nativeDeferredReservationInputKeys,
                    nativeDeferredReservationObjectsMaterialized,
                    nativeDeferredReservationObjectsAvoided,
                    nativeMailboxDirectSerializationFallbackKeys,
                    nativeMailboxDirectSerializationFallbackBytes,
                    nativeDirectPreparedBatches,
                    nativeDirectPreparedKeys,
                    nativeDirectPreparedFallbacks,
                    nativeDirectPreparedThresholdFallbacks,
                    nativeRuntimeFailures,
                    nativeGenerationAdvances,
                    nativeMutationAttempts,
                    nativeMutationApplied,
                    nativeMutationWriteThroughSkipped,
                    nativeMutationReadInactiveSkipped,
                    nativeMutationResidentMissSkipped,
                    nativeValueReadActivations,
                    nativeMutationSuperseded,
                    nativeMutationFailures,
                    nativeMutationTombstonesApplied,
                    nativeMutationBatchScopes,
                    nativeMutationBatchScopeKeys,
                    nativeMutationBatchFlushes,
                    nativeMutationBatchFlushKeys,
                    nativeMutationBatchCoalesced,
                    nativeMutationResidentHintChecks,
                    nativeMutationResidentHintPositives,
                    nativeMutationResidentHintNegatives,
                    nativeMutationFenceOnlyFlushes,
                    nativeMutationAdaptiveBatchObservedScopes,
                    nativeMutationAdaptiveBatchBypassedScopes,
                    nativeMutationAdaptiveBatchAttempts,
                    NATIVE_RESIDENT_MUTATION_ADAPTIVE_ENABLED,
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
            if (nativePointAdaptiveBypassEnabled
                    || nativePointAdaptiveActiveToBypassTransitions > 0L) {
                LOG.info(
                        "[CACHEKIT VALUE POINT ADAPTIVE] enabled={} mode={} windowProbes={} "
                                + "zeroWindows={} resampleInterval={} sampleSlots={} "
                                + "eligibleLookups={} "
                                + "evaluatedWindows={} zeroHitWindows={} activeToBypassTransitions={} "
                                + "targetedRecoveryTransitions={} bypassedProbes={} bypassedFills={} "
                                + "trialProbes={} trialPositiveHits={} trialNegativeHits={} "
                                + "trialMisses={} trialSlotDeferrals={} samplesRecorded={} "
                                + "sampleReplacements={} targetedProbes={} targetedPositiveHits={} "
                                + "targetedNegativeHits={} targetedMisses={} targetedSlotDeferrals={}",
                        nativePointAdaptiveBypassEnabled,
                        nativePointAdaptiveMode,
                        nativePointAdaptiveWindowProbes,
                        nativePointAdaptiveZeroWindows,
                        nativePointAdaptiveResampleIntervalProbes,
                        nativePointAdaptiveSampleSlots,
                        nativePointAdaptiveEligibleLookups,
                        nativePointAdaptiveEvaluatedWindows,
                        nativePointAdaptiveZeroHitWindows,
                        nativePointAdaptiveActiveToBypassTransitions,
                        nativePointAdaptiveTargetedRecoveryTransitions,
                        nativePointAdaptiveBypassedProbes,
                        nativePointAdaptiveBypassedFills,
                        nativePointAdaptiveTrialProbes,
                        nativePointAdaptiveTrialPositiveHits,
                        nativePointAdaptiveTrialNegativeHits,
                        nativePointAdaptiveTrialMisses,
                        nativePointAdaptiveTrialSlotDeferrals,
                        nativePointAdaptiveSamplesRecorded,
                        nativePointAdaptiveSampleReplacements,
                        nativePointAdaptiveTargetedProbes,
                        nativePointAdaptiveTargetedPositiveHits,
                        nativePointAdaptiveTargetedNegativeHits,
                        nativePointAdaptiveTargetedMisses,
                        nativePointAdaptiveTargetedSlotDeferrals);
            }
            if ((nativeRequestPlaneCoordinator != null
                            && nativeRequestPlaneCoordinator
                                    .options()
                                    .directArenaMultiGetEnabled())
                    || nativeDirectArenaMultiGetBatches > 0
                    || nativeDirectArenaMultiGetFallbackBatches > 0) {
                LOG.info(
                        "[CACHEKIT DIRECT ARENA MULTIGET] configured={} disabled={} "
                                + "readOnlyConfigured={} readOnlyBatches={} readOnlyKeys={} readOnlyCancelledKeys={} "
                                + "speculativePreCompactDrops={} speculativePreCompactKeys={} "
                                + "speculativePostCompactDrops={} speculativePostCompactKeys={} "
                                + "speculativeTailDrops={} speculativeTailKeys={} "
                                + "negativeHandoffConfigured={} negativeHandoffStaged={} "
                                + "negativeHandoffPromoted={} negativeHandoffInvalidated={} "
                                + "eagerMaterializationConfigured={} eagerValues={} eagerValueBytes={} "
                                + "eagerMissingValues={} eagerFallbackValues={} "
                                + "batches={} keys={} completedBatches={} completedKeys={} valueBytesCopied={} "
                                + "configuredBatchSize={} configuredValueStride={} "
                                + "batchHistogram=1:{},2-3:{},4-7:{},8-15:{},16-31:{},32-63:{},64+:{} "
                                + "batch65To127={} batch128={} "
                                + "found={} notFound={} overflowStatuses={} overflowBatches={} fallbackBatches={} "
                                + "fallbackKeys={} thresholdFallbacks={} capabilityFallbacks={} linkageFallbacks={} protocolFallbacks={} "
                                + "heapKeyCopies={}",
                        nativeRequestPlaneCoordinator != null
                                && nativeRequestPlaneCoordinator
                                        .options()
                                        .directArenaMultiGetEnabled(),
                        nativeDirectArenaMultiGetDisabled,
                        nativeRequestPlaneCoordinator != null
                                && nativeRequestPlaneCoordinator
                                        .options()
                                        .directArenaReadOnlyEnabled(),
                        nativeDirectArenaReadOnlyBatches,
                        nativeDirectArenaReadOnlyKeys,
                        nativeDirectArenaReadOnlyCancelledKeys,
                        nativeDirectArenaSpeculativePreCompactDrops,
                        nativeDirectArenaSpeculativePreCompactKeys,
                        nativeDirectArenaSpeculativePostCompactDrops,
                        nativeDirectArenaSpeculativePostCompactKeys,
                        nativeDirectArenaSpeculativeTailDrops,
                        nativeDirectArenaSpeculativeTailKeys,
                        nativeRequestPlaneCoordinator != null
                                && nativeRequestPlaneCoordinator
                                        .options()
                                        .negativeHandoffEnabled(),
                        nativeDirectArenaNegativeHandoffStaged,
                        nativeDirectArenaNegativeHandoffPromoted,
                        nativeDirectArenaNegativeHandoffInvalidated,
                        nativeRequestPlaneCoordinator != null
                                && nativeRequestPlaneCoordinator
                                        .options()
                                        .directArenaEagerMaterializationEnabled(),
                        nativeDirectArenaEagerMaterializedValues,
                        nativeDirectArenaEagerMaterializedValueBytes,
                        nativeDirectArenaEagerMissingValues,
                        nativeDirectArenaEagerFallbackValues,
                        nativeDirectArenaMultiGetBatches,
                        nativeDirectArenaMultiGetKeys,
                        nativeDirectArenaMultiGetCompletedBatches,
                        nativeDirectArenaMultiGetCompletedKeys,
                        nativeDirectArenaMultiGetValueBytesCopied,
                        directArenaChunkSize(),
                        nativeRequestPlaneCoordinator == null
                                ? 0
                                : nativeRequestPlaneCoordinator.options().batchValueArenaBytes()
                                        / RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH,
                        nativeDirectArenaMultiGetBatch1,
                        nativeDirectArenaMultiGetBatch2To3,
                        nativeDirectArenaMultiGetBatch4To7,
                        nativeDirectArenaMultiGetBatch8To15,
                        nativeDirectArenaMultiGetBatch16To31,
                        nativeDirectArenaMultiGetBatch32To63,
                        nativeDirectArenaMultiGetBatch64,
                        nativeDirectArenaMultiGetBatch65To127,
                        nativeDirectArenaMultiGetBatch128,
                        nativeDirectArenaMultiGetFound,
                        nativeDirectArenaMultiGetNotFound,
                        nativeDirectArenaMultiGetOverflowStatuses,
                        nativeDirectArenaMultiGetOverflows,
                        nativeDirectArenaMultiGetFallbackBatches,
                        nativeDirectArenaMultiGetFallbackKeys,
                        nativeDirectArenaMultiGetThresholdFallbacks,
                        nativeDirectArenaMultiGetCapabilityFallbacks,
                        nativeDirectArenaMultiGetLinkageFallbacks,
                        nativeDirectArenaMultiGetProtocolFallbacks,
                        nativeDirectArenaMultiGetHeapKeyCopies);
            }
        }
        if (adaptiveNativeProbeController != null) {
            LOG.info(
                    "[CACHEKIT NATIVE ADAPTIVE PROBE] mode={} transitions={} windows={} "
                            + "bypassedBatches={} bypassedKeys={}",
                    adaptiveNativeProbeController.mode(),
                    adaptiveNativeProbeController.transitions(),
                    adaptiveNativeProbeController.completedWindows(),
                    adaptiveNativeProbeController.bypassedBatches(),
                    adaptiveNativeProbeController.bypassedKeys());
        }
        if (adaptiveNativeMailboxDensityController != null) {
            LOG.info(
                    "[CACHEKIT NATIVE MAILBOX DENSITY] mode={} transitions={} windows={} "
                            + "bypassedBatches={} bypassedInputKeys={} droppedSpeculativePrefetchTasks={}",
                    adaptiveNativeMailboxDensityController.mode(),
                    adaptiveNativeMailboxDensityController.transitions(),
                    adaptiveNativeMailboxDensityController.completedWindows(),
                    adaptiveNativeMailboxDensityController.bypassedBatches(),
                    adaptiveNativeMailboxDensityController.bypassedInputKeys(),
                    adaptiveNativeMailboxDensityController.droppedSpeculativePrefetchTasks());
        }
    }

    long getPrefetchMultiGetCallsForTesting() {
        return prefetchMultiGetCalls;
    }

    long getPrefetchPointGetCallsForTesting() {
        return prefetchPointGetCalls;
    }

    long getPrefetchSmallBatchDropsForTesting() {
        return prefetchSmallBatchDrops;
    }

    long getPrefetchSmallBatchKeysDroppedForTesting() {
        return prefetchSmallBatchKeysDropped;
    }

    long getPrefetchMissingValuesStagedForTesting() {
        return prefetchMissingValuesStaged;
    }

    long getPrefetchValuesPromotedForTesting() {
        return prefetchValuesPromoted;
    }

    void setAdaptiveNativeProbeControllerForTesting(
            AdaptiveNativeProbeController controller) {
        this.adaptiveNativeProbeController = controller;
    }

    void setAdaptiveNativeMailboxDensityControllerForTesting(
            AdaptiveNativeMailboxDensityController controller) {
        this.adaptiveNativeMailboxDensityController = controller;
    }

    void setAdaptiveNativeMailboxDropSpeculativePrefetchEnabledForTesting(boolean enabled) {
        this.adaptiveNativeMailboxDropSpeculativePrefetchEnabled = enabled;
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

    long getNativeDirectArenaNegativeHandoffStagedForTesting() {
        return nativeDirectArenaNegativeHandoffStaged;
    }

    long getNativeDirectArenaNegativeHandoffPromotedForTesting() {
        return nativeDirectArenaNegativeHandoffPromoted;
    }

    long getNativeDirectArenaNegativeHandoffInvalidatedForTesting() {
        return nativeDirectArenaNegativeHandoffInvalidated;
    }

    long getStagingRetainedBytesForTesting() {
        return stagingRetainedBytes.get();
    }

    long getPrefetchStagingAdmissionDropsForTesting() {
        return prefetchStagingAdmissionDrops;
    }

    long getPrefetchWorkerFailuresForTesting() {
        return prefetchWorkerFailures;
    }

    long getPrefetchKeyScopedInvalidationsForTesting() {
        return prefetchKeyScopedInvalidations;
    }

    long getPrefetchKeyScopedInFlightCancelledForTesting() {
        return prefetchKeyScopedInFlightCancelled;
    }

    long getPrefetchKeyScopedFastNegativeSkipsForTesting() {
        return prefetchKeyScopedFastNegativeSkips;
    }

    long getPrefetchKeyScopedStagedRemovedForTesting() {
        return prefetchKeyScopedStagedRemoved;
    }

    long getPrefetchStaleAbortsForTesting() {
        return prefetchStaleAborts;
    }

    long getPrefetchLiveReadRacedInFlightForTesting() {
        return prefetchLiveReadRacedInFlight;
    }

    long getPrefetchLiveReadCancellationsForTesting() {
        return prefetchLiveReadCancellations;
    }

    long getPrefetchDispatchKeysExaminedForTesting() {
        return prefetchDispatchKeysExamined;
    }

    long getPrefetchDispatchCancellationsForTesting() {
        return prefetchDispatchCancellations;
    }

    long getPrefetchDispatchAlreadyStagedForTesting() {
        return prefetchDispatchAlreadyStaged;
    }

    long getPrefetchDispatchNoReservationForTesting() {
        return prefetchDispatchNoReservation;
    }

    long getPrefetchWorkerCancelledBeforeReadForTesting() {
        return prefetchWorkerCancelledBeforeRead;
    }

    long getPrefetchWorkerDiscardedAfterReadForTesting() {
        return prefetchWorkerDiscardedAfterRead;
    }

    void reReserveForTesting(K key, N namespace) {
        inFlight.put(
                new KeyNamespaceKey<>(key, namespace, keySerializer, namespaceSerializer),
                newPrefetchReservation(writeGen));
    }

    boolean hasInFlightReservationForTesting(K key, N namespace) {
        return inFlight.containsKey(new KeyNamespaceKey<>(key, namespace));
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

    boolean isNativePointAdaptiveBypassingForTesting() {
        return nativePointAdaptiveMode == NativePointAdaptiveMode.BYPASS;
    }

    long getNativePointAdaptiveEvaluatedWindowsForTesting() {
        return nativePointAdaptiveEvaluatedWindows;
    }

    long getNativePointAdaptiveZeroHitWindowsForTesting() {
        return nativePointAdaptiveZeroHitWindows;
    }

    long getNativePointAdaptiveActiveToBypassTransitionsForTesting() {
        return nativePointAdaptiveActiveToBypassTransitions;
    }

    long getNativePointAdaptiveTargetedRecoveryTransitionsForTesting() {
        return nativePointAdaptiveTargetedRecoveryTransitions;
    }

    long getNativePointAdaptiveBypassedProbesForTesting() {
        return nativePointAdaptiveBypassedProbes;
    }

    long getNativePointAdaptiveBypassedFillsForTesting() {
        return nativePointAdaptiveBypassedFills;
    }

    long getNativePointAdaptiveTrialProbesForTesting() {
        return nativePointAdaptiveTrialProbes;
    }

    long getNativePointAdaptiveTrialPositiveHitsForTesting() {
        return nativePointAdaptiveTrialPositiveHits;
    }

    long getNativePointAdaptiveTrialNegativeHitsForTesting() {
        return nativePointAdaptiveTrialNegativeHits;
    }

    long getNativePointAdaptiveTrialMissesForTesting() {
        return nativePointAdaptiveTrialMisses;
    }

    long getNativePointAdaptiveTrialSlotDeferralsForTesting() {
        return nativePointAdaptiveTrialSlotDeferrals;
    }

    long getNativePointAdaptiveSamplesRecordedForTesting() {
        return nativePointAdaptiveSamplesRecorded;
    }

    long getNativePointAdaptiveSampleReplacementsForTesting() {
        return nativePointAdaptiveSampleReplacements;
    }

    long getNativePointAdaptiveTargetedProbesForTesting() {
        return nativePointAdaptiveTargetedProbes;
    }

    long getNativePointAdaptiveTargetedPositiveHitsForTesting() {
        return nativePointAdaptiveTargetedPositiveHits;
    }

    long getNativePointAdaptiveTargetedNegativeHitsForTesting() {
        return nativePointAdaptiveTargetedNegativeHits;
    }

    long getNativePointAdaptiveTargetedMissesForTesting() {
        return nativePointAdaptiveTargetedMisses;
    }

    long getNativePointAdaptiveTargetedSlotDeferralsForTesting() {
        return nativePointAdaptiveTargetedSlotDeferrals;
    }

    long getNativeMailboxCompactBatchesForTesting() {
        return nativeMailboxCompactBatches;
    }

    long getNativeMailboxCompactScratchBatchesForTesting() {
        return nativeMailboxCompactScratchBatches;
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

    long getNativeMailboxCompactSlotMissFallbacksForTesting() {
        return nativeMailboxCompactSlotMissFallbacks;
    }

    long getNativeMailboxCompactCapacityFallbacksForTesting() {
        return nativeMailboxCompactCapacityFallbacks;
    }

    long getNativeMailboxCompactOperationFallbacksForTesting() {
        return nativeMailboxCompactOperationFallbacks;
    }

    long getNativeCompactSelectedProbeBatchesForTesting() {
        return nativeCompactSelectedProbeBatches;
    }

    long getNativeCompactSelectedProbeKeysForTesting() {
        return nativeCompactSelectedProbeKeys;
    }

    long getNativeCompactSelectedLazyHeapKeyCopiesForTesting() {
        return nativeCompactSelectedLazyHeapKeyCopies;
    }

    long getNativeDeferredReservationInputKeysForTesting() {
        return nativeDeferredReservationInputKeys;
    }

    long getNativeDeferredReservationObjectsMaterializedForTesting() {
        return nativeDeferredReservationObjectsMaterialized;
    }

    long getNativeDeferredReservationObjectsAvoidedForTesting() {
        return nativeDeferredReservationObjectsAvoided;
    }

    long getNativeDirectArenaMultiGetBatchesForTesting() {
        return nativeDirectArenaMultiGetBatches;
    }

    long getNativeDirectArenaMultiGetKeysForTesting() {
        return nativeDirectArenaMultiGetKeys;
    }

    long getNativeDirectArenaMultiGetCompletedBatchesForTesting() {
        return nativeDirectArenaMultiGetCompletedBatches;
    }

    long getNativeDirectArenaMultiGetCompletedKeysForTesting() {
        return nativeDirectArenaMultiGetCompletedKeys;
    }

    long getNativeDirectArenaMultiGetValueBytesCopiedForTesting() {
        return nativeDirectArenaMultiGetValueBytesCopied;
    }

    long getNativeDirectArenaEagerMaterializedValuesForTesting() {
        return nativeDirectArenaEagerMaterializedValues;
    }

    long getNativeDirectArenaEagerMaterializedValueBytesForTesting() {
        return nativeDirectArenaEagerMaterializedValueBytes;
    }

    long getNativeDirectArenaEagerMissingValuesForTesting() {
        return nativeDirectArenaEagerMissingValues;
    }

    long getNativeDirectArenaEagerFallbackValuesForTesting() {
        return nativeDirectArenaEagerFallbackValues;
    }

    long getNativeDirectArenaMultiGetFoundForTesting() {
        return nativeDirectArenaMultiGetFound;
    }

    long getNativeDirectArenaMultiGetNotFoundForTesting() {
        return nativeDirectArenaMultiGetNotFound;
    }

    long getNativeDirectArenaMultiGetOverflowStatusesForTesting() {
        return nativeDirectArenaMultiGetOverflowStatuses;
    }

    long[] getNativeDirectArenaMultiGetBatchHistogramForTesting() {
        return new long[] {
            nativeDirectArenaMultiGetBatch1,
            nativeDirectArenaMultiGetBatch2To3,
            nativeDirectArenaMultiGetBatch4To7,
            nativeDirectArenaMultiGetBatch8To15,
            nativeDirectArenaMultiGetBatch16To31,
            nativeDirectArenaMultiGetBatch32To63,
            nativeDirectArenaMultiGetBatch64
        };
    }

    long getNativeDirectArenaMultiGetBatch128ForTesting() {
        return nativeDirectArenaMultiGetBatch128;
    }

    long getNativeDirectArenaMultiGetOverflowsForTesting() {
        return nativeDirectArenaMultiGetOverflows;
    }

    long getNativeDirectArenaMultiGetFallbackBatchesForTesting() {
        return nativeDirectArenaMultiGetFallbackBatches;
    }

    long getNativeDirectArenaMultiGetFallbackKeysForTesting() {
        return nativeDirectArenaMultiGetFallbackKeys;
    }

    long getNativeDirectArenaMultiGetThresholdFallbacksForTesting() {
        return nativeDirectArenaMultiGetThresholdFallbacks;
    }

    long getNativeDirectArenaMultiGetCapabilityFallbacksForTesting() {
        return nativeDirectArenaMultiGetCapabilityFallbacks;
    }

    long getNativeDirectArenaMultiGetLinkageFallbacksForTesting() {
        return nativeDirectArenaMultiGetLinkageFallbacks;
    }

    long getNativeDirectArenaMultiGetProtocolFallbacksForTesting() {
        return nativeDirectArenaMultiGetProtocolFallbacks;
    }

    long getNativeDirectArenaMultiGetHeapKeyCopiesForTesting() {
        return nativeDirectArenaMultiGetHeapKeyCopies;
    }

    long getNativeDirectArenaReadOnlyBatchesForTesting() {
        return nativeDirectArenaReadOnlyBatches;
    }

    long getNativeDirectArenaReadOnlyKeysForTesting() {
        return nativeDirectArenaReadOnlyKeys;
    }

    long getNativeDirectArenaReadOnlyCancelledKeysForTesting() {
        return nativeDirectArenaReadOnlyCancelledKeys;
    }

    long getNativeDirectArenaSpeculativePreCompactDropsForTesting() {
        return nativeDirectArenaSpeculativePreCompactDrops;
    }

    long getNativeDirectArenaSpeculativePreCompactKeysForTesting() {
        return nativeDirectArenaSpeculativePreCompactKeys;
    }

    long getNativeDirectArenaSpeculativePostCompactDropsForTesting() {
        return nativeDirectArenaSpeculativePostCompactDrops;
    }

    long getNativeDirectArenaSpeculativePostCompactKeysForTesting() {
        return nativeDirectArenaSpeculativePostCompactKeys;
    }

    long getNativeDirectArenaSpeculativeTailDropsForTesting() {
        return nativeDirectArenaSpeculativeTailDrops;
    }

    long getNativeDirectArenaSpeculativeTailKeysForTesting() {
        return nativeDirectArenaSpeculativeTailKeys;
    }

    boolean isNativeDirectArenaMultiGetDisabledForTesting() {
        return nativeDirectArenaMultiGetDisabled;
    }

    long getNativeCompactPostCompactBytesRecopiedForTesting() {
        return nativeCompactPostCompactBytesRecopied;
    }

    long getNativeMailboxDirectSerializationFallbackKeysForTesting() {
        return nativeMailboxDirectSerializationFallbackKeys;
    }

    long getNativeMailboxDirectSerializationFallbackBytesForTesting() {
        return nativeMailboxDirectSerializationFallbackBytes;
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

    long getNativeMutationReadInactiveSkippedForTesting() {
        return nativeMutationReadInactiveSkipped;
    }

    long getNativeMutationResidentMissSkippedForTesting() {
        return nativeMutationResidentMissSkipped;
    }

    long getNativeMutationBatchScopesForTesting() {
        return nativeMutationBatchScopes;
    }

    long getNativeMutationBatchFlushesForTesting() {
        return nativeMutationBatchFlushes;
    }

    long getNativeMutationBatchCoalescedForTesting() {
        return nativeMutationBatchCoalesced;
    }

    long getNativeMutationResidentHintChecksForTesting() {
        return nativeMutationResidentHintChecks;
    }

    long getNativeMutationResidentHintPositivesForTesting() {
        return nativeMutationResidentHintPositives;
    }

    long getNativeMutationResidentHintNegativesForTesting() {
        return nativeMutationResidentHintNegatives;
    }

    long getNativeMutationFenceOnlyFlushesForTesting() {
        return nativeMutationFenceOnlyFlushes;
    }

    long getNativeMutationAdaptiveBatchObservedScopesForTesting() {
        return nativeMutationAdaptiveBatchObservedScopes;
    }

    long getNativeMutationAdaptiveBatchBypassedScopesForTesting() {
        return nativeMutationAdaptiveBatchBypassedScopes;
    }

    long getNativeMutationAdaptiveBatchAttemptsForTesting() {
        return nativeMutationAdaptiveBatchAttempts;
    }

    long getNativeWriteEpochForTesting() {
        return nativeWriteEpoch.get();
    }

    long getNativeValueReadActivationsForTesting() {
        return nativeValueReadActivations;
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
        if (!admitAsyncPrefetchWorkerTask()) {
            return null;
        }
        if (multiGetPrefetchEnabled && delegate instanceof RocksDBBatchValueReader<?, ?, ?>) {
            return buildPreparedMultiGetTask(keys, currentNamespace);
        }
        java.util.ArrayList<byte[]> serialized = new java.util.ArrayList<>();
        java.util.ArrayList<KeyNamespaceKey<K, N>> reservations = new java.util.ArrayList<>();
        final long gen = writeGen;
        final PrefetchReservation reservation = newPrefetchReservation(gen);
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
                if (inFlight.putIfAbsent(storageKey, reservation) != null) {
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
            releaseReservations(reservations, reservation);
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
            releaseReservations(reservations, reservation);
            return null;
        }
        prefetchTasksBuilt++;
        prefetchKeysPrepared += serialized.size();
        return trackedTask(
                reservations,
                reservation,
                () -> fetchIntoStaging(serialized, defaultValue, gen));
    }

    /**
     * Builds and submits one speculative batch with a completion result suitable for an ordered
     * mailbox ready gate.
     *
     * <p>A {@code false} result is deliberately conservative: it means the state could not prove
     * that this batch completed against the same generation without a worker-side fallback. The
     * caller must dispatch the records normally, allowing the authoritative state path to resolve
     * every miss. Queue rejection and an uncaught worker failure complete the future
     * exceptionally. The legacy {@link #buildAsyncPrefetchTask(Iterable)} contract is unchanged.
     */
    public java.util.concurrent.CompletableFuture<Boolean> prefetchWithCompletion(
            Iterable<? extends K> keys) {
        final long generation = writeGen;
        final Runnable task = buildAsyncPrefetchTask(keys);
        if (task == null) {
            return java.util.concurrent.CompletableFuture.completedFuture(false);
        }

        // Capture after construction: build-time fallbacks already return null, while these
        // counters now describe only work performed after the task was handed to the executor.
        final long workerFailures = prefetchWorkerFailures;
        final long staleAborts = prefetchStaleAborts;
        final long stagingAdmissionDrops = prefetchStagingAdmissionDrops;
        final long smallBatchDrops = prefetchSmallBatchDrops;
        final long adaptiveAdmissionSkips = prefetchAdaptiveAdmissionSkips;
        final long discardedAfterRead = prefetchWorkerDiscardedAfterRead;
        return PrefetchExecutor.submitWithCompletion(task)
                .thenApply(
                        ignored ->
                                !closed
                                        && generation == writeGen
                                        && workerFailures == prefetchWorkerFailures
                                        && staleAborts == prefetchStaleAborts
                                        && stagingAdmissionDrops == prefetchStagingAdmissionDrops
                                        && smallBatchDrops == prefetchSmallBatchDrops
                                        && adaptiveAdmissionSkips == prefetchAdaptiveAdmissionSkips
                                        && discardedAfterRead
                                                == prefetchWorkerDiscardedAfterRead);
    }

    /**
     * Mailbox-side half of the RocksDB MultiGet path. It prepares the exact composite RocksDB key
     * once and keeps the corresponding immutable cache key beside it. The worker can therefore
     * issue each chunk directly, without deserializing query-wire keys and serializing them again
     * inside RocksDBValueState.
     */
    @SuppressWarnings("unchecked")
    private Runnable buildPreparedMultiGetTask(Iterable<? extends K> keys, N namespace) {
        final boolean nativeMailboxConfigured =
                nativeRequestPlaneCoordinator != null
                        && nativeRequestPlaneCoordinator.isActive()
                        && nativeRequestPlaneCoordinator.options().mailboxBatchEnabled();
        final boolean nativeMailboxDensityBypassed =
                nativeMailboxConfigured
                        && adaptiveNativeMailboxDensityController != null
                        && !adaptiveNativeMailboxDensityController.shouldUseNativeMailbox();
        if (nativeMailboxDensityBypassed
                && adaptiveNativeMailboxDropSpeculativePrefetchEnabled) {
            // This work is speculative. In low-density phases the Java fallback still pays key
            // copies, reservation traffic and RocksDB I/O while almost none of the prefetched
            // values are promoted. Reject the new task before any of those side effects. A later
            // ValueState.value() remains authoritative and periodic recovery probes are selected
            // by shouldUseNativeMailbox() before reaching this branch.
            adaptiveNativeMailboxDensityController.recordDroppedSpeculativePrefetchTask();
            return null;
        }
        java.util.ArrayList<byte[]> rocksDBKeys = new java.util.ArrayList<>();
        java.util.ArrayList<KeyNamespaceKey<K, N>> storageKeys = new java.util.ArrayList<>();
        final long gen = writeGen;
        final PrefetchReservation reservation = newPrefetchReservation(gen);
        final boolean nativeMailboxBatch =
                nativeMailboxConfigured && !nativeMailboxDensityBypassed;
        final boolean nativeDirectPrefetch =
                !nativeMailboxConfigured
                        && nativeRequestPlaneCoordinator != null
                        && nativeRequestPlaneCoordinator.isActive()
                        && nativeRequestPlaneCoordinator.options().prefetchEnabled();
        final boolean deferReservationMaterialization =
                nativeMailboxBatch
                        && nativeRequestPlaneCoordinator
                                .options()
                                .deferredReservationMaterializationEnabled();
        final java.util.ArrayList<K> deferredKeys =
                deferReservationMaterialization ? new java.util.ArrayList<>() : null;
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
                if (deferReservationMaterialization) {
                    deferredKeys.add(key);
                    continue;
                }
                KeyNamespaceKey<K, N> storageKey =
                        new KeyNamespaceKey<>(key, namespace, keySerializer, namespaceSerializer);
                if (!nativeMailboxBatch) {
                    if (inFlight.putIfAbsent(storageKey, reservation) != null) {
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
                releaseReservations(storageKeys, reservation);
            }
            return null; // Best-effort: an unserializable key aborts this batch only.
        }
        final int initialCandidateCount =
                deferReservationMaterialization ? deferredKeys.size() : storageKeys.size();
        if (initialCandidateCount == 0) {
            return null;
        }
        if (nativeMailboxDensityBypassed) {
            adaptiveNativeMailboxDensityController.recordBypassedInputKeys(initialCandidateCount);
        }
        if (deferReservationMaterialization) {
            nativeDeferredReservationInputKeys += initialCandidateCount;
        }
        final boolean nativeDirectReadOnlyMailbox =
                nativeMailboxBatch
                        && nativeRequestPlaneCoordinator
                                .options()
                                .directArenaReadOnlyEnabled();
        if (nativeDirectReadOnlyMailbox && initialCandidateCount < multiGetMinBatchSize) {
            // This is speculative work. A sub-MultiGet batch would ultimately be dropped by the
            // worker so the authoritative mailbox read can run. Do that before native compact,
            // key-arena serialization, slot leasing and task allocation.
            recordNativeDirectArenaSpeculativePreCompactDrop(initialCandidateCount);
            if (deferReservationMaterialization) {
                nativeDeferredReservationObjectsAvoided += initialCandidateCount;
            }
            return null;
        }
        NativeRequestPlaneCoordinator.BatchSlot nativeBatchSlot = null;
        java.util.List<byte[]> preparedRocksDBKeys = rocksDBKeys;
        boolean compactSelectedPrepared = false;
        if (nativeMailboxBatch) {
            nativeBatchSlot =
                    deferReservationMaterialization
                            ? compactNativeMailboxBatchDeferred(
                                    batchReader, rocksDBKeys, deferredKeys, namespace)
                            : compactNativeMailboxBatch(batchReader, rocksDBKeys, storageKeys);
            final int postCompactCandidateCount =
                    deferReservationMaterialization ? deferredKeys.size() : storageKeys.size();
            if (nativeBatchSlot != null
                    && nativeDirectReadOnlyMailbox
                    && postCompactCandidateCount < multiGetMinBatchSize) {
                // Native compaction can turn a large duplicate-heavy lookahead into only a few
                // exact keys. Reading those keys as speculative point Gets duplicates the later
                // authoritative mailbox path, so release the slot before reserving/submitting.
                recordNativeDirectArenaSpeculativePostCompactDrop(postCompactCandidateCount);
                if (deferReservationMaterialization) {
                    nativeDeferredReservationObjectsAvoided += initialCandidateCount;
                }
                nativeBatchSlot.close();
                return null;
            }
            if (deferReservationMaterialization) {
                if (!materializeDeferredReservationKeys(deferredKeys, namespace, storageKeys)) {
                    if (nativeBatchSlot != null) {
                        nativeBatchSlot.close();
                    }
                    return null;
                }
                nativeDeferredReservationObjectsMaterialized += storageKeys.size();
                nativeDeferredReservationObjectsAvoided +=
                        Math.max(0, initialCandidateCount - storageKeys.size());
            }
            if (nativeBatchSlot != null
                    && nativeRequestPlaneCoordinator.options().compactSelectedProbeEnabled()) {
                try {
                    if (!nativeRequestPlaneCoordinator.options().directArenaReadOnlyEnabled()) {
                        activateNativeValueRead();
                    }
                    reserveCompactedPreparedKeys(storageKeys, reservation, nativeBatchSlot);
                    if (storageKeys.isEmpty()) {
                        nativeBatchSlot.close();
                        return null;
                    }
                    preparedRocksDBKeys =
                            preparedKeyView(nativeBatchSlot, storageKeys.size(), true);
                    compactSelectedPrepared = true;
                } catch (RuntimeException | LinkageError failure) {
                    nativeRequestPlaneCoordinator.disable(failure);
                    prefetchBuildFailures++;
                    releaseReservations(storageKeys, reservation);
                    nativeBatchSlot.close();
                    return null;
                }
            } else {
                reservePreparedKeys(rocksDBKeys, storageKeys, reservation);
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
                            activateNativeValueRead();
                            nativeBatchSlot.prepareLatest(
                                    nativeStateId, nativeWriteEpoch.get(), rocksDBKeys);
                            nativeCompactPostCompactBytesRecopied +=
                                    serializedKeyBytes(rocksDBKeys);
                        } catch (IOException | RuntimeException failure) {
                            nativeBatchSlot.close();
                            nativeBatchSlot = null;
                            nativeFallbackBatches++;
                        }
                    }
                }
            }
        }
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
                    releaseReservations(storageKeys, reservation);
                    return null;
                }
            }
        }
        if (preparedRocksDBKeys.isEmpty()) {
            if (nativeBatchSlot != null) {
                nativeBatchSlot.close();
            }
            releaseReservations(storageKeys, reservation);
            return null;
        }
        final V defaultValue;
        try {
            defaultValue = copyBatchDefaultValueForAsyncTask();
        } catch (Throwable t) {
            prefetchBuildFailures++;
            releaseReservations(storageKeys, reservation);
            if (nativeBatchSlot != null) {
                nativeBatchSlot.close();
            }
            return null;
        }
        prefetchTasksBuilt++;
        prefetchKeysPrepared += preparedRocksDBKeys.size();
        if (nativeBatchSlot == null && !nativeMailboxDensityBypassed) {
            nativeBatchSlot = prepareNativeBatchSlot(preparedRocksDBKeys);
        }
        final java.util.List<byte[]> taskRocksDBKeys = preparedRocksDBKeys;
        final NativeRequestPlaneCoordinator.BatchSlot preparedNativeBatchSlot = nativeBatchSlot;
        final boolean taskCompactSelectedPrepared = compactSelectedPrepared;
        return trackedTask(
                storageKeys,
                reservation,
                () ->
                        fetchPreparedChunksIntoStaging(
                                taskRocksDBKeys,
                                storageKeys,
                                defaultValue,
                                gen,
                                reservation,
                                preparedNativeBatchSlot,
                                taskCompactSelectedPrepared),
                preparedNativeBatchSlot == null ? null : preparedNativeBatchSlot::close);
    }

    private boolean admitAsyncPrefetchWorkerTask() {
        if (!ASYNC_ADAPTIVE_ADMISSION_ENABLED
                || prefetchAsyncValuesRead < ASYNC_ADAPTIVE_MIN_SAMPLES
                || prefetchAsyncUsefulValues
                        >= prefetchAsyncValuesRead * ASYNC_ADAPTIVE_MIN_USEFUL_RATE) {
            return true;
        }
        prefetchAdaptiveAdmissionSkips++;
        long skipOrdinal = prefetchAdaptiveSkipSequence.incrementAndGet();
        if (skipOrdinal % ASYNC_ADAPTIVE_PROBE_EVERY_TASKS != 0) {
            return false;
        }
        prefetchAdaptiveProbeTasks++;
        return true;
    }

    private void recordAsyncPrefetchOutcome(byte[] serializedValue) {
        prefetchAsyncValuesRead++;
        if (serializedValue != null) {
            prefetchAsyncUsefulValues++;
        }
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
            activateNativeValueRead();
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
        return preparedKeyView(slot, size, false);
    }

    private java.util.List<byte[]> preparedKeyView(
            NativeRequestPlaneCoordinator.BatchSlot slot,
            int size,
            boolean compactSelectedMaterialization) {
        return new java.util.AbstractList<byte[]>() {
            @Override
            public byte[] get(int index) {
                byte[] key = slot.copyPreparedKey(index);
                if (compactSelectedMaterialization) {
                    nativeCompactSelectedLazyHeapKeyCopies++;
                }
                return key;
            }

            @Override
            public int size() {
                return size;
            }
        };
    }

    private static long serializedKeyBytes(java.util.List<byte[]> keys) {
        long bytes = 0;
        for (byte[] key : keys) {
            bytes += key.length;
        }
        return bytes;
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
        if (storageKeys.size() > nativeRequestPlaneCoordinator.options().batchEntries()
                && (!nativeRequestPlaneCoordinator.options().compactionScratchSlotEnabled()
                        || storageKeys.size()
                                > nativeRequestPlaneCoordinator.options().compactionScratchEntries())) {
            nativeFallbackBatches++;
            nativeMailboxCompactFallbacks++;
            nativeMailboxCompactCapacityFallbacks++;
            materializeMailboxFallbackKeys(batchReader, storageKeys, rocksDBKeys);
            return null;
        }
        NativeRequestPlaneCoordinator.BatchSlot slot =
                acquireNativeMailboxCompactionSlot(storageKeys.size());
        if (slot == null) {
            nativeFallbackBatches++;
            nativeMailboxCompactFallbacks++;
            nativeMailboxCompactSlotMissFallbacks++;
            materializeMailboxFallbackKeys(batchReader, storageKeys, rocksDBKeys);
            return null;
        }
        try {
            // When this prepared arena can later be probed, activate before capturing its
            // generation. This closes the only construction-time window in which a concurrent
            // mutation could still be admitted as "read inactive" after the batch generation was
            // chosen. Compaction-only mailbox batching does not activate write-through.
            if (nativeRequestPlaneCoordinator.options().prefetchEnabled()
                    && !nativeRequestPlaneCoordinator.options().directArenaReadOnlyEnabled()) {
                activateNativeValueRead();
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
                nativeMailboxDirectSerializationFallbackKeys += rocksDBKeys.size();
                nativeMailboxDirectSerializationFallbackBytes += serializedKeyBytes(rocksDBKeys);
                slot.prepareLatest(nativeStateId, nativeWriteEpoch.get(), rocksDBKeys);
            }
            int uniqueCount = nativeRequestPlaneCoordinator.compact(slot);
            nativeMailboxCompactBatches++;
            nativeMailboxCompactUniqueKeys += uniqueCount;
            if (adaptiveNativeMailboxDensityController != null) {
                adaptiveNativeMailboxDensityController.recordCompaction(
                        storageKeys.size(), uniqueCount);
            }
            rocksDBKeys.clear();
            boolean retainPreparedArena =
                    nativeRequestPlaneCoordinator.options().compactSelectedProbeEnabled()
                            && !slot.isCompactionScratch();
            for (int target = 0; target < uniqueCount; target++) {
                int source = slot.compactedSourceIndex(target);
                storageKeys.set(target, storageKeys.get(source));
                if (!retainPreparedArena) {
                    rocksDBKeys.add(slot.copyPreparedKey(source));
                }
            }
            int duplicates = storageKeys.size() - uniqueCount;
            if (duplicates > 0) {
                prefetchKeysDeduplicated += duplicates;
                storageKeys.subList(uniqueCount, storageKeys.size()).clear();
            }
            if (slot.isCompactionScratch()) {
                nativeMailboxCompactScratchBatches++;
                slot.close();
                return null;
            }
            return slot;
        } catch (IOException failure) {
            nativeFallbackBatches++;
            nativeMailboxCompactFallbacks++;
            nativeMailboxCompactCapacityFallbacks++;
            slot.close();
            materializeMailboxFallbackKeys(batchReader, storageKeys, rocksDBKeys);
            return null;
        } catch (Exception failure) {
            nativeFallbackBatches++;
            nativeMailboxCompactFallbacks++;
            nativeMailboxCompactOperationFallbacks++;
            slot.close();
            materializeMailboxFallbackKeys(batchReader, storageKeys, rocksDBKeys);
            return null;
        } catch (LinkageError failure) {
            nativeFallbackBatches++;
            nativeMailboxCompactFallbacks++;
            nativeMailboxCompactOperationFallbacks++;
            slot.close();
            materializeMailboxFallbackKeys(batchReader, storageKeys, rocksDBKeys);
            return null;
        }
    }

    /**
     * Native duplicate compaction over mailbox-confined key references. No key or namespace is
     * published to the worker from this method; the surviving entries are deep-copied into
     * {@link KeyNamespaceKey} reservations immediately afterwards, before the task is submitted.
     */
    private NativeRequestPlaneCoordinator.BatchSlot compactNativeMailboxBatchDeferred(
            RocksDBBatchValueReader<K, N, V> batchReader,
            java.util.ArrayList<byte[]> rocksDBKeys,
            java.util.ArrayList<K> keys,
            N namespace) {
        nativeMailboxCompactInputKeys += keys.size();
        if (keys.size() < nativeRequestPlaneCoordinator.options().minBatchSize()) {
            nativeFallbackBatches++;
            nativeMailboxCompactFallbacks++;
            nativeMailboxCompactThresholdFallbacks++;
            materializeDeferredMailboxFallbackKeys(batchReader, keys, namespace, rocksDBKeys);
            return null;
        }
        if (keys.size() > nativeRequestPlaneCoordinator.options().batchEntries()
                && (!nativeRequestPlaneCoordinator.options().compactionScratchSlotEnabled()
                        || keys.size()
                                > nativeRequestPlaneCoordinator.options().compactionScratchEntries())) {
            nativeFallbackBatches++;
            nativeMailboxCompactFallbacks++;
            nativeMailboxCompactCapacityFallbacks++;
            materializeDeferredMailboxFallbackKeys(batchReader, keys, namespace, rocksDBKeys);
            return null;
        }
        NativeRequestPlaneCoordinator.BatchSlot slot =
                acquireNativeMailboxCompactionSlot(keys.size());
        if (slot == null) {
            nativeFallbackBatches++;
            nativeMailboxCompactFallbacks++;
            nativeMailboxCompactSlotMissFallbacks++;
            materializeDeferredMailboxFallbackKeys(batchReader, keys, namespace, rocksDBKeys);
            return null;
        }
        try {
            if (nativeRequestPlaneCoordinator.options().prefetchEnabled()
                    && !nativeRequestPlaneCoordinator.options().directArenaReadOnlyEnabled()) {
                activateNativeValueRead();
            }
            try {
                slot.prepareLatestDirect(
                        nativeStateId,
                        nativeWriteEpoch.get(),
                        keys.size(),
                        (index, output) -> {
                            try {
                                batchReader.serializeBatchKeyAndNamespace(
                                        keys.get(index),
                                        namespace,
                                        keySerializer,
                                        namespaceSerializer,
                                        output);
                            } catch (IOException failure) {
                                throw failure;
                            } catch (Exception failure) {
                                throw new IOException(
                                        "Failed to serialize a deferred native mailbox key.",
                                        failure);
                            }
                        });
            } catch (IOException directFailure) {
                rocksDBKeys.clear();
                for (K key : keys) {
                    rocksDBKeys.add(
                            batchReader.serializeBatchKeyAndNamespace(
                                    key,
                                    namespace,
                                    keySerializer,
                                    namespaceSerializer));
                }
                nativeMailboxDirectSerializationFallbackKeys += rocksDBKeys.size();
                nativeMailboxDirectSerializationFallbackBytes += serializedKeyBytes(rocksDBKeys);
                slot.prepareLatest(nativeStateId, nativeWriteEpoch.get(), rocksDBKeys);
            }
            int uniqueCount = nativeRequestPlaneCoordinator.compact(slot);
            nativeMailboxCompactBatches++;
            nativeMailboxCompactUniqueKeys += uniqueCount;
            if (adaptiveNativeMailboxDensityController != null) {
                adaptiveNativeMailboxDensityController.recordCompaction(keys.size(), uniqueCount);
            }
            rocksDBKeys.clear();
            boolean retainPreparedArena =
                    nativeRequestPlaneCoordinator.options().compactSelectedProbeEnabled()
                            && !slot.isCompactionScratch();
            for (int target = 0; target < uniqueCount; target++) {
                int source = slot.compactedSourceIndex(target);
                keys.set(target, keys.get(source));
                if (!retainPreparedArena) {
                    rocksDBKeys.add(slot.copyPreparedKey(source));
                }
            }
            int duplicates = keys.size() - uniqueCount;
            if (duplicates > 0) {
                prefetchKeysDeduplicated += duplicates;
                keys.subList(uniqueCount, keys.size()).clear();
            }
            if (slot.isCompactionScratch()) {
                nativeMailboxCompactScratchBatches++;
                slot.close();
                return null;
            }
            return slot;
        } catch (IOException failure) {
            nativeFallbackBatches++;
            nativeMailboxCompactFallbacks++;
            nativeMailboxCompactCapacityFallbacks++;
            slot.close();
            materializeDeferredMailboxFallbackKeys(batchReader, keys, namespace, rocksDBKeys);
            return null;
        } catch (Exception failure) {
            nativeFallbackBatches++;
            nativeMailboxCompactFallbacks++;
            nativeMailboxCompactOperationFallbacks++;
            slot.close();
            materializeDeferredMailboxFallbackKeys(batchReader, keys, namespace, rocksDBKeys);
            return null;
        } catch (LinkageError failure) {
            nativeFallbackBatches++;
            nativeMailboxCompactFallbacks++;
            nativeMailboxCompactOperationFallbacks++;
            slot.close();
            materializeDeferredMailboxFallbackKeys(batchReader, keys, namespace, rocksDBKeys);
            return null;
        }
    }

    private NativeRequestPlaneCoordinator.BatchSlot acquireNativeMailboxCompactionSlot(
            int candidateCount) {
        if (candidateCount > nativeRequestPlaneCoordinator.options().batchEntries()) {
            return nativeRequestPlaneCoordinator.tryAcquireCompactionScratchSlot();
        }
        NativeRequestPlaneCoordinator.BatchSlot slot =
                nativeRequestPlaneCoordinator.tryAcquireBatchSlot();
        if (slot != null) {
            return slot;
        }
        return nativeRequestPlaneCoordinator.tryAcquireCompactionScratchSlot();
    }

    private void materializeDeferredMailboxFallbackKeys(
            RocksDBBatchValueReader<K, N, V> batchReader,
            java.util.ArrayList<K> keys,
            N namespace,
            java.util.ArrayList<byte[]> rocksDBKeys) {
        rocksDBKeys.clear();
        try {
            for (K key : keys) {
                rocksDBKeys.add(
                        batchReader.serializeBatchKeyAndNamespace(
                                key,
                                namespace,
                                keySerializer,
                                namespaceSerializer));
            }
        } catch (Throwable failure) {
            rocksDBKeys.clear();
            keys.clear();
            prefetchBuildFailures++;
        }
    }

    private boolean materializeDeferredReservationKeys(
            java.util.ArrayList<K> keys,
            N namespace,
            java.util.ArrayList<KeyNamespaceKey<K, N>> storageKeys) {
        storageKeys.clear();
        try {
            for (K key : keys) {
                storageKeys.add(
                        new KeyNamespaceKey<>(
                                key, namespace, keySerializer, namespaceSerializer));
            }
            return true;
        } catch (Throwable failure) {
            storageKeys.clear();
            prefetchBuildFailures++;
            return false;
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
            PrefetchReservation reservation) {
        int writeIndex = 0;
        for (int readIndex = 0; readIndex < storageKeys.size(); readIndex++) {
            KeyNamespaceKey<K, N> storageKey = storageKeys.get(readIndex);
            if (inFlight.putIfAbsent(storageKey, reservation) != null) {
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

    private void reserveCompactedPreparedKeys(
            java.util.ArrayList<KeyNamespaceKey<K, N>> storageKeys,
            PrefetchReservation reservation,
            NativeRequestPlaneCoordinator.BatchSlot slot) {
        int writeIndex = 0;
        int originalSize = storageKeys.size();
        for (int readIndex = 0; readIndex < originalSize; readIndex++) {
            KeyNamespaceKey<K, N> storageKey = storageKeys.get(readIndex);
            if (inFlight.putIfAbsent(storageKey, reservation) != null) {
                prefetchKeysDeduplicated++;
                continue;
            }
            if (writeIndex != readIndex) {
                storageKeys.set(writeIndex, storageKey);
            }
            slot.retainCompactedSource(readIndex, writeIndex);
            writeIndex++;
        }
        slot.projectRetainedCompactedSources(writeIndex);
        if (writeIndex < originalSize) {
            storageKeys.subList(writeIndex, originalSize).clear();
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
        prefetchForImmediateUse(keys, false);
    }

    /**
     * Immediate prefetch with optional exact in-flight revocation fused into its existing key scan.
     */
    @SuppressWarnings("unchecked")
    public void prefetchForImmediateUse(
            Iterable<? extends K> keys, boolean cancelPrefetchOnDispatch) {
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
        final boolean cancelInFlight =
                cancelPrefetchOnDispatch && supportsDispatchPrefetchCancellation();
        try {
            for (K key : keys) {
                if (key == null) {
                    continue;
                }
                if (findCachedValueFor(key, namespace) != null) {
                    if (cancelInFlight) {
                        // A cache hit still makes an older speculative reservation redundant.
                        // Reuse the fused lookup for exact revocation, but never enqueue an
                        // immediate RocksDB read for an already-authoritative cached value.
                        hasStagedOrInFlightValue(key, namespace, gen, true);
                    }
                    continue;
                }
                if (hasStagedOrInFlightValue(key, namespace, gen, cancelInFlight)) {
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
                boolean handled =
                        nativeRequestPlaneCoordinator.options().directArenaReadOnlyEnabled()
                                ? executeNativeDirectReadOnlyBatch(
                                        rocksDBKeys,
                                        storageKeys,
                                        defaultValue,
                                        gen,
                                        nativeBatchSlot,
                                        null,
                                        true)
                                : executeNativePreparedBatch(
                                        rocksDBKeys,
                                        storageKeys,
                                        defaultValue,
                                        gen,
                                        nativeBatchSlot,
                                        true,
                                        null,
                                        false);
                if (handled) {
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
                            prefetchImmediatePointGetCalls++;
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
        return hasStagedOrInFlightValue(key, namespace, gen, false);
    }

    private boolean hasStagedOrInFlightValue(
            K key, N namespace, long gen, boolean cancelInFlight) {
        setLookupKey(key, namespace);
        if (cancelInFlight) {
            prefetchDispatchKeysExamined++;
        }
        Object stagedEntry = staging.get(lookupKey);
        if (stagedEntry != null) {
            boolean current =
                    isNativeNegativeStagedEntry(stagedEntry)
                            ? gen == writeGen
                            : ((StagedValue<?>) stagedEntry).gen == gen;
            if (current) {
                if (cancelInFlight) {
                    prefetchDispatchAlreadyStaged++;
                }
                prefetchKeysDeduplicated++;
                return true;
            }
            removeStagedEntry(lookupKey, stagedEntry);
        }
        PrefetchReservation reservation = inFlight.get(lookupKey);
        if (reservation != null) {
            if (reservation.generation == gen) {
                if (cancelInFlight) {
                    synchronized (staging) {
                        stagedEntry = staging.get(lookupKey);
                        if (stagedEntry != null
                                && (isNativeNegativeStagedEntry(stagedEntry)
                                        ? gen == writeGen
                                        : ((StagedValue<?>) stagedEntry).gen == gen)) {
                            prefetchDispatchAlreadyStaged++;
                            prefetchKeysDeduplicated++;
                            return true;
                        }
                        if (inFlight.remove(lookupKey, reservation)) {
                            prefetchDispatchCancellations++;
                            return false;
                        }
                        prefetchDispatchNoReservation++;
                        return false;
                    }
                }
                prefetchKeysDeduplicated++;
                return true;
            }
            inFlight.remove(lookupKey, reservation);
        }
        if (cancelInFlight) {
            prefetchDispatchNoReservation++;
        }
        return false;
    }

    private Runnable trackedTask(
            java.util.List<KeyNamespaceKey<K, N>> reservations,
            PrefetchReservation reservation,
            Runnable task) {
        return trackedTask(reservations, reservation, task, null);
    }

    private Runnable trackedTask(
            java.util.List<KeyNamespaceKey<K, N>> reservations,
            PrefetchReservation reservation,
            Runnable task,
            Runnable completion) {
        final class TrackedPrefetchTask implements PrefetchExecutor.DropAwareTask {
            // 0=pending, 1=running, 2=completed. The transition makes run/onDrop idempotent even
            // when queue cancellation races executor dequeue.
            private final java.util.concurrent.atomic.AtomicInteger state =
                    new java.util.concurrent.atomic.AtomicInteger();

            @Override
            public void run() {
                if (!state.compareAndSet(0, 1)) {
                    return;
                }
                final long startedNanos = System.nanoTime();
                final long queueNanos =
                        Math.max(0L, startedNanos - reservation.submittedNanos);
                prefetchWorkerQueueNanos += queueNanos;
                prefetchWorkerQueueNanosMax =
                        Math.max(prefetchWorkerQueueNanosMax, queueNanos);
                try {
                    task.run();
                } finally {
                    final long runNanos = Math.max(0L, System.nanoTime() - startedNanos);
                    prefetchWorkerRunNanos += runNanos;
                    prefetchWorkerRunNanosMax =
                            Math.max(prefetchWorkerRunNanosMax, runNanos);
                    finish(false);
                }
            }

            @Override
            public void onDrop() {
                if (state.compareAndSet(0, 2)) {
                    finish(true);
                }
            }

            private void finish(boolean dropped) {
                try {
                    if (dropped) {
                        prefetchTasksDropped++;
                    }
                    releaseReservations(reservations, reservation);
                    if (completion != null) {
                        completion.run();
                    }
                } finally {
                    state.set(2);
                    synchronized (prefetchTaskMonitor) {
                        outstandingPrefetchTasks.remove(this);
                        prefetchTaskMonitor.notifyAll();
                    }
                }
            }
        }

        TrackedPrefetchTask tracked = new TrackedPrefetchTask();
        boolean accepted;
        synchronized (prefetchTaskMonitor) {
            accepted = !closed;
            if (accepted) {
                outstandingPrefetchTasks.add(tracked);
            }
        }
        if (!accepted) {
            tracked.onDrop();
        }
        return tracked;
    }

    /**
     * Cancels only this state's queued tasks, then waits for any task already running to finish its
     * completion callback and release its native slot. No lifecycle write lock is held while
     * waiting, so a worker already inside a guarded RocksDB read can observe {@link #closed}, leave
     * the read side, and complete without a lock cycle.
     */
    private void cancelQueuedAndAwaitPrefetchTasks() {
        java.util.List<PrefetchExecutor.DropAwareTask> snapshot;
        synchronized (prefetchTaskMonitor) {
            snapshot = new java.util.ArrayList<>(outstandingPrefetchTasks);
        }
        for (PrefetchExecutor.DropAwareTask tracked : snapshot) {
            PrefetchExecutor.cancelIfQueued(tracked);
        }

        boolean interrupted = false;
        synchronized (prefetchTaskMonitor) {
            while (!outstandingPrefetchTasks.isEmpty()) {
                try {
                    prefetchTaskMonitor.wait();
                } catch (InterruptedException ignored) {
                    // Teardown cannot safely destroy the native plane while a task still owns a
                    // slot. Preserve the interrupt after the correctness barrier has drained.
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void releaseReservations(
            java.util.List<KeyNamespaceKey<K, N>> reservations,
            PrefetchReservation reservation) {
        for (KeyNamespaceKey<K, N> key : reservations) {
            inFlight.remove(key, reservation);
        }
    }

    private PrefetchReservation newPrefetchReservation(long generation) {
        return new PrefetchReservation(
                generation,
                prefetchReservationSequence.incrementAndGet(),
                System.nanoTime());
    }

    private static long nanosAverageMicros(long totalNanos, long samples) {
        return samples <= 0 ? 0L : totalNanos / samples / 1_000L;
    }

    private static long nanosToMicros(long nanos) {
        return nanos / 1_000L;
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
            if (!admitAsyncPrefetchWorkerTask()) {
                return;
            }
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
        } catch (Throwable failure) {
            recordPrefetchWorkerFailure("prepared-chunks", failure);
            // Best-effort cache warmup; the authoritative read path is untouched.
        }
    }

    private void recordPrefetchWorkerFailure(String phase, Throwable failure) {
        prefetchWorkerFailures++;
        if (FIRST_PREFETCH_WORKER_FAILURE_LOGGED.compareAndSet(false, true)) {
            LOG.warn(
                    "[CACHEKIT PREFETCH WORKER FAILURE] phase={} delegate={} nativeStateId={} "
                            + "directReadOnly={} directBatchSize={} message={}",
                    phase,
                    delegate.getClass().getName(),
                    nativeStateId,
                    nativeRequestPlaneCoordinator != null
                            && nativeRequestPlaneCoordinator.options().directArenaReadOnlyEnabled(),
                    directArenaChunkSize(),
                    failure.toString(),
                    failure);
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
            PrefetchReservation reservation,
            NativeRequestPlaneCoordinator.BatchSlot nativeBatchSlot,
            boolean compactSelectedPrepared) {
        prefetchTasksExecuted++;
        try {
            if (!admitAsyncPrefetchWorkerTask()) {
                return;
            }
            boolean shouldProbeNative = nativeBatchSlot != null;
            boolean directArenaReadOnly =
                    shouldProbeNative
                            && nativeRequestPlaneCoordinator
                                    .options()
                                    .directArenaReadOnlyEnabled();
            if (directArenaReadOnly
                    && executeNativeDirectReadOnlyBatch(
                            rocksDBKeys,
                            storageKeys,
                            defaultValue,
                            gen,
                            nativeBatchSlot,
                            reservation,
                            false)) {
                return;
            }
            shouldProbeNative = shouldProbeNative && !directArenaReadOnly;
            if (shouldProbeNative
                    && compactSelectedPrepared
                    && adaptiveNativeProbeController != null) {
                shouldProbeNative =
                        adaptiveNativeProbeController.shouldProbe(rocksDBKeys.size());
            }
            if (shouldProbeNative
                    && executeNativePreparedBatch(
                            rocksDBKeys,
                            storageKeys,
                            defaultValue,
                            gen,
                            nativeBatchSlot,
                            false,
                            reservation,
                            compactSelectedPrepared)) {
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
                        rocksDBKeys,
                        storageKeys,
                        start,
                        end,
                        defaultValue,
                        gen,
                        reservation);
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
            if (!nativeRequestPlaneCoordinator.options().directArenaReadOnlyEnabled()) {
                activateNativeValueRead();
            }
            slot.prepareLatest(nativeStateId, nativeWriteEpoch.get(), rocksDBKeys);
            return slot;
        } catch (IOException | RuntimeException failure) {
            nativeFallbackBatches++;
            slot.close();
            return null;
        }
    }

    /**
     * Issues the authoritative RocksDB read directly from the mailbox-compacted native key arena.
     *
     * <p>This mode is intended for write-heavy states where generation fencing makes the native
     * point cache effectively hitless. It deliberately skips both native probe and fill while
     * preserving the existing exact reservation, generation, cancellation, and staging guards.
     * Returning false is fail-open to the existing prepared-key Java MultiGet path.
     */
    @SuppressWarnings("unchecked")
    private boolean executeNativeDirectReadOnlyBatch(
            java.util.List<byte[]> rocksDBKeys,
            java.util.List<KeyNamespaceKey<K, N>> storageKeys,
            V defaultValue,
            long gen,
            NativeRequestPlaneCoordinator.BatchSlot slot,
            PrefetchReservation reservation,
            boolean immediate)
            throws Exception {
        if (closed
                || gen != writeGen
                || nativeDirectArenaMultiGetDisabled
                || !nativeRequestPlaneCoordinator.isActive()) {
            nativeFallbackBatches++;
            return false;
        }
        RocksDBBatchValueReader<K, N, V> batchReader =
                (RocksDBBatchValueReader<K, N, V>) delegate;
        if (!batchReader.supportsDirectArenaMultiGet()) {
            nativeDirectArenaMultiGetCapabilityFallbacks++;
            nativeDirectArenaMultiGetDisabled = true;
            nativeFallbackBatches++;
            return false;
        }

        int[] preparedIndices = new int[rocksDBKeys.size()];
        int activeCount = 0;
        for (int index = 0; index < rocksDBKeys.size(); index++) {
            if (reservation != null
                    && !isPrefetchReservationActive(storageKeys.get(index), reservation)) {
                prefetchWorkerCancelledBeforeRead++;
                nativeDirectArenaReadOnlyCancelledKeys++;
                continue;
            }
            preparedIndices[activeCount++] = index;
        }
        if (activeCount == 0) {
            return true;
        }
        if (reservation != null && activeCount < multiGetMinBatchSize) {
            recordNativeDirectArenaSpeculativePostCompactDrop(activeCount);
            return true;
        }

        if (reservation != null) {
            int directChunkSize = directArenaChunkSize(batchReader);
            int tail = activeCount % directChunkSize;
            if (activeCount > directChunkSize && tail > 0 && tail < multiGetMinBatchSize) {
                // The tail would otherwise copy prepared keys back to heap and issue point Gets.
                // Keep only complete/useful direct chunks; tracked-task completion releases the
                // unconsumed tail reservations for their authoritative mailbox reads.
                activeCount -= tail;
                recordNativeDirectArenaSpeculativeTailDrop(tail);
            }
        }

        if (nativeRequestPlaneCoordinator
                .options()
                .directArenaEagerMaterializationEnabled()) {
            nativeDirectArenaReadOnlyBatches++;
            nativeDirectArenaReadOnlyKeys += activeCount;
            return fetchAndPublishDirectArenaPreparedValues(
                    batchReader,
                    slot,
                    preparedIndices,
                    activeCount,
                    storageKeys,
                    defaultValue,
                    gen,
                    reservation,
                    immediate);
        }

        java.util.List<byte[]> values =
                fetchDirectArenaPreparedMissValues(
                        batchReader, slot, preparedIndices, activeCount, gen);
        if (values == null) {
            return true;
        }
        nativeDirectArenaReadOnlyBatches++;
        nativeDirectArenaReadOnlyKeys += activeCount;
        for (int resultIndex = 0; resultIndex < activeCount; resultIndex++) {
            if (closed || gen != writeGen) {
                prefetchStaleAborts++;
                return true;
            }
            int originalIndex = preparedIndices[resultIndex];
            byte[] serializedValue = values.get(resultIndex);
            boolean published;
            if (immediate) {
                V value = deserializeImmediateValueOrCopyDefault(serializedValue, defaultValue);
                published =
                        publishStagedValue(
                                StagedValue.materialized(
                                        storageKeys.get(originalIndex), value, gen),
                                serializedValue == null,
                                reservation);
            } else if (serializedValue == null
                    && nativeRequestPlaneCoordinator.options().negativeHandoffEnabled()
                    && keyScopedPrefetchInvalidationEnabled
                    && reservation != null) {
                published =
                        publishNativeNegativeStagedValue(
                                storageKeys.get(originalIndex), gen, reservation);
            } else {
                published =
                        stagePreparedValue(
                                storageKeys.get(originalIndex),
                                serializedValue,
                                defaultValue,
                                gen,
                                reservation);
            }
            if (!published) {
                return true;
            }
        }
        return true;
    }

    /**
     * Reads direct-arena values and materializes them before the bounded slot is released.
     *
     * <p>This is the zero-intermediate-copy counterpart of {@link
     * #fetchDirectArenaPreparedMissValues}. It deliberately preserves the same transactional
     * chunk fallback, generation fencing, reservation checks, and staging publication rules. The
     * only successful fast-path difference is that the value serializer consumes the reusable
     * direct-memory view instead of a temporary heap byte array.
     */
    private boolean fetchAndPublishDirectArenaPreparedValues(
            RocksDBBatchValueReader<K, N, V> batchReader,
            NativeRequestPlaneCoordinator.BatchSlot slot,
            int[] preparedIndices,
            int count,
            java.util.List<KeyNamespaceKey<K, N>> storageKeys,
            V defaultValue,
            long gen,
            PrefetchReservation reservation,
            boolean immediate)
            throws Exception {
        int directChunkSize = directArenaChunkSize(batchReader);
        for (int start = 0; start < count; start += directChunkSize) {
            if (closed || gen != writeGen) {
                prefetchStaleAborts++;
                return true;
            }
            int chunkCount = Math.min(directChunkSize, count - start);
            boolean fallback = nativeDirectArenaMultiGetDisabled;
            if (!fallback && chunkCount < multiGetMinBatchSize) {
                nativeDirectArenaMultiGetThresholdFallbacks++;
                fallback = true;
            }
            if (!fallback) {
                slot.prepareDirectArenaMultiGet(
                        preparedIndices,
                        start,
                        chunkCount,
                        RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH);
                int presentCount;
                try {
                    lifecycleLock.readLock().lock();
                    try {
                        if (closed || gen != writeGen) {
                            prefetchStaleAborts++;
                            return true;
                        }
                        nativeDirectArenaMultiGetBatches++;
                        nativeDirectArenaMultiGetKeys += chunkCount;
                        recordNativeDirectArenaMultiGetBatchSize(chunkCount);
                        presentCount =
                                batchReader.getSerializedValuesByRocksDBKeyArena(
                                        slot.directMultiGetKeyArena(),
                                        slot.directMultiGetDescriptors(),
                                        chunkCount,
                                        slot.directMultiGetValueArena(),
                                        slot.directMultiGetValueStride());
                        prefetchMultiGetCalls++;
                        prefetchMultiGetKeys += chunkCount;
                    } finally {
                        lifecycleLock.readLock().unlock();
                    }
                } catch (LinkageError missingNativeSymbol) {
                    nativeDirectArenaMultiGetLinkageFallbacks++;
                    nativeDirectArenaMultiGetDisabled = true;
                    fallback = true;
                    presentCount = 0;
                } catch (UnsupportedOperationException invalidCapabilityAdvertisement) {
                    nativeDirectArenaMultiGetProtocolFallbacks++;
                    nativeDirectArenaMultiGetDisabled = true;
                    fallback = true;
                    presentCount = 0;
                } catch (IllegalArgumentException invalidDirectAbi) {
                    nativeDirectArenaMultiGetProtocolFallbacks++;
                    nativeDirectArenaMultiGetDisabled = true;
                    fallback = true;
                    presentCount = 0;
                }

                if (!fallback) {
                    int observedPresent = 0;
                    int observedNotFound = 0;
                    int observedOverflow = 0;
                    boolean overflow = false;
                    boolean invalidProtocol = presentCount < 0 || presentCount > chunkCount;
                    for (int index = 0; index < chunkCount && !invalidProtocol; index++) {
                        int result = slot.directMultiGetResult(index);
                        if (result >= 0 && result <= slot.directMultiGetValueStride()) {
                            observedPresent++;
                        } else if (result == RocksDBBatchValueReader.DIRECT_ARENA_NOT_FOUND) {
                            observedNotFound++;
                        } else if (result == RocksDBBatchValueReader.DIRECT_ARENA_OVERFLOW) {
                            observedPresent++;
                            observedOverflow++;
                            overflow = true;
                        } else {
                            invalidProtocol = true;
                        }
                    }
                    if (observedPresent != presentCount) {
                        invalidProtocol = true;
                    }
                    if (invalidProtocol) {
                        nativeDirectArenaMultiGetProtocolFallbacks++;
                        nativeDirectArenaMultiGetDisabled = true;
                        fallback = true;
                    } else if (overflow) {
                        nativeDirectArenaMultiGetFound += observedPresent - observedOverflow;
                        nativeDirectArenaMultiGetNotFound += observedNotFound;
                        nativeDirectArenaMultiGetOverflowStatuses += observedOverflow;
                        nativeDirectArenaMultiGetOverflows++;
                        fallback = true;
                    } else {
                        nativeDirectArenaMultiGetFound += observedPresent;
                        nativeDirectArenaMultiGetNotFound += observedNotFound;
                        nativeDirectArenaMultiGetCompletedBatches++;
                        nativeDirectArenaMultiGetCompletedKeys += chunkCount;
                        for (int index = 0; index < chunkCount; index++) {
                            if (closed || gen != writeGen) {
                                prefetchStaleAborts++;
                                return true;
                            }
                            int originalIndex = preparedIndices[start + index];
                            KeyNamespaceKey<K, N> storageKey = storageKeys.get(originalIndex);
                            int result = slot.directMultiGetResult(index);
                            boolean missing =
                                    result == RocksDBBatchValueReader.DIRECT_ARENA_NOT_FOUND;
                            if (missing) {
                                nativeDirectArenaEagerMissingValues++;
                            } else {
                                nativeDirectArenaEagerMaterializedValues++;
                                nativeDirectArenaEagerMaterializedValueBytes += result;
                            }
                            if (!publishDirectArenaEagerValue(
                                    slot,
                                    index,
                                    storageKey,
                                    defaultValue,
                                    gen,
                                    reservation,
                                    immediate,
                                    missing)) {
                                return true;
                            }
                        }
                    }
                }
            }
            if (fallback) {
                java.util.List<byte[]> fallbackValues =
                        fetchPreparedIndexFallbackValues(
                                batchReader, slot, preparedIndices, start, chunkCount, gen);
                if (fallbackValues == null) {
                    return true;
                }
                nativeDirectArenaEagerFallbackValues += chunkCount;
                for (int index = 0; index < chunkCount; index++) {
                    if (closed || gen != writeGen) {
                        prefetchStaleAborts++;
                        return true;
                    }
                    int originalIndex = preparedIndices[start + index];
                    byte[] serializedValue = fallbackValues.get(index);
                    boolean missing = serializedValue == null;
                    boolean published;
                    if (!immediate
                            && missing
                            && nativeRequestPlaneCoordinator.options().negativeHandoffEnabled()
                            && keyScopedPrefetchInvalidationEnabled
                            && reservation != null) {
                        published =
                                publishNativeNegativeStagedValue(
                                        storageKeys.get(originalIndex), gen, reservation);
                    } else {
                        V value =
                                immediate
                                        ? deserializeImmediateValueOrCopyDefault(
                                                serializedValue, defaultValue)
                                        : deserializeValueOrCopyDefault(
                                                serializedValue, defaultValue);
                        published =
                                publishStagedValue(
                                        StagedValue.materialized(
                                                storageKeys.get(originalIndex), value, gen),
                                        missing,
                                        reservation);
                    }
                    if (!published) {
                        return true;
                    }
                }
            }
        }
        return true;
    }

    private boolean publishDirectArenaEagerValue(
            NativeRequestPlaneCoordinator.BatchSlot slot,
            int resultIndex,
            KeyNamespaceKey<K, N> storageKey,
            V defaultValue,
            long gen,
            PrefetchReservation reservation,
            boolean immediate,
            boolean missing)
            throws IOException {
        if (!immediate
                && missing
                && nativeRequestPlaneCoordinator.options().negativeHandoffEnabled()
                && keyScopedPrefetchInvalidationEnabled
                && reservation != null) {
            return publishNativeNegativeStagedValue(storageKey, gen, reservation);
        }
        V value;
        if (immediate) {
            if (immediateValueSerializer == null) {
                immediateValueSerializer = delegate.getValueSerializer().duplicate();
            }
            value =
                    missing
                            ? (defaultValue == null
                                    ? null
                                    : immediateValueSerializer.copy(defaultValue))
                            : immediateValueSerializer.deserialize(
                                    slot.directMultiGetValueInput(resultIndex));
        } else {
            prepareWorkerValueState();
            value =
                    missing
                            ? (defaultValue == null
                                    ? null
                                    : workerValueSerializer.copy(defaultValue))
                            : workerValueSerializer.deserialize(
                                    slot.directMultiGetValueInput(resultIndex));
        }
        return publishStagedValue(
                StagedValue.materialized(storageKey, value, gen), missing, reservation);
    }

    private void recordNativeDirectArenaSpeculativePreCompactDrop(int keys) {
        prefetchSmallBatchDrops++;
        prefetchSmallBatchKeysDropped += keys;
        nativeDirectArenaSpeculativePreCompactDrops++;
        nativeDirectArenaSpeculativePreCompactKeys += keys;
    }

    private void recordNativeDirectArenaSpeculativePostCompactDrop(int keys) {
        prefetchSmallBatchDrops++;
        prefetchSmallBatchKeysDropped += keys;
        nativeDirectArenaSpeculativePostCompactDrops++;
        nativeDirectArenaSpeculativePostCompactKeys += keys;
    }

    private void recordNativeDirectArenaSpeculativeTailDrop(int keys) {
        prefetchSmallBatchDrops++;
        prefetchSmallBatchKeysDropped += keys;
        nativeDirectArenaSpeculativeTailDrops++;
        nativeDirectArenaSpeculativeTailKeys += keys;
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
            boolean immediate,
            PrefetchReservation reservation,
            boolean compactSelectedPrepared)
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
        if (compactSelectedPrepared) {
            nativeCompactSelectedProbeBatches++;
            nativeCompactSelectedProbeKeys += processed;
        }
        if (closed || gen != writeGen) {
            prefetchStaleAborts++;
            return true;
        }

        Object[] valuesByOriginalIndex = new Object[processed];
        boolean[] skipPublication = new boolean[processed];
        final RocksDBBatchValueReader<K, N, V> batchReader =
                (RocksDBBatchValueReader<K, N, V>) delegate;
        final boolean directArenaRequested =
                compactSelectedPrepared
                        && nativeRequestPlaneCoordinator
                                .options()
                                .directArenaMultiGetEnabled()
                        && !nativeDirectArenaMultiGetDisabled;
        final boolean directArenaSupported =
                !directArenaRequested || batchReader.supportsDirectArenaMultiGet();
        if (directArenaRequested && !directArenaSupported) {
            // A configured experiment must never look like a zero-miss workload when the loaded
            // RocksDB artifact is actually missing the Direct Arena capability. Latch the optional
            // path off, retain the authoritative ordinary MultiGet fallback, and make the mismatch
            // observable in the terminal audit record.
            nativeDirectArenaMultiGetCapabilityFallbacks++;
            nativeDirectArenaMultiGetDisabled = true;
        }
        final boolean directArenaMultiGet = directArenaRequested && directArenaSupported;
        java.util.ArrayList<byte[]> missKeys =
                directArenaMultiGet ? null : new java.util.ArrayList<>(processed);
        int[] missOriginalIndices = new int[processed];
        int missCount = 0;
        int batchHits = 0;
        long batchHitBytesDirect = 0;
        int batchNegativeHits = 0;
        int batchMisses = 0;
        int batchCancelledMisses = 0;
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
                    if (reservation != null
                            && !isPrefetchReservationActive(storageKeys.get(i), reservation)) {
                        // The mailbox already became authoritative for this key. Do not issue a
                        // duplicate RocksDB read; the publication guard below remains in place for
                        // cancellations racing after this check.
                        prefetchWorkerCancelledBeforeRead++;
                        batchCancelledMisses++;
                        skipPublication[i] = true;
                    } else {
                        if (!directArenaMultiGet) {
                            missKeys.add(rocksDBKeys.get(i));
                        }
                        missOriginalIndices[missCount++] = i;
                    }
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
        if (compactSelectedPrepared && adaptiveNativeProbeController != null) {
            adaptiveNativeProbeController.recordProbe(
                    processed, batchHits + batchNegativeHits);
        }

        java.util.List<byte[]> missValues;
        if (reservation != null
                && batchCancelledMisses > 0
                && missCount > 0
                && missCount < multiGetMinBatchSize) {
            // Speculation must not turn a cancellation-shrunk batch into duplicate point Gets.
            // Native hits and negatives are still publishable; only the remaining RocksDB misses
            // are left for their later authoritative mailbox reads.
            prefetchSmallBatchDrops++;
            prefetchSmallBatchKeysDropped += missCount;
            for (int i = 0; i < missCount; i++) {
                skipPublication[missOriginalIndices[i]] = true;
            }
            if (missKeys != null) {
                missKeys.clear();
            }
            missCount = 0;
            missValues = java.util.Collections.emptyList();
        } else if (directArenaMultiGet) {
            missValues =
                    fetchDirectArenaPreparedMissValues(
                            batchReader, slot, missOriginalIndices, missCount, gen);
        } else {
            missValues = fetchCompactPreparedMissValues(missKeys, gen);
        }
        if (missValues == null) {
            return true;
        }
        for (int i = 0; i < missValues.size(); i++) {
            valuesByOriginalIndex[missOriginalIndices[i]] = missValues.get(i);
        }

        if (missCount > 0 && !closed && gen == writeGen) {
            try {
                if (directArenaMultiGet) {
                    slot.prepareFillFromPreparedIndices(
                            nativeStateId,
                            slot.preparedGeneration(),
                            missOriginalIndices,
                            missCount,
                            missValues);
                } else {
                    slot.prepareFill(
                            nativeStateId, slot.preparedGeneration(), missKeys, missValues);
                }
                int filled = nativeRequestPlaneCoordinator.fill(slot);
                if (filled != missCount) {
                    IllegalStateException failure =
                            new IllegalStateException(
                                    "Native fill processed "
                                            + filled
                                            + " of "
                                            + missCount
                                            + " compacted misses.");
                    nativeRequestPlaneCoordinator.disable(failure);
                    nativeRuntimeFailures++;
                    nativeFillRejected += missCount;
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
                nativeFillRejected += missCount;
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
            if (skipPublication[i]) {
                continue;
            }
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
                                StagedValue.materialized(storageKeys.get(i), value, gen),
                                false,
                                reservation);
            } else if (immediate) {
                byte[] serializedValue = (byte[]) valuesByOriginalIndex[i];
                V value =
                        deserializeImmediateValueOrCopyDefault(serializedValue, defaultValue);
                published =
                        publishStagedValue(
                                StagedValue.materialized(storageKeys.get(i), value, gen),
                                serializedValue == null,
                                reservation);
            } else {
                byte[] serializedValue = (byte[]) valuesByOriginalIndex[i];
                published =
                        stagePreparedValue(
                                storageKeys.get(i),
                                serializedValue,
                                defaultValue,
                                gen,
                                reservation);
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

    /**
     * Reads compacted native misses without first materializing the prepared key arena on heap.
     *
     * <p>The direct ABI is transactional per chunk: one overflow invalidates every value slot in
     * that chunk. Linkage and descriptor-protocol failures latch this optional optimization off;
     * ordinary RocksDB failures remain authoritative and propagate to the caller.
     */
    private java.util.List<byte[]> fetchDirectArenaPreparedMissValues(
            RocksDBBatchValueReader<K, N, V> batchReader,
            NativeRequestPlaneCoordinator.BatchSlot slot,
            int[] preparedIndices,
            int count,
            long gen)
            throws Exception {
        if (count == 0) {
            return java.util.Collections.emptyList();
        }
        java.util.ArrayList<byte[]> values = new java.util.ArrayList<>(count);
        int directChunkSize = directArenaChunkSize(batchReader);
        for (int start = 0; start < count; start += directChunkSize) {
            if (closed || gen != writeGen) {
                prefetchStaleAborts++;
                return null;
            }
            int chunkCount = Math.min(directChunkSize, count - start);
            boolean fallback = nativeDirectArenaMultiGetDisabled;
            if (!fallback && chunkCount < multiGetMinBatchSize) {
                nativeDirectArenaMultiGetThresholdFallbacks++;
                fallback = true;
            }
            if (!fallback) {
                slot.prepareDirectArenaMultiGet(
                        preparedIndices,
                        start,
                        chunkCount,
                        RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH);
                int presentCount;
                try {
                    lifecycleLock.readLock().lock();
                    try {
                        if (closed || gen != writeGen) {
                            prefetchStaleAborts++;
                            return null;
                        }
                        nativeDirectArenaMultiGetBatches++;
                        nativeDirectArenaMultiGetKeys += chunkCount;
                        recordNativeDirectArenaMultiGetBatchSize(chunkCount);
                        presentCount =
                                batchReader.getSerializedValuesByRocksDBKeyArena(
                                        slot.directMultiGetKeyArena(),
                                        slot.directMultiGetDescriptors(),
                                        chunkCount,
                                        slot.directMultiGetValueArena(),
                                        slot.directMultiGetValueStride());
                        prefetchMultiGetCalls++;
                        prefetchMultiGetKeys += chunkCount;
                    } finally {
                        lifecycleLock.readLock().unlock();
                    }
                } catch (LinkageError missingNativeSymbol) {
                    nativeDirectArenaMultiGetLinkageFallbacks++;
                    nativeDirectArenaMultiGetDisabled = true;
                    fallback = true;
                    presentCount = 0;
                } catch (UnsupportedOperationException invalidCapabilityAdvertisement) {
                    nativeDirectArenaMultiGetProtocolFallbacks++;
                    nativeDirectArenaMultiGetDisabled = true;
                    fallback = true;
                    presentCount = 0;
                } catch (IllegalArgumentException invalidDirectAbi) {
                    nativeDirectArenaMultiGetProtocolFallbacks++;
                    nativeDirectArenaMultiGetDisabled = true;
                    fallback = true;
                    presentCount = 0;
                }

                if (!fallback) {
                    int observedPresent = 0;
                    int observedNotFound = 0;
                    int observedOverflow = 0;
                    boolean overflow = false;
                    boolean invalidProtocol = presentCount < 0 || presentCount > chunkCount;
                    for (int index = 0; index < chunkCount && !invalidProtocol; index++) {
                        int result = slot.directMultiGetResult(index);
                        if (result >= 0 && result <= slot.directMultiGetValueStride()) {
                            observedPresent++;
                        } else if (result == RocksDBBatchValueReader.DIRECT_ARENA_NOT_FOUND) {
                            // Missing values are represented by null in the ordered result.
                            observedNotFound++;
                        } else if (result == RocksDBBatchValueReader.DIRECT_ARENA_OVERFLOW) {
                            observedPresent++;
                            observedOverflow++;
                            overflow = true;
                        } else {
                            invalidProtocol = true;
                        }
                    }
                    if (observedPresent != presentCount) {
                        invalidProtocol = true;
                    }
                    if (invalidProtocol) {
                        nativeDirectArenaMultiGetProtocolFallbacks++;
                        nativeDirectArenaMultiGetDisabled = true;
                        fallback = true;
                    } else if (overflow) {
                        nativeDirectArenaMultiGetFound += observedPresent - observedOverflow;
                        nativeDirectArenaMultiGetNotFound += observedNotFound;
                        nativeDirectArenaMultiGetOverflowStatuses += observedOverflow;
                        // The JNI contract leaves the complete value arena untouched on overflow.
                        // Re-read the entire chunk rather than mixing direct and fallback values.
                        nativeDirectArenaMultiGetOverflows++;
                        fallback = true;
                    } else {
                        nativeDirectArenaMultiGetFound += observedPresent;
                        nativeDirectArenaMultiGetNotFound += observedNotFound;
                        nativeDirectArenaMultiGetCompletedBatches++;
                        nativeDirectArenaMultiGetCompletedKeys += chunkCount;
                        for (int index = 0; index < chunkCount; index++) {
                            int result = slot.directMultiGetResult(index);
                            if (result == RocksDBBatchValueReader.DIRECT_ARENA_NOT_FOUND) {
                                values.add(null);
                            } else {
                                byte[] copy = slot.copyDirectMultiGetValue(index);
                                nativeDirectArenaMultiGetValueBytesCopied += copy.length;
                                values.add(copy);
                            }
                        }
                    }
                }
            }
            if (fallback) {
                java.util.List<byte[]> fallbackValues =
                        fetchPreparedIndexFallbackValues(
                                batchReader, slot, preparedIndices, start, chunkCount, gen);
                if (fallbackValues == null) {
                    return null;
                }
                values.addAll(fallbackValues);
            }
        }
        if (values.size() != count) {
            throw new IllegalStateException(
                    "Direct-arena MultiGet result count does not match compacted miss count.");
        }
        return values;
    }

    private void recordNativeDirectArenaMultiGetBatchSize(int count) {
        if (count == 1) {
            nativeDirectArenaMultiGetBatch1++;
        } else if (count <= 3) {
            nativeDirectArenaMultiGetBatch2To3++;
        } else if (count <= 7) {
            nativeDirectArenaMultiGetBatch4To7++;
        } else if (count <= 15) {
            nativeDirectArenaMultiGetBatch8To15++;
        } else if (count <= 31) {
            nativeDirectArenaMultiGetBatch16To31++;
        } else if (count <= 63) {
            nativeDirectArenaMultiGetBatch32To63++;
        } else {
            nativeDirectArenaMultiGetBatch64++;
            if (count < RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH) {
                nativeDirectArenaMultiGetBatch65To127++;
            } else {
                nativeDirectArenaMultiGetBatch128++;
            }
        }
    }

    private int directArenaChunkSize() {
        int configured =
                nativeRequestPlaneCoordinator == null
                        ? RocksDBBatchValueReader.DIRECT_ARENA_DEFAULT_BATCH
                        : nativeRequestPlaneCoordinator.options().directArenaBatchSize();
        return Math.min(configured, multiGetChunkSize);
    }

    private int directArenaChunkSize(RocksDBBatchValueReader<K, N, V> batchReader) {
        return Math.min(
                directArenaChunkSize(),
                Math.max(
                        1,
                        Math.min(
                                RocksDBBatchValueReader.DIRECT_ARENA_MAX_BATCH,
                                batchReader.directArenaMultiGetMaxBatch())));
    }

    /** Materializes only one rejected direct chunk and uses the existing authoritative reader. */
    private java.util.List<byte[]> fetchPreparedIndexFallbackValues(
            RocksDBBatchValueReader<K, N, V> batchReader,
            NativeRequestPlaneCoordinator.BatchSlot slot,
            int[] preparedIndices,
            int fromIndex,
            int count,
            long gen)
            throws Exception {
        java.util.ArrayList<byte[]> keys = new java.util.ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            keys.add(slot.copyPreparedKey(preparedIndices[fromIndex + index]));
        }
        nativeDirectArenaMultiGetFallbackBatches++;
        nativeDirectArenaMultiGetFallbackKeys += count;
        nativeDirectArenaMultiGetHeapKeyCopies += count;
        nativeCompactSelectedLazyHeapKeyCopies += count;

        java.util.List<byte[]> fallbackValues;
        lifecycleLock.readLock().lock();
        try {
            if (closed || gen != writeGen) {
                prefetchStaleAborts++;
                return null;
            }
            if (count < multiGetMinBatchSize) {
                fallbackValues = new java.util.ArrayList<>(count);
                for (byte[] key : keys) {
                    prefetchPointGetCalls++;
                    fallbackValues.add(batchReader.getSerializedValueByRocksDBKey(key));
                }
            } else {
                prefetchMultiGetCalls++;
                prefetchMultiGetKeys += count;
                fallbackValues =
                        batchReader.getSerializedValuesByRocksDBKeys(keys, 0, count);
            }
        } finally {
            lifecycleLock.readLock().unlock();
        }
        if (fallbackValues.size() != count) {
            throw new IllegalStateException(
                    "RocksDB direct-arena fallback result count does not match miss count.");
        }
        return fallbackValues;
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
            long gen,
            PrefetchReservation reservation)
            throws Exception {
        RocksDBBatchValueReader<K, N, V> batchReader =
                (RocksDBBatchValueReader<K, N, V>) delegate;
        java.util.ArrayList<byte[]> activeRocksDBKeys =
                new java.util.ArrayList<>(end - start);
        java.util.ArrayList<KeyNamespaceKey<K, N>> activeStorageKeys =
                new java.util.ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            KeyNamespaceKey<K, N> storageKey = storageKeys.get(i);
            if (isPrefetchReservationActive(storageKey, reservation)) {
                activeRocksDBKeys.add(rocksDBKeys.get(i));
                activeStorageKeys.add(storageKey);
            } else {
                prefetchWorkerCancelledBeforeRead++;
            }
        }
        if (activeRocksDBKeys.isEmpty()) {
            return;
        }
        java.util.List<byte[]> valueBytes;
        lifecycleLock.readLock().lock();
        try {
            if (closed || gen != writeGen) {
                prefetchStaleAborts++;
                return;
            }
            if (activeRocksDBKeys.size() < multiGetMinBatchSize) {
                // This is a speculative path: the authoritative mailbox read will still execute
                // if no staged value exists. Falling back to point Get here duplicates exactly
                // that work and was observed to turn every nominal async batch into point reads
                // after only one live-read cancellation. Drop the shrunken batch instead; this is
                // semantics-neutral and guarantees that async prepared-key I/O remains batched.
                prefetchSmallBatchDrops++;
                prefetchSmallBatchKeysDropped += activeRocksDBKeys.size();
                return;
            }
            prefetchMultiGetCalls++;
            prefetchMultiGetKeys += activeRocksDBKeys.size();
            valueBytes =
                    batchReader.getSerializedValuesByRocksDBKeys(
                            activeRocksDBKeys, 0, activeRocksDBKeys.size());
        } finally {
            lifecycleLock.readLock().unlock();
        }

        if (closed || gen != writeGen || valueBytes.size() != activeStorageKeys.size()) {
            prefetchStaleAborts++;
            return;
        }
        for (int i = 0; i < valueBytes.size(); i++) {
            if (gen != writeGen) {
                prefetchStaleAborts++;
                return;
            }
            KeyNamespaceKey<K, N> storageKey = activeStorageKeys.get(i);
            byte[] serializedValue = valueBytes.get(i);
            recordAsyncPrefetchOutcome(serializedValue);
            if (!stagePreparedValue(
                    storageKey, serializedValue, defaultValue, gen, reservation)) {
                return;
            }
        }
    }

    private boolean isPrefetchReservationActive(
            KeyNamespaceKey<K, N> storageKey, PrefetchReservation reservation) {
        return inFlight.get(storageKey) == reservation;
    }

    private boolean stagePreparedValue(
            KeyNamespaceKey<K, N> storageKey, byte[] valueBytes, V defaultValue, long gen)
            throws IOException {
        return stagePreparedValue(storageKey, valueBytes, defaultValue, gen, null);
    }

    private boolean stagePreparedValue(
            KeyNamespaceKey<K, N> storageKey,
            byte[] valueBytes,
            V defaultValue,
            long gen,
            PrefetchReservation reservation)
            throws IOException {
        StagedValue<V> staged;
        if (lazyStagingEnabled) {
            staged = StagedValue.serialized(storageKey, valueBytes, defaultValue, gen);
        } else {
            V value = deserializeValueOrCopyDefault(valueBytes, defaultValue);
            staged = StagedValue.materialized(storageKey, value, gen);
        }
        return publishStagedValue(staged, valueBytes == null, reservation);
    }

    private boolean publishStagedValue(StagedValue<V> staged, boolean missing) {
        return publishStagedValue(staged, missing, null);
    }

    private boolean publishStagedValue(
            StagedValue<V> staged,
            boolean missing,
            PrefetchReservation requiredReservation) {
        lifecycleLock.readLock().lock();
        try {
            if (closed || staged.gen != writeGen) {
                prefetchStaleAborts++;
                return false;
            }
            synchronized (staging) {
                if (requiredReservation != null
                        && !isPrefetchReservationActive(
                                staged.storageKey(), requiredReservation)) {
                    prefetchWorkerDiscardedAfterRead++;
                    return true;
                }
                Object previous = staging.get(staged.storageKey());
                if (previous == null && staging.size() >= asyncStagingMaxEntries) {
                    prefetchStagingAdmissionDrops++;
                    return false;
                }
                long retainedBytes =
                        stagingRetainedBytes.get()
                                - stagedEntryRetainedBytes(previous)
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

    private Object removeStagedEntry(Object key) {
        synchronized (staging) {
            Object removed = staging.remove(key);
            if (removed != null) {
                stagingRetainedBytes.addAndGet(-stagedEntryRetainedBytes(removed));
            }
            return removed;
        }
    }

    private boolean publishNativeNegativeStagedValue(
            KeyNamespaceKey<K, N> storageKey,
            long gen,
            PrefetchReservation requiredReservation) {
        lifecycleLock.readLock().lock();
        try {
            if (closed || gen != writeGen || !keyScopedPrefetchInvalidationEnabled) {
                prefetchStaleAborts++;
                return false;
            }
            synchronized (staging) {
                if (!isPrefetchReservationActive(storageKey, requiredReservation)) {
                    prefetchWorkerDiscardedAfterRead++;
                    return true;
                }
                Object previous = staging.get(storageKey);
                if (previous == null && staging.size() >= asyncStagingMaxEntries) {
                    prefetchStagingAdmissionDrops++;
                    return false;
                }
                if (previous != null) {
                    stagingRetainedBytes.addAndGet(-stagedEntryRetainedBytes(previous));
                }
                staging.put(storageKey, storageKey);
            }
            prefetchValuesStaged++;
            prefetchMissingValuesStaged++;
            nativeDirectArenaNegativeHandoffStaged++;
            return true;
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    private boolean removeStagedEntry(Object key, Object expected) {
        synchronized (staging) {
            if (!staging.remove(key, expected)) {
                return false;
            }
            stagingRetainedBytes.addAndGet(-stagedEntryRetainedBytes(expected));
            return true;
        }
    }

    private static boolean isNativeNegativeStagedEntry(Object entry) {
        return entry instanceof KeyNamespaceKey<?, ?>;
    }

    @SuppressWarnings("unchecked")
    private KeyNamespaceKey<K, N> nativeNegativeStorageKey(Object entry) {
        return (KeyNamespaceKey<K, N>) entry;
    }

    private static long stagedEntryRetainedBytes(Object entry) {
        return entry instanceof StagedValue<?> ? ((StagedValue<?>) entry).retainedBytes() : 0L;
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
            recordAsyncPrefetchOutcome(serializedValue);
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
        recordAsyncPrefetchOutcome(valueBytes);
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

    @SuppressWarnings("unchecked")
    private V copyBatchDefaultValueForMailbox() {
        if (!(delegate instanceof RocksDBBatchValueReader<?, ?, ?>)) {
            return null;
        }
        V defaultValue =
                ((RocksDBBatchValueReader<K, N, V>) delegate).getBatchDefaultValue();
        if (defaultValue == null) {
            return null;
        }
        if (stagedValueSerializer == null) {
            stagedValueSerializer = delegate.getValueSerializer().duplicate();
            stagedValueInput = new org.apache.flink.core.memory.DataInputDeserializer();
        }
        return stagedValueSerializer.copy(defaultValue);
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
            long nativeEpoch = prepareDelegateWrite(key);
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
            if (nativeResidentMutationBatchActive) {
                if (nativeResidentMutationBatchEpoch == 0L) {
                    nativeResidentMutationBatchEpoch = nativeWriteEpoch.incrementAndGet();
                }
                return nativeResidentMutationBatchEpoch;
            }
            return nativeWriteEpoch.incrementAndGet();
        }
        return 0L;
    }

    /** Prepare a delegate-visible write using the mailbox-thread lookup scratch key. */
    private long prepareDelegateWrite(K key, N namespace) {
        if (!keyScopedPrefetchInvalidationEnabled) {
            return advanceWriteGeneration();
        }
        setLookupKey(key, namespace);
        return prepareDelegateWrite(lookupKey);
    }

    /**
     * Revoke speculative ownership for exactly the written key before RocksDB is mutated.
     *
     * <p>The worker publishes under the same {@code staging} monitor and must still own the exact
     * reservation object. Therefore both race orders are safe: a pre-write publish is removed
     * here; a post-write publish observes that its reservation was revoked and is discarded.
     */
    private long prepareDelegateWrite(KeyNamespaceKey<K, N> key) {
        if (!keyScopedPrefetchInvalidationEnabled) {
            return advanceWriteGeneration();
        }
        prefetchKeyScopedInvalidations++;
        // The mailbox thread installs every reservation before submitting its worker. Therefore a
        // write that observes neither a reservation nor a staged value precedes any future read of
        // this key and can safely avoid mutating either ConcurrentHashMap or taking staging's
        // accounting monitor. This is the overwhelmingly common path on q9.
        PrefetchReservation reservation = inFlight.get(key);
        if (reservation != null) {
            // A worker may already be inside publishStagedValue after validating reservation
            // identity. Serialize cancellation with that validation+put sequence: either the
            // worker publishes first and we delete its stale value, or we revoke first and its
            // identity check fails.
            synchronized (staging) {
                if (inFlight.remove(key, reservation)) {
                    prefetchKeyScopedInFlightCancelled++;
                }
                Object staged = staging.get(key);
                if (staged != null && staging.remove(key, staged)) {
                    stagingRetainedBytes.addAndGet(-stagedEntryRetainedBytes(staged));
                    prefetchKeyScopedStagedRemoved++;
                    if (isNativeNegativeStagedEntry(staged)) {
                        nativeDirectArenaNegativeHandoffInvalidated++;
                    }
                }
            }
            return 0L;
        }
        // A worker releases its reservation only after publishing. Therefore, when the mailbox
        // thread sees no reservation, an absent staged value is a true negative; a future task was
        // submitted after this write and will read the new delegate value.
        Object staged = staging.get(key);
        if (staged == null) {
            prefetchKeyScopedFastNegativeSkips++;
            return 0L;
        }
        if (removeStagedEntry(key, staged)) {
            prefetchKeyScopedStagedRemoved++;
            if (isNativeNegativeStagedEntry(staged)) {
                nativeDirectArenaNegativeHandoffInvalidated++;
            }
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
        boolean residentOnly =
                nativeRequestPlaneCoordinator.options().readActivatedWriteThrough();
        if (residentOnly
                && nativeValueReadActivation != null
                && !nativeValueReadActivation.isActive()) {
            nativeMutationReadInactiveSkipped++;
            return;
        }
        if (residentOnly
                && nativeRequestPlaneCoordinator.options().residentMutationBatchEnabled()
                && nativeResidentMutationBatchActive) {
            queueNativeResidentMutation(key, namespace, value);
            return;
        }
        RocksDBBatchValueReader<K, N, V> batchReader =
                (RocksDBBatchValueReader<K, N, V>) delegate;
        try {
            int status;
            if (residentOnly) {
                status =
                        nativeRequestPlaneCoordinator.updateExactKeyIfPresent(
                                nativeStateId,
                                nativeEpoch,
                                output -> serializeNativeKey(batchReader, key, namespace, output),
                                value == null
                                        ? null
                                        : output ->
                                                nativeMutationValueSerializer.serialize(
                                                        value, output));
            } else {
                status =
                        nativeRequestPlaneCoordinator.updateExactKey(
                                nativeStateId,
                                nativeEpoch,
                                output -> serializeNativeKey(batchReader, key, namespace, output),
                                value == null
                                        ? null
                                        : output ->
                                                nativeMutationValueSerializer.serialize(
                                                        value, output));
            }
            if (status == NativeRequestPlaneBridge.FILL_REJECTED_STALE_GENERATION) {
                nativeMutationSuperseded++;
            } else if (status == NativeRequestPlaneBridge.FILL_NOT_PRESENT) {
                nativeMutationResidentMissSkipped++;
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

    private void serializeNativeKey(
            RocksDBBatchValueReader<K, N, V> batchReader,
            K key,
            N namespace,
            PositionedDataOutputView output)
            throws IOException {
        try {
            batchReader.serializeBatchKeyAndNamespace(
                    key,
                    namespace,
                    nativeMutationKeySerializer,
                    nativeMutationNamespaceSerializer,
                    output);
        } catch (IOException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IOException("Failed to serialize a native ValueState key.", failure);
        }
    }

    /**
     * Starts one mailbox-confined resident-only mutation batch for the incoming dispatch.
     *
     * <p>Mutations are coalesced during the dispatch and exact residency is checked only at its
     * exit. This matters for read-modify-write queries: a key is commonly absent at mailbox flush
     * entry and becomes resident only after asynchronous prefetch finishes while records are being
     * consumed. Values are retained with {@link TypeSerializer#copy(Object)} so delaying their
     * native serialization cannot observe a later mutation of a user object. The first real
     * delegate mutation advances the epoch once; a read-only dispatch never moves the epoch.
     */
    @SuppressWarnings("unchecked")
    public boolean beginNativeResidentMutationBatch(Iterable<? extends K> keys) {
        if (nativeResidentMutationBatchActive
                || keys == null
                || currentNamespace == null
                || nativeRequestPlaneCoordinator == null
                || !nativeRequestPlaneCoordinator.isActive()
                || !nativeRequestPlaneCoordinator.options().residentMutationBatchEnabled()
                || !(delegate instanceof RocksDBBatchValueReader<?, ?, ?>)) {
            return false;
        }
        if (NATIVE_RESIDENT_MUTATION_ADAPTIVE_ENABLED
                && !isResidentMutationBatchProfitable(
                        nativeMutationAdaptiveBatchObservedScopes,
                        nativeMutationAdaptiveBatchAttempts,
                        nativeMutationResidentHintPositives,
                        NATIVE_RESIDENT_MUTATION_ADAPTIVE_MIN_SCOPES,
                        NATIVE_RESIDENT_MUTATION_ADAPTIVE_MIN_MUTATIONS_PER_SCOPE,
                        NATIVE_RESIDENT_MUTATION_ADAPTIVE_MAX_HINT_POSITIVE_RATE)) {
            nativeMutationAdaptiveBatchBypassedScopes++;
            return false;
        }
        try {
            int scopeKeys = 0;
            for (K key : keys) {
                if (key != null) {
                    scopeKeys++;
                }
            }
            if (scopeKeys == 0) {
                return false;
            }
            nativeResidentBatchDirtyKeys.clear();
            pendingNativeResidentMutations.clear();
            nativeResidentMutationBatchEpoch = 0L;
            nativeResidentMutationBatchFenceKey = null;
            if (NATIVE_RESIDENT_MUTATION_ADAPTIVE_ENABLED) {
                nativeMutationAdaptiveBatchStartAttempts = nativeMutationAttempts;
            }
            nativeResidentMutationBatchActive = true;
            nativeMutationBatchScopes++;
            nativeMutationBatchScopeKeys += scopeKeys;
            return true;
        } catch (Exception | LinkageError failure) {
            nativeRequestPlaneCoordinator.disable(failure);
            nativeMutationFailures++;
            nativeRuntimeFailures++;
            nativeResidentBatchDirtyKeys.clear();
            pendingNativeResidentMutations.clear();
            nativeResidentMutationBatchFenceKey = null;
            nativeMutationAdaptiveBatchStartAttempts = 0L;
            nativeResidentMutationBatchActive = false;
            return false;
        }
    }

    static boolean isResidentMutationBatchProfitable(
            long observedScopes,
            long observedAttempts,
            long observedHintPositives,
            long minimumScopes,
            double minimumMutationsPerScope,
            double maximumHintPositiveRate) {
        if (observedScopes < minimumScopes) {
            return true;
        }
        if (observedScopes <= 0L || observedAttempts <= 0L) {
            return false;
        }
        double mutationsPerScope = (double) observedAttempts / observedScopes;
        double hintPositiveRate = (double) observedHintPositives / observedAttempts;
        return mutationsPerScope >= minimumMutationsPerScope
                && hintPositiveRate <= maximumHintPositiveRate;
    }

    /** Flushes and clears the mailbox-confined resident-only mutation batch, if active. */
    public void endNativeResidentMutationBatch() {
        if (!nativeResidentMutationBatchActive) {
            return;
        }
        try {
            if (nativeResidentMutationBatchFenceKey != null) {
                nativeMutationBatchFlushes++;
            }
            if (!pendingNativeResidentMutations.isEmpty()) {
                if (nativeRequestPlaneCoordinator == null
                        || !nativeRequestPlaneCoordinator.isActive()) {
                    nativeMutationFailures += pendingNativeResidentMutations.size();
                    return;
                }
                java.util.ArrayList<PendingNativeMutation<V>> mutations =
                        new java.util.ArrayList<>(pendingNativeResidentMutations.values());
                java.util.ArrayList<byte[]> keys =
                        new java.util.ArrayList<>(mutations.size());
                for (PendingNativeMutation<V> mutation : mutations) {
                    keys.add(mutation.preparedKey);
                }
                nativeMutationBatchFlushKeys += keys.size();

                int[] present =
                        nativeRequestPlaneCoordinator.tryCheckExactKeysPresent(
                                nativeStateId, nativeResidentMutationBatchEpoch, keys);
                if (present == null) {
                    // A bounded prefetch slot is temporarily unavailable. Preserve the established
                    // fail-closed semantics with the coordinator's reserved mutation slot; its
                    // value writer is invoked only when the exact key is resident.
                    for (int index = 0; index < mutations.size(); index++) {
                        PendingNativeMutation<V> mutation = mutations.get(index);
                        byte[] preparedKey = keys.get(index);
                        int status =
                                nativeRequestPlaneCoordinator.updateExactKeyIfPresent(
                                        nativeStateId,
                                        nativeResidentMutationBatchEpoch,
                                        output -> output.write(preparedKey),
                                        mutation.tombstone
                                                ? null
                                                : output ->
                                                        nativeMutationValueSerializer.serialize(
                                                                mutation.value, output));
                        recordNativeResidentMutationStatus(status, mutation.tombstone);
                    }
                    return;
                }

                java.util.ArrayList<byte[]> residentKeys = new java.util.ArrayList<>();
                java.util.ArrayList<byte[]> residentValues = new java.util.ArrayList<>();
                java.util.ArrayList<Boolean> residentTombstones = new java.util.ArrayList<>();
                for (int index = 0; index < present.length; index++) {
                    int status = present[index];
                    PendingNativeMutation<V> mutation = mutations.get(index);
                    if (status == NativeRequestPlaneBridge.FILL_UPDATED) {
                        residentKeys.add(keys.get(index));
                        residentValues.add(
                                mutation.tombstone
                                        ? null
                                        : KvStateSerializer.serializeValue(
                                                mutation.value, nativeMutationValueSerializer));
                        residentTombstones.add(mutation.tombstone);
                    } else if (status == NativeRequestPlaneBridge.FILL_NOT_PRESENT) {
                        nativeMutationResidentMissSkipped++;
                    } else {
                        nativeMutationSuperseded++;
                    }
                }
                if (!residentKeys.isEmpty()) {
                    int[] updates =
                            nativeRequestPlaneCoordinator.updateExactKeysIfPresent(
                                    nativeStateId,
                                    nativeResidentMutationBatchEpoch,
                                    residentKeys,
                                    residentValues);
                    for (int index = 0; index < updates.length; index++) {
                        recordNativeResidentMutationStatus(
                                updates[index], residentTombstones.get(index));
                    }
                }
            } else if (nativeResidentMutationBatchFenceKey != null
                    && nativeResidentMutationBatchEpoch != 0L
                    && nativeRequestPlaneCoordinator != null
                    && nativeRequestPlaneCoordinator.isActive()) {
                // Hint-negative mutations still changed authoritative RocksDB state. Advance the
                // state watermark once so an older asynchronous fill cannot later become visible.
                nativeRequestPlaneCoordinator.advanceGenerationFence(
                        nativeStateId,
                        nativeResidentMutationBatchEpoch,
                        nativeResidentMutationBatchFenceKey);
                nativeMutationFenceOnlyFlushes++;
            }
        } catch (Exception | LinkageError failure) {
            if (nativeRequestPlaneCoordinator != null) {
                nativeRequestPlaneCoordinator.disable(failure);
            }
            nativeMutationFailures += pendingNativeResidentMutations.size();
            nativeRuntimeFailures++;
        } finally {
            if (NATIVE_RESIDENT_MUTATION_ADAPTIVE_ENABLED) {
                nativeMutationAdaptiveBatchObservedScopes++;
                nativeMutationAdaptiveBatchAttempts +=
                        Math.max(
                                0L,
                                nativeMutationAttempts
                                        - nativeMutationAdaptiveBatchStartAttempts);
            }
            nativeResidentMutationBatchActive = false;
            nativeResidentMutationBatchEpoch = 0L;
            nativeResidentMutationBatchFenceKey = null;
            nativeMutationAdaptiveBatchStartAttempts = 0L;
            nativeResidentBatchDirtyKeys.clear();
            pendingNativeResidentMutations.clear();
        }
    }

    private void recordNativeResidentMutationStatus(int status, boolean tombstone) {
        if (status == NativeRequestPlaneBridge.FILL_UPDATED) {
            nativeMutationApplied++;
            if (tombstone) {
                nativeMutationTombstonesApplied++;
            }
        } else if (status == NativeRequestPlaneBridge.FILL_NOT_PRESENT) {
            nativeMutationResidentMissSkipped++;
        } else {
            nativeMutationSuperseded++;
        }
    }

    private void queueNativeResidentMutation(K key, N namespace, V value) {
        KeyNamespaceKey<K, N> dirtyKey =
                new KeyNamespaceKey<>(key, namespace, keySerializer, namespaceSerializer);
        nativeResidentBatchDirtyKeys.add(dirtyKey);
        try {
            @SuppressWarnings("unchecked")
            RocksDBBatchValueReader<K, N, V> batchReader =
                    (RocksDBBatchValueReader<K, N, V>) delegate;
            byte[] preparedKey =
                    batchReader.serializeBatchKeyAndNamespace(
                            key,
                            namespace,
                            nativeMutationKeySerializer,
                            nativeMutationNamespaceSerializer);
            if (nativeResidentMutationBatchFenceKey == null) {
                nativeResidentMutationBatchFenceKey = preparedKey;
            }
            nativeMutationResidentHintChecks++;
            if (!nativeRequestPlaneCoordinator.mightContainResidentKey(
                    nativeStateId, preparedKey)) {
                nativeMutationResidentHintNegatives++;
                nativeMutationResidentMissSkipped++;
                return;
            }
            nativeMutationResidentHintPositives++;
            V valueCopy = value == null ? null : nativeMutationValueSerializer.copy(value);
            PendingNativeMutation<V> previous =
                    pendingNativeResidentMutations.put(
                            dirtyKey,
                            new PendingNativeMutation<>(
                                    preparedKey, valueCopy, value == null));
            if (previous != null) {
                nativeMutationBatchCoalesced++;
            }
        } catch (Exception | LinkageError failure) {
            nativeMutationFailures += pendingNativeResidentMutations.size() + 1L;
            nativeRuntimeFailures++;
            nativeRequestPlaneCoordinator.disable(failure);
            pendingNativeResidentMutations.clear();
        }
    }

    private static final class PendingNativeMutation<V> {
        private final byte[] preparedKey;
        private final V value;
        private final boolean tombstone;

        private PendingNativeMutation(
                byte[] preparedKey, V value, boolean tombstone) {
            this.preparedKey = preparedKey;
            this.value = value;
            this.tombstone = tombstone;
        }
    }

    private void activateNativeValueRead() {
        if (nativeValueReadActivation != null && nativeValueReadActivation.activate()) {
            nativeValueReadActivations++;
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

    /**
     * Ownership token for one submitted prefetch task. The state generation protects conservative
     * mode against writes; in key-scoped mode, exact reservation identity protects the written key.
     * Ticket identity prevents a later same-generation reservation from reviving an older,
     * cancelled worker (the classic ABA case).
     */
    private static final class PrefetchReservation {
        private final long generation;
        @SuppressWarnings("unused")
        private final long ticket;
        private final long submittedNanos;

        private PrefetchReservation(long generation, long ticket, long submittedNanos) {
            this.generation = generation;
            this.ticket = ticket;
            this.submittedNanos = submittedNanos;
        }
    }

    private enum NativePointAdaptiveMode {
        ACTIVE,
        BYPASS
    }

    private enum NativePointProbePermit {
        NORMAL,
        TRIAL,
        TARGETED,
        SKIP
    }

    private enum NativePointCacheDisposition {
        HIT,
        DELEGATE_AND_FILL,
        DELEGATE_NO_FILL
    }

    private static final class NativePointCacheResult<V> {
        private static final NativePointCacheResult<?> DELEGATE_AND_FILL =
                new NativePointCacheResult<>(
                        NativePointCacheDisposition.DELEGATE_AND_FILL, null, false);
        private static final NativePointCacheResult<?> ADAPTIVE_BYPASS =
                new NativePointCacheResult<>(
                        NativePointCacheDisposition.DELEGATE_NO_FILL, null, true);

        private final NativePointCacheDisposition disposition;
        private final V value;
        private final boolean adaptiveFillSuppressed;

        private NativePointCacheResult(
                NativePointCacheDisposition disposition,
                V value,
                boolean adaptiveFillSuppressed) {
            this.disposition = disposition;
            this.value = value;
            this.adaptiveFillSuppressed = adaptiveFillSuppressed;
        }

        @SuppressWarnings("unchecked")
        private static <V> NativePointCacheResult<V> delegateAndFill() {
            return (NativePointCacheResult<V>) DELEGATE_AND_FILL;
        }

        @SuppressWarnings("unchecked")
        private static <V> NativePointCacheResult<V> adaptiveBypass() {
            return (NativePointCacheResult<V>) ADAPTIVE_BYPASS;
        }

        private static <V> NativePointCacheResult<V> hit(V value) {
            return new NativePointCacheResult<>(NativePointCacheDisposition.HIT, value, false);
        }

        private boolean isHit() {
            return disposition == NativePointCacheDisposition.HIT;
        }

        private boolean shouldFillAfterDelegateRead() {
            return disposition == NativePointCacheDisposition.DELEGATE_AND_FILL;
        }

        private boolean adaptiveFillSuppressed() {
            return adaptiveFillSuppressed;
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
