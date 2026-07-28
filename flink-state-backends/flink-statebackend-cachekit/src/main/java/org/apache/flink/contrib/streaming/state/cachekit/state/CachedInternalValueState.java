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

    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;

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

    /** Cap on staged entries; the worker clears the whole map beyond this (all droppable). */
    private static final int ASYNC_STAGING_MAX_ENTRIES = loadStagingMaxEntries();
    private static final int MULTIGET_CHUNK_SIZE = loadMultiGetChunkSize();
    private static final int MULTIGET_MIN_BATCH_SIZE = loadMultiGetMinBatchSize();

    /**
     * Values fetched by the shared prefetch worker, waiting to be promoted into L1 by the mailbox
     * thread. Worker only puts; mailbox only removes/promotes. A staged entry may only be promoted
     * while {@code writeGen} still equals the generation captured at submission — any write or
     * dirty flush on this state in between makes the RocksDB read potentially stale.
     */
    private final java.util.concurrent.ConcurrentHashMap<KeyNamespaceKey<K, N>, StagedValue<V>>
            staging = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Keys reserved by submitted-but-not-yet-finished prefetch tasks. Without this set, adjacent
     * early-lookahead chunks can enqueue the same key repeatedly while the first task is still
     * waiting behind the shared worker. Values are write generations so stale reservations can be
     * reclaimed without waiting for their old task.
     */
    private final java.util.concurrent.ConcurrentHashMap<KeyNamespaceKey<K, N>, Long> inFlight =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Write generation: bumped on every {@link #update}, {@link #clear} and dirty flush-through.
     * Single writer (mailbox thread); the prefetch worker only reads it to abort stale batches.
     */
    private volatile long writeGen;

    // Diagnostic counters. Each field has a single writer (mailbox or the shared worker), and is
    // volatile so close() can publish an accurate per-wrapper path summary without adding atomics
    // to the ValueState hot path.
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
    private volatile long prefetchStaleAborts;
    private volatile long prefetchBuildFailures;
    private volatile long prefetchWorkerFailures;

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
                MULTIGET_CHUNK_SIZE,
                MULTIGET_MIN_BATCH_SIZE,
                stickyUpdateInPlaceEnabled);
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
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.currentKeyProvider = Objects.requireNonNull(currentKeyProvider, "currentKeyProvider");
        this.keyContextSetter = Objects.requireNonNull(keyContextSetter, "keyContextSetter");
        this.cachePolicyType = Objects.requireNonNull(cachePolicyType, "cachePolicyType");
        this.lruOverflow = Math.max(0, lruOverflow);
        this.bypassEnabled = bypassEnabled;
        this.hitRateThreshold = hitRateThreshold;
        this.hitRateWindow = hitRateWindow;
        this.multiGetPrefetchEnabled = multiGetPrefetchEnabled;
        this.multiGetChunkSize = Math.max(2, multiGetChunkSize);
        this.multiGetMinBatchSize =
                Math.max(2, Math.min(this.multiGetChunkSize, multiGetMinBatchSize));
        this.stickyUpdateInPlaceEnabled = stickyUpdateInPlaceEnabled;

        this.keySerializer = delegate.getKeySerializer();
        this.namespaceSerializer = delegate.getNamespaceSerializer();

        // L1 Cache: ~20% of maxEntries or at least 128
        int l1Size = Math.max(128, maxEntries / 5);
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
        if (!staging.isEmpty()) {
            StagedValue<V> staged = staging.remove(lookupKey);
            if (staged != null && staged.gen == writeGen) {
                prefetchValuesPromoted++;
                KeyNamespaceKey<K, N> storageKey = staged.storageKey();
                CachedValue<V> newValue = CachedValue.of(storageKey, staged.value, false);
                l1Cache.put(storageKey, newValue);
                updateSticky(storageKey, newValue);
                recordAccess(true); // Hit
                return newValue.valueOrNull();
            }
        }

        // 5. Miss -> Load from Delegate
        V loaded = delegate.value();
        recordAccess(false); // Miss

        // 6. Update L1 (Clean)
        // Even if bypassing (sampled), we populate L1 to allow hit rate recovery
        KeyNamespaceKey<K, N> storageKey = new KeyNamespaceKey<>(currentKey, currentNamespace, keySerializer,
                namespaceSerializer);
        CachedValue<V> newValue = CachedValue.of(storageKey, loaded, false);
        l1Cache.put(storageKey, newValue);

        updateSticky(storageKey, newValue);
        return loaded;
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
            // A clean L1 value may be moved to L2 by eviction. Only mutate when the sticky object
            // is still the exact value owned by L1; otherwise a shared L2 alias could become dirty.
            CachedValue<V> reusableValue =
                    stickyUpdateInPlaceEnabled
                                    && l1Cache.get(lastAccessKey) == lastAccessValue
                            ? lastAccessValue
                            : null;
            CachedValue<V> newValue;
            if (bypassEnabled && isBypassing) {
                writeGen++; // direct delegate write: staged RocksDB reads may now be stale
                delegate.update(value);
                if (reusableValue != null) {
                    reusableValue.replace(value, false);
                    return;
                }
                newValue =
                        CachedValue.of(
                                lastAccessKey, value, false); // Clean because written to delegate
            } else {
                if (reusableValue != null) {
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
            writeGen++; // direct delegate write: staged RocksDB reads may now be stale
            delegate.update(value);

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
            writeGen++; // direct delegate write: staged RocksDB reads may now be stale
            delegate.clear();
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
        } finally {
            lifecycleLock.writeLock().unlock();
        }
        if (prefetchTasksBuilt > 0 || multiGetPrefetchEnabled) {
            LOG.info(
                    "[CACHEKIT VALUE PREFETCH] delegate={} namespaceSerializer={} "
                            + "recordKeyPrefetch={} multiGet={} chunkSize={} minBatchSize={} "
                            + "tasksBuilt={} tasksExecuted={} tasksDropped={} keysPrepared={} "
                            + "keysDeduplicated={} multiGetCalls={} "
                            + "multiGetKeys={} pointGetCalls={} staged={} missingStaged={} "
                            + "promoted={} staleAborts={} buildFailures={} workerFailures={}",
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
                    prefetchStaleAborts,
                    prefetchBuildFailures,
                    prefetchWorkerFailures);
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

    long getPrefetchKeysDeduplicatedForTesting() {
        return prefetchKeysDeduplicated;
    }

    long getPrefetchTasksDroppedForTesting() {
        return prefetchTasksDropped;
    }

    /**
     * Mailbox-side half of the async prefetch: serialize (key, namespace) for every key that is
     * not already cached or staged, then hand the byte[] batch to the shared worker thread. The
     * only work on the mailbox thread is key serialization; the RocksDB reads and value
     * deserialization happen off-thread and overlap with record dispatch / backpressure waits.
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
        final V defaultValue = getBatchDefaultValue();
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
                if (inFlight.putIfAbsent(storageKey, gen) != null) {
                    prefetchKeysDeduplicated++;
                    continue;
                }
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
            releaseReservations(storageKeys, gen);
            return null; // Best-effort: an unserializable key aborts this batch only.
        }
        if (rocksDBKeys.isEmpty()) {
            return null;
        }
        final V defaultValue = batchReader.getBatchDefaultValue();
        prefetchTasksBuilt++;
        prefetchKeysPrepared += rocksDBKeys.size();
        return trackedTask(
                storageKeys,
                gen,
                () -> fetchPreparedChunksIntoStaging(rocksDBKeys, storageKeys, defaultValue, gen));
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
                    if (serializedValue == null) {
                        prefetchMissingValuesStaged++;
                    }
                    V value =
                            deserializeImmediateValueOrCopyDefault(
                                    serializedValue, defaultValue);
                    staging.put(
                            storageKeys.get(start + i),
                            new StagedValue<>(storageKeys.get(start + i), value, gen));
                    prefetchValuesStaged++;
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
            staging.remove(lookupKey, staged);
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
        return new PrefetchExecutor.DropAwareTask() {
            @Override
            public void run() {
                try {
                    task.run();
                } finally {
                    releaseReservations(reservations, gen);
                }
            }

            @Override
            public void onDrop() {
                prefetchTasksDropped++;
                releaseReservations(reservations, gen);
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
     * uses concurrently with the task thread — and parks deserialized values in {@link #staging}.
     */
    private void fetchIntoStaging(
            java.util.List<byte[]> serializedKeyAndNamespaces, V defaultValue, long gen) {
        prefetchTasksExecuted++;
        try {
            prepareWorkerState();
            if (multiGetPrefetchEnabled && delegate instanceof RocksDBBatchValueReader<?, ?, ?>) {
                fetchChunksIntoStaging(serializedKeyAndNamespaces, defaultValue, gen);
                return;
            }
            for (byte[] skn : serializedKeyAndNamespaces) {
                if (gen != writeGen) {
                    prefetchStaleAborts++;
                    return; // a write already invalidated this batch; stop wasting reads
                }
                fetchSingleIntoStaging(skn, defaultValue, gen);
            }
        } catch (Throwable ignored) {
            prefetchWorkerFailures++;
            // Best-effort cache warmup; the authoritative read path is untouched.
        }
    }

    private void prepareWorkerState() {
        if (workerValueSerializer == null) {
            workerKeySerializer = keySerializer.duplicate();
            workerNamespaceSerializer = namespaceSerializer.duplicate();
            workerValueSerializer = delegate.getValueSerializer().duplicate();
            workerKeyNamespaceInput = new org.apache.flink.core.memory.DataInputDeserializer();
            workerValueInput = new org.apache.flink.core.memory.DataInputDeserializer();
        }
        if (staging.size() > ASYNC_STAGING_MAX_ENTRIES) {
            staging.clear(); // all entries are droppable cache; also purges stale generations
        }
    }

    private void fetchPreparedChunksIntoStaging(
            java.util.List<byte[]> rocksDBKeys,
            java.util.List<KeyNamespaceKey<K, N>> storageKeys,
            V defaultValue,
            long gen) {
        prefetchTasksExecuted++;
        try {
            prepareWorkerState();
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
            stagePreparedValue(
                    storageKeys.get(start + i), serializedValue, defaultValue, gen);
        }
    }

    private void stagePreparedValue(
            KeyNamespaceKey<K, N> storageKey, byte[] valueBytes, V defaultValue, long gen)
            throws IOException {
        if (valueBytes == null) {
            prefetchMissingValuesStaged++;
        }
        V value = deserializeValueOrCopyDefault(valueBytes, defaultValue);
        staging.put(storageKey, new StagedValue<>(storageKey, value, gen));
        prefetchValuesStaged++;
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
                    fetchSingleIntoStaging(
                            serializedKeyAndNamespaces.get(i), defaultValue, gen);
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
            stageSerializedValue(
                    serializedKeyAndNamespaces.get(i), serializedValue, defaultValue, gen);
        }
    }

    private void fetchSingleIntoStaging(byte[] skn, V defaultValue, long gen) throws Exception {
        byte[] valueBytes;
        lifecycleLock.readLock().lock();
        try {
            if (closed || gen != writeGen) {
                prefetchStaleAborts++;
                return;
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
            return;
        }
        if (!closed && gen == writeGen) {
            stageSerializedValue(skn, valueBytes, defaultValue, gen);
        } else {
            prefetchStaleAborts++;
        }
    }

    private void stageSerializedValue(
            byte[] skn, byte[] valueBytes, V defaultValue, long gen) throws IOException {
        workerKeyNamespaceInput.setBuffer(skn, 0, skn.length);
        K key = workerKeySerializer.deserialize(workerKeyNamespaceInput);
        workerKeyNamespaceInput.readByte(); // magic number
        N namespace = workerNamespaceSerializer.deserialize(workerKeyNamespaceInput);
        if (valueBytes == null) {
            prefetchMissingValuesStaged++;
        }
        V value = deserializeValueOrCopyDefault(valueBytes, defaultValue);
        KeyNamespaceKey<K, N> storageKey = new KeyNamespaceKey<>(key, namespace);
        staging.put(storageKey, new StagedValue<>(storageKey, value, gen));
        prefetchValuesStaged++;
    }

    @SuppressWarnings("unchecked")
    private V getBatchDefaultValue() {
        if (delegate instanceof RocksDBBatchValueReader<?, ?, ?>) {
            return ((RocksDBBatchValueReader<K, N, V>) delegate).getBatchDefaultValue();
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
            writeGen++;
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

    /** A value fetched by the prefetch worker, tagged with the write generation at submission. */
    private static final class StagedValue<V> {
        private final KeyNamespaceKey<?, ?> storageKey;
        private final V value;
        private final long gen;

        private StagedValue(KeyNamespaceKey<?, ?> storageKey, V value, long gen) {
            this.storageKey = storageKey;
            this.value = value;
            this.gen = gen;
        }

        @SuppressWarnings("unchecked")
        private <K, N> KeyNamespaceKey<K, N> storageKey() {
            return (KeyNamespaceKey<K, N>) storageKey;
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
