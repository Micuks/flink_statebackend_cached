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
     * Lifecycle guard for the off-thread async prefetch worker. The worker reads RocksDB through
     * {@code delegate.getSerializedValue()} on the SHARED, TM-JVM-level {@code PrefetchExecutor} —
     * whose lifetime is longer than this backend's. Without a guard, a worker still holding a native
     * ColumnFamilyHandle can race {@code delegate.dispose()} ({@code closeQuietly(db)}) and SIGSEGV
     * in librocksdbjni (a native crash the worker's {@code catch (Throwable)} cannot catch). The
     * worker takes the read lock around each delegate read and bails if {@link #closed}; {@link
     * #close()} sets {@code closed} then takes the write lock as a barrier, guaranteeing no read is
     * in flight before the backend disposes the RocksDB delegate.
     */
    private volatile boolean closed = false;
    private final java.util.concurrent.locks.ReadWriteLock lifecycleLock =
            new java.util.concurrent.locks.ReentrantReadWriteLock();

    // ---- Backpressure-driven async prefetch (off-mailbox worker -> staging -> L1) ----

    /** Cap on staged entries; the worker clears the whole map beyond this (all droppable). */
    private static final int ASYNC_STAGING_MAX_ENTRIES = loadStagingMaxEntries();

    /**
     * Values fetched by the shared prefetch worker, waiting to be promoted into L1 by the mailbox
     * thread. Worker only puts; mailbox only removes/promotes. A staged entry may only be promoted
     * while {@code writeGen} still equals the generation captured at submission — any write or
     * dirty flush on this state in between makes the RocksDB read potentially stale.
     */
    private final java.util.concurrent.ConcurrentHashMap<KeyNamespaceKey<K, N>, StagedValue<V>>
            staging = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Write generation: bumped on every {@link #update}, {@link #clear} and dirty flush-through.
     * Single writer (mailbox thread); the prefetch worker only reads it to abort stale batches.
     */
    private volatile long writeGen;

    // Worker-thread-confined duplicated serializers (single shared worker thread => no races).
    private TypeSerializer<K> workerKeySerializer;
    private TypeSerializer<N> workerNamespaceSerializer;
    private TypeSerializer<V> workerValueSerializer;

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
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.currentKeyProvider = Objects.requireNonNull(currentKeyProvider, "currentKeyProvider");
        this.keyContextSetter = Objects.requireNonNull(keyContextSetter, "keyContextSetter");
        this.cachePolicyType = Objects.requireNonNull(cachePolicyType, "cachePolicyType");
        this.lruOverflow = Math.max(0, lruOverflow);
        this.bypassEnabled = bypassEnabled;
        this.hitRateThreshold = hitRateThreshold;
        this.hitRateWindow = hitRateWindow;

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
            // Sticky update needs an immutable/storage key.
            // If we found it in L1, the key in L1 IS a storage key.
            // BUT we don't have access to the entry's key directly from .get() value.
            // We have to create a new key OR look it up from entries (inefficient).
            // Actually, for sticky cache, we just need A key copy.
            // Creating a new storage key is unavoidable if we want to store it in sticky.
            KeyNamespaceKey<K, N> storageKey = new KeyNamespaceKey<>(currentKey, currentNamespace, keySerializer,
                    namespaceSerializer);
            updateSticky(storageKey, l1Cached);

            recordAccess(true); // Hit
            return l1Cached.valueOrNull();
        }

        // 4. Check L2 Cache
        CachedValue<V> l2Cached = l2Cache.get(lookupKey);
        if (l2Cached != null) {
            // Promote to L1 (Clean)
            KeyNamespaceKey<K, N> storageKey = new KeyNamespaceKey<>(currentKey, currentNamespace, keySerializer,
                    namespaceSerializer);
            CachedValue<V> newValue = CachedValue.of(l2Cached.valueOrNull(), false);
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
                KeyNamespaceKey<K, N> storageKey = new KeyNamespaceKey<>(currentKey, currentNamespace,
                        keySerializer, namespaceSerializer);
                CachedValue<V> newValue = CachedValue.of(staged.value, false);
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
        CachedValue<V> newValue = CachedValue.of(loaded, false);
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
        // If current key matches sticky key, we can update in place without new
        // allocation
        if (lastAccessKey != null && lastAccessKey.isSame(currentKey, currentNamespace)) {
            CachedValue<V> newValue;
            if (bypassEnabled && isBypassing) {
                writeGen++; // direct delegate write: staged RocksDB reads may now be stale
                delegate.update(value);
                newValue = CachedValue.of(value, false); // Clean because written to delegate
            } else {
                newValue = CachedValue.of(value, true); // Dirty
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
            CachedValue<V> newValue = CachedValue.of(value, false); // Clean
            l1Cache.put(cacheKey, newValue);
            updateSticky(cacheKey, newValue);
            return;
        }

        KeyNamespaceKey<K, N> cacheKey = new KeyNamespaceKey<>(currentKey, currentNamespace, keySerializer,
                namespaceSerializer);
        CachedValue<V> newValue = CachedValue.of(value, true);

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
            newValue = CachedValue.of(null, false);
        } else if (existing != null && existing.isNull && !existing.dirty) {
            // Known clean null: avoid scheduling an extra delete.
            newValue = existing;
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

    /**
     * Mailbox-side half of the async prefetch: serialize (key, namespace) for every key that is
     * not already cached or staged, then hand the byte[] batch to the shared worker thread. The
     * only work on the mailbox thread is key serialization; the RocksDB reads and value
     * deserialization happen off-thread and overlap with record dispatch / backpressure waits.
     *
     * @return a worker task to run via PrefetchExecutor, or null if there is nothing to fetch.
     */
    /**
     * Quiesce the async prefetch for this state before the backend disposes its RocksDB delegate.
     * Sets {@link #closed} so no new work touches the delegate, then takes the write lock as a
     * barrier so any in-flight worker read on the shared PrefetchExecutor has drained. After this
     * returns it is safe for the backend to call {@code delegate.dispose()} / {@code close()}.
     */
    public void close() {
        closed = true;
        lifecycleLock.writeLock().lock();
        lifecycleLock.writeLock().unlock();
    }

    public Runnable buildAsyncPrefetchTask(Iterable<? extends K> keys) {
        if (closed || keys == null || currentNamespace == null) {
            return null;
        }
        java.util.ArrayList<byte[]> serialized = new java.util.ArrayList<>();
        try {
            org.apache.flink.core.memory.DataOutputSerializer out =
                    new org.apache.flink.core.memory.DataOutputSerializer(64);
            for (K key : keys) {
                if (key == null || findCachedValueFor(key, currentNamespace) != null) {
                    continue;
                }
                setLookupKey(key, currentNamespace);
                if (staging.containsKey(lookupKey)) {
                    continue;
                }
                out.clear();
                keySerializer.serialize(key, out);
                out.writeByte(42); // KvStateSerializer.MAGIC_NUMBER wire format
                namespaceSerializer.serialize(currentNamespace, out);
                serialized.add(out.getCopyOfBuffer());
            }
        } catch (Throwable t) {
            return null; // Best-effort: an unserializable key aborts this batch only.
        }
        if (serialized.isEmpty()) {
            return null;
        }
        final long gen = writeGen;
        return () -> fetchIntoStaging(serialized, gen);
    }

    /**
     * Worker-side half: runs on the single shared prefetch thread. Reads RocksDB through the
     * delegate's {@code getSerializedValue} — the same thread-safe path Flink's queryable state
     * uses concurrently with the task thread — and parks deserialized values in {@link #staging}.
     */
    private void fetchIntoStaging(java.util.List<byte[]> serializedKeyAndNamespaces, long gen) {
        try {
            if (workerValueSerializer == null) {
                workerKeySerializer = keySerializer.duplicate();
                workerNamespaceSerializer = namespaceSerializer.duplicate();
                workerValueSerializer = delegate.getValueSerializer().duplicate();
            }
            if (staging.size() > ASYNC_STAGING_MAX_ENTRIES) {
                staging.clear(); // all entries are droppable cache; also purges stale generations
            }
            for (byte[] skn : serializedKeyAndNamespaces) {
                if (gen != writeGen) {
                    return; // a write already invalidated this batch; stop wasting reads
                }
                byte[] valueBytes;
                lifecycleLock.readLock().lock();
                try {
                    if (closed) {
                        return; // backend is disposing — never touch the delegate's RocksDB handles
                    }
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
                if (valueBytes == null) {
                    // Absent key: let the authoritative read apply default-value semantics.
                    continue;
                }
                org.apache.flink.core.memory.DataInputDeserializer in =
                        new org.apache.flink.core.memory.DataInputDeserializer(
                                skn, 0, skn.length);
                K key = workerKeySerializer.deserialize(in);
                in.readByte(); // magic number
                N namespace = workerNamespaceSerializer.deserialize(in);
                org.apache.flink.core.memory.DataInputDeserializer valueIn =
                        new org.apache.flink.core.memory.DataInputDeserializer(
                                valueBytes, 0, valueBytes.length);
                V value = workerValueSerializer.deserialize(valueIn);
                staging.put(
                        new KeyNamespaceKey<>(key, namespace),
                        new StagedValue<>(value, gen));
            }
        } catch (Throwable ignored) {
            // Best-effort cache warmup; the authoritative read path is untouched.
        }
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
                l1Cache.put(storageKey, CachedValue.of(loaded, false));
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
            l2Cache.put(key, CachedValue.of(value.valueOrNull(), false));

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
            return Objects.hash(key, namespace);
        }
    }

    /** A value fetched by the prefetch worker, tagged with the write generation at submission. */
    private static final class StagedValue<V> {
        private final V value;
        private final long gen;

        private StagedValue(V value, long gen) {
            this.value = value;
            this.gen = gen;
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
