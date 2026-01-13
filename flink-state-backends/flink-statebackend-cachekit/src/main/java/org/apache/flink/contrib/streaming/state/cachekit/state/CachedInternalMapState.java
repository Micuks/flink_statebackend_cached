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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicy;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CaffeineCachePolicy;
import org.apache.flink.contrib.streaming.state.cachekit.cache.LruCachePolicy;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PresenceCacheImplementation;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PrimitivePresenceCache;
import org.apache.flink.contrib.streaming.state.cachekit.util.MurmurHash3;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import javax.annotation.Nonnull;

import java.io.BufferedWriter;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * Minimal {@link InternalMapState} wrapper that adds a per-state key presence cache.
 *
 * <p>
 * Keying: (currentKey, namespace, userKey).
 */
public final class CachedInternalMapState<K, N, UK, UV> implements InternalMapState<K, N, UK, UV> {

    private final InternalMapState<K, N, UK, UV> delegate;
    private final CurrentKeyProvider<K> currentKeyProvider;
    private final CachePolicy<KeyNamespaceUserKey<K, N, UK>, Boolean> l1PresenceCache;
    private final CachePolicy<KeyNamespaceUserKey<K, N, UK>, Boolean> l2PresenceCache;
    private final CachePolicy<Long, Byte> l1PrimitivePresenceCache;
    private final CachePolicy<Long, Byte> l2PrimitivePresenceCache;
    private final CachePolicyType cachePolicyType;
    private final int lruOverflow;
    private final boolean presenceCacheEnabled;
    private final PresenceCacheImplementation presenceCacheImplementation;
    private final boolean usePrimitivePresenceCache;
    private final TypeSerializer<K> keySerializer;
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<UK> userKeySerializer;
    private final ThreadLocal<DataOutputSerializer> serializerView;
    private final Counter getCalls;
    private final Counter putCalls;
    private final Counter putAllCalls;
    private final Counter removeCalls;
    private final Counter containsCalls;
    private final Counter entriesCalls;
    private final Counter keysCalls;
    private final Counter valuesCalls;
    private final Counter iteratorCalls;
    private final Counter clearCalls;
    private final Counter getSerializedValueCalls;
    private final Counter delegateGetCalls;
    private final Counter delegatePutCalls;
    private final Counter delegatePutAllCalls;
    private final Counter delegateRemoveCalls;
    private final Counter delegateContainsCalls;
    private final Counter delegateClearCalls;
    private final Counter delegateGetMisses;
    private final Counter delegateContainsMisses;
    private final Counter presenceLookups;
    private final Counter presenceHits;
    private final Counter presenceMisses;
    private final Counter presenceHitPresent;
    private final Counter presenceHitAbsent;
    private final Counter presenceUpdates;
    private final Counter getShortCircuits;
    private final Counter containsShortCircuits;
    private final Counter getAbsentShortCircuits;
    private final Counter containsPresentShortCircuits;
    private final Counter containsAbsentShortCircuits;
    private final Counter presencePresentDelegateCalls;
    private final Counter presencePresentDelegateHits;
    private final Counter presencePresentDelegateMisses;
    private final Counter l1Evictions;
    private final Counter l2Evictions;
    private final KeyAccessStats<KeyNamespaceUserKey<K, N, UK>> keyAccessStats;
    private final KeyAccessStats<K> globalKeyAccessStats;
    private final MapAccessLogger<K, N, UK> accessLogger;

    private N currentNamespace;

    public CachedInternalMapState(
            InternalMapState<K, N, UK, UV> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            int maxEntries,
            CachePolicyType cachePolicyType,
            int lruOverflow,
            PresenceCacheImplementation presenceCacheImplementation) {
        this(
                delegate,
                currentKeyProvider,
                maxEntries,
                cachePolicyType,
                lruOverflow,
                presenceCacheImplementation,
                null,
                null,
                null,
                maxEntries,
                null,
                null,
                null);
    }

    public CachedInternalMapState(
            InternalMapState<K, N, UK, UV> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            int maxEntries,
            CachePolicyType cachePolicyType,
            int lruOverflow,
            PresenceCacheImplementation presenceCacheImplementation,
            MetricGroup metricGroup,
            String stateName,
            KeyAccessStats<K> globalKeyAccessStats,
            int keyStatsWindow,
            String keyLogDir,
            String operatorIdentifier,
            String taskNameWithSubtasks) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.currentKeyProvider = Objects.requireNonNull(currentKeyProvider, "currentKeyProvider");
        this.cachePolicyType = Objects.requireNonNull(cachePolicyType, "cachePolicyType");
        this.lruOverflow = Math.max(0, lruOverflow);
        this.presenceCacheEnabled = maxEntries > 0;
        this.presenceCacheImplementation = Objects.requireNonNull(
                presenceCacheImplementation, "presenceCacheImplementation");
        this.keySerializer = delegate.getKeySerializer();
        this.namespaceSerializer = delegate.getNamespaceSerializer();
        TypeSerializer<UK> resolvedUserKeySerializer = null;
        TypeSerializer<Map<UK, UV>> valueSerializer = delegate.getValueSerializer();
        if (valueSerializer instanceof MapSerializer) {
            resolvedUserKeySerializer = ((MapSerializer<UK, UV>) valueSerializer).getKeySerializer();
        }
        this.userKeySerializer = resolvedUserKeySerializer;
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
                this.l1PrimitivePresenceCache = new PrimitivePresenceCache(
                        l1Size, this::onL1PrimitiveEviction);
                this.l2PrimitivePresenceCache = new PrimitivePresenceCache(
                        maxEntries, this::onL2PrimitiveEviction);
                this.l1PresenceCache = new NoOpCachePolicy<>();
                this.l2PresenceCache = new NoOpCachePolicy<>();
            } else {
                this.l1PresenceCache = createCachePolicy(l1Size, this::onL1Eviction);
                this.l2PresenceCache = createCachePolicy(maxEntries, this::onL2Eviction);
                this.l1PrimitivePresenceCache = new NoOpCachePolicy<>();
                this.l2PrimitivePresenceCache = new NoOpCachePolicy<>();
            }
        } else {
            this.l1PresenceCache = new NoOpCachePolicy<>();
            this.l2PresenceCache = new NoOpCachePolicy<>();
            this.l1PrimitivePresenceCache = new NoOpCachePolicy<>();
            this.l2PrimitivePresenceCache = new NoOpCachePolicy<>();
        }

        MetricGroup stateMetrics = null;
        if (metricGroup != null && stateName != null) {
            stateMetrics = metricGroup.addGroup("map_state").addGroup(stateName);
        }

        if (stateMetrics != null) {
            MetricGroup callGroup = stateMetrics.addGroup("calls");
            getCalls = callGroup.counter("get");
            putCalls = callGroup.counter("put");
            putAllCalls = callGroup.counter("put_all");
            removeCalls = callGroup.counter("remove");
            containsCalls = callGroup.counter("contains");
            entriesCalls = callGroup.counter("entries");
            keysCalls = callGroup.counter("keys");
            valuesCalls = callGroup.counter("values");
            iteratorCalls = callGroup.counter("iterator");
            clearCalls = callGroup.counter("clear");
            getSerializedValueCalls = callGroup.counter("get_serialized_value");

            MetricGroup delegateGroup = stateMetrics.addGroup("delegate");
            delegateGetCalls = delegateGroup.counter("get");
            delegatePutCalls = delegateGroup.counter("put");
            delegatePutAllCalls = delegateGroup.counter("put_all");
            delegateRemoveCalls = delegateGroup.counter("remove");
            delegateContainsCalls = delegateGroup.counter("contains");
            delegateClearCalls = delegateGroup.counter("clear");
            delegateGetMisses = delegateGroup.counter("get_miss");
            delegateContainsMisses = delegateGroup.counter("contains_miss");

            MetricGroup presenceGroup = stateMetrics.addGroup("presence");
            presenceLookups = presenceGroup.counter("lookups");
            presenceHits = presenceGroup.counter("hits");
            presenceMisses = presenceGroup.counter("misses");
            presenceHitPresent = presenceGroup.counter("hit_present");
            presenceHitAbsent = presenceGroup.counter("hit_absent");
            presenceUpdates = presenceGroup.counter("updates");
            getShortCircuits = presenceGroup.counter("get_short_circuit");
            containsShortCircuits = presenceGroup.counter("contains_short_circuit");
            getAbsentShortCircuits = presenceGroup.counter("get_absent_short_circuit");
            containsPresentShortCircuits = presenceGroup.counter("contains_present_short_circuit");
            containsAbsentShortCircuits = presenceGroup.counter("contains_absent_short_circuit");
            presencePresentDelegateCalls = presenceGroup.counter("present_delegate_calls");
            presencePresentDelegateHits = presenceGroup.counter("present_delegate_hits");
            presencePresentDelegateMisses = presenceGroup.counter("present_delegate_misses");
            l1Evictions = presenceGroup.counter("l1_evictions");
            l2Evictions = presenceGroup.counter("l2_evictions");
            presenceGroup.gauge("l1_size", this::l1PresenceSize);
            presenceGroup.gauge("l2_size", this::l2PresenceSize);
            presenceGroup.gauge(
                    "hit_ratio",
                    () -> ratio(presenceHits, presenceLookups));
            presenceGroup.gauge(
                    "absent_hit_ratio",
                    () -> ratio(presenceHitAbsent, presenceLookups));
            presenceGroup.gauge(
                    "get_short_circuit_ratio",
                    () -> ratio(getShortCircuits, getCalls));
            presenceGroup.gauge(
                    "contains_short_circuit_ratio",
                    () -> ratio(containsShortCircuits, containsCalls));

            MetricGroup keyGroup = stateMetrics.addGroup("keys");
            keyAccessStats = new KeyAccessStats<>(Math.max(1, keyStatsWindow));
            keyGroup.gauge("total_accesses", () -> keyAccessStats.getWindowedAccesses());
            keyGroup.gauge("total_unique_keys", () -> keyAccessStats.getWindowedUniqueKeys());
            keyGroup.gauge("total_repeat_ratio", () -> keyAccessStats.getWindowedRepeatRatio());
            keyGroup.gauge("total_unique_ratio", () -> keyAccessStats.getWindowedUniqueRatio());
        } else {
            getCalls = null;
            putCalls = null;
            putAllCalls = null;
            removeCalls = null;
            containsCalls = null;
            entriesCalls = null;
            keysCalls = null;
            valuesCalls = null;
            iteratorCalls = null;
            clearCalls = null;
            getSerializedValueCalls = null;
            delegateGetCalls = null;
            delegatePutCalls = null;
            delegatePutAllCalls = null;
            delegateRemoveCalls = null;
            delegateContainsCalls = null;
            delegateClearCalls = null;
            delegateGetMisses = null;
            delegateContainsMisses = null;
            presenceLookups = null;
            presenceHits = null;
            presenceMisses = null;
            presenceHitPresent = null;
            presenceHitAbsent = null;
            presenceUpdates = null;
            getShortCircuits = null;
            containsShortCircuits = null;
            getAbsentShortCircuits = null;
            containsPresentShortCircuits = null;
            containsAbsentShortCircuits = null;
            presencePresentDelegateCalls = null;
            presencePresentDelegateHits = null;
            presencePresentDelegateMisses = null;
            l1Evictions = null;
            l2Evictions = null;
            keyAccessStats = null;
        }
        this.globalKeyAccessStats = globalKeyAccessStats;
        this.accessLogger = MapAccessLogger.create(
                keyLogDir,
                operatorIdentifier,
                taskNameWithSubtasks,
                stateName);
    }

    @Override
    public UV get(UK userKey) throws Exception {
        if (getCalls != null) {
            getCalls.inc();
        }
        if (userKey == null) {
            return null;
        }
        ensureDelegateNamespace();
        recordKeyAccess(userKey);
        Boolean present = null;
        if (presenceCacheEnabled) {
            present = getPresence(userKey);
            if (present != null && !present) {
                if (getShortCircuits != null) {
                    getShortCircuits.inc();
                }
                if (getAbsentShortCircuits != null) {
                    getAbsentShortCircuits.inc();
                }
                logAccess("get", userKey, present, false, false);
                return null;
            }
        }
        UV value = delegate.get(userKey);
        if (delegateGetCalls != null) {
            delegateGetCalls.inc();
        }
        if (Boolean.TRUE.equals(present) && presencePresentDelegateCalls != null) {
            presencePresentDelegateCalls.inc();
            if (value != null) {
                if (presencePresentDelegateHits != null) {
                    presencePresentDelegateHits.inc();
                }
            } else if (presencePresentDelegateMisses != null) {
                presencePresentDelegateMisses.inc();
            }
        }
        if (value == null && delegateGetMisses != null) {
            delegateGetMisses.inc();
        }
        if (presenceCacheEnabled) {
            updatePresence(userKey, value != null);
        }
        logAccess("get", userKey, present, true, value != null);
        return value;
    }

    @Override
    public void put(UK userKey, UV userValue) throws Exception {
        if (putCalls != null) {
            putCalls.inc();
        }
        if (userKey == null) {
            return;
        }
        ensureDelegateNamespace();
        recordKeyAccess(userKey);
        if (userValue == null) {
            remove(userKey);
            return;
        }
        delegate.put(userKey, userValue);
        if (delegatePutCalls != null) {
            delegatePutCalls.inc();
        }
        if (presenceCacheEnabled) {
            updatePresence(userKey, true);
        }
        logAccess("put", userKey, null, true, true);
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        if (putAllCalls != null) {
            putAllCalls.inc();
        }
        if (map == null || map.isEmpty()) {
            return;
        }
        ensureDelegateNamespace();
        delegate.putAll(map);
        if (delegatePutAllCalls != null) {
            delegatePutAllCalls.inc();
        }
        if (presenceCacheEnabled || accessLogger != null || keyAccessStats != null || globalKeyAccessStats != null) {
            for (Map.Entry<UK, UV> entry : map.entrySet()) {
                if (entry.getKey() == null) {
                    continue;
                }
                recordKeyAccess(entry.getKey());
                if (presenceCacheEnabled) {
                    updatePresence(entry.getKey(), entry.getValue() != null);
                }
                logAccess("put_all", entry.getKey(), null, true, entry.getValue() != null);
            }
        }
    }

    @Override
    public void remove(UK userKey) throws Exception {
        if (removeCalls != null) {
            removeCalls.inc();
        }
        if (userKey == null) {
            return;
        }
        ensureDelegateNamespace();
        recordKeyAccess(userKey);
        delegate.remove(userKey);
        if (delegateRemoveCalls != null) {
            delegateRemoveCalls.inc();
        }
        if (presenceCacheEnabled) {
            updatePresence(userKey, false);
        }
        logAccess("remove", userKey, null, true, false);
    }

    @Override
    public boolean contains(UK userKey) throws Exception {
        if (containsCalls != null) {
            containsCalls.inc();
        }
        if (userKey == null) {
            return false;
        }
        ensureDelegateNamespace();
        recordKeyAccess(userKey);
        if (presenceCacheEnabled) {
            Boolean present = getPresence(userKey);
            if (present != null) {
                if (containsShortCircuits != null) {
                    containsShortCircuits.inc();
                }
                if (present && containsPresentShortCircuits != null) {
                    containsPresentShortCircuits.inc();
                } else if (!present && containsAbsentShortCircuits != null) {
                    containsAbsentShortCircuits.inc();
                }
                logAccess("contains", userKey, present, false, present);
                return present;
            }
        }
        boolean exists = delegate.contains(userKey);
        if (delegateContainsCalls != null) {
            delegateContainsCalls.inc();
        }
        if (!exists && delegateContainsMisses != null) {
            delegateContainsMisses.inc();
        }
        if (presenceCacheEnabled) {
            updatePresence(userKey, exists);
        }
        logAccess("contains", userKey, null, true, exists);
        return exists;
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        if (entriesCalls != null) {
            entriesCalls.inc();
        }
        ensureDelegateNamespace();
        return delegate.entries();
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        if (keysCalls != null) {
            keysCalls.inc();
        }
        ensureDelegateNamespace();
        return delegate.keys();
    }

    @Override
    public Iterable<UV> values() throws Exception {
        if (valuesCalls != null) {
            valuesCalls.inc();
        }
        ensureDelegateNamespace();
        return delegate.values();
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        if (iteratorCalls != null) {
            iteratorCalls.inc();
        }
        ensureDelegateNamespace();
        return delegate.iterator();
    }

    @Override
    public boolean isEmpty() throws Exception {
        ensureDelegateNamespace();
        return delegate.isEmpty();
    }

    @Override
    public void clear() {
        if (clearCalls != null) {
            clearCalls.inc();
        }
        ensureDelegateNamespace();
        delegate.clear();
        if (delegateClearCalls != null) {
            delegateClearCalls.inc();
        }
        clearPresenceCaches();
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
        if (getSerializedValueCalls != null) {
            getSerializedValueCalls.inc();
        }
        ensureDelegateNamespace();
        return delegate.getSerializedValue(
                serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer, safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, Map<UK, UV>> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        ensureDelegateNamespace();
        return delegate.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    private void ensureDelegateNamespace() {
        if (currentNamespace != null) {
            delegate.setCurrentNamespace(currentNamespace);
        }
    }

    private Boolean getPresence(UK userKey) {
        K currentKey = currentKeyProvider.getCurrentKey();
        if (currentKey == null || currentNamespace == null) {
            return null;
        }
        if (presenceLookups != null) {
            presenceLookups.inc();
        }
        if (usePrimitivePresenceCache) {
            long fp = fingerprint(currentKey, currentNamespace, userKey);
            Byte l1 = l1PrimitivePresenceCache.get(fp);
            if (l1 != null) {
                if (l1 == PrimitivePresenceCache.ABSENT) {
                    recordPresenceHit(false);
                    return false;
                }
                l1PrimitivePresenceCache.remove(fp);
                recordPresenceMiss();
            }
            Byte l2 = l2PrimitivePresenceCache.get(fp);
            if (l2 != null) {
                if (l2 == PrimitivePresenceCache.ABSENT) {
                    l1PrimitivePresenceCache.put(fp, l2);
                    recordPresenceHit(false);
                    return false;
                }
                l2PrimitivePresenceCache.remove(fp);
                recordPresenceMiss();
            }
            recordPresenceMiss();
            return null;
        }

        KeyNamespaceUserKey<K, N, UK> probe =
                new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey, false);
        Boolean present = l1PresenceCache.get(probe);
        if (present != null) {
            if (!present) {
                recordPresenceHit(false);
                return false;
            }
            KeyNamespaceUserKey<K, N, UK> storage =
                    new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey, true);
            l1PresenceCache.remove(storage);
            recordPresenceMiss();
            return null;
        }
        present = l2PresenceCache.get(probe);
        if (present != null) {
            KeyNamespaceUserKey<K, N, UK> storage =
                    new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey, true);
            if (!present) {
                l1PresenceCache.put(storage, false);
                recordPresenceHit(false);
                return false;
            }
            l2PresenceCache.remove(storage);
            recordPresenceMiss();
        }
        if (present == null) {
            recordPresenceMiss();
        }
        return null;
    }

    private void updatePresence(UK userKey, boolean present) {
        K currentKey = currentKeyProvider.getCurrentKey();
        if (currentKey == null || currentNamespace == null) {
            return;
        }
        if (presenceUpdates != null) {
            presenceUpdates.inc();
        }
        if (usePrimitivePresenceCache) {
            long fp = fingerprint(currentKey, currentNamespace, userKey);
            if (present) {
                l1PrimitivePresenceCache.remove(fp);
                l2PrimitivePresenceCache.remove(fp);
                return;
            }
            l2PrimitivePresenceCache.remove(fp);
            l1PrimitivePresenceCache.put(fp, PrimitivePresenceCache.ABSENT);
            return;
        }
        KeyNamespaceUserKey<K, N, UK> storage =
                new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey, true);
        if (present) {
            l1PresenceCache.remove(storage);
            l2PresenceCache.remove(storage);
            return;
        }
        l2PresenceCache.remove(storage);
        l1PresenceCache.put(storage, false);
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

    private CachePolicy<KeyNamespaceUserKey<K, N, UK>, Boolean> createCachePolicy(
            int maxEntries,
            java.util.function.BiConsumer<KeyNamespaceUserKey<K, N, UK>, Boolean> evictionListener) {
        if (cachePolicyType == CachePolicyType.CAFFEINE) {
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
        if (key == null || value == null || value) {
            return;
        }
        if (l1Evictions != null) {
            l1Evictions.inc();
        }
        l2PresenceCache.put(key, value);
    }

    private void onL2Eviction(KeyNamespaceUserKey<K, N, UK> key, Boolean value) {
        if (l2Evictions != null) {
            l2Evictions.inc();
        }
        // Presence cache doesn't need flush on L2 eviction.
    }

    private void onL1PrimitiveEviction(Long key, Byte value) {
        if (key == null || value == null || value.byteValue() != PrimitivePresenceCache.ABSENT) {
            return;
        }
        if (l1Evictions != null) {
            l1Evictions.inc();
        }
        l2PrimitivePresenceCache.put(key, value);
    }

    private void onL2PrimitiveEviction(Long key, Byte value) {
        if (l2Evictions != null) {
            l2Evictions.inc();
        }
        // Presence cache doesn't need flush on L2 eviction.
    }

    private int l1PresenceSize() {
        if (!presenceCacheEnabled) {
            return 0;
        }
        if (usePrimitivePresenceCache) {
            return l1PrimitivePresenceCache.size();
        }
        return l1PresenceCache.size();
    }

    private int l2PresenceSize() {
        if (!presenceCacheEnabled) {
            return 0;
        }
        if (usePrimitivePresenceCache) {
            return l2PrimitivePresenceCache.size();
        }
        return l2PresenceCache.size();
    }

    private void recordPresenceHit(boolean present) {
        if (presenceHits != null) {
            presenceHits.inc();
        }
        if (present) {
            if (presenceHitPresent != null) {
                presenceHitPresent.inc();
            }
        } else if (presenceHitAbsent != null) {
            presenceHitAbsent.inc();
        }
    }

    private void recordPresenceMiss() {
        if (presenceMisses != null) {
            presenceMisses.inc();
        }
    }

    private void recordKeyAccess(UK userKey) {
        if (userKey == null) {
            return;
        }
        K currentKey = currentKeyProvider.getCurrentKey();
        if (keyAccessStats != null && currentKey != null && currentNamespace != null) {
            KeyNamespaceUserKey<K, N, UK> statsKey =
                    new KeyNamespaceUserKey<>(currentKey, currentNamespace, userKey, true);
            keyAccessStats.record(statsKey);
        }
        if (globalKeyAccessStats != null && currentKey != null) {
            globalKeyAccessStats.record(currentKey);
        }
    }

    private void logAccess(
            String op,
            UK userKey,
            Boolean presence,
            boolean delegateUsed,
            Boolean result) {
        if (accessLogger == null) {
            return;
        }
        K currentKey = currentKeyProvider.getCurrentKey();
        accessLogger.log(
                op,
                currentKey,
                currentNamespace,
                userKey,
                presenceStatus(presence),
                delegateUsed,
                result);
    }

    private static String presenceStatus(Boolean presence) {
        if (presence == null) {
            return "MISS";
        }
        return presence ? "HIT_PRESENT" : "HIT_ABSENT";
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

    private static final class MapAccessLogger<K, N, UK> {
        private final BufferedWriter writer;

        private MapAccessLogger(BufferedWriter writer) {
            this.writer = writer;
        }

        static <K, N, UK> MapAccessLogger<K, N, UK> create(
                String baseDir,
                String operatorIdentifier,
                String taskNameWithSubtasks,
                String stateName) {
            if (baseDir == null || baseDir.isBlank() || stateName == null) {
                return null;
            }
            String task = sanitize(taskNameWithSubtasks);
            String operator = sanitize(operatorIdentifier);
            String state = sanitize(stateName);
            java.nio.file.Path dir = java.nio.file.Paths.get(baseDir, task, operator);
            java.nio.file.Path file = dir.resolve(state + ".map.log");
            try {
                java.nio.file.Files.createDirectories(dir);
                BufferedWriter bw = java.nio.file.Files.newBufferedWriter(
                        file,
                        java.nio.charset.StandardCharsets.UTF_8,
                        java.nio.file.StandardOpenOption.CREATE,
                        java.nio.file.StandardOpenOption.APPEND);
                return new MapAccessLogger<>(bw);
            } catch (Exception e) {
                return null;
            }
        }

        void log(
                String op,
                K key,
                N namespace,
                UK userKey,
                String presence,
                boolean delegateUsed,
                Boolean result) {
            if (writer == null) {
                return;
            }
            try {
                long ts = System.currentTimeMillis();
                writer.write(Long.toString(ts));
                writer.write('\t');
                writer.write(String.valueOf(op));
                writer.write('\t');
                writer.write(String.valueOf(key));
                writer.write('\t');
                writer.write(String.valueOf(namespace));
                writer.write('\t');
                writer.write(String.valueOf(userKey));
                writer.write('\t');
                writer.write(String.valueOf(presence));
                writer.write('\t');
                writer.write(Boolean.toString(delegateUsed));
                writer.write('\t');
                writer.write(String.valueOf(result));
                writer.write('\n');
            } catch (Exception e) {
                // best-effort logging only
            }
        }

        void close() {
            if (writer == null) {
                return;
            }
            try {
                writer.flush();
                writer.close();
            } catch (Exception e) {
                // best-effort close
            }
        }

        private static String sanitize(String value) {
            if (value == null || value.isEmpty()) {
                return "unknown";
            }
            String sanitized = value.replaceAll("[^A-Za-z0-9._-]", "_");
            return sanitized.isEmpty() ? "unknown" : sanitized;
        }
    }

    public void close() {
        if (accessLogger != null) {
            accessLogger.close();
        }
    }

    private static final class KeyNamespaceUserKey<K, N, UK> {
        private final K key;
        private final N namespace;
        private final UK userKey;

        private KeyNamespaceUserKey(K key, N namespace, UK userKey, boolean deepCopy) {
            if (deepCopy && key instanceof BinaryRowData) {
                this.key = (K) ((BinaryRowData) key).copy();
            } else {
                this.key = key;
            }
            if (deepCopy && namespace instanceof BinaryRowData) {
                this.namespace = (N) ((BinaryRowData) namespace).copy();
            } else {
                this.namespace = namespace;
            }
            if (deepCopy && userKey instanceof BinaryRowData) {
                this.userKey = (UK) ((BinaryRowData) userKey).copy();
            } else {
                this.userKey = userKey;
            }
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
            return Objects.hash(key, namespace, userKey);
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
        public void clear() {
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Iterable<Map.Entry<K, V>> entries() {
            return Collections.emptyList();
        }
    }
}
