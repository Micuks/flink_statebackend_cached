/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.dataview;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * A batch-scoped read-your-writes overlay for a DISTINCT aggregate's {@link StateMapView}.
 *
 * <p>Flink's generated exact-DISTINCT code performs one {@code get} and, when the bit mask changes,
 * one {@code put} for every input record. Local pre-aggregation already invokes all records for one
 * outer key together. This view uses that exact lifetime to collapse repeated accesses to the same
 * distinct key and commits only the final values through {@link StateMapView#putAll(Map)}.
 *
 * <p>The overlay is deliberately not a long-lived cache: it is empty and inactive outside one
 * successful {@code processBatchForKey} call. Null keys retain the established nullable-key state
 * path because their value is stored in a separate {@code ValueState}. Iterator-style operations
 * flush the overlay before delegating, preserving the complete MapView contract even though the
 * generated DISTINCT hot path uses only get/put/remove.
 */
@Internal
final class DistinctBatchStateMapView<N, EK, EV> extends StateMapView<N, EK, EV>
        implements BatchPrefetchableMapView<EK> {

    private final StateMapView<N, EK, EV> delegate;
    private final TypeSerializer<EK> keySerializer;
    private final TypeSerializer<EV> valueSerializer;
    private final boolean copyKeys;
    private final boolean copyValues;
    private final boolean prefetchEnabled;
    private final int minPrefetchUniqueKeys;
    private final boolean flatOverlayEnabled;
    private HashMap<EK, BufferedValue<EK, EV>> overlay = new HashMap<>();
    private final FlatOverlay<EK, EV> flatOverlay;
    private final ArrayList<EK> pendingPrefetchKeys = new ArrayList<>();
    private final HashSet<EK> pendingPrefetchKeySet = new HashSet<>();
    private final PreparedPrefetch<EK, EV> noOpPreparedPrefetch;

    private boolean active;
    private boolean prefetchActive;
    private boolean collectingPrefetchKeys;
    private boolean directValuesPrimed;
    private long logicalGets;
    private long delegateGets;
    private long overlayHits;
    private long logicalPuts;
    private long logicalRemoves;
    private long committedEntries;
    private long committedBatches;
    private long abortedBatches;
    private long forcedFlushes;
    private long prefetchCollections;
    private long prefetchKeysCollected;
    private long prefetchRejectedBelowMinimum;
    private long prefetchRejectedBeforeKeyScan;
    private long directOverlayValues;
    private long prefetchSize2;
    private long prefetchSize3;
    private long prefetchSize4To7;
    private long prefetchSize8To15;
    private long prefetchSize16Plus;
    private long flatOverlayBatches;
    private long flatOverlayLookups;
    private long flatOverlayHits;
    private long flatOverlayInsertions;
    private long flatOverlayResizes;
    private long flatOverlayCapacityFallbacks;
    private long flatOverlayFallbackBatches;
    private long flatOverlayCommittedWrites;
    private long flatOverlayCommittedRemoves;
    private long flatOverlayPeakEntries;
    private boolean legacyFallbackActive;

    DistinctBatchStateMapView(
            StateMapView<N, EK, EV> delegate,
            TypeSerializer<EK> keySerializer,
            TypeSerializer<EV> valueSerializer) {
        this(delegate, keySerializer, valueSerializer, true, 2);
    }

    DistinctBatchStateMapView(
            StateMapView<N, EK, EV> delegate,
            TypeSerializer<EK> keySerializer,
            TypeSerializer<EV> valueSerializer,
            int minPrefetchUniqueKeys) {
        this(delegate, keySerializer, valueSerializer, true, minPrefetchUniqueKeys);
    }

    DistinctBatchStateMapView(
            StateMapView<N, EK, EV> delegate,
            TypeSerializer<EK> keySerializer,
            TypeSerializer<EV> valueSerializer,
            boolean prefetchEnabled,
            int minPrefetchUniqueKeys) {
        this(
                delegate,
                keySerializer,
                valueSerializer,
                prefetchEnabled,
                minPrefetchUniqueKeys,
                false,
                1024);
    }

    DistinctBatchStateMapView(
            StateMapView<N, EK, EV> delegate,
            TypeSerializer<EK> keySerializer,
            TypeSerializer<EV> valueSerializer,
            boolean prefetchEnabled,
            int minPrefetchUniqueKeys,
            boolean flatOverlayEnabled,
            int flatOverlayMaxEntries) {
        this.delegate = delegate;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.copyKeys = !keySerializer.isImmutableType();
        this.copyValues = !valueSerializer.isImmutableType();
        this.prefetchEnabled = prefetchEnabled;
        this.minPrefetchUniqueKeys = Math.max(2, minPrefetchUniqueKeys);
        this.flatOverlayEnabled = flatOverlayEnabled;
        this.flatOverlay =
                flatOverlayEnabled ? new FlatOverlay<>(Math.max(16, flatOverlayMaxEntries)) : null;
        this.noOpPreparedPrefetch =
                new PreparedPrefetch<>(this, Collections.emptyList(), null, true);
    }

    void beginBatch() {
        if (active || (!overlayIsEmpty() && !directValuesPrimed)) {
            throw new IllegalStateException("DISTINCT batch overlay is already active");
        }
        active = true;
        if (useFlatOverlay()) {
            flatOverlayBatches++;
        }
    }

    void commitBatch() throws Exception {
        if (!active) {
            return;
        }
        try {
            flushOverlay(false);
            committedBatches++;
        } finally {
            resetOverlayScope();
            active = false;
            directValuesPrimed = false;
            endPrefetchScope();
        }
    }

    void abortBatch() {
        if (active) {
            abortedBatches++;
        }
        resetOverlayScope();
        active = false;
        directValuesPrimed = false;
        endPrefetchScope();
    }

    boolean isBatchActive() {
        return active;
    }

    @Override
    public void beginPrefetchKeyCollection(int expectedKeys) {
        if (active || prefetchActive || collectingPrefetchKeys) {
            throw new IllegalStateException("DISTINCT batch prefetch is already active");
        }
        pendingPrefetchKeys.clear();
        pendingPrefetchKeySet.clear();
        pendingPrefetchKeys.ensureCapacity(Math.max(0, expectedKeys));
        collectingPrefetchKeys = true;
    }

    @Override
    public boolean tryBeginPrefetchKeyCollection(int expectedKeys) {
        if (active || prefetchActive || collectingPrefetchKeys) {
            throw new IllegalStateException("DISTINCT batch prefetch is already active");
        }
        pendingPrefetchKeys.clear();
        pendingPrefetchKeySet.clear();
        if (!prefetchEnabled) {
            return false;
        }
        if (expectedKeys < minPrefetchUniqueKeys) {
            prefetchCollections++;
            prefetchRejectedBelowMinimum++;
            prefetchRejectedBeforeKeyScan++;
            return false;
        }
        pendingPrefetchKeys.ensureCapacity(expectedKeys);
        collectingPrefetchKeys = true;
        return true;
    }

    @Override
    public void addPrefetchKey(EK key) {
        if (!collectingPrefetchKeys) {
            throw new IllegalStateException("DISTINCT prefetch key collection is not active");
        }
        if (key != null) {
            EK stableKey = copyKey(key);
            if (pendingPrefetchKeySet.add(stableKey)) {
                pendingPrefetchKeys.add(stableKey);
            }
        }
    }

    @Override
    public boolean finishPrefetchKeyCollection() throws Exception {
        if (!collectingPrefetchKeys) {
            return false;
        }
        collectingPrefetchKeys = false;
        try {
            int size = pendingPrefetchKeys.size();
            recordPrefetchSize(size);
            if (size < minPrefetchUniqueKeys) {
                prefetchRejectedBelowMinimum++;
                return false;
            }
            if (delegate.supportsDirectPrefetchedValues()) {
                List<EV> values = delegate.prefetchUniqueKeyValues(pendingPrefetchKeys);
                if (values == null || values.size() != size) {
                    resetOverlayScope();
                    directValuesPrimed = false;
                    return false;
                }
                try {
                    for (int i = 0; i < size; i++) {
                        EK stableKey = pendingPrefetchKeys.get(i);
                        EV stableValue = copyValue(values.get(i));
                        putOverlayValue(stableKey, stableValue, false, false);
                    }
                } catch (RuntimeException | Error failure) {
                    resetOverlayScope();
                    directValuesPrimed = false;
                    throw failure;
                }
                directValuesPrimed = true;
                directOverlayValues += size;
                return true;
            }
            return beginPrefetchKeys(pendingPrefetchKeys);
        } finally {
            pendingPrefetchKeys.clear();
            pendingPrefetchKeySet.clear();
        }
    }

    @Override
    public Object finishPreparedPrefetchKeyCollection() throws Exception {
        if (!collectingPrefetchKeys) {
            return null;
        }
        collectingPrefetchKeys = false;
        try {
            int size = pendingPrefetchKeys.size();
            recordPrefetchSize(size);
            if (size < minPrefetchUniqueKeys) {
                prefetchRejectedBelowMinimum++;
                // The generated session was admitted from the outer group size, but de-duplication
                // can leave fewer useful keys than the backend threshold. This is a successful
                // per-view no-op, not a preparation failure: invalidating the composite capture
                // here would cancel useful tokens already prepared by sibling DISTINCT views.
                return noOpPreparedPrefetch;
            }
            if (!delegate.supportsDirectPrefetchedValues()) {
                return null;
            }
            ArrayList<EK> stableKeys = new ArrayList<>(pendingPrefetchKeys);
            Object backendPrepared = delegate.prepareUniqueKeyValues(stableKeys);
            return backendPrepared == null
                    ? null
                    : new PreparedPrefetch<>(this, stableKeys, backendPrepared, false);
        } finally {
            pendingPrefetchKeys.clear();
            pendingPrefetchKeySet.clear();
        }
    }

    @Override
    public boolean installPreparedPrefetch(Object prepared) throws Exception {
        if (prepared == noOpPreparedPrefetch) {
            return true;
        }
        if (!(prepared instanceof PreparedPrefetch)) {
            return false;
        }
        PreparedPrefetch<?, ?> token = (PreparedPrefetch<?, ?>) prepared;
        if (token.owner != this || token.consumed || token.noOp) {
            return false;
        }
        token.consumed = true;
        @SuppressWarnings("unchecked")
        List<EK> keys = (List<EK>) token.keys;
        List<EV> values = delegate.awaitPreparedUniqueKeyValues(token.backendPrepared);
        if (values == null || values.size() != keys.size()) {
            resetOverlayScope();
            directValuesPrimed = false;
            return false;
        }
        try {
            for (int i = 0; i < keys.size(); i++) {
                EK stableKey = keys.get(i);
                EV stableValue = copyValue(values.get(i));
                putOverlayValue(stableKey, stableValue, false, false);
            }
        } catch (RuntimeException | Error failure) {
            resetOverlayScope();
            directValuesPrimed = false;
            throw failure;
        }
        directValuesPrimed = true;
        directOverlayValues += keys.size();
        return true;
    }

    @Override
    public Object preparedBackendValueForWave(Object prepared) {
        if (!(prepared instanceof PreparedPrefetch)) {
            return null;
        }
        PreparedPrefetch<?, ?> token = (PreparedPrefetch<?, ?>) prepared;
        return token.owner == this && !token.consumed && !token.noOp ? token.backendPrepared : null;
    }

    @Override
    public boolean isPreparedWaveNoOp(Object prepared) {
        return prepared == noOpPreparedPrefetch;
    }

    @Override
    public void abortPreparedPrefetch(Object prepared) {
        if (prepared == noOpPreparedPrefetch) {
            return;
        }
        if (!(prepared instanceof PreparedPrefetch)) {
            return;
        }
        PreparedPrefetch<?, ?> token = (PreparedPrefetch<?, ?>) prepared;
        if (token.owner == this) {
            if (!token.consumed) {
                token.consumed = true;
                delegate.abortPreparedUniqueKeyValues(token.backendPrepared);
            }
            if (!active && directValuesPrimed) {
                resetOverlayScope();
                directValuesPrimed = false;
            }
        }
    }

    @Override
    public void abortPrefetchKeyCollection() {
        collectingPrefetchKeys = false;
        pendingPrefetchKeys.clear();
        pendingPrefetchKeySet.clear();
        if (!active && directValuesPrimed) {
            resetOverlayScope();
            directValuesPrimed = false;
        }
        endPrefetchScope();
    }

    private static final class PreparedPrefetch<EK, EV> {
        private final DistinctBatchStateMapView<?, EK, EV> owner;
        private final List<EK> keys;
        private final Object backendPrepared;
        private final boolean noOp;
        private boolean consumed;

        private PreparedPrefetch(
                DistinctBatchStateMapView<?, EK, EV> owner,
                List<EK> keys,
                Object backendPrepared,
                boolean noOp) {
            this.owner = owner;
            this.keys = keys;
            this.backendPrepared = backendPrepared;
            this.noOp = noOp;
        }
    }

    /** Prefetches exact keys before the batch overlay starts recording reads and writes. */
    @Override
    boolean beginPrefetchKeys(Iterable<? extends EK> keys) throws Exception {
        if (active || prefetchActive) {
            throw new IllegalStateException("DISTINCT batch prefetch is already active");
        }
        prefetchActive = delegate.beginPrefetchKeys(keys);
        return prefetchActive;
    }

    @Override
    public EV get(EK key) throws Exception {
        if (!active || key == null) {
            return delegate.get(key);
        }
        logicalGets++;
        if (useFlatOverlay()) {
            flatOverlayLookups++;
            int slot = flatOverlay.find(key);
            if (slot >= 0) {
                overlayHits++;
                flatOverlayHits++;
                return flatOverlay.isRemoved(slot) ? null : flatOverlay.valueAt(slot);
            }
            EV value = delegate.get(key);
            delegateGets++;
            EK stableKey = copyKey(key);
            EV stableValue = copyValue(value);
            putOverlayValue(stableKey, stableValue, false, false);
            return stableValue;
        }
        BufferedValue<EK, EV> buffered = overlay.get(key);
        if (buffered != null) {
            overlayHits++;
            return buffered.removed ? null : buffered.value;
        }
        EV value = delegate.get(key);
        delegateGets++;
        EK stableKey = copyKey(key);
        EV stableValue = copyValue(value);
        overlay.put(stableKey, new BufferedValue<>(stableKey, stableValue, false, false));
        return stableValue;
    }

    @Override
    public void put(EK key, EV value) throws Exception {
        if (!active || key == null) {
            delegate.put(key, value);
            return;
        }
        logicalPuts++;
        if (useFlatOverlay()) {
            flatOverlayLookups++;
            int slot = flatOverlay.find(key);
            EV stableValue = copyValue(value);
            if (slot < 0) {
                EK stableKey = copyKey(key);
                putOverlayValue(stableKey, stableValue, true, false);
            } else {
                flatOverlayHits++;
                flatOverlay.update(slot, stableValue, true, false);
            }
            return;
        }
        BufferedValue<EK, EV> buffered = overlay.get(key);
        if (buffered == null) {
            EK stableKey = copyKey(key);
            overlay.put(stableKey, new BufferedValue<>(stableKey, copyValue(value), true, false));
        } else {
            buffered.value = copyValue(value);
            buffered.dirty = true;
            buffered.removed = false;
        }
    }

    @Override
    public void putAll(Map<EK, EV> map) throws Exception {
        if (!active) {
            delegate.putAll(map);
            return;
        }
        for (Map.Entry<EK, EV> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void remove(EK key) throws Exception {
        if (!active || key == null) {
            delegate.remove(key);
            return;
        }
        logicalRemoves++;
        if (useFlatOverlay()) {
            flatOverlayLookups++;
            int slot = flatOverlay.find(key);
            if (slot < 0) {
                EK stableKey = copyKey(key);
                putOverlayValue(stableKey, null, true, true);
            } else {
                flatOverlayHits++;
                flatOverlay.update(slot, null, true, true);
            }
            return;
        }
        BufferedValue<EK, EV> buffered = overlay.get(key);
        if (buffered == null) {
            EK stableKey = copyKey(key);
            overlay.put(stableKey, new BufferedValue<>(stableKey, null, true, true));
        } else {
            buffered.value = null;
            buffered.dirty = true;
            buffered.removed = true;
        }
    }

    @Override
    public boolean contains(EK key) throws Exception {
        if (!active || key == null) {
            return delegate.contains(key);
        }
        if (useFlatOverlay()) {
            flatOverlayLookups++;
            int slot = flatOverlay.find(key);
            if (slot >= 0) {
                overlayHits++;
                flatOverlayHits++;
                return !flatOverlay.isRemoved(slot) && flatOverlay.valueAt(slot) != null;
            }
            return get(key) != null;
        }
        BufferedValue<EK, EV> buffered = overlay.get(key);
        if (buffered != null) {
            overlayHits++;
            return !buffered.removed && buffered.value != null;
        }
        return get(key) != null;
    }

    @Override
    public Iterable<Map.Entry<EK, EV>> entries() throws Exception {
        flushForCompleteView();
        return delegate.entries();
    }

    @Override
    public Iterable<EK> keys() throws Exception {
        flushForCompleteView();
        return delegate.keys();
    }

    @Override
    public Iterable<EV> values() throws Exception {
        flushForCompleteView();
        return delegate.values();
    }

    @Override
    public Iterator<Map.Entry<EK, EV>> iterator() throws Exception {
        flushForCompleteView();
        return delegate.iterator();
    }

    @Override
    public boolean isEmpty() throws Exception {
        flushForCompleteView();
        return delegate.isEmpty();
    }

    @Override
    public void clear() {
        resetOverlayScope();
        directValuesPrimed = false;
        abortPrefetchKeyCollection();
        endPrefetchScope();
        delegate.clear();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        if (active && !overlayIsEmpty()) {
            throw new IllegalStateException(
                    "Cannot change namespace with an uncommitted DISTINCT batch overlay");
        }
        if (!active && directValuesPrimed) {
            resetOverlayScope();
            directValuesPrimed = false;
        }
        endPrefetchScope();
        delegate.setCurrentNamespace(namespace);
    }

    private void recordPrefetchSize(int size) {
        prefetchCollections++;
        prefetchKeysCollected += size;
        if (size == 2) {
            prefetchSize2++;
        } else if (size == 3) {
            prefetchSize3++;
        } else if (size >= 4 && size <= 7) {
            prefetchSize4To7++;
        } else if (size >= 8 && size <= 15) {
            prefetchSize8To15++;
        } else if (size >= 16) {
            prefetchSize16Plus++;
        }
    }

    private void endPrefetchScope() {
        if (prefetchActive) {
            delegate.endPrefetchKeys();
            prefetchActive = false;
        }
    }

    private void flushForCompleteView() throws Exception {
        if (active && !overlayIsEmpty()) {
            flushOverlay(true);
        }
    }

    private void flushOverlay(boolean forced) throws Exception {
        if (overlayIsEmpty()) {
            return;
        }
        if (useFlatOverlay()) {
            int writes = flatOverlay.writeCount();
            if (writes > 0) {
                delegate.putAllKnownNonNullKeys(flatOverlay.writesView());
                committedEntries += writes;
                flatOverlayCommittedWrites += writes;
            }
            int removes = 0;
            for (int i = 0; i < flatOverlay.touchedCount(); i++) {
                int slot = flatOverlay.touchedSlotAt(i);
                if (flatOverlay.isDirty(slot) && flatOverlay.isRemoved(slot)) {
                    delegate.remove(flatOverlay.keyAt(slot));
                    committedEntries++;
                    removes++;
                }
            }
            flatOverlayCommittedRemoves += removes;
            if (forced) {
                forcedFlushes++;
            }
            flatOverlay.clear();
            return;
        }
        LinkedHashMap<EK, EV> writes = new LinkedHashMap<>();
        for (BufferedValue<EK, EV> buffered : overlay.values()) {
            if (buffered.dirty && !buffered.removed) {
                writes.put(buffered.key, buffered.value);
            }
        }
        if (!writes.isEmpty()) {
            delegate.putAll(writes);
            committedEntries += writes.size();
        }
        for (BufferedValue<EK, EV> buffered : overlay.values()) {
            if (buffered.dirty && buffered.removed) {
                delegate.remove(buffered.key);
                committedEntries++;
            }
        }
        if (forced) {
            forcedFlushes++;
        }
        overlay.clear();
    }

    private EK copyKey(EK key) {
        return copyKeys ? keySerializer.copy(key) : key;
    }

    private EV copyValue(EV value) {
        return value != null && copyValues ? valueSerializer.copy(value) : value;
    }

    private boolean useFlatOverlay() {
        return flatOverlayEnabled && !legacyFallbackActive;
    }

    private boolean overlayIsEmpty() {
        return useFlatOverlay() ? flatOverlay.isEmpty() : overlay.isEmpty();
    }

    private void putOverlayValue(EK stableKey, EV stableValue, boolean dirty, boolean removed) {
        if (!useFlatOverlay()) {
            overlay.put(stableKey, new BufferedValue<>(stableKey, stableValue, dirty, removed));
            return;
        }
        int previousSize = flatOverlay.size();
        long previousResizes = flatOverlay.resizeCount();
        if (flatOverlay.put(stableKey, stableValue, dirty, removed)) {
            if (flatOverlay.size() > previousSize) {
                flatOverlayInsertions++;
                flatOverlayPeakEntries = Math.max(flatOverlayPeakEntries, flatOverlay.size());
            }
            flatOverlayResizes += flatOverlay.resizeCount() - previousResizes;
            return;
        }
        flatOverlayCapacityFallbacks++;
        migrateFlatToLegacy();
        overlay.put(stableKey, new BufferedValue<>(stableKey, stableValue, dirty, removed));
    }

    private void migrateFlatToLegacy() {
        HashMap<EK, BufferedValue<EK, EV>> migrated =
                new HashMap<>(Math.max(16, flatOverlay.size() * 2));
        for (int i = 0; i < flatOverlay.touchedCount(); i++) {
            int slot = flatOverlay.touchedSlotAt(i);
            EK key = flatOverlay.keyAt(slot);
            migrated.put(
                    key,
                    new BufferedValue<>(
                            key,
                            flatOverlay.valueAt(slot),
                            flatOverlay.isDirty(slot),
                            flatOverlay.isRemoved(slot)));
        }
        overlay = migrated;
        flatOverlay.clear();
        legacyFallbackActive = true;
        flatOverlayFallbackBatches++;
    }

    private void resetOverlayScope() {
        overlay.clear();
        if (flatOverlay != null) {
            flatOverlay.clear();
        }
        legacyFallbackActive = false;
    }

    long logicalGets() {
        return logicalGets;
    }

    long delegateGets() {
        return delegateGets;
    }

    long overlayHits() {
        return overlayHits;
    }

    long logicalPuts() {
        return logicalPuts;
    }

    long logicalRemoves() {
        return logicalRemoves;
    }

    long committedEntries() {
        return committedEntries;
    }

    long committedBatches() {
        return committedBatches;
    }

    long abortedBatches() {
        return abortedBatches;
    }

    long forcedFlushes() {
        return forcedFlushes;
    }

    long prefetchCollections() {
        return prefetchCollections;
    }

    long prefetchKeysCollected() {
        return prefetchKeysCollected;
    }

    long prefetchRejectedBelowMinimum() {
        return prefetchRejectedBelowMinimum;
    }

    long prefetchRejectedBeforeKeyScan() {
        return prefetchRejectedBeforeKeyScan;
    }

    long directOverlayValues() {
        return directOverlayValues;
    }

    long prefetchSize2() {
        return prefetchSize2;
    }

    long prefetchSize3() {
        return prefetchSize3;
    }

    long prefetchSize4To7() {
        return prefetchSize4To7;
    }

    long prefetchSize8To15() {
        return prefetchSize8To15;
    }

    long prefetchSize16Plus() {
        return prefetchSize16Plus;
    }

    long flatOverlayBatches() {
        return flatOverlayBatches;
    }

    long flatOverlayLookups() {
        return flatOverlayLookups;
    }

    long flatOverlayHits() {
        return flatOverlayHits;
    }

    long flatOverlayInsertions() {
        return flatOverlayInsertions;
    }

    long flatOverlayResizes() {
        return flatOverlayResizes;
    }

    long flatOverlayCapacityFallbacks() {
        return flatOverlayCapacityFallbacks;
    }

    long flatOverlayFallbackBatches() {
        return flatOverlayFallbackBatches;
    }

    long flatOverlayCommittedWrites() {
        return flatOverlayCommittedWrites;
    }

    long flatOverlayCommittedRemoves() {
        return flatOverlayCommittedRemoves;
    }

    long flatOverlayPeakEntries() {
        return flatOverlayPeakEntries;
    }

    private static final class BufferedValue<K, V> {
        private final K key;
        private V value;
        private boolean dirty;
        private boolean removed;

        private BufferedValue(K key, V value, boolean dirty, boolean removed) {
            this.key = key;
            this.value = value;
            this.dirty = dirty;
            this.removed = removed;
        }
    }

    /** Reusable open-addressed batch table. Entries are physically removed only at scope reset. */
    private static final class FlatOverlay<K, V> {
        private static final byte OCCUPIED = 1;
        private static final byte DIRTY = 2;
        private static final byte REMOVED = 4;
        private static final float LOAD_FACTOR = 0.625f;

        private final int maxEntries;
        private final WritesMap writesMap = new WritesMap();
        private Object[] keys;
        private Object[] values;
        private int[] hashes;
        private byte[] flags;
        private int[] touchedSlots;
        private Object[] entryViews;
        private int mask;
        private int resizeThreshold;
        private int size;
        private int touchedCount;
        private int writeCount;
        private long resizeCount;

        private FlatOverlay(int maxEntries) {
            this.maxEntries = maxEntries;
            allocate(16);
        }

        private boolean isEmpty() {
            return size == 0;
        }

        private int size() {
            return size;
        }

        private long resizeCount() {
            return resizeCount;
        }

        private int touchedCount() {
            return touchedCount;
        }

        private int touchedSlotAt(int index) {
            return touchedSlots[index];
        }

        @SuppressWarnings("unchecked")
        private K keyAt(int slot) {
            return (K) keys[slot];
        }

        @SuppressWarnings("unchecked")
        private V valueAt(int slot) {
            return (V) values[slot];
        }

        private boolean isDirty(int slot) {
            return (flags[slot] & DIRTY) != 0;
        }

        private boolean isRemoved(int slot) {
            return (flags[slot] & REMOVED) != 0;
        }

        private int writeCount() {
            return writeCount;
        }

        private Map<K, V> writesView() {
            return writesMap;
        }

        @SuppressWarnings("unchecked")
        private Map.Entry<K, V> entryAt(int slot) {
            EntryView entry = (EntryView) entryViews[slot];
            if (entry == null) {
                entry = new EntryView(slot);
                entryViews[slot] = entry;
            }
            return entry;
        }

        private int find(K query) {
            int hash = spread(query.hashCode());
            int slot = hash & mask;
            while ((flags[slot] & OCCUPIED) != 0) {
                Object stored = keys[slot];
                if (hashes[slot] == hash && (query == stored || query.equals(stored))) {
                    return slot;
                }
                slot = (slot + 1) & mask;
            }
            return -1;
        }

        private boolean put(K stableKey, V value, boolean dirty, boolean removed) {
            int hash = spread(stableKey.hashCode());
            int slot = hash & mask;
            while ((flags[slot] & OCCUPIED) != 0) {
                Object stored = keys[slot];
                if (hashes[slot] == hash && (stableKey == stored || stableKey.equals(stored))) {
                    update(slot, value, dirty, removed);
                    return true;
                }
                slot = (slot + 1) & mask;
            }
            if (size >= maxEntries) {
                return false;
            }
            if (size + 1 > resizeThreshold) {
                grow();
                return put(stableKey, value, dirty, removed);
            }
            keys[slot] = stableKey;
            values[slot] = value;
            hashes[slot] = hash;
            flags[slot] = flags(dirty, removed);
            touchedSlots[touchedCount++] = slot;
            size++;
            if (dirty && !removed) {
                writeCount++;
            }
            return true;
        }

        private void update(int slot, V value, boolean dirty, boolean removed) {
            boolean wasWrite = isDirty(slot) && !isRemoved(slot);
            values[slot] = value;
            flags[slot] = flags(dirty, removed);
            boolean isWrite = dirty && !removed;
            if (wasWrite != isWrite) {
                writeCount += isWrite ? 1 : -1;
            }
        }

        private void clear() {
            for (int i = 0; i < touchedCount; i++) {
                int slot = touchedSlots[i];
                keys[slot] = null;
                values[slot] = null;
                hashes[slot] = 0;
                flags[slot] = 0;
                touchedSlots[i] = 0;
            }
            size = 0;
            touchedCount = 0;
            writeCount = 0;
        }

        private void grow() {
            int oldCapacity = keys.length;
            int newCapacity = oldCapacity << 1;
            Object[] oldKeys = keys;
            Object[] oldValues = values;
            int[] oldHashes = hashes;
            byte[] oldFlags = flags;
            int[] oldTouchedSlots = touchedSlots;
            int oldTouchedCount = touchedCount;

            allocate(newCapacity);
            size = 0;
            touchedCount = 0;
            writeCount = 0;
            for (int i = 0; i < oldTouchedCount; i++) {
                int oldSlot = oldTouchedSlots[i];
                @SuppressWarnings("unchecked")
                K key = (K) oldKeys[oldSlot];
                @SuppressWarnings("unchecked")
                V value = (V) oldValues[oldSlot];
                insertRehashed(key, value, oldHashes[oldSlot], oldFlags[oldSlot]);
            }
            resizeCount++;
        }

        private void insertRehashed(K key, V value, int hash, byte entryFlags) {
            int slot = hash & mask;
            while ((flags[slot] & OCCUPIED) != 0) {
                slot = (slot + 1) & mask;
            }
            keys[slot] = key;
            values[slot] = value;
            hashes[slot] = hash;
            flags[slot] = entryFlags;
            touchedSlots[touchedCount++] = slot;
            size++;
            if ((entryFlags & DIRTY) != 0 && (entryFlags & REMOVED) == 0) {
                writeCount++;
            }
        }

        private void allocate(int capacity) {
            keys = new Object[capacity];
            values = new Object[capacity];
            hashes = new int[capacity];
            flags = new byte[capacity];
            touchedSlots = new int[capacity];
            entryViews = new Object[capacity];
            mask = capacity - 1;
            resizeThreshold = Math.min(maxEntries, (int) (capacity * LOAD_FACTOR));
        }

        private static byte flags(boolean dirty, boolean removed) {
            return (byte) (OCCUPIED | (dirty ? DIRTY : 0) | (removed ? REMOVED : 0));
        }

        private static int spread(int hash) {
            return hash ^ (hash >>> 16);
        }

        private final class WritesMap extends AbstractMap<K, V> {
            private final Set<Map.Entry<K, V>> entries = new WriteEntrySet();

            @Override
            public int size() {
                return writeCount;
            }

            @Override
            public Set<Map.Entry<K, V>> entrySet() {
                return entries;
            }
        }

        private final class WriteEntrySet extends AbstractSet<Map.Entry<K, V>> {
            @Override
            public int size() {
                return writeCount;
            }

            @Override
            public Iterator<Map.Entry<K, V>> iterator() {
                return new WriteIterator();
            }
        }

        private final class WriteIterator implements Iterator<Map.Entry<K, V>> {
            private int scanIndex;
            private int nextSlot = -1;

            @Override
            public boolean hasNext() {
                if (nextSlot >= 0) {
                    return true;
                }
                while (scanIndex < touchedCount) {
                    int slot = touchedSlots[scanIndex++];
                    if (isDirty(slot) && !isRemoved(slot)) {
                        nextSlot = slot;
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Map.Entry<K, V> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                int currentSlot = nextSlot;
                nextSlot = -1;
                return entryAt(currentSlot);
            }
        }

        private final class EntryView implements Map.Entry<K, V> {
            private final int slot;

            private EntryView(int slot) {
                this.slot = slot;
            }

            @Override
            public K getKey() {
                return keyAt(slot);
            }

            @Override
            public V getValue() {
                return valueAt(slot);
            }

            @Override
            public V setValue(V value) {
                throw new UnsupportedOperationException("read-only transient batch entry");
            }

            @Override
            public boolean equals(Object other) {
                if (!(other instanceof Map.Entry)) {
                    return false;
                }
                Map.Entry<?, ?> entry = (Map.Entry<?, ?>) other;
                return Objects.equals(getKey(), entry.getKey())
                        && Objects.equals(getValue(), entry.getValue());
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(getKey()) ^ Objects.hashCode(getValue());
            }
        }
    }
}
