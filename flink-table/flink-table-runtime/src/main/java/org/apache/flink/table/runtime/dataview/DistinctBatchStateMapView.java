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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    private final HashMap<EK, BufferedValue<EK, EV>> overlay = new HashMap<>();
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
        this.delegate = delegate;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.copyKeys = !keySerializer.isImmutableType();
        this.copyValues = !valueSerializer.isImmutableType();
        this.prefetchEnabled = prefetchEnabled;
        this.minPrefetchUniqueKeys = Math.max(2, minPrefetchUniqueKeys);
        this.noOpPreparedPrefetch =
                new PreparedPrefetch<>(this, Collections.emptyList(), null, true);
    }

    void beginBatch() {
        if (active || (!overlay.isEmpty() && !directValuesPrimed)) {
            throw new IllegalStateException("DISTINCT batch overlay is already active");
        }
        active = true;
    }

    void commitBatch() throws Exception {
        if (!active) {
            return;
        }
        try {
            flushOverlay(false);
            committedBatches++;
        } finally {
            overlay.clear();
            active = false;
            directValuesPrimed = false;
            endPrefetchScope();
        }
    }

    void abortBatch() {
        if (active) {
            abortedBatches++;
        }
        overlay.clear();
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
                    overlay.clear();
                    directValuesPrimed = false;
                    return false;
                }
                for (int i = 0; i < size; i++) {
                    EK stableKey = pendingPrefetchKeys.get(i);
                    EV stableValue = copyValue(values.get(i));
                    overlay.put(
                            stableKey, new BufferedValue<>(stableKey, stableValue, false, false));
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
            overlay.clear();
            directValuesPrimed = false;
            return false;
        }
        for (int i = 0; i < keys.size(); i++) {
            EK stableKey = keys.get(i);
            EV stableValue = copyValue(values.get(i));
            overlay.put(stableKey, new BufferedValue<>(stableKey, stableValue, false, false));
        }
        directValuesPrimed = true;
        directOverlayValues += keys.size();
        return true;
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
                overlay.clear();
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
            overlay.clear();
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
        overlay.clear();
        directValuesPrimed = false;
        abortPrefetchKeyCollection();
        endPrefetchScope();
        delegate.clear();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        if (active && !overlay.isEmpty()) {
            throw new IllegalStateException(
                    "Cannot change namespace with an uncommitted DISTINCT batch overlay");
        }
        if (!active && directValuesPrimed) {
            overlay.clear();
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
        if (active && !overlay.isEmpty()) {
            flushOverlay(true);
        }
    }

    private void flushOverlay(boolean forced) throws Exception {
        if (overlay.isEmpty()) {
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
}
