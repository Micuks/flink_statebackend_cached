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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;

import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation of {@link StateDataViewStore} that currently forwards state registration
 * to a {@link RuntimeContext}.
 */
@Internal
public final class PerKeyStateDataViewStore implements StateDataViewStore {

    private static final String NULL_STATE_POSTFIX = "_null_state";
    private static final String DISTINCT_STATE_PREFIX = "distinctAcc_";
    private static final String DISTINCT_BATCH_OVERLAY_KEY =
            "state.backend.cachekit.local-preagg.distinct-overlay.enabled";
    private static final String NATIVE_MAP_DISTINCT_BATCH_PREFETCH_KEY =
            "state.backend.cachekit.native.map-distinct-batch-prefetch.enabled";

    private final RuntimeContext ctx;
    private final StateTtlConfig stateTtlConfig;
    private final boolean distinctBatchOverlayEnabled;
    private final List<DistinctBatchStateMapView<?, ?, ?>> distinctBatchViews = new ArrayList<>();

    public PerKeyStateDataViewStore(RuntimeContext ctx) {
        this(ctx, StateTtlConfig.DISABLED);
    }

    public PerKeyStateDataViewStore(RuntimeContext ctx, StateTtlConfig stateTtlConfig) {
        this(
                ctx,
                stateTtlConfig,
                isDistinctBatchEnabled(GlobalConfiguration.loadConfiguration()));
    }

    static boolean isDistinctBatchEnabled(Configuration configuration) {
        return configuration.getBoolean(DISTINCT_BATCH_OVERLAY_KEY, false)
                || configuration.getBoolean(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_KEY, false);
    }

    PerKeyStateDataViewStore(
            RuntimeContext ctx,
            StateTtlConfig stateTtlConfig,
            boolean distinctBatchOverlayEnabled) {
        this.ctx = ctx;
        this.stateTtlConfig = stateTtlConfig;
        // Batching across TTL reads would change access-time refresh semantics. Keep the first
        // implementation deliberately fail-closed until a TTL-specific contract is proven.
        this.distinctBatchOverlayEnabled =
                distinctBatchOverlayEnabled && !stateTtlConfig.isEnabled();
    }

    @Override
    public <N, EK, EV> StateMapView<N, EK, EV> getStateMapView(
            String stateName,
            boolean supportNullKey,
            TypeSerializer<EK> keySerializer,
            TypeSerializer<EV> valueSerializer) {
        final MapStateDescriptor<EK, EV> mapStateDescriptor =
                new MapStateDescriptor<>(stateName, keySerializer, valueSerializer);

        if (stateTtlConfig.isEnabled()) {
            mapStateDescriptor.enableTimeToLive(stateTtlConfig);
        }
        final MapState<EK, EV> mapState = ctx.getMapState(mapStateDescriptor);

        final StateMapView<N, EK, EV> view;
        if (supportNullKey) {
            final ValueStateDescriptor<EV> nullStateDescriptor =
                    new ValueStateDescriptor<>(stateName + NULL_STATE_POSTFIX, valueSerializer);
            if (stateTtlConfig.isEnabled()) {
                nullStateDescriptor.enableTimeToLive(stateTtlConfig);
            }
            final ValueState<EV> nullState = ctx.getState(nullStateDescriptor);
            view = new StateMapView.KeyedStateMapViewWithKeysNullable<>(mapState, nullState);
        } else {
            view = new StateMapView.KeyedStateMapViewWithKeysNotNull<>(mapState);
        }
        if (!distinctBatchOverlayEnabled || !stateName.startsWith(DISTINCT_STATE_PREFIX)) {
            return view;
        }
        DistinctBatchStateMapView<N, EK, EV> batchingView =
                new DistinctBatchStateMapView<>(view, keySerializer, valueSerializer);
        distinctBatchViews.add(batchingView);
        return batchingView;
    }

    /** Starts one outer-key batch on every exact-DISTINCT state view created by this store. */
    public boolean beginDistinctBatch() {
        if (distinctBatchViews.isEmpty()) {
            return false;
        }
        try {
            for (DistinctBatchStateMapView<?, ?, ?> view : distinctBatchViews) {
                view.beginBatch();
            }
        } catch (RuntimeException failure) {
            // A generated aggregate can own more than one DISTINCT view. Never leave the earlier
            // views active when a later view rejects the batch.
            for (DistinctBatchStateMapView<?, ?, ?> view : distinctBatchViews) {
                view.abortBatch();
            }
            throw failure;
        }
        return true;
    }

    /** Commits final per-distinct-key values before accumulator state/output becomes visible. */
    public void commitDistinctBatch() throws Exception {
        Exception failure = null;
        for (DistinctBatchStateMapView<?, ?, ?> view : distinctBatchViews) {
            try {
                view.commitBatch();
            } catch (Exception current) {
                if (failure == null) {
                    failure = current;
                } else {
                    failure.addSuppressed(current);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** Drops an uncommitted batch after a generated aggregate failure. */
    public void abortDistinctBatch() {
        for (DistinctBatchStateMapView<?, ?, ?> view : distinctBatchViews) {
            view.abortBatch();
        }
    }

    /** Stable close-time counters used to prove that the optimization reached real DISTINCT state. */
    public String distinctBatchDiagnosticSummary() {
        long logicalGets = 0;
        long delegateGets = 0;
        long overlayHits = 0;
        long logicalPuts = 0;
        long logicalRemoves = 0;
        long committedEntries = 0;
        long committedBatches = 0;
        long abortedBatches = 0;
        long forcedFlushes = 0;
        for (DistinctBatchStateMapView<?, ?, ?> view : distinctBatchViews) {
            logicalGets += view.logicalGets();
            delegateGets += view.delegateGets();
            overlayHits += view.overlayHits();
            logicalPuts += view.logicalPuts();
            logicalRemoves += view.logicalRemoves();
            committedEntries += view.committedEntries();
            committedBatches += view.committedBatches();
            abortedBatches += view.abortedBatches();
            forcedFlushes += view.forcedFlushes();
        }
        return "views="
                + distinctBatchViews.size()
                + " logicalGets="
                + logicalGets
                + " delegateGets="
                + delegateGets
                + " overlayHits="
                + overlayHits
                + " logicalPuts="
                + logicalPuts
                + " logicalRemoves="
                + logicalRemoves
                + " committedEntries="
                + committedEntries
                + " committedBatches="
                + committedBatches
                + " abortedBatches="
                + abortedBatches
                + " forcedFlushes="
                + forcedFlushes;
    }

    @Override
    public <N, EE> StateListView<N, EE> getStateListView(
            String stateName, TypeSerializer<EE> elementSerializer) {
        final ListStateDescriptor<EE> listStateDescriptor =
                new ListStateDescriptor<>(stateName, elementSerializer);

        if (stateTtlConfig.isEnabled()) {
            listStateDescriptor.enableTimeToLive(stateTtlConfig);
        }
        final ListState<EE> listState = ctx.getListState(listStateDescriptor);

        return new StateListView.KeyedStateListView<>(listState);
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return ctx;
    }
}
