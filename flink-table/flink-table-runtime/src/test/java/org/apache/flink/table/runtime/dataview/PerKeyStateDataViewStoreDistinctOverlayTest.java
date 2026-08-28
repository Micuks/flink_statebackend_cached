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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.internal.BatchPrefetchableMapState;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class PerKeyStateDataViewStoreDistinctOverlayTest {

    @Test
    void eitherPublicFeatureKeyEnablesTheRequiredDistinctBatchView() {
        Configuration configuration = new Configuration();
        assertFalse(PerKeyStateDataViewStore.isDistinctBatchEnabled(configuration));

        configuration.setBoolean(
                "state.backend.cachekit.native.map-distinct-batch-prefetch.enabled", true);
        assertTrue(PerKeyStateDataViewStore.isDistinctBatchEnabled(configuration));

        configuration = new Configuration();
        configuration.setBoolean(
                "state.backend.cachekit.local-preagg.distinct-overlay.enabled", true);
        assertTrue(PerKeyStateDataViewStore.isDistinctBatchEnabled(configuration));

        configuration = new Configuration();
        configuration.setBoolean(
                "state.backend.cachekit.local-preagg.distinct-overlay.flat.enabled", true);
        assertFalse(PerKeyStateDataViewStore.isDistinctBatchEnabled(configuration));
    }

    @Test
    @SuppressWarnings("unchecked")
    void wrapsOnlyGeneratedExactDistinctStateNames() {
        RuntimeContext context = mock(RuntimeContext.class);
        when(context.getMapState(any())).thenReturn(mock(MapState.class));
        PerKeyStateDataViewStore distinctStore =
                new PerKeyStateDataViewStore(context, StateTtlConfig.DISABLED, true);

        distinctStore.getStateMapView(
                "distinctAcc_0", false, StringSerializer.INSTANCE, LongSerializer.INSTANCE);

        assertTrue(distinctStore.beginDistinctBatch());
        distinctStore.abortDistinctBatch();

        PerKeyStateDataViewStore ordinaryStore =
                new PerKeyStateDataViewStore(context, StateTtlConfig.DISABLED, true);
        ordinaryStore.getStateMapView(
                "ordinaryMap", false, StringSerializer.INSTANCE, LongSerializer.INSTANCE);
        assertFalse(ordinaryStore.beginDistinctBatch());
    }

    @Test
    @SuppressWarnings("unchecked")
    void localOverlayDoesNotCollectPrefetchKeysWhenNativePrefetchIsDisabled() {
        RuntimeContext context = mock(RuntimeContext.class);
        when(context.getMapState(any())).thenReturn(mock(MapState.class));
        PerKeyStateDataViewStore store =
                new PerKeyStateDataViewStore(context, StateTtlConfig.DISABLED, true, false, 2);

        StateMapView<?, String, Long> view =
                store.getStateMapView(
                        "distinctAcc_0", false, StringSerializer.INSTANCE, LongSerializer.INSTANCE);

        assertNull(DistinctBatchPrefetchSupport.beginSession(view, 8));
    }

    @Test
    @SuppressWarnings("unchecked")
    void nativePrefetchFeatureEnablesGeneratedKeyCollection() {
        RuntimeContext context = mock(RuntimeContext.class);
        when(context.getMapState(any())).thenReturn(mock(MapState.class));
        PerKeyStateDataViewStore store =
                new PerKeyStateDataViewStore(context, StateTtlConfig.DISABLED, true, true, 2);

        StateMapView<?, String, Long> view =
                store.getStateMapView(
                        "distinctAcc_0", false, StringSerializer.INSTANCE, LongSerializer.INSTANCE);

        Object session = DistinctBatchPrefetchSupport.beginSession(view, 8);
        assertTrue(session instanceof BatchPrefetchableMapView);
        DistinctBatchPrefetchSupport.abortSession(session);
    }

    @Test
    @SuppressWarnings("unchecked")
    void flatFeatureIsWiredIntoConstructedDistinctView() throws Exception {
        RuntimeContext context = mock(RuntimeContext.class);
        when(context.getMapState(any())).thenReturn(mock(MapState.class));
        PerKeyStateDataViewStore store =
                new PerKeyStateDataViewStore(
                        context, StateTtlConfig.DISABLED, true, false, 2, true, 64);

        StateMapView<?, String, Long> stateView =
                store.getStateMapView(
                        "distinctAcc_0", false, StringSerializer.INSTANCE, LongSerializer.INSTANCE);
        DistinctBatchStateMapView<?, String, Long> distinctView =
                (DistinctBatchStateMapView<?, String, Long>) stateView;

        assertTrue(store.beginDistinctBatch());
        distinctView.put("key", 1L);
        store.commitDistinctBatch();
        assertTrue(distinctView.flatOverlayBatches() > 0);
        assertTrue(distinctView.flatOverlayLookups() > 0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void failsClosedForTtlState() {
        RuntimeContext context = mock(RuntimeContext.class);
        when(context.getMapState(any())).thenReturn(mock(MapState.class));
        StateTtlConfig ttlConfig =
                StateTtlConfig.newBuilder(Time.seconds(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();
        PerKeyStateDataViewStore store =
                new PerKeyStateDataViewStore(context, ttlConfig, true, false, 2, true, 64);

        store.getStateMapView(
                "distinctAcc_0", false, StringSerializer.INSTANCE, LongSerializer.INSTANCE);

        assertFalse(store.beginDistinctBatch());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void preparedCommitFusesDirtyDistinctColumnsWithoutMapStateReplay() throws Exception {
        RuntimeContext context = mock(RuntimeContext.class);
        MapState<String, Long> firstState = preparedMapState();
        MapState<String, Long> secondState = preparedMapState();
        BatchPrefetchableMapState.PreparedValues firstToken = preparedToken(List.of(1L, 2L));
        BatchPrefetchableMapState.PreparedValues secondToken = preparedToken(List.of(3L, 4L));
        BatchPrefetchableMapState<String> firstBatch = (BatchPrefetchableMapState) firstState;
        BatchPrefetchableMapState<String> secondBatch = (BatchPrefetchableMapState) secondState;
        when(firstBatch.prepareCurrentUniqueKeyValues(List.of("a", "b")))
                .thenReturn(firstToken);
        when(secondBatch.prepareCurrentUniqueKeyValues(List.of("a", "b")))
                .thenReturn(secondToken);
        when(firstToken.commitPreparedCohort(anyList(), anyList(), anyList(), anyList()))
                .thenReturn(true);
        when(context.<Object, Object>getMapState(any()))
                .thenReturn((MapState) firstState, (MapState) secondState);
        PerKeyStateDataViewStore store = preparedCommitStore(context);
        StateMapView<?, String, Long> firstView =
                store.getStateMapView(
                        "distinctAcc_0", false, StringSerializer.INSTANCE, LongSerializer.INSTANCE);
        StateMapView<?, String, Long> secondView =
                store.getStateMapView(
                        "distinctAcc_1", false, StringSerializer.INSTANCE, LongSerializer.INSTANCE);

        installPrepared(List.of(firstView, secondView), List.of("a", "b"));
        assertTrue(store.beginDistinctBatch());
        firstView.put("a", 11L);
        secondView.put("b", 44L);
        store.commitDistinctBatch();

        verify(firstToken, times(1))
                .commitPreparedCohort(anyList(), anyList(), anyList(), anyList());
        verify(firstState, never()).putAll(any());
        verify(secondState, never()).putAll(any());
        assertTrue(store.distinctBatchDiagnosticSummary().contains("preparedCommitBatches=2"));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void preparedCommitFalseFallsBackBeforeWrite() throws Exception {
        RuntimeContext context = mock(RuntimeContext.class);
        MapState<String, Long> state = preparedMapState();
        BatchPrefetchableMapState<String> batchState = (BatchPrefetchableMapState) state;
        BatchPrefetchableMapState.PreparedValues token = preparedToken(List.of(1L, 2L));
        when(batchState.prepareCurrentUniqueKeyValues(List.of("a", "b"))).thenReturn(token);
        when(token.commitPreparedCohort(anyList(), anyList(), anyList(), anyList()))
                .thenReturn(false);
        when(context.<Object, Object>getMapState(any())).thenReturn((MapState) state);
        PerKeyStateDataViewStore store = preparedCommitStore(context);
        StateMapView<?, String, Long> view =
                store.getStateMapView(
                        "distinctAcc_0", false, StringSerializer.INSTANCE, LongSerializer.INSTANCE);

        installPrepared(List.of(view), List.of("a", "b"));
        assertTrue(store.beginDistinctBatch());
        view.put("a", 11L);
        store.commitDistinctBatch();

        verify(state, times(1)).putAll(any());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void preparedCommitExceptionNeverReplaysMapStateWrites() throws Exception {
        RuntimeContext context = mock(RuntimeContext.class);
        MapState<String, Long> state = preparedMapState();
        BatchPrefetchableMapState<String> batchState = (BatchPrefetchableMapState) state;
        BatchPrefetchableMapState.PreparedValues token = preparedToken(List.of(1L, 2L));
        when(batchState.prepareCurrentUniqueKeyValues(List.of("a", "b"))).thenReturn(token);
        when(token.commitPreparedCohort(anyList(), anyList(), anyList(), anyList()))
                .thenThrow(new IllegalStateException("db.write outcome unknown"));
        when(context.<Object, Object>getMapState(any())).thenReturn((MapState) state);
        PerKeyStateDataViewStore store = preparedCommitStore(context);
        StateMapView<?, String, Long> view =
                store.getStateMapView(
                        "distinctAcc_0", false, StringSerializer.INSTANCE, LongSerializer.INSTANCE);

        installPrepared(List.of(view), List.of("a", "b"));
        assertTrue(store.beginDistinctBatch());
        view.put("a", 11L);

        assertThrows(IllegalStateException.class, store::commitDistinctBatch);
        verify(state, never()).putAll(any());
        store.abortDistinctBatch();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static MapState<String, Long> preparedMapState() {
        MapState<String, Long> state =
                mock(MapState.class, withSettings().extraInterfaces(BatchPrefetchableMapState.class));
        BatchPrefetchableMapState<String> batchState = (BatchPrefetchableMapState) state;
        when(batchState.supportsDirectPrefetchedValues()).thenReturn(true);
        return state;
    }

    private static BatchPrefetchableMapState.PreparedValues preparedToken(List<Long> values)
            throws Exception {
        BatchPrefetchableMapState.PreparedValues token =
                mock(BatchPrefetchableMapState.PreparedValues.class);
        doReturn(values).when(token).awaitValues();
        when(token.supportsPreparedCommit()).thenReturn(true);
        return token;
    }

    private static PerKeyStateDataViewStore preparedCommitStore(RuntimeContext context) {
        return new PerKeyStateDataViewStore(
                context, StateTtlConfig.DISABLED, true, true, 2, false, 64, true);
    }

    private static void installPrepared(
            List<? extends StateMapView<?, String, Long>> views, List<String> keys)
            throws Exception {
        List<Object> sessions = new java.util.ArrayList<>(views.size());
        for (StateMapView<?, String, Long> view : views) {
            sessions.add(DistinctBatchPrefetchSupport.beginSession(view, keys.size()));
        }
        DistinctBatchPrefetchSupport.beginPreparedCapture();
        for (String key : keys) {
            for (Object session : sessions) {
                DistinctBatchPrefetchSupport.addSession(session, key);
            }
        }
        for (Object session : sessions) {
            assertTrue(DistinctBatchPrefetchSupport.finishSession(session));
        }
        assertTrue(
                DistinctBatchPrefetchSupport.installPreparedCapture(
                        DistinctBatchPrefetchSupport.endPreparedCapture()));
    }
}
