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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    void failsClosedForTtlState() {
        RuntimeContext context = mock(RuntimeContext.class);
        when(context.getMapState(any())).thenReturn(mock(MapState.class));
        StateTtlConfig ttlConfig =
                StateTtlConfig.newBuilder(Time.seconds(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();
        PerKeyStateDataViewStore store = new PerKeyStateDataViewStore(context, ttlConfig, true);

        store.getStateMapView(
                "distinctAcc_0", false, StringSerializer.INSTANCE, LongSerializer.INSTANCE);

        assertFalse(store.beginDistinctBatch());
    }
}
