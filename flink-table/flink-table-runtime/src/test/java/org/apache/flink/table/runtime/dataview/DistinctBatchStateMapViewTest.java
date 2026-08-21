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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DistinctBatchStateMapViewTest {

    @Test
    @SuppressWarnings("unchecked")
    void collapsesRepeatedGetAndPutToOneDelegateReadAndOneFinalWrite() throws Exception {
        StateMapView<Void, String, Long> delegate = mock(StateMapView.class);
        when(delegate.get("bidder-7")).thenReturn(1L);
        DistinctBatchStateMapView<Void, String, Long> view = createView(delegate);

        view.beginBatch();
        assertEquals(1L, view.get("bidder-7"));
        view.put("bidder-7", 3L);
        assertEquals(3L, view.get("bidder-7"));
        view.put("bidder-7", 7L);
        view.commitBatch();

        verify(delegate, times(1)).get("bidder-7");
        ArgumentCaptor<Map<String, Long>> writes = ArgumentCaptor.forClass(Map.class);
        verify(delegate, times(1)).putAll(writes.capture());
        assertEquals(Collections.singletonMap("bidder-7", 7L), writes.getValue());
        assertEquals(2, view.logicalGets());
        assertEquals(1, view.delegateGets());
        assertEquals(1, view.overlayHits());
        assertEquals(2, view.logicalPuts());
        assertEquals(1, view.committedEntries());
        assertEquals(1, view.committedBatches());
        assertFalse(view.isBatchActive());
    }

    @Test
    @SuppressWarnings("unchecked")
    void abortDropsUncommittedWrites() throws Exception {
        StateMapView<Void, String, Long> delegate = mock(StateMapView.class);
        DistinctBatchStateMapView<Void, String, Long> view = createView(delegate);

        view.beginBatch();
        view.put("auction-3", 5L);
        view.abortBatch();

        verify(delegate, never()).putAll(anyMap());
        assertEquals(1, view.abortedBatches());
        assertFalse(view.isBatchActive());
    }

    @Test
    @SuppressWarnings("unchecked")
    void removeWinsOverEarlierBufferedPut() throws Exception {
        StateMapView<Void, String, Long> delegate = mock(StateMapView.class);
        DistinctBatchStateMapView<Void, String, Long> view = createView(delegate);

        view.beginBatch();
        view.put("bidder-9", 1L);
        view.remove("bidder-9");
        assertNull(view.get("bidder-9"));
        view.commitBatch();

        verify(delegate, never()).putAll(anyMap());
        verify(delegate, times(1)).remove("bidder-9");
        assertEquals(1, view.committedEntries());
    }

    @Test
    @SuppressWarnings("unchecked")
    void nullKeyKeepsDedicatedDelegateStateSemantics() throws Exception {
        StateMapView<Void, String, Long> delegate = mock(StateMapView.class);
        when(delegate.get(null)).thenReturn(11L);
        DistinctBatchStateMapView<Void, String, Long> view = createView(delegate);

        view.beginBatch();
        assertEquals(11L, view.get(null));
        view.put(null, 12L);
        view.commitBatch();

        verify(delegate, times(1)).get(null);
        verify(delegate, times(1)).put(null, 12L);
        verify(delegate, never()).putAll(anyMap());
    }

    @Test
    @SuppressWarnings("unchecked")
    void completeViewOperationFlushesBeforeDelegating() throws Exception {
        StateMapView<Void, String, Long> delegate = mock(StateMapView.class);
        when(delegate.isEmpty()).thenReturn(false);
        DistinctBatchStateMapView<Void, String, Long> view = createView(delegate);

        view.beginBatch();
        view.put("bidder-1", 4L);
        assertFalse(view.isEmpty());
        assertTrue(view.isBatchActive());
        view.commitBatch();

        verify(delegate, times(1)).putAll(anyMap());
        verify(delegate, times(1)).isEmpty();
        assertEquals(1, view.forcedFlushes());
        assertEquals(1, view.committedBatches());
    }

    @Test
    @SuppressWarnings("unchecked")
    void nullableViewBatchesNonNullEntriesAndKeepsNullInValueState() throws Exception {
        MapState<String, Long> mapState = mock(MapState.class);
        ValueState<Long> nullState = mock(ValueState.class);
        StateMapView<Void, String, Long> view =
                new StateMapView.KeyedStateMapViewWithKeysNullable<>(mapState, nullState);
        Map<String, Long> values = new HashMap<>();
        values.put("bidder-1", 3L);
        values.put("bidder-2", 5L);
        values.put(null, 7L);

        view.putAll(values);

        ArgumentCaptor<Map<String, Long>> writes = ArgumentCaptor.forClass(Map.class);
        verify(mapState, times(1)).putAll(writes.capture());
        assertEquals(2, writes.getValue().size());
        assertEquals(3L, writes.getValue().get("bidder-1"));
        assertEquals(5L, writes.getValue().get("bidder-2"));
        verify(nullState, times(1)).update(7L);
    }

    private static DistinctBatchStateMapView<Void, String, Long> createView(
            StateMapView<Void, String, Long> delegate) {
        return new DistinctBatchStateMapView<>(
                delegate, StringSerializer.INSTANCE, LongSerializer.INSTANCE);
    }
}
