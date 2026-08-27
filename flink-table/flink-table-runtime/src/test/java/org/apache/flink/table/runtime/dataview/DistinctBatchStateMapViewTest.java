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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DistinctBatchStateMapViewTest {

    @Test
    @SuppressWarnings("unchecked")
    void installsPreparedValuesIntoOverlayWithoutSynchronousLookup() throws Exception {
        StateMapView<Void, String, Long> delegate = mock(StateMapView.class);
        Object backendPrepared = new Object();
        when(delegate.supportsDirectPrefetchedValues()).thenReturn(true);
        when(delegate.prepareUniqueKeyValues(any())).thenReturn(backendPrepared);
        when(delegate.awaitPreparedUniqueKeyValues(backendPrepared))
                .thenReturn(java.util.Arrays.asList(7L, null));
        DistinctBatchStateMapView<Void, String, Long> view = createView(delegate);

        view.beginPrefetchKeyCollection(2);
        view.addPrefetchKey("bidder-1");
        view.addPrefetchKey("bidder-2");
        Object prepared = view.finishPreparedPrefetchKeyCollection();
        assertTrue(view.installPreparedPrefetch(prepared));
        view.beginBatch();
        assertEquals(7L, view.get("bidder-1"));
        assertNull(view.get("bidder-2"));
        view.commitBatch();

        verify(delegate, never()).prefetchUniqueKeyValues(any());
        verify(delegate, never()).get(any());
        assertEquals(2, view.directOverlayValues());
        assertEquals(2, view.overlayHits());
    }

    @Test
    @SuppressWarnings("unchecked")
    void compositeCaptureKeepsDetachedSiblingWhenOneViewDeduplicatesBelowMinimum()
            throws Exception {
        StateMapView<Void, String, Long> asyncDelegate = mock(StateMapView.class);
        Object backendPrepared = new Object();
        when(asyncDelegate.supportsDirectPrefetchedValues()).thenReturn(true);
        when(asyncDelegate.prepareUniqueKeyValues(any())).thenReturn(backendPrepared);
        when(asyncDelegate.awaitPreparedUniqueKeyValues(backendPrepared))
                .thenReturn(java.util.Arrays.asList(7L, null));
        DistinctBatchStateMapView<Void, String, Long> asyncView = createView(asyncDelegate);

        StateMapView<Void, String, Long> noOpDelegate = mock(StateMapView.class);
        when(noOpDelegate.supportsDirectPrefetchedValues()).thenReturn(true);
        DistinctBatchStateMapView<Void, String, Long> noOpView = createView(noOpDelegate);

        Object asyncSession = DistinctBatchPrefetchSupport.beginSession(asyncView, 2);
        Object noOpSession = DistinctBatchPrefetchSupport.beginSession(noOpView, 2);
        DistinctBatchPrefetchSupport.beginPreparedCapture();
        for (String key : java.util.Arrays.asList("bidder-1", "bidder-2")) {
            DistinctBatchPrefetchSupport.addSession(asyncSession, key);
            DistinctBatchPrefetchSupport.addSession(noOpSession, "duplicate-key");
        }
        assertTrue(DistinctBatchPrefetchSupport.finishSession(asyncSession));
        assertTrue(DistinctBatchPrefetchSupport.finishSession(noOpSession));
        Object capture = DistinctBatchPrefetchSupport.endPreparedCapture();

        assertTrue(DistinctBatchPrefetchSupport.installPreparedCapture(capture));
        verify(asyncDelegate, times(1)).awaitPreparedUniqueKeyValues(backendPrepared);
        verify(asyncDelegate, never()).prefetchUniqueKeyValues(any());
        verify(noOpDelegate, never()).prepareUniqueKeyValues(any());
        verify(noOpDelegate, never()).prefetchUniqueKeyValues(any());
        verify(noOpDelegate, never()).awaitPreparedUniqueKeyValues(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void compositeCaptureStillFailsClosedWhenSiblingBackendPreparationFails() throws Exception {
        StateMapView<Void, String, Long> asyncDelegate = mock(StateMapView.class);
        Object backendPrepared = new Object();
        when(asyncDelegate.supportsDirectPrefetchedValues()).thenReturn(true);
        when(asyncDelegate.prepareUniqueKeyValues(any())).thenReturn(backendPrepared);
        DistinctBatchStateMapView<Void, String, Long> asyncView = createView(asyncDelegate);

        StateMapView<Void, String, Long> failedDelegate = mock(StateMapView.class);
        when(failedDelegate.supportsDirectPrefetchedValues()).thenReturn(true);
        when(failedDelegate.prepareUniqueKeyValues(any())).thenReturn(null);
        DistinctBatchStateMapView<Void, String, Long> failedView = createView(failedDelegate);

        Object asyncSession = DistinctBatchPrefetchSupport.beginSession(asyncView, 2);
        Object failedSession = DistinctBatchPrefetchSupport.beginSession(failedView, 2);
        DistinctBatchPrefetchSupport.beginPreparedCapture();
        for (String key : java.util.Arrays.asList("bidder-1", "bidder-2")) {
            DistinctBatchPrefetchSupport.addSession(asyncSession, key);
            DistinctBatchPrefetchSupport.addSession(failedSession, key);
        }
        assertTrue(DistinctBatchPrefetchSupport.finishSession(asyncSession));
        assertFalse(DistinctBatchPrefetchSupport.finishSession(failedSession));
        Object capture = DistinctBatchPrefetchSupport.endPreparedCapture();

        assertFalse(DistinctBatchPrefetchSupport.installPreparedCapture(capture));
        verify(asyncDelegate, times(1)).abortPreparedUniqueKeyValues(backendPrepared);
        verify(asyncDelegate, never()).awaitPreparedUniqueKeyValues(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void compositePreparedCaptureIsAllOrNothingAcrossDistinctViews() throws Exception {
        BatchPrefetchableMapView<Object> first = mock(BatchPrefetchableMapView.class);
        BatchPrefetchableMapView<Object> second = mock(BatchPrefetchableMapView.class);
        Object firstToken = new Object();
        Object secondToken = new Object();
        when(first.finishPreparedPrefetchKeyCollection()).thenReturn(firstToken);
        when(second.finishPreparedPrefetchKeyCollection()).thenReturn(secondToken);
        when(first.installPreparedPrefetch(firstToken)).thenReturn(true);
        when(second.installPreparedPrefetch(secondToken)).thenReturn(false);

        DistinctBatchPrefetchSupport.beginPreparedCapture();
        assertTrue(DistinctBatchPrefetchSupport.finishSession(first));
        assertTrue(DistinctBatchPrefetchSupport.finishSession(second));
        Object capture = DistinctBatchPrefetchSupport.endPreparedCapture();

        assertFalse(DistinctBatchPrefetchSupport.installPreparedCapture(capture));
        verify(first, times(1)).abortPreparedPrefetch(firstToken);
        verify(second, times(1)).abortPreparedPrefetch(secondToken);
    }

    @Test
    @SuppressWarnings("unchecked")
    void delegatesExactKeyPrefetchBeforeBatchStarts() throws Exception {
        StateMapView<Void, String, Long> delegate = mock(StateMapView.class);
        List<String> keys = List.of("bidder-1", "bidder-2");
        when(delegate.beginPrefetchKeys(keys)).thenReturn(true);
        DistinctBatchStateMapView<Void, String, Long> view = createView(delegate);

        assertTrue(view.beginPrefetchKeys(keys));
        view.beginBatch();
        view.commitBatch();

        verify(delegate, times(1)).beginPrefetchKeys(keys);
        verify(delegate, times(1)).endPrefetchKeys();
    }

    @Test
    @SuppressWarnings("unchecked")
    void collectsGeneratedDistinctKeysAndAbortsWithoutLeakingScope() throws Exception {
        StateMapView<Void, String, Long> delegate = mock(StateMapView.class);
        doAnswer(
                        invocation -> {
                            assertEquals(
                                    List.of("bidder-1", "bidder-2"), invocation.getArgument(0));
                            return true;
                        })
                .when(delegate)
                .beginPrefetchKeys(any());
        DistinctBatchStateMapView<Void, String, Long> view = createView(delegate);

        view.beginPrefetchKeyCollection(3);
        view.addPrefetchKey("bidder-1");
        view.addPrefetchKey(null);
        view.addPrefetchKey("bidder-2");
        assertTrue(view.finishPrefetchKeyCollection());
        view.abortPrefetchKeyCollection();

        verify(delegate, times(1)).beginPrefetchKeys(any());
        verify(delegate, times(1)).endPrefetchKeys();
    }

    @Test
    @SuppressWarnings("unchecked")
    void skipsSingletonAndDuplicateOnlyCollectionsBeforeBackendPrefetch() throws Exception {
        StateMapView<Void, String, Long> delegate = mock(StateMapView.class);
        DistinctBatchStateMapView<Void, String, Long> view = createView(delegate);

        view.beginPrefetchKeyCollection(1);
        view.addPrefetchKey("bidder-1");
        assertFalse(view.finishPrefetchKeyCollection());

        view.beginPrefetchKeyCollection(3);
        view.addPrefetchKey("bidder-2");
        view.addPrefetchKey("bidder-2");
        view.addPrefetchKey("bidder-2");
        assertFalse(view.finishPrefetchKeyCollection());

        verify(delegate, never()).beginPrefetchKeys(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void directPrefetchedValuesPrimeOverlayWithoutSecondMapStateLookup() throws Exception {
        StateMapView<Void, String, Long> delegate = mock(StateMapView.class);
        when(delegate.supportsDirectPrefetchedValues()).thenReturn(true);
        when(delegate.prefetchUniqueKeyValues(any())).thenReturn(java.util.Arrays.asList(7L, null));
        DistinctBatchStateMapView<Void, String, Long> view = createView(delegate);

        view.beginPrefetchKeyCollection(2);
        view.addPrefetchKey("bidder-1");
        view.addPrefetchKey("bidder-2");
        assertTrue(view.finishPrefetchKeyCollection());
        view.beginBatch();
        assertEquals(7L, view.get("bidder-1"));
        assertNull(view.get("bidder-2"));
        view.commitBatch();

        verify(delegate, times(1)).prefetchUniqueKeyValues(any());
        verify(delegate, never()).beginPrefetchKeys(any());
        verify(delegate, never()).get(any());
        verify(delegate, never()).putAll(anyMap());
        assertEquals(2, view.directOverlayValues());
        assertEquals(2, view.overlayHits());
        assertEquals(0, view.delegateGets());
    }

    @Test
    @SuppressWarnings("unchecked")
    void configurableMinimumRejectsTinyUniqueBatchesBeforeBackendBoundary() throws Exception {
        StateMapView<Void, String, Long> delegate = mock(StateMapView.class);
        DistinctBatchStateMapView<Void, String, Long> view =
                new DistinctBatchStateMapView<>(
                        delegate, StringSerializer.INSTANCE, LongSerializer.INSTANCE, 4);

        view.beginPrefetchKeyCollection(3);
        view.addPrefetchKey("bidder-1");
        view.addPrefetchKey("bidder-2");
        view.addPrefetchKey("bidder-3");
        assertFalse(view.finishPrefetchKeyCollection());

        verify(delegate, never()).beginPrefetchKeys(any());
        verify(delegate, never()).prefetchUniqueKeyValues(any());
        assertEquals(1, view.prefetchRejectedBelowMinimum());
        assertEquals(1, view.prefetchSize3());
    }

    @Test
    @SuppressWarnings("unchecked")
    void configurableMinimumRejectsByInputCountBeforeCopyingAnyKey() throws Exception {
        StateMapView<Void, String, Long> delegate = mock(StateMapView.class);
        DistinctBatchStateMapView<Void, String, Long> view =
                new DistinctBatchStateMapView<>(
                        delegate, StringSerializer.INSTANCE, LongSerializer.INSTANCE, 4);

        assertFalse(view.tryBeginPrefetchKeyCollection(3));

        verify(delegate, never()).beginPrefetchKeys(any());
        verify(delegate, never()).prefetchUniqueKeyValues(any());
        assertEquals(1, view.prefetchRejectedBelowMinimum());
        assertEquals(1, view.prefetchRejectedBeforeKeyScan());
        assertEquals(0, view.prefetchKeysCollected());
    }

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
