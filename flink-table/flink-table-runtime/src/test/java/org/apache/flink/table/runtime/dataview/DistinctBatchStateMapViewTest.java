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
import org.apache.flink.runtime.state.internal.BatchPrefetchableMapState;
import org.apache.flink.streaming.api.operators.PipelinedBatchableKeyedFunction.BatchWindowPreparationResult;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
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
    void executesSingleOwnerPreparedWaveWithoutInstallingOrConsumingCaptures() throws Exception {
        StateMapView<Void, String, Long> delegate = mock(StateMapView.class);
        BatchPrefetchableMapState.PreparedValues first =
                mock(BatchPrefetchableMapState.PreparedValues.class);
        BatchPrefetchableMapState.PreparedValues second =
                mock(BatchPrefetchableMapState.PreparedValues.class);
        Object owner = new Object();
        when(delegate.supportsDirectPrefetchedValues()).thenReturn(true);
        when(delegate.prepareUniqueKeyValues(List.of("a", "b"))).thenReturn(first);
        when(delegate.prepareUniqueKeyValues(List.of("c", "d"))).thenReturn(second);
        when(first.waveOwner()).thenReturn(owner);
        when(second.waveOwner()).thenReturn(owner);
        when(first.waveParticipation())
                .thenReturn(BatchPrefetchableMapState.PreparedValues.WaveParticipation.ELIGIBLE);
        when(second.waveParticipation())
                .thenReturn(BatchPrefetchableMapState.PreparedValues.WaveParticipation.ELIGIBLE);
        List<BatchPrefetchableMapState.PreparedValues> invokedWave = new ArrayList<>();
        doAnswer(
                        invocation -> {
                            invokedWave.addAll(invocation.getArgument(0));
                            return true;
                        })
                .when(first)
                .executeWave(any());
        DistinctBatchStateMapView<Void, String, Long> view = createView(delegate);

        Object firstCapture = capturePrepared(view, List.of("a", "b"));
        Object secondCapture = capturePrepared(view, List.of("c", "d"));

        assertEquals(
                BatchWindowPreparationResult.EXECUTED,
                DistinctBatchPrefetchSupport.executePreparedWave(
                        new Object[] {firstCapture, secondCapture}, 2));
        verify(first, times(1)).executeWave(any());
        assertEquals(List.of(first, second), invokedWave);
        verify(delegate, never()).awaitPreparedUniqueKeyValues(any());

        DistinctBatchPrefetchSupport.abortPreparedCapture(firstCapture);
        DistinctBatchPrefetchSupport.abortPreparedCapture(secondCapture);
    }

    @Test
    @SuppressWarnings("unchecked")
    void distinguishesDisabledWaveFromIneligibleHeavyToken() throws Exception {
        StateMapView<Void, String, Long> disabledDelegate = mock(StateMapView.class);
        BatchPrefetchableMapState.PreparedValues disabled =
                mock(BatchPrefetchableMapState.PreparedValues.class);
        when(disabledDelegate.supportsDirectPrefetchedValues()).thenReturn(true);
        when(disabledDelegate.prepareUniqueKeyValues(any())).thenReturn(disabled);
        when(disabled.waveParticipation())
                .thenReturn(BatchPrefetchableMapState.PreparedValues.WaveParticipation.DISABLED);
        DistinctBatchStateMapView<Void, String, Long> disabledView = createView(disabledDelegate);
        Object disabledFirst = capturePrepared(disabledView, List.of("a", "b"));
        Object disabledSecond = capturePrepared(disabledView, List.of("c", "d"));

        assertEquals(
                BatchWindowPreparationResult.UNSUPPORTED,
                DistinctBatchPrefetchSupport.executePreparedWave(
                        new Object[] {disabledFirst, disabledSecond}, 2));

        StateMapView<Void, String, Long> heavyDelegate = mock(StateMapView.class);
        BatchPrefetchableMapState.PreparedValues heavy =
                mock(BatchPrefetchableMapState.PreparedValues.class);
        when(heavyDelegate.supportsDirectPrefetchedValues()).thenReturn(true);
        when(heavyDelegate.prepareUniqueKeyValues(any())).thenReturn(heavy);
        when(heavy.waveParticipation())
                .thenReturn(BatchPrefetchableMapState.PreparedValues.WaveParticipation.INELIGIBLE);
        DistinctBatchStateMapView<Void, String, Long> heavyView = createView(heavyDelegate);
        Object heavyFirst = capturePrepared(heavyView, List.of("e", "f"));
        Object heavySecond = capturePrepared(heavyView, List.of("g", "h"));

        assertEquals(
                BatchWindowPreparationResult.RETRY_AFTER_COHORT,
                DistinctBatchPrefetchSupport.executePreparedWave(
                        new Object[] {heavyFirst, heavySecond}, 2));

        DistinctBatchPrefetchSupport.abortPreparedCapture(disabledFirst);
        DistinctBatchPrefetchSupport.abortPreparedCapture(disabledSecond);
        DistinctBatchPrefetchSupport.abortPreparedCapture(heavyFirst);
        DistinctBatchPrefetchSupport.abortPreparedCapture(heavySecond);
    }

    @Test
    @SuppressWarnings("unchecked")
    void executesIndependentPreparedWaveForEachDistinctViewColumn() throws Exception {
        StateMapView<Void, String, Long> firstDelegate = mock(StateMapView.class);
        StateMapView<Void, String, Long> secondDelegate = mock(StateMapView.class);
        BatchPrefetchableMapState.PreparedValues firstA = preparedValue(new Object());
        BatchPrefetchableMapState.PreparedValues firstB = preparedValue(firstA.waveOwner());
        BatchPrefetchableMapState.PreparedValues secondA = preparedValue(new Object());
        BatchPrefetchableMapState.PreparedValues secondB = preparedValue(secondA.waveOwner());
        when(firstDelegate.supportsDirectPrefetchedValues()).thenReturn(true);
        when(secondDelegate.supportsDirectPrefetchedValues()).thenReturn(true);
        when(firstDelegate.prepareUniqueKeyValues(List.of("a", "b"))).thenReturn(firstA);
        when(firstDelegate.prepareUniqueKeyValues(List.of("c", "d"))).thenReturn(firstB);
        when(secondDelegate.prepareUniqueKeyValues(List.of("a", "b"))).thenReturn(secondA);
        when(secondDelegate.prepareUniqueKeyValues(List.of("c", "d"))).thenReturn(secondB);
        List<BatchPrefetchableMapState.PreparedValues> firstWave = new ArrayList<>();
        List<BatchPrefetchableMapState.PreparedValues> secondWave = new ArrayList<>();
        doAnswer(
                        invocation -> {
                            firstWave.addAll(invocation.getArgument(0));
                            return true;
                        })
                .when(firstA)
                .executeWave(any());
        doAnswer(
                        invocation -> {
                            secondWave.addAll(invocation.getArgument(0));
                            return true;
                        })
                .when(secondA)
                .executeWave(any());
        List<DistinctBatchStateMapView<Void, String, Long>> views =
                List.of(createView(firstDelegate), createView(secondDelegate));

        Object firstCapture = capturePrepared(views, List.of("a", "b"));
        Object secondCapture = capturePrepared(views, List.of("c", "d"));

        assertEquals(
                BatchWindowPreparationResult.EXECUTED,
                DistinctBatchPrefetchSupport.executePreparedWave(
                        new Object[] {firstCapture, secondCapture}, 2));
        assertEquals(List.of(firstA, firstB), firstWave);
        assertEquals(List.of(secondA, secondB), secondWave);
        verify(firstA, times(1)).executeWave(any());
        verify(secondA, times(1)).executeWave(any());

        DistinctBatchPrefetchSupport.abortPreparedCapture(firstCapture);
        DistinctBatchPrefetchSupport.abortPreparedCapture(secondCapture);
    }

    @Test
    @SuppressWarnings("unchecked")
    void eligibleViewWaveIsNotBlockedByIneligibleSiblingView() throws Exception {
        StateMapView<Void, String, Long> eligibleDelegate = mock(StateMapView.class);
        StateMapView<Void, String, Long> heavyDelegate = mock(StateMapView.class);
        BatchPrefetchableMapState.PreparedValues eligibleA = preparedValue(new Object());
        BatchPrefetchableMapState.PreparedValues eligibleB = preparedValue(eligibleA.waveOwner());
        BatchPrefetchableMapState.PreparedValues heavyA =
                mock(BatchPrefetchableMapState.PreparedValues.class);
        BatchPrefetchableMapState.PreparedValues heavyB =
                mock(BatchPrefetchableMapState.PreparedValues.class);
        when(heavyA.waveParticipation())
                .thenReturn(BatchPrefetchableMapState.PreparedValues.WaveParticipation.INELIGIBLE);
        when(heavyB.waveParticipation())
                .thenReturn(BatchPrefetchableMapState.PreparedValues.WaveParticipation.INELIGIBLE);
        when(eligibleDelegate.supportsDirectPrefetchedValues()).thenReturn(true);
        when(heavyDelegate.supportsDirectPrefetchedValues()).thenReturn(true);
        when(eligibleDelegate.prepareUniqueKeyValues(List.of("a", "b"))).thenReturn(eligibleA);
        when(eligibleDelegate.prepareUniqueKeyValues(List.of("c", "d"))).thenReturn(eligibleB);
        when(heavyDelegate.prepareUniqueKeyValues(List.of("a", "b"))).thenReturn(heavyA);
        when(heavyDelegate.prepareUniqueKeyValues(List.of("c", "d"))).thenReturn(heavyB);
        doAnswer(invocation -> true).when(eligibleA).executeWave(any());
        List<DistinctBatchStateMapView<Void, String, Long>> views =
                List.of(createView(eligibleDelegate), createView(heavyDelegate));

        Object firstCapture = capturePrepared(views, List.of("a", "b"));
        Object secondCapture = capturePrepared(views, List.of("c", "d"));

        assertEquals(
                BatchWindowPreparationResult.EXECUTED,
                DistinctBatchPrefetchSupport.executePreparedWave(
                        new Object[] {firstCapture, secondCapture}, 2));
        verify(eligibleA, times(1)).executeWave(any());
        verify(heavyA, never()).executeWave(any());

        DistinctBatchPrefetchSupport.abortPreparedCapture(firstCapture);
        DistinctBatchPrefetchSupport.abortPreparedCapture(secondCapture);
    }

    @Test
    @SuppressWarnings("unchecked")
    void stableViewWaveIsNotBlockedByDifferentOwnerSiblingView() throws Exception {
        StateMapView<Void, String, Long> mismatchedDelegate = mock(StateMapView.class);
        StateMapView<Void, String, Long> stableDelegate = mock(StateMapView.class);
        BatchPrefetchableMapState.PreparedValues mismatchedA = preparedValue(new Object());
        BatchPrefetchableMapState.PreparedValues mismatchedB = preparedValue(new Object());
        BatchPrefetchableMapState.PreparedValues stableA = preparedValue(new Object());
        BatchPrefetchableMapState.PreparedValues stableB = preparedValue(stableA.waveOwner());
        when(mismatchedDelegate.supportsDirectPrefetchedValues()).thenReturn(true);
        when(stableDelegate.supportsDirectPrefetchedValues()).thenReturn(true);
        when(mismatchedDelegate.prepareUniqueKeyValues(List.of("a", "b"))).thenReturn(mismatchedA);
        when(mismatchedDelegate.prepareUniqueKeyValues(List.of("c", "d"))).thenReturn(mismatchedB);
        when(stableDelegate.prepareUniqueKeyValues(List.of("a", "b"))).thenReturn(stableA);
        when(stableDelegate.prepareUniqueKeyValues(List.of("c", "d"))).thenReturn(stableB);
        doAnswer(invocation -> true).when(stableA).executeWave(any());
        List<DistinctBatchStateMapView<Void, String, Long>> views =
                List.of(createView(mismatchedDelegate), createView(stableDelegate));

        Object firstCapture = capturePrepared(views, List.of("a", "b"));
        Object secondCapture = capturePrepared(views, List.of("c", "d"));

        assertEquals(
                BatchWindowPreparationResult.EXECUTED,
                DistinctBatchPrefetchSupport.executePreparedWave(
                        new Object[] {firstCapture, secondCapture}, 2));
        verify(mismatchedA, never()).executeWave(any());
        verify(stableA, times(1)).executeWave(any());

        DistinctBatchPrefetchSupport.abortPreparedCapture(firstCapture);
        DistinctBatchPrefetchSupport.abortPreparedCapture(secondCapture);
    }

    @Test
    @SuppressWarnings("unchecked")
    void failedPreparedWaveLeavesEveryCaptureInstallableThroughAuthoritativeFallback()
            throws Exception {
        StateMapView<Void, String, Long> delegate = mock(StateMapView.class);
        BatchPrefetchableMapState.PreparedValues first = preparedValue(new Object());
        BatchPrefetchableMapState.PreparedValues second = preparedValue(first.waveOwner());
        when(delegate.supportsDirectPrefetchedValues()).thenReturn(true);
        when(delegate.prepareUniqueKeyValues(List.of("a", "b"))).thenReturn(first);
        when(delegate.prepareUniqueKeyValues(List.of("c", "d"))).thenReturn(second);
        when(delegate.awaitPreparedUniqueKeyValues(first)).thenReturn(List.of(1L, 2L));
        when(delegate.awaitPreparedUniqueKeyValues(second)).thenReturn(List.of(3L, 4L));
        doAnswer(
                        invocation -> {
                            throw new IllegalStateException("speculative wave failure");
                        })
                .when(first)
                .executeWave(any());
        DistinctBatchStateMapView<Void, String, Long> view = createView(delegate);

        Object firstCapture = capturePrepared(view, List.of("a", "b"));
        Object secondCapture = capturePrepared(view, List.of("c", "d"));

        assertEquals(
                BatchWindowPreparationResult.RETRY_AFTER_COHORT,
                DistinctBatchPrefetchSupport.executePreparedWave(
                        new Object[] {firstCapture, secondCapture}, 2));
        assertTrue(DistinctBatchPrefetchSupport.installPreparedCapture(firstCapture));
        assertTrue(DistinctBatchPrefetchSupport.installPreparedCapture(secondCapture));
        verify(delegate, times(1)).awaitPreparedUniqueKeyValues(first);
        verify(delegate, times(1)).awaitPreparedUniqueKeyValues(second);
    }

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

    private static Object capturePrepared(
            DistinctBatchStateMapView<Void, String, Long> view, List<String> keys)
            throws Exception {
        Object session = DistinctBatchPrefetchSupport.beginSession(view, keys.size());
        DistinctBatchPrefetchSupport.beginPreparedCapture();
        for (String key : keys) {
            DistinctBatchPrefetchSupport.addSession(session, key);
        }
        assertTrue(DistinctBatchPrefetchSupport.finishSession(session));
        return DistinctBatchPrefetchSupport.endPreparedCapture();
    }

    private static Object capturePrepared(
            List<DistinctBatchStateMapView<Void, String, Long>> views, List<String> keys)
            throws Exception {
        List<Object> sessions = new ArrayList<>(views.size());
        for (DistinctBatchStateMapView<Void, String, Long> view : views) {
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
        return DistinctBatchPrefetchSupport.endPreparedCapture();
    }

    private static BatchPrefetchableMapState.PreparedValues preparedValue(Object owner) {
        BatchPrefetchableMapState.PreparedValues prepared =
                mock(BatchPrefetchableMapState.PreparedValues.class);
        when(prepared.waveOwner()).thenReturn(owner);
        when(prepared.waveParticipation())
                .thenReturn(BatchPrefetchableMapState.PreparedValues.WaveParticipation.ELIGIBLE);
        return prepared;
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
