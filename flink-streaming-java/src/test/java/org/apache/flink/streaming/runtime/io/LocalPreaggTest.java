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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.state.BatchKeyGroupingSupport;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BatchableKeyedFunction;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.PipelinedBatchableKeyedFunction;
import org.apache.flink.streaming.api.operators.PipelinedBatchableKeyedFunction.BatchWindowPreparationResult;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class LocalPreaggTest {

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testMaterializedPipelinePreparesNextKeyBeforeProcessingCurrentKey() throws Exception {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "b", "a", "c"),
                        Arrays.asList(1, 2, 3, 4),
                        new int[] {3, 0, 1, 0, 2});
        List<String> events = new ArrayList<>();
        PipelinedBatchableKeyedFunction pipeline =
                new PipelinedBatchableKeyedFunction<Object, Object>() {
                    @Override
                    public Object prepareBatchForKey(Object key, List<Object> inputs) {
                        events.add("prepare:" + key + ":" + new ArrayList<>(inputs));
                        return "token-" + key;
                    }

                    @Override
                    public void processPreparedBatchForKey(
                            Object key,
                            List<Object> inputs,
                            Object prepared,
                            Collector<Object> out) {
                        events.add(
                                "process:" + key + ":" + new ArrayList<>(inputs) + ":" + prepared);
                    }

                    @Override
                    public void abortPreparedBatch(Object prepared) {
                        events.add("abort:" + prepared);
                    }

                    @Override
                    public void processBatchForKey(
                            Object currentKey, List<Object> inputs, Collector<Object> out) {
                        throw new AssertionError("pipeline must use prepared dispatch");
                    }
                };

        LocalPreagg.dispatchMaterializedPipeline(
                mock(AbstractStreamOperator.class),
                pipeline,
                groups,
                mock(TimestampedCollector.class));

        assertEquals(
                Arrays.asList(
                        "prepare:a:[1, 3]",
                        "prepare:b:[2]",
                        "process:a:[1, 3]:token-a",
                        "prepare:c:[4]",
                        "process:b:[2]:token-b",
                        "process:c:[4]:token-c"),
                events);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testMaterializedPipelineAbortsCurrentAndNextPreparationOnFailure() {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "b"), Arrays.asList(1, 2), new int[] {2, 0, 1});
        List<String> events = new ArrayList<>();
        PipelinedBatchableKeyedFunction pipeline =
                new PipelinedBatchableKeyedFunction<Object, Object>() {
                    @Override
                    public Object prepareBatchForKey(Object key, List<Object> inputs) {
                        events.add("prepare:" + key);
                        return "token-" + key;
                    }

                    @Override
                    public void processPreparedBatchForKey(
                            Object key,
                            List<Object> inputs,
                            Object prepared,
                            Collector<Object> out) {
                        events.add("process:" + key);
                        throw new IllegalStateException("expected test failure");
                    }

                    @Override
                    public void abortPreparedBatch(Object prepared) {
                        events.add("abort:" + prepared);
                    }

                    @Override
                    public void processBatchForKey(
                            Object currentKey, List<Object> inputs, Collector<Object> out) {
                        throw new AssertionError("pipeline must use prepared dispatch");
                    }
                };

        assertThrows(
                IllegalStateException.class,
                () ->
                        LocalPreagg.dispatchMaterializedPipeline(
                                mock(AbstractStreamOperator.class),
                                pipeline,
                                groups,
                                mock(TimestampedCollector.class)));

        assertEquals(
                Arrays.asList(
                        "prepare:a", "prepare:b", "process:a", "abort:token-a", "abort:token-b"),
                events);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testMaterializedPipelineLookaheadFourKeepsFiveGroupsPrepared() throws Exception {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "b", "c", "d", "e", "f"),
                        Arrays.asList(1, 2, 3, 4, 5, 6),
                        null);
        RecordingPipeline pipeline = new RecordingPipeline(null, null);

        LocalPreagg.dispatchMaterializedPipeline(
                mock(AbstractStreamOperator.class),
                pipeline,
                groups,
                mock(TimestampedCollector.class),
                4);

        assertEquals(
                Arrays.asList(
                        "prepare:a",
                        "prepare:b",
                        "prepare:c",
                        "prepare:d",
                        "prepare:e",
                        "process:a:token-a",
                        "prepare:f",
                        "process:b:token-b",
                        "process:c:token-c",
                        "process:d:token-d",
                        "process:e:token-e",
                        "process:f:token-f"),
                pipeline.events);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testMaterializedPipelineLookaheadFourHandlesFewerGroupsThanWindow() throws Exception {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(Arrays.asList("a", "b"), Arrays.asList(1, 2), null);
        RecordingPipeline pipeline = new RecordingPipeline(null, null);

        LocalPreagg.dispatchMaterializedPipeline(
                mock(AbstractStreamOperator.class),
                pipeline,
                groups,
                mock(TimestampedCollector.class),
                4);

        assertEquals(
                Arrays.asList("prepare:a", "prepare:b", "process:a:token-a", "process:b:token-b"),
                pipeline.events);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testMaterializedPipelineLookaheadFourAbortsOutstandingGroupsOnTailPrepareFailure() {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "b", "c", "d", "e", "f"),
                        Arrays.asList(1, 2, 3, 4, 5, 6),
                        null);
        RecordingPipeline pipeline = new RecordingPipeline("f", null);

        assertThrows(
                IllegalStateException.class,
                () ->
                        LocalPreagg.dispatchMaterializedPipeline(
                                mock(AbstractStreamOperator.class),
                                pipeline,
                                groups,
                                mock(TimestampedCollector.class),
                                4));

        assertEquals(
                Arrays.asList(
                        "prepare:a",
                        "prepare:b",
                        "prepare:c",
                        "prepare:d",
                        "prepare:e",
                        "process:a:token-a",
                        "prepare:f",
                        "abort:token-b",
                        "abort:token-c",
                        "abort:token-d",
                        "abort:token-e"),
                pipeline.events);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testMaterializedPipelineLookaheadFourAbortsWholeWindowOnProcessFailure() {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "b", "c", "d", "e", "f"),
                        Arrays.asList(1, 2, 3, 4, 5, 6),
                        null);
        RecordingPipeline pipeline = new RecordingPipeline(null, "a");

        assertThrows(
                IllegalStateException.class,
                () ->
                        LocalPreagg.dispatchMaterializedPipeline(
                                mock(AbstractStreamOperator.class),
                                pipeline,
                                groups,
                                mock(TimestampedCollector.class),
                                4));

        assertEquals(
                Arrays.asList(
                        "prepare:a",
                        "prepare:b",
                        "prepare:c",
                        "prepare:d",
                        "prepare:e",
                        "process:a:token-a",
                        "abort:token-a",
                        "abort:token-b",
                        "abort:token-c",
                        "abort:token-d",
                        "abort:token-e"),
                pipeline.events);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testPipelineCountersPublishExactSuccessfulWindowDeltas() throws Exception {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "b", "c", "d", "e", "f"),
                        Arrays.asList(1, 2, 3, 4, 5, 6),
                        null);
        long windows = pipelineCounter("PIPELINE_WINDOWS");
        long groupCount = pipelineCounter("PIPELINE_GROUPS");
        long prepared = pipelineCounter("PIPELINE_PREPARED_GROUPS");
        long preparedAhead = pipelineCounter("PIPELINE_PREPARED_AHEAD_GROUPS");
        long consumed = pipelineCounter("PIPELINE_CONSUMED_GROUPS");
        long cancelled = pipelineCounter("PIPELINE_CANCELLED_GROUPS");
        long inFlight = pipelineCounter("PIPELINE_PROCESS_WITH_FUTURE_IN_FLIGHT");
        long exceptionAborts = pipelineCounter("PIPELINE_EXCEPTION_ABORTS");

        LocalPreagg.dispatchMaterializedPipeline(
                mock(AbstractStreamOperator.class),
                new RecordingPipeline(null, null),
                groups,
                mock(TimestampedCollector.class),
                4);

        assertEquals(1L, pipelineCounter("PIPELINE_WINDOWS") - windows);
        assertEquals(6L, pipelineCounter("PIPELINE_GROUPS") - groupCount);
        assertEquals(6L, pipelineCounter("PIPELINE_PREPARED_GROUPS") - prepared);
        assertEquals(5L, pipelineCounter("PIPELINE_PREPARED_AHEAD_GROUPS") - preparedAhead);
        assertEquals(6L, pipelineCounter("PIPELINE_CONSUMED_GROUPS") - consumed);
        assertEquals(0L, pipelineCounter("PIPELINE_CANCELLED_GROUPS") - cancelled);
        assertEquals(5L, pipelineCounter("PIPELINE_PROCESS_WITH_FUTURE_IN_FLIGHT") - inFlight);
        assertEquals(0L, pipelineCounter("PIPELINE_EXCEPTION_ABORTS") - exceptionAborts);
        assertTrue(pipelineCounter("PIPELINE_PEAK_PREPARED_AHEAD") >= 4L);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testPipelineCountersPublishExactlyOnceAfterProcessFailure() throws Exception {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "b", "c", "d", "e", "f"),
                        Arrays.asList(1, 2, 3, 4, 5, 6),
                        null);
        long windows = pipelineCounter("PIPELINE_WINDOWS");
        long groupCount = pipelineCounter("PIPELINE_GROUPS");
        long prepared = pipelineCounter("PIPELINE_PREPARED_GROUPS");
        long preparedAhead = pipelineCounter("PIPELINE_PREPARED_AHEAD_GROUPS");
        long consumed = pipelineCounter("PIPELINE_CONSUMED_GROUPS");
        long cancelled = pipelineCounter("PIPELINE_CANCELLED_GROUPS");
        long inFlight = pipelineCounter("PIPELINE_PROCESS_WITH_FUTURE_IN_FLIGHT");
        long exceptionAborts = pipelineCounter("PIPELINE_EXCEPTION_ABORTS");

        assertThrows(
                IllegalStateException.class,
                () ->
                        LocalPreagg.dispatchMaterializedPipeline(
                                mock(AbstractStreamOperator.class),
                                new RecordingPipeline(null, "a"),
                                groups,
                                mock(TimestampedCollector.class),
                                4));

        assertEquals(1L, pipelineCounter("PIPELINE_WINDOWS") - windows);
        assertEquals(6L, pipelineCounter("PIPELINE_GROUPS") - groupCount);
        assertEquals(5L, pipelineCounter("PIPELINE_PREPARED_GROUPS") - prepared);
        assertEquals(4L, pipelineCounter("PIPELINE_PREPARED_AHEAD_GROUPS") - preparedAhead);
        assertEquals(0L, pipelineCounter("PIPELINE_CONSUMED_GROUPS") - consumed);
        assertEquals(5L, pipelineCounter("PIPELINE_CANCELLED_GROUPS") - cancelled);
        assertEquals(1L, pipelineCounter("PIPELINE_PROCESS_WITH_FUTURE_IN_FLIGHT") - inFlight);
        assertEquals(1L, pipelineCounter("PIPELINE_EXCEPTION_ABORTS") - exceptionAborts);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testSparsePipelineCountsLookaheadOnlyAcrossPreparationCandidates() throws Exception {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "a", "b", "b", "b", "c", "d", "d", "d"),
                        Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9),
                        null);
        SparseRecordingPipeline pipeline = new SparseRecordingPipeline(3, null);
        long candidates = pipelineCounter("PIPELINE_PREPARATION_CANDIDATE_GROUPS");
        long bypassed = pipelineCounter("PIPELINE_BYPASSED_GROUPS");
        long prepared = pipelineCounter("PIPELINE_PREPARED_GROUPS");
        long consumed = pipelineCounter("PIPELINE_CONSUMED_GROUPS");
        long inFlight = pipelineCounter("PIPELINE_PROCESS_WITH_FUTURE_IN_FLIGHT");

        LocalPreagg.dispatchMaterializedPipeline(
                mock(AbstractStreamOperator.class),
                pipeline,
                groups,
                mock(TimestampedCollector.class),
                1);

        assertEquals(
                Arrays.asList(
                        "prepare:b", "sync:a", "prepare:d", "process:b", "sync:c", "process:d"),
                pipeline.events);
        assertEquals(2L, pipelineCounter("PIPELINE_PREPARATION_CANDIDATE_GROUPS") - candidates);
        assertEquals(2L, pipelineCounter("PIPELINE_BYPASSED_GROUPS") - bypassed);
        assertEquals(2L, pipelineCounter("PIPELINE_PREPARED_GROUPS") - prepared);
        assertEquals(4L, pipelineCounter("PIPELINE_CONSUMED_GROUPS") - consumed);
        assertEquals(3L, pipelineCounter("PIPELINE_PROCESS_WITH_FUTURE_IN_FLIGHT") - inFlight);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testSparsePipelineAbortsAllHeavyPreparationsWhenSynchronousGroupFails() throws Exception {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "b", "b", "b", "c", "c", "c"),
                        Arrays.asList(1, 2, 3, 4, 5, 6, 7),
                        null);
        SparseRecordingPipeline pipeline = new SparseRecordingPipeline(3, "a");
        long cancelled = pipelineCounter("PIPELINE_CANCELLED_GROUPS");
        long exceptionAborts = pipelineCounter("PIPELINE_EXCEPTION_ABORTS");

        assertThrows(
                IllegalStateException.class,
                () ->
                        LocalPreagg.dispatchMaterializedPipeline(
                                mock(AbstractStreamOperator.class),
                                pipeline,
                                groups,
                                mock(TimestampedCollector.class),
                                2));

        assertEquals(
                Arrays.asList("prepare:b", "prepare:c", "sync:a", "abort:token-b", "abort:token-c"),
                pipeline.events);
        assertEquals(2L, pipelineCounter("PIPELINE_CANCELLED_GROUPS") - cancelled);
        assertEquals(1L, pipelineCounter("PIPELINE_EXCEPTION_ABORTS") - exceptionAborts);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testSuccessfulPreparedWaveKeepsEachCohortStableUntilConsumed() throws Exception {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "b", "c", "d", "e", "f"),
                        Arrays.asList(1, 2, 3, 4, 5, 6),
                        null);
        WaveRecordingPipeline pipeline = new WaveRecordingPipeline();

        LocalPreagg.dispatchMaterializedPipeline(
                mock(AbstractStreamOperator.class),
                pipeline,
                groups,
                mock(TimestampedCollector.class),
                4);

        assertEquals(
                Arrays.asList(
                        "prepare:a",
                        "prepare:b",
                        "prepare:c",
                        "prepare:d",
                        "prepare:e",
                        "wave:token-b,token-c,token-d,token-e",
                        "process:a:token-a",
                        "process:b:token-b",
                        "process:c:token-c",
                        "process:d:token-d",
                        "process:e:token-e",
                        "prepare:f",
                        "process:f:token-f"),
                pipeline.events);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testSparseFutureWaveKeepsFirstPreparedTokenWhenCurrentGroupIsLight() throws Exception {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "b", "b", "b", "c", "c", "c", "d"),
                        Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8),
                        null);
        WaveRecordingPipeline pipeline = new WaveRecordingPipeline(3);

        LocalPreagg.dispatchMaterializedPipeline(
                mock(AbstractStreamOperator.class),
                pipeline,
                groups,
                mock(TimestampedCollector.class),
                2);

        assertEquals(
                Arrays.asList(
                        "prepare:b",
                        "prepare:c",
                        "wave:token-b,token-c",
                        "sync:a",
                        "process:b:token-b",
                        "process:c:token-c",
                        "sync:d"),
                pipeline.events);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testSparseFutureWaveExcludesPreparedCurrentGroup() throws Exception {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "a", "a", "b", "b", "b", "c", "c", "c"),
                        Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9),
                        null);
        WaveRecordingPipeline pipeline = new WaveRecordingPipeline(3);

        LocalPreagg.dispatchMaterializedPipeline(
                mock(AbstractStreamOperator.class),
                pipeline,
                groups,
                mock(TimestampedCollector.class),
                2);

        assertEquals(
                Arrays.asList(
                        "prepare:a",
                        "prepare:b",
                        "prepare:c",
                        "wave:token-b,token-c",
                        "process:a:token-a",
                        "process:b:token-b",
                        "process:c:token-c"),
                pipeline.events);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testUnsupportedPreparedWaveIsNotRetriedForEverySlidingGroup() throws Exception {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "b", "c", "d", "e", "f"),
                        Arrays.asList(1, 2, 3, 4, 5, 6),
                        null);
        WaveResultPipeline pipeline =
                new WaveResultPipeline(BatchWindowPreparationResult.UNSUPPORTED);

        LocalPreagg.dispatchMaterializedPipeline(
                mock(AbstractStreamOperator.class),
                pipeline,
                groups,
                mock(TimestampedCollector.class),
                2);

        assertEquals(1, pipeline.waveAttempts);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testRejectedPreparedWaveRetriesOnlyAfterRepresentedCohortDrains() throws Exception {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "b", "c", "d", "e", "f"),
                        Arrays.asList(1, 2, 3, 4, 5, 6),
                        null);
        WaveResultPipeline pipeline =
                new WaveResultPipeline(BatchWindowPreparationResult.RETRY_AFTER_COHORT);

        LocalPreagg.dispatchMaterializedPipeline(
                mock(AbstractStreamOperator.class),
                pipeline,
                groups,
                mock(TimestampedCollector.class),
                2);

        assertEquals(2, pipeline.waveAttempts);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void testSparseRejectedWaveDoesNotConsumeCooldownAcrossLightGroups() throws Exception {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList(
                                "a", "a", "a", "b", "c", "c", "c", "d", "e", "e", "e", "f", "g",
                                "g", "g", "h", "i", "i", "i"),
                        Arrays.asList(
                                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
                        null);
        WaveResultPipeline pipeline =
                new WaveResultPipeline(BatchWindowPreparationResult.RETRY_AFTER_COHORT, 3);

        LocalPreagg.dispatchMaterializedPipeline(
                mock(AbstractStreamOperator.class),
                pipeline,
                groups,
                mock(TimestampedCollector.class),
                2);

        assertEquals(2, pipeline.waveAttempts);
    }

    @Test
    void testDetectsDirectBatchableOperator() {
        assertTrue(LocalPreagg.hasBatchableTarget(new BatchableInputOperator()));
        assertFalse(LocalPreagg.hasBatchableTarget(mock(Input.class)));
    }

    @Test
    void testNativeGroupPlanPreservesFirstSeenOrderAndInputOrder() {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "b", "a", "c", "b"),
                        Arrays.asList(1, 2, 3, 4, 5),
                        new int[] {3, 0, 1, 0, 2, 1});

        assertTrue(groups.nativeGrouped);
        assertEquals(Arrays.asList("a", "b", "c"), groups.keys);
        assertEquals(Arrays.asList(1, 3), groups.values.get(0));
        assertEquals(Arrays.asList(2, 5), groups.values.get(1));
        assertEquals(Arrays.asList(4), groups.values.get(2));
        assertTrue(groups.values.get(0) instanceof LocalPreagg.MutableArraySliceList);
    }

    @Test
    void testNativeGroupSliceSupportsIteratorRemovalWithoutAffectingOtherGroups() {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "b", "a", "c", "b"),
                        Arrays.asList(1, 2, 3, 4, 5),
                        new int[] {3, 0, 1, 0, 2, 1});

        Iterator<Object> firstGroup = groups.values.get(0).iterator();
        assertEquals(1, firstGroup.next());
        firstGroup.remove();

        assertEquals(Arrays.asList(3), groups.values.get(0));
        assertEquals(Arrays.asList(2, 5), groups.values.get(1));
        assertEquals(Arrays.asList(4), groups.values.get(2));
    }

    @Test
    void testMismatchedKeyValueVectorsAreRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        LocalPreagg.groupInputs(
                                Arrays.asList("a", "b"), Arrays.asList(1), new int[] {2, 0, 1}));
    }

    @Test
    void testInvalidNativeGroupPlanFallsBackToJavaGrouping() {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "b", "a"),
                        Arrays.asList(1, 2, 3),
                        new int[] {2, 0, 9, 0});

        assertFalse(groups.nativeGrouped);
        assertEquals(Arrays.asList("a", "b"), groups.keys);
        assertEquals(Arrays.asList(1, 3), groups.values.get(0));
    }

    @Test
    void testNativeGroupCollisionFallsBackToJavaGrouping() {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "b", "a"),
                        Arrays.asList(1, 2, 3),
                        new int[] {1, 0, 0, 0});

        assertFalse(groups.nativeGrouped);
        assertEquals(Arrays.asList("a", "b"), groups.keys);
        assertEquals(Arrays.asList(1, 3), groups.values.get(0));
        assertEquals(Arrays.asList(2), groups.values.get(1));
    }

    @Test
    void testPermutedNativeGroupIdsFallBackToJavaFirstSeenOrder() {
        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputs(
                        Arrays.asList("a", "b", "a"),
                        Arrays.asList(1, 2, 3),
                        new int[] {2, 1, 0, 1});

        assertFalse(groups.nativeGrouped);
        assertEquals(Arrays.asList("a", "b"), groups.keys);
        assertEquals(Arrays.asList(1, 3), groups.values.get(0));
        assertEquals(Arrays.asList(2), groups.values.get(1));
    }

    @Test
    void testPackedPlanPreservesFirstSeenAndArrivalOrder() {
        LocalPreagg.NativeGroupingWorkspace workspace =
                packedWorkspace(
                        5,
                        3,
                        new int[] {0, 1, 3},
                        new int[] {0, 2, 4, 5},
                        new int[] {0, 1, 0, 2, 1});

        LocalPreagg.GroupedInputs groups =
                LocalPreagg.groupInputsPacked(
                        Arrays.asList("a", "b", "a", "c", "b"),
                        Arrays.asList(1, 2, 3, 4, 5),
                        workspace,
                        3);

        assertTrue(groups.nativeGrouped);
        assertEquals(Arrays.asList("a", "b", "c"), groups.keys);
        assertEquals(Arrays.asList(1, 3), groups.values.get(0));
        assertEquals(Arrays.asList(2, 5), groups.values.get(1));
        assertEquals(Arrays.asList(4), groups.values.get(2));
    }

    @Test
    void testPackedPlanHashCollisionIsRejectedBeforeDispatch() {
        CollisionKey left = new CollisionKey("left");
        CollisionKey right = new CollisionKey("right");
        LocalPreagg.NativeGroupingWorkspace workspace =
                packedWorkspace(2, 1, new int[] {0}, new int[] {0, 2}, new int[] {0, 0});

        assertNull(
                LocalPreagg.groupInputsPacked(
                        Arrays.asList(left, right), Arrays.asList(1, 2), workspace, 1));
    }

    @Test
    void testPackedPlanRejectsUncommittedHeaderAndBadOffsets() {
        LocalPreagg.NativeGroupingWorkspace uncommitted =
                packedWorkspace(2, 2, new int[] {0, 1}, new int[] {0, 1, 2}, new int[] {0, 1});
        uncommitted.planBuffer().putInt(0, 0);
        assertNull(
                LocalPreagg.groupInputsPacked(
                        Arrays.asList("a", "b"), Arrays.asList(1, 2), uncommitted, 2));

        LocalPreagg.NativeGroupingWorkspace badOffsets =
                packedWorkspace(2, 2, new int[] {0, 1}, new int[] {0, 2, 2}, new int[] {0, 1});
        assertNull(
                LocalPreagg.groupInputsPacked(
                        Arrays.asList("a", "b"), Arrays.asList(1, 2), badOffsets, 2));
    }

    @Test
    void testIndexedPackedPlanConsumesOriginalRecordsInGroupArrivalOrder() {
        LocalPreagg.NativeGroupingWorkspace workspace =
                packedWorkspace(
                        5,
                        3,
                        new int[] {0, 1, 3},
                        new int[] {0, 2, 4, 5},
                        new int[] {0, 1, 0, 2, 1});
        List<String> keys = Arrays.asList("a", "b", "a", "c", "b");
        for (int source = 0; source < keys.size(); source++) {
            workspace.putIndexedSource(
                    source, source, keys.get(source), keys.get(source).hashCode());
        }

        LocalPreagg.IndexedGroups groups = LocalPreagg.validatePackedIndexedGroups(workspace, 5, 3);

        assertEquals(Arrays.asList("a", "b", "c"), groups.keys);
        assertEquals(3, groups.groupCount);
        StreamRecord<?>[] records =
                new StreamRecord<?>[] {
                    new StreamRecord<>(1),
                    new StreamRecord<>(2),
                    new StreamRecord<>(3),
                    new StreamRecord<>(4),
                    new StreamRecord<>(5)
                };
        LocalPreagg.IndexedRecordValueList values = new LocalPreagg.IndexedRecordValueList();
        values.reset(
                records,
                groups.bufferIndexesByGroup,
                groups.groupOffsets[0],
                groups.groupOffsets[1]);
        assertEquals(Arrays.asList(1, 3), values);
        values.reset(
                records,
                groups.bufferIndexesByGroup,
                groups.groupOffsets[1],
                groups.groupOffsets[2]);
        assertEquals(Arrays.asList(2, 5), values);
        values.reset(
                records,
                groups.bufferIndexesByGroup,
                groups.groupOffsets[2],
                groups.groupOffsets[3]);
        assertEquals(Arrays.asList(4), values);
    }

    @Test
    void testIndexedValueRemovalDoesNotChangeFollowingGroup() {
        LocalPreagg.NativeGroupingWorkspace workspace =
                packedWorkspace(
                        4, 2, new int[] {0, 1}, new int[] {0, 2, 4}, new int[] {0, 1, 0, 1});
        List<String> keys = Arrays.asList("a", "b", "a", "b");
        for (int source = 0; source < keys.size(); source++) {
            workspace.putIndexedSource(
                    source, source, keys.get(source), keys.get(source).hashCode());
        }
        LocalPreagg.IndexedGroups groups = LocalPreagg.validatePackedIndexedGroups(workspace, 4, 2);
        StreamRecord<?>[] records =
                new StreamRecord<?>[] {
                    new StreamRecord<>(1),
                    new StreamRecord<>(2),
                    new StreamRecord<>(3),
                    new StreamRecord<>(4)
                };
        LocalPreagg.IndexedRecordValueList values = new LocalPreagg.IndexedRecordValueList();
        values.reset(
                records,
                groups.bufferIndexesByGroup,
                groups.groupOffsets[0],
                groups.groupOffsets[1]);
        Iterator<Object> firstGroup = values.iterator();
        assertEquals(1, firstGroup.next());
        firstGroup.remove();
        assertEquals(Arrays.asList(3), values);

        values.reset(
                records,
                groups.bufferIndexesByGroup,
                groups.groupOffsets[1],
                groups.groupOffsets[2]);
        assertEquals(Arrays.asList(2, 4), values);
    }

    @Test
    void testIndexedPackedPlanRejectsHashCollisionBeforeProcessing() {
        CollisionKey left = new CollisionKey("left");
        CollisionKey right = new CollisionKey("right");
        LocalPreagg.NativeGroupingWorkspace workspace =
                packedWorkspace(2, 1, new int[] {0}, new int[] {0, 2}, new int[] {0, 0});
        workspace.putIndexedSource(0, 0, left, left.hashCode());
        workspace.putIndexedSource(1, 1, right, right.hashCode());

        assertNull(LocalPreagg.validatePackedIndexedGroups(workspace, 2, 1));
    }

    @Test
    void testIndexedPackedPlanMatchesJavaGroupingAcrossRandomBatchesAndBufferHoles() {
        Random random = new Random(0x4b554e50454e47L);
        for (int trial = 0; trial < 250; trial++) {
            int sourceCount = 1 + random.nextInt(64);
            int distinctKeyBound = 1 + random.nextInt(Math.min(12, sourceCount));
            List<Object> keys = new ArrayList<>(sourceCount);
            List<Object> expectedValues = new ArrayList<>(sourceCount);
            LinkedHashMap<Object, Integer> groupIds = new LinkedHashMap<>();
            int[] sourceGroups = new int[sourceCount];
            int[] firstSources = new int[distinctKeyBound];
            int[] counts = new int[distinctKeyBound];
            Arrays.fill(firstSources, -1);
            for (int source = 0; source < sourceCount; source++) {
                String key = "key-" + random.nextInt(distinctKeyBound);
                Integer group = groupIds.get(key);
                if (group == null) {
                    group = groupIds.size();
                    groupIds.put(key, group);
                    firstSources[group] = source;
                }
                keys.add(key);
                expectedValues.add(trial * 1000 + source);
                sourceGroups[source] = group;
                counts[group]++;
            }
            int groupCount = groupIds.size();
            int[] offsets = new int[groupCount + 1];
            for (int group = 0; group < groupCount; group++) {
                offsets[group + 1] = offsets[group] + counts[group];
            }
            firstSources = Arrays.copyOf(firstSources, groupCount);

            LocalPreagg.NativeGroupingWorkspace workspace =
                    packedWorkspace(sourceCount, groupCount, firstSources, offsets, sourceGroups);
            StreamRecord<?>[] records = new StreamRecord<?>[sourceCount * 2 + 3];
            int bufferIndex = random.nextInt(3);
            for (int source = 0; source < sourceCount; source++) {
                records[bufferIndex] = new StreamRecord<>(expectedValues.get(source));
                workspace.putIndexedSource(
                        source, bufferIndex, keys.get(source), keys.get(source).hashCode());
                bufferIndex += 1 + random.nextInt(2);
            }

            LocalPreagg.IndexedGroups actual =
                    LocalPreagg.validatePackedIndexedGroups(workspace, sourceCount, groupCount);
            assertEquals(new ArrayList<>(groupIds.keySet()), actual.keys, "trial=" + trial);

            LinkedHashMap<Object, List<Object>> expectedGroups = new LinkedHashMap<>();
            for (int source = 0; source < sourceCount; source++) {
                expectedGroups
                        .computeIfAbsent(keys.get(source), ignored -> new ArrayList<>())
                        .add(expectedValues.get(source));
            }
            LocalPreagg.IndexedRecordValueList actualValues =
                    new LocalPreagg.IndexedRecordValueList();
            int group = 0;
            for (Map.Entry<Object, List<Object>> expected : expectedGroups.entrySet()) {
                actualValues.reset(
                        records,
                        actual.bufferIndexesByGroup,
                        actual.groupOffsets[group],
                        actual.groupOffsets[group + 1]);
                assertEquals(expected.getValue(), actualValues, "trial=" + trial);
                group++;
            }
        }
    }

    private static LocalPreagg.NativeGroupingWorkspace packedWorkspace(
            int sourceCount,
            int groupCount,
            int[] firstSources,
            int[] offsets,
            int[] sourceGroups) {
        LocalPreagg.NativeGroupingWorkspace workspace = new LocalPreagg.NativeGroupingWorkspace();
        workspace.prepare(sourceCount);
        ByteBuffer plan = workspace.planBuffer();
        int firstBase = BatchKeyGroupingSupport.PACKED_PLAN_HEADER_BYTES;
        int offsetsBase = firstBase + groupCount * Integer.BYTES;
        int groupsBase = offsetsBase + (groupCount + 1) * Integer.BYTES;
        for (int group = 0; group < groupCount; group++) {
            plan.putInt(firstBase + group * Integer.BYTES, firstSources[group]);
        }
        for (int group = 0; group <= groupCount; group++) {
            plan.putInt(offsetsBase + group * Integer.BYTES, offsets[group]);
        }
        for (int source = 0; source < sourceCount; source++) {
            plan.putInt(groupsBase + source * Integer.BYTES, sourceGroups[source]);
        }
        plan.putInt(Integer.BYTES, BatchKeyGroupingSupport.PACKED_PLAN_VERSION);
        plan.putInt(2 * Integer.BYTES, sourceCount);
        plan.putInt(3 * Integer.BYTES, groupCount);
        plan.putInt(0, BatchKeyGroupingSupport.PACKED_PLAN_MAGIC);
        return workspace;
    }

    private static final class CollisionKey {
        private final String value;

        private CollisionKey(String value) {
            this.value = value;
        }

        @Override
        public int hashCode() {
            return 7;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof CollisionKey && value.equals(((CollisionKey) other).value);
        }
    }

    private static final class RecordingPipeline
            implements PipelinedBatchableKeyedFunction<Object, Object> {
        private final Object failPrepareKey;
        private final Object failProcessKey;
        private final List<String> events = new ArrayList<>();

        private RecordingPipeline(Object failPrepareKey, Object failProcessKey) {
            this.failPrepareKey = failPrepareKey;
            this.failProcessKey = failProcessKey;
        }

        @Override
        public Object prepareBatchForKey(Object key, List<Object> inputs) {
            events.add("prepare:" + key);
            if (key.equals(failPrepareKey)) {
                throw new IllegalStateException("expected prepare failure");
            }
            return "token-" + key;
        }

        @Override
        public void processPreparedBatchForKey(
                Object key, List<Object> inputs, Object prepared, Collector<Object> out) {
            events.add("process:" + key + ":" + prepared);
            if (key.equals(failProcessKey)) {
                throw new IllegalStateException("expected process failure");
            }
        }

        @Override
        public void abortPreparedBatch(Object prepared) {
            events.add("abort:" + prepared);
        }

        @Override
        public void processBatchForKey(
                Object currentKey, List<Object> inputs, Collector<Object> out) {
            throw new AssertionError("pipeline must use prepared dispatch");
        }
    }

    private static final class SparseRecordingPipeline
            implements PipelinedBatchableKeyedFunction<Object, Object> {
        private final int minimumInputs;
        private final Object failSyncKey;
        private final List<String> events = new ArrayList<>();

        private SparseRecordingPipeline(int minimumInputs, Object failSyncKey) {
            this.minimumInputs = minimumInputs;
            this.failSyncKey = failSyncKey;
        }

        @Override
        public int minimumBatchPreparationInputCount() {
            return minimumInputs;
        }

        @Override
        public Object prepareBatchForKey(Object key, List<Object> inputs) {
            events.add("prepare:" + key);
            return "token-" + key;
        }

        @Override
        public void processPreparedBatchForKey(
                Object key, List<Object> inputs, Object prepared, Collector<Object> out) {
            events.add("process:" + key);
        }

        @Override
        public void abortPreparedBatch(Object prepared) {
            events.add("abort:" + prepared);
        }

        @Override
        public void processBatchForKey(
                Object currentKey, List<Object> inputs, Collector<Object> out) {
            events.add("sync:" + currentKey);
            if (currentKey.equals(failSyncKey)) {
                throw new IllegalStateException("expected synchronous failure");
            }
        }
    }

    private static final class WaveRecordingPipeline
            implements PipelinedBatchableKeyedFunction<Object, Object> {
        private final List<String> events = new ArrayList<>();
        private final int minimumInputs;

        private WaveRecordingPipeline() {
            this(1);
        }

        private WaveRecordingPipeline(int minimumInputs) {
            this.minimumInputs = minimumInputs;
        }

        @Override
        public int minimumBatchPreparationInputCount() {
            return minimumInputs;
        }

        @Override
        public Object prepareBatchForKey(Object key, List<Object> inputs) {
            events.add("prepare:" + key);
            return "token-" + key;
        }

        @Override
        public BatchWindowPreparationResult prepareBatchWindow(
                Object[] prepared, int head, int count) {
            StringBuilder event = new StringBuilder("wave:");
            for (int index = 0; index < count; index++) {
                if (index > 0) {
                    event.append(',');
                }
                event.append(prepared[(head + index) % prepared.length]);
            }
            events.add(event.toString());
            return BatchWindowPreparationResult.EXECUTED;
        }

        @Override
        public void processPreparedBatchForKey(
                Object key, List<Object> inputs, Object prepared, Collector<Object> out) {
            events.add("process:" + key + ":" + prepared);
        }

        @Override
        public void abortPreparedBatch(Object prepared) {
            events.add("abort:" + prepared);
        }

        @Override
        public void processBatchForKey(
                Object currentKey, List<Object> inputs, Collector<Object> out) {
            events.add("sync:" + currentKey);
        }
    }

    private static final class WaveResultPipeline
            implements PipelinedBatchableKeyedFunction<Object, Object> {
        private final BatchWindowPreparationResult result;
        private final int minimumInputs;
        private int waveAttempts;

        private WaveResultPipeline(BatchWindowPreparationResult result) {
            this(result, 1);
        }

        private WaveResultPipeline(BatchWindowPreparationResult result, int minimumInputs) {
            this.result = result;
            this.minimumInputs = minimumInputs;
        }

        @Override
        public int minimumBatchPreparationInputCount() {
            return minimumInputs;
        }

        @Override
        public Object prepareBatchForKey(Object key, List<Object> inputs) {
            return "token-" + key;
        }

        @Override
        public BatchWindowPreparationResult prepareBatchWindow(
                Object[] prepared, int head, int count) {
            waveAttempts++;
            return result;
        }

        @Override
        public void processPreparedBatchForKey(
                Object key, List<Object> inputs, Object prepared, Collector<Object> out) {}

        @Override
        public void abortPreparedBatch(Object prepared) {}

        @Override
        public void processBatchForKey(
                Object currentKey, List<Object> inputs, Collector<Object> out) {}
    }

    private static final class BatchableInputOperator extends AbstractStreamOperator<Object>
            implements Input<Object>, BatchableKeyedFunction<Object, Object> {

        @Override
        public void processElement(StreamRecord<Object> element) {}

        @Override
        public void processWatermark(Watermark mark) {}

        @Override
        public void processWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public void processLatencyMarker(LatencyMarker latencyMarker) {}

        @Override
        public void setKeyContextElement(StreamRecord<Object> record) {}

        @Override
        public void processBatchForKey(
                Object currentKey, List<Object> inputs, Collector<Object> out) {}
    }

    private static long pipelineCounter(String fieldName) throws Exception {
        Field field = LocalPreagg.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return ((AtomicLong) field.get(null)).get();
    }
}
