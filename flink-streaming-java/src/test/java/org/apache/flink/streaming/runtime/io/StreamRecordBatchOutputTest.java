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

import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/** Ordering tests for the mailbox-owned lookahead buffer. */
class StreamRecordBatchOutputTest {

    @Test
    void testReadyGateOverlapsTwoBatchesAndNeverOvertakesHead() throws Exception {
        List<String> emitted = new ArrayList<>();
        List<String> dispatchThreads = new ArrayList<>();
        DataOutput<String> wrapped = collectingOutput(emitted, dispatchThreads);
        CompletableFuture<Boolean> first = new CompletableFuture<>();
        CompletableFuture<Boolean> second = new CompletableFuture<>();
        TestingReadyOutput output =
                new TestingReadyOutput(
                        wrapped, Arrays.asList(first, second), TimeUnit.SECONDS.toNanos(10), false);

        emitValues(output, "r0", "r1", "r2", "r3");

        assertEquals(4, output.size());
        assertTrue(output.isInputBlocked());
        assertEquals(2, output.maxObservedInFlightDepthForTesting());
        assertEquals(0L, output.headSequenceForTesting());
        second.complete(true);
        output.drainReadyBatches();
        assertTrue(emitted.isEmpty(), "a later completed batch must not overtake the head");
        assertFalse(
                output.getAvailableFuture(new CompletableFuture<>()).isDone(),
                "ring-full availability must be owned by the incomplete head");

        String mailboxThread = Thread.currentThread().getName();
        first.complete(true);
        output.drainReadyBatches();

        assertEquals(Arrays.asList("r0", "r1", "r2", "r3"), emitted);
        assertEquals(Collections.nCopies(4, mailboxThread), dispatchThreads);
        assertEquals(2, output.readyBeforeDispatchForTesting());
        assertEquals(0, output.dispatchBeforeReadyFallbacksForTesting());
        assertEquals(2, output.getReadyBatchesStarted());
        assertEquals(4, output.getReadyRecordsDispatched());
        assertEquals(0, output.getFallbackRecordsDispatched());
        assertEquals(4, output.getMaxRetainedReadyRecords());
        assertEquals(4L * Long.BYTES, output.getMaxRetainedReadyReferenceBytes());
        assertFalse(output.isInputBlocked());
        assertEquals(0, output.size());
    }

    @Test
    void testReadyGateRingFullFutureIgnoresReadableInputUntilHeadCompletes() throws Exception {
        CompletableFuture<Boolean> first = new CompletableFuture<>();
        CompletableFuture<Boolean> second = new CompletableFuture<>();
        TestingReadyOutput output =
                new TestingReadyOutput(
                        collectingOutput(new ArrayList<>()),
                        Arrays.asList(first, second),
                        TimeUnit.SECONDS.toNanos(10),
                        false);
        emitValues(output, "r0", "r1", "r2", "r3");
        CompletableFuture<Void> readableInput = CompletableFuture.completedFuture(null);

        CompletableFuture<?> available = output.getAvailableFuture(readableInput);
        assertFalse(available.isDone());
        first.complete(true);
        available.get(10, TimeUnit.SECONDS);
        output.drainReadyBatches();

        assertFalse(output.isInputBlocked());
        assertTrue(output.getAvailableFuture(readableInput).isDone());
        second.complete(true);
        output.drainReadyBatches();
    }

    @Test
    void testReadyGateFailureAndFalseCompletionDispatchAuthoritativelyInOrder() throws Exception {
        List<String> emitted = new ArrayList<>();
        CompletableFuture<Boolean> failed = new CompletableFuture<>();
        CompletableFuture<Boolean> rejected = new CompletableFuture<>();
        TestingReadyOutput output =
                new TestingReadyOutput(
                        collectingOutput(emitted),
                        Arrays.asList(failed, rejected),
                        TimeUnit.SECONDS.toNanos(10),
                        false);
        emitValues(output, "r0", "r1", "r2", "r3");

        failed.completeExceptionally(new IllegalStateException("worker failed"));
        rejected.complete(false);
        output.drainReadyBatches();

        assertEquals(Arrays.asList("r0", "r1", "r2", "r3"), emitted);
        assertEquals(2, output.cancelledBatches);
        assertEquals(2, output.dispatchBeforeReadyFallbacksForTesting());
        assertEquals(0, output.readyBeforeDispatchForTesting());
        assertEquals(1, output.getReadyFailureFallbacks());
        assertEquals(1, output.getReadyNotProvenFallbacks());
        assertEquals(0, output.getReadyTimeoutFallbacks());
        assertEquals(4, output.getFallbackRecordsDispatched());
    }

    @Test
    void testReadyGateSynchronousStartFailureFallsBackWithoutLosingRecords() throws Exception {
        List<String> emitted = new ArrayList<>();
        TestingReadyOutput output =
                new TestingReadyOutput(
                        collectingOutput(emitted),
                        Collections.singletonList(CompletableFuture.completedFuture(true)),
                        TimeUnit.SECONDS.toNanos(10),
                        false) {
                    @Override
                    CompletableFuture<Boolean> startReadyPrefetch(
                            StreamRecord<String>[] records, int n) {
                        throw new IllegalStateException("synchronous start failure");
                    }
                };

        emitValues(output, "r0", "r1");
        output.drainReadyBatches();

        assertEquals(Arrays.asList("r0", "r1"), emitted);
        assertEquals(1, output.getReadyFailureFallbacks());
        assertEquals(1, output.cancelledBatches);
    }

    @Test
    void testReadyGateTimeoutWakesAndFallsBackWithoutBlockingMailbox() throws Exception {
        List<String> emitted = new ArrayList<>();
        CompletableFuture<Boolean> never = new CompletableFuture<>();
        TestingReadyOutput output =
                new TestingReadyOutput(
                        collectingOutput(emitted),
                        Collections.singletonList(never),
                        TimeUnit.MILLISECONDS.toNanos(1),
                        false);
        emitValues(output, "r0", "r1");

        output.getAvailableFuture(new CompletableFuture<>()).get(10, TimeUnit.SECONDS);
        output.drainReadyBatches();

        assertEquals(Arrays.asList("r0", "r1"), emitted);
        assertEquals(1, output.cancelledBatches);
        assertEquals(1, output.dispatchBeforeReadyFallbacksForTesting());
        assertEquals(1, output.getReadyTimeoutFallbacks());
        assertFalse(never.isDone(), "fallback must not run worker completion on the mailbox");
    }

    @Test
    void testReadyGateWatermarkFencesPendingAndPartialBatches() throws Exception {
        List<String> events = new ArrayList<>();
        DataOutput<String> wrapped = collectingOutput(events);
        org.mockito.Mockito.doAnswer(
                        invocation -> {
                            events.add(
                                    "watermark:"
                                            + ((Watermark) invocation.getArgument(0))
                                                    .getTimestamp());
                            return null;
                        })
                .when(wrapped)
                .emitWatermark(org.mockito.ArgumentMatchers.any());
        TestingReadyOutput output =
                new TestingReadyOutput(
                        wrapped,
                        Arrays.asList(new CompletableFuture<>(), new CompletableFuture<>()),
                        TimeUnit.SECONDS.toNanos(10),
                        false);

        emitValues(output, "r0", "r1", "r2");
        output.emitWatermark(new Watermark(99L));

        assertEquals(Arrays.asList("r0", "r1", "r2", "watermark:99"), events);
        assertEquals(2, output.cancelledBatches);
        assertEquals(2, output.getReadyForcedFallbacks());
        assertEquals(0, output.size());
    }

    @Test
    void testReadyGateStatusAndLatencyMarkerFenceOlderRecords() throws Exception {
        List<String> events = new ArrayList<>();
        DataOutput<String> wrapped = collectingOutput(events);
        org.mockito.Mockito.doAnswer(
                        invocation -> {
                            events.add("status");
                            return null;
                        })
                .when(wrapped)
                .emitWatermarkStatus(org.mockito.ArgumentMatchers.any());
        org.mockito.Mockito.doAnswer(
                        invocation -> {
                            events.add("latency");
                            return null;
                        })
                .when(wrapped)
                .emitLatencyMarker(org.mockito.ArgumentMatchers.any());
        TestingReadyOutput output =
                new TestingReadyOutput(
                        wrapped,
                        Arrays.asList(new CompletableFuture<>(), new CompletableFuture<>()),
                        TimeUnit.SECONDS.toNanos(10),
                        false);

        output.emitRecord(new StreamRecord<>("before-status"));
        output.emitWatermarkStatus(WatermarkStatus.IDLE);
        output.emitRecord(new StreamRecord<>("before-latency"));
        output.emitLatencyMarker(
                new LatencyMarker(9L, new org.apache.flink.runtime.jobgraph.OperatorID(), 1));

        assertEquals(Arrays.asList("before-status", "status", "before-latency", "latency"), events);
        assertEquals(2, output.cancelledBatches);
    }

    @Test
    void testReadyGateFailsClosedUnderObjectReuse() throws Exception {
        List<String> emitted = new ArrayList<>();
        TestingReadyOutput output =
                new TestingReadyOutput(
                        collectingOutput(emitted),
                        Collections.singletonList(new CompletableFuture<>()),
                        TimeUnit.SECONDS.toNanos(10),
                        true);

        emitValues(output, "r0", "r1");

        assertFalse(output.readyGatedPrefetchEnabledForTesting());
        assertEquals(Arrays.asList("r0", "r1"), emitted);
        assertEquals(0, output.prefetchCalls);
    }

    @Test
    void testDispatchCancellationRunsBeforeOrdinaryRecordReplay() throws Exception {
        List<String> events = new ArrayList<>();
        @SuppressWarnings("unchecked")
        DataOutput<String> wrapped = mock(DataOutput.class);
        org.mockito.Mockito.doAnswer(
                        invocation -> {
                            StreamRecord<String> record = invocation.getArgument(0);
                            events.add("emit:" + record.getValue());
                            return null;
                        })
                .when(wrapped)
                .emitRecord(org.mockito.ArgumentMatchers.any());
        @SuppressWarnings("unchecked")
        Input<String> input = mock(Input.class);
        StreamRecordBatchOutput<String> output =
                new StreamRecordBatchOutput<String>(
                        wrapped,
                        input,
                        true,
                        false,
                        2,
                        0,
                        null,
                        true,
                        () -> false,
                        false,
                        true,
                        2,
                        0,
                        0,
                        true) {
                    @Override
                    int cancelPrefetchForDispatch(int n) {
                        events.add("cancel:" + n);
                        return n;
                    }
                };

        output.emitRecord(new StreamRecord<>("first"));
        output.emitRecord(new StreamRecord<>("second"));

        assertEquals(Arrays.asList("cancel:2", "emit:first", "emit:second"), events);
    }

    @Test
    void testWatermarkFlushesAllPriorRecordsInArrivalOrder() throws Exception {
        @SuppressWarnings("unchecked")
        DataOutput<String> wrapped = mock(DataOutput.class);
        @SuppressWarnings("unchecked")
        Input<String> input = mock(Input.class);
        StreamRecordBatchOutput<String> output =
                new StreamRecordBatchOutput<>(wrapped, input, true, false, 4, 0, null);
        StreamRecord<String> first = new StreamRecord<>("first");
        StreamRecord<String> second = new StreamRecord<>("second");
        Watermark watermark = new Watermark(123L);

        output.emitRecord(first);
        output.emitRecord(second);
        verifyNoInteractions(wrapped);
        output.emitWatermark(watermark);

        InOrder order = inOrder(wrapped);
        order.verify(wrapped).emitRecord(first);
        order.verify(wrapped).emitRecord(second);
        order.verify(wrapped).emitWatermark(watermark);
        assertEquals(0, output.size());
        assertFalse(output.shouldFlush());
    }

    @Test
    void testStatusAndLatencyMarkerEachFlushPriorRecord() throws Exception {
        @SuppressWarnings("unchecked")
        DataOutput<String> wrapped = mock(DataOutput.class);
        @SuppressWarnings("unchecked")
        Input<String> input = mock(Input.class);
        StreamRecordBatchOutput<String> output =
                new StreamRecordBatchOutput<>(wrapped, input, true, false, 4, 0, null);
        StreamRecord<String> beforeStatus = new StreamRecord<>("before-status");
        StreamRecord<String> beforeLatency = new StreamRecord<>("before-latency");
        WatermarkStatus status = WatermarkStatus.IDLE;
        LatencyMarker marker =
                new LatencyMarker(9L, new org.apache.flink.runtime.jobgraph.OperatorID(), 1);

        output.emitRecord(beforeStatus);
        output.emitWatermarkStatus(status);
        output.emitRecord(beforeLatency);
        output.emitLatencyMarker(marker);

        InOrder order = inOrder(wrapped);
        order.verify(wrapped).emitRecord(beforeStatus);
        order.verify(wrapped).emitWatermarkStatus(status);
        order.verify(wrapped).emitRecord(beforeLatency);
        order.verify(wrapped).emitLatencyMarker(marker);
        assertEquals(0, output.size());
    }

    @Test
    void testAsyncPrefetchProtectsConsumerFacingHeadAndSlidesIntoTail() {
        @SuppressWarnings("unchecked")
        DataOutput<String> wrapped = mock(DataOutput.class);
        @SuppressWarnings("unchecked")
        Input<String> input = mock(Input.class);
        StreamRecordBatchOutput<String> output =
                new StreamRecordBatchOutput<>(
                        wrapped,
                        input,
                        true,
                        false,
                        8,
                        0,
                        null,
                        true,
                        () -> false,
                        false,
                        true,
                        2,
                        3);

        output.append(new StreamRecord<>("head-0"));
        output.append(new StreamRecord<>("head-1"));
        output.append(new StreamRecord<>("head-2"));
        output.append(new StreamRecord<>("tail-3"));
        assertEquals(3, output.asyncPrefetchScheduledUntilForTesting());

        output.append(new StreamRecord<>("tail-4"));
        assertEquals(5, output.asyncPrefetchScheduledUntilForTesting());
    }

    @Test
    void testRollingWindowDrainsHeadRefillsAndFlushesTailBeforeWatermark() throws Exception {
        @SuppressWarnings("unchecked")
        DataOutput<String> wrapped = mock(DataOutput.class);
        @SuppressWarnings("unchecked")
        Input<String> input = mock(Input.class);
        StreamRecordBatchOutput<String> output =
                new StreamRecordBatchOutput<>(
                        wrapped,
                        input,
                        true,
                        false,
                        6,
                        0,
                        null,
                        true,
                        () -> false,
                        false,
                        true,
                        2,
                        2,
                        2);
        StreamRecord<String>[] records = new StreamRecord[8];
        for (int i = 0; i < records.length; i++) {
            records[i] = new StreamRecord<>("record-" + i);
            output.emitRecord(records[i]);
        }

        // High watermark 6 drains two records twice and retains a four-record lookahead tail.
        assertEquals(4, output.size());
        Watermark watermark = new Watermark(456L);
        output.emitWatermark(watermark);

        InOrder order = inOrder(wrapped);
        for (StreamRecord<String> record : records) {
            order.verify(wrapped).emitRecord(record);
        }
        order.verify(wrapped).emitWatermark(watermark);
        assertEquals(0, output.size());
        assertFalse(output.shouldFlush());
    }

    @SuppressWarnings("unchecked")
    private static DataOutput<String> collectingOutput(List<String> values) throws Exception {
        return collectingOutput(values, null);
    }

    @SuppressWarnings("unchecked")
    private static DataOutput<String> collectingOutput(
            List<String> values, List<String> dispatchThreads) throws Exception {
        DataOutput<String> wrapped = mock(DataOutput.class);
        org.mockito.Mockito.doAnswer(
                        invocation -> {
                            StreamRecord<String> record = invocation.getArgument(0);
                            values.add(record.getValue());
                            if (dispatchThreads != null) {
                                dispatchThreads.add(Thread.currentThread().getName());
                            }
                            return null;
                        })
                .when(wrapped)
                .emitRecord(org.mockito.ArgumentMatchers.any());
        return wrapped;
    }

    private static void emitValues(StreamRecordBatchOutput<String> output, String... values)
            throws Exception {
        for (String value : values) {
            output.emitRecord(new StreamRecord<>(value));
        }
    }

    private static class TestingReadyOutput extends StreamRecordBatchOutput<String> {
        private final Deque<CompletableFuture<Boolean>> completions;
        private int cancelledBatches;
        private int prefetchCalls;

        private TestingReadyOutput(
                DataOutput<String> wrapped,
                List<CompletableFuture<Boolean>> completions,
                long timeoutNanos,
                boolean objectReuseEnabled) {
            super(
                    wrapped,
                    mock(Input.class),
                    true,
                    false,
                    2,
                    0L,
                    null,
                    true,
                    () -> false,
                    false,
                    false,
                    2,
                    0,
                    0,
                    false,
                    true,
                    2,
                    timeoutNanos,
                    objectReuseEnabled);
            this.completions = new ArrayDeque<>(completions);
        }

        @Override
        CompletableFuture<Boolean> startReadyPrefetch(StreamRecord<String>[] records, int n) {
            prefetchCalls++;
            return completions.removeFirst();
        }

        @Override
        int cancelPrefetchForDispatch(StreamRecord<String>[] records, int n) {
            cancelledBatches++;
            return n;
        }
    }
}
