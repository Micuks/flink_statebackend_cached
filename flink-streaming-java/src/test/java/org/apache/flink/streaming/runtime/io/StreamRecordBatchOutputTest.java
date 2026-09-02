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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/** Ordering tests for the mailbox-owned lookahead buffer. */
class StreamRecordBatchOutputTest {

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
}
