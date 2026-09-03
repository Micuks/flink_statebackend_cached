/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StreamOneInputProcessorTest {

    @Test
    @SuppressWarnings("unchecked")
    void testBlockedBatchOutputPausesInputButUsesHeadAvailability() throws Exception {
        StreamTaskInput<Integer> input = mock(StreamTaskInput.class);
        when(input.getAvailableFuture()).thenReturn(CompletableFuture.completedFuture(null));
        BoundedMultiInput endAware = mock(BoundedMultiInput.class);
        BlockingBatchOutput output = new BlockingBatchOutput();
        StreamOneInputProcessor<Integer> processor =
                new StreamOneInputProcessor<>(input, output, endAware);

        assertFalse(processor.getAvailableFuture().isDone());
        assertEquals(DataInputStatus.NOTHING_AVAILABLE, processor.processInput());
        assertEquals(1, output.drains);
        verify(input, never()).emitNext(output);

        output.blocked = false;
        output.available.complete(null);
        when(input.emitNext(output)).thenReturn(DataInputStatus.MORE_AVAILABLE);
        assertEquals(DataInputStatus.MORE_AVAILABLE, processor.processInput());
        verify(input).emitNext(output);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testEndOfDataFlushesBeforeOperatorEndInput() throws Exception {
        List<String> events = new ArrayList<>();
        StreamTaskInput<Integer> input = mock(StreamTaskInput.class);
        when(input.getInputIndex()).thenReturn(0);
        when(input.emitNext(org.mockito.ArgumentMatchers.any()))
                .thenReturn(DataInputStatus.END_OF_DATA);
        BoundedMultiInput endAware = mock(BoundedMultiInput.class);
        doAnswer(
                        ignored -> {
                            events.add("end");
                            return null;
                        })
                .when(endAware)
                .endInput(1);
        BlockingBatchOutput output = new BlockingBatchOutput(events);
        output.blocked = false;
        StreamOneInputProcessor<Integer> processor =
                new StreamOneInputProcessor<>(input, output, endAware);

        assertEquals(DataInputStatus.END_OF_DATA, processor.processInput());

        assertEquals(Arrays.asList("drain", "flush", "end"), events);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCheckpointAndCloseFlushBeforeDelegating() throws Exception {
        List<String> events = new ArrayList<>();
        StreamTaskInput<Integer> input = mock(StreamTaskInput.class);
        when(input.prepareSnapshot(
                        org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyLong()))
                .thenAnswer(
                        ignored -> {
                            events.add("snapshot");
                            return CompletableFuture.completedFuture(null);
                        });
        doAnswer(
                        ignored -> {
                            events.add("close");
                            return null;
                        })
                .when(input)
                .close();
        BlockingBatchOutput output = new BlockingBatchOutput(events);
        output.blocked = false;
        StreamOneInputProcessor<Integer> processor =
                new StreamOneInputProcessor<>(input, output, mock(BoundedMultiInput.class));

        processor.prepareSnapshot(mock(ChannelStateWriter.class), 7L).join();
        processor.close();

        assertEquals(Arrays.asList("flush", "snapshot", "flush", "close"), events);
        assertTrue(output.flushed == 2);
    }

    private static final class BlockingBatchOutput
            implements DataOutput<Integer>, BatchOutput<Integer> {
        private final CompletableFuture<Void> available = new CompletableFuture<>();
        private final List<String> events;
        private boolean blocked = true;
        private int drains;
        private int flushed;

        private BlockingBatchOutput() {
            this(new ArrayList<>());
        }

        private BlockingBatchOutput(List<String> events) {
            this.events = events;
        }

        @Override
        public void emitRecord(StreamRecord<Integer> record) {}

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}

        @Override
        public void append(StreamRecord<Integer> record) {}

        @Override
        public boolean shouldFlush() {
            return false;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public void flushBatch() {
            flushed++;
            events.add("flush");
        }

        @Override
        public void drainReadyBatches() {
            drains++;
            events.add("drain");
        }

        @Override
        public boolean isInputBlocked() {
            return blocked;
        }

        @Override
        public CompletableFuture<?> getAvailableFuture(CompletableFuture<?> inputAvailable) {
            return blocked ? available : inputAvailable;
        }
    }
}
