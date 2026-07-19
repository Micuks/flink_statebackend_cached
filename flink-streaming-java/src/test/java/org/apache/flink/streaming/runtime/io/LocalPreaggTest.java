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

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BatchableKeyedFunction;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class LocalPreaggTest {

    @Test
    void testDetectsDirectBatchableOperator() {
        assertTrue(LocalPreagg.hasBatchableTarget(new BatchableInputOperator()));
        assertFalse(LocalPreagg.hasBatchableTarget(mock(Input.class)));
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
}
