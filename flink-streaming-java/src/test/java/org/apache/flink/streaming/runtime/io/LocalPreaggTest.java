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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class LocalPreaggTest {

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
                                Arrays.asList("a", "b"),
                                Arrays.asList(1),
                                new int[] {2, 0, 1}));
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
