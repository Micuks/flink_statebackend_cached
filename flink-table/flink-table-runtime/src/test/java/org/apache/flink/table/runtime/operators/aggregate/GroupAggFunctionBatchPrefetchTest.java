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

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.lang.reflect.Field;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GroupAggFunctionBatchPrefetchTest {

    @Test
    void exposesEightRecordsAsSparsePreparationLowerBound() {
        GroupAggFunction function =
                new GroupAggFunction(
                        mock(GeneratedAggsHandleFunction.class),
                        mock(GeneratedRecordEqualiser.class),
                        new LogicalType[0],
                        -1,
                        false,
                        0L);

        assertEquals(8, function.minimumBatchPreparationInputCount());
    }

    @Test
    @SuppressWarnings("unchecked")
    void bindsDistinctViewsBeforeCollectingExactPrefetchKeys() throws Exception {
        AggsHandleFunction handler = mock(AggsHandleFunction.class);
        ValueState<RowData> accumulatorState = mock(ValueState.class);
        RecordEqualiser equaliser = mock(RecordEqualiser.class);
        Collector<RowData> output = mock(Collector.class);

        RowData key = new GenericRowData(0);
        RowData accumulator = new GenericRowData(0);
        RowData aggregateValue = new GenericRowData(0);
        RowData input = new GenericRowData(0);
        List<RowData> inputs = List.of(input);

        when(accumulatorState.value()).thenReturn(accumulator);
        when(handler.getValue()).thenReturn(aggregateValue);
        when(handler.getAccumulators()).thenReturn(accumulator);
        when(equaliser.equals(aggregateValue, aggregateValue)).thenReturn(true);

        GroupAggFunction function =
                new GroupAggFunction(
                        mock(GeneratedAggsHandleFunction.class),
                        mock(GeneratedRecordEqualiser.class),
                        new LogicalType[0],
                        -1,
                        false,
                        0L);
        setField(function, "function", handler);
        setField(function, "accState", accumulatorState);
        setField(function, "equaliser", equaliser);
        setField(function, "resultRow", new JoinedRowData());
        setField(
                function,
                "dataViewStore",
                new PerKeyStateDataViewStore(mock(RuntimeContext.class)));

        function.processBatchForKey(key, inputs, output);

        InOrder order = inOrder(handler);
        order.verify(handler).setAccumulators(accumulator);
        order.verify(handler).prefetchDistinctBatch(inputs);
        order.verify(handler).getValue();
        order.verify(handler).accumulate(input);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
