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

package org.apache.flink.table.runtime.operators.deduplicate;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.operators.BatchableKeyedFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import java.util.List;

import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.checkInsertOnly;
import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.isDuplicate;
import static org.apache.flink.table.runtime.operators.deduplicate.DeduplicateFunctionHelper.updateDeduplicateResult;

/** This function is used to deduplicate on keys and keeps only first or last row on row time. */
public class RowTimeDeduplicateFunction
        extends DeduplicateFunctionBase<RowData, RowData, RowData, RowData>
        implements BatchableKeyedFunction<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private final boolean generateUpdateBefore;
    private final boolean generateInsert;
    private final int rowtimeIndex;
    private final boolean keepLastRow;

    public RowTimeDeduplicateFunction(
            InternalTypeInfo<RowData> typeInfo,
            long minRetentionTime,
            int rowtimeIndex,
            boolean generateUpdateBefore,
            boolean generateInsert,
            boolean keepLastRow) {
        super(typeInfo, null, minRetentionTime);
        this.generateUpdateBefore = generateUpdateBefore;
        this.generateInsert = generateInsert;
        this.rowtimeIndex = rowtimeIndex;
        this.keepLastRow = keepLastRow;
    }

    @Override
    public void processElement(RowData input, Context ctx, Collector<RowData> out)
            throws Exception {
        deduplicateOnRowTime(
                state, input, out, generateUpdateBefore, generateInsert, rowtimeIndex, keepLastRow);
    }

    /**
     * Local pre-aggregation fold: first collapse the batch to its dedup winner with the same
     * {@code isDuplicate} chain the per-record path walks (a scan keeping the rowtime extremum;
     * ties resolve by arrival order exactly as sequential processing would), then touch keyed
     * state once. The skipped intermediate emissions are UPDATE_BEFORE/UPDATE_AFTER redundant
     * pairs, so the CDC-replayed final state is unchanged — same argument as the
     * GroupAggFunction fold.
     */
    @Override
    public void processBatchForKey(Object currentKey, List<RowData> inputs, Collector<RowData> out)
            throws Exception {
        if (inputs == null || inputs.isEmpty()) {
            return;
        }
        RowData winner = null;
        for (RowData input : inputs) {
            checkInsertOnly(input);
            if (winner == null || isDuplicate(winner, input, rowtimeIndex, keepLastRow)) {
                winner = input;
            }
        }
        RowData preRow = state.value();
        if (isDuplicate(preRow, winner, rowtimeIndex, keepLastRow)) {
            updateDeduplicateResult(generateUpdateBefore, generateInsert, preRow, winner, out);
            state.update(winner);
        }
    }

    /**
     * Processes element to deduplicate on keys with row time semantic, sends current element if it
     * is last or first row, retracts previous element if needed.
     *
     * @param state state of function
     * @param currentRow latest row received by deduplicate function
     * @param out underlying collector
     * @param generateUpdateBefore flag to generate UPDATE_BEFORE message or not
     * @param generateInsert flag to gennerate INSERT message or not
     * @param rowtimeIndex the index of rowtime field
     * @param keepLastRow flag to keep last row or keep first row
     */
    public static void deduplicateOnRowTime(
            ValueState<RowData> state,
            RowData currentRow,
            Collector<RowData> out,
            boolean generateUpdateBefore,
            boolean generateInsert,
            int rowtimeIndex,
            boolean keepLastRow)
            throws Exception {
        checkInsertOnly(currentRow);
        RowData preRow = state.value();

        if (isDuplicate(preRow, currentRow, rowtimeIndex, keepLastRow)) {
            updateDeduplicateResult(generateUpdateBefore, generateInsert, preRow, currentRow, out);
            state.update(currentRow);
        }
    }
}
