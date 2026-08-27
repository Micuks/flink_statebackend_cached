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

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.PipelinedBatchableKeyedFunction;
import org.apache.flink.streaming.api.operators.PipelinedBatchableKeyedFunction.BatchWindowPreparationResult;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.DistinctBatchPrefetchSupport;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.table.data.util.RowDataUtil.isAccumulateMsg;
import static org.apache.flink.table.data.util.RowDataUtil.isRetractMsg;
import static org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig;

/** Aggregate Function used for the groupby (without window) aggregate. */
public class GroupAggFunction extends KeyedProcessFunction<RowData, RowData, RowData>
        implements PipelinedBatchableKeyedFunction<RowData, RowData> {

    private static final long serialVersionUID = -4767158666069797704L;
    private static final Logger LOG = LoggerFactory.getLogger(GroupAggFunction.class);
    private static final int MIN_SPARSE_PREPARATION_INPUTS = 8;

    /** The code generated function used to handle aggregates. */
    private final GeneratedAggsHandleFunction genAggsHandler;

    /** The code generated equaliser used to equal RowData. */
    private final GeneratedRecordEqualiser genRecordEqualiser;

    /** The accumulator types. */
    private final LogicalType[] accTypes;

    /** Used to count the number of added and retracted input records. */
    private final RecordCounter recordCounter;

    /** Whether this operator will generate UPDATE_BEFORE messages. */
    private final boolean generateUpdateBefore;

    /** State idle retention time which unit is MILLISECONDS. */
    private final long stateRetentionTime;

    /** Reused output row. */
    private transient JoinedRowData resultRow = null;

    // function used to handle all aggregates
    private transient AggsHandleFunction function = null;

    // function used to equal RowData
    private transient RecordEqualiser equaliser = null;

    // stores the accumulators
    private transient ValueState<RowData> accState = null;
    private transient TypeSerializer<RowData> accSerializer = null;
    private transient ArrayDeque<BatchPreparation> batchPreparationPool;
    private transient Object[] preparedWaveCaptures;
    private transient long batchPreparationCalls;
    private transient long batchPreparationAccumulatorCopies;

    // Owns the exact-DISTINCT MapViews and their optional batch-scoped overlays.
    private transient PerKeyStateDataViewStore dataViewStore = null;

    /**
     * Creates a {@link GroupAggFunction}.
     *
     * @param genAggsHandler The code generated function used to handle aggregates.
     * @param genRecordEqualiser The code generated equaliser used to equal RowData.
     * @param accTypes The accumulator types.
     * @param indexOfCountStar The index of COUNT(*) in the aggregates. -1 when the input doesn't
     *     contain COUNT(*), i.e. doesn't contain retraction messages. We make sure there is a
     *     COUNT(*) if input stream contains retraction.
     * @param generateUpdateBefore Whether this operator will generate UPDATE_BEFORE messages.
     * @param stateRetentionTime state idle retention time which unit is MILLISECONDS.
     */
    public GroupAggFunction(
            GeneratedAggsHandleFunction genAggsHandler,
            GeneratedRecordEqualiser genRecordEqualiser,
            LogicalType[] accTypes,
            int indexOfCountStar,
            boolean generateUpdateBefore,
            long stateRetentionTime) {
        this.genAggsHandler = genAggsHandler;
        this.genRecordEqualiser = genRecordEqualiser;
        this.accTypes = accTypes;
        this.recordCounter = RecordCounter.of(indexOfCountStar);
        this.generateUpdateBefore = generateUpdateBefore;
        this.stateRetentionTime = stateRetentionTime;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // instantiate function
        StateTtlConfig ttlConfig = createTtlConfig(stateRetentionTime);
        function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        dataViewStore = new PerKeyStateDataViewStore(getRuntimeContext(), ttlConfig);
        function.open(dataViewStore);
        // instantiate equaliser
        equaliser = genRecordEqualiser.newInstance(getRuntimeContext().getUserCodeClassLoader());

        InternalTypeInfo<RowData> accTypeInfo = InternalTypeInfo.ofFields(accTypes);
        accSerializer = accTypeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
        ValueStateDescriptor<RowData> accDesc = new ValueStateDescriptor<>("accState", accTypeInfo);
        if (ttlConfig.isEnabled()) {
            accDesc.enableTimeToLive(ttlConfig);
        }
        accState = getRuntimeContext().getState(accDesc);

        resultRow = new JoinedRowData();
    }

    @Override
    public void processElement(RowData input, Context ctx, Collector<RowData> out)
            throws Exception {
        RowData currentKey = ctx.getCurrentKey();
        boolean firstRow;
        RowData accumulators = accState.value();
        if (null == accumulators) {
            // Don't create a new accumulator for a retraction message. This
            // might happen if the retraction message is the first message for the
            // key or after a state clean up.
            if (isRetractMsg(input)) {
                return;
            }
            firstRow = true;
            accumulators = function.createAccumulators();
        } else {
            firstRow = false;
        }

        // set accumulators to handler first
        function.setAccumulators(accumulators);
        // get previous aggregate result
        RowData prevAggValue = function.getValue();

        // update aggregate result and set to the newRow
        if (isAccumulateMsg(input)) {
            // accumulate input
            function.accumulate(input);
        } else {
            // retract input
            function.retract(input);
        }
        // get current aggregate result
        RowData newAggValue = function.getValue();

        // get accumulator
        accumulators = function.getAccumulators();

        if (!recordCounter.recordCountIsZero(accumulators)) {
            // we aggregated at least one record for this key

            // update the state
            accState.update(accumulators);

            // if this was not the first row and we have to emit retractions
            if (!firstRow) {
                if (stateRetentionTime <= 0 && equaliser.equals(prevAggValue, newAggValue)) {
                    // newRow is the same as before and state cleaning is not enabled.
                    // We do not emit retraction and acc message.
                    // If state cleaning is enabled, we have to emit messages to prevent too early
                    // state eviction of downstream operators.
                    return;
                } else {
                    // retract previous result
                    if (generateUpdateBefore) {
                        // prepare UPDATE_BEFORE message for previous row
                        resultRow
                                .replace(currentKey, prevAggValue)
                                .setRowKind(RowKind.UPDATE_BEFORE);
                        out.collect(resultRow);
                    }
                    // prepare UPDATE_AFTER message for new row
                    resultRow.replace(currentKey, newAggValue).setRowKind(RowKind.UPDATE_AFTER);
                }
            } else {
                // this is the first, output new result
                // prepare INSERT message for new row
                resultRow.replace(currentKey, newAggValue).setRowKind(RowKind.INSERT);
            }

            out.collect(resultRow);

        } else {
            // we retracted the last record for this key
            // sent out a delete message
            if (!firstRow) {
                // prepare delete message for previous row
                resultRow.replace(currentKey, prevAggValue).setRowKind(RowKind.DELETE);
                out.collect(resultRow);
            }
            // and clear all state
            accState.clear();
            // cleanup dataview under current key
            function.cleanup();
        }
    }

    @Override
    public void processBatchForKey(
            Object currentKey, List<RowData> inputRows, Collector<RowData> out) throws Exception {
        if (inputRows == null || inputRows.isEmpty()) {
            return;
        }
        final RowData key = (RowData) currentKey;
        boolean firstRow = false;

        RowData accumulators = accState.value();
        if (accumulators == null) {
            Iterator<RowData> inputIter = inputRows.iterator();
            while (inputIter.hasNext()) {
                RowData current = inputIter.next();
                if (isRetractMsg(current)) {
                    inputIter.remove();
                } else {
                    break;
                }
            }
            if (inputRows.isEmpty()) {
                return;
            }
            accumulators = function.createAccumulators();
            firstRow = true;
        }

        processBatchFromPreparation(
                key, inputRows, new BatchPreparation(accumulators, firstRow, 0, false, null), out);
    }

    @Override
    public int minimumBatchPreparationInputCount() {
        // unique DISTINCT keys cannot exceed input rows. This is a zero-allocation lower-bound
        // gate; exact de-duplication remains in the generated prepared-capture path.
        return MIN_SPARSE_PREPARATION_INPUTS;
    }

    @Override
    public Object prepareBatchForKey(Object currentKey, List<RowData> inputRows) throws Exception {
        batchPreparationCalls++;
        if (inputRows == null || inputRows.isEmpty()) {
            return BatchPreparation.SKIP;
        }
        RowData accumulators = accState.value();
        int inputStart = 0;
        boolean firstRow = false;
        if (accumulators == null) {
            while (inputStart < inputRows.size() && isRetractMsg(inputRows.get(inputStart))) {
                inputStart++;
            }
            if (inputStart == inputRows.size()) {
                return BatchPreparation.SKIP;
            }
            accumulators = function.createAccumulators();
            firstRow = true;
        }

        // Reading the next outer key may reuse backend deserialization buffers. Retain a stable
        // accumulator while its immutable RocksDB keys execute off mailbox.
        BatchPreparation preparation = acquireBatchPreparation();
        boolean captureStarted = false;
        boolean captureEnded = false;
        try {
            RowData stableAccumulators =
                    preparation.accumulators == null
                            ? accSerializer.copy(accumulators)
                            : accSerializer.copy(accumulators, preparation.accumulators);
            batchPreparationAccumulatorCopies++;
            function.setAccumulators(stableAccumulators);
            DistinctBatchPrefetchSupport.beginPreparedCapture();
            captureStarted = true;
            function.prefetchDistinctBatch(
                    inputStart == 0 ? inputRows : inputRows.subList(inputStart, inputRows.size()));
            Object distinctPrepared = DistinctBatchPrefetchSupport.endPreparedCapture();
            captureEnded = true;
            preparation.reset(stableAccumulators, firstRow, inputStart, distinctPrepared);
            return preparation;
        } finally {
            if (!captureEnded) {
                if (captureStarted) {
                    DistinctBatchPrefetchSupport.abortPreparedCapture();
                }
                releaseBatchPreparation(preparation);
            }
        }
    }

    @Override
    public void processPreparedBatchForKey(
            Object currentKey, List<RowData> inputRows, Object prepared, Collector<RowData> out)
            throws Exception {
        if (!(prepared instanceof BatchPreparation)) {
            processBatchForKey(currentKey, inputRows, out);
            return;
        }
        BatchPreparation preparation = (BatchPreparation) prepared;
        if (preparation.skip) {
            return;
        }
        try {
            processBatchFromPreparation((RowData) currentKey, inputRows, preparation, out);
        } finally {
            releaseBatchPreparation(preparation);
        }
    }

    @Override
    public void abortPreparedBatch(Object prepared) {
        if (prepared instanceof BatchPreparation) {
            BatchPreparation preparation = (BatchPreparation) prepared;
            if (preparation.leased) {
                DistinctBatchPrefetchSupport.abortPreparedCapture(preparation.distinctPrepared);
                releaseBatchPreparation(preparation);
            }
        }
    }

    private BatchPreparation acquireBatchPreparation() {
        if (batchPreparationPool == null) {
            batchPreparationPool = new ArrayDeque<>();
        }
        BatchPreparation preparation = batchPreparationPool.pollFirst();
        if (preparation == null) {
            preparation = new BatchPreparation();
        }
        if (preparation.leased) {
            throw new IllegalStateException("Batch preparation slot is already leased");
        }
        preparation.leased = true;
        return preparation;
    }

    @Override
    public BatchWindowPreparationResult prepareBatchWindow(Object[] prepared, int head, int count)
            throws Exception {
        if (count < 2 || prepared == null || prepared.length == 0) {
            return BatchWindowPreparationResult.UNSUPPORTED;
        }
        if (preparedWaveCaptures == null || preparedWaveCaptures.length < count) {
            preparedWaveCaptures = new Object[Math.max(9, count)];
        }
        try {
            for (int index = 0; index < count; index++) {
                Object candidate = prepared[(head + index) % prepared.length];
                if (!(candidate instanceof BatchPreparation)) {
                    return BatchWindowPreparationResult.UNSUPPORTED;
                }
                BatchPreparation preparation = (BatchPreparation) candidate;
                if (preparation.skip || !preparation.leased) {
                    return BatchWindowPreparationResult.RETRY_AFTER_COHORT;
                }
                preparedWaveCaptures[index] = preparation.distinctPrepared;
            }
            return DistinctBatchPrefetchSupport.executePreparedWave(preparedWaveCaptures, count);
        } finally {
            java.util.Arrays.fill(preparedWaveCaptures, 0, count, null);
        }
    }

    private void releaseBatchPreparation(BatchPreparation preparation) {
        if (preparation == null || preparation.skip || !preparation.leased) {
            return;
        }
        preparation.firstRow = false;
        preparation.inputStart = 0;
        preparation.distinctPrepared = null;
        preparation.leased = false;
        batchPreparationPool.addLast(preparation);
    }

    private void processBatchFromPreparation(
            RowData key,
            List<RowData> inputRows,
            BatchPreparation preparation,
            Collector<RowData> out)
            throws Exception {
        RowData accumulators = preparation.accumulators;
        boolean firstRow = preparation.firstRow;
        function.setAccumulators(accumulators);
        boolean installed =
                DistinctBatchPrefetchSupport.installPreparedCapture(preparation.distinctPrepared);
        if (!installed) {
            function.prefetchDistinctBatch(
                    preparation.inputStart == 0
                            ? inputRows
                            : inputRows.subList(preparation.inputStart, inputRows.size()));
        }
        final boolean distinctBatch = dataViewStore.beginDistinctBatch();
        boolean distinctBatchCommitted = false;
        try {
            RowData prevAggValue = function.getValue();
            for (int inputIndex = preparation.inputStart;
                    inputIndex < inputRows.size();
                    inputIndex++) {
                RowData input = inputRows.get(inputIndex);
                if (isAccumulateMsg(input)) {
                    function.accumulate(input);
                } else {
                    function.retract(input);
                }
            }
            RowData newAggValue = function.getValue();
            accumulators = function.getAccumulators();

            // Make the DISTINCT map update durable before the accumulator/output of this batch is
            // made visible. A failure therefore cannot emit a result whose dedup state was lost.
            if (distinctBatch) {
                dataViewStore.commitDistinctBatch();
                distinctBatchCommitted = true;
            }

            if (!recordCounter.recordCountIsZero(accumulators)) {
                accState.update(accumulators);
                if (!firstRow) {
                    if (stateRetentionTime <= 0 && equaliser.equals(prevAggValue, newAggValue)) {
                        return;
                    }
                    if (generateUpdateBefore) {
                        resultRow.replace(key, prevAggValue).setRowKind(RowKind.UPDATE_BEFORE);
                        out.collect(resultRow);
                    }
                    resultRow.replace(key, newAggValue).setRowKind(RowKind.UPDATE_AFTER);
                } else {
                    resultRow.replace(key, newAggValue).setRowKind(RowKind.INSERT);
                }
                out.collect(resultRow);
            } else {
                if (!firstRow) {
                    resultRow.replace(key, prevAggValue).setRowKind(RowKind.DELETE);
                    out.collect(resultRow);
                }
                accState.clear();
                function.cleanup();
            }
        } finally {
            if (distinctBatch && !distinctBatchCommitted) {
                dataViewStore.abortDistinctBatch();
            }
        }
    }

    private static final class BatchPreparation {
        private static final BatchPreparation SKIP = new BatchPreparation(true);

        private RowData accumulators;
        private boolean firstRow;
        private int inputStart;
        private final boolean skip;
        private Object distinctPrepared;
        private boolean leased;

        private BatchPreparation() {
            this(false);
        }

        private BatchPreparation(boolean skip) {
            this.skip = skip;
        }

        private BatchPreparation(
                RowData accumulators,
                boolean firstRow,
                int inputStart,
                boolean skip,
                Object distinctPrepared) {
            this.accumulators = accumulators;
            this.firstRow = firstRow;
            this.inputStart = inputStart;
            this.skip = skip;
            this.distinctPrepared = distinctPrepared;
        }

        private void reset(
                RowData accumulators, boolean firstRow, int inputStart, Object distinctPrepared) {
            if (!leased || skip) {
                throw new IllegalStateException("Batch preparation slot is not leased");
            }
            this.accumulators = accumulators;
            this.firstRow = firstRow;
            this.inputStart = inputStart;
            this.distinctPrepared = distinctPrepared;
        }
    }

    @Override
    public void close() throws Exception {
        if (function != null) {
            function.close();
        }
        if (dataViewStore != null) {
            LOG.info(
                    "[CACHEKIT DISTINCT BATCH OVERLAY] {}",
                    dataViewStore.distinctBatchDiagnosticSummary());
        }
        LOG.info(
                "[CACHEKIT DISTINCT SPARSE PREPARE] minInputRecords={} preparationCalls={} accumulatorCopies={}",
                MIN_SPARSE_PREPARATION_INPUTS,
                batchPreparationCalls,
                batchPreparationAccumulatorCopies);
    }
}
