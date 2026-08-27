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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Explicit opt-in for preparing immutable state reads for a later same-key batch.
 *
 * <p>Preparation runs on the mailbox thread while {@code currentKey} is installed. Implementations
 * may submit only immutable backend reads; accumulator mutation, state writes and output remain in
 * {@link #processPreparedBatchForKey} on the mailbox thread.
 */
public interface PipelinedBatchableKeyedFunction<IN, OUT>
        extends ReusableBatchableKeyedFunction<IN, OUT> {

    /** Outcome of one optional shared read-wave dispatch attempt. */
    enum BatchWindowPreparationResult {
        /** The complete represented cohort now owns one shared read wave. */
        EXECUTED,
        /** This cohort was rejected; retry only after all its members have drained. */
        RETRY_AFTER_COHORT,
        /** Shared read waves are unavailable for this dispatch. */
        UNSUPPORTED
    }

    /**
     * Cheap lower bound for batches that can profitably create a prepared read.
     *
     * <p>The runtime uses only the already-materialized record count for this gate. Implementations
     * must return {@code 1} unless fewer records mathematically cannot reach their backend's useful
     * key threshold. A rejected batch is processed through {@link #processBatchForKey} in original
     * key order and never calls {@link #prepareBatchForKey}.
     */
    default int minimumBatchPreparationInputCount() {
        return 1;
    }

    /** Captures one batch and starts its best-effort read. The input list must not be retained. */
    Object prepareBatchForKey(Object currentKey, List<IN> inputs) throws Exception;

    /**
     * Gives a bounded ring of already prepared outer-key tokens one chance to execute a shared
     * mailbox-owned backend read before any represented group is consumed.
     *
     * <p>The ring starts at {@code head} and contains {@code count} entries. {@link
     * BatchWindowPreparationResult#EXECUTED} asks the runtime to consume that cohort without
     * sliding in additional prepared groups. {@link
     * BatchWindowPreparationResult#RETRY_AFTER_COHORT} preserves the ordinary sliding pipeline and
     * suppresses another attempt until the represented cohort has drained. {@link
     * BatchWindowPreparationResult#UNSUPPORTED} disables attempts for the rest of this dispatch, so
     * a disabled control does not pay repeated cohort-inspection overhead.
     */
    default BatchWindowPreparationResult prepareBatchWindow(Object[] prepared, int head, int count)
            throws Exception {
        return BatchWindowPreparationResult.UNSUPPORTED;
    }

    /** Consumes one prepared token under the same current key. */
    void processPreparedBatchForKey(
            Object currentKey, List<IN> inputs, Object prepared, Collector<OUT> out)
            throws Exception;

    /** Releases a token that cannot be consumed after a later preparation or dispatch failure. */
    void abortPreparedBatch(Object prepared);
}
