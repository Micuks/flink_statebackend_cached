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
 * Opt-in SPI for keyed {@code KeyedProcessFunction}s that can process a batch of same-key inputs in
 * one shot (heap pre-aggregation), instead of one record at a time. Used by the runtime local
 * pre-aggregation path ({@code state.backend.cachekit.local-preagg.enabled}): the prefetch buffer
 * groups a flushed batch by key and, for each key, sets the operator's current key and calls {@link
 * #processBatchForKey} once with all that key's inputs — collapsing N state round-trips into one
 * read + one write and N intermediate UPDATE_BEFORE/UPDATE_AFTER emits into one collapsed pair.
 *
 * <p>The terminal keyed state is byte-identical to the per-record path; only intermediate
 * (retracted-then-superseded) emits are suppressed — i.e. CDC-collapse equivalent. The batched-fold
 * semantics match Flink's table-layer batch aggregation, but are realised at the runtime/operator
 * layer without any query-plan change.
 *
 * <p>ONLY commutative aggregations (where the per-key result is independent of intra-batch input
 * order) may implement this. Order-sensitive functions (KeepFirstRow/LAST_VALUE/LAG/LEAD/TopN) must
 * NOT implement it — they fall back to per-record dispatch.
 *
 * @param <IN> input record type
 * @param <OUT> output record type
 */
public interface BatchableKeyedFunction<IN, OUT> {

    /**
     * Process all buffered inputs for the CURRENT key (already set on the operator by the caller
     * via {@code setCurrentKey}, so keyed-state access resolves to this key). Reads the key's
     * accumulator once, folds every input in, writes once, and emits the single collapsed result
     * transition.
     *
     * @param currentKey the key (passed explicitly because the batch path has no per-record {@code
     *     Context}); used to populate the output row's key fields.
     * @param inputs the batch of inputs for the current key, in arrival order.
     * @param out the operator's output collector.
     */
    void processBatchForKey(Object currentKey, List<IN> inputs, Collector<OUT> out)
            throws Exception;
}
