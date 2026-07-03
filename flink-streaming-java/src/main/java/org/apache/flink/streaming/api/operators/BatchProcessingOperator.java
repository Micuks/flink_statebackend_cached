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

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Optional SPI implemented by stream operators that want to receive a batch of records in a single
 * call instead of one-by-one. Used by the CacheKit mailbox-level batching path (Direction 6).
 *
 * <p>The default implementation falls back to per-record processing so any existing {@code
 * OneInputStreamOperator} can opt-in by simply declaring this interface.
 *
 * <p>Contract:
 *
 * <ul>
 *   <li>The provided buffer is owned by the caller; the operator MUST NOT retain references to the
 *       array beyond the call.
 *   <li>Records {@code 0..count-1} are valid; entries past {@code count} are undefined.
 *   <li>Watermarks, checkpoint barriers, latency markers and watermark-status events are flushed by
 *       the framework before any non-record event is dispatched.
 * </ul>
 *
 * @param <IN> input record type
 * @param <OUT> output record type (declared for symmetry with {@code OneInputStreamOperator}; not
 *     used by the default fallback).
 */
public interface BatchProcessingOperator<IN, OUT> {

    /**
     * Process a batch of records. Default implementation iterates and calls {@link
     * OneInputStreamOperator#processElement(StreamRecord)} for each entry, providing a functional
     * fallback for v1 batch buffering without per-operator changes.
     *
     * @param buf array of records; only entries {@code [0, count)} are valid.
     * @param count number of valid records in {@code buf}.
     * @throws Exception propagated from the underlying per-record path.
     */
    @SuppressWarnings("unchecked")
    default void processElementBatch(StreamRecord<IN>[] buf, int count) throws Exception {
        if (!(this instanceof OneInputStreamOperator)) {
            throw new IllegalStateException(
                    "Default processElementBatch fallback requires OneInputStreamOperator; "
                            + "operator "
                            + getClass().getName()
                            + " must override processElementBatch.");
        }
        OneInputStreamOperator<IN, OUT> op = (OneInputStreamOperator<IN, OUT>) this;
        for (int i = 0; i < count; i++) {
            op.setKeyContextElement(buf[i]);
            op.processElement(buf[i]);
        }
    }
}
