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

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Mailbox-level batch buffer used by the CacheKit batching path.
 *
 * <p>Implementations accumulate stream records and dispatch them either via {@link
 * org.apache.flink.streaming.api.operators.BatchProcessingOperator#processElementBatch} (when the
 * head operator opts-in) or by falling back to per-record dispatch.
 *
 * @param <T> record type carried by the buffer.
 */
public interface BatchOutput<T> {

    /** Append a record to the in-flight batch. */
    void append(StreamRecord<T> record) throws Exception;

    /**
     * @return {@code true} when the batch should be flushed, either because the configured {@code
     *     batchSize} has been reached or {@code batchTimeoutNanos} has elapsed since the first
     *     append.
     */
    boolean shouldFlush();

    /** @return number of records currently buffered. */
    int size();

    /**
     * Flush the buffered records to the head operator. Safe to call when empty (no-op). After
     * return, the buffer is empty and ready for reuse.
     */
    void flushBatch() throws Exception;
}
