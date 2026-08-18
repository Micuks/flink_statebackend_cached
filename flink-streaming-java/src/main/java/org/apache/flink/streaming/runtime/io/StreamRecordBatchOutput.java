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

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.BatchProcessingOperator;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

/**
 * Buffering wrapper around a {@link DataOutput} that accumulates stream records up to a
 * configurable {@code batchSize} (or until {@code batchTimeoutNanos} has elapsed) and then
 * dispatches them in one shot. Watermarks, watermark-status, and latency markers immediately flush
 * the in-flight batch before being forwarded — preserving Flink's barrier-alignment and watermark
 * semantics.
 *
 * <p>If the head operator implements {@link BatchProcessingOperator}, the records are dispatched
 * via {@link BatchProcessingOperator#processElementBatch(StreamRecord[], int)}; otherwise the
 * buffered records are replayed one-by-one via the wrapped {@link DataOutput#emitRecord} (which
 * calls {@code setKeyContextElement} + {@code processElement} per record), giving an exact
 * functional equivalent to the unbatched path.
 *
 * <p>This class is NOT thread-safe; the mailbox owns it.
 *
 * @param <T> input record type.
 */
public class StreamRecordBatchOutput<T> implements DataOutput<T>, BatchOutput<T> {

    private static final boolean BP_PREFETCH_ASYNC_CHUNKS =
            GlobalConfiguration.loadConfiguration()
                    .get(
                            ConfigOptions.key(
                                            "state.backend.cachekit.bp-prefetch.async-chunks.enabled")
                                    .booleanType()
                                    .defaultValue(false));

    private static final int BP_PREFETCH_ASYNC_CHUNK_SIZE =
            Math.max(
                    2,
                    Math.min(
                            1024,
                            GlobalConfiguration.loadConfiguration()
                                    .get(
                                            ConfigOptions.key(
                                                            "state.backend.cachekit.bp-prefetch.async-chunks.size")
                                                    .intType()
                                                    .defaultValue(16))));

    private final DataOutput<T> wrapped;
    private final Input<T> headOperator;
    private final BatchProcessingOperator<T, ?> batchOperator; // non-null iff head op opts in
    private final boolean enabled;
    private final boolean commutativeKeySort; // v2: same-key run amortization for keyed ops
    private final int batchSize;
    private final long batchTimeoutNanos;
    private final Counter numRecordsIn;

    // ---- Backpressure-driven state prefetch (MVP, synchronous form B) ----
    /**
     * When true, the buffer is used as a lookahead window: warm state for the batch, then emit each
     * record in arrival order (no reorder). Takes precedence over commutativeKeySort.
     */
    private final boolean prefetchMode;
    /** Backpressure signal ({@code !recordWriter.isAvailable()}); may be null when ungated. */
    private final java.util.function.BooleanSupplier backpressured;
    /**
     * When true, prefetch only fires while backpressured; when false, prefetch fires every flush.
     */
    private final boolean backpressureGated;
    /** Local-preagg may consume the whole batch, making any early state read dead work. */
    private final boolean localPreaggCandidate;

    // Reusable record buffer. Sized at construction.
    @SuppressWarnings({"unchecked", "rawtypes"})
    private final StreamRecord<T>[] buf;

    private int count;
    private long firstAppendNanos;
    private int asyncPrefetchScheduledUntil;

    public StreamRecordBatchOutput(
            DataOutput<T> wrapped,
            Input<T> headOperator,
            boolean enabled,
            int batchSize,
            long batchTimeoutNanos,
            Counter numRecordsIn) {
        this(wrapped, headOperator, enabled, true, batchSize, batchTimeoutNanos, numRecordsIn);
    }

    public StreamRecordBatchOutput(
            DataOutput<T> wrapped,
            Input<T> headOperator,
            boolean enabled,
            boolean commutativeKeySort,
            int batchSize,
            long batchTimeoutNanos,
            Counter numRecordsIn) {
        this(
                wrapped,
                headOperator,
                enabled,
                commutativeKeySort,
                batchSize,
                batchTimeoutNanos,
                numRecordsIn,
                false,
                null,
                true);
    }

    /**
     * Full constructor including backpressure-driven prefetch mode. When {@code prefetchMode} is
     * true, {@link #flushBatch()} warms the keyed state for the batch (a single prefetch via {@link
     * org.apache.flink.streaming.runtime.tasks.StatePrefetcher}) and then emits each record in
     * arrival order — it never takes the {@code commutativeKeySort} reorder path.
     */
    public StreamRecordBatchOutput(
            DataOutput<T> wrapped,
            Input<T> headOperator,
            boolean enabled,
            boolean commutativeKeySort,
            int batchSize,
            long batchTimeoutNanos,
            Counter numRecordsIn,
            boolean prefetchMode,
            java.util.function.BooleanSupplier backpressured,
            boolean backpressureGated) {
        this.wrapped = wrapped;
        this.headOperator = headOperator;
        this.enabled = enabled && batchSize > 1;
        this.commutativeKeySort = commutativeKeySort;
        this.batchSize = Math.max(1, batchSize);
        this.batchTimeoutNanos = batchTimeoutNanos;
        this.numRecordsIn = numRecordsIn;
        this.prefetchMode = prefetchMode;
        this.backpressured = backpressured;
        this.backpressureGated = backpressureGated;
        this.localPreaggCandidate = LocalPreagg.mayHandle(headOperator);
        @SuppressWarnings({"unchecked", "rawtypes"})
        StreamRecord<T>[] tmp = new StreamRecord[this.batchSize];
        this.buf = tmp;
        this.count = 0;
        this.firstAppendNanos = 0L;
        this.batchOperator =
                (this.enabled && headOperator instanceof BatchProcessingOperator)
                        ? (BatchProcessingOperator<T, ?>) headOperator
                        : null;
        this.asyncPrefetchScheduledUntil = 0;
    }

    // ------------------------------------------------------------------------
    //  DataOutput<T> -- the upstream emitter calls these.
    // ------------------------------------------------------------------------

    @Override
    public void emitRecord(StreamRecord<T> record) throws Exception {
        if (!enabled) {
            // Pure pass-through; no buffering overhead.
            wrapped.emitRecord(record);
            return;
        }
        append(record);
        if (count >= batchSize) {
            flushBatch();
        }
    }

    @Override
    public void emitWatermark(Watermark watermark) throws Exception {
        // Watermark must observe all preceding records.
        if (count > 0) {
            flushBatch();
        }
        wrapped.emitWatermark(watermark);
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        if (count > 0) {
            flushBatch();
        }
        wrapped.emitWatermarkStatus(watermarkStatus);
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        if (count > 0) {
            flushBatch();
        }
        wrapped.emitLatencyMarker(latencyMarker);
    }

    // ------------------------------------------------------------------------
    //  BatchOutput<T> -- the StreamTask / processor calls these.
    // ------------------------------------------------------------------------

    @Override
    public void append(StreamRecord<T> record) {
        if (count == 0) {
            firstAppendNanos = System.nanoTime();
        }
        buf[count++] = record;
        scheduleAsyncPrefetchChunks(false);
    }

    @Override
    public boolean shouldFlush() {
        if (count == 0) {
            return false;
        }
        if (count >= batchSize) {
            return true;
        }
        return batchTimeoutNanos > 0 && (System.nanoTime() - firstAppendNanos) >= batchTimeoutNanos;
    }

    @Override
    public int size() {
        return count;
    }

    @Override
    public void flushBatch() throws Exception {
        if (count == 0) {
            return;
        }
        final int n = count;
        // THROWAWAY collapse-ratio diagnostic (no-op unless collapse-probe.enabled). Counts
        // distinct keys per batch before dispatch; does not change correctness.
        CollapseProbe.observe(headOperator, buf, n);
        try {
            if (prefetchMode && BP_PREFETCH_ASYNC_CHUNKS) {
                // Fire-and-forget: the backend-side prefetch is asynchronous (shared worker
                // thread + staging cache), so there is nothing to join — blocking the mailbox
                // here would defeat the purpose of prefetching.
                scheduleAsyncPrefetchChunks(true);
            }
            if (LocalPreagg.dispatch(headOperator, buf, n, numRecordsIn)) {
                // Runtime local pre-aggregation handled the whole batch (key-grouped fold with
                // collapsed emit). Falls through to the paths below when disabled or the head
                // operator is not a BatchableKeyedFunction.
                return;
            } else if (prefetchMode) {
                // Backpressure-driven prefetch: warm the keyed state for the whole lookahead
                // window with a single prefetch. By default this emits each record in arrival
                // order; when enabled, the existing conservative key-sort adapter may amortize
                // same-key dispatch for known commutative operators and falls back otherwise.
                if (!BP_PREFETCH_ASYNC_CHUNKS
                        && (!backpressureGated
                                || (backpressured != null && backpressured.getAsBoolean()))) {
                    org.apache.flink.streaming.runtime.tasks.StatePrefetcher.prefetch(
                            headOperator, buf, n);
                }
                if (commutativeKeySort) {
                    org.apache.flink.streaming.runtime.tasks.BatchedKeyedOperatorAdapter
                            .dispatchSorted(headOperator, buf, n, numRecordsIn);
                } else {
                    for (int i = 0; i < n; i++) {
                        wrapped.emitRecord(buf[i]);
                    }
                }
            } else if (batchOperator != null) {
                // Operator-aware fast path. The operator sees the whole array and decides
                // when to setKeyContextElement (e.g., per-key run amortisation in v2).
                // The operator implementation is responsible for counting records via the
                // standard runtime context if it wants to bypass the wrapped DataOutput.
                if (numRecordsIn != null) {
                    numRecordsIn.inc(n);
                }
                batchOperator.processElementBatch(buf, n);
            } else if (commutativeKeySort) {
                // v2: same-key run amortization. The adapter classifies the operator and either
                // sorts + dispatches with one setCurrentKey per run, or falls back to per-record
                // arrival-order dispatch internally if the operator is not commutative or any
                // reflection step fails. Either way, numRecordsIn is incremented per record.
                org.apache.flink.streaming.runtime.tasks.BatchedKeyedOperatorAdapter.dispatchSorted(
                        headOperator, buf, n, numRecordsIn);
            } else {
                // Fallback: replay through the wrapped DataOutput so setKeyContextElement
                // and per-record metrics bookkeeping happens exactly as the unbatched path.
                for (int i = 0; i < n; i++) {
                    wrapped.emitRecord(buf[i]);
                }
            }
        } finally {
            // Clear refs so records can be GC'd.
            for (int i = 0; i < n; i++) {
                buf[i] = null;
            }
            count = 0;
            firstAppendNanos = 0L;
            asyncPrefetchScheduledUntil = 0;
        }
    }

    private void scheduleAsyncPrefetchChunks(boolean includeRemainder) {
        if (!enabled
                || !prefetchMode
                || !BP_PREFETCH_ASYNC_CHUNKS
                || count <= 1) {
            return;
        }
        if (backpressureGated && (backpressured == null || !backpressured.getAsBoolean())) {
            return;
        }
        while (asyncPrefetchScheduledUntil < count) {
            int remaining = count - asyncPrefetchScheduledUntil;
            if (remaining < BP_PREFETCH_ASYNC_CHUNK_SIZE && !includeRemainder) {
                return;
            }
            int start = asyncPrefetchScheduledUntil;
            int end = Math.min(count, start + BP_PREFETCH_ASYNC_CHUNK_SIZE);
            if (end - start <= 1) {
                asyncPrefetchScheduledUntil = end;
                return;
            }
            // StatePrefetcher only extracts keys and invokes CacheKit's non-blocking submission
            // hook. Read the live range directly: copying a slice and chaining an already-complete
            // future added allocation without providing ordering or backpressure semantics.
            // LocalPreagg candidates are intentionally included: early chunks can overlap their
            // later grouping/fold work. At flush, immediate prefetch skips staged/in-flight keys,
            // so the two paths do not issue the same batch read twice.
            org.apache.flink.streaming.runtime.tasks.StatePrefetcher.prefetch(
                    headOperator, buf, start, end);
            asyncPrefetchScheduledUntil = end;
        }
    }

    // ------------------------------------------------------------------------
    //  Diagnostics
    // ------------------------------------------------------------------------

    public boolean isEnabled() {
        return enabled;
    }

    public int batchSize() {
        return batchSize;
    }

    public long batchTimeoutNanos() {
        return batchTimeoutNanos;
    }
}
