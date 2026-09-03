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

import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.BatchProcessingOperator;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
    /** Whether completed lookahead chunks are submitted before the mailbox batch flushes. */
    private final boolean asyncPrefetchChunks;
    /** Records per early prefetch submission. */
    private final int asyncPrefetchChunkSize;
    /** Consumer-facing records that are too close to execution to prefetch profitably. */
    private final int asyncPrefetchHeadGuardRecords;
    /**
     * Records drained when the bounded lookahead reaches its high watermark. Zero preserves the
     * original whole-batch flush. A positive value keeps the prefetched tail resident while the
     * mailbox consumes only the head and then refills the window from the network input.
     */
    private final int asyncPrefetchSlidingDrainRecords;
    /** Revoke still-speculative prepared-key ownership for records selected for dispatch. */
    private final boolean cancelPrefetchOnDispatch;
    /** Ordered multi-batch ready gate. Opt-in and disabled under object reuse. */
    private final boolean readyGatedPrefetch;

    private final int readyGatedMaxInFlight;
    private final long readyGatedTimeoutNanos;
    private final ArrayDeque<ReadyBatch<T>> readyBatches = new ArrayDeque<>();

    private long nextReadyBatchSequence;
    private long readyBeforeDispatch;
    private long prefetchWaitNanos;
    private long dispatchBeforeReadyFallbacks;
    private long ringFullSinceNanos;
    private long ringFullNanos;
    private long maxObservedInFlightDepth;
    private long retainedReadyRecords;
    private long retainedReadyReferenceBytes;
    private long maxRetainedReadyRecords;
    private long maxRetainedReadyReferenceBytes;
    private long readyBatchesStarted;
    private long readyTimeoutFallbacks;
    private long readyFailureFallbacks;
    private long readyNotProvenFallbacks;
    private long readyForcedFallbacks;
    private long readyRecordsDispatched;
    private long fallbackRecordsDispatched;

    private static final ScheduledThreadPoolExecutor READY_TIMEOUT_EXECUTOR;

    static {
        READY_TIMEOUT_EXECUTOR =
                new ScheduledThreadPoolExecutor(
                        1,
                        runnable -> {
                            Thread thread = new Thread(runnable, "cachekit-ready-prefetch-timeout");
                            thread.setDaemon(true);
                            return thread;
                        });
        READY_TIMEOUT_EXECUTOR.setRemoveOnCancelPolicy(true);
        READY_TIMEOUT_EXECUTOR.setKeepAliveTime(60L, TimeUnit.SECONDS);
        READY_TIMEOUT_EXECUTOR.allowCoreThreadTimeOut(true);
    }

    // Reusable record buffer. Sized at construction.
    @SuppressWarnings({"unchecked", "rawtypes"})
    private final StreamRecord<T>[] buf;

    private int count;
    private long firstAppendNanos;
    private int asyncPrefetchScheduledUntil;

    private enum ReadyBatchPhase {
        PREFETCHING,
        READY,
        DISPATCHING
    }

    private static final class ReadyBatch<T> {
        private final long sequence;
        private final StreamRecord<T>[] records;
        private final int count;
        private final long sealedNanos;
        private final long deadlineNanos;
        private final CompletableFuture<Void> wakeUp = new CompletableFuture<>();
        private final ScheduledFuture<?> timeout;
        private volatile ReadyBatchPhase phase = ReadyBatchPhase.PREFETCHING;
        private volatile boolean completionObserved;
        private volatile boolean ready;
        private volatile Throwable failure;

        private ReadyBatch(
                long sequence,
                StreamRecord<T>[] records,
                int count,
                long timeoutNanos,
                CompletableFuture<Boolean> completion) {
            this.sequence = sequence;
            this.records = records;
            this.count = count;
            this.sealedNanos = System.nanoTime();
            this.deadlineNanos = sealedNanos + timeoutNanos;
            this.timeout =
                    READY_TIMEOUT_EXECUTOR.schedule(
                            () -> wakeUp.complete(null), timeoutNanos, TimeUnit.NANOSECONDS);
            completion.whenComplete(
                    (result, error) -> {
                        failure = error;
                        ready = error == null && Boolean.TRUE.equals(result);
                        completionObserved = true;
                        phase = ReadyBatchPhase.READY;
                        wakeUp.complete(null);
                    });
        }

        private boolean timedOut(long nowNanos) {
            return !completionObserved && nowNanos >= deadlineNanos;
        }

        private void release() {
            timeout.cancel(false);
            Arrays.fill(records, null);
        }
    }

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
                true,
                false,
                16,
                0,
                0);
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
        this(
                wrapped,
                headOperator,
                enabled,
                commutativeKeySort,
                batchSize,
                batchTimeoutNanos,
                numRecordsIn,
                prefetchMode,
                backpressured,
                backpressureGated,
                false,
                16,
                0,
                0);
    }

    /** Full constructor with task-configuration-derived asynchronous lookahead controls. */
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
            boolean backpressureGated,
            boolean asyncPrefetchChunks,
            int asyncPrefetchChunkSize) {
        this(
                wrapped,
                headOperator,
                enabled,
                commutativeKeySort,
                batchSize,
                batchTimeoutNanos,
                numRecordsIn,
                prefetchMode,
                backpressured,
                backpressureGated,
                asyncPrefetchChunks,
                asyncPrefetchChunkSize,
                0,
                0,
                false);
    }

    /** Full constructor including a sliding consumer-head guard for speculative prefetch. */
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
            boolean backpressureGated,
            boolean asyncPrefetchChunks,
            int asyncPrefetchChunkSize,
            int asyncPrefetchHeadGuardRecords) {
        this(
                wrapped,
                headOperator,
                enabled,
                commutativeKeySort,
                batchSize,
                batchTimeoutNanos,
                numRecordsIn,
                prefetchMode,
                backpressured,
                backpressureGated,
                asyncPrefetchChunks,
                asyncPrefetchChunkSize,
                asyncPrefetchHeadGuardRecords,
                0,
                false);
    }

    /** Full constructor including bounded rolling-window consumption. */
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
            boolean backpressureGated,
            boolean asyncPrefetchChunks,
            int asyncPrefetchChunkSize,
            int asyncPrefetchHeadGuardRecords,
            int asyncPrefetchSlidingDrainRecords) {
        this(
                wrapped,
                headOperator,
                enabled,
                commutativeKeySort,
                batchSize,
                batchTimeoutNanos,
                numRecordsIn,
                prefetchMode,
                backpressured,
                backpressureGated,
                asyncPrefetchChunks,
                asyncPrefetchChunkSize,
                asyncPrefetchHeadGuardRecords,
                asyncPrefetchSlidingDrainRecords,
                false);
    }

    /** Full constructor including exact dispatch-time prepared-prefetch cancellation. */
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
            boolean backpressureGated,
            boolean asyncPrefetchChunks,
            int asyncPrefetchChunkSize,
            int asyncPrefetchHeadGuardRecords,
            int asyncPrefetchSlidingDrainRecords,
            boolean cancelPrefetchOnDispatch) {
        this(
                wrapped,
                headOperator,
                enabled,
                commutativeKeySort,
                batchSize,
                batchTimeoutNanos,
                numRecordsIn,
                prefetchMode,
                backpressured,
                backpressureGated,
                asyncPrefetchChunks,
                asyncPrefetchChunkSize,
                asyncPrefetchHeadGuardRecords,
                asyncPrefetchSlidingDrainRecords,
                cancelPrefetchOnDispatch,
                false,
                2,
                TimeUnit.MILLISECONDS.toNanos(1),
                false);
    }

    /** Complete constructor including the bounded ordered ready gate. */
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
            boolean backpressureGated,
            boolean asyncPrefetchChunks,
            int asyncPrefetchChunkSize,
            int asyncPrefetchHeadGuardRecords,
            int asyncPrefetchSlidingDrainRecords,
            boolean cancelPrefetchOnDispatch,
            boolean readyGatedPrefetch,
            int readyGatedMaxInFlight,
            long readyGatedTimeoutNanos,
            boolean objectReuseEnabled) {
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
        this.readyGatedPrefetch =
                readyGatedPrefetch && this.enabled && prefetchMode && !objectReuseEnabled;
        this.readyGatedMaxInFlight = Math.max(2, Math.min(4, readyGatedMaxInFlight));
        this.readyGatedTimeoutNanos = Math.max(1L, readyGatedTimeoutNanos);
        this.asyncPrefetchChunks = asyncPrefetchChunks && !this.readyGatedPrefetch;
        this.asyncPrefetchChunkSize = Math.max(2, Math.min(1024, asyncPrefetchChunkSize));
        this.asyncPrefetchHeadGuardRecords =
                Math.max(0, Math.min(this.batchSize - 1, asyncPrefetchHeadGuardRecords));
        this.asyncPrefetchSlidingDrainRecords =
                !this.asyncPrefetchChunks
                        ? 0
                        : Math.max(
                                0, Math.min(this.batchSize - 1, asyncPrefetchSlidingDrainRecords));
        this.cancelPrefetchOnDispatch = cancelPrefetchOnDispatch && this.asyncPrefetchChunks;
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
        this.nextReadyBatchSequence = 0L;
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
            if (readyGatedPrefetch) {
                sealReadyBatch();
            } else if (asyncPrefetchSlidingDrainRecords > 0) {
                drainSlidingHead();
            } else {
                flushBatch();
            }
        }
    }

    @Override
    public void emitWatermark(Watermark watermark) throws Exception {
        // Watermark must observe all preceding records.
        if (size() > 0) {
            flushBatch();
        }
        wrapped.emitWatermark(watermark);
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        if (size() > 0) {
            flushBatch();
        }
        wrapped.emitWatermarkStatus(watermarkStatus);
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        if (size() > 0) {
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
        return readyGatedPrefetch ? count + (int) retainedReadyRecords : count;
    }

    @Override
    public void flushBatch() throws Exception {
        if (readyGatedPrefetch) {
            if (count > 0) {
                sealReadyBatch();
            }
            drainReadyBatches();
            while (!readyBatches.isEmpty()) {
                dispatchReadyHead(true);
            }
            return;
        }
        if (count == 0) {
            return;
        }
        final int n = count;
        // THROWAWAY collapse-ratio diagnostic (no-op unless collapse-probe.enabled). Counts
        // distinct keys per batch before dispatch; does not change correctness.
        CollapseProbe.observe(headOperator, buf, n);
        try {
            if (prefetchMode && asyncPrefetchChunks) {
                // Fire-and-forget: the backend-side prefetch is asynchronous (shared worker
                // thread + staging cache), so there is nothing to join — blocking the mailbox
                // here would defeat the purpose of prefetching.
                scheduleAsyncPrefetchChunks(true);
            }
            dispatchPrefix(n);
        } finally {
            discardPrefix(n);
        }
    }

    @Override
    public void drainReadyBatches() throws Exception {
        if (!readyGatedPrefetch) {
            return;
        }
        while (!readyBatches.isEmpty()) {
            ReadyBatch<T> head = readyBatches.peekFirst();
            if (!head.completionObserved && !head.timedOut(System.nanoTime())) {
                return;
            }
            dispatchReadyHead(false);
        }
    }

    @Override
    public boolean isInputBlocked() {
        return readyGatedPrefetch && readyBatches.size() >= readyGatedMaxInFlight;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture(CompletableFuture<?> inputAvailable) {
        if (!readyGatedPrefetch || readyBatches.isEmpty()) {
            return inputAvailable;
        }
        CompletableFuture<?> headReady = readyBatches.peekFirst().wakeUp;
        if (isInputBlocked()) {
            return headReady;
        }
        return CompletableFuture.anyOf(inputAvailable, headReady);
    }

    /** Move the current filling buffer into the ordered in-flight ring. */
    private void sealReadyBatch() {
        if (count == 0) {
            return;
        }
        if (readyBatches.size() >= readyGatedMaxInFlight) {
            throw new IllegalStateException("ready-gated prefetch ring exceeded its bound");
        }
        final int n = count;
        StreamRecord<T>[] records = Arrays.copyOf(buf, n);
        Arrays.fill(buf, 0, n, null);
        count = 0;
        firstAppendNanos = 0L;
        asyncPrefetchScheduledUntil = 0;

        CompletableFuture<Boolean> completion = startReadyPrefetch(records, n);
        ReadyBatch<T> batch =
                new ReadyBatch<>(
                        nextReadyBatchSequence++, records, n, readyGatedTimeoutNanos, completion);
        readyBatches.addLast(batch);
        readyBatchesStarted++;
        retainedReadyRecords += n;
        retainedReadyReferenceBytes += (long) n * Long.BYTES;
        maxRetainedReadyRecords = Math.max(maxRetainedReadyRecords, retainedReadyRecords);
        maxRetainedReadyReferenceBytes =
                Math.max(maxRetainedReadyReferenceBytes, retainedReadyReferenceBytes);
        maxObservedInFlightDepth = Math.max(maxObservedInFlightDepth, readyBatches.size());
        if (readyBatches.size() >= readyGatedMaxInFlight && ringFullSinceNanos == 0L) {
            ringFullSinceNanos = System.nanoTime();
        }
    }

    CompletableFuture<Boolean> startReadyPrefetch(StreamRecord<T>[] records, int n) {
        if (backpressureGated && (backpressured == null || !backpressured.getAsBoolean())) {
            return CompletableFuture.completedFuture(false);
        }
        return org.apache.flink.streaming.runtime.tasks.StatePrefetcher.prefetchWithCompletion(
                headOperator, records, 0, n);
    }

    /** Dispatch exactly one ring head, falling back immediately when forced or not proven ready. */
    private void dispatchReadyHead(boolean force) throws Exception {
        ReadyBatch<T> head = readyBatches.peekFirst();
        if (head == null) {
            return;
        }
        long now = System.nanoTime();
        boolean provenReady = head.completionObserved && head.ready && head.failure == null;
        if (!force && !provenReady && !head.completionObserved && !head.timedOut(now)) {
            return;
        }
        head.phase = ReadyBatchPhase.DISPATCHING;
        if (provenReady) {
            readyBeforeDispatch++;
            readyRecordsDispatched += head.count;
            prefetchWaitNanos += Math.max(0L, now - head.sealedNanos);
        } else {
            dispatchBeforeReadyFallbacks++;
            fallbackRecordsDispatched += head.count;
            if (force) {
                readyForcedFallbacks++;
            } else if (!head.completionObserved) {
                readyTimeoutFallbacks++;
            } else if (head.failure != null) {
                readyFailureFallbacks++;
            } else {
                readyNotProvenFallbacks++;
            }
            cancelPrefetchForDispatch(head.records, head.count);
        }
        CollapseProbe.observe(headOperator, head.records, head.count);
        try {
            dispatchRecords(head.records, head.count, false);
        } finally {
            ReadyBatch<T> removed = readyBatches.removeFirst();
            if (removed != head) {
                throw new IllegalStateException("ready-gated prefetch ring order changed");
            }
            retainedReadyRecords -= head.count;
            retainedReadyReferenceBytes -= (long) head.count * Long.BYTES;
            head.release();
            if (ringFullSinceNanos != 0L && readyBatches.size() < readyGatedMaxInFlight) {
                ringFullNanos += Math.max(0L, System.nanoTime() - ringFullSinceNanos);
                ringFullSinceNanos = 0L;
            }
        }
    }

    /**
     * Consume only the mailbox-facing prefix and retain the already-prefetched tail. This is the
     * actual sliding step: after return, network deserialization refills the freed slots while the
     * tail remains far enough from consumption for its asynchronous MultiGet to complete.
     */
    private void drainSlidingHead() throws Exception {
        final int n = Math.min(asyncPrefetchSlidingDrainRecords, count);
        if (n <= 0) {
            return;
        }
        CollapseProbe.observe(headOperator, buf, n);
        try {
            dispatchPrefix(n);
        } finally {
            discardPrefix(n);
        }
    }

    /** Dispatch a prefix without changing buffer ownership or queue indices. */
    private void dispatchPrefix(int n) throws Exception {
        dispatchRecords(buf, n, true);
    }

    /** Dispatch caller-owned records without changing their buffer or ring ownership. */
    private void dispatchRecords(
            StreamRecord<T>[] records, int n, boolean applyConfiguredCancellation)
            throws Exception {
        // Selection is the exact point where a lookahead key stops being speculative. Revoke only
        // the matching prepared-MultiGet reservation before LocalPreagg or ordinary per-record
        // execution can race the worker. Already-published staging remains usable; unsupported
        // backends fail closed inside StatePrefetcher.
        if (!readyGatedPrefetch
                && LocalPreagg.dispatch(
                        headOperator,
                        records,
                        n,
                        numRecordsIn,
                        cancelPrefetchOnDispatch && applyConfiguredCancellation)) {
            return;
        }
        // LocalPreagg reuses its already-deduplicated group keys for cancellation. Only the
        // ordinary replay path still needs to extract keys from the record prefix here.
        if (cancelPrefetchOnDispatch && applyConfiguredCancellation) {
            cancelPrefetchForDispatch(n);
        }
        if (prefetchMode) {
            if (!readyGatedPrefetch
                    && !asyncPrefetchChunks
                    && (!backpressureGated
                            || (backpressured != null && backpressured.getAsBoolean()))) {
                org.apache.flink.streaming.runtime.tasks.StatePrefetcher.prefetch(
                        headOperator, records, n);
            }
        }
        boolean nativeMutationBatchStarted =
                org.apache.flink.streaming.runtime.tasks.StatePrefetcher
                        .beginNativeResidentMutationBatch(headOperator, records, 0, n);
        try {
            if (prefetchMode) {
                if (commutativeKeySort && !readyGatedPrefetch) {
                    org.apache.flink.streaming.runtime.tasks.BatchedKeyedOperatorAdapter
                            .dispatchSorted(headOperator, records, n, numRecordsIn);
                } else {
                    for (int i = 0; i < n; i++) {
                        wrapped.emitRecord(records[i]);
                    }
                }
            } else if (batchOperator != null) {
                if (numRecordsIn != null) {
                    numRecordsIn.inc(n);
                }
                batchOperator.processElementBatch(records, n);
            } else if (commutativeKeySort) {
                org.apache.flink.streaming.runtime.tasks.BatchedKeyedOperatorAdapter.dispatchSorted(
                        headOperator, records, n, numRecordsIn);
            } else {
                for (int i = 0; i < n; i++) {
                    wrapped.emitRecord(records[i]);
                }
            }
        } finally {
            if (nativeMutationBatchStarted) {
                org.apache.flink.streaming.runtime.tasks.StatePrefetcher
                        .endNativeResidentMutationBatch(headOperator);
            }
        }
    }

    /** Test seam for proving that revocation precedes any state-consuming dispatch path. */
    int cancelPrefetchForDispatch(int n) {
        return cancelPrefetchForDispatch(buf, n);
    }

    /** Test seam for proving fallback cancellation on a detached ready batch. */
    int cancelPrefetchForDispatch(StreamRecord<T>[] records, int n) {
        return org.apache.flink.streaming.runtime.tasks.StatePrefetcher.cancelPrefetchForDispatch(
                headOperator, records, 0, n);
    }

    /** Remove a dispatched prefix while preserving arrival order and scheduled-tail ownership. */
    private void discardPrefix(int n) {
        final int remaining = count - n;
        if (remaining > 0) {
            System.arraycopy(buf, n, buf, 0, remaining);
        }
        for (int i = remaining; i < count; i++) {
            buf[i] = null;
        }
        count = remaining;
        if (remaining == 0) {
            firstAppendNanos = 0L;
            asyncPrefetchScheduledUntil = 0;
        } else {
            asyncPrefetchScheduledUntil = Math.max(0, asyncPrefetchScheduledUntil - n);
        }
    }

    private void scheduleAsyncPrefetchChunks(boolean includeRemainder) {
        if (!enabled || !prefetchMode || !asyncPrefetchChunks || count <= 1) {
            return;
        }
        if (backpressureGated && (backpressured == null || !backpressured.getAsBoolean())) {
            return;
        }
        // Do not spend I/O on the records nearest to mailbox consumption. The eligible tail grows
        // as append() advances the lookahead window; each consumed batch resets this boundary.
        asyncPrefetchScheduledUntil =
                Math.max(asyncPrefetchScheduledUntil, asyncPrefetchHeadGuardRecords);
        while (asyncPrefetchScheduledUntil < count) {
            int remaining = count - asyncPrefetchScheduledUntil;
            if (remaining < asyncPrefetchChunkSize && !includeRemainder) {
                return;
            }
            int start = asyncPrefetchScheduledUntil;
            int end = Math.min(count, start + asyncPrefetchChunkSize);
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

    public boolean isReadyGatedPrefetchEnabled() {
        return readyGatedPrefetch;
    }

    public long getReadyBatchesStarted() {
        return readyBatchesStarted;
    }

    public long getReadyBeforeDispatch() {
        return readyBeforeDispatch;
    }

    public long getPrefetchWaitNanos() {
        return prefetchWaitNanos;
    }

    public long getDispatchBeforeReadyFallbacks() {
        return dispatchBeforeReadyFallbacks;
    }

    public long getReadyTimeoutFallbacks() {
        return readyTimeoutFallbacks;
    }

    public long getReadyFailureFallbacks() {
        return readyFailureFallbacks;
    }

    public long getReadyNotProvenFallbacks() {
        return readyNotProvenFallbacks;
    }

    public long getReadyForcedFallbacks() {
        return readyForcedFallbacks;
    }

    public long getReadyRecordsDispatched() {
        return readyRecordsDispatched;
    }

    public long getFallbackRecordsDispatched() {
        return fallbackRecordsDispatched;
    }

    public int getInFlightDepth() {
        return readyBatches.size();
    }

    public long getMaxObservedInFlightDepth() {
        return maxObservedInFlightDepth;
    }

    public long getRingFullNanos() {
        return ringFullNanos
                + (ringFullSinceNanos == 0L
                        ? 0L
                        : Math.max(0L, System.nanoTime() - ringFullSinceNanos));
    }

    public long getRetainedReadyRecords() {
        return retainedReadyRecords;
    }

    /** Conservative 8-byte-per-reference estimate, excluding retained record object graphs. */
    public long getRetainedReadyReferenceBytes() {
        return retainedReadyReferenceBytes;
    }

    public long getMaxRetainedReadyRecords() {
        return maxRetainedReadyRecords;
    }

    public long getMaxRetainedReadyReferenceBytes() {
        return maxRetainedReadyReferenceBytes;
    }

    int asyncPrefetchScheduledUntilForTesting() {
        return asyncPrefetchScheduledUntil;
    }

    boolean readyGatedPrefetchEnabledForTesting() {
        return isReadyGatedPrefetchEnabled();
    }

    long readyBeforeDispatchForTesting() {
        return getReadyBeforeDispatch();
    }

    long dispatchBeforeReadyFallbacksForTesting() {
        return getDispatchBeforeReadyFallbacks();
    }

    long maxObservedInFlightDepthForTesting() {
        return getMaxObservedInFlightDepth();
    }

    long prefetchWaitNanosForTesting() {
        return getPrefetchWaitNanos();
    }

    long ringFullNanosForTesting() {
        return getRingFullNanos();
    }

    long headSequenceForTesting() {
        return readyBatches.isEmpty() ? -1L : readyBatches.peekFirst().sequence;
    }
}
