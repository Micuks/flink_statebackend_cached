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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.sort.SortingDataInput;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamRecordBatchOutput;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInputFactory;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.checkpointing.InputProcessorUtil;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.apache.flink.shaded.curator5.com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.streaming.api.graph.StreamConfig.requiresSorting;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** A {@link StreamTask} for executing a {@link OneInputStreamOperator}. */
@Internal
public class OneInputStreamTask<IN, OUT> extends StreamTask<OUT, OneInputStreamOperator<IN, OUT>> {

    private static final Logger LOG = LoggerFactory.getLogger(OneInputStreamTask.class);

    @Nullable private CheckpointBarrierHandler checkpointBarrierHandler;

    private final WatermarkGauge inputWatermarkGauge = new WatermarkGauge();

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * @param env The task environment for this task.
     */
    public OneInputStreamTask(Environment env) throws Exception {
        super(env);
    }

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * <p>This constructor accepts a special {@link TimerService}. By default (and if null is passes
     * for the time provider) a {@link SystemProcessingTimeService DefaultTimerService} will be
     * used.
     *
     * @param env The task environment for this task.
     * @param timeProvider Optionally, a specific time provider to use.
     */
    @VisibleForTesting
    public OneInputStreamTask(Environment env, @Nullable TimerService timeProvider)
            throws Exception {
        super(env, timeProvider);
    }

    @Override
    public void init() throws Exception {
        StreamConfig configuration = getConfiguration();
        int numberOfInputs = configuration.getNumberOfNetworkInputs();

        if (numberOfInputs > 0) {
            CheckpointedInputGate inputGate = createCheckpointedInputGate();
            Counter numRecordsIn = setupNumRecordsInCounter(mainOperator);
            DataOutput<IN> output = createDataOutput(numRecordsIn);
            output = maybeWrapWithBatchOutput(output, numRecordsIn);
            StreamTaskInput<IN> input = createTaskInput(inputGate);

            StreamConfig.InputConfig[] inputConfigs =
                    configuration.getInputs(getUserCodeClassLoader());
            StreamConfig.InputConfig inputConfig = inputConfigs[0];
            if (requiresSorting(inputConfig)) {
                checkState(
                        !configuration.isCheckpointingEnabled(),
                        "Checkpointing is not allowed with sorted inputs.");
                input = wrapWithSorted(input);
            }

            getEnvironment()
                    .getMetricGroup()
                    .getIOMetricGroup()
                    .reuseRecordsInputCounter(numRecordsIn);

            inputProcessor = new StreamOneInputProcessor<>(input, output, operatorChain);
        }
        mainOperator
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, inputWatermarkGauge);
        // wrap watermark gauge since registered metrics must be unique
        getEnvironment()
                .getMetricGroup()
                .gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, inputWatermarkGauge::getValue);
    }

    @Override
    protected Optional<CheckpointBarrierHandler> getCheckpointBarrierHandler() {
        return Optional.ofNullable(checkpointBarrierHandler);
    }

    private StreamTaskInput<IN> wrapWithSorted(StreamTaskInput<IN> input) {
        ClassLoader userCodeClassLoader = getUserCodeClassLoader();
        return new SortingDataInput<>(
                input,
                configuration.getTypeSerializerIn(input.getInputIndex(), userCodeClassLoader),
                configuration.getStateKeySerializer(userCodeClassLoader),
                configuration.getStatePartitioner(input.getInputIndex(), userCodeClassLoader),
                getEnvironment().getMemoryManager(),
                getEnvironment().getIOManager(),
                getExecutionConfig().isObjectReuseEnabled(),
                configuration.getManagedMemoryFractionOperatorUseCaseOfSlot(
                        ManagedMemoryUseCase.OPERATOR,
                        getEnvironment().getTaskConfiguration(),
                        userCodeClassLoader),
                getEnvironment().getTaskManagerInfo().getConfiguration(),
                this,
                getExecutionConfig());
    }

    @SuppressWarnings("unchecked")
    private CheckpointedInputGate createCheckpointedInputGate() {
        IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();

        checkpointBarrierHandler =
                InputProcessorUtil.createCheckpointBarrierHandler(
                        this,
                        configuration,
                        getCheckpointCoordinator(),
                        getTaskNameWithSubtaskAndId(),
                        new List[] {Arrays.asList(inputGates)},
                        Collections.emptyList(),
                        mainMailboxExecutor,
                        systemTimerService);

        CheckpointedInputGate[] checkpointedInputGates =
                InputProcessorUtil.createCheckpointedMultipleInputGate(
                        mainMailboxExecutor,
                        new List[] {Arrays.asList(inputGates)},
                        getEnvironment().getMetricGroup().getIOMetricGroup(),
                        checkpointBarrierHandler,
                        configuration);

        return Iterables.getOnlyElement(Arrays.asList(checkpointedInputGates));
    }

    private DataOutput<IN> createDataOutput(Counter numRecordsIn) {
        return new StreamTaskNetworkOutput<>(
                operatorChain.getFinishedOnRestoreInputOrDefault(mainOperator),
                inputWatermarkGauge,
                numRecordsIn);
    }

    private DataOutput<IN> maybeWrapWithBatchOutput(DataOutput<IN> output, Counter numRecordsIn) {
        try {
            org.apache.flink.configuration.Configuration cfg =
                    getEnvironment().getTaskManagerInfo().getConfiguration();

            boolean bpPrefetchEnabled =
                    cfg.getBoolean(
                            org.apache.flink.configuration.ConfigOptions.key(
                                            "state.backend.cachekit.bp-prefetch.enabled")
                                    .booleanType()
                                    .defaultValue(false));
            org.apache.flink.metrics.MetricGroup runtimePrefetchMetrics =
                    mainOperator.getMetricGroup().addGroup("cachekit").addGroup("runtimePrefetch");
            runtimePrefetchMetrics.gauge("configured", () -> bpPrefetchEnabled ? 1 : 0);
            if (bpPrefetchEnabled) {
                runtimePrefetchMetrics.gauge("attempts", StatePrefetcher::getPrefetchAttempts);
                runtimePrefetchMetrics.gauge("invalidInputs", StatePrefetcher::getInvalidInputs);
                runtimePrefetchMetrics.gauge(
                        "nonAbstractOperators", StatePrefetcher::getNonAbstractOperators);
                runtimePrefetchMetrics.gauge(
                        "backendAccessFailures", StatePrefetcher::getBackendAccessFailures);
                runtimePrefetchMetrics.gauge(
                        "missingBackends", StatePrefetcher::getMissingBackends);
                runtimePrefetchMetrics.gauge(
                        "missingPrefetchMethods", StatePrefetcher::getMissingPrefetchMethods);
                runtimePrefetchMetrics.gauge(
                        "noPrefetchableStates", StatePrefetcher::getNoPrefetchableStates);
                runtimePrefetchMetrics.gauge(
                        "missingKeySelectors", StatePrefetcher::getMissingKeySelectors);
                runtimePrefetchMetrics.gauge(
                        "keyExtractionFailures", StatePrefetcher::getKeyExtractionFailures);
                runtimePrefetchMetrics.gauge(
                        "emptyKeyBatches", StatePrefetcher::getEmptyKeyBatches);
                runtimePrefetchMetrics.gauge(
                        "backendInvocationAttempts", StatePrefetcher::getBackendInvocationAttempts);
                runtimePrefetchMetrics.gauge(
                        "backendInvocations", StatePrefetcher::getBackendInvocations);
                runtimePrefetchMetrics.gauge("failures", StatePrefetcher::getFailures);
                int distance =
                        cfg.getInteger(
                                org.apache.flink.configuration.ConfigOptions.key(
                                                "state.backend.cachekit.bp-prefetch.distance")
                                        .intType()
                                        .defaultValue(64));
                distance = Math.max(1, Math.min(4096, distance));
                boolean backpressureGated =
                        cfg.getBoolean(
                                org.apache.flink.configuration.ConfigOptions.key(
                                                "state.backend.cachekit.bp-prefetch.backpressure-gated")
                                        .booleanType()
                                        .defaultValue(true));
                boolean bpPrefetchKeySort =
                        cfg.getBoolean(
                                org.apache.flink.configuration.ConfigOptions.key(
                                                "state.backend.cachekit.bp-prefetch.commutative-key-sort")
                                        .booleanType()
                                        .defaultValue(false));
                boolean asyncPrefetchChunks =
                        cfg.getBoolean(
                                org.apache.flink.configuration.ConfigOptions.key(
                                                "state.backend.cachekit.bp-prefetch.async-chunks.enabled")
                                        .booleanType()
                                        .defaultValue(false));
                int asyncPrefetchChunkSize =
                        cfg.getInteger(
                                org.apache.flink.configuration.ConfigOptions.key(
                                                "state.backend.cachekit.bp-prefetch.async-chunks.size")
                                        .intType()
                                        .defaultValue(16));
                int asyncPrefetchHeadGuardRecords =
                        cfg.getInteger(
                                org.apache.flink.configuration.ConfigOptions.key(
                                                "state.backend.cachekit.bp-prefetch.head-guard-records")
                                        .intType()
                                        .defaultValue(0));
                int asyncPrefetchSlidingDrainRecords =
                        cfg.getInteger(
                                org.apache.flink.configuration.ConfigOptions.key(
                                                "state.backend.cachekit.bp-prefetch.sliding-drain-records")
                                        .intType()
                                        .defaultValue(0));
                boolean cancelPrefetchOnDispatch =
                        cfg.getBoolean(
                                org.apache.flink.configuration.ConfigOptions.key(
                                                "state.backend.cachekit.bp-prefetch.cancel-on-dispatch.enabled")
                                        .booleanType()
                                        .defaultValue(false));
                boolean readyGatedPrefetch =
                        cfg.getBoolean(
                                org.apache.flink.configuration.ConfigOptions.key(
                                                "state.backend.cachekit.bp-prefetch.ready-gated.enabled")
                                        .booleanType()
                                        .defaultValue(false));
                int readyGatedMaxInFlight =
                        cfg.getInteger(
                                org.apache.flink.configuration.ConfigOptions.key(
                                                "state.backend.cachekit.bp-prefetch.ready-gated.max-in-flight-batches")
                                        .intType()
                                        .defaultValue(2));
                readyGatedMaxInFlight = Math.max(2, Math.min(4, readyGatedMaxInFlight));
                long readyGatedTimeoutUs =
                        cfg.getLong(
                                org.apache.flink.configuration.ConfigOptions.key(
                                                "state.backend.cachekit.bp-prefetch.ready-gated.timeout-us")
                                        .longType()
                                        .defaultValue(5_000L));
                readyGatedTimeoutUs = Math.max(1L, Math.min(1_000_000L, readyGatedTimeoutUs));
                boolean unalignedCheckpoints =
                        cfg.getBoolean(
                                org.apache.flink.configuration.ConfigOptions.key(
                                                "execution.checkpointing.unaligned.enabled")
                                        .booleanType()
                                        .defaultValue(false));
                // A retained lookahead tail is not part of unaligned channel state. Until it has
                // an explicit snapshot serializer, fail closed to the original whole-batch flush.
                if (unalignedCheckpoints) {
                    asyncPrefetchSlidingDrainRecords = 0;
                    readyGatedPrefetch = false;
                }
                @SuppressWarnings("unchecked")
                Input<IN> headInput = (Input<IN>) mainOperator;
                java.util.function.BooleanSupplier bp =
                        () -> recordWriter != null && !recordWriter.isAvailable();
                StreamRecordBatchOutput<IN> batchOutput =
                        new StreamRecordBatchOutput<>(
                                output,
                                headInput,
                                true,
                                bpPrefetchKeySort,
                                distance,
                                0L,
                                numRecordsIn,
                                true,
                                bp,
                                backpressureGated,
                                asyncPrefetchChunks,
                                asyncPrefetchChunkSize,
                                asyncPrefetchHeadGuardRecords,
                                asyncPrefetchSlidingDrainRecords,
                                cancelPrefetchOnDispatch,
                                readyGatedPrefetch,
                                readyGatedMaxInFlight,
                                readyGatedTimeoutUs * 1_000L,
                                getExecutionConfig().isObjectReuseEnabled());
                org.apache.flink.metrics.MetricGroup readyMetrics =
                        runtimePrefetchMetrics.addGroup("readyGate");
                readyMetrics.gauge(
                        "active", () -> batchOutput.isReadyGatedPrefetchEnabled() ? 1 : 0);
                readyMetrics.gauge("batchesStarted", batchOutput::getReadyBatchesStarted);
                readyMetrics.gauge("readyBeforeDispatch", batchOutput::getReadyBeforeDispatch);
                readyMetrics.gauge("prefetchWaitNanos", batchOutput::getPrefetchWaitNanos);
                readyMetrics.gauge(
                        "dispatchBeforeReadyFallbacks",
                        batchOutput::getDispatchBeforeReadyFallbacks);
                readyMetrics.gauge("timeoutFallbacks", batchOutput::getReadyTimeoutFallbacks);
                readyMetrics.gauge("failureFallbacks", batchOutput::getReadyFailureFallbacks);
                readyMetrics.gauge("notProvenFallbacks", batchOutput::getReadyNotProvenFallbacks);
                readyMetrics.gauge("forcedFallbacks", batchOutput::getReadyForcedFallbacks);
                readyMetrics.gauge(
                        "readyRecordsDispatched", batchOutput::getReadyRecordsDispatched);
                readyMetrics.gauge(
                        "fallbackRecordsDispatched", batchOutput::getFallbackRecordsDispatched);
                readyMetrics.gauge("inFlightDepth", batchOutput::getInFlightDepth);
                readyMetrics.gauge("maxInFlightDepth", batchOutput::getMaxObservedInFlightDepth);
                readyMetrics.gauge("ringFullNanos", batchOutput::getRingFullNanos);
                readyMetrics.gauge("retainedRecords", batchOutput::getRetainedReadyRecords);
                readyMetrics.gauge(
                        "retainedReferenceBytes", batchOutput::getRetainedReadyReferenceBytes);
                readyMetrics.gauge("maxRetainedRecords", batchOutput::getMaxRetainedReadyRecords);
                readyMetrics.gauge(
                        "maxRetainedReferenceBytes",
                        batchOutput::getMaxRetainedReadyReferenceBytes);
                readyMetrics.gauge(
                        "workerQueueNanos",
                        () -> StatePrefetcher.getReadyGatedBackendMetric(headInput, 0));
                readyMetrics.gauge(
                        "workerServiceNanos",
                        () -> StatePrefetcher.getReadyGatedBackendMetric(headInput, 1));
                readyMetrics.gauge(
                        "valuesStaged",
                        () -> StatePrefetcher.getReadyGatedBackendMetric(headInput, 2));
                readyMetrics.gauge(
                        "stagedValuesConsumed",
                        () -> StatePrefetcher.getReadyGatedBackendMetric(headInput, 3));
                readyMetrics.gauge(
                        "stagedValuesDiscarded",
                        () -> StatePrefetcher.getReadyGatedBackendMetric(headInput, 4));
                readyMetrics.gauge(
                        "authoritativeReadsAvoided",
                        () -> StatePrefetcher.getReadyGatedBackendMetric(headInput, 5));
                readyMetrics.gauge(
                        "workerDiscardedAfterRead",
                        () -> StatePrefetcher.getReadyGatedBackendMetric(headInput, 6));
                readyMetrics.gauge(
                        "stagingAdmissionDrops",
                        () -> StatePrefetcher.getReadyGatedBackendMetric(headInput, 7));
                return batchOutput;
            }

            boolean enabled =
                    cfg.getBoolean(
                            org.apache.flink.configuration.ConfigOptions.key(
                                            "state.backend.cachekit.mailbox-batch.enabled")
                                    .booleanType()
                                    .defaultValue(false));
            if (!enabled) {
                return output;
            }
            int batchSize =
                    cfg.getInteger(
                            org.apache.flink.configuration.ConfigOptions.key(
                                            "state.backend.cachekit.mailbox-batch.size")
                                    .intType()
                                    .defaultValue(64));
            batchSize = Math.max(1, Math.min(4096, batchSize));
            long timeoutUs =
                    cfg.getLong(
                            org.apache.flink.configuration.ConfigOptions.key(
                                            "state.backend.cachekit.mailbox-batch.timeout-us")
                                    .longType()
                                    .defaultValue(100L));
            long timeoutNanos = timeoutUs > 0 ? timeoutUs * 1_000L : 0L;
            boolean commutativeKeySort =
                    cfg.getBoolean(
                            org.apache.flink.configuration.ConfigOptions.key(
                                            "state.backend.cachekit.mailbox-batch.commutative-key-sort")
                                    .booleanType()
                                    .defaultValue(true));
            double cardinalityThreshold =
                    cfg.getDouble(
                            org.apache.flink.configuration.ConfigOptions.key(
                                            "state.backend.cachekit.mailbox-batch.sort-cardinality-probe-threshold")
                                    .doubleType()
                                    .defaultValue(0.75));
            int probeSize =
                    cfg.getInteger(
                            org.apache.flink.configuration.ConfigOptions.key(
                                            "state.backend.cachekit.mailbox-batch.sort-cardinality-probe-size")
                                    .intType()
                                    .defaultValue(8));
            BatchedKeyedOperatorAdapter.setCardinalityThreshold(cardinalityThreshold);
            BatchedKeyedOperatorAdapter.setProbeSize(probeSize);

            @SuppressWarnings("unchecked")
            Input<IN> headInput = (Input<IN>) mainOperator;
            return new StreamRecordBatchOutput<>(
                    output,
                    headInput,
                    true,
                    commutativeKeySort,
                    batchSize,
                    timeoutNanos,
                    numRecordsIn);
        } catch (Throwable t) {
            LOG.warn("Failed to configure CacheKit batch output; using direct output", t);
            return output;
        }
    }

    private StreamTaskInput<IN> createTaskInput(CheckpointedInputGate inputGate) {
        int numberOfInputChannels = inputGate.getNumberOfInputChannels();
        StatusWatermarkValve statusWatermarkValve = new StatusWatermarkValve(numberOfInputChannels);

        TypeSerializer<IN> inSerializer =
                configuration.getTypeSerializerIn1(getUserCodeClassLoader());

        return StreamTaskNetworkInputFactory.create(
                inputGate,
                inSerializer,
                getEnvironment().getIOManager(),
                statusWatermarkValve,
                0,
                getEnvironment().getTaskStateManager().getInputRescalingDescriptor(),
                gateIndex ->
                        configuration
                                .getInPhysicalEdges(getUserCodeClassLoader())
                                .get(gateIndex)
                                .getPartitioner(),
                getEnvironment().getTaskInfo());
    }

    /**
     * The network data output implementation used for processing stream elements from {@link
     * StreamTaskNetworkInput} in one input processor.
     */
    private static class StreamTaskNetworkOutput<IN> implements DataOutput<IN> {

        private final Input<IN> operator;

        private final WatermarkGauge watermarkGauge;
        private final Counter numRecordsIn;

        private StreamTaskNetworkOutput(
                Input<IN> operator, WatermarkGauge watermarkGauge, Counter numRecordsIn) {

            this.operator = checkNotNull(operator);
            this.watermarkGauge = checkNotNull(watermarkGauge);
            this.numRecordsIn = checkNotNull(numRecordsIn);
        }

        @Override
        public void emitRecord(StreamRecord<IN> record) throws Exception {
            numRecordsIn.inc();
            operator.setKeyContextElement(record);
            operator.processElement(record);
        }

        @Override
        public void emitWatermark(Watermark watermark) throws Exception {
            watermarkGauge.setCurrentWatermark(watermark.getTimestamp());
            operator.processWatermark(watermark);
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
            operator.processWatermarkStatus(watermarkStatus);
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception {
            operator.processLatencyMarker(latencyMarker);
        }
    }
}
