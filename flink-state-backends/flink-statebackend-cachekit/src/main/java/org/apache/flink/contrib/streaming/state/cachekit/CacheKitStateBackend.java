/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit;

import java.io.IOException;
import java.util.Collection;
import javax.annotation.Nonnull;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PresenceCacheImplementation;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.delegate.DelegatingStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

/**
 * A minimal, extensible {@link StateBackend} wrapper used as the starting point for a production
 * caching framework.
 *
 * <p>Current scope:
 *
 * <ul>
 * <li>Delegates persistence/checkpointing to a delegate {@link StateBackend}.
 *   <li>Adds LRU caching for {@code ValueState} via {@link CacheKitKeyedStateBackend}.
 * </ul>
 */
public class CacheKitStateBackend extends AbstractStateBackend
        implements ConfigurableStateBackend, CheckpointStorage, DelegatingStateBackend {

    private static final long serialVersionUID = 1L;

    private final StateBackend delegateBackend;
    private final int valueCacheMaxEntries;
    private final CachePolicyType valueCachePolicy;
    private final int valueCacheLruOverflow;
    private final boolean valueBypassEnabled;
    private final double valueHitRateThreshold;
    private final int valueHitRateWindow;
    private final int mapPresenceCacheMaxEntries;
    private final CachePolicyType mapPresenceCachePolicy;
    private final int mapPresenceCacheLruOverflow;
    private final PresenceCacheImplementation mapPresenceCacheImplementation;
    private final int mapCacheMaxEntries;
    private final CachePolicyType mapCachePolicy;
    private final int mapCacheLruOverflow;
    private final boolean mapBypassEnabled;
    private final double mapHitRateThreshold;
    private final int mapHitRateWindow;
    private final boolean mapIterationCacheFillEnabled;
    private final int mapSnapshotCacheMaxEntries;
    private final int mapSnapshotSmallMaxEntries;
    private final boolean listStateCowEnabled;
    private final boolean listStateRywEnabled;
    private final int listStateClearedKeysCapacity;
    private final boolean priorityQueueOptEnabled;
    private final boolean diagnosticsEnabled;
    private final NativeRequestPlaneOptions nativeRequestPlaneOptions;
    private final boolean keyScopedPrefetchInvalidationEnabled;
    private final boolean nativePrefetchAccessGuidedStateEnabled;
    private final boolean nativeMapDistinctBatchPrefetchEnabled;
    private final boolean nativeMapDistinctBatchPrefetchDirectArenaEnabled;
    private final boolean nativeMapDistinctBatchPrefetchCrossKeyPipelineEnabled;
    private final boolean nativeMapDistinctBatchPrefetchWorkFirstEnabled;
    private final int nativeMapDistinctBatchPrefetchAsyncMinUniqueKeys;
    private final int nativeMapDistinctBatchPrefetchLookaheadGroups;

    public CacheKitStateBackend(
            StateBackend delegateBackend,
            int valueCacheMaxEntries,
            CachePolicyType valueCachePolicy,
            int valueCacheLruOverflow,
            boolean valueBypassEnabled,
            double valueHitRateThreshold,
            int valueHitRateWindow,
            int mapPresenceCacheMaxEntries,
            CachePolicyType mapPresenceCachePolicy,
            int mapPresenceCacheLruOverflow,
            PresenceCacheImplementation mapPresenceCacheImplementation,
            int mapCacheMaxEntries,
            CachePolicyType mapCachePolicy,
            int mapCacheLruOverflow,
            boolean mapBypassEnabled,
            double mapHitRateThreshold,
            int mapHitRateWindow,
            boolean mapIterationCacheFillEnabled,
            int mapSnapshotCacheMaxEntries,
            boolean listStateCowEnabled,
            boolean listStateRywEnabled,
            int listStateClearedKeysCapacity,
            boolean priorityQueueOptEnabled,
            boolean diagnosticsEnabled) {
        this(
                delegateBackend,
                valueCacheMaxEntries,
                valueCachePolicy,
                valueCacheLruOverflow,
                valueBypassEnabled,
                valueHitRateThreshold,
                valueHitRateWindow,
                mapPresenceCacheMaxEntries,
                mapPresenceCachePolicy,
                mapPresenceCacheLruOverflow,
                mapPresenceCacheImplementation,
                mapCacheMaxEntries,
                mapCachePolicy,
                mapCacheLruOverflow,
                mapBypassEnabled,
                mapHitRateThreshold,
                mapHitRateWindow,
                mapIterationCacheFillEnabled,
                mapSnapshotCacheMaxEntries,
                listStateCowEnabled,
                listStateRywEnabled,
                listStateClearedKeysCapacity,
                priorityQueueOptEnabled,
                diagnosticsEnabled,
                NativeRequestPlaneOptions.disabled());
    }

    public CacheKitStateBackend(
            StateBackend delegateBackend,
            int valueCacheMaxEntries,
            CachePolicyType valueCachePolicy,
            int valueCacheLruOverflow,
            boolean valueBypassEnabled,
            double valueHitRateThreshold,
            int valueHitRateWindow,
            int mapPresenceCacheMaxEntries,
            CachePolicyType mapPresenceCachePolicy,
            int mapPresenceCacheLruOverflow,
            PresenceCacheImplementation mapPresenceCacheImplementation,
            int mapCacheMaxEntries,
            CachePolicyType mapCachePolicy,
            int mapCacheLruOverflow,
            boolean mapBypassEnabled,
            double mapHitRateThreshold,
            int mapHitRateWindow,
            boolean mapIterationCacheFillEnabled,
            int mapSnapshotCacheMaxEntries,
            boolean listStateCowEnabled,
            boolean listStateRywEnabled,
            int listStateClearedKeysCapacity,
            boolean priorityQueueOptEnabled,
            boolean diagnosticsEnabled,
            NativeRequestPlaneOptions nativeRequestPlaneOptions) {
        this(
                delegateBackend,
                valueCacheMaxEntries,
                valueCachePolicy,
                valueCacheLruOverflow,
                valueBypassEnabled,
                valueHitRateThreshold,
                valueHitRateWindow,
                mapPresenceCacheMaxEntries,
                mapPresenceCachePolicy,
                mapPresenceCacheLruOverflow,
                mapPresenceCacheImplementation,
                mapCacheMaxEntries,
                mapCachePolicy,
                mapCacheLruOverflow,
                mapBypassEnabled,
                mapHitRateThreshold,
                mapHitRateWindow,
                mapIterationCacheFillEnabled,
                mapSnapshotCacheMaxEntries,
                1,
                listStateCowEnabled,
                listStateRywEnabled,
                listStateClearedKeysCapacity,
                priorityQueueOptEnabled,
                diagnosticsEnabled,
                nativeRequestPlaneOptions,
                false,
                false,
                false,
                false,
                false,
                false,
                8,
                1);
    }

    public CacheKitStateBackend(
            StateBackend delegateBackend,
            int valueCacheMaxEntries,
            CachePolicyType valueCachePolicy,
            int valueCacheLruOverflow,
            boolean valueBypassEnabled,
            double valueHitRateThreshold,
            int valueHitRateWindow,
            int mapPresenceCacheMaxEntries,
            CachePolicyType mapPresenceCachePolicy,
            int mapPresenceCacheLruOverflow,
            PresenceCacheImplementation mapPresenceCacheImplementation,
            int mapCacheMaxEntries,
            CachePolicyType mapCachePolicy,
            int mapCacheLruOverflow,
            boolean mapBypassEnabled,
            double mapHitRateThreshold,
            int mapHitRateWindow,
            boolean mapIterationCacheFillEnabled,
            int mapSnapshotCacheMaxEntries,
            int mapSnapshotSmallMaxEntries,
            boolean listStateCowEnabled,
            boolean listStateRywEnabled,
            int listStateClearedKeysCapacity,
            boolean priorityQueueOptEnabled,
            boolean diagnosticsEnabled,
            NativeRequestPlaneOptions nativeRequestPlaneOptions) {
        this(
                delegateBackend,
                valueCacheMaxEntries,
                valueCachePolicy,
                valueCacheLruOverflow,
                valueBypassEnabled,
                valueHitRateThreshold,
                valueHitRateWindow,
                mapPresenceCacheMaxEntries,
                mapPresenceCachePolicy,
                mapPresenceCacheLruOverflow,
                mapPresenceCacheImplementation,
                mapCacheMaxEntries,
                mapCachePolicy,
                mapCacheLruOverflow,
                mapBypassEnabled,
                mapHitRateThreshold,
                mapHitRateWindow,
                mapIterationCacheFillEnabled,
                mapSnapshotCacheMaxEntries,
                mapSnapshotSmallMaxEntries,
                listStateCowEnabled,
                listStateRywEnabled,
                listStateClearedKeysCapacity,
                priorityQueueOptEnabled,
                diagnosticsEnabled,
                nativeRequestPlaneOptions,
                false,
                false,
                false,
                false,
                false,
                false,
                8,
                1);
    }

    public CacheKitStateBackend(
            StateBackend delegateBackend,
            int valueCacheMaxEntries,
            CachePolicyType valueCachePolicy,
            int valueCacheLruOverflow,
            boolean valueBypassEnabled,
            double valueHitRateThreshold,
            int valueHitRateWindow,
            int mapPresenceCacheMaxEntries,
            CachePolicyType mapPresenceCachePolicy,
            int mapPresenceCacheLruOverflow,
            PresenceCacheImplementation mapPresenceCacheImplementation,
            int mapCacheMaxEntries,
            CachePolicyType mapCachePolicy,
            int mapCacheLruOverflow,
            boolean mapBypassEnabled,
            double mapHitRateThreshold,
            int mapHitRateWindow,
            boolean mapIterationCacheFillEnabled,
            int mapSnapshotCacheMaxEntries,
            int mapSnapshotSmallMaxEntries,
            boolean listStateCowEnabled,
            boolean listStateRywEnabled,
            int listStateClearedKeysCapacity,
            boolean priorityQueueOptEnabled,
            boolean diagnosticsEnabled,
            NativeRequestPlaneOptions nativeRequestPlaneOptions,
            boolean keyScopedPrefetchInvalidationEnabled,
            boolean nativePrefetchAccessGuidedStateEnabled,
            boolean nativeMapDistinctBatchPrefetchEnabled,
            boolean nativeMapDistinctBatchPrefetchDirectArenaEnabled,
            boolean nativeMapDistinctBatchPrefetchCrossKeyPipelineEnabled,
            boolean nativeMapDistinctBatchPrefetchWorkFirstEnabled,
            int nativeMapDistinctBatchPrefetchAsyncMinUniqueKeys,
            int nativeMapDistinctBatchPrefetchLookaheadGroups) {
        this.delegateBackend = delegateBackend;
        this.valueCacheMaxEntries = valueCacheMaxEntries;
        this.valueCachePolicy = valueCachePolicy;
        this.valueCacheLruOverflow = valueCacheLruOverflow;
        this.valueBypassEnabled = valueBypassEnabled;
        this.valueHitRateThreshold = valueHitRateThreshold;
        this.valueHitRateWindow = valueHitRateWindow;
        this.mapPresenceCacheMaxEntries = mapPresenceCacheMaxEntries;
        this.mapPresenceCachePolicy = mapPresenceCachePolicy;
        this.mapPresenceCacheLruOverflow = mapPresenceCacheLruOverflow;
        this.mapPresenceCacheImplementation = mapPresenceCacheImplementation;
        this.mapCacheMaxEntries = mapCacheMaxEntries;
        this.mapCachePolicy = mapCachePolicy;
        this.mapCacheLruOverflow = mapCacheLruOverflow;
        this.mapBypassEnabled = mapBypassEnabled;
        this.mapHitRateThreshold = mapHitRateThreshold;
        this.mapHitRateWindow = mapHitRateWindow;
        this.mapIterationCacheFillEnabled = mapIterationCacheFillEnabled;
        this.mapSnapshotCacheMaxEntries = mapSnapshotCacheMaxEntries;
        this.mapSnapshotSmallMaxEntries = Math.max(1, Math.min(16, mapSnapshotSmallMaxEntries));
        this.listStateCowEnabled = listStateCowEnabled;
        this.listStateRywEnabled = listStateRywEnabled;
        this.listStateClearedKeysCapacity = listStateClearedKeysCapacity;
        this.priorityQueueOptEnabled = priorityQueueOptEnabled;
        this.diagnosticsEnabled = diagnosticsEnabled;
        this.nativeRequestPlaneOptions =
                java.util.Objects.requireNonNull(
                        nativeRequestPlaneOptions, "nativeRequestPlaneOptions");
        this.keyScopedPrefetchInvalidationEnabled = keyScopedPrefetchInvalidationEnabled;
        this.nativePrefetchAccessGuidedStateEnabled = nativePrefetchAccessGuidedStateEnabled;
        this.nativeMapDistinctBatchPrefetchEnabled = nativeMapDistinctBatchPrefetchEnabled;
        this.nativeMapDistinctBatchPrefetchDirectArenaEnabled =
                nativeMapDistinctBatchPrefetchDirectArenaEnabled;
        this.nativeMapDistinctBatchPrefetchCrossKeyPipelineEnabled =
                nativeMapDistinctBatchPrefetchCrossKeyPipelineEnabled;
        this.nativeMapDistinctBatchPrefetchWorkFirstEnabled =
                nativeMapDistinctBatchPrefetchWorkFirstEnabled;
        this.nativeMapDistinctBatchPrefetchAsyncMinUniqueKeys =
                Math.max(2, nativeMapDistinctBatchPrefetchAsyncMinUniqueKeys);
        this.nativeMapDistinctBatchPrefetchLookaheadGroups =
                normalizeNativeMapDistinctBatchPrefetchLookaheadGroups(
                        nativeMapDistinctBatchPrefetchLookaheadGroups);
    }

    boolean keyScopedPrefetchInvalidationEnabledForTesting() {
        return keyScopedPrefetchInvalidationEnabled;
    }

    boolean nativePrefetchAccessGuidedStateEnabledForTesting() {
        return nativePrefetchAccessGuidedStateEnabled;
    }

    boolean nativeMapDistinctBatchPrefetchEnabledForTesting() {
        return nativeMapDistinctBatchPrefetchEnabled;
    }

    boolean nativeMapDistinctBatchPrefetchDirectArenaEnabledForTesting() {
        return nativeMapDistinctBatchPrefetchDirectArenaEnabled;
    }

    boolean nativeMapDistinctBatchPrefetchWorkFirstEnabledForTesting() {
        return nativeMapDistinctBatchPrefetchWorkFirstEnabled;
    }

    int nativeMapDistinctBatchPrefetchAsyncMinUniqueKeysForTesting() {
        return effectiveNativeMapDistinctBatchPrefetchAsyncMinUniqueKeys();
    }

    int nativeMapDistinctBatchPrefetchLookaheadGroupsForTesting() {
        return effectiveNativeMapDistinctBatchPrefetchLookaheadGroups();
    }

    @Override
    public StateBackend getDelegatedStateBackend() {
        return delegateBackend;
    }

    public int getValueCacheMaxEntries() {
        return valueCacheMaxEntries;
    }

    public CachePolicyType getValueCachePolicy() {
        return valueCachePolicy;
    }

    public int getValueCacheLruOverflow() {
        return valueCacheLruOverflow;
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws IOException {
        AbstractKeyedStateBackend<K> delegated;
        try {
            delegated =
                    (AbstractKeyedStateBackend<K>)
                            delegateBackend.createKeyedStateBackend(
                    env,
                    jobID,
                    operatorIdentifier,
                    keySerializer,
                    numberOfKeyGroups,
                    keyGroupRange,
                    kvStateRegistry,
                    ttlTimeProvider,
                    metricGroup,
                    stateHandles,
                    cancelStreamRegistry);
        } catch (Exception e) {
            throw new IOException("Failed to create delegate keyed state backend", e);
        }

        ExecutionConfig executionConfig = env.getExecutionConfig();
        ClassLoader userCodeClassLoader = env.getUserCodeClassLoader().asClassLoader();

        try {
            return new CacheKitKeyedStateBackend<>(
                    delegated,
                    kvStateRegistry,
                    keySerializer,
                    userCodeClassLoader,
                    executionConfig,
                    ttlTimeProvider,
                    cancelStreamRegistry,
                    metricGroup,
                    valueCacheMaxEntries,
                    valueCachePolicy,
                    valueCacheLruOverflow,
                    valueBypassEnabled,
                    valueHitRateThreshold,
                    valueHitRateWindow,
                    mapPresenceCacheMaxEntries,
                    mapPresenceCachePolicy,
                    mapPresenceCacheLruOverflow,
                    mapPresenceCacheImplementation,
                    mapCacheMaxEntries,
                    mapCachePolicy,
                    mapCacheLruOverflow,
                    mapBypassEnabled,
                    mapHitRateThreshold,
                    mapHitRateWindow,
                    mapIterationCacheFillEnabled,
                    mapSnapshotCacheMaxEntries,
                    mapSnapshotSmallMaxEntries,
                    listStateCowEnabled,
                    listStateRywEnabled,
                    listStateClearedKeysCapacity,
                    priorityQueueOptEnabled,
                    diagnosticsEnabled,
                    effectiveNativeRequestPlaneOptions(),
                    keyScopedPrefetchInvalidationEnabled,
                    nativePrefetchAccessGuidedStateEnabled,
                    nativeMapDistinctBatchPrefetchEnabled,
                    nativeMapDistinctBatchPrefetchDirectArenaEnabled,
                    nativeMapDistinctBatchPrefetchCrossKeyPipelineEnabled,
                    nativeMapDistinctBatchPrefetchWorkFirstEnabled,
                    effectiveNativeMapDistinctBatchPrefetchAsyncMinUniqueKeys(),
                    effectiveNativeMapDistinctBatchPrefetchLookaheadGroups());
        } catch (RuntimeException | LinkageError failure) {
            disposeAfterInitializationFailure(delegated, failure);
            throw new IOException(
                    "Failed to initialize CacheKit keyed state backend. "
                            + "An explicitly enabled native request plane never silently falls back "
                            + "during creation.",
                    failure);
        } catch (Error failure) {
            disposeAfterInitializationFailure(delegated, failure);
            throw failure;
        }
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            Environment env,
            String operatorIdentifier,
            @Nonnull Collection<OperatorStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws Exception {
        return delegateBackend.createOperatorStateBackend(
                env, operatorIdentifier, stateHandles, cancelStreamRegistry);
    }

    @Override
    public boolean useManagedMemory() {
        return delegateBackend.useManagedMemory();
    }

    @Override
    public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer)
            throws IOException {
        if (delegateBackend instanceof CheckpointStorage) {
            return ((CheckpointStorage) delegateBackend).resolveCheckpoint(externalPointer);
        }
        throw new UnsupportedOperationException(
                "Delegate backend does not implement CheckpointStorage: "
                        + delegateBackend.getClass().getName());
    }

    @Override
    public CheckpointStorageAccess createCheckpointStorage(@Nonnull JobID jobId)
            throws IOException {
        if (delegateBackend instanceof CheckpointStorage) {
            return ((CheckpointStorage) delegateBackend).createCheckpointStorage(jobId);
        }
        throw new UnsupportedOperationException(
                "Delegate backend does not implement CheckpointStorage: "
                        + delegateBackend.getClass().getName());
    }

    @Override
    public StateBackend configure(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {
        final StateBackend configuredDelegate =
                delegateBackend instanceof ConfigurableStateBackend
                        ? ((ConfigurableStateBackend) delegateBackend)
                                .configure(config, classLoader)
                : delegateBackend;

        final int maxEntries =
                Math.max(0, config.get(CacheKitStateBackendFactory.VALUE_CACHE_MAX_ENTRIES));
        final CachePolicyType policyType =
                config.get(CacheKitStateBackendFactory.VALUE_CACHE_POLICY);
        final int lruOverflow =
                Math.max(0, config.get(CacheKitStateBackendFactory.VALUE_CACHE_LRU_OVERFLOW));
        final boolean bypassEnabled = config.get(CacheKitStateBackendFactory.VALUE_BYPASS_ENABLED);
        final double hitRateThreshold =
                config.get(CacheKitStateBackendFactory.VALUE_HIT_RATE_THRESHOLD);
        final int hitRateWindow = config.get(CacheKitStateBackendFactory.VALUE_HIT_RATE_WINDOW);
        final int mapPresenceMaxEntries =
                Math.max(0, config.get(CacheKitStateBackendFactory.MAP_PRESENCE_CACHE_MAX_ENTRIES));
        final CachePolicyType mapPresencePolicy =
                config.get(CacheKitStateBackendFactory.MAP_PRESENCE_CACHE_POLICY);
        final int mapPresenceLruOverflow =
                Math.max(
                        0, config.get(CacheKitStateBackendFactory.MAP_PRESENCE_CACHE_LRU_OVERFLOW));
        final PresenceCacheImplementation mapPresenceImpl =
                config.get(CacheKitStateBackendFactory.MAP_PRESENCE_CACHE_IMPLEMENTATION);
        final int mapCacheMaxEntries =
                Math.max(0, config.get(CacheKitStateBackendFactory.MAP_CACHE_MAX_ENTRIES));
        final CachePolicyType mapCachePolicy =
                config.get(CacheKitStateBackendFactory.MAP_CACHE_POLICY);
        final int mapCacheLruOverflow =
                Math.max(0, config.get(CacheKitStateBackendFactory.MAP_CACHE_LRU_OVERFLOW));
        final boolean mapBypassEnabled = config.get(CacheKitStateBackendFactory.MAP_BYPASS_ENABLED);
        final double mapHitRateThreshold =
                config.get(CacheKitStateBackendFactory.MAP_HIT_RATE_THRESHOLD);
        final int mapHitRateWindow = config.get(CacheKitStateBackendFactory.MAP_HIT_RATE_WINDOW);
        final boolean mapIterationCacheFillEnabled =
                config.get(CacheKitStateBackendFactory.MAP_ITERATION_CACHE_FILL_ENABLED);
        final int mapSnapshotMaxEntries =
                Math.max(0, config.get(CacheKitStateBackendFactory.MAP_SNAPSHOT_CACHE_MAX_ENTRIES));
        final int mapSnapshotSmallMaxEntries =
                Math.max(
                        1,
                        Math.min(
                                16,
                                config.get(
                                        CacheKitStateBackendFactory
                                                .MAP_SNAPSHOT_SMALL_MAX_ENTRIES)));
        final boolean listStateCowEnabled =
                config.get(CacheKitStateBackendFactory.LIST_STATE_COW_ENABLED);
        final boolean listStateRywEnabled =
                config.get(CacheKitStateBackendFactory.LIST_STATE_RYW_ENABLED);
        final int clearedKeysCapacity =
                Math.max(
                        1,
                        config.get(CacheKitStateBackendFactory.LIST_STATE_CLEARED_KEYS_CAPACITY));
        final boolean priorityQueueOptEnabled =
                config.get(CacheKitStateBackendFactory.PRIORITY_QUEUE_OPT_ENABLED);
        final boolean diagnosticsEnabled =
                config.get(CacheKitStateBackendFactory.DIAGNOSTICS_ENABLED);
        final NativeRequestPlaneOptions nativeOptions =
                CacheKitStateBackendFactory.nativeRequestPlaneOptions(config);
        final boolean keyScopedPrefetchInvalidationEnabled =
                config.get(CacheKitStateBackendFactory.BP_PREFETCH_KEY_SCOPED_INVALIDATION_ENABLED);
        final boolean nativePrefetchAccessGuidedStateEnabled =
                config.get(CacheKitStateBackendFactory.NATIVE_PREFETCH_ACCESS_GUIDED_STATE_ENABLED);
        final boolean nativeMapDistinctBatchPrefetchEnabled =
                config.get(CacheKitStateBackendFactory.NATIVE_MAP_DISTINCT_BATCH_PREFETCH_ENABLED);
        final boolean nativeMapDistinctBatchPrefetchDirectArenaEnabled =
                config.get(
                        CacheKitStateBackendFactory
                                .NATIVE_MAP_DISTINCT_BATCH_PREFETCH_DIRECT_ARENA_ENABLED);
        final boolean nativeMapDistinctBatchPrefetchCrossKeyPipelineEnabled =
                config.get(
                        CacheKitStateBackendFactory
                                .NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_ENABLED)
                        && config.get(CacheKitStateBackendFactory.DISTINCT_BATCH_OVERLAY_ENABLED);
        final boolean nativeMapDistinctBatchPrefetchWorkFirstEnabled =
                nativeMapDistinctBatchPrefetchEnabled
                        && nativeMapDistinctBatchPrefetchDirectArenaEnabled
                        && nativeMapDistinctBatchPrefetchCrossKeyPipelineEnabled
                        && config.get(
                                CacheKitStateBackendFactory
                                        .NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_WORK_FIRST_ENABLED);
        final int nativeMapDistinctBatchPrefetchAsyncMinUniqueKeys =
                Math.max(
                        2,
                        config.get(
                                CacheKitStateBackendFactory
                                        .NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_ASYNC_MIN_UNIQUE_KEYS));
        final int nativeMapDistinctBatchPrefetchLookaheadGroups =
                normalizeNativeMapDistinctBatchPrefetchLookaheadGroups(
                        config.get(
                                CacheKitStateBackendFactory
                                        .NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_LOOKAHEAD_GROUPS));

        return new CacheKitStateBackend(
                configuredDelegate,
                maxEntries,
                policyType,
                lruOverflow,
                bypassEnabled,
                hitRateThreshold,
                hitRateWindow,
                mapPresenceMaxEntries,
                mapPresencePolicy,
                mapPresenceLruOverflow,
                mapPresenceImpl,
                mapCacheMaxEntries,
                mapCachePolicy,
                mapCacheLruOverflow,
                mapBypassEnabled,
                mapHitRateThreshold,
                mapHitRateWindow,
                mapIterationCacheFillEnabled,
                mapSnapshotMaxEntries,
                mapSnapshotSmallMaxEntries,
                listStateCowEnabled,
                listStateRywEnabled,
                clearedKeysCapacity,
                priorityQueueOptEnabled,
                diagnosticsEnabled,
                nativeOptions,
                keyScopedPrefetchInvalidationEnabled,
                nativePrefetchAccessGuidedStateEnabled,
                nativeMapDistinctBatchPrefetchEnabled,
                nativeMapDistinctBatchPrefetchDirectArenaEnabled,
                nativeMapDistinctBatchPrefetchCrossKeyPipelineEnabled,
                nativeMapDistinctBatchPrefetchWorkFirstEnabled,
                nativeMapDistinctBatchPrefetchAsyncMinUniqueKeys,
                nativeMapDistinctBatchPrefetchLookaheadGroups);
    }

    private NativeRequestPlaneOptions effectiveNativeRequestPlaneOptions() {
        // This field did not exist in older serialized CacheKitStateBackend instances. Preserve
        // serialVersionUID compatibility by treating a deserialized null as the historical
        // disabled behavior.
        return normalizeNativeRequestPlaneOptions(nativeRequestPlaneOptions);
    }

    private int effectiveNativeMapDistinctBatchPrefetchAsyncMinUniqueKeys() {
        // This field did not exist in older serialized CacheKitStateBackend instances. A missing
        // field is restored as zero by Java serialization, so preserve the new default rather than
        // accidentally changing old jobs to the minimum legal threshold of two.
        return normalizeNativeMapDistinctBatchPrefetchAsyncMinUniqueKeys(
                nativeMapDistinctBatchPrefetchAsyncMinUniqueKeys);
    }

    private int effectiveNativeMapDistinctBatchPrefetchLookaheadGroups() {
        return normalizeNativeMapDistinctBatchPrefetchLookaheadGroups(
                nativeMapDistinctBatchPrefetchLookaheadGroups);
    }

    static int normalizeNativeMapDistinctBatchPrefetchAsyncMinUniqueKeys(int configuredValue) {
        return configuredValue <= 0 ? 8 : Math.max(2, configuredValue);
    }

    static int normalizeNativeMapDistinctBatchPrefetchLookaheadGroups(int configuredValue) {
        return configuredValue <= 0 ? 1 : Math.min(8, configuredValue);
    }

    static NativeRequestPlaneOptions normalizeNativeRequestPlaneOptions(
            NativeRequestPlaneOptions options) {
        return options == null ? NativeRequestPlaneOptions.disabled() : options;
    }

    private static void disposeAfterInitializationFailure(
            AbstractKeyedStateBackend<?> delegated, Throwable failure) {
        try {
            delegated.dispose();
        } catch (Throwable cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }
}
