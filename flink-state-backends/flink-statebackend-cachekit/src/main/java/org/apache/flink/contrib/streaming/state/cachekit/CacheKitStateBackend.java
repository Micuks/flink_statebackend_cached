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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
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
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;

/**
 * A minimal, extensible {@link StateBackend} wrapper used as the starting point
 * for a production
 * caching framework.
 *
 * <p>
 * Current scope:
 *
 * <ul>
 * <li>Delegates persistence/checkpointing to a delegate {@link StateBackend}.
 * <li>Adds LRU caching for {@code ValueState} via
 * {@link CacheKitKeyedStateBackend}.
 * </ul>
 */
public class CacheKitStateBackend extends AbstractStateBackend
        implements ConfigurableStateBackend, CheckpointStorage, DelegatingStateBackend {

    private static final long serialVersionUID = 1L;

    private final StateBackend delegateBackend;
    private final int valueCacheMaxEntries;
    private final CachePolicyType valueCachePolicy;
    private final int valueCacheLruOverflow;
    private final double valueCacheL1Ratio;
    private final boolean valueBypassEnabled;
    private final long valueBypassMinAccesses;
    private final int valueBypassSampleEvery;
    private final double valueBypassHysteresis;
    private final int valueBypassCooldownWindows;
    private final double valueHitRateThreshold;
    private final int valueHitRateWindow;

    public CacheKitStateBackend(
            StateBackend delegateBackend,
            int valueCacheMaxEntries,
            CachePolicyType valueCachePolicy,
            int valueCacheLruOverflow,
            double valueCacheL1Ratio,
            boolean valueBypassEnabled,
            long valueBypassMinAccesses,
            int valueBypassSampleEvery,
            double valueBypassHysteresis,
            int valueBypassCooldownWindows,
            double valueHitRateThreshold,
            int valueHitRateWindow) {
        this.delegateBackend = delegateBackend;
        this.valueCacheMaxEntries = valueCacheMaxEntries;
        this.valueCachePolicy = valueCachePolicy;
        this.valueCacheLruOverflow = valueCacheLruOverflow;
        this.valueCacheL1Ratio = valueCacheL1Ratio;
        this.valueBypassEnabled = valueBypassEnabled;
        this.valueBypassMinAccesses = valueBypassMinAccesses;
        this.valueBypassSampleEvery = valueBypassSampleEvery;
        this.valueBypassHysteresis = valueBypassHysteresis;
        this.valueBypassCooldownWindows = valueBypassCooldownWindows;
        this.valueHitRateThreshold = valueHitRateThreshold;
        this.valueHitRateWindow = valueHitRateWindow;
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
            delegated = (AbstractKeyedStateBackend<K>) delegateBackend.createKeyedStateBackend(
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

        return new CacheKitKeyedStateBackend<>(
                delegated,
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                cancelStreamRegistry,
                valueCacheMaxEntries,
                valueCachePolicy,
                valueCacheLruOverflow,
                valueCacheL1Ratio,
                valueBypassEnabled,
                valueBypassMinAccesses,
                valueBypassSampleEvery,
                valueBypassHysteresis,
                valueBypassCooldownWindows,
                valueHitRateThreshold,
                valueHitRateWindow);
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
    public CheckpointStorageAccess createCheckpointStorage(@Nonnull JobID jobId) throws IOException {
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
        final StateBackend configuredDelegate = delegateBackend instanceof ConfigurableStateBackend
                ? ((ConfigurableStateBackend) delegateBackend).configure(config, classLoader)
                : delegateBackend;

        final int maxEntries = Math.max(0, config.get(CacheKitStateBackendFactory.VALUE_CACHE_MAX_ENTRIES));
        final CachePolicyType policyType = config.get(CacheKitStateBackendFactory.VALUE_CACHE_POLICY);
        final int lruOverflow = Math.max(0, config.get(CacheKitStateBackendFactory.VALUE_CACHE_LRU_OVERFLOW));
        final double l1Ratio =
                Math.max(0.0, Math.min(1.0, config.get(CacheKitStateBackendFactory.VALUE_CACHE_L1_RATIO)));
        final boolean bypassEnabled = config.get(CacheKitStateBackendFactory.VALUE_BYPASS_ENABLED);
        final long bypassMinAccesses = Math.max(
                0L, config.get(CacheKitStateBackendFactory.VALUE_BYPASS_MIN_ACCESSES));
        final int bypassSampleEvery = Math.max(
                1, config.get(CacheKitStateBackendFactory.VALUE_BYPASS_SAMPLE_EVERY));
        final double bypassHysteresis = Math.max(
                0.0, Math.min(1.0, config.get(CacheKitStateBackendFactory.VALUE_BYPASS_HYSTERESIS)));
        final int bypassCooldownWindows = Math.max(
                0, config.get(CacheKitStateBackendFactory.VALUE_BYPASS_COOLDOWN_WINDOWS));
        final double hitRateThreshold = config.get(CacheKitStateBackendFactory.VALUE_HIT_RATE_THRESHOLD);
        final int hitRateWindow = Math.max(1, config.get(CacheKitStateBackendFactory.VALUE_HIT_RATE_WINDOW));

        return new CacheKitStateBackend(
                configuredDelegate,
                maxEntries,
                policyType,
                lruOverflow,
                l1Ratio,
                bypassEnabled,
                bypassMinAccesses,
                bypassSampleEvery,
                bypassHysteresis,
                bypassCooldownWindows,
                hitRateThreshold,
                hitRateWindow);
    }
}
