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

package org.apache.flink.contrib.streaming.state;

import java.io.IOException;
import java.util.Collection;
import javax.annotation.Nonnull;
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
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;



/**
 * A state backend that wraps another state backend (e.g., RocksDBStateBackend) to provide an L1/L2
 * caching layer for deserialized objects.
 */
public class CachingStateBackend extends AbstractStateBackend
        implements ConfigurableStateBackend, CheckpointStorage {

    private static final long serialVersionUID = 1L;

    private final StateBackend delegateBackend;
    private final long l1CacheSize;
    private final long l2CacheSize;
    private final long maxActiveNamespaces;
    private final long maxCacheMemoryMb;
    private final CachingStateBackendFactory.CachePolicyType cachePolicyType;
    private final long mapL1KeyPresenceCacheSize;
    private final long mapL2KeyPresenceCacheSize;

    private final double mapCacheHitRateThreshold;
    private final long mapCacheHitRateWindowSize;
    private final long mapCacheMinAccessesForBypassCheck;
    private final boolean mapKeyPresenceCacheEnabled;
    private final boolean mapBypassEnabled;

    public CachingStateBackend(
            StateBackend delegateBackend,
            long l1CacheSize,
            long l2CacheSize,
            long maxActiveNamespaces,
            long maxCacheMemoryMb, CachingStateBackendFactory.CachePolicyType cachePolicyType,
            long mapL1KeyPresenceCacheSize, long mapL2KeyPresenceCacheSize,
            double mapCacheHitRateThreshold, long mapCacheHitRateWindowSize, long mapCacheMinAccessesForBypassCheck,
            boolean mapKeyPresenceCacheEnabled,
            boolean mapBypassEnabled) {
        this.delegateBackend = delegateBackend;
        this.l1CacheSize = l1CacheSize;
        this.l2CacheSize = l2CacheSize;
        this.maxActiveNamespaces = maxActiveNamespaces;
        this.maxCacheMemoryMb = maxCacheMemoryMb;
        this.cachePolicyType = cachePolicyType;
        this.mapL1KeyPresenceCacheSize = mapL1KeyPresenceCacheSize;
        this.mapL2KeyPresenceCacheSize = mapL2KeyPresenceCacheSize;
        this.mapCacheHitRateThreshold = mapCacheHitRateThreshold;
        this.mapCacheHitRateWindowSize = mapCacheHitRateWindowSize;
        this.mapCacheMinAccessesForBypassCheck = mapCacheMinAccessesForBypassCheck;
        this.mapKeyPresenceCacheEnabled = mapKeyPresenceCacheEnabled;
        this.mapBypassEnabled = mapBypassEnabled;

        if (!(delegateBackend instanceof AbstractStateBackend)) {
            System.err.println(
                    "Warning: CachingStateBackend delegate is not an AbstractStateBackend. Some features like checkpoint resolution might fail if not overridden by the specific StateBackend implementation.");
        }
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

        AbstractKeyedStateBackend<K> delegateKeyedStateBackend;
        try {
            delegateKeyedStateBackend =
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

        return new CachingKeyedStateBackend<K>(
                kvStateRegistry,
                keySerializer,
                env.getUserCodeClassLoader().asClassLoader(),
                env.getExecutionConfig(),
                ttlTimeProvider,
                metricGroup,
                stateHandles,
                cancelStreamRegistry,
                delegateKeyedStateBackend,
                (int) l1CacheSize,
                (int) l2CacheSize,
                (int) maxActiveNamespaces,
                this.maxCacheMemoryMb, this.cachePolicyType,
                (int) this.mapL1KeyPresenceCacheSize, (int) this.mapL2KeyPresenceCacheSize,
                this.mapCacheHitRateThreshold, this.mapCacheHitRateWindowSize, this.mapCacheMinAccessesForBypassCheck,
                this.mapKeyPresenceCacheEnabled,
                this.mapBypassEnabled);
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
        } else {
            throw new UnsupportedOperationException(
                    "Delegate backend of type "
                            + delegateBackend.getClass().getName()
                            + " does not support resolveCheckpoint directly and is not an instance of AbstractStateBackend that formerly provided this.");
        }
    }

    @Override
    public CheckpointStorageAccess createCheckpointStorage(@Nonnull JobID jobId)
            throws IOException {
        if (delegateBackend instanceof CheckpointStorage) {
            return ((CheckpointStorage) delegateBackend).createCheckpointStorage(jobId);
        } else {
            throw new UnsupportedOperationException(
                    "Delegate backend of type "
                            + delegateBackend.getClass().getName()
                            + " does not support createCheckpointStorage directly and is not an instance of AbstractStateBackend that formerly provided this.");
        }
    }

    // Getter methods for cache configuration, useful for testing
    public long getL1CacheSize() {
        return l1CacheSize;
    }

    public long getL2CacheSize() {
        return l2CacheSize;
    }

    public long getMaxActiveNamespaces() {
        return maxActiveNamespaces;
    }

    public long getMaxCacheMemoryMb() {
        return maxCacheMemoryMb;
    }

    public CachingStateBackendFactory.CachePolicyType getCachePolicyType() {
        return cachePolicyType;
    }

    public long getMapL1KeyPresenceCacheSize() {
        return mapL1KeyPresenceCacheSize;
    }

    public long getMapL2KeyPresenceCacheSize() {
        return mapL2KeyPresenceCacheSize;
    }

    public double getMapCacheHitRateThreshold() {
        return mapCacheHitRateThreshold;
    }

    public long getMapCacheHitRateWindowSize() {
        return mapCacheHitRateWindowSize;
    }

    public long getMapCacheMinAccessesForBypassCheck() {
        return mapCacheMinAccessesForBypassCheck;
    }

    public boolean isMapKeyPresenceCacheEnabled() {
        return mapKeyPresenceCacheEnabled;
    }

    public boolean isMapBypassEnabled() {
        return mapBypassEnabled;
    }

    @Override
    public StateBackend configure(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {
        return this;
    }
}
