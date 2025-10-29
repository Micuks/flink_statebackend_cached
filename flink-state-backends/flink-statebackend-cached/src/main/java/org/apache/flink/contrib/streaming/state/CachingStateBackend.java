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
import org.apache.flink.configuration.Configuration;
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
    // Per-state overrides (fall back to global when not set)
    private final long valueMaxActiveNamespaces;
    private final long mapMaxActiveNamespaces;
    private final long listMaxActiveNamespaces;
    private final long aggregatingMaxActiveNamespaces;
    private final long maxCacheMemoryMb;
    // Global and per-state cache policy types
    private final CachingStateBackendFactory.CachePolicyType globalCachePolicyType;
    private final CachingStateBackendFactory.CachePolicyType mapCachePolicyType;
    private final CachingStateBackendFactory.CachePolicyType valueCachePolicyType;
    private final CachingStateBackendFactory.CachePolicyType listCachePolicyType;
    private final CachingStateBackendFactory.CachePolicyType aggregatingCachePolicyType;
    private final long mapL1KeyPresenceCacheSize;
    private final long mapL2KeyPresenceCacheSize;

    private final double mapCacheHitRateThreshold;
    private final long mapCacheHitRateWindowSize;
    private final long mapCacheMinAccessesForBypassCheck;
    private final boolean mapKeyPresenceCacheEnabled;
    private final boolean mapBypassEnabled;
    private final CachingStateBackendFactory.PresenceCacheImplementation mapPresenceCacheImpl;
    private final boolean l2ManagedMemoryEnabled;
    // Configuration to pass down to keyed backend for per-state toggles
    private final org.apache.flink.configuration.Configuration taskConfiguration;

    public CachingStateBackend(
            StateBackend delegateBackend,
            long l1CacheSize,
            long l2CacheSize,
            long maxActiveNamespaces,
            long maxCacheMemoryMb, CachingStateBackendFactory.CachePolicyType cachePolicyType,
            long mapL1KeyPresenceCacheSize, long mapL2KeyPresenceCacheSize,
            double mapCacheHitRateThreshold, long mapCacheHitRateWindowSize, long mapCacheMinAccessesForBypassCheck,
            boolean mapKeyPresenceCacheEnabled,
            boolean mapBypassEnabled,
            CachingStateBackendFactory.PresenceCacheImplementation mapPresenceCacheImpl,
            boolean l2ManagedMemoryEnabled) {
        this.delegateBackend = delegateBackend;
        this.l1CacheSize = l1CacheSize;
        this.l2CacheSize = l2CacheSize;
        this.maxActiveNamespaces = maxActiveNamespaces;
        // default per-state to the global value in this constructor variant
        this.valueMaxActiveNamespaces = maxActiveNamespaces;
        this.mapMaxActiveNamespaces = maxActiveNamespaces;
        this.listMaxActiveNamespaces = maxActiveNamespaces;
        this.aggregatingMaxActiveNamespaces = maxActiveNamespaces;
        this.maxCacheMemoryMb = maxCacheMemoryMb;
        // When constructed directly, apply the same policy to all states
        this.globalCachePolicyType = cachePolicyType;
        this.mapCachePolicyType = cachePolicyType;
        this.valueCachePolicyType = cachePolicyType;
        this.listCachePolicyType = cachePolicyType;
        this.aggregatingCachePolicyType = cachePolicyType;
        this.mapL1KeyPresenceCacheSize = mapL1KeyPresenceCacheSize;
        this.mapL2KeyPresenceCacheSize = mapL2KeyPresenceCacheSize;
        this.mapCacheHitRateThreshold = mapCacheHitRateThreshold;
        this.mapCacheHitRateWindowSize = mapCacheHitRateWindowSize;
        this.mapCacheMinAccessesForBypassCheck = mapCacheMinAccessesForBypassCheck;
        this.mapKeyPresenceCacheEnabled = mapKeyPresenceCacheEnabled;
        this.mapBypassEnabled = mapBypassEnabled;
        this.mapPresenceCacheImpl = mapPresenceCacheImpl;
        // Disable managed L2 when L2 capacity is zero to avoid unnecessary off-heap setup
        this.l2ManagedMemoryEnabled = l2ManagedMemoryEnabled && l2CacheSize > 0;

        // Build a minimal task configuration reflecting relevant options
        org.apache.flink.configuration.Configuration cfg = new org.apache.flink.configuration.Configuration();
        cfg.set(CachingStateBackendFactory.MAP_CACHE_ENABLED_CONFIG, true);
        cfg.set(CachingStateBackendFactory.MAP_BYPASS_ENABLED_CONFIG, this.mapBypassEnabled);
        cfg.set(CachingStateBackendFactory.MAP_KEY_PRESENCE_CACHE_ENABLED_CONFIG, this.mapKeyPresenceCacheEnabled);
        cfg.set(CachingStateBackendFactory.MAP_CACHE_HIT_RATE_THRESHOLD_CONFIG, this.mapCacheHitRateThreshold);
        cfg.set(CachingStateBackendFactory.MAP_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG, this.mapCacheHitRateWindowSize);
        cfg.set(CachingStateBackendFactory.MAP_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG, this.mapCacheMinAccessesForBypassCheck);
        cfg.set(CachingStateBackendFactory.MAP_PRESENCE_CACHE_IMPL, this.mapPresenceCacheImpl);
        cfg.set(CachingStateBackendFactory.L2_MANAGED_MEMORY_ENABLED_CONFIG, this.l2ManagedMemoryEnabled);
        // Lightweight wrapper profiling defaults (disabled unless explicitly enabled via configure())
        cfg.set(CachingStateBackendFactory.PROFILE_ENABLED_CONFIG, false);
        cfg.set(CachingStateBackendFactory.PROFILE_SAMPLE_RATE_CONFIG, 1024);
        cfg.set(CachingStateBackendFactory.AUTO_LEFT_BYPASS_ENABLED_CONFIG, true);
        // Defaults for ValueState: enabled with bypass off unless overridden by configure(path)
        cfg.set(CachingStateBackendFactory.VALUE_CACHE_ENABLED_CONFIG, true);
        cfg.set(CachingStateBackendFactory.VALUE_BYPASS_ENABLED_CONFIG, true);
        cfg.set(CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_THRESHOLD_CONFIG, 0.0);
        cfg.set(CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG, 1000L);
        cfg.set(CachingStateBackendFactory.VALUE_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG, 100L);
        cfg.set(CachingStateBackendFactory.WRITE_BEHIND_ENABLED_CONFIG, false);
        this.taskConfiguration = cfg;

        if (!(delegateBackend instanceof AbstractStateBackend)) {
            System.err.println(
                    "Warning: CachingStateBackend delegate is not an AbstractStateBackend. Some features like checkpoint resolution might fail if not overridden by the specific StateBackend implementation.");
        }
    }

    public CachingStateBackend(StateBackend delegateBackend, ReadableConfig config) {
        this.delegateBackend = delegateBackend;
        this.l1CacheSize = config.get(CachingStateBackendFactory.L1_CACHE_SIZE_CONFIG);
        this.l2CacheSize = config.get(CachingStateBackendFactory.L2_CACHE_SIZE_CONFIG);
        this.maxActiveNamespaces = config.get(CachingStateBackendFactory.MAX_ACTIVE_NAMESPACES_CONFIG);
        this.maxCacheMemoryMb = config.get(CachingStateBackendFactory.MAX_CACHE_MEMORY_MB_CONFIG);
        // Read global policy
        this.globalCachePolicyType = config.get(CachingStateBackendFactory.CACHE_POLICY_CONFIG);
        this.mapL1KeyPresenceCacheSize = config.get(CachingStateBackendFactory.MAP_L1_KEY_PRESENCE_CACHE_SIZE_CONFIG);
        this.mapL2KeyPresenceCacheSize = config.get(CachingStateBackendFactory.MAP_L2_KEY_PRESENCE_CACHE_SIZE_CONFIG);
        this.mapCacheHitRateThreshold = config.get(CachingStateBackendFactory.MAP_CACHE_HIT_RATE_THRESHOLD_CONFIG);
        this.mapCacheHitRateWindowSize = config.get(CachingStateBackendFactory.MAP_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG);
        this.mapCacheMinAccessesForBypassCheck = config.get(CachingStateBackendFactory.MAP_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG);
        this.mapKeyPresenceCacheEnabled = config.get(CachingStateBackendFactory.MAP_KEY_PRESENCE_CACHE_ENABLED_CONFIG);
        this.mapBypassEnabled = config.get(CachingStateBackendFactory.MAP_BYPASS_ENABLED_CONFIG);
        this.mapPresenceCacheImpl = config.get(CachingStateBackendFactory.MAP_PRESENCE_CACHE_IMPL);
        // Disable managed L2 when L2 capacity is zero to avoid unnecessary off-heap setup
        boolean cfgL2Managed = config.get(CachingStateBackendFactory.L2_MANAGED_MEMORY_ENABLED_CONFIG);
        this.l2ManagedMemoryEnabled = cfgL2Managed && this.l2CacheSize > 0;
        // Resolve per-state policies with fallback to the global policy when option is absent
        CachingStateBackendFactory.CachePolicyType tmpMapPolicy = this.globalCachePolicyType;
        CachingStateBackendFactory.CachePolicyType tmpValuePolicy = this.globalCachePolicyType;
        CachingStateBackendFactory.CachePolicyType tmpListPolicy = this.globalCachePolicyType;
        CachingStateBackendFactory.CachePolicyType tmpAggPolicy = this.globalCachePolicyType;
        if (config instanceof Configuration) {
            Configuration conf = (Configuration) config;
            if (conf.contains(CachingStateBackendFactory.MAP_CACHE_POLICY_CONFIG)) {
                tmpMapPolicy = conf.get(CachingStateBackendFactory.MAP_CACHE_POLICY_CONFIG);
            }
            if (conf.contains(CachingStateBackendFactory.VALUE_CACHE_POLICY_CONFIG)) {
                tmpValuePolicy = conf.get(CachingStateBackendFactory.VALUE_CACHE_POLICY_CONFIG);
            }
            if (conf.contains(CachingStateBackendFactory.LIST_CACHE_POLICY_CONFIG)) {
                tmpListPolicy = conf.get(CachingStateBackendFactory.LIST_CACHE_POLICY_CONFIG);
            }
            if (conf.contains(CachingStateBackendFactory.AGGREGATING_CACHE_POLICY_CONFIG)) {
                tmpAggPolicy = conf.get(CachingStateBackendFactory.AGGREGATING_CACHE_POLICY_CONFIG);
            }
        } else {
            // Best-effort fallback when we cannot check presence: use per-state value directly
            // which will be equal to its default if not set. This may not reflect global override.
            tmpMapPolicy = config.get(CachingStateBackendFactory.MAP_CACHE_POLICY_CONFIG);
            tmpValuePolicy = config.get(CachingStateBackendFactory.VALUE_CACHE_POLICY_CONFIG);
            tmpListPolicy = config.get(CachingStateBackendFactory.LIST_CACHE_POLICY_CONFIG);
            tmpAggPolicy = config.get(CachingStateBackendFactory.AGGREGATING_CACHE_POLICY_CONFIG);
        }
        this.mapCachePolicyType = tmpMapPolicy;
        this.valueCachePolicyType = tmpValuePolicy;
        this.listCachePolicyType = tmpListPolicy;
        this.aggregatingCachePolicyType = tmpAggPolicy;

        // Resolve per-state max active namespaces with fallback to the global value
        long tmpValueMaxNs = this.maxActiveNamespaces;
        long tmpMapMaxNs = this.maxActiveNamespaces;
        long tmpListMaxNs = this.maxActiveNamespaces;
        long tmpAggMaxNs = this.maxActiveNamespaces;
        if (config instanceof Configuration) {
            Configuration conf = (Configuration) config;
            if (conf.contains(CachingStateBackendFactory.VALUE_MAX_ACTIVE_NAMESPACES_CONFIG)) {
                tmpValueMaxNs = conf.get(CachingStateBackendFactory.VALUE_MAX_ACTIVE_NAMESPACES_CONFIG);
            }
            if (conf.contains(CachingStateBackendFactory.MAP_MAX_ACTIVE_NAMESPACES_CONFIG)) {
                tmpMapMaxNs = conf.get(CachingStateBackendFactory.MAP_MAX_ACTIVE_NAMESPACES_CONFIG);
            }
            if (conf.contains(CachingStateBackendFactory.LIST_MAX_ACTIVE_NAMESPACES_CONFIG)) {
                tmpListMaxNs = conf.get(CachingStateBackendFactory.LIST_MAX_ACTIVE_NAMESPACES_CONFIG);
            }
            if (conf.contains(CachingStateBackendFactory.AGGREGATING_MAX_ACTIVE_NAMESPACES_CONFIG)) {
                tmpAggMaxNs = conf.get(CachingStateBackendFactory.AGGREGATING_MAX_ACTIVE_NAMESPACES_CONFIG);
            }
        } else {
            // Best-effort when presence cannot be checked
            try { tmpValueMaxNs = config.get(CachingStateBackendFactory.VALUE_MAX_ACTIVE_NAMESPACES_CONFIG); } catch (Throwable ignore) {}
            try { tmpMapMaxNs = config.get(CachingStateBackendFactory.MAP_MAX_ACTIVE_NAMESPACES_CONFIG); } catch (Throwable ignore) {}
            try { tmpListMaxNs = config.get(CachingStateBackendFactory.LIST_MAX_ACTIVE_NAMESPACES_CONFIG); } catch (Throwable ignore) {}
            try { tmpAggMaxNs = config.get(CachingStateBackendFactory.AGGREGATING_MAX_ACTIVE_NAMESPACES_CONFIG); } catch (Throwable ignore) {}
            if (tmpValueMaxNs == 0) tmpValueMaxNs = this.maxActiveNamespaces;
            if (tmpMapMaxNs == 0) tmpMapMaxNs = this.maxActiveNamespaces;
            if (tmpListMaxNs == 0) tmpListMaxNs = this.maxActiveNamespaces;
            if (tmpAggMaxNs == 0) tmpAggMaxNs = this.maxActiveNamespaces;
        }
        this.valueMaxActiveNamespaces = (int) Math.max(0, tmpValueMaxNs);
        this.mapMaxActiveNamespaces = (int) Math.max(0, tmpMapMaxNs);
        this.listMaxActiveNamespaces = (int) Math.max(0, tmpListMaxNs);
        this.aggregatingMaxActiveNamespaces = (int) Math.max(0, tmpAggMaxNs);

        boolean autoLeftBypassEnabled =
                config.get(CachingStateBackendFactory.AUTO_LEFT_BYPASS_ENABLED_CONFIG);

        // Preserve full configuration relevant to keyed backend
        org.apache.flink.configuration.Configuration cfg = new org.apache.flink.configuration.Configuration();
        cfg.set(CachingStateBackendFactory.MAP_CACHE_ENABLED_CONFIG, config.get(CachingStateBackendFactory.MAP_CACHE_ENABLED_CONFIG));
        cfg.set(CachingStateBackendFactory.MAP_BYPASS_ENABLED_CONFIG, this.mapBypassEnabled);
        cfg.set(CachingStateBackendFactory.MAP_KEY_PRESENCE_CACHE_ENABLED_CONFIG, this.mapKeyPresenceCacheEnabled);
        cfg.set(CachingStateBackendFactory.MAP_CACHE_HIT_RATE_THRESHOLD_CONFIG, this.mapCacheHitRateThreshold);
        cfg.set(CachingStateBackendFactory.MAP_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG, this.mapCacheHitRateWindowSize);
        cfg.set(CachingStateBackendFactory.MAP_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG, this.mapCacheMinAccessesForBypassCheck);
        cfg.set(CachingStateBackendFactory.MAP_PRESENCE_CACHE_IMPL, this.mapPresenceCacheImpl);
        cfg.set(CachingStateBackendFactory.L2_MANAGED_MEMORY_ENABLED_CONFIG, this.l2ManagedMemoryEnabled);
        cfg.set(CachingStateBackendFactory.MAP_FORCE_BYPASS_STATES_REGEX,
                config.get(CachingStateBackendFactory.MAP_FORCE_BYPASS_STATES_REGEX));
        cfg.set(CachingStateBackendFactory.AUTO_LEFT_BYPASS_ENABLED_CONFIG, autoLeftBypassEnabled);
        // Lightweight wrapper profiling options (propagate exactly as configured)
        try {
            cfg.set(CachingStateBackendFactory.PROFILE_ENABLED_CONFIG, config.get(CachingStateBackendFactory.PROFILE_ENABLED_CONFIG));
            cfg.set(CachingStateBackendFactory.PROFILE_SAMPLE_RATE_CONFIG, Math.max(1, config.get(CachingStateBackendFactory.PROFILE_SAMPLE_RATE_CONFIG)));
        } catch (Throwable t) {
            // Keep safe defaults if not present
        }
        // Expose resolved per-state policies to the keyed backend (for completeness/testing)
        cfg.set(CachingStateBackendFactory.CACHE_POLICY_CONFIG, this.globalCachePolicyType);
        cfg.set(CachingStateBackendFactory.MAP_CACHE_POLICY_CONFIG, this.mapCachePolicyType);
        cfg.set(CachingStateBackendFactory.VALUE_CACHE_POLICY_CONFIG, this.valueCachePolicyType);
        cfg.set(CachingStateBackendFactory.LIST_CACHE_POLICY_CONFIG, this.listCachePolicyType);
        cfg.set(CachingStateBackendFactory.AGGREGATING_CACHE_POLICY_CONFIG, this.aggregatingCachePolicyType);
        // Package-level logging controls
        try {
            cfg.set(CachingStateBackendFactory.LOGGING_ENABLED_CONFIG, config.get(CachingStateBackendFactory.LOGGING_ENABLED_CONFIG));
            String lvl = config.get(CachingStateBackendFactory.LOGGING_LEVEL_CONFIG);
            if (lvl != null) { cfg.set(CachingStateBackendFactory.LOGGING_LEVEL_CONFIG, lvl); }
        } catch (Throwable t) {
            // ignore, keep defaults
        }

        // ValueState toggles
        cfg.set(CachingStateBackendFactory.VALUE_CACHE_ENABLED_CONFIG, config.get(CachingStateBackendFactory.VALUE_CACHE_ENABLED_CONFIG));
        cfg.set(CachingStateBackendFactory.VALUE_BYPASS_ENABLED_CONFIG, config.get(CachingStateBackendFactory.VALUE_BYPASS_ENABLED_CONFIG));
        cfg.set(CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_THRESHOLD_CONFIG, config.get(CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_THRESHOLD_CONFIG));
        cfg.set(CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG, config.get(CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG));
        cfg.set(CachingStateBackendFactory.VALUE_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG, config.get(CachingStateBackendFactory.VALUE_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG));
        cfg.set(CachingStateBackendFactory.WRITE_BEHIND_ENABLED_CONFIG, config.get(CachingStateBackendFactory.WRITE_BEHIND_ENABLED_CONFIG));
        // Force-bypass regex (if any)
        cfg.set(CachingStateBackendFactory.MAP_FORCE_BYPASS_STATES_REGEX,
                config.get(CachingStateBackendFactory.MAP_FORCE_BYPASS_STATES_REGEX));
        // Advanced/kill-switch option left default true unless present elsewhere
        this.taskConfiguration = cfg;
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
                this.maxCacheMemoryMb,
                this.valueCachePolicyType,
                this.mapCachePolicyType,
                this.listCachePolicyType,
                this.aggregatingCachePolicyType,
                (int) this.mapL1KeyPresenceCacheSize, (int) this.mapL2KeyPresenceCacheSize,
                this.mapCacheHitRateThreshold, this.mapCacheHitRateWindowSize, this.mapCacheMinAccessesForBypassCheck,
                this.mapKeyPresenceCacheEnabled,
                this.mapBypassEnabled,
                this.mapPresenceCacheImpl,
                this.l2ManagedMemoryEnabled,
                env.getMemoryManager(),
                this.taskConfiguration,
                0,
                0,
                (int) this.valueMaxActiveNamespaces,
                (int) this.mapMaxActiveNamespaces,
                (int) this.listMaxActiveNamespaces,
                (int) this.aggregatingMaxActiveNamespaces);
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

    public long getValueMaxActiveNamespaces() { return valueMaxActiveNamespaces; }
    public long getMapMaxActiveNamespaces() { return mapMaxActiveNamespaces; }
    public long getListMaxActiveNamespaces() { return listMaxActiveNamespaces; }
    public long getAggregatingMaxActiveNamespaces() { return aggregatingMaxActiveNamespaces; }

    public long getMaxCacheMemoryMb() {
        return maxCacheMemoryMb;
    }

    public CachingStateBackendFactory.CachePolicyType getGlobalCachePolicyType() { return globalCachePolicyType; }
    public CachingStateBackendFactory.CachePolicyType getMapCachePolicyType() { return mapCachePolicyType; }
    public CachingStateBackendFactory.CachePolicyType getValueCachePolicyType() { return valueCachePolicyType; }
    public CachingStateBackendFactory.CachePolicyType getListCachePolicyType() { return listCachePolicyType; }
    public CachingStateBackendFactory.CachePolicyType getAggregatingCachePolicyType() { return aggregatingCachePolicyType; }

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

    public boolean isL2ManagedMemoryEnabled() {
        return l2ManagedMemoryEnabled;
    }

    @Override
    public StateBackend configure(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {

        if (delegateBackend instanceof ConfigurableStateBackend) {
            return new CachingStateBackend(((ConfigurableStateBackend) delegateBackend).configure(config, classLoader), config);
        } else {
            return new CachingStateBackend(delegateBackend, config);
        }
    }
}
