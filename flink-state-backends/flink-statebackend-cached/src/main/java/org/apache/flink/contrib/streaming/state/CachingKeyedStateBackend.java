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

package org.apache.flink.contrib.streaming.state;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricMonitor;
import org.apache.flink.contrib.streaming.state.RocksDBResourceContainer;
import org.apache.flink.contrib.streaming.state.RocksDBWriteBatchWrapper;
import org.apache.flink.contrib.streaming.state.snapshot.RocksDBSnapshotStrategyBase;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





// Added for metrics

// Added for logging

/**
 * The keyed state backend that implements caching. It wraps a delegate AbstractKeyedStateBackend
 * (e.g., RocksDBKeyedStateBackend) and creates CachingInternal*State objects.
 */
public class CachingKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CachingKeyedStateBackend.class);

    private final AbstractKeyedStateBackend<K> delegateKeyedStateBackend; // RocksDBKeyedStateBackend
    private final ExecutionConfig executionConfig; // Store execution config as field
    private final int l1EntryCacheSize;
    private final int l2EntryCacheSize;
    private final int maxActiveNamespaceOrPerKeyCacheContainers;
    private final long maxCacheMemoryMb;
    // Per-state cache policy types
    private final CachingStateBackendFactory.CachePolicyType valueCachePolicyType;
    private final CachingStateBackendFactory.CachePolicyType mapCachePolicyType;
    private final CachingStateBackendFactory.CachePolicyType listCachePolicyType;
    private final CachingStateBackendFactory.CachePolicyType aggregatingCachePolicyType;
    private final int mapL1KeyPresenceCacheSize;
    private final int mapL2KeyPresenceCacheSize;
    private final MetricGroup metricGroup;

    private final double mapCacheHitRateThreshold;
    private final long mapCacheHitRateWindowSize;
    private final long mapCacheMinAccessesForBypassCheck;
    private final boolean mapKeyPresenceCacheEnabled;
    private final boolean mapBypassEnabled;
    private final CachingStateBackendFactory.PresenceCacheImplementation mapPresenceCacheImpl;
    private final boolean l2ManagedMemoryEnabled;
    private final int mapSpecificL1EntryCacheSize;
    private final int mapSpecificL2EntryCacheSize;
    // Per-state overrides for max active namespaces
    private final int valueMaxActiveNamespaces;
    private final int mapMaxActiveNamespaces;
    private final int listMaxActiveNamespaces;
    private final int listMaxElementsPerEntry;
    private final int listIncrementalFlushThreshold;
    private final int aggregatingMaxActiveNamespaces;
    private final long l2TimeBucketSizeMillis;
    private final boolean perKeyMetricsEnabled;
    // Profiling controls (lightweight wrapper overhead estimation)
    private final boolean profileEnabled;
    private final int profileSampleRate;

    private final List<CachingInternalState<K, ?, ?, ?>> registeredStates;
    // Ensure one caching wrapper per delegate state to avoid duplicated, diverging caches
    private final Map<InternalKvState<?, ?, ?>, CachingInternalState<K, ?, ?, ?>> stateWrapperByDelegate;
    // Fallback for when delegate returns distinct objects for the same logical state
    private final Map<String, CachingInternalState<K, ?, ?, ?>> stateWrapperByName;

    private transient ManagedPagePool managedPagePool;
    private final transient MemoryManager memoryManager;
    private final transient org.apache.flink.configuration.Configuration taskConfiguration;

    // Added for memory capping
    private transient AtomicLong currentEstimatedCacheSizeBytes;
    private transient long maxConfiguredCacheSizeBytes;
    private static final long EVICTION_CHECK_THRESHOLD_BYTES = 10 * 1024 * 1024; // 10MB batching threshold
    private transient AtomicLong bytesSinceLastEvictionCheck;
    // Using the static ValueSizeUtils for now, but a Function could be injected here
    // private transient Function<Object, Long> valueSizeEstimator;

    // Global L2 map entry counting
    private transient AtomicLong globalL2MapEntryCount;
    private final long maxGlobalL2Entries;

    public CachingKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            @Nonnull CloseableRegistry cancelStreamRegistry,
            AbstractKeyedStateBackend<K> delegateKeyedStateBackend,
            int l1EntryCacheSize,
            int l2EntryCacheSize,
            int maxActiveNamespaceOrPerKeyCacheContainers,
            long maxCacheMemoryMb,
            CachingStateBackendFactory.CachePolicyType valueCachePolicyType,
            CachingStateBackendFactory.CachePolicyType mapCachePolicyType,
            CachingStateBackendFactory.CachePolicyType listCachePolicyType,
            CachingStateBackendFactory.CachePolicyType aggregatingCachePolicyType,
            int mapL1KeyPresenceCacheSize, int mapL2KeyPresenceCacheSize,
            double mapCacheHitRateThreshold, long mapCacheHitRateWindowSize, long mapCacheMinAccessesForBypassCheck,
            boolean mapKeyPresenceCacheEnabled, boolean mapBypassEnabled, CachingStateBackendFactory.PresenceCacheImplementation mapPresenceCacheImpl,
            boolean l2ManagedMemoryEnabled, MemoryManager memoryManager,
            org.apache.flink.configuration.Configuration taskConfiguration,
            int mapSpecificL1EntryCacheSize, int mapSpecificL2EntryCacheSize,
            int valueMaxActiveNamespaces, int mapMaxActiveNamespaces,
            int listMaxActiveNamespaces, int aggregatingMaxActiveNamespaces,
            int listMaxElementsPerEntry, int listIncrementalFlushThreshold) {

        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                CachingKeyedStateBackend.buildLatencyTrackingConfig(metricGroup, executionConfig),
                cancelStreamRegistry,
                delegateKeyedStateBackend.getKeyGroupCompressionDecorator(),
                delegateKeyedStateBackend.getKeyContext());

        this.delegateKeyedStateBackend = delegateKeyedStateBackend;
        this.executionConfig = executionConfig; // Initialize execution config field
        this.l1EntryCacheSize = l1EntryCacheSize;
        this.l2EntryCacheSize = l2EntryCacheSize;
        this.maxActiveNamespaceOrPerKeyCacheContainers = maxActiveNamespaceOrPerKeyCacheContainers;
        this.maxCacheMemoryMb = maxCacheMemoryMb;
        this.registeredStates = new ArrayList<>();
        this.stateWrapperByDelegate = new IdentityHashMap<>();
        this.stateWrapperByName = new java.util.HashMap<>();
        this.valueCachePolicyType = valueCachePolicyType;
        this.mapCachePolicyType = mapCachePolicyType;
        this.listCachePolicyType = listCachePolicyType;
        this.aggregatingCachePolicyType = aggregatingCachePolicyType;
        this.mapL1KeyPresenceCacheSize = mapL1KeyPresenceCacheSize;
        this.mapL2KeyPresenceCacheSize = mapL2KeyPresenceCacheSize;
        this.metricGroup = metricGroup;
        this.mapCacheHitRateThreshold = mapCacheHitRateThreshold;
        this.mapCacheHitRateWindowSize = mapCacheHitRateWindowSize;
        this.mapCacheMinAccessesForBypassCheck = mapCacheMinAccessesForBypassCheck;
        this.mapKeyPresenceCacheEnabled = mapKeyPresenceCacheEnabled;
        this.mapBypassEnabled = mapBypassEnabled;
        this.mapPresenceCacheImpl = mapPresenceCacheImpl;
        // If L2 capacity is zero, disable managed L2 to avoid unnecessary Off-Heap setup
        this.l2ManagedMemoryEnabled = l2ManagedMemoryEnabled && (this.l2EntryCacheSize > 0);
        this.memoryManager = memoryManager;
        this.taskConfiguration = taskConfiguration;

        this.mapSpecificL1EntryCacheSize = mapSpecificL1EntryCacheSize;
        this.mapSpecificL2EntryCacheSize = mapSpecificL2EntryCacheSize;
        this.valueMaxActiveNamespaces = (valueMaxActiveNamespaces > 0) ? valueMaxActiveNamespaces : maxActiveNamespaceOrPerKeyCacheContainers;
        this.mapMaxActiveNamespaces = (mapMaxActiveNamespaces > 0) ? mapMaxActiveNamespaces : maxActiveNamespaceOrPerKeyCacheContainers;
        this.listMaxActiveNamespaces = (listMaxActiveNamespaces > 0) ? listMaxActiveNamespaces : maxActiveNamespaceOrPerKeyCacheContainers;
        this.listMaxElementsPerEntry = listMaxElementsPerEntry;
        this.listIncrementalFlushThreshold = listIncrementalFlushThreshold;
        this.aggregatingMaxActiveNamespaces = (aggregatingMaxActiveNamespaces > 0) ? aggregatingMaxActiveNamespaces : maxActiveNamespaceOrPerKeyCacheContainers;

        // Initialize global L2 entry limit from configuration
        this.maxGlobalL2Entries = taskConfiguration.getLong("state.backend.cached.map.l2.size.entries", 16384L);

        long cfgBucket = 0L;
        try {
            cfgBucket = this.taskConfiguration.getLong("state.backend.cached.map.l2.time-bucket.size", 0L);
        } catch (Throwable t) {
            cfgBucket = 0L;
        }
        this.l2TimeBucketSizeMillis = Math.max(0L, cfgBucket);

        // Initialize memory capping fields
        this.currentEstimatedCacheSizeBytes = new AtomicLong(0L);
        this.maxConfiguredCacheSizeBytes = this.maxCacheMemoryMb * 1024L * 1024L;
        this.bytesSinceLastEvictionCheck = new AtomicLong(0L);
        this.globalL2MapEntryCount = new AtomicLong(0L);
        // Initialize per-key metrics flag (default false to avoid high cardinality)
        this.perKeyMetricsEnabled = (this.taskConfiguration != null)
                ? this.taskConfiguration.getBoolean(CachingStateBackendFactory.MAP_PER_KEY_METRICS_ENABLED)
                : false;
        // Lightweight profiling flags (default off to avoid overhead)
        boolean pEnabled = false;
        int pRate = 1024;
        try {
            if (this.taskConfiguration != null) {
                pEnabled = this.taskConfiguration.getBoolean(CachingStateBackendFactory.PROFILE_ENABLED_CONFIG);
                pRate = Math.max(1, this.taskConfiguration.getInteger(CachingStateBackendFactory.PROFILE_SAMPLE_RATE_CONFIG));
            }
        } catch (Throwable t) {
            // keep defaults on error
        }
        this.profileEnabled = pEnabled;
        this.profileSampleRate = pRate;
        // Initialize managed page pool.
        // It's crucial to check both the feature flag and the availability of the memory manager.
        if (this.l2ManagedMemoryEnabled && this.memoryManager != null && this.memoryManager.getMemorySize() > 0) {
            LOG.info("L2 cache is configured to use managed memory. Max size: {} MB", this.maxCacheMemoryMb);
            this.managedPagePool = new ManagedPagePool(this.memoryManager);
        } else {
            if (!this.l2ManagedMemoryEnabled) {
                LOG.info("L2 cache is not configured to use managed memory (l2ManagedMemoryEnabled is false).");
            } else if (this.memoryManager == null) {
                LOG.info("L2 cache cannot use managed memory because MemoryManager is null.");
            } else {
                LOG.info("L2 cache cannot use managed memory because no managed memory is allocated (size is 0).");
            }
            // Fallback to a no-op pool if managed memory is not used.
            this.managedPagePool = new ManagedPagePool(null);
        }

        // Register memory usage gauge
        if (this.metricGroup != null) {
            this.metricGroup.gauge("estimatedCacheMemoryBytes",
                    currentEstimatedCacheSizeBytes::get);
            this.metricGroup.gauge("globalL2MapEntries",
                    globalL2MapEntryCount::get);
            this.metricGroup.gauge("l2TimeBucketSizeMillis", () -> this.l2TimeBucketSizeMillis);
        }
        // Configure auto left-bypass from task configuration (kill-switch)
        try {
            boolean autoLeftBypass = this.taskConfiguration.getBoolean(
                    "state.backend.cached.auto-left-bypass.enabled", true);
            CachingInternalMapState.setAutoLeftBypass(autoLeftBypass);
            LOG.info("Auto left-bypass for cached backend is {}", autoLeftBypass ? "ENABLED" : "DISABLED");
        } catch (Throwable t) {
            LOG.warn("Failed to read config for auto-left-bypass; defaulting to ENABLED", t);
            CachingInternalMapState.setAutoLeftBypass(true);
        }

        // Configure package logging level if requested
        try {
            boolean cachedLoggingEnabled = (this.taskConfiguration != null) &&
                    this.taskConfiguration.getBoolean(CachingStateBackendFactory.LOGGING_ENABLED_CONFIG);
            String cachedLoggingLevel = (this.taskConfiguration != null)
                    ? this.taskConfiguration.get(CachingStateBackendFactory.LOGGING_LEVEL_CONFIG)
                    : null;
            if (!cachedLoggingEnabled) {
                String lvl = (cachedLoggingLevel == null || cachedLoggingLevel.isEmpty()) ? "ERROR" : cachedLoggingLevel;
                LoggingUtils.setPackageLogLevel("org.apache.flink.contrib.streaming.state", lvl);
                LOG.info("Cached backend logging disabled via config; using level {}", lvl);
            } else if (cachedLoggingLevel != null && !cachedLoggingLevel.isEmpty()) {
                LoggingUtils.setPackageLogLevel("org.apache.flink.contrib.streaming.state", cachedLoggingLevel);
                LOG.info("Cached backend logging level set to {} via config", cachedLoggingLevel);
            }
        } catch (Throwable t) {
            // best-effort; ignore failures
        }
        // this.valueSizeEstimator = ValueSizeUtils::estimate; // Example if Function was used
    }

    // create a new RocksDB backend
    public CachingKeyedStateBackend(
            ClassLoader userCodeClassLoader,
            File instanceBasePath,
            RocksDBResourceContainer optionsContainer,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            RocksDB db,
            LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            int keyGroupPrefixBytes,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            ResourceGuard rocksDBResourceGuard,
            RocksDBSnapshotStrategyBase<K, ?> checkpointSnapshotStrategy,
            RocksDBWriteBatchWrapper writeBatchWrapper,
            ColumnFamilyHandle defaultColumnFamilyHandle,
            RocksDBNativeMetricMonitor nativeMetricMonitor,
            SerializedCompositeKeyBuilder<K> sharedRocksKeyBuilder,
            PriorityQueueSetFactory priorityQueueFactory,
            RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            InternalKeyContext<K> keyContext,
            @Nonnegative long writeBatchSize,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            AbstractKeyedStateBackend<K> delegateKeyedStateBackend, // Add this
            int l1EntryCacheSize,
            int l2EntryCacheSize,
            int maxActiveNamespaceOrPerKeyCacheContainers,
            long maxCacheMemoryMb,
            CachingStateBackendFactory.CachePolicyType valueCachePolicyType,
            CachingStateBackendFactory.CachePolicyType mapCachePolicyType,
            CachingStateBackendFactory.CachePolicyType listCachePolicyType,
            CachingStateBackendFactory.CachePolicyType aggregatingCachePolicyType,
            int mapL1KeyPresenceCacheSize, int mapL2KeyPresenceCacheSize,
            double mapCacheHitRateThreshold, long mapCacheHitRateWindowSize, long mapCacheMinAccessesForBypassCheck,
            boolean mapKeyPresenceCacheEnabled, boolean mapBypassEnabled, CachingStateBackendFactory.PresenceCacheImplementation mapPresenceCacheImpl,
            boolean l2ManagedMemoryEnabled,
            MemoryManager memoryManager,
            org.apache.flink.configuration.Configuration taskConfiguration,
            int mapSpecificL1EntryCacheSize, int mapSpecificL2EntryCacheSize,
            int valueMaxActiveNamespaces, int mapMaxActiveNamespaces,
            int listMaxActiveNamespaces, int aggregatingMaxActiveNamespaces,
            int listMaxElementsPerEntry, int listIncrementalFlushThreshold
    ) {
        // Call super constructor first, using direct parameters where available
        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader, // direct parameter
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig, // direct parameter
                cancelStreamRegistry, // direct parameter
                keyGroupCompressionDecorator, // direct parameter
                keyContext); // direct parameter

        // Now initialize the delegateKeyedStateBackend
        this.delegateKeyedStateBackend = delegateKeyedStateBackend;
        this.executionConfig = executionConfig; // Initialize execution config field
        this.metricGroup = metricGroup;

        this.l1EntryCacheSize = l1EntryCacheSize;
        this.l2EntryCacheSize = l2EntryCacheSize;
        this.maxActiveNamespaceOrPerKeyCacheContainers = maxActiveNamespaceOrPerKeyCacheContainers;
        this.maxCacheMemoryMb = maxCacheMemoryMb;
        this.registeredStates = new ArrayList<>();
        this.stateWrapperByDelegate = new IdentityHashMap<>();
        this.stateWrapperByName = new java.util.HashMap<>();
        this.valueCachePolicyType = valueCachePolicyType;
        this.mapCachePolicyType = mapCachePolicyType;
        this.listCachePolicyType = listCachePolicyType;
        this.aggregatingCachePolicyType = aggregatingCachePolicyType;
        this.mapL1KeyPresenceCacheSize = mapL1KeyPresenceCacheSize;
        this.mapL2KeyPresenceCacheSize = mapL2KeyPresenceCacheSize;
        this.mapCacheHitRateThreshold = mapCacheHitRateThreshold;
        this.mapCacheHitRateWindowSize = mapCacheHitRateWindowSize;
        this.mapCacheMinAccessesForBypassCheck = mapCacheMinAccessesForBypassCheck;
        this.mapKeyPresenceCacheEnabled = mapKeyPresenceCacheEnabled;
        this.mapBypassEnabled = mapBypassEnabled;
        this.mapPresenceCacheImpl = mapPresenceCacheImpl;
        // If L2 capacity is zero, disable managed L2 to avoid unnecessary Off-Heap setup
        this.l2ManagedMemoryEnabled = l2ManagedMemoryEnabled && (this.l2EntryCacheSize > 0);
        this.memoryManager = memoryManager;
        this.taskConfiguration = taskConfiguration;

        this.mapSpecificL1EntryCacheSize = mapSpecificL1EntryCacheSize;
        this.mapSpecificL2EntryCacheSize = mapSpecificL2EntryCacheSize;
        this.valueMaxActiveNamespaces = (valueMaxActiveNamespaces > 0) ? valueMaxActiveNamespaces : maxActiveNamespaceOrPerKeyCacheContainers;
        this.mapMaxActiveNamespaces = (mapMaxActiveNamespaces > 0) ? mapMaxActiveNamespaces : maxActiveNamespaceOrPerKeyCacheContainers;
        this.listMaxActiveNamespaces = (listMaxActiveNamespaces > 0) ? listMaxActiveNamespaces : maxActiveNamespaceOrPerKeyCacheContainers;
        this.listMaxElementsPerEntry = listMaxElementsPerEntry;
        this.listIncrementalFlushThreshold = listIncrementalFlushThreshold;
        this.aggregatingMaxActiveNamespaces = (aggregatingMaxActiveNamespaces > 0) ? aggregatingMaxActiveNamespaces : maxActiveNamespaceOrPerKeyCacheContainers;

        long cfgBucket2 = 0L;
        try {
            cfgBucket2 = (this.taskConfiguration != null)
                    ? this.taskConfiguration.getLong("state.backend.cached.map.l2.time-bucket.size", 0L)
                    : 0L;
        } catch (Throwable t) {
            cfgBucket2 = 0L;
        }
        this.l2TimeBucketSizeMillis = Math.max(0L, cfgBucket2);

        // Initialize global L2 entry limit from configuration (same default as other constructor)
        this.maxGlobalL2Entries = taskConfiguration != null
                ? taskConfiguration.getLong("state.backend.cached.map.l2.size.entries", 16384L)
                : 16384L;

        // Initialize memory capping fields
        this.currentEstimatedCacheSizeBytes = new AtomicLong(0L);
        this.maxConfiguredCacheSizeBytes = this.maxCacheMemoryMb * 1024L * 1024L;
        this.bytesSinceLastEvictionCheck = new AtomicLong(0L);
        this.globalL2MapEntryCount = new AtomicLong(0L);
        // Initialize per-key metrics flag (default false to avoid high cardinality)
        this.perKeyMetricsEnabled = (this.taskConfiguration != null)
                ? this.taskConfiguration.getBoolean(CachingStateBackendFactory.MAP_PER_KEY_METRICS_ENABLED)
                : false;

        // Register memory usage gauge
        if (this.metricGroup != null) {
            this.metricGroup.gauge("estimatedCacheMemoryBytes",
                    currentEstimatedCacheSizeBytes::get);
            this.metricGroup.gauge("globalL2MapEntries",
                    globalL2MapEntryCount::get);
            this.metricGroup.gauge("l2TimeBucketSizeMillis", () -> this.l2TimeBucketSizeMillis);
        }

        // Ensure managedPagePool is initialized based on configuration
        if (this.l2ManagedMemoryEnabled && this.memoryManager != null && this.memoryManager.getMemorySize() > 0) {
            this.managedPagePool = new ManagedPagePool(this.memoryManager);
            LOG.info("L2 cache is using managed memory via RocksDB constructor path. Max size: {} MB", this.maxCacheMemoryMb);
        } else {
            this.managedPagePool = new ManagedPagePool(null); // Fallback to no-op pool
        }

        // Lightweight profiling flags (default off to avoid overhead)
        boolean pEnabled = false;
        int pRate = 1024;
        try {
            if (this.taskConfiguration != null) {
                pEnabled = this.taskConfiguration.getBoolean(CachingStateBackendFactory.PROFILE_ENABLED_CONFIG);
                pRate = Math.max(1, this.taskConfiguration.getInteger(CachingStateBackendFactory.PROFILE_SAMPLE_RATE_CONFIG));
            }
        } catch (Throwable t) {
            // keep defaults on error
        }
        this.profileEnabled = pEnabled;
        this.profileSampleRate = pRate;

        // Configure auto left-bypass from task configuration (kill-switch)
        try {
            boolean autoLeftBypass = this.taskConfiguration != null
                    ? this.taskConfiguration.getBoolean(
                            "state.backend.cached.auto-left-bypass.enabled", true)
                    : true;
            CachingInternalMapState.setAutoLeftBypass(autoLeftBypass);
            LOG.info("Auto left-bypass for cached backend is {} (RocksDB path)", autoLeftBypass ? "ENABLED" : "DISABLED");
        } catch (Throwable t) {
            LOG.warn("Failed to read config for auto-left-bypass (RocksDB path); defaulting to ENABLED", t);
            CachingInternalMapState.setAutoLeftBypass(true);
        }

        // Configure package logging level if requested
        try {
            boolean cachedLoggingEnabled = (this.taskConfiguration != null) &&
                    this.taskConfiguration.getBoolean(CachingStateBackendFactory.LOGGING_ENABLED_CONFIG);
            String cachedLoggingLevel = (this.taskConfiguration != null)
                    ? this.taskConfiguration.get(CachingStateBackendFactory.LOGGING_LEVEL_CONFIG)
                    : null;
            if (!cachedLoggingEnabled) {
                String lvl = (cachedLoggingLevel == null || cachedLoggingLevel.isEmpty()) ? "ERROR" : cachedLoggingLevel;
                LoggingUtils.setPackageLogLevel("org.apache.flink.contrib.streaming.state", lvl);
                LOG.info("Cached backend logging disabled via config; using level {}", lvl);
            } else if (cachedLoggingLevel != null && !cachedLoggingLevel.isEmpty()) {
                LoggingUtils.setPackageLogLevel("org.apache.flink.contrib.streaming.state", cachedLoggingLevel);
                LOG.info("Cached backend logging level set to {} via config", cachedLoggingLevel);
            }
        } catch (Throwable t) {
            // best-effort; ignore failures
        }
    }

    public ManagedPagePool getManagedPagePool() {
        return managedPagePool;
    }

    public long getL2TimeBucketSizeMillis() {
        return l2TimeBucketSizeMillis;
    }

    public AtomicLong getGlobalL2MapEntryCount() {
        return globalL2MapEntryCount;
    }

    public long getMaxGlobalL2Entries() {
        return maxGlobalL2Entries;
    }

    public int getMaxActiveNamespaceOrPerKeyCacheContainers() {
        return maxActiveNamespaceOrPerKeyCacheContainers;
    }

    /**
     * Hook to notify cached states about watermark advancement for time-bucketed eviction.
     * Safe to call even if no cached MapState is present.
     */
    public void onWatermark(long watermarkMillis) {
        try {
            synchronized (registeredStates) {
                for (CachingInternalState<K, ?, ?, ?> state : registeredStates) {
                    if (state instanceof CachingInternalMapState) {
                        @SuppressWarnings("unchecked")
                        CachingInternalMapState<K, Object, Object, Object> mapState =
                                (CachingInternalMapState<K, Object, Object, Object>) state;
                        mapState.onWatermarkEvict(watermarkMillis);
                    }
                }
            }
        } catch (Throwable t) {
            LOG.debug("onWatermark hook failed: {}", t.getMessage());
        }
    }

    @Nonnull
    @Override
    @SuppressWarnings({
        "unchecked",
        "rawtypes"
    }) // V_SD is the value in StateDescriptor, UK/UV for Map, V_ELE for List
    public <N, S extends State, V_SD> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, V_SD> stateDescriptor)
            throws Exception {
        // Wrapper reuse keyed by delegate identity happens after obtaining the delegate from underlying backend.

        S actualState = delegateKeyedStateBackend.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);

        if (!(actualState instanceof InternalKvState)) {
            return actualState; // Return directly if not an InternalKvState, cannot cache
        }

        InternalKvState<K, N, ?> actualStateRaw = (InternalKvState<K, N, ?>) actualState;

        // Try reuse by identity first, then by logical name
        synchronized (registeredStates) {
            CachingInternalState<K, ?, ?, ?> existing = stateWrapperByDelegate.get(actualStateRaw);
            if (existing != null) {
                @SuppressWarnings("unchecked")
                S casted = (S) existing;
                return casted;
            }
            String nameKey = stateDescriptor.getType() + "#" + stateDescriptor.getName();
            CachingInternalState<K, ?, ?, ?> existingByName = stateWrapperByName.get(nameKey);
            if (existingByName != null) {
                stateWrapperByDelegate.put(actualStateRaw, existingByName);
                @SuppressWarnings("unchecked")
                S casted = (S) existingByName;
                return casted;
            }
        }
        // Reuse existing wrapper if we've already created one for this delegate state
        synchronized (registeredStates) {
            @SuppressWarnings("unchecked")
            S existing = (S) stateWrapperByDelegate.get(actualStateRaw);
            if (existing != null) {
                return existing;
            }
        }
        CachingInternalState<K, N, ?, ?> cachingStateToRegister = null;

        if (stateDescriptor.getType() == StateDescriptor.Type.VALUE && actualStateRaw instanceof InternalValueState) {
            // Check if ValueState caching is enabled
            boolean valueCacheEnabled = true;
            double valueHitRateThreshold = 0.0;
            long valueHitRateWindow = 1000L;
            long valueMinAccessesForBypassCheck = 100L;
            boolean valueBypassEnabled = true;
            boolean writeBehindEnabled = false;
            try {
                if (this.taskConfiguration != null) {
                    valueCacheEnabled = this.taskConfiguration.getBoolean(
                            CachingStateBackendFactory.VALUE_CACHE_ENABLED_CONFIG);
                    valueHitRateThreshold = this.taskConfiguration.getDouble(
                            CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_THRESHOLD_CONFIG);
                    valueHitRateWindow = this.taskConfiguration.getLong(
                            CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG);
                    valueMinAccessesForBypassCheck = this.taskConfiguration.getLong(
                            CachingStateBackendFactory.VALUE_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG);
                    valueBypassEnabled = this.taskConfiguration.getBoolean(
                            CachingStateBackendFactory.VALUE_BYPASS_ENABLED_CONFIG);
                    writeBehindEnabled = this.taskConfiguration.getBoolean(
                            CachingStateBackendFactory.WRITE_BEHIND_ENABLED_CONFIG);
                }
            } catch (Throwable t) {
                // keep defaults
            }

            // Scope correction: force write-through for ValueState in Table window code paths
            if (isTableWindowStack()) {
                // writeBehindEnabled = false;
            }

            if (!valueCacheEnabled) {
                LOG.info("Value state caching is disabled by config. Returning raw state.");
                return (S) actualStateRaw;
            }

            InternalValueState<K, N, V_SD> actualStateValue = (InternalValueState<K, N, V_SD>) actualStateRaw;
            cachingStateToRegister = new CachingInternalValueState<K, N, V_SD>(
                    actualStateValue,
                    this,
                    l1EntryCacheSize,
                    l2EntryCacheSize,
                    this.valueMaxActiveNamespaces,
                    this.maxCacheMemoryMb,
                    this.valueCachePolicyType,
                    valueHitRateThreshold,
                    valueHitRateWindow,
                    valueMinAccessesForBypassCheck,
                    valueBypassEnabled,
                    writeBehindEnabled,
                    this.metricGroup.addGroup("state").addGroup(stateDescriptor.getName()));
        } else if (stateDescriptor.getType() == StateDescriptor.Type.MAP && actualStateRaw instanceof InternalMapState) {
            boolean mapCacheEnabled = taskConfiguration.get(CachingStateBackendFactory.MAP_CACHE_ENABLED_CONFIG);
            if (!mapCacheEnabled) {
                LOG.info("Map state caching is disabled by config. Returning raw state.");
                return (S) actualStateRaw; // caching disabled by config
            }
            InternalMapState<K, N, ?, ?> actualDelegateMapState = (InternalMapState<K, N, ?, ?>) actualStateRaw;
            String stateName = stateDescriptor.getName(); // Get state name for metrics
            LOG.info("[CachedBackend] Creating MapState for state '{}'", stateName);
            MetricGroup mapMetricsGroup = this.metricGroup.addGroup("state").addGroup(stateName);

            int l1SizeForMap = mapSpecificL1EntryCacheSize > 0 ? mapSpecificL1EntryCacheSize : l1EntryCacheSize;
            int l2SizeForMap = mapSpecificL2EntryCacheSize > 0 ? mapSpecificL2EntryCacheSize : l2EntryCacheSize;

            boolean forceBypass = false;
            try {
                String pattern = this.taskConfiguration.getString(CachingStateBackendFactory.MAP_FORCE_BYPASS_STATES_REGEX, "");
                forceBypass = pattern != null && stateName != null && stateName.matches(pattern);
            } catch (Throwable t) {
                LOG.warn("Failed to parse force-bypass pattern for map state '{}'", stateName, t);
            }
            // If a state is force-bypassed and configured to return raw, hand back the delegate
            // state directly instead of wrapping it at all. This avoids any wrapper overhead and
            // guarantees vanilla behavior for these states.
            try {
                boolean returnRawOnForceBypass = this.taskConfiguration.getBoolean(
                        CachingStateBackendFactory.MAP_FORCE_BYPASS_RETURN_RAW);
                if (forceBypass && returnRawOnForceBypass) {
                    LOG.info("Map state '{}' matched force-bypass; returning raw delegate state.", stateName);
                    return (S) actualDelegateMapState;
                }
            } catch (Throwable t) {
                // best-effort; fall through to wrapper creation
            }

            cachingStateToRegister = new CachingInternalMapState<>(
                    (InternalMapState<K, N, ?, ?>) actualDelegateMapState,
                    this,
                    l1SizeForMap,
                    l2SizeForMap,
                    maxActiveNamespaceOrPerKeyCacheContainers,
                    this.maxCacheMemoryMb,
                    this.mapCachePolicyType,
                    this.mapL1KeyPresenceCacheSize,
                    this.mapL2KeyPresenceCacheSize,
                    mapMetricsGroup,
                    this.mapCacheHitRateThreshold,
                    this.mapCacheHitRateWindowSize,
                    this.mapCacheMinAccessesForBypassCheck,
                    this.mapKeyPresenceCacheEnabled,
                    this.mapBypassEnabled,
                    this.mapPresenceCacheImpl,
                    this.l2ManagedMemoryEnabled,
                    this.perKeyMetricsEnabled,
                    forceBypass,
                    this.mapMaxActiveNamespaces);
        } else if (stateDescriptor.getType() == StateDescriptor.Type.LIST && actualStateRaw instanceof InternalListState) {
            InternalListState<K, N, V_SD> actualDelegateListState = (InternalListState<K, N, V_SD>) actualStateRaw;
            cachingStateToRegister = new CachingInternalListState<>(
                    actualDelegateListState, this, l1EntryCacheSize, l2EntryCacheSize,
                    this.listMaxActiveNamespaces, this.listCachePolicyType, this.listMaxElementsPerEntry,
                    this.listIncrementalFlushThreshold);
        } else if (stateDescriptor.getType() == StateDescriptor.Type.AGGREGATING && actualStateRaw instanceof InternalAggregatingState) {
            InternalAggregatingState actualDelegateAggState = (InternalAggregatingState) actualStateRaw;
            AggregatingStateDescriptor aggStateDesc = (AggregatingStateDescriptor) stateDescriptor;
            String stateName = aggStateDesc.getName();
            MetricGroup aggMetricsGroup = this.metricGroup.addGroup("state").addGroup(stateName).addGroup("cache");
            cachingStateToRegister = new CachingInternalAggregatingState(
                actualDelegateAggState,
                this,
                aggStateDesc.getAggregateFunction(),
                l1EntryCacheSize,
                l2EntryCacheSize,
                this.aggregatingCachePolicyType,
                aggMetricsGroup,
                this.aggregatingMaxActiveNamespaces);
        } else {
            // For unsupported types or if actualStateRaw is not an instance of the expected internal type,
            // return the raw state from the delegate directly.
            LOG.warn("State type {} not supported for caching or type mismatch. Returning raw state.", stateDescriptor.getType());
            return (S) actualStateRaw;
        }

        if (cachingStateToRegister == null) {
            // Should not happen if logic above is correct and creates a caching wrapper
            LOG.error("Failed to create a caching wrapper for a supported state type: {}", stateDescriptor.getType());
            return (S) actualStateRaw; // Fallback, though indicates an issue
        }

        synchronized (registeredStates) {
            // Double-check if another thread registered meanwhile
            @SuppressWarnings("unchecked")
            S existing = (S) stateWrapperByDelegate.get(actualStateRaw);
            if (existing != null) {
                return existing;
            }
            stateWrapperByDelegate.put(actualStateRaw, cachingStateToRegister);
            String nameKey = stateDescriptor.getType() + "#" + stateDescriptor.getName();
            stateWrapperByName.put(nameKey, cachingStateToRegister);
            boolean alreadyExists = registeredStates.stream()
                    .anyMatch(st -> st.getDelegateState() == actualStateRaw);
            if (!alreadyExists) {
                registeredStates.add(cachingStateToRegister);
            }
        }
        return (S) cachingStateToRegister;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <N, S extends State> S getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, ?> stateDescriptor)
            throws Exception {
        // Get raw state from delegate first
        S actualState = delegateKeyedStateBackend.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
        if (!(actualState instanceof InternalKvState)) {
            return actualState;
        }

        InternalKvState<K, N, ?> actualStateRaw = (InternalKvState<K, N, ?>) actualState;

        // If we already have a wrapper for this delegate state, reuse it and set namespace;
        // fall back to name-based reuse if delegate identity differs
        synchronized (registeredStates) {
            CachingInternalState<K, ?, ?, ?> existing = stateWrapperByDelegate.get(actualStateRaw);
            if (existing != null) {
                if (existing instanceof CachingInternalValueState) {
                    ((CachingInternalValueState) existing).setCurrentNamespace(namespace);
                } else if (existing instanceof CachingInternalMapState) {
                    ((CachingInternalMapState) existing).setCurrentNamespace(namespace);
                } else if (existing instanceof CachingInternalListState) {
                    ((CachingInternalListState) existing).setCurrentNamespace(namespace);
                } else if (existing instanceof CachingInternalAggregatingState) {
                    ((CachingInternalAggregatingState) existing).setCurrentNamespace(namespace);
                }
                return (S) existing;
            }
            String nameKey = stateDescriptor.getType() + "#" + stateDescriptor.getName();
            CachingInternalState<K, ?, ?, ?> existingByName = stateWrapperByName.get(nameKey);
            if (existingByName != null) {
                stateWrapperByDelegate.put(actualStateRaw, existingByName);
                if (existingByName instanceof CachingInternalValueState) {
                    ((CachingInternalValueState) existingByName).setCurrentNamespace(namespace);
                } else if (existingByName instanceof CachingInternalMapState) {
                    ((CachingInternalMapState) existingByName).setCurrentNamespace(namespace);
                } else if (existingByName instanceof CachingInternalListState) {
                    ((CachingInternalListState) existingByName).setCurrentNamespace(namespace);
                } else if (existingByName instanceof CachingInternalAggregatingState) {
                    ((CachingInternalAggregatingState) existingByName).setCurrentNamespace(namespace);
                }
                return (S) existingByName;
            }
        }

        // Otherwise, create an appropriate wrapper as in getOrCreateKeyedState
        CachingInternalState<K, N, ?, ?> cachingStateToRegister = null;
        switch (stateDescriptor.getType()) {
            case VALUE: {
                boolean valueCacheEnabled = true;
                double valueHitRateThreshold = 0.0;
                long valueHitRateWindow = 1000L;
                long valueMinAccessesForBypassCheck = 100L;
                boolean valueBypassEnabled = true;
                boolean writeBehindEnabled = false;
                try {
                    if (this.taskConfiguration != null) {
                        valueCacheEnabled = this.taskConfiguration.getBoolean(
                                CachingStateBackendFactory.VALUE_CACHE_ENABLED_CONFIG);
                        valueHitRateThreshold = this.taskConfiguration.getDouble(
                                CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_THRESHOLD_CONFIG);
                        valueHitRateWindow = this.taskConfiguration.getLong(
                                CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG);
                        valueMinAccessesForBypassCheck = this.taskConfiguration.getLong(
                                CachingStateBackendFactory.VALUE_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG);
                        valueBypassEnabled = this.taskConfiguration.getBoolean(
                                CachingStateBackendFactory.VALUE_BYPASS_ENABLED_CONFIG);
                        writeBehindEnabled = this.taskConfiguration.getBoolean(
                                CachingStateBackendFactory.WRITE_BEHIND_ENABLED_CONFIG);
                    }
                } catch (Throwable t) {
                    // keep defaults
                }

                if (isTableWindowStack()) {
                    // writeBehindEnabled = false;
                }
                if (!valueCacheEnabled) {
                    return (S) actualStateRaw;
                }
                InternalValueState<K, N, Object> delegateValue = (InternalValueState<K, N, Object>) actualStateRaw;
                CachingInternalValueState<K, N, Object> wrapper = new CachingInternalValueState<>(
                        delegateValue,
                        this,
                        l1EntryCacheSize,
                        l2EntryCacheSize,
                        this.valueMaxActiveNamespaces,
                        this.maxCacheMemoryMb,
                        this.valueCachePolicyType,
                        valueHitRateThreshold,
                        valueHitRateWindow,
                        valueMinAccessesForBypassCheck,
                        valueBypassEnabled,
                        writeBehindEnabled,
                        this.metricGroup.addGroup("state").addGroup(stateDescriptor.getName()));
                wrapper.setCurrentNamespace(namespace);
                cachingStateToRegister = wrapper;
                break;
            }
            case MAP: {
                boolean mapCacheEnabled = taskConfiguration.get(CachingStateBackendFactory.MAP_CACHE_ENABLED_CONFIG);
                if (!mapCacheEnabled) {
                    return (S) actualStateRaw;
                }
                InternalMapState<K, N, ?, ?> delegateMap = (InternalMapState<K, N, ?, ?>) actualStateRaw;
                String stateName = stateDescriptor.getName();
                MetricGroup mapMetricsGroup = this.metricGroup.addGroup("state").addGroup(stateName);
                int l1SizeForMap = mapSpecificL1EntryCacheSize > 0 ? mapSpecificL1EntryCacheSize : l1EntryCacheSize;
                int l2SizeForMap = mapSpecificL2EntryCacheSize > 0 ? mapSpecificL2EntryCacheSize : l2EntryCacheSize;
                boolean forceBypass = false;
                try {
                    String pattern = this.taskConfiguration.getString(CachingStateBackendFactory.MAP_FORCE_BYPASS_STATES_REGEX, "");
                    forceBypass = pattern != null && stateName != null && stateName.matches(pattern);
                } catch (Throwable t) {
                    LOG.warn("Failed to parse force-bypass pattern for map state '{}'", stateName, t);
                }
                try {
                    boolean returnRawOnForceBypass = this.taskConfiguration.getBoolean(
                            CachingStateBackendFactory.MAP_FORCE_BYPASS_RETURN_RAW);
                    if (forceBypass && returnRawOnForceBypass) {
                        return (S) delegateMap;
                    }
                } catch (Throwable t) {
                    // ignore
                }
                CachingInternalMapState<K, N, Object, Object> mapWrapper = new CachingInternalMapState<>(
                        (InternalMapState<K, N, Object, Object>) delegateMap,
                        this,
                        l1SizeForMap,
                        l2SizeForMap,
                        maxActiveNamespaceOrPerKeyCacheContainers,
                        this.maxCacheMemoryMb,
                        this.mapCachePolicyType,
                        this.mapL1KeyPresenceCacheSize,
                        this.mapL2KeyPresenceCacheSize,
                        mapMetricsGroup,
                        this.mapCacheHitRateThreshold,
                        this.mapCacheHitRateWindowSize,
                        this.mapCacheMinAccessesForBypassCheck,
                        this.mapKeyPresenceCacheEnabled,
                        this.mapBypassEnabled,
                        this.mapPresenceCacheImpl,
                        this.l2ManagedMemoryEnabled,
                        this.perKeyMetricsEnabled,
                        forceBypass,
                        this.mapMaxActiveNamespaces);
                mapWrapper.setCurrentNamespace(namespace);
                cachingStateToRegister = mapWrapper;
                break;
            }
            case LIST: {
                InternalListState<K, N, Object> delegateList = (InternalListState<K, N, Object>) actualStateRaw;
                CachingInternalListState<K, N, Object> listWrapper = new CachingInternalListState<>(
                        delegateList,
                        this,
                        l1EntryCacheSize,
                        l2EntryCacheSize,
                        this.listMaxActiveNamespaces,
                        this.listCachePolicyType, this.listMaxElementsPerEntry,
                        this.listIncrementalFlushThreshold);
                listWrapper.setCurrentNamespace(namespace);
                cachingStateToRegister = listWrapper;
                break;
            }
            case AGGREGATING: {
                InternalAggregatingState<K, N, Object, Object, Object> delegateAgg =
                        (InternalAggregatingState<K, N, Object, Object, Object>) actualStateRaw;
                MetricGroup aggMetricsGroup = this.metricGroup.addGroup("state").addGroup(stateDescriptor.getName()).addGroup("cache");
                CachingInternalAggregatingState<K, N, Object, Object, Object> aggWrapper = new CachingInternalAggregatingState(
                        delegateAgg,
                        this,
                        ((AggregatingStateDescriptor) stateDescriptor).getAggregateFunction(),
                        l1EntryCacheSize,
                        l2EntryCacheSize,
                        this.aggregatingCachePolicyType,
                        aggMetricsGroup,
                        this.aggregatingMaxActiveNamespaces);
                aggWrapper.setCurrentNamespace(namespace);
                cachingStateToRegister = aggWrapper;
                break;
            }
            default:
                return (S) actualStateRaw;
        }

        synchronized (registeredStates) {
            CachingInternalState<K, ?, ?, ?> existing = stateWrapperByDelegate.get(actualStateRaw);
            if (existing != null) {
                if (existing instanceof CachingInternalValueState) {
                    ((CachingInternalValueState) existing).setCurrentNamespace(namespace);
                } else if (existing instanceof CachingInternalMapState) {
                    ((CachingInternalMapState) existing).setCurrentNamespace(namespace);
                } else if (existing instanceof CachingInternalListState) {
                    ((CachingInternalListState) existing).setCurrentNamespace(namespace);
                } else if (existing instanceof CachingInternalAggregatingState) {
                    ((CachingInternalAggregatingState) existing).setCurrentNamespace(namespace);
                }
                return (S) existing;
            }
            stateWrapperByDelegate.put(actualStateRaw, cachingStateToRegister);
            String nameKey = stateDescriptor.getType() + "#" + stateDescriptor.getName();
            stateWrapperByName.put(nameKey, cachingStateToRegister);
            boolean alreadyExists = registeredStates.stream()
                    .anyMatch(st -> st.getDelegateState() == actualStateRaw);
            if (!alreadyExists) {
                registeredStates.add(cachingStateToRegister);
            }
        }
        return (S) cachingStateToRegister;
    }

    private void registerCachingState(CachingInternalState<K, ?, ?, ?> cachingState) {
        synchronized (registeredStates) {
            boolean alreadyExists =
                    registeredStates.stream()
                            .anyMatch(st -> st.getDelegateState() == cachingState.getDelegateState());
            if (!alreadyExists) {
                registeredStates.add(cachingState);
            }
        }
    }

    // --- Methods to delegate to underlyingKeyedStateBackend ---
    // Most methods of AbstractKeyedStateBackend should be delegated. Some might require
    // interaction with the cache (e.g., snapshotting).

    @Override
    public void setCurrentKey(K newKey) {
        if (newKey == null) {
            // The CachingKeyedStateBackend manages its perception of the current key via its keyContext.
            getKeyContext().setCurrentKey(null); // InternalKeyContext can handle null.
            // Explicitly DO NOT call delegateKeyedStateBackend.setCurrentKey(null) here,
            // as AbstractKeyedStateBackend (and thus RocksDBKeyedStateBackend) throws NPE.
        } else {
            super.setCurrentKey(newKey); // This is for non-null keys, should be safe.
            if (delegateKeyedStateBackend != null) {
                delegateKeyedStateBackend.setCurrentKey(newKey);
            }
        }
    }

    @Override
    public void dispose() {
        super.dispose();
        delegateKeyedStateBackend.dispose();
        synchronized (registeredStates) {
            registeredStates.clear();
            stateWrapperByDelegate.clear();
            stateWrapperByName.clear();
        }
    }

    @Nonnull
    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull
                    StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                            snapshotTransformFactory)
            throws Exception {
        // Wrapper reuse keyed by delegate identity happens after obtaining the delegate from underlying backend.

        IS actual = delegateKeyedStateBackend.createOrUpdateInternalState(
                namespaceSerializer, stateDesc, snapshotTransformFactory);
        if (!(actual instanceof InternalKvState)) {
            return actual;
        }
        @SuppressWarnings("unchecked")
        InternalKvState<K, N, ?> actualStateRaw = (InternalKvState<K, N, ?>) actual;

        synchronized (registeredStates) {
            @SuppressWarnings("unchecked")
            IS existing = (IS) stateWrapperByDelegate.get(actualStateRaw);
            if (existing != null) {
                return existing;
            }
        }

        CachingInternalState<K, N, ?, ?> wrapper = null;
        switch (stateDesc.getType()) {
            case VALUE: {
                boolean valueCacheEnabled = true;
                double valueHitRateThreshold = 0.0;
                long valueHitRateWindow = 1000L;
                long valueMinAccessesForBypassCheck = 100L;
                boolean valueBypassEnabled = true;
                boolean writeBehindEnabled = false;
                try {
                    if (this.taskConfiguration != null) {
                        valueCacheEnabled = this.taskConfiguration.getBoolean(
                                CachingStateBackendFactory.VALUE_CACHE_ENABLED_CONFIG);
                        valueHitRateThreshold = this.taskConfiguration.getDouble(
                                CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_THRESHOLD_CONFIG);
                        valueHitRateWindow = this.taskConfiguration.getLong(
                                CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG);
                        valueMinAccessesForBypassCheck = this.taskConfiguration.getLong(
                                CachingStateBackendFactory.VALUE_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG);
                        valueBypassEnabled = this.taskConfiguration.getBoolean(
                                CachingStateBackendFactory.VALUE_BYPASS_ENABLED_CONFIG);
                        writeBehindEnabled = this.taskConfiguration.getBoolean(
                                CachingStateBackendFactory.WRITE_BEHIND_ENABLED_CONFIG);
                    }
                } catch (Throwable t) {
                    // keep defaults
                }
                if (!valueCacheEnabled) {
                    return actual;
                }
                @SuppressWarnings("unchecked")
                InternalValueState<K, N, SV> delegateValue = (InternalValueState<K, N, SV>) actualStateRaw;
                wrapper = new CachingInternalValueState<>(
                        delegateValue,
                        this,
                        l1EntryCacheSize,
                        l2EntryCacheSize,
                        this.valueMaxActiveNamespaces,
                        this.maxCacheMemoryMb,
                        this.valueCachePolicyType,
                        valueHitRateThreshold,
                        valueHitRateWindow,
                        valueMinAccessesForBypassCheck,
                        valueBypassEnabled,
                        writeBehindEnabled,
                        this.metricGroup.addGroup("state").addGroup(stateDesc.getName()));
                break;
            }
            case MAP: {
                boolean mapCacheEnabled = taskConfiguration.get(CachingStateBackendFactory.MAP_CACHE_ENABLED_CONFIG);
                if (!mapCacheEnabled) {
                    return actual;
                }
                @SuppressWarnings("unchecked")
                InternalMapState<K, N, ?, ?> delegateMap = (InternalMapState<K, N, ?, ?>) actualStateRaw;
                String stateName = stateDesc.getName();
                MetricGroup mapMetricsGroup = this.metricGroup.addGroup("state").addGroup(stateName);
                int l1SizeForMap = mapSpecificL1EntryCacheSize > 0 ? mapSpecificL1EntryCacheSize : l1EntryCacheSize;
                int l2SizeForMap = mapSpecificL2EntryCacheSize > 0 ? mapSpecificL2EntryCacheSize : l2EntryCacheSize;
            boolean forceBypass = false;
            try {
                String pattern = this.taskConfiguration.getString(CachingStateBackendFactory.MAP_FORCE_BYPASS_STATES_REGEX, "");
                forceBypass = pattern != null && stateName != null && stateName.matches(pattern);
            } catch (Throwable t) {
                LOG.warn("Failed to parse force-bypass pattern for map state '{}'", stateName, t);
            }
            try {
                boolean returnRawOnForceBypass = this.taskConfiguration.getBoolean(
                        CachingStateBackendFactory.MAP_FORCE_BYPASS_RETURN_RAW);
                if (forceBypass && returnRawOnForceBypass) {
                    return actual;
                }
            } catch (Throwable t) {
                // ignore
            }
                wrapper = new CachingInternalMapState<>(
                        delegateMap,
                        this,
                        l1SizeForMap,
                        l2SizeForMap,
                        maxActiveNamespaceOrPerKeyCacheContainers,
                        this.maxCacheMemoryMb,
                        this.mapCachePolicyType,
                        this.mapL1KeyPresenceCacheSize,
                        this.mapL2KeyPresenceCacheSize,
                        mapMetricsGroup,
                        this.mapCacheHitRateThreshold,
                        this.mapCacheHitRateWindowSize,
                        this.mapCacheMinAccessesForBypassCheck,
                        this.mapKeyPresenceCacheEnabled,
                        this.mapBypassEnabled,
                        this.mapPresenceCacheImpl,
                        this.l2ManagedMemoryEnabled,
                        this.perKeyMetricsEnabled,
                        forceBypass,
                        this.mapMaxActiveNamespaces);
                break;
            }
            case LIST: {
                @SuppressWarnings("unchecked")
                InternalListState<K, N, SV> delegateList = (InternalListState<K, N, SV>) actualStateRaw;
                wrapper = new CachingInternalListState<>(
                        delegateList,
                        this,
                        l1EntryCacheSize,
                        l2EntryCacheSize,
                        this.listMaxActiveNamespaces,
                        this.listCachePolicyType, this.listMaxElementsPerEntry,
                        this.listIncrementalFlushThreshold);
                break;
            }
            case AGGREGATING: {
                @SuppressWarnings("unchecked")
                InternalAggregatingState<K, N, Object, Object, Object> delegateAgg =
                        (InternalAggregatingState<K, N, Object, Object, Object>) actualStateRaw;
                MetricGroup aggMetricsGroup = this.metricGroup.addGroup("state").addGroup(stateDesc.getName()).addGroup("cache");
                wrapper = new CachingInternalAggregatingState(
                        delegateAgg,
                        this,
                        ((AggregatingStateDescriptor) stateDesc).getAggregateFunction(),
                        l1EntryCacheSize,
                        l2EntryCacheSize,
                        this.aggregatingCachePolicyType,
                        aggMetricsGroup,
                        this.aggregatingMaxActiveNamespaces);
                break;
            }
            default:
                return actual;
        }

        if (wrapper == null) {
            return actual;
        }

        synchronized (registeredStates) {
            @SuppressWarnings("unchecked")
            IS existing = (IS) stateWrapperByDelegate.get(actualStateRaw);
            if (existing != null) {
                return existing;
            }
            stateWrapperByDelegate.put(actualStateRaw, wrapper);
            boolean alreadyExists = registeredStates.stream()
                    .anyMatch(st -> st.getDelegateState() == actualStateRaw);
            if (!alreadyExists) {
                registeredStates.add(wrapper);
            }
        }
        @SuppressWarnings("unchecked")
        IS casted = (IS) wrapper;
        return casted;
    }

    @Nonnull
    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull
            StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                    snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws Exception {
        // Wrapper reuse keyed by delegate identity happens after obtaining the delegate from underlying backend.

        IS actual = delegateKeyedStateBackend.createOrUpdateInternalState(
                namespaceSerializer, stateDesc, snapshotTransformFactory, allowFutureMetadataUpdates);
        if (!(actual instanceof InternalKvState)) {
            return actual;
        }
        @SuppressWarnings("unchecked")
        InternalKvState<K, N, ?> actualStateRaw = (InternalKvState<K, N, ?>) actual;

        synchronized (registeredStates) {
            @SuppressWarnings("unchecked")
            IS existing = (IS) stateWrapperByDelegate.get(actualStateRaw);
            if (existing != null) {
                return existing;
            }
        }

        // Reuse the same wrapping logic as the other overload
        // Reuse the same wrapping logic as the other overload
        @SuppressWarnings("unchecked")
        IS wrapped = this.createOrUpdateInternalState(namespaceSerializer, stateDesc, snapshotTransformFactory);
        return wrapped;
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        // Use delegate queue directly to preserve semantics of isEmpty/size/iterator/remove
        return delegateKeyedStateBackend.create(stateName, byteOrderedElementSerializer);
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
    KeyGroupedInternalPriorityQueue<T> create(
            @Nonnull String stateName,
            @Nonnull TypeSerializer<T> byteOrderedElementSerializer,
            boolean allowFutureMetadataUpdates) {
        // Use delegate queue directly to preserve semantics of isEmpty/size/iterator/remove
        return delegateKeyedStateBackend
                .create(stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
    }

    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {

        synchronized (registeredStates) {
            for (CachingInternalState<K, ?, ?, ?> state : registeredStates) {
                try {
                    state.flushToUnderlyingState();
                } catch (IOException e) {
                    System.err.println(
                            "Error flushing state "
                                    + state
                                    + " during snapshot: "
                                    + e.getMessage());
                }
            }
        }
        return delegateKeyedStateBackend.snapshot(
                checkpointId, timestamp, streamFactory, checkpointOptions);
    }

    // Other delegated methods (many of them, simplified here for brevity)
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        delegateKeyedStateBackend.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        delegateKeyedStateBackend.notifyCheckpointAborted(checkpointId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        return delegateKeyedStateBackend.getKeys(state, namespace);
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String stateName) {
        return delegateKeyedStateBackend.getKeysAndNamespaces(stateName);
    }

    @VisibleForTesting
    ColumnFamilyHandle getColumnFamilyHandle(String state) {
        if (delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend) {
            return ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend)
                    .getColumnFamilyHandle(state);
        }
        throw new UnsupportedOperationException("Delegate is not a RocksDBKeyedStateBackend");
    }

    public int getKeyGroupPrefixBytes() {
        if (delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend) {
            return ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend)
                    .getKeyGroupPrefixBytes();
        }
        throw new UnsupportedOperationException("Delegate is not a RocksDBKeyedStateBackend");
    }

    @VisibleForTesting
    PriorityQueueSetFactory getPriorityQueueFactory() {
        if (delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend) {
            return ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend)
                    .getPriorityQueueFactory();
        }
        throw new UnsupportedOperationException("Delegate is not a RocksDBKeyedStateBackend");
    }

    public WriteOptions getWriteOptions() {
        if (delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend) {
            return ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend).getWriteOptions();
        }
        throw new UnsupportedOperationException("Delegate is not a RocksDBKeyedStateBackend");
    }

    public ReadOptions getReadOptions() {
        if (delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend) {
            return ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend).getReadOptions();
        }
        throw new UnsupportedOperationException("Delegate is not a RocksDBKeyedStateBackend");
    }

    SerializedCompositeKeyBuilder<K> getSharedRocksKeyBuilder() {
        if (delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend) {
            return ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend)
                    .getSharedRocksKeyBuilder();
        }
        throw new UnsupportedOperationException("Delegate is not a RocksDBKeyedStateBackend");
    }

    @VisibleForTesting
    boolean isDisposed(){
        return ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend).isDisposed();
    }

    public boolean isProfileEnabled() {
        return profileEnabled;
    }

    public int getProfileSampleRate() {
        return profileSampleRate;
    }



    @Override
    public void registerKeySelectionListener(KeySelectionListener<K> listener) {
        delegateKeyedStateBackend.registerKeySelectionListener(listener);
    }

    @Override
    public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
        return delegateKeyedStateBackend.deregisterKeySelectionListener(listener);
    }

    @Override
    public SavepointResources<K> savepoint() throws Exception {
        synchronized (registeredStates) {
            for (CachingInternalState<K, ?, ?, ?> state : registeredStates) {
                try {
                    state.flushToUnderlyingState();
                } catch (IOException e) {
                    System.err.println(
                            "Error flushing state "
                                    + state
                                    + " during savepoint: "
                                    + e.getMessage());
                }
            }
        }
        return delegateKeyedStateBackend.savepoint();
    }

    @Override
    @org.apache.flink.annotation.VisibleForTesting
    public int numKeyValueStateEntries() {
        return delegateKeyedStateBackend.numKeyValueStateEntries();
    }

    @Override
    public boolean requiresLegacySynchronousTimerSnapshots(SnapshotType checkpointType) {
        return delegateKeyedStateBackend.requiresLegacySynchronousTimerSnapshots(checkpointType);
    }

    @Override
    public boolean isSafeToReuseKVState(){
        return delegateKeyedStateBackend.isSafeToReuseKVState();
    }

    public boolean useManagedMemory() {
        // honour explicit cache-level switch
        if (l2ManagedMemoryEnabled) {
            return true;
        }

        /*
         * Fall back to the delegate decision. In Flink, {@code RocksDBKeyedStateBackend}
         * is the concrete implementation that actually consumes the managed memory quota
         * that has been reserved for the RocksDB state backend. The previous check only
         * looked for the simple substring "RocksDBStateBackend" which is not contained
         * in the concrete class name and therefore always returned {@code false}. As a
         * result, the Task was not marked as requiring managed memory and the runtime
         * handed out a zero-byte quota – hence the 0 B usage you observed.
         */
        return delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend;
    }

    @VisibleForTesting
    public void compactState(StateDescriptor<?, ?> stateDesc) throws RocksDBException {
        if (delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend) {
            ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend).compactState(stateDesc);
        } else {
            throw new UnsupportedOperationException("Delegate is not a RocksDBKeyedStateBackend");
        }
    }

    public static class CachingKvStateInfo extends RocksDBKeyedStateBackend.RocksDbKvStateInfo {
        public CachingKvStateInfo(
                ColumnFamilyHandle columnFamilyHandle,
                RegisteredStateMetaInfoBase metaInfo) {
                super(columnFamilyHandle, metaInfo);
        }
    }

    @Nonnegative
    long getWriteBatchSize() {
        if (delegateKeyedStateBackend instanceof RocksDBKeyedStateBackend) {
            return ((RocksDBKeyedStateBackend<K>) delegateKeyedStateBackend).getWriteBatchSize();
        }
        throw new UnsupportedOperationException("Delegate is not a RocksDBKeyedStateBackend");
    }

    private static LatencyTrackingStateConfig buildLatencyTrackingConfig(
            MetricGroup metricGroup, ExecutionConfig executionConfig) {
        LatencyTrackingStateConfig.Builder latencyBuilder = LatencyTrackingStateConfig.newBuilder();
        if (metricGroup != null) {
            latencyBuilder.setMetricGroup(metricGroup);
        }
        return latencyBuilder
                .setEnabled(executionConfig.getLatencyTrackingInterval() > 0)
                .build();
    }

    // Added for memory capping: To be called by CachingInternal*State when entries are added
    public void reportCacheMemoryAdded(long sizeBytes) {
        if (sizeBytes <= 0) return;
        currentEstimatedCacheSizeBytes.addAndGet(sizeBytes);
        long acc = bytesSinceLastEvictionCheck.addAndGet(sizeBytes);
        if (acc >= EVICTION_CHECK_THRESHOLD_BYTES) {
            bytesSinceLastEvictionCheck.addAndGet(-acc);
            checkAndTriggerGlobalEviction();
        }
    }

    // Added for memory capping: To be called by CachingInternal*State when entries are released
    public void reportCacheMemoryReleased(long sizeBytes) {
        if (sizeBytes <= 0) return;
        currentEstimatedCacheSizeBytes.addAndGet(-sizeBytes);
    }

    private void checkAndTriggerGlobalEviction() {
        if (maxConfiguredCacheSizeBytes <= 0) { // Memory capping disabled if limit is zero or negative
            return;
        }
        if (currentEstimatedCacheSizeBytes.get() > maxConfiguredCacheSizeBytes) {
            long memoryToFree = currentEstimatedCacheSizeBytes.get() - maxConfiguredCacheSizeBytes;
            if (memoryToFree <= 0) return; // Should not happen if check above is true, but for safety

            // System.out.println("Need to free memory: " + memoryToFree + " bytes. Current: " + currentEstimatedCacheSizeBytes.get());

            synchronized (registeredStates) { // Synchronize access to registeredStates list
                // Simple strategy: Iterate registered states and ask each to free a proportional amount or just iterate until enough is freed.
                // This could be made more sophisticated (e.g., based on state sizes, LRU of states etc.)
                long freedSoFar = 0;
                for (CachingInternalState<K, ?, ?, ?> state : registeredStates) {
                    if (freedSoFar >= memoryToFree) {
                        break;
                    }
                    // Ask state to free up to remaining needed, or its fair share
                    long remainingToFreeThisIteration = memoryToFree - freedSoFar;
                    // Simple: ask it to free up to the remaining. Could be smarter.
                    long freedByThisState = state.evictEntriesToFreeMemory(remainingToFreeThisIteration);
                    freedSoFar += freedByThisState;
                }
                // System.out.println("Freed memory: " + freedSoFar + " bytes. New current: " + currentEstimatedCacheSizeBytes.get());
            }
        }
    }

    @VisibleForTesting
    long getCurrentEstimatedCacheSizeBytesValue() {
        return currentEstimatedCacheSizeBytes.get();
    }

    @VisibleForTesting
    long getMaxConfiguredCacheSizeBytesValue() {
        return maxConfiguredCacheSizeBytes;
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

    // Heuristic: detect Table window operator on call stack to scope ValueState write-through
    private boolean isTableWindowStack() {
        // HACK: Detect Table window operator on call stack to scope ValueState write-through
        try {
            StackTraceElement[] st = Thread.currentThread().getStackTrace();
            for (StackTraceElement el : st) {
                String cn = el.getClassName();
                if (cn != null && cn.startsWith("org.apache.flink.table.runtime.operators.window")) {
                    return true;
                }
            }
        } catch (Throwable t) {
            // best-effort; default false
        }
        return false;
    }

    // This is the crucial method needed by CachingInternalMapState
    CloseableRegistry getCloseableRegistry() {
        return this.cancelStreamRegistry;
    }

    public <R> R runWithSpecificContext(
        K key,
        Object namespace, // Use Object to be generic for N
        java.util.concurrent.Callable<R> callable) throws Exception {

        K originalKey = getCurrentKey();
        Object originalNamespace = null;
        CachingInternalState<?, ?, ?, ?> stateForNs = registeredStates.stream().findFirst().orElse(null);
        if (stateForNs instanceof CachingInternalMapState) {
            originalNamespace = ((CachingInternalMapState) stateForNs).getCurrentNamespace();
        } else if (stateForNs instanceof CachingInternalValueState) {
            originalNamespace = ((CachingInternalValueState) stateForNs).getCurrentNamespace();
        }

        try {
            setCurrentKey(key);
            if (stateForNs instanceof CachingInternalMapState) {
                ((CachingInternalMapState) stateForNs).setCurrentNamespace(namespace);
            } else if (stateForNs instanceof CachingInternalValueState) {
                ((CachingInternalValueState) stateForNs).setCurrentNamespace(namespace);
            }
            return callable.call();
        } finally {
            setCurrentKey(originalKey);
            if (stateForNs instanceof CachingInternalMapState) {
                ((CachingInternalMapState) stateForNs).setCurrentNamespace(originalNamespace);
            } else if (stateForNs instanceof CachingInternalValueState) {
                ((CachingInternalValueState) stateForNs).setCurrentNamespace(originalNamespace);
            }
        }
    }
}
