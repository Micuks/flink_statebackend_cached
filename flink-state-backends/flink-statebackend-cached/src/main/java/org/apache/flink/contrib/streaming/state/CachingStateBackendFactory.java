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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackendFactory;

/**
 * A factory for creating {@link CachingStateBackend} instances. This factory allows configuring the
 * CachingStateBackend with specific cache sizes and a delegate state backend (defaulting to
 * RocksDBStateBackend if not specified).
 */
public class CachingStateBackendFactory implements StateBackendFactory<CachingStateBackend> {

    // Configuration keys for cache sizes
    public static final String L1_CACHE_SIZE_KEY_OLD_STRING = "state.backend.cached.l1.size";
    public static final String L2_CACHE_SIZE_KEY_OLD_STRING = "state.backend.cached.l2.size";

    public static final ConfigOption<Long> L1_CACHE_SIZE_CONFIG =
            ConfigOptions.key("state.backend.cached.l1.size.entries")
                    .longType()
                    .defaultValue(1024L)
                    .withDescription(
                            "The number of entries for the L1 cache per state instance (e.g., per keyed ValueState, or per user key in MapState).");

    public static final ConfigOption<Long> L2_CACHE_SIZE_CONFIG =
            ConfigOptions.key("state.backend.cached.l2.size.entries")
                    .longType()
                    .defaultValue(8192L)
                    .withDescription("The number of entries for the L2 cache per state instance.");

    public static final ConfigOption<Long> MAX_ACTIVE_NAMESPACES_CONFIG =
            ConfigOptions.key("state.backend.cached.max.active.namespaces")
                    .longType()
                    .defaultValue(100L)
                    .withDescription(
                            "The maximum number of active namespaces (or Flink Key for MapState) whose caches are kept in memory.");

    public static final ConfigOption<Long> MAX_CACHE_MEMORY_MB_CONFIG =
            ConfigOptions.key("state.backend.cached.max.memory.mb")
                    .longType()
                    .defaultValue(20L) // Default to 20MB
                    .withDescription(
                            "The maximum total memory in megabytes for all caches in this backend instance.");

    public static final ConfigOption<Long> MAP_L1_KEY_PRESENCE_CACHE_SIZE_CONFIG =
            ConfigOptions.key("state.backend.cached.map.l1.key-presence.size.entries")
                    .longType()
                    .defaultValue(2048L) // Default, can be tuned
                    .withDescription(
                            "The number of entries for the L1 key presence cache per MapState instance (per Flink key/namespace). Stores boolean presence.");

    public static final ConfigOption<Long> MAP_L2_KEY_PRESENCE_CACHE_SIZE_CONFIG =
            ConfigOptions.key("state.backend.cached.map.l2.key-presence.size.entries")
                    .longType()
                    .defaultValue(8192L) // Default, can be tuned
                    .withDescription(
                            "The number of entries for the L2 key presence cache per MapState instance (per Flink key/namespace). Stores boolean presence.");

    public enum CachePolicyType {
            LRU, TINYLFU
    }

    public static final ConfigOption<CachePolicyType> CACHE_POLICY_CONFIG =
                    ConfigOptions.key("state.backend.cached.policy").enumType(CachePolicyType.class)
                                    .defaultValue(CachePolicyType.LRU)
                                    .withDescription("The caching policy to use (LRU or TINYLFU).");

    // Per-state cache policy configuration (falls back to global CACHE_POLICY_CONFIG when unset)
    public static final ConfigOption<CachePolicyType> MAP_CACHE_POLICY_CONFIG =
            ConfigOptions.key("state.backend.cached.map.policy")
                    .enumType(CachePolicyType.class)
                    .defaultValue(CachePolicyType.LRU)
                    .withDescription("Cache policy for MapState (overrides global policy if set).");

    public static final ConfigOption<CachePolicyType> VALUE_CACHE_POLICY_CONFIG =
            ConfigOptions.key("state.backend.cached.value.policy")
                    .enumType(CachePolicyType.class)
                    .defaultValue(CachePolicyType.LRU)
                    .withDescription("Cache policy for ValueState (overrides global policy if set).");

    public static final ConfigOption<CachePolicyType> LIST_CACHE_POLICY_CONFIG =
            ConfigOptions.key("state.backend.cached.list.policy")
                    .enumType(CachePolicyType.class)
                    .defaultValue(CachePolicyType.LRU)
                    .withDescription("Cache policy for ListState (overrides global policy if set).");

    public static final ConfigOption<CachePolicyType> AGGREGATING_CACHE_POLICY_CONFIG =
            ConfigOptions.key("state.backend.cached.aggregating.policy")
                    .enumType(CachePolicyType.class)
                    .defaultValue(CachePolicyType.LRU)
                    .withDescription("Cache policy for AggregatingState (overrides global policy if set).");

    public static final ConfigOption<Double> MAP_CACHE_HIT_RATE_THRESHOLD_CONFIG =
            ConfigOptions.key("state.backend.cached.map.hit-rate.threshold")
                    .doubleType()
                    .defaultValue(0.0) // Disabled by default
                    .withDescription(
                            "Hit rate threshold for CachingInternalMapState (0.0 to 1.0). If the hit rate falls below this, the cache is bypassed. 0.0 disables this feature.");

    public static final ConfigOption<Long> MAP_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG =
            ConfigOptions.key("state.backend.cached.map.hit-rate.window-size")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription(
                            "Number of accesses (get/contains operations) in CachingInternalMapState to calculate hit rate for bypass decisions.");

    public static final ConfigOption<Long> MAP_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG =
            ConfigOptions.key("state.backend.cached.map.min-accesses-for-bypass-check")
                    .longType()
                    .defaultValue(100L)
                    .withDescription(
                            "Minimum number of accesses (get/contains operations) in CachingInternalMapState before the hit rate bypass check becomes active.");

    public static final ConfigOption<Boolean> MAP_CACHE_ENABLED_CONFIG =
            ConfigOptions.key("state.backend.cached.map.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable caching for MapState. If false, MapState will not be wrapped by caching layer.");

    public static final ConfigOption<Boolean> MAP_KEY_PRESENCE_CACHE_ENABLED_CONFIG =
            ConfigOptions.key("state.backend.cached.map.key-presence.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable key presence cache for MapState to optimize contains() and get() operations.");

    public static final ConfigOption<Boolean> MAP_BYPASS_ENABLED_CONFIG =
            ConfigOptions.key("state.backend.cached.map.bypass.enabled")
                    .booleanType()
                    .defaultValue(true);

    // Value state caching configuration
    public static final ConfigOption<Boolean> VALUE_CACHE_ENABLED_CONFIG =
            ConfigOptions.key("state.backend.cached.value.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable caching for ValueState. If false, ValueState will not be wrapped by caching layer.");

    public static final ConfigOption<Double> VALUE_CACHE_HIT_RATE_THRESHOLD_CONFIG =
            ConfigOptions.key("state.backend.cached.value.hit-rate.threshold")
                    .doubleType()
                    .defaultValue(0.0)
                    .withDescription("Hit rate threshold for CachingInternalValueState (0.0 to 1.0). 0.0 disables adaptive bypass.");

    public static final ConfigOption<Long> VALUE_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG =
            ConfigOptions.key("state.backend.cached.value.hit-rate.window-size")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription("Number of accesses in CachingInternalValueState to calculate hit rate for bypass decisions.");

    public static final ConfigOption<Long> VALUE_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG =
            ConfigOptions.key("state.backend.cached.value.min-accesses-for-bypass-check")
                    .longType()
                    .defaultValue(100L)
                    .withDescription("Minimum number of accesses in CachingInternalValueState before bypass check becomes active.");

    public static final ConfigOption<Boolean> VALUE_BYPASS_ENABLED_CONFIG =
            ConfigOptions.key("state.backend.cached.value.bypass.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable adaptive bypass for ValueState caching based on hit rate.");

    // Write-behind configuration (used by ValueState caching)
    public static final ConfigOption<Boolean> WRITE_BEHIND_ENABLED_CONFIG =
            ConfigOptions.key("state.backend.cached.write-behind.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable write-behind (defer writes to underlying backend) for cached states where applicable.");

    public static final ConfigOption<Boolean> L2_MANAGED_MEMORY_ENABLED_CONFIG =
            ConfigOptions.key("state.backend.cached.l2.managed.enable")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable managed memory for L2 cache.");

    // Time-bucket size for L2 off-heap pages (in milliseconds). When > 0, new writes are grouped
    // into pages by time bucket to enable O(1) bucket evictions on watermark/time progression.
    public static final ConfigOption<Long> MAP_L2_TIME_BUCKET_SIZE_MILLIS =
            ConfigOptions.key("state.backend.cached.map.l2.time-bucket.size")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("Time bucket size (ms) for L2 off-heap cache pages; 0 disables time-bucketed eviction.");

    public enum PresenceCacheImplementation {
        DEFAULT,
        PRIMITIVE_MAP
    }

    public static final ConfigOption<PresenceCacheImplementation> MAP_PRESENCE_CACHE_IMPL =
            ConfigOptions.key("state.backend.cached.map.presence.impl")
                    .enumType(PresenceCacheImplementation.class)
                    .defaultValue(PresenceCacheImplementation.DEFAULT)
                    .withDescription("Implementation for MapState key presence cache. PRIMITIVE_MAP uses a more memory-efficient implementation.");

    // Per-key metrics can explode cardinality causing large pushes; default disabled.
    public static final ConfigOption<Boolean> MAP_PER_KEY_METRICS_ENABLED =
            ConfigOptions.key("state.backend.cached.map.metrics.per-key.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable per-key/namespace cache metrics (high-cardinality). Default false. Aggregated metrics are recommended.");

    // Potentially, a config for delegate backend factory if it's not hardcoded to RocksDB
    // For now, assumes RocksDBStateBackend is the default delegate and is configured using its own
    // factory/options.

    @Override
    public CachingStateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader)
            throws IllegalStateException, java.io.IOException {
        

        // Create the delegate backend. Default to RocksDBStateBackend for now.
        // A more flexible approach might allow specifying the delegate factory in config.
        StateBackend delegateBackend;
        try {
            // Attempt to create RocksDBStateBackend using its factory and current config
            // This assumes RocksDBStateBackendFactory is available and configured as usual
            RocksDBStateBackendFactory rocksFactory = new RocksDBStateBackendFactory();
            delegateBackend = rocksFactory.createFromConfig(config, classLoader);
    } catch (org.apache.flink.configuration.IllegalConfigurationException e) {
            // Propagate configuration errors as the test expects this.
            throw e;
        } catch (Exception e) {
            System.err.println(
                            "Failed to configure underlying RocksDBStateBackend from factory (non-configuration error), falling back to MemoryStateBackend for CachingStateBackend. Error: "
                            + e.getMessage());
            System.err.println(
                            "WARNING: CachingStateBackend is using MemoryStateBackend as delegate due to RocksDB setup failure. THIS IS A FALLBACK.");
            delegateBackend = new MemoryStateBackendFactory().createFromConfig(config, classLoader);
        }

        // The CachingStateBackend constructor expects StateBackend.
        // The check for AbstractKeyedStateBackend is more relevant for createKeyedStateBackend
        // logic within CachingStateBackend itself.
        // if (!(delegateBackend
        // instanceof org.apache.flink.runtime.state.AbstractKeyedStateBackend)) {
        // throw new IllegalStateException(
        // "CachingStateBackend requires a delegate backend that is an instance of
        // AbstractKeyedStateBackend.");
        // }

        // Call the CachingStateBackend constructor with matching types (StateBackend, long, long,
        // long)
        // delegateBackend is already StateBackend. l1CacheSize, l2CacheSize, maxActiveNamespaces
        // are already long.
        return new CachingStateBackend(delegateBackend, config);
    }
}
