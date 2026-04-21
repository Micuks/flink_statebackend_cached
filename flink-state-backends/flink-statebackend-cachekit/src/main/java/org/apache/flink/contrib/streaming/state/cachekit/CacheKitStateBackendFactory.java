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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PresenceCacheImplementation;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;

import java.io.IOException;

/**
 * A minimal factory for creating {@link CacheKitStateBackend} instances.
 *
 * <p>
 * This is intentionally small and only exposes a single feature to start with:
 * LRU caching for {@code ValueState}.
 */
public class CacheKitStateBackendFactory implements StateBackendFactory<CacheKitStateBackend> {

        public static final ConfigOption<Integer> VALUE_CACHE_MAX_ENTRIES = ConfigOptions
                        .key("state.backend.cachekit.value.cache.max-entries")
                        .intType()
                        .defaultValue(1024)
                        .withDescription("Max entries for per-ValueState LRU cache.");

        public static final ConfigOption<CachePolicyType> VALUE_CACHE_POLICY = ConfigOptions
                        .key("state.backend.cachekit.value.cache.policy")
                        .enumType(CachePolicyType.class)
                        .defaultValue(CachePolicyType.LRU)
                        .withDescription("Cache policy for ValueState (LRU or CAFFEINE).");

        public static final ConfigOption<Integer> VALUE_CACHE_LRU_OVERFLOW = ConfigOptions
                        .key("state.backend.cachekit.value.cache.lru.overflow")
                        .intType()
                        .defaultValue(256)
                        .withDescription(
                                        "Overflow entries for LRU before batch eviction triggers.");

        public static final ConfigOption<Boolean> VALUE_BYPASS_ENABLED = ConfigOptions
                        .key("state.backend.cachekit.value.bypass.enabled")
                        .booleanType()
                        .defaultValue(true)
                        .withDescription("Enable adaptive bypass for ValueState caching based on hit rate.");

        public static final ConfigOption<Double> VALUE_HIT_RATE_THRESHOLD = ConfigOptions
                        .key("state.backend.cachekit.value.hit-rate.threshold")
                        .doubleType()
                        .defaultValue(0.05)
                        .withDescription(
                                        "Hit rate threshold (0.0 to 1.0) below which cache is bypassed. Default 0.05 (5%).");

        public static final ConfigOption<Integer> VALUE_HIT_RATE_WINDOW = ConfigOptions
                        .key("state.backend.cachekit.value.hit-rate.window")
                        .intType()
                        .defaultValue(1000)
                        .withDescription("Number of accesses to calculate hit rate over.");

        public static final ConfigOption<Integer> MAP_PRESENCE_CACHE_MAX_ENTRIES = ConfigOptions
                        .key("state.backend.cachekit.map.presence.cache.max-entries")
                        .intType()
                        .defaultValue(8192)
                        .withDescription("Max entries for per-MapState key presence cache.");

        public static final ConfigOption<CachePolicyType> MAP_PRESENCE_CACHE_POLICY = ConfigOptions
                        .key("state.backend.cachekit.map.presence.cache.policy")
                        .enumType(CachePolicyType.class)
                        .defaultValue(CachePolicyType.LRU)
                        .withDescription("Cache policy for MapState key presence cache (LRU or CAFFEINE).");

        public static final ConfigOption<Integer> MAP_PRESENCE_CACHE_LRU_OVERFLOW = ConfigOptions
                        .key("state.backend.cachekit.map.presence.cache.lru.overflow")
                        .intType()
                        .defaultValue(256)
                        .withDescription(
                                        "Overflow entries for MapState key presence LRU before batch eviction triggers.");

        public static final ConfigOption<PresenceCacheImplementation> MAP_PRESENCE_CACHE_IMPLEMENTATION = ConfigOptions
                        .key("state.backend.cachekit.map.presence.cache.impl")
                        .enumType(PresenceCacheImplementation.class)
                        .defaultValue(PresenceCacheImplementation.PRIMITIVE)
                        .withDescription(
                                        "Presence cache implementation for MapState (PRIMITIVE or OBJECT).");

        public static final ConfigOption<Integer> MAP_CACHE_MAX_ENTRIES = ConfigOptions
                        .key("state.backend.cachekit.map.cache.max-entries")
                        .intType()
                        .defaultValue(4096)
                        .withDescription("Max entries for per-MapState cache.");

        public static final ConfigOption<CachePolicyType> MAP_CACHE_POLICY = ConfigOptions
                        .key("state.backend.cachekit.map.cache.policy")
                        .enumType(CachePolicyType.class)
                        .defaultValue(CachePolicyType.LRU)
                        .withDescription("Cache policy for MapState (LRU or CAFFEINE).");

        public static final ConfigOption<Integer> MAP_CACHE_LRU_OVERFLOW = ConfigOptions
                        .key("state.backend.cachekit.map.cache.lru.overflow")
                        .intType()
                        .defaultValue(256)
                        .withDescription(
                                        "Overflow entries for MapState LRU before batch eviction triggers.");

        public static final ConfigOption<Boolean> MAP_BYPASS_ENABLED = ConfigOptions
                        .key("state.backend.cachekit.map.bypass.enabled")
                        .booleanType()
                        .defaultValue(false)
                        .withDescription("Enable adaptive bypass for MapState caching based on hit rate.");

        public static final ConfigOption<Double> MAP_HIT_RATE_THRESHOLD = ConfigOptions
                        .key("state.backend.cachekit.map.hit-rate.threshold")
                        .doubleType()
                        .defaultValue(0.05)
                        .withDescription(
                                        "Hit rate threshold (0.0 to 1.0) below which MapState cache is bypassed.");

        public static final ConfigOption<Integer> MAP_HIT_RATE_WINDOW = ConfigOptions
                        .key("state.backend.cachekit.map.hit-rate.window")
                        .intType()
                        .defaultValue(1000)
                        .withDescription("Number of MapState accesses to calculate hit rate over.");

        public static final ConfigOption<Boolean> MAP_ITERATION_CACHE_FILL_ENABLED = ConfigOptions
                        .key("state.backend.cachekit.map.iteration.cache-fill.enabled")
                        .booleanType()
                        .defaultValue(true)
                        .withDescription("Enable cache backfill during MapState iteration.");

        public static final ConfigOption<Integer> MAP_SNAPSHOT_CACHE_MAX_ENTRIES = ConfigOptions
                        .key("state.backend.cachekit.map.snapshot.cache.max-entries")
                        .intType()
                        .defaultValue(0)
                        .withDescription(
                                        "Max entries for per-MapState snapshot cache (entries() fast path). "
                                                        + "Caches (Key, Namespace) -> {EMPTY | SINGLE(UserKey)} to short-circuit "
                                                        + "entries()/iterator() calls. Set 0 to disable.");

        public static final ConfigOption<String> DELEGATE_BACKEND = ConfigOptions.key("state.backend.cachekit.delegate")
                        .stringType()
                        .noDefaultValue()
                        .withDescription(
                                        "Optional fully-qualified StateBackend class name used as delegate. "
                                                        + "If absent, EmbeddedRocksDBStateBackend is used.");

        @Override
        public CacheKitStateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader)
                        throws IOException {
                final int maxEntries = Math.max(0, config.get(VALUE_CACHE_MAX_ENTRIES));
                final CachePolicyType policyType = config.get(VALUE_CACHE_POLICY);
                final int lruOverflow = Math.max(0, config.get(VALUE_CACHE_LRU_OVERFLOW));
                final boolean bypassEnabled = config.get(VALUE_BYPASS_ENABLED);
                final double hitRateThreshold = config.get(VALUE_HIT_RATE_THRESHOLD);
                final int hitRateWindow = config.get(VALUE_HIT_RATE_WINDOW);
                final int mapPresenceMaxEntries = Math.max(0, config.get(MAP_PRESENCE_CACHE_MAX_ENTRIES));
                final CachePolicyType mapPresencePolicy = config.get(MAP_PRESENCE_CACHE_POLICY);
                final int mapPresenceLruOverflow = Math.max(0, config.get(MAP_PRESENCE_CACHE_LRU_OVERFLOW));
                final PresenceCacheImplementation mapPresenceImpl = config.get(MAP_PRESENCE_CACHE_IMPLEMENTATION);
                final int mapCacheMaxEntries = Math.max(0, config.get(MAP_CACHE_MAX_ENTRIES));
                final CachePolicyType mapCachePolicy = config.get(MAP_CACHE_POLICY);
                final int mapCacheLruOverflow = Math.max(0, config.get(MAP_CACHE_LRU_OVERFLOW));
                final boolean mapBypassEnabled = config.get(MAP_BYPASS_ENABLED);
                final double mapHitRateThreshold = config.get(MAP_HIT_RATE_THRESHOLD);
                final int mapHitRateWindow = config.get(MAP_HIT_RATE_WINDOW);
                final boolean mapIterationCacheFillEnabled = config.get(MAP_ITERATION_CACHE_FILL_ENABLED);
                final int mapSnapshotMaxEntries = Math.max(0, config.get(MAP_SNAPSHOT_CACHE_MAX_ENTRIES));
                final String delegateClass = config.get(DELEGATE_BACKEND);

                System.out.printf(
                                "CacheKit Factory: maxEntries=%d, policy=%s, lruOverflow=%d, bypass=%s, threshold=%.2f, window=%d, mapPresenceMax=%d, mapPresencePolicy=%s, mapPresenceOverflow=%d, mapPresenceImpl=%s, mapCacheMax=%d, mapCachePolicy=%s, mapCacheOverflow=%d, mapBypass=%s, mapHitThreshold=%.2f, mapHitWindow=%d, mapIterFill=%s, mapSnapshotMax=%d, delegate=%s%n",
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
                                delegateClass);

                StateBackend delegate;
                if (delegateClass == null || delegateClass.isBlank()) {
                        // Default to RocksDB using the factory pattern
                        try {
                                RocksDBStateBackendFactory rocksFactory = new RocksDBStateBackendFactory();
                                delegate = rocksFactory.createFromConfig(config, classLoader);
                        } catch (org.apache.flink.configuration.IllegalConfigurationException e) {
                                throw e;
                        } catch (Exception e) {
                                System.err.println(
                                                "Failed to create RocksDBStateBackend, falling back to HashMapStateBackend: "
                                                                + e.getMessage());
                                delegate = new HashMapStateBackend();
                        }
                } else {
                        delegate = instantiateBackend(delegateClass, classLoader);
                }

                return new CacheKitStateBackend(
                                delegate,
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
                                mapSnapshotMaxEntries);
        }

        private static StateBackend instantiateBackend(String className, ClassLoader classLoader) {
                try {
                        Class<?> clazz = Class.forName(className, true, classLoader);
                        Object instance = clazz.getDeclaredConstructor().newInstance();
                        if (!(instance instanceof StateBackend)) {
                                throw new IllegalArgumentException(
                                                "Configured delegate backend class does not implement StateBackend: "
                                                                + className);
                        }
                        return (StateBackend) instance;
                } catch (Exception e) {
                        throw new IllegalArgumentException(
                                        "Failed to instantiate delegate backend: " + className, e);
                }
        }
}
