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

        public static final ConfigOption<Double> VALUE_CACHE_L1_RATIO = ConfigOptions
                        .key("state.backend.cachekit.value.cache.l1.ratio")
                        .doubleType()
                        .defaultValue(0.2)
                        .withDescription("Ratio of max-entries allocated to L1 cache (0.0 to 1.0).");

        public static final ConfigOption<Boolean> VALUE_BYPASS_ENABLED = ConfigOptions
                        .key("state.backend.cachekit.value.bypass.enabled")
                        .booleanType()
                        .defaultValue(true)
                        .withDescription("Enable adaptive bypass for ValueState caching based on hit rate.");

        public static final ConfigOption<Long> VALUE_BYPASS_MIN_ACCESSES = ConfigOptions
                        .key("state.backend.cachekit.value.bypass.min-accesses")
                        .longType()
                        .defaultValue(1000L)
                        .withDescription("Minimum accesses before bypass decisions are applied.");

        public static final ConfigOption<Integer> VALUE_BYPASS_SAMPLE_EVERY = ConfigOptions
                        .key("state.backend.cachekit.value.bypass.sample-every")
                        .intType()
                        .defaultValue(512)
                        .withDescription("Sample every N accesses while bypassing to re-evaluate hit rate.");

        public static final ConfigOption<Double> VALUE_BYPASS_HYSTERESIS = ConfigOptions
                        .key("state.backend.cachekit.value.bypass.hysteresis")
                        .doubleType()
                        .defaultValue(0.10)
                        .withDescription(
                                        "Hysteresis applied to hit-rate threshold to avoid thrashing. "
                                                        + "The enter-bypass threshold is (threshold - hysteresis).");

        public static final ConfigOption<Integer> VALUE_BYPASS_COOLDOWN_WINDOWS = ConfigOptions
                        .key("state.backend.cachekit.value.bypass.cooldown-windows")
                        .intType()
                        .defaultValue(2)
                        .withDescription("Cooldown windows after a bypass toggle before another switch is allowed.");

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
                final double l1Ratio =
                                Math.max(0.0, Math.min(1.0, config.get(VALUE_CACHE_L1_RATIO)));
                final boolean bypassEnabled = config.get(VALUE_BYPASS_ENABLED);
                final long bypassMinAccesses = Math.max(0L, config.get(VALUE_BYPASS_MIN_ACCESSES));
                final int bypassSampleEvery = Math.max(1, config.get(VALUE_BYPASS_SAMPLE_EVERY));
                final double bypassHysteresis = Math.max(0.0, Math.min(1.0, config.get(VALUE_BYPASS_HYSTERESIS)));
                final int bypassCooldownWindows = Math.max(0, config.get(VALUE_BYPASS_COOLDOWN_WINDOWS));
                final double hitRateThreshold = config.get(VALUE_HIT_RATE_THRESHOLD);
                final int hitRateWindow = Math.max(1, config.get(VALUE_HIT_RATE_WINDOW));
                final String delegateClass = config.get(DELEGATE_BACKEND);

                System.out.printf(
                                "CacheKit Factory: maxEntries=%d, l1Ratio=%.2f, policy=%s, lruOverflow=%d, bypass=%s, minAccesses=%d, sampleEvery=%d, hysteresis=%.2f, cooldown=%d, threshold=%.2f, window=%d, delegate=%s%n",
                                maxEntries, l1Ratio, policyType, lruOverflow, bypassEnabled, bypassMinAccesses,
                                bypassSampleEvery, bypassHysteresis, bypassCooldownWindows, hitRateThreshold,
                                hitRateWindow, delegateClass);

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
                                l1Ratio,
                                bypassEnabled,
                                bypassMinAccesses,
                                bypassSampleEvery,
                                bypassHysteresis,
                                bypassCooldownWindows,
                                hitRateThreshold,
                                hitRateWindow);
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
