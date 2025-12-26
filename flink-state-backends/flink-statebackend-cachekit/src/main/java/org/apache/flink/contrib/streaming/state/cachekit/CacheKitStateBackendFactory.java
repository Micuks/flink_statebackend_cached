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
                final String delegateClass = config.get(DELEGATE_BACKEND);

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

                return new CacheKitStateBackend(delegate, maxEntries, policyType, lruOverflow);
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
