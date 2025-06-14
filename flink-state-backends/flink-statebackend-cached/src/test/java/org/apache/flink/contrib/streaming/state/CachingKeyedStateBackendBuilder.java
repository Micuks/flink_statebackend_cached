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

import java.util.Collection;
import java.util.Collections;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A builder for creating {@link CachingKeyedStateBackend} instances for testing.
 */
public class CachingKeyedStateBackendBuilder<K> {

    private final AbstractKeyedStateBackend<K> delegateBackend;
    private final Configuration configuration;
    private final TypeSerializer<K> keySerializer;
    private final ClassLoader userCodeClassLoader = Thread.currentThread().getContextClassLoader();

    public CachingKeyedStateBackendBuilder(AbstractKeyedStateBackend<K> delegate, Configuration config) {
        this.delegateBackend = delegate;
        this.configuration = config;
        this.keySerializer = delegate.getKeySerializer();
    }

    public CachingKeyedStateBackend<K> build() {
        TaskKvStateRegistry kvStateRegistry = mock(TaskKvStateRegistry.class);
        ExecutionConfig executionConfig = new ExecutionConfig();
        TtlTimeProvider ttlTimeProvider = TtlTimeProvider.DEFAULT;
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        Collection<KeyedStateHandle> stateHandles = Collections.emptyList();

        // Ensure that the delegate backend has a non-null key context to avoid NPEs inside
        // AbstractKeyedStateBackend's constructor when CachingKeyedStateBackend forwards the call
        // delegateBackend.getKeyContext(). For Mockito based delegates used in tests this would
        // otherwise return null.
        try {
            if (delegateBackend.getKeyContext() == null) {
                // lazily create and inject a simple mocked InternalKeyContext
                org.apache.flink.runtime.state.heap.InternalKeyContext<K> mockedKeyContext = mock(org.apache.flink.runtime.state.heap.InternalKeyContext.class);
                when(mockedKeyContext.getNumberOfKeyGroups()).thenReturn(1);
                when(mockedKeyContext.getKeyGroupRange()).thenReturn(org.apache.flink.runtime.state.KeyGroupRange.of(0, 0));
                when(mockedKeyContext.getCurrentKeyGroupIndex()).thenReturn(0);
                when(delegateBackend.getKeyContext()).thenReturn(mockedKeyContext);
            }
        } catch (Exception ignored) {
            // If delegateBackend is a pure Mockito mock getKeyContext() may throw. In that case we
            // still inject a mocked context.
            org.apache.flink.runtime.state.heap.InternalKeyContext<K> mockedKeyContext = mock(org.apache.flink.runtime.state.heap.InternalKeyContext.class);
            when(mockedKeyContext.getNumberOfKeyGroups()).thenReturn(1);
            when(mockedKeyContext.getKeyGroupRange()).thenReturn(org.apache.flink.runtime.state.KeyGroupRange.of(0, 0));
            when(mockedKeyContext.getCurrentKeyGroupIndex()).thenReturn(0);
            when(delegateBackend.getKeyContext()).thenReturn(mockedKeyContext);
        }

        // Ensure non-null LatencyTrackingStateConfig
        try {
            if (delegateBackend.getLatencyTrackingStateConfig() == null) {
                when(delegateBackend.getLatencyTrackingStateConfig())
                        .thenReturn(org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig.disabled());
            }
        } catch (Exception ignored) {
            // in case of pure mock
            when(delegateBackend.getLatencyTrackingStateConfig())
                    .thenReturn(org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig.disabled());
        }

        int l1CacheSize = configuration.get(CachingStateBackendFactory.L1_CACHE_SIZE_CONFIG).intValue();
        int l2CacheSize = configuration.get(CachingStateBackendFactory.L2_CACHE_SIZE_CONFIG).intValue();
        int maxActiveNamespaces = configuration.get(CachingStateBackendFactory.MAX_ACTIVE_NAMESPACES_CONFIG).intValue();
        long maxCacheMemoryMb = configuration.get(CachingStateBackendFactory.MAX_CACHE_MEMORY_MB_CONFIG);
        CachingStateBackendFactory.CachePolicyType cachePolicyType = configuration.get(CachingStateBackendFactory.CACHE_POLICY_CONFIG);

        int mapL1KeyPresenceCacheSize = configuration.get(CachingStateBackendFactory.MAP_L1_KEY_PRESENCE_CACHE_SIZE_CONFIG).intValue();
        int mapL2KeyPresenceCacheSize = configuration.get(CachingStateBackendFactory.MAP_L2_KEY_PRESENCE_CACHE_SIZE_CONFIG).intValue();
        double mapCacheHitRateThreshold = configuration.get(CachingStateBackendFactory.MAP_CACHE_HIT_RATE_THRESHOLD_CONFIG);
        long mapCacheHitRateWindowSize = configuration.get(CachingStateBackendFactory.MAP_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG);
        long mapCacheMinAccessesForBypassCheck = configuration.get(CachingStateBackendFactory.MAP_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG);
        boolean mapKeyPresenceCacheEnabled = configuration.get(CachingStateBackendFactory.MAP_KEY_PRESENCE_CACHE_ENABLED_CONFIG);
        boolean mapBypassEnabled = configuration.get(CachingStateBackendFactory.MAP_BYPASS_ENABLED_CONFIG);

        double valueCacheHitRateThreshold = configuration.get(CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_THRESHOLD_CONFIG);
        long valueCacheHitRateWindowSize = configuration.get(CachingStateBackendFactory.VALUE_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG);
        long valueCacheMinAccessesForBypassCheck = configuration.get(CachingStateBackendFactory.VALUE_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG);
        boolean valueBypassEnabled = configuration.get(CachingStateBackendFactory.VALUE_BYPASS_ENABLED_CONFIG);

        boolean writeBehindEnabled = configuration.get(CachingStateBackendFactory.WRITE_BEHIND_ENABLED_CONFIG);

        return new CachingKeyedStateBackend<>(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                stateHandles,
                cancelStreamRegistry,
                delegateBackend,
                l1CacheSize,
                l2CacheSize,
                maxActiveNamespaces,
                maxCacheMemoryMb,
                cachePolicyType,
                mapL1KeyPresenceCacheSize,
                mapL2KeyPresenceCacheSize,
                mapCacheHitRateThreshold,
                mapCacheHitRateWindowSize,
                mapCacheMinAccessesForBypassCheck,
                mapKeyPresenceCacheEnabled,
                mapBypassEnabled,
                valueCacheHitRateThreshold,
                valueCacheHitRateWindowSize,
                valueCacheMinAccessesForBypassCheck,
                valueBypassEnabled,
                writeBehindEnabled);
    }
} 