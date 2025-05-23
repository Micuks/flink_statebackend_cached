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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.contrib.streaming.state.CachingStateBackendFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Tests for metrics exposed by {@link CachingKeyedStateBackend} and its components.
 */
@Disabled("Placeholder for CachingKeyedStateBackend metric tests")
class CachingKeyedStateBackendMetricTest {

    @TempDir
    Path temporaryFolder;

    private CachingKeyedStateBackend<String> cachingBackend;
    private TestTaskStateManager taskStateManager;
    private UnregisteredMetricsGroup metricsGroup;
    private AbstractKeyedStateBackend<String> delegateBackend;

    @BeforeEach
    void setUp() throws Exception {
        metricsGroup = new UnregisteredMetricsGroup();
        taskStateManager = new TestTaskStateManager();
        Environment mockEnv = MockEnvironment.builder().build();

        // Using MemoryStateBackend as a lightweight delegate for metrics tests
        delegateBackend = new MemoryStateBackend().createKeyedStateBackend(mockEnv, new JobID(),
                "testOperator", StringSerializer.INSTANCE, 1, new KeyGroupRange(0, 0),
                mockEnv.getTaskKvStateRegistry(), TtlTimeProvider.DEFAULT, metricsGroup,
                Collections.emptyList(), new CloseableRegistry());

        cachingBackend = new CachingKeyedStateBackend<String>(mockEnv.getTaskKvStateRegistry(),
                StringSerializer.INSTANCE, mockEnv.getUserCodeClassLoader().asClassLoader(),
                new ExecutionConfig(), TtlTimeProvider.DEFAULT, metricsGroup,
                Collections.emptyList(), new CloseableRegistry(), delegateBackend, 5, 5, 2, 1L,
                CachingStateBackendFactory.CachePolicyType.LRU);
        cachingBackend.setCurrentKey("testKey");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (cachingBackend != null) {
            cachingBackend.dispose();
        }
        if (delegateBackend != null) {
            delegateBackend.dispose();
        }
        if (taskStateManager != null) {
            taskStateManager.close();
        }
    }

    @Test
    void testL1CacheHitMissMetrics() throws Exception {
        // TODO: Requires CachingInternalMapState (and others) to expose metrics for L1/L2
        // hits/misses.
        // Example:
        // Gauge<Long> l1Hits = metricsGroup.gauge("l1CacheHits", () -> ...);
        // Gauge<Long> l1Misses = metricsGroup.gauge("l1CacheMisses", () -> ...);

        // ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("metricValueState",
        // String.class);
        // ValueState<String> valueState =
        // cachingBackend.getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, descriptor);

        // valueState.value(); // Expected L1 miss, L2 miss, delegate fetch
        // assertEquals(1L, l1Misses.getValue());

        // valueState.update("val1");
        // valueState.value(); // Expected L1 hit
        // assertEquals(1L, l1Hits.getValue());
    }

    @Test
    void testL2CacheHitMissMetrics() throws Exception {
        // TODO: Similar to L1, requires CachingInternalMapState (and others) to expose L2 metrics.
    }

    @Test
    void testCacheEvictionMetrics() throws Exception {
        // TODO: Requires LRUMap or PerKeyMapCache to expose eviction counts.
        // Gauge<Long> l1Evictions = metricsGroup.gauge("l1CacheEvictions", () -> ...);
    }

    @Test
    void testCacheSizeMetrics() throws Exception {
        // TODO: Requires PerKeyMapCache/LRUMap to expose current size.
        // Gauge<Long> l1CacheSize = metricsGroup.gauge("l1CacheSize", () -> ...);
    }

    // Add more tests for other relevant metrics as they are implemented.
}
