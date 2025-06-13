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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;
import java.util.Collections;

/**
 * Test utilities for creating instances of Caching-related state backends.
 */
public class CachingStateBackendTestUtils {

    public static Configuration createConfig(boolean async, CachingStateBackendFactory.CachePolicyType policy) {
        Configuration config = new Configuration();
        config.set(CachingStateBackendFactory.L1_CACHE_SIZE_CONFIG, 10L);
        config.set(CachingStateBackendFactory.L2_CACHE_SIZE_CONFIG, 20L);
        config.set(CachingStateBackendFactory.MAX_ACTIVE_NAMESPACES_CONFIG, 5L);
        config.set(CachingStateBackendFactory.MAX_CACHE_MEMORY_MB_CONFIG, 1L);
        config.set(CachingStateBackendFactory.CACHE_POLICY_CONFIG, policy);
        return config;
    }
} 