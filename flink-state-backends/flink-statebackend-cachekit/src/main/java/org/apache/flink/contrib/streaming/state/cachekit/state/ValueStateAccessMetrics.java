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

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import java.util.concurrent.atomic.AtomicLong;

/** Opt-in, per-state ValueState access diagnostics for keyed aggregation workloads. */
public final class ValueStateAccessMetrics {

    private static final ValueStateAccessMetrics DISABLED = new ValueStateAccessMetrics(false);

    private final boolean enabled;
    private final AtomicLong reads = new AtomicLong();
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();
    private final AtomicLong updates = new AtomicLong();
    private final AtomicLong clears = new AtomicLong();

    private ValueStateAccessMetrics(boolean enabled) {
        this.enabled = enabled;
    }

    public static ValueStateAccessMetrics create(
            MetricGroup metricGroup, boolean enabled, String stateName) {
        if (!enabled || metricGroup == null) {
            return DISABLED;
        }

        ValueStateAccessMetrics metrics = new ValueStateAccessMetrics(true);
        MetricGroup diagnostics =
                metricGroup
                        .addGroup("cachekit")
                        .addGroup("diagnostics")
                        .addGroup("state", stateName);
        registerGauge(diagnostics, "value_state_reads", metrics.reads);
        registerGauge(diagnostics, "value_state_cache_hits", metrics.cacheHits);
        registerGauge(diagnostics, "value_state_cache_misses", metrics.cacheMisses);
        registerGauge(diagnostics, "value_state_updates", metrics.updates);
        registerGauge(diagnostics, "value_state_clears", metrics.clears);
        return metrics;
    }

    public static ValueStateAccessMetrics disabled() {
        return DISABLED;
    }

    void recordRead(boolean cacheHit) {
        if (!enabled) {
            return;
        }
        reads.incrementAndGet();
        if (cacheHit) {
            cacheHits.incrementAndGet();
        } else {
            cacheMisses.incrementAndGet();
        }
    }

    void recordUpdate() {
        if (enabled) {
            updates.incrementAndGet();
        }
    }

    void recordClear() {
        if (enabled) {
            clears.incrementAndGet();
        }
    }

    private static void registerGauge(MetricGroup metricGroup, String name, AtomicLong value) {
        metricGroup.gauge(name, (Gauge<Long>) value::get);
    }
}
