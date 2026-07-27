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

/**
 * Opt-in ListState cardinality diagnostics.
 *
 * <p>Metrics are registered per state name. This keeps the two sides of window joins (for example
 * {@code left-records} and {@code right-records}) separate while showing how many rows each
 * key/window contains when it is read.
 */
public final class ListStateDistributionMetrics {

    private static final ListStateDistributionMetrics DISABLED =
            new ListStateDistributionMetrics(false);

    private final boolean enabled;
    private final AtomicLong addCalls = new AtomicLong();
    private final AtomicLong addedElements = new AtomicLong();
    private final AtomicLong getCalls = new AtomicLong();
    private final AtomicLong readElements = new AtomicLong();
    private final AtomicLong readCardinality0 = new AtomicLong();
    private final AtomicLong readCardinality1 = new AtomicLong();
    private final AtomicLong readCardinality2To4 = new AtomicLong();
    private final AtomicLong readCardinality5To16 = new AtomicLong();
    private final AtomicLong readCardinality17To64 = new AtomicLong();
    private final AtomicLong readCardinality65To256 = new AtomicLong();
    private final AtomicLong readCardinality257Plus = new AtomicLong();
    private final AtomicLong maxReadCardinality = new AtomicLong();
    private final AtomicLong clearCalls = new AtomicLong();

    private ListStateDistributionMetrics(boolean enabled) {
        this.enabled = enabled;
    }

    public static ListStateDistributionMetrics create(
            MetricGroup metricGroup, boolean enabled, String stateName) {
        if (!enabled || metricGroup == null) {
            return DISABLED;
        }

        ListStateDistributionMetrics metrics = new ListStateDistributionMetrics(true);
        MetricGroup diagnostics =
                metricGroup
                        .addGroup("cachekit")
                        .addGroup("diagnostics")
                        .addGroup("state", stateName);
        registerGauge(diagnostics, "list_state_add_calls", metrics.addCalls);
        registerGauge(diagnostics, "list_state_added_elements", metrics.addedElements);
        registerGauge(diagnostics, "list_state_get_calls", metrics.getCalls);
        registerGauge(diagnostics, "list_state_read_elements", metrics.readElements);
        registerGauge(
                diagnostics, "list_state_read_cardinality_0", metrics.readCardinality0);
        registerGauge(
                diagnostics, "list_state_read_cardinality_1", metrics.readCardinality1);
        registerGauge(
                diagnostics, "list_state_read_cardinality_2_4", metrics.readCardinality2To4);
        registerGauge(
                diagnostics, "list_state_read_cardinality_5_16", metrics.readCardinality5To16);
        registerGauge(
                diagnostics, "list_state_read_cardinality_17_64", metrics.readCardinality17To64);
        registerGauge(
                diagnostics, "list_state_read_cardinality_65_256", metrics.readCardinality65To256);
        registerGauge(
                diagnostics,
                "list_state_read_cardinality_257_plus",
                metrics.readCardinality257Plus);
        registerGauge(
                diagnostics, "list_state_max_read_cardinality", metrics.maxReadCardinality);
        registerGauge(diagnostics, "list_state_clear_calls", metrics.clearCalls);
        return metrics;
    }

    public static ListStateDistributionMetrics disabled() {
        return DISABLED;
    }

    void recordAdd(int count) {
        if (!enabled) {
            return;
        }
        addCalls.incrementAndGet();
        addedElements.addAndGet(count);
    }

    void recordRead(int cardinality) {
        if (!enabled) {
            return;
        }
        getCalls.incrementAndGet();
        readElements.addAndGet(cardinality);
        maxReadCardinality.accumulateAndGet(cardinality, Math::max);
        if (cardinality == 0) {
            readCardinality0.incrementAndGet();
        } else if (cardinality == 1) {
            readCardinality1.incrementAndGet();
        } else if (cardinality <= 4) {
            readCardinality2To4.incrementAndGet();
        } else if (cardinality <= 16) {
            readCardinality5To16.incrementAndGet();
        } else if (cardinality <= 64) {
            readCardinality17To64.incrementAndGet();
        } else if (cardinality <= 256) {
            readCardinality65To256.incrementAndGet();
        } else {
            readCardinality257Plus.incrementAndGet();
        }
    }

    void recordClear() {
        if (enabled) {
            clearCalls.incrementAndGet();
        }
    }

    private static void registerGauge(MetricGroup metricGroup, String name, AtomicLong value) {
        metricGroup.gauge(name, (Gauge<Long>) value::get);
    }
}
