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

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import java.util.concurrent.atomic.AtomicLong;

/** Task-level audit counters for the opt-in Direct State Transit Lane. */
public final class DirectStateTransitMetrics {

    private static final DirectStateTransitMetrics DISABLED = new DirectStateTransitMetrics(false);

    private final boolean enabled;
    private final AtomicLong batchesPrepared = new AtomicLong();
    private final AtomicLong keysPrepared = new AtomicLong();
    private final AtomicLong singleJniCalls = new AtomicLong();
    private final AtomicLong perKeyJniCalls = new AtomicLong();
    private final AtomicLong valuesMaterialized = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong emptyValues = new AtomicLong();
    private final AtomicLong overflowFallbacks = new AtomicLong();
    private final AtomicLong failureFallbacks = new AtomicLong();
    private final AtomicLong generationDrops = new AtomicLong();
    private final AtomicLong busySkips = new AtomicLong();

    private DirectStateTransitMetrics(boolean enabled) {
        this.enabled = enabled;
    }

    public static DirectStateTransitMetrics create(MetricGroup metricGroup, boolean enabled) {
        if (!enabled || metricGroup == null) {
            return DISABLED;
        }
        DirectStateTransitMetrics metrics = new DirectStateTransitMetrics(true);
        MetricGroup diagnostics = metricGroup.addGroup("cachekit").addGroup("diagnostics");
        register(diagnostics, "dstl_batches_prepared", metrics.batchesPrepared);
        register(diagnostics, "dstl_keys_prepared", metrics.keysPrepared);
        register(diagnostics, "dstl_single_jni_calls", metrics.singleJniCalls);
        register(diagnostics, "dstl_per_key_jni_calls", metrics.perKeyJniCalls);
        register(diagnostics, "dstl_values_materialized", metrics.valuesMaterialized);
        register(diagnostics, "dstl_misses", metrics.misses);
        register(diagnostics, "dstl_empty_values", metrics.emptyValues);
        register(diagnostics, "dstl_overflow_fallbacks", metrics.overflowFallbacks);
        register(diagnostics, "dstl_failure_fallbacks", metrics.failureFallbacks);
        register(diagnostics, "dstl_generation_drops", metrics.generationDrops);
        register(diagnostics, "dstl_busy_skips", metrics.busySkips);
        return metrics;
    }

    public static DirectStateTransitMetrics disabled() {
        return DISABLED;
    }

    static DirectStateTransitMetrics forTesting() {
        return new DirectStateTransitMetrics(true);
    }

    void recordPrepared(int keys) {
        if (enabled) {
            batchesPrepared.incrementAndGet();
            keysPrepared.addAndGet(keys);
        }
    }

    void recordReadCall(boolean singleJni, int keys) {
        if (enabled) {
            if (singleJni) {
                singleJniCalls.incrementAndGet();
            } else {
                perKeyJniCalls.addAndGet(keys);
            }
        }
    }

    void recordMaterialized() {
        increment(valuesMaterialized);
    }

    void recordMiss() {
        increment(misses);
    }

    void recordEmptyValue() {
        increment(emptyValues);
    }

    void recordOverflowFallback() {
        increment(overflowFallbacks);
    }

    void recordFailureFallback() {
        increment(failureFallbacks);
    }

    void recordGenerationDrop() {
        increment(generationDrops);
    }

    void recordBusySkip() {
        increment(busySkips);
    }

    long batchesPrepared() {
        return batchesPrepared.get();
    }

    long keysPrepared() {
        return keysPrepared.get();
    }

    long singleJniCalls() {
        return singleJniCalls.get();
    }

    long perKeyJniCalls() {
        return perKeyJniCalls.get();
    }

    long overflowFallbacks() {
        return overflowFallbacks.get();
    }

    long generationDrops() {
        return generationDrops.get();
    }

    private void increment(AtomicLong counter) {
        if (enabled) {
            counter.incrementAndGet();
        }
    }

    private static void register(MetricGroup metricGroup, String name, AtomicLong counter) {
        metricGroup.gauge(name, (Gauge<Long>) counter::get);
    }
}
