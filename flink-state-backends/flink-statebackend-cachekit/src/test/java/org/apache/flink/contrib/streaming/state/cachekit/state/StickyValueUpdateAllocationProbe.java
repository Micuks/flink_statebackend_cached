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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalValueState;

import com.sun.management.ThreadMXBean;

import java.lang.management.ManagementFactory;

/** Manual allocation probe for repeated updates to one non-void-namespace ValueState entry. */
public final class StickyValueUpdateAllocationProbe {

    private static final int WARMUP_ITERATIONS = 1_000_000;
    private static final int MEASURED_ITERATIONS = 5_000_000;
    private static final Integer VALUE_A = 1;
    private static final Integer VALUE_B = 2;

    private StickyValueUpdateAllocationProbe() {}

    public static void main(String[] args) throws Exception {
        ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!bean.isThreadAllocatedMemorySupported()) {
            throw new IllegalStateException("Thread allocation measurement is not supported.");
        }
        bean.setThreadAllocatedMemoryEnabled(true);

        CachedInternalValueState<String, String, Integer> baseline = newState(false);
        CachedInternalValueState<String, String, Integer> optimized = newState(true);
        int sink = run(baseline, WARMUP_ITERATIONS);
        sink += run(optimized, WARMUP_ITERATIONS);

        long baselineBytes = measure(bean, baseline);
        long optimizedBytes = measure(bean, optimized);
        sink += baseline.value() + optimized.value();

        System.out.printf(
                "iterations=%d baseline_bytes=%d baseline_bytes_per_update=%.3f "
                        + "optimized_bytes=%d optimized_bytes_per_update=%.3f sink=%d%n",
                MEASURED_ITERATIONS,
                baselineBytes,
                (double) baselineBytes / MEASURED_ITERATIONS,
                optimizedBytes,
                (double) optimizedBytes / MEASURED_ITERATIONS,
                sink);
        if (baselineBytes <= 0) {
            throw new AssertionError("Disabled control unexpectedly allocated no memory.");
        }
        if (optimizedBytes != 0) {
            throw new AssertionError(
                    "Sticky in-place update allocated " + optimizedBytes + " bytes.");
        }
    }

    private static long measure(
            ThreadMXBean bean, CachedInternalValueState<String, String, Integer> state)
            throws Exception {
        long threadId = Thread.currentThread().getId();
        long before = bean.getThreadAllocatedBytes(threadId);
        run(state, MEASURED_ITERATIONS);
        return bean.getThreadAllocatedBytes(threadId) - before;
    }

    private static int run(
            CachedInternalValueState<String, String, Integer> state, int iterations)
            throws Exception {
        int sink = 0;
        for (int iteration = 0; iteration < iterations; iteration++) {
            state.update((iteration & 1) == 0 ? VALUE_A : VALUE_B);
            sink += state.value();
        }
        return sink;
    }

    private static CachedInternalValueState<String, String, Integer> newState(
            boolean stickyUpdateInPlaceEnabled)
            throws Exception {
        ProbeValueState delegate = new ProbeValueState();
        CachedInternalValueState<String, String, Integer> state =
                stickyUpdateInPlaceEnabled
                        ? new CachedInternalValueState<>(
                                delegate,
                                () -> "account-7",
                                ignored -> {},
                                128,
                                CachePolicyType.LRU,
                                0,
                                false,
                                0.05,
                                1000,
                                false,
                                true)
                        : new CachedInternalValueState<>(
                                delegate,
                                () -> "account-7",
                                ignored -> {},
                                128,
                                CachePolicyType.LRU,
                                0,
                                false,
                                0.05,
                                1000,
                                false);
        state.setCurrentNamespace("window-9");
        state.update(VALUE_A);
        return state;
    }

    private static final class ProbeValueState
            implements InternalValueState<String, String, Integer> {

        private Integer value;

        @Override
        public Integer value() {
            return value;
        }

        @Override
        public void update(Integer value) {
            this.value = value;
        }

        @Override
        public void clear() {
            value = null;
        }

        @Override
        public TypeSerializer<String> getKeySerializer() {
            return StringSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer<String> getNamespaceSerializer() {
            return StringSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer<Integer> getValueSerializer() {
            return IntSerializer.INSTANCE;
        }

        @Override
        public void setCurrentNamespace(String namespace) {}

        @Override
        public byte[] getSerializedValue(
                byte[] serializedKeyAndNamespace,
                TypeSerializer<String> safeKeySerializer,
                TypeSerializer<String> safeNamespaceSerializer,
                TypeSerializer<Integer> safeValueSerializer) {
            return null;
        }

        @Override
        public InternalKvState.StateIncrementalVisitor<String, String, Integer>
                getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
            throw new UnsupportedOperationException();
        }
    }
}
