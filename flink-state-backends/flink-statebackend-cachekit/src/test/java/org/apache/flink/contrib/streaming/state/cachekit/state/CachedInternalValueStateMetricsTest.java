package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.internal.InternalValueState;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CachedInternalValueStateMetricsTest {

    private TestMetricGroup rootGroup;
    private final Map<String, TestCounter> counters = new HashMap<>();

    @BeforeEach
    void setup() {
        counters.clear();
        rootGroup = new TestMetricGroup(counters);
    }

    @Test
    void testMetricsBasicHitMiss() throws IOException {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalValueState<String, VoidNamespace, Integer> delegate = mock(InternalValueState.class);
        when(delegate.value()).thenReturn(100);

        CachedInternalValueState<String, VoidNamespace, Integer> state = new CachedInternalValueState<>(
                delegate,
                currentKey::get,
                k -> {
                },
                100,
                CachePolicyType.LRU,
                0,
                false, // No bypass
                0.0,
                100,
                rootGroup,
                "myState",
                null,
                100);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        // 1. Miss (loading from delegate)
        assertEquals(100, state.value());

        // Counters path: value_state -> myState -> calls -> value
        // value_state -> myState -> cache -> misses
        assertCounter("value_state.myState.calls.value", 1);
        assertCounter("value_state.myState.cache.misses", 1);
        assertCounter("value_state.myState.cache.hits", 0);

        // 2. Hit L1 (Sticky or L1)
        assertEquals(100, state.value());
        assertCounter("value_state.myState.calls.value", 2);
        assertCounter("value_state.myState.cache.misses", 1); // Unchanged
        assertCounter("value_state.myState.cache.hits", 1); // Increment
    }

    @Test
    void testMetricsWithBypass() throws IOException {
        AtomicReference<String> currentKey = new AtomicReference<>("k1");
        InternalValueState<String, VoidNamespace, Integer> delegate = mock(InternalValueState.class);
        when(delegate.value()).thenReturn(200);

        CachedInternalValueState<String, VoidNamespace, Integer> state = new CachedInternalValueState<>(
                delegate,
                currentKey::get,
                k -> {
                },
                100,
                CachePolicyType.LRU,
                0,
                true, // Bypass enabled
                1.0, // Threshold 1.0 (will trigger bypass on first miss)
                2, // Window 2
                rootGroup,
                "myState",
                null,
                100);
        state.setCurrentNamespace(VoidNamespace.INSTANCE);

        // 1. First Access: Miss. Window=1/2. HitRate=0.0.
        assertEquals(200, state.value());
        assertCounter("value_state.myState.calls.value", 1);
        assertCounter("value_state.myState.cache.misses", 1);

        // 2. Second Access: Hit (L1 is populated on miss). Window=2/2. HitRate=0.5 <
        // 1.0 -> Bypass ON.
        assertEquals(200, state.value());
        assertCounter("value_state.myState.calls.value", 2);
        assertCounter("value_state.myState.cache.hits", 1);

        // Now Bypass is ON.

        // 3. Third Access: Bypassed.
        // It's a sampling logic. opsSinceLastSample starts at 0.
        // ops++ (1). if (1 < 100) -> Bypass.
        assertEquals(200, state.value());

        assertCounter("value_state.myState.calls.value", 3);
        assertCounter("value_state.myState.cache.bypass", 1);
        // Hits/Misses should NOT increment
        assertCounter("value_state.myState.cache.hits", 1);
        assertCounter("value_state.myState.cache.misses", 1);

        // 4. Verify Total Equation
        long total = getCount("value_state.myState.calls.value");
        long hits = getCount("value_state.myState.cache.hits");
        long misses = getCount("value_state.myState.cache.misses");
        long bypass = getCount("value_state.myState.cache.bypass");

        assertEquals(total, hits + misses + bypass, "Total calls should equal hits + misses + bypass");
    }

    private void assertCounter(String path, long expected) {
        assertEquals(expected, getCount(path), "Counter mismatch for " + path);
    }

    private long getCount(String path) {
        TestCounter c = counters.get(path);
        return c == null ? 0 : c.getCount();
    }

    // --- Mocks ---

    private static class TestCounter implements Counter {
        private long count = 0;

        @Override
        public void inc() {
            count++;
        }

        @Override
        public void inc(long n) {
            count += n;
        }

        @Override
        public void dec() {
            count--;
        }

        @Override
        public void dec(long n) {
            count -= n;
        }

        @Override
        public long getCount() {
            return count;
        }
    }

    private static class TestMetricGroup implements MetricGroup {
        private final Map<String, TestCounter> counters;
        private final String prefix;

        TestMetricGroup(Map<String, TestCounter> counters) {
            this(counters, "");
        }

        TestMetricGroup(Map<String, TestCounter> counters, String prefix) {
            this.counters = counters;
            this.prefix = prefix;
        }

        @Override
        public MetricGroup addGroup(String name) {
            return new TestMetricGroup(counters, prefix.isEmpty() ? name : prefix + "." + name);
        }

        @Override
        public MetricGroup addGroup(String key, String value) {
            return addGroup(key).addGroup(value);
        }

        @Override
        public String[] getScopeComponents() {
            return new String[0];
        }

        @Override
        public Map<String, String> getAllVariables() {
            return new HashMap<>();
        }

        @Override
        public String getMetricIdentifier(String metricName) {
            return prefix + "." + metricName;
        }

        @Override
        public String getMetricIdentifier(String metricName, org.apache.flink.metrics.CharacterFilter filter) {
            return getMetricIdentifier(metricName);
        }

        @Override
        public Counter counter(String name) {
            String fullPath = prefix + "." + name;
            return counters.computeIfAbsent(fullPath, k -> new TestCounter());
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            String fullPath = prefix + "." + name;
            counters.put(fullPath, (TestCounter) counter); // We assume test only uses our TestCounter or we wrap it
            return counter;
        }

        @Override
        public <T, G extends org.apache.flink.metrics.Gauge<T>> G gauge(String name, G gauge) {
            return gauge; // No-op for gauges in this test
        }

        @Override
        public <H extends org.apache.flink.metrics.Histogram> H histogram(String name, H histogram) {
            return histogram;
        }

        @Override
        public <M extends org.apache.flink.metrics.Meter> M meter(String name, M meter) {
            return meter;
        }
    }
}
