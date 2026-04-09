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

package org.apache.flink.contrib.streaming.state.cachekit.window;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Tests for {@link WindowLifecycleTracker}. */
class WindowLifecycleTrackerTest {

    /** Simple TimeWindow-like class for testing ETT extraction via reflection. */
    static class FakeTimeWindow {
        private final long start;
        private final long end;

        FakeTimeWindow(long start, long end) {
            this.start = start;
            this.end = end;
        }

        public long getStart() {
            return start;
        }

        public long getEnd() {
            return end;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FakeTimeWindow)) return false;
            FakeTimeWindow that = (FakeTimeWindow) o;
            return start == that.start && end == that.end;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(start * 31 + end);
        }
    }

    @Test
    void testETTExtractionViaReflection() {
        WindowLifecycleTracker tracker = new WindowLifecycleTracker();
        FakeTimeWindow window = new FakeTimeWindow(0, 10000);

        tracker.onWindowAccess("key1", window);

        assertEquals(10000L, tracker.getETT(window));
    }

    @Test
    void testUnknownNamespaceReturnsMaxETT() {
        WindowLifecycleTracker tracker = new WindowLifecycleTracker();

        assertEquals(Long.MAX_VALUE, tracker.getETT("unknown"));
    }

    @Test
    void testNamespaceWithoutGetEndReturnsMaxETT() {
        WindowLifecycleTracker tracker = new WindowLifecycleTracker();
        // String has no getEnd() method
        tracker.onWindowAccess("key1", "stringNamespace");

        assertEquals(Long.MAX_VALUE, tracker.getETT("stringNamespace"));
    }

    @Test
    void testEvictionPriority_closerWindowHasHigherPriority() {
        WindowLifecycleTracker tracker = new WindowLifecycleTracker();
        FakeTimeWindow nearWindow = new FakeTimeWindow(0, 5000);
        FakeTimeWindow farWindow = new FakeTimeWindow(0, 50000);

        tracker.onWindowAccess("key1", nearWindow);
        tracker.onWindowAccess("key2", farWindow);
        tracker.setCurrentWatermark(3000);

        long nearPriority = tracker.getEvictionPriority(nearWindow);
        long farPriority = tracker.getEvictionPriority(farWindow);

        // Near window should have HIGHER priority (keep it, don't evict)
        assertTrue(nearPriority > farPriority,
                "Near-trigger window should have higher eviction priority (protected)");
    }

    @Test
    void testEvictionPriority_aboutToTriggerIsProtected() {
        WindowLifecycleTracker tracker = new WindowLifecycleTracker();
        FakeTimeWindow window = new FakeTimeWindow(0, 5000);

        tracker.onWindowAccess("key1", window);
        tracker.setCurrentWatermark(5000); // watermark == ETT

        long priority = tracker.getEvictionPriority(window);
        assertEquals(Long.MAX_VALUE, priority, "About-to-trigger window should be maximally protected");
    }

    @Test
    void testEvictionPriority_expiredIsZero() {
        WindowLifecycleTracker tracker = new WindowLifecycleTracker();
        FakeTimeWindow window = new FakeTimeWindow(0, 5000);

        tracker.onWindowAccess("key1", window);
        tracker.markExpired(window);

        assertEquals(0L, tracker.getEvictionPriority(window));
    }

    @Test
    void testEvictionPriority_triggeringIsMaxProtected() {
        WindowLifecycleTracker tracker = new WindowLifecycleTracker();
        FakeTimeWindow window = new FakeTimeWindow(0, 5000);

        tracker.onWindowAccess("key1", window);
        tracker.markTriggering(window);

        assertEquals(Long.MAX_VALUE, tracker.getEvictionPriority(window));
    }

    @Test
    void testWindowLifecyclePhases() {
        WindowLifecycleTracker tracker = new WindowLifecycleTracker();
        FakeTimeWindow window = new FakeTimeWindow(0, 10000);

        tracker.onWindowAccess("key1", window);
        assertEquals(WindowLifecycleTracker.WindowPhase.ACCUMULATING, tracker.getPhase(window));

        tracker.markTriggering(window);
        assertEquals(WindowLifecycleTracker.WindowPhase.TRIGGERING, tracker.getPhase(window));

        tracker.markExpired(window);
        assertEquals(WindowLifecycleTracker.WindowPhase.EXPIRED, tracker.getPhase(window));
    }

    @Test
    void testMultipleWindowsTracked() {
        WindowLifecycleTracker tracker = new WindowLifecycleTracker();
        FakeTimeWindow w1 = new FakeTimeWindow(0, 10000);
        FakeTimeWindow w2 = new FakeTimeWindow(10000, 20000);
        FakeTimeWindow w3 = new FakeTimeWindow(20000, 30000);

        tracker.onWindowAccess("key1", w1);
        tracker.onWindowAccess("key2", w2);
        tracker.onWindowAccess("key3", w3);

        assertEquals(3, tracker.getActiveWindowCount());
        assertEquals(10000L, tracker.getETT(w1));
        assertEquals(20000L, tracker.getETT(w2));
        assertEquals(30000L, tracker.getETT(w3));
    }

    @Test
    void testRemoveWindow() {
        WindowLifecycleTracker tracker = new WindowLifecycleTracker();
        FakeTimeWindow window = new FakeTimeWindow(0, 10000);

        tracker.onWindowAccess("key1", window);
        assertEquals(1, tracker.getActiveWindowCount());

        tracker.removeWindow(window);
        assertEquals(0, tracker.getActiveWindowCount());
        assertNull(tracker.getPhase(window));
    }

    @Test
    void testAccessStatistics() {
        WindowLifecycleTracker tracker = new WindowLifecycleTracker();
        FakeTimeWindow window = new FakeTimeWindow(0, 10000);

        tracker.onWindowAccess("key1", window);
        tracker.onWindowAccess("key2", window);
        tracker.onWindowAccess("key1", window);

        assertEquals(3, tracker.getTotalAccesses());
        assertEquals(1, tracker.getTotalWindowsTracked());
    }
}
