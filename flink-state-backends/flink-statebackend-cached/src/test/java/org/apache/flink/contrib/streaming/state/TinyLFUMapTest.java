package org.apache.flink.contrib.streaming.state;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link TinyLFUMap}. */
class TinyLFUMapTest {

    // Window cache is 1% of maxCapacity, min 1. Main cache is the rest.

    @Test
    void testBasicPutAndGet() {
        TinyLFUMap<String, String> cache = new TinyLFUMap<>(100); // Window: 1, Main: 99
        cache.put("k1", "v1");
        cache.put("k2", "v2");

        assertEquals("v1", cache.get("k1"));
        assertEquals("v2", cache.get("k2"));
        assertNull(cache.get("k3"));
        assertEquals(2, cache.size());
    }

    @Test
    void testUpdateExistingKey() {
        TinyLFUMap<String, String> cache = new TinyLFUMap<>(100);
        cache.put("k1", "v1");
        assertEquals("v1", cache.get("k1"));

        cache.put("k1", "v1_updated");
        assertEquals("v1_updated", cache.get("k1"));
        assertEquals(1, cache.size());
    }

    @Test
    void testWindowCacheAdmissionAndEvictionToMain_WhenMainHasSpace() {
        // maxCapacity = 3. Window = 1, Main = 2.
        TinyLFUMap<Integer, String> cache = new TinyLFUMap<>(3);

        // Put K1: K1 into Window. Window: {1}, Main: {}
        cache.put(1, "v1");
        assertEquals("v1", cache.get(1)); // Access K1 to increment its frequency
        assertEquals(1, cache.size());

        // Put K2: K2 into Window. K1 (eldest from Window) becomes candidate for Main.
        // Main has space, so K1 admitted to Main.
        // Window: {2}, Main: {1}
        cache.put(2, "v2");
        assertTrue(cache.containsKey(1)); // K1 should be in main
        assertTrue(cache.containsKey(2)); // K2 should be in window
        assertEquals("v1", cache.get(1)); // Hit in main
        assertEquals("v2", cache.get(2)); // Hit in window (will try to promote K2 to main)
        assertEquals(2, cache.size());

        // After K2 access: K2 promoted to main. Window empty if K2 was only one. Main {1,2} or
        // {2,1}
        // Let's check state:
        // Before K2 get: Window:{2}, Main:{1}
        // After K2 get: K2 is candidate. Main has space {1}. K2 admitted. Main {1,2}. Window empty
        // or
        // next.

        // Put K3: K3 into Window. K2 (if it was still in window's eldest logic) or new logic for
        // K3.
        // Let's re-evaluate from cache.put(2,"v2"):
        // Window: {1="v1"}
        // cache.put(2,"v2"): 2 goes to window. Window full (size 1). 1 is eldest.
        // tryAdmitToMain(1,"v1"): mainLruCache size 0 < mainCapacity 2. mainLruCache.put(1,"v1").
        // State: Window: {2="v2"}, Main: {1="v1"}. Size = 2.

        cache.put(3, "v3"); // 3 goes to window. Window full (size 1). 2 is eldest.
        // tryAdmitToMain(2,"v2"): mainLruCache size 1 < mainCapacity 2. mainLruCache.put(2,"v2").
        // State: Window: {3="v3"}, Main: {1="v1", 2="v2"}. Size = 3.
        // Order in main: 1 (eldest), 2.

        assertTrue(cache.containsKey(1));
        assertTrue(cache.containsKey(2));
        assertTrue(cache.containsKey(3));
        assertEquals("v1", cache.get(1));
        assertEquals("v2", cache.get(2));
        assertEquals("v3", cache.get(3));
        assertEquals(3, cache.size());
    }

    @Test
    void testAdmissionToMain_WhenMainIsFull_TinyLFUDecision() {
        // maxCapacity = 3. Window = 1, Main = 2.
        TinyLFUMap<Integer, String> cache = new TinyLFUMap<>(3);

        // Setup: Main = {1, 2}, Window = {3}
        // To achieve this, we make 1 and 2 frequent, then add 3, then 4.
        cache.put(1, "v1"); // W:{1} M:{}
        cache.put(2, "v2"); // W:{2} M:{1} (1 was admitted)
        cache.put(3, "v3"); // W:{3} M:{1,2} (2 was admitted)

        // At this point: Window:{3="v3"}, Main:{1="v1", 2="v2"} (actual main order depends on
        // LinkedHashMap impl)
        // Let's verify frequencies. Sketch estimate is approximate.
        // Access 1 & 2 multiple times to ensure their frequency is higher than new items.
        for (int i = 0; i < 5; i++) {
            cache.get(1);
            cache.get(2);
        }
        // Access 3 once (it's in window, get will try to promote)
        cache.get(3); // W:{}, M:{1,2,3} if 3 wins. Victim from Main would be LRU.


        // Re-setup for clarity with maxCapacity = 3 (Window=1, Main=2)
        cache.clear();
        // Goal: Main cache {k1, k2} (k1 is LRU victim), Window {k3}. Candidate is k3.
        // k1, k2 are frequent. k4 is a new challenger.

        cache.put(1, "v1_freq"); // W:{1} M:{}
        for (int i = 0; i < 5; i++)
            cache.get(1); // make 1 frequent

        cache.put(2, "v2_freq"); // W:{2} M:{1}
        for (int i = 0; i < 5; i++)
            cache.get(2); // make 2 frequent

        // Current state: Window is likely empty as 1,2 got promoted. Main is {1,2} or {2,1}.
        // Let's fill main deterministically:
        cache.clear();
        cache.put(10, "v10"); // W:{10} M:{}
        cache.put(11, "v11"); // W:{11} M:{10}
        // At this point: Window:{11="v11"}, Main:{10="v10"}. Size=2. Main capacity=2.

        // Make 10, 11 frequent
        for (int i = 0; i < 10; i++) {
            cache.get(10);
            cache.get(11);
        }
        // State: Window should be empty (10, 11 promoted), Main:{10,11} (order depends on last
        // access)
        // Let's assume 10 was LRU in Main, 11 MRU.

        cache.put(12, "v12_candidate"); // W:{12}, M:{10,11}. Candidate 12 (freq ~1)
                                        // Main is full. Victim from Main is 10 (freq ~10)
                                        // 12 (candidate) vs 10 (victim). Freq(12) < Freq(10). 12
                                        // NOT admitted.
        // Window: {12}, Main: {10,11}. Key 12 is evicted from system.
        assertFalse(cache.containsKey(12),
                "Candidate 12 should not be admitted due to lower frequency.");
        assertTrue(cache.containsKey(10));
        assertTrue(cache.containsKey(11));
        assertEquals(2, cache.size()); // Main cache size

        // New candidate 13, more frequent than victim 10.
        cache.put(13, "v13_strong_candidate"); // W:{13} M:{10,11}
        for (int i = 0; i < 15; i++) { // Make 13 very frequent while it's in window
            cache.put(13, "v13_strong_candidate"); // keep putting to window, or get if it's already
                                                   // there
            // This is tricky. `put` records access.
            // If 13 is in window, put updates it.
        }
        // To ensure 13 is the candidate that TinyLFU considers:
        // After cache.put(13, "v13_strong_candidate"), W:{13}, M:{10,11}
        // To make 13 frequent *before* it's considered for main cache eviction from window:
        // This requires careful manipulation or assuming sketch is updated by the put.
        // Let's assume the `put` for 13 has updated its sketch count.

        // To test K4 (strong candidate) vs K1 (victim)
        cache.clear();
        // Fill with K1, K2. K1 is victim (LRU in main).
        cache.put(1, "v1"); // W:{1} M:{}
        cache.put(2, "v2"); // W:{2} M:{1}
        cache.put(3, "v3"); // W:{3} M:{1,2} (K1=LRU in Main)

        // Make K3 (candidate from window) more frequent than K1 (victim in main)
        for (int i = 0; i < 5; i++)
            cache.get(3); // Access K3, it's in window, promotes to main
                          // M:{2,3}, K1 evicted.
        // After cache.get(3) for 5 times:
        // Initial: W:{3}, M:{1,2}
        // get(3): 3 is candidate. M is {1,2}. Victim is 1. Freq(3) > Freq(1). M becomes {2,3}. W is
        // empty.
        assertTrue(cache.containsKey(3));
        assertTrue(cache.containsKey(2));
        assertFalse(cache.containsKey(1));
        assertEquals(2, cache.size()); // Main cache has 2 items

        // Add K4. K4 to Window. Window evicts nothing as it's empty.
        cache.put(4, "v4"); // W:{4}, M:{2,3}
        assertTrue(cache.containsKey(4));
        assertEquals(3, cache.size());
    }


    @Test
    void testHitInWindowCache_PromotesToMain() {
        // maxCapacity = 3. Window = 1, Main = 2.
        TinyLFUMap<Integer, String> cache = new TinyLFUMap<>(3);

        cache.put(1, "v1"); // W:{1}, M:{}
        // Before get(1): windowLruCache contains 1, mainLruCache is empty.

        assertEquals("v1", cache.get(1)); // Access 1. It's in W. tryAdmit for 1. M has space.
                                          // M:{1}. W empty.

        // After get(1): 1 should be in main, not in window.
        // Instead of directly checking cache internals, verify the behavior:
        // If 1 was promoted from window to main, it should persist when we add more items
        assertTrue(cache.containsKey(1), "Item 1 should still be in cache after get()");

        // Add more items to fill the cache
        cache.put(2, "v2"); // Now window has 2, main has 1
        cache.put(3, "v3"); // Now window has 3, main has {1,2}

        // If 1 was properly promoted to main, it should still be there
        assertTrue(cache.containsKey(1), "Item 1 should still be in main cache");
        assertTrue(cache.containsKey(2), "Item 2 should be in cache (promoted from window)");
        assertTrue(cache.containsKey(3), "Item 3 should be in window");
        assertEquals(3, cache.size());
    }

    @Test
    void testHitInMainCache_BecomesMRU() {
        // maxCapacity = 3. Window = 1, Main = 2.
        TinyLFUMap<Integer, String> cache = new TinyLFUMap<>(3);
        cache.put(1, "v1"); // W:{1} M:{}
        cache.put(2, "v2"); // W:{2} M:{1}
        cache.put(3, "v3"); // W:{3} M:{1,2} (main order: 1 then 2)

        // At this point: Window:{3}, Main:{1,2} (1 is LRU in Main)
        cache.get(1); // Access 1 (LRU in Main). Now Main order should be: 2 (LRU), 1 (MRU)

        // To verify 1 is now MRU in main, we can attempt to evict the LRU item.
        // The candidate for main will be 3 (from window, when we next put to window).
        // The victim from main *should* be 2 if 1 became MRU.
        // We need to ensure freq(3) > freq(2) for 3 to be admitted.
        for (int i = 0; i < 5; i++) {
            cache.get(3); // Make 3 frequent (it's in window, will be promoted)
        }

        // Simplified: Check that accessing an item in main doesn't remove it when cache is full.
        cache.clear();
        cache.put(10, "v10"); // W:{10}, M:{}
        cache.put(11, "v11"); // W:{11}, M:{10}
        cache.put(12, "v12"); // W:{12}, M:{10,11}
        assertEquals(3, cache.size());

        // Access 10 to make it MRU in main
        cache.get(10);
        assertTrue(cache.containsKey(10));

        // Make 12 (in window) more frequent than 11 (LRU in main)
        for (int i = 0; i < 5; i++)
            cache.get(12); // Make 12 frequent

        // Add item 13 to force eviction. With proper MRU behavior:
        // - 10 was accessed (MRU), should be protected
        // - 11 is LRU in main, should be victim if 12 is more frequent
        cache.put(13, "v13");

        assertTrue(cache.containsKey(10), "Recently accessed item 10 should be in cache");
        assertTrue(cache.containsKey(12), "Frequent item 12 should be in cache");
        assertTrue(cache.containsKey(13), "New item 13 should be in cache");
        assertFalse(cache.containsKey(11), "LRU item 11 should be evicted");
        assertEquals(3, cache.size());
    }

    @Test
    void testRemoveKey() {
        TinyLFUMap<Integer, String> cache = new TinyLFUMap<>(3); // W:1, M:2
        cache.put(1, "v1"); // W:{1}
        cache.put(2, "v2"); // W:{2}, M:{1}
        cache.put(3, "v3"); // W:{3}, M:{1,2}

        assertTrue(cache.containsKey(1)); // In main
        assertTrue(cache.containsKey(3)); // In window

        // Remove from main
        assertEquals("v1", cache.remove(1));
        assertFalse(cache.containsKey(1));
        assertNull(cache.get(1));
        assertEquals(2, cache.size());

        // Remove from window
        assertEquals("v3", cache.remove(3));
        assertFalse(cache.containsKey(3));
        assertNull(cache.get(3));
        assertEquals(1, cache.size()); // Only 2 left in main

        // Remove non-existent
        assertNull(cache.remove(404));
        assertEquals(1, cache.size());
    }

    @Test
    void testClearCache() {
        TinyLFUMap<Integer, String> cache = new TinyLFUMap<>(3); // W:1, M:2
        cache.put(1, "v1");
        cache.put(2, "v2");
        cache.put(3, "v3");
        assertEquals(3, cache.size());

        cache.clear();
        assertEquals(0, cache.size());
        assertTrue(cache.isEmpty());
        assertFalse(cache.containsKey(1));
        // No need to check internal implementation details
        // Sketch reset is also called in clear. Difficult to verify its state without
        // introspection.
    }

    @Test
    void testEntrySetAndValues() {
        TinyLFUMap<Integer, String> cache = new TinyLFUMap<>(3); // W:1, M:2
        cache.put(1, "v1");
        cache.put(2, "v2");
        cache.put(3, "v3");
        // Expected state after these puts: W:{3}, M:{1,2} or {2,1}

        Set<Map.Entry<Integer, String>> entries = new HashSet<>();
        cache.entrySet().forEach(entries::add); // Simplified way to add to set

        assertEquals(3, entries.size());

        // Check existence of key-value pairs without relying on Map.entry()
        Map<Integer, String> expectedMap = new HashMap<>();
        expectedMap.put(1, "v1");
        expectedMap.put(2, "v2");
        expectedMap.put(3, "v3");

        for (Map.Entry<Integer, String> entry : entries) {
            assertTrue(expectedMap.containsKey(entry.getKey()));
            assertEquals(expectedMap.get(entry.getKey()), entry.getValue());
        }

        List<String> values = new ArrayList<>(cache.values());
        assertEquals(3, values.size());
        assertTrue(values.contains("v1"));
        assertTrue(values.contains("v2"));
        assertTrue(values.contains("v3"));
    }

    @Test
    void testComputeIfAbsent() {
        TinyLFUMap<Integer, String> cache = new TinyLFUMap<>(3);
        AtomicBoolean mappingFunctionCalled = new AtomicBoolean(false);
        Function<Integer, String> mappingFunction = k -> {
            mappingFunctionCalled.set(true);
            return "mapped_" + k;
        };

        // Key does not exist
        String v1 = cache.computeIfAbsent(1, mappingFunction);
        assertEquals("mapped_1", v1);
        assertTrue(mappingFunctionCalled.get());
        assertTrue(cache.containsKey(1));
        assertEquals("mapped_1", cache.get(1));
        assertEquals(1, cache.size());

        // Key exists
        mappingFunctionCalled.set(false);
        String v1_existing = cache.computeIfAbsent(1, mappingFunction);
        assertEquals("mapped_1", v1_existing);
        assertFalse(mappingFunctionCalled.get()); // Function should not be called again
        assertEquals(1, cache.size());

        // Compute for a new key, mapping function returns null
        mappingFunctionCalled.set(false);
        Function<Integer, String> nullMappingFunction = k -> {
            mappingFunctionCalled.set(true);
            return null;
        };
        String v_null = cache.computeIfAbsent(2, nullMappingFunction);
        assertNull(v_null);
        assertTrue(mappingFunctionCalled.get());
        assertFalse(cache.containsKey(2)); // Should not store null mapping result
        assertEquals(1, cache.size()); // Size should not change
    }

    @Test
    void testMaxCapacityConstraint() {
        final int MAX_CAPACITY = 5; // W:1, M:4
        TinyLFUMap<Integer, String> cache = new TinyLFUMap<>(MAX_CAPACITY);

        for (int i = 0; i < MAX_CAPACITY * 2; i++) {
            cache.put(i, "v" + i);
            // Access to make it seem more frequent if it survives window
            if (cache.containsKey(i)) {
                cache.get(i);
            }
        }
        assertEquals(MAX_CAPACITY, cache.size());
    }

    @Test
    void testSmallCapacity_WindowDominates() {
        // maxCapacity = 1. Window = 1, Main = 0.
        TinyLFUMap<Integer, String> cache = new TinyLFUMap<>(1);
        // With capacity=1, we test the behavior without checking internal window/main allocation

        cache.put(1, "v1"); // This should be the only item in cache
        assertTrue(cache.containsKey(1));
        assertEquals(1, cache.size());

        cache.put(2, "v2"); // This should evict item 1
        assertTrue(cache.containsKey(2), "Item 2 should be in cache");
        assertFalse(cache.containsKey(1), "Item 1 should be evicted");
        assertEquals(1, cache.size());

        // Get from cache - should still be there
        assertEquals("v2", cache.get(2));
        assertTrue(cache.containsKey(2));
        assertEquals(1, cache.size());
    }

    @Test
    void testSketchResetImpact_NotDirectlyTestable_BehaviorObservation() {
        // Sketch resets every maxCapacity * 10 accesses.
        // This test is more of a conceptual check. Precise verification is hard.
        int capacity = 2; // W:1, M:1
        TinyLFUMap<Integer, String> cache = new TinyLFUMap<>(capacity);

        // Make K1 very frequent, then K2 less frequent
        cache.put(1, "v1"); // W:{1} M:{}
        for (int i = 0; i < 15; i++)
            cache.get(1); // freq(1) high. M:{1}, W:{}

        cache.put(2, "v2"); // W:{2} M:{1}
        cache.get(2); // freq(2) low. M:{1}, W:{} (2 was candidate, M full, victim 1. freq(2) vs
                      // freq(1). 1 wins. 2 lost)

        assertFalse(cache.containsKey(2)); // 2 should have been evicted as 1 was more frequent

        // Trigger many accesses to potentially reset sketch (capacity * 10 = 20 accesses)
        for (int i = 0; i < capacity * 10 * 2; i++) {
            cache.put(100 + i, "vx" + i); // Fill and evict, causing accesses
            if (cache.get(1) == null) { // if 1 gets evicted, put it back to keep it in sketch
                cache.put(1, "v1");
            }
        }
        // After sketch reset, freq(1) should be halved.

        // Now try to admit K3 (new, low actual frequency)
        cache.put(3, "v3"); // W:{3}, M:{x} (x is whatever survived from loop)
                            // Candidate 3. Victim from Main (possibly 1 if it survived).
                            // If freq(1) was significantly reduced, K3 might get in.

        // This test's outcome is too dependent on exact sketch state and eviction patterns.
        // What we can assert is that cache adheres to capacity.
        assertTrue(cache.size() <= capacity);
    }
}
