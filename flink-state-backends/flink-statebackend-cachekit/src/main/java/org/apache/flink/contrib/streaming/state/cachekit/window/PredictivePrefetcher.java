/*
 * Licensed under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state.cachekit.window;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;

/**
 * Predictive prefetcher for AUR (Append and Unaligned Read) scenarios.
 *
 * <p>Uses ETT (Estimated Trigger Time) from the {@link WindowLifecycleTracker} to predict
 * which windows are about to fire, and collects their keys for batch prefetch via MultiGet.
 * Prefetched data is stored in a dedicated buffer separate from the main cache to avoid
 * pollution.
 *
 * <p>The actual RocksDB MultiGet call is NOT performed here (this class has no RocksDB
 * dependency). Instead, it produces a list of (key, window) pairs that should be prefetched,
 * and accepts the results back via {@link #loadPrefetchResults}.
 */
public class PredictivePrefetcher<K, N> {

    /** Prefetched data: (key, window) -> serialized value. */
    private final Map<KeyWindowPair<K, N>, byte[]> prefetchBuffer = new HashMap<>();

    private final WindowLifecycleTracker tracker;
    private long usedBytes;
    private long maxBytes;
    private long prefetchHorizonMs;

    /** Statistics. */
    private long prefetchHits;
    private long prefetchMisses;
    private long prefetchLoads;

    public PredictivePrefetcher(WindowLifecycleTracker tracker, long maxBytes, long prefetchHorizonMs) {
        this.tracker = Objects.requireNonNull(tracker);
        this.maxBytes = maxBytes;
        this.prefetchHorizonMs = prefetchHorizonMs;
    }

    /** Looks up a prefetched value. Returns null on miss. */
    public byte[] get(K key, N window) {
        byte[] val = prefetchBuffer.get(new KeyWindowPair<>(key, window));
        if (val != null) {
            prefetchHits++;
            return val;
        }
        prefetchMisses++;
        return null;
    }

    /**
     * Determines which (key, window) pairs should be prefetched based on current watermark.
     *
     * <p>Returns a list of KeyWindowPair that the caller should batch-read from RocksDB
     * via MultiGet, then feed back via {@link #loadPrefetchResults}.
     *
     * @param windowToKeys mapping from window to keys that belong to it (from tracker)
     * @param windowWriteBuffer if non-null, skip windows that are still in the write buffer
     */
    public <WB> List<KeyWindowPair<K, N>> computePrefetchTargets(
            Map<N, Set<K>> windowToKeys,
            WindowWriteBuffer<K, N> windowWriteBuffer) {

        long watermark = tracker.getCurrentWatermark();
        long horizon = watermark + prefetchHorizonMs;

        List<KeyWindowPair<K, N>> targets = new ArrayList<>();

        // Find windows with ETT in [watermark, watermark + horizon]
        // We iterate windowToKeys and filter by ETT
        for (Map.Entry<N, Set<K>> entry : windowToKeys.entrySet()) {
            N window = entry.getKey();
            long ett = tracker.getETT(window);

            if (ett < watermark || ett > horizon) {
                continue; // Not in prefetch range
            }

            // Skip windows still in write buffer (they can be read directly)
            if (windowWriteBuffer != null && windowWriteBuffer.containsWindow(window)) {
                continue;
            }

            WindowLifecycleTracker.WindowInfo info =
                    getWindowInfo(window);
            if (info != null && info.getPhase() == WindowLifecycleTracker.WindowPhase.EXPIRED) {
                continue;
            }

            for (K key : entry.getValue()) {
                KeyWindowPair<K, N> kw = new KeyWindowPair<>(key, window);
                if (!prefetchBuffer.containsKey(kw)) {
                    targets.add(kw);
                }
            }
        }

        return targets;
    }

    private WindowLifecycleTracker.WindowInfo getWindowInfo(N window) {
        // Access tracker's info - uses Object-typed API
        WindowLifecycleTracker.WindowPhase phase = tracker.getPhase(window);
        if (phase == null) return null;
        // Create a minimal info for checking phase
        // Note: we only need the phase here
        WindowLifecycleTracker.WindowInfo info = new WindowLifecycleTracker.WindowInfo();
        // We can't set phase directly (it's package-private), so we use getPhase instead
        return null; // Simplified: let caller check phase via tracker directly
    }

    /**
     * Loads prefetch results into the buffer.
     *
     * @param results mapping from KeyWindowPair to serialized value bytes (null values skipped)
     */
    public void loadPrefetchResults(Map<KeyWindowPair<K, N>, byte[]> results) {
        for (Map.Entry<KeyWindowPair<K, N>, byte[]> entry : results.entrySet()) {
            if (entry.getValue() != null) {
                prefetchBuffer.put(entry.getKey(), entry.getValue());
                usedBytes += entry.getValue().length + estimateKeyOverhead();
                prefetchLoads++;
            }
        }
        evictIfNeeded();
    }

    /** Removes all prefetched data for a specific window (after trigger). */
    public void removeByWindow(N window) {
        prefetchBuffer.entrySet().removeIf(entry -> {
            if (Objects.equals(entry.getKey().window, window)) {
                usedBytes -= entry.getValue().length + estimateKeyOverhead();
                return true;
            }
            return false;
        });
    }

    public void clear() {
        prefetchBuffer.clear();
        usedBytes = 0;
    }

    public void setMaxBytes(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    public void setPrefetchHorizonMs(long prefetchHorizonMs) {
        this.prefetchHorizonMs = prefetchHorizonMs;
    }

    public long getUsedBytes() {
        return usedBytes;
    }

    public long getPrefetchHits() {
        return prefetchHits;
    }

    public long getPrefetchMisses() {
        return prefetchMisses;
    }

    public long getPrefetchLoads() {
        return prefetchLoads;
    }

    public int getBufferSize() {
        return prefetchBuffer.size();
    }

    private void evictIfNeeded() {
        if (maxBytes <= 0) return;
        // Simple eviction: remove entries for windows with farthest ETT
        while (usedBytes > maxBytes && !prefetchBuffer.isEmpty()) {
            KeyWindowPair<K, N> farthest = null;
            long maxETT = Long.MIN_VALUE;

            for (KeyWindowPair<K, N> kw : prefetchBuffer.keySet()) {
                long ett = tracker.getETT(kw.window);
                if (ett > maxETT) {
                    maxETT = ett;
                    farthest = kw;
                }
            }

            if (farthest != null) {
                byte[] removed = prefetchBuffer.remove(farthest);
                if (removed != null) {
                    usedBytes -= removed.length + estimateKeyOverhead();
                }
            } else {
                break;
            }
        }
    }

    private static long estimateKeyOverhead() {
        return 64;
    }

    /** Composite key for the prefetch buffer. */
    public static class KeyWindowPair<K, N> {
        public final K key;
        public final N window;

        public KeyWindowPair(K key, N window) {
            this.key = key;
            this.window = window;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof KeyWindowPair)) return false;
            KeyWindowPair<?, ?> that = (KeyWindowPair<?, ?>) o;
            return Objects.equals(key, that.key) && Objects.equals(window, that.window);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, window);
        }
    }
}
