/*
 * Licensed under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state.cachekit.window;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Per-window write buffer for AAR (Append and Aligned Read) scenarios.
 *
 * <p>Accumulates writes in memory organized by window. On window trigger, data can be read
 * directly from the buffer (pure memory, zero I/O). Data is only flushed to the delegate
 * backend when the buffer exceeds its memory budget or at checkpoint time.
 *
 * <p>Eviction selects the window farthest from triggering (highest ETT) to flush first,
 * protecting windows that are about to fire.
 */
public class WindowWriteBuffer<K, N> {

    private final Map<N, PerWindowData<K>> windowBuffers = new HashMap<>();
    private final WindowLifecycleTracker tracker;
    private long usedBytes;
    private long maxBytes;

    /** Statistics. */
    private long bufferHits;
    private long bufferMisses;
    private long evictionFlushes;
    private long totalCoalesced;

    public WindowWriteBuffer(WindowLifecycleTracker tracker, long maxBytes) {
        this.tracker = Objects.requireNonNull(tracker);
        this.maxBytes = maxBytes;
    }

    /**
     * Writes a serialized value for (key, window) into the buffer.
     * Returns the previous value if the key existed (write coalescing).
     */
    public byte[] put(K key, N window, byte[] serializedValue) {
        PerWindowData<K> buf = windowBuffers.computeIfAbsent(window, w -> new PerWindowData<>());
        byte[] old = buf.entries.put(key, serializedValue);

        if (old != null) {
            // Write coalescing: same key updated again
            buf.coalesceCount++;
            totalCoalesced++;
            long delta = serializedValue.length - old.length;
            buf.sizeBytes += delta;
            usedBytes += delta;
        } else {
            long entrySize = serializedValue.length + estimateKeyOverhead();
            buf.sizeBytes += entrySize;
            usedBytes += entrySize;
        }
        buf.dirty = true;
        return old;
    }

    /** Gets a cached value for (key, window). Returns null on miss. */
    public byte[] get(K key, N window) {
        PerWindowData<K> buf = windowBuffers.get(window);
        if (buf != null) {
            byte[] val = buf.entries.get(key);
            if (val != null) {
                bufferHits++;
                return val;
            }
        }
        bufferMisses++;
        return null;
    }

    /** Returns all entries for a window (for batch read on trigger). */
    public Map<K, byte[]> getWindowEntries(N window) {
        PerWindowData<K> buf = windowBuffers.get(window);
        return buf != null ? buf.entries : null;
    }

    /** Checks if buffer contains data for the given window. */
    public boolean containsWindow(N window) {
        return windowBuffers.containsKey(window);
    }

    /** Removes all data for a window from the buffer. */
    public PerWindowData<K> removeWindow(N window) {
        PerWindowData<K> buf = windowBuffers.remove(window);
        if (buf != null) {
            usedBytes -= buf.sizeBytes;
        }
        return buf;
    }

    /**
     * Selects and removes the coldest (farthest ETT) window for eviction.
     * Returns null if buffer is empty.
     */
    public EvictionResult<K, N> evictColdestWindow() {
        if (windowBuffers.isEmpty()) {
            return null;
        }

        N coldest = null;
        long maxETT = Long.MIN_VALUE;

        for (N window : windowBuffers.keySet()) {
            long ett = tracker.getETT(window);
            if (ett > maxETT) {
                maxETT = ett;
                coldest = window;
            }
        }

        if (coldest != null) {
            PerWindowData<K> data = removeWindow(coldest);
            evictionFlushes++;
            return new EvictionResult<>(coldest, data);
        }
        return null;
    }

    /** Returns true if the buffer has exceeded its memory budget. */
    public boolean needsEviction() {
        return maxBytes > 0 && usedBytes > maxBytes;
    }

    /** Returns all dirty windows that need flushing (for checkpoint). */
    public Map<N, PerWindowData<K>> getDirtyWindows() {
        Map<N, PerWindowData<K>> dirty = new HashMap<>();
        for (Map.Entry<N, PerWindowData<K>> entry : windowBuffers.entrySet()) {
            if (entry.getValue().dirty) {
                dirty.put(entry.getKey(), entry.getValue());
            }
        }
        return dirty;
    }

    /** Marks a window as clean (after flushing to delegate). */
    public void markClean(N window) {
        PerWindowData<K> buf = windowBuffers.get(window);
        if (buf != null) {
            buf.dirty = false;
        }
    }

    public void setMaxBytes(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    public long getUsedBytes() {
        return usedBytes;
    }

    public long getMaxBytes() {
        return maxBytes;
    }

    public int getWindowCount() {
        return windowBuffers.size();
    }

    public long getBufferHits() {
        return bufferHits;
    }

    public long getBufferMisses() {
        return bufferMisses;
    }

    public long getEvictionFlushes() {
        return evictionFlushes;
    }

    public long getTotalCoalesced() {
        return totalCoalesced;
    }

    public void clear() {
        windowBuffers.clear();
        usedBytes = 0;
    }

    private static long estimateKeyOverhead() {
        return 64; // approximate: HashMap.Entry + key reference + hash
    }

    /** Data for a single window. */
    public static class PerWindowData<K> {
        final Map<K, byte[]> entries = new HashMap<>();
        long sizeBytes;
        boolean dirty;
        int coalesceCount;

        public Map<K, byte[]> getEntries() {
            return entries;
        }

        public boolean isDirty() {
            return dirty;
        }

        public int getCoalesceCount() {
            return coalesceCount;
        }
    }

    /** Result of an eviction operation. */
    public static class EvictionResult<K, N> {
        public final N window;
        public final PerWindowData<K> data;

        EvictionResult(N window, PerWindowData<K> data) {
            this.window = window;
            this.data = data;
        }
    }
}
