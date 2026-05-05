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

package org.apache.flink.contrib.streaming.state;

import java.util.ArrayList;
import java.util.List;

/**
 * A cache entry for ListState in the dedicated ListState cache.
 *
 * <p>This class encapsulates the state for a single (key, namespace) pair in the ListState
 * cache, combining the write-behind pattern from DirtyBufferEntry with access tracking
 * for LRU eviction.
 *
 * <p>Design:
 * <ul>
 *   <li>{@code flushedList}: the base list already flushed to RocksDB (may be null).</li>
 *   <li>{@code dirtyBuffer}: new elements appended since the last flush (never null).</li>
 *   <li>{@code dirty}: true if dirtyBuffer has unflushed elements.</li>
 *   <li>{@code updated}: true if this entry was created by a full list replace (update()).</li>
 *   <li>{@code accessCount/lastAccessTime}: used for LRU eviction decisions.</li>
 * </ul>
 *
 * @param <V> element type of the list.
 */
public class ListStateCacheEntry<V> {

    // ========== Core data from DirtyBufferEntry ==========
    private List<V> flushedList;
    private List<V> dirtyBuffer;
    private boolean dirty;
    private boolean updated;

    // ========== Access tracking for LRU ==========
    private final long createdAt;
    private volatile long lastAccessTime;
    private volatile long accessCount;

    // ========== Memory estimation ==========
    private volatile long estimatedSizeBytes;

    // ========== Hot entry detection ==========
    private volatile boolean isHotEntry;
    private static final long HOT_THRESHOLD_ACCESS_COUNT = 100;

    // ========== Constructors ==========

    /**
     * Creates an empty entry.
     */
    public ListStateCacheEntry() {
        this.createdAt = System.nanoTime();
        this.lastAccessTime = this.createdAt;
        this.accessCount = 0;
        this.flushedList = null;
        this.dirtyBuffer = new ArrayList<>();
        this.dirty = false;
        this.updated = false;
        this.isHotEntry = false;
        this.estimatedSizeBytes = estimateSize();
    }

    /**
     * Creates an entry with initial flushed list.
     */
    public ListStateCacheEntry(List<V> flushedList) {
        this.createdAt = System.nanoTime();
        this.lastAccessTime = this.createdAt;
        this.accessCount = 0;
        this.flushedList = flushedList != null ? new ArrayList<>(flushedList) : null;
        this.dirtyBuffer = new ArrayList<>();
        this.dirty = false;
        this.updated = false;
        this.isHotEntry = false;
        this.estimatedSizeBytes = estimateSize();
    }

    // ========== Core operations ==========

    /**
     * Appends a single element to the dirty buffer.
     */
    public void add(V element) {
        if (element == null) {
            return;
        }
        if (dirtyBuffer == null) {
            dirtyBuffer = new ArrayList<>();
        }
        dirtyBuffer.add(element);
        dirty = true;
        recordAccess();
        estimatedSizeBytes = estimateSize();
    }

    /**
     * Appends all elements to the dirty buffer.
     */
    public void addAll(List<V> elements) {
        if (elements == null || elements.isEmpty()) {
            return;
        }
        if (dirtyBuffer == null) {
            dirtyBuffer = new ArrayList<>();
        }
        dirtyBuffer.addAll(elements);
        dirty = true;
        recordAccess();
        estimatedSizeBytes = estimateSize();
    }

    /**
     * Returns the merged view: flushedList + dirtyBuffer.
     * Returns null if both are null/empty.
     */
    public List<V> getMergedList() {
        if ((flushedList == null || flushedList.isEmpty()) &&
            (dirtyBuffer == null || dirtyBuffer.isEmpty())) {
            return null;
        }
        if (dirtyBuffer == null || dirtyBuffer.isEmpty()) {
            return flushedList;
        }
        if (flushedList == null || flushedList.isEmpty()) {
            return dirtyBuffer;
        }
        List<V> merged = new ArrayList<>(flushedList.size() + dirtyBuffer.size());
        merged.addAll(flushedList);
        merged.addAll(dirtyBuffer);
        return merged;
    }

    /**
     * Records an access for LRU tracking and hot entry detection.
     */
    public void recordAccess() {
        this.lastAccessTime = System.nanoTime();
        this.accessCount++;
        if (accessCount >= HOT_THRESHOLD_ACCESS_COUNT && !isHotEntry) {
            isHotEntry = true;
        }
    }

    /**
     * Marks the entry as flushed: merges dirtyBuffer into flushedList and clears dirty flag.
     * This should be called after the dirty data has been written to RocksDB.
     */
    public void markFlushed() {
        if (dirtyBuffer != null && !dirtyBuffer.isEmpty()) {
            if (flushedList == null) {
                flushedList = new ArrayList<>(dirtyBuffer);
            } else {
                flushedList.addAll(dirtyBuffer);
            }
            dirtyBuffer.clear();
        }
        this.dirty = false;
        this.updated = false;
        estimatedSizeBytes = estimateSize();
    }

    /**
     * Marks the entry as updated (full list replace via update()).
     */
    public void markUpdated(List<V> newList) {
        this.flushedList = newList != null ? new ArrayList<>(newList) : null;
        this.dirtyBuffer.clear();
        this.dirty = false;
        this.updated = true;
        recordAccess();
        estimatedSizeBytes = estimateSize();
    }

    /**
     * Clears both flushed and dirty data.
     */
    public void clear() {
        this.flushedList = null;
        if (dirtyBuffer != null) {
            dirtyBuffer.clear();
        } else {
            dirtyBuffer = new ArrayList<>();
        }
        this.dirty = false;
        this.updated = false;
        estimatedSizeBytes = estimateSize();
    }

    /**
     * Merges dirtyBuffer into flushedList and clears the dirty flag.
     * Called after RocksDB has received the dirty data via addAll.
     */
    public void mergeDirtyIntoFlushed() {
        if (dirtyBuffer == null || dirtyBuffer.isEmpty()) {
            this.dirty = false;
            estimatedSizeBytes = estimateSize();
            return;
        }
        if (flushedList == null) {
            flushedList = new ArrayList<>(dirtyBuffer);
        } else {
            flushedList.addAll(dirtyBuffer);
        }
        dirtyBuffer.clear();
        dirty = false;
        estimatedSizeBytes = estimateSize();
    }

    /**
     * Resets the entry to a clean state with a new flushed list.
     */
    public void resetToFlushed(List<V> flushedList) {
        this.flushedList = flushedList;
        if (dirtyBuffer != null) {
            dirtyBuffer.clear();
        } else {
            dirtyBuffer = new ArrayList<>();
        }
        this.dirty = false;
        this.updated = false;
        estimatedSizeBytes = estimateSize();
    }

    // ========== Memory estimation ==========

    /**
     * Estimates the heap memory size of this entry.
     */
    public long estimateSize() {
        long flushedSize = flushedList != null ?
                ListStateSizeEstimator.estimateListSize(flushedList) : 0;
        long dirtySize = dirtyBuffer != null ?
                ListStateSizeEstimator.estimateListSize(dirtyBuffer) : 0;

        // Object overhead: header(12) + refs(16) + booleans(2) + padding(2)
        long objectOverhead = 32L;

        return flushedSize + dirtySize + objectOverhead;
    }

    // ========== Getters and setters ==========

    public List<V> getFlushedList() {
        return flushedList;
    }

    public void setFlushedList(List<V> flushedList) {
        this.flushedList = flushedList;
        estimatedSizeBytes = estimateSize();
    }

    public List<V> getDirtyBuffer() {
        return dirtyBuffer;
    }

    public boolean isDirty() {
        return dirty;
    }

    public boolean isUpdated() {
        return updated;
    }

    public boolean isHot() {
        return isHotEntry;
    }

    public long getAccessCount() {
        return accessCount;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public long getEstimatedSizeBytes() {
        return estimatedSizeBytes;
    }

    // ========== Factory methods ==========

    /**
     * Creates an empty entry (no flushed data, empty dirty buffer, not dirty).
     */
    public static <V> ListStateCacheEntry<V> empty() {
        return new ListStateCacheEntry<>();
    }
}
