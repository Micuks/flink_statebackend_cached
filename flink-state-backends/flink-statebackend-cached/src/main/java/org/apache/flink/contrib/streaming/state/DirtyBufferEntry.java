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
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */

package org.apache.flink.contrib.streaming.state;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates the write-behind state for a single (Flink key, namespace) pair in ListState caching.
 *
 * <p>Architecture:
 * <ul>
 *   <li>{@code flushedList}: the base list already flushed to RocksDB (may be null if never flushed).</li>
 *   <li>{@code dirtyBuffer}: new elements appended since the last flush (never null, may be empty).</li>
 *   <li>{@code dirty}: true if dirtyBuffer has unflushed elements.</li>
 * </ul>
 *
 * <p>This design enables {@code add()} to append to dirtyBuffer without reading the flushed base from
 * RocksDB, leveraging RocksDB's merge semantics on flush for O(append) writes instead of O(full-list)
 * reads.
 *
 * @param <V_ELE> element type of the list.
 */
public class DirtyBufferEntry<V_ELE> {

    /** Base list already flushed to RocksDB (null = never flushed or cleared). */
    private List<V_ELE> flushedList;

    /** New elements appended since last flush (never null). */
    private List<V_ELE> dirtyBuffer;

    /** True if dirtyBuffer is non-empty. */
    private boolean dirty;

    /** True if this entry was created by a full list replace (update()) operation, not an append (add/addAll). */
    private boolean updated;

    /** Estimated heap size in bytes. */
    private transient long estimatedSizeBytes;

    public DirtyBufferEntry(List<V_ELE> flushedList, List<V_ELE> dirtyBuffer, boolean dirty, boolean updated) {
        this.flushedList = flushedList;
        this.dirtyBuffer = dirtyBuffer != null ? dirtyBuffer : new ArrayList<>();
        this.dirty = dirty;
        this.updated = updated;
        this.estimatedSizeBytes = estimateSize();
    }

    public List<V_ELE> getFlushedList() {
        return flushedList;
    }

    public List<V_ELE> getDirtyBuffer() {
        return dirtyBuffer;
    }

    public boolean isDirty() {
        return dirty;
    }

    /**
     * Returns true if this entry was created by a full list replace (update()) operation.
     * In this case, flushedList contains the new complete state and dirtyBuffer is empty.
     */
    public boolean isUpdated() {
        return updated;
    }

    public long getEstimatedSizeBytes() {
        return estimatedSizeBytes;
    }

    /**
     * Appends a single element to the dirty buffer.
     *
     * @param element the element to append
     */
    public void addToDirty(V_ELE element) {
        if (dirtyBuffer == null) {
            dirtyBuffer = new ArrayList<>();
        }
        dirtyBuffer.add(element);
        dirty = true;
        estimatedSizeBytes = estimateSize();
    }

    /**
     * Appends all elements to the dirty buffer.
     *
     * @param elements the elements to append
     */
    public void addAllToDirty(List<V_ELE> elements) {
        if (elements == null || elements.isEmpty()) {
            return;
        }
        if (dirtyBuffer == null) {
            dirtyBuffer = new ArrayList<>();
        }
        dirtyBuffer.addAll(elements);
        dirty = true;
        estimatedSizeBytes = estimateSize();
    }

    /**
     * Marks the entry as flushed: clears dirty flag.
     * The caller is responsible for writing dirtyBuffer to RocksDB via addAll before calling this.
     */
    public void markFlushed() {
        this.dirty = false;
        this.updated = false;
        estimatedSizeBytes = estimateSize();
    }

    /**
     * Clears both flushed and dirty data.
     */
    public void clear() {
        this.flushedList = null;
        this.dirtyBuffer = new ArrayList<>();
        this.dirty = false;
        this.updated = false;
        estimatedSizeBytes = estimateSize();
    }

    /**
     * Returns the merged view: flushedList + dirtyBuffer.
     * Returns null if both are null/empty.
     */
    public List<V_ELE> getMergedList() {
        if ((flushedList == null || flushedList.isEmpty()) && dirtyBuffer.isEmpty()) {
            return null;
        }
        if (dirtyBuffer.isEmpty()) {
            return flushedList;
        }
        if (flushedList == null || flushedList.isEmpty()) {
            return dirtyBuffer;
        }
        List<V_ELE> merged = new ArrayList<>(flushedList.size() + dirtyBuffer.size());
        merged.addAll(flushedList);
        merged.addAll(dirtyBuffer);
        return merged;
    }

    /**
     * Merges dirtyBuffer into flushedList and clears the dirty flag.
     * Called after RocksDB has received the dirty data via addAll (which uses merge semantics).
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
     * Resets the entry to a clean state with a new flushed list, clearing the dirty buffer.
     * Used after flushToUnderlyingState() has written the data to RocksDB.
     *
     * @param flushedList the fully flushed list (may be null to represent empty/cleared state)
     */
    public void resetToFlushed(List<V_ELE> flushedList) {
        this.flushedList = flushedList;
        this.dirtyBuffer.clear();
        this.dirty = false;
        this.updated = false;
        estimatedSizeBytes = estimateSize();
    }

    /**
     * Truncates both flushedList and dirtyBuffer to at most maxElements, keeping the newest elements.
     *
     * @param maxElements maximum allowed elements (must be &gt; 0)
     * @return number of elements removed
     */
    public int truncateToMaxSize(int maxElements) {
        if (maxElements <= 0) {
            return 0;
        }
        int flushedSize = flushedList != null ? flushedList.size() : 0;
        int dirtySize = dirtyBuffer != null ? dirtyBuffer.size() : 0;
        int total = flushedSize + dirtySize;
        if (total <= maxElements) {
            return 0;
        }

        int removed = total - maxElements;

        // Remove oldest elements from flushedList head first
        int flushedRemoved = 0;
        if (flushedList != null && !flushedList.isEmpty()) {
            flushedRemoved = Math.min(removed, flushedList.size());
            int keepFromFlushed = flushedList.size() - flushedRemoved;
            if (keepFromFlushed == 0) {
                flushedList = null;
            } else {
                flushedList = new ArrayList<>(
                        flushedList.subList(flushedList.size() - keepFromFlushed, flushedList.size()));
            }
        }

        int remainingToRemove = removed - flushedRemoved;
        if (remainingToRemove > 0 && dirtyBuffer != null && !dirtyBuffer.isEmpty()) {
            int keepFromDirty = dirtyBuffer.size() - remainingToRemove;
            if (keepFromDirty <= 0) {
                dirtyBuffer.clear();
            } else {
                dirtyBuffer = new ArrayList<>(
                        dirtyBuffer.subList(dirtyBuffer.size() - keepFromDirty, dirtyBuffer.size()));
            }
        }

        estimatedSizeBytes = estimateSize();
        return removed;
    }

    private long estimateSize() {
        long size = 0;
        if (flushedList != null) {
            for (V_ELE ele : flushedList) {
                if (ele != null) {
                    size += ValueSizeUtils.estimate(ele);
                }
            }
        }
        if (dirtyBuffer != null) {
            for (V_ELE ele : dirtyBuffer) {
                if (ele != null) {
                    size += ValueSizeUtils.estimate(ele);
                }
            }
        }
        return Math.max(size, 0);
    }

    /** Creates an empty entry (no flushed data, empty dirty buffer, not dirty). */
    public static <V_ELE> DirtyBufferEntry<V_ELE> empty() {
        return new DirtyBufferEntry<>(null, new ArrayList<>(), false, false);
    }
}
