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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A key-value store that stores its data in off-heap memory pages managed by a {@link ManagedPagePool}.
 * This store is designed to be used as the L2 cache implementation.
 */
public class OffHeapKVStore implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(OffHeapKVStore.class);

    private final ManagedPagePool pagePool;
    // Optional: report page allocations/frees to cache memory accounting
    private final CachingKeyedStateBackend<?> ownerBackend;
    private final List<MemorySegment> pages;
    private final int pageSize;
    
    // Map from key hash to pointer for efficient lookups
    private final Map<ByteArrayWrapper, OffHeapPointer> keyIndex;
    private int entryCount = 0;

    private static final int PAGE_HEADER_SIZE = 4; // Stores the next free offset
    private static final int FREE_POINTER_OFFSET = 0;

    private static final int RECORD_HEADER_SIZE = 8; // key length (int) + value length (int)
    private static final int KEY_LENGTH_OFFSET = 0;
    private static final int VALUE_LENGTH_OFFSET = 4;

    // --- Time-bucketed eviction support ---
    // Size of one time bucket in milliseconds. When > 0, new records are written to pages
    // dedicated to the current time bucket (based on processing time), and we can evict
    // whole buckets in O(number_of_pages_in_bucket).
    private final long timeBucketSizeMillis;

    // pageId -> bucketId (derived from time);
    private final Map<Integer, Long> pageToBucket = new HashMap<>();
    // bucketId -> list of pageIds (append-only, pages remain addressable by id)
    private final Map<Long, List<Integer>> bucketToPages = new HashMap<>();

    // Basic counters for observability
    private long pagesFreedByCapacity = 0L;
    private long pagesFreedByWatermark = 0L;
    private long bytesFreedByCapacity = 0L;
    private long bytesFreedByWatermark = 0L;

    // For a simple approx pages/sec gauge
    private long lastGaugeCheckNanos = System.nanoTime();
    private long lastPagesFreedTotal = 0L;

    public OffHeapKVStore(ManagedPagePool pagePool) {
        this.pagePool = Preconditions.checkNotNull(pagePool);
        this.ownerBackend = null;
        this.pages = new ArrayList<>();
        this.pageSize = pagePool.getPageSize();
        this.keyIndex = new HashMap<>();
        this.timeBucketSizeMillis = 0L;
    }

    public OffHeapKVStore(ManagedPagePool pagePool, CachingKeyedStateBackend<?> ownerBackend) {
        this.pagePool = Preconditions.checkNotNull(pagePool);
        this.ownerBackend = ownerBackend; // may be null in tests
        this.pages = new ArrayList<>();
        this.pageSize = pagePool.getPageSize();
        this.keyIndex = new HashMap<>();
        this.timeBucketSizeMillis = 0L;
    }

    public OffHeapKVStore(ManagedPagePool pagePool,
                          CachingKeyedStateBackend<?> ownerBackend,
                          long timeBucketSizeMillis) {
        this.pagePool = Preconditions.checkNotNull(pagePool);
        this.ownerBackend = ownerBackend; // may be null in tests
        this.pages = new ArrayList<>();
        this.pageSize = pagePool.getPageSize();
        this.keyIndex = new HashMap<>();
        this.timeBucketSizeMillis = Math.max(0L, timeBucketSizeMillis);
    }

    /**
     * Puts a key-value pair into the store.
     *
     * @param key   The key.
     * @param value The value.
     * @return A pointer to the stored data.
     * @throws IOException If a new page cannot be allocated.
     */
    public OffHeapPointer put(byte[] key, byte[] value) throws IOException {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);

        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        
        // Remove old entry if it exists
        OffHeapPointer oldPointer = keyIndex.remove(keyWrapper);
        if (oldPointer != null) {
            remove(oldPointer);
            entryCount--;
            // Replacing existing entry keeps global count unchanged
        }

        int requiredSize = RECORD_HEADER_SIZE + key.length + value.length;
        if (requiredSize > pageSize - PAGE_HEADER_SIZE) {
            throw new IOException("Record size (" + requiredSize + " bytes) is larger than page payload size (" + (pageSize - PAGE_HEADER_SIZE) + " bytes).");
        }

        int pageId;
        if (timeBucketSizeMillis > 0L) {
            long bucketId = System.currentTimeMillis() / timeBucketSizeMillis;
            pageId = findPageFor(requiredSize, bucketId);
        } else {
            pageId = findPageFor(requiredSize);
        }
        MemorySegment page = pages.get(pageId);

        int freeOffset = page.getInt(FREE_POINTER_OFFSET);
        
        // Write record: [key_len, value_len, key, value]
        page.putInt(freeOffset + KEY_LENGTH_OFFSET, key.length);
        page.putInt(freeOffset + VALUE_LENGTH_OFFSET, value.length);
        page.put(freeOffset + RECORD_HEADER_SIZE, key, 0, key.length);
        page.put(freeOffset + RECORD_HEADER_SIZE + key.length, value, 0, value.length);

        page.putInt(FREE_POINTER_OFFSET, freeOffset + requiredSize);

        OffHeapPointer pointer = new OffHeapPointer(pageId, freeOffset, key.length, value.length);
        keyIndex.put(keyWrapper, pointer);
        entryCount++;
        if (oldPointer == null && ownerBackend != null) {
            try { ownerBackend.getGlobalL2MapEntryCount().incrementAndGet(); } catch (Throwable ignore) { }
        }
        
        return pointer;
    }
    
    /**
     * Gets the value for a given key.
     *
     * @param key The key to look up.
     * @return The value as a byte array, or null if the key is not found.
     */
    public byte[] get(byte[] key) {
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        OffHeapPointer pointer = keyIndex.get(keyWrapper);
        if (pointer == null) {
            return null;
        }
        return get(pointer, false); // false = get value, not key
    }
    
    /**
     * Removes an entry by key.
     *
     * @param key The key to remove.
     * @return The removed value, or null if the key was not found.
     */
    public byte[] remove(byte[] key) {
        ByteArrayWrapper keyWrapper = new ByteArrayWrapper(key);
        OffHeapPointer pointer = keyIndex.remove(keyWrapper);
        if (pointer == null) {
            return null;
        }
        
        byte[] value = get(pointer, false);
        remove(pointer);
        entryCount--;
        if (ownerBackend != null) {
            try { ownerBackend.getGlobalL2MapEntryCount().decrementAndGet(); } catch (Throwable ignore) { }
        }
        return value;
    }
    
    /**
     * Returns true if this store contains no entries.
     */
    public boolean isEmpty() {
        return entryCount == 0;
    }
    
    /**
     * Returns the number of entries in this store.
     */
    public int size() {
        return entryCount;
    }
    
    /**
     * Returns an iterator over the keys in this store.
     */
    public Iterator<byte[]> keyIterator() {
        return keyIndex.keySet().stream().map(wrapper -> wrapper.data).iterator();
    }
    
    /**
     * Clears all entries from this store.
     */
    public void clear() {
        int removed = entryCount;
        keyIndex.clear();
        entryCount = 0;
        if (!pages.isEmpty()) {
            List<MemorySegment> toFree = new ArrayList<>();
            for (MemorySegment p : pages) {
                if (p != null) toFree.add(p);
            }
            long freedBytes = (long) toFree.size() * pageSize;
            if (!toFree.isEmpty()) {
                pagePool.freePages(toFree);
            }
            pages.clear();
            pageToBucket.clear();
            bucketToPages.clear();
            if (ownerBackend != null && freedBytes > 0) {
                ownerBackend.reportCacheMemoryReleased(freedBytes);
            }
        }
        if (ownerBackend != null && removed > 0) {
            try { ownerBackend.getGlobalL2MapEntryCount().addAndGet(-removed); } catch (Throwable ignore) { }
        }
    }
    
    /**
     * Evicts approximately the specified number of bytes from the store.
     * This is a simple implementation that just clears pages from the end.
     * 
     * @param targetBytes The target number of bytes to free.
     * @return The actual number of bytes freed.
     */
    public long evict(long targetBytes) {
        if (targetBytes <= 0 || pages.isEmpty()) {
            return 0;
        }
        long freed = 0L;
        long bytesToFree = Math.min(targetBytes, getEstimatedMemoryUsageBytes());

        // Prefer evicting oldest pages (by bucket if enabled, otherwise by index order)
        if (timeBucketSizeMillis > 0L && !bucketToPages.isEmpty()) {
            // Iterate buckets in ascending order (oldest first)
            List<Long> bucketIds = new ArrayList<>(bucketToPages.keySet());
            bucketIds.sort(Long::compareTo);
            for (Long bId : bucketIds) {
                List<Integer> pids = bucketToPages.get(bId);
                if (pids == null) continue;
                // Iterate page ids in insertion order
                for (int i = 0; i < pids.size() && freed < bytesToFree; i++) {
                    Integer pid = pids.get(i);
                    if (pid == null) continue;
                    if (freePageIfPresent(pid, false)) {
                        freed += pageSize;
                        // mark in list as removed
                        pids.set(i, null);
                        pagesFreedByCapacity++;
                        bytesFreedByCapacity += pageSize;
                    }
                }
                // Optionally compact the list to avoid too many nulls
                // but keep it simple here.
                if (freed >= bytesToFree) break;
            }
        } else {
            // Fallback: scan pages by index, freeing from the oldest indices
            for (int pid = 0; pid < pages.size() && freed < bytesToFree; pid++) {
                if (freePageIfPresent(pid, false)) {
                    freed += pageSize;
                    pagesFreedByCapacity++;
                    bytesFreedByCapacity += pageSize;
                }
            }
        }
        if (ownerBackend != null && freed > 0) {
            ownerBackend.reportCacheMemoryReleased(freed);
        }
        return freed;
    }
    
    /**
     * Returns an estimate of the memory usage in bytes.
     */
    public long getEstimatedMemoryUsageBytes() {
        long count = 0;
        for (MemorySegment p : pages) {
            if (p != null) count++;
        }
        return count * (long) pageSize;
    }

    /** Evict all pages that belong to buckets strictly older than the given watermark time. */
    public long evictBucketsUpTo(long watermarkMillis) {
        if (timeBucketSizeMillis <= 0L || bucketToPages.isEmpty()) {
            return 0L;
        }
        long watermarkBucket = watermarkMillis / timeBucketSizeMillis;
        long freed = 0L;
        List<Long> bucketIds = new ArrayList<>(bucketToPages.keySet());
        for (Long bId : bucketIds) {
            if (bId < watermarkBucket) {
                List<Integer> pids = bucketToPages.get(bId);
                if (pids == null) continue;
                for (int i = 0; i < pids.size(); i++) {
                    Integer pid = pids.get(i);
                    if (pid == null) continue;
                    if (freePageIfPresent(pid, true)) {
                        freed += pageSize;
                        pids.set(i, null);
                        pagesFreedByWatermark++;
                        bytesFreedByWatermark += pageSize;
                    }
                }
                // Optionally remove the bucket entry after full scan
                // Keep it to avoid concurrent modification risks.
            }
        }
        if (ownerBackend != null && freed > 0) {
            ownerBackend.reportCacheMemoryReleased(freed);
        }
        return freed;
    }

    /**
     * Gets the key or value for a given pointer.
     *
     * @param pointer The pointer to the data.
     * @param getKey  If true, returns the key; otherwise, returns the value.
     * @return The key or value as a byte array.
     */
    public byte[] get(OffHeapPointer pointer, boolean getKey) {
        Preconditions.checkNotNull(pointer);
        MemorySegment page = pages.get(pointer.pageId);
        
        int dataOffset = pointer.offset + RECORD_HEADER_SIZE;
        byte[] result;

        if (getKey) {
            result = new byte[pointer.keyLength];
            page.get(dataOffset, result, 0, pointer.keyLength);
        } else {
            result = new byte[pointer.valueLength];
            page.get(dataOffset + pointer.keyLength, result, 0, pointer.valueLength);
        }
        return result;
    }

    /**
     * Marks the space pointed to by the pointer as reclaimable.
     * Note: This is a no-op for now. Compaction is not yet implemented.
     *
     * @param pointer The pointer to the data to remove.
     */
    public void remove(OffHeapPointer pointer) {
        // No-op. Space will be reclaimed when the store is cleared/closed.
    }

    private int findPageFor(int requiredSize) throws IOException {
        for (int i = 0; i < pages.size(); i++) {
            MemorySegment page = pages.get(i);
            if (page == null) {
                continue;
            }
            if (page.getInt(FREE_POINTER_OFFSET) + requiredSize <= pageSize) {
                return i;
            }
        }
        // No page with enough space, allocate a new one.
        try {
            List<MemorySegment> newPages = pagePool.allocatePages(1);
            if (newPages.isEmpty()) {
                throw new IOException("No managed memory pages allocated for OffHeapKVStore.",
                        new org.apache.flink.runtime.memory.MemoryAllocationException("No pages"));
            }
            MemorySegment newPage = newPages.get(0);
            newPage.putInt(FREE_POINTER_OFFSET, PAGE_HEADER_SIZE); // Initialize free pointer
            pages.add(newPage);
            // Register page without a time bucket
            pageToBucket.put(pages.size() - 1, null);
            if (ownerBackend != null) {
                ownerBackend.reportCacheMemoryAdded((long) pageSize * newPages.size());
            }
            return pages.size() - 1;
        } catch (IOException allocEx) {
            // Best-effort: try to evict current contents and retry once
            long currentlyUsed = getEstimatedMemoryUsageBytes();
            if (currentlyUsed > 0) {
                long freed = evict(currentlyUsed);
                if (freed > 0) {
                    List<MemorySegment> newPages = pagePool.allocatePages(1);
                    if (newPages.isEmpty()) {
                        throw new IOException("No managed memory pages allocated for OffHeapKVStore after eviction.",
                                new org.apache.flink.runtime.memory.MemoryAllocationException("No pages after eviction"));
                    }
                    MemorySegment newPage = newPages.get(0);
                    newPage.putInt(FREE_POINTER_OFFSET, PAGE_HEADER_SIZE);
                    pages.add(newPage);
                    pageToBucket.put(pages.size() - 1, null);
                    if (ownerBackend != null) {
                        ownerBackend.reportCacheMemoryAdded((long) pageSize * newPages.size());
                    }
                    return pages.size() - 1;
                }
            }
            throw allocEx;
        }
    }

    private int findPageFor(int requiredSize, long bucketId) throws IOException {
        // Try to find a non-full page for the given bucket
        for (Map.Entry<Integer, Long> e : pageToBucket.entrySet()) {
            Integer pid = e.getKey();
            Long bId = e.getValue();
            if (pid == null || bId == null) continue;
            MemorySegment page = pages.get(pid);
            if (page == null) continue;
            if (bId == bucketId && page.getInt(FREE_POINTER_OFFSET) + requiredSize <= pageSize) {
                return pid;
            }
        }
        // Else allocate a new page dedicated to this bucket
        int pid = findPageFor(requiredSize);
        pageToBucket.put(pid, bucketId);
        bucketToPages.computeIfAbsent(bucketId, k -> new ArrayList<>()).add(pid);
        return pid;
    }

    private boolean freePageIfPresent(int pageId, boolean watermarkReason) {
        if (pageId < 0 || pageId >= pages.size()) return false;
        MemorySegment page = pages.get(pageId);
        if (page == null) return false;

        // Scan the page and remove any keys whose current pointer still points to this page/offset
        int freeOffset = page.getInt(FREE_POINTER_OFFSET);
        int pos = PAGE_HEADER_SIZE;
        while (pos + RECORD_HEADER_SIZE <= freeOffset) {
            int kLen = page.getInt(pos + KEY_LENGTH_OFFSET);
            int vLen = page.getInt(pos + VALUE_LENGTH_OFFSET);
            int recordSize = RECORD_HEADER_SIZE + kLen + vLen;
            if (pos + recordSize > freeOffset) {
                break; // Defensive: corrupted or partial
            }
            byte[] keyBytes = new byte[kLen];
            page.get(pos + RECORD_HEADER_SIZE, keyBytes, 0, kLen);
            ByteArrayWrapper keyWrapper = new ByteArrayWrapper(keyBytes);
            OffHeapPointer p = keyIndex.get(keyWrapper);
            if (p != null && p.pageId == pageId && p.offset == pos) {
                keyIndex.remove(keyWrapper);
                entryCount--;
                if (ownerBackend != null) {
                    try { ownerBackend.getGlobalL2MapEntryCount().decrementAndGet(); } catch (Throwable ignore) { }
                }
            }
            pos += recordSize;
        }

        // Free the page memory and mark as null to keep pageId stable
        pagePool.freePages(java.util.Collections.singletonList(page));
        pages.set(pageId, null);
        // Clean up bucket mappings
        Long bId = pageToBucket.remove(pageId);
        if (bId != null) {
            List<Integer> pids = bucketToPages.get(bId);
            if (pids != null) {
                for (int i = 0; i < pids.size(); i++) {
                    if (pids.get(i) != null && pids.get(i) == pageId) {
                        pids.set(i, null);
                    }
                }
            }
        }
        return true;
    }

    @Override
    public void close() {
        int removed = entryCount;
        keyIndex.clear();
        if (!pages.isEmpty()) {
            List<MemorySegment> toFree = new ArrayList<>();
            for (MemorySegment p : pages) {
                if (p != null) toFree.add(p);
            }
            long freedBytes = (long) toFree.size() * pageSize;
            if (!toFree.isEmpty()) {
                pagePool.freePages(toFree);
            }
            pages.clear();
            pageToBucket.clear();
            bucketToPages.clear();
            if (ownerBackend != null && freedBytes > 0) {
                ownerBackend.reportCacheMemoryReleased(freedBytes);
            }
        }
        if (ownerBackend != null && removed > 0) {
            try { ownerBackend.getGlobalL2MapEntryCount().addAndGet(-removed); } catch (Throwable ignore) { }
        }
    }

    /**
     * Returns the current number of allocated managed pages.
     */
    public int getPageCount() {
        int c = 0;
        for (MemorySegment p : pages) {
            if (p != null) c++;
        }
        return c;
    }

    // --- Observability accessors ---
    public long getPagesFreedByCapacity() { return pagesFreedByCapacity; }
    public long getPagesFreedByWatermark() { return pagesFreedByWatermark; }
    public long getBytesFreedByCapacity() { return bytesFreedByCapacity; }
    public long getBytesFreedByWatermark() { return bytesFreedByWatermark; }
    public long getTimeBucketSizeMillis() { return timeBucketSizeMillis; }
    public double getApproxPagesFreedPerSecond() {
        long now = System.nanoTime();
        long total = pagesFreedByCapacity + pagesFreedByWatermark;
        long deltaPages = total - lastPagesFreedTotal;
        long deltaNanos = Math.max(1L, now - lastGaugeCheckNanos);
        double perSec = (double) deltaPages * 1_000_000_000.0 / (double) deltaNanos;
        // update snapshot
        lastPagesFreedTotal = total;
        lastGaugeCheckNanos = now;
        return perSec;
    }
    
    /**
     * Wrapper class for byte arrays to use as keys in HashMap.
     */
    private static final class ByteArrayWrapper {
        final byte[] data;
        private final int hash;
        
        ByteArrayWrapper(byte[] data) {
            this.data = data;
            this.hash = Arrays.hashCode(data);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof ByteArrayWrapper)) return false;
            return Arrays.equals(data, ((ByteArrayWrapper) obj).data);
        }
        
        @Override
        public int hashCode() {
            return hash;
        }
    }
} 
