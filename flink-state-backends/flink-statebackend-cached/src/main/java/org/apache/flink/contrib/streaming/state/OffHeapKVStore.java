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

    public OffHeapKVStore(ManagedPagePool pagePool) {
        this.pagePool = Preconditions.checkNotNull(pagePool);
        this.ownerBackend = null;
        this.pages = new ArrayList<>();
        this.pageSize = pagePool.getPageSize();
        this.keyIndex = new HashMap<>();
    }

    public OffHeapKVStore(ManagedPagePool pagePool, CachingKeyedStateBackend<?> ownerBackend) {
        this.pagePool = Preconditions.checkNotNull(pagePool);
        this.ownerBackend = ownerBackend; // may be null in tests
        this.pages = new ArrayList<>();
        this.pageSize = pagePool.getPageSize();
        this.keyIndex = new HashMap<>();
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
        }

        int requiredSize = RECORD_HEADER_SIZE + key.length + value.length;
        if (requiredSize > pageSize - PAGE_HEADER_SIZE) {
            throw new IOException("Record size (" + requiredSize + " bytes) is larger than page payload size (" + (pageSize - PAGE_HEADER_SIZE) + " bytes).");
        }

        int pageId = findPageFor(requiredSize);
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
        keyIndex.clear();
        entryCount = 0;
        if (!pages.isEmpty()) {
            long freedBytes = (long) pages.size() * pageSize;
            pagePool.freePages(pages);
            pages.clear();
            if (ownerBackend != null && freedBytes > 0) {
                ownerBackend.reportCacheMemoryReleased(freedBytes);
            }
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
        
        // Simple eviction: clear everything if requested
        long freedBytes = getEstimatedMemoryUsageBytes();
        clear();
        return Math.min(freedBytes, targetBytes);
    }
    
    /**
     * Returns an estimate of the memory usage in bytes.
     */
    public long getEstimatedMemoryUsageBytes() {
        return (long) pages.size() * pageSize;
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
            if (page.getInt(FREE_POINTER_OFFSET) + requiredSize <= pageSize) {
                return i;
            }
        }
        // No page with enough space, allocate a new one.
        try {
            List<MemorySegment> newPages = pagePool.allocatePages(1);
            MemorySegment newPage = newPages.get(0);
            newPage.putInt(FREE_POINTER_OFFSET, PAGE_HEADER_SIZE); // Initialize free pointer
            pages.add(newPage);
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
                    MemorySegment newPage = newPages.get(0);
                    newPage.putInt(FREE_POINTER_OFFSET, PAGE_HEADER_SIZE);
                    pages.add(newPage);
                    if (ownerBackend != null) {
                        ownerBackend.reportCacheMemoryAdded((long) pageSize * newPages.size());
                    }
                    return pages.size() - 1;
                }
            }
            throw allocEx;
        }
    }

    @Override
    public void close() {
        keyIndex.clear();
        if (!pages.isEmpty()) {
            long freedBytes = (long) pages.size() * pageSize;
            pagePool.freePages(pages);
            pages.clear();
            if (ownerBackend != null && freedBytes > 0) {
                ownerBackend.reportCacheMemoryReleased(freedBytes);
            }
        }
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
