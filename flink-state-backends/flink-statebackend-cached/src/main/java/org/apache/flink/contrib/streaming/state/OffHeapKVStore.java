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
import java.util.List;

/**
 * A key-value store that stores its data in off-heap memory pages managed by a {@link ManagedPagePool}.
 * This store is designed to be used as the L2 cache implementation.
 */
public class OffHeapKVStore implements Closeable {

    private final ManagedPagePool pagePool;
    private final List<MemorySegment> pages;
    private final int pageSize;

    private static final int PAGE_HEADER_SIZE = 4; // Stores the next free offset
    private static final int FREE_POINTER_OFFSET = 0;

    private static final int RECORD_HEADER_SIZE = 8; // key length (int) + value length (int)
    private static final int KEY_LENGTH_OFFSET = 0;
    private static final int VALUE_LENGTH_OFFSET = 4;

    public OffHeapKVStore(ManagedPagePool pagePool) {
        this.pagePool = Preconditions.checkNotNull(pagePool);
        this.pages = new ArrayList<>();
        this.pageSize = pagePool.getPageSize();
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

        return new OffHeapPointer(pageId, freeOffset, key.length, value.length);
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
        List<MemorySegment> newPages = pagePool.allocatePages(1);
        MemorySegment newPage = newPages.get(0);
        newPage.putInt(FREE_POINTER_OFFSET, PAGE_HEADER_SIZE); // Initialize free pointer
        pages.add(newPage);
        return pages.size() - 1;
    }

    @Override
    public void close() {
        if (pagePool != null && pages != null && !pages.isEmpty()) {
            pagePool.freePages(pages);
            pages.clear();
        }
    }
} 