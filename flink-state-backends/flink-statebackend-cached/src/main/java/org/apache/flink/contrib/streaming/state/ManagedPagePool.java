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
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A pool for managing memory pages allocated from Flink's {@link MemoryManager}.
 * This class helps in abstracting page allocation and freeing for off-heap caches.
 */
public class ManagedPagePool implements Closeable {

    private final MemoryManager memoryManager;
    private final int pageSize;
    private final List<MemorySegment> allocatedPages;

    public ManagedPagePool(MemoryManager memoryManager) {
        this.memoryManager = memoryManager;
        this.pageSize = memoryManager != null ? memoryManager.getPageSize() : 4096; // Default page size
        this.allocatedPages = new ArrayList<>();
    }

    /**
     * Allocates a specified number of memory pages.
     *
     * @param numPages The number of pages to allocate.
     * @return A list of allocated {@link MemorySegment}s.
     * @throws IOException If the memory allocation fails.
     */
    public List<MemorySegment> allocatePages(int numPages) throws IOException {
        if (memoryManager == null) {
            // Return empty list for demo purposes when no memory manager is available
            return new ArrayList<>();
        }
        try {
            final List<MemorySegment> pages = new ArrayList<>(numPages);
            memoryManager.allocatePages(this, pages, numPages);
            synchronized (allocatedPages) {
                allocatedPages.addAll(pages);
            }
            return pages;
        } catch (Exception e) {
            throw new IOException("Failed to allocate " + numPages + " pages from MemoryManager.", e);
        }
    }

    /**
     * Frees a collection of memory pages.
     *
     * @param pages The pages to free.
     */
    public void freePages(@Nonnull Collection<MemorySegment> pages) {
        if (pages.isEmpty()) {
            return;
        }
        synchronized (allocatedPages) {
            allocatedPages.removeAll(pages);
        }
        if (memoryManager != null) {
            memoryManager.release(pages);
        }
    }

    /**
     * @return The page size in bytes.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * @return The total number of pages currently allocated by this pool.
     */
    public int getNumberOfAllocatedPages() {
        synchronized (allocatedPages) {
            return allocatedPages.size();
        }
    }

    @Override
    public void close() {
        synchronized (allocatedPages) {
            if (!allocatedPages.isEmpty()) {
                if (memoryManager != null) {
                    memoryManager.release(allocatedPages);
                }
                allocatedPages.clear();
            }
        }
    }
} 