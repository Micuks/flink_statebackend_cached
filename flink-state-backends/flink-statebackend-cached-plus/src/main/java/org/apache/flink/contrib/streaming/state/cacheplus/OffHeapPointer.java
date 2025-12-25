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

package org.apache.flink.contrib.streaming.state.cacheplus;

import org.apache.flink.contrib.streaming.state.cacheplus.OffHeapKVStore;

import java.util.Objects;

/**
 * A pointer to a key-value pair stored in off-heap memory, managed by {@link OffHeapKVStore}.
 */
public final class OffHeapPointer {
    final int pageId;
    final int offset;
    final int keyLength;
    final int valueLength;

    public OffHeapPointer(int pageId, int offset, int keyLength, int valueLength) {
        this.pageId = pageId;
        this.offset = offset;
        this.keyLength = keyLength;
        this.valueLength = valueLength;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OffHeapPointer that = (OffHeapPointer) o;
        return pageId == that.pageId &&
                offset == that.offset &&
                keyLength == that.keyLength &&
                valueLength == that.valueLength;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pageId, offset, keyLength, valueLength);
    }

    @Override
    public String toString() {
        return "OffHeapPointer{" +
                "pageId=" + pageId +
                ", offset=" + offset +
                ", keyLength=" + keyLength +
                ", valueLength=" + valueLength +
                '}';
    }
} 
