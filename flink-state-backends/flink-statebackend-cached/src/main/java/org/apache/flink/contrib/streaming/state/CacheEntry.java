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

/**
 * Represents an entry in the cache.
 *
 * @param <V> The type of the cached value.
 */
public class CacheEntry<V> {

    private V value;
    private boolean dirty;
    private transient long estimatedSizeBytes;

    public CacheEntry(V value, boolean dirty) {
        this.value = value;
        this.dirty = dirty;
        this.estimatedSizeBytes = ValueSizeUtils.estimate(value);
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
        this.estimatedSizeBytes = ValueSizeUtils.estimate(value);
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public long getEstimatedSizeBytes() {
        return estimatedSizeBytes;
    }

    public static <V> CacheEntry<V> clean(V value) {
        return new CacheEntry<>(value, false);
    }

    public static <V> CacheEntry<V> dirty(V value) {
        return new CacheEntry<>(value, true);
    }
}

class ValueSizeUtils {
    // JVM object shell (compressed oops, 64-bit JVM)
    private static final long OBJECT_SHELL_SIZE = 12L;
    // Reference pointer size (compressed oops)
    private static final long REFERENCE_SIZE = 4L;
    // Array header size (includes length field)
    private static final long ARRAY_HEADER_SIZE = 16L;
    // HashMap: 16-byte table array header + 4 bytes per bucket entry (key+val refs)
    private static final long HASHMAP_BUCKET_REF_SIZE = 8L;
    // HashMap entry (RocksDB StateMap-like): key ref + value ref + hashcode int + next ref
    private static final long HASHMAP_ENTRY_SIZE = 32L;
    // String: object header + int hash32 + int coder + reference to char[]
    private static final long STRING_HEADER_SIZE = 40L;
    // char[] array header + 2 bytes per char
    private static final long CHAR_ARRAY_HEADER = 16L;
    private static final long CHAR_SIZE = 2L;

    @SuppressWarnings("rawtypes")
    public static long estimate(Object o) {
        if (o == null) {
            return 0;
        }
        if (o instanceof String) {
            String s = (String) o;
            int len = s.length();
            return STRING_HEADER_SIZE
                    + CHAR_ARRAY_HEADER
                    + (long) len * CHAR_SIZE
                    + REFERENCE_SIZE; // char[] reference inside String
        }
        if (o instanceof Integer) {
            return OBJECT_SHELL_SIZE + 4;
        }
        if (o instanceof Long) {
            return OBJECT_SHELL_SIZE + 8;
        }
        if (o instanceof Double) {
            return OBJECT_SHELL_SIZE + 8;
        }
        if (o instanceof Float) {
            return OBJECT_SHELL_SIZE + 4;
        }
        if (o instanceof Byte) {
            return OBJECT_SHELL_SIZE + 1;
        }
        if (o instanceof Short) {
            return OBJECT_SHELL_SIZE + 2;
        }
        if (o instanceof Character) {
            return OBJECT_SHELL_SIZE + 2;
        }
        if (o instanceof Boolean) {
            return OBJECT_SHELL_SIZE + 1;
        }
        if (o instanceof java.util.List) {
            java.util.List<?> list = (java.util.List<?>) o;
            int size = list.size();
            long elemRefsSize = (long) size * REFERENCE_SIZE;
            return ARRAY_HEADER_SIZE + elemRefsSize;
        }
        if (o instanceof java.util.Map) {
            java.util.Map<?, ?> map = (java.util.Map<?, ?>) o;
            int size = map.size();
            if (size == 0) {
                return OBJECT_SHELL_SIZE + REFERENCE_SIZE; // empty HashMap shell + table ref
            }
            // HashMap internal: shell(12) + table ref(4) + size int(4) + modCount int(4) + threshold int(4) + loadFactor float(4) = 32
            long mapShell = 32L;
            // Bucket array: first array header(16) + capacity * bucket-ref-size(8)
            int capacity = Integer.highestOneBit((int) Math.ceil(size / 0.75));
            if (capacity < 16) {
                capacity = 16;
            }
            long bucketArraySize = ARRAY_HEADER_SIZE + (long) capacity * HASHMAP_BUCKET_REF_SIZE;
            // Entry nodes: key-ref(4) + value-ref(4) + hash(4) + next-ref(4) + overhead(~16) = 32 each
            long entriesSize = (long) size * HASHMAP_ENTRY_SIZE;
            return mapShell + bucketArraySize + entriesSize;
        }
        // Fallback for unknown objects: shell + small reference
        return OBJECT_SHELL_SIZE + REFERENCE_SIZE;
    }
}
