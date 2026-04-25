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
    static final long AVG_COLLECTION_ELEMENT_SIZE = 16L;

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
    private static final long OBJECT_SHELL_SIZE = 16;
    private static final long STRING_CHAR_SIZE = 2;
    private static final long AVG_COLLECTION_ELEMENT_SIZE = 16;
    private static final long MAP_ENTRY_OVERHEAD = 32;

    @SuppressWarnings("rawtypes")
    public static long estimate(Object o) {
        if (o == null) {
            return 0;
        }
        if (o instanceof String) {
            return ((String) o).length() * STRING_CHAR_SIZE + OBJECT_SHELL_SIZE;
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
        if (o instanceof java.util.List) {
            return ((java.util.List) o).size() * AVG_COLLECTION_ELEMENT_SIZE + OBJECT_SHELL_SIZE;
        }
        if (o instanceof java.util.Map) {
            long size = OBJECT_SHELL_SIZE;
            size += ((java.util.Map) o).size() * (AVG_COLLECTION_ELEMENT_SIZE + AVG_COLLECTION_ELEMENT_SIZE + MAP_ENTRY_OVERHEAD);
            return size;
        }
        if (o instanceof Boolean) {
            return 4; // Approximate size for a Boolean object
        }
        return AVG_COLLECTION_ELEMENT_SIZE + OBJECT_SHELL_SIZE;
    }
}
