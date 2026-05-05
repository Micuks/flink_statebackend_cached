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

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Estimates heap memory size for List elements in ListState caching.
 *
 * <p>This class provides more accurate size estimation than the generic ValueSizeUtils,
 * especially for complex objects like MapRecord that may contain nested HashMaps.
 *
 * <p>The estimator uses type-based lookup tables with fallback to serialization-based estimation
 * for unknown types.
 */
public class ListStateSizeEstimator {

    // JVM object shell size (compressed oops, 64-bit JVM)
    private static final long OBJECT_SHELL_SIZE = 12L;
    private static final long REFERENCE_SIZE = 4L;
    private static final long ARRAY_HEADER_SIZE = 16L;

    // Type-specific size estimates (in bytes)
    // These values are calibrated based on actual benchmarks
    private static final Map<Class<?>, Long> TYPE_SIZE_ESTIMATES = new HashMap<>();

    static {
        // Primitive wrappers
        TYPE_SIZE_ESTIMATES.put(Boolean.class, 17L);
        TYPE_SIZE_ESTIMATES.put(Byte.class, 17L);
        TYPE_SIZE_ESTIMATES.put(Character.class, 18L);
        TYPE_SIZE_ESTIMATES.put(Short.class, 18L);
        TYPE_SIZE_ESTIMATES.put(Integer.class, 16L);
        TYPE_SIZE_ESTIMATES.put(Float.class, 16L);
        TYPE_SIZE_ESTIMATES.put(Long.class, 24L);
        TYPE_SIZE_ESTIMATES.put(Double.class, 24L);

        // String average size (depends on content length)
        TYPE_SIZE_ESTIMATES.put(String.class, 64L);

        // Array types
        TYPE_SIZE_ESTIMATES.put(byte[].class, 64L);     // average byte array
        TYPE_SIZE_ESTIMATES.put(int[].class, 128L);     // average int array
        TYPE_SIZE_ESTIMATES.put(long[].class, 256L);     // average long array
        TYPE_SIZE_ESTIMATES.put(Object[].class, 128L);   // average object array

        // Default for complex objects (POJOs, nested structures)
        // This should be calibrated based on actual benchmarks
        TYPE_SIZE_ESTIMATES.put(Object.class, 512L);

        // Special registration point for known complex types
        // Users can register custom types via the registerTypeSize method
    }

    // User-registered type size estimates (takes precedence over static estimates)
    private static final Map<Class<?>, Long> REGISTERED_TYPES = new HashMap<>();

    /**
     * Registers a custom type size estimate for a specific class.
     * This is useful for complex objects like MapRecord that have non-standard sizes.
     *
     * @param clazz the class to register
     * @param estimatedSize the estimated heap size in bytes
     */
    public static void registerTypeSize(Class<?> clazz, long estimatedSize) {
        REGISTERED_TYPES.put(clazz, estimatedSize);
    }

    /**
     * Estimates the heap memory size of a List and all its elements.
     *
     * @param list the list to estimate (may be null)
     * @return the estimated heap size in bytes
     */
    public static long estimateListSize(List<?> list) {
        if (list == null || list.isEmpty()) {
            // Empty ArrayList: header(12) + elementData ref(4) = ~16 bytes
            return OBJECT_SHELL_SIZE + REFERENCE_SIZE;
        }

        // ArrayList object shell: header(12) + modCount(4) + size(4) + elementData ref(4) = 24
        // Plus the elementData array: header(16) + element refs(size * 4)
        long size = OBJECT_SHELL_SIZE + 3 * REFERENCE_SIZE; // shell overhead
        size += ARRAY_HEADER_SIZE; // elementData array header
        size += (long) list.size() * REFERENCE_SIZE; // element references

        // Add estimated size for each element
        for (Object elem : list) {
            if (elem != null) {
                size += estimateElementSize(elem);
            }
        }

        return size;
    }

    /**
     * Estimates the heap memory size of a single element.
     *
     * @param element the element to estimate (may be null)
     * @return the estimated heap size in bytes
     */
    public static long estimateElementSize(Object element) {
        if (element == null) {
            return 0;
        }

        Class<?> clazz = element.getClass();

        // 1. Check user-registered types first (highest priority)
        Long registered = REGISTERED_TYPES.get(clazz);
        if (registered != null) {
            return registered;
        }

        // 2. Check static type estimates
        Long estimated = TYPE_SIZE_ESTIMATES.get(clazz);
        if (estimated != null) {
            return estimated;
        }

        // 3. Interface/superclass matching
        for (Map.Entry<Class<?>, Long> entry : TYPE_SIZE_ESTIMATES.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return entry.getValue();
            }
        }

        // 4. Fallback: serialization-based estimation
        try {
            return estimateBySerialization(element);
        } catch (Exception e) {
            // 5. Final fallback: conservative estimate
            return OBJECT_SHELL_SIZE + REFERENCE_SIZE;
        }
    }

    /**
     * Estimates size by serializing the object and applying a ratio.
     * This is useful for complex objects where the serialized form is a good proxy for heap size.
     */
    private static long estimateBySerialization(Object element) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(element);
        oos.close();
        byte[] serialized = baos.toByteArray();
        // Serialized size is typically larger than heap size due to class metadata,
        // so we apply a ratio to estimate the actual heap footprint
        return (long) (serialized.length * 0.75);
    }

    /**
     * Estimates the size of a DirtyBufferEntry including its internal structures.
     *
     * @param flushedList the flushed list (may be null)
     * @param dirtyBuffer the dirty buffer (may be null)
     * @return the estimated heap size in bytes
     */
    public static long estimateDirtyBufferEntrySize(List<?> flushedList, List<?> dirtyBuffer) {
        long size = OBJECT_SHELL_SIZE; // DirtyBufferEntry object header
        size += REFERENCE_SIZE; // flushedList reference
        size += REFERENCE_SIZE; // dirtyBuffer reference
        size += 2; // dirty and updated booleans (padded)
        size += 8; // estimatedSizeBytes long

        size += estimateListSize(flushedList);
        size += estimateListSize(dirtyBuffer);

        return size;
    }

    private ListStateSizeEstimator() {
        // Utility class, no instantiation
    }
}
