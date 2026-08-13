/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneBridge;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneOptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NativeRequestPlaneConfigurationTest {

    @Test
    void testNativeRequestPlaneIsOffByDefault() {
        NativeRequestPlaneOptions options =
                CacheKitStateBackendFactory.nativeRequestPlaneOptions(new Configuration());

        assertFalse(options.enabled());
        assertEquals("auto", options.kernel());
        assertEquals(NativeRequestPlaneBridge.KERNEL_AUTO, options.kernelPreference());
        assertEquals(64, options.minBatchSize());
        assertTrue(options.aarch64Only());
        assertFalse(options.writeThroughMutations());
    }

    @Test
    void testFlinkConfigurationCarriesExplicitSve256AndCapacities() {
        Configuration config = new Configuration();
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_ENABLED, true);
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_LIBRARY, "/tmp/native.so");
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_KERNEL, "sve256");
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_CAPACITY_ENTRIES, 333);
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_BATCH_ENTRIES, 17);
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_MIN_BATCH_SIZE, 5);
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_BATCH_SLOTS, 3);
        config.set(CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_AARCH64_ONLY, false);
        config.set(
                CacheKitStateBackendFactory.NATIVE_REQUEST_PLANE_WRITE_THROUGH_MUTATIONS, true);

        NativeRequestPlaneOptions options =
                CacheKitStateBackendFactory.nativeRequestPlaneOptions(config);

        assertTrue(options.enabled());
        assertEquals("/tmp/native.so", options.libraryPath());
        assertEquals("sve256", options.kernel());
        assertEquals(NativeRequestPlaneBridge.KERNEL_SVE256, options.kernelPreference());
        assertEquals(333, options.capacityEntries());
        assertEquals(17, options.batchEntries());
        assertEquals(5, options.minBatchSize());
        assertEquals(3, options.batchSlots());
        assertFalse(options.aarch64Only());
        assertTrue(options.writeThroughMutations());
    }

    @Test
    void testSVE2AndRelativeLibraryAreRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new NativeRequestPlaneOptions(
                                true,
                                "",
                                "sve2",
                                1,
                                1,
                                1,
                                1,
                                1,
                                1,
                                1,
                                1));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new NativeRequestPlaneOptions(
                                true,
                                "relative.so",
                                "auto",
                                1,
                                1,
                                1,
                                1,
                                1,
                                1,
                                1,
                                1));
    }

    @Test
    void testMissingFieldFromOlderSerializedBackendNormalizesToDisabled() {
        NativeRequestPlaneOptions options =
                CacheKitStateBackend.normalizeNativeRequestPlaneOptions(null);

        assertFalse(options.enabled());
        assertEquals("auto", options.kernel());
    }
}
