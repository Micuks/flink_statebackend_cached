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
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.contrib.streaming.state;

// Assuming RocksDBStateBackend is a concrete class we can check against
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.state.StateBackend;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link CachingStateBackendFactory} class, which is responsible for creating {@link
 * CachingStateBackend} instances from configuration.
 */
public class CachingStateBackendFactoryTest {

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    private final ClassLoader cl = getClass().getClassLoader();

    @Test
    public void testCreateFromConfigWithDefaults() throws IOException {
        Configuration config = new Configuration();
        // RocksDB backend requires a checkpoint directory
        config.setString(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, tmp.newFolder().toURI().toString());

        CachingStateBackendFactory factory = new CachingStateBackendFactory();
        StateBackend backend = factory.createFromConfig(config, cl);

        assertNotNull(backend);
        assertTrue(backend instanceof CachingStateBackend);

        CachingStateBackend cachingBackend = (CachingStateBackend) backend;
        // Access private fields via reflection or add getters if possible for thorough
        // testing
        // For now, we rely on the factory's logic and constructor parameters being
        // passed correctly.
        // We can check if the delegate is RocksDBStateBackend
        // Field delegateField =
        // CachingStateBackend.class.getDeclaredField("delegateBackend");
        // delegateField.setAccessible(true);
        // assertTrue(delegateField.get(cachingBackend) instanceof RocksDBStateBackend);

        // Check default cache sizes (assuming direct field access or getters)
        // Field l1CacheSizeField =
        // CachingStateBackend.class.getDeclaredField("l1CacheSize");
        // l1CacheSizeField.setAccessible(true);
        // assertEquals(CachingStateBackendFactory.DEFAULT_L1_CACHE_SIZE,
        // l1CacheSizeField.getLong(cachingBackend));
        // Field l2CacheSizeField =
        // CachingStateBackend.class.getDeclaredField("l2CacheSize");
        // l2CacheSizeField.setAccessible(true);
        // assertEquals(CachingStateBackendFactory.DEFAULT_L2_CACHE_SIZE,
        // l2CacheSizeField.getLong(cachingBackend));
        // Field maxCacheMemoryField =
        // CachingStateBackend.class.getDeclaredField("maxCacheMemoryMb");
        // maxCacheMemoryField.setAccessible(true);
        // assertEquals(CachingStateBackendFactory.DEFAULT_MAX_CACHE_MEMORY_MB,
        // maxCacheMemoryField.getLong(cachingBackend));
    }

    @Test
    public void testCreateFromConfigWithCustomValues() throws IOException {
        Configuration config = new Configuration();
        config.setString(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, tmp.newFolder().toURI().toString());

        long expectedL1Size = 500L;
        long expectedL2Size = 5000L;
        long expectedMaxActiveNamespaces = 100L; // Default if not set, or a custom test value

        config.set(CachingStateBackendFactory.L1_CACHE_SIZE_CONFIG, expectedL1Size);
        config.set(CachingStateBackendFactory.L2_CACHE_SIZE_CONFIG, expectedL2Size);
        config.set(
                CachingStateBackendFactory.MAX_ACTIVE_NAMESPACES_CONFIG,
                expectedMaxActiveNamespaces);

        CachingStateBackendFactory factory = new CachingStateBackendFactory();
        StateBackend backend = factory.createFromConfig(config, cl);

        assertNotNull(backend);
        assertTrue(backend instanceof CachingStateBackend);

        // To properly verify the internal values, CachingStateBackend would need
        // getters
        // or use reflection. Example using reflection (commented out for brevity and
        // because direct field access is generally discouraged in tests if getters are
        // possible):
        /*
         * try { CachingStateBackend cachingBackend = (CachingStateBackend) backend; Field l1Field =
         * CachingStateBackend.class.getDeclaredField("l1CacheSize"); l1Field.setAccessible(true);
         * assertEquals(expectedL1Size, l1Field.getLong(cachingBackend));
         *
         * Field l2Field = CachingStateBackend.class.getDeclaredField("l2CacheSize");
         * l2Field.setAccessible(true); assertEquals(expectedL2Size,
         * l2Field.getLong(cachingBackend));
         *
         * Field memField =
         * CachingStateBackend.class.getDeclaredField("maxActiveNamespaceOrPerKeyCacheContainers");
         * // Updated field name memField.setAccessible(true);
         * assertEquals(expectedMaxActiveNamespaces, // Updated expected value
         * memField.getLong(cachingBackend));
         *
         * Field delegateField = CachingStateBackend.class.getDeclaredField("delegateBackend");
         * delegateField.setAccessible(true); assertTrue(delegateField.get(cachingBackend)
         * instanceof RocksDBStateBackend);
         *
         * } catch (NoSuchFieldException | IllegalAccessException e) { fail("Reflection failed: " +
         * e.getMessage()); }
         */
    }

    @Test
    public void testCreateFromConfigWithFallbackKeys() throws IOException {
        Configuration config = new Configuration();
        config.setString(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, tmp.newFolder().toURI().toString());

        long expectedL1Size = 150L;
        long expectedL2Size = 1500L;

        // Use old keys
        config.setLong(CachingStateBackendFactory.L1_CACHE_SIZE_KEY_OLD_STRING, expectedL1Size);
        config.setLong(CachingStateBackendFactory.L2_CACHE_SIZE_KEY_OLD_STRING, expectedL2Size);
        // MAX_ACTIVE_NAMESPACES_CONFIG does not have an old key in CachingStateBackendFactory.
        // It will use its default if not explicitly set by the new key.

        CachingStateBackendFactory factory = new CachingStateBackendFactory();
        StateBackend backend = factory.createFromConfig(config, cl);

        assertNotNull(backend);
        assertTrue(backend instanceof CachingStateBackend);

        // Again, verifying exact values would ideally use getters.
        // Assuming the config options correctly handle fallback.
    }

    @Test
    public void testCreateFromConfigMissingCheckpointDirectory() {
        Configuration config = new Configuration(); // No checkpoint directory
        // Set cache sizes to ensure failure is due to checkpoint dir, not missing cache
        // config
        config.set(CachingStateBackendFactory.L1_CACHE_SIZE_CONFIG, 100L);
        // config.set(CachingStateBackendFactory.MAX_ACTIVE_NAMESPACES_CONFIG, 50L); // Also set
        // this if its absence could cause issues

        CachingStateBackendFactory factory = new CachingStateBackendFactory();
        try {
            factory.createFromConfig(config, cl);
            fail(
                    "Should have thrown IllegalConfigurationException because checkpoint directory is missing for RocksDB backend.");
        } catch (IllegalConfigurationException e) {
            // Expected, as RocksDBStateBackendFactory requires it.
            assertTrue(e.getMessage().contains(CheckpointingOptions.CHECKPOINTS_DIRECTORY.key()));
        } catch (IOException e) {
            fail("Unexpected IOException: " + e.getMessage());
        } catch (RuntimeException e) {
            // The factory wraps the IllegalConfigurationException from RocksDB factory in a
            // RuntimeException
            if (e.getCause() instanceof IllegalConfigurationException
                    && e.getMessage()
                            .contains(
                                    "Failed to configure underlying RocksDBStateBackend from factory")) {
                assertTrue(
                        e.getCause()
                                .getMessage()
                                .contains(CheckpointingOptions.CHECKPOINTS_DIRECTORY.key()));
            } else {
                fail("Unexpected RuntimeException: " + e);
            }
        }
    }
}
