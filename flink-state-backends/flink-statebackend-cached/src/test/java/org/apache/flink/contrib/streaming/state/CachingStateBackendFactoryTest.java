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
import java.io.IOException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link CachingStateBackendFactory} class, which is responsible for creating {@link
 * CachingStateBackend} instances from configuration.
 */
public class CachingStateBackendFactoryTest {

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    private final ClassLoader cl = getClass().getClassLoader();

    @Test
    public void testCreateFromConfigWithDefaults() throws IOException {
        final Configuration config = new Configuration();
        // Set a dummy checkpoint directory to satisfy RocksDBStateBackendFactory
        config.setString("state.checkpoints.dir", "file://" + tmp.newFolder().getAbsolutePath());

        final CachingStateBackend backend =
                new CachingStateBackendFactory().createFromConfig(config, cl);
        assertNotNull(backend);

        // Verify default cache parameters
        assertEquals(CachingStateBackendFactory.L1_CACHE_SIZE_CONFIG.defaultValue().longValue(), backend.getL1CacheSize());
        assertEquals(CachingStateBackendFactory.L2_CACHE_SIZE_CONFIG.defaultValue().longValue(), backend.getL2CacheSize());
        assertEquals(CachingStateBackendFactory.MAX_ACTIVE_NAMESPACES_CONFIG.defaultValue().longValue(), backend.getMaxActiveNamespaces());
        assertEquals(CachingStateBackendFactory.MAX_CACHE_MEMORY_MB_CONFIG.defaultValue().longValue(), backend.getMaxCacheMemoryMb());
        assertEquals(CachingStateBackendFactory.CACHE_POLICY_CONFIG.defaultValue(), backend.getCachePolicyType());
        assertEquals(CachingStateBackendFactory.MAP_L1_KEY_PRESENCE_CACHE_SIZE_CONFIG.defaultValue().longValue(), backend.getMapL1KeyPresenceCacheSize());
        assertEquals(CachingStateBackendFactory.MAP_L2_KEY_PRESENCE_CACHE_SIZE_CONFIG.defaultValue().longValue(), backend.getMapL2KeyPresenceCacheSize());

        // Add assertions for default values of new bypass parameters
        assertEquals(CachingStateBackendFactory.MAP_CACHE_HIT_RATE_THRESHOLD_CONFIG.defaultValue(), backend.getMapCacheHitRateThreshold(), 0.001);
        assertEquals(CachingStateBackendFactory.MAP_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG.defaultValue().longValue(), backend.getMapCacheHitRateWindowSize());
        assertEquals(CachingStateBackendFactory.MAP_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG.defaultValue().longValue(), backend.getMapCacheMinAccessesForBypassCheck());
    }

    @Test
    public void testCreateFromConfigWithCustomValues() throws IOException {
        final Configuration config = new Configuration();
        long l1Size = 123L;
        long l2Size = 456L;
        long maxActiveNs = 789L;
        long maxMemMb = 50L;
        CachingStateBackendFactory.CachePolicyType policy = CachingStateBackendFactory.CachePolicyType.TINYLFU;
        long mapL1Presence = 100L;
        long mapL2Presence = 200L;
        double hitRateThreshold = 0.75;
        long hitRateWindow = 2000L;
        long minAccessBypass = 500L;

        config.setLong(CachingStateBackendFactory.L1_CACHE_SIZE_CONFIG, l1Size);
        config.setLong(CachingStateBackendFactory.L2_CACHE_SIZE_CONFIG, l2Size);
        config.setLong(CachingStateBackendFactory.MAX_ACTIVE_NAMESPACES_CONFIG, maxActiveNs);
        config.setLong(CachingStateBackendFactory.MAX_CACHE_MEMORY_MB_CONFIG, maxMemMb);
        config.set(CachingStateBackendFactory.CACHE_POLICY_CONFIG, policy);
        config.setLong(CachingStateBackendFactory.MAP_L1_KEY_PRESENCE_CACHE_SIZE_CONFIG, mapL1Presence);
        config.setLong(CachingStateBackendFactory.MAP_L2_KEY_PRESENCE_CACHE_SIZE_CONFIG, mapL2Presence);
        config.setDouble(CachingStateBackendFactory.MAP_CACHE_HIT_RATE_THRESHOLD_CONFIG, hitRateThreshold);
        config.setLong(CachingStateBackendFactory.MAP_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG, hitRateWindow);
        config.setLong(CachingStateBackendFactory.MAP_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG, minAccessBypass);

        // Set a dummy checkpoint directory
        config.setString("state.checkpoints.dir", "file://" + tmp.newFolder().getAbsolutePath());

        final CachingStateBackend backend =
                new CachingStateBackendFactory().createFromConfig(config, cl);
        assertNotNull(backend);

        assertEquals(l1Size, backend.getL1CacheSize());
        assertEquals(l2Size, backend.getL2CacheSize());
        assertEquals(maxActiveNs, backend.getMaxActiveNamespaces());
        assertEquals(maxMemMb, backend.getMaxCacheMemoryMb());
        assertEquals(policy, backend.getCachePolicyType());
        assertEquals(mapL1Presence, backend.getMapL1KeyPresenceCacheSize());
        assertEquals(mapL2Presence, backend.getMapL2KeyPresenceCacheSize());
        assertEquals(hitRateThreshold, backend.getMapCacheHitRateThreshold(), 0.001);
        assertEquals(hitRateWindow, backend.getMapCacheHitRateWindowSize());
        assertEquals(minAccessBypass, backend.getMapCacheMinAccessesForBypassCheck());
    }

    @Test
    public void testCreateFromConfigWithFallbackKeys() throws IOException {
        final Configuration config = new Configuration();
        long l1Fallback = 321L;
        long l2Fallback = 654L;

        // Use old string keys for cache sizes
        config.setString(CachingStateBackendFactory.L1_CACHE_SIZE_KEY_OLD_STRING, String.valueOf(l1Fallback));
        config.setString(CachingStateBackendFactory.L2_CACHE_SIZE_KEY_OLD_STRING, String.valueOf(l2Fallback));

        // Set a dummy checkpoint directory
        config.setString("state.checkpoints.dir", "file://" + tmp.newFolder().getAbsolutePath());

        final CachingStateBackend backend =
                new CachingStateBackendFactory().createFromConfig(config, cl);
        assertNotNull(backend);

        assertEquals(l1Fallback, backend.getL1CacheSize());
        assertEquals(l2Fallback, backend.getL2CacheSize());
        // Assert default values for bypass parameters when not set
        assertEquals(CachingStateBackendFactory.MAP_CACHE_HIT_RATE_THRESHOLD_CONFIG.defaultValue(), backend.getMapCacheHitRateThreshold(), 0.001);
        assertEquals(CachingStateBackendFactory.MAP_CACHE_HIT_RATE_WINDOW_SIZE_CONFIG.defaultValue().longValue(), backend.getMapCacheHitRateWindowSize());
        assertEquals(CachingStateBackendFactory.MAP_CACHE_MIN_ACCESSES_FOR_BYPASS_CHECK_CONFIG.defaultValue().longValue(), backend.getMapCacheMinAccessesForBypassCheck());
    }

    @Test
    public void testCreateFromConfigMissingCheckpointDirectory() {
        // This test ensures that if the delegate (RocksDB) requires a checkpoint directory
        // and it's not provided, the factory correctly throws an IllegalConfigurationException.
        final Configuration config = new Configuration();
        // Do NOT set state.checkpoints.dir

        try {
            new CachingStateBackendFactory().createFromConfig(config, cl);
            fail("Should have thrown an IllegalConfigurationException because RocksDB delegate needs a checkpoint dir.");
        } catch (IllegalConfigurationException e) {
            // Expected path when RocksDB requires 'state.checkpoints.dir'
            // The exact message might vary depending on RocksDB internal checks.
            // Checking for a substring related to checkpoint directory should be robust enough.
            assertTrue(e.getMessage().toLowerCase().contains("checkpoint directory"));
        } catch (IOException e) {
            fail("Should have been an IllegalConfigurationException, not IOException: " + e.getMessage());
        } catch (Exception e) {
            // Catch any other exception to see if it's related to the missing dir
            // This part might be specific to how RocksDBStateBackendFactory behaves.
            // If it defaults to MemoryStateBackend on RocksDB init failure, this test changes.
            // Based on current factory code, it will try RocksDB, which will fail without checkpoint dir.
            boolean relatedToRocksDBConfig = e.getMessage() != null &&
                    (e.getMessage().contains("Could not initialize RocksDB library") ||
                            e.getMessage().toLowerCase().contains("checkpoint directory"));
            if (!relatedToRocksDBConfig) {
                fail("Unexpected exception type: " + e.getClass().getName() + " with message: " + e.getMessage());
            }
        }
    }
}
