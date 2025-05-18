package com.micuks.flink.cachingstate;

// import com.micuks.flink.cachingstate.CachingStateBackend; // Assuming this will be in the same package
// import org.apache.flink.configuration.Configuration; // Available in Flink
// import org.apache.flink.runtime.state.StateBackend; // Available in Flink
// import org.apache.flink.runtime.state.StateBackendFactory; // Available in Flink
// import org.apache.flink.contrib.streaming.state.RocksDBStateBackend; // To be used as delegate

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend; // Assuming this is the one to use

/**
 * A factory that creates a {@link CachingStateBackend}.
 * The Flink job needs to be configured to use this factory,
 * e.g., by setting 'state.backend: com.micuks.flink.cachingstate.CachingStateBackendFactory'
 * in flink-conf.yaml, or if this class is in default package, the fully qualified name might be simpler.
 * For now, assuming default package due to linter.
 */
public class CachingStateBackendFactory implements StateBackendFactory {

    public static final String L1_CACHE_SIZE_KEY_STRING = "state.backend.cache.l1.size";
    public static final String L1_CACHE_SIZE_KEY_OLD_STRING = "state.backend.cache.l1-size"; // for backward compatibility
    public static final long DEFAULT_L1_CACHE_SIZE = 1000L;

    public static final ConfigOption<Long> L1_CACHE_SIZE_CONFIG =
            ConfigOptions.key(L1_CACHE_SIZE_KEY_STRING)
                    .longType()
                    .defaultValue(DEFAULT_L1_CACHE_SIZE)
                    .withFallbackKeys(L1_CACHE_SIZE_KEY_OLD_STRING);

    public static final String L2_CACHE_SIZE_KEY_STRING = "state.backend.cache.l2.size";
    public static final String L2_CACHE_SIZE_KEY_OLD_STRING = "state.backend.cache.l2-size"; // for backward compatibility
    public static final long DEFAULT_L2_CACHE_SIZE = 10000L;

    public static final ConfigOption<Long> L2_CACHE_SIZE_CONFIG =
            ConfigOptions.key(L2_CACHE_SIZE_KEY_STRING)
                    .longType()
                    .defaultValue(DEFAULT_L2_CACHE_SIZE)
                    .withFallbackKeys(L2_CACHE_SIZE_KEY_OLD_STRING);

    public static final String MAX_CACHE_MEMORY_KEY_STRING = "state.backend.cache.max-memory";
    public static final long DEFAULT_MAX_CACHE_MEMORY_MB = 20L; // Default 20MB

    public static final ConfigOption<Long> MAX_CACHE_MEMORY_CONFIG = // Renamed for clarity
            ConfigOptions.key(MAX_CACHE_MEMORY_KEY_STRING)
                    .longType()
                    .defaultValue(DEFAULT_MAX_CACHE_MEMORY_MB);


    @Override
    public StateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader) {
        // Use RocksDBStateBackendFactory to create the delegate
        org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory rocksFactory =
                new org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory();
        StateBackend underlyingDelegateBackend;
        try {
            underlyingDelegateBackend = rocksFactory.createFromConfig(config, classLoader);
        } catch (IllegalConfigurationException e) {
            throw new RuntimeException("Failed to configure underlying RocksDBStateBackend from factory", e);
        }
        // It's possible createFromConfig throws other runtime exceptions if Flink's internal config parsing fails.

        if (!(underlyingDelegateBackend instanceof RocksDBStateBackend)) {
            throw new IllegalStateException(
                    "Underlying state backend created by RocksDBStateBackendFactory " +
                    "is not a RocksDBStateBackend instance: " +
                    underlyingDelegateBackend.getClass().getName() +
                    ". CachingStateBackend requires a RocksDBStateBackend as delegate.");
        }
        RocksDBStateBackend underlyingRocksDBStateBackend = (RocksDBStateBackend) underlyingDelegateBackend;

        long l1CacheSize = config.get(L1_CACHE_SIZE_CONFIG);
        long l2CacheSize = config.get(L2_CACHE_SIZE_CONFIG);
        long maxCacheMemoryMb = config.get(MAX_CACHE_MEMORY_CONFIG);

        return new CachingStateBackend(underlyingRocksDBStateBackend, l1CacheSize, l2CacheSize, maxCacheMemoryMb);
    }
} 