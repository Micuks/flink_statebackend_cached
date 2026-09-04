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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ArmPointMemTableConfig;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Filter;
import org.rocksdb.IndexType;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.ReadOptions;
import org.rocksdb.Statistics;
import org.rocksdb.TableFormatConfig;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The container for RocksDB resources, including predefined options, option factory and shared
 * resource among instances.
 *
 * <p>This should be the only entrance for {@link EmbeddedRocksDBStateBackend} to get RocksDB
 * options, and should be properly (and necessarily) closed to prevent resource leak.
 */
public final class RocksDBResourceContainer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBResourceContainer.class);

    private static final AtomicBoolean ARMLOCAL_ACTIVATION_LOGGED = new AtomicBoolean();
    // the filename length limit is 255 on most operating systems
    private static final int INSTANCE_PATH_LENGTH_LIMIT = 255 - "_LOG".length();

    @Nullable private final File instanceRocksDBPath;

    /** The configurations from file. */
    private final ReadableConfig configuration;

    /** The pre-configured option settings. */
    private final PredefinedOptions predefinedOptions;

    /** The options factory to create the RocksDB options. */
    @Nullable private final RocksDBOptionsFactory optionsFactory;

    /**
     * The shared resource among RocksDB instances. This resource is not part of the
     * 'handlesToClose', because the handles to close are closed quietly, whereas for this one, we
     * want exceptions to be reported.
     */
    @Nullable private final OpaqueMemoryResource<RocksDBSharedResources> sharedResources;

    private final boolean enableStatistics;

    /** The handles to be closed when the container is closed. */
    private final ArrayList<AutoCloseable> handlesToClose;

    @VisibleForTesting
    public RocksDBResourceContainer() {
        this(new Configuration(), PredefinedOptions.DEFAULT, null, null, null, false);
    }

    @VisibleForTesting
    public RocksDBResourceContainer(
            PredefinedOptions predefinedOptions, @Nullable RocksDBOptionsFactory optionsFactory) {
        this(new Configuration(), predefinedOptions, optionsFactory, null, null, false);
    }

    @VisibleForTesting
    public RocksDBResourceContainer(
            PredefinedOptions predefinedOptions,
            @Nullable RocksDBOptionsFactory optionsFactory,
            @Nullable OpaqueMemoryResource<RocksDBSharedResources> sharedResources) {
        this(new Configuration(), predefinedOptions, optionsFactory, sharedResources, null, false);
    }

    public RocksDBResourceContainer(
            ReadableConfig configuration,
            PredefinedOptions predefinedOptions,
            @Nullable RocksDBOptionsFactory optionsFactory,
            @Nullable OpaqueMemoryResource<RocksDBSharedResources> sharedResources,
            boolean enableStatistics) {
        this(
                configuration,
                predefinedOptions,
                optionsFactory,
                sharedResources,
                null,
                enableStatistics);
    }

    RocksDBResourceContainer(
            ReadableConfig configuration,
            PredefinedOptions predefinedOptions,
            @Nullable RocksDBOptionsFactory optionsFactory,
            @Nullable OpaqueMemoryResource<RocksDBSharedResources> sharedResources,
            @Nullable File instanceBasePath,
            boolean enableStatistics) {

        this.configuration = configuration;
        this.predefinedOptions = checkNotNull(predefinedOptions);
        this.optionsFactory = optionsFactory;
        this.sharedResources = sharedResources;

        this.instanceRocksDBPath =
                instanceBasePath != null
                        ? RocksDBKeyedStateBackendBuilder.getInstanceRocksDBPath(instanceBasePath)
                        : null;

        this.enableStatistics = enableStatistics;
        this.handlesToClose = new ArrayList<>();
    }

    /** Gets the RocksDB {@link DBOptions} to be used for RocksDB instances. */
    public DBOptions getDbOptions() {
        // initial options from common profile
        DBOptions opt = createBaseCommonDBOptions();
        handlesToClose.add(opt);

        // load configurable options on top of pre-defined profile
        setDBOptionsFromConfigurableOptions(opt);

        // add user-defined options factory, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createDBOptions(opt, handlesToClose);
        }

        // add necessary default options
        opt = opt.setCreateIfMissing(true);

        // if sharedResources is non-null, use the write buffer manager from it.
        if (sharedResources != null) {
            opt.setWriteBufferManager(sharedResources.getResourceHandle().getWriteBufferManager());
        }

        if (enableStatistics) {
            Statistics statistics = new Statistics();
            opt.setStatistics(statistics);
            handlesToClose.add(statistics);
        }

        return opt;
    }

    /**
     * Gets write buffer manager capacity.
     *
     * @return the capacity of the write buffer manager, or null if write buffer manager is not
     *     enabled.
     */
    public Long getWriteBufferManagerCapacity() {
        if (sharedResources == null) {
            return null;
        }

        return sharedResources.getResourceHandle().getWriteBufferManagerCapacity();
    }

    /** Gets the RocksDB {@link ColumnFamilyOptions} to be used for all RocksDB instances. */
    public ColumnFamilyOptions getColumnOptions() {
        // initial options from common profile
        ColumnFamilyOptions opt = createBaseCommonColumnOptions();
        handlesToClose.add(opt);

        // load configurable options on top of pre-defined profile
        setColumnFamilyOptionsFromConfigurableOptions(opt, handlesToClose);

        // add user-defined options, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createColumnOptions(opt, handlesToClose);
        }

        // if sharedResources is non-null, use the block cache from it and
        // set necessary options for performance consideration with memory control
        if (sharedResources != null) {
            final RocksDBSharedResources rocksResources = sharedResources.getResourceHandle();
            final Cache blockCache = rocksResources.getCache();
            TableFormatConfig tableFormatConfig = opt.tableFormatConfig();
            BlockBasedTableConfig blockBasedTableConfig;
            if (tableFormatConfig == null) {
                blockBasedTableConfig = new BlockBasedTableConfig();
            } else {
                Preconditions.checkArgument(
                        tableFormatConfig instanceof BlockBasedTableConfig,
                        "We currently only support BlockBasedTableConfig When bounding total memory.");
                blockBasedTableConfig = (BlockBasedTableConfig) tableFormatConfig;
            }
            if (rocksResources.isUsingPartitionedIndexFilters()
                    && overwriteFilterIfExist(blockBasedTableConfig)) {
                blockBasedTableConfig.setIndexType(IndexType.kTwoLevelIndexSearch);
                blockBasedTableConfig.setPartitionFilters(true);
                blockBasedTableConfig.setPinTopLevelIndexAndFilter(true);
            }
            blockBasedTableConfig.setBlockCache(blockCache);
            blockBasedTableConfig.setCacheIndexAndFilterBlocks(true);
            blockBasedTableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
            blockBasedTableConfig.setPinL0FilterAndIndexBlocksInCache(true);
            opt.setTableFormatConfig(blockBasedTableConfig);
        }

        return opt;
    }

    /**
     * Gets state-aware column-family options without applying point-only layouts to range state.
     */
    public ColumnFamilyOptions getColumnOptions(
            @Nullable RegisteredStateMetaInfoBase stateMetaInfo) {
        final ColumnFamilyOptions options = getColumnOptions();
        final boolean configured =
                internalGetOption(RocksDBConfigurableOptions.MEMTABLE_ARM_POINT_ENABLED);
        final String configuredProbeMode =
                internalGetOption(RocksDBConfigurableOptions.MEMTABLE_ARM_POINT_PROBE_MODE);
        final ArmPointMemTableRuntime.Selection selection =
                ArmPointMemTableRuntime.currentSelection(configured, configuredProbeMode);
        ArmPointMemTableRuntime.initializeAndRecord(selection);
        final String stateType =
                stateMetaInfo instanceof RegisteredKeyValueStateBackendMetaInfo
                        ? ((RegisteredKeyValueStateBackendMetaInfo<?, ?>) stateMetaInfo)
                                .getStateType()
                                .name()
                        : "NON_KV";
        final boolean valueState =
                stateMetaInfo instanceof RegisteredKeyValueStateBackendMetaInfo
                        && ((RegisteredKeyValueStateBackendMetaInfo<?, ?>) stateMetaInfo)
                                        .getStateType()
                                == StateDescriptor.Type.VALUE;
        final boolean mapState =
                stateMetaInfo instanceof RegisteredKeyValueStateBackendMetaInfo
                        && ((RegisteredKeyValueStateBackendMetaInfo<?, ?>) stateMetaInfo)
                                        .getStateType()
                                == StateDescriptor.Type.MAP;
        final boolean mapFlatAuthority =
                internalGetOption(
                        RocksDBConfigurableOptions.MEMTABLE_ARM_POINT_MAP_FLAT_AUTHORITY);
        final boolean mapKeyHeadPointIndex =
                internalGetOption(
                        RocksDBConfigurableOptions.MEMTABLE_ARM_POINT_MAP_KEYHEAD_POINT_INDEX);
        Preconditions.checkArgument(
                !(mapFlatAuthority && mapKeyHeadPointIndex),
                "ArmPoint MapState flat authority and KeyHead point index are mutually exclusive");
        final boolean flatAuthority =
                armPointFlatAuthority(valueState, mapState, mapFlatAuthority);
        final boolean keyHeadPointIndex = mapState && mapKeyHeadPointIndex;

        if (selection.appliesTo(stateMetaInfo)) {
            final String previousFactory = options.memTableFactoryName();
            Preconditions.checkState(
                    "SkipListFactory".equals(previousFactory),
                    "ArmPoint refuses to overwrite user memtable factory %s for state %s",
                    previousFactory,
                    stateMetaInfo.getName());

            final int bucketCount =
                    internalGetOption(RocksDBConfigurableOptions.MEMTABLE_ARM_POINT_BUCKET_COUNT);
            // Flat append authority removes ordered-write maintenance. ValueState always uses
            // it. MapState may opt in experimentally; every other state retains ordered authority.
            final String factoryProbeMode =
                    armPointFactoryProbeMode(
                            selection.probeMode, flatAuthority, keyHeadPointIndex);
            options.setMemTableConfig(
                    new ArmPointMemTableConfig()
                            .setBucketCount(bucketCount)
                            .setProbeMode(factoryProbeMode));
            LOG.info(
                    "[CACHEKIT_ARM_POINT] state={} state_type={} enabled=true factory={} "
                            + "bucket_count={} tag_bits=16 probe_mode={} authority={} scope={} "
                            + "sve_supported={}",
                    stateMetaInfo.getName(),
                    stateType,
                    options.memTableFactoryName(),
                    bucketCount,
                    selection.probeMode,
                    flatAuthority
                            ? "flat"
                            : (keyHeadPointIndex ? "ordered-keyhead" : "ordered"),
                    selection.allKeyValueStates ? "all-kv" : "value-only",
                    ArmPointMemTableConfig.isSveSupported());
        } else {
            LOG.info(
                    "[CACHEKIT_ARM_POINT] state={} state_type={} enabled=false factory={} "
                            + "bucket_count=0 tag_bits=0 probe_mode=off sve_supported={}",
                    stateMetaInfo == null ? "<default>" : stateMetaInfo.getName(),
                    stateType,
                    options.memTableFactoryName(),
                    ArmPointMemTableConfig.isSveSupported());
        }
        return options;
    }

    @VisibleForTesting
    static String armPointFactoryProbeMode(
            String probeMode, boolean flatAuthority, boolean keyHeadPointIndex) {
        Preconditions.checkArgument(
                !(flatAuthority && keyHeadPointIndex),
                "ArmPoint flat authority and KeyHead point index are mutually exclusive");
        return flatAuthority
                ? probeMode + "-flat"
                : (keyHeadPointIndex ? probeMode + "-keyhead" : probeMode);
    }

    @VisibleForTesting
    static boolean armPointFlatAuthority(
            boolean valueState, boolean mapState, boolean mapFlatAuthority) {
        return valueState || (mapState && mapFlatAuthority);
    }

    /** Gets the RocksDB {@link WriteOptions} to be used for write operations. */
    public WriteOptions getWriteOptions() {
        // Disable WAL by default
        WriteOptions opt = new WriteOptions().setDisableWAL(true);
        handlesToClose.add(opt);

        // add user-defined options factory, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createWriteOptions(opt, handlesToClose);
        }

        return opt;
    }

    /** Gets the RocksDB {@link ReadOptions} to be used for read operations. */
    public ReadOptions getReadOptions() {
        ReadOptions opt = new ReadOptions();
        handlesToClose.add(opt);

        // add user-defined options factory, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createReadOptions(opt, handlesToClose);
        }

        return opt;
    }

    RocksDBNativeMetricOptions getMemoryWatcherOptions(
            RocksDBNativeMetricOptions defaultMetricOptions) {
        return optionsFactory == null
                ? defaultMetricOptions
                : optionsFactory.createNativeMetricsOptions(defaultMetricOptions);
    }

    PredefinedOptions getPredefinedOptions() {
        return predefinedOptions;
    }

    @Nullable
    RocksDBOptionsFactory getOptionsFactory() {
        return optionsFactory;
    }

    @Override
    public void close() throws Exception {
        handlesToClose.forEach(IOUtils::closeQuietly);
        handlesToClose.clear();

        if (sharedResources != null) {
            sharedResources.close();
        }
    }

    /**
     * Overwrite configured {@link Filter} if enable partitioned filter. Partitioned filter only
     * worked in full bloom filter, not blocked based.
     */
    private boolean overwriteFilterIfExist(BlockBasedTableConfig blockBasedTableConfig) {
        if (blockBasedTableConfig.filterPolicy() != null) {
            // TODO Can get filter's config in the future RocksDB version, and build new filter use
            // existing config.
            BloomFilter newFilter = new BloomFilter(10, false);
            LOG.info(
                    "Existing filter has been overwritten to full filters since partitioned index filters is enabled.");
            blockBasedTableConfig.setFilterPolicy(newFilter);
            handlesToClose.add(newFilter);
        }
        return true;
    }

    /** Create a {@link DBOptions} for RocksDB, including some common settings. */
    DBOptions createBaseCommonDBOptions() {
        return new DBOptions()
                .setUseFsync(false)
                .setStatsDumpPeriodSec(0)
                // A Flink task backend is disposable state. Its durable copy is a checkpoint;
                // flushing a canceled task's private memtables while closing the local DB cannot
                // improve recovery, but it can make RocksDB.closeDatabase() wait indefinitely
                // after a write-heavy join. Skip that shutdown-only flush. Normal runtime flushes
                // and checkpoint semantics are unchanged.
                .setAvoidFlushDuringShutdown(true);
    }

    /** Create a {@link ColumnFamilyOptions} for RocksDB, including some common settings. */
    ColumnFamilyOptions createBaseCommonColumnOptions() {
        return new ColumnFamilyOptions();
    }

    /**
     * Get a value for option from pre-defined option and configurable option settings. The priority
     * relationship is as below.
     *
     * <p>Configured value > pre-defined value > default value.
     *
     * @param option the wanted option
     * @param <T> the value type
     * @return the final value for the option according to the priority above.
     */
    @Nullable
    private <T> T internalGetOption(ConfigOption<T> option) {
        return configuration
                .getOptional(option)
                .orElseGet(() -> predefinedOptions.getValue(option));
    }

    @SuppressWarnings("ConstantConditions")
    private DBOptions setDBOptionsFromConfigurableOptions(DBOptions currentOptions) {

        currentOptions.setMaxBackgroundJobs(
                internalGetOption(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS));

        currentOptions.setMaxOpenFiles(
                internalGetOption(RocksDBConfigurableOptions.MAX_OPEN_FILES));

        currentOptions.setInfoLogLevel(internalGetOption(RocksDBConfigurableOptions.LOG_LEVEL));

        String logDir = internalGetOption(RocksDBConfigurableOptions.LOG_DIR);
        if (logDir == null || logDir.isEmpty()) {
            if (instanceRocksDBPath == null
                    || instanceRocksDBPath.getAbsolutePath().length()
                            <= INSTANCE_PATH_LENGTH_LIMIT) {
                relocateDefaultDbLogDir(currentOptions);
            } else {
                // disable log relocate when instance path length exceeds limit to prevent rocksdb
                // log file creation failure, details in FLINK-31743
                LOG.warn(
                        "RocksDB instance path length exceeds limit : {}, disable log relocate.",
                        instanceRocksDBPath);
            }
        } else {
            currentOptions.setDbLogDir(logDir);
        }

        currentOptions.setMaxLogFileSize(
                internalGetOption(RocksDBConfigurableOptions.LOG_MAX_FILE_SIZE).getBytes());

        currentOptions.setKeepLogFileNum(
                internalGetOption(RocksDBConfigurableOptions.LOG_FILE_NUM));

        return currentOptions;
    }

    @SuppressWarnings("ConstantConditions")
    private ColumnFamilyOptions setColumnFamilyOptionsFromConfigurableOptions(
            ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {

        currentOptions.setCompactionStyle(
                internalGetOption(RocksDBConfigurableOptions.COMPACTION_STYLE));

        final CompressionType compressionType =
                internalGetOption(RocksDBConfigurableOptions.COMPRESSION_TYPE);
        final int uncompressedHotLevels =
                internalGetOption(RocksDBConfigurableOptions.UNCOMPRESSED_HOT_LEVELS);
        currentOptions.setCompressionType(compressionType);
        if (uncompressedHotLevels > 0) {
            final List<CompressionType> compressionPerLevel = new ArrayList<>(7);
            for (int level = 0; level < 7; level++) {
                compressionPerLevel.add(
                        level < uncompressedHotLevels
                                ? CompressionType.NO_COMPRESSION
                                : compressionType);
            }
            currentOptions.setCompressionPerLevel(compressionPerLevel);
        }
        LOG.info(
                "Configured RocksDB compression type: {}, uncompressed hot levels: {}, per-level policy: {}",
                currentOptions.compressionType(),
                uncompressedHotLevels,
                currentOptions.compressionPerLevel());

        currentOptions.setLevelCompactionDynamicLevelBytes(
                internalGetOption(RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE));

        currentOptions.setTargetFileSizeBase(
                internalGetOption(RocksDBConfigurableOptions.TARGET_FILE_SIZE_BASE).getBytes());

        currentOptions.setMaxBytesForLevelBase(
                internalGetOption(RocksDBConfigurableOptions.MAX_SIZE_LEVEL_BASE).getBytes());

        currentOptions.setWriteBufferSize(
                internalGetOption(RocksDBConfigurableOptions.WRITE_BUFFER_SIZE).getBytes());

        currentOptions.setMaxWriteBufferNumber(
                internalGetOption(RocksDBConfigurableOptions.MAX_WRITE_BUFFER_NUMBER));

        currentOptions.setMinWriteBufferNumberToMerge(
                internalGetOption(RocksDBConfigurableOptions.MIN_WRITE_BUFFER_NUMBER_TO_MERGE));

        final double memtableBloomRatio =
                internalGetOption(RocksDBConfigurableOptions.MEMTABLE_BLOOM_RATIO);
        final boolean memtableBloomWholeKey =
                internalGetOption(RocksDBConfigurableOptions.MEMTABLE_BLOOM_WHOLE_KEY);
        currentOptions.setMemtablePrefixBloomSizeRatio(memtableBloomRatio);
        currentOptions.setMemtableWholeKeyFiltering(memtableBloomWholeKey);
        LOG.info(
                "Configured RocksDB memtable Bloom filter: ratio={}, whole-key={}",
                memtableBloomRatio,
                memtableBloomWholeKey);

        TableFormatConfig tableFormatConfig = currentOptions.tableFormatConfig();

        BlockBasedTableConfig blockBasedTableConfig;
        if (tableFormatConfig == null) {
            blockBasedTableConfig = new BlockBasedTableConfig();
        } else {
            if (tableFormatConfig instanceof PlainTableConfig) {
                // if the table format config is PlainTableConfig, we just return current
                // column-family options
                return currentOptions;
            } else {
                blockBasedTableConfig = (BlockBasedTableConfig) tableFormatConfig;
            }
        }

        blockBasedTableConfig.setBlockSize(
                internalGetOption(RocksDBConfigurableOptions.BLOCK_SIZE).getBytes());

        blockBasedTableConfig.setMetadataBlockSize(
                internalGetOption(RocksDBConfigurableOptions.METADATA_BLOCK_SIZE).getBytes());

        blockBasedTableConfig.setBlockCacheSize(
                internalGetOption(RocksDBConfigurableOptions.BLOCK_CACHE_SIZE).getBytes());

        if (internalGetOption(RocksDBConfigurableOptions.USE_BLOOM_FILTER)) {
            final double bitsPerKey =
                    internalGetOption(RocksDBConfigurableOptions.BLOOM_FILTER_BITS_PER_KEY);
            final boolean blockBasedMode =
                    internalGetOption(RocksDBConfigurableOptions.BLOOM_FILTER_BLOCK_BASED_MODE);
            final boolean armLocalConfigured =
                    configuration
                                    .getOptional(
                                            RocksDBConfigurableOptions
                                                    .BLOOM_FILTER_FASTLOCAL_BLOCK_BYTES)
                                    .isPresent()
                            || configuration
                                    .getOptional(
                                            RocksDBConfigurableOptions
                                                    .BLOOM_FILTER_FASTLOCAL_PROBE_MODE)
                                    .isPresent()
                            || configuration
                                    .getOptional(
                                            RocksDBConfigurableOptions
                                                    .BLOOM_FILTER_FASTLOCAL_RUNTIME_DISPATCH)
                                    .isPresent();
            final BloomFilter bloomFilter;
            if (armLocalConfigured) {
                if (blockBasedMode) {
                    throw new IllegalArgumentException(
                            "CacheKit ArmLocal Bloom requires full-filter mode");
                }
                final boolean runtimeDispatch =
                        internalGetOption(
                                RocksDBConfigurableOptions.BLOOM_FILTER_FASTLOCAL_RUNTIME_DISPATCH);
                final int blockBytes =
                        runtimeDispatch
                                ? 0
                                : internalGetOption(
                                        RocksDBConfigurableOptions
                                                .BLOOM_FILTER_FASTLOCAL_BLOCK_BYTES);
                final BloomFilter.CacheKitFastLocalProbeMode probeMode =
                        runtimeDispatch
                                ? BloomFilter.CacheKitFastLocalProbeMode.AUTO
                                : parseArmLocalProbeMode(
                                        internalGetOption(
                                                RocksDBConfigurableOptions
                                                        .BLOOM_FILTER_FASTLOCAL_PROBE_MODE));
                bloomFilter = new BloomFilter(bitsPerKey, blockBytes, probeMode);
                if (ARMLOCAL_ACTIVATION_LOGGED.compareAndSet(false, true)) {
                    LOG.info(
                            "CACHEKIT_ARMLOCAL_ACTIVATION block_bytes={} probe_mode={} runtime_dispatch={}",
                            blockBytes,
                            probeMode.name().toLowerCase(Locale.ROOT),
                            runtimeDispatch);
                }
            } else {
                bloomFilter = new BloomFilter(bitsPerKey, blockBasedMode);
            }
            handlesToClose.add(bloomFilter);
            blockBasedTableConfig.setFilterPolicy(bloomFilter);
        }

        return currentOptions.setTableFormatConfig(blockBasedTableConfig);
    }

    private static BloomFilter.CacheKitFastLocalProbeMode parseArmLocalProbeMode(String value) {
        switch (value.toLowerCase(Locale.ROOT)) {
            case "scalar":
                return BloomFilter.CacheKitFastLocalProbeMode.SCALAR;
            case "sve":
                return BloomFilter.CacheKitFastLocalProbeMode.SVE;
            case "auto":
                return BloomFilter.CacheKitFastLocalProbeMode.AUTO;
            case "platform-default":
                return BloomFilter.CacheKitFastLocalProbeMode.PLATFORM_DEFAULT;
            default:
                throw new IllegalArgumentException(
                        "Unknown CacheKit ArmLocal probe mode: " + value);
        }
    }

    /**
     * Relocates the default log directory of RocksDB with the Flink log directory. Finds the Flink
     * log directory using log.file Java property that is set during startup.
     *
     * @param dbOptions The RocksDB {@link DBOptions}.
     */
    private void relocateDefaultDbLogDir(DBOptions dbOptions) {
        String logFilePath = System.getProperty("log.file");
        if (logFilePath != null) {
            File logFile = resolveFileLocation(logFilePath);
            if (logFile != null && resolveFileLocation(logFile.getParent()) != null) {
                dbOptions.setDbLogDir(logFile.getParent());
            }
        }
    }

    /**
     * Verify log file location.
     *
     * @param logFilePath Path to log file
     * @return File or null if not a valid log file
     */
    private File resolveFileLocation(String logFilePath) {
        File logFile = new File(logFilePath);
        return (logFile.exists() && logFile.canRead()) ? logFile : null;
    }
}
