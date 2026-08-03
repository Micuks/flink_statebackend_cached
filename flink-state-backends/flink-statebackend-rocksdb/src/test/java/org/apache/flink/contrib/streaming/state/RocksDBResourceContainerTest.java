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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.IndexType;
import org.rocksdb.LRUCache;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.TableFormatConfig;
import org.rocksdb.WriteBufferManager;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests to guard {@link RocksDBResourceContainer}. */
public class RocksDBResourceContainerTest {

    private static final String LEGACY_MEMTABLE_BLOOM_RATIO_KEY =
            "state.backend.rocksdb.memtable-bloom.ratio";
    private static final String LEGACY_MEMTABLE_BLOOM_WHOLE_KEY_KEY =
            "state.backend.rocksdb.memtable-bloom.whole-key";

    @ClassRule public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    @BeforeClass
    public static void ensureRocksDbNativeLibraryLoaded() throws IOException {
        NativeLibraryLoader.getInstance().loadLibrary(TMP_FOLDER.newFolder().getAbsolutePath());
    }

    // ------------------------------------------------------------------------

    @Test
    public void testFreeDBOptionsAfterClose() throws Exception {
        RocksDBResourceContainer container = new RocksDBResourceContainer();
        DBOptions dbOptions = container.getDbOptions();
        assertThat(dbOptions.isOwningHandle(), is(true));
        container.close();
        assertThat(dbOptions.isOwningHandle(), is(false));
    }

    @Test
    public void testFreeMultipleDBOptionsAfterClose() throws Exception {
        RocksDBResourceContainer container = new RocksDBResourceContainer();
        final int optionNumber = 20;
        ArrayList<DBOptions> dbOptions = new ArrayList<>(optionNumber);
        for (int i = 0; i < optionNumber; i++) {
            dbOptions.add(container.getDbOptions());
        }
        container.close();
        for (DBOptions dbOption : dbOptions) {
            assertThat(dbOption.isOwningHandle(), is(false));
        }
    }

    /**
     * Guard the shared resources will be released after {@link RocksDBResourceContainer#close()}
     * when the {@link RocksDBResourceContainer} instance is initiated with {@link
     * OpaqueMemoryResource}.
     *
     * @throws Exception if unexpected error happened.
     */
    @Test
    public void testSharedResourcesAfterClose() throws Exception {
        OpaqueMemoryResource<RocksDBSharedResources> sharedResources = getSharedResources();
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(PredefinedOptions.DEFAULT, null, sharedResources);
        container.close();
        RocksDBSharedResources rocksDBSharedResources = sharedResources.getResourceHandle();
        assertThat(rocksDBSharedResources.getCache().isOwningHandle(), is(false));
        assertThat(rocksDBSharedResources.getWriteBufferManager().isOwningHandle(), is(false));
    }

    /**
     * Guard that {@link RocksDBResourceContainer#getDbOptions()} shares the same {@link
     * WriteBufferManager} instance if the {@link RocksDBResourceContainer} instance is initiated
     * with {@link OpaqueMemoryResource}.
     *
     * @throws Exception if unexpected error happened.
     */
    @Test
    public void testGetDbOptionsWithSharedResources() throws Exception {
        final int optionNumber = 20;
        OpaqueMemoryResource<RocksDBSharedResources> sharedResources = getSharedResources();
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(PredefinedOptions.DEFAULT, null, sharedResources);
        HashSet<WriteBufferManager> writeBufferManagers = new HashSet<>();
        for (int i = 0; i < optionNumber; i++) {
            DBOptions dbOptions = container.getDbOptions();
            WriteBufferManager writeBufferManager = getWriteBufferManager(dbOptions);
            writeBufferManagers.add(writeBufferManager);
        }
        assertThat(writeBufferManagers.size(), is(1));
        assertThat(
                writeBufferManagers.iterator().next(),
                is(sharedResources.getResourceHandle().getWriteBufferManager()));
        container.close();
    }

    /**
     * Guard that {@link RocksDBResourceContainer#getColumnOptions()} shares the same {@link Cache}
     * instance if the {@link RocksDBResourceContainer} instance is initiated with {@link
     * OpaqueMemoryResource}.
     *
     * @throws Exception if unexpected error happened.
     */
    @Test
    public void testGetColumnFamilyOptionsWithSharedResources() throws Exception {
        final int optionNumber = 20;
        OpaqueMemoryResource<RocksDBSharedResources> sharedResources = getSharedResources();
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(PredefinedOptions.DEFAULT, null, sharedResources);
        HashSet<Cache> caches = new HashSet<>();
        for (int i = 0; i < optionNumber; i++) {
            ColumnFamilyOptions columnOptions = container.getColumnOptions();
            Cache cache = getBlockCache(columnOptions);
            caches.add(cache);
        }
        assertThat(caches.size(), is(1));
        assertThat(caches.iterator().next(), is(sharedResources.getResourceHandle().getCache()));
        container.close();
    }

    private OpaqueMemoryResource<RocksDBSharedResources> getSharedResources() {
        final long cacheSize = 1024L, writeBufferSize = 512L;
        final LRUCache cache = new LRUCache(cacheSize, -1, false, 0.1);
        final WriteBufferManager wbm = new WriteBufferManager(writeBufferSize, cache);
        RocksDBSharedResources rocksDBSharedResources =
                new RocksDBSharedResources(cache, wbm, writeBufferSize, false);
        return new OpaqueMemoryResource<>(
                rocksDBSharedResources, cacheSize, rocksDBSharedResources::close);
    }

    private Cache getBlockCache(ColumnFamilyOptions columnOptions) {
        BlockBasedTableConfig blockBasedTableConfig = null;
        try {
            blockBasedTableConfig = (BlockBasedTableConfig) columnOptions.tableFormatConfig();
        } catch (ClassCastException e) {
            fail("Table config got from ColumnFamilyOptions is not BlockBasedTableConfig");
        }
        Field cacheField = null;
        try {
            cacheField = BlockBasedTableConfig.class.getDeclaredField("blockCache");
        } catch (NoSuchFieldException e) {
            fail("blockCache is not defined");
        }
        cacheField.setAccessible(true);
        try {
            return (Cache) cacheField.get(blockBasedTableConfig);
        } catch (IllegalAccessException e) {
            fail("Cannot access blockCache field.");
            return null;
        }
    }

    private WriteBufferManager getWriteBufferManager(DBOptions dbOptions) {

        Field writeBufferManagerField = null;
        try {
            writeBufferManagerField = DBOptions.class.getDeclaredField("writeBufferManager_");
        } catch (NoSuchFieldException e) {
            fail("writeBufferManager_ is not defined.");
        }
        writeBufferManagerField.setAccessible(true);
        try {
            return (WriteBufferManager) writeBufferManagerField.get(dbOptions);
        } catch (IllegalAccessException e) {
            fail("Cannot access writeBufferManager_ field.");
            return null;
        }
    }

    @Test
    public void testFreeColumnOptionsAfterClose() throws Exception {
        RocksDBResourceContainer container = new RocksDBResourceContainer();
        ColumnFamilyOptions columnFamilyOptions = container.getColumnOptions();
        assertThat(columnFamilyOptions.isOwningHandle(), is(true));
        container.close();
        assertThat(columnFamilyOptions.isOwningHandle(), is(false));
    }

    @Test
    public void testFreeMultipleColumnOptionsAfterClose() throws Exception {
        RocksDBResourceContainer container = new RocksDBResourceContainer();
        final int optionNumber = 20;
        ArrayList<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(optionNumber);
        for (int i = 0; i < optionNumber; i++) {
            columnFamilyOptions.add(container.getColumnOptions());
        }
        container.close();
        for (ColumnFamilyOptions columnFamilyOption : columnFamilyOptions) {
            assertThat(columnFamilyOption.isOwningHandle(), is(false));
        }
    }

    @Test
    public void testFreeMultipleColumnOptionsWithPredefinedOptions() throws Exception {
        for (PredefinedOptions predefinedOptions : PredefinedOptions.values()) {
            RocksDBResourceContainer container =
                    new RocksDBResourceContainer(predefinedOptions, null);
            final int optionNumber = 20;
            ArrayList<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(optionNumber);
            for (int i = 0; i < optionNumber; i++) {
                columnFamilyOptions.add(container.getColumnOptions());
            }
            container.close();
            for (ColumnFamilyOptions columnFamilyOption : columnFamilyOptions) {
                assertThat(columnFamilyOption.isOwningHandle(), is(false));
            }
        }
    }

    @Test
    public void testMemtableBloomOptionsUseStockNativeParser() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(RocksDBConfigurableOptions.USE_BLOOM_FILTER, false);
        configuration.set(RocksDBConfigurableOptions.CACHEKIT_MEMTABLE_BLOOM_RATIO, 0.1);
        configuration.set(RocksDBConfigurableOptions.CACHEKIT_MEMTABLE_BLOOM_WHOLE_KEY, true);

        assertMemtableBloomOptionsApplied(configuration);
    }

    @Test
    public void testLegacyMemtableBloomKeysRemainSupported() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(RocksDBConfigurableOptions.USE_BLOOM_FILTER, false);
        configuration.setDouble(LEGACY_MEMTABLE_BLOOM_RATIO_KEY, 0.1);
        configuration.setBoolean(LEGACY_MEMTABLE_BLOOM_WHOLE_KEY_KEY, true);

        assertMemtableBloomOptionsApplied(configuration);
    }

    @Test
    public void testConflictingMemtableBloomRatioKeysAreRejected() {
        final Configuration configuration = new Configuration();
        configuration.set(RocksDBConfigurableOptions.CACHEKIT_MEMTABLE_BLOOM_RATIO, 0.1);
        configuration.setDouble(LEGACY_MEMTABLE_BLOOM_RATIO_KEY, 0.2);

        assertConflictingMemtableBloomKeysRejected(configuration);
    }

    @Test
    public void testConflictingMemtableBloomWholeKeyKeysAreRejected() {
        final Configuration configuration = new Configuration();
        configuration.set(RocksDBConfigurableOptions.CACHEKIT_MEMTABLE_BLOOM_WHOLE_KEY, true);
        configuration.setBoolean(LEGACY_MEMTABLE_BLOOM_WHOLE_KEY_KEY, false);

        assertConflictingMemtableBloomKeysRejected(configuration);
    }

    private static void assertConflictingMemtableBloomKeysRejected(Configuration configuration) {
        try {
            new RocksDBResourceContainer(
                    configuration, PredefinedOptions.DEFAULT, null, null, null, false);
            fail("Expected conflicting CacheKit and deprecated memtable Bloom keys to fail");
        } catch (IllegalArgumentException expected) {
            assertThat(expected.getMessage(), containsString("Conflicting values"));
        }
    }

    private static void assertMemtableBloomOptionsApplied(Configuration configuration)
            throws Exception {

        final File dbDirectory = TMP_FOLDER.newFolder();
        final ArrayList<ColumnFamilyHandle> handles = new ArrayList<>();
        try (RocksDBResourceContainer container =
                new RocksDBResourceContainer(
                        configuration, PredefinedOptions.DEFAULT, null, null, null, false)) {
            final DBOptions dbOptions = container.getDbOptions();
            final ColumnFamilyOptions columnOptions = container.getColumnOptions();
            try (RocksDB db =
                    RocksDB.open(
                            dbOptions,
                            dbDirectory.getAbsolutePath(),
                            Collections.singletonList(
                                    new ColumnFamilyDescriptor(
                                            RocksDB.DEFAULT_COLUMN_FAMILY, columnOptions)),
                            handles)) {
                assertEquals(0.1, columnOptions.memtablePrefixBloomSizeRatio(), 0.0);
                assertNull(
                        ((BlockBasedTableConfig) columnOptions.tableFormatConfig()).filterPolicy());
            }
        } finally {
            handles.forEach(ColumnFamilyHandle::close);
        }

        final File[] optionsFiles =
                dbDirectory.listFiles((ignored, name) -> name.startsWith("OPTIONS-"));
        assertNotNull(optionsFiles);
        assertTrue(optionsFiles.length > 0);
        final String optionsText =
                new String(
                        java.nio.file.Files.readAllBytes(optionsFiles[0].toPath()),
                        StandardCharsets.UTF_8);
        assertThat(optionsText, containsString("memtable_whole_key_filtering=true"));
    }

    @Test
    public void testFreeSharedResourcesAfterClose() throws Exception {
        LRUCache cache = new LRUCache(1024L);
        WriteBufferManager wbm = new WriteBufferManager(1024L, cache);
        RocksDBSharedResources sharedResources =
                new RocksDBSharedResources(cache, wbm, 1024L, false);
        final ThrowingRunnable<Exception> disposer = sharedResources::close;
        OpaqueMemoryResource<RocksDBSharedResources> opaqueResource =
                new OpaqueMemoryResource<>(sharedResources, 1024L, disposer);

        RocksDBResourceContainer container =
                new RocksDBResourceContainer(PredefinedOptions.DEFAULT, null, opaqueResource);

        container.close();
        assertThat(cache.isOwningHandle(), is(false));
        assertThat(wbm.isOwningHandle(), is(false));
    }

    @Test
    public void testFreeWriteReadOptionsAfterClose() throws Exception {
        RocksDBResourceContainer container = new RocksDBResourceContainer();
        WriteOptions writeOptions = container.getWriteOptions();
        ReadOptions readOptions = container.getReadOptions();
        assertThat(writeOptions.isOwningHandle(), is(true));
        assertThat(readOptions.isOwningHandle(), is(true));
        container.close();
        assertThat(writeOptions.isOwningHandle(), is(false));
        assertThat(readOptions.isOwningHandle(), is(false));
    }

    @Test
    public void testGetColumnFamilyOptionsWithPartitionedIndex() throws Exception {
        LRUCache cache = new LRUCache(1024L);
        WriteBufferManager wbm = new WriteBufferManager(1024L, cache);
        RocksDBSharedResources sharedResources =
                new RocksDBSharedResources(cache, wbm, 1024L, true);
        final ThrowingRunnable<Exception> disposer = sharedResources::close;
        OpaqueMemoryResource<RocksDBSharedResources> opaqueResource =
                new OpaqueMemoryResource<>(sharedResources, 1024L, disposer);
        BloomFilter blockBasedFilter = new BloomFilter();
        RocksDBOptionsFactory blockBasedBloomFilterOptionFactory =
                new RocksDBOptionsFactory() {

                    @Override
                    public DBOptions createDBOptions(
                            DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
                        return currentOptions;
                    }

                    @Override
                    public ColumnFamilyOptions createColumnOptions(
                            ColumnFamilyOptions currentOptions,
                            Collection<AutoCloseable> handlesToClose) {
                        TableFormatConfig tableFormatConfig = currentOptions.tableFormatConfig();
                        BlockBasedTableConfig blockBasedTableConfig =
                                tableFormatConfig == null
                                        ? new BlockBasedTableConfig()
                                        : (BlockBasedTableConfig) tableFormatConfig;
                        blockBasedTableConfig.setFilter(blockBasedFilter);
                        handlesToClose.add(blockBasedFilter);
                        currentOptions.setTableFormatConfig(blockBasedTableConfig);
                        return currentOptions;
                    }
                };
        try (RocksDBResourceContainer container =
                new RocksDBResourceContainer(
                        PredefinedOptions.DEFAULT,
                        blockBasedBloomFilterOptionFactory,
                        opaqueResource)) {
            ColumnFamilyOptions columnOptions = container.getColumnOptions();
            BlockBasedTableConfig actual =
                    (BlockBasedTableConfig) columnOptions.tableFormatConfig();
            assertThat(actual.indexType(), is(IndexType.kTwoLevelIndexSearch));
            assertThat(actual.partitionFilters(), is(true));
            assertThat(actual.pinTopLevelIndexAndFilter(), is(true));
            assertThat(actual.filterPolicy(), not(blockBasedFilter));
        }
        assertFalse("Block based filter is left unclosed.", blockBasedFilter.isOwningHandle());
    }
}
