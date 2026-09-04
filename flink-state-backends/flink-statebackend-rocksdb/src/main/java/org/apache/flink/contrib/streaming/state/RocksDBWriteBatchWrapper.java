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
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchWithIndex;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * It's a wrapper class around RocksDB's {@link WriteBatch} for writing in bulk.
 *
 * <p>IMPORTANT: This class is not thread safe.
 */
public class RocksDBWriteBatchWrapper implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBWriteBatchWrapper.class);

    private static final int MIN_CAPACITY = 100;
    private static final int MAX_CAPACITY = 1000;
    private static final int PER_RECORD_BYTES = 100;
    // default 0 for disable memory size based flush
    private static final long DEFAULT_BATCH_SIZE = 0;

    private final RocksDB db;

    private final WriteBatch batch;

    @Nullable private final WriteBatchWithIndex indexedBatch;

    private final WriteOptions options;

    private final int capacity;

    @Nonnegative private final long batchSize;

    private long indexedPuts;
    private long indexedDeletes;
    private long indexedMapPuts;
    private long indexedMapDeletes;
    private long indexedPointReads;
    private long indexedPointReadsWithPendingWrites;
    private long indexedDirectPointReads;
    private long indexedMergedIterators;
    private long indexedMergedIteratorsWithPendingWrites;
    private long indexedBaseIterators;
    private long indexedFlushes;
    private long indexedFlushedEntries;
    private long indexedMaxEntriesPerFlush;
    private long indexedCountFlushes;
    private long indexedSizeFlushes;
    private long indexedEstimatedBytes;
    private final long[] indexedFenceFlushes = new long[FlushReason.values().length];

    enum FlushReason {
        BULK_READ,
        DIRECT_BULK_READ,
        CLEAR,
        SNAPSHOT,
        SAVEPOINT,
        ENUMERATE_KEYS,
        ENUMERATE_KEYS_AND_NAMESPACES,
        MIGRATION,
        COUNT_ENTRIES,
        CLOSE
    }

    public RocksDBWriteBatchWrapper(@Nonnull RocksDB rocksDB, long writeBatchSize) {
        this(rocksDB, null, 500, writeBatchSize);
    }

    public RocksDBWriteBatchWrapper(@Nonnull RocksDB rocksDB, @Nullable WriteOptions options) {
        this(rocksDB, options, 500, DEFAULT_BATCH_SIZE);
    }

    public RocksDBWriteBatchWrapper(
            @Nonnull RocksDB rocksDB, @Nullable WriteOptions options, long batchSize) {
        this(rocksDB, options, 500, batchSize);
    }

    public RocksDBWriteBatchWrapper(
            @Nonnull RocksDB rocksDB,
            @Nullable WriteOptions options,
            int capacity,
            long batchSize) {
        this(rocksDB, options, capacity, batchSize, false);
    }

    public RocksDBWriteBatchWrapper(
            @Nonnull RocksDB rocksDB,
            @Nullable WriteOptions options,
            int capacity,
            long batchSize,
            boolean indexed) {
        Preconditions.checkArgument(
                capacity >= MIN_CAPACITY && capacity <= MAX_CAPACITY,
                "capacity should be between " + MIN_CAPACITY + " and " + MAX_CAPACITY);
        Preconditions.checkArgument(batchSize >= 0, "Max batch size have to be no negative.");

        this.db = rocksDB;
        this.options = options;
        this.capacity = capacity;
        this.batchSize = batchSize;
        if (this.batchSize > 0) {
            this.batch =
                    new WriteBatch(
                            (int) Math.min(this.batchSize, this.capacity * PER_RECORD_BYTES));
        } else {
            this.batch = new WriteBatch(this.capacity * PER_RECORD_BYTES);
        }
        this.indexedBatch = indexed ? new WriteBatchWithIndex(true) : null;
    }

    public void put(@Nonnull ColumnFamilyHandle handle, @Nonnull byte[] key, @Nonnull byte[] value)
            throws RocksDBException {

        batch.put(handle, key, value);

        flushIfNeeded();
    }

    public void remove(@Nonnull ColumnFamilyHandle handle, @Nonnull byte[] key)
            throws RocksDBException {

        batch.remove(handle, key);

        flushIfNeeded();
    }

    void putMapState(
            @Nonnull ColumnFamilyHandle handle, @Nonnull byte[] key, @Nonnull byte[] value)
            throws RocksDBException {
        Preconditions.checkState(indexedBatch != null, "Indexed write batch is not enabled.");
        indexedBatch.put(handle, key, value);
        indexedPuts++;
        indexedMapPuts++;
        indexedEstimatedBytes += key.length + value.length + 16L;
        flushIfNeeded();
    }

    void removeMapState(@Nonnull ColumnFamilyHandle handle, @Nonnull byte[] key)
            throws RocksDBException {
        Preconditions.checkState(indexedBatch != null, "Indexed write batch is not enabled.");
        indexedBatch.remove(handle, key);
        indexedDeletes++;
        indexedMapDeletes++;
        indexedEstimatedBytes += key.length + 12L;
        flushIfNeeded();
    }

    public void flush() throws RocksDBException {
        int indexedCount = indexedBatch == null ? 0 : indexedBatch.count();
        if (batch.count() == 0 && indexedCount == 0) {
            return;
        }
        if (options != null) {
            writeBatch(options);
        } else {
            // use the default WriteOptions, if wasn't provided.
            try (WriteOptions writeOptions = new WriteOptions()) {
                writeBatch(writeOptions);
            }
        }
        clear();
        if (indexedCount != 0) {
            indexedFlushes++;
            indexedFlushedEntries += indexedCount;
            indexedMaxEntriesPerFlush = Math.max(indexedMaxEntriesPerFlush, indexedCount);
        }
    }

    void flush(FlushReason reason) throws RocksDBException {
        if (indexedBatch != null && indexedBatch.count() != 0) {
            indexedFenceFlushes[reason.ordinal()]++;
        }
        flush();
    }

    public boolean isIndexed() {
        return indexedBatch != null;
    }

    public boolean hasPendingWrites() {
        return indexedBatch != null && indexedBatch.count() != 0;
    }

    public byte[] getFromBatchAndDB(
            @Nonnull ColumnFamilyHandle handle,
            @Nonnull ReadOptions readOptions,
            @Nonnull byte[] key)
            throws RocksDBException {
        Preconditions.checkState(indexedBatch != null, "Indexed write batch is not enabled.");
        indexedPointReads++;
        if (indexedBatch.count() == 0) {
            indexedDirectPointReads++;
            return db.get(handle, readOptions, key);
        }
        indexedPointReadsWithPendingWrites++;
        return indexedBatch.getFromBatchAndDB(db, handle, readOptions, key);
    }

    public RocksIteratorWrapper newIteratorWithBase(
            @Nonnull ColumnFamilyHandle handle, @Nonnull ReadOptions readOptions) {
        Preconditions.checkState(indexedBatch != null, "Indexed write batch is not enabled.");
        indexedMergedIterators++;
        if (indexedBatch.count() == 0) {
            indexedBaseIterators++;
            return new RocksIteratorWrapper(db.newIterator(handle, readOptions));
        }
        indexedMergedIteratorsWithPendingWrites++;
        return new RocksIteratorWrapper(
                indexedBatch.newIteratorWithBase(
                        handle, db.newIterator(handle, readOptions), readOptions));
    }

    public WriteOptions getOptions() {
        return options;
    }

    @Override
    public void close() throws RocksDBException {
        if (count() != 0) {
            flush(FlushReason.CLOSE);
        }
        IOUtils.closeQuietly(batch);
        IOUtils.closeQuietly(indexedBatch);
        if (indexedBatch != null) {
            LOG.info(
                    "[CACHEKIT ROCKSDB INDEXED WRITE BATCH] enabled=true puts={} deletes={} "
                            + "mapPuts={} mapDeletes={} "
                            + "pointReads={} pointReadsWithPendingWrites={} directPointReads={} "
                            + "iteratorRequests={} mergedIteratorsWithPendingWrites={} "
                            + "baseIterators={} flushes={} flushedEntries={} "
                            + "maxEntriesPerFlush={} countFlushes={} sizeFlushes={} "
                            + "bulkReadFences={} directBulkReadFences={} clearFences={} "
                            + "snapshotFences={} savepointFences={} enumerateKeysFences={} "
                            + "enumerateKeysAndNamespacesFences={} migrationFences={} "
                            + "countEntriesFences={} closeFences={}",
                    indexedPuts,
                    indexedDeletes,
                    indexedMapPuts,
                    indexedMapDeletes,
                    indexedPointReads,
                    indexedPointReadsWithPendingWrites,
                    indexedDirectPointReads,
                    indexedMergedIterators,
                    indexedMergedIteratorsWithPendingWrites,
                    indexedBaseIterators,
                    indexedFlushes,
                    indexedFlushedEntries,
                    indexedMaxEntriesPerFlush,
                    indexedCountFlushes,
                    indexedSizeFlushes,
                    indexedFenceFlushes[FlushReason.BULK_READ.ordinal()],
                    indexedFenceFlushes[FlushReason.DIRECT_BULK_READ.ordinal()],
                    indexedFenceFlushes[FlushReason.CLEAR.ordinal()],
                    indexedFenceFlushes[FlushReason.SNAPSHOT.ordinal()],
                    indexedFenceFlushes[FlushReason.SAVEPOINT.ordinal()],
                    indexedFenceFlushes[FlushReason.ENUMERATE_KEYS.ordinal()],
                    indexedFenceFlushes[FlushReason.ENUMERATE_KEYS_AND_NAMESPACES.ordinal()],
                    indexedFenceFlushes[FlushReason.MIGRATION.ordinal()],
                    indexedFenceFlushes[FlushReason.COUNT_ENTRIES.ordinal()],
                    indexedFenceFlushes[FlushReason.CLOSE.ordinal()]);
        }
    }

    private void flushIfNeeded() throws RocksDBException {
        if (count() == capacity) {
            if (hasPendingWrites()) {
                indexedCountFlushes++;
            }
            flush();
        } else if (batchSize > 0
                && (batch.getDataSize() >= batchSize
                        || indexedBatch == null
                        || indexedEstimatedBytes >= batchSize)
                && getDataSize() >= batchSize) {
            if (hasPendingWrites()) {
                indexedSizeFlushes++;
            }
            flush();
        }
    }

    @VisibleForTesting
    long getDataSize() {
        long dataSize = batch.getDataSize();
        if (indexedBatch == null) {
            return dataSize;
        }
        try (WriteBatch rawBatch = indexedBatch.getWriteBatch()) {
            return dataSize + rawBatch.getDataSize();
        }
    }

    @VisibleForTesting
    int count() {
        return batch.count() + (indexedBatch == null ? 0 : indexedBatch.count());
    }

    private void writeBatch(WriteOptions writeOptions) throws RocksDBException {
        if (batch.count() != 0) {
            db.write(writeOptions, batch);
        }
        if (indexedBatch != null && indexedBatch.count() != 0) {
            db.write(writeOptions, indexedBatch);
        }
    }

    private void clear() {
        batch.clear();
        if (indexedBatch != null) {
            indexedBatch.clear();
            indexedEstimatedBytes = 0L;
        }
    }
}
