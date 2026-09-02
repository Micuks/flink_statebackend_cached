/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.VisibleForTesting;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.PackedTinyMapScanIterator;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/** One bounded, fail-closed idle slot per column family for packed prefix scans. */
final class RocksDBPackedTinyMapIteratorPool implements AutoCloseable {

    private final RocksDB db;
    private final ReadOptions readOptions;
    private final boolean eligible;
    private final Map<ColumnFamilyHandle, PackedTinyMapScanIterator> idleByColumnFamily =
            new IdentityHashMap<>();
    private final Map<ColumnFamilyHandle, Integer> activeByColumnFamily =
            new IdentityHashMap<>();

    private final LongAdder borrows = new LongAdder();
    private final LongAdder refreshSuccesses = new LongAdder();
    private final LongAdder freshCreates = new LongAdder();
    private final LongAdder concurrentCreates = new LongAdder();
    private final LongAdder refreshFallbacks = new LongAdder();
    private final LongAdder scanFallbacks = new LongAdder();
    private final LongAdder returns = new LongAdder();
    private final LongAdder discards = new LongAdder();
    private boolean closed;
    private boolean readOptionsClosed;

    RocksDBPackedTinyMapIteratorPool(RocksDB db, ReadOptions readOptions) {
        this.db = db;
        this.readOptions = new ReadOptions(readOptions);
        this.eligible =
                readOptions.snapshot() == null
                        && readOptions.iterateLowerBound() == null
                        && readOptions.iterateUpperBound() == null
                        && !readOptions.tailing()
                        && !readOptions.pinData();
    }

    boolean isEligible() {
        return eligible;
    }

    RocksDBPackedTinyMapScan.Result tryScan(
            ColumnFamilyHandle columnFamily,
            byte[] seekPrefix,
            int prefixCompareOffset,
            int maxEntries,
            int maxBytes)
            throws RocksDBException {
        if (!eligible) {
            return freshScan(
                    columnFamily,
                    seekPrefix,
                    prefixCompareOffset,
                    maxEntries,
                    maxBytes);
        }

        final BorrowedIterator borrowed = borrow(columnFamily);
        boolean reusable = false;
        try {
            try {
                borrowed.iterator.refresh();
                refreshSuccesses.increment();
            } catch (RocksDBException refreshFailure) {
                discard(borrowed);
                if (refreshFailure.getStatus().getCode() == Status.Code.NotSupported) {
                    refreshFallbacks.increment();
                    return freshScan(
                            columnFamily,
                            seekPrefix,
                            prefixCompareOffset,
                            maxEntries,
                            maxBytes);
                }
                throw refreshFailure;
            } catch (UnsatisfiedLinkError | NoSuchMethodError refreshAbiFailure) {
                refreshFallbacks.increment();
                discard(borrowed);
                return freshScan(
                        columnFamily,
                        seekPrefix,
                        prefixCompareOffset,
                        maxEntries,
                        maxBytes);
            } catch (RuntimeException | Error refreshFailure) {
                discard(borrowed);
                throw refreshFailure;
            }

            try {
                final RocksDBPackedTinyMapScan.Result result =
                        RocksDBPackedTinyMapScan.tryScan(
                                db,
                                borrowed.iterator,
                                seekPrefix,
                                prefixCompareOffset,
                                maxEntries,
                                maxBytes);
                reusable = true;
                return result;
            } catch (UnsatisfiedLinkError | NoSuchMethodError scanAbiFailure) {
                scanFallbacks.increment();
                discard(borrowed);
                return freshScan(
                        columnFamily,
                        seekPrefix,
                        prefixCompareOffset,
                        maxEntries,
                        maxBytes);
            } catch (RocksDBException | RuntimeException | Error scanFailure) {
                discard(borrowed);
                throw scanFailure;
            }
        } finally {
            if (reusable) {
                release(borrowed);
            }
        }
    }

    private RocksDBPackedTinyMapScan.Result freshScan(
            ColumnFamilyHandle columnFamily,
            byte[] seekPrefix,
            int prefixCompareOffset,
            int maxEntries,
            int maxBytes)
            throws RocksDBException {
        return RocksDBPackedTinyMapScan.tryScan(
                db,
                columnFamily,
                readOptions,
                seekPrefix,
                prefixCompareOffset,
                maxEntries,
                maxBytes);
    }

    @VisibleForTesting
    synchronized BorrowedIterator borrow(ColumnFamilyHandle columnFamily) {
        if (closed) {
            throw new IllegalStateException("packed iterator pool is closed");
        }
        borrows.increment();
        PackedTinyMapScanIterator iterator = idleByColumnFamily.remove(columnFamily);
        final boolean reused = iterator != null;
        final int active = activeByColumnFamily.getOrDefault(columnFamily, 0);
        if (!reused) {
            iterator = db.newPackedTinyMapScanIterator(columnFamily, readOptions);
            freshCreates.increment();
            if (active > 0) {
                concurrentCreates.increment();
            }
        }
        activeByColumnFamily.put(columnFamily, active + 1);
        return new BorrowedIterator(columnFamily, iterator, reused);
    }

    @VisibleForTesting
    synchronized void release(BorrowedIterator borrowed) {
        finishBorrow(borrowed);
        markInactive(borrowed.columnFamily);
        if (!closed && !idleByColumnFamily.containsKey(borrowed.columnFamily)) {
            idleByColumnFamily.put(borrowed.columnFamily, borrowed.iterator);
            returns.increment();
        } else {
            borrowed.iterator.close();
            discards.increment();
        }
        closeReadOptionsIfDrained();
    }

    private synchronized void discard(BorrowedIterator borrowed) {
        finishBorrow(borrowed);
        markInactive(borrowed.columnFamily);
        borrowed.iterator.close();
        discards.increment();
        closeReadOptionsIfDrained();
    }

    private void finishBorrow(BorrowedIterator borrowed) {
        if (!borrowed.active) {
            throw new IllegalStateException("packed iterator lease has already been returned");
        }
        borrowed.active = false;
    }

    private void markInactive(ColumnFamilyHandle columnFamily) {
        final int active = activeByColumnFamily.getOrDefault(columnFamily, 0);
        if (active <= 1) {
            activeByColumnFamily.remove(columnFamily);
        } else {
            activeByColumnFamily.put(columnFamily, active - 1);
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (PackedTinyMapScanIterator iterator : idleByColumnFamily.values()) {
            iterator.close();
        }
        idleByColumnFamily.clear();
        closeReadOptionsIfDrained();
    }

    private void closeReadOptionsIfDrained() {
        if (closed && activeByColumnFamily.isEmpty() && !readOptionsClosed) {
            readOptions.close();
            readOptionsClosed = true;
        }
    }

    long borrows() {
        return borrows.sum();
    }

    long refreshSuccesses() {
        return refreshSuccesses.sum();
    }

    long freshCreates() {
        return freshCreates.sum();
    }

    long concurrentCreates() {
        return concurrentCreates.sum();
    }

    long refreshFallbacks() {
        return refreshFallbacks.sum();
    }

    long scanFallbacks() {
        return scanFallbacks.sum();
    }

    long returns() {
        return returns.sum();
    }

    long discards() {
        return discards.sum();
    }

    @VisibleForTesting
    synchronized int activeLeases() {
        int active = 0;
        for (Integer count : activeByColumnFamily.values()) {
            active += count;
        }
        return active;
    }

    @VisibleForTesting
    synchronized boolean isReadOptionsClosed() {
        return readOptionsClosed;
    }

    synchronized void ensureDrained() {
        if (!activeByColumnFamily.isEmpty()) {
            throw new IllegalStateException(
                    "cannot dispose RocksDB while packed iterator leases remain active: "
                            + activeLeases());
        }
    }

    static final class BorrowedIterator {
        private final ColumnFamilyHandle columnFamily;
        private final PackedTinyMapScanIterator iterator;
        private final boolean reused;
        private boolean active = true;

        private BorrowedIterator(
                ColumnFamilyHandle columnFamily,
                PackedTinyMapScanIterator iterator,
                boolean reused) {
            this.columnFamily = columnFamily;
            this.iterator = iterator;
            this.reused = reused;
        }
    }
}
