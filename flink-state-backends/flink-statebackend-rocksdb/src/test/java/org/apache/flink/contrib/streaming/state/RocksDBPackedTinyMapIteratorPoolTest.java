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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Lifecycle tests for the bounded packed-scan iterator pool. */
public class RocksDBPackedTinyMapIteratorPoolTest {

    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testActiveLeaseIsClosedAfterPoolCloseAndCannotBeReturnedTwice()
            throws Exception {
        RocksDB.loadLibrary();
        try (ReadOptions baseReadOptions = new ReadOptions();
                RocksDB db = RocksDB.open(tempFolder.newFolder().getAbsolutePath());
                ColumnFamilyHandle columnFamily =
                        db.createColumnFamily(new ColumnFamilyDescriptor("state".getBytes()))) {
            RocksDBPackedTinyMapIteratorPool pool =
                    new RocksDBPackedTinyMapIteratorPool(db, baseReadOptions);
            RocksDBPackedTinyMapIteratorPool.BorrowedIterator lease = pool.borrow(columnFamily);

            assertEquals(1, pool.activeLeases());
            pool.close();
            assertFalse(pool.isReadOptionsClosed());
            try {
                pool.ensureDrained();
                fail("Expected active-lease quiescence failure.");
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage().contains("leases remain active: 1"));
            }

            pool.release(lease);
            assertEquals(0, pool.activeLeases());
            assertEquals(1, pool.discards());
            assertTrue(pool.isReadOptionsClosed());
            pool.ensureDrained();

            try {
                pool.release(lease);
                fail("Expected a duplicate-return failure.");
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage().contains("already been returned"));
            }
            assertEquals(0, pool.activeLeases());
            assertEquals(1, pool.discards());
        }
    }
}
