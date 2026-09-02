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

import org.rocksdb.RocksDB;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class RocksDBDisposeGuardTest {

    @AfterEach
    void clearProperty() {
        System.clearProperty(RocksDBDisposeGuard.SKIP_NATIVE_CLOSE_PROPERTY);
    }

    @Test
    void closesByDefault() {
        RocksDB db = mock(RocksDB.class);

        RocksDBDisposeGuard.closeQuietly(db);

        verify(db).close();
    }

    @Test
    void skipsCloseOnlyWhenExplicitlyEnabled() {
        RocksDB db = mock(RocksDB.class);
        System.setProperty(RocksDBDisposeGuard.SKIP_NATIVE_CLOSE_PROPERTY, "true");

        RocksDBDisposeGuard.closeQuietly(db);

        verify(db, never()).close();
    }
}
