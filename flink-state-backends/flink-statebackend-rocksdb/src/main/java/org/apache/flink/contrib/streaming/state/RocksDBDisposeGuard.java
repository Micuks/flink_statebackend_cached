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

import org.apache.flink.util.IOUtils;

import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Controls the final native RocksDB close for short-lived, isolated benchmark workers. */
final class RocksDBDisposeGuard {

    static final String SKIP_NATIVE_CLOSE_PROPERTY =
            "cachekit.rocksdb.skip-native-close-on-dispose";

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBDisposeGuard.class);

    private RocksDBDisposeGuard() {}

    static void closeQuietly(RocksDB db) {
        if (Boolean.getBoolean(SKIP_NATIVE_CLOSE_PROPERTY)) {
            // This escape hatch is deliberately opt-in. It is intended only for isolated
            // benchmark TaskManager processes that are destroyed immediately after each leg,
            // when the vendor FRocksDB closeDatabase() can block forever after all state access
            // has stopped. Runtime reads/writes and measured job execution are unaffected.
            LOG.warn(
                    "Skipping native RocksDB close because -D{}=true; the containing process "
                            + "must be terminated after the benchmark leg",
                    SKIP_NATIVE_CLOSE_PROPERTY);
            return;
        }
        IOUtils.closeQuietly(db);
    }
}
