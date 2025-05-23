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

import org.apache.flink.runtime.state.StateBackendMigrationTestBase;
import org.junit.jupiter.api.Disabled;

/**
 * Migration tests for {@link CachingKeyedStateBackend}. This test should cover scenarios like
 * schema evolution for cached states and compatibility with snapshots from previous versions if
 * applicable.
 */
@Disabled("Placeholder for CachingKeyedStateBackend migration tests")
class CachingKeyedStateBackendMigrationTest /*
                                             * extends
                                             * StateBackendMigrationTestBase<CachingStateBackend>
                                             */ {
    // TODO: Implement migration tests. E.g.:
    // - Test migrating a snapshot taken with a non-cached backend to a cached backend.
    // - Test migrating a snapshot with different cache configurations.
    // - Test schema evolution for cached ValueState, ListState, MapState.

    // @Override
    // protected CachingStateBackend getStateBackend() throws Exception {
    // // Setup CachingStateBackend with a delegate (e.g., MemoryStateBackend for testing)
    // return null;
    // }

    // @Override
    // protected CheckpointStorage getCheckpointStorage() {
    // // Setup a checkpoint storage (e.g., MemoryCheckpointStorage for testing)
    // return null;
    // }
}
