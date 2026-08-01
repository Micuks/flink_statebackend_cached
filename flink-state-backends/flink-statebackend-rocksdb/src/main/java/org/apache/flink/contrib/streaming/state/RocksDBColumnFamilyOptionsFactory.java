/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;

import org.rocksdb.ColumnFamilyOptions;

import javax.annotation.Nullable;

import java.util.function.Function;

/** Internal column-family options factory that retains the Flink state type. */
@FunctionalInterface
interface RocksDBColumnFamilyOptionsFactory extends Function<String, ColumnFamilyOptions> {

    ColumnFamilyOptions create(
            String stateName, @Nullable RegisteredStateMetaInfoBase stateMetaInfo);

    @Override
    default ColumnFamilyOptions apply(String stateName) {
        return create(stateName, null);
    }
}
