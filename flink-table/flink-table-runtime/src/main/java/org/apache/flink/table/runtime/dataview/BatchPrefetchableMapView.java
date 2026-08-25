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

package org.apache.flink.table.runtime.dataview;

import org.apache.flink.annotation.Internal;

/** Generated exact-DISTINCT code uses this interface to stage one batch of MapView keys. */
@Internal
public interface BatchPrefetchableMapView<K> {

    /** Starts collecting keys for the next exact-DISTINCT batch. */
    void beginPrefetchKeyCollection(int expectedKeys);

    /** Adds one exact, non-null key. */
    void addPrefetchKey(K key);

    /** Starts backend prefetch for the collected keys and clears the collection. */
    boolean finishPrefetchKeyCollection() throws Exception;

    /** Clears an unfinished collection and any backend prefetch scope. */
    void abortPrefetchKeyCollection();
}
