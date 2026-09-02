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

package org.apache.flink.runtime.state.internal;

import org.apache.flink.annotation.Internal;

import java.util.List;

/** Optional exact-key batch-prefetch capability exposed by an internal MapState wrapper. */
@Internal
public interface BatchPrefetchableMapState<UK> {

    /**
     * One fail-closed exact-key read prepared while the owning key/namespace is current.
     *
     * <p>The backend may execute only the immutable native read off mailbox. Awaiting and value
     * materialization happen on mailbox when the corresponding outer-key batch is consumed.
     */
    interface PreparedValues {

        /** Returns insertion-ordered values, or {@code null} when the async read was rejected. */
        List<?> awaitValues() throws Exception;

        /** Best-effort cancellation. Implementations must make this method idempotent. */
        void cancel();
    }

    /**
     * Makes the current key/namespace's user keys available to subsequent point reads.
     * Implementations must preserve authoritative MapState semantics and fail closed.
     *
     * @return true only when a backend batch-prefetch path was exercised
     */
    boolean beginPrefetchCurrentKeys(Iterable<? extends UK> userKeys) throws Exception;

    /**
     * Whether this state can return values aligned with an already de-duplicated key list. This
     * lets a batch-scoped caller seed its own read overlay directly instead of staging values in a
     * second map and looking them up again one by one.
     */
    default boolean supportsDirectPrefetchedValues() {
        return false;
    }

    /**
     * Returns values aligned with {@code uniqueUserKeys}, or {@code null} after a fail-closed
     * backend failure. The caller must supply stable, non-null, insertion-ordered unique keys.
     * Missing entries and stored null values are both represented by {@code null}, matching {@link
     * org.apache.flink.api.common.state.MapState#get(Object)} semantics.
     */
    default List<?> prefetchCurrentUniqueKeyValues(List<? extends UK> uniqueUserKeys)
            throws Exception {
        return null;
    }

    /**
     * Captures the current key/namespace and submits an immutable exact-key backend read.
     * Implementations that cannot safely overlap the read return {@code null}.
     */
    default PreparedValues prepareCurrentUniqueKeyValues(List<? extends UK> uniqueUserKeys)
            throws Exception {
        return null;
    }

    /**
     * Ends the current batch-prefetch scope and discards every transient staged value. This method
     * must be idempotent and must not change authoritative state.
     */
    void endPrefetchCurrentKeys();
}
