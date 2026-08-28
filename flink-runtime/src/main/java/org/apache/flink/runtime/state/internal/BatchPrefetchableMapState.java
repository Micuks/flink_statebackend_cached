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

        /** Whether this token can take part in the optional cross-key read wave. */
        enum WaveParticipation {
            /** The owning backend has the feature disabled for this dispatch. */
            DISABLED,
            /** The feature is enabled, but this token must retain its established path. */
            INELIGIBLE,
            /** This token is eligible for a shared read wave. */
            ELIGIBLE
        }

        /** Returns insertion-ordered values, or {@code null} when the async read was rejected. */
        List<?> awaitValues() throws Exception;

        /** Best-effort cancellation. Implementations must make this method idempotent. */
        void cancel();

        /**
         * Stable identity for a bounded speculative read wave, or {@code null} when this token
         * cannot participate.
         *
         * <p>Tokens may share a wave only when this identity is reference-equal. Backends normally
         * return the state wrapper that owns one RocksDB reader and column family.
         */
        default Object waveOwner() {
            return null;
        }

        /** Distinguishes global disablement from a temporarily ineligible token. */
        default WaveParticipation waveParticipation() {
            return WaveParticipation.DISABLED;
        }

        /**
         * Submits one all-or-none read wave for future tokens with the same {@link #waveOwner()}.
         *
         * <p>The method is invoked on the mailbox thread before any represented future outer-key
         * batch is consumed. Implementations must return without performing the backend read on the
         * caller. A false result leaves every token usable through its authoritative fallback.
         */
        default boolean executeWave(List<? extends PreparedValues> tokens) throws Exception {
            return false;
        }

        /**
         * Submits one all-or-none read across multiple state columns of the same prepared cohort.
         *
         * <p>Every nested list contains tokens for one {@link #waveOwner()}, while different lists
         * may belong to different MapState column families backed by the same database. A backend
         * that can fuse those columns into one worker task or one native MultiGet may override this
         * method. Returning {@code false} leaves every token untouched so the caller can use the
         * established per-column wave or authoritative fallback.
         */
        default boolean executeCohortWave(List<? extends List<? extends PreparedValues>> columns)
                throws Exception {
            return false;
        }

        /**
         * Whether this token can commit mutations through its prepared exact-key backend session.
         *
         * <p>This is an optional, fail-closed capability. A false result leaves the established
         * MapState mutation path authoritative.
         */
        default boolean supportsPreparedCommit() {
            return false;
        }

        /**
         * Commits mutations aligned with this token's insertion-ordered user keys.
         *
         * <p>A false result means the token was rejected before any backend write and the caller
         * may use its established fallback. Once the implementation invokes a backend write, all
         * failures must be thrown; they must never be converted to false because replay could
         * duplicate a partially successful mutation.
         */
        default boolean commitPreparedValues(
                Object[] values, boolean[] dirty, boolean[] removed) throws Exception {
            return false;
        }

        /**
         * Commits several prepared state columns through one backend write when supported.
         *
         * <p>Every values/dirty/removed row is aligned with the corresponding token's ordered
         * keys. Implementations must validate the complete cohort before writing. False has the
         * same pre-write-only meaning as {@link #commitPreparedValues(Object[], boolean[],
         * boolean[])}.
         */
        default boolean commitPreparedCohort(
                List<? extends PreparedValues> tokens,
                List<Object[]> values,
                List<boolean[]> dirty,
                List<boolean[]> removed)
                throws Exception {
            return false;
        }
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
