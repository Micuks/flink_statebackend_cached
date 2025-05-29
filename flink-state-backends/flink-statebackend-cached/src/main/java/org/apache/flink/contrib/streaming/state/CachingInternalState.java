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

import org.apache.flink.runtime.state.internal.InternalKvState;

import java.io.IOException;

/**
 * Base interface for internal Flink states that are enhanced with caching capabilities.
 *
 * @param <K> Key type of the Flink state.
 * @param <N> Namespace type.
 * @param <SV> State Value type (e.g., V for ValueState, Map for MapState, List for ListState).
 * @param <S_DEL> Type of the delegate state being wrapped (e.g., InternalValueState,
 *     InternalMapState, InternalListState).
 */
public interface CachingInternalState<K, N, SV, S_DEL extends InternalKvState<K, N, SV>>
        extends InternalKvState<K, N, SV> {

    void flushToUnderlyingState() throws IOException;

    S_DEL getDelegateState();

    long evictEntriesToFreeMemory(long targetBytesToFreeThisState);
}
