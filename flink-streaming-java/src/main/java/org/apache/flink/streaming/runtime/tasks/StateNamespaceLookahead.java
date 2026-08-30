/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;

/**
 * Optional mailbox lookahead seam for operators that can derive exact state namespaces without
 * executing user code or mutating operator state.
 *
 * <p>The caller supplies a stable keyed-state key. Implementations append zero or more matching
 * key/namespace pairs. Returning no pair is always safe: the later operator invocation keeps its
 * authoritative state-read path. Implementations must not register timers, merge windows, touch
 * keyed state, or retain either output list.
 */
@Internal
public interface StateNamespaceLookahead {

    void appendStatePrefetchKeyNamespaces(
            StreamRecord<?> record,
            Object stableKey,
            List<Object> keys,
            List<Object> namespaces)
            throws Exception;
}
