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

package org.apache.flink.streaming.runtime.tasks;

/**
 * Opt-in marker for the {@code Safe(O)} per-edge mutation-safety predicate.
 *
 * <p>An operator may implement this marker iff, over its full lifecycle, it neither (U1) writes
 * the backing memory of an incoming record in place, nor (U2) lets an incoming record reference
 * escape past the synchronous {@code processElement} scope (into state, a buffer, an async queue,
 * or a downstream-forwarded alias retained for later). Such an operator can correctly receive a
 * zero-copy (object-reuse) input edge.
 *
 * <p>The predicate is sound by fail-closed default: operators that do <em>not</em> carry this
 * marker are treated as potentially retaining/mutating and receive a copying edge under
 * object-reuse. See {@link SafeChainClassifier}.
 */
public interface NonCopySafe {}
