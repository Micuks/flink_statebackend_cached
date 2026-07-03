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

package org.apache.flink.streaming.api.operators;

/**
 * Marker interface declaring that a keyed operator is <em>commutative within the same key</em> for
 * the purpose of mailbox-level same-key amortization (CacheKit Direction-6 v2).
 *
 * <p>Operators that implement this interface assert that processing a batch of records that share
 * the same key in any order (relative to each other) produces the same final state and the same
 * downstream output as processing them in arrival order. This permits the framework to:
 *
 * <ol>
 *   <li>radix-sort a batch by {@code (keyGroup, keyHash)}, and
 *   <li>walk the sorted runs of equal keys and call {@code setCurrentKey} once per run rather than
 *       once per record — amortising the per-record key-context cost.
 * </ol>
 *
 * <p><strong>Records with different keys remain in arrival order at coarse granularity (sort is
 * stable within keyGroup).</strong> Cross-key reordering is allowed by the streaming SQL contract
 * since per-key state is independent.
 *
 * <p>Examples that opt in:
 *
 * <ul>
 *   <li>{@code GroupAggFunction} / {@code MiniBatchGroupAggFunction} (sums, counts, averages — all
 *       commute over same-key inserts/retracts).
 *   <li>Set-merge / hash-aggregation operators where the accumulator is associative and
 *       commutative.
 * </ul>
 *
 * <p>Examples that DO NOT opt in (must continue with per-record dispatch):
 *
 * <ul>
 *   <li><strong>Deduplication operators</strong> — {@code ProcTimeDeduplicateKeepFirstRowFunction}
 *       / {@code ProcTimeDeduplicateKeepLastRowFunction} keep the row by arrival order; {@code
 *       RowTimeDeduplicateFunction} uses arrival order for timestamp tie-breaks. Reordering
 *       same-key records changes which row survives.
 *   <li>LAG / LEAD / FIRST_VALUE / LAST_VALUE and any operator whose output depends on per-key
 *       arrival order.
 *   <li>Two-input join operators where input1/input2 retract ordering matters.
 *   <li>Window operators where timer-firing ordering interleaves with element ordering.
 *   <li>Stateful operators with non-commutative state machines.
 * </ul>
 *
 * <p>This interface has no methods — it is a pure typing marker. The runtime classifier in {@code
 * BatchedKeyedOperatorAdapter} also recognises a hard-coded list of well-known commutative function
 * FQNs as a fallback when an operator class cannot be modified directly (e.g. Calcite code-gen'd
 * subclasses).
 */
public interface CommutativeKeyedOperator {}
