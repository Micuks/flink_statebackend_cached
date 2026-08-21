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
 * Explicit opt-in for consuming a reusable indexed view of a same-key input batch.
 *
 * <p>The {@code inputs} list passed to {@link #processBatchForKey} is valid only for the duration
 * of that call and must never be retained. Implementations may read it and remove elements through
 * its iterator, but must not add, replace, or reorder elements. This narrower contract lets the
 * runtime avoid copying values and allocating a list for every key while preserving the ordinary
 * {@link BatchableKeyedFunction} contract for third-party implementations.
 */
public interface ReusableBatchableKeyedFunction<IN, OUT> extends BatchableKeyedFunction<IN, OUT> {}
