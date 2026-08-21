/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;

import java.nio.ByteBuffer;

/**
 * Optional keyed-backend capability for grouping caller-owned Java hash tokens.
 *
 * <p>The runtime owns both direct buffers and the implementation consumes them synchronously. A
 * successful call writes a stable, first-seen packed plan and returns its group count. A negative
 * return value means that the caller must use its existing Java grouping path.
 *
 * <p>Tokens are only a filter: unequal Java keys may have the same hash code. The caller must
 * validate every source key with Java equality before processing any record.
 */
@Internal
public interface BatchKeyGroupingSupport {

    int PACKED_PLAN_MAGIC = 0x434B4750;
    int PACKED_PLAN_VERSION = 1;
    int PACKED_PLAN_HEADER_INTS = 4;
    int PACKED_PLAN_HEADER_BYTES = PACKED_PLAN_HEADER_INTS * Integer.BYTES;

    /** Maximum source entries accepted by {@link #groupHashTokens}. */
    int maxGroupingEntries();

    /** Whether this backend enables the reusable indexed LocalPreAgg consumer for this job. */
    default boolean indexedBatchFoldEnabled() {
        return false;
    }

    /**
     * Groups {@code count} native-order 32-bit tokens and writes a packed plan.
     *
     * <p>Layout: {@code [magic, version, sourceCount, groupCount]}, followed by {@code
     * firstSource[groupCount]}, {@code offsets[groupCount + 1]}, and {@code
     * sourceGroup[sourceCount]}. The implementation commits the header last.
     */
    int groupHashTokens(ByteBuffer tokens, int count, ByteBuffer packedPlan);

    /** Worst-case packed-plan capacity for {@code sourceCount} entries. */
    static int requiredPackedPlanBytes(int sourceCount) {
        if (sourceCount < 0 || sourceCount > (Integer.MAX_VALUE - 20) / 12) {
            throw new IllegalArgumentException("Invalid sourceCount: " + sourceCount);
        }
        return 20 + 12 * sourceCount;
    }
}
