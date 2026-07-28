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

package org.apache.flink.contrib.streaming.state.cachekit.nativeplane;

import org.apache.flink.annotation.Internal;

import java.nio.ByteBuffer;

/** Narrow request-plane contract used by the ValueState integration and its differential tests. */
@Internal
public interface NativeRequestPlane extends AutoCloseable {

    int fillBatch(
            SerializedKeyBatch<?, ?> keys,
            ByteBuffer valueArena,
            ByteBuffer valueMetadata,
            ByteBuffer fillResults);

    int probeBatch(
            SerializedKeyBatch<?, ?> keys, ByteBuffer valueOutput, ByteBuffer probeResults);

    String selectedKernel();

    long detectedFeatureBits();

    @Override
    void close();
}
