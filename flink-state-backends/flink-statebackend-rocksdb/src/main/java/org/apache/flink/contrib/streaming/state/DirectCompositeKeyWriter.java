/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Reusable exact-key serializer that writes into a caller-owned direct arena. */
final class DirectCompositeKeyWriter<K> {

    private final TypeSerializer<K> keySerializer;
    private final int keyGroupPrefixBytes;
    private final DataOutputSerializer scratch;

    DirectCompositeKeyWriter(
            TypeSerializer<K> keySerializer, int keyGroupPrefixBytes, int initialCapacity) {
        this.keySerializer = keySerializer.duplicate();
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.scratch = new DataOutputSerializer(initialCapacity);
    }

    <N> int write(
            K key,
            int keyGroup,
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            ByteBuffer target)
            throws IOException {
        scratch.clear();
        CompositeKeySerializationUtils.writeKeyGroup(
                keyGroup, keyGroupPrefixBytes, scratch);
        boolean ambiguous =
                CompositeKeySerializationUtils.isAmbiguousKeyPossible(
                        keySerializer, namespaceSerializer);
        CompositeKeySerializationUtils.writeKey(key, keySerializer, scratch, ambiguous);
        CompositeKeySerializationUtils.writeNameSpace(
                namespace, namespaceSerializer, scratch, ambiguous);

        int length = scratch.length();
        target.put(scratch.getSharedBuffer(), 0, length);
        return length;
    }
}
