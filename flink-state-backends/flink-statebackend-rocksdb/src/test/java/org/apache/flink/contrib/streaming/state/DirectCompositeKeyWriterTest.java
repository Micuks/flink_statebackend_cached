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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Byte-parity tests for the no-per-key-array composite key writer. */
class DirectCompositeKeyWriterTest {

    @Test
    void writesFixedWidthCompositeKeyWithOneByteKeyGroupPrefix() throws Exception {
        assertParity(1, 37, 1234, "namespace");
    }

    @Test
    void writesVariableWidthCompositeKeyWithTwoByteKeyGroupPrefix() throws Exception {
        DirectCompositeKeyWriter<String> writer =
                new DirectCompositeKeyWriter<>(StringSerializer.INSTANCE, 2);
        SerializedCompositeKeyBuilder<String> oracle =
                new SerializedCompositeKeyBuilder<>(StringSerializer.INSTANCE, 2, 8);

        String key = "variable-key-\u4e2d\u6587";
        String namespace = "variable-namespace-\u03bb";
        int keyGroup = 4097;
        DataOutputSerializer target = new DataOutputSerializer(512);

        writer.write(key, keyGroup, namespace, StringSerializer.INSTANCE, target);
        oracle.setKeyAndKeyGroup(key, keyGroup);

        assertThat(target.getCopyOfBuffer())
                .isEqualTo(oracle.buildCompositeKeyNamespace(namespace, StringSerializer.INSTANCE));
    }

    @Test
    void reusesScratchWithoutLeakingLongerPreviousKeyBytes() throws Exception {
        DirectCompositeKeyWriter<String> writer =
                new DirectCompositeKeyWriter<>(StringSerializer.INSTANCE, 1);
        DataOutputSerializer target = new DataOutputSerializer(512);

        writer.write(
                "a-key-that-forces-the-scratch-buffer-to-grow",
                3,
                "long-namespace",
                StringSerializer.INSTANCE,
                target);
        target.clear();
        writer.write("k", 3, "n", StringSerializer.INSTANCE, target);

        SerializedCompositeKeyBuilder<String> oracle =
                new SerializedCompositeKeyBuilder<>(StringSerializer.INSTANCE, 1, 4);
        oracle.setKeyAndKeyGroup("k", 3);
        byte[] expected = oracle.buildCompositeKeyNamespace("n", StringSerializer.INSTANCE);
        assertThat(target.length()).isEqualTo(expected.length);
        assertThat(target.getCopyOfBuffer()).isEqualTo(expected);
    }

    private static void assertParity(int prefixBytes, int keyGroup, int key, String namespace)
            throws Exception {
        DirectCompositeKeyWriter<Integer> writer =
                new DirectCompositeKeyWriter<>(IntSerializer.INSTANCE, prefixBytes);
        SerializedCompositeKeyBuilder<Integer> oracle =
                new SerializedCompositeKeyBuilder<>(IntSerializer.INSTANCE, prefixBytes, 4);
        DataOutputSerializer target = new DataOutputSerializer(128);

        writer.write(key, keyGroup, namespace, StringSerializer.INSTANCE, target);
        oracle.setKeyAndKeyGroup(key, keyGroup);
        byte[] expected = oracle.buildCompositeKeyNamespace(namespace, StringSerializer.INSTANCE);
        assertThat(target.getCopyOfBuffer()).isEqualTo(expected);
    }
}
