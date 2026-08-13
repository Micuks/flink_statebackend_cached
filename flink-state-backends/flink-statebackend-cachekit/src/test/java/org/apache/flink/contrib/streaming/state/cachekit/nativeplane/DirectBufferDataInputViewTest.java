/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of the
 * License at
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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DirectBufferDataInputViewTest {

    @Test
    void testSerializerReadsBoundedDirectRegionWithoutHeapCopy() throws Exception {
        DataOutputSerializer serialized = new DataOutputSerializer(64);
        IntSerializer.INSTANCE.serialize(42, serialized);
        StringSerializer.INSTANCE.serialize("kunpeng-\u20ac", serialized);

        ByteBuffer arena = ByteBuffer.allocateDirect(96);
        int offset = 17;
        arena.position(offset);
        arena.put(serialized.getCopyOfBuffer());

        DirectBufferDataInputView input = new DirectBufferDataInputView(arena);
        input.reset(offset, serialized.length());

        assertEquals(42, IntSerializer.INSTANCE.deserialize(input));
        assertEquals("kunpeng-\u20ac", StringSerializer.INSTANCE.deserialize(input));
        assertEquals(0, input.remaining());
    }

    @Test
    void testResetReusesViewAndBoundsReads() throws Exception {
        ByteBuffer arena = ByteBuffer.allocateDirect(16);
        arena.putInt(0, 7);
        arena.putInt(8, 9);
        DirectBufferDataInputView input = new DirectBufferDataInputView(arena);

        input.reset(0, Integer.BYTES);
        assertEquals(7, input.readInt());
        assertThrows(EOFException.class, input::readByte);

        input.reset(8, Integer.BYTES);
        assertEquals(9, input.readInt());
        assertThrows(IndexOutOfBoundsException.class, () -> input.reset(14, 4));
    }
}
