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

package org.apache.flink.contrib.streaming.state.cachekit.nativeplane;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DirectBufferDataInputViewTest {

    @Test
    void readsOutputViewEncodingWithoutChangingCallerState() throws Exception {
        ByteBuffer caller = ByteBuffer.allocateDirect(128);
        caller.position(7);
        caller.limit(103);
        DirectBufferDataOutputView output = new DirectBufferDataOutputView(caller);
        output.writeBoolean(true);
        output.writeInt(0x10203040);
        output.writeLong(0x0102030405060708L);
        output.writeUTF("kunpeng-\u9cb2\u9e4f");

        DirectBufferDataInputView input = new DirectBufferDataInputView(caller);
        input.setRegion(7, output.position());

        assertEquals(true, input.readBoolean());
        assertEquals(0x10203040, input.readInt());
        assertEquals(0x0102030405060708L, input.readLong());
        assertEquals("kunpeng-\u9cb2\u9e4f", input.readUTF());
        assertEquals(0, input.available());
        assertEquals(7, caller.position());
        assertEquals(103, caller.limit());
    }

    @Test
    void reusesOneViewAcrossRegionsAndRejectsUnderflow() throws Exception {
        ByteBuffer values = ByteBuffer.allocateDirect(32);
        values.putInt(0, 11);
        values.putInt(16, 22);
        DirectBufferDataInputView input = new DirectBufferDataInputView(values);

        input.setRegion(0, Integer.BYTES);
        assertEquals(11, input.readInt());
        assertThrows(EOFException.class, input::readByte);

        input.setRegion(16, Integer.BYTES);
        assertEquals(22, input.readInt());
        assertEquals(0, values.position());
        assertEquals(32, values.limit());
    }
}
