/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDB;
import java.util.Arrays;
import java.util.Collections;
import static org.junit.Assert.*;

/** Real RocksDB roundtrip for the synchronous cache-eviction batch capability. */
public class RocksDBValueWriteBatchTest {
    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    @SuppressWarnings("unchecked")
    public void testOrderedBatchDeletesEncodingNamespacesAndContext() throws Exception {
        RocksDB.loadLibrary();
        RocksDBKeyedStateBackend<Integer> backend = RocksDBTestUtils
                .builderForTestDefaults(tempFolder.newFolder(), IntSerializer.INSTANCE).build();
        try {
            InternalValueState<Integer, Integer, Integer> state =
                    (InternalValueState<Integer, Integer, Integer>) backend.getPartitionedState(
                            9, IntSerializer.INSTANCE,
                            new ValueStateDescriptor<>("batch", IntSerializer.INSTANCE));
            RocksDBBatchValueReader<Integer, Integer, Integer> writer =
                    (RocksDBBatchValueReader<Integer, Integer, Integer>) state;
            assertTrue(writer.supportsSynchronousValueWriteBatch());
            backend.setCurrentKey(77);
            state.update(77);
            byte[] first = writer.serializeBatchKeyAndNamespace(
                    1, 1, IntSerializer.INSTANCE, IntSerializer.INSTANCE);
            byte[] second = writer.serializeBatchKeyAndNamespace(
                    2, 2, IntSerializer.INSTANCE, IntSerializer.INSTANCE);
            writer.writeSerializedValueBatch(Arrays.asList(first, second, first),
                    Arrays.asList(10, 20, null));
            assertEquals(Integer.valueOf(77), state.value());
            assertNull(writer.getSerializedValueByRocksDBKey(first));
            backend.setCurrentKey(2);
            state.setCurrentNamespace(2);
            assertEquals(Integer.valueOf(20), state.value());
            byte[] batchBytes = writer.getSerializedValueByRocksDBKey(second);
            state.update(20);
            assertArrayEquals(batchBytes, writer.getSerializedValueByRocksDBKey(second));
            assertThrows(IllegalArgumentException.class, () ->
                    writer.writeSerializedValueBatch(Collections.singletonList(second),
                            Collections.emptyList()));
            writer.writeSerializedValueBatch(Collections.emptyList(), Collections.emptyList());
            assertEquals(Integer.valueOf(20), state.value());
        } finally {
            backend.dispose();
        }
    }
}
