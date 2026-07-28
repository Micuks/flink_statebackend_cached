/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBBatchValueReader;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalValueState;

import com.sun.management.ThreadMXBean;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Manual worker-allocation and deferred-materialization probe for speculative lazy staging. */
public final class LazyStagingWorkProbe {

    private static final int WARMUP_ENTRIES = 512;
    private static final int MEASURED_ENTRIES = 4096;
    private static final int USEFUL_ENTRIES = 512;

    private LazyStagingWorkProbe() {}

    public static void main(String[] args) throws Exception {
        ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!bean.isThreadAllocatedMemorySupported()) {
            throw new IllegalStateException("Thread allocation measurement is not supported.");
        }
        bean.setThreadAllocatedMemoryEnabled(true);

        for (int iteration = 0; iteration < 3; iteration++) {
            Probe eagerWarmup = newProbe(WARMUP_ENTRIES, false);
            eagerWarmup.task.run();
            Probe lazyWarmup = newProbe(WARMUP_ENTRIES, true);
            lazyWarmup.task.run();
        }

        Probe eager = newProbe(MEASURED_ENTRIES, false);
        long eagerWorkerBytes = measureTask(bean, eager.task);
        Probe lazy = newProbe(MEASURED_ENTRIES, true);
        long lazyWorkerBytes = measureTask(bean, lazy.task);

        int lazyWorkerDeserializations = lazy.deserializations.get();
        int sink = 0;
        for (int index = 0; index < USEFUL_ENTRIES; index++) {
            lazy.currentKey.set(lazy.keys.get(index));
            sink += lazy.state.value();
        }
        int lazyUsefulDeserializations = lazy.deserializations.get();

        System.out.printf(
                "entries=%d useful=%d eager_worker_bytes=%d eager_bytes_per_entry=%.3f "
                        + "lazy_worker_bytes=%d lazy_bytes_per_entry=%.3f "
                        + "eager_worker_deserializations=%d lazy_worker_deserializations=%d "
                        + "lazy_useful_deserializations=%d lazy_staged=%d "
                        + "lazy_materialized=%d sink=%d%n",
                MEASURED_ENTRIES,
                USEFUL_ENTRIES,
                eagerWorkerBytes,
                (double) eagerWorkerBytes / MEASURED_ENTRIES,
                lazyWorkerBytes,
                (double) lazyWorkerBytes / MEASURED_ENTRIES,
                eager.deserializations.get(),
                lazyWorkerDeserializations,
                lazyUsefulDeserializations,
                lazy.state.getPrefetchLazyValuesStagedForTesting(),
                lazy.state.getPrefetchLazyValuesMaterializedForTesting(),
                sink);

        if (eager.deserializations.get() != MEASURED_ENTRIES) {
            throw new AssertionError("Eager worker did not materialize every staged value.");
        }
        if (lazyWorkerDeserializations != 0) {
            throw new AssertionError("Lazy worker unexpectedly materialized values.");
        }
        if (lazyUsefulDeserializations != USEFUL_ENTRIES) {
            throw new AssertionError("Lazy path did not materialize exactly the useful values.");
        }
        if (lazyWorkerBytes >= eagerWorkerBytes) {
            throw new AssertionError("Lazy worker did not reduce allocated bytes.");
        }
    }

    private static long measureTask(ThreadMXBean bean, Runnable task) {
        long threadId = Thread.currentThread().getId();
        long before = bean.getThreadAllocatedBytes(threadId);
        task.run();
        return bean.getThreadAllocatedBytes(threadId) - before;
    }

    private static Probe newProbe(int entries, boolean lazy) throws Exception {
        AtomicReference<String> currentKey = new AtomicReference<>("unused");
        AtomicInteger deserializations = new AtomicInteger();
        CountingIntSerializer valueSerializer = new CountingIntSerializer(deserializations);
        ProbeValueState delegate = new ProbeValueState(entries, valueSerializer);
        CachedInternalValueState<String, String, Integer> state =
                new CachedInternalValueState<>(
                        delegate,
                        currentKey::get,
                        currentKey::set,
                        entries * 2,
                        CachePolicyType.LRU,
                        0,
                        false,
                        0.05,
                        1000,
                        true,
                        entries,
                        2,
                        false,
                        lazy);
        state.setCurrentNamespace("window-probe");
        List<String> keys = new ArrayList<>(entries);
        for (int index = 0; index < entries; index++) {
            keys.add("key-" + index);
        }
        Runnable task = state.buildAsyncPrefetchTask(keys);
        if (task == null) {
            throw new AssertionError("Probe failed to build a prefetch task.");
        }
        return new Probe(state, task, currentKey, keys, deserializations);
    }

    private static final class Probe {
        private final CachedInternalValueState<String, String, Integer> state;
        private final Runnable task;
        private final AtomicReference<String> currentKey;
        private final List<String> keys;
        private final AtomicInteger deserializations;

        private Probe(
                CachedInternalValueState<String, String, Integer> state,
                Runnable task,
                AtomicReference<String> currentKey,
                List<String> keys,
                AtomicInteger deserializations) {
            this.state = state;
            this.task = task;
            this.currentKey = currentKey;
            this.keys = keys;
            this.deserializations = deserializations;
        }
    }

    private static final class ProbeValueState
            implements InternalValueState<String, String, Integer>,
                    RocksDBBatchValueReader<String, String, Integer> {

        private final List<byte[]> serializedValues;
        private final CountingIntSerializer valueSerializer;
        private Integer value;

        private ProbeValueState(int entries, CountingIntSerializer valueSerializer) {
            this.valueSerializer = valueSerializer;
            this.serializedValues = new ArrayList<>(entries);
            for (int index = 0; index < entries; index++) {
                int value = 1000 + index;
                serializedValues.add(
                        new byte[] {
                            (byte) (value >>> 24),
                            (byte) (value >>> 16),
                            (byte) (value >>> 8),
                            (byte) value
                        });
            }
        }

        @Override
        public Integer value() {
            return value;
        }

        @Override
        public void update(Integer value) {
            this.value = value;
        }

        @Override
        public void clear() {
            value = null;
        }

        @Override
        public TypeSerializer<String> getKeySerializer() {
            return StringSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer<String> getNamespaceSerializer() {
            return StringSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer<Integer> getValueSerializer() {
            return valueSerializer;
        }

        @Override
        public void setCurrentNamespace(String namespace) {}

        @Override
        public byte[] getSerializedValue(
                byte[] serializedKeyAndNamespace,
                TypeSerializer<String> safeKeySerializer,
                TypeSerializer<String> safeNamespaceSerializer,
                TypeSerializer<Integer> safeValueSerializer) {
            return null;
        }

        @Override
        public InternalKvState.StateIncrementalVisitor<String, String, Integer>
                getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Integer getBatchDefaultValue() {
            return null;
        }

        @Override
        public byte[] serializeBatchKeyAndNamespace(
                String key,
                String namespace,
                TypeSerializer<String> safeKeySerializer,
                TypeSerializer<String> safeNamespaceSerializer) {
            int hash = key.hashCode();
            return new byte[] {
                (byte) (hash >>> 24), (byte) (hash >>> 16), (byte) (hash >>> 8), (byte) hash
            };
        }

        @Override
        public byte[] getSerializedValueByRocksDBKey(byte[] rocksDBKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<byte[]> getSerializedValuesByRocksDBKeys(
                List<byte[]> rocksDBKeys, int fromIndex, int toIndex) {
            return serializedValues.subList(fromIndex, toIndex);
        }

        @Override
        public List<byte[]> getSerializedValues(
                List<byte[]> serializedKeyAndNamespaces,
                TypeSerializer<String> safeKeySerializer,
                TypeSerializer<String> safeNamespaceSerializer) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class CountingIntSerializer extends TypeSerializer<Integer> {

        private static final long serialVersionUID = 1L;
        private final AtomicInteger deserializations;

        private CountingIntSerializer(AtomicInteger deserializations) {
            this.deserializations = deserializations;
        }

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TypeSerializer<Integer> duplicate() {
            return new CountingIntSerializer(deserializations);
        }

        @Override
        public Integer createInstance() {
            return 0;
        }

        @Override
        public Integer copy(Integer from) {
            return from;
        }

        @Override
        public Integer copy(Integer from, Integer reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return 4;
        }

        @Override
        public void serialize(Integer record, DataOutputView target) throws IOException {
            target.writeInt(record);
        }

        @Override
        public Integer deserialize(DataInputView source) throws IOException {
            deserializations.incrementAndGet();
            return source.readInt();
        }

        @Override
        public Integer deserialize(Integer reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeInt(source.readInt());
        }

        @Override
        public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
            return IntSerializer.INSTANCE.snapshotConfiguration();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof CountingIntSerializer;
        }

        @Override
        public int hashCode() {
            return CountingIntSerializer.class.hashCode();
        }
    }
}
