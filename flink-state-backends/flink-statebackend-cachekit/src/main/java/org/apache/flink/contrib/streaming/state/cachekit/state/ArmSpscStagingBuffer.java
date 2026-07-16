/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import java.util.Objects;

/**
 * Bounded single-producer/single-consumer ring for the prefetch-worker to mailbox hand-off.
 *
 * <p>The producer and consumer cursors live in distinct objects whose long payload is 128 bytes.
 * Consequently their volatile cursor fields cannot occupy the same 128-byte cache line even when
 * the allocator places the objects consecutively. This avoids relying on {@code @Contended} JVM
 * flags and matches the Kunpeng 920 cache-line size.
 *
 * <p>This class is deliberately package-private: CacheKit owns both threads and can enforce the
 * one-producer/one-consumer contract.
 */
final class ArmSpscStagingBuffer<E> {

    private final Object[] entries;
    private final int mask;
    private final int capacity;
    private final PaddedCursor producer = new PaddedCursor();
    private final PaddedCursor consumer = new PaddedCursor();

    ArmSpscStagingBuffer(int requestedCapacity) {
        if (requestedCapacity < 2) {
            throw new IllegalArgumentException("capacity must be at least 2");
        }
        int actualCapacity = 2;
        while (actualCapacity < requestedCapacity) {
            if (actualCapacity >= (1 << 30)) {
                throw new IllegalArgumentException("capacity is too large");
            }
            actualCapacity <<= 1;
        }
        this.entries = new Object[actualCapacity];
        this.mask = actualCapacity - 1;
        this.capacity = actualCapacity;
    }

    /** Publishes one element, or returns false when the bounded ring is full. */
    boolean offer(E element) {
        Objects.requireNonNull(element, "element");
        long producerIndex = producer.index;
        long wrapPoint = producerIndex - capacity;
        if (producer.cachedOtherIndex <= wrapPoint) {
            long observedConsumerIndex = consumer.index;
            producer.cachedOtherIndex = observedConsumerIndex;
            if (observedConsumerIndex <= wrapPoint) {
                return false;
            }
        }
        entries[(int) producerIndex & mask] = element;
        producer.index = producerIndex + 1;
        return true;
    }

    /** Returns the next published element, or null when the ring is empty. */
    @SuppressWarnings("unchecked")
    E poll() {
        long consumerIndex = consumer.index;
        if (consumerIndex >= consumer.cachedOtherIndex) {
            long observedProducerIndex = producer.index;
            consumer.cachedOtherIndex = observedProducerIndex;
            if (consumerIndex >= observedProducerIndex) {
                return null;
            }
        }
        int offset = (int) consumerIndex & mask;
        E element = (E) entries[offset];
        entries[offset] = null;
        consumer.index = consumerIndex + 1;
        return element;
    }

    /**
     * One hot cursor and its thread-confined cached opposite cursor, padded to a 128-byte long
     * payload. The object header makes the total allocation larger than 128 bytes.
     */
    private static final class PaddedCursor {
        private volatile long index;
        private long cachedOtherIndex;
        @SuppressWarnings("unused")
        private long p02, p03, p04, p05, p06, p07, p08, p09, p10, p11, p12, p13, p14, p15;
    }
}
