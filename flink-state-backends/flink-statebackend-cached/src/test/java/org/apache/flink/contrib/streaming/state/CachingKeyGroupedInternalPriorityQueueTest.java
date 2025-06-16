package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueue;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.InternalKeyContextImpl;
import org.apache.flink.runtime.state.heap.KeyGroupPartitionedPriorityQueue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;

/**
 * Verifies that {@link CachingKeyGroupedInternalPriorityQueue} copies buffered elements and
 * therefore tolerates key mutations that would previously trigger an IllegalArgumentException
 * in the delegate {@link KeyGroupPartitionedPriorityQueue} (see FLINK-XXXXX).
 */
class CachingKeyGroupedInternalPriorityQueueTest {

    /** Simple queue element whose key can be mutated after insertion. */
    private static class MutableElement
            implements HeapPriorityQueueElement,
                    org.apache.flink.runtime.state.PriorityComparable<MutableElement>,
                    org.apache.flink.runtime.state.Keyed<Integer> {

        private int key;
        private int internalIndex = NOT_CONTAINED;

        MutableElement(int key) {
            this.key = key;
        }

        void setKey(int newKey) {
            this.key = newKey;
        }

        @Override
        public int getInternalIndex() {
            return internalIndex;
        }

        @Override
        public void setInternalIndex(int newIndex) {
            this.internalIndex = newIndex;
        }

        @Override
        public int comparePriorityTo(MutableElement other) {
            return Integer.compare(this.key, other.key);
        }

        @Override
        public Integer getKey() {
            return key;
        }
    }

    /** Minimal serializer that performs a deep copy via object reconstruction. */
    private static class MutableElementSerializer extends TypeSerializerSingleton<MutableElement> {

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public MutableElement createInstance() {
            return new MutableElement(0);
        }

        @Override
        public MutableElement copy(MutableElement from) {
            return new MutableElement(from.key);
        }

        @Override
        public MutableElement copy(MutableElement from, MutableElement reuse) {
            reuse.key = from.key;
            return reuse;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(MutableElement record, DataOutputView target) {
            // Not required for this unit test.
            throw new UnsupportedOperationException();
        }

        @Override
        public MutableElement deserialize(DataInputView source) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MutableElement deserialize(MutableElement reuse, DataInputView source) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) {
            // Not required for this unit test.
            throw new UnsupportedOperationException();
        }

        @Override
        public TypeSerializerSnapshot<MutableElement> snapshotConfiguration() {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void testKeyMutationDoesNotBreakPartitioning() {
        final int totalKeyGroups = 4;
        KeyGroupRange localRange = KeyGroupRange.of(0, 1); // sub-task owns KG 0 and 1
        InternalKeyContextImpl<Integer> keyContext =
                new InternalKeyContextImpl<>(localRange, totalKeyGroups);

        // Factory creating HeapPriorityQueues for each key group
        KeyGroupPartitionedPriorityQueue.PartitionQueueSetFactory<
                        MutableElement, KeyGroupHeapPQSet<MutableElement>>
                pqFactory =
                        (kg, numKgs, keyExtractor, comparator) ->
                                new KeyGroupHeapPQSet<>(
                                        comparator,
                                        keyExtractor,
                                        32,
                                        KeyGroupRange.of(kg, kg),
                                        numKgs);

        PriorityComparator<MutableElement> elementComparator = MutableElement::comparePriorityTo;
        KeyExtractorFunction<MutableElement> keyExtractor = MutableElement::getKey;

        KeyGroupPartitionedPriorityQueue<
                        MutableElement, KeyGroupHeapPQSet<MutableElement>>
                delegate =
                        new KeyGroupPartitionedPriorityQueue<>(
                                keyExtractor,
                                elementComparator,
                                pqFactory,
                                localRange,
                                totalKeyGroups);

        CachingKeyGroupedInternalPriorityQueue<MutableElement> queue =
                new CachingKeyGroupedInternalPriorityQueue<>(
                        delegate,
                        keyContext,
                        null, // backend is not used in this test
                        new MutableElementSerializer());

        // ----- add element and mutate key afterwards ----
        MutableElement e = new MutableElement(0); // key group 0
        keyContext.setCurrentKey(e.getKey());
        int originalKeyGroupIdx =
                KeyGroupRangeAssignment.assignToKeyGroup(e.getKey(), totalKeyGroups);
        keyContext.setCurrentKeyGroupIndex(originalKeyGroupIdx);

        Assertions.assertTrue(queue.add(e));

        // Mutate key so that it maps to a *different* key-group outside local range.
        e.setKey(3); // key group 3 (not owned by this queue)

        // Accessing the queue forces a flush. If mutation propagated, we would get an exception.
        keyContext.setCurrentKeyGroupIndex(originalKeyGroupIdx);
        MutableElement head = queue.peek();

        Assertions.assertNotNull(head, "Queue should contain an element after peek.");
        Assertions.assertEquals(0, head.getKey(), "Buffered copy must preserve original key.");
    }

    /**
     * Minimal key-group aware priority queue that is itself a heap element, fulfilling the generic
     * bounds required by {@link KeyGroupPartitionedPriorityQueue}.
     */
    private static class KeyGroupHeapPQSet<T extends HeapPriorityQueueElement>
            extends HeapPriorityQueueSet<T> implements HeapPriorityQueueElement {

        private int internalIndex = NOT_CONTAINED;

        KeyGroupHeapPQSet(
                PriorityComparator<T> elementComparator,
                KeyExtractorFunction<T> keyExtractor,
                int minimumCapacity,
                KeyGroupRange keyGroupRange,
                int totalNumberOfKeyGroups) {
            super(
                    elementComparator,
                    keyExtractor,
                    minimumCapacity,
                    keyGroupRange,
                    totalNumberOfKeyGroups);
        }

        @Override
        public int getInternalIndex() {
            return internalIndex;
        }

        @Override
        public void setInternalIndex(int newIndex) {
            this.internalIndex = newIndex;
        }
    }
} 