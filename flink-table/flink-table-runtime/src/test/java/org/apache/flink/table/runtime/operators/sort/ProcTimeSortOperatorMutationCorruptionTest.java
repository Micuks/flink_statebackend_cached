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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Demonstrates SILENT STATE CORRUPTION (failure mode U2) in the UNMODIFIED {@link
 * ProcTimeSortOperator}, caused by the bare-reference escape at {@code processElement}:
 *
 * <pre>{@code
 *   RowData input = element.getValue();   // ProcTimeSortOperator.java:79
 *   dataState.add(input);                 // ProcTimeSortOperator.java:83  <-- bare ref, no CopyShield
 * }</pre>
 *
 * <p>The default test harness uses {@code MemoryStateBackend} (heap), and {@code HeapListState.add}
 * stores the BARE object reference ({@code list.add(value)} — no serialize-on-add). Under
 * {@code pipeline.object-reuse} / {@code NonCopyingChainingOutput}, the upstream operator hands the
 * SAME mutable {@link RowData} object to {@code processElement} across records, then mutates it for
 * the next record. Because state holds the bare reference, the value emitted later by
 * {@code onProcessingTime} (state re-read at ProcTimeSortOperator.java:93, forwarded at line 103)
 * reflects the MUTATED value — silent, un-flagged corruption.
 *
 * <p>This is precisely the case the name-based whitelist marks SAFE for the sort operator.
 *
 * <p>We mutate a NON-sort field (the BIGINT at index 1) so the corruption changes only the emitted
 * payload, NOT the sort order — {@link IntRecordComparator} keys only on field 0. This keeps the
 * scenario realistic (a quiet wrong value, not a crash). The two tests form the discriminating A/B:
 *
 * <ul>
 *   <li>{@link #testNoCopyObjectReuseCorruption()} — feeds ONE reused mutable row, mutates it after
 *       {@code add} but before the timer fires; asserts the emitted value equals the MUTATED
 *       (corrupted) value and NOT the original. This passing test DOCUMENTS the bug.
 *   <li>{@link #testCopyShieldSafe()} — same scenario but simulating a CopyShield by feeding a fresh
 *       independent instance; asserts the emitted value equals the ORIGINAL value. This proves the
 *       corruption is attributable to the missing copy, not to the operator's sort logic.
 * </ul>
 */
public class ProcTimeSortOperatorMutationCorruptionTest {

    private final InternalTypeInfo<RowData> inputRowType =
            InternalTypeInfo.ofFields(
                    new IntType(), new BigIntType(), VarCharType.STRING_TYPE, new IntType());

    private final GeneratedRecordComparator gComparator =
            new GeneratedRecordComparator("", "", new Object[0]) {

                private static final long serialVersionUID = -6067266199060901331L;

                @Override
                public RecordComparator newInstance(ClassLoader classLoader) {
                    return IntRecordComparator.INSTANCE;
                }
            };

    /** Builds a mutable GenericRowData: (int sortKey, long payload, string, int). */
    private static GenericRowData mutableRow(int sortKey, long payload, String s, int last) {
        GenericRowData row = new GenericRowData(4);
        row.setRowKind(RowKind.INSERT);
        row.setField(0, sortKey);
        row.setField(1, payload);
        row.setField(2, StringData.fromString(s));
        row.setField(3, last);
        return row;
    }

    /**
     * U2 repro: object-reuse hands the SAME mutable row to the operator; state keeps the bare ref;
     * mutating the row before the timer corrupts the emitted output. PASSES = bug is present.
     */
    @Test
    public void testNoCopyObjectReuseCorruption() throws Exception {
        ProcTimeSortOperator operator = createSortOperator();
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);
        testHarness.open();
        testHarness.setProcessingTime(0L);

        final long originalPayload = 100L;
        final long mutatedPayload = 999L;

        // Simulate NonCopyingChainingOutput / pipeline.object-reuse: ONE shared mutable instance.
        GenericRowData reused = mutableRow(7, originalPayload, "reused", 7);

        // processElement stores the BARE reference into HeapListState (no copy-on-add).
        testHarness.processElement(new StreamRecord<>(reused));

        // Upstream now reuses the SAME object for the next logical record by mutating it in place.
        // Mutate a NON-sort field so order is preserved but the payload is corrupted.
        reused.setField(1, mutatedPayload);
        // Also flip the RowKind to make the corruption unmistakable in the second assertion.
        reused.setRowKind(RowKind.DELETE);

        // Timer fires: onProcessingTime re-reads state (bare ref) and forwards it.
        testHarness.setProcessingTime(1L);

        List<RowData> out = extractOutput(testHarness);
        assertEquals("expected exactly one emitted row", 1, out.size());
        RowData emitted = out.get(0);

        // SILENT CORRUPTION: the emitted payload is the MUTATED value, not the one buffered at add().
        assertEquals(
                "BUG (U2): emitted payload reflects the post-add mutation -> silent state corruption",
                mutatedPayload,
                emitted.getLong(1));
        assertNotEquals(
                "BUG (U2): the originally-buffered payload was lost",
                originalPayload,
                emitted.getLong(1));
        // RowKind was likewise corrupted via the shared reference.
        assertEquals(
                "BUG (U2): emitted RowKind reflects the post-add mutation",
                RowKind.DELETE,
                emitted.getRowKind());

        testHarness.close();
    }

    /**
     * Controlled baseline: a CopyShield (deep copy before add) would feed an independent instance,
     * so a later upstream mutation cannot reach buffered state. PASSES = copy path is intact.
     */
    @Test
    public void testCopyShieldSafe() throws Exception {
        ProcTimeSortOperator operator = createSortOperator();
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);
        testHarness.open();
        testHarness.setProcessingTime(0L);

        final long originalPayload = 100L;
        final long mutatedPayload = 999L;

        GenericRowData reused = mutableRow(7, originalPayload, "reused", 7);

        // CopyShield: hand the operator an independent snapshot, exactly what copy-on-add yields.
        GenericRowData defensiveCopy = mutableRow(7, originalPayload, "reused", 7);
        testHarness.processElement(new StreamRecord<>(defensiveCopy));

        // Upstream mutates its reused buffer; with a copy in state this must NOT leak through.
        reused.setField(1, mutatedPayload);
        reused.setRowKind(RowKind.DELETE);
        // We do NOT touch defensiveCopy after handing it over, faithfully modelling the shield.

        testHarness.setProcessingTime(1L);

        List<RowData> out = extractOutput(testHarness);
        assertEquals("expected exactly one emitted row", 1, out.size());
        RowData emitted = out.get(0);

        // SAFE: the emitted payload is the ORIGINAL value; the mutation did not reach state.
        assertEquals(
                "BASELINE: copy-shielded payload is the original buffered value",
                originalPayload,
                emitted.getLong(1));
        assertNotEquals(
                "BASELINE: corruption did not leak through the copy",
                mutatedPayload,
                emitted.getLong(1));
        assertEquals("BASELINE: RowKind is intact", RowKind.INSERT, emitted.getRowKind());

        testHarness.close();
    }

    private ProcTimeSortOperator createSortOperator() {
        return new ProcTimeSortOperator(inputRowType, gComparator);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            StreamOperator<RowData> operator) throws Exception {
        OneInputStreamOperatorTestHarness testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        (BaseTemporalSortOperator) operator,
                        EmptyRowDataKeySelector.INSTANCE,
                        EmptyRowDataKeySelector.INSTANCE.getProducedType());
        return testHarness;
    }

    @SuppressWarnings("unchecked")
    private static List<RowData> extractOutput(
            OneInputStreamOperatorTestHarness<RowData, RowData> harness) {
        List<RowData> rows = new ArrayList<>();
        for (Object o : harness.getOutput()) {
            if (o instanceof StreamRecord) {
                rows.add(((StreamRecord<RowData>) o).getValue());
            }
        }
        return rows;
    }
}
