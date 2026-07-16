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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/** Target-host screening benchmark; Nexmark remains the performance authority. */
public final class KunpengStagingBenchmark {

    private static final int TRANSFER_ENTRIES =
            Integer.getInteger("cachekit.staging.bench.transfer-entries", 1_000_000);
    private static final int MATERIALIZATION_VALUES =
            Integer.getInteger("cachekit.staging.bench.materialization-values", 200_000);
    private static final int ROUNDS = Integer.getInteger("cachekit.staging.bench.rounds", 5);
    private static final int RING_CAPACITY = 8192;
    private static final byte[][] PAYLOADS = makePayloads();
    private static volatile long sink;

    private KunpengStagingBenchmark() {}

    public static void main(String[] args) throws Exception {
        if (TRANSFER_ENTRIES < 10_000 || MATERIALIZATION_VALUES < 10_000 || ROUNDS < 1) {
            throw new IllegalArgumentException("benchmark sizes are too small");
        }
        System.out.println("kind,param,leg,operations,ns_per_op,checksum");
        benchmarkTransfer();
        for (int hitPercent : new int[] {0, 25, 50, 75, 100}) {
            benchmarkMaterialization(hitPercent);
        }
    }

    private static void benchmarkTransfer() throws Exception {
        TransferKey[] keys = new TransferKey[TRANSFER_ENTRIES];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = new TransferKey(i);
        }
        runConcurrentMapTransfer(keys);
        runSpscTransfer(keys);

        List<Double> baseline = new ArrayList<>();
        List<Double> candidate = new ArrayList<>();
        for (int round = 1; round <= ROUNDS; round++) {
            baseline.add(reportTransfer("chm-a-r" + round, keys, false));
            candidate.add(reportTransfer("spsc-a-r" + round, keys, true));
            candidate.add(reportTransfer("spsc-b-r" + round, keys, true));
            baseline.add(reportTransfer("chm-b-r" + round, keys, false));
        }
        reportSummary("transfer", RING_CAPACITY, median(baseline), median(candidate));
    }

    private static double reportTransfer(String leg, TransferKey[] keys, boolean useSpsc)
            throws Exception {
        TimedResult result = useSpsc ? runSpscTransfer(keys) : runConcurrentMapTransfer(keys);
        double nsPerOp = (double) result.elapsedNanos / keys.length;
        System.out.printf(
                "transfer,%d,%s,%d,%.3f,%d%n",
                RING_CAPACITY, leg, keys.length, nsPerOp, result.checksum);
        return nsPerOp;
    }

    private static TimedResult runConcurrentMapTransfer(TransferKey[] keys) throws Exception {
        ConcurrentHashMap<TransferKey, TransferValue> staging =
                new ConcurrentHashMap<>(RING_CAPACITY);
        return runTransfer(
                keys,
                sequence -> staging.put(keys[sequence], new TransferValue(sequence)),
                sequence -> staging.remove(keys[sequence]));
    }

    private static TimedResult runSpscTransfer(TransferKey[] keys) throws Exception {
        ArmSpscStagingBuffer<TransferValue> staging =
                new ArmSpscStagingBuffer<>(RING_CAPACITY);
        return runTransfer(
                keys,
                sequence -> {
                    TransferValue value = new TransferValue(sequence);
                    while (!staging.offer(value)) {
                        Thread.yield();
                    }
                },
                ignored -> staging.poll());
    }

    private static TimedResult runTransfer(
            TransferKey[] keys, Producer producer, Consumer consumer) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        long[] checksum = new long[1];
        Thread producerThread =
                new Thread(
                        () -> {
                            try {
                                start.await();
                                for (int i = 0; i < keys.length; i++) {
                                    producer.publish(i);
                                }
                            } catch (Throwable t) {
                                failure.compareAndSet(null, t);
                            }
                        },
                        "staging-bench-producer");
        Thread consumerThread =
                new Thread(
                        () -> {
                            try {
                                start.await();
                                long localChecksum = 0;
                                for (int expected = 0; expected < keys.length; ) {
                                    TransferValue value = consumer.consume(expected);
                                    if (value == null) {
                                        Thread.yield();
                                    } else if (value.sequence != expected) {
                                        throw new AssertionError(
                                                "expected "
                                                        + expected
                                                        + " but received "
                                                        + value.sequence);
                                    } else {
                                        localChecksum += value.sequence;
                                        expected++;
                                    }
                                }
                                checksum[0] = localChecksum;
                            } catch (Throwable t) {
                                failure.compareAndSet(null, t);
                            }
                        },
                        "staging-bench-consumer");

        producerThread.start();
        consumerThread.start();
        long startNanos = System.nanoTime();
        start.countDown();
        producerThread.join();
        consumerThread.join();
        long elapsedNanos = System.nanoTime() - startNanos;
        if (failure.get() != null) {
            throw new AssertionError("transfer benchmark failed", failure.get());
        }
        sink = checksum[0];
        return new TimedResult(elapsedNanos, checksum[0]);
    }

    private static void benchmarkMaterialization(int hitPercent) {
        runEagerMaterialization(hitPercent);
        runLazyMaterialization(hitPercent);

        List<Double> baseline = new ArrayList<>();
        List<Double> candidate = new ArrayList<>();
        for (int round = 1; round <= ROUNDS; round++) {
            baseline.add(reportMaterialization(hitPercent, "eager-a-r" + round, false));
            candidate.add(reportMaterialization(hitPercent, "lazy-a-r" + round, true));
            candidate.add(reportMaterialization(hitPercent, "lazy-b-r" + round, true));
            baseline.add(reportMaterialization(hitPercent, "eager-b-r" + round, false));
        }
        reportSummary("materialization", hitPercent, median(baseline), median(candidate));
    }

    private static double reportMaterialization(int hitPercent, String leg, boolean lazy) {
        TimedResult result =
                lazy
                        ? runLazyMaterialization(hitPercent)
                        : runEagerMaterialization(hitPercent);
        double nsPerValue = (double) result.elapsedNanos / MATERIALIZATION_VALUES;
        System.out.printf(
                "materialization,%d,%s,%d,%.3f,%d%n",
                hitPercent, leg, MATERIALIZATION_VALUES, nsPerValue, result.checksum);
        return nsPerValue;
    }

    private static TimedResult runEagerMaterialization(int hitPercent) {
        long startNanos = System.nanoTime();
        DecodedValue[] staged = new DecodedValue[MATERIALIZATION_VALUES];
        for (int i = 0; i < staged.length; i++) {
            staged[i] = decode(PAYLOADS[i & (PAYLOADS.length - 1)]);
        }
        int hits = staged.length * hitPercent / 100;
        long checksum = 0;
        for (int i = 0; i < hits; i++) {
            checksum += staged[i].checksum();
        }
        long elapsedNanos = System.nanoTime() - startNanos;
        sink = checksum;
        return new TimedResult(elapsedNanos, checksum);
    }

    private static TimedResult runLazyMaterialization(int hitPercent) {
        long startNanos = System.nanoTime();
        byte[][] staged = new byte[MATERIALIZATION_VALUES][];
        for (int i = 0; i < staged.length; i++) {
            staged[i] = PAYLOADS[i & (PAYLOADS.length - 1)];
        }
        int hits = staged.length * hitPercent / 100;
        long checksum = 0;
        for (int i = 0; i < hits; i++) {
            checksum += decode(staged[i]).checksum();
        }
        long elapsedNanos = System.nanoTime() - startNanos;
        sink = checksum;
        return new TimedResult(elapsedNanos, checksum);
    }

    private static DecodedValue decode(byte[] bytes) {
        long a = 0;
        long b = 0;
        long c = 0;
        long d = 0;
        for (int i = 0; i < bytes.length; i += 8) {
            long value =
                    ((long) bytes[i] & 0xff)
                            | (((long) bytes[i + 1] & 0xff) << 8)
                            | (((long) bytes[i + 2] & 0xff) << 16)
                            | (((long) bytes[i + 3] & 0xff) << 24)
                            | (((long) bytes[i + 4] & 0xff) << 32)
                            | (((long) bytes[i + 5] & 0xff) << 40)
                            | (((long) bytes[i + 6] & 0xff) << 48)
                            | (((long) bytes[i + 7] & 0xff) << 56);
            switch ((i >>> 3) & 3) {
                case 0:
                    a ^= value;
                    break;
                case 1:
                    b += value;
                    break;
                case 2:
                    c ^= Long.rotateLeft(value, i & 63);
                    break;
                default:
                    d += Long.rotateRight(value, i & 63);
                    break;
            }
        }
        return new DecodedValue(a, b, c, d);
    }

    private static byte[][] makePayloads() {
        byte[][] payloads = new byte[4096][128];
        for (int i = 0; i < payloads.length; i++) {
            for (int j = 0; j < payloads[i].length; j++) {
                payloads[i][j] = (byte) (i * 31 + j * 17);
            }
        }
        return payloads;
    }

    private static void reportSummary(
            String kind, int parameter, double baselineMedian, double candidateMedian) {
        System.out.printf(
                "SUMMARY,%s,param=%d,baseline_median_ns=%.3f,candidate_median_ns=%.3f,uplift_pct=%.3f%n",
                kind,
                parameter,
                baselineMedian,
                candidateMedian,
                (baselineMedian / candidateMedian - 1.0) * 100.0);
    }

    private static double median(List<Double> samples) {
        ArrayList<Double> sorted = new ArrayList<>(samples);
        Collections.sort(sorted);
        int middle = sorted.size() / 2;
        return sorted.size() % 2 == 0
                ? (sorted.get(middle - 1) + sorted.get(middle)) / 2.0
                : sorted.get(middle);
    }

    private interface Producer {
        void publish(int sequence);
    }

    private interface Consumer {
        TransferValue consume(int expectedSequence);
    }

    private static final class TransferKey {
        private final int sequence;

        private TransferKey(int sequence) {
            this.sequence = sequence;
        }

        @Override
        public int hashCode() {
            return sequence;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof TransferKey
                    && sequence == ((TransferKey) other).sequence;
        }
    }

    private static final class TransferValue {
        private final int sequence;

        private TransferValue(int sequence) {
            this.sequence = sequence;
        }
    }

    private static final class DecodedValue {
        private final long a;
        private final long b;
        private final long c;
        private final long d;

        private DecodedValue(long a, long b, long c, long d) {
            this.a = a;
            this.b = b;
            this.c = c;
            this.d = d;
        }

        private long checksum() {
            return a ^ b ^ c ^ d;
        }
    }

    private static final class TimedResult {
        private final long elapsedNanos;
        private final long checksum;

        private TimedResult(long elapsedNanos, long checksum) {
            this.elapsedNanos = elapsedNanos;
            this.checksum = checksum;
        }
    }
}
