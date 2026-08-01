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

package org.apache.flink.contrib.streaming.state.cachekit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class RocksDBBackgroundThreadAffinityTest {

    @TempDir Path temporaryDirectory;

    @Test
    void recognizesOnlyRocksDbEnvBackgroundPoolNames() {
        for (String name :
                Arrays.asList(
                        "rocksdb:low0",
                        "rocksdb:low12",
                        "rocksdb:high1",
                        "rocksdb:bottom9",
                        "rocksdb:user3")) {
            assertTrue(RocksDBBackgroundThreadAffinity.isRocksDbBackgroundThreadName(name));
        }
        for (String name :
                Arrays.asList(
                        "rocksdb:listener",
                        "rocksdb:low",
                        "rocksdb:lowx",
                        "rocksdbx:low0",
                        "cachekit-bp-pre",
                        "Source Data Fetcher")) {
            assertFalse(RocksDBBackgroundThreadAffinity.isRocksDbBackgroundThreadName(name));
        }
    }

    @Test
    void discoversKnownPoolsWithoutCapturingForeignThreads() throws Exception {
        writeComm("301", "rocksdb:high2");
        writeComm("102", "Source Data Fet");
        writeComm("203", "rocksdb:listener");
        writeComm("101", "rocksdb:low0");
        writeComm("not-a-tid", "rocksdb:low9");

        List<LinuxThreadAffinity.NativeThread> threads =
                RocksDBBackgroundThreadAffinity.discover(temporaryDirectory);

        assertEquals(
                Arrays.asList("101:rocksdb:low0", "301:rocksdb:high2"),
                threads.stream()
                        .map(thread -> thread.getTid() + ":" + thread.getName())
                        .collect(Collectors.toList()));
    }

    @Test
    void bindsARealKnownPoolThreadButLeavesForeignThreadUntouched() throws Exception {
        assumeTrue(LinuxThreadAffinity.isLinux());
        assumeTrue(Files.isExecutable(Paths.get("/usr/bin/taskset")));
        Set<Integer> allowed =
                LinuxThreadAffinity.parseCpuList(
                        LinuxThreadAffinity.readAllowedCpuList(
                                Paths.get("/proc/self/status")));
        assumeTrue(!allowed.isEmpty());
        String requestedCpu = String.valueOf(allowed.iterator().next());
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        Thread target = newWaitingThread("rocksdb:low7", started, release);
        Thread foreign = newWaitingThread("rocksdb:other", started, release);
        target.start();
        foreign.start();

        try {
            assertTrue(started.await(5L, TimeUnit.SECONDS));
            String targetTid =
                    LinuxThreadAffinity.findUniqueTid(
                                    LinuxThreadAffinity.PROC_SELF_TASK, "rocksdb:low7")
                            .orElseThrow(AssertionError::new);
            String foreignTid =
                    LinuxThreadAffinity.findUniqueTid(
                                    LinuxThreadAffinity.PROC_SELF_TASK, "rocksdb:other")
                            .orElseThrow(AssertionError::new);
            String foreignBefore =
                    LinuxThreadAffinity.readAllowedCpuList(
                            LinuxThreadAffinity.PROC_SELF_TASK
                                    .resolve(foreignTid)
                                    .resolve("status"));

            List<RocksDBBackgroundThreadAffinity.BindingAttempt> attempts =
                    RocksDBBackgroundThreadAffinity.bindDiscoveredOnce(
                            LinuxThreadAffinity.PROC_SELF_TASK,
                            requestedCpu,
                            new HashSet<>());

            assertEquals(1, attempts.size());
            assertEquals(targetTid, attempts.get(0).getThread().getTid());
            assertTrue(
                    attempts.get(0).getResult().isSuccess(),
                    attempts.get(0).getResult().getReason());
            assertEquals(
                    requestedCpu,
                    LinuxThreadAffinity.readAllowedCpuList(
                            LinuxThreadAffinity.PROC_SELF_TASK
                                    .resolve(targetTid)
                                    .resolve("status")));
            assertEquals(
                    foreignBefore,
                    LinuxThreadAffinity.readAllowedCpuList(
                            LinuxThreadAffinity.PROC_SELF_TASK
                                    .resolve(foreignTid)
                                    .resolve("status")));
        } finally {
            release.countDown();
            target.join(5_000L);
            foreign.join(5_000L);
        }
        assertFalse(target.isAlive());
        assertFalse(foreign.isAlive());
    }

    private static Thread newWaitingThread(
            String name, CountDownLatch started, CountDownLatch release) {
        return new Thread(
                () -> {
                    started.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                name);
    }

    private void writeComm(String tid, String name) throws Exception {
        Path directory = Files.createDirectories(temporaryDirectory.resolve(tid));
        Files.write(
                directory.resolve("comm"),
                Arrays.asList(name),
                StandardCharsets.UTF_8);
    }
}
