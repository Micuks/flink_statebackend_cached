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
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class LinuxThreadAffinityTest {

    @TempDir Path temporaryDirectory;

    @Test
    void parsesCanonicalAndExpandedCpuLists() {
        assertEquals(
                new TreeSet<>(Arrays.asList(2, 3, 4, 7)),
                LinuxThreadAffinity.parseCpuList("2-4,7"));
        assertThrows(
                IllegalArgumentException.class,
                () -> LinuxThreadAffinity.parseCpuList("7-2"));
        assertThrows(
                IllegalArgumentException.class,
                () -> LinuxThreadAffinity.parseCpuList("1; touch /tmp/not-allowed"));
        assertThrows(
                IllegalArgumentException.class,
                () -> LinuxThreadAffinity.parseCpuList("2147483647"));
    }

    @Test
    void findsOnlyAnUnambiguousNamedTid() throws Exception {
        Path first = Files.createDirectories(temporaryDirectory.resolve("101"));
        Path second = Files.createDirectories(temporaryDirectory.resolve("202"));
        Files.write(
                first.resolve("comm"),
                Collections.singletonList("cachekit-bp-pre"),
                StandardCharsets.UTF_8);
        Files.write(
                second.resolve("comm"),
                Collections.singletonList("other"),
                StandardCharsets.UTF_8);

        assertEquals(
                Optional.of("101"),
                LinuxThreadAffinity.findUniqueTid(temporaryDirectory, "cachekit-bp-pre"));

        Files.write(
                second.resolve("comm"),
                Collections.singletonList("cachekit-bp-pre"),
                StandardCharsets.UTF_8);
        assertFalse(
                LinuxThreadAffinity.findUniqueTid(temporaryDirectory, "cachekit-bp-pre")
                        .isPresent());
    }

    @Test
    void bindsAndVerifiesARealNamedWorkerOnLinux() throws Exception {
        assumeTrue(LinuxThreadAffinity.isLinux());
        assumeTrue(Files.isExecutable(Paths.get("/usr/bin/taskset")));
        String allowed =
                LinuxThreadAffinity.readAllowedCpuList(Paths.get("/proc/self/status"));
        Set<Integer> cpus = LinuxThreadAffinity.parseCpuList(allowed);
        assumeTrue(!cpus.isEmpty());
        String cpu = String.valueOf(cpus.iterator().next());
        AtomicReference<LinuxThreadAffinity.BindingResult> result = new AtomicReference<>();

        Thread worker =
                new Thread(
                        () ->
                                result.set(
                                        LinuxThreadAffinity.bindCurrentThread(
                                                PrefetchExecutor.WORKER_NATIVE_THREAD_NAME, cpu)),
                        PrefetchExecutor.WORKER_THREAD_NAME);
        worker.start();
        worker.join(10_000L);

        assertFalse(worker.isAlive());
        assertTrue(result.get().isSuccess(), result.get().getReason());
        assertEquals(cpu, result.get().getObservedCpuList());
    }
}
