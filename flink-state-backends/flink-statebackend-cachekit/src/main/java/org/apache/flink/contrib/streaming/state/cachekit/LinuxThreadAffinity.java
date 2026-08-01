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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.concurrent.TimeUnit;

/** Linux-only affinity helper for CacheKit-selected JVM and native worker TIDs. */
final class LinuxThreadAffinity {

    static final Path PROC_SELF_TASK = Paths.get("/proc/self/task");
    private static final int TID_LOOKUP_ATTEMPTS = 20;
    private static final int MAX_CPU_ID = 1_048_575;

    private LinuxThreadAffinity() {}

    static BindingResult bindCurrentThread(String threadName, String configuredCpuList) {
        String cpuList = configuredCpuList == null ? "" : configuredCpuList.trim();
        if (cpuList.isEmpty()) {
            return BindingResult.disabled();
        }

        final Set<Integer> requested;
        try {
            requested = parseCpuList(cpuList);
        } catch (IllegalArgumentException e) {
            return BindingResult.failure(cpuList, null, "invalid CPU list: " + e.getMessage());
        }
        if (!isLinux()) {
            return BindingResult.failure(cpuList, null, "Linux /proc is unavailable");
        }

        Optional<String> tid = Optional.empty();
        for (int attempt = 0; attempt < TID_LOOKUP_ATTEMPTS && !tid.isPresent(); attempt++) {
            try {
                tid = findUniqueTid(PROC_SELF_TASK, threadName);
            } catch (IOException e) {
                return BindingResult.failure(cpuList, null, "cannot inspect /proc: " + e);
            }
            if (!tid.isPresent()) {
                try {
                    Thread.sleep(10L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return BindingResult.failure(cpuList, null, "interrupted during TID lookup");
                }
            }
        }
        if (!tid.isPresent()) {
            return BindingResult.failure(cpuList, null, "named worker TID not found");
        }

        return bindThread(
                PROC_SELF_TASK,
                new NativeThread(tid.get(), threadName),
                cpuList,
                requested);
    }

    static BindingResult bindThread(
            Path taskDirectory, NativeThread thread, String configuredCpuList) {
        String cpuList = configuredCpuList == null ? "" : configuredCpuList.trim();
        if (cpuList.isEmpty()) {
            return BindingResult.disabled();
        }
        final Set<Integer> requested;
        try {
            requested = parseCpuList(cpuList);
        } catch (IllegalArgumentException e) {
            return BindingResult.failure(
                    cpuList, thread.getTid(), "invalid CPU list: " + e.getMessage());
        }
        return bindThread(taskDirectory, thread, cpuList, requested);
    }

    private static BindingResult bindThread(
            Path taskDirectory,
            NativeThread thread,
            String cpuList,
            Set<Integer> requested) {
        if (!thread.getTid().matches("[0-9]+")) {
            return BindingResult.failure(cpuList, thread.getTid(), "invalid native TID");
        }
        Path task = taskDirectory.resolve(thread.getTid());
        try {
            if (!thread.getName().equals(readThreadName(task.resolve("comm")))) {
                return BindingResult.failure(
                        cpuList, thread.getTid(), "native thread name changed before binding");
            }
        } catch (IOException e) {
            return BindingResult.failure(
                    cpuList, thread.getTid(), "cannot verify native thread before binding: " + e);
        }

        Process process = null;
        try {
            process =
                    new ProcessBuilder("taskset", "-pc", cpuList, thread.getTid())
                            .redirectErrorStream(true)
                            .start();
            if (!process.waitFor(5L, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                return BindingResult.failure(cpuList, thread.getTid(), "taskset timed out");
            }
            String output;
            try (BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(
                                    process.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder builder = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    if (builder.length() > 0) {
                        builder.append(' ');
                    }
                    builder.append(line);
                }
                output = builder.toString();
            }
            if (process.exitValue() != 0) {
                return BindingResult.failure(
                        cpuList,
                        thread.getTid(),
                        "taskset exit=" + process.exitValue() + " output=" + output);
            }

            if (!thread.getName().equals(readThreadName(task.resolve("comm")))) {
                return BindingResult.failure(
                        cpuList, thread.getTid(), "native thread name changed during binding");
            }
            String observed = readAllowedCpuList(task.resolve("status"));
            if (!requested.equals(parseCpuList(observed))) {
                return BindingResult.failure(
                        cpuList,
                        thread.getTid(),
                        "affinity verification mismatch: observed=" + observed);
            }
            return BindingResult.success(cpuList, thread.getTid(), observed);
        } catch (IOException e) {
            return BindingResult.failure(
                    cpuList, thread.getTid(), "cannot bind or verify native thread: " + e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return BindingResult.failure(
                    cpuList, thread.getTid(), "interrupted while waiting for taskset");
        } finally {
            if (process != null && process.isAlive()) {
                process.destroyForcibly();
            }
        }
    }

    static boolean isLinux() {
        return Files.isDirectory(PROC_SELF_TASK);
    }

    static Optional<String> findUniqueTid(Path taskDirectory, String threadName) throws IOException {
        List<NativeThread> matches =
                findThreads(taskDirectory, observed -> threadName.equals(observed));
        return matches.size() == 1
                ? Optional.of(matches.get(0).getTid())
                : Optional.empty();
    }

    static String readThreadName(Path comm) throws IOException {
        return new String(Files.readAllBytes(comm), StandardCharsets.UTF_8).trim();
    }

    static List<NativeThread> findThreads(Path taskDirectory, Predicate<String> nameFilter)
            throws IOException {
        List<NativeThread> matches = new ArrayList<>();
        try (DirectoryStream<Path> tasks = Files.newDirectoryStream(taskDirectory)) {
            for (Path task : tasks) {
                String tid = task.getFileName().toString();
                if (!tid.matches("[0-9]+")) {
                    continue;
                }
                Path comm = task.resolve("comm");
                if (!Files.isRegularFile(comm)) {
                    continue;
                }
                String observed = readThreadName(comm);
                if (nameFilter.test(observed)) {
                    matches.add(new NativeThread(tid, observed));
                }
            }
        }
        matches.sort(Comparator.comparingLong(thread -> Long.parseLong(thread.getTid())));
        return matches;
    }

    static String readAllowedCpuList(Path status) throws IOException {
        for (String line : Files.readAllLines(status, StandardCharsets.UTF_8)) {
            if (line.startsWith("Cpus_allowed_list:")) {
                String value = line.substring("Cpus_allowed_list:".length()).trim();
                if (!value.isEmpty()) {
                    return value;
                }
            }
        }
        throw new IOException("Cpus_allowed_list missing from " + status);
    }

    static Set<Integer> parseCpuList(String value) {
        if (value == null || value.isEmpty() || !value.matches("[0-9,-]+")) {
            throw new IllegalArgumentException(String.valueOf(value));
        }
        Set<Integer> cpus = new TreeSet<>();
        for (String item : value.split(",")) {
            if (item.isEmpty()) {
                throw new IllegalArgumentException(value);
            }
            String[] bounds = item.split("-", -1);
            if (bounds.length == 1) {
                cpus.add(parseCpu(bounds[0]));
            } else if (bounds.length == 2) {
                int first = parseCpu(bounds[0]);
                int last = parseCpu(bounds[1]);
                if (last < first) {
                    throw new IllegalArgumentException(item);
                }
                for (int cpu = first; ; cpu++) {
                    cpus.add(cpu);
                    if (cpu == last) {
                        break;
                    }
                }
            } else {
                throw new IllegalArgumentException(item);
            }
        }
        return Collections.unmodifiableSet(cpus);
    }

    private static int parseCpu(String value) {
        try {
            int cpu = Integer.parseInt(value);
            if (cpu < 0 || cpu > MAX_CPU_ID) {
                throw new IllegalArgumentException(value);
            }
            return cpu;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(value, e);
        }
    }

    static final class BindingResult {
        private final boolean enabled;
        private final boolean success;
        private final String requestedCpuList;
        private final String tid;
        private final String observedCpuList;
        private final String reason;

        private BindingResult(
                boolean enabled,
                boolean success,
                String requestedCpuList,
                String tid,
                String observedCpuList,
                String reason) {
            this.enabled = enabled;
            this.success = success;
            this.requestedCpuList = requestedCpuList;
            this.tid = tid;
            this.observedCpuList = observedCpuList;
            this.reason = reason;
        }

        static BindingResult disabled() {
            return new BindingResult(false, false, "", null, null, null);
        }

        static BindingResult success(String requestedCpuList, String tid, String observedCpuList) {
            return new BindingResult(
                    true, true, requestedCpuList, tid, observedCpuList, null);
        }

        static BindingResult failure(String requestedCpuList, String tid, String reason) {
            return new BindingResult(true, false, requestedCpuList, tid, null, reason);
        }

        boolean isEnabled() {
            return enabled;
        }

        boolean isSuccess() {
            return success;
        }

        String getRequestedCpuList() {
            return requestedCpuList;
        }

        String getTid() {
            return tid;
        }

        String getObservedCpuList() {
            return observedCpuList;
        }

        String getReason() {
            return reason;
        }
    }

    static final class NativeThread {
        private final String tid;
        private final String name;

        NativeThread(String tid, String name) {
            this.tid = tid;
            this.name = name;
        }

        String getTid() {
            return tid;
        }

        String getName() {
            return name;
        }

        String key() {
            return tid + ":" + name;
        }
    }
}
