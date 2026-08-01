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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/** Pins explicitly named RocksDB Env background pools without capturing foreign threads. */
final class RocksDBBackgroundThreadAffinity {

    private static final Logger LOG =
            LoggerFactory.getLogger(RocksDBBackgroundThreadAffinity.class);

    private static final Pattern BACKGROUND_THREAD_NAME =
            Pattern.compile("rocksdb:(?:low|high|bottom|user)[0-9]+");
    private static final long SCAN_INTERVAL_MILLIS = 1_000L;
    private static final AtomicBoolean STARTED = new AtomicBoolean();

    static final String WATCHER_THREAD_NAME = "cachekit-rdb-affinity";
    static final String WATCHER_NATIVE_THREAD_NAME = "cachekit-rdb-af";

    private RocksDBBackgroundThreadAffinity() {}

    static boolean isRocksDbBackgroundThreadName(String name) {
        return name != null && BACKGROUND_THREAD_NAME.matcher(name).matches();
    }

    static List<LinuxThreadAffinity.NativeThread> discover(Path taskDirectory)
            throws IOException {
        return LinuxThreadAffinity.findThreads(
                taskDirectory, RocksDBBackgroundThreadAffinity::isRocksDbBackgroundThreadName);
    }

    static void start(String configuredCpuList) {
        String cpuList = configuredCpuList == null ? "" : configuredCpuList.trim();
        if (cpuList.isEmpty()) {
            return;
        }
        try {
            LinuxThreadAffinity.parseCpuList(cpuList);
        } catch (IllegalArgumentException e) {
            LOG.error(
                    "CACHEKIT_ROCKSDB_BG_AFFINITY status=failed requested={} reason={}",
                    cpuList,
                    "invalid CPU list: " + e.getMessage());
            return;
        }
        if (!STARTED.compareAndSet(false, true)) {
            return;
        }
        Thread watcher =
                new Thread(
                        () -> watch(LinuxThreadAffinity.PROC_SELF_TASK, cpuList),
                        WATCHER_THREAD_NAME);
        watcher.setDaemon(true);
        watcher.start();
    }

    static List<BindingAttempt> bindDiscoveredOnce(
            Path taskDirectory, String cpuList, Set<String> successfullyBound)
            throws IOException {
        List<LinuxThreadAffinity.NativeThread> threads = discover(taskDirectory);
        Set<String> current = new HashSet<>();
        for (LinuxThreadAffinity.NativeThread thread : threads) {
            current.add(thread.key());
        }
        successfullyBound.retainAll(current);

        List<BindingAttempt> attempts = new ArrayList<>();
        for (LinuxThreadAffinity.NativeThread thread : threads) {
            if (successfullyBound.contains(thread.key())) {
                continue;
            }
            LinuxThreadAffinity.BindingResult result =
                    LinuxThreadAffinity.bindThread(taskDirectory, thread, cpuList);
            attempts.add(new BindingAttempt(thread, result));
            if (result.isSuccess()) {
                successfullyBound.add(thread.key());
            }
        }
        return attempts;
    }

    private static void watch(Path taskDirectory, String cpuList) {
        Set<String> successfullyBound = new HashSet<>();
        Set<String> loggedFailures = new HashSet<>();
        try {
            String watcherTid =
                    LinuxThreadAffinity.findUniqueTid(taskDirectory, WATCHER_NATIVE_THREAD_NAME)
                            .orElse("unknown");
            LOG.info(
                    "CACHEKIT_ROCKSDB_BG_AFFINITY status=watcher-started watcher_tid={} requested={}",
                    watcherTid,
                    cpuList);
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    for (BindingAttempt attempt :
                            bindDiscoveredOnce(taskDirectory, cpuList, successfullyBound)) {
                        LinuxThreadAffinity.NativeThread thread = attempt.getThread();
                        LinuxThreadAffinity.BindingResult result = attempt.getResult();
                        if (result.isSuccess()) {
                            LOG.info(
                                    "CACHEKIT_ROCKSDB_BG_AFFINITY status=bound tid={} name={} "
                                            + "requested={} observed={}",
                                    thread.getTid(),
                                    thread.getName(),
                                    result.getRequestedCpuList(),
                                    result.getObservedCpuList());
                        } else {
                            String failure = thread.key() + ":" + result.getReason();
                            if (loggedFailures.add(failure)) {
                                LOG.error(
                                        "CACHEKIT_ROCKSDB_BG_AFFINITY status=failed tid={} name={} "
                                                + "requested={} reason={}",
                                        thread.getTid(),
                                        thread.getName(),
                                        result.getRequestedCpuList(),
                                        result.getReason());
                            }
                        }
                    }
                } catch (IOException e) {
                    String failure = "scan:" + e;
                    if (loggedFailures.add(failure)) {
                        LOG.error(
                                "CACHEKIT_ROCKSDB_BG_AFFINITY status=scan-failed "
                                        + "requested={} reason={}",
                                cpuList,
                                e.toString());
                    }
                }
                Thread.sleep(SCAN_INTERVAL_MILLIS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            LOG.error(
                    "CACHEKIT_ROCKSDB_BG_AFFINITY status=scan-failed requested={} reason={}",
                    cpuList,
                    e.toString());
        }
    }

    static final class BindingAttempt {
        private final LinuxThreadAffinity.NativeThread thread;
        private final LinuxThreadAffinity.BindingResult result;

        BindingAttempt(
                LinuxThreadAffinity.NativeThread thread,
                LinuxThreadAffinity.BindingResult result) {
            this.thread = thread;
            this.result = result;
        }

        LinuxThreadAffinity.NativeThread getThread() {
            return thread;
        }

        LinuxThreadAffinity.BindingResult getResult() {
            return result;
        }
    }
}
