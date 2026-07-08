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

import java.util.concurrent.atomic.AtomicLong;

/**
 * JVM-wide counters that expose <em>why</em> the async backpressure prefetch does or does not pay
 * off. Enabled by {@code state.backend.cachekit.bp-prefetch.diag.enabled=true}; when off every
 * increment is a single predictable branch and the periodic dumper never starts, so it is safe to
 * leave the calls in the hot path.
 *
 * <p>The ratios that matter:
 *
 * <ul>
 *   <li><b>promoted / submitted</b> — the effective prefetch hit rate. Low means the mechanism
 *       rarely helps; high means it helps but the value it saves may be cheap (cache already warm).
 *   <li><b>staleReject / (promoted + staleReject)</b> — how often a staged value was ready but a
 *       write invalidated it first (read-after-write kills prefetch).
 *   <li><b>overflowCleared</b> — staged entries dropped before use because the worker fell behind
 *       or the lookahead ran deeper than the staging cap.
 *   <li><b>staged / submitted</b> — worker throughput: how many submitted keys the single worker
 *       actually managed to fetch.
 * </ul>
 */
public final class PrefetchDiagnostics {

    private static final boolean ENABLED = loadEnabled();

    private static final AtomicLong SUBMITTED = new AtomicLong();
    private static final AtomicLong STAGED = new AtomicLong();
    private static final AtomicLong PROMOTED = new AtomicLong();
    private static final AtomicLong STALE_REJECT = new AtomicLong();
    private static final AtomicLong OVERFLOW_CLEARED = new AtomicLong();
    private static final AtomicLong GEN_ABORT = new AtomicLong();
    private static final AtomicLong ABSENT = new AtomicLong();

    static {
        if (ENABLED) {
            Thread dumper =
                    new Thread(PrefetchDiagnostics::dumpLoop, "cachekit-bp-prefetch-diag");
            dumper.setDaemon(true);
            dumper.start();
        }
    }

    private PrefetchDiagnostics() {}

    private static boolean loadEnabled() {
        try {
            return org.apache.flink.configuration.GlobalConfiguration.loadConfiguration()
                    .get(
                            org.apache.flink.configuration.ConfigOptions.key(
                                            "state.backend.cachekit.bp-prefetch.diag.enabled")
                                    .booleanType()
                                    .defaultValue(false));
        } catch (Throwable t) {
            return false;
        }
    }

    public static boolean enabled() {
        return ENABLED;
    }

    public static void submitted(long n) {
        if (ENABLED) {
            SUBMITTED.addAndGet(n);
        }
    }

    public static void staged(long n) {
        if (ENABLED) {
            STAGED.addAndGet(n);
        }
    }

    public static void promoted() {
        if (ENABLED) {
            PROMOTED.incrementAndGet();
        }
    }

    public static void staleReject() {
        if (ENABLED) {
            STALE_REJECT.incrementAndGet();
        }
    }

    public static void overflowCleared(long n) {
        if (ENABLED) {
            OVERFLOW_CLEARED.addAndGet(n);
        }
    }

    public static void genAbort() {
        if (ENABLED) {
            GEN_ABORT.incrementAndGet();
        }
    }

    public static void absent(long n) {
        if (ENABLED) {
            ABSENT.addAndGet(n);
        }
    }

    private static void dumpLoop() {
        long lastSubmitted = 0;
        while (true) {
            try {
                Thread.sleep(10_000L);
            } catch (InterruptedException e) {
                return;
            }
            long sub = SUBMITTED.get();
            if (sub == lastSubmitted) {
                continue; // nothing happened this window; stay quiet
            }
            lastSubmitted = sub;
            long staged = STAGED.get();
            long promoted = PROMOTED.get();
            long stale = STALE_REJECT.get();
            long overflow = OVERFLOW_CLEARED.get();
            long absent = ABSENT.get();
            long genAbort = GEN_ABORT.get();
            double hit = sub == 0 ? 0.0 : 100.0 * promoted / sub;
            double stageRate = sub == 0 ? 0.0 : 100.0 * staged / sub;
            System.out.printf(
                    "[bp-prefetch-diag] submitted=%d staged=%d(%.1f%%) promoted=%d(hit %.1f%%) "
                            + "staleReject=%d overflowCleared=%d absent=%d genAbort=%d%n",
                    sub, staged, stageRate, promoted, hit, stale, overflow, absent, genAbort);
        }
    }
}
