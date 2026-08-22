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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.configuration.Configuration;

import java.util.Locale;

/**
 * One-time admission policy for the CacheKit Kunpeng chain-copy elision.
 *
 * <p>The global Flink object-reuse switch removes every chained-record copy and therefore also
 * changes the contract for user operators. CacheKit instead admits only exact, audited Table
 * runtime consumers whose synchronous {@code processElement} path does not retain or mutate its
 * input. Unknown operators fail closed to the stock {@link CopyingChainingOutput} path. The check
 * runs once while the operator chain is built; there is no per-record classifier or JNI call.
 */
final class CacheKitArmChainCopyElision {

    static final String ENABLED_KEY = "state.backend.cachekit.arm.chain-copy-elision.enabled";
    static final String AARCH64_ONLY_KEY =
            "state.backend.cachekit.arm.chain-copy-elision.aarch64-only";

    private static final String GENERATED_CALC_PREFIX =
            "org.apache.flink.table.runtime.operators.calc.StreamExecCalc$";
    private static final String WATERMARK_ASSIGNER =
            "org.apache.flink.table.runtime.operators.wmassigners.WatermarkAssignerOperator";
    private static final String SINK_OPERATOR =
            "org.apache.flink.table.runtime.operators.sink.SinkOperator";

    private final boolean configured;
    private final boolean active;
    private final String architecture;

    CacheKitArmChainCopyElision(Configuration configuration) {
        this(
                configuration.getBoolean(ENABLED_KEY, false),
                configuration.getBoolean(AARCH64_ONLY_KEY, true),
                System.getProperty("os.arch", "unknown"));
    }

    CacheKitArmChainCopyElision(boolean configured, boolean aarch64Only, String architecture) {
        this.configured = configured;
        this.architecture = architecture == null ? "unknown" : architecture;
        this.active = configured && (!aarch64Only || isAarch64(this.architecture));
    }

    static CacheKitArmChainCopyElision disabled() {
        return new CacheKitArmChainCopyElision(false, true, "unknown");
    }

    boolean isConfigured() {
        return configured;
    }

    boolean isActive() {
        return active;
    }

    String getArchitecture() {
        return architecture;
    }

    boolean isEligible(Object downstream) {
        return active && downstream != null && isEligibleClassName(downstream.getClass().getName());
    }

    static boolean isEligibleClassName(String className) {
        if (className == null || className.isEmpty()) {
            return false;
        }

        // Janino generates numbered Calc subclasses under the Table-runtime package. Restrict the
        // suffix to decimal digits so a user lookalike is not admitted.
        if (className.startsWith(GENERATED_CALC_PREFIX)) {
            String suffix = className.substring(GENERATED_CALC_PREFIX.length());
            if (!suffix.isEmpty()) {
                boolean decimal = true;
                for (int i = 0; i < suffix.length(); i++) {
                    decimal &= Character.isDigit(suffix.charAt(i));
                }
                if (decimal) {
                    return true;
                }
            }
        }

        return WATERMARK_ASSIGNER.equals(className) || SINK_OPERATOR.equals(className);
    }

    static boolean isAarch64(String architecture) {
        if (architecture == null) {
            return false;
        }
        String normalized = architecture.toLowerCase(Locale.ROOT);
        return "aarch64".equals(normalized) || "arm64".equals(normalized);
    }
}
