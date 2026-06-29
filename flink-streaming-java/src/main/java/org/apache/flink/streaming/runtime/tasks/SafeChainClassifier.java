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

import org.apache.flink.streaming.api.operators.StreamOperator;

/**
 * Per-edge realization of the {@code Safe(O)} mutation-safety predicate for zero-copy operator
 * chaining (the {@code Z_safe} configuration). A chained edge may skip the per-record deep copy
 * only when the <em>consuming</em> operator is provably non-retaining and non-mutating of its
 * input reference; otherwise the edge is given a copying output even under global object-reuse.
 *
 * <p><b>Soundness.</b> The default verdict is {@code copy} (fail-closed): any operator not proven
 * safe — all user code, stateful buffering operators (sort/rank/join/window), and anything else —
 * receives a copying edge. The predicate therefore never authorizes a behaviorally-unsafe edge
 * (no false-SAFE), so {@code Z_safe} is observationally equivalent to the copying baseline {@code B}.
 *
 * <p><b>Tightness.</b> Operators that opt in via {@link NonCopySafe} keep their zero-copy edge, so
 * only the genuinely-unsafe edges pay the copy — {@code Z_safe} preserves the zero-copy fast path
 * everywhere it is safe.
 *
 * <p><b>Scope.</b> This gates the chained-operator output path
 * ({@link OperatorChain#wrapOperatorIntoOutput}). The chained-source and broadcasting paths would
 * need analogous gating for a complete deployment; they are out of scope for this prototype.
 *
 * <p>Enabled via {@code -Dflink.safeChain.enabled=true}. When disabled the behavior is byte-for-byte
 * stock Flink (global object-reuse decides alone), which makes the {@code Z_all} vs {@code Z_safe}
 * comparison a clean apples-to-apples toggle in one build.
 */
public final class SafeChainClassifier {

    /** Whether per-edge Safe(O) gating is active. */
    public static final boolean ENABLED =
            Boolean.parseBoolean(System.getProperty("flink.safeChain.enabled", "false"));

    /** Whether to log each per-edge verdict (for evidence/tightness accounting). */
    private static final boolean LOG =
            Boolean.parseBoolean(System.getProperty("flink.safeChain.log", "false"));

    /**
     * Whether to honor the {@link NonCopySafe} marker as a trusted escape-hatch override when the
     * automatic analyzer does not prove safety. Defaults to {@code true} for backward compatibility
     * with hand-marked operators, but an override is a TRUSTED, UNVERIFIED assertion (it bypasses
     * the sound bytecode proof) and every use is logged. Set {@code -Dflink.safeChain.trustMarker=false}
     * to require an analyzer proof for every edge.
     */
    private static final boolean TRUST_MARKER =
            Boolean.parseBoolean(System.getProperty("flink.safeChain.trustMarker", "true"));

    private SafeChainClassifier() {}

    /**
     * One-input convenience overload: the chained {@code OneInputStreamOperator} edge always targets
     * input ordinal 0.
     *
     * @param consumer the operator that will receive records through the edge under construction
     */
    public static boolean isNonCopySafe(StreamOperator<?> consumer) {
        return isNonCopySafe(consumer, 0);
    }

    /**
     * Returns {@code true} iff skipping the input-record copy for {@code consumer} on its
     * {@code inputIndex}-th input is provably safe.
     *
     * <p><b>Primary decision = {@link Safe0Analyzer}.</b> The edge is authorized zero-copy only if
     * the sound, automatic per-input bytecode analyzer <em>proves</em> the consumer neither mutates
     * its input in place (U1) nor lets it escape past {@code processElement} without a CopyShield
     * (U2). The analyzer derives this from the operator's actual {@code processElement} /
     * {@code processElement1} / {@code processElement2} bytecode — no trusted class names, no
     * required hand marker. Anything it cannot prove is UNSAFE (fail-closed), so this never
     * authorizes a behaviorally-unsafe edge.
     *
     * <p><b>Marker = trusted escape hatch only.</b> The legacy {@link NonCopySafe} marker is OR-ed in
     * (when {@code -Dflink.safeChain.trustMarker=true}, the default) ONLY for operators the analyzer
     * cannot see (e.g. proof failed but the author asserts safety). It is a trusted, unverified
     * override, not the default path, and each override is logged distinctly.
     *
     * @param consumer the operator that will receive records through the edge under construction
     * @param inputIndex the 0-based input ordinal of this edge (0 for one-input; 0/1 for two-input;
     *     the {@code inputId-1} for multi-input)
     */
    public static boolean isNonCopySafe(StreamOperator<?> consumer, int inputIndex) {
        if (!ENABLED) {
            // Stock Flink: global object-reuse alone decides; this predicate is inert.
            return true;
        }
        if (consumer == null) {
            if (LOG) {
                System.err.println(
                        "[SafeChain] edge -> consumer=null input="
                                + inputIndex
                                + " : COPY (fail-closed)");
            }
            return false;
        }

        // PRIMARY: sound + automatic bytecode proof.
        final Safe0Analyzer.Verdict verdict =
                Safe0Analyzer.analyze(consumer.getClass(), inputIndex);
        final boolean provenSafe = verdict == Safe0Analyzer.Verdict.SAFE;

        // ESCAPE HATCH (trusted, logged): marker only matters when the proof did not succeed.
        final boolean markerOverride =
                !provenSafe && TRUST_MARKER && (consumer instanceof NonCopySafe);

        final boolean safe = provenSafe || markerOverride;

        if (LOG) {
            final String name = consumer.getClass().getName();
            final String reason;
            if (provenSafe) {
                reason = "ZERO-COPY (Safe0Analyzer: SAFE)";
            } else if (markerOverride) {
                reason =
                        "ZERO-COPY (TRUSTED OVERRIDE via NonCopySafe marker; analyzer verdict="
                                + verdict
                                + " — unverified escape hatch)";
            } else {
                reason =
                        "COPY (fail-closed; Safe0Analyzer verdict="
                                + verdict
                                + ", no marker override)";
            }
            System.err.println(
                    "[SafeChain] edge -> consumer="
                            + name
                            + " input="
                            + inputIndex
                            + " : "
                            + reason);
        }
        return safe;
    }
}
