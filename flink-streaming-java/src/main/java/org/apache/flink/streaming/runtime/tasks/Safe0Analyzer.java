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

import org.apache.flink.shaded.asm9.org.objectweb.asm.ClassReader;
import org.apache.flink.shaded.asm9.org.objectweb.asm.Opcodes;
import org.apache.flink.shaded.asm9.org.objectweb.asm.Type;
import org.apache.flink.shaded.asm9.org.objectweb.asm.tree.AbstractInsnNode;
import org.apache.flink.shaded.asm9.org.objectweb.asm.tree.ClassNode;
import org.apache.flink.shaded.asm9.org.objectweb.asm.tree.FieldInsnNode;
import org.apache.flink.shaded.asm9.org.objectweb.asm.tree.MethodInsnNode;
import org.apache.flink.shaded.asm9.org.objectweb.asm.tree.MethodNode;
import org.apache.flink.shaded.asm9.org.objectweb.asm.tree.analysis.Analyzer;
import org.apache.flink.shaded.asm9.org.objectweb.asm.tree.analysis.AnalyzerException;
import org.apache.flink.shaded.asm9.org.objectweb.asm.tree.analysis.Frame;
import org.apache.flink.shaded.asm9.org.objectweb.asm.tree.analysis.Interpreter;
import org.apache.flink.shaded.asm9.org.objectweb.asm.tree.analysis.Value;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Safe0Analyzer — a <em>sound</em> and <em>automatic</em> per-input mutation-safety analyzer
 * realizing the paper {@code Safe(O, input_i)} predicate via conservative abstract interpretation
 * over JVM bytecode (shaded ASM). It replaces both the unsound class-name whitelist
 * ({@code MutationSafetyClassifier.SAFE_OMNI_OVERRIDES}) and the not-automatic hand-applied
 * {@link NonCopySafe} marker.
 *
 * <p><b>What it proves.</b> For a chained consumer operator class and a target input ordinal, the
 * analyzer returns {@link Verdict#SAFE} only if it can prove that, on every reachable path of the
 * relevant {@code processElement}, the incoming record reference (and all aliases / sub-fields /
 * backing segments) is <em>never</em> written in place (U1) and <em>never</em> escapes the
 * synchronous {@code processElement} scope without first passing through a CopyShield (U2).
 * Forwarding to the chained successor via {@code Output.collect} is, by the forward-is-not-escape
 * lemma, NOT an escape (R5). Anything the analyzer cannot model routes to {@link Verdict#UNSAFE}
 * (fail-closed, R6). Any exception (missing bytecode, ASM failure) also yields UNSAFE.
 *
 * <p><b>Soundness boundary (TCB).</b> Intra-procedural plus one level of same-class private-helper
 * recursion; the CopyShield list is trusted to be genuinely deep-copying; ASM models JVM opcode
 * semantics. See the class-level rule comments (R1..R6) for the mapping to the paper rules.
 *
 * <p>Result is {@link Verdict#UNDECIDABLE} only for the narrow case where the operator does not
 * implement the expected {@code processElement} signature for the requested input ordinal at all
 * (e.g. asking input-2 of a one-input operator); callers MUST treat UNDECIDABLE as not-safe.
 */
public final class Safe0Analyzer {

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------

    /** Verdict of the analyzer for one (operator class, input ordinal) pair. */
    public enum Verdict {
        /** Proven read-only-and-non-retaining: zero-copy edge is behaviorally == copying baseline. */
        SAFE,
        /** Not proven safe (a rule fired, or analysis could not model something): must copy. */
        UNSAFE,
        /** The requested input ordinal does not exist on this operator: must copy. */
        UNDECIDABLE
    }

    /** Per-operator-class analysis result: verdict per input ordinal (0,1,2,...). */
    public static final class Result {
        private final Map<Integer, Verdict> perInput;

        Result(Map<Integer, Verdict> perInput) {
            this.perInput = perInput;
        }

        /** @return true iff input {@code inputIndex} is provably SAFE for zero-copy. */
        public boolean isSafe(int inputIndex) {
            return perInput.getOrDefault(inputIndex, Verdict.UNSAFE) == Verdict.SAFE;
        }

        public Verdict verdict(int inputIndex) {
            return perInput.getOrDefault(inputIndex, Verdict.UNDECIDABLE);
        }

        @Override
        public String toString() {
            return perInput.toString();
        }
    }

    private static final boolean DEBUG =
            Boolean.parseBoolean(System.getProperty("flink.safe0.debug", "false"));

    /** One ASM pass per operator class per JVM. */
    private static final ConcurrentHashMap<String, Result> CACHE = new ConcurrentHashMap<>();

    private Safe0Analyzer() {}

    /**
     * Analyze {@code opClass} for all of its input ordinals. Cached. Never throws: any failure to
     * read or model the bytecode is captured as UNSAFE for the affected method.
     */
    public static Result analyze(Class<?> opClass) {
        if (opClass == null) {
            return new Result(java.util.Collections.singletonMap(0, Verdict.UNSAFE));
        }
        return CACHE.computeIfAbsent(opClass.getName(), n -> analyzeUncached(opClass));
    }

    /** Convenience: verdict for a single (class, input ordinal). */
    public static Verdict analyze(Class<?> opClass, int inputIndex) {
        return analyze(opClass).verdict(inputIndex);
    }

    // ------------------------------------------------------------------------
    // Core analysis
    // ------------------------------------------------------------------------

    private static Result analyzeUncached(Class<?> opClass) {
        Map<Integer, Verdict> verdicts = new HashMap<>();
        try {
            ClassNode cn = loadClassNode(opClass);
            if (cn == null) {
                // No bytecode available -> fail closed for every plausible input.
                verdicts.put(0, Verdict.UNSAFE);
                return new Result(verdicts);
            }

            // Build the (methodName -> MethodNode) index for this class, used by R6 1-level recursion.
            Map<String, MethodNode> sameClassMethods = new HashMap<>();
            for (MethodNode mn : cn.methods) {
                sameClassMethods.put(mn.name + mn.desc, mn);
            }

            // R1 / PERINPUT: pick the processElement method(s) the operator actually overrides.
            // OneInput / Input: processElement(StreamRecord)  -> input 0
            // TwoInput:         processElement1(StreamRecord) -> input 0
            //                   processElement2(StreamRecord) -> input 1
            // (MultipleInput Input.processElement lives on a separate inner class; for a chained
            //  edge the consumer object is the operator itself, so we analyze the signatures it owns.)
            MethodNode pe = find(cn, "processElement", DESC_PROCESS_ELEMENT);
            MethodNode pe1 = find(cn, "processElement1", DESC_PROCESS_ELEMENT);
            MethodNode pe2 = find(cn, "processElement2", DESC_PROCESS_ELEMENT);

            // HOLE-A FIX (round-5): both processElement (one-input real entry) and processElement1
            // (two-input first entry) feed INPUT 0. The old code did two unconditional
            // verdicts.put(0, ...) calls, so whichever ran LAST overwrote the other — a SAFE decoy
            // processElement1 could mask a real UNSAFE processElement (or vice versa). We now COMBINE
            // every entry method that can feed an input ordinal under UNSAFE-WINS (lattice meet toward
            // UNSAFE): input i is SAFE only if EVERY contributing method that EXISTS is SAFE; if any is
            // UNSAFE/UNDECIDABLE, input i is UNSAFE. No later put can ever downgrade an UNSAFE to SAFE.
            boolean any = false;
            if (pe != null) {
                combineVerdict(verdicts, 0, classifyMethod(cn, pe, sameClassMethods, opClass));
                any = true;
            }
            if (pe1 != null) {
                combineVerdict(verdicts, 0, classifyMethod(cn, pe1, sameClassMethods, opClass));
                any = true;
            }
            if (pe2 != null) {
                combineVerdict(verdicts, 1, classifyMethod(cn, pe2, sameClassMethods, opClass));
                any = true;
            }

            if (!any) {
                // Operator overrides none of the expected entry points in its own bytecode. It may
                // inherit one from a superclass we did not walk -> fail closed, NOT undecidable.
                verdicts.put(0, Verdict.UNSAFE);
            }
        } catch (Throwable t) {
            if (DEBUG) {
                System.err.println("[Safe0] analyze threw for " + opClass.getName() + ": " + t);
            }
            // Fail-closed totality.
            verdicts.clear();
            verdicts.put(0, Verdict.UNSAFE);
            verdicts.put(1, Verdict.UNSAFE);
        }
        if (DEBUG) {
            System.err.println("[Safe0] " + opClass.getName() + " -> " + verdicts);
        }
        return new Result(verdicts);
    }

    /**
     * HOLE-A combine: merge a newly classified verdict into the per-input map under UNSAFE-WINS.
     * The meet toward UNSAFE is: UNSAFE/UNDECIDABLE absorb everything; SAFE only survives if the
     * existing entry is absent or already SAFE. This guarantees that no contributing entry method can
     * downgrade a more-unsafe verdict already recorded for the same input ordinal (a SAFE decoy can
     * never mask a real UNSAFE). UNDECIDABLE is treated as not-safe and folded to UNSAFE on conflict.
     */
    private static void combineVerdict(Map<Integer, Verdict> verdicts, int input, Verdict v) {
        Verdict prev = verdicts.get(input);
        verdicts.put(input, meetTowardUnsafe(prev, v));
    }

    private static Verdict meetTowardUnsafe(Verdict a, Verdict b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        // SAFE is the TOP of the safety lattice; anything not-SAFE pulls the meet down. If either is
        // UNSAFE -> UNSAFE. If either is UNDECIDABLE (and neither UNSAFE) -> UNSAFE (callers must
        // treat UNDECIDABLE as not-safe; folding to UNSAFE here keeps a single not-safe sink).
        if (a == Verdict.UNSAFE || b == Verdict.UNSAFE) {
            return Verdict.UNSAFE;
        }
        if (a == Verdict.UNDECIDABLE || b == Verdict.UNDECIDABLE) {
            return Verdict.UNSAFE;
        }
        return Verdict.SAFE; // both SAFE
    }

    /** {@code (Lorg/apache/flink/streaming/runtime/streamrecord/StreamRecord;)V} */
    private static final String DESC_PROCESS_ELEMENT =
            "(Lorg/apache/flink/streaming/runtime/streamrecord/StreamRecord;)V";

    private static MethodNode find(ClassNode cn, String name, String desc) {
        for (MethodNode mn : cn.methods) {
            if (mn.name.equals(name) && mn.desc.equals(desc)) {
                return mn;
            }
        }
        return null;
    }

    private static ClassNode loadClassNode(Class<?> opClass) throws IOException {
        String resource = opClass.getName().replace('.', '/') + ".class";
        ClassLoader cl = opClass.getClassLoader();
        InputStream in =
                (cl != null)
                        ? cl.getResourceAsStream(resource)
                        : ClassLoader.getSystemResourceAsStream(resource);
        if (in == null) {
            in = opClass.getResourceAsStream("/" + resource);
        }
        if (in == null) {
            return null;
        }
        try {
            ClassReader cr = new ClassReader(in);
            ClassNode cn = new ClassNode(Opcodes.ASM9);
            cr.accept(cn, ClassReader.SKIP_FRAMES | ClassReader.SKIP_DEBUG);
            return cn;
        } finally {
            in.close();
        }
    }

    /**
     * Load a {@link ClassNode} by JVM internal name (slashes), resolving its bytecode through the
     * operator's own classloader (so user classes on the job classloader are found, the same way the
     * leaf operator class was loaded). Returns {@code null} if the bytecode cannot be read — every
     * caller treats {@code null} as fail-closed (NOT trusted).
     */
    private static ClassNode loadClassNodeByInternalName(String internalName, ClassLoader cl) {
        if (internalName == null) {
            return null;
        }
        String resource = internalName + ".class";
        InputStream in = null;
        try {
            if (cl != null) {
                in = cl.getResourceAsStream(resource);
            }
            if (in == null) {
                in = ClassLoader.getSystemResourceAsStream(resource);
            }
            if (in == null) {
                in = Safe0Analyzer.class.getResourceAsStream("/" + resource);
            }
            if (in == null) {
                return null;
            }
            ClassReader cr = new ClassReader(in);
            ClassNode cn = new ClassNode(Opcodes.ASM9);
            cr.accept(cn, ClassReader.SKIP_FRAMES | ClassReader.SKIP_DEBUG);
            return cn;
        } catch (Throwable t) {
            return null;
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ignored) {
                    // best effort
                }
            }
        }
    }

    /** Internal names of the two Flink runtime operator bases that own the genuine {@code output}. */
    private static final String ABSTRACT_STREAM_OPERATOR =
            "org/apache/flink/streaming/api/operators/AbstractStreamOperator";

    private static final String ABSTRACT_STREAM_OPERATOR_V2 =
            "org/apache/flink/streaming/api/operators/AbstractStreamOperatorV2";

    /** The genuine erased descriptor of the inherited {@code output} field. */
    private static final String GENUINE_OUTPUT_DESC =
            "Lorg/apache/flink/streaming/api/operators/Output;";

    /**
     * ROUND-9 UNFORGEABLE inherited-output resolver. Walks the superclass chain of {@code leafCn}
     * (leaf upward, via {@link #loadClassNodeByInternalName}) and finds the FIRST class that declares
     * a field named {@code output}. The inherited-output promotion is granted (returns {@code true})
     * ONLY IF that first declaring class is EXACTLY {@link #ABSTRACT_STREAM_OPERATOR} (or
     * {@link #ABSTRACT_STREAM_OPERATOR_V2}) AND the declared field's descriptor is the genuine
     * {@link #GENUINE_OUTPUT_DESC}.
     *
     * <p>Fail-closed (returns {@code false}) when: an intermediate USER class shadows {@code output};
     * no class in the chain declares {@code output}; any superclass bytecode cannot be resolved
     * before the genuine base is reached (we cannot prove no shadowing exists); or the genuine base's
     * {@code output} type is not the genuine Output. Bounded by a defensive depth cap; cycles cannot
     * occur in a well-formed class hierarchy but the cap also guards a corrupt one.
     *
     * <p>Unforgeable: a user cannot add an {@code output} field to Flink's
     * {@code AbstractStreamOperator} (it is part of the build, not the job jar), and we resolve the
     * real declaration site rather than trusting a name + a leaf-only field-set proxy. The Object
     * root terminates the walk with no {@code output} found -> not trusted.
     */
    private static boolean resolvesToGenuineInheritedOutput(ClassNode leafCn, Class<?> opClass) {
        if (leafCn == null) {
            return false;
        }
        ClassLoader cl = (opClass != null) ? opClass.getClassLoader() : null;
        ClassNode cur = leafCn;
        int guard = 0;
        while (cur != null && guard++ < 64) {
            if (cur.fields != null) {
                for (org.apache.flink.shaded.asm9.org.objectweb.asm.tree.FieldNode fn : cur.fields) {
                    if ("output".equals(fn.name)) {
                        // FIRST declaration of 'output' from the leaf upward. Trusted IFF it is the
                        // genuine Flink runtime base AND the genuine Output type. ANY other declaring
                        // class (a user class shadowing 'output') -> NOT trusted (fail-closed). We
                        // STOP at the first declarer: a deeper genuine base is irrelevant because the
                        // SHADOWING field is what GETFIELD off 'this' resolves to.
                        boolean genuineBase =
                                ABSTRACT_STREAM_OPERATOR.equals(cur.name)
                                        || ABSTRACT_STREAM_OPERATOR_V2.equals(cur.name);
                        return genuineBase && GENUINE_OUTPUT_DESC.equals(fn.desc);
                    }
                }
            }
            String superName = cur.superName;
            if (superName == null || "java/lang/Object".equals(superName)) {
                // Reached the root without finding an 'output' declaration -> not the inherited
                // runtime sink -> not trusted.
                return false;
            }
            // Continue the walk into the superclass. We resolve its bytecode (the genuine
            // AbstractStreamOperator IS on the build classpath, so it loads and its declared 'output'
            // is inspected by the next iteration). If a superclass between the leaf and the genuine
            // base cannot be resolved, we CANNOT prove that this unresolved intermediate USER class
            // does not shadow 'output' -> fail-closed.
            ClassNode parent = loadClassNodeByInternalName(superName, cl);
            if (parent == null) {
                return false;
            }
            cur = parent;
        }
        return false;
    }

    /**
     * Run the taint interpreter over one processElement method to fixpoint and read the verdict.
     * The seed is installed by {@link TaintInterpreter#newParameterValue}: the lone StreamRecord
     * parameter local is born TAINTED_RECORD. Any rule firing flips a shared verdict flag to UNSAFE.
     */
    private static Verdict classifyMethod(
            ClassNode cn,
            MethodNode mn,
            Map<String, MethodNode> sameClassMethods,
            Class<?> opClass) {
        try {
            // HOLE-B: collect the set of field names the operator class declares ITSELF, so the
            // interpreter can distinguish a USER-declared `output`/`collector` field (untrusted) from
            // the INHERITED AbstractStreamOperator `output` field (the runtime forward sink). javac
            // emits the Fieldref owner as the accessing subclass even for inherited fields, so owner
            // name alone cannot tell them apart; the declared-field set is the discriminator.
            java.util.Set<String> ownDeclaredFields = new java.util.HashSet<>();
            if (cn.fields != null) {
                for (org.apache.flink.shaded.asm9.org.objectweb.asm.tree.FieldNode fn : cn.fields) {
                    ownDeclaredFields.add(fn.name);
                }
            }
            // ROUND-9 HOLE FIX (UNFORGEABLE inherited-output provenance): the leaf-only
            // !ownDeclaredFields.contains("output") proxy is FORGEABLE. A user abstract superclass
            //   class EvilBase extends AbstractStreamOperator { protected Output output = retainer; }
            // with leaf EvilOp extends EvilBase (leaf does NOT declare 'output') passed that proxy:
            // the leaf field set has no 'output', so the inherited-output promotion fired and a
            // this.output.collect(record) on the SHADOWING user field was trusted -> false SAFE.
            //
            // We now resolve the TRUE declaration site of the 'output' field by WALKING THE
            // SUPERCLASS CHAIN of the operator class (loading each superclass's bytecode and
            // inspecting its declared fields). The inherited-output promotion is granted ONLY IF the
            // FIRST class in the chain (from the leaf upward) that declares a field named 'output' is
            // EXACTLY org/apache/flink/streaming/api/operators/AbstractStreamOperator (or
            // ...AbstractStreamOperatorV2) AND that field's descriptor is the genuine Output type.
            // If ANY intermediate USER class declares (shadows) 'output', or the declaring class /
            // any superclass bytecode cannot be resolved, or the field type is not the genuine
            // Output, the flag is false -> the promotion fails closed. This is unforgeable: a user
            // cannot add a field to Flink's sealed-by-build AbstractStreamOperator, and we resolve
            // the real declaration site instead of trusting a name + a leaf-only proxy.
            boolean genuineInheritedOutput = resolvesToGenuineInheritedOutput(cn, opClass);
            TaintInterpreter interp =
                    new TaintInterpreter(
                            cn.name,
                            sameClassMethods,
                            /* recursionBudget= */ 1,
                            ownDeclaredFields,
                            genuineInheritedOutput);
            Analyzer<TaintValue> analyzer = new Analyzer<>(interp);
            analyzer.analyze(cn.name, mn);
            return interp.unsafe ? Verdict.UNSAFE : Verdict.SAFE;
        } catch (AnalyzerException | RuntimeException | Error e) {
            if (DEBUG) {
                System.err.println(
                        "[Safe0] classify " + cn.name + "#" + mn.name + " threw: " + e + " -> UNSAFE");
            }
            // Any opcode/flow the interpreter cannot model -> fail closed (R6).
            return Verdict.UNSAFE;
        }
    }

    // ------------------------------------------------------------------------
    // Abstract domain
    // ------------------------------------------------------------------------

    /** Lattice tags (Section 1). */
    enum Tag {
        BOTTOM, // unreached
        CLEAN, // provably unrelated / fresh / primitive
        CLEAN_COPY, // provably a CopyShield result; escaping it is safe
        THIS_REF, // the receiver 'this' (local 0 of an instance method) — used for field provenance
        TRUSTED_OUTPUT, // the operator's runtime-bound output sink (inherited 'output' field or a
        // runtime-supplied Output/Collector parameter / known wrapper): collect() on it is a
        // legitimate chain forward (HOLE-2 provenance)
        TAINTED_RECORD, // the StreamRecord arg itself (R5-risk mitigation: non-getValue access fails closed)
        TAINTED_INPUT, // is, or aliases, the extracted input record ref
        TAINTED_FIELD, // a sub-value loaded out of the input (conservative aliasing)
        TAINTED_SEGMENT, // a MemorySegment / backing array reachable from the input
        TOP // unknown
    }

    /** An abstract value: a {@link Tag} plus a JVM size (1 or 2) so ASM frame bookkeeping is valid. */
    static final class TaintValue implements Value {
        final Tag tag;
        final int size;

        TaintValue(Tag tag, int size) {
            this.tag = tag;
            this.size = size;
        }

        @Override
        public int getSize() {
            return size;
        }

        boolean tainted() {
            return tag == Tag.TAINTED_RECORD
                    || tag == Tag.TAINTED_INPUT
                    || tag == Tag.TAINTED_FIELD
                    || tag == Tag.TAINTED_SEGMENT;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TaintValue)) {
                return false;
            }
            TaintValue tv = (TaintValue) o;
            return tag == tv.tag && size == tv.size;
        }

        @Override
        public int hashCode() {
            return tag.hashCode() * 31 + size;
        }

        @Override
        public String toString() {
            return tag + "/" + size;
        }
    }

    // Canonical size-1 / size-2 singletons for the common tags.
    private static final TaintValue V_BOTTOM = new TaintValue(Tag.BOTTOM, 1);
    private static final TaintValue V_CLEAN_1 = new TaintValue(Tag.CLEAN, 1);
    private static final TaintValue V_CLEAN_2 = new TaintValue(Tag.CLEAN, 2);
    private static final TaintValue V_TOP_1 = new TaintValue(Tag.TOP, 1);

    // ------------------------------------------------------------------------
    // The interpreter implementing R1..R6
    // ------------------------------------------------------------------------

    static class TaintInterpreter extends Interpreter<TaintValue> implements Opcodes {

        /** Set to true by any R3/R4/R6 firing; verdict = SAFE iff this stays false. */
        boolean unsafe = false;

        private final String ownerInternalName;
        private final Map<String, MethodNode> sameClassMethods;
        private final int recursionBudget;
        /** HOLE-B: field names the operator class declares itself (vs. inherited). */
        final java.util.Set<String> ownDeclaredFields;
        /**
         * ROUND-9: true IFF the superclass-chain walk proved the operator's {@code output} field is
         * declared FIRST (leaf upward) by the genuine Flink {@code AbstractStreamOperator(V2)} with
         * the genuine Output type — i.e. the inherited runtime sink, not a user shadow. This is the
         * UNFORGEABLE replacement for the old leaf-only {@code !ownDeclaredFields.contains("output")}
         * proxy.
         */
        final boolean genuineInheritedOutput;

        TaintInterpreter(
                String ownerInternalName,
                Map<String, MethodNode> sameClassMethods,
                int recursionBudget,
                java.util.Set<String> ownDeclaredFields,
                boolean genuineInheritedOutput) {
            super(ASM9);
            this.ownerInternalName = ownerInternalName;
            this.sameClassMethods = sameClassMethods;
            this.recursionBudget = recursionBudget;
            this.ownDeclaredFields =
                    ownDeclaredFields == null ? java.util.Collections.emptySet() : ownDeclaredFields;
            this.genuineInheritedOutput = genuineInheritedOutput;
        }

        // ---- value factory ----

        @Override
        public TaintValue newValue(Type type) {
            if (type == null) {
                return V_BOTTOM; // uninitialized slot
            }
            if (type == Type.VOID_TYPE) {
                return null; // no value produced
            }
            return cleanOf(type);
        }

        /**
         * R1 / RISK-5: seed the StreamRecord parameter local as TAINTED_RECORD. The Analyzer calls
         * this for each parameter (and 'this' at index 0 for instance methods).
         */
        @Override
        public TaintValue newParameterValue(boolean isInstanceMethod, int local, Type type) {
            // HOLE-2 provenance: 'this' (local 0 of an instance method) is THIS_REF so that a
            // GETFIELD of the inherited 'output' off 'this' can be recognized as the trusted sink.
            if (isInstanceMethod && local == 0) {
                return new TaintValue(Tag.THIS_REF, 1);
            }
            if (type != null
                    && type.getSort() == Type.OBJECT
                    && STREAM_RECORD.equals(type.getInternalName())) {
                return new TaintValue(Tag.TAINTED_RECORD, 1);
            }
            // A runtime-supplied Output/Collector parameter (e.g. MultipleInput's Input.processElement
            // receives the chain collector): trusted forward sink. A user cannot inject an arbitrary
            // collector into the fixed processElement signature.
            //
            // ROUND-9 TRUST AUDIT: tightened from isTrustedOutputType(type) (which also accepted the
            // NON-FINAL TCB concrete forwarder types) to the GENUINE Output/Collector INTERFACES by
            // EXACT internal name only. The provenance here is that the RUNTIME supplies the value
            // for this parameter slot (the user does not construct it), but the only unforgeable
            // anchor is the declared interface type — a NON-FINAL concrete type as the param type
            // would be an impersonable proxy (a user could pass a retaining subclass at that static
            // type), so it is dropped. The standard processElement(StreamRecord) signature has no
            // such parameter, so this is a precision path for non-standard runtime-collector
            // signatures and never under-fires the corpus/keystone.
            if (type != null
                    && type.getSort() == Type.OBJECT
                    && (type.getInternalName().equals("org/apache/flink/util/Collector")
                            || type.getInternalName()
                                    .equals("org/apache/flink/streaming/api/operators/Output"))) {
                return new TaintValue(Tag.TRUSTED_OUTPUT, 1);
            }
            return newValue(type);
        }

        @Override
        public TaintValue newReturnTypeValue(Type type) {
            if (type == null || type == Type.VOID_TYPE) {
                return null;
            }
            return cleanOf(type);
        }

        @Override
        public TaintValue newEmptyValue(int local) {
            return V_BOTTOM;
        }

        private static TaintValue cleanOf(Type type) {
            return type.getSize() == 2 ? V_CLEAN_2 : V_CLEAN_1;
        }

        // ---- R2: copy/move within frame ----

        @Override
        public TaintValue copyOperation(AbstractInsnNode insn, TaintValue value) {
            // ALOAD/ASTORE/DUP/SWAP etc.: tag follows the moved cell verbatim.
            return value;
        }

        // ---- newOperation: constants, NEW, GETSTATIC ----

        @Override
        public TaintValue newOperation(AbstractInsnNode insn) {
            int op = insn.getOpcode();
            switch (op) {
                case LCONST_0:
                case LCONST_1:
                case DCONST_0:
                case DCONST_1:
                    return V_CLEAN_2;
                case LDC:
                    {
                        // long/double constants are size 2.
                        org.apache.flink.shaded.asm9.org.objectweb.asm.tree.LdcInsnNode ldc =
                                (org.apache.flink.shaded.asm9.org.objectweb.asm.tree.LdcInsnNode) insn;
                        Object c = ldc.cst;
                        if (c instanceof Long || c instanceof Double) {
                            return V_CLEAN_2;
                        }
                        return V_CLEAN_1;
                    }
                case NEW:
                    // A freshly allocated object is CLEAN (its <init> is handled at naryOperation;
                    // if a tainted arg is captured there, the result is conservatively handled).
                    return V_CLEAN_1;
                case GETSTATIC:
                    {
                        Type t = Type.getType(((FieldInsnNode) insn).desc);
                        return cleanOf(t);
                    }
                default:
                    return V_CLEAN_1; // *CONST_*, BIPUSH, SIPUSH, ACONST_NULL, NEWARRAY-target etc.
            }
        }

        // ---- unaryOperation: GETFIELD, CHECKCAST, casts, ARRAYLENGTH, conversions, return-ish ----

        @Override
        public TaintValue unaryOperation(AbstractInsnNode insn, TaintValue value) {
            int op = insn.getOpcode();
            switch (op) {
                case CHECKCAST:
                    // identity cast: preserve taint (R2).
                    return value;
                case GETFIELD:
                    {
                        FieldInsnNode f = (FieldInsnNode) insn;
                        Type ft = Type.getType(f.desc);
                        // HOLE-B FIX (round-5): promote a GETFIELD to TRUSTED_OUTPUT ONLY when its
                        // declaring OWNER is the inherited runtime output, not merely when the field
                        // NAME is 'output'/'collector'. The old test (receiver THIS_REF + trusted
                        // field type + name in {output,collector}) was unsound: a USER operator that
                        // declares its OWN field `Collector collector` (or `Output output`) and
                        // populates it with a RETAINING collector would have that GETFIELD promoted to
                        // TRUSTED_OUTPUT off `this`, and a subsequent collect() on it would be falsely
                        // authorized as a chain forward -> false SAFE.
                        //
                        // We now require the GETFIELD's DECLARING OWNER (f.owner) to be the inherited
                        // operator output channel: the AbstractStreamOperator(V2).output field of type
                        // Output. A field whose declaring owner is the user's own operator class (or
                        // anything not on the inherited/runtime allowlist) does NOT get TRUSTED_OUTPUT,
                        // so a collect() on it is an untrusted sink and fails closed.
                        //
                        // Note: receiver provenance (THIS_REF) is still required — the field must be
                        // read off `this`. There are exactly two sound promotion paths:
                        //   (P1) the INHERITED AbstractStreamOperator(V2) `output` field: name=='output'
                        //        AND the operator class does NOT declare it itself (so it is the
                        //        inherited runtime sink) AND its static type is a trusted Output. A
                        //        USER field literally named `output`/`collector` of interface type
                        //        Output/Collector is therefore NOT promoted (it IS in
                        //        ownDeclaredFields) -> a collect on it later is an untrusted sink.
                        //
                        // ROUND-8 FINDING #2 FIX (provenance-only forward trust): the former (P2)
                        // path — promote a field whose STATIC TYPE is a TCB concrete forwarder
                        // (TimestampedCollector / CountingOutput / ...) to TRUSTED_OUTPUT — was
                        // UNSOUND. These concrete forwarders are PUBLIC NON-FINAL with NON-FINAL
                        // collect(); a user can declare
                        //     class RetainingCountingOutput extends CountingOutput { collect(r){leak=r;} }
                        // hold it in a field of static type CountingOutput, and the virtual dispatch
                        // goes to the overriding retainer -> the input escapes -> false SAFE. Type
                        // identity of a non-final class is NOT provenance, so (P2) is DROPPED. The
                        // ONLY remaining promotion is (P1): the INHERITED AbstractStreamOperator(V2)
                        // `output` field read off `this` (the operator's own runtime-bound sink). A
                        // collect() on any field whose receiver is not established as TRUSTED_OUTPUT
                        // by this provenance falls through to the fail-closed escape rules.
                        //
                        // ROUND-9 HOLE FIX (UNFORGEABLE declaring-class resolution): the leaf-only
                        // !ownDeclaredFields.contains("output") proxy was FORGEABLE by a user
                        // abstract superclass that declares (shadows) its OWN `output` field while the
                        // LEAF does not declare `output`. The leaf field set then has no `output`, the
                        // old proxy passed, and a this.output.collect(record) on the SHADOWING user
                        // field (a RetainingOutput) was trusted -> false SAFE. We now gate the
                        // promotion on {@code genuineInheritedOutput}: the superclass-chain walk
                        // (resolvesToGenuineInheritedOutput) proved the FIRST class declaring `output`
                        // from the leaf upward is EXACTLY Flink's AbstractStreamOperator(V2) with the
                        // genuine Output type. We additionally require the GETFIELD's STATIC field type
                        // to be the GENUINE Output interface by EXACT internal name — a TCB concrete
                        // forwarder field type (CountingOutput/...) is NOT accepted here (those are
                        // non-final and a user could declare such a field), so the inherited-output
                        // promotion rests solely on unforgeable hierarchy ground-truth.
                        boolean inheritedOutput =
                                value != null
                                        && value.tag == Tag.THIS_REF
                                        && f.name.equals("output")
                                        && genuineInheritedOutput
                                        && !ownDeclaredFields.contains(f.name)
                                        && ft.getSort() == Type.OBJECT
                                        && ft.getInternalName()
                                                .equals(
                                                        "org/apache/flink/streaming/api/operators/Output");
                        if (inheritedOutput) {
                            return new TaintValue(Tag.TRUSTED_OUTPUT, 1);
                        }
                        if (value != null && value.tainted()) {
                            // R2: sub-object of the input may share backing -> TAINTED_FIELD,
                            // or TAINTED_SEGMENT if the field type is a MemorySegment/array.
                            if (isSegmentType(ft)) {
                                return new TaintValue(Tag.TAINTED_SEGMENT, ft.getSize());
                            }
                            if (ft.getSort() == Type.OBJECT || ft.getSort() == Type.ARRAY) {
                                return new TaintValue(Tag.TAINTED_FIELD, 1);
                            }
                            // primitive field read: cannot alias -> CLEAN.
                            return cleanOf(ft);
                        }
                        return cleanOf(ft);
                    }
                case ARRAYLENGTH:
                    return V_CLEAN_1; // primitive int
                case INSTANCEOF:
                    return V_CLEAN_1;
                case NEWARRAY:
                case ANEWARRAY:
                    return V_CLEAN_1; // fresh array, length arg is irrelevant to taint
                case PUTSTATIC:
                    {
                        // R4: escape into a static field.
                        if (value != null && value.tainted()) {
                            fire("PUTSTATIC escape of tainted input");
                        }
                        return null;
                    }
                case ARETURN:
                    {
                        // R6: escape via return.
                        if (value != null && value.tainted()) {
                            fire("ARETURN escape of tainted input");
                        }
                        return null;
                    }
                case ATHROW:
                    {
                        // SOUNDNESS FIX (reviewer R2-#3): a tainted object thrown can be caught and
                        // re-stored (`try { throw input; } catch (T t){ this.f = t; }`). ASM seeds the
                        // catch-handler stack value from the catch TYPE (born CLEAN here), laundering
                        // the taint. We cannot taint the handler frame through this Interpreter hook,
                        // so we fail closed at the throw site: throwing a tainted value is treated as
                        // a potential escape into a handler we cannot track.
                        if (value != null && value.tainted()) {
                            fire("ATHROW of tainted input (may be caught and re-stored); fail-closed");
                        }
                        return null;
                    }
                case IRETURN:
                case LRETURN:
                case FRETURN:
                case DRETURN:
                    return null; // primitive return: never an escape
                default:
                    {
                        // numeric conversions (I2L, L2I, ...), INEG, etc.: result is primitive CLEAN.
                        // Default size from the produced type when known; conversions are size-defined
                        // by opcode, but CLEAN_1/CLEAN_2 only matters for long/double producers.
                        switch (op) {
                            case I2L:
                            case F2L:
                            case D2L:
                            case I2D:
                            case F2D:
                            case L2D:
                                return V_CLEAN_2;
                            default:
                                return V_CLEAN_1;
                        }
                    }
            }
        }

        // ---- binaryOperation: array load, PUTFIELD, arithmetic, comparisons ----

        @Override
        public TaintValue binaryOperation(AbstractInsnNode insn, TaintValue v1, TaintValue v2) {
            int op = insn.getOpcode();
            switch (op) {
                case PUTFIELD:
                    {
                        // stack: ..., objectref(v1), value(v2)
                        // R4: storing a tainted value into a heap object field is an escape.
                        if (v2 != null && v2.tainted()) {
                            fire("PUTFIELD escape of tainted input into heap field");
                        }
                        // R3 (in-place write channel): writing a field of ANY object reachable from
                        // the input (TAINTED_INPUT, a sub-object TAINTED_FIELD, a TAINTED_RECORD
                        // wrapper, or a TAINTED_SEGMENT) mutates input-reachable memory in place = U1.
                        // SOUNDNESS FIX (reviewer R1-#5): previously only TAINTED_INPUT/SEGMENT fired,
                        // so `input.child.count = 7` (receiver tagged TAINTED_FIELD from a prior
                        // GETFIELD) silently passed. Any tainted receiver of a PUTFIELD now fires.
                        if (v1 != null && v1.tainted()) {
                            fire("PUTFIELD in-place write into tainted (input-reachable) object");
                        }
                        return null;
                    }
                case AALOAD:
                    {
                        // reading an element of a tainted array -> still tainted (R2).
                        if (v1 != null && v1.tainted()) {
                            return new TaintValue(Tag.TAINTED_FIELD, 1);
                        }
                        return V_TOP_1; // unknown array element reference; conservative but not tainted
                    }
                case BALOAD:
                case CALOAD:
                case SALOAD:
                case IALOAD:
                case FALOAD:
                    return V_CLEAN_1; // primitive element
                case LALOAD:
                case DALOAD:
                    return V_CLEAN_2;
                default:
                    {
                        // arithmetic / comparisons -> primitive CLEAN.
                        switch (op) {
                            case LADD:
                            case LSUB:
                            case LMUL:
                            case LDIV:
                            case LREM:
                            case LAND:
                            case LOR:
                            case LXOR:
                            case LSHL:
                            case LSHR:
                            case LUSHR:
                            case DADD:
                            case DSUB:
                            case DMUL:
                            case DDIV:
                            case DREM:
                                return V_CLEAN_2;
                            default:
                                return V_CLEAN_1;
                        }
                    }
            }
        }

        // ---- ternaryOperation: array stores ----

        @Override
        public TaintValue ternaryOperation(
                AbstractInsnNode insn, TaintValue v1, TaintValue v2, TaintValue v3) {
            int op = insn.getOpcode();
            // stack: ..., arrayref(v1), index(v2), value(v3)
            if (op == AASTORE) {
                // R3/R4: storing a tainted ref into an array. If the array is reachable from input
                // (tainted) it is an in-place mutation of input backing (R3). If it is some other
                // array, storing a tainted ref makes that array a retaining container -> escape (R4),
                // unless the array was freshly allocated and stays local; we cannot prove locality
                // here, so fail closed.
                if (v3 != null && v3.tainted()) {
                    fire("AASTORE: tainted ref stored into an array (escape/mutation)");
                }
                if (v1 != null && v1.tainted()) {
                    // mutating an array reachable from the input == U1.
                    fire("AASTORE: write into tainted (input-reachable) array");
                }
            }
            // BASTORE/IASTORE/... into a tainted array = writing input backing in place (U1).
            if ((op == BASTORE
                            || op == CASTORE
                            || op == SASTORE
                            || op == IASTORE
                            || op == FASTORE
                            || op == LASTORE
                            || op == DASTORE)
                    && v1 != null
                    && v1.tainted()) {
                fire("primitive ASTORE into tainted (input backing) array");
            }
            return null;
        }

        // ---- naryOperation: method invocations + INVOKEDYNAMIC (the heart of R3/R4/R5/R6) ----

        @Override
        public TaintValue naryOperation(AbstractInsnNode insn, List<? extends TaintValue> args) {
            int op = insn.getOpcode();

            if (op == INVOKEDYNAMIC) {
                // R6: lambda capture / indy. If it captures a tainted value, fail closed.
                for (TaintValue a : args) {
                    if (a != null && a.tainted()) {
                        fire("INVOKEDYNAMIC captures tainted input (lambda escape)");
                        break;
                    }
                }
                org.apache.flink.shaded.asm9.org.objectweb.asm.tree.InvokeDynamicInsnNode idy =
                        (org.apache.flink.shaded.asm9.org.objectweb.asm.tree.InvokeDynamicInsnNode)
                                insn;
                Type ret = Type.getReturnType(idy.desc);
                return ret == Type.VOID_TYPE ? null : cleanOf(ret);
            }

            MethodInsnNode m = (MethodInsnNode) insn;
            String owner = m.owner;
            String name = m.name;
            String desc = m.desc;
            Type ret = Type.getReturnType(desc);

            // For instance calls, args[0] is the receiver.
            boolean hasReceiver =
                    op == INVOKEVIRTUAL || op == INVOKEINTERFACE || op == INVOKESPECIAL;
            TaintValue receiver = hasReceiver && !args.isEmpty() ? args.get(0) : null;
            int firstRealArg = hasReceiver ? 1 : 0;

            // ---------- R1 SEED: StreamRecord.getValue() / getField accessor ----------
            if (STREAM_RECORD.equals(owner)
                    && (name.equals("getValue") || name.equals("getField"))) {
                // The extracted record value is the per-input seed.
                return new TaintValue(Tag.TAINTED_INPUT, 1);
            }
            // StreamRecord.getTimestamp/hasTimestamp/asRecord etc. on the tainted record:
            // timestamp is primitive (no alias); allow.
            if (STREAM_RECORD.equals(owner)
                    && (name.equals("getTimestamp")
                            || name.equals("hasTimestamp")
                            || name.equals("getKey"))) {
                return ret == Type.VOID_TYPE ? null : cleanOf(ret);
            }
            // StreamRecord.replace(x): mutates the RECEIVER record in place so that its payload field
            // points at x (StreamRecord.value = x), and returns the receiver.
            // SOUNDNESS FIX (reviewer R2-#1): if x is the tainted input, `replace` stores a BARE
            // input reference into the receiver StreamRecord object. When that receiver is a SEPARATE
            // operator-field record (e.g. a reused `new StreamRecord<>(null)` held in a field), the
            // bare input escapes past processElement inside that field object — a U2 violation that
            // the old code missed (it returned TAINTED_RECORD without firing). We cannot, intra-
            // procedurally and without alias analysis, prove the receiver is the transient param
            // record being immediately forwarded, so we FAIL CLOSED whenever a tainted value is
            // passed to replace(). (No stock SAFE operator relies on replace(tainted); LimitOperator
            // forwards the original param record and ReusingOperator wraps a CLEAN array.)
            if (STREAM_RECORD.equals(owner) && name.equals("replace")) {
                // SOUNDNESS FIX (reviewer R4-#1): replace(x) ALWAYS mutates the receiver record in
                // place (receiver.value = x). If the RECEIVER is the tainted INPUT wrapper, that is a
                // U1 in-place write of the input StreamRecord — even when x is fresh/clean — and the
                // mutated input is then typically forwarded via output.collect(record). The old code
                // only fired on a tainted *argument*, so `record.replace(new MyValue())` on the input
                // record slipped through as SAFE. We now fail closed whenever the receiver is tainted,
                // REGARDLESS of arg taint. The legitimate reuse pattern `reuse.replace(value)` uses a
                // CLEAN operator-OWN field receiver (read off `this`, not the input), so it does not
                // fire here.
                if (receiver != null && receiver.tainted()) {
                    fire("StreamRecord.replace on a TAINTED receiver (input wrapper): in-place U1 "
                            + "write of the input record; fail-closed");
                }
                // Keep the existing tainted-arg fire too: a bare input ref stored into ANY (possibly
                // escaping) StreamRecord receiver is also a violation.
                for (int i = firstRealArg; i < args.size(); i++) {
                    TaintValue a = args.get(i);
                    if (a != null && a.tainted()) {
                        fire("StreamRecord.replace(tainted): bare input stored into a (possibly "
                                + "escaping) StreamRecord receiver; fail-closed");
                        break;
                    }
                }
                // Result aliases the receiver record; keep its taint so any further escape also fails
                // closed. A clean receiver stays clean; a tainted receiver stays TAINTED_RECORD.
                if (receiver != null && receiver.tainted()) {
                    return new TaintValue(Tag.TAINTED_RECORD, 1);
                }
                return cleanOf(ret);
            }

            // ---------- R5 FORWARD-IS-NOT-ESCAPE: Output/Collector.collect ----------
            if (isCollectSink(op, owner, name, desc)) {
                // HOLE-2 FIX (round-3 CLEAN review): a collect() is a legitimate chain forward ONLY
                // when its RECEIVER is the operator's own runtime-bound output sink — provenance, not
                // just the declared interface type. The REAL Flink ListCollector (list.add(record))
                // also implements org.apache.flink.util.Collector and is dispatched via the same
                // INVOKEINTERFACE on the Collector interface owner, so owner+name recognition alone
                // cannot tell a user-held retaining collector apart from the chaining output. We now
                // require:
                //   (a) the receiver value is TRUSTED_OUTPUT (the inherited 'output' field read off
                //       'this', or a runtime-supplied Output/Collector parameter).
                //
                // ROUND-8 FINDING #2 FIX (provenance-only forward trust): the former alternative
                // "(b) the static dispatch owner is a TCB concrete forwarder (CountingOutput /
                // TimestampedCollector / ...)" was UNSOUND. Those forwarders are PUBLIC NON-FINAL
                // with NON-FINAL collect(); a `RetainingCountingOutput extends CountingOutput`
                // held at static type CountingOutput is virtually-dispatched to the overriding
                // retainer -> input escapes -> false SAFE. Type identity of a non-final class is
                // NOT provenance, so the dispatch-owner trust is DROPPED. A collect() is a trusted
                // forward ONLY when its RECEIVER is TRUSTED_OUTPUT by provenance. A collect() on ANY
                // OTHER receiver (a field of concrete forwarder static type, an arbitrary
                // Collector/Output-typed field, a local collector, etc.) is NOT a trusted forward:
                // its tainted argument escapes and we fall through to the fail-closed escape rules.
                boolean trustedReceiver = receiver != null && receiver.tag == Tag.TRUSTED_OUTPUT;
                if (trustedReceiver) {
                    // A genuine hand-off to the chained successor. The forward-is-not-escape lemma
                    // applies. We still forbid forwarding a BARE extracted input ref (the operator
                    // output forwards a StreamRecord WRAPPER (TAINTED_RECORD) or a CLEAN value; a bare
                    // TAINTED_INPUT/FIELD/SEGMENT slipping into the output is a defensive fail-close).
                    for (int i = firstRealArg; i < args.size(); i++) {
                        TaintValue a = args.get(i);
                        if (a != null
                                && (a.tag == Tag.TAINTED_INPUT
                                        || a.tag == Tag.TAINTED_FIELD
                                        || a.tag == Tag.TAINTED_SEGMENT)) {
                            fire("collect() of a BARE extracted input ref (not a forwarded wrapper); "
                                    + "fail-closed: " + owner);
                            return ret == Type.VOID_TYPE ? null : cleanOf(ret);
                        }
                    }
                    return ret == Type.VOID_TYPE ? null : cleanOf(ret);
                }
                // Untrusted receiver (e.g. a user ListCollector field): any tainted argument escapes.
                for (int i = firstRealArg; i < args.size(); i++) {
                    TaintValue a = args.get(i);
                    if (a != null && a.tainted()) {
                        fire("collect() on an UNTRUSTED collector receiver (provenance not the "
                                + "operator's runtime output) — possible retaining collector; "
                                + "fail-closed: " + owner);
                        break;
                    }
                }
                return ret == Type.VOID_TYPE ? null : cleanOf(ret);
            }

            // new StreamRecord(tainted) / StreamRecord(tainted, ts).
            // SOUNDNESS FIX (reviewer R1-#6): a constructor invocation returns VOID; ASM discards
            // this naryOperation result, so we CANNOT retro-tag the freshly NEW'd StreamRecord that
            // is already sitting on the stack as CLEAN (from newOperation). That means a wrapper
            // built around a tainted payload would silently become CLEAN and could then escape via
            // PUTFIELD/state/return WITHOUT firing. Because we cannot soundly track the wrapper's
            // taint, we FAIL CLOSED whenever a tainted value is passed into a StreamRecord ctor.
            //
            // This is conservative: the legitimate "wrap-then-forward" forward pattern is still
            // supported for the common cases that do NOT pass a tainted payload into a new wrapper
            // (e.g. an operator that re-wraps a freshly computed CLEAN output, or that forwards the
            // ORIGINAL StreamRecord param directly to output.collect). An operator that specifically
            // does `output.collect(new StreamRecord<>(input))` is rejected as UNSAFE (must copy);
            // that is a precision loss in the safe direction, not a soundness loss.
            if (op == INVOKESPECIAL
                    && STREAM_RECORD.equals(owner)
                    && name.equals("<init>")) {
                for (int i = firstRealArg; i < args.size(); i++) {
                    TaintValue a = args.get(i);
                    if (a != null && a.tainted()) {
                        fire("new StreamRecord(tainted payload): wrapper taint cannot be tracked "
                                + "across a void <init>; fail-closed");
                        break;
                    }
                }
                return V_CLEAN_1;
            }

            // ---------- R4 CopyShield: result is CLEAN_COPY, taint does not propagate ----------
            // HOLE C FIX (round-6 codex): a copy is a shield ONLY for the value in the SOURCE
            // position. Flink's two-arg TypeSerializer.copy(from, reuse) WRITES INTO the reuse (2nd)
            // argument (e.g. CopyableValueSerializer.copy(from,reuse){ from.copyTo(reuse); return
            // reuse; }). If the TAINTED input sits in the reuse/target position (any non-source arg,
            // or any real arg of an instance x.copy(...) whose source is the receiver), the call
            // mutates the input in place (U1) / lets the bare input escape into the target (U2) and
            // MUST fire — it must NOT be laundered to CLEAN_COPY. We therefore consult a 3-state
            // decision and, on TAINTED_TARGET, FALL THROUGH to the in-place/escape checks below
            // (without returning CLEAN_COPY) after firing.
            CopyShield csd = copyShieldDecision(owner, name, desc, op, receiver, args, firstRealArg);
            if (csd == CopyShield.CLEAN_COPY) {
                // HOLE D FIX (round-7 codex) — STRUCTURAL GUARD (point 2): never RETURN a CLEAN/forward
                // result for a TAINTED receiver before the R3 in-place + R4 escape checks have run. A
                // CLEAN_COPY may only be granted to a tainted receiver via the legitimate data-owner
                // INSTANCE copy path (x.copy(): the receiver-as-source IS the documented deep-copy
                // shield, and "copy" is not a mutator name so R3 will not fire). For ANY tainted
                // receiver we therefore route THROUGH the R3 mutator / R4 escape checks below instead
                // of short-circuiting here. This is defense-in-depth: it guarantees that even if some
                // future name-trust path were to mis-classify a tainted-receiver mutator/escape as
                // CLEAN_COPY, the mutator/sink checks still get the last word (fail-closed). The legit
                // data-copy shield (receiver tainted, method "copy", deep copy by TCB assumption) is
                // preserved: "copy" is not a mutator name and the result is CLEAN_COPY, returned just
                // below the R3/R4 checks. A NON-tainted receiver (serializer own-field copy, static
                // copy) short-circuits to CLEAN_COPY immediately as before.
                boolean taintedReceiver = receiver != null && receiver.tainted();
                if (!taintedReceiver) {
                    return new TaintValue(Tag.CLEAN_COPY, 1);
                }
                // Tainted receiver: run R3 (in-place mutator) first. If "copy" is ever name-listed as a
                // mutator, or any later check fires, we fail closed; otherwise we grant CLEAN_COPY at
                // the dedicated post-R3/R4 site below.
                if (isMutatorName(name)) {
                    fire("R3 in-place mutation on tainted receiver of a name-trusted copy owner "
                            + "(provenance defeated name-trust): " + owner + "#" + name);
                    return ret == Type.VOID_TYPE ? null : cleanOf(ret);
                }
                // Tainted receiver, non-mutator "copy" of a trusted DATA owner = the documented deep-
                // copy shield. Result is a fresh copy with no aliasing to the input -> CLEAN_COPY.
                return new TaintValue(Tag.CLEAN_COPY, 1);
            }
            if (csd == CopyShield.TAINTED_TARGET) {
                fire("HOLE C: tainted input in the reuse/target (non-source) position of a trusted "
                        + "copy(from, reuse) — in-place write / escape of the input; fail-closed: "
                        + owner + "#" + name + desc);
                return ret == Type.VOID_TYPE ? null : cleanOf(ret);
            }
            // csd == NOT_SHIELD: fall through to the normal R3 in-place / R4 escape checks.

            // ---------- R3 U1: in-place mutation (tainted receiver of a mutator) ----------
            if (receiver != null && receiver.tainted() && isMutatorName(name)) {
                fire("R3 in-place mutation: " + owner + "#" + name + " on tainted input");
                return ret == Type.VOID_TYPE ? null : cleanOf(ret);
            }

            // ---------- R4 U2: escape into retaining/keying state & container sinks ----------
            boolean anyTaintedArg = false;
            for (int i = firstRealArg; i < args.size(); i++) {
                TaintValue a = args.get(i);
                if (a != null && a.tainted()) {
                    anyTaintedArg = true;
                    break;
                }
            }
            // Also: a tainted RECEIVER passed to a non-recognized method is itself an escape risk
            // (R6 unknown-sink with tainted receiver). E.g. record.someUnknownMethod().
            boolean taintedReceiverUnknown = receiver != null && receiver.tainted();

            // R6-READ EXEMPTION (soundness-preserving, TIGHTENED per reviewer R1-#7/#8).
            // A primitive return alone does NOT prove the method is side-effect-free: a method named
            // e.g. `sizeAndStash()` could append `this` to a static queue and return an int (escape),
            // and a method named e.g. `append(b)` could write the receiver's backing array and return
            // a length (in-place mutation). The old heuristic (any primitive-returning, non-mutator-
            // -named call on a tainted receiver is a pure read) was therefore UNSOUND.
            //
            // We now exempt ONLY an explicit allowlist of calls that are provably pure reads of the
            // receiver: JDK boxed-primitive unboxing (Long.longValue() etc.) and Object identity/
            // hashing-free trivial reads. Everything else with a tainted receiver fails closed.
            if (taintedReceiverUnknown && !anyTaintedArg && isPureReadAccessor(owner, name, desc)) {
                return cleanOf(ret); // pure read of the tainted input -> CLEAN primitive result.
            }

            if (anyTaintedArg || taintedReceiverUnknown) {
                if (isStateSink(owner)) {
                    fire("R4 escape into state API: " + owner + "#" + name);
                    return ret == Type.VOID_TYPE ? null : cleanOf(ret);
                }
                if (isContainerMutator(owner, name)) {
                    // Container provenance unresolved here -> fail closed (R4).
                    fire("R4 escape into container mutator (unresolved provenance): " + owner + "#" + name);
                    return ret == Type.VOID_TYPE ? null : cleanOf(ret);
                }
                // ---------- R6: same-class private helper, recurse exactly one level ----------
                if (op == INVOKESPECIAL
                        && owner.equals(ownerInternalName)
                        && recursionBudget > 0) {
                    MethodNode callee = sameClassMethods.get(name + desc);
                    if (callee != null && analyzeHelper(callee, args, hasReceiver)) {
                        // helper proven safe with these tainted args -> continue; result CLEAN
                        // unless it returns a reference, which we conservatively treat as TAINTED
                        // (could return the input) -> TAINTED_FIELD.
                        if (ret.getSort() == Type.OBJECT || ret.getSort() == Type.ARRAY) {
                            return new TaintValue(Tag.TAINTED_FIELD, 1);
                        }
                        return ret == Type.VOID_TYPE ? null : cleanOf(ret);
                    }
                    fire("R6 helper not proven safe / not found: " + owner + "#" + name);
                    return ret == Type.VOID_TYPE ? null : cleanOf(ret);
                }
                // ---------- R6: any other method receiving the tainted ref -> fail closed ----------
                fire("R6 unknown sink for tainted input: " + owner + "#" + name + desc);
                return ret == Type.VOID_TYPE ? null : cleanOf(ret);
            }

            // No tainted value involved. Result is CLEAN (or its declared size). If the method
            // returns a reference that could be derived from a non-tracked source, CLEAN is safe:
            // it carries no taint, so it cannot later be an input-escape.
            return ret == Type.VOID_TYPE ? null : cleanOf(ret);
        }

        @Override
        public void returnOperation(AbstractInsnNode insn, TaintValue value, TaintValue expected) {
            // ARETURN handled in unaryOperation; primitive returns are no-ops. Nothing to do.
        }

        // ---- LUB / control-flow merge: any disagreement climbs the lattice; TOP poisons ----

        @Override
        public TaintValue merge(TaintValue a, TaintValue b) {
            if (a == b) {
                return a;
            }
            if (a == null) {
                return b;
            }
            if (b == null) {
                return a;
            }
            if (a.tag == Tag.BOTTOM) {
                return b;
            }
            if (b.tag == Tag.BOTTOM) {
                return a;
            }
            if (a.tag == b.tag && a.size == b.size) {
                return a;
            }
            // Conservative LUB: if either side is tainted, the merge stays tainted (so a value that
            // is tainted on ANY path is tracked as tainted). The exact tainted flavor climbs to the
            // most conservative: TAINTED_SEGMENT > TAINTED_INPUT > TAINTED_RECORD > TAINTED_FIELD.
            if (a.tainted() || b.tainted()) {
                Tag t = mergeTainted(a, b);
                return new TaintValue(t, 1);
            }
            // Neither tainted, but tags differ (e.g. CLEAN vs CLEAN_COPY vs TOP) -> TOP is harmless
            // for soundness (TOP is not tainted, so it never fires a rule); keep size consistent.
            int size = Math.max(a.size, b.size);
            return size == 2 ? V_CLEAN_2 : V_TOP_1;
        }

        private static Tag mergeTainted(TaintValue a, TaintValue b) {
            // Most conservative wins. SEGMENT is the strongest (mutation channel), then INPUT,
            // then RECORD (the wrapper), then FIELD.
            if (a.tag == Tag.TAINTED_SEGMENT || b.tag == Tag.TAINTED_SEGMENT) {
                return Tag.TAINTED_SEGMENT;
            }
            if (a.tag == Tag.TAINTED_INPUT || b.tag == Tag.TAINTED_INPUT) {
                return Tag.TAINTED_INPUT;
            }
            if (a.tag == Tag.TAINTED_RECORD || b.tag == Tag.TAINTED_RECORD) {
                return Tag.TAINTED_RECORD;
            }
            return Tag.TAINTED_FIELD;
        }

        // ---- 1-level same-class helper recursion (R6) ----

        /**
         * Analyze a same-class private helper with the tainted args mapped onto its parameter locals.
         * Returns true iff the helper is provably safe for these arguments. Recursion budget is
         * decremented to forbid deeper nesting.
         */
        private boolean analyzeHelper(
                MethodNode callee, List<? extends TaintValue> callArgs, boolean callerHadReceiver) {
            if (recursionBudget <= 0) {
                return false;
            }
            try {
                HelperInterpreter helper =
                        new HelperInterpreter(
                                ownerInternalName,
                                sameClassMethods,
                                recursionBudget - 1,
                                ownDeclaredFields,
                                genuineInheritedOutput,
                                mapArgsToParams(callee, callArgs));
                Analyzer<TaintValue> az = new Analyzer<>(helper);
                az.analyze(ownerInternalName, callee);
                return !helper.unsafe;
            } catch (Throwable t) {
                return false; // unmodelable helper -> not safe
            }
        }

        /**
         * Map the call-site argument tags onto the callee's parameter local indices. Long/double
         * occupy two slots. For an INVOKESPECIAL the call args include 'this' at index 0, which maps
         * to the callee local 0 ('this').
         */
        private Map<Integer, Tag> mapArgsToParams(
                MethodNode callee, List<? extends TaintValue> callArgs) {
            Map<Integer, Tag> seed = new HashMap<>();
            int local = 0;
            // INVOKESPECIAL same-class private helper is an instance method: arg0 is 'this'.
            int idx = 0;
            // 'this'
            if (!callArgs.isEmpty()) {
                TaintValue thisV = callArgs.get(0);
                if (thisV != null && thisV.tainted()) {
                    seed.put(0, thisV.tag);
                }
                local = 1;
                idx = 1;
            }
            Type[] paramTypes = Type.getArgumentTypes(callee.desc);
            for (Type pt : paramTypes) {
                if (idx < callArgs.size()) {
                    TaintValue a = callArgs.get(idx);
                    if (a != null && a.tainted()) {
                        seed.put(local, a.tag);
                    }
                }
                local += pt.getSize();
                idx++;
            }
            return seed;
        }

        void fire(String why) {
            if (!unsafe && DEBUG) {
                System.err.println("[Safe0]   UNSAFE: " + why);
            }
            unsafe = true;
        }
    }

    /**
     * Helper interpreter for the 1-level recursion: identical to {@link TaintInterpreter} except the
     * seed comes from a caller-supplied param->tag map instead of the StreamRecord-parameter rule.
     */
    static final class HelperInterpreter extends TaintInterpreter {
        private final Map<Integer, Tag> seededParams;

        HelperInterpreter(
                String ownerInternalName,
                Map<String, MethodNode> sameClassMethods,
                int recursionBudget,
                java.util.Set<String> ownDeclaredFields,
                boolean genuineInheritedOutput,
                Map<Integer, Tag> seededParams) {
            super(
                    ownerInternalName,
                    sameClassMethods,
                    recursionBudget,
                    ownDeclaredFields,
                    genuineInheritedOutput);
            this.seededParams = seededParams;
        }

        @Override
        public TaintValue newParameterValue(boolean isInstanceMethod, int local, Type type) {
            Tag t = seededParams.get(local);
            if (t != null) {
                return new TaintValue(t, type == null ? 1 : type.getSize());
            }
            // Do NOT apply the StreamRecord-parameter seeding here: the helper's taint origin is
            // fully described by the caller's mapped args.
            if (type == null) {
                return V_BOTTOM;
            }
            if (type == Type.VOID_TYPE) {
                return null;
            }
            return type.getSize() == 2 ? V_CLEAN_2 : V_CLEAN_1;
        }
    }

    // ------------------------------------------------------------------------
    // Rule tables (confirmed against flink-statebackend-cached sources)
    // ------------------------------------------------------------------------

    private static final String STREAM_RECORD =
            "org/apache/flink/streaming/runtime/streamrecord/StreamRecord";

    /**
     * Output / Collector forward sinks (R5). SOUNDNESS-CRITICAL: this must recognize ONLY genuine
     * downstream hand-offs (the chained successor is independently classified). A user method that
     * merely happens to be named {@code collect(Object)} but RETAINS its argument (e.g. a buffer's
     * {@code collect}) must NOT be treated as a forward — otherwise the analyzer would falsely
     * authorize the escape. We therefore gate on the receiver OWNER being on the Flink
     * Output/Collector hierarchy (by name pattern, since we cannot do a hierarchy walk here), in
     * addition to the method name and one of the two known collect descriptor shapes.
     *
     * <p>Recognized owners: anything in the Flink output/collect packages, or whose simple name ends
     * in {@code Output} or {@code Collector} (CountingOutput, RecordWriterOutput, ChainingOutput,
     * BroadcastingOutputCollector, TimestampedCollector, the {@code Output}/{@code Collector}
     * interfaces themselves, …). An arbitrary {@code com.example.Buffer#collect} is NOT recognized
     * and falls through to the unknown-sink / container-escape rules (fail-closed).
     */
    private static boolean isCollectSink(int op, String owner, String name, String desc) {
        if (!name.equals("collect")) {
            return false;
        }
        if (!isCollectorOwner(op, owner)) {
            return false;
        }
        // Collector.collect(T) -> 1 arg ; Output.collect(OutputTag, StreamRecord) -> 2 args.
        Type[] a = Type.getArgumentTypes(desc);
        return a.length == 1
                || (a.length == 2
                        && a[1].getSort() == Type.OBJECT
                        && a[1].getInternalName().equals(STREAM_RECORD));
    }

    /**
     * True iff this {@code collect} call is a genuine downstream hand-off through a Flink
     * Output/Collector, recognized SOUNDLY (no name-pattern guesswork).
     *
     * <p>SOUNDNESS FIX (reviewer R2): the earlier "owner simple-name ends in Output/Collector within
     * org/apache/flink" heuristic was unsound — a class such as
     * {@code org/apache/flink/.../RetainingOutput} that RETAINS its {@code collect} argument matched
     * the pattern and was falsely treated as a forward. We now recognize ONLY:
     *   (1) an INVOKEINTERFACE on the canonical {@code org.apache.flink.util.Collector} or
     *       {@code org.apache.flink.streaming.api.operators.Output} interfaces — this is exactly how
     *       every real operator forwards (its {@code output}/{@code collector} field is declared with
     *       the interface type, so {@code javac} emits INVOKEINTERFACE on the interface owner), and a
     *       user/evil concrete class invoking its own {@code collect} via INVOKEVIRTUAL cannot forge
     *       this; OR
     *   (2) an explicit allowlist of trusted concrete Flink runtime output classes (part of the TCB),
     *       which are known to forward (not retain) their argument.
     */
    private static boolean isCollectorOwner(int op, String owner) {
        if (owner == null) {
            return false;
        }
        // (1) Canonical interfaces, dispatched as interface calls only.
        if (op == Opcodes.INVOKEINTERFACE
                && (owner.equals("org/apache/flink/util/Collector")
                        || owner.equals("org/apache/flink/streaming/api/operators/Output"))) {
            return true;
        }
        // (2) Trusted concrete Flink output/collector forwarders (TCB allowlist).
        return isTcbConcreteOutputOwner(owner);
    }

    /**
     * TCB allowlist of trusted concrete Flink runtime output/collector forwarder classes whose
     * {@code collect()} is a verified pass-through to the chained successor (HOLE-2: these wrap the
     * inherited operator output and a user cannot trivially fabricate one). A {@code collect()} whose
     * static dispatch owner is one of these is trusted regardless of receiver provenance.
     */
    private static boolean isTcbConcreteOutputOwner(String owner) {
        if (owner == null) {
            return false;
        }
        switch (owner) {
            case "org/apache/flink/streaming/api/operators/CountingOutput":
            case "org/apache/flink/streaming/api/operators/TimestampedCollector":
            case "org/apache/flink/streaming/runtime/tasks/OperatorChain$ChainingOutput":
            case "org/apache/flink/streaming/runtime/tasks/ChainingOutput":
            case "org/apache/flink/streaming/runtime/tasks/CopyingChainingOutput":
            case "org/apache/flink/streaming/runtime/io/RecordWriterOutput":
            case "org/apache/flink/streaming/runtime/tasks/BroadcastingOutputCollector":
            case "org/apache/flink/streaming/runtime/tasks/CopyingBroadcastingOutputCollector":
                return true;
            default:
                return false;
        }
    }

    // ROUND-9 TRUST AUDIT: isTrustedOutputType(Type) was REMOVED. Both former callers (the inherited
    // 'output' GETFIELD promotion and the runtime-supplied collector parameter) now anchor on the
    // GENUINE Output/Collector interface by EXACT internal name, so the non-final TCB-concrete
    // forwarder TYPE — an impersonable proxy a user could match by subclassing — is no longer an
    // accepted anchor for a TRUSTED_OUTPUT grant anywhere. (isTcbConcreteOutputOwner remains, used
    // ONLY by isCollectorOwner to classify the dispatch OWNER of a collect() insn for sink-shape
    // recognition; it never by itself grants a forward — the collect() forward is granted ONLY when
    // the RECEIVER is TRUSTED_OUTPUT by provenance, see naryOperation's R5 block.)

    /** Methods whose tainted receiver is an in-place mutation (R3 / U1). */
    private static boolean isMutatorName(String name) {
        return name.equals("setRowKind")
                || name.startsWith("set")
                || name.startsWith("replace") // replace* on a row (not StreamRecord.replace, handled earlier)
                || name.startsWith("write")
                || name.startsWith("update")
                || name.equals("clear")
                || name.startsWith("put") // MemorySegment.put*, Unsafe.put*
                || name.startsWith("merge")
                || name.equals("reset");
    }

    /**
     * Allowlist of calls that are provably PURE READS of a tainted receiver (no retain, no in-place
     * mutation), used by the tightened R6-read exemption. Conservative: anything not on this list
     * with a tainted receiver fails closed. Members must be audited to genuinely have no
     * receiver-escaping or receiver-mutating side effect.
     *
     * <p>Currently: JDK boxed-primitive unboxing (the auto-unboxing of a boxed input element, e.g.
     * {@code ((Long) e.getValue()).longValue()}). These return a primitive copy of the wrapped
     * value and neither store {@code this} anywhere nor mutate it.
     */
    private static boolean isPureReadAccessor(String owner, String name, String desc) {
        // Method must take no reference args is already guaranteed by the caller (no tainted args);
        // here we additionally require the JDK boxed-number / Character / Boolean unboxers, which are
        // the only pure reads we are willing to trust by identity.
        //
        // HOLE-1 FIX (round-3 CLEAN review): the owner MUST be one of the FINAL JDK boxed-primitive
        // classes, recognized by EXACT internal name. java/lang/Number (and any other type) is
        // ABSTRACT / overridable, so a user `class EvilNumber extends Number { public long
        // longValue(){ leak = this; return 0; } }` would leak the bare input at runtime while the
        // static method name/descriptor still looks like a pure unboxer. Only the eight final boxed
        // classes below cannot be subclassed/overridden, so their longValue()/intValue()/... are
        // guaranteed-pure reads by identity. Any other owner (incl java/lang/Number) -> fail closed.
        switch (owner) {
            case "java/lang/Long":
                return name.equals("longValue") && desc.equals("()J");
            case "java/lang/Integer":
                return name.equals("intValue") && desc.equals("()I");
            case "java/lang/Short":
                return name.equals("shortValue") && desc.equals("()S");
            case "java/lang/Byte":
                return name.equals("byteValue") && desc.equals("()B");
            case "java/lang/Double":
                return name.equals("doubleValue") && desc.equals("()D");
            case "java/lang/Float":
                return name.equals("floatValue") && desc.equals("()F");
            case "java/lang/Character":
                return name.equals("charValue") && desc.equals("()C");
            case "java/lang/Boolean":
                return name.equals("booleanValue") && desc.equals("()Z");
            default:
                // NOT exempt: java/lang/Number (abstract/overridable) or any user/other type.
                return false;
        }
    }

    /** State-API owners that retain/key on their argument (R4 / U2). */
    private static boolean isStateSink(String owner) {
        return owner.startsWith("org/apache/flink/api/common/state/")
                || owner.endsWith("ListState")
                || owner.endsWith("MapState")
                || owner.endsWith("ValueState")
                || owner.endsWith("AppendingState")
                || owner.endsWith("ReducingState")
                || owner.endsWith("AggregatingState")
                || owner.contains("StateFuture")
                || owner.contains("/state/internal/");
    }

    /** Heap container mutators that retain the argument (R4 / U2). */
    private static boolean isContainerMutator(String owner, String name) {
        boolean container =
                owner.startsWith("java/util/")
                        || owner.equals("java/util/Collection")
                        || owner.equals("java/util/List")
                        || owner.equals("java/util/Map")
                        || owner.equals("java/util/Queue")
                        || owner.equals("java/util/Deque")
                        || owner.equals("java/util/Set");
        if (!container) {
            return false;
        }
        return name.equals("add")
                || name.equals("addAll")
                || name.equals("addFirst")
                || name.equals("addLast")
                || name.equals("put")
                || name.equals("putIfAbsent")
                || name.equals("offer")
                || name.equals("offerFirst")
                || name.equals("offerLast")
                || name.equals("push")
                || name.equals("set");
    }

    /**
     * Three-state outcome of the CopyShield test (R4-shield).
     *
     * <ul>
     *   <li>{@link #NOT_SHIELD}: the call is not a trusted-owner {@code copy} of a tainted value;
     *       defer to the normal R3 in-place / R4 escape checks.
     *   <li>{@link #CLEAN_COPY}: the tainted value is only in the SOURCE (read) position of a
     *       trusted deep-copy; the result is a fresh deep copy with no aliasing to the input, so
     *       escaping the result is safe.
     *   <li>{@link #TAINTED_TARGET}: the tainted value sits in a NON-source (reuse/target) position
     *       of a trusted {@code copy(from, reuse)} (or as a real arg of an instance {@code
     *       x.copy(...)} whose source is the receiver). Such a copy WRITES INTO that argument, so
     *       the tainted input is mutated in place / a bare input ref escapes into the target. This
     *       MUST fire (U1/U2); it must NOT be laundered to CLEAN_COPY.
     * </ul>
     */
    enum CopyShield {
        NOT_SHIELD,
        CLEAN_COPY,
        TAINTED_TARGET
    }

    /**
     * CopyShield set (R4-shield). A tainted value passing through one of these in the SOURCE
     * position produces a fresh deep copy with no aliasing to the input; escaping the copy is safe.
     * This list is part of the TCB (assumption 2) and must be audited against the serializer
     * sources.
     *
     * <p>HOLE C FIX (round-6 codex): position matters. For a static/2-arg {@code copy(from, reuse)},
     * arg0 is the read source and arg1.. are reuse/target arguments that the copy WRITES INTO; a
     * tainted value in any of arg1.. is an in-place write / escape ({@link CopyShield#TAINTED_TARGET}).
     * For an instance {@code x.copy(...)} the receiver is the source; any tainted REAL arg is a
     * target. Only a tainted value confined to the source position yields {@link
     * CopyShield#CLEAN_COPY}. The one-arg {@code copy(from)} / {@code x.copy()} source-shield path
     * (used by the corpus/keystone) is preserved.
     */
    private static CopyShield copyShieldDecision(
            String owner,
            String name,
            String desc,
            int op,
            TaintValue receiver,
            List<? extends TaintValue> args,
            int firstRealArg) {
        boolean serializerOwner = isTrustedSerializerOwner(owner);
        boolean dataOwner = isTrustedDataOwner(owner);
        if (!name.equals("copy") || (!serializerOwner && !dataOwner)) {
            return CopyShield.NOT_SHIELD;
        }
        boolean hasReceiver =
                op == Opcodes.INVOKEVIRTUAL
                        || op == Opcodes.INVOKEINTERFACE
                        || op == Opcodes.INVOKESPECIAL;

        // Determine which value is the SOURCE (the legitimately-shielded read), and from which arg
        // index the REUSE/TARGET slots (the copy WRITES INTO these) begin. A tainted value at or
        // after firstTargetArg is an in-place write / escape -> TAINTED_TARGET (HOLE C).
        int firstTargetArg;
        if (serializerOwner) {
            // serializer.copy(from) / serializer.copy(from, reuse): the RECEIVER is the serializer
            // (NOT data). The data SOURCE is the first real argument (`from`); any subsequent
            // argument is a reuse/target slot the serializer writes into. (Static serializer copy
            // would likewise have `from` as its first real arg, with firstRealArg == 0.)
            //
            // HOLE D FIX (round-7 codex): isTrustedSerializerOwner trusts ANY owner that merely
            // starts with "org/apache/flink/" and ends with "Serializer" (etc.). An adversary can
            // NAME their INPUT data type ...EvilSerializer (in an org.apache.flink.* package) and do
            // taintedInput.copy(cleanArg); pre-fix this branch read cleanArg as the SOURCE and never
            // checked the RECEIVER taint, returning CLEAN_COPY BEFORE the R3 tainted-receiver mutator
            // check could fire — laundering an in-place write (U1) / bare-input escape (U2) to SAFE.
            //
            // ROOT-CAUSE FIX: the name "...Serializer" may ROUTE us here, but it must NEVER be the
            // sole basis for granting CLEAN_COPY on a TAINTED receiver. A genuine serializer-copy
            // shield has a NON-TAINTED receiver (the operator's own serializer field/local). If the
            // receiver is itself TAINTED, the "serializer" is really input data: we must NOT treat
            // this as a serializer shield. Fall through (NOT_SHIELD) so the R3 receiver-mutator + R4
            // escape checks run on the tainted receiver. (Over-firing toward UNSAFE is fine.)
            //
            // Note: a NON-instance (static) serializer copy has no receiver (receiver == null), which
            // is correctly non-tainted, so the legitimate static/instance own-field shield is kept.
            if (hasReceiver && receiver != null && receiver.tainted()) {
                return CopyShield.NOT_SHIELD;
            }
            int sourceArg = firstRealArg;
            firstTargetArg = sourceArg + 1;
        } else if (hasReceiver) {
            // Data-type instance copy x.copy(...): the RECEIVER is the source data; every passed
            // argument is a reuse/target slot. Require a tainted receiver (the value being shielded)
            // for this to be a meaningful data-copy shield; otherwise treat as NOT_SHIELD so the
            // normal (clean) path handles it.
            if (receiver == null || !receiver.tainted()) {
                // Receiver is clean: even if an arg is tainted, this is not "copy the input" — it is
                // writing the input into a clean data target. Fall through to in-place/escape checks.
                for (int i = firstRealArg; i < args.size(); i++) {
                    TaintValue a = args.get(i);
                    if (a != null && a.tainted()) {
                        return CopyShield.TAINTED_TARGET;
                    }
                }
                return CopyShield.NOT_SHIELD;
            }
            firstTargetArg = firstRealArg;
        } else {
            // Data-type static copy: copy(from[, reuse, ...]): arg0 (== firstRealArg == 0) is the
            // source; everything after it is a reuse/target slot.
            firstTargetArg = firstRealArg + 1;
        }

        for (int i = firstTargetArg; i < args.size(); i++) {
            TaintValue a = args.get(i);
            if (a != null && a.tainted()) {
                return CopyShield.TAINTED_TARGET;
            }
        }
        // No tainted value sits in a non-source position. The result is a fresh deep copy of the
        // (possibly tainted) source -> CLEAN_COPY.
        return CopyShield.CLEAN_COPY;
    }

    /**
     * True when {@code owner} is a recognized trusted Flink TypeSerializer owner whose {@code copy}
     * is a genuine deep copy. For such an owner the receiver is the SERIALIZER (not data): the data
     * source is the first real argument ({@code from}), and any later argument is a reuse/target.
     *
     * <p>SOUNDNESS NOTE (reviewer R1-#3): CopyShield remains a documented TCB assumption. We trust
     * that a method named {@code copy} on a Flink TypeSerializer performs a genuine deep copy. The
     * owner test is tightened to recognized Flink serializer owners only, so a user {@code
     * BadSerializer#copy} outside the flink namespace is no longer blindly trusted.
     */
    private static boolean isTrustedSerializerOwner(String owner) {
        return owner.startsWith("org/apache/flink/")
                && (owner.endsWith("TypeSerializer")
                        || owner.contains("/typeutils/")
                        || owner.contains("/serializer/")
                        || owner.endsWith("Serializer"));
    }

    /**
     * True when {@code owner} is a recognized trusted Flink data-type owner whose {@code copy} is a
     * genuine deep copy: RowData#copy(), GenericRowData#copy(), BinaryRowData#copy(), Tuple#copy(),
     * RowDataUtil.copy(from), etc. For an instance call the RECEIVER is the source data; for a
     * static call arg0 is the source. Later args are reuse/target slots (HOLE C).
     */
    private static boolean isTrustedDataOwner(String owner) {
        return owner.startsWith("org/apache/flink/")
                && (owner.endsWith("RowData")
                        || owner.endsWith("GenericRowData")
                        || owner.endsWith("BinaryRowData")
                        || owner.endsWith("RowDataUtil")
                        || owner.contains("/data/")
                        || owner.startsWith("org/apache/flink/api/java/tuple/"));
    }

    /** Field/type whose value should be tracked as a backing segment (R2 TAINTED_SEGMENT). */
    private static boolean isSegmentType(Type t) {
        if (t.getSort() == Type.ARRAY) {
            return true;
        }
        if (t.getSort() == Type.OBJECT) {
            String in = t.getInternalName();
            return in.endsWith("MemorySegment") || in.endsWith("MemorySegment;");
        }
        return false;
    }

    /** For tests / diagnostics: clear the cache. */
    static void clearCacheForTest() {
        CACHE.clear();
    }
}
