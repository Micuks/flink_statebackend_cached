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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Backpressure-driven state prefetch (key extraction + submission side).
 *
 * <p>Given a lookahead buffer of upcoming {@link StreamRecord}s — accumulated by {@link
 * org.apache.flink.streaming.runtime.io.StreamRecordBatchOutput} in prefetch mode — this helper
 * extracts each record's keyed-state key (via the head operator's {@code stateKeySelector1}, reused
 * from the {@link BatchedKeyedOperatorAdapter} reflection pattern), dedups it, and hands the key
 * set to the keyed-state backend through an optional {@code prefetch(Collection)} method. Whether
 * the backend fetches synchronously or on a dedicated worker thread is the backend's choice; the
 * CacheKit backend enqueues the reads to a shared off-mailbox worker so they overlap with record
 * dispatch and backpressure waits. The batch is then dispatched <em>in arrival order</em> (no
 * reorder) by the caller.
 *
 * <p>Correctness gates (all enforced):
 *
 * <ul>
 *   <li><b>No reorder</b> — this class only warms the cache; record dispatch order is the caller's
 *       unchanged arrival-order replay.
 *   <li><b>Stale-safe</b> — the backend prefetch hook is best-effort and runs on the mailbox
 *       thread.
 *   <li><b>Best-effort</b> — every path is wrapped in try/catch; a failed prefetch never touches
 *       the authoritative read path or the {@code emitRecord} dispatch.
 * </ul>
 *
 * <p>This class is stateless; the mailbox-thread invariant of the surrounding output provides the
 * exclusion. A small static reflection cache mirrors {@link BatchedKeyedOperatorAdapter}.
 */
public final class StatePrefetcher {

    /** Cache of {@code stateKeySelector1} {@link Field} per operator class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Field>
            KEY_SELECTOR_FIELD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional {@code prefetch(Collection)} {@link Method} per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            PREFETCH_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional synchronous local-preagg bulk-prefetch methods per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            IMMEDIATE_PREFETCH_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional {@code hasPrefetchableState()} {@link Method} per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            HAS_PREFETCHABLE_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Cache of optional native mailbox-batch capability probes per backend class. */
    private static final java.util.concurrent.ConcurrentHashMap<Class<?>, Method>
            NATIVE_MAILBOX_METHOD_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

    /** Sentinel field used to mark "no stateKeySelector1 available" in the field cache. */
    private static final Field NO_FIELD;

    /** Sentinel method used to mark "no prefetch hook available" in the method cache. */
    private static final Method NO_METHOD;

    static {
        try {
            NO_FIELD = StatePrefetcher.class.getDeclaredField("NO_FIELD");
            NO_METHOD =
                    StatePrefetcher.class.getDeclaredMethod(
                            "noopPrefetch", java.util.Collection.class);
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        } catch (NoSuchMethodException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private StatePrefetcher() {}

    @SuppressWarnings("unused")
    private static void noopPrefetch(java.util.Collection<?> ignored) {}

    /**
     * Best-effort prefetch of the keyed state for the first {@code n} records of {@code buf}.
     *
     * @param headOperator the head operator (typically the chain's main operator).
     * @param buf record buffer; only entries {@code [0, n)} are valid.
     * @param n number of valid records in {@code buf}.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void prefetch(Input<?> headOperator, StreamRecord<?>[] buf, int n) {
        prefetch(headOperator, buf, 0, n);
    }

    /**
     * Best-effort prefetch for a live range in the caller-owned record buffer. The call is
     * synchronous only through key extraction and backend submission; CacheKit performs the actual
     * RocksDB reads on its worker. Keeping the range avoids allocating a copied record slice for
     * every early-lookahead chunk.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void prefetch(
            Input<?> headOperator, StreamRecord<?>[] buf, int fromIndex, int toIndex) {
        if (headOperator == null
                || buf == null
                || fromIndex < 0
                || toIndex > buf.length
                || toIndex - fromIndex <= 1) {
            return; // single record gains nothing from a batched prefetch
        }
        try {
            if (!(headOperator instanceof AbstractStreamOperator)) {
                return;
            }
            AbstractStreamOperator<?> op = (AbstractStreamOperator<?>) headOperator;

            KeyedStateBackend<?> ksb;
            try {
                ksb = op.getKeyedStateBackend();
            } catch (Throwable t) {
                return;
            }
            Method prefetchMethod = findPrefetchMethod(ksb);
            if (prefetchMethod == null) {
                return;
            }

            // Skip the whole per-batch key extraction when the backend has nothing to warm
            // (e.g. window operators whose namespaced states are never wrapped). Wrappers
            // register lazily, so this is re-checked on every batch, not cached.
            if (!hasPrefetchableState(ksb)) {
                return;
            }

            KeySelector selector = extractStateKeySelector1(op);
            if (selector == null) {
                return;
            }

            // Native mailbox mode preserves the raw arrival-order key vector so the selected
            // AArch64 kernel can compact exact duplicates after serialization. Other backends keep
            // the original LinkedHashSet behavior.
            java.util.Collection keys =
                    newKeyCollection(ksb, Math.max(2, toIndex - fromIndex));
            if (extractKeys(selector, buf, fromIndex, toIndex, keys) && !keys.isEmpty()) {
                prefetchMethod.invoke(ksb, keys);
            }
        } catch (Throwable t) {
            // best-effort: prefetch must never affect the authoritative dispatch path.
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static boolean extractKeys(
            KeySelector selector,
            StreamRecord<?>[] buf,
            int fromIndex,
            int toIndex,
            java.util.Collection keys) {
        for (int i = fromIndex; i < toIndex; i++) {
            StreamRecord<?> rec = buf[i];
            if (rec == null) {
                continue;
            }
            Object key;
            try {
                key = selector.getKey(rec.getValue());
            } catch (Throwable t) {
                return false; // an unkeyed/odd record: bail, the prefetch is optional
            }
            if (key != null) {
                keys.add(key);
            }
        }
        return true;
    }

    /**
     * Bulk-load the already grouped keys immediately before local pre-aggregation consumes them.
     *
     * <p>Unlike record lookahead, these keys are no longer speculative: {@code LocalPreagg} has
     * already built its exact group set and will access each key once. CacheKit may therefore use a
     * blocking RocksDB MultiGet here; other backends simply lack the optional reflective hook.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static boolean prefetchKeysImmediately(
            Input<?> headOperator, java.util.Collection<?> keys) {
        if (headOperator == null
                || keys == null
                || keys.isEmpty()
                || !(headOperator instanceof AbstractStreamOperator)) {
            return false;
        }
        try {
            KeyedStateBackend<?> backend =
                    ((AbstractStreamOperator<?>) headOperator).getKeyedStateBackend();
            return prefetchKeysImmediately(backend, keys);
        } catch (Throwable t) {
            return false;
        }
    }

    static boolean prefetchKeysImmediately(
            KeyedStateBackend<?> backend, java.util.Collection<?> keys) {
        if (backend == null || keys == null || keys.isEmpty() || !hasPrefetchableState(backend)) {
            return false;
        }
        try {
            Method method =
                    IMMEDIATE_PREFETCH_METHOD_CACHE.computeIfAbsent(
                            backend.getClass(), StatePrefetcher::lookupImmediatePrefetchMethod);
            if (method == NO_METHOD) {
                return false;
            }
            method.invoke(backend, keys);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    private static Method lookupImmediatePrefetchMethod(Class<?> backendClass) {
        try {
            Method method =
                    backendClass.getMethod("prefetchForImmediateUse", java.util.Collection.class);
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException ignored) {
            return NO_METHOD;
        }
    }

    /** Best-effort {@code hasPrefetchableState()} probe; defaults to true when absent. */
    private static boolean hasPrefetchableState(KeyedStateBackend<?> backend) {
        try {
            Method method =
                    HAS_PREFETCHABLE_METHOD_CACHE.computeIfAbsent(
                            backend.getClass(), StatePrefetcher::lookupHasPrefetchableMethod);
            if (method == NO_METHOD) {
                return true; // backend without the probe: keep the old behavior
            }
            Object result = method.invoke(backend);
            return !(result instanceof Boolean) || (Boolean) result;
        } catch (Throwable t) {
            return true;
        }
    }

    private static Method lookupHasPrefetchableMethod(Class<?> backendClass) {
        Class<?> c = backendClass;
        while (c != null && c != Object.class) {
            try {
                Method method = c.getDeclaredMethod("hasPrefetchableState");
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException ignored) {
                c = c.getSuperclass();
            }
        }
        return NO_METHOD;
    }

    private static boolean usesNativeMailboxBatch(KeyedStateBackend<?> backend) {
        try {
            Method method =
                    NATIVE_MAILBOX_METHOD_CACHE.computeIfAbsent(
                            backend.getClass(), StatePrefetcher::lookupNativeMailboxMethod);
            if (method == NO_METHOD) {
                return false;
            }
            Object result = method.invoke(backend);
            return result instanceof Boolean && (Boolean) result;
        } catch (Throwable t) {
            return false;
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    static java.util.Collection newKeyCollection(
            KeyedStateBackend<?> backend, int expectedEntries) {
        return usesNativeMailboxBatch(backend)
                ? new java.util.ArrayList(expectedEntries)
                : new java.util.LinkedHashSet(expectedEntries);
    }

    private static Method lookupNativeMailboxMethod(Class<?> backendClass) {
        try {
            Method method = backendClass.getMethod("nativeMailboxBatchEnabled");
            method.setAccessible(true);
            return method;
        } catch (NoSuchMethodException ignored) {
            return NO_METHOD;
        }
    }

    private static Method findPrefetchMethod(KeyedStateBackend<?> backend) {
        if (backend == null) {
            return null;
        }
        Method method =
                PREFETCH_METHOD_CACHE.computeIfAbsent(
                        backend.getClass(), StatePrefetcher::lookupPrefetchMethod);
        return method == NO_METHOD ? null : method;
    }

    private static Method lookupPrefetchMethod(Class<?> backendClass) {
        Class<?> c = backendClass;
        while (c != null && c != Object.class) {
            try {
                Method method = c.getDeclaredMethod("prefetch", java.util.Collection.class);
                method.setAccessible(true);
                return method;
            } catch (NoSuchMethodException ignored) {
                c = c.getSuperclass();
            }
        }
        return NO_METHOD;
    }

    // ------------------------------------------------------------------------
    //  KeySelector access (mirrors BatchedKeyedOperatorAdapter)
    // ------------------------------------------------------------------------

    private static KeySelector<?, ?> extractStateKeySelector1(AbstractStreamOperator<?> op) {
        Field f =
                KEY_SELECTOR_FIELD_CACHE.computeIfAbsent(
                        op.getClass(), StatePrefetcher::findKeySelectorField);
        if (f == NO_FIELD) {
            return null;
        }
        try {
            Object v = f.get(op);
            return (KeySelector<?, ?>) v;
        } catch (IllegalAccessException e) {
            return null;
        }
    }

    private static Field findKeySelectorField(Class<?> opClass) {
        Class<?> c = opClass;
        while (c != null && c != Object.class) {
            try {
                Field f = c.getDeclaredField("stateKeySelector1");
                f.setAccessible(true);
                return f;
            } catch (NoSuchFieldException ignored) {
                c = c.getSuperclass();
            }
        }
        return NO_FIELD;
    }
}
