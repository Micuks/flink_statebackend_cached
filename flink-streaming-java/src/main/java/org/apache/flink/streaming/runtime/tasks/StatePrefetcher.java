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
 * Backpressure-driven state prefetch (MVP, synchronous form B).
 *
 * <p>Given a lookahead buffer of upcoming {@link StreamRecord}s — accumulated by {@link
 * org.apache.flink.streaming.runtime.io.StreamRecordBatchOutput} in prefetch mode — this helper
 * extracts each record's keyed-state key (via the head operator's {@code stateKeySelector1}, reused
 * from the {@link BatchedKeyedOperatorAdapter} reflection pattern) and asks the keyed-state backend
 * to warm its cache for the whole batch through an optional {@code prefetch(Collection)} method.
 * The batch is then dispatched <em>in arrival order</em> (no reorder) by the caller, so every
 * {@code value()} can hit cache when the backend supports prefetch.
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
        if (n <= 1 || headOperator == null) {
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

            KeySelector selector = extractStateKeySelector1(op);
            if (selector == null) {
                return;
            }

            java.util.List keys = new java.util.ArrayList(n);
            for (int i = 0; i < n; i++) {
                StreamRecord<?> rec = buf[i];
                if (rec == null) {
                    continue;
                }
                Object key;
                try {
                    key = selector.getKey(rec.getValue());
                } catch (Throwable t) {
                    return; // an unkeyed/odd record: bail, the prefetch is optional
                }
                if (key != null) {
                    keys.add(key);
                }
            }
            if (!keys.isEmpty()) {
                prefetchMethod.invoke(ksb, keys);
            }
        } catch (Throwable t) {
            // best-effort: prefetch must never affect the authoritative dispatch path.
        }
    }

    public static java.util.concurrent.CompletableFuture<Void> prefetchAsync(
            Input<?> headOperator, StreamRecord<?>[] buf, int n) {
        if (n <= 1 || headOperator == null) {
            return java.util.concurrent.CompletableFuture.completedFuture(null);
        }
        prefetch(headOperator, buf, n);
        return java.util.concurrent.CompletableFuture.completedFuture(null);
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
