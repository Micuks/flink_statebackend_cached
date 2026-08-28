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
package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * Mailbox-owned stable keys aligned with a {@link StreamRecordBatchOutput} record buffer.
 *
 * <p>The selector identity is part of the lease. A consumer may reuse a key only when it holds the
 * exact selector instance that produced it. The sidecar never enters checkpoint state; its owner
 * shifts and clears it together with the buffered record prefix.
 */
public final class MailboxStableKeySidecar {

    private final Object[] keys;
    private final boolean[] ready;
    private KeySelector<?, ?> selector;

    public MailboxStableKeySidecar(int capacity) {
        this.keys = new Object[Math.max(1, capacity)];
        this.ready = new boolean[this.keys.length];
    }

    public int capacity() {
        return keys.length;
    }

    public void invalidate(int index) {
        keys[index] = null;
        ready[index] = false;
    }

    public void capture(int index, Object key, KeySelector<?, ?> producingSelector) {
        if (producingSelector == null) {
            invalidate(index);
            return;
        }
        if (selector != null && selector != producingSelector) {
            clearAll();
        }
        selector = producingSelector;
        keys[index] = key;
        ready[index] = true;
    }

    public boolean isReady(int index, KeySelector<?, ?> consumingSelector) {
        return selector == consumingSelector && index >= 0 && index < ready.length && ready[index];
    }

    public Object keyAt(int index) {
        return keys[index];
    }

    public void clearRange(int fromIndex, int toIndex) {
        int from = Math.max(0, fromIndex);
        int to = Math.min(keys.length, toIndex);
        for (int index = from; index < to; index++) {
            invalidate(index);
        }
    }

    public void discardPrefix(int prefixSize, int priorCount) {
        int remaining = priorCount - prefixSize;
        if (remaining > 0) {
            System.arraycopy(keys, prefixSize, keys, 0, remaining);
            System.arraycopy(ready, prefixSize, ready, 0, remaining);
        }
        clearRange(remaining, priorCount);
        if (remaining == 0) {
            selector = null;
        }
    }

    public void clearAll() {
        clearRange(0, keys.length);
        selector = null;
    }
}
