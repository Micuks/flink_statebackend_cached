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

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.table.data.binary.BinaryRowData;

import java.util.Objects;

final class KeyNamespaceKey<K, N> {
    final K key;
    final N namespace;

    KeyNamespaceKey(K key, N namespace, boolean deepCopy) {
        if (deepCopy && key instanceof BinaryRowData) {
            this.key = (K) ((BinaryRowData) key).copy();
        } else {
            this.key = key;
        }
        if (deepCopy && namespace instanceof BinaryRowData) {
            this.namespace = (N) ((BinaryRowData) namespace).copy();
        } else {
            this.namespace = namespace;
        }
    }

    boolean isSame(K otherKey, N otherNamespace) {
        return Objects.equals(this.key, otherKey) && Objects.equals(this.namespace, otherNamespace);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof KeyNamespaceKey)) {
            return false;
        }
        KeyNamespaceKey<?, ?> that = (KeyNamespaceKey<?, ?>) other;
        return Objects.equals(key, that.key) && Objects.equals(namespace, that.namespace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, namespace);
    }
}
