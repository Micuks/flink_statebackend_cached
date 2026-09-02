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

import java.util.Objects;

/** Allocation-free equivalents of the {@link Objects#hash(Object...)} cache-key forms. */
final class CacheKeyHash {

    private CacheKeyHash() {}

    static int hash(Object key, Object namespace) {
        int result = 31 + Objects.hashCode(key);
        return 31 * result + Objects.hashCode(namespace);
    }

    static int hash(Object key, Object namespace, Object userKey) {
        int result = 31 + Objects.hashCode(key);
        result = 31 * result + Objects.hashCode(namespace);
        return 31 * result + Objects.hashCode(userKey);
    }
}
