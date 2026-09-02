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

import org.junit.jupiter.api.Test;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CacheKeyHashTest {

    private static final Object[] VALUES = {
        null, "", "key", Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE
    };

    @Test
    void testTwoPartHashExactlyMatchesObjectsHash() {
        for (Object key : VALUES) {
            for (Object namespace : VALUES) {
                assertEquals(
                        Objects.hash(key, namespace), CacheKeyHash.hash(key, namespace));
            }
        }
    }

    @Test
    void testThreePartHashExactlyMatchesObjectsHash() {
        for (Object key : VALUES) {
            for (Object namespace : VALUES) {
                for (Object userKey : VALUES) {
                    assertEquals(
                            Objects.hash(key, namespace, userKey),
                            CacheKeyHash.hash(key, namespace, userKey));
                }
            }
        }
    }
}
