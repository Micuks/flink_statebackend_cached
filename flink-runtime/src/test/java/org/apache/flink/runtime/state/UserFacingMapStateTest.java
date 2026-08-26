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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.runtime.state.internal.BatchPrefetchableMapState;

import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/** Tests the optional batch-prefetch capability through the user-facing MapState wrapper. */
public class UserFacingMapStateTest {

    @Test
    public void testDelegatesBatchPrefetchCapability() throws Exception {
        RecordingMapState original = new RecordingMapState();
        UserFacingMapState<String, Long> state = new UserFacingMapState<>(original);

        assertTrue(state.beginPrefetchCurrentKeys(java.util.Arrays.asList("a", "b")));
        assertEquals(2, original.prefetchedKeys);
        state.endPrefetchCurrentKeys();
        assertTrue(original.prefetchEnded);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUnsupportedDelegateFailsClosed() throws Exception {
        UserFacingMapState<String, Long> state =
                new UserFacingMapState<>((MapState<String, Long>) mock(MapState.class));

        assertFalse(state.beginPrefetchCurrentKeys(Collections.singletonList("a")));
        state.endPrefetchCurrentKeys();
    }

    private static final class RecordingMapState
            implements MapState<String, Long>, BatchPrefetchableMapState<String> {
        private int prefetchedKeys;
        private boolean prefetchEnded;

        @Override
        public boolean beginPrefetchCurrentKeys(Iterable<? extends String> keys) {
            for (String ignored : keys) {
                prefetchedKeys++;
            }
            return true;
        }

        @Override
        public void endPrefetchCurrentKeys() {
            prefetchEnded = true;
        }

        @Override
        public Long get(String key) {
            return null;
        }

        @Override
        public void put(String key, Long value) {}

        @Override
        public void putAll(Map<String, Long> map) {}

        @Override
        public void remove(String key) {}

        @Override
        public boolean contains(String key) {
            return false;
        }

        @Override
        public Iterable<Map.Entry<String, Long>> entries() {
            return Collections.emptyList();
        }

        @Override
        public Iterable<String> keys() {
            return Collections.emptyList();
        }

        @Override
        public Iterable<Long> values() {
            return Collections.emptyList();
        }

        @Override
        public Iterator<Map.Entry<String, Long>> iterator() {
            return Collections.<Map.Entry<String, Long>>emptyList().iterator();
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public void clear() {}
    }
}
