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

package org.apache.flink.table.runtime.dataview;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DistinctBatchPrefetchSupportTest {

    @Test
    void forwardsOneCompleteCollectionScopeToRuntimeOwnedView() throws Exception {
        RecordingView view = new RecordingView();

        DistinctBatchPrefetchSupport.begin(view, 4);
        DistinctBatchPrefetchSupport.add(view, "a");
        DistinctBatchPrefetchSupport.add(view, "b");

        assertThat(DistinctBatchPrefetchSupport.finish(view)).isTrue();
        assertThat(view.calls).containsExactly("begin:4", "add:a", "add:b", "finish");
    }

    @Test
    void unsupportedViewsRemainFailClosed() throws Exception {
        Object unsupported = new Object();

        DistinctBatchPrefetchSupport.begin(unsupported, 2);
        DistinctBatchPrefetchSupport.add(unsupported, "a");
        assertThat(DistinctBatchPrefetchSupport.finish(unsupported)).isFalse();
        DistinctBatchPrefetchSupport.abort(unsupported);
    }

    private static final class RecordingView implements BatchPrefetchableMapView<String> {
        private final List<String> calls = new ArrayList<>();

        @Override
        public void beginPrefetchKeyCollection(int expectedKeys) {
            calls.add("begin:" + expectedKeys);
        }

        @Override
        public void addPrefetchKey(String key) {
            calls.add("add:" + key);
        }

        @Override
        public boolean finishPrefetchKeyCollection() {
            calls.add("finish");
            return true;
        }

        @Override
        public void abortPrefetchKeyCollection() {
            calls.add("abort");
        }
    }
}
