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

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

public final class KeyAccessStats<T> {
    private final int windowSize;
    private final ArrayDeque<T> window;
    private final Map<T, Integer> counts;
    private long totalAccesses;
    private long repeatAccesses;

    public KeyAccessStats(int windowSize) {
        this.windowSize = Math.max(1, windowSize);
        this.window = new ArrayDeque<>(this.windowSize);
        this.counts = new HashMap<>();
    }

    public void record(T key) {
        totalAccesses++;
        Integer current = counts.get(key);
        if (current != null) {
            repeatAccesses++;
            counts.put(key, current + 1);
        } else {
            counts.put(key, 1);
        }
        window.addLast(key);
        if (window.size() > windowSize) {
            T evicted = window.removeFirst();
            Integer evictedCount = counts.get(evicted);
            if (evictedCount != null) {
                if (evictedCount <= 1) {
                    counts.remove(evicted);
                } else {
                    counts.put(evicted, evictedCount - 1);
                }
            }
        }
    }

    public long getTotalAccesses() {
        return totalAccesses;
    }

    public long getRepeatAccesses() {
        return repeatAccesses;
    }

    public int getWindowedAccesses() {
        return window.size();
    }

    public int getWindowedUniqueKeys() {
        return counts.size();
    }

    public double getWindowedRepeatRatio() {
        int windowedAccesses = window.size();
        if (windowedAccesses == 0) {
            return 0.0d;
        }
        long windowedRepeats = Math.max(0, windowedAccesses - counts.size());
        return (double) windowedRepeats / windowedAccesses;
    }

    public double getWindowedUniqueRatio() {
        int windowedAccesses = window.size();
        if (windowedAccesses == 0) {
            return 0.0d;
        }
        return (double) counts.size() / windowedAccesses;
    }
}
