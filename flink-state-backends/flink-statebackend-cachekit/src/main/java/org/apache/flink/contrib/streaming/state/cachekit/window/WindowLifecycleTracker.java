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

package org.apache.flink.contrib.streaming.state.cachekit.window;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the lifecycle of active windows and computes estimated trigger times (ETT).
 *
 * <p>This is the information backbone shared by window-aware cache policies. It maintains
 * per-window metadata including ETT, phase, and access statistics. The tracker uses reflection
 * to extract ETT from {@code TimeWindow.getEnd()} when available, falling back to heuristics
 * for unknown window types.
 *
 * <p>Thread safety: single-threaded access assumed (Flink state is per-task, single-threaded).
 */
public class WindowLifecycleTracker {

    /** Per-window metadata. Keyed by the namespace (window) object. */
    private final Map<Object, WindowInfo> windowInfos = new HashMap<>(256);

    /** ETT-ordered index for prefetch scheduling and eviction priority. */
    private final TreeMap<Long, Set<Object>> ettIndex = new TreeMap<>();

    /** Current watermark, advanced externally. */
    private long currentWatermark = Long.MIN_VALUE;

    /** Cached reflection method for TimeWindow.getEnd(). Null if not yet resolved. */
    private volatile Method getEndMethod;
    private volatile boolean getEndMethodResolved = false;

    /** Statistics for monitoring. */
    private long totalAccesses;
    private long totalWindowsTracked;

    public void onWindowAccess(Object key, Object namespace) {
        WindowInfo info = windowInfos.get(namespace);
        if (info == null) {
            info = new WindowInfo();
            info.namespace = namespace;
            info.createTime = System.currentTimeMillis();
            info.estimatedTriggerTime = extractETT(namespace);
            info.phase = WindowPhase.ACCUMULATING;
            windowInfos.put(namespace, info);
            addToEttIndex(info.estimatedTriggerTime, namespace);
            totalWindowsTracked++;
        }
        info.totalAccessCount++;
        info.lastAccessTime = System.currentTimeMillis();
        totalAccesses++;
    }

    public void onWindowWrite(Object namespace) {
        WindowInfo info = windowInfos.get(namespace);
        if (info != null) {
            info.totalWriteCount++;
            info.lastWriteTime = System.currentTimeMillis();
        }
    }

    public void onWindowRead(Object namespace) {
        WindowInfo info = windowInfos.get(namespace);
        if (info != null) {
            info.totalReadCount++;
        }
    }

    /**
     * Returns the estimated trigger time for the given namespace.
     * Returns {@code Long.MAX_VALUE} if the namespace is unknown (safest: never evict unknown).
     */
    public long getETT(Object namespace) {
        WindowInfo info = windowInfos.get(namespace);
        if (info != null) {
            return info.estimatedTriggerTime;
        }
        return Long.MAX_VALUE;
    }

    /**
     * Computes eviction priority for a namespace.
     *
     * <p>Higher value = keep longer (protect from eviction).
     * Lower value = evict first.
     *
     * <ul>
     *   <li>EXPIRED phase: 0 (evict immediately)</li>
     *   <li>TRIGGERING phase or ETT <= watermark: Long.MAX_VALUE (protect)</li>
     *   <li>ACCUMULATING: inversely proportional to time-to-trigger (farther = lower priority)</li>
     * </ul>
     */
    public long getEvictionPriority(Object namespace) {
        WindowInfo info = windowInfos.get(namespace);
        if (info == null) {
            // Unknown namespace: treat as moderately important (middle priority)
            return Long.MAX_VALUE / 2;
        }

        if (info.phase == WindowPhase.EXPIRED) {
            return 0L;
        }
        if (info.phase == WindowPhase.TRIGGERING) {
            return Long.MAX_VALUE;
        }

        long timeToTrigger = info.estimatedTriggerTime - currentWatermark;
        if (timeToTrigger <= 0) {
            // About to trigger or overdue: protect
            return Long.MAX_VALUE;
        }

        // Invert: closer to trigger = higher priority (keep), farther = lower priority (evict)
        // Use Long.MAX_VALUE - timeToTrigger to map large timeToTrigger to low priority
        return Long.MAX_VALUE - Math.min(timeToTrigger, Long.MAX_VALUE - 1);
    }

    public void markTriggering(Object namespace) {
        WindowInfo info = windowInfos.get(namespace);
        if (info != null) {
            info.phase = WindowPhase.TRIGGERING;
        }
    }

    public void markExpired(Object namespace) {
        WindowInfo info = windowInfos.get(namespace);
        if (info != null) {
            removeFromEttIndex(info.estimatedTriggerTime, namespace);
            info.phase = WindowPhase.EXPIRED;
        }
    }

    public void removeWindow(Object namespace) {
        WindowInfo info = windowInfos.remove(namespace);
        if (info != null) {
            removeFromEttIndex(info.estimatedTriggerTime, namespace);
        }
    }

    public void setCurrentWatermark(long watermark) {
        this.currentWatermark = watermark;
    }

    public long getCurrentWatermark() {
        return currentWatermark;
    }

    public WindowPhase getPhase(Object namespace) {
        WindowInfo info = windowInfos.get(namespace);
        return info != null ? info.phase : null;
    }

    public int getActiveWindowCount() {
        return windowInfos.size();
    }

    public long getTotalAccesses() {
        return totalAccesses;
    }

    public long getTotalWindowsTracked() {
        return totalWindowsTracked;
    }

    /** Extracts ETT from namespace object. Uses reflection for TimeWindow compatibility. */
    private long extractETT(Object namespace) {
        if (namespace == null) {
            return Long.MAX_VALUE;
        }

        // Try to use cached reflection method for TimeWindow.getEnd()
        if (!getEndMethodResolved) {
            try {
                getEndMethod = namespace.getClass().getMethod("getEnd");
                getEndMethodResolved = true;
            } catch (NoSuchMethodException e) {
                getEndMethod = null;
                getEndMethodResolved = true;
            }
        }

        if (getEndMethod != null) {
            try {
                Object result = getEndMethod.invoke(namespace);
                if (result instanceof Long) {
                    return (Long) result;
                }
            } catch (Exception e) {
                // Fall through to heuristic
            }
        }

        // Fallback: no ETT available, use MAX_VALUE (behaves like LRU)
        return Long.MAX_VALUE;
    }

    private void addToEttIndex(long ett, Object namespace) {
        ettIndex.computeIfAbsent(ett, k -> new HashSet<>()).add(namespace);
    }

    private void removeFromEttIndex(long ett, Object namespace) {
        Set<Object> set = ettIndex.get(ett);
        if (set != null) {
            set.remove(namespace);
            if (set.isEmpty()) {
                ettIndex.remove(ett);
            }
        }
    }

    /** Per-window metadata. */
    public static class WindowInfo {
        Object namespace;
        long createTime;
        long estimatedTriggerTime;
        long lastAccessTime;
        long lastWriteTime;
        int totalAccessCount;
        int totalWriteCount;
        int totalReadCount;
        WindowPhase phase;

        public long getEstimatedTriggerTime() {
            return estimatedTriggerTime;
        }

        public WindowPhase getPhase() {
            return phase;
        }

        public int getTotalAccessCount() {
            return totalAccessCount;
        }
    }

    public enum WindowPhase {
        ACCUMULATING,
        PREFETCHING,
        TRIGGERING,
        EXPIRED
    }
}
