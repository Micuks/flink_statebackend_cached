/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.cache;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** Simple access-ordered LRU cache with max entry count. */
public final class LruCachePolicy<K, V> implements CachePolicy<K, V> {

    private final int maxEntries;
    private final int maxEntriesWithOverflow;
    private final LinkedHashMap<K, V> map;
    private final java.util.function.BiConsumer<K, V> evictionListener;

    // Key monitoring data structures
    private final Map<K, Integer> keyToId = new ConcurrentHashMap<>();
    private final Map<K, List<AccessEvent>> keyAccessHistory = new ConcurrentHashMap<>();
    private int nextKeyId = 1;

    // Log file for key access events
    private final String logFilePath;
    private final BufferedWriter logWriter;
    private final Object logLock = new Object();

    /** Access event type enumeration. */
    public enum AccessEventType {
        GET,
        PUT,
        REMOVE
    }

    /** Represents a single access event for a key. */
    public static class AccessEvent {
        private final AccessEventType eventType;
        private final long timestamp;

        public AccessEvent(AccessEventType eventType, long timestamp) {
            this.eventType = eventType;
            this.timestamp = timestamp;
        }

        public AccessEventType getEventType() {
            return eventType;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "AccessEvent{eventType=" + eventType + ", timestamp=" + timestamp + "}";
        }
    }

    public LruCachePolicy(int maxEntries) {
        this(maxEntries, Math.max(1, maxEntries / 16), (k, v) -> {
        }, null);
    }

    public LruCachePolicy(int maxEntries, java.util.function.BiConsumer<K, V> evictionListener) {
        this(maxEntries, Math.max(1, maxEntries / 16), evictionListener, "./cache_access_key_log.txt");
    }

    public LruCachePolicy(
            int maxEntries,
            int overflowEntries,
            java.util.function.BiConsumer<K, V> evictionListener) {
        this(maxEntries, overflowEntries, evictionListener, null);
    }

    public LruCachePolicy(
            int maxEntries,
            int overflowEntries,
            java.util.function.BiConsumer<K, V> evictionListener,
            String logFilePath) {
        this.maxEntries = Math.max(0, maxEntries);
        int overflow = Math.max(0, overflowEntries);
        this.maxEntriesWithOverflow = this.maxEntries > 0 ? this.maxEntries + overflow : 0;
        this.evictionListener = evictionListener;
        this.map = new LinkedHashMap<K, V>(16, 0.75f, true);
        this.logFilePath = logFilePath;
        
        // Initialize log writer if log file path is provided
        if (logFilePath != null && !logFilePath.isEmpty()) {
            try {
                Path path = Paths.get(logFilePath);
                // Create parent directories if they don't exist
                if (path.getParent() != null) {
                    Files.createDirectories(path.getParent());
                }
                this.logWriter = new BufferedWriter(new FileWriter(logFilePath, true));
            } catch (IOException e) {
                throw new RuntimeException("Failed to initialize log file: " + logFilePath, e);
            }
        } else {
            this.logWriter = null;
        }
    }

    @Override
    public V get(K key) {
        V value = map.get(key);
        if (key != null) {
            recordAccess(key, AccessEventType.GET);
        }
        return value;
    }

    @Override
    public V put(K key, V value) {
        Objects.requireNonNull(key, "key");
        V previous = map.put(key, value);
        recordAccess(key, AccessEventType.PUT);
        evictIfNeeded();
        return previous;
    }

    @Override
    public V remove(K key) {
        V value = map.remove(key);
        if (key != null) {
            recordAccess(key, AccessEventType.REMOVE);
        }
        return value;
    }

    @Override
    public void clear() {
        map.clear();
        keyToId.clear();
        keyAccessHistory.clear();
        nextKeyId = 1;
    }

    /**
     * Closes the log file writer if it was opened.
     * Should be called when the cache is no longer needed to ensure all data is flushed.
     */
    public void close() {
        if (logWriter != null) {
            synchronized (logLock) {
                try {
                    logWriter.flush();
                    logWriter.close();
                } catch (IOException e) {
                    System.err.println("Failed to close log file: " + e.getMessage());
                }
            }
        }
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public Iterable<Map.Entry<K, V>> entries() {
        return Collections.unmodifiableSet(map.entrySet());
    }

    private void evictIfNeeded() {
        if (maxEntries <= 0 || map.size() <= maxEntriesWithOverflow) {
            return;
        }
        java.util.Iterator<Map.Entry<K, V>> iterator = map.entrySet().iterator();
        while (map.size() > maxEntries && iterator.hasNext()) {
            Map.Entry<K, V> entry = iterator.next();
            iterator.remove();
            if (evictionListener != null) {
                evictionListener.accept(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Records an access event for the given key.
     *
     * @param key the key being accessed
     * @param eventType the type of access event
     */
    private void recordAccess(K key, AccessEventType eventType) {
        // Assign key ID if this is the first time we see this key
        Integer keyId;
        if (!keyToId.containsKey(key)) {
            keyId = nextKeyId++;
            keyToId.put(key, keyId);
        } else {
            keyId = keyToId.get(key);
        }

        long timestamp = System.currentTimeMillis();
        
        // Record the access event
        keyAccessHistory.computeIfAbsent(key, k -> new ArrayList<>())
                .add(new AccessEvent(eventType, timestamp));

        // Write to log file if enabled
        if (logWriter != null) {
            synchronized (logLock) {
                try {
                    logWriter.write(String.format("%d\t%s\t%s\t%d%n", 
                            keyId, key, eventType, timestamp));
                    logWriter.flush();
                } catch (IOException e) {
                    // Log error but don't throw to avoid breaking cache operations
                    System.err.println("Failed to write to log file: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Gets the encoded ID for a key (key1=1, key2=2, etc.).
     *
     * @param key the key to get the ID for
     * @return the encoded ID, or null if the key has never been accessed
     */
    public Integer getKeyId(K key) {
        return keyToId.get(key);
    }

    /**
     * Gets the access history for a specific key.
     *
     * @param key the key to get access history for
     * @return a list of access events for the key, or an empty list if the key has never been accessed
     */
    public List<AccessEvent> getKeyAccessHistory(K key) {
        return keyAccessHistory.getOrDefault(key, Collections.emptyList());
    }

    /**
     * Gets all keys that have been accessed, mapped to their encoded IDs.
     *
     * @return an unmodifiable map of keys to their encoded IDs
     */
    public Map<K, Integer> getAllKeyIds() {
        return Collections.unmodifiableMap(keyToId);
    }

    /**
     * Gets the complete access history for all keys.
     *
     * @return an unmodifiable map of keys to their access history lists
     */
    public Map<K, List<AccessEvent>> getAllKeyAccessHistory() {
        return Collections.unmodifiableMap(keyAccessHistory);
    }

    /**
     * Gets the total number of unique keys that have been accessed.
     *
     * @return the number of unique keys accessed
     */
    public int getTotalKeysAccessed() {
        return keyToId.size();
    }
}
