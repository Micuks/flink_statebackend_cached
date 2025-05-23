package org.apache.flink.contrib.streaming.state;

import java.util.Map;

/**
 * Interface for cache policies used in CachingStateBackend.
 */
public interface CachePolicy<K, V> {
    V get(K key);

    V put(K key, V value);

    V remove(K key);

    boolean containsKey(K key);

    int size();

    void clear();

    Iterable<Map.Entry<K, V>> entrySet();

    V computeIfAbsent(K key, java.util.function.Function<? super K, ? extends V> mappingFunction);

    java.util.Collection<V> values();

    boolean isEmpty();
}
