package org.apache.flink.contrib.streaming.state;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.Collection;

/**
 * A cache policy that performs no operations and stores no entries.
 * Useful for disabling a cache layer.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class NoOpCachePolicy<K, V> implements CachePolicy<K, V> {

    @Override
    public V get(K key) {
        return null;
    }

    @Override
    public V put(K key, V value) {
        // Typically, put returns the previous value associated with key, or null if there was no mapping for key.
        // For a NoOp cache, there's never a previous value.
        return null;
    }

    @Override
    public V remove(K key) {
        // Typically, remove returns the previous value associated with key, or null if there was no mapping for key.
        return null;
    }

    @Override
    public boolean containsKey(K key) {
        return false;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public void clear() {
        // No-op
    }

    @Override
    public Iterable<Map.Entry<K, V>> entrySet() {
        return Collections.emptySet();
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        // A NoOp cache never finds the key, so it would "compute" the value.
        // It doesn't store it, so it's effectively a pass-through for the computation.
        return mappingFunction.apply(key);
    }

    @Override
    public Collection<V> values() {
        return Collections.emptyList();
    }

    @Override
    public boolean isEmpty() {
        return true;
    }
} 