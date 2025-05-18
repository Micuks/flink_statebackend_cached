package com.micuks.flink.cachingstate;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * A simple LRU (Least Recently Used) cache map.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class LRUMap<K, V> extends LinkedHashMap<K, V> {
    private final int maxCapacity;
    private final Consumer<Map.Entry<K, V>> evictionListener;

    public LRUMap(int maxCapacity) {
        this(maxCapacity, null);
    }

    public LRUMap(int maxCapacity, Consumer<Map.Entry<K, V>> evictionListener) {
        super(maxCapacity, 0.75f, true); // true for access-order
        this.maxCapacity = maxCapacity;
        this.evictionListener = evictionListener;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        boolean remove = size() > maxCapacity;
        if (remove && evictionListener != null) {
            evictionListener.accept(eldest);
        }
        return remove;
    }

    public V getOrDefault(Object key, V defaultValue) {
        V v;
        return (((v = get(key)) != null) || containsKey(key))
            ? v
            : defaultValue;
    }
} 