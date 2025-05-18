package com.micuks.flink.cachingstate;

/**
 * Represents an entry in the cache.
 *
 * @param <V> The type of the cached value.
 */
public class CacheEntry<V> {
    private V value;
    private boolean dirty;

    public CacheEntry(V value, boolean dirty) {
        this.value = value;
        this.dirty = dirty;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public static <V> CacheEntry<V> clean(V value) {
        return new CacheEntry<>(value, false);
    }

    public static <V> CacheEntry<V> dirty(V value) {
        return new CacheEntry<>(value, true);
    }
} 