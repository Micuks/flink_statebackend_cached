package org.apache.flink.contrib.streaming.state;

import it.unimi.dsi.fastutil.longs.Long2ByteLinkedOpenHashMap;
import java.util.Map;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;
import java.util.List;
import java.util.ArrayList;
import java.util.AbstractCollection;
import java.util.Iterator;
import it.unimi.dsi.fastutil.bytes.ByteIterator;
import it.unimi.dsi.fastutil.longs.Long2ByteMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * A lightweight cache that stores a single byte (0 or 1) per 64-bit fingerprint key.
 *
 * <p>This implementation is intended as a drop-in replacement for the existing
 * {@code CachePolicy<Long, CacheEntry<Boolean>>} that is used by the key-presence cache
 * in {@link CachingInternalMapState}.  It keeps the API surface of {@link CachePolicy}
 * but internally delegates to {@link Long2ByteLinkedOpenHashMap} configured for
 * access-order (= true) so that eviction behaviour is identical to the former
 * {@link LRUMap} implementation.</p>
 *
 * <p>Memory footprint per entry is ~16 B (8 B key + 1 B value + overhead/alignment)
 * compared to ≥48 B with the boxed Boolean wrapper.  The map itself is NOT
 * thread-safe and must be used from the same thread that accesses Flink state.</p>
 */
public class PrimitivePresenceCache implements CachePolicy<Long, Byte> {

    /** Byte constants for absent/present flags */
    public static final byte ABSENT = (byte) 0;
    public static final byte PRESENT = (byte) 1;

    private final Long2ByteLinkedOpenHashMap delegate;
    private final int maxCapacity;

    public PrimitivePresenceCache(int maxCapacity) {
        this(maxCapacity, null);
    }

    public PrimitivePresenceCache(
            int maxCapacity,
            java.util.function.Consumer<Map.Entry<Long, Byte>> evictionListener) {
        this.maxCapacity = maxCapacity;
        // The last boolean parameter `accessOrder` is not available in the constructor of fastutil's Long2ByteLinkedOpenHashMap
        // Instead, get() and put() calls must be handled to update the access order.
        // We will use getAndMoveToLast() and putAndMoveToLast() to simulate access-order behavior.
        this.delegate = new Long2ByteLinkedOpenHashMap(Math.max(1, maxCapacity), 0.75f);
        this.delegate.defaultReturnValue((byte) -1); // -1 indicates "not set"
        this.evictionListener = evictionListener;
    }

    private final java.util.function.Consumer<Map.Entry<Long, Byte>> evictionListener;

    // ------------------------------------------------------------------ CachePolicy impl
    @Override
    public Byte get(Long key) {
        if (key == null) {
            return null;
        }
        byte v = delegate.getAndMoveToLast(key.longValue());
        return v == -1 ? null : v;
    }

    @Override
    public Byte put(Long key, Byte value) {
        if (key == null || value == null) {
            return null;
        }
        byte prev = delegate.putAndMoveToLast(key.longValue(), value.byteValue());
        if (maxCapacity > 0 && delegate.size() > maxCapacity) {
            long eldestKey = delegate.firstLongKey();
            byte eldestVal = delegate.removeFirstByte();
            if (evictionListener != null) {
                evictionListener.accept(new java.util.AbstractMap.SimpleImmutableEntry<>(eldestKey, eldestVal));
            }
        }
        return prev == -1 ? null : prev;
    }

    @Override
    public Byte remove(Long key) {
        if (key == null) {
            return null;
        }
        byte removed = delegate.remove(key.longValue());
        // For normal removals (explicit invalidation), do not treat as eviction; caller handles memory.
        return removed == -1 ? null : removed;
    }

    @Override
    public boolean containsKey(Long key) {
        return key != null && delegate.containsKey(key.longValue());
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public void clear() {
        if (evictionListener != null && !delegate.isEmpty()) {
            // Use an iterator to avoid creating a large array of keys on the heap.
            // The fastutil iterator is efficient and supports removal during iteration.
            final ObjectIterator<Long2ByteMap.Entry> iterator =
                    delegate.long2ByteEntrySet().iterator();
            while (iterator.hasNext()) {
                // The fastutil iterator reuses the same Entry object. It's safe to pass to the
                // listener as it's consumed immediately.
                evictionListener.accept(iterator.next());
                iterator.remove();
            }
        }
        // If the listener was null or the map was empty, ensure it's cleared.
        // If the loop ran, the map should already be empty.
        if (!delegate.isEmpty()) {
            delegate.clear();
        }
    }

    @Override
    public Iterable<Map.Entry<Long, Byte>> entrySet() {
        // fastutil's entry set implements java.util.Map.Entry so we can just return it
        // However, to make it a proper Iterable, we might need to wrap it.
        // The object2ObjectEntrySet returns ObjectSet<Long2ByteMap.Entry> which is iterable.
        return (Iterable) delegate.long2ByteEntrySet();
    }

    @Override
    public Byte computeIfAbsent(Long key, Function<? super Long, ? extends Byte> mappingFunction) {
        if (key == null) {
            return null;
        }
        byte current = delegate.get(key.longValue());
        if (current != -1) {
            delegate.getAndMoveToLast(key.longValue()); // maintain access order
            return current;
        }
        Byte newVal = mappingFunction.apply(key);
        if (newVal != null) {
            put(key, newVal);
        }
        return newVal;
    }

    @Override
    public Collection<Byte> values() {
        // This avoids creating a huge ArrayList and boxing all values upfront.
        // It boxes them one by one as the iterator is consumed.
        return new AbstractCollection<Byte>() {
            @Override
            public Iterator<Byte> iterator() {
                final ByteIterator primitiveIterator = delegate.values().iterator();
                return new Iterator<Byte>() {
                    @Override
                    public boolean hasNext() {
                        return primitiveIterator.hasNext();
                    }

                    @Override
                    public Byte next() {
                        return primitiveIterator.nextByte(); // Auto-boxed here
                    }
                };
            }

            @Override
            public int size() {
                return delegate.size();
            }

            @Override
            public boolean isEmpty() {
                return delegate.isEmpty();
            }
        };
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }
} 
