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

package org.apache.flink.contrib.streaming.state.cachekit.cache;

import it.unimi.dsi.fastutil.longs.Long2ByteLinkedOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ByteMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Collections;
import java.util.Map;

/**
 * LRU cache for a byte presence flag keyed by 64-bit fingerprints.
 */
public final class PrimitivePresenceCache implements CachePolicy<Long, Byte> {

    public static final byte ABSENT = (byte) 0;
    public static final byte PRESENT = (byte) 1;

    private final Long2ByteLinkedOpenHashMap delegate;
    private final int maxCapacity;
    private final java.util.function.BiConsumer<Long, Byte> evictionListener;

    public PrimitivePresenceCache(int maxCapacity, java.util.function.BiConsumer<Long, Byte> evictionListener) {
        this.maxCapacity = Math.max(0, maxCapacity);
        this.evictionListener = evictionListener;
        this.delegate = new Long2ByteLinkedOpenHashMap(Math.max(1, this.maxCapacity), 0.75f);
        this.delegate.defaultReturnValue((byte) -1);
    }

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
                evictionListener.accept(eldestKey, eldestVal);
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
        return removed == -1 ? null : removed;
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public Iterable<Map.Entry<Long, Byte>> entries() {
        if (delegate.isEmpty()) {
            return Collections.emptyList();
        }
        return (Iterable) delegate.long2ByteEntrySet();
    }

    boolean containsKey(long key) {
        return delegate.containsKey(key);
    }

    Iterable<Long2ByteMap.Entry> primitiveEntries() {
        return delegate.long2ByteEntrySet();
    }

    ObjectIterator<Long2ByteMap.Entry> primitiveIterator() {
        return delegate.long2ByteEntrySet().iterator();
    }
}
