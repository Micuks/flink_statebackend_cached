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
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */

package org.apache.flink.contrib.streaming.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * A lazy copy-on-read wrapper for List that delays defensive copying until absolutely necessary.
 *
 * <p>This class provides a view over an underlying list without immediately copying it.
 * The copy is only created when:
 * <ul>
 *   <li>{@link #getCopy()} is called explicitly</li>
 *   <li>The list is modified through one of the mutation methods</li>
 *   <li>The list is serialized (toArray)</li>
 * </ul>
 *
 * <p>For read-only access patterns (which are the most common for ListState.get()),
 * this avoids the overhead of creating an ArrayList copy on every call.
 *
 * <p>Note: This class is NOT thread-safe for concurrent modifications.
 *
 * @param <T> element type
 */
public class LazyCopyList<T> implements List<T> {

    private final List<T> original;
    private volatile List<T> copy;

    /**
     * Creates a LazyCopyList wrapping the given list.
     *
     * @param original the list to wrap (must not be null)
     */
    public LazyCopyList(List<T> original) {
        if (original == null) {
            throw new IllegalArgumentException("Original list must not be null");
        }
        this.original = original;
    }

    /**
     * Returns the underlying list directly.
     *
     * <p><b>WARNING:</b> This returns the original list without copying.
     * Any modifications to the returned list will corrupt the cache state.
     * Only use this method when you are certain the caller will not modify the list.
     *
     * @return the original list
     */
    public List<T> getOriginal() {
        return original;
    }

    /**
     * Returns a copy of the list, creating it lazily on first call.
     *
     * @return a mutable copy of the list
     */
    public List<T> getCopy() {
        List<T> c = copy;
        if (c == null) {
            synchronized (this) {
                c = copy;
                if (c == null) {
                    c = new ArrayList<>(original);
                    copy = c;
                }
            }
        }
        return c;
    }

    // --- Read operations that operate directly on original ---

    @Override
    public int size() {
        return original.size();
    }

    @Override
    public boolean isEmpty() {
        return original.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return original.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        // Return iterator over original - safe for read-only use
        return original.iterator();
    }

    @Override
    public Object[] toArray() {
        return original.toArray();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> E[] toArray(E[] a) {
        return original.toArray(a);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return original.containsAll(c);
    }

    @Override
    public T get(int index) {
        return original.get(index);
    }

    @Override
    public int indexOf(Object o) {
        return original.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return original.lastIndexOf(o);
    }

    @Override
    public ListIterator<T> listIterator() {
        // Return iterator over original - safe for read-only use
        return original.listIterator();
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        return original.listIterator(index);
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        return original.subList(fromIndex, toIndex);
    }

    // --- Mutation operations that trigger copy-on-write ---

    @Override
    public boolean add(T t) {
        getCopy().add(t);
        return true;
    }

    @Override
    public boolean remove(Object o) {
        return getCopy().remove(o);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return getCopy().addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        return getCopy().addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return getCopy().removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return getCopy().retainAll(c);
    }

    @Override
    public void clear() {
        getCopy().clear();
    }

    @Override
    public T set(int index, T element) {
        return getCopy().set(index, element);
    }

    @Override
    public void add(int index, T element) {
        getCopy().add(index, element);
    }

    @Override
    public T remove(int index) {
        return getCopy().remove(index);
    }

    @Override
    public boolean equals(Object o) {
        return original.equals(o);
    }

    @Override
    public int hashCode() {
        return original.hashCode();
    }

    @Override
    public String toString() {
        return original.toString();
    }
}
