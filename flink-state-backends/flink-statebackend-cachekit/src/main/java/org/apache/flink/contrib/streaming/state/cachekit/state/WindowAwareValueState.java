/*
 * Licensed under the Apache License, Version 2.0.
 */

package org.apache.flink.contrib.streaming.state.cachekit.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.cachekit.window.PredictivePrefetcher;
import org.apache.flink.contrib.streaming.state.cachekit.window.WindowLifecycleTracker;
import org.apache.flink.contrib.streaming.state.cachekit.window.WindowWriteBuffer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalValueState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;

/**
 * Window-semantic-aware wrapper for {@link InternalValueState}.
 *
 * <p>For AAR (Append and Aligned Read) scenarios, writes accumulate in a per-window
 * {@link WindowWriteBuffer}. On window trigger, data is read directly from the buffer
 * (zero RocksDB I/O). The buffer is flushed to the delegate only when evicted or at
 * checkpoint time.
 *
 * <p>For AUR (Append and Unaligned Read) scenarios, the {@link PredictivePrefetcher}
 * is checked before falling through to the delegate.
 *
 * <p>For RMW and non-window namespaces, this wrapper is transparent and delegates
 * directly to the underlying state.
 */
public final class WindowAwareValueState<K, N, V> implements InternalValueState<K, N, V> {

    private final InternalValueState<K, N, V> delegate;
    private final CurrentKeyProvider<K> currentKeyProvider;
    private final TypeSerializer<V> valueSerializer;

    @Nullable
    private final WindowWriteBuffer<K, N> writeBuffer;
    @Nullable
    private final PredictivePrefetcher<K, N> prefetcher;
    @Nullable
    private final WindowLifecycleTracker windowTracker;

    /** Callback to flush a window buffer entry to the delegate. */
    private final java.util.function.Consumer<K> keyContextSetter;

    private N currentNamespace;

    public WindowAwareValueState(
            InternalValueState<K, N, V> delegate,
            CurrentKeyProvider<K> currentKeyProvider,
            java.util.function.Consumer<K> keyContextSetter,
            @Nullable WindowWriteBuffer<K, N> writeBuffer,
            @Nullable PredictivePrefetcher<K, N> prefetcher,
            @Nullable WindowLifecycleTracker windowTracker) {
        this.delegate = delegate;
        this.currentKeyProvider = currentKeyProvider;
        this.keyContextSetter = keyContextSetter;
        this.writeBuffer = writeBuffer;
        this.prefetcher = prefetcher;
        this.windowTracker = windowTracker;
        this.valueSerializer = delegate.getValueSerializer();
    }

    @Override
    public V value() throws IOException {
        K key = currentKeyProvider.getCurrentKey();

        // 1. Check WindowWriteBuffer (AAR fast path)
        if (writeBuffer != null) {
            byte[] cached = writeBuffer.get(key, currentNamespace);
            if (cached != null) {
                return deserialize(cached);
            }
        }

        // 2. Check PredictivePrefetcher (AUR path)
        if (prefetcher != null) {
            byte[] prefetched = prefetcher.get(key, currentNamespace);
            if (prefetched != null) {
                return deserialize(prefetched);
            }
        }

        // 3. Delegate (CacheKit + RocksDB)
        return delegate.value();
    }

    @Override
    public void update(V value) throws IOException {
        if (value == null) {
            clear();
            return;
        }

        K key = currentKeyProvider.getCurrentKey();

        // AAR: write to per-window buffer instead of delegate
        if (writeBuffer != null) {
            byte[] serialized = serialize(value);
            writeBuffer.put(key, currentNamespace, serialized);

            // Evict coldest window if over budget
            while (writeBuffer.needsEviction()) {
                WindowWriteBuffer.EvictionResult<K, N> evicted = writeBuffer.evictColdestWindow();
                if (evicted != null) {
                    flushWindowToDelegate(evicted.window, evicted.data);
                } else {
                    break;
                }
            }

            if (windowTracker != null) {
                windowTracker.onWindowWrite(currentNamespace);
            }
            return;
        }

        // Non-AAR: delegate directly
        delegate.update(value);
        if (windowTracker != null) {
            windowTracker.onWindowWrite(currentNamespace);
        }
    }

    @Override
    public void clear() {
        K key = currentKeyProvider.getCurrentKey();

        // Remove from write buffer if present
        // Note: we only remove the single key, not the whole window
        if (writeBuffer != null) {
            Map<K, byte[]> windowEntries = writeBuffer.getWindowEntries(currentNamespace);
            if (windowEntries != null) {
                windowEntries.remove(key);
            }
        }

        // Remove from prefetch buffer
        if (prefetcher != null) {
            // Single-key removal not supported in current API; window-level cleanup
            // happens on window expiry
        }

        delegate.clear();
    }

    @Override
    public void setCurrentNamespace(@Nonnull N namespace) {
        this.currentNamespace = namespace;
        delegate.setCurrentNamespace(namespace);

        if (windowTracker != null) {
            K key = currentKeyProvider.getCurrentKey();
            windowTracker.onWindowAccess(key, namespace);
        }
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return delegate.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return delegate.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return delegate.getValueSerializer();
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<V> safeValueSerializer) throws Exception {
        flush();
        return delegate.getSerializedValue(
                serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer, safeValueSerializer);
    }

    @Override
    public InternalKvState.StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        flush();
        return delegate.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    /**
     * Flushes all dirty window buffer data to the delegate.
     * Called before checkpoint and before operations that require delegate to be up-to-date.
     */
    public void flush() {
        if (writeBuffer == null) return;

        Map<N, WindowWriteBuffer.PerWindowData<K>> dirtyWindows = writeBuffer.getDirtyWindows();
        for (Map.Entry<N, WindowWriteBuffer.PerWindowData<K>> entry : dirtyWindows.entrySet()) {
            flushWindowToDelegate(entry.getKey(), entry.getValue());
            writeBuffer.markClean(entry.getKey());
        }
    }

    /**
     * Clears data for a specific window from all buffers.
     * Called when a window expires.
     */
    public void clearWindow(N window) {
        if (writeBuffer != null) {
            writeBuffer.removeWindow(window);
        }
        if (prefetcher != null) {
            prefetcher.removeByWindow(window);
        }
        if (windowTracker != null) {
            windowTracker.markExpired(window);
        }
    }

    @Nullable
    public WindowWriteBuffer<K, N> getWriteBuffer() {
        return writeBuffer;
    }

    @Nullable
    public PredictivePrefetcher<K, N> getPrefetcher() {
        return prefetcher;
    }

    private void flushWindowToDelegate(N window, WindowWriteBuffer.PerWindowData<K> data) {
        if (data == null || !data.isDirty()) return;

        K previousKey = currentKeyProvider.getCurrentKey();
        N previousNamespace = currentNamespace;

        try {
            delegate.setCurrentNamespace(window);
            for (Map.Entry<K, byte[]> entry : data.getEntries().entrySet()) {
                keyContextSetter.accept(entry.getKey());
                V val = deserialize(entry.getValue());
                delegate.update(val);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush window buffer to delegate", e);
        } finally {
            keyContextSetter.accept(previousKey);
            if (previousNamespace != null) {
                delegate.setCurrentNamespace(previousNamespace);
            }
        }
    }

    private byte[] serialize(V value) throws IOException {
        org.apache.flink.core.memory.DataOutputSerializer out =
                new org.apache.flink.core.memory.DataOutputSerializer(64);
        valueSerializer.serialize(value, out);
        return out.getCopyOfBuffer();
    }

    private V deserialize(byte[] bytes) throws IOException {
        org.apache.flink.core.memory.DataInputDeserializer in =
                new org.apache.flink.core.memory.DataInputDeserializer(bytes);
        return valueSerializer.deserialize(in);
    }
}
