package com.micuks.flink.cachingstate;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
// import com.micuks.flink.cachingstate.LRUMap; // Assuming this is in the same package
// import com.micuks.flink.cachingstate.CacheEntry; // Assuming this is in the same package
// import com.micuks.flink.cachingstate.CachingInternalValueState; // Assuming this is in the same package
import com.micuks.flink.cachingstate.CachingInternalState; // Added import

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map; // For CachingInternalMapState's SV type, and MapStateDescriptor
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The keyed state backend that implements caching.
 * It wraps a delegate AbstractKeyedStateBackend (e.g., RocksDBKeyedStateBackend)
 * and creates CachingInternal*State objects.
 */
public class CachingKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private final AbstractKeyedStateBackend<K> delegateKeyedStateBackend;
    private final int l1EntryCacheSize; // Renamed for clarity, used for ValueState entries, Map entries, List cache instances
    private final int l2EntryCacheSize; // Renamed for clarity
    // private final long maxCacheMemoryMb; // Keep if used for global limit, otherwise remove if per-state cache handles memory
    private final int maxActiveNamespaceOrPerKeyCacheContainers; // Max N->K LRUMap size for Value/List, or N->K->PerKeyMapCache LRUMap size

    private final List<CachingInternalState<K, ?, ?, ?>> registeredStates;

    public CachingKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            @Nonnull CloseableRegistry cancelStreamRegistry,
            AbstractKeyedStateBackend<K> delegateKeyedStateBackend,
            int l1EntryCacheSize, 
            int l2EntryCacheSize,
            int maxActiveNamespaceOrPerKeyCacheContainers
            // long maxCacheMemoryMb // if used globally
            ) {
        super(
            kvStateRegistry,
            keySerializer,
            userCodeClassLoader,
            executionConfig,
            ttlTimeProvider,
            metricGroup,
            stateHandles,
            cancelStreamRegistry,
            delegateKeyedStateBackend.getKeyGroupCompressionDecorator()
        );
        this.delegateKeyedStateBackend = delegateKeyedStateBackend;
        this.l1EntryCacheSize = l1EntryCacheSize;
        this.l2EntryCacheSize = l2EntryCacheSize;
        // this.maxCacheMemoryMb = maxCacheMemoryMb;
        this.maxActiveNamespaceOrPerKeyCacheContainers = maxActiveNamespaceOrPerKeyCacheContainers;
        this.registeredStates = new ArrayList<>();
    }

    @Nonnull
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"}) // V_SD is the value in StateDescriptor, UK/UV for Map, V_ELE for List
    public <N, S extends State, V_SD> S getOrCreateKeyedState(
        TypeSerializer<N> namespaceSerializer,
        StateDescriptor<S, V_SD> stateDescriptor)
        throws Exception {

        InternalKvState<K, N, ?> actualStateRaw =
            (InternalKvState<K, N, ?>) delegateKeyedStateBackend.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);

        if (stateDescriptor.getType() == StateDescriptor.Type.VALUE && actualStateRaw instanceof InternalValueState) {
            InternalValueState<K, N, V_SD> actualState = (InternalValueState<K, N, V_SD>) actualStateRaw;
            CachingInternalValueState<K, N, V_SD> cachingState = new CachingInternalValueState<>(
                    actualState,
                    this,
                    l1EntryCacheSize, // Max L1 entries (values) per (Namespace,Key) for this ValueState instance
                    l2EntryCacheSize, // Max L2 entries (values) per (Namespace,Key) for this ValueState instance
                    maxActiveNamespaceOrPerKeyCacheContainers // Max active Namespaces for this ValueState instance
            );
            synchronized (registeredStates) {
                boolean alreadyExists = registeredStates.stream().anyMatch(st -> st.getDelegateState() == actualState);
                if (!alreadyExists) {
                    registeredStates.add(cachingState);
                }
            }
            return (S) cachingState;
        } else if (stateDescriptor.getType() == StateDescriptor.Type.MAP && actualStateRaw instanceof InternalMapState) {
            // For MapStateDescriptor<UK, UV>, V_SD is Map<UK, UV>.
            // We need to cast actualStateRaw to its specific InternalMapState type.
            InternalMapState<K, N, Object, Object> actualState = (InternalMapState<K, N, Object, Object>) actualStateRaw;

            CachingInternalMapState<K, N, Object, Object> cachingMapState = new CachingInternalMapState<>(
                actualState,
                this,
                l1EntryCacheSize, // Max L1 entries (UK-UV pairs) per (Namespace,Key) for this MapState instance
                l2EntryCacheSize, // Max L2 entries (UK-UV pairs) per (Namespace,Key) for this MapState instance
                maxActiveNamespaceOrPerKeyCacheContainers // Max active (Namespace,Key) combinations holding map caches
            );
            synchronized (registeredStates) {
                boolean alreadyExists = registeredStates.stream().anyMatch(st -> st.getDelegateState() == actualState);
                if (!alreadyExists) {
                    registeredStates.add(cachingMapState);
                }
            }
            return (S) cachingMapState;
        } else if (stateDescriptor.getType() == StateDescriptor.Type.LIST && actualStateRaw instanceof InternalListState) {
            // For ListStateDescriptor<V_ELE>, V_SD is List<V_ELE>.
            InternalListState<K, N, Object> actualState = (InternalListState<K, N, Object>) actualStateRaw;
            
            CachingInternalListState<K, N, Object> cachingListState = new CachingInternalListState<>(
                actualState,
                this,
                l1EntryCacheSize, // Max L1 <K, CacheEntry<List>> entries per Namespace for this ListState instance
                l2EntryCacheSize, // Max L2 <K, CacheEntry<List>> entries per Namespace for this ListState instance
                maxActiveNamespaceOrPerKeyCacheContainers // Max active Namespaces for this ListState instance
            );
            synchronized (registeredStates) {
                boolean alreadyExists = registeredStates.stream().anyMatch(st -> st.getDelegateState() == actualState);
                if (!alreadyExists) {
                    registeredStates.add(cachingListState);
                }
            }
            return (S) cachingListState;
        }
        return (S) actualStateRaw;
    }
    
    // --- Methods to delegate to underlyingKeyedStateBackend ---
    // Most methods of AbstractKeyedStateBackend should be delegated. Some might require
    // interaction with the cache (e.g., snapshotting).

    @Override
    public void setCurrentKey(K newKey) {
        super.setCurrentKey(newKey);
        delegateKeyedStateBackend.setCurrentKey(newKey); // Keep delegate in sync
    }

    @Override
    public void dispose() {
        super.dispose();
        delegateKeyedStateBackend.dispose();
        synchronized (registeredStates) {
            registeredStates.clear();
        }
    }

    @Nonnull
    @Override
    public <N, S extends State, V_SD_UNUSED> S createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, V_SD_UNUSED> stateDesc,
            @Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<V_SD_UNUSED> snapshotTransformFactory)
            throws Exception {
        return delegateKeyedStateBackend.createOrUpdateInternalState(namespaceSerializer, stateDesc, snapshotTransformFactory);
    }

    @Nonnull
    @Override
    public <T> KeyGroupedInternalPriorityQueue<T> create(
            @Nonnull String stateName,
            @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        return delegateKeyedStateBackend.create(stateName, byteOrderedElementSerializer);
    }

    @Nonnull
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed>
            KeyGroupedInternalPriorityQueue<T> createPqState(
            @Nonnull String stateName,
            @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        return delegateKeyedStateBackend.create(stateName, byteOrderedElementSerializer);
    }

    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions) throws Exception {
        
        synchronized (registeredStates) {
            for (CachingInternalState<K, ?, ?, ?> state : registeredStates) {
                try {
                    state.flushToUnderlyingState();
                } catch (IOException e) {
                    System.err.println("Error flushing state " + state + " during snapshot: " + e.getMessage());
                }
            }
        }
        return delegateKeyedStateBackend.snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
    }

    // Other delegated methods (many of them, simplified here for brevity)
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        delegateKeyedStateBackend.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        delegateKeyedStateBackend.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        return delegateKeyedStateBackend.getKeys(state, namespace);
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String stateName) { 
        return delegateKeyedStateBackend.getKeysAndNamespaces(stateName);
    }

    @Override
    public void registerKeySelectionListener(KeySelectionListener<K> listener) {
        delegateKeyedStateBackend.registerKeySelectionListener(listener);
    }

    @Override
    public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
        return delegateKeyedStateBackend.deregisterKeySelectionListener(listener);
    }

    @Override
    public SavepointResources<K> savepoint() throws Exception {
        synchronized (registeredStates) {
             for (CachingInternalState<K, ?, ?, ?> state : registeredStates) {
                try {
                    state.flushToUnderlyingState();
                } catch (IOException e) {
                    System.err.println("Error flushing state " + state + " during savepoint: " + e.getMessage());
                }
            }
        }
        return delegateKeyedStateBackend.savepoint();
    }
} 