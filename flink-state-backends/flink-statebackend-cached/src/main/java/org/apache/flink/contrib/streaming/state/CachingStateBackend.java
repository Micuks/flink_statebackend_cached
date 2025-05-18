package com.micuks.flink.cachingstate;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend; // Delegate

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;

/**
 * A state backend that wraps another state backend (e.g., RocksDBStateBackend)
 * to provide an L1/L2 caching layer for deserialized objects.
 */
public class CachingStateBackend extends AbstractStateBackend {

    private final StateBackend delegateBackend; // This will be RocksDBStateBackend
    private final long l1CacheSize;
    private final long l2CacheSize;
    private final long maxCacheMemoryMb;

    public CachingStateBackend(StateBackend delegateBackend, long l1CacheSize, long l2CacheSize, long maxCacheMemoryMb) {
        this.delegateBackend = delegateBackend;
        this.l1CacheSize = l1CacheSize;
        this.l2CacheSize = l2CacheSize;
        this.maxCacheMemoryMb = maxCacheMemoryMb;
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws IOException {

        // Create the delegate keyed state backend
        AbstractKeyedStateBackend<K> delegateKeyedStateBackend = 
            (AbstractKeyedStateBackend<K>) delegateBackend.createKeyedStateBackend(
                env, jobID, operatorIdentifier, keySerializer, numberOfKeyGroups,
                keyGroupRange, kvStateRegistry, ttlTimeProvider, metricGroup, 
                stateHandles, cancelStreamRegistry);

        return new CachingKeyedStateBackend<>(
                kvStateRegistry,                             // 1. TaskKvStateRegistry
                keySerializer,                               // 2. TypeSerializer<K>
                env.getUserCodeClassLoader().asClassLoader(),// 3. ClassLoader
                env.getExecutionConfig(),                    // 4. ExecutionConfig
                ttlTimeProvider,                             // 5. TtlTimeProvider
                metricGroup,                                 // 6. MetricGroup
                stateHandles,                                // 7. Collection<KeyedStateHandle>
                cancelStreamRegistry,                        // 8. CloseableRegistry
                delegateKeyedStateBackend,                   // 9. AbstractKeyedStateBackend<K>
                l1CacheSize,                                 // 10. long
                l2CacheSize,                                 // 11. long
                maxCacheMemoryMb                             // 12. long
        );
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            Environment env,
            String operatorIdentifier,
            @Nonnull Collection<OperatorStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws Exception {
        // Operator state is not cached in this example, pass directly to delegate
        return delegateBackend.createOperatorStateBackend(env, operatorIdentifier, stateHandles, cancelStreamRegistry);
    }

    // Delegate other methods if AbstractStateBackend doesn't cover them or if specific logic is needed.
    // For example, checkpointing-related methods.

    @Override
    public boolean useManagedMemory() {
        return delegateBackend.useManagedMemory();
    }

    @Override
    public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException {
        // StateBackend interface declares this, so direct delegation is correct.
        return delegateBackend.resolveCheckpoint(externalPointer);
    }

    @Override
    public CheckpointStorageAccess createCheckpointStorage(@Nonnull JobID jobId) throws IOException {
        // StateBackend interface declares this, so direct delegation is correct.
        return delegateBackend.createCheckpointStorage(jobId);
    }
} 