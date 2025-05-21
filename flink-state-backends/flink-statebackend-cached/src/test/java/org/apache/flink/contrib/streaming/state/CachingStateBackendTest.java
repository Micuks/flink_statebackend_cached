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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProvider;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProviderImpl;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.SimpleUserCodeClassLoader;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/** Tests for the {@link CachingStateBackend}. */
class CachingStateBackendTest {

    @TempDir
    File temporaryFolder;

    private CachingStateBackend cachingStateBackend;
    private AbstractKeyedStateBackend<String> delegateKeyedStateBackend;
    private Environment mockEnv;
    private TestTaskStateManager taskStateManager;
    private JobID jobId;
    private StateBackend delegatePlainBackend; // For non-keyed state backend tests

    @BeforeEach
    void setUp() throws IOException {
        mockEnv = createMockEnvironment(temporaryFolder);
        jobId = new JobID();
        when(mockEnv.getJobID()).thenReturn(jobId);

        File localStateDir = new File(temporaryFolder, "localrecovery");
        assertTrue(localStateDir.mkdirs());
        LocalRecoveryDirectoryProvider directoryProvider =
                new LocalRecoveryDirectoryProviderImpl(localStateDir, jobId, new JobVertexID(), 0);
        taskStateManager = new TestTaskStateManager(new LocalRecoveryConfig(directoryProvider));

        MemoryStateBackend actualDelegateBackend = new MemoryStateBackend();
        delegateKeyedStateBackend = actualDelegateBackend.createKeyedStateBackend(mockEnv, jobId,
                "testOperator", StringSerializer.INSTANCE, 1, new KeyGroupRange(0, 0),
                mock(TaskKvStateRegistry.class), TtlTimeProvider.DEFAULT,
                new UnregisteredMetricsGroup(), Collections.emptyList(), new CloseableRegistry());
        delegatePlainBackend = actualDelegateBackend; // Keep a reference to the plain backend for
                                                      // other tests

        cachingStateBackend = new CachingStateBackend(delegatePlainBackend, 10, 100, 10, 20L);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (delegateKeyedStateBackend != null) {
            delegateKeyedStateBackend.dispose();
        }
        if (taskStateManager != null) {
            taskStateManager.close();
        }
        FileUtils.deleteDirectory(temporaryFolder);
    }

    @Test
    void testCreateKeyedStateBackend() throws Exception {
        TypeSerializer<String> keySerializer = StringSerializer.INSTANCE;
        MetricGroup metricGroup = new UnregisteredMetricsGroup();
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();

        AbstractKeyedStateBackend<String> keyedBackend = cachingStateBackend
                .createKeyedStateBackend(mockEnv, jobId, "testOperator", keySerializer, 2, // numKeyGroups
                        new KeyGroupRange(0, 1), mock(TaskKvStateRegistry.class),
                        TtlTimeProvider.DEFAULT, metricGroup, Collections.emptyList(),
                        cancelStreamRegistry);

        assertNotNull(keyedBackend);
        assertTrue(keyedBackend instanceof CachingKeyedStateBackend);

        keyedBackend.dispose();
        cancelStreamRegistry.close();
    }

    @Test
    void testCreateOperatorStateBackend() throws Exception {
        CachingStateBackend cachingBackendWithPlainDelegate =
                new CachingStateBackend(delegatePlainBackend, 10, 100, 10, 20L);

        assertNotNull(cachingBackendWithPlainDelegate.createOperatorStateBackend(mockEnv,
                "testOperator", Collections.emptyList(), new CloseableRegistry()));
    }

    @Test
    void testUseManagedMemoryDelegation() {
        AbstractStateBackend mockDelegate = mock(AbstractStateBackend.class);
        when(mockDelegate.useManagedMemory()).thenReturn(true);
        CachingStateBackend cachingBackend =
                new CachingStateBackend(mockDelegate, 10, 100, 10, 20L);
        assertTrue(cachingBackend.useManagedMemory());
        verify(mockDelegate).useManagedMemory();
    }

    @Test
    void testResolveCheckpointDelegation() throws IOException {
        StateBackend mockDelegate =
                mock(AbstractStateBackend.class,
                        withSettings().extraInterfaces(CheckpointStorage.class));
        CompletedCheckpointStorageLocation mockLocation =
                mock(CompletedCheckpointStorageLocation.class);
        String pointer = "testPointer";
        when(((CheckpointStorage) mockDelegate).resolveCheckpoint(pointer))
                .thenReturn(mockLocation);

        CachingStateBackend cachingBackend =
                new CachingStateBackend(mockDelegate, 10, 100, 10, 20L);
        CompletedCheckpointStorageLocation resolvedLocation =
                cachingBackend.resolveCheckpoint(pointer);

        assertEquals(mockLocation, resolvedLocation);
        verify((CheckpointStorage) mockDelegate).resolveCheckpoint(pointer);
    }

    @Test
    void testCreateCheckpointStorageDelegation() throws IOException {
        StateBackend mockDelegate =
                mock(AbstractStateBackend.class,
                        withSettings().extraInterfaces(CheckpointStorage.class));
        CheckpointStorageAccess mockStorageAccess = mock(CheckpointStorageAccess.class);
        JobID jobID = new JobID();

        when(((CheckpointStorage) mockDelegate).createCheckpointStorage(jobID))
                .thenReturn(mockStorageAccess);

        CachingStateBackend cachingBackend =
                new CachingStateBackend(mockDelegate, 10, 100, 10, 20L);
        CheckpointStorageAccess createdStorageAccess =
                cachingBackend.createCheckpointStorage(jobID);

        assertEquals(mockStorageAccess, createdStorageAccess);
        verify((CheckpointStorage) mockDelegate).createCheckpointStorage(jobID);
    }

    private static Environment createMockEnvironment(File tempFolder) throws IOException {
        TaskManagerRuntimeInfo taskManagerRuntimeInfo = mock(TaskManagerRuntimeInfo.class);
        when(taskManagerRuntimeInfo.getTmpWorkingDirectory()).thenReturn(tempFolder);

        Environment env = mock(Environment.class);
        when(env.getJobID()).thenReturn(new JobID());
        when(env.getJobVertexId()).thenReturn(new JobVertexID());
        when(env.getExecutionConfig()).thenReturn(new ExecutionConfig());
        when(env.getUserCodeClassLoader()).thenReturn(
                SimpleUserCodeClassLoader.create(CachingStateBackendTest.class.getClassLoader()));
        when(env.getMetricGroup())
                .thenReturn(UnregisteredMetricGroups.createUnregisteredTaskMetricGroup());
        when(env.getTaskKvStateRegistry()).thenReturn(mock(TaskKvStateRegistry.class));
        when(env.getTaskManagerInfo()).thenReturn(taskManagerRuntimeInfo);

        org.apache.flink.runtime.state.TaskStateManager mockTaskStateManager =
                mock(org.apache.flink.runtime.state.TaskStateManager.class);

        @SuppressWarnings("rawtypes")
        StateChangelogStorage mockRawStateChangelogStorage =
                Mockito.mock(StateChangelogStorage.class);
        when(mockTaskStateManager.getStateChangelogStorage())
                .thenReturn(mockRawStateChangelogStorage);
        when(env.getTaskStateManager()).thenReturn(mockTaskStateManager);

        return env;
    }
}
