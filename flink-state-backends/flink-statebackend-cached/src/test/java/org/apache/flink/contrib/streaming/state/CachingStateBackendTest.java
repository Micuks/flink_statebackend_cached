package com.micuks.flink.cachingstate;
} 

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CachingStateBackendTest {

    @Mock
    private StateBackend mockDelegateStateBackend;
    @Mock
    private Environment mockEnv;
    @Mock
    private TtlTimeProvider mockTtlTimeProvider;

    private final String TEST_JOB_NAME = "testJob";

    @Test
    void testCreateKeyedStateBackend_returnsCachingKeyedStateBackend() throws Exception {
        // TODO: Implement test
    }

    @Test
    void testCreateOperatorStateBackend_delegates() throws Exception {
        // TODO: Implement test
    }

    @Test
    void testCreateCheckpointStorage_delegates() throws IOException {
        // TODO: Implement test
    }
    
    @Test
    void testIsChangelogStateBackendEnabled_delegates() {
        // TODO: Implement test
    }

    // Potentially more tests if CachingStateBackend has more logic than just wrapping a delegate
    // and passing configured parameters.
} 
 
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CachingStateBackendTest {

    @Mock
    private StateBackend mockDelegateStateBackend;
    @Mock
    private Environment mockEnv;
    @Mock
    private TtlTimeProvider mockTtlTimeProvider;

    private final String TEST_JOB_NAME = "testJob";

    @Test
    void testCreateKeyedStateBackend_returnsCachingKeyedStateBackend() throws Exception {
        // TODO: Implement test
    }

    @Test
    void testCreateOperatorStateBackend_delegates() throws Exception {
        // TODO: Implement test
    }

    @Test
    void testCreateCheckpointStorage_delegates() throws IOException {
        // TODO: Implement test
    }
    
    @Test
    void testIsChangelogStateBackendEnabled_delegates() {
        // TODO: Implement test
    }

    // Potentially more tests if CachingStateBackend has more logic than just wrapping a delegate
    // and passing configured parameters.
 
 