package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Verifies that {@link CachingKeyedStateBackend#flushOnMiniBatchEnd()} flushes
 * buffered writes of all registered caching states (Map/Value/List/Aggregating)
 * to their respective delegate states.
 */
@ExtendWith(MockitoExtension.class)
class CachingKeyedStateBackendMiniBatchTest {

    // Delegate states (mocks) -------------------------------------------------
    @Mock private InternalMapState<String, String, String, String> mockMapDelegate;
    @Mock private InternalValueState<String, String, String> mockValueDelegate;
    @Mock private InternalListState<String, String, String> mockListDelegate;
    @Mock private InternalAggregatingState<String, String, String, String, String> mockAggDelegate;

    // Backend & serializers ---------------------------------------------------
    @Mock private AbstractKeyedStateBackend<String> mockAbstractDelegateBackend;
    @Mock private TypeSerializer<String> mockKeySerializer;
    @Mock private TypeSerializer<String> mockNamespaceSerializer;
    @Mock private TypeSerializer<String> mockValueSerializer;

    private CachingKeyedStateBackend<String> cachingBackend;

    // Caching state wrappers --------------------------------------------------
    private CachingInternalMapState<String, String, String, String> cachingMapState;
    private CachingInternalValueState<String, String, String> cachingValueState;
    private CachingInternalListState<String, String, String> cachingListState;
    private CachingInternalAggregatingState<String, String, String, String, String> cachingAggState;

    // Test constants ----------------------------------------------------------
    private static final String KEY = "testKey";
    private static final String NAMESPACE = "testNamespace";

    @BeforeEach
    void setUp() throws Exception {
        // Delegating backend basic stubbing -----------------------------------
        when(mockKeySerializer.duplicate()).thenReturn(mockKeySerializer);
        when(mockAbstractDelegateBackend.getKeySerializer()).thenReturn(mockKeySerializer);
        // Provide a simple key context so that backend.setCurrentKey() works.
        KeyGroupRange kgRange = new KeyGroupRange(0, 1);
        when(mockAbstractDelegateBackend.getKeyContext())
                .thenReturn(new org.apache.flink.runtime.state.heap.InternalKeyContextImpl<>(kgRange, 2));

        // Backend instantiation -----------------------------------------------
        TaskKvStateRegistry kvStateRegistry = mock(TaskKvStateRegistry.class);
        ExecutionConfig execConf = new ExecutionConfig();
        TtlTimeProvider ttl = TtlTimeProvider.DEFAULT;
        MetricGroup metrics = new UnregisteredMetricsGroup();
        CloseableRegistry cancelReg = new CloseableRegistry();

        int l1Size = 2;
        int l2Size = 2;
        int maxNs = 2;
        long maxMemMb = 10;
        CachingStateBackendFactory.CachePolicyType policy = CachingStateBackendFactory.CachePolicyType.LRU;

        cachingBackend = new CachingKeyedStateBackend<>(
                kvStateRegistry,
                mockKeySerializer,
                getClass().getClassLoader(),
                execConf,
                ttl,
                metrics,
                Collections.emptyList(),
                cancelReg,
                mockAbstractDelegateBackend,
                l1Size,
                l2Size,
                maxNs,
                maxMemMb,
                policy,
                /* map presence cache sizes */ 2, 2,
                /* bypass params */0.0, 5, 10,
                /* presence cache enabled */ false,
                /* bypass enabled */ false);

        cachingBackend.setCurrentKey(KEY);

        // Basic serializer stubs for delegates --------------------------------
        when(mockMapDelegate.getKeySerializer()).thenReturn(mockKeySerializer);
        when(mockMapDelegate.getNamespaceSerializer()).thenReturn(mockNamespaceSerializer);
        when(mockValueDelegate.getKeySerializer()).thenReturn(mockKeySerializer);
        when(mockValueDelegate.getNamespaceSerializer()).thenReturn(mockNamespaceSerializer);
        when(mockListDelegate.getKeySerializer()).thenReturn(mockKeySerializer);
        when(mockListDelegate.getNamespaceSerializer()).thenReturn(mockNamespaceSerializer);
        when(mockAggDelegate.getKeySerializer()).thenReturn(mockKeySerializer);
        when(mockAggDelegate.getNamespaceSerializer()).thenReturn(mockNamespaceSerializer);

        // Map serializer mocks -------------------------------------------------
        org.apache.flink.api.common.typeutils.base.MapSerializer<String,String> mapSerializer =
                mock(org.apache.flink.api.common.typeutils.base.MapSerializer.class);
        when(mapSerializer.getKeySerializer()).thenReturn(mockKeySerializer);
        when(mapSerializer.getValueSerializer()).thenReturn(mockValueSerializer);
        when(mockMapDelegate.getValueSerializer()).thenReturn(mapSerializer);

        // Value/List/Agg value serializers ------------------------------------
        @SuppressWarnings("unchecked")
        TypeSerializer<List<String>> mockListSer = mock(TypeSerializer.class);
        when(mockListDelegate.getValueSerializer()).thenReturn((TypeSerializer) mockListSer);
        when(mockValueDelegate.getValueSerializer()).thenReturn(mockValueSerializer);
        when(mockAggDelegate.getValueSerializer()).thenReturn(mockValueSerializer);

        // ---- Instantiate caching states -------------------------------------
        MetricGroup stateMetrics = metrics.addGroup("state");

        // Map State
        cachingMapState = new CachingInternalMapState<>(
                mockMapDelegate,
                cachingBackend,
                l1Size,
                l2Size,
                /* max flink keys per ns*/2,
                maxMemMb,
                policy,
                /* presence cache sizes */2,2,
                stateMetrics,
                /* bypass params */0.0,5,10,
                /* presence cache disabled */false,
                /* bypass disabled */false);
        cachingMapState.setCurrentNamespace(NAMESPACE);

        // Value State
        cachingValueState = new CachingInternalValueState<>(
                mockValueDelegate,
                cachingBackend,
                l1Size,
                l2Size,
                maxNs,
                maxMemMb,
                policy,
                /* bypass params */0.0,5,10,
                /* bypassEnabled */false,
                stateMetrics);
        cachingValueState.setCurrentNamespace(NAMESPACE);

        // List State
        cachingListState = new CachingInternalListState<>(
                mockListDelegate,
                cachingBackend,
                l1Size,
                l2Size,
                maxNs,
                policy);
        cachingListState.setCurrentNamespace(NAMESPACE);

        // Aggregating State
        AggregateFunction<String,String,String> aggFn = new AggregateFunction<String,String,String>() {
            @Override public String createAccumulator() { return ""; }
            @Override public String add(String value, String accumulator) { return accumulator + value; }
            @Override public String getResult(String accumulator) { return accumulator; }
            @Override public String merge(String a, String b) { return a + b; }
        };
        cachingAggState = new CachingInternalAggregatingState<>(
                mockAggDelegate,
                cachingBackend,
                aggFn,
                l1Size,
                l2Size,
                policy,
                stateMetrics);
        cachingAggState.setCurrentNamespace(NAMESPACE);

        // ---- Register states with backend via reflection --------------------
        Field rsField = CachingKeyedStateBackend.class.getDeclaredField("registeredStates");
        rsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<CachingInternalState<String,?, ?, ?>> registeredStates = (List<CachingInternalState<String,?, ?, ?>>) rsField.get(cachingBackend);
        registeredStates.add(cachingMapState);
        registeredStates.add(cachingValueState);
        registeredStates.add(cachingListState);
        registeredStates.add(cachingAggState);
    }

    @Test
    void testFlushOnMiniBatchEndFlushesAllStates() throws Exception {
        // 1) Perform buffered writes -----------------------------------------
        // Map
        cachingMapState.put("uk1", "uv1");
        // Value
        cachingValueState.update("val1");
        // List
        cachingListState.add("elem1");
        // Aggregating
        when(mockAggDelegate.getInternal()).thenReturn(null); // accumulator initially null
        cachingAggState.add("a");

        // No delegate interactions yet
        verifyNoInteractions(mockMapDelegate, mockValueDelegate, mockListDelegate, mockAggDelegate);

        // 2) flush mini-batch -----------------------------------------------
        cachingBackend.flushOnMiniBatchEnd();

        // 3) Verify delegate interactions ------------------------------------
        verify(mockMapDelegate, times(1)).put(eq("uk1"), eq("uv1"));
        verify(mockValueDelegate, times(1)).update(eq("val1"));
        verify(mockListDelegate, times(1)).update(any(List.class));
        verify(mockAggDelegate, times(1)).updateInternal(eq("a"));
    }
} 