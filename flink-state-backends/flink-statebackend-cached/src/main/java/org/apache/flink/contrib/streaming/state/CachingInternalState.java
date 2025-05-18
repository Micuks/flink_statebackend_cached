package com.micuks.flink.cachingstate;

import org.apache.flink.runtime.state.internal.InternalKvState;
import java.io.IOException;

// K = Key type of the Flink state
// N = Namespace type
// SV = State Value type (e.g., V for ValueState, java.util.Map<UK,UV> for MapState, java.util.List<V_ELE> for ListState)
// S_DEL = Type of the delegate state being wrapped (e.g., InternalValueState, InternalMapState, InternalListState)
public interface CachingInternalState<K, N, SV, S_DEL extends InternalKvState<K, N, SV>> extends InternalKvState<K, N, SV> {

    void flushToUnderlyingState() throws IOException;

    S_DEL getDelegateState();
} 