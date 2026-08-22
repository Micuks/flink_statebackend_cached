/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CacheKitArmChainCopyElisionTest {

    @Test
    void activatesOnlyOnAarch64ByDefault() {
        assertThat(new CacheKitArmChainCopyElision(true, true, "aarch64").isActive()).isTrue();
        assertThat(new CacheKitArmChainCopyElision(true, true, "arm64").isActive()).isTrue();
        assertThat(new CacheKitArmChainCopyElision(true, true, "amd64").isActive()).isFalse();
        assertThat(new CacheKitArmChainCopyElision(false, true, "aarch64").isActive()).isFalse();
    }

    @Test
    void admitsOnlyExactAuditedQ17Consumers() {
        assertThat(
                        CacheKitArmChainCopyElision.isEligibleClassName(
                                "org.apache.flink.table.runtime.operators.calc.StreamExecCalc$28"))
                .isTrue();
        assertThat(CacheKitArmChainCopyElision.isEligibleClassName("StreamExecCalc$76")).isTrue();
        assertThat(CacheKitArmChainCopyElision.isEligibleClassName("StreamExecExpand$87"))
                .isTrue();
        assertThat(CacheKitArmChainCopyElision.isEligibleClassName("StreamExecCorrelate$19"))
                .isTrue();
        assertThat(
                        CacheKitArmChainCopyElision.isEligibleClassName(
                                "org.apache.flink.table.runtime.operators.wmassigners."
                                        + "WatermarkAssignerOperator"))
                .isTrue();
        assertThat(
                        CacheKitArmChainCopyElision.isEligibleClassName(
                                "org.apache.flink.table.runtime.operators.sink.SinkOperator"))
                .isTrue();
    }

    @Test
    void failsClosedForUnknownOrLookalikeConsumers() {
        assertThat(
                        CacheKitArmChainCopyElision.isEligibleClassName(
                                "org.apache.flink.table.runtime.operators.calc."
                                        + "StreamExecCalc$unsafe"))
                .isFalse();
        assertThat(CacheKitArmChainCopyElision.isEligibleClassName("StreamExecCalc$lookalike"))
                .isFalse();
        assertThat(
                        CacheKitArmChainCopyElision.isEligibleClassName(
                                "org.apache.flink.streaming.api.operators.StreamMap"))
                .isFalse();
        assertThat(
                        CacheKitArmChainCopyElision.isEligibleClassName(
                                "org.apache.flink.streaming.api.operators.KeyedProcessOperator"))
                .isFalse();
        assertThat(CacheKitArmChainCopyElision.isEligibleClassName(null)).isFalse();
    }
}
