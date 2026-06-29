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

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.utils.TestingTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * T1: Does the non-copying by-reference output installed between fused sub-operators of a BATCH SQL
 * {@code MultipleInput} operator (gated by object-reuse, which {@code DefaultExecutor} forces ON in
 * batch) corrupt query output relative to the same plan WITHOUT fusion (MultipleInput disabled =
 * copying exchanges between operators = behavioral oracle B)?
 *
 * <p>Mechanism (source-confirmed): {@code MultipleInputStreamOperatorBase} line 213 reads {@code
 * getContainingTask().getExecutionConfig().isObjectReuseEnabled()} and installs a non-copying
 * {@code OneInputStreamOperatorOutput} between fused sub-operators when true, a {@code
 * CopyingOneInputStreamOperatorOutput} when false. {@code DefaultExecutor.configureBatchSpecificProperties}
 * (line 104) unconditionally calls {@code enableObjectReuse()} for batch, so the non-copying fused
 * edge is the stock default. The proven MI-forming plan fuses {@code HashJoin -> Calc -> HashJoin}
 * (see MultipleInputCreationTest#testBasicMultipleInput).
 *
 * <p>Oracle: same batch engine, same SQL, with {@code table.optimizer.multiple-input-enabled=false}
 * so no MI forms and operators are separated by copying exchanges.
 */
public class BatchMultipleInputObjectReuseITCase extends TestLogger {

    @ClassRule
    public static MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    private static Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("64m"));
        return config;
    }

    @Before
    public void before() throws Exception {
        registerSourceTables();
    }

    @After
    public void after() {
        TestValuesTableFactory.clearAllData();
    }

    private String x; // data ids captured so each TableEnvironment can re-use them

    private String y;

    private String z;

    private void registerSourceTables() {
        // x: large probe-ish side. (a, b, c)
        List<Row> xRows = new ArrayList<>();
        xRows.add(Row.of(1, 100L, "x1"));
        xRows.add(Row.of(2, 200L, "x2"));
        xRows.add(Row.of(3, 300L, "x3"));
        x = TestValuesTableFactory.registerData(xRows);

        // y: build side. (d, e, f)
        List<Row> yRows = new ArrayList<>();
        yRows.add(Row.of(1, 11L, "y1"));
        yRows.add(Row.of(2, 22L, "y2"));
        yRows.add(Row.of(3, 33L, "y3"));
        y = TestValuesTableFactory.registerData(yRows);

        // z: second build side. (g, h, i)
        List<Row> zRows = new ArrayList<>();
        zRows.add(Row.of(1, 111L, "z1"));
        zRows.add(Row.of(2, 222L, "z2"));
        zRows.add(Row.of(3, 333L, "z3"));
        z = TestValuesTableFactory.registerData(zRows);
    }

    private TableEnvironment newBatchTEnv(boolean multipleInputEnabled) {
        TableEnvironment tEnv =
                TestingTableEnvironment.create(
                        EnvironmentSettings.newInstance().inBatchMode().build(),
                        null,
                        TableConfig.getDefault());
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tEnv.getConfig()
                .set(ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_PIPELINED);
        // Force HashJoin (so the proven MI fusion forms). Disable broadcast so the joins keep
        // pipelined hash exchanges that MI fuses across.
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "NestedLoopJoin,SortMergeJoin");
        tEnv.getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1L);
        tEnv.getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED, multipleInputEnabled);

        createTable(tEnv, "x", "a INT, b BIGINT, c VARCHAR", x);
        createTable(tEnv, "y", "d INT, e BIGINT, f VARCHAR", y);
        createTable(tEnv, "z", "g INT, h BIGINT, i VARCHAR", z);
        return tEnv;
    }

    private static void createTable(
            TableEnvironment tEnv, String name, String schema, String dataId) {
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE %s (%s) WITH ("
                                + " 'connector' = 'values',"
                                + " 'data-id' = '%s',"
                                + " 'bounded' = 'true')",
                        name, schema, dataId));
    }

    private void createSink(TableEnvironment tEnv, String name, String schema) {
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE %s (%s) WITH ("
                                + " 'connector' = 'values',"
                                + " 'sink-insert-only' = 'false',"
                                + " 'bounded' = 'true')",
                        name, schema));
    }

    /**
     * The proven MI-forming plan: a multi-way INNER JOIN whose two HashJoins fuse with a pass-through
     * Calc between them (HashJoin -> Calc -> HashJoin), the exact members layout of
     * MultipleInputCreationTest#testBasicMultipleInput.
     */
    private static final String SQL =
            "SELECT * FROM"
                    + "  (SELECT a FROM x INNER JOIN y ON x.a = y.d) T1"
                    + "  INNER JOIN"
                    + "  (SELECT d FROM y INNER JOIN z ON y.d = z.g) T2"
                    + "  ON T1.a = T2.d";

    @Test
    public void testMultipleInputObjectReuseCorruption() throws Exception {
        // -------- ENABLED (stock batch default: MI forms, fused edge is NON-COPYING) --------
        TableEnvironment on = newBatchTEnv(true);

        String explainOn = on.explainSql(SQL);
        System.out.println("==== EXPLAIN (MultipleInput ENABLED) ====");
        System.out.println(explainOn);
        boolean miFormed = explainOn.contains("MultipleInput");
        System.out.println("MULTIPLE_INPUT_FORMED=" + miFormed);
        assertTrue(
                "MultipleInput operator did NOT form in the batch plan; cannot test the fused "
                        + "non-copying edge. Adjust the SQL / join strategy.",
                miFormed);

        createSink(on, "sink_on", "a INT, d INT");
        on.executeSql("INSERT INTO sink_on " + SQL).await(120, TimeUnit.SECONDS);
        List<String> outOn = new ArrayList<>(TestValuesTableFactory.getResults("sink_on"));
        Collections.sort(outOn);
        System.out.println("==== OUTPUT (ENABLED, MI fused non-copying) ==== " + outOn);

        // -------- DISABLED (oracle B: no MI, copying exchanges between operators) --------
        TableEnvironment off = newBatchTEnv(false);
        String explainOff = off.explainSql(SQL);
        System.out.println("==== EXPLAIN (MultipleInput DISABLED) ====");
        System.out.println(explainOff);
        System.out.println(
                "MULTIPLE_INPUT_IN_DISABLED_PLAN=" + explainOff.contains("MultipleInput"));

        createSink(off, "sink_off", "a INT, d INT");
        off.executeSql("INSERT INTO sink_off " + SQL).await(120, TimeUnit.SECONDS);
        List<String> outOff = new ArrayList<>(TestValuesTableFactory.getResults("sink_off"));
        Collections.sort(outOff);
        System.out.println("==== OUTPUT (DISABLED, oracle B copying) ==== " + outOff);

        // -------- DIFF --------
        boolean identical = outOn.equals(outOff);
        System.out.println("==== DIFF RESULT: identical=" + identical + " ====");
        if (!identical) {
            System.out.println("CORRUPTION_REACHABLE=YES");
            System.out.println("  ENABLED  : " + outOn);
            System.out.println("  DISABLED : " + outOff);
        } else {
            System.out.println(
                    "CORRUPTION_REACHABLE=NO (batch operators copy on ingest -> alias neutralized "
                            + "even though MI fused a non-copying edge)");
        }
        // We do NOT fail on identical: an identical result is an honest NEGATIVE (batch copies).
        // We print machine-greppable markers above for the harness to parse.
    }
}
