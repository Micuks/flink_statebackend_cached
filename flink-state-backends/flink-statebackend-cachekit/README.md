# CacheKit 原生三阶段使用说明

CacheKit 是包装 `EmbeddedRocksDBStateBackend` 的状态后端。本分支把三阶段原生优化合并到
一个可部署栈中；每个阶段均可独立配置，`FullOpt` 表示 Stage 1、Stage 2、Stage 3 同时开启。

当前 `FullOpt` **不包含** Chen COW/RYW/priority-queue 优化，也不包含 Flink
`OperatorChain` 的 chain-copy-elision。若实验额外打开 Chen 组件，名称应明确写成
`FullOpt+Chen`，避免错误归因。

## 编译产物

FullOpt 在部署目录中需要两个文件：

| 文件 | 用途 | 是否放入 `flink/lib` |
|---|---|---|
| `flink-statebackend-cachekit-1.16-SNAPSHOT.jar` | CacheKit Java 代码、依赖重定位、Flink 补丁类，以及内嵌的 Stage 2 snapshot JNI 库 | 是 |
| `libcachekit_native_request_plane_jni.so` | Stage 1 和 Stage 3 共用的 request-plane JNI | 不要求；推荐放在各 TaskManager 相同的绝对路径，并在配置中显式指定 |

JAR 内已经包含 `META-INF/native/libcachekit_snapshot_jni.so`。Stage 2 在库路径留空时会把它
解压到临时目录并加载，因此不要再复制一份 snapshot `.so`。Stage 1/3 的 request-plane
库当前没有嵌入 JAR；仅把它放进 `flink/lib` 并不能保证出现在 JVM 的
`java.library.path` 中，推荐始终配置绝对路径。

原生库与 CPU 架构相关。应在与 TaskManager 相同的架构和兼容工具链上构建，不能把 x86-64
生成的 `.so` 部署到 AArch64/Kunpeng。

## 构建

要求 Linux、JDK 11、CMake 3.16+ 和支持 C++17 的编译器。完整 Flink 构建会先生成
CacheKit 打包时覆盖的 sibling module 类：

```bash
./mvnw -DskipTests install
```

已有本分支 sibling artifacts 时，可以只重新打 CacheKit：

```bash
./mvnw -pl flink-state-backends/flink-statebackend-cachekit -DskipTests package
```

生成的 JAR 位于：

```text
flink-state-backends/flink-statebackend-cachekit/target/
  flink-statebackend-cachekit-1.16-SNAPSHOT.jar
```

上述 Maven 构建会自动编译并嵌入 Stage 2 snapshot JNI。Stage 1/3 的 request-plane JNI
需要单独构建：

```bash
cmake \
  -S flink-state-backends/flink-statebackend-cachekit/src/main/native \
  -B flink-state-backends/flink-statebackend-cachekit/target/native-request-plane-jni \
  -DCMAKE_BUILD_TYPE=Release \
  -DCACHEKIT_NATIVE_BUILD_JNI=ON \
  -DCACHEKIT_NATIVE_BUILD_TESTS=OFF

cmake --build \
  flink-state-backends/flink-statebackend-cachekit/target/native-request-plane-jni \
  --parallel
```

生成的独立库为：

```text
flink-state-backends/flink-statebackend-cachekit/target/native-request-plane-jni/
  libcachekit_native_request_plane_jni.so
```

## 部署和启用

以下操作要在所有 JobManager/TaskManager 节点使用同一版本文件。先移除旧版 CacheKit JAR，
避免重复类，然后部署新产物：

```bash
cp flink-state-backends/flink-statebackend-cachekit/target/\
flink-statebackend-cachekit-1.16-SNAPSHOT.jar "$FLINK_HOME/lib/"

install -D -m 0755 \
  flink-state-backends/flink-statebackend-cachekit/target/native-request-plane-jni/\
libcachekit_native_request_plane_jni.so \
  "$FLINK_HOME/native/libcachekit_native_request_plane_jni.so"
```

在 `flink-conf.yaml` 中先启用 CacheKit 后端和 request-plane 的绝对路径：

```yaml
state.backend: org.apache.flink.contrib.streaming.state.cachekit.CacheKitStateBackendFactory
state.backend.cachekit.delegate: org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
state.backend.cachekit.native.request-plane.library: /absolute/flink/path/native/libcachekit_native_request_plane_jni.so
```

再把以下文件中需要的阶段配置合并到同一份 `flink-conf.yaml`：

- Stage 1：[`conf/flink-conf-cachekit-native-stage1-example.yaml`](../../conf/flink-conf-cachekit-native-stage1-example.yaml)
- Stage 2：[`conf/flink-conf-cachekit-native-stage2-example.yaml`](../../conf/flink-conf-cachekit-native-stage2-example.yaml)
- Stage 3：[`conf/flink-conf-cachekit-native-stage3-example.yaml`](../../conf/flink-conf-cachekit-native-stage3-example.yaml)
- FullOpt：[`conf/flink-conf-cachekit-native-fullopt-example.yaml`](../../conf/flink-conf-cachekit-native-fullopt-example.yaml)

这些文件是 treatment override，不是完整集群配置。修改配置和 JAR 后需要停止并重新启动
Flink 集群；仅重新提交作业不能替换已经加载的类和 JNI 库。

### 只用 Stage 2

Stage 2 不使用 request-plane，因此只部署 JAR 即可。保持：

```yaml
state.backend.cachekit.map.snapshot.cache.native.enabled: true
state.backend.cachekit.map.snapshot.cache.native.library-path: ""
state.backend.cachekit.native.request-plane.enabled: false
state.backend.cachekit.native.map-snapshot.enabled: false
```

### Stage 1、Stage 3 或 FullOpt

这三种配置都需要独立的 request-plane `.so`。Stage 1 和 Stage 3 没有各自单一的总开关：
`state.backend.cachekit.native.request-plane.enabled` 是共享基础设施开关，各组件开关共同定义
该阶段。应直接使用对应示例文件，避免只打开一半组件。

FullOpt 中 Stage 2 的 standalone snapshot 与旧 request-plane snapshot 不是叠加关系：

```yaml
state.backend.cachekit.map.snapshot.cache.native.enabled: true
state.backend.cachekit.native.map-snapshot.enabled: false
```

两者同时开启会被配置校验拒绝，防止双重 snapshot cache。

## 如何确认生效

部署后至少检查以下三层：

1. JAR 内容包含 `CacheKitStateBackendFactory`、覆盖的 Flink 类和
   `META-INF/native/libcachekit_snapshot_jni.so`。
2. TaskManager 日志没有 `UnsatisfiedLinkError`、JNI ABI mismatch 或重复类加载错误；Stage 2
   应出现 embedded snapshot JNI 的加载日志。
3. 用 savepoint/checkpoint 恢复和目标查询做功能验证，再做同拓扑、同输入、成对顺序反转的
   性能实验。代码成功加载不等价于性能收益已经成立。

可在部署前检查 JAR：

```bash
jar tf flink-state-backends/flink-statebackend-cachekit/target/\
flink-statebackend-cachekit-1.16-SNAPSHOT.jar \
  | grep -E 'CacheKitStateBackendFactory|META-INF/native/libcachekit_snapshot_jni.so'
```

## 合并与性能修复的验证结果

三阶段合并基线的验证覆盖如下；这些结果证明构建和相关测试通过，不是新的 Nexmark 性能结论：

| 范围 | 结果 |
|---|---:|
| CacheKit Java tests | 286，0 failure/error，17 个按平台跳过 |
| Stage 2 snapshot native tests | 1/1 PASS |
| Stage 1/3 request-plane native tests | 2/2 PASS |
| flink-runtime | 3/3 PASS |
| flink-streaming-java | 33/33 PASS |
| flink-statebackend-rocksdb | 22/22 PASS |
| flink-table-runtime | 24/24 PASS |

本次 Stage 1/3 hot-path review 的逐项问题、风险门槛和完成状态见仓库根目录
[`NATIVE_STAGE1_STAGE3_PERFORMANCE_TODO.md`](../../NATIVE_STAGE1_STAGE3_PERFORMANCE_TODO.md)。
修复后完整回归结果为：CacheKit Java 294 个测试、0 failure/error、17 个按测试条件跳过；
request-plane Debug + ASan/UBSan 与 Release + JNI 均为 2/2 CTest PASS；Stage 3
`LocalPreaggTest` 和 `StatePrefetcherTest` 合计 28/28 PASS。

`flink-table-planner` 在 Java 24 + Scala 2.12.7 环境中于源码 typecheck 前触发
`bad constant pool index`，属于当前工具链边界，不能写成 planner 已通过。

历史鲲鹏 Nexmark 原始 K/s/core、逐 query 提升和证据审计见
[`docs/benchmarks/cachekit-kunpeng-results-20260805/`](../../docs/benchmarks/cachekit-kunpeng-results-20260805/)。
这些报告来自更早的源码快照，其中部分 `FullOpt` 定义包含 Chen 组件，因此不能直接当作当前
`Stage1+Stage2+Stage3` 合并提交的性能证明。当前提交若要发布性能数字，需要按相同口径重新
跑 exact-binary 实验，并保留每个 query 两位小数的原始 K/s/core。

## 合并来源

- reduced native snapshot：`774b47015351e611dedcccbe2fd68ae4ca5dbec3`
- Stage 1 native hot point caches：`0f6eca13192b36e5401a4a991c8a13d8d6599100`
- Stage 3 native cross-record coordination：`940f514f2514efe18401918b417b2185e8ded733`

当前合并关系是代码和开关集合的 FullOpt 定义；它不替代 exact-binary 的 CDC、
checkpoint/restore、Nexmark 15Q 和性能验收。

## 2026-09-02 自适应 ValueState point-cache 验证

Stage 1 的 native ValueState point cache 位于 Java L1/L2 之后。q5 的残余冷 miss 会进入
native cache，但此前没有产生有效命中，反而承担 probe、fill 和 LRU 成本。本分支增加默认关闭的
自适应旁路：连续零命中窗口后停止逐条 native probe/fill，同时保留 mutation write-through 和
prepared prefetch；周期 trial 与定向 fingerprint probe 用于检测工作集变化并恢复 active 状态。

双机使用同一提交 `694a257d37476668846e1e7e719163f4bddd899f`、同一 JAR、100M events、
checkpoint 关闭、8 TM/16 slots。以下 `Adaptive / Aligned` 是本次单一开关的隔离增量；
`Adaptive / reused RDB` 是完整栈相对复用同机 RocksDB 的比较，不是本组件的单独收益。

| 平台 | q5 Adaptive K/s/core | q5 Aligned | q5 增量 | 达到 value-cache-off | 前八增量 | 后七增量 | 15Q 增量 | 15Q 完整栈 / reused RDB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Kunpeng | 73.94 | 51.74 | +42.91% | 94.88% | +5.77% | +0.17% | +3.16% | +28.91% |
| 云 x86 | 96.39 | 71.01 | +35.74% | 99.81% | +4.65% | +0.15% | +2.55% | +23.14% |

所有分组百分比均为逐 query 百分比的算术平均。两边均为 15/15 有效腿；所有腿
`real_job_completed=true`、8 TM coverage、cores 不超过 16.05，且 owned containers 在结束后为
0。Kunpeng 与 x86 的 q5 都证明 mutation attempted=applied、runtime failure=0。q4 在 Kunpeng 为
`+0.57%`、在 x86 为 `-2.03%`，因此仍是独立的未解释边界，不能把 q5 结论外推到所有 query。

历史 200M 表中的 `+40.52%` 是实测的 Java FullOpt treatment=Off 相对非同期 RocksDB R1；
该实验的 native request-plane、native Value/Map cache、native LocalPreAgg/Mailbox/Prefetch 均为
关闭。它不能标成 all-native，也不能与 Stage1native+FullOpt 或 Stage3native+FullOpt 的完整栈
百分比相加。真正的组件增量必须在同一 FullOpt parent 上只改变目标 native 开关后计算。
