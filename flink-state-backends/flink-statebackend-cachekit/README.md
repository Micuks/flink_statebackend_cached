# CacheKit 状态后端使用指南

CacheKit 是面向 Apache Flink 的状态后端扩展。它包装 Flink 的
`EmbeddedRocksDBStateBackend`，通过**多级缓存**、**C++ 原生请求平面（Native Request
Plane）**、**本地预聚合**以及**背压感知的异步预取**，用于降低 RocksDB 的 JNI 调用、
序列化和存储访问开销。

---

## 1. 核心架构与三阶段优化

CacheKit 将性能优化划分为三个正交的原生阶段（Stage），各阶段既可单独启用，也可组合使用；全部开启即为 **FullOpt（全面优化模式）**。

```
                    ┌────────────────────────────────────────────────────────┐
                    │                      Apache Flink                      │
                    └───────────────────────────┬────────────────────────────┘
                                                │
       ┌────────────────────────────────────────┼────────────────────────────────────────┐
       ▼                                        ▼                                        ▼
【Stage 3 跨记录协同】                  【Stage 1 原生点查缓存】                【Stage 2 原生快照缓存】
 • Mailbox 批处理                        • C++ 堆外高性能 ValueState 缓存         • 轻量级 MapSnapshot 缓存
 • 本地算子预聚合 (Local Pre-agg)        • 自适应冷热感知旁路 (Adaptive Bypass)   • 针对 EMPTY / SINGLE 小状态
 • 背压驱动异步预取 (BP-Prefetch)        • 减少热路径临时对象与 JNI 调用            • 内嵌在 JAR 中开箱即用
       │                                        │                                        │
       └───────────────────┬────────────────────┘                                        │
                           ▼                                                             │
             【共享 Native Request Plane】                                               │
                           │                                                             │
                           └────────────────────────────┬────────────────────────────────┘
                                                        ▼
                                       ┌────────────────────────────────┐
                                       │   EmbeddedRocksDBStateBackend  │
                                       └────────────────────────────────┘
```

### 优化阶段概览

| 阶段 | 核心机制 | 适用场景 / 解决痛点 | 依赖的原生库 |
|---|---|---|---|
| **Stage 1**<br>原生热点点查缓存 | 堆外 C++ 点查缓存 + 自适应旁路 | 频繁读写 ValueState 的热点场景；在无命中率的冷数据场景下自动旁路，避免无效探查开销 | `libcachekit_native_request_plane_jni.so` |
| **Stage 2**<br>精简原生快照缓存 | 针对 EMPTY/SINGLE Entry 的快照优化 | 存在大量空 Map 或单条目 MapState 的作业，减少序列化与状态创建开销 | **已内嵌在 JAR 中**<br>（无需单独编译/配置 .so） |
| **Stage 3**<br>跨记录协同与异步预取 | Mailbox 批处理 + 本地预聚合 + 批量预取 | 聚合算子（GroupAgg、去重）、高吞吐状态读写场景，将零散点查转为批量预取，隐藏存储延迟 | `libcachekit_native_request_plane_jni.so` |
| **FullOpt**<br>全面优化模式 | 同时开启 Stage 1 + Stage 2 + Stage 3 | 综合型复杂流计算任务，获得全方位的性能提升 | 需要上述独立 `.so` 库 |

> [!NOTE]
> **关于 FullOpt 的范围界定**：当前标准的 FullOpt 专注于 CacheKit 原生三阶段优化。它**不包含**历史研究性质的 Chen 组件（COW/RYW/优先队列优化），也**不包含**对 Flink 算子链深度侵入的特殊改动。如需对比特定实验分支，请显式标注为 `FullOpt+Chen`。

---

## 2. 编译与产物清单

在启用完整优化（FullOpt）时，集群需要部署以下两个核心产物：

| 产物文件 | 说明 | 部署位置 |
|---|---|---|
| `flink-statebackend-cachekit-1.16-SNAPSHOT.jar` | CacheKit Java 实现与 Flink 适配层。**已内嵌 Stage 2 的快照原生库**（运行时自动解压加载）。 | 所有节点的 `$FLINK_HOME/lib/` |
| `libcachekit_native_request_plane_jni.so` | Stage 1 与 Stage 3 共享的高性能 C++ 请求平面 JNI 动态库。 | 各节点统一的绝对路径（如 `/opt/cachekit/native/`）并在配置中指定 |

> [!WARNING]
> 原生动态库与底层 CPU 指令集强相关。请在与目标 TaskManager **相同架构**（x86-64 或 AArch64/鲲鹏）的机器与工具链上编译，切勿跨架构部署。

### 编译环境要求
- Linux 操作系统
- JDK 11
- CMake 3.16 及以上
- 支持 C++17 的编译器（GCC 8+ 或 Clang 10+）

### 编译步骤

#### 步骤一：编译 Java JAR 包
若首次在当前源码树下构建，建议先在根目录安装依赖模块：
```bash
./mvnw -DskipTests install
```
后续如仅修改 CacheKit 模块代码，可单独打包：
```bash
./mvnw -pl flink-state-backends/flink-statebackend-cachekit -DskipTests package
```
打包成功后，JAR 文件位于：
`flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT.jar`

#### 步骤二：编译 Request Plane 原生动态库（Stage 1 & 3 必需）
如果需要使用 Stage 1、Stage 3 或 FullOpt，需使用 CMake 编译 JNI 动态库：
```bash
# 1. 创建并配置构建目录
cmake \
  -S flink-state-backends/flink-statebackend-cachekit/src/main/native \
  -B flink-state-backends/flink-statebackend-cachekit/target/native-request-plane-jni \
  -DCMAKE_BUILD_TYPE=Release \
  -DCACHEKIT_NATIVE_BUILD_JNI=ON \
  -DCACHEKIT_NATIVE_BUILD_TESTS=OFF

# 2. 多核并行编译
cmake --build \
  flink-state-backends/flink-statebackend-cachekit/target/native-request-plane-jni \
  --parallel
```
编译完成后，动态库位于：
`flink-state-backends/flink-statebackend-cachekit/target/native-request-plane-jni/libcachekit_native_request_plane_jni.so`

---

## 3. 部署与配置指南

### 3.1 部署产物到集群
将编译好的产物同步分发到所有 JobManager 与 TaskManager 节点：

```bash
# 1. 部署 JAR 到 Flink lib 目录（若有旧版本请先清理）
cp flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT.jar \
   "$FLINK_HOME/lib/"

# 2. 部署 JNI 动态库到固定系统路径（以 /opt/cachekit 为例）
mkdir -p /opt/cachekit/native/
cp flink-state-backends/flink-statebackend-cachekit/target/native-request-plane-jni/libcachekit_native_request_plane_jni.so \
   /opt/cachekit/native/
chmod 0755 /opt/cachekit/native/libcachekit_native_request_plane_jni.so
```

### 3.2 基础状态后端配置
在集群各节点的 `conf/flink-conf.yaml` 中配置启用 CacheKit：

```yaml
# 指定状态后端工厂为 CacheKit
state.backend: org.apache.flink.contrib.streaming.state.cachekit.CacheKitStateBackendFactory

# 底层委托的存储引擎（默认即为 EmbeddedRocksDBStateBackend）
state.backend.cachekit.delegate: org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend

# 指定 Stage 1 / Stage 3 原生动态库的绝对路径
state.backend.cachekit.native.request-plane.library: /opt/cachekit/native/libcachekit_native_request_plane_jni.so
```

### 3.3 常用模式配置模板
根据业务需求，将对应阶段的配置项追加合并到 `flink-conf.yaml` 中：

- **模式 A：FullOpt 全面优化（强烈推荐）**
  - 完整配置文件示例：[`conf/flink-conf-cachekit-native-fullopt-example.yaml`](../../conf/flink-conf-cachekit-native-fullopt-example.yaml)
  - 同时开启 Stage 1、Stage 2 和 Stage 3 的各项协同能力。
- **模式 B：极简开箱 Stage 2（仅优化 Map 快照）**
  - 配置文件示例：[`conf/flink-conf-cachekit-native-stage2-example.yaml`](../../conf/flink-conf-cachekit-native-stage2-example.yaml)
  - **特点**：无需部署外部 `.so` 库，仅依靠 JAR 即可运行。
- **模式 C：仅开启 Stage 1（单点读缓存优化）**
  - 配置文件示例：[`conf/flink-conf-cachekit-native-stage1-example.yaml`](../../conf/flink-conf-cachekit-native-stage1-example.yaml)
- **模式 D：仅开启 Stage 3（跨记录协同与异步预取）**
  - 配置文件示例：[`conf/flink-conf-cachekit-native-stage3-example.yaml`](../../conf/flink-conf-cachekit-native-stage3-example.yaml)

> [!IMPORTANT]
> **配置注意事项**：
> 1. **必须完全重启集群**：修改 `flink-conf.yaml` 或替换 JAR / `.so` 之后，必须**停止并重新启动整个 Flink 集群**。仅取消并重新提交作业无法重新加载 JVM 已经加载的 Class 和 JNI 符号。
> 2. **Stage 2 原生快照与旧版互斥**：FullOpt 中开启的是 Stage 2 独立的快照缓存（`state.backend.cachekit.map.snapshot.cache.native.enabled: true`），旧版的 `state.backend.cachekit.native.map-snapshot.enabled` 必须保持 `false`。两者不可同时启用，否则配置校验会拒绝启动以防止重复缓存。
> 3. **Stage 1/3 没有各自单一总开关**：`state.backend.cachekit.native.request-plane.enabled`
>    是共享基础设施开关，各组件开关共同定义对应阶段。建议直接使用阶段示例文件。

---

## 4. 验证与排错

### 4.1 检查部署完整性
部署前可通过以下命令确认 JAR 包内组件齐全：
```bash
jar tf "$FLINK_HOME/lib/flink-statebackend-cachekit-1.16-SNAPSHOT.jar" \
  | grep -E 'CacheKitStateBackendFactory|META-INF/native/libcachekit_snapshot_jni.so'
```
预期应同时输出工厂类和内置的 snapshot `.so` 路径。

### 4.2 查看运行日志确认生效
启动集群并提交作业后，检查 TaskManager 日志（`log/flink-*-taskmanager-*.log`）：

1. **检查 JNI 库加载**：
   - 确认无 `java.lang.UnsatisfiedLinkError`。
   - 若启用了 Stage 1/3，应看到请求平面原生库加载成功的日志。
   - 若启用了 Stage 2，应看到内置快照库解压并成功加载的提示。
2. **检查状态后端激活**：
   - 日志中应打印使用 `CacheKitKeyedStateBackend` 代理 `RocksDBKeyedStateBackend` 的信息。
3. **正确性与容错测试**：
   - 执行一次 Savepoint 或触发几次 Checkpoint，并尝试从 Checkpoint / Savepoint 恢复作业，验证状态数据持久化与一致性工作正常。

### 4.3 常见问题排查 (FAQ)

- **Q1: 启动时抛出 `UnsatisfiedLinkError: ... libcachekit_native_request_plane_jni.so: cannot open shared object file`**
  - **排查**：检查 `state.backend.cachekit.native.request-plane.library` 是否配置了**正确的绝对路径**；检查该文件在每个 TaskManager 节点上是否存在且具备读取/执行权限（`chmod 0755`）。
- **Q2: 提示动态库 ELF 格式或架构不匹配（wrong ELF class）**
  - **排查**：编译机器与运行机器的 CPU 架构不一致（例如在 x86 机器上编译了 `.so` 拷贝到了鲲鹏 AArch64 机器）。请在同构环境下重新执行 CMake 编译。
- **Q3: 为什么修改了配置参数后，指标没有变化？**
  - **排查**：必须完全重启 TaskManager 进程，确保 JVM 加载了全新的类与配置。

---

## 5. 实测基准性能（2026-09-02，100M R1）

双机实验使用同一提交 `694a257d37476668846e1e7e719163f4bddd899f`、同一 JAR、100M
events、checkpoint 关闭、8 TM/16 slots。`Adaptive / Aligned` 是只改变 native ValueState
point-cache 自适应旁路开关的隔离增量；`Adaptive / reused RDB` 使用复用的同机 RocksDB
结果，只表示完整栈比较，不是该组件的单独收益。

| 平台 | q5 Adaptive K/s/core | q5 Aligned | q5 增量 | 达到 value-cache-off | 前八增量 | 后七增量 | 15Q 增量 | 15Q 完整栈 / reused RDB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Kunpeng | 73.94 | 51.74 | +42.91% | 94.88% | +5.77% | +0.17% | +3.16% | +28.91% |
| 云 x86 | 96.39 | 71.01 | +35.74% | 99.81% | +4.65% | +0.15% | +2.55% | +23.14% |

所有分组百分比均为逐 query 百分比的算术平均。两边均为 15/15 有效腿；所有腿
`real_job_completed=true`、8 TM coverage、cores 不超过 16.05，且 owned containers 在结束后为
0。Kunpeng 与 x86 的 q5 都证明 mutation attempted=applied、runtime failure=0。q4 在 Kunpeng 为
`+0.57%`、在 x86 为 `-2.03%`，因此不能把 q5 结论外推到所有 query。

历史 200M 表中的 `+40.52%` 是 Java FullOpt treatment=Off 相对非同期 RocksDB R1；该实验的
native request-plane、native Value/Map cache、native LocalPreAgg/Mailbox/Prefetch 均关闭。
它不能标成 all-native，也不能与其他完整栈百分比相加。

---

## 6. 核心配置参数速查表

以下为 CacheKit 最常用的核心配置项汇总：

| 配置键名 | 默认值 | 推荐值 | 作用说明 |
|---|---|---|---|
| `state.backend.cachekit.delegate` | *RocksDB* | - | 底层委托的状态后端，默认为 RocksDB |
| **[Stage 1]** `state.backend.cachekit.native.request-plane.enabled` | `false` | `true` | 是否启用 C++ 原生请求平面基础设施 |
| **[Stage 1]** `state.backend.cachekit.native.request-plane.library` | 空 | *绝对路径* | 原生 JNI 动态库文件的物理绝对路径 |
| **[Stage 1]** `state.backend.cachekit.value.cache.max-entries` | `0` | `8000` | ValueState 点查缓存最大容量 |
| **[Stage 1]** `state.backend.cachekit.native.value-cache.enabled` | `false` | `true` | 是否开启原生层 ValueState 热点缓存 |
| **[Stage 1]** `state.backend.cachekit.native.value-cache.adaptive-bypass.enabled` | `false` | `true` | 连续零有效命中窗口后旁路 native point probe/fill |
| **[Stage 2]** `state.backend.cachekit.map.snapshot.cache.native.enabled` | `false` | `true` | 是否启用精简原生 MapSnapshot 缓存 |
| **[Stage 2]** `state.backend.cachekit.map.snapshot.cache.max-entries` | `0` | `2000~8000` | Map 快照缓存条目数 |
| **[Stage 3]** `state.backend.cachekit.mailbox-batch.enabled` | `false` | `true` | 是否开启 Mailbox 批处理协同 |
| **[Stage 3]** `state.backend.cachekit.local-preagg.enabled` | `false` | `true` | 是否开启算子端本地预聚合（Local Pre-aggregation） |
| **[Stage 3]** `state.backend.cachekit.bp-prefetch.enabled` | `false` | `true` | 是否开启背压感知的状态批量预取 |
| **[Stage 3]** `state.backend.cachekit.bp-prefetch.async.enabled` | `false` | `true` | 预取是否采用真正异步线程池执行 |
| **[Stage 3]** `state.backend.cachekit.bp-prefetch.multiget.enabled` | `false` | `true` | 预取时是否合并为 RocksDB MultiGet 批查询 |

## 7. 合并与性能修复的验证结果

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
`Stage1+Stage2+Stage3` 合并提交的性能证明。当前分支的 exact-binary 实验结果单独列在下文，
并保留每个 query 两位小数的原始 K/s/core、CPU 和有效性证据。

## 8. 合并来源

- reduced native snapshot：`774b47015351e611dedcccbe2fd68ae4ca5dbec3`
- Stage 1 native hot point caches：`0f6eca13192b36e5401a4a991c8a13d8d6599100`
- Stage 3 native cross-record coordination：`940f514f2514efe18401918b417b2185e8ded733`

当前合并关系是代码和开关集合的 FullOpt 定义；它不替代 exact-binary 的 CDC、
checkpoint/restore、Nexmark 15Q 和性能验收。
