# CacheKit 状态后端使用指南

CacheKit 是面向 Apache Flink 的高性能状态后端扩展。它深度包装了 Flink 原生的 `EmbeddedRocksDBStateBackend`，通过**多级堆外缓存**、**C++ 原生请求平面（Native Request Plane）**、**本地预聚合**以及**背压感知的异步预取**，大幅降低 RocksDB 的 JNI 跨语言调用开销、序列化负担和深层 I/O 延迟。

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
 • 背压驱动异步预取 (BP-Prefetch)        • 零 GC、极低 JNI 调用开销               • 内嵌在 JAR 中开箱即用
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

## 5. 实测基准性能

在标准的 **Nexmark 15 Queries** 流处理基准测试中（100M events，8 TaskManagers / 16 Slots），CacheKit FullOpt 相比原生 RocksDB 取得了显著且稳定的吞吐提升。

### 5.1 鲲鹏与云 x86 双平台压测数据对比

| 评测维度 | 华为鲲鹏 (AArch64) | 云服务器 (x86-64) | 说明 |
|---|---:|---:|---|
| **Nexmark 15Q 全量平均吞吐提升** | **+28.91%** | **+23.14%** | 完整 FullOpt 相比原生 RocksDB 的端到端 TPS 增量 |
| **q5 单项吞吐提升（自适应优化）** | **+42.91%** | **+35.74%** | 从 51.74 提升至 73.94 K/s/core（自适应旁路生效） |
| **前八 Query 平均提升** | **+5.77%** | **+4.65%** | 针对纯计算与简单状态操作的平均增量 |
| **状态读写重度 Query（如 q15/q16/q17）** | **+30% ~ +300%** | **+30% ~ +200%** | 大状态查找、去重、复杂窗口下收益最为显著 |

> [!TIP]
> **关于 q5 的自适应冷热感知机制**：
> 在滑动窗口类负载（如 q5）中，存在大量天然的冷数据查找（Cold Miss）。传统缓存层如果盲目执行探查（Probe）、LRU 淘汰和填充（Fill），会徒增 JNI 和锁开销。
> CacheKit Stage 1 引入了**自适应旁路控制**：当监控窗口内命中率低于阈值时，自动旁路跳过原生缓存的逐条探查，直接走存储读，同时保留写穿更新与预取，彻底化解了负收益风险。

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
| **[Stage 1]** `state.backend.cachekit.value.hit-rate.threshold` | `0.0` | `0.03` | 自适应旁路触发的命中率阈值（低于此值转为旁路） |
| **[Stage 2]** `state.backend.cachekit.map.snapshot.cache.native.enabled` | `false` | `true` | 是否启用精简原生 MapSnapshot 缓存 |
| **[Stage 2]** `state.backend.cachekit.map.snapshot.cache.max-entries` | `0` | `2000~8000` | Map 快照缓存条目数 |
| **[Stage 3]** `state.backend.cachekit.mailbox-batch.enabled` | `false` | `true` | 是否开启 Mailbox 批处理协同 |
| **[Stage 3]** `state.backend.cachekit.local-preagg.enabled` | `false` | `true` | 是否开启算子端本地预聚合（Local Pre-aggregation） |
| **[Stage 3]** `state.backend.cachekit.bp-prefetch.enabled` | `false` | `true` | 是否开启背压感知的状态批量预取 |
| **[Stage 3]** `state.backend.cachekit.bp-prefetch.async.enabled` | `false` | `true` | 预取是否采用真正异步线程池执行 |
| **[Stage 3]** `state.backend.cachekit.bp-prefetch.multiget.enabled` | `false` | `true` | 预取时是否合并为 RocksDB MultiGet 批查询 |
