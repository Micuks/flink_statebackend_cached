# Kunpeng Native snapshot cache：设计、实现与验证记录

基线为 `8be59704462a8f2c6228b6b42e1f0c32f9e32589`。P1--P4 开发分支为
`cachekit/dev_wutb_kunpeng_native`；P5 混合边界在
`cachekit/dev_wutb_kunpeng_native_hybrid` 独立工作树中推进。

## 目标与成功条件

Native 化的目标不是把 Java 代码机械改写成 C++，而是验证以下不等式在鲲鹏机器上是否成立：

```text
省掉的 Java probe/object/GC 成本
  > key 编码 + JNI 边界 + Native probe + 返回结果
```

最终成功条件是 q4、q9、q20 的 Nexmark 吞吐可重复提升。反汇编和 microbenchmark 只用于证明实现及实验有效。

## P0 基线审计

- snapshot cache 位于 `CachedInternalMapState`，当前实现是 Java `LruCachePolicy<KeyNamespace, MapSnapshot>`。
- lookup 只发生在同步 `iterator()/entries()/isEmpty()` 短路路径；现有 bp-prefetch 只服务 `CachedInternalValueState`，不能直接提供 MapState snapshot batch。
- cache 是派生数据，不参与 checkpoint；backend `close()` 已是未来释放 Native handle 的生命周期挂点。
- `newStoredKeyNamespace()` 对 `BinaryRowData` 有专门的 `copy()` 分支。由此推断 Nexmark Table/SQL 的真实 key 很可能是 binary row，而非可直接传入 JNI 的 primitive；必须通过运行日志确认 serializer 后才能选择生产 codec。
- P0 曾在 snapshot cache 启用时临时记录 key、namespace、userKey serializer 和 delegate
  类型；取得运行证据后已删除诊断代码，生产 Java 路径没有遗留日志开销。

## P1 最小原型边界

第一版原型不接 Flink，仅回答两个问题：

1. `long key + long namespace` 下，single JNI 的固定成本是多少；
2. 相同 Native SoA 表上 scalar、NEON、SVE（硬件支持时）的 lookup 是否存在稳定差异，batch 能否摊薄 JNI。

该 primitive 原型是 JNI 成本下限，不代表已经覆盖 Nexmark。它用于暴露成本结构，
而不是作为停止端到端开发的硬门槛。后续实现必须满足：

- q4/q9/q20 的 serializer 日志已确认；
- 若 key 是 `BinaryRowData`，已有无需每次新分配的 canonical-bytes/direct-buffer 方案；
- 随机 differential test 覆盖 hit、miss、hash collision、更新、删除和 wrap-around。

## 可归因实验组

| 组 | 归因 |
|---|---|
| Java scalar | Java 基线 |
| Native scalar generic | JNI + off-heap/SoA |
| Native scalar Kunpeng tune | `-march=armv8.2-a -mtune=tsv110` |
| Native NEON | 128-bit control group compare |
| Native SVE | 运行时检测后的可选宽向量 compare |

Native 库不得整体依赖 SVE。baseline/NEON 和 SVE 放在不同编译单元，由 `AT_HWCAP` 运行时分派；实验输出必须打印实际 kernel 和 SVE vector length。

## 2026-08-10 P1 原型结果

已在 `src/native/snapshot-cache/` 完成长整型 key/namespace 的固定容量 SoA 哈希表、
scalar/NEON/SVE probe、运行时 HWCAP 分派、JNI single/batch 接口、随机差分测试和
Java scalar 对照。该 long 表仍是成本下限实验；真实 Flink 路径使用后述 bytes 表。

测试机器上的 SVE vector length 为 32 bytes。20 万次随机操作的 scalar、NEON、SVE
结果均通过；反汇编确认 NEON 使用了 `cmeq v?.16b`，SVE 使用了 `ptrue`/`ld1b`，
因此结果不是“源码写了向量、实际退化成标量”。

固定容量 4096、装载 2048、75% hit 的 lookup 结果如下：

| 路径 | ns/op | Mops/s |
|---|---:|---:|
| Native core scalar | 21.53 | 46.46 |
| Native core NEON | 26.67 | 37.50 |
| Native core SVE | 26.76 | 37.36 |
| Java scalar SoA | 19.96 | 50.10 |
| JNI scalar single | 40.31 | 24.81 |
| JNI scalar batch 8 | 29.08 | 34.39 |
| JNI scalar batch 32 | 21.79 | 45.89 |
| JNI scalar batch 64 | 20.53 | 48.71 |
| JNI NEON batch 64 | 23.15 | 43.20 |
| JNI SVE batch 64 | 24.96 | 40.06 |

所有 JNI 组 checksum 都是 `411949377536`。结论是：在 50% 装载、短 probe chain
的表上，向量一次比较更多 control byte 的收益小于向量装载、mask 和分支成本；single
JNI 又约把 Java lookup 成本翻倍。batch 64 基本摊薄 JNI 后，Native scalar 仍比 Java
scalar 慢约 2.9%。因此 primitive long 路径本身没有收益，不能仅凭“鲲鹏向量能力强”
就声称优化成立；但这个结果不再阻止完成真实 RowData 端到端实现。
收尾复跑的 core 结果为 scalar/NEON/SVE `21.03/21.83/23.89 ns/op`；绝对差距有波动，
但 scalar 仍最快，因此不改变判定。

验证命令：

```bash
cmake -S src/native/snapshot-cache -B /tmp/cachekit-kunpeng-native-make-build \
  -DCMAKE_BUILD_TYPE=Release
cmake --build /tmp/cachekit-kunpeng-native-make-build -j
ctest --test-dir /tmp/cachekit-kunpeng-native-make-build --output-on-failure
/tmp/cachekit-kunpeng-native-make-build/cachekit_snapshot_bench
```

Java/JNI benchmark 使用 BiSheng JDK 8 运行 `NativeSnapshotBench`。带 serializer 诊断的 CacheKit JAR 也已
由 JDK 11 完整构建，SHA-256 为
`91a1fe22bce3f2c67127550d4c1bb7159ec7be895d1e625def6bdcf15336883a`。

## Nexmark 运行时类型证据

使用独立 Compose 项目 `ckkunpnativep0`、REST 端口 `19091`、SSH 端口
`19221`—`19223` 启动了 100 万事件的 q4/q9/q20 smoke。输入快照和日志位于：

```text
/home/wutb/nexmark-bench-v2/results/
  20260810T170619+0800_kunpeng-native-p0-serializer-smoke/
```

q4 的 TaskManager 实际日志一致显示：

```text
key=org.apache.flink.table.runtime.typeutils.RowDataSerializer
namespace=org.apache.flink.runtime.state.VoidNamespaceSerializer
userKey=org.apache.flink.table.runtime.typeutils.RowDataSerializer
delegate=org.apache.flink.contrib.streaming.state.RocksDBMapState
```

这推翻了 primitive long 可直接覆盖 Nexmark 的假设，并确认 namespace 可省略、真正问题
是两个 RowData 的稳定字节表示。q4 在类型证据产生后因 Nexmark metric reporter 没有采到
指标而退出异常；启动器随后进入 q9，但为避免把一次诊断烟测拖成 900 秒超时任务而终止，
所以这次运行**不能**用于 q4/q9/q20 吞吐比较。所有 `ckkunpnativep0` 容器和网络均已清理，
未操作已有长期任务。

## 第一性原理复核

一次 snapshot 命中的最低工作量，是确定 `(key, namespace)` 是否存在并拿到 snapshot
引用。向量化只能降低“一次探测中比较 control byte”的指令数，不能消除 hash、内存
访问、JNI、key 编码和对象/句柄转换。当前装载率下绝大多数 probe 很短，因而没有足够
的独立比较供 NEON/SVE 摊销固定开销。只有真实 workload 出现长 probe、可批量请求，
或 key 已天然位于连续 off-heap 内存时，Native/vector 路径才可能改变上述不等式。

更关键的是，`long + long` 是最有利于 Native 的假设。如果 q4/q9/q20 实际使用
`BinaryRowData`，生产实现还要支付 canonical encoding、变长比较和 snapshot Java
对象回传成本，收益只会更难成立。因此本实现先保证完整、可回退和可测，再由
q4/q9/q20 决定实际收益，不能从硬件规格直接外推结果。

## P2：真实 RowData 端到端实现

当前已经完成 snapshot cache 下沉，而不再只是 JNI microbenchmark。数据路径为：

```text
(currentKey, namespace)
  -> BinaryRowData + VoidNamespace 时直接使用 canonical row bytes
     （其他类型回退到 Flink TypeSerializer 的复用 byte[]）
  -> JNI
  -> C++ ByteSnapshotTable 查找/更新
  -> EMPTY，或 Native 持有的已深拷贝 userKey GlobalRef
  -> SINGLE 命中时直接返回原 Java userKey，再调用原有 point-get
```

C++ 层保存完整 key bytes、snapshot kind 和 SINGLE userKey 的 JNI GlobalRef；GlobalRef 在
更新、淘汰、remove、clear 和 close 时精确释放，Java 侧不维护镜像 Map。表具有固定 entry
上限、LRU 淘汰、deleted slot、scalar、NEON、SVE probe 和运行时 HWCAP 分派。
`CachedInternalMapState.close()` 会释放 Native handle；Java 入口同步保护
lookup/put/remove 与 close，避免取消阶段 use-after-free。

实现默认关闭，原有 Java snapshot cache 行为不变。启用配置为：

```yaml
state.backend.cachekit.map.snapshot.cache.max-entries: 2000
state.backend.cachekit.map.snapshot.cache.native.enabled: true
state.backend.cachekit.map.snapshot.cache.native.kernel: SCALAR # AUTO/SCALAR/NEON/SVE
state.backend.cachekit.map.snapshot.cache.native.library-path: /absolute/path/libcachekit_snapshot_jni.so
```

若 library-path 为空则使用 `System.loadLibrary("cachekit_snapshot_jni")`。显式要求的 NEON
或 SVE 在硬件不支持时会拒绝启动，不会静默伪装成向量路径；`AUTO` 才允许运行时选择。

## P2 验证结果

- Native core：long 表和 bytes 表两个 CTest 均通过；bytes 测试覆盖 EMPTY、SINGLE、
  变长 payload、更新、LRU 淘汰、删除和 clear，并分别运行 scalar/NEON/SVE。
- Java/JNI：`NativeMapSnapshotCacheTest` 覆盖 reusable buffer、GlobalRef 的替换/淘汰/
  remove/clear/close，以及带非零 offset 的真实 `BinaryRowData` key；SINGLE 命中返回的
  userKey 与写入对象保持 identity。
- Flink：`CachedInternalMapStateTest` 覆盖 Native SINGLE backfill 后第二次 `entries()`
  不再调用 delegate iterator。当前合计 22 项测试全部通过。
- API 兼容：保留 `CacheKitStateBackend` 和 `CacheKitKeyedStateBackend` 的旧构造器签名；
  旧调用路径统一委托到 Native 默认关闭的新构造器。
- JDK 11 reactor compile 与 package 均成功。2026-08-11 收尾构建的 JAR SHA-256 为
  `6fc4ce7372cc7a476025bf6afb79fd1abdc2bada39c22ab9fb1fcfbcf6539ca8`，Native 库为
  `bf5ead04a88a2b21dc2f4ebc84a53d885c7336fbde2f445825ef32324137ff4f`。

使用独立镜像 `nexmark-bench-v2:kunpeng-native-snapshot-20260810` 和 Compose 项目
`ckkunpnativee2e` 启动了真实 q4。TaskManager 日志确认多个 MapState 使用：

```text
CacheKit Native MapSnapshot cache initialized: maxEntries=2000, kernel=scalar
```

未发现 `UnsatisfiedLinkError`、JNI 异常、SIGSEGV 或 Flink FAILED。烟测完成 warmup 后，
Nexmark runner 长时间停在主查询切换阶段，因此保存日志后主动终止；该次运行目录为：

```text
/home/wutb/nexmark-bench-v2/results/
  20260810T192752+0800_kunpeng-native-e2e-smoke/
```

它证明 JAR、配置、动态库和真实 TaskManager RowData 路径能共同启动，但不是吞吐结果。
所有 `ckkunpnativee2e` 容器和网络均已清理。

## Java 与 Native snapshot 的关系

两份实现同时保留在代码中，用于开关回退和 A/B 测试，但同一个
`CachedInternalMapState` 实例只使用其中一份。`native.enabled=true` 时，Java
`mapSnapshotCache` 被设置成 `NoOpCachePolicy`，lookup/store/remove 只访问
`NativeMapSnapshotCache`；关闭 Native 时才创建 Java `LruCachePolicy`。因此不存在双写、
两份缓存互相覆盖或一致性冲突，也不会同时占用两份有效缓存容量。

## JNI 改造先例、当前 Native 边界与工程量

### Flink/RocksDB 已有先例

Flink 的常规 RocksDB MapState 没有自建一套范围查询 JNI：Java 先构造
`key + namespace` 的序列化 prefix，再通过 RocksJava JNI 创建 `RocksIterator`，调用
`seek/next/key`，并在 Java 中判断 key 是否仍匹配 prefix。也就是说，RocksDB 存储访问
已经在 C++，但范围语义和迭代控制仍在 Java。官方实现可参见
[`RocksDBMapState`](https://raw.githubusercontent.com/apache/flink/master/flink-state-backends/flink-statebackend-rocksdb/src/main/java/org/apache/flink/state/rocksdb/RocksDBMapState.java)。

Flink 为深层优化扩展 Native/JNI 有明确先例。State TTL 的 RocksDB compaction cleanup
要求实现 Flink 专用 C++ compaction filter；当时既采用过临时 RocksDB 分支 FRocksDB，
也规划过把独立 C++ 扩展打进 JNI Java client JAR。参见
[`FLINK-10471`](https://issues.apache.org/jira/browse/FLINK-10471)。对应 Flink 实现提交
约涉及 46 个文件、`+1232/-370` 行，而且这还不包含独立 FRocksDB 原生分支的改动。
后续又出现 Native compaction thread 回调 Java 时拿不到用户 ClassLoader 的问题，说明
JNI 深度接入的真实维护成本还包括线程上下文、ClassLoader、异常和对象生命周期，参见
[`FLINK-16686`](https://issues.apache.org/jira/browse/FLINK-16686)。

因此结论不是“Flink 不能改 JNI”，而是：有成功先例，但社区通常先复用 RocksJava 已有
接口；只有现有接口无法消除主要成本时，才承担定制 rocksdbjni 的构建和长期维护成本。

### 当前实现算到哪一层

P2 已经把 **snapshot cache 的索引表、查找、更新、淘汰及 GlobalRef 生命周期**下沉到
C++；当时 cache miss 后的 snapshot 构造仍依赖 Java 侧 MapState/RocksIterator。P3 已于
2026-08-11 接通下述一次 JNI 内的 prefix 分类，当前实现不再依赖 Java iterator backfill
来构造 EMPTY/SINGLE snapshot。

按本项目的最终目标，只有下面这段也在一次 Native 调用内完成，才能称为“完整的
snapshot native 化”：

```text
serialized prefix
  -> RocksDB iterator Seek(prefix)
  -> 检查第 1 条是否匹配
  -> 检查第 2 条是否仍匹配
  -> 返回 EMPTY / SINGLE(first userKey) / MULTI
```

因此 P2 应准确称为 **Native snapshot cache table** 或“部分 Native 化”；启用 P3
classifier 后，snapshot 的缓存表与 miss 分类链路都位于 Native，可称为完整 Native
snapshot 判定。SINGLE 后读取 value 仍复用 Flink point-get，MULTI 仍复用原 Flink iterator；
这两部分不是 snapshot 分类本身，不能混称为“整个 MapState 已 Native 化”。

### 范围语义是否必须大改 JNI

不需要把 Flink Serializer、任意 Comparator 或完整范围迭代器都暴露给 C++。当前
RocksDB MapState 的逻辑范围已经编码成规范化 prefix；Native 只需得到正确的 DB、
ColumnFamily、ReadOptions/一致性读取视图和 prefix bytes。最窄接口可以设计为：

```java
native PrefixResult classifyPrefix(
    long dbHandle,
    long columnFamilyHandle,
    long readOptionsHandle,
    byte[] prefix,
    int userKeyOffset);
```

返回值只表达 `EMPTY/SINGLE/MULTI`，SINGLE 附带第一条 userKey bytes 或安全 token；无需
扫描完整范围。RocksJava 对象公开底层 `nativeHandle`，参见
[`RocksObject.getNativeHandle()`](https://javadoc.io/static/org.rocksdb/rocksdbjni/7.0.3/org/rocksdb/RocksObject.html)，
但“能取得指针”不等于“独立动态库能安全解引用指针”。如果 CacheKit JNI `.so` 与
Flink 实际加载的 rocksdbjni 不是同一个 RocksDB 二进制、编译选项和 C++ ABI，直接传入
DB 指针存在未定义行为、崩溃和升级失效风险。

生产实现优先把 `classifyPrefix` 加入 **Flink 实际使用的同一份 rocksdbjni/FRocksDB
构建**，或者由该构建导出稳定的窄 C 接口；不得让独立 CacheKit `.so` 猜测并解引用外部
C++ 对象布局，也不得重新打开一个 RocksDB 实例冒充原 backend 的一致性读取视图。

### 工程量估算

以下是基于当前代码边界的工程估算，不是官方工期：

| 方案 | 是否完整 Native snapshot 判定 | 预计改动/周期 |
|---|---|---|
| 继续复用 RocksJava iterator，Java 中只看前两条 | 否；仅减少无效全范围扫描 | 约 150–300 行，3–7 天 |
| 在同一 rocksdbjni 中新增窄 `classifyPrefix` | 是；一次 JNI 内完成 0/1/2+ 判定 | 约 300–700 行；鲲鹏内部原型 2–4 周 |
| 完整 Native range iterator、缓存和异步 ownership | 是，但接口更通用也更复杂 | 约 1000–3000+ 行，1–2 个月以上 |
| 长期维护定制 rocksdbjni/FRocksDB 与多平台发布 | 是 | 首版通常 4–8 周起，之后持续跟随版本维护 |

改一个 JNI 方法本身并不大；主要工作是确保 DB/CF/ReadOptions 生命周期、snapshot
一致性、close/cancel 并发、异常转换、JAR/SO 打包、鲲鹏 ABI、RocksDB 版本固定以及
SIGSEGV/资源泄漏测试。

### P3：完整 snapshot native 化计划

1. **冻结语义**：用现有 RocksJava iterator 写最小参考实现，只读取至多两条记录；针对
   EMPTY、SINGLE、MULTI、相邻 prefix、删除、并发 close 和 snapshot/read view 建立
   differential tests。这一步是 Native 实现的 oracle，不作为性能成果。
2. **确认二进制边界**：记录 Flink 实际 rocksdbjni/FRocksDB 版本、构建参数、动态/静态
   链接方式和符号可见性；确认 `RocksDB`、`ColumnFamilyHandle`、`ReadOptions` 的 handle
   都来自同一 Native 库。若不能证明 ABI 同一，不进入传指针实现。
3. **实现窄接口**：在同一 rocksdbjni 构建内增加 `classifyPrefix`，C++ 中 Seek 后最多
   读取两条；不回调 Java Serializer，不返回完整 iterator，不持有超出调用期的 RocksDB
   Slice 指针。
4. **接入 CacheKit**：miss 时优先调用 Native classifier，EMPTY/SINGLE 写入现有 Native
   cache table；MULTI 继续原 Flink iterator。classifier 是显式实验开关，开启后若库、
   ABI 或 delegate 不兼容则启动失败，不做会污染 A/B 归因的静默回退。hit 路径继续复用
   当前 Native table，保持 `native.enabled=false` 的默认止损策略。
5. **正确性与失效测试**：覆盖 key-group prefix、namespace、变长 BinaryRowData、CF
   隔离、remove/clear、checkpoint read view、backend close、Task cancellation 和重复
   load/unload；运行 ASan/UBSan 或等价 Native 检查，并增加错误 handle 的拒绝测试。
6. **性能归因**：至少设置 Java snapshot、现有 Native table、完整 Native classifier
   三组；同 JAR、同输入、交错多轮跑 q4/q9/q20。分别统计 hit 和 miss 路径，只有完整
   Native 组相对 Java 中位数不回退，才继续 NEON/SVE classifier 或批处理研究。

这条路线与后述 batch lookup 是两种不同优化：batch 试图摊薄“访问 CacheKit Native
table”的 JNI；`classifyPrefix` 试图消除 cache miss 时 Java 与 RocksIterator 之间的多次
JNI/对象往返。实验中必须分别开关，不能把两者合并后声称收益来自 snapshot native 化。

### 2026-08-11 P3 实现记录：一次 JNI 内完成 snapshot miss 分类

P3 已实现到可构建、可测试状态。没有修改 `/home/wutb/opt/frocksdb-6.20.3`，也没有替换
Flink 的 `com.ververica:frocksdbjni:6.20.3-ververica-1.0`。选择的是更窄的私有 ABI 适配：
CacheKit `.so` 只解析**当前进程已经加载**的 `librocksdbjni`，校验唯一实例和 ELF Build
ID，再解析该库已经导出的七个 RocksJava JNI iterator 入口。CacheKit 不链接另一份
RocksDB，也不解引用 `rocksdb::DB*`、`ColumnFamilyHandle*` 或 `ReadOptions*` 的 C++ 布局。

当前锁定的 aarch64 二进制身份如下：

```text
frocksdbjni JAR SHA-256:
cac69829b440e814775b25c31b25e557cd4c43688d27d848b05ec5901b7991d3

librocksdbjni-linux-aarch64.so SHA-256:
4c02c0828eb4ef8bfc1698755122068a4056403321d85b769ee591717a738e9b

ELF Build ID:
b4d1b52ddf0f5a33b41010a1dc981eefd834af74
```

Build ID、库数量或所需符号不匹配时，classifier 初始化直接失败。这个约束把“可能拿错
handle 后随机 SIGSEGV”前移成确定的启动错误；升级 FRocksDB 时必须重新审计符号签名并
更新 Build ID，不能只改常量绕过测试。

Java/Flink 侧新增 `RocksDBMapStateNativeSnapshotAccess`，只暴露当前 prefix、DB/CF/
ReadOptions native handle 和 key-group prefix 长度。真实 miss 数据流为：

```text
entries()/iterator()/keys()/values()/isEmpty()
  -> Native snapshot table miss
  -> flush 当前 (key, namespace) 的 CacheKit 脏写回
  -> 单次 CacheKit JNI classifyPrefix
     -> 使用同一 librocksdbjni 创建 iterator
     -> seek(prefix)
     -> 最多检查第一、第二条 key
     -> status + dispose
  -> EMPTY：回填 Native EMPTY，并直接短路
  -> SINGLE：反序列化第一条 key 的 user-key 后缀，回填 Native SINGLE；读取 value 时走 point-get
  -> MULTI：不回填，回到原 Flink iterator
```

prefix 匹配从 `keyGroupPrefixBytes` 之后开始，保持现有
`RocksDBMapState.startWithKeyPrefix()` 语义。Native 不持有 iterator、prefix byte[] 或
RocksDB Slice 超出本次调用；CacheKit close 释放 snapshot GlobalRef、classifier bridge
以及通过 `RTLD_NOLOAD` 获得的动态库引用。classifier 模式关闭 Java iterator backfill，
避免一份 miss 同时由两套分类器产生结果。

配置保持两级显式开关，默认均为 `false`：

```yaml
state.backend.cachekit.map.snapshot.cache.max-entries: 2000
state.backend.cachekit.map.snapshot.cache.native.enabled: true
state.backend.cachekit.map.snapshot.cache.native.classifier.enabled: true
state.backend.cachekit.map.snapshot.cache.native.kernel: SCALAR
state.backend.cachekit.map.snapshot.cache.native.library-path: /absolute/path/libcachekit_snapshot_jni.so
```

classifier 要求 Native snapshot cache 已开启且 delegate 是 `RocksDBMapState`，否则构造期
拒绝。`native.classifier.enabled=false` 时完整保留 P2 行为，旧构造器也继续默认传 `false`。

第一性原理上，P3 只优化 **snapshot cache miss**，不改变 Native table hit 的成本：

- EMPTY 把 Java iterator 创建、seek、valid/key/status/dispose 往返收敛为一次 JNI；
- SINGLE 收敛分类往返，但仍必须 point-get value；
- MULTI 为了判定会先读两条，随后仍需原 iterator，因此若 MULTI 比例高可能回退；
- q4 已测得约 87.88% snapshot hit，所以 P3 对 q4 的理论作用域最多是剩余约 12.12%，
  不能期待它自动偿还 P2 高频 hit 路径已有的 JNI 成本。

因此正确性通过不等于性能提升。必须用后述 A/B/C Nexmark 结果决定是否保留实验开关，
尤其要观察 EMPTY/SINGLE miss 占比能否覆盖 MULTI 的重复 iterator 成本。

已完成的验证：

- CMake Release 构建成功，long/bytes 两个 CTest 为 2/2；
- 使用真实 aarch64 `frocksdbjni`、真实 RocksDB/CF/ReadOptions handle 验证
  EMPTY -> SINGLE -> MULTI；
- `CachedInternalMapState` 数据流测试验证 SINGLE 首次 miss 即短路、后续命中 Native table，
  MULTI 连续两次都保留 delegate iterator 且不会被 Java backfill；
- Maven clean 定向测试共 24 项，全部通过；JDK 11 reactor package 成功；
- ASan+UBSan 构建下两个 Native CTest 通过，预加载 sanitizer runtime 后，使用真实 RocksDB
  handle 的 4 项 JNI 测试也通过。宿主环境处于 ptrace 下，LeakSanitizer 无法启动，因此
  这轮使用 `ASAN_OPTIONS=detect_leaks=0`；这不是“已完成 Native 泄漏证明”。

提交前最终制品 SHA-256：

```text
flink-statebackend-cachekit-1.16-SNAPSHOT.jar
d0b491241f9d1f67d56c201ee99832157a6681f386fb301c58458dce404c4cb7

libcachekit_snapshot_jni.so
7ece0c76ed6f363f00793843a68834dea8b130be822d19a262dc24b40578abef
```

验证命令：

```bash
cmake -S flink-state-backends/flink-statebackend-cachekit/src/native/snapshot-cache \
  -B /tmp/cachekit-p3-build -DCMAKE_BUILD_TYPE=Release
cmake --build /tmp/cachekit-p3-build -j
ctest --test-dir /tmp/cachekit-p3-build --output-on-failure

JAVA_HOME=/home/wutb/opt/jdk-11.0.31+11 mvn \
  -pl flink-state-backends/flink-statebackend-cachekit -am \
  -DskipITs -Dfast -Dcheckstyle.skip -Dspotless.check.skip=true \
  -Dcachekit.native.snapshot.library=/tmp/cachekit-p3-build/libcachekit_snapshot_jni.so \
  -Dtest=NativeMapSnapshotCacheTest,CachedInternalMapStateTest \
  -Dsurefire.failIfNoSpecifiedTests=false clean test
```

P3 的 q4/q9/q20 A/B/C 吞吐实验已经完成。尚未完成的是不受 ptrace 限制的
LeakSanitizer/长期资源泄漏测试。性能实验保持同 JAR、同输入、同 CPU 集合，并只改变
下面两个开关：

| 组 | native.enabled | native.classifier.enabled | 含义 |
|---|---:|---:|---|
| A | false | false | Java snapshot 基线 |
| B | true | false | P2 Native table |
| C | true | true | P3 完整 Native snapshot 分类 |

每个 query 至少交错运行 `A -> B -> C -> C -> B -> A`，报告中位数和每轮值；C 相对 B
回答 classifier 是否有效，C 相对 A 回答完整 Native 方案是否值得继续。不得用 B/C 不同
JAR，也不得把 microbenchmark、辅助指标或单轮烟测当成 Nexmark 吞吐结论。

### 2026-08-11 P3 A/B/C 实验结果

实际部署必须同时替换同一次 reactor 构建产生的两个 JAR：

- `flink-statebackend-cachekit-1.16-SNAPSHOT.jar`，SHA-256
  `d0b491241f9d1f67d56c201ee99832157a6681f386fb301c58458dce404c4cb7`；
- `flink-statebackend-rocksdb-1.16-SNAPSHOT.jar`，SHA-256
  `0c8916b3667f843133e6178fe92107c95e035529bf65aee41aee8f4e13d1ea4b`。

原因是 `RocksDBMapStateNativeSnapshotAccess` 和对应 `RocksDBMapState` 实现位于 RocksDB
backend 模块。第一次只替换 CacheKit JAR 的 1M smoke 因
`ClassNotFoundException: RocksDBMapStateNativeSnapshotAccess` 失败；补齐 RocksDB backend
JAR 后，q4 20M smoke 通过，吞吐 `407600 events/s`。TaskManager 明确记录 classifier
加载目标 rocksdbjni Build ID
`b4d1b52ddf0f5a33b41010a1dc981eefd834af74`。该单点只证明接入有效，不作为性能结论。

完整 20M 预筛已经完成。每个 query 按 `A -> B -> C -> C -> B -> A` 运行；三组
使用同一镜像、同一组 JAR/SO 和同一 CPU 集合，差异严格只有表中的两个开关。实验 JM
固定 CPU `240-247`，两个 TM 分别固定 `248-255`、`256-263`；与当时观测到的 `wuql`
CPU `278-310` 不重叠。Compose project、端口、runtime 和结果目录也完全独立。运行入口
和进度文件位于：

```text
/home/wutb/nexmark-bench-v2/runtime/kunpeng-native-p3-abc-20260811/
  run-campaign.sh
  campaign.log
  runs.csv
```

smoke 结果位于：

```text
/home/wutb/nexmark-bench-v2/results/
  20260811T141751+0800_kunpeng-p3-classifier-smoke-q4-20m-20260811/
```

18 次查询全部通过；所有 C 组 TaskManager 日志都确认 classifier 加载的 rocksdbjni
Build ID 为上述目标值。每组只有两个观测，因此下表的“中位数”是两者算术平均，足够做
方向性预筛，但不等价于有统计置信区间的长期基准。

| Query | A 两次值（中位数） | B 两次值（中位数） | C 两次值（中位数） | C 相对 B | C 相对 A |
|---|---:|---:|---:|---:|---:|
| q4 | 500730 / 485550（493140） | 401260 / 401440（401350） | 404230 / 404820（404525） | +0.79% | -17.97% |
| q9 | 249620 / 252690（251155） | 230900 / 230750（230825） | 228570 / 232750（230660） | -0.07% | -8.16% |
| q20 | 365260 / 361730（363495） | 356420 / 356520（356470） | 359770 / 359170（359470） | +0.84% | -1.11% |

吞吐结论是否定的：classifier 相对 P2 table 的变化全部在约 `±1%` 内，不能证明稳定
收益；完整 P3 相对 Java 在三个 query 上均未提升，其中 q4、q9 明显回退，q20 小幅回退。
因此保持两个 Native 开关默认关闭，不把 P3 合入默认性能路径，也没有依据继续做 100M
放大实验。

第一性原理上，P3 classifier 只在 snapshot miss 时参与，不能消除 P2 Native table 在每次
高频 hit 上支付的同步 JNI 与 Native probe 成本。以 q4 已测得的 `87.88%` hit 为例，P3
最多影响约 `12.12%` 的 miss；而剩余绝大多数访问仍承担 B 的固定边界成本。C 相对 B
仅 `+0.79%`、但相对 A 仍 `-17.97%`，正好验证了这一成本上界。q9/q20 也显示 miss
分类节省不足以覆盖或显著改变整条执行路径。若继续追求 Native 亲和，必须先改变边界
形态，例如批量化或让调用链长期停留在 Native，而不是继续优化单次 miss classifier。

全部正式结果目录匹配：

```text
/home/wutb/nexmark-bench-v2/results/
  20260811T*_kunpeng-p3-q{4,9,20}-{1..6}-{a,b,c}-20m-20260811/
```

## 2026-08-10 q4/q9/q20 成对结果

在共享机器存在明显资源竞争的情况下，最初的 100M Java 运行中 q4 成功完成，吞吐为
`624720 events/s`；q9 运行约 11.5 分钟时只生成约 7306 万事件。为了缩短 Java/Native
之间的机器状态间隔，停止并清理了仅属于本实验的 `ckkunpcompare` 项目，改用 20M
正式事件、10M warmup、关闭 checkpoint 的成对归因测试。两组使用完全相同的 JAR
（SHA-256 均为 `5c43f2a38e190d9796324a76c0004c825c2ca6e350bcbc700f2211e1374e3e8b`），
配置差异只有 Native enable、SCALAR kernel 和动态库路径。

| Query | Java events/s | Native Scalar events/s | Native 相对变化 |
|---|---:|---:|---:|
| q4 | 781400 | 639490 | -18.16% |
| q9 | 350020 | 348480 | -0.44% |
| q20 | 539870 | 517240 | -4.19% |

Native TaskManager 日志确认实际 kernel 为 `scalar`，三项查询均通过且无 JNI、SIGSEGV
或 Flink failure。结果目录为：

```text
/home/wutb/nexmark-bench-v2/results/
  20260810T210038+0800_kunpeng-snapshot-java-q4-q9-q20-20m-20260810/
  20260810T210602+0800_kunpeng-snapshot-native-scalar-q4-q9-q20-20m-20260810/
```

这轮单次、短规模测试不能给出严谨置信区间，但足以否定“当前 Native 版本在三个查询上
依然普遍提升”：q4 明显回退，q20 回退，q9 持平。它也符合第一性原理成本模型——当前
路径每次 probe 额外支付 key/namespace 序列化、JNI 和返回 payload 反序列化，而短 probe
链上的 Native scalar 查找不足以偿还这些成本。

## 2026-08-10 P2 阶段的下一步（历史记录）

1. 若继续验证，固定独占 CPU 集合后对 Java/Native SCALAR 做多轮交错 100M 测试，报告
   中位数和离散度；当前单轮结果不支持默认启用 Native。
2. 若继续优化，优先消除逐次序列化/JNI，而不是先换 NEON/SVE probe；只有让 key 保持
   canonical bytes 或实现可摊薄 JNI 的批处理后，向量 probe 才可能成为主要变量。
3. 优化后再对比 SCALAR/NEON/SVE。性能结果无论正负都保留，不设置任意接入门槛。

## 2026-08-11 q4 热路径优化与止损结论

诊断运行确认 q4 的 snapshot cache 不是低命中缓存：Join[10] 共 probe `16,904,216`
次，hit `14,855,692` 次，命中率约 `87.88%`；其中 SINGLE short-circuit
`14,263,371` 次。因此 q4 的主要成本是高频命中路径，而不是 miss 后的 RocksDB 扫描。
诊断结果位于：

```text
/home/wutb/nexmark-bench-v2/results/
  20260811T001212+0800_kunpeng-native-q4-hit-diagnostics-20m-20260811/
```

已保留两项能直接减少必付成本且通过正确性测试的改动：

1. SINGLE payload 不再序列化/反序列化 userKey，而是保存 backfill 时已经深拷贝的
   Java userKey GlobalRef；
2. q4 实际类型为单段 on-heap `BinaryRowData + VoidNamespace` 时，lookup 直接传 row
   的底层 byte[]、offset 和 length，不再调用 key/namespace serializer；多段、off-heap
   或其他类型继续走通用序列化回退。

第一项把相同规模 Native q4 从最初 `639490` 提高到 `640330 events/s` 的量级，并消除
大量对象编解码；第二项后 Native 为 `640120 events/s`，说明 key 序列化已不是剩余主
瓶颈。第二项的严格同 JAR 配对结果为：

| 路径 | q4 events/s | 相对 Java |
|---|---:|---:|
| Java snapshot | 678290 | 基线 |
| Native GlobalRef + raw BinaryRow key | 640120 | -5.63% |

两份输入快照中的 CacheKit JAR SHA-256 均为
`e44b934482a0e340afd2665307b501cd6843ad705cb630348a2be102503d67db`，配置差异仍只有
Native enable/kernel/library-path。结果目录为：

```text
/home/wutb/nexmark-bench-v2/results/
  20260811T002843+0800_kunpeng-rawkey-java-q4-20m-20260811/
  20260811T003024+0800_kunpeng-rawkey-native-q4-20m-20260811/
```

另外做了四个单变量候选，均未达到可保留标准：

| 候选 | Native q4 events/s | 判定 |
|---|---:|---|
| 单条 JNI result memo | 622040 | 回退，撤销 |
| 256 槽 JVM near-cache | 638260 | 无收益且增加双层复杂度，撤销 |
| 小 key 栈拷贝 + 直接返回 GlobalRef | 640960 | 噪声范围，撤销 |
| 移除 Java monitor | 637770 | 无收益且削弱生命周期保护，撤销 |

这些候选不是各自带 Java 对照的正式配对数据，只用于快速淘汰实现方向；不能横向解释
几千 events/s 的差异。它们共同给出稳定结论：q4 Native 吞吐长期停在约
`0.64M events/s`，剩余主成本是每次同步 probe 的 JNI 边界和 Native scalar table，
不是 serializer、短期局部性、GC critical API 或 Java monitor。

### q4 不回退策略与 P4 下一步规划

当前生产止损策略保持不变：`native.enabled` 默认 `false`，所以默认 q4 仍走原 Java
snapshot cache，不会因实验 Native 路径回退。不能把“打开 Native 后偷偷改走 Java”
记作 Native 优化成功；Native 开关的 A/B 语义必须保持真实。

P3 结果已经否定继续优化单次 miss classifier。下一阶段若仍要研究 Native snapshot，
只能做一个有明确停止条件的 **P4 batch 可行性验证**，不能直接修改 Flink 热路径。

#### 第一性原理与当前能力缺口

一次批量 Native lookup 的单位成本可以写成：

```text
Tbatch/item = Tpack + Tjni / N + Tnative-table + Tresult + Tstale-check
```

要让它胜过 Java snapshot，至少必须同时满足：

1. 包含真实 key 打包和结果消费后的 `Tbatch/item` 低于 Java snapshot lookup；
2. q4 在不改变记录、watermark、barrier 和双输入调度语义的前提下，能提前得到足够大的
   `N`；现有数据只证明 `N=32/64` 才能明显摊薄 JNI；
3. lookahead 必须知道 input side 和将要访问的具体 MapState，不能把所有 future key 盲目
   查询到所有 MapState table；
4. 批量结果从生成到消费之间若发生同 key 写入，必须检测 stale，不能牺牲状态语义。

当前工程尚不具备这些条件：

- `NativeSnapshotBench.lookupBatch` 只查询 primitive `SnapshotTable`，不是生产使用的
  `JniByteSnapshotCache`，也没有计算 `BinaryRowData` 打包、GlobalRef 结果和 Java 消费成本；
- `StreamRecordBatchOutput` 只接入 `OneInputStreamTask`；q4 热点是 TwoInput
  `StreamingJoinOperator`，其 `StreamTwoInputProcessorFactory.StreamTaskNetworkOutput`
  当前每次立即处理一个 record；
- `StatePrefetcher` 只读取 `stateKeySelector1`，且
  `CacheKitKeyedStateBackend.hasPrefetchableState()` 只把 `CachedInternalValueState` 视为可
  prefetch；它不能正确覆盖 q4 的左右输入和 MapState snapshot；
- 一个 q4 record 内可合并的 snapshot probe 数量远小于 32。没有 future-record
  lookahead，单纯增加生产 `lookupBatch` API 不会被热路径使用。

#### P4-0：冻结当前基线

- 保留 P3 代码和负结果，两个 Native 开关继续默认 `false`；
- 不再为当前 P3 跑 100M，也不做 NEON/SVE classifier；
- 若实施 P4，在 `019d57cda5` 上新建独立 `cachekit/dev_wutb_kunpeng_native_batch_spike`
  分支/工作树，避免把高风险 runtime spike 混入已验证分支。

#### P4-1：只做生产路径 batch microbenchmark

先修改 Native benchmark 和最窄 JNI 原型，不接入 `CachedInternalMapState`：

1. 给生产 `JniByteSnapshotCache` 增加 bench-only `lookupBatch`：输入为复用的 direct byte
   arena 加 offsets/lengths，输出写入复用的 Java `Object[]`，覆盖 MISS、EMPTY、SINGLE；
2. key 使用 q4 已确认的单段 on-heap `BinaryRowData + VoidNamespace`，同时覆盖 q4 实际
   key 长度；计时必须包含把多个 row 拷入 arena 和 Java 读取 kind/userKey 的成本；
3. Java 对照必须调用当前真实 `MapSnapshot` Java cache lookup，不得只拿 C++ batch 与
   C++ scalar 比；分别报告 single、batch 8/32/64；
4. 随机 differential test 覆盖 hit/miss、EMPTY/SINGLE、hash collision、重复 key、容量
   淘汰和 close 后 GlobalRef 生命周期。

**硬停止条件**：若 batch 32/64 的完整单位成本仍不低于 Java cache lookup，就停止 P4，
不修改 Flink runtime。集成只会再增加 result ring、generation 和调度成本，不可能反转
底层 lookup 已经更慢的事实。

#### P4-2：TwoInput lookahead 能力 spike

仅当 P4-1 通过，才研究 q4 的 future key 来源。该阶段仍不接 Native cache：

1. 为 TwoInput 路径设计显式 `(inputId, key)` lookahead，input 1 使用
   `stateKeySelector1`，input 2 使用 `stateKeySelector2`；禁止沿用当前只读 selector1 的
   `StatePrefetcher`；
2. 只允许观察已经由 runtime 合法缓冲、但尚未执行的 record。不得为了凑 batch 主动消费
   另一个 network record，因为这会改变 `StreamMultipleInputProcessor` 的左右输入选择和
   checkpoint barrier 边界；
3. 为 `StreamingJoinOperator` 建立窄的 input-side 到 state-name 映射：处理左输入只预查
   将读取的 right state，处理右输入只预查 left state；禁止遍历所有 MapState wrapper；
4. 用 runtime 单元测试证明 record 顺序、左右输入选择、watermark、aligned/unaligned
   checkpoint 和 end-of-input 行为与无 lookahead 路径一致；
5. 记录实际可形成的 batch 大小。若当前 runtime 无法在不改变语义的情况下稳定提供至少
   32 个 future key，就停止 P4。当前 `emitNext()` 一次只产生一个 record、且没有 peek
   API，因此这是预期风险最高、也最可能终止路线的一步。

#### P4-3：生产接入（仅在两个必要条件都成立后）

1. 在 `NativeMapSnapshotCache` 增加生产 byte-key `lookupBatch`，复用 P4-1 已验证的 buffer
   布局；scalar API 和 Java cache 均保留为明确 fallback；
2. 每个 MapState wrapper 建立有界、短生命周期 batch result ring；结果携带 key 和
   generation，不保存超出 cache 生命周期的 JNI local ref；
3. `put/remove/clear` 对对应 key 提升 generation；消费前验证，stale 结果丢弃并走当前
   同步权威路径；随机测试必须覆盖“批次前查到 hit、前一 record 随后修改同 key”的场景；
4. 第一版只覆盖已经由 q4 证实的 `BinaryRowData + VoidNamespace`，其他类型不猜测通用
   codec，直接走 Java；classifier 先保持关闭，单独隔离高频 table hit 的批量化收益。

#### P4-4：性能决策顺序

1. 先跑固定 CPU、同 JAR 的 q4 20M `A -> D -> D -> A`：A 为 Java snapshot，D 为
   Native batch table 且 classifier 关闭；只看 Nexmark events/s；
2. D 的 q4 中位数不低于 A 且所有正确性测试通过后，再加入 E（D + P3 classifier），
   用 `D -> E -> E -> D` 判断完整 Native 是否重新引入回退；
3. 只有 E 也不低于 A，才跑 q9/q20；只有 scalar batch 端到端成立，才比较
   SCALAR/NEON/SVE。向量指令优化的是 Native table 内部计算，不能偿还未摊薄的 JNI、
   打包和双输入调度成本。

#### P4 失败后的方向

任一硬条件失败，就把 Native snapshot 标记为“正确性原型完成、性能路线关闭”，保留
默认 Java snapshot。后续鲲鹏亲和应转向天然具有连续数据和批量工作的路径，例如 RocksDB
MultiGet、批量序列化/反序列化、checkpoint checksum/compression；这些位置才更可能让
NEON/SVE 的吞吐覆盖 JNI 和数据搬运成本。该转向需要单独立项，不能继续记为 snapshot
cache 优化。

## P5：保留 Java 热命中，只下沉 RocksDB miss 分类

### 第一性原理边界

q4 已测得 snapshot cache 命中率约为 `87.88%`。因此一次 `entries()` 的期望成本为：

```text
E[T] = H * Thit + (1 - H) * Tmiss
```

其中 `H` 接近 0.88。现有数据又证明 Native single lookup 约 `223 ns`，Java snapshot
lookup 约 `93 ns`。把高频 `Thit` 下沉会让绝大多数访问额外支付 JNI 和 Native table
成本；这就是完整 Native table 在 q4 回退的根因。真正仍可能受益的是低频但昂贵的
`Tmiss`：Java 原路径创建 RocksDB iterator、跨 JNI 迭代并在 Java 中判断
EMPTY/SINGLE/MULTI，而 P3 classifier 能在一次 JNI 内完成相同 prefix 分类。

因此 P5 的职责边界是：

```text
Java LRU:  lookup / hit / put / remove / eviction
Native:    只在 Java miss 后读取 RocksDB prefix，返回 EMPTY / SINGLE(userKey) / MULTI
```

这不是“Native 开关打开后偷偷退回 Java”。两个开关具有独立、可验证的语义：

| native.enabled | classifier.enabled | snapshot table | miss 分类 |
|---|---|---|---|
| false | false | Java LRU | 原 Java iterator 路径 |
| true | false | Native table | 原 Java iterator 路径 |
| true | true | Native table | Native RocksDB classifier |
| false | true | **Java LRU** | **Native RocksDB classifier** |

### 2026-08-11 P5 实现记录

已基于 P3 HEAD `a86742d0be` 创建独立分支
`cachekit/dev_wutb_kunpeng_native_hybrid`，完成第四种组合：

1. classifier 不再要求 `native.enabled=true`，只要求 snapshot cache 容量大于零且 delegate
   实现 `RocksDBMapStateNativeSnapshotAccess`；
2. `native.enabled=false` 时始终创建原有 Java `LruCachePolicy`，所有高频 probe、回填、失效
   和淘汰均不进入 JNI；
3. `classifier.enabled=true` 时创建 Native JNI handle，仅在 Java cache miss 后调用 P3
   `classify()`；EMPTY/SINGLE 回填 Java LRU，MULTI 继续走 Flink 原始 iterator；
4. 增加端到端测试，分别覆盖“Native table + Native classifier”和“Java LRU + Native
   classifier”，验证 SINGLE 第二次访问命中缓存、MULTI 不回填且两次都走 delegate；
5. 初始化日志改称 `Native snapshot JNI`，避免 classifier-only 模式被误读为 Native table
   已启用。

当前实现为降低改动风险，classifier-only 模式复用 P3 `NativeMapSnapshotCache` handle；C++
内部会分配一个未被 lookup/put/remove 使用的 bytes table。它不进入热路径，但有固定内存
成本。只有端到端 q4 证明该边界有收益后，才拆出物理独立的
`NativeSnapshotClassifier` handle；在收益未知前先拆 C++ 类型不会改变每条记录的成本。

已完成的验证：

```text
NativeMapSnapshotCacheTest + CachedInternalMapStateTest
Tests run: 25, Failures: 0, Errors: 0, Skipped: 0
```

### P5 Nexmark 单变量实验

只用本分支构建的同一个 CacheKit JAR、P3 同一份 Native `.so` 和同一份 RocksDB backend
JAR。固定 CPU、20M events、无 checkpoint，先跑 q4 配对：

```text
A: native.enabled=false, classifier.enabled=false  # Java table + Java miss
H: native.enabled=false, classifier.enabled=true   # Java table + Native miss
顺序: A -> H -> H -> A
```

必须在 TaskManager 日志确认：A 不加载 Native snapshot JNI；H 打印
`Native snapshot JNI initialized` 且 classifier 为 RocksDB bridge，同时两组配置中的
`native.enabled` 均为 false。输入快照必须记录完全相同的 CacheKit/RocksDB JAR SHA-256。

P5 不预设必然提升。H 在 EMPTY/SINGLE miss 上可能省掉 Java iterator 往返，但 MULTI 会先
做 Native 两步分类、随后再走原 iterator，形成重复扫描。q4 若不能在成对中位数上至少不
回退，就停止 P5，不跑 q9/q20；q4 不回退后才以相同 `A -> H -> H -> A` 顺序测试 q9、
q20。只以 Nexmark events/s 作性能结论，内部指标仅用于证明实验开关生效，不能代替吞吐。

### 2026-08-11 P5 q4 结果与停止判定

P5 实现提交为 `891cfdb09ba62d04bb41dcfd978975f433ecabf0`。定向测试及构建命令为：

```bash
JAVA_HOME=/home/wutb/opt/jdk-11.0.31+11 /home/wutb/.local/bin/mvn \
  -pl flink-state-backends/flink-statebackend-cachekit -am \
  -DskipITs -Dfast -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip \
  -Dcachekit.native.snapshot.library=/tmp/cachekit-p3-build/libcachekit_snapshot_jni.so \
  -Dtest=NativeMapSnapshotCacheTest,CachedInternalMapStateTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

结果为 25/25 通过。q4 campaign 位于：

```text
/home/wutb/nexmark-bench-v2/runtime/kunpeng-native-p5-hybrid-q4-20260811/
```

四次 20M 运行均通过，Nexmark 吞吐如下：

| 位置 | 组 | q4 events/s |
|---:|---|---:|
| 1 | A：Java hit + Java miss | 500880 |
| 2 | H：Java hit + Native miss | 500000 |
| 3 | H：Java hit + Native miss | 506250 |
| 4 | A：Java hit + Java miss | 512780 |

A 两次中位数为 `506830 events/s`，H 两次中位数为 `503125 events/s`，H 相对 A 为
`-0.73%`。这个差值不支持“明显回退”，但同样不能证明 Native miss 有提升或严格不回退；
按预先写明的 q4 硬门槛，P5 到此停止，没有继续 q9/q20。

实验有效性已经确认：四次输入快照的 CacheKit JAR SHA-256 均为
`1b843a95c56e5adc3f422b51495c3e3d0ed4379301d2c44a26241040ccac3cde`；两次 A 运行各自的
8 个 TaskManager 日志均没有 Native JNI 初始化记录，两次 H 也各有 8 个，并都打印
rocksdbjni Build ID `b4d1b52ddf0f5a33b41010a1dc981eefd834af74`。两组
`native.enabled` 都为 false，说明被比较的确实是同一个 Java LRU，仅 miss 路径不同。
四份完整结果为：

```text
/home/wutb/nexmark-bench-v2/results/
  20260811T232925+0800_kunpeng-p5-hybrid-q4-1-a-20m-20260811/
  20260811T233114+0800_kunpeng-p5-hybrid-q4-2-h-20m-20260811/
  20260811T233304+0800_kunpeng-p5-hybrid-q4-3-h-20m-20260811/
  20260811T233454+0800_kunpeng-p5-hybrid-q4-4-a-20m-20260811/
```

### P6 边界：若继续 Native snapshot，必须下沉一次完整 miss traversal

P5 已经证明“Java table hit、Native miss classify”能够保住 q4 的大部分性能，但单独
下沉 cardinality 判断没有可测收益。原因不是 C++ 代码还不够快，而是一次 cache miss
仍然存在两个权威读取链：

```text
SINGLE: Native iterator Seek/Valid/Next -> 返回 userKey -> Java RocksDB point-get value
MULTI:  Native iterator Seek/Valid/Next -> 返回 MULTI   -> Java iterator 再 Seek/遍历
```

也就是说，P5 消除了部分 JNI 控制往返，却没有消除第二次 RocksDB 访问；MULTI 甚至重复
前缀扫描。继续调 NEON/SVE 或拆掉未使用的 Native table 内存，都不能改变这个每次 miss
必付成本。

若仍继续 snapshot Native 化，下一条值得实现的边界不是 Native LRU，而是一次 JNI 内的
`probePrefix`，并且一次 miss 只能有一个 RocksDB traversal：

1. Java LRU 的 lookup、回填、失效和淘汰保持不变；
2. EMPTY 直接返回；
3. SINGLE 必须同时返回 raw user-key suffix 和 raw value，Java 完成反序列化、回填 Java
   snapshot，并直接构造本次 singleton entry，禁止再调用 `this.get(userKey)`；
4. MULTI 不能先 classify 再重新创建 iterator。要么 Native 返回一个已定位的 iterator
   handle，并把已经读取的前两条记录作为 buffered entries 交给 Java；要么该调用点完全
   保持原 Java iterator。两者必须二选一，不能双扫；
5. Native iterator handle 必须在耗尽、异常、state/backend close 时精确释放，且继续使用
   RocksDB 的 ReadOptions、column family 和 key-group prefix 语义；写回仍须在 Seek 前
   flush。

P6 的必要正确性测试包括 EMPTY/SINGLE/MULTI、SINGLE value 反序列化、iterator 提前停止、
异常释放、状态修改后的失效以及 backend close。性能仍先只跑同 JAR 的 q4 20M
`A -> P6 -> P6 -> A`。只有 P6 中位数不低于 A 才跑 q9/q20；若仍无收益，则 snapshot
Native 路线应正式关闭，因为再往下就等价于重写 RocksDB Java iterator，而不是优化
snapshot cache。
