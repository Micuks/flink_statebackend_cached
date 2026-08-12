# 鲲鹏亲和：Flink CacheKit Snapshot Cache Native 化探索

> PPT 汇报大纲，建议 10 页，汇报时长约 12～15 分钟。
>
> 口径说明：本文的 snapshot 指 CacheKit 对 `MapState.entries()/iterator()/isEmpty()` 的
> EMPTY/SINGLE 快照缓存，不是 Flink checkpoint/snapshot。近期工作没有修改
> `bp-prefetch` 的实现或配置。

## 第 1 页：标题页

**标题**：鲲鹏亲和：Flink CacheKit Snapshot Cache Native 化探索

**副标题**：从向量化原型到完整 Native miss traversal

**一句话结论**：完成了可运行、可回退、可验证的 Native snapshot 路径；q4/q9/q20 性能
回退控制在约 2% 内，但尚未观察到吞吐提升。

## 第 2 页：背景与目标

### 背景

- CacheKit 在 `CachedInternalMapState` 中维护 snapshot cache。
- 对当前 key/namespace，缓存其 MapState 为 EMPTY 或 SINGLE，从而避免完整 RocksDB 范围遍历。
- 希望利用鲲鹏 AArch64/NEON/SVE 和 C++ 批处理能力，形成硬件亲和优化。

### 目标

- 将 snapshot cache 的关键工作下沉到 Native 层。
- 保持 Flink MapState 语义、生命周期和回退能力。
- 最终只以 Nexmark q4/q9/q20 吞吐判断是否有效。

### 范围边界

- 本项目不修改 Flink checkpoint。
- 本项目不修改 CacheKit `bp-prefetch`。
- Native 开关默认关闭，不影响现有 Java 生产路径。
- 鲲鹏是优化目标；x86_64 只保证 scalar Native 兼容，不开发 SSE/AVX 亲和路径。

**建议配图**：`Flink operator → CacheKit MapState → Snapshot cache → RocksDB` 四层结构图。

## 第 3 页：第一性原理——Native 为什么不一定更快

一次 Native cache lookup 的总成本近似为：

```text
Tnative = key 编码 + JNI 边界 + Native probe + 结果对象化
```

Java 路径的成本近似为：

```text
Tjava = Java hash/probe + 对象访问
```

要获得收益，必须满足：

```text
省掉的 Java probe / iterator / GC 成本
  > key 编码 + JNI + Native probe + 结果搬运
```

关键事实：q4 snapshot cache 命中率约 `87.88%`。因此高频热命中非常短，JNI 固定成本很难
被摊薄；真正可能值得下沉的是低频但更重的 RocksDB miss 路径。

**建议配图**：成本天平，左侧为节省项，右侧为 JNI、编码和数据搬运。

## 第 4 页：方法一——Native SoA + NEON/SVE 向量探测

### 做法

- 在 C++ 实现固定容量 SoA 哈希表。
- 实现 scalar、NEON、SVE 三种 control-byte probe。
- 增加 single JNI 和 batch 8/32/64 接口。
- 通过反汇编确认实际生成 NEON/SVE 指令。

### 结果

| 路径 | ns/op |
|---|---:|
| Java scalar SoA | 19.96 |
| Native core scalar | 21.53 |
| JNI scalar single | 40.31 |
| JNI scalar batch 64 | 20.53 |
| JNI NEON batch 64 | 23.15 |
| JNI SVE batch 64 | 24.96 |

### 结论

- 向量指令确实生效，但短 probe chain 没有足够并行比较工作。
- NEON/SVE 的装载、mask 和分支成本超过收益，scalar 反而最快。
- batch 64 能摊薄 JNI，但仍没有超过 Java。

**核心认识**：硬件“支持更强向量化”不等于该数据结构天然适合向量化。

## 第 5 页：方法二——完整 Native snapshot table

### 做法

- 将 Java `LruCachePolicy<KeyNamespace, MapSnapshot>` 替换为 C++ bytes table。
- 支持真实 `BinaryRowData` canonical bytes。
- Native 保存 EMPTY/SINGLE 和 SINGLE userKey `GlobalRef`。
- 实现 LRU 淘汰、删除、clear、close 和 scalar/NEON/SVE 分派。
- miss 分类通过同一进程的 rocksdbjni iterator 完成。

### 优化尝试

- SINGLE 直接保存 Java userKey `GlobalRef`，减少序列化。
- `BinaryRowData + VoidNamespace` 直接使用底层 raw bytes。
- 尝试 JNI result memo、JVM near-cache、小 key 栈拷贝、移除 Java monitor。

### 结果

完整 Native table 的 P3 A/B/C 实验中，P3 相对 Java：

| 查询 | 性能变化 |
|---|---:|
| q4 | -17.97% |
| q9 | -8.16% |
| q20 | -1.11% |

raw key + GlobalRef 优化后，q4 相对 Java仍为 `-5.63%`；其他几个局部优化没有稳定收益，均撤销。

### 结论

q4 的高命中热路径每次都支付同步 JNI 和 Native probe，成为主要回退来源。

## 第 6 页：方法三——Java 热缓存 + Native miss classifier

### 设计转变

根据 q4 `87.88%` 命中率，保留 Java LRU 的高频 hit，只下沉 Java cache miss：

```text
Java LRU hit → 原 Java 快路径
Java LRU miss → Native RocksDB prefix classifier
                 ├─ EMPTY
                 ├─ SINGLE(userKey)
                 └─ MULTI
```

### 实现

- Native 在一次 JNI 内创建 RocksIterator、Seek prefix、最多读取两条、分类并释放 iterator。
- EMPTY/SINGLE 回填 Java LRU。
- MULTI 回到原 Java iterator。
- 通过 rocksdbjni Build ID 和符号检查保护私有 JNI ABI。

### q4 结果

| 路径 | 中位数 events/s | 相对 A |
|---|---:|---:|
| A：Java hit + Java miss | 506830 | 基线 |
| P5：Java hit + Native classify | 503125 | -0.73% |

### 结论

保住了 Java 热路径，回退显著缩小；但 SINGLE 仍需 point-get，MULTI 仍会重新扫描 prefix，
Native classifier 没有消除第二条权威读取链。

## 第 7 页：方法四——Native prefix batch，完整下沉 miss traversal

### 做法

- 新增 `nativeReadPrefixBatch`，一次返回 raw key/value。
- SINGLE：同一次 Native traversal 返回 key 和 value，不再 RocksDB point-get。
- MULTI：每批最多 128 条，从上一批最后一个 raw key 继续，不再从 prefix 起点重扫。
- 每次 JNI 调用内创建并释放 RocksIterator，不把无 `close()` 保障的 iterator handle 暴露给 Java。
- Java 只保留 Flink `TypeSerializer` 反序列化及 Java LRU 热缓存。

### 正确性覆盖

- EMPTY、SINGLE、合法 null value、MULTI。
- 130 条数据跨 `128 + 2` 两页，无重复、无遗漏。
- SINGLE 冷 miss 不调用 point-get。
- MULTI 不调用 `delegate.entries()`。
- C++ 2/2、Java/JNI 25/25 测试通过。

**建议配图**：P5“双遍历”与 P6“单遍历分页”的左右对比图。

## 第 8 页：最终 Nexmark 性能结果

实验采用相同 JAR、相同镜像、固定 CPU，顺序为 `A → P6 → P6 → A`，每次 20M events。

| 查询 | Java A 中位数 | Native P6 中位数 | 相对变化 |
|---|---:|---:|---:|
| q4 | 506245 | 496630 | -1.90% |
| q9 | 248355 | 244090 | -1.72% |
| q20 | 371555 | 364620 | -1.87% |

### 实验有效性

- 所有运行输入 JAR 哈希一致。
- A 组没有 Native 初始化日志，P6 组所有 TaskManager 均确认加载 JNI。
- 没有 `UnsatisfiedLinkError`、JNI symbol 缺失、SIGSEGV 或 Flink fatal error。

### 结论

- 三个查询方向一致：没有性能提升，回退约 `1.7%～1.9%`。
- Native miss 完整化明显好于“整个 snapshot table 下沉”的早期方案。
- 当前版本可作为完整原型和后续优化基础，但不应默认开启。

## 第 9 页：失败实验与经验沉淀

### 失败或终止的方法

| 方法 | 结果/原因 |
|---|---|
| NEON/SVE probe | 短 probe，向量固定成本高于收益 |
| 整个 snapshot table 下沉 | 高频 hit 每次支付 JNI，q4 最大回退约 18% |
| JNI memo / JVM near-cache | 没有稳定收益，增加双层一致性复杂度 |
| 移除 Java monitor | 无收益且削弱生命周期安全 |
| P5 两条记录 classifier | MULTI 会重复扫描，q4 -0.73% |
| P6.1 紧凑 JNI Object[] | local refs 管理抵消分配节省，q4 -0.78%，已回退 |
| runtime batch/lookahead | q4 是 TwoInput，缺少不改变调度/barrier 语义的 32/64 条 future-key lookahead |

### 通用经验

1. 先证明数据连续、批量规模和热点位置，再谈向量化。
2. JNI 优化的核心不是 C++ 单条指令更快，而是减少边界次数和重复权威读取。
3. 生产热路径必须保留明确开关和可验证的 A/B 语义。
4. 负结果同样有价值：已经排除多个看似合理但不适合当前 workload 的方向。

## 第 10 页：阶段结论与下一步

### 已完成

- 建立了真实 RowData → JNI → C++ → RocksDB 的完整 Native 数据通路。
- 完成 Native table、Native classifier、Java/Native hybrid、Native prefix batch 四种边界探索。
- 完成 ABI 身份检查、Native 生命周期管理、语义测试和 q4/q9/q20 对照实验。
- 将早期最大约 18% 的 q4 回退收敛到最终约 2%。

### 当前结论

```text
功能目标：完成
性能提升：未实现
性能回退：约 2%，可控
默认策略：继续使用 Java snapshot，Native 实验开关默认关闭
```

### 下一步建议

1. 若继续 snapshot Native 化，先测 EMPTY/SINGLE/MULTI 占比及分类耗时，定位约 2% 回退来源。
2. 优先减少每条 key/value 的 JNI byte[] 物化，而不是继续优化 control-byte probe。
3. 只有能形成真实批量并摊薄 JNI，才重新比较 scalar/NEON/SVE。
4. 鲲鹏向量亲和可考虑转向天然连续、批量计算更重的路径，例如 checksum、compression、
   批量序列化或 RocksDB MultiGet。

## 附录：提交与材料索引

- 完整 Native miss traversal：`0338d1df46 feat(cachekit): stream snapshot misses through native batches`
- 最终性能记录：`885b27a768 docs(cachekit): record final native batch q4 result`
- 详细设计与实验记录：`kunpeng-native-snapshot-cache-plan.md`
- 最终 q4 campaign：
  `/home/wutb/nexmark-bench-v2/runtime/kunpeng-native-p6-final-q4-20260812/`
- 最终 q9/q20 campaign：
  `/home/wutb/nexmark-bench-v2/runtime/kunpeng-native-p6-final-q9-q20-20260812/`
