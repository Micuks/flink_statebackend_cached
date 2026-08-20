# Kunpeng Native Snapshot 指令亲和分析

## 1. 结论

当前最适合 Native snapshot 热路径的鲲鹏指令不是 SVE，而是 **CRC32C 的 `CRC32CX`**。
生产 q4 的 Native key 平均为 16 B，旧 FNV-1a 必须串行执行 16 组 byte xor + 64-bit multiply；
CRC32C 快路径只需两次 64-bit load、两条 `CRC32CX` 和一次 avalanche。

已实现的选择逻辑：

```text
HiSilicon TSV110/HIP09 + Linux HWCAP_CRC32 + key size == 16
    -> CRC32CX table hash
其他 CPU 或其他 key size
    -> 原 FNV64 hash
```

这是真正发生在 C++ `ByteSnapshotTable` 的优化。Java remove membership hint 继续使用原 FNV64；
Native 淘汰时对被淘汰 key 计算并返回 FNV64，因此不会破坏已验证的 Java/Native 维护协议。

## 2. 第一性原理

一次 Native snapshot lookup 可拆成：

```text
JNI 边界 -> 16 B hash -> control/hash probe -> 完整 key 比较 -> LRU touch -> JNI 返回
```

指令优化只有命中 Native core 的计算部分才可能有效：

- hash 每次 lookup/put/remove 必做，且旧实现存在逐 byte 的串行乘法依赖链。
- control probe 通常一两个槽就遇到 EMPTY 或命中，连续工作太少。
- 完整 key 比较只在 fingerprint 与 hash 都匹配后执行，不是每次操作必做。
- LRU 是依赖前一次索引的随机读写，不能直接 SIMD 化。
- JNI 固定成本不可能被任何 C++ 指令消除。

因此优先优化 hash，比继续扩大 control-byte SIMD 更符合真实热路径。

## 3. 已接入：CRC32CX 16 B hash

本机 CPU flags 包含 `crc32`，构建参数已经是：

```text
-O3 -march=armv8.2-a -mtune=tsv110
```

Huawei 官方建议 Kunpeng 920 使用 `-march=armv8.2-a -mtune=tsv110`；Kunpeng ISA-L 也使用
CRC32 指令和流水并行优化 CRC 热点，说明该执行资源是鲲鹏明确支持的优化方向：

- [Kunpeng compiler tuning](https://www.hikunpeng.com/document/detail/en/kunpenghpcs/tngg/kunpenghpcsolution_05_0023.html)
- [Kunpeng instruction-based ISA-L optimization](https://www.hikunpeng.com/document/detail/en/kunpengsdss/twp/sds_twp_041.html)
- [Arm runtime CPU feature detection](https://developer.arm.com/community/arm-community-blogs/b/operating-systems-blog/posts/runtime-detection-of-cpu-features-on-an-armv8-a-cpu)

运行时同时检查 CPU 型号与 `AT_HWCAP/HWCAP_CRC32`，不会仅凭编译架构执行可选指令。日志会
输出 `hash=crc32c-16` 或 `hash=fnv64`；反汇编已确认生成 `CRC32CX`。

### 3.1 微基准

固定 CPU 160、相同数据与编译参数：

| 路径 | FNV64 | CRC32C | 差异 |
| --- | ---: | ---: | ---: |
| 纯 16 B hash | 11.74 ns/op | 1.93 ns/op | -83.5% |
| SCALAR 16 B table lookup | 33.17 ns/op | 17.20 ns/op | -48.1% |
| SCALAR 32 B table lookup | 约 52.5 ns/op | 约 52.5 ns/op | 无回退 |

表级数字比纯 hash 更重要：它包含 hash、probe、key compare 和 LRU，而不是只测一条指令。

## 4. 其他指令候选

| 指令/能力 | 当前判断 | 原因与适用条件 |
| --- | --- | --- |
| NEON 128-bit compare | 暂不接入 | 16 B SCALAR lookup 17.2 ns，NEON 约 19.3 ns；短探测无法摊薄向量准备和 lane 提取 |
| SVE control scan | 暂不接入 | 本机 SVE VL=32 B，但 16 B lookup 约 22.0 ns；当前数据结构不是长连续扫描 |
| `PRFM` 软件预取 | 低优先级实验 | hash 后才能知道随机槽，正常 probe 很短；只有 perf 证明 cache miss 主导且能找到自然提前量才值得加入 |
| PMULL | 不用于当前 16 B key | 更适合长块 CRC/多项式折叠；两条 CRC32CX 已覆盖 16 B，PMULL 初始化成本难摊薄 |
| LSE atomics | 不适用 | Java wrapper 当前串行化访问，Native 表不是多线程共享原子哈希表 |
| DC ZVA/memset | 不进热路径 | 只可能影响 clear/rebuild，频率远低于 lookup/remove；标准库 memset 通常已使用平台优化 |

NEON 是 Kunpeng 920 支持的 128-bit SIMD，但“支持”不代表对任何数据结构都更快。官方材料也
将 NEON 描述为并行处理多个同类元素的手段；当前短 probe、随机访存和 LRU 依赖不满足这个
前提：[Kunpeng NEON intrinsics](https://www.hikunpeng.com/document/detail/zh/perftuning/progtuneg/kunpengprogramming_05_0024.html)。

## 5. 验证门槛

CRC32C 版本必须同时满足：

1. Native CTest 和 Java 16 B/淘汰 hash 协议测试通过。
2. 反汇编存在 `CRC32CX`，启动日志报告 `hash=crc32c-16`。
3. 32 B fallback 不回退。
4. Nexmark q4 严格 `FNV -> CRC -> CRC -> FNV` 不回退；只有 q4 有稳定收益后才扩大 q9/q20。

四项均已验证。q4 使用同一份 Java 字节码、相同配置和 20 M events，两个 JAR 仅内嵌的
Native hash 构建不同；每轮日志分别确认 `hash=fnv64` 或 `hash=crc32c-16`：

| 顺序 | hash | throughput (events/s) | 相邻配对差异 |
| ---: | --- | ---: | ---: |
| 1 | FNV64 | 797,100 | — |
| 2 | CRC32C | 827,540 | +3.82% |
| 3 | CRC32C | 800,900 | -3.21%（相对顺序 4） |
| 4 | FNV64 | 827,440 | — |
| 平均 | FNV64 / CRC32C | 812,270 / 814,220 | **+0.24%** |

因此不能宣称 q4 有稳定提升：两个配对方向相反，均值差异处于运行波动范围。但也没有观察到
总体回退。第一性原因是 CRC32C 只缩短了 Native table core；JNI、key transport、Java 状态逻辑
和 RocksDB 工作量没有减少。它适合作为鲲鹏硬件亲和的低层优化保留，不应包装成已证实的
Nexmark 吞吐优化。由于未跨过“q4 稳定收益”门槛，本轮不扩大到 q9/q20。

### 5.1 16 B primitive JNI lookup

为减少 CRC32CX 优化之外的固定开销，常见的 16 B on-heap `BinaryRowData` 不再把 `byte[]`、
offset 和 length 传给 JNI lookup，而是在 Java 侧直接读取两个 `long`，Native 在栈上恢复完全
相同的 16 B。表、hash、probe、LRU 和返回对象语义均未改变；其他 key 和诊断采样仍走原路径。

相同 Native 表和查询分布的完整 JNI lookup 微基准：

| 路径 | 平均耗时 | 差异 |
| --- | ---: | ---: |
| `byte[]` 16 B lookup | 148.69 ns/op | — |
| two-`long` 16 B lookup | 88.91 ns/op | **-40.2%** |

严格 q4 采用 `old -> word16 -> word16 -> old`、20 M events，唯一变量是 CacheKit JAR：

| 顺序 | 版本 | throughput (events/s) |
| ---: | --- | ---: |
| 1 | old | 820,920 |
| 2 | word16 | 824,160 |
| 3 | word16 | 824,130 |
| 4 | old | 814,070 |
| 平均 | old / word16 | 817,495 / 824,145（**+0.81%**） |

两个 word16 样本都高于两个 old 样本，且 word16 两次只差 30 events/s，说明有正向信号；但
样本数仅 2+2，不能据此宣称稳定的生产收益。结果目录位于
`/home/wutb/nexmark-bench-v2/results/20260821T01*kunpeng-native-word16-q4-*`。

随后把 two-`long` 传输扩展到 16 B `put/remove`，并用
`lookup-only -> full16 -> full16 -> lookup-only` 做增量 q4 A/B：

| 顺序 | 版本 | throughput (events/s) |
| ---: | --- | ---: |
| 1 | lookup-only | 841,720 |
| 2 | full16 | 824,160 |
| 3 | full16 | 813,600 |
| 4 | lookup-only | 803,860 |
| 平均 | lookup-only / full16 | 822,790 / 818,880（**-0.48%**） |

整组存在明显时间漂移，两个相邻配对方向相反，无法证明 put/remove primitive 化有收益；均值还
有小幅回退。因此生产代码撤回 `nativePut16/nativeRemove16`，仅保留 lookup 快路径。第一性原因
是 lookup 是每次 snapshot 查询必经路径，而 put 只发生在 miss 后，remove 又大多被 membership
hint 跳过；继续优化低频调用只增加接口和维护成本，难以转化为端到端吞吐。

用于严格 A/B 的 CMake 选项为 `CACHEKIT_FORCE_FNV_HASH=ON`，默认 `OFF`，只影响对照构建；
生产构建仍按 CPU 型号、HWCAP 和 key 长度自动选择。

## 6. 代码位置

- CPU/HWCAP 检测、CRC32C hash：`src/native/snapshot-cache/src/byte_snapshot_table.cc`
- JNI hash 名称：`src/native/snapshot-cache/src/snapshot_jni.cc`
- Java 初始化日志：`src/main/java/.../state/NativeMapSnapshotCache.java`
- 16 B/FNV 协议测试：`src/native/snapshot-cache/test/byte_snapshot_table_test.cc`
- 16 B/32 B 微基准：`src/native/snapshot-cache/bench/snapshot_table_bench.cc`
- q4 有效结果：`/home/wutb/nexmark-bench-v2/results/20260820T201902+0800_kunpeng-native-crc32c-q4-1-fnv-20m-20260818`、
  `20260820T202039+0800_kunpeng-native-crc32c-q4-2-crc-20m-20260818`、
  `20260820T202217+0800_kunpeng-native-crc32c-q4-3-crc-20m-20260818`、
  `20260820T202355+0800_kunpeng-native-crc32c-q4-4-fnv-20m-20260818`
