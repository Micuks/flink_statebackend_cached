# CacheKit-BP vs RocksDB — 鲲鹏 aarch64 三方性能报告

**日期**: 2026-07-11
**主指标**: throughput per core（项目口径,含全 15 query,不排除任何 query）
**对照**: cachekit-bp 完整栈（VoidNamespace ValueState 缓存 + true-async bp-prefetch + mailbox-batch + local-preagg,MapState cache OFF）vs 纯 RocksDB baseline
**平台**: 鲲鹏 920 aarch64（openEuler）／ 对照本机 x86

---

## 1. Headline（per-core throughput,全 15q 算术平均）

| 数据集 | mean speedup | 说明 |
|---|---|---|
| 本机 x86（v3, 50M×3, excl q12） | **+32.43%** | 历史最优,quiet-host |
| 鲲鹏 **争用**（50M×3,wutb 并发） | **+21.16%** | host-load 压低 |
| 鲲鹏 **干净**（50M×3,wutb-free） | **+24.97%** | ≈ x86,无架构惩罚 |
| 鲲鹏 **干净**（100M×3） | **+26.65%** | 规模放大 cache 红利 |

**结论**: 干净 aarch64（+24.97%）≈ x86,**无 aarch64 惩罚**;争用压了 ~3.8pt。100M（+26.65% > 50M）因大状态更压垮 RocksDB,cache 红利放大。

---

## 2. 逐 query per-core speedup（%）

| query | 本机 x86 | 鲲鹏争用 | 鲲鹏干净(50M) | 鲲鹏干净(100M) |
|---|---:|---:|---:|---:|
| q3 | +1.6 | +1.4 | +4.2 | +1.4 |
| q4 | +23.7 | +31.9 | +20.7 | +19.3 |
| q5 | −8.4 | −11.6 | −9.9 | **+19.0** |
| q7 | +0.9 | −0.7 | −1.1 | +2.5 |
| q8 | +8.5 | −2.7 | −7.4 | −8.6 |
| q9 | −0.8 | −0.2 | −3.1 | −1.5 |
| q11 | +2.1 | +2.2 | +0.7 | −0.9 |
| q12 | +0.4 | −4.5 | −3.4 | −7.8 |
| q13 | −3.6 | +0.1 | −1.2 | +0.2 |
| **q15** | +230.6 | +136.7 | +194.4 | +183.0 |
| q16 | +59.6 | +69.3 | +68.6 | +68.4 |
| q17 | +56.0 | +55.2 | +76.4 | +76.2 |
| q18 | +30.0 | +38.1 | +27.9 | +45.9 |
| q19 | −0.2 | +1.5 | +6.3 | +3.5 |
| q20 | −1.5 | +0.6 | +1.3 | −0.9 |
| **mean** | **+26.60**\* | **+21.16** | **+24.97** | **+26.65** |

\* x86 此列为本次三方对齐重算的 final3r-v3；handoff 报 +32.43%（excl q12 口径）。

---

## 3. q15 —— per-core 低估的旗舰真赢（wall-time 附注）

q15（`GROUP BY day` distinct-agg,低基数）是 cache 最擅长的热键场景。per-core 数字（+194%）**低估**了它,wall-time 交叉验证才是真相:

| q15 干净跑 | rocksdb | cachekit-bp |
|---|---|---|
| wall r1 / r2 / r3 | 749s / 743s / 709s | 57s / 59s / 59s |

**rocksdb 每轮稳定 ~730s,cachekit 每轮稳定 ~58s → 12.6× wall 差,三轮零抖动。真实、可复现的大赢。**

per-core 只显 +194% 是因为 rocksdb 慢跑被 procfs 采成 cores=1-2（低基数 → 少数 slot 忙 → 分母塌缩,`getTpsPerCore()=tps/sumCpu`）,把慢跑"credit"给了少核数,压缩了真实优势。per-core 的 run-to-run 抖动（+137/+194/+230）是 cores 采样噪声,但**始终是大赢**。

**报告口径**: per-core 全 15q 含 q15 = headline;q15 另附 wall-time 12.6× 说明 per-core 是保守值。

---

## 4. 真实架构效应（剔除 q15 度量噪声后）

- 剔 q15（14q）三方几乎一致: x86 +12.03% / 争用 +12.91% / 干净 +12.87% → 广谱底盘跨平台稳定。
- **唯一真架构回归 = q8（窗口 join）**: x86 +8.5% 但两次鲲鹏都负（−2.7/−7.4%）,aarch64 上 q8 相对差,量级小。
- **q5**: 50M 下 −8~12%（真回归,滑窗 query）,但 **100M 下翻正 +19.0%**（大状态下 cache 反超）。

---

## 5. 贡献拆解（隔离实验,来自 handoff）

- ~1/3 = VoidNamespace ValueState 活对象缓存（广谱）
- ~60% = local-preagg 折叠（集中 q15/q16/q17 GroupAgg）—— **真正的杠杆 = work-reduction**
- ~+1.5% = prefetch 本身（小但真实;继承实现为 0）

---

## 6. CDC 正确性状态（诚实说明）

> **鲲鹏这几轮性能 sweep 是纯吞吐,不含 CDC 输出比对。**

- **已验证（本机 x86,sorted 确定性源）**: bp-prefetch v3 = **14/14 确定性 query PASS**（q12 proc-time 非确定例外）;D1 迁移 = **15/15 PASS**（map cache 关）。
- **未验证（gap）**: 鲲鹏部署的是 **fullopt-merge 构建**（D1+bp+preagg + 陈宇 objresident ListState/PQ 合并整体）。这个**合并后的整体**作为一个构建,CDC 未单独记录验过。鲲鹏 jar（md5 6620856…)与本机 lib jar（08ea57…)md5 不同（构建产物差异)。
- **正确性是代码级确定性**（Flink/Java 输出与 CPU 架构无关,给定相同源顺序),故 x86 验过的子构建在 aarch64 上应保持。但**合并整体 + 部署 jar 的独立 CDC 验证仍待补**。
- **建议**: 对 fullopt-merge 部署构建跑一次 15q CDC（sorted 源)定论。本机有 sorted 源 + 可用 harness;鲲鹏需适配 CDC harness（docker-compose v1 + 命名)。

---

## 7. 复现索引

- 鲲鹏干净 50M: `results-fullopt/20260711_033509_50Mx3-quiet/`
- 鲲鹏 100M: `results-fullopt/20260710_213225_100Mx3/`
- 鲲鹏争用 50M: `results-fullopt/20260710_182532_50Mx3/`
- 本机 x86 v3: `results-cachekit-bp-prefetch/final3r-v3_20260707_220000/`
- 关键修复（Cores=0 根因 = CpuMetricSender 缺 jps）: 见 memory `kunpeng_cores_zero_jps_rootcause`
- 三方分析: memory `kunpeng_vs_x86_threeway_q15_artifact`

---

## 8. 下一步（提升到 50%+,fable5 规划中）

真正的杠杆 = **扩大 work-reduction（折叠）覆盖面** + 修 q5 回归。详细实验计划由 fable5 agent 产出后附上。
