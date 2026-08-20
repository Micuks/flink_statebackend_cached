# Kunpeng 滑动延迟预取实验记录（2026-08-20）

## 实现

- 队首保护：不预取距离消费头部 64 条以内的记录。
- 滑动尾部窗口：随 mailbox 消费进度，只把窗口尾部按 64 条一块提交给异步 worker。
- 精确取消：每个预取 reservation 使用独立 ticket；下游 live-read 选中同一 key 时，撤销对应 ticket。worker 在 RocksDB 读取前和发布 staging 前分别复核。
- 自适应准入：累计 512 次异步读取后，若 useful rate 低于 2%，停止构造普通任务，仅保留低频探测。本次筛选把探测间隔设为 1,000,000,000 个任务，以快速隔离无效开销。
- 生命周期修复：backend dispose 前先关闭并排空预取 worker，避免访问已释放的 RocksDB delegate。

实现分支：`codex/cachekit-prefetch-sliding-window-20260819`

实验源码提交：`0af88f3828e41ad68af0aad5a1d7d4a6e4a4c77d`

## 实验协议

- 平台：Kunpeng 920。
- Nexmark：15q，50M events，一轮快速筛选，checkpoint 关闭。
- 拓扑：2 个容器，每容器 4 个 TaskManager，共 8 个 TaskManager。
- Control：原 flush-time bp-prefetch，`async-chunks=false`。
- Candidate：滑动尾部预取，`head-guard=64`、`chunk=64`、精确取消、自适应准入。
- 两腿共同配置：mailbox-batch、local-preagg、bypass=false；Chen 的 COW/RYW/PQ 均关闭。
- `/tmp` 已被其他数据占满，权威 v6 的两腿统一使用 expdir 内磁盘 scratch。不得把早期 tmpfs 腿混入 v6。
- 为绕过目标机 RocksDB 6.20.3 在 q9/q18 终止阶段永久卡在 `RocksDB.closeDatabase` 的问题，两腿共同启用 benchmark-only 属性 `-Dcachekit.rocksdb.skip-native-close-on-dispose=true`；每腿结束均销毁 TaskManager 容器。该属性不是 treatment。

权威远端 expdir：`/home/wuql/flink-cluster/experiments/cachekit-prefetch-adaptive-15q-50m-v6-20260820`

本地 compact 证据：`/mnt/data2/wuql/flink-cluster/OmniStateStore/dse_results/cachekit-prefetch-adaptive-15q-50m-v6-20260820`

运行时 Flink dist SHA-256：`ccbf79aad3a85bb9ebd3cea38d8b594dcc65d579c1d4d5bf1a7f470a279e5e96`

## 一轮结果

单位为 K/s/core；提升为 `(candidate / control - 1) × 100%`。

| Query | Control | Candidate | 提升 |
|---|---:|---:|---:|
| q4 | 28.93 | 29.05 | +0.41% |
| q5 | 44.40 | 45.02 | +1.40% |
| q8 | 83.19 | 83.82 | +0.76% |
| q9 | 15.64 | 15.77 | +0.83% |
| q11 | 26.31 | 25.61 | -2.66% |
| q18 | 60.11 | 59.65 | -0.77% |
| q19 | 37.14 | 37.13 | -0.03% |
| q20 | 25.07 | 24.70 | -1.48% |
| q3 | 97.59 | 103.04 | +5.58% |
| q7 | 26.91 | 26.81 | -0.37% |
| q12 | 69.11 | 72.34 | +4.67% |
| q13 | 93.10 | 93.68 | +0.62% |
| q15 | 33.23 | 32.19 | -3.13% |
| q16 | 7.50 | 7.90 | +5.33% |
| q17 | 82.44 | 78.66 | -4.59% |

分组的逐 query 提升算术平均：

- 大状态 8q：-0.19%。
- 小状态 7q：+1.16%。
- ValueState 11q：+0.17%。
- 任意状态 14q：+0.43%。
- 全部 15q：+0.44%。

## Gate 结论

10% Nexmark 整体提升 gate 未通过。本次实现显著减少了无效任务，但没有产生有效 overlap：所有真正构造 candidate 预取任务的 query 中，`asyncUsefulValues=0` 且 `promoted=0`。例如：

- q4：250 个任务、2,063 次异步读取、1,663 次 race/1,662 次取消，0 useful。
- q9：210 个任务、2,079 次异步读取、1,300 次 race/1,300 次取消，0 useful。
- q15：349 个任务、4,648 次异步读取、363 次 race/363 次取消，0 useful。
- q16：529 个任务、8,297 次异步读取、2,633 次 race/2,632 次取消，0 useful。
- q17：332 个任务、2,067 次异步读取、2,447 次 race/2,447 次取消，0 useful。

因此当前瓶颈不是窗口太靠前，而是 mailbox flush 时才观察未来 key，预取启动仍晚于 live-read。q3/q19/q20 不适用 ValueState prefetch；q13 无 ValueState/MapState，其正提升属于运行噪声。q7 candidate 吞吐证据有效，但 measured-leg prefetch summary 未出现，已明确标为 `mechanism_attribution_complete=false`，不用于机制归因。

下一步若继续追求 10%，应把 key 暴露点前移到网络反序列化/入 mailbox 队列时，而不是继续调 head-guard 或 chunk size；否则只是在缩小无效工作，无法形成可被消费命中的 staging 生命周期。
