# CacheKit Stage 1 / Stage 3 native 性能修复 TODO

基线提交：`6511742619fe2a26ce2f86453396a974f5d8e673`

目标分支：`wuql/cachekit/dev`

实验口径：Kunpeng 与云 x86，canonical FullOpt，100M events，checkpoint 关闭；不重跑 RocksDB，历史 RocksDB 只作身份兼容的完整栈参照。

## 验收原则

- 每一项先补定向测试，再修改实现；定向测试通过后才标记完成。
- JNI/C++ 改动必须同时通过 native 单测和 Java bridge/coordinator 单测。
- 不能只以作业成功判定性能改进：最终必须保留 control/candidate 身份、配置、原始吞吐、CPU/profile 证据。
- FullOpt 只包含 Stage 1 + Stage 2 + Stage 3；Chen COW/RYW/priority-queue 和 Flink chain-copy-elision 不计入本轮收益。
- 任一优化若改变 authoritative RocksDB、generation fence、negative entry 或 stable grouping 语义，立即回退并标为阻塞，不用吞吐覆盖正确性失败。

## P0：本轮逐项修复

- [x] **P0-1 按功能开关维护 resident hint。** 当前 FullOpt 未启用 `resident-mutation-batch`，但每次 fill 仍执行 seqlock、逐项 status/error 读取、key hash 与 CAS。仅在该功能实际启用时分配和更新 hint；关闭时查询接口保守返回“可能存在”，避免假阴性。
  - 测试：disabled 模式不更新 hint 且保持保守语义；enabled 模式继续拒绝从未 resident 的 key，并在成功 fill 后接受该 key。
- [x] **P0-2 ValueState 单点 probe 改为 direct key serialization。** 删除 `byte[] preparedKey + Collections.singletonList` 热路径，直接写入租用槽。
  - 测试：单点 native probe 调用五参数 direct serializer，且不调用四参数 heap serializer；hit/miss/negative/fallback 语义不变。
- [x] **P0-3 FullOpt 非 resident-only write-through 改为 direct key/value serialization。** 删除每次 authoritative mutation 后额外的 key/value 堆数组；仍保持 RocksDB 先写、native 后发布和 generation fence 顺序。
  - 测试：update/tombstone 走 direct writers；序列化/JNI 失败仍 fail closed，RocksDB 保持 authoritative。
- [x] **P0-4 去掉 probe/fill 状态读取的逐项 ByteBuffer duplicate。** 结果缓冲区构造时已固定 native byte order，absolute `getInt` 可直接读取。
  - 测试：所有状态/error/offset 读取结果不变；运行 bridge/coordinator 全套单测。
- [x] **P0-5 Stage3 token grouping 去掉 native scratch token 全量复制。** `RequestPlane` 从只读 byte buffer 通过 `memcpy` 按项读取未对齐的 native-order token，保留 stable first-seen plan 与 hash collision 精确 token 比较。
  - 测试：native request-plane 与 JNI codec 测试覆盖空输入、重复 token、碰撞、容量边界、epoch wrap；Java Stage3 测试覆盖 plan 校验与 fallback。

## P0 实测复核与归因

- [x] **对齐 Java mailbox parent 后完成双机 15Q。** 仅把先前误设的 parent 开关对齐为 `true`，其余 Stage1/2/3、100M、no-checkpoint 配置不变。相对同源 false-parent P0，Kunpeng 15Q 为 `+0.62%`，云 x86 为 `+0.33%`，说明该开关不是当前 native 性能差异的原因；BP-prefetch 先构造 batch output，实际路径不会进入后续 parent 分支。
- [x] **完成双机 q5 JFR。** Kunpeng measured-q5 active samples 中 CacheKit 占 `15.50%`，云 x86 占 `23.57%`；两机共同热点包括 ValueState native point cache、Java LRU put/eviction、`BinarySegmentUtils.copyToBytes`。该结论只归因于 q5 profile，不外推为 15Q 收益。
- [x] **用单开关关闭 native ValueState point cache 做 q4/q5 定向归因。** 相对 aligned-on，q5 在 Kunpeng 为 `+50.62%`、云 x86 为 `+35.99%`，并分别恢复到旧 Stage3 参照附近；q5 的数千万次 native point probe/fill 为零有效命中，根因为 Java L1/L2 后的残余冷 miss 仍进入共享 native cache。q4 仅为 `+0.34%` / `+1.38%`，其相对旧 Stage3 的回退仍未解释，禁止把 q5 结论外推到 q4 或 15Q。
- [x] **校正历史 200M 口径。** `+40.52%` 是实测的 Off/FullOpt 完整栈相对非同期 RocksDB R1；同一实验中 Segmented/Off 的隔离因果效果为 `-4.81%`，Segmented/RocksDB 完整栈为 `+35.61%`。Stage1native+FullOpt、Stage3native+FullOpt、Off/FullOpt 等完整栈百分比不能相加或相乘。

## P1：完成 P0 后按 profile 决定是否改动

- [ ] **P1-1 缩短 `planeLock` 持有时间。** 当前 direct serializers 在单 owner 锁内运行。候选方案是 mutation slot 独立互斥，先在 slot 锁内序列化，再只在 native 调用期间持有 `planeLock`；resident check + conditional update 仍须原子。
  - 门槛：JFR/async-profiler 显示该 monitor 的等待或持锁序列化是显著热点；必须补 probe/mutation/disable/close 并发测试。
- [ ] **P1-2 降低 mutation slot 常驻 direct memory。** 默认专用槽约占 `256 KiB key + 4 MiB value + metadata`，每 keyed backend 约 4.25 MiB。不能简单缩小固定容量，否则大 key/value 会把原本正确的 workload 变成 fail-closed。
  - 候选：可增长的专用 direct arena，或复用已知上限且保持超限透明 fallback；先补大 value 和 direct-memory 预算测试。
- [ ] **P1-3 缓存 JNI 参数 view。** 当前每批仍会构造多个 `slice/duplicate/readOnlyBuffer` 对象；若 allocation profile 确认占比明显，再增加显式 used-length ABI 或安全复用 view。
- [ ] **P1-4 评估 exact LRU 的 hit-write 成本。** native cache hit 会更新精确 LRU；只有 profile 显示链表写/缓存行争用显著时，才比较 sampling/clock 方案，并单独验证 eviction 质量。
- [ ] **P1-5 评估 Stage3 Java 计数器与 collector cache。** 检查 indexed dispatch 的原子计数成本，以及 `identityHashCode` 静态 collector cache 的碰撞/生命周期问题；修复不得引入每批 collector 分配。
- [ ] **P1-6 自适应旁路无收益的 native ValueState point cache。** 实现与单测已完成、性能门待验。开关默认关闭；显式启用后按每个 `CachedInternalValueState` 独立统计 positive/negative useful hits。连续两个 4096-probe 零命中窗口后进入 bypass，保留 mutation write-through 和 prepared prefetch；每 4096 次旁路机会只做一次 trial，trial miss 回填并记录到 64-slot primitive fingerprint 表，只有该 key 后续真正穿透 Java L1/L2 时才做 targeted recovery probe。
  - 正确性门：两窗口进入旁路、最后一次 miss 不回填、无预置数据的新只读工作集跨 Java eviction horizon 后可自然恢复、trial/targeted slot unavailable 不逐记录抢锁、positive/negative hit 均可恢复、read-activated mutation 不被抑制、不同 state 的控制器互不影响、哈希碰撞只增加一次安全 probe 而不改变返回值。
  - 性能门：先在 Kunpeng/x86 各跑 q5 100M canary；必须在日志中证明开关生效、进入旁路、probe/fill 大幅下降且 mutation 保留，并接近 value-cache-off q5。通过后才扩到 15Q；q4 单独保留为未解释回退。

## 完整验证门

- [x] Java 格式/编译与 CacheKit 定向单测通过。
- [x] native Debug/Release 构建和 native tests 通过。
- [ ] `.jar` 与 `.so` 产物身份、SHA-256、架构、动态依赖记录完整。
- [x] Kunpeng preflight：机器身份、空闲、Flink 停止、配置有效、native library 可加载。
- [x] P0 对齐版在双机完成 15Q 100M、no-checkpoint；保留 raw throughput、CPU process-tree coverage、配置/源码哈希与有效性门。
- [x] q5 profiling 覆盖 JVM 与 native 请求路径，并用 value-cache-off 定向实验确认主要低效路径；q4 仍是未解释边界。
- [ ] 自适应旁路 q5 双机 canary 与通过后的 15Q 扩展完成。
- [x] 通过 loop 持续轮询到 P0 终态，并把开始、异常/恢复、最终结果发到飞书；自适应候选仍需继续轮询和汇报。
