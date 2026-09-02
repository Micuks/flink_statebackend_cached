# CacheKit Stage 1 / Stage 3 native 性能修复 TODO

基线提交：`6511742619fe2a26ce2f86453396a974f5d8e673`

目标分支：`wuql/cachekit/dev`

实验口径：Kunpeng，canonical FullOpt，100M events，checkpoint 关闭。

## 验收原则

- 每一项先补定向测试，再修改实现；定向测试通过后才标记完成。
- JNI/C++ 改动必须同时通过 native 单测和 Java bridge/coordinator 单测。
- 不能只以作业成功判定性能改进：最终必须保留 control/candidate 身份、配置、原始吞吐、CPU/profile 证据。
- FullOpt 只包含 Stage 1 + Stage 2 + Stage 3；Chen COW/RYW/priority-queue 和 Flink chain-copy-elision 不计入本轮收益。
- 任一优化若改变 authoritative RocksDB、generation fence、negative entry 或 stable grouping 语义，立即回退并标为阻塞，不用吞吐覆盖正确性失败。

## P0：本轮逐项修复

- [x] **P0-1 按功能开关维护 resident hint。** 当前 FullOpt 未启用 `resident-mutation-batch`，但每次 fill 仍执行 seqlock、逐项 status/error 读取、key hash 与 CAS。仅在该功能实际启用时分配和更新 hint；关闭时查询接口保守返回“可能存在”，避免假阴性。
  - 测试：disabled 模式不更新 hint 且保持保守语义；enabled 模式继续拒绝从未 resident 的 key，并在成功 fill 后接受该 key。
- [ ] **P0-2 ValueState 单点 probe 改为 direct key serialization。** 删除 `byte[] preparedKey + Collections.singletonList` 热路径，直接写入租用槽。
  - 测试：单点 native probe 调用五参数 direct serializer，且不调用四参数 heap serializer；hit/miss/negative/fallback 语义不变。
- [ ] **P0-3 FullOpt 非 resident-only write-through 改为 direct key/value serialization。** 删除每次 authoritative mutation 后额外的 key/value 堆数组；仍保持 RocksDB 先写、native 后发布和 generation fence 顺序。
  - 测试：update/tombstone 走 direct writers；序列化/JNI 失败仍 fail closed，RocksDB 保持 authoritative。
- [ ] **P0-4 去掉 probe/fill 状态读取的逐项 ByteBuffer duplicate。** 结果缓冲区构造时已固定 native byte order，absolute `getInt` 可直接读取。
  - 测试：所有状态/error/offset 读取结果不变；运行 bridge/coordinator 全套单测。
- [ ] **P0-5 Stage3 token grouping 去掉 native scratch token 全量复制。** `RequestPlane` 从只读 byte buffer 通过 `memcpy` 按项读取未对齐的 native-order token，保留 stable first-seen plan 与 hash collision 精确 token 比较。
  - 测试：native request-plane 与 JNI codec 测试覆盖空输入、重复 token、碰撞、容量边界、epoch wrap；Java Stage3 测试覆盖 plan 校验与 fallback。

## P1：完成 P0 后按 profile 决定是否改动

- [ ] **P1-1 缩短 `planeLock` 持有时间。** 当前 direct serializers 在单 owner 锁内运行。候选方案是 mutation slot 独立互斥，先在 slot 锁内序列化，再只在 native 调用期间持有 `planeLock`；resident check + conditional update 仍须原子。
  - 门槛：JFR/async-profiler 显示该 monitor 的等待或持锁序列化是显著热点；必须补 probe/mutation/disable/close 并发测试。
- [ ] **P1-2 降低 mutation slot 常驻 direct memory。** 默认专用槽约占 `256 KiB key + 4 MiB value + metadata`，每 keyed backend 约 4.25 MiB。不能简单缩小固定容量，否则大 key/value 会把原本正确的 workload 变成 fail-closed。
  - 候选：可增长的专用 direct arena，或复用已知上限且保持超限透明 fallback；先补大 value 和 direct-memory 预算测试。
- [ ] **P1-3 缓存 JNI 参数 view。** 当前每批仍会构造多个 `slice/duplicate/readOnlyBuffer` 对象；若 allocation profile 确认占比明显，再增加显式 used-length ABI 或安全复用 view。
- [ ] **P1-4 评估 exact LRU 的 hit-write 成本。** native cache hit 会更新精确 LRU；只有 profile 显示链表写/缓存行争用显著时，才比较 sampling/clock 方案，并单独验证 eviction 质量。
- [ ] **P1-5 评估 Stage3 Java 计数器与 collector cache。** 检查 indexed dispatch 的原子计数成本，以及 `identityHashCode` 静态 collector cache 的碰撞/生命周期问题；修复不得引入每批 collector 分配。

## 完整验证门

- [ ] Java 格式/编译与 CacheKit 定向单测通过。
- [ ] native Debug/Release 构建和 native tests 通过。
- [ ] `.jar` 与 `.so` 产物身份、SHA-256、架构、动态依赖记录完整。
- [ ] Kunpeng preflight：机器身份、空闲、Flink 停止、配置有效、native library 可加载。
- [ ] 同一 canonical FullOpt 配置下完成可比 control/candidate 100M、no-checkpoint 长跑；除代码身份外参数一致。
- [ ] profiling 同时覆盖 JVM 与 native 栈，给出优化命中路径、剩余瓶颈和收益归因边界。
- [ ] 通过 loop 持续轮询到终态，并把开始、异常/恢复、最终结果发到飞书。
