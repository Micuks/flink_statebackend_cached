### 写后缓冲一致性工作计划（全部状态类型）

以下工作分为计划 A、B、C 三个阶段，每个阶段都覆盖 `ValueState`、`ListState`、`AggregatingState`、`MapState` 四种状态实现（对应类：`CachingInternalValueState`、`CachingInternalListState`、`CachingInternalAggregatingState`、`CachingInternalMapState`）。

---

#### 计划 A – 引入全局写后（Write-Behind）开关
1. **读取配置**  
   - 通过 `CachingKeyedStateBackend#isWriteBehindEnabled()` 向各 `CachingInternal*State` 暴露布尔值。  
   - 配置项：`state.backend.cached.write-behind.enabled`（默认 **true**）。
2. **在各状态实现中生效**  
   - `CachingInternalValueState.update` 已实现；保持不变。  
   - `CachingInternalListState.update/add/addAll` **TODO**：当开关关闭时跳过 `namespaceWriteBuffers`，直接调用 `delegateState`。  
   - `CachingInternalAggregatingState.add/updateInternal` **TODO**：同上。  
   - `CachingInternalMapState.put/putAll/remove` 可选：保持强制写后模式，或加入同样的开关控制（评估后决定）。
3. **尺寸估算与内存计数**  
   - 当写后关闭时，仍需维持 L1/L2 读缓存，但写操作不再增加 `currentEstimatedCacheSizeBytes`。

---

#### 计划 B – 写直达（Write-Through）逻辑
1. **跳过内存缓冲**  
   - 关闭写后时，不向 `namespaceWriteBuffers` 写入；相关 `Map#computeIfAbsent` 调用移除。  
   - 若 L1/L2 缓存存在脏条目，先同步刷新再执行写直达。  
2. **直接下推到代理后端**  
   - 调用 `delegateState.update/put/remove` 并捕获异常包装为 `IOException`。  
3. **同步更新缓存**  
   - 把最新值以 *clean* 状态写回 L1（必要时拷贝）。  
   - 为保持一致性，清除同 key 的 L2 缓存条目。

---

#### 计划 C – 测试、文档与回归保障
1. **单元测试**  
   - 参数化测试，分别在 `write-behind.enabled=true/false` 下运行。  
   - 使用 Mockito 统计 `delegateState` 调用次数：  
     - 开启写后：每次 `flushToUnderlyingState` 前 delegate 调用应为 0。  
     - 关闭写后：每次写操作应立即触发 delegate 调用。  
   - 每个状态类型至少覆盖：`update`、`add`、`put`、`remove` 等高频路径。
2. **文档**  
   - 更新 `WRITE_BEHIND_CN.md` 与英文版 README：说明支持的状态类型及其行为差异。  
   - 在 Javadoc 中标注 "当 `state.backend.cached.write-behind.enabled` 关闭时，此方法写直达"。
3. **性能回归**  
   - 在 Nexmark Q4 基准上验证：关闭写后时吞吐与 RocksDB 等价；开启时无显著回退。  
   - 对比增量：确保 `checkAndTriggerGlobalEviction` 不再成为 >10 % CPU 热点。

---

> **备注**：
> - 计划 A、B 可以并行开发；但 Plan B 必须在 Plan A 基础上才能合并。
> - 完成 Plan C 前不得关闭相关 GitHub Issue。 