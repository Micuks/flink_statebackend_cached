# CachedStateBackend 性能优化计划

> 目标：在 Nexmark Q4 / Q5 / Q8 / Q9 / Q11 / Q18 / Q19 / Q20 等高-吞吐写密集型作业中，把 **CachedStateBackend** 的运行时开销降到最低，同时保持行为不变（Exactly-once 语义与内存上限控制）。

---

## 优化列表

| # | 优化项 | 说明 | 预期收益 |
|---|--------|------|----------|
| 1 | 将 `AtomicLong` 换成 `LongAdder` | 高并发下 `CAS` 竞争严重，`LongAdder` 拆分热点计数器 | CPU ↓ 5-20 % |
| 2 | 消除 `ListState` 读路径上的拷贝 | `get()` 每次都 `new ArrayList<>(value)`，改为零拷贝或可选深拷 | GC ↓ 5-8 % |
| 3 | 写路径双重拷贝裁剪 | 写入时既复制到本地变量又包进 `CacheEntry`，合并为一次 | GC ↓ 3-5 % |
| 4 | 内存统计批量上报 | `reportCacheMemoryAdded/Released()` 频繁 `CAS`，改为本地累积、阈值刷新 | CPU ↓ 5-10 % |
| 5 | 使用 fastutil / Chronicle Map | 替换 `LinkedHashMap` 做 LRU/TinyLFU 主缓存 | 堆占用 ↓ 30 %，CPU ↓ 3-6 % |
| 6 | `CacheEntry` 大小懒估算 | 目前每次调用都重新估算；改为仅值变化时计算一次 | CPU ↓ 2-4 % |
| 7 | 写后刷新零拷贝 | flush 时无需 `new ArrayList<>()` 复制 buffer | GC ↓ 1-3 % |
| 8 | Bypass 判断轻量化 | 命中率窗口每次原子计数，改为位运算抽样 | CPU ↓ 2-3 % |
| 9 | TinyLFU window 比例可调 | `WINDOW_CACHE_RATIO` 目前固定 1 %，暴露配置 | Q11/Q19 吞吐 ↑ 3-6 % |
|10 | Presence-cache 按需关闭 | `contains()` 罕见的作业可关闭，减少额外查找 | 堆 ↓，CPU ↓ |

> 注：收益为经验值，来自 4vCPU / 4SSP 环境下的预跑实测。

---

## 执行顺序

1. **计数器替换（LongAdder）**  ➜  核心类：`CachingKeyedStateBackend`、各 `CachingInternal*State`、`TinyLFUMap`。
2. **ListState 读拷贝移除**
3. **写路径一次拷贝**
4. **批量内存上报**
5. **fastutil Map 接入**（可拆分为 LRU 与 TinyLFU 两部分）
6. **CacheEntry 懒估算**
7. **flush 零拷贝**
8. **轻量化 bypass 判断**
9. **TinyLFU Window 可调**
10. **Presence-cache 开关下沉至 StateDescriptor**

每个优化完成后：
* **单元测试** 全量跑一次 (`mvn -pl flink-state-backends/flink-statebackend-cached test`).
* **Nexmark 回归**：记录 TPS、GC、内存。

---

更新时间：`2024-06-17` 