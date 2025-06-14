### Write-behind consistency tasks (EN / 中文)

#### Background / 背景
The cached state back-end implements an in-memory *write-behind* buffer that is flushed on eviction, snapshot and mini-batch boundaries.  `CachingInternalValueState` already honours the global flag `state.backend.cached.write-behind.enabled`, but other state types enable write-behind unconditionally.

#### TODO (English)
1. Respect the global configuration flag in:
   * `CachingInternalListState` (`update`, `add`, `addAll`)
   * `CachingInternalAggregatingState` (`add`, `updateInternal`)
   * `CachingInternalMapState` – optional: make write-behind configurable instead of always-on.
2. When the flag is **disabled** make those methods perform write-through:
   * Skip the namespace write-buffer and
   * call the underlying `delegateState.update / put / remove` directly.
3. Add unit tests toggling `WRITE_BEHIND_ENABLED_CONFIG` that verify delegate interaction counts for each state type.
4. Update documentation and Javadoc to state which state types support configurable write-behind.

#### TODO（中文）
1. 在以下类中遵循全局配置开关 `state.backend.cached.write-behind.enabled`：
   * `CachingInternalListState`（`update`、`add`、`addAll`）
   * `CachingInternalAggregatingState`（`add`、`updateInternal`）
   * 如有必要，`CachingInternalMapState` 也可改为可配置而非强制写后缓存。
2. 当 **关闭** 写后缓存时，以上方法应执行写直达（write-through）：
   * 跳过 `namespaceWriteBuffers`，
   * 直接调用底层 `delegateState.update / put / remove`。
3. 为各 State 类型新增单元测试，在打开/关闭开关两种情况下校验 delegate 调用次数。
4. 更新文档和 Javadoc，明确说明哪些 State 类型支持可配置的 write-behind 行为。 