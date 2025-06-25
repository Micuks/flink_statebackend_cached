# TODO – Migrate L2 Cache to Managed (Off-Heap) Memory

Goal
-----
Move all L2 value & presence caches of every CachingInternal*State implementation into the Task's managed-memory pool while keeping the hot L1 caches on the JVM heap.  This should eliminate old-gen GC pressure without hurting the per-record fast path.

Feature name: **managed-mem-l2-cache**

High-level differences vs current design
---------------------------------------
1. Layer
   • Today: L2 = Java objects on heap.  
   • After: L2 = raw bytes inside MemorySegment pages allocated from MemoryManager.
2. Granularity
   • One L2 entry per user key.  Serialised separately (no RocksDB block mixing).
3. Access path changes
   • Demote (L1→L2): serialise K & V into page → free old objects.  
   • Promote (L2→L1): copy bytes to thread-local buffer → deserialise → new CacheEntry.
4. Memory accounting
   • Pages are booked against `task.managed` → visible in JM metrics and subject to Flink's global eviction logic.
5. Interaction with RocksDB block cache
   • Block cache stays untouched; it still services cold misses after L2.

Task break-down
---------------
- [ ] 1. Add `ManagedPagePool` helper (wrapper around `MemoryManager.allocatePages`).
- [ ] 2. Define `L2Page` layout (header + var-len key/value slices).
- [ ] 3. Implement `OffHeapKVStore` with APIs: `put(keyBytes,valBytes)`, `get(keyBytes)`, `remove(keyBytes)`.
- [ ] 4. Wire `PerKeyMapCache` to use OffHeapKVStore for `l2MapEntries` & (optionally) `l2KeyPresenceCache`.
- [ ] 5. Provide serializers via existing `userKeySerializer` / `userValueSerializer` (use `DataOutputSerializer`, `DataInputDeserializer`).  Reuse buffers using ThreadLocal.
- [ ] 6. Memory accounting hooks: on put → `backend.reportCacheMemoryAdded(pageSize)` when new page allocated; on evict/remove → `backend.reportCacheMemoryReleased(bytes)`.
- [ ] 7. Eviction policy: keep TinyLFU/LRU meta on-heap but store payload off-heap.
- [ ] 8. Add config knobs
      * `state.backend.cached.l2.managed.enable: boolean` (default false)
      * `state.backend.cached.l2.page-size: 32kb` etc.
- [ ] 9. Metrics: gauge current off-heap L2 bytes, counter (de)serialisation ops.
- [ ] 10. Unit tests: correctness (put/get/remove), stress memory cap, recovery from snapshot.
- [ ] 11. Benchmark with Nexmark Q3/Q5 on 1JM+8TMs (8 GB, 3 GB managed). Expect:
      * No GC-overhead OOM
      * <5 % CPU delta
      * 99p latency ‑15 ms.

Open questions
--------------
* Page fragmentation / compaction strategy?
* Should we spill dirty L1 evictions directly into OffHeap store or flush to RocksDB first?
* How to expose L2 off-heap usage to Web-UI.

Reference numbers (current defaults)
------------------------------------
L1 size = 1 024, L2 size = 8 192 ⇒ ~120 k objects/operator. Moving them off-heap saves ≈50–150 MB/task.

See also: docs/memory_management.drawio.xml, previous discussion in PR #GC-overhead-fix. 