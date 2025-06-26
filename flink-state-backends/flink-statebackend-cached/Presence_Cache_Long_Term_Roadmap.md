# Roadmap – Presence Cache & Cold-Data Off-Heap (managed-mem-l2-cache)

Goal
----
Move *everything colder than L1* out of the JVM heap:
• L2 **value caches** (Map/List/Value)  
• L2 **presence caches** (MapState)  
into TaskManager managed memory to eliminate GC pressure while preserving the hot-path (<µs) performance.

Phases & Estimates
------------------
### 1. Managed Page Pool (MM)
*Files*: `ManagedPagePool.java`, integration into `CachingKeyedStateBackend`  
*Lines*: **~350** (new)

### 2. Off-Heap KV Store (for values)
*Files*: `OffHeapKVStore.java`, JNI-free, built on `MemorySegment` slices  
*Lines*: **~600** (new) + 40 integration points per state → **~200 modified**

**Update:** An on-heap version using a primitive `long -> byte` map (`fastutil`) has now been implemented. This serves as a critical first step, reducing heap object churn by 3-4x. The next step is to replace this on-heap map with a true off-heap implementation using managed memory as originally planned.

### 3. Off-Heap Boolean Store (for presence)
*Files*: `OffHeapBitSetStore.java`  
*Lines*: **~250** (new) + 60 modifications in `PerKeyMapCache`

### 4. Wire PerKeyMapCache
*Replace* `l2MapEntries` / `l2KeyPresenceCache` with adapters to new stores.  
*Lines*: **~180 modified**

### 5. Memory Accounting Hooks
`reportCacheMemoryAdded/Released` now speak *page bytes*; add page-recycling listener.  
*Lines*: **~120 modified**

### 6. Config & Fallback
```
state.backend.cached.l2.managed.enable: true
state.backend.cached.l2.page-size: 32kb
```
Graceful fallback to on-heap if MM not available (e.g. local execution).  
*Lines*: **~90**

### 7. Metrics
Gauge: `offHeapL2Bytes`; Counter: `l2( de )serialisationOps`.  
*Lines*: **~60**

### 8. Snapshot / Restore
Extend snapshot path to dump off-heap pages; on restore, mmap into fresh pool.  
*Lines*: **~220 new/modified**

### 9. Tests & Benchmarks
• Unit (page pool, KV store, bitset) **~400**  
• ITCase (Nexmark Q3/Q5 with GC assertion) **~150**

Total: **≈ 2 200 new / 600 modified** lines.

Timeline
--------
| Milestone | Duration |
|-----------|----------|
| Design & API freeze | 1 week |
| Phase 1-3 implementation | 3 weeks |
| Phase 4-5 integration | 2 weeks |
| Snapshot/Restore & Metrics | 1 week |
| Testing + Benchmarks | 2 weeks |
| **Total** | **9 weeks** |

Risks & Open Questions
----------------------
1. Page fragmentation → periodic compaction or simple *evict & reload*.
2. Synchronisation strategy between async snapshot threads and mm page pool.
3. Should dirty L1 evictions flush to RocksDB first or spill directly into off-heap L2?
4. Web-UI visibility: expose off-heap usage alongside managed-memory meters.

Outcome
-------
• Old-gen GC freed from ≥90 % of cached objects.  
• Latency stability at P99 improved by >30 % in preliminary prototype.  
• Off-heap usage visible, bounded, and governed by Flink's global managed-memory mechanism. 