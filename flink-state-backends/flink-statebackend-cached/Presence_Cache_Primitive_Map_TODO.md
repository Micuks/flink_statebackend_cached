# TODO – Replace Presence Cache with Primitive LRU Map

Context
-------
The current key-presence cache in `PerKeyMapCache` stores each entry as a boxed `CacheEntry<Boolean>` inside an `LRUMap`.  This costs ~40-60 B per key and allocates millions of objects, creating GC pressure.  We can cut heap usage 3–4× (and remove most old-gen scanning) by switching to a **primitive** access-ordered map that stores a single byte per key.

Chosen structure: `Long2ByteLinkedOpenHashMap` from *fastutil* (access order = true).
Each entry:
```
8 bytes  – long fingerprint (key)
1 byte   – 0 / 1 (absent / present)
~7 bytes – load-factor slack & alignment
```
≈ 16 B instead of ≥ 48 B today.

Migration steps
--------------
1. **Add dependency**
   ```xml
   <dependency>
       <groupId>it.unimi.dsi</groupId>
       <artifactId>fastutil</artifactId>
       <version>8.5.12</version>
   </dependency>
   ```
2. **Utility — hashing helper**  *(~20 LoC)*
   ```java
   static long fingerprint(byte[] serializedKey) { return MurmurHash3.hash64(serializedKey); }
   ```
3. **Replace cache creation**  *(~30 LoC)*
   • In `PerKeyMapCache.createCachePolicyInstance()` add a branch `PRIMITIVE_PRESENCE` that
     instantiates `Long2ByteLinkedOpenHashMap` wrapped by a small adapter implementing `CachePolicy`.
4. **Adapter class `PrimitivePresenceCache`** *(~120 LoC)*
   Implements `CachePolicy<Long, Byte>` and forwards memory accounting via `reportCacheMemoryAdded/Released(1)`.
5. **Key serialisation once per operation** *(~50 LoC)*
   Cache `DataOutputSerializer` in thread-local; serialize UK into byte[].
6. **Update call sites** *(~60 LoC)*
   • `getValuePresence`, `updatePresenceCacheOnGet/Put/Remove/Invalidate` now work with `byte` instead of `CacheEntry<Boolean>`.
7. **Metrics** *(~15 LoC)*
   Gauge for primitive map size; hit/miss counters unchanged.
8. **Config knob** *(~15 LoC)*
   `state.backend.cached.map.presence.impl = primitive-map | default` (default keeps old impl until GA).
9. **Unit tests** *(~80 LoC)*
   • Add `PrimitivePresenceCacheTest` covering put/get/eviction & memory accounting.

Estimated effort: **≈ 400 lines modified/added**, all Java.
Roll-back safe because we keep old path behind config flag.

Target release: *next patch* (1 week dev + review). 