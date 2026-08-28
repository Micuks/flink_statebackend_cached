/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit;

import java.io.IOException;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory;
import org.apache.flink.contrib.streaming.state.cachekit.cache.CachePolicyType;
import org.apache.flink.contrib.streaming.state.cachekit.cache.PresenceCacheImplementation;
import org.apache.flink.contrib.streaming.state.cachekit.nativeplane.NativeRequestPlaneOptions;
import org.apache.flink.runtime.state.BatchKeyGroupingSupport;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendFactory;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;

/**
 * A minimal factory for creating {@link CacheKitStateBackend} instances.
 *
 * <p>This is intentionally small and only exposes a single feature to start with: LRU caching for
 * {@code ValueState}.
 */
public class CacheKitStateBackendFactory implements StateBackendFactory<CacheKitStateBackend> {

    public static final ConfigOption<Integer> VALUE_CACHE_MAX_ENTRIES =
            ConfigOptions.key("state.backend.cachekit.value.cache.max-entries")
                        .intType()
                        .defaultValue(1024)
                        .withDescription("Max entries for per-ValueState LRU cache.");

    public static final ConfigOption<CachePolicyType> VALUE_CACHE_POLICY =
            ConfigOptions.key("state.backend.cachekit.value.cache.policy")
                        .enumType(CachePolicyType.class)
                        .defaultValue(CachePolicyType.LRU)
                        .withDescription("Cache policy for ValueState (LRU or CAFFEINE).");

    public static final ConfigOption<Integer> VALUE_CACHE_LRU_OVERFLOW =
            ConfigOptions.key("state.backend.cachekit.value.cache.lru.overflow")
                        .intType()
                        .defaultValue(256)
                    .withDescription("Overflow entries for LRU before batch eviction triggers.");

    public static final ConfigOption<Boolean> VALUE_BYPASS_ENABLED =
            ConfigOptions.key("state.backend.cachekit.value.bypass.enabled")
                        .booleanType()
                        .defaultValue(true)
                    .withDescription(
                            "Enable adaptive bypass for ValueState caching based on hit rate.");

    public static final ConfigOption<Double> VALUE_HIT_RATE_THRESHOLD =
            ConfigOptions.key("state.backend.cachekit.value.hit-rate.threshold")
                        .doubleType()
                        .defaultValue(0.05)
                        .withDescription(
                                        "Hit rate threshold (0.0 to 1.0) below which cache is bypassed. Default 0.05 (5%).");

    public static final ConfigOption<Integer> VALUE_HIT_RATE_WINDOW =
            ConfigOptions.key("state.backend.cachekit.value.hit-rate.window")
                        .intType()
                        .defaultValue(1000)
                        .withDescription("Number of accesses to calculate hit rate over.");

    public static final ConfigOption<Integer> MAP_PRESENCE_CACHE_MAX_ENTRIES =
            ConfigOptions.key("state.backend.cachekit.map.presence.cache.max-entries")
                        .intType()
                        .defaultValue(8192)
                        .withDescription("Max entries for per-MapState key presence cache.");

    public static final ConfigOption<CachePolicyType> MAP_PRESENCE_CACHE_POLICY =
            ConfigOptions.key("state.backend.cachekit.map.presence.cache.policy")
                        .enumType(CachePolicyType.class)
                        .defaultValue(CachePolicyType.LRU)
                    .withDescription(
                            "Cache policy for MapState key presence cache (LRU or CAFFEINE).");

    public static final ConfigOption<Integer> MAP_PRESENCE_CACHE_LRU_OVERFLOW =
            ConfigOptions.key("state.backend.cachekit.map.presence.cache.lru.overflow")
                        .intType()
                        .defaultValue(256)
                        .withDescription(
                                        "Overflow entries for MapState key presence LRU before batch eviction triggers.");

    public static final ConfigOption<PresenceCacheImplementation>
            MAP_PRESENCE_CACHE_IMPLEMENTATION =
                    ConfigOptions.key("state.backend.cachekit.map.presence.cache.impl")
                        .enumType(PresenceCacheImplementation.class)
                        .defaultValue(PresenceCacheImplementation.PRIMITIVE)
                        .withDescription(
                                        "Presence cache implementation for MapState (PRIMITIVE or OBJECT).");

    public static final ConfigOption<Integer> MAP_CACHE_MAX_ENTRIES =
            ConfigOptions.key("state.backend.cachekit.map.cache.max-entries")
                        .intType()
                        .defaultValue(4096)
                        .withDescription("Max entries for per-MapState cache.");

    public static final ConfigOption<CachePolicyType> MAP_CACHE_POLICY =
            ConfigOptions.key("state.backend.cachekit.map.cache.policy")
                        .enumType(CachePolicyType.class)
                        .defaultValue(CachePolicyType.LRU)
                        .withDescription("Cache policy for MapState (LRU or CAFFEINE).");

    public static final ConfigOption<Integer> MAP_CACHE_LRU_OVERFLOW =
            ConfigOptions.key("state.backend.cachekit.map.cache.lru.overflow")
                        .intType()
                        .defaultValue(256)
                        .withDescription(
                                        "Overflow entries for MapState LRU before batch eviction triggers.");

    public static final ConfigOption<Boolean> MAP_BYPASS_ENABLED =
            ConfigOptions.key("state.backend.cachekit.map.bypass.enabled")
                        .booleanType()
                        .defaultValue(false)
                    .withDescription(
                            "Enable adaptive bypass for MapState caching based on hit rate.");

    public static final ConfigOption<Double> MAP_HIT_RATE_THRESHOLD =
            ConfigOptions.key("state.backend.cachekit.map.hit-rate.threshold")
                        .doubleType()
                        .defaultValue(0.05)
                        .withDescription(
                                        "Hit rate threshold (0.0 to 1.0) below which MapState cache is bypassed.");

    public static final ConfigOption<Integer> MAP_HIT_RATE_WINDOW =
            ConfigOptions.key("state.backend.cachekit.map.hit-rate.window")
                        .intType()
                        .defaultValue(1000)
                        .withDescription("Number of MapState accesses to calculate hit rate over.");

    public static final ConfigOption<Boolean> MAP_ITERATION_CACHE_FILL_ENABLED =
            ConfigOptions.key("state.backend.cachekit.map.iteration.cache-fill.enabled")
                        .booleanType()
                        .defaultValue(true)
                        .withDescription("Enable cache backfill during MapState iteration.");

    public static final ConfigOption<Integer> MAP_SNAPSHOT_CACHE_MAX_ENTRIES =
            ConfigOptions.key("state.backend.cachekit.map.snapshot.cache.max-entries")
                        .intType()
                        .defaultValue(0)
                        .withDescription(
                                        "Max entries for per-MapState snapshot cache (entries() fast path). "
                                                        + "Caches (Key, Namespace) -> {EMPTY | SINGLE(UserKey)} to short-circuit "
                                                        + "entries()/iterator() calls. Set 0 to disable.");

    public static final ConfigOption<Integer> MAP_SNAPSHOT_SMALL_MAX_ENTRIES =
            ConfigOptions.key("state.backend.cachekit.map.snapshot.small.max-entries")
                        .intType()
                        .defaultValue(1)
                        .withDescription(
                                        "Largest completely observed small MapState admitted to the Java snapshot cache. "
                                                        + "1 preserves EMPTY/SINGLE behavior; 2-16 enables bounded point-get "
                                                        + "replay in original iteration order. Native snapshots remain EMPTY/SINGLE.");

    public static final ConfigOption<Boolean> DIAGNOSTICS_ENABLED =
            ConfigOptions.key("state.backend.cachekit.diagnostics.enabled")
                        .booleanType()
                        .defaultValue(false)
                        .withDescription(
                                        "Expose opt-in CacheKit diagnostic metrics through Flink REST reporters.");

        public static final ConfigOption<Boolean> NATIVE_REQUEST_PLANE_ENABLED =
                        ConfigOptions.key("state.backend.cachekit.native.request-plane.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Enable the experimental native prepared-key ValueState request plane.");

        public static final ConfigOption<String> NATIVE_REQUEST_PLANE_LIBRARY =
                        ConfigOptions.key("state.backend.cachekit.native.request-plane.library")
                                        .stringType()
                                        .defaultValue("")
                    .withDescription("Absolute JNI library path. Empty uses java.library.path.");

        public static final ConfigOption<String> NATIVE_REQUEST_PLANE_KERNEL =
                        ConfigOptions.key("state.backend.cachekit.native.request-plane.kernel")
                                        .stringType()
                                        .defaultValue("auto")
                                        .withDescription(
                                                        "Native fingerprint kernel: auto, neon, or sve256. "
                                                                        + "Kunpeng SVE-256 does not imply SVE2.");

        public static final ConfigOption<Integer> NATIVE_REQUEST_PLANE_CAPACITY_ENTRIES =
                        ConfigOptions.key("state.backend.cachekit.native.request-plane.capacity-entries")
                                        .intType()
                                        .defaultValue(16_384)
                                        .withDescription("Shared native clean-entry capacity per keyed backend.");

        public static final ConfigOption<Long> NATIVE_REQUEST_PLANE_KEY_ARENA_BYTES =
                        ConfigOptions.key("state.backend.cachekit.native.request-plane.key-arena-bytes")
                                        .longType()
                                        .defaultValue(4L << 20)
                                        .withDescription("Shared native serialized-key arena bytes.");

        public static final ConfigOption<Long> NATIVE_REQUEST_PLANE_VALUE_ARENA_BYTES =
                        ConfigOptions.key("state.backend.cachekit.native.request-plane.value-arena-bytes")
                                        .longType()
                                        .defaultValue(16L << 20)
                                        .withDescription("Shared native serialized-value arena bytes.");

        public static final ConfigOption<Integer> NATIVE_REQUEST_PLANE_BATCH_ENTRIES =
                        ConfigOptions.key("state.backend.cachekit.native.request-plane.batch-entries")
                                        .intType()
                                        .defaultValue(1024)
                                        .withDescription("Maximum entries in one prepared native batch.");

        public static final ConfigOption<Integer> NATIVE_REQUEST_PLANE_BATCH_KEY_ARENA_BYTES =
            ConfigOptions.key("state.backend.cachekit.native.request-plane.batch-key-arena-bytes")
                                        .intType()
                                        .defaultValue(256 << 10)
                                        .withDescription("Direct prepared-key bytes per bounded batch slot.");

        public static final ConfigOption<Integer> NATIVE_REQUEST_PLANE_BATCH_VALUE_ARENA_BYTES =
            ConfigOptions.key("state.backend.cachekit.native.request-plane.batch-value-arena-bytes")
                                        .intType()
                                        .defaultValue(4 << 20)
                                        .withDescription("Direct probe/fill value bytes per bounded batch slot.");

        public static final ConfigOption<Integer> NATIVE_REQUEST_PLANE_MIN_BATCH_SIZE =
                        ConfigOptions.key("state.backend.cachekit.native.request-plane.min-batch-size")
                                        .intType()
                                        .defaultValue(64)
                                        .withDescription(
                                                        "Batches below this threshold use the existing Java/RocksDB path.");

        public static final ConfigOption<Integer> NATIVE_REQUEST_PLANE_BATCH_SLOTS =
                        ConfigOptions.key("state.backend.cachekit.native.request-plane.batch-slots")
                                        .intType()
                                        .defaultValue(2)
                    .withDescription("Bounded direct batch slots; exhaustion falls back to Java.");

        public static final ConfigOption<Boolean> NATIVE_MAILBOX_COMPACTION_SCRATCH_SLOT_ENABLED =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.mailbox-compaction.scratch-slot.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "When every full native batch slot is leased, use one short-lived compaction-only scratch slot, "
                                                                        + "copy only unique prepared keys to the established Java MultiGet queue, and release it before submission. "
                                                                        + "The scratch slot has no probe/fill value arena and is disabled by default.");

        public static final ConfigOption<Integer> NATIVE_MAILBOX_COMPACTION_SCRATCH_ENTRIES =
            ConfigOptions.key("state.backend.cachekit.native.mailbox-compaction.scratch-entries")
                                        .intType()
                                        .defaultValue(4096)
                                        .withDescription(
                                                        "Maximum prepared keys in the key-only mailbox compaction scratch slot. This may exceed the full request-plane batch size because the scratch slot has no value arena.");

        public static final ConfigOption<Integer> NATIVE_MAILBOX_COMPACTION_SCRATCH_KEY_ARENA_BYTES =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.mailbox-compaction.scratch-key-arena-bytes")
                                        .intType()
                                        .defaultValue(2 << 20)
                                        .withDescription(
                                                        "Prepared-key bytes in the key-only mailbox compaction scratch slot.");

        public static final ConfigOption<Boolean> NATIVE_REQUEST_PLANE_AARCH64_ONLY =
                        ConfigOptions.key("state.backend.cachekit.native.request-plane.aarch64-only")
                                        .booleanType()
                                        .defaultValue(true)
                                        .withDescription(
                                                        "Fail closed when the native request plane is enabled on a non-AArch64 host. "
                                                                        + "Set false only for an explicit portable x86 comparison.");

        public static final ConfigOption<Boolean> NATIVE_REQUEST_PLANE_WRITE_THROUGH_MUTATIONS =
            ConfigOptions.key("state.backend.cachekit.native.request-plane.write-through-mutations")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Also serialize and publish every authoritative ValueState mutation to the native plane. "
                                                                        + "Disabled by default: exact-generation probes invalidate older native entries without duplicate JNI writes.");

    public static final ConfigOption<Boolean> NATIVE_VALUE_CACHE_READ_ACTIVATED_WRITE_THROUGH =
                                        ConfigOptions.key(
                                                                        "state.backend.cachekit.native.value-cache.read-activated-write-through.enabled")
                                                        .booleanType()
                                                        .defaultValue(false)
                                                        .withDescription(
                                                                        "Selective native ValueState mutation mode: skip publication until the state is first probed; afterwards advance the generation fence for every mutation, "
                                                                                        + "but serialize and publish the value only when that exact key is already resident. "
                                                                                        + "The check and conditional update are atomic with respect to native probes.");

        public static final ConfigOption<Boolean> NATIVE_VALUE_CACHE_RESIDENT_MUTATION_BATCH =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.value-cache.resident-mutation-batch.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Batch resident-only ValueState mutation admission at the mailbox dispatch boundary. "
                                                                        + "The batch advances the native generation fence before dispatch, bypasses native reads for pending dirty keys, "
                                                                        + "and publishes only prechecked resident keys in one bounded JNI fill at batch end.");

    public static final ConfigOption<Boolean> NATIVE_VALUE_CACHE_RESIDENT_MUTATION_BATCH_ADAPTIVE =
                                        ConfigOptions.key(
                                                                        "state.backend.cachekit.native.value-cache.resident-mutation-batch.adaptive.enabled")
                                                        .booleanType()
                                                        .defaultValue(false)
                                                        .withDescription(
                                                                        "Adaptively use resident mutation batching only after a state demonstrates enough mutations per mailbox scope and a low enough resident-hint positive rate. "
                                                                                        + "Unprofitable states fall back to the established exact per-mutation path.");

        public static final ConfigOption<Integer>
                        NATIVE_VALUE_CACHE_RESIDENT_MUTATION_BATCH_ADAPTIVE_MIN_SCOPES =
                                        ConfigOptions.key(
                                                                        "state.backend.cachekit.native.value-cache.resident-mutation-batch.adaptive.min-scopes")
                                                        .intType()
                                                        .defaultValue(4096)
                                                        .withDescription(
                                                                        "Mailbox scopes observed before adaptive resident mutation batching may bypass an unprofitable state.");

        public static final ConfigOption<Double>
                        NATIVE_VALUE_CACHE_RESIDENT_MUTATION_BATCH_ADAPTIVE_MIN_MUTATIONS_PER_SCOPE =
                                        ConfigOptions.key(
                                                                        "state.backend.cachekit.native.value-cache.resident-mutation-batch.adaptive.min-mutations-per-scope")
                                                        .doubleType()
                                                        .defaultValue(8.0)
                                                        .withDescription(
                                                                        "Minimum observed native mutation attempts per mailbox scope required to keep resident mutation batching active.");

        public static final ConfigOption<Double>
                        NATIVE_VALUE_CACHE_RESIDENT_MUTATION_BATCH_ADAPTIVE_MAX_HINT_POSITIVE_RATE =
                                        ConfigOptions.key(
                                                                        "state.backend.cachekit.native.value-cache.resident-mutation-batch.adaptive.max-hint-positive-rate")
                                                        .doubleType()
                                                        .defaultValue(0.10)
                                                        .withDescription(
                                                                        "Maximum observed resident-hint positive fraction allowed for adaptive mutation batching; higher fractions retain too much copy and exact-check work.");

        public static final ConfigOption<Boolean> NATIVE_VALUE_CACHE_ENABLED =
                        ConfigOptions.key("state.backend.cachekit.native.value-cache.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Enable the independently attributable native ValueState point-cache path. "
                                                                        + "Disabled by default so Mailbox, Prefetch, and LocalPreAgg treatments do not implicitly enable VCache.");

        public static final ConfigOption<Boolean> NATIVE_MAP_CACHE_ENABLED =
                        ConfigOptions.key("state.backend.cachekit.native.map-cache.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Enable the independently attributable native MapState point-cache path. "
                                                                        + "Range and iterator operations remain on the authoritative Java/RocksDB path.");

        public static final ConfigOption<Boolean> NATIVE_MAP_SNAPSHOT_ENABLED =
                        ConfigOptions.key("state.backend.cachekit.native.map-snapshot.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Store EMPTY/SINGLE MapState snapshot metadata in the native request plane. "
                                                                        + "Java retains snapshot ownership, mutation generation, and iterator semantics.");

        public static final ConfigOption<Boolean> NATIVE_MAP_SNAPSHOT_ADAPTIVE_BYPASS_ENABLED =
            ConfigOptions.key("state.backend.cachekit.native.map-snapshot.adaptive-bypass.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Bypass low-useful-hit native MapSnapshot probe/fill per MapState after a bounded observation window. "
                                                                        + "The authoritative delegate and Java MapSnapshot cache remain unchanged.");

        public static final ConfigOption<Integer> NATIVE_MAP_SNAPSHOT_ADAPTIVE_WINDOW_PROBES =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.map-snapshot.adaptive-bypass.window-probes")
                                        .intType()
                                        .defaultValue(8192)
                                        .withDescription(
                                                        "Native MapSnapshot probes in each observation window before evaluating useful-hit rate.");

    public static final ConfigOption<Double> NATIVE_MAP_SNAPSHOT_ADAPTIVE_MIN_USEFUL_HIT_RATE =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.map-snapshot.adaptive-bypass.min-useful-hit-rate")
                                        .doubleType()
                                        .defaultValue(0.02)
                                        .withDescription(
                                                        "Minimum combined EMPTY and SINGLE native MapSnapshot hit rate required to stay active.");

        public static final ConfigOption<Integer>
                        NATIVE_MAP_SNAPSHOT_ADAPTIVE_RESAMPLE_INTERVAL_PROBES =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.map-snapshot.adaptive-bypass.resample-interval-probes")
                                        .intType()
                                        .defaultValue(262144)
                                        .withDescription(
                                                        "Bypassed lookup opportunities before reopening a full observation window for workload phase changes.");

        public static final ConfigOption<Boolean> NATIVE_MAILBOX_BATCH_ENABLED =
                        ConfigOptions.key("state.backend.cachekit.native.mailbox-batch.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Use the runtime-selected native kernel to compact exact duplicate keys "
                                                                        + "from Mailbox lookahead batches before reservation and MultiGet.");

        public static final ConfigOption<Boolean> BP_PREFETCH_CANCEL_ON_DISPATCH_ENABLED =
            ConfigOptions.key("state.backend.cachekit.bp-prefetch.cancel-on-dispatch.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Revoke exact prepared-MultiGet reservations when their records are selected "
                                                                        + "for mailbox dispatch. Published staging values are retained and "
                                                                        + "unsupported prefetch paths fail closed.");

        public static final ConfigOption<Boolean> BP_PREFETCH_KEY_SCOPED_INVALIDATION_ENABLED =
            ConfigOptions.key("state.backend.cachekit.bp-prefetch.key-scoped-invalidation.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Invalidate only the exact prefetched key touched by a delegate-visible write. "
                                                                        + "The value is carried through the configured backend instance so TaskManager class-loading cannot freeze a stale GlobalConfiguration value.");

        public static final ConfigOption<Boolean> NATIVE_PREFETCH_ENABLED =
                        ConfigOptions.key("state.backend.cachekit.native.prefetch.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Enable native prepared-key probe, miss compaction, direct-hit deserialization, "
                                                                        + "and fill around the authoritative RocksDB MultiGet path.");

        public static final ConfigOption<Boolean> NATIVE_COMPACT_SELECTED_PROBE_ENABLED =
            ConfigOptions.key("state.backend.cachekit.native.compact-selected-probe.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Probe mailbox-compacted prepared keys in their original direct arena. "
                                                                        + "Only RocksDB misses materialize heap key arrays; requires native mailbox and prefetch.");

        public static final ConfigOption<Boolean> NATIVE_DIRECT_ARENA_MULTIGET_ENABLED =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.prefetch.direct-arena-multiget.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Read compact-selected RocksDB misses directly from the prepared-key arena through one bounded MultiGet JNI call. "
                                                                        + "Any value-slot overflow falls back for the complete direct chunk.");

        public static final ConfigOption<Integer> NATIVE_DIRECT_ARENA_BATCH_SIZE =
            ConfigOptions.key("state.backend.cachekit.native.prefetch.direct-arena.batch-size")
                                        .intType()
                                        .defaultValue(64)
                                        .withDescription(
                                                        "Bound the number of prepared keys passed to one direct-arena RocksDB MultiGet. "
                                                                        + "The default retains the legacy 64-key call geometry; the extended JNI ABI accepts up to 128.");

        public static final ConfigOption<Boolean> NATIVE_DIRECT_ARENA_READ_ONLY_ENABLED =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.prefetch.direct-arena-read-only.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Bypass the native ValueState cache probe/fill for mailbox-compacted prepared keys and issue the authoritative RocksDB MultiGet directly from the native key arena. "
                                                                        + "Generation, reservation, cancellation, and staging publication guards remain unchanged; requires direct-arena MultiGet.");

    public static final ConfigOption<Boolean> NATIVE_DIRECT_ARENA_EAGER_MATERIALIZATION_ENABLED =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.prefetch.direct-arena-eager-materialization.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Deserialize direct-read-only RocksDB results from the native value arena on the prefetch worker before releasing its batch slot. "
                                                                        + "This avoids the intermediate heap byte[] copy while retaining exact reservation, generation, and staging guards.");

        public static final ConfigOption<Boolean> NATIVE_PREFETCH_NEGATIVE_HANDOFF_ENABLED =
            ConfigOptions.key("state.backend.cachekit.native.prefetch.negative-handoff.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Retain speculative direct-read-only NOT_FOUND results without allocating one lazy staged-value wrapper per key. "
                                                                        + "The exact prepared-key reservation and key-scoped write invalidation remain authoritative.");

        public static final ConfigOption<Boolean> NATIVE_PREFETCH_ACCESS_GUIDED_STATE_ENABLED =
            ConfigOptions.key("state.backend.cachekit.native.prefetch.access-guided-state.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Submit record-lookahead prefetch only to ValueState wrappers that have already served a real mailbox read. "
                                                                        + "The first access and every skipped state retain the authoritative point-read path.");

    public static final ConfigOption<Boolean> NATIVE_PREFETCH_PROMOTION_YIELD_ADMISSION_ENABLED =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.prefetch.promotion-yield-admission.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Suppress speculative work for ValueState wrappers whose staged values are not promoted by mailbox reads. "
                                                                        + "Periodic probes preserve phase-change recovery and authoritative reads are never bypassed.");

    public static final ConfigOption<Integer> NATIVE_PREFETCH_PROMOTION_YIELD_MIN_STAGED_VALUES =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.prefetch.promotion-yield-admission.min-staged-values")
                                        .intType()
                                        .defaultValue(4096)
                                        .withDescription(
                                                        "Number of staged values required before promotion-yield admission can suppress a state.");

    public static final ConfigOption<Double> NATIVE_PREFETCH_PROMOTION_YIELD_MIN_PROMOTION_RATE =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.prefetch.promotion-yield-admission.min-promotion-rate")
                                        .doubleType()
                                        .defaultValue(0.02)
                                        .withDescription(
                                                        "Minimum cumulative promoted/staged ratio for continuously admitting a ValueState prefetch stream.");

    public static final ConfigOption<Integer> NATIVE_PREFETCH_PROMOTION_YIELD_PROBE_EVERY_TASKS =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.prefetch.promotion-yield-admission.probe-every-tasks")
                                        .intType()
                                        .defaultValue(256)
                                        .withDescription(
                                                        "Low-yield admission decisions between recovery probes for a suppressed ValueState.");

        public static final ConfigOption<Boolean>
                        NATIVE_PREFETCH_DEFERRED_RESERVATION_MATERIALIZATION_ENABLED =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.prefetch.deferred-reservation-materialization.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Serialize and compact a native mailbox batch before deep-copying Java reservation keys. "
                                                                        + "Only compacted unique keys receive KeyNamespaceKey objects; fallback and authoritative reads are unchanged.");

        public static final ConfigOption<Boolean> NATIVE_MAILBOX_ADAPTIVE_DENSITY_ENABLED =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.mailbox-batch.adaptive-density.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Route duplicate-heavy ValueState lookahead batches through the existing Java prepared-MultiGet path. "
                                                                        + "The decision uses sampled compacted-unique/input density and periodically re-samples native compaction for phase recovery.");

        public static final ConfigOption<Boolean>
                        NATIVE_MAILBOX_ADAPTIVE_DENSITY_DROP_SPECULATIVE_PREFETCH_ENABLED =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.mailbox-batch.adaptive-density.drop-speculative-prefetch.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Drop a newly requested speculative ValueState prefetch task while the adaptive mailbox-density controller is in bypass mode. "
                                                                        + "The authoritative synchronous state read and periodic native recovery probes are unchanged.");

    public static final ConfigOption<Integer> NATIVE_MAILBOX_ADAPTIVE_DENSITY_WINDOW_INPUT_KEYS =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.mailbox-batch.adaptive-density.window-input-keys")
                                        .intType()
                                        .defaultValue(8192);

    public static final ConfigOption<Integer> NATIVE_MAILBOX_ADAPTIVE_DENSITY_WINDOW_BATCHES =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.mailbox-batch.adaptive-density.window-batches")
                                        .intType()
                                        .defaultValue(64);

    public static final ConfigOption<Integer> NATIVE_MAILBOX_ADAPTIVE_DENSITY_LOW_WINDOWS =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.mailbox-batch.adaptive-density.low-density-windows")
                                        .intType()
                                        .defaultValue(2);

    public static final ConfigOption<Integer> NATIVE_MAILBOX_ADAPTIVE_DENSITY_COOLDOWN_BATCHES =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.mailbox-batch.adaptive-density.cooldown-batches")
                                        .intType()
                                        .defaultValue(4096);

    public static final ConfigOption<Double> NATIVE_MAILBOX_ADAPTIVE_DENSITY_MIN_UNIQUE_RATE =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.mailbox-batch.adaptive-density.min-unique-rate")
                                        .doubleType()
                                        .defaultValue(0.10);

    public static final ConfigOption<Double> NATIVE_MAILBOX_ADAPTIVE_DENSITY_RECOVERY_UNIQUE_RATE =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.mailbox-batch.adaptive-density.recovery-unique-rate")
                                        .doubleType()
                                        .defaultValue(0.15);

        public static final ConfigOption<Boolean>
                        NATIVE_COMPACT_SELECTED_PROBE_ADAPTIVE_BYPASS_ENABLED =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.compact-selected-probe.adaptive-bypass.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Temporarily bypass zero-useful-hit native prepared-key probe/fill per ValueState. "
                                                                        + "Mailbox compaction and the authoritative RocksDB path remain unchanged.");

    public static final ConfigOption<Integer> NATIVE_COMPACT_SELECTED_PROBE_ADAPTIVE_WINDOW_KEYS =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.compact-selected-probe.adaptive-bypass.window-keys")
                                        .intType()
                                        .defaultValue(4096);

        public static final ConfigOption<Integer>
                        NATIVE_COMPACT_SELECTED_PROBE_ADAPTIVE_WINDOW_BATCHES =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.compact-selected-probe.adaptive-bypass.window-batches")
                                        .intType()
                                        .defaultValue(64);

    public static final ConfigOption<Integer> NATIVE_COMPACT_SELECTED_PROBE_ADAPTIVE_ZERO_WINDOWS =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.compact-selected-probe.adaptive-bypass.zero-windows")
                                        .intType()
                                        .defaultValue(2);

        public static final ConfigOption<Integer>
                        NATIVE_COMPACT_SELECTED_PROBE_ADAPTIVE_COOLDOWN_BATCHES =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.compact-selected-probe.adaptive-bypass.cooldown-batches")
                                        .intType()
                                        .defaultValue(256);

        public static final ConfigOption<Integer>
                        NATIVE_COMPACT_SELECTED_PROBE_ADAPTIVE_RECOVERY_MIN_USEFUL =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.compact-selected-probe.adaptive-bypass.recovery-min-useful")
                                        .intType()
                                        .defaultValue(16);

        public static final ConfigOption<Double>
                        NATIVE_COMPACT_SELECTED_PROBE_ADAPTIVE_RECOVERY_USEFUL_RATE =
                        ConfigOptions.key(
                                                        "state.backend.cachekit.native.compact-selected-probe.adaptive-bypass.recovery-useful-rate")
                                        .doubleType()
                                        .defaultValue(0.02);

        public static final ConfigOption<Boolean> NATIVE_LOCAL_PREAGG_ENABLED =
                        ConfigOptions.key("state.backend.cachekit.native.local-preagg.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Group serialized keyed records in the native runtime using stable first-seen "
                                                                        + "group ids; Java retains accumulator, emission, and ordering semantics.");

        public static final ConfigOption<Boolean> NATIVE_LOCAL_PREAGG_INDEXED_FOLD_ENABLED =
            ConfigOptions.key("state.backend.cachekit.native.local-preagg.indexed-fold.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Consume a validated native LocalPreAgg packed plan directly over the mailbox "
                                                                        + "record buffer, avoiding materialized value vectors and per-group lists. "
                                                                        + "Requires native LocalPreAgg and is disabled by default.");

        public static final ConfigOption<Boolean> DISTINCT_BATCH_OVERLAY_ENABLED =
            ConfigOptions.key("state.backend.cachekit.local-preagg.distinct-overlay.enabled")
                                        .booleanType()
                                        .defaultValue(false)
                                        .withDescription(
                                                        "Collapse repeated exact-DISTINCT MapView reads and writes within one "
                                                                        + "LocalPreagg outer-key batch. Disabled for TTL state; final values "
                                                                        + "are committed through MapState.putAll before output.");

    public static final ConfigOption<Boolean> DISTINCT_BATCH_FLAT_OVERLAY_ENABLED =
            ConfigOptions.key("state.backend.cachekit.local-preagg.distinct-overlay.flat.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Store one batch of exact-DISTINCT keys and values in a reusable flat "
                                    + "open-addressed table and expose a transient direct commit view. "
                                    + "Capacity overflow falls back to the legacy HashMap overlay.");

    public static final ConfigOption<Integer> DISTINCT_BATCH_FLAT_OVERLAY_MAX_ENTRIES =
            ConfigOptions.key(
                            "state.backend.cachekit.local-preagg.distinct-overlay.flat.max-entries")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "Maximum unique keys retained by one flat exact-DISTINCT batch before "
                                    + "transactional fallback to the legacy overlay.");

    public static final ConfigOption<Boolean> NATIVE_MAP_DISTINCT_BATCH_PREFETCH_ENABLED =
            ConfigOptions.key("state.backend.cachekit.native.map-distinct-batch-prefetch.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Prefetch exact DISTINCT MapState user keys with one ordered native RocksDB "
                                    + "MultiGet per outer-key batch. Results are scoped staging only and "
                                    + "fail closed to authoritative MapState reads.");

    public static final ConfigOption<Integer> NATIVE_MAP_DISTINCT_BATCH_PREFETCH_MIN_UNIQUE_KEYS =
            ConfigOptions.key(
                            "state.backend.cachekit.native.map-distinct-batch-prefetch.min-unique-keys")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "Minimum number of stable unique exact-DISTINCT user keys required "
                                    + "before crossing the MapView/backend/JNI MultiGet boundary. "
                                    + "Values below two are clamped to two.");

    public static final ConfigOption<Boolean>
            NATIVE_MAP_DISTINCT_BATCH_PREFETCH_DIRECT_ARENA_ENABLED =
                    ConfigOptions.key(
                                    "state.backend.cachekit.native.map-distinct-batch-prefetch.direct-arena.enabled")
                            .booleanType()
                            .defaultValue(false)
                            .withDescription(
                                    "Consume exact-DISTINCT MapState MultiGet values from the bounded "
                                            + "native direct arena instead of allocating one returned byte array "
                                            + "per hit. Requires the native request plane and direct-arena "
                                            + "MultiGet; any rejected chunk falls back transactionally.");

    public static final ConfigOption<Boolean> NATIVE_MAP_DISTINCT_PREPARED_COMMIT_ENABLED =
            ConfigOptions.key(
                            "state.backend.cachekit.native.map-distinct-batch-prefetch.prepared-commit.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Reuse validated exact-DISTINCT prepared RocksDB keys for final mutations "
                                    + "and commit eligible state columns through one WriteBatch. The "
                                    + "feature is disabled by default and any pre-write validation "
                                    + "mismatch falls back to the established MapState path.");

    public static final ConfigOption<Boolean>
            NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_ENABLED =
                    ConfigOptions.key(
                                    "state.backend.cachekit.native.map-distinct-batch-prefetch.cross-key-pipeline.enabled")
                            .booleanType()
                            .defaultValue(false)
                            .withDescription(
                                    "Prepare the next LocalPreAgg outer key's immutable exact-DISTINCT "
                                            + "RocksDB keys and overlap its native MultiGet with mailbox-owned "
                                            + "processing of the current key. Requires exact-DISTINCT batch "
                                            + "prefetch and the DISTINCT batch overlay; failures fall back to "
                                            + "the synchronous authoritative path.");

    public static final ConfigOption<Integer>
            NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_ASYNC_MIN_UNIQUE_KEYS =
                    ConfigOptions.key(
                                    "state.backend.cachekit.native.map-distinct-batch-prefetch.cross-key-pipeline.async-min-unique-keys")
                            .intType()
                            .defaultValue(8)
                            .withDescription(
                                    "Minimum exact-key count for submitting one outer-key read to "
                                            + "the cross-key async worker. Smaller eligible batches retain "
                                            + "the synchronous direct-arena transport at consumption time. "
                                            + "Values below two are clamped to two.");

    public static final ConfigOption<Integer>
            NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_LOOKAHEAD_GROUPS =
                    ConfigOptions.key(
                                    "state.backend.cachekit.native.map-distinct-batch-prefetch.cross-key-pipeline.lookahead-groups")
                            .intType()
                            .defaultValue(1)
                            .withDescription(
                                    "Maximum number of future LocalPreAgg outer-key groups prepared "
                                            + "while the current group is consumed. One preserves the "
                                            + "original current-plus-next pipeline; values are clamped "
                                            + "to the bounded range one through thirty-two when the pipeline "
                                            + "is enabled.");

    public static final ConfigOption<Boolean>
            NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_WORK_FIRST_ENABLED =
                    ConfigOptions.key(
                                    "state.backend.cachekit.native.map-distinct-batch-prefetch.cross-key-pipeline.work-first-on-backlog.enabled")
                            .booleanType()
                            .defaultValue(false)
                            .withDescription(
                                    "When the shared native MapState prefetch worker is active and "
                                            + "already has queued work, execute the newest direct-arena "
                                            + "lookahead on the mailbox caller. This keeps one queued unit "
                                            + "while bounding queue delay; disabled by default.");

    public static final ConfigOption<Boolean>
            NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_DEFERRED_WAVE_ENABLED =
                    ConfigOptions.key(
                                    "state.backend.cachekit.native.map-distinct-batch-prefetch.cross-key-deferred-wave.enabled")
                            .booleanType()
                            .defaultValue(false)
                            .withDescription(
                                    "Fuse one bounded LocalPreAgg window of exact-DISTINCT reads "
                                            + "owned by the same MapState/column family into one mailbox-owned "
                                            + "direct-arena MultiGet. Any owner, capacity, lifecycle, or protocol "
                                            + "mismatch fails closed to the existing per-group path.");

    public static final ConfigOption<Boolean>
            NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_DEFERRED_WAVE_DUAL_WORKER_ENABLED =
                    ConfigOptions.key(
                                    "state.backend.cachekit.native.map-distinct-batch-prefetch.cross-key-deferred-wave.dual-worker.enabled")
                            .booleanType()
                            .defaultValue(false)
                            .withDescription(
                                    "Submit immutable exact-map deferred waves to an isolated "
                                            + "two-worker bounded executor. The default remains the "
                                            + "single generic prefetch worker; only the frozen-key, "
                                            + "all-or-none wave path is eligible for this experiment.");

    public static final ConfigOption<Boolean>
            NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_DEFERRED_WAVE_DIRECT_ARENA_RESULTS_ENABLED =
                    ConfigOptions.key(
                                    "state.backend.cachekit.native.map-distinct-batch-prefetch.cross-key-deferred-wave.direct-arena-results.enabled")
                            .booleanType()
                            .defaultValue(false)
                            .withDescription(
                                    "Keep one single-column deferred exact-key wave's RocksDB "
                                            + "result vector in the bounded native request-plane value arena. "
                                            + "The worker publishes only in-arena result lengths; mailbox "
                                            + "consumption releases the lease after all tokens complete. Any "
                                            + "lease, overflow, lifecycle, or protocol failure falls back to "
                                            + "the established heap-result wave.");

    public static final ConfigOption<Boolean>
            NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_COLUMN_WAVE_ENABLED =
                    ConfigOptions.key(
                                    "state.backend.cachekit.native.map-distinct-batch-prefetch.cross-column-wave.enabled")
                            .booleanType()
                            .defaultValue(false)
                            .withDescription(
                                    "Fuse eligible exact-DISTINCT MapState columns backed by one "
                                            + "RocksDB instance into one executor task and one ordered "
                                            + "cross-column-family MultiGet. Disabled by default; any "
                                            + "owner, lifecycle, capacity, or protocol mismatch preserves "
                                            + "the existing per-column wave and authoritative fallback.");

    public static final ConfigOption<String> DELEGATE_BACKEND =
            ConfigOptions.key("state.backend.cachekit.delegate")
			.stringType()
			.noDefaultValue()
			.withDescription(
					"Optional fully-qualified StateBackend class name used as delegate. "
									+ "If absent, EmbeddedRocksDBStateBackend is used.");

	// ===== ListState COW + RYW 配置 (fullOpt) =====
	public static final ConfigOption<Boolean> LIST_STATE_COW_ENABLED =
			ConfigOptions.key("state.backend.cachekit.list-state.cow")
					.booleanType()
					.defaultValue(false)
					.withDescription(
							"Enable Async COW (Copy-On-Write) Flush for ListState. "
									+ "add() operations are buffered in memory and flushed asynchronously "
									+ "when threshold (4096 keys or 500 elements per list) is reached.");

	public static final ConfigOption<Boolean> LIST_STATE_RYW_ENABLED =
			ConfigOptions.key("state.backend.cachekit.list-state.ryw")
					.booleanType()
					.defaultValue(false)
					.withDescription(
							"Enable Read-Your-Writes for ListState. Tracks cleared keys and returns "
									+ "in-memory data directly without backend I/O. Can be used independently of COW.");

	public static final ConfigOption<Integer> LIST_STATE_CLEARED_KEYS_CAPACITY =
			ConfigOptions.key("state.backend.cachekit.list-state.cleared-keys.capacity")
					.intType()
					.defaultValue(200_000)
					.withDescription(
							"Max entries in the cleared-keys LRU cache for ListState RYW. "
									+ "Used to bound memory when many keys are cleared.");

	// ===== PriorityQueue 配置 (fullOpt) =====
	public static final ConfigOption<Boolean> PRIORITY_QUEUE_OPT_ENABLED =
			ConfigOptions.key("state.backend.cachekit.priority-queue.opt")
					.booleanType()
					.defaultValue(false)
					.withDescription(
							"Enable async pending buffer optimization for PriorityQueue (timers). "
									+ "ONLY effective when the delegate backend is Heap-based. "
									+ "For RocksDB delegate, this is automatically skipped "
									+ "to avoid double-buffering with the delegate's own async buffer.");

	@Override
	public CacheKitStateBackend createFromConfig(ReadableConfig config, ClassLoader classLoader)
					throws IOException {
                final int maxEntries = Math.max(0, config.get(VALUE_CACHE_MAX_ENTRIES));
                final CachePolicyType policyType = config.get(VALUE_CACHE_POLICY);
                final int lruOverflow = Math.max(0, config.get(VALUE_CACHE_LRU_OVERFLOW));
                final boolean bypassEnabled = config.get(VALUE_BYPASS_ENABLED);
                final double hitRateThreshold = config.get(VALUE_HIT_RATE_THRESHOLD);
                final int hitRateWindow = config.get(VALUE_HIT_RATE_WINDOW);
                final int mapPresenceMaxEntries = Math.max(0, config.get(MAP_PRESENCE_CACHE_MAX_ENTRIES));
                final CachePolicyType mapPresencePolicy = config.get(MAP_PRESENCE_CACHE_POLICY);
                final int mapPresenceLruOverflow = Math.max(0, config.get(MAP_PRESENCE_CACHE_LRU_OVERFLOW));
        final PresenceCacheImplementation mapPresenceImpl =
                config.get(MAP_PRESENCE_CACHE_IMPLEMENTATION);
                final int mapCacheMaxEntries = Math.max(0, config.get(MAP_CACHE_MAX_ENTRIES));
                final CachePolicyType mapCachePolicy = config.get(MAP_CACHE_POLICY);
                final int mapCacheLruOverflow = Math.max(0, config.get(MAP_CACHE_LRU_OVERFLOW));
                final boolean mapBypassEnabled = config.get(MAP_BYPASS_ENABLED);
                final double mapHitRateThreshold = config.get(MAP_HIT_RATE_THRESHOLD);
                final int mapHitRateWindow = config.get(MAP_HIT_RATE_WINDOW);
                final boolean mapIterationCacheFillEnabled = config.get(MAP_ITERATION_CACHE_FILL_ENABLED);
                final int mapSnapshotMaxEntries = Math.max(0, config.get(MAP_SNAPSHOT_CACHE_MAX_ENTRIES));
		final int mapSnapshotSmallMaxEntries =
				Math.max(1, Math.min(16, config.get(MAP_SNAPSHOT_SMALL_MAX_ENTRIES)));
		final boolean diagnosticsEnabled = config.get(DIAGNOSTICS_ENABLED);
		final NativeRequestPlaneOptions nativeRequestPlaneOptions =
				nativeRequestPlaneOptions(config);
		final boolean keyScopedPrefetchInvalidationEnabled =
				config.get(BP_PREFETCH_KEY_SCOPED_INVALIDATION_ENABLED);
		final String delegateClass = config.get(DELEGATE_BACKEND);

			final boolean listStateCowEnabled = config.get(LIST_STATE_COW_ENABLED);
			final boolean listStateRywEnabled = config.get(LIST_STATE_RYW_ENABLED);
			final int clearedKeysCapacity = Math.max(1, config.get(LIST_STATE_CLEARED_KEYS_CAPACITY));
			final boolean priorityQueueOptEnabled = config.get(PRIORITY_QUEUE_OPT_ENABLED);

			System.out.printf(
				"CacheKit Factory: maxEntries=%d, policy=%s, lruOverflow=%d, bypass=%s, threshold=%.2f, window=%d, mapPresenceMax=%d, mapPresencePolicy=%s, mapPresenceOverflow=%d, mapPresenceImpl=%s, mapCacheMax=%d, mapCachePolicy=%s, mapCacheOverflow=%d, mapBypass=%s, mapHitThreshold=%.2f, mapHitWindow=%d, mapIterFill=%s, mapSnapshotMax=%d, mapSnapshotSmallMax=%d, diagnostics=%s, delegate=%s, listStateCow=%s, listStateRyw=%s, clearedKeysCap=%d, priorityQueueOpt=%s%n",
                                maxEntries,
                                policyType,
                                lruOverflow,
                                bypassEnabled,
                                hitRateThreshold,
                                hitRateWindow,
                                mapPresenceMaxEntries,
                                mapPresencePolicy,
                                mapPresenceLruOverflow,
                                mapPresenceImpl,
                                mapCacheMaxEntries,
                                mapCachePolicy,
                                mapCacheLruOverflow,
                                mapBypassEnabled,
                                mapHitRateThreshold,
							mapHitRateWindow,
							mapIterationCacheFillEnabled,
							mapSnapshotMaxEntries,
							mapSnapshotSmallMaxEntries,
							diagnosticsEnabled,
							delegateClass,
							listStateCowEnabled,
							listStateRywEnabled,
							clearedKeysCapacity,
							priorityQueueOptEnabled);

                StateBackend delegate;
                if (delegateClass == null || delegateClass.isBlank()) {
                        // Default to RocksDB using the factory pattern
                        try {
                                RocksDBStateBackendFactory rocksFactory = new RocksDBStateBackendFactory();
                                delegate = rocksFactory.createFromConfig(config, classLoader);
                        } catch (org.apache.flink.configuration.IllegalConfigurationException e) {
                                throw e;
                        } catch (Exception e) {
                                System.err.println(
                                                "Failed to create RocksDBStateBackend, falling back to HashMapStateBackend: "
                                                                + e.getMessage());
                                delegate = new HashMapStateBackend();
                        }
                } else {
                        delegate = instantiateBackend(delegateClass, classLoader);
                }

				return new CacheKitStateBackend(
								delegate,
								maxEntries,
								policyType,
								lruOverflow,
								bypassEnabled,
								hitRateThreshold,
								hitRateWindow,
								mapPresenceMaxEntries,
								mapPresencePolicy,
								mapPresenceLruOverflow,
								mapPresenceImpl,
								mapCacheMaxEntries,
								mapCachePolicy,
								mapCacheLruOverflow,
								mapBypassEnabled,
								mapHitRateThreshold,
								mapHitRateWindow,
								mapIterationCacheFillEnabled,
								mapSnapshotMaxEntries,
								mapSnapshotSmallMaxEntries,
								listStateCowEnabled,
								listStateRywEnabled,
							clearedKeysCapacity,
							priorityQueueOptEnabled,
								diagnosticsEnabled,
								nativeRequestPlaneOptions,
								keyScopedPrefetchInvalidationEnabled,
					config.get(NATIVE_PREFETCH_ACCESS_GUIDED_STATE_ENABLED),
					config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_ENABLED),
					config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_DIRECT_ARENA_ENABLED),
					config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_ENABLED)
							&& config.get(DISTINCT_BATCH_OVERLAY_ENABLED),
                config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_ENABLED)
                        && config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_DIRECT_ARENA_ENABLED)
                        && config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_ENABLED)
                        && config.get(DISTINCT_BATCH_OVERLAY_ENABLED)
                        && config.get(
                                NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_WORK_FIRST_ENABLED),
					config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_ENABLED)
							&& config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_DIRECT_ARENA_ENABLED)
							&& config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_ENABLED)
							&& config.get(DISTINCT_BATCH_OVERLAY_ENABLED)
							&& config.get(
									NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_DEFERRED_WAVE_ENABLED),
					config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_ENABLED)
							&& config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_DIRECT_ARENA_ENABLED)
							&& config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_ENABLED)
							&& config.get(DISTINCT_BATCH_OVERLAY_ENABLED)
							&& config.get(
									NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_DEFERRED_WAVE_ENABLED)
								&& config.get(
										NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_DEFERRED_WAVE_DUAL_WORKER_ENABLED),
					config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_ENABLED)
							&& config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_DIRECT_ARENA_ENABLED)
							&& config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_ENABLED)
							&& config.get(DISTINCT_BATCH_OVERLAY_ENABLED)
							&& config.get(
									NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_DEFERRED_WAVE_ENABLED)
							&& config.get(
										NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_DEFERRED_WAVE_DIRECT_ARENA_RESULTS_ENABLED),
						config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_ENABLED)
								&& config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_DIRECT_ARENA_ENABLED)
								&& config.get(NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_ENABLED)
								&& config.get(DISTINCT_BATCH_OVERLAY_ENABLED)
								&& config.get(
										NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_DEFERRED_WAVE_ENABLED)
								&& config.get(
										NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_COLUMN_WAVE_ENABLED),
						config.get(NATIVE_MAP_DISTINCT_PREPARED_COMMIT_ENABLED),
						Math.max(
							2,
							config.get(
									NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_ASYNC_MIN_UNIQUE_KEYS)),
					Math.max(
							1,
							Math.min(
									BatchKeyGroupingSupport
											.MAX_CROSS_KEY_PIPELINE_LOOKAHEAD_GROUPS,
									config.get(
											NATIVE_MAP_DISTINCT_BATCH_PREFETCH_CROSS_KEY_PIPELINE_LOOKAHEAD_GROUPS))));
		}

        static NativeRequestPlaneOptions nativeRequestPlaneOptions(ReadableConfig config) {
                if (config.get(NATIVE_LOCAL_PREAGG_INDEXED_FOLD_ENABLED)
                                && !config.get(NATIVE_LOCAL_PREAGG_ENABLED)) {
                        throw new IllegalArgumentException(
                                        "Native LocalPreAgg indexed fold requires native LocalPreAgg grouping to be enabled.");
                }
                return new NativeRequestPlaneOptions(
                                config.get(NATIVE_REQUEST_PLANE_ENABLED),
                                config.get(NATIVE_REQUEST_PLANE_LIBRARY),
                                config.get(NATIVE_REQUEST_PLANE_KERNEL),
                                config.get(NATIVE_REQUEST_PLANE_CAPACITY_ENTRIES),
                                config.get(NATIVE_REQUEST_PLANE_KEY_ARENA_BYTES),
                                config.get(NATIVE_REQUEST_PLANE_VALUE_ARENA_BYTES),
                                config.get(NATIVE_REQUEST_PLANE_BATCH_ENTRIES),
                                config.get(NATIVE_REQUEST_PLANE_BATCH_KEY_ARENA_BYTES),
                                config.get(NATIVE_REQUEST_PLANE_BATCH_VALUE_ARENA_BYTES),
                                config.get(NATIVE_REQUEST_PLANE_MIN_BATCH_SIZE),
                                config.get(NATIVE_REQUEST_PLANE_BATCH_SLOTS),
                                config.get(NATIVE_REQUEST_PLANE_AARCH64_ONLY),
                                config.get(NATIVE_REQUEST_PLANE_WRITE_THROUGH_MUTATIONS),
                                config.get(NATIVE_VALUE_CACHE_ENABLED),
                                config.get(NATIVE_MAP_CACHE_ENABLED),
                                config.get(NATIVE_MAP_SNAPSHOT_ENABLED),
                                config.get(NATIVE_PREFETCH_ENABLED),
                                config.get(NATIVE_MAILBOX_BATCH_ENABLED),
                                config.get(NATIVE_LOCAL_PREAGG_ENABLED),
                                config.get(NATIVE_COMPACT_SELECTED_PROBE_ENABLED),
                                config.get(NATIVE_DIRECT_ARENA_MULTIGET_ENABLED),
                                config.get(NATIVE_MAP_SNAPSHOT_ADAPTIVE_BYPASS_ENABLED),
                                config.get(NATIVE_MAP_SNAPSHOT_ADAPTIVE_WINDOW_PROBES),
                                config.get(NATIVE_MAP_SNAPSHOT_ADAPTIVE_MIN_USEFUL_HIT_RATE),
                                config.get(NATIVE_MAP_SNAPSHOT_ADAPTIVE_RESAMPLE_INTERVAL_PROBES),
                                config.get(NATIVE_LOCAL_PREAGG_INDEXED_FOLD_ENABLED),
                                config.get(NATIVE_VALUE_CACHE_READ_ACTIVATED_WRITE_THROUGH),
                                config.get(NATIVE_VALUE_CACHE_RESIDENT_MUTATION_BATCH),
                                config.get(NATIVE_DIRECT_ARENA_READ_ONLY_ENABLED),
                                config.get(NATIVE_PREFETCH_NEGATIVE_HANDOFF_ENABLED),
                        config.get(NATIVE_PREFETCH_DEFERRED_RESERVATION_MATERIALIZATION_ENABLED),
                                config.get(NATIVE_MAILBOX_COMPACTION_SCRATCH_SLOT_ENABLED),
                                config.get(NATIVE_MAILBOX_COMPACTION_SCRATCH_ENTRIES),
                                config.get(NATIVE_MAILBOX_COMPACTION_SCRATCH_KEY_ARENA_BYTES),
                                config.get(NATIVE_DIRECT_ARENA_EAGER_MATERIALIZATION_ENABLED))
                                .withDirectArenaBatchSize(config.get(NATIVE_DIRECT_ARENA_BATCH_SIZE));
        }

        private static StateBackend instantiateBackend(String className, ClassLoader classLoader) {
                try {
                        Class<?> clazz = Class.forName(className, true, classLoader);
                        Object instance = clazz.getDeclaredConstructor().newInstance();
                        if (!(instance instanceof StateBackend)) {
                                throw new IllegalArgumentException(
                                                "Configured delegate backend class does not implement StateBackend: "
                                                                + className);
                        }
                        return (StateBackend) instance;
                } catch (Exception e) {
                        throw new IllegalArgumentException(
                                        "Failed to instantiate delegate backend: " + className, e);
                }
        }
}
