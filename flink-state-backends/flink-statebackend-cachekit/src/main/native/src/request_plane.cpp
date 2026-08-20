/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "cachekit/native_request_plane.h"

#include "kernels_internal.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iterator>
#include <limits>
#include <map>
#include <memory_resource>
#include <new>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

namespace cachekit {
namespace native {
namespace {

using internal::Bucket;
using internal::KernelOps;
using internal::kEmptyTag;
using internal::kSlotsPerBucket;
using internal::kTombstoneTag;

class ByteArena {
public:
    ByteArena(std::size_t capacity, std::size_t max_free_blocks)
            : bytes_(capacity),
              free_by_offset_(&node_pool_),
              free_by_size_(&node_pool_) {
        // Prime both PMR map node-size pools up to the proven maximum number
        // of disjoint free intervals. Subsequent fill/evict churn reuses these
        // nodes instead of returning to the process allocator.
        for (std::size_t index = 0; index < max_free_blocks; ++index) {
            free_by_offset_.emplace(index, 0);
            free_by_size_.emplace(
                    std::make_pair(std::size_t{0}, index), std::uint8_t{0});
        }
        free_by_offset_.clear();
        free_by_size_.clear();
        Reset();
    }

    bool Allocate(std::size_t size, std::size_t* offset) {
        if (offset == nullptr || size > bytes_.size()) {
            return false;
        }
        if (size == 0) {
            *offset = 0;
            return true;
        }
        const auto sized =
                free_by_size_.lower_bound(std::make_pair(size, std::size_t{0}));
        if (sized == free_by_size_.end()) {
            return false;
        }
        const std::size_t block_size = sized->first.first;
        const std::size_t block_offset = sized->first.second;
        RemoveFreeBlock(block_offset, block_size);
        *offset = block_offset;
        if (block_size > size) {
            AddFreeBlock(block_offset + size, block_size - size);
        }
        return true;
    }

    void Release(std::size_t offset, std::size_t size) {
        if (size == 0) {
            return;
        }
        if (offset > bytes_.size() || size > bytes_.size() - offset) {
            return;
        }
        std::size_t merged_offset = offset;
        std::size_t merged_size = size;
        auto next = free_by_offset_.lower_bound(offset);
        if (next != free_by_offset_.begin()) {
            const auto previous = std::prev(next);
            if (previous->first + previous->second == offset) {
                merged_offset = previous->first;
                merged_size += previous->second;
                RemoveFreeBlock(previous->first, previous->second);
            }
        }
        if (next != free_by_offset_.end() &&
            merged_offset + merged_size == next->first) {
            merged_size += next->second;
            RemoveFreeBlock(next->first, next->second);
        }
        AddFreeBlock(merged_offset, merged_size);
    }

    void Write(std::size_t offset, const std::uint8_t* data, std::size_t size) {
        if (size != 0) {
            std::memcpy(bytes_.data() + offset, data, size);
        }
    }

    const std::uint8_t* Pointer(std::size_t offset, std::size_t size) const {
        return size == 0 ? nullptr : bytes_.data() + offset;
    }

    std::size_t capacity() const noexcept {
        return bytes_.size();
    }

    void Reset() {
        free_by_offset_.clear();
        free_by_size_.clear();
        if (!bytes_.empty()) {
            AddFreeBlock(0, bytes_.size());
        }
    }

private:
    void AddFreeBlock(std::size_t offset, std::size_t size) {
        free_by_offset_.emplace(offset, size);
        free_by_size_.emplace(
                std::make_pair(size, offset), std::uint8_t{0});
    }

    void RemoveFreeBlock(std::size_t offset, std::size_t size) {
        free_by_offset_.erase(offset);
        free_by_size_.erase(std::make_pair(size, offset));
    }

    std::vector<std::uint8_t> bytes_;
    std::pmr::unsynchronized_pool_resource node_pool_;
    std::pmr::map<std::size_t, std::size_t> free_by_offset_;
    std::pmr::map<std::pair<std::size_t, std::size_t>, std::uint8_t> free_by_size_;
};

struct Entry {
    bool occupied = false;
    bool negative = false;
    std::uint32_t state_id = 0;
    std::uint32_t lru_previous = 0;
    std::uint32_t lru_next = 0;
    std::uint64_t generation = 0;
    std::size_t key_offset = 0;
    std::size_t key_size = 0;
    std::size_t value_offset = 0;
    std::size_t value_size = 0;
    std::size_t bucket_index = 0;
    std::size_t slot_index = 0;
};

struct SlotLocation {
    std::size_t bucket_index = 0;
    std::size_t slot_index = 0;
};

class BucketArray {
public:
    explicit BucketArray(std::size_t count) : size_(count) {
        if (count > std::numeric_limits<std::size_t>::max() / sizeof(Bucket)) {
            throw std::overflow_error("bucket allocation size overflow");
        }
        const std::size_t bytes = count * sizeof(Bucket);
        data_ = static_cast<Bucket*>(std::aligned_alloc(alignof(Bucket), bytes));
        if (data_ == nullptr) {
            throw std::bad_alloc();
        }
        std::memset(data_, 0, bytes);
    }

    ~BucketArray() {
        std::free(data_);
    }

    BucketArray(const BucketArray&) = delete;
    BucketArray& operator=(const BucketArray&) = delete;

    Bucket& operator[](std::size_t index) noexcept {
        return data_[index];
    }

    const Bucket& operator[](std::size_t index) const noexcept {
        return data_[index];
    }

    Bucket* begin() noexcept {
        return data_;
    }

    Bucket* end() noexcept {
        return data_ + size_;
    }

    std::size_t size() const noexcept {
        return size_;
    }

    void Reset() noexcept {
        std::memset(data_, 0, size_ * sizeof(Bucket));
    }

private:
    Bucket* data_ = nullptr;
    std::size_t size_ = 0;
};

bool IsValidBytes(const std::uint8_t* data, std::size_t size) noexcept {
    return size == 0 || data != nullptr;
}

std::size_t RequiredBucketCount(std::size_t capacity_entries) {
    // Keep the metadata table at or below 50% occupancy on either the x86
    // 64-byte/eight-slot or Kunpeng 128-byte/sixteen-slot layout.
    const std::size_t entries_per_bucket = kSlotsPerBucket / 2U;
    std::size_t required = capacity_entries / entries_per_bucket;
    if ((capacity_entries % entries_per_bucket) != 0) {
        ++required;
    }
    required = std::max<std::size_t>(required, 1U);

    std::size_t result = 1;
    while (result < required) {
        if (result > std::numeric_limits<std::size_t>::max() / 2U) {
            throw std::overflow_error("bucket count overflow");
        }
        result *= 2U;
    }
    return result;
}

std::size_t RequiredGroupTableCount(std::size_t max_batch_entries) {
    if (max_batch_entries > std::numeric_limits<std::size_t>::max() / 2U) {
        throw std::overflow_error("group table size overflow");
    }
    const std::size_t required =
            std::max<std::size_t>(2U, max_batch_entries * 2U);
    std::size_t result = 1U;
    while (result < required) {
        if (result > std::numeric_limits<std::size_t>::max() / 2U) {
            throw std::overflow_error("group table size overflow");
        }
        result *= 2U;
    }
    return result;
}

std::size_t GroupHash(
        std::uint32_t fingerprint,
        std::uint32_t state_id,
        std::uint64_t generation,
        std::size_t key_size) noexcept {
    std::uint64_t value =
            (static_cast<std::uint64_t>(fingerprint) << 32U) | state_id;
    value ^= generation + 0x9e3779b97f4a7c15ULL + (value << 6U) + (value >> 2U);
    value ^= static_cast<std::uint64_t>(key_size) + 0x9e3779b97f4a7c15ULL +
            (value << 6U) + (value >> 2U);
    value ^= value >> 30U;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27U;
    value *= 0x94d049bb133111ebULL;
    value ^= value >> 31U;
    return static_cast<std::size_t>(value);
}

std::uint32_t NormalizeTag(
        std::uint32_t fingerprint, std::uint32_t fingerprint_mask) noexcept {
    std::uint32_t tag = fingerprint & fingerprint_mask;
    if (tag == kEmptyTag || tag == kTombstoneTag) {
        tag += 2U;
    }
    return tag;
}

void SetError(
        ErrorCode code,
        const char* text,
        ErrorCode* error,
        std::string* message) {
    if (error != nullptr) {
        *error = code;
    }
    if (message != nullptr) {
        *message = text;
    }
}

}  // namespace

struct RequestPlane::Impl {
    Impl(
            const Options& requested_options,
            const KernelOps& selected_kernel,
            HostFeatures detected_features)
            : options(requested_options),
              kernel(selected_kernel),
              features(detected_features),
              buckets(RequiredBucketCount(options.capacity_entries)),
              entries(options.capacity_entries + 1U),
              key_arena(
                      options.key_arena_bytes,
                      options.capacity_entries + 1U),
              value_arena(
                      options.value_arena_bytes,
                      options.capacity_entries + 1U),
              group_table(
                      RequiredGroupTableCount(options.max_batch_entries), 0U),
              group_epochs(group_table.size(), 0U),
              group_fingerprints(options.max_batch_entries, 0U) {
        state_generation_watermarks.reserve(
                std::min<std::size_t>(options.capacity_entries, 64U));
        free_entry_ids.reserve(options.capacity_entries);
        for (std::size_t id = options.capacity_entries; id > 0; --id) {
            free_entry_ids.push_back(static_cast<std::uint32_t>(id));
        }
    }

    void UnlinkLru(std::uint32_t id) noexcept {
        Entry& entry = entries[id];
        if (entry.lru_previous == 0) {
            lru_head = entry.lru_next;
        } else {
            entries[entry.lru_previous].lru_next = entry.lru_next;
        }
        if (entry.lru_next == 0) {
            lru_tail = entry.lru_previous;
        } else {
            entries[entry.lru_next].lru_previous = entry.lru_previous;
        }
        entry.lru_previous = 0;
        entry.lru_next = 0;
    }

    void LinkLruTail(std::uint32_t id) noexcept {
        Entry& entry = entries[id];
        entry.lru_previous = lru_tail;
        entry.lru_next = 0;
        if (lru_tail == 0) {
            lru_head = id;
        } else {
            entries[lru_tail].lru_next = id;
        }
        lru_tail = id;
    }

    void TouchLru(std::uint32_t id) noexcept {
        if (id == lru_tail) {
            return;
        }
        UnlinkLru(id);
        LinkLruTail(id);
    }

    std::uint32_t TagFor(const KeyView& key) const noexcept {
        return NormalizeTag(
                kernel.fingerprint(key.state_id, key.data, key.size),
                options.fingerprint_mask);
    }

    bool FindExact(
            const KeyView& key,
            std::uint32_t tag,
            SlotLocation* location,
            std::uint32_t* entry_id) const noexcept {
        const std::size_t first =
                static_cast<std::size_t>(tag) & (buckets.size() - 1U);
        for (std::size_t step = 0; step < buckets.size(); ++step) {
            const std::size_t bucket_index = (first + step) & (buckets.size() - 1U);
            const Bucket& bucket = buckets[bucket_index];
            const std::uint16_t matches = kernel.match_tags(bucket, tag);
            for (std::size_t slot = 0; slot < kSlotsPerBucket; ++slot) {
                if ((matches & static_cast<std::uint16_t>(1U << slot)) == 0) {
                    continue;
                }
                const std::uint32_t candidate_id = bucket.entry_ids[slot];
                if (candidate_id == 0 || candidate_id >= entries.size()) {
                    continue;
                }
                const Entry& candidate = entries[candidate_id];
                if (!candidate.occupied || candidate.state_id != key.state_id ||
                    candidate.key_size != key.size) {
                    continue;
                }
                const std::uint8_t* stored =
                        key_arena.Pointer(candidate.key_offset, candidate.key_size);
                if (!kernel.equal_bytes(stored, key.data, key.size)) {
                    continue;
                }
                if (location != nullptr) {
                    *location = SlotLocation{bucket_index, slot};
                }
                if (entry_id != nullptr) {
                    *entry_id = candidate_id;
                }
                return true;
            }
            if (kernel.match_tags(bucket, kEmptyTag) != 0) {
                return false;
            }
        }
        return false;
    }

    bool FindFreeSlot(std::uint32_t tag, SlotLocation* location) const noexcept {
        const std::size_t first =
                static_cast<std::size_t>(tag) & (buckets.size() - 1U);
        bool saw_tombstone = false;
        SlotLocation tombstone;
        for (std::size_t step = 0; step < buckets.size(); ++step) {
            const std::size_t bucket_index = (first + step) & (buckets.size() - 1U);
            const Bucket& bucket = buckets[bucket_index];
            for (std::size_t slot = 0; slot < kSlotsPerBucket; ++slot) {
                if (bucket.tags[slot] == kTombstoneTag && !saw_tombstone) {
                    tombstone = SlotLocation{bucket_index, slot};
                    saw_tombstone = true;
                } else if (bucket.tags[slot] == kEmptyTag) {
                    *location =
                            saw_tombstone ? tombstone : SlotLocation{bucket_index, slot};
                    return true;
                }
            }
        }
        if (saw_tombstone) {
            *location = tombstone;
            return true;
        }
        return false;
    }

    void RebuildBuckets() noexcept {
        buckets.Reset();
        for (std::size_t id = 1; id < entries.size(); ++id) {
            Entry& entry = entries[id];
            if (!entry.occupied) {
                continue;
            }
            const KeyView key{
                    entry.state_id,
                    entry.generation,
                    key_arena.Pointer(entry.key_offset, entry.key_size),
                    entry.key_size};
            const std::uint32_t tag = TagFor(key);
            SlotLocation location;
            if (!FindFreeSlot(tag, &location)) {
                // The table is provisioned for <=50% occupancy, so rebuilding
                // only live entries cannot fail.
                std::terminate();
            }
            Bucket& bucket = buckets[location.bucket_index];
            bucket.tags[location.slot_index] = tag;
            bucket.entry_ids[location.slot_index] =
                    static_cast<std::uint32_t>(id);
            entry.bucket_index = location.bucket_index;
            entry.slot_index = location.slot_index;
        }
        tombstone_count = 0;
    }

    void RemoveEntry(std::uint32_t id, bool eviction) noexcept {
        if (id == 0 || id >= entries.size() || !entries[id].occupied) {
            return;
        }
        Entry& entry = entries[id];
        UnlinkLru(id);
        Bucket& bucket = buckets[entry.bucket_index];
        bucket.tags[entry.slot_index] = kTombstoneTag;
        bucket.entry_ids[entry.slot_index] = 0;
        ++tombstone_count;
        key_arena.Release(entry.key_offset, entry.key_size);
        if (!entry.negative) {
            value_arena.Release(entry.value_offset, entry.value_size);
        }
        entry = Entry{};
        free_entry_ids.push_back(id);
        --entry_count;
        if (eviction) {
            ++eviction_count;
        }
        // Linear-probe tombstones otherwise accumulate until every miss scans
        // the complete metadata table. Periodic allocation-free rehashing
        // keeps lookup cost bounded and is O(1) amortized over churn.
        const std::size_t rebuild_threshold =
                std::max<std::size_t>(1U, options.capacity_entries / 4U);
        if (tombstone_count >= rebuild_threshold) {
            RebuildBuckets();
        }
    }

    bool EvictOldest(std::uint32_t excluded_id = 0) noexcept {
        // The intrusive LRU makes capacity-full miss/fill churn O(1) instead
        // of scanning every configured entry for each eviction.
        std::uint32_t victim = lru_head;
        if (victim == excluded_id && victim != 0) {
            victim = entries[victim].lru_next;
        }
        if (victim == 0) {
            return false;
        }
        RemoveEntry(victim, true);
        return true;
    }

    FillResult UpdateExisting(std::uint32_t id, const FillView& fill) noexcept {
        Entry& entry = entries[id];
        if (fill.key.generation < entry.generation) {
            return FillResult{
                    FillStatus::kRejectedStaleGeneration, ErrorCode::kOk};
        }

        if (fill.negative) {
            if (!entry.negative) {
                value_arena.Release(entry.value_offset, entry.value_size);
            }
            entry.negative = true;
            entry.value_offset = 0;
            entry.value_size = 0;
            entry.generation = fill.key.generation;
            TouchLru(id);
            return FillResult{FillStatus::kUpdated, ErrorCode::kOk};
        }

        if (!entry.negative && fill.value_size <= entry.value_size) {
            value_arena.Write(entry.value_offset, fill.value, fill.value_size);
            if (fill.value_size < entry.value_size) {
                value_arena.Release(
                        entry.value_offset + fill.value_size,
                        entry.value_size - fill.value_size);
            }
            entry.value_size = fill.value_size;
            entry.generation = fill.key.generation;
            TouchLru(id);
            return FillResult{FillStatus::kUpdated, ErrorCode::kOk};
        }

        std::size_t new_offset = 0;
        bool released_old_value = false;
        bool allocated = value_arena.Allocate(fill.value_size, &new_offset);
        while (!allocated) {
            if (!EvictOldest(id)) {
                break;
            }
            allocated = value_arena.Allocate(fill.value_size, &new_offset);
        }
        if (!allocated) {
            if (!entry.negative) {
                value_arena.Release(entry.value_offset, entry.value_size);
                released_old_value = true;
            }
            allocated = value_arena.Allocate(fill.value_size, &new_offset);
            if (!allocated) {
                if (released_old_value) {
                    entry.negative = true;
                    entry.value_offset = 0;
                    entry.value_size = 0;
                }
                RemoveEntry(id, false);
                return FillResult{
                        FillStatus::kRejectedCapacity,
                        ErrorCode::kCapacityExceeded};
            }
        }

        value_arena.Write(new_offset, fill.value, fill.value_size);
        if (!entry.negative && !released_old_value) {
            value_arena.Release(entry.value_offset, entry.value_size);
        }
        entry.negative = false;
        entry.value_offset = new_offset;
        entry.value_size = fill.value_size;
        entry.generation = fill.key.generation;
        TouchLru(id);
        return FillResult{FillStatus::kUpdated, ErrorCode::kOk};
    }

    FillResult InsertNew(const FillView& fill, std::uint32_t tag) noexcept {
        std::size_t attempts = 0;
        while (attempts <= options.capacity_entries) {
            ++attempts;
            if (free_entry_ids.empty()) {
                if (!EvictOldest()) {
                    return FillResult{
                            FillStatus::kRejectedCapacity,
                            ErrorCode::kCapacityExceeded};
                }
                continue;
            }

            std::size_t key_offset = 0;
            std::size_t value_offset = 0;
            const bool key_allocated = key_arena.Allocate(fill.key.size, &key_offset);
            const bool value_allocated =
                    fill.negative ||
                    value_arena.Allocate(fill.value_size, &value_offset);
            if (!key_allocated || !value_allocated) {
                if (key_allocated) {
                    key_arena.Release(key_offset, fill.key.size);
                }
                if (value_allocated && !fill.negative) {
                    value_arena.Release(value_offset, fill.value_size);
                }
                if (!EvictOldest()) {
                    return FillResult{
                            FillStatus::kRejectedCapacity,
                            ErrorCode::kCapacityExceeded};
                }
                continue;
            }

            SlotLocation location;
            if (!FindFreeSlot(tag, &location)) {
                key_arena.Release(key_offset, fill.key.size);
                if (!fill.negative) {
                    value_arena.Release(value_offset, fill.value_size);
                }
                return FillResult{
                        FillStatus::kInternalError, ErrorCode::kInternal};
            }

            const std::uint32_t id = free_entry_ids.back();
            free_entry_ids.pop_back();
            key_arena.Write(key_offset, fill.key.data, fill.key.size);
            if (!fill.negative) {
                value_arena.Write(value_offset, fill.value, fill.value_size);
            }

            Entry& entry = entries[id];
            entry.occupied = true;
            entry.negative = fill.negative;
            entry.state_id = fill.key.state_id;
            entry.generation = fill.key.generation;
            entry.key_offset = key_offset;
            entry.key_size = fill.key.size;
            entry.value_offset = value_offset;
            entry.value_size = fill.negative ? 0 : fill.value_size;
            entry.bucket_index = location.bucket_index;
            entry.slot_index = location.slot_index;

            Bucket& bucket = buckets[location.bucket_index];
            if (bucket.tags[location.slot_index] == kTombstoneTag) {
                --tombstone_count;
            }
            bucket.tags[location.slot_index] = tag;
            bucket.entry_ids[location.slot_index] = id;
            LinkLruTail(id);
            ++entry_count;
            return FillResult{FillStatus::kInserted, ErrorCode::kOk};
        }
        return FillResult{
                FillStatus::kRejectedCapacity, ErrorCode::kCapacityExceeded};
    }

    ProbeResult ProbeOne(const KeyView& key) noexcept {
        ProbeResult result;
        if (!IsValidBytes(key.data, key.size)) {
            result.error = ErrorCode::kInvalidArgument;
            return result;
        }
        if (key.size > key_arena.capacity()) {
            return result;
        }

        const std::uint32_t tag = TagFor(key);
        std::uint32_t id = 0;
        if (!FindExact(key, tag, nullptr, &id)) {
            return result;
        }
        Entry& entry = entries[id];
        // Exact-generation probes preserve mismatch semantics. CacheKit's prepared request path
        // uses the explicit latest sentinel because exact-key write-through makes the stored
        // version authoritative; Java's write-generation gate blocks in-flight stale publication.
        if (key.generation != kLatestGeneration &&
            entry.generation != key.generation) {
            return result;
        }
        TouchLru(id);
        if (entry.negative) {
            result.status = ProbeStatus::kNegative;
            return result;
        }
        result.status = ProbeStatus::kHit;
        result.value = value_arena.Pointer(entry.value_offset, entry.value_size);
        result.value_size = entry.value_size;
        return result;
    }

    FillResult FillOne(const FillView& fill) noexcept {
        if (!IsValidBytes(fill.key.data, fill.key.size) ||
            (!fill.negative && !IsValidBytes(fill.value, fill.value_size)) ||
            fill.key.generation == kLatestGeneration) {
            return FillResult{
                    FillStatus::kInvalidArgument, ErrorCode::kInvalidArgument};
        }

        // A ValueState write generation is state-wide. Keep its high-watermark
        // independently of cache entries so eviction cannot let a delayed
        // asynchronous fill resurrect an older value. Equal generations are
        // deliberately accepted because one prepared batch contains many keys
        // captured at the same generation. Advance before every capacity or
        // arena check: an authoritative update/clear must fence older fills
        // even when its own cache insertion cannot be admitted.
        try {
            const auto found =
                    state_generation_watermarks.find(fill.key.state_id);
            if (found != state_generation_watermarks.end()) {
                if (fill.key.generation < found->second) {
                    return FillResult{
                            FillStatus::kRejectedStaleGeneration,
                            ErrorCode::kOk};
                }
                if (fill.key.generation > found->second) {
                    found->second = fill.key.generation;
                }
            } else {
                state_generation_watermarks.emplace(
                        fill.key.state_id, fill.key.generation);
            }
        } catch (const std::bad_alloc&) {
            return FillResult{
                    FillStatus::kInternalError,
                    ErrorCode::kAllocationFailed};
        } catch (...) {
            return FillResult{
                    FillStatus::kInternalError, ErrorCode::kInternal};
        }

        if (fill.key.size > key_arena.capacity()) {
            return FillResult{
                    FillStatus::kRejectedCapacity,
                    ErrorCode::kCapacityExceeded};
        }

        const std::uint32_t tag = TagFor(fill.key);
        std::uint32_t existing_id = 0;
        const bool existing =
                FindExact(fill.key, tag, nullptr, &existing_id);
        if (!fill.negative && fill.value_size > value_arena.capacity()) {
            // The authoritative value no longer matches this exact resident
            // key. Preserve other keys in the state, but never leave the old
            // exact value observable through a latest-generation probe.
            if (existing) {
                RemoveEntry(existing_id, false);
            }
            return FillResult{
                    FillStatus::kRejectedCapacity,
                    ErrorCode::kCapacityExceeded};
        }
        if (existing) {
            return UpdateExisting(existing_id, fill);
        }
        return InsertNew(fill, tag);
    }

    void Clear() noexcept {
        buckets.Reset();
        std::fill(entries.begin(), entries.end(), Entry{});
        free_entry_ids.clear();
        for (std::size_t id = options.capacity_entries; id > 0; --id) {
            free_entry_ids.push_back(static_cast<std::uint32_t>(id));
        }
        key_arena.Reset();
        value_arena.Reset();
        // Clear is an explicit lifecycle reset, unlike a ValueState key clear
        // (which arrives above as a negative fill and advances the watermark).
        state_generation_watermarks.clear();
        entry_count = 0;
        tombstone_count = 0;
        lru_head = 0;
        lru_tail = 0;
        eviction_count = 0;
    }

    Options options;
    const KernelOps& kernel;
    HostFeatures features;
    BucketArray buckets;
    std::vector<Entry> entries;
    std::vector<std::uint32_t> free_entry_ids;
    ByteArena key_arena;
    ByteArena value_arena;
    std::unordered_map<std::uint32_t, std::uint64_t>
            state_generation_watermarks;
    // GroupBatch is serialized by its single-thread-owned RequestPlane. Reuse
    // this preallocated scratch so mailbox batches do not allocate in the hot path.
    mutable std::vector<std::uint32_t> group_table;
    // An 8-bit epoch clears one byte per slot only once every 255 batches. At
    // the configured 512-entry batch limit this is a few MiB over a 100M job,
    // instead of clearing a capacity-sized table on every approximately 64-key batch.
    mutable std::vector<std::uint8_t> group_epochs;
    mutable std::vector<std::uint32_t> group_fingerprints;
    mutable std::uint8_t group_epoch = 0;
    mutable GroupBatchDiagnostics group_diagnostics;
    std::size_t entry_count = 0;
    std::size_t tombstone_count = 0;
    std::uint32_t lru_head = 0;
    std::uint32_t lru_tail = 0;
    std::uint64_t eviction_count = 0;
};

std::unique_ptr<RequestPlane> RequestPlane::Create(
        const Options& options, ErrorCode* error, std::string* message) {
    SetError(ErrorCode::kOk, "", error, message);
    if (options.capacity_entries == 0 || options.min_native_batch_size == 0) {
        SetError(
                ErrorCode::kInvalidArgument,
                "capacity_entries and min_native_batch_size must be non-zero",
                error,
                message);
        return nullptr;
    }
    if (options.capacity_entries >=
        static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
        SetError(
                ErrorCode::kOverflow,
                "capacity_entries exceeds the 32-bit entry-id space",
                error,
                message);
        return nullptr;
    }
    Options normalized_options = options;
    if (normalized_options.max_batch_entries == 0) {
        normalized_options.max_batch_entries = normalized_options.capacity_entries;
    }
    if (normalized_options.max_batch_entries >=
        static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
        SetError(
                ErrorCode::kOverflow,
                "max_batch_entries exceeds the 32-bit group-id space",
                error,
                message);
        return nullptr;
    }

    const HostFeatures features = DetectHostFeatures();
    const char* kernel_message = "";
    ErrorCode kernel_error = ErrorCode::kOk;
    const KernelOps* kernel = internal::SelectKernel(
            normalized_options.kernel,
            features,
            &kernel_error,
            &kernel_message);
    if (kernel == nullptr) {
        SetError(kernel_error, kernel_message, error, message);
        return nullptr;
    }

    try {
        std::unique_ptr<Impl> impl(
                new Impl(normalized_options, *kernel, features));
        return std::unique_ptr<RequestPlane>(
                new RequestPlane(std::move(impl)));
    } catch (const std::overflow_error& exception) {
        SetError(ErrorCode::kOverflow, exception.what(), error, message);
    } catch (const std::bad_alloc&) {
        SetError(
                ErrorCode::kAllocationFailed,
                "native request-plane allocation failed",
                error,
                message);
    } catch (const std::length_error& exception) {
        SetError(ErrorCode::kOverflow, exception.what(), error, message);
    } catch (...) {
        SetError(
                ErrorCode::kInternal,
                "native request-plane initialization failed",
                error,
                message);
    }
    return nullptr;
}

RequestPlane::RequestPlane(std::unique_ptr<Impl> impl) noexcept
        : impl_(std::move(impl)) {}

RequestPlane::~RequestPlane() = default;
RequestPlane::RequestPlane(RequestPlane&&) noexcept = default;
RequestPlane& RequestPlane::operator=(RequestPlane&&) noexcept = default;

ErrorCode RequestPlane::ProbeBatch(
        const KeyView* keys, ProbeResult* results, std::size_t count) noexcept {
    if (count != 0 && (keys == nullptr || results == nullptr)) {
        return ErrorCode::kInvalidArgument;
    }
    for (std::size_t index = 0; index < count; ++index) {
        results[index] = impl_->ProbeOne(keys[index]);
    }
    return ErrorCode::kOk;
}

ErrorCode RequestPlane::FillBatch(
        const FillView* fills, FillResult* results, std::size_t count) noexcept {
    if (count != 0 && (fills == nullptr || results == nullptr)) {
        return ErrorCode::kInvalidArgument;
    }
    for (std::size_t index = 0; index < count; ++index) {
        results[index] = impl_->FillOne(fills[index]);
    }
    return ErrorCode::kOk;
}

ErrorCode RequestPlane::CompactBatch(
        const KeyView* keys,
        std::uint32_t* unique_source_indexes,
        std::size_t count,
        std::size_t* unique_count) const noexcept {
    return GroupBatch(keys, unique_source_indexes, nullptr, count, unique_count);
}

ErrorCode RequestPlane::GroupBatch(
        const KeyView* keys,
        std::uint32_t* unique_source_indexes,
        std::uint32_t* source_group_indexes,
        std::size_t count,
        std::size_t* unique_count) const noexcept {
    if (unique_count == nullptr) {
        return ErrorCode::kInvalidArgument;
    }
    *unique_count = 0;
    if ((count != 0 && (keys == nullptr || unique_source_indexes == nullptr)) ||
        count > std::numeric_limits<std::uint32_t>::max()) {
        return ErrorCode::kInvalidArgument;
    }
    if (count > impl_->options.max_batch_entries) {
        return ErrorCode::kCapacityExceeded;
    }

    std::uint8_t next_epoch = static_cast<std::uint8_t>(impl_->group_epoch + 1U);
    if (next_epoch == 0U) {
        std::fill(impl_->group_epochs.begin(), impl_->group_epochs.end(), 0U);
        next_epoch = 1U;
        ++impl_->group_diagnostics.epoch_resets;
    }
    impl_->group_epoch = next_epoch;
    ++impl_->group_diagnostics.batches;
    const std::size_t table_mask = impl_->group_table.size() - 1U;
    std::size_t written = 0;
    for (std::size_t index = 0; index < count; ++index) {
        const KeyView& candidate = keys[index];
        if (!IsValidBytes(candidate.data, candidate.size)) {
            return ErrorCode::kInvalidArgument;
        }
        const std::uint32_t candidate_fingerprint =
                impl_->kernel.fingerprint(
                        candidate.state_id, candidate.data, candidate.size) &
                impl_->options.fingerprint_mask;
        ++impl_->group_diagnostics.fingerprint_calls;
        std::size_t slot = GroupHash(
                                   candidate_fingerprint,
                                   candidate.state_id,
                                   candidate.generation,
                                   candidate.size) &
                table_mask;
        bool resolved = false;
        for (std::size_t probe = 0; probe < impl_->group_table.size(); ++probe) {
            ++impl_->group_diagnostics.probe_steps;
            if (impl_->group_epochs[slot] != impl_->group_epoch) {
                const std::uint32_t group = static_cast<std::uint32_t>(written);
                unique_source_indexes[written] = static_cast<std::uint32_t>(index);
                impl_->group_fingerprints[written] = candidate_fingerprint;
                impl_->group_table[slot] = group + 1U;
                impl_->group_epochs[slot] = impl_->group_epoch;
                ++written;
                if (source_group_indexes != nullptr) {
                    source_group_indexes[index] = group;
                }
                resolved = true;
                break;
            }

            const std::uint32_t encoded_group = impl_->group_table[slot];
            const std::size_t group = static_cast<std::size_t>(encoded_group - 1U);
            const KeyView& existing = keys[unique_source_indexes[group]];
            const bool identity_matches =
                    impl_->group_fingerprints[group] == candidate_fingerprint &&
                    existing.state_id == candidate.state_id &&
                    existing.generation == candidate.generation &&
                    existing.size == candidate.size;
            if (identity_matches) {
                bool exact_match = candidate.size == 0;
                if (!exact_match) {
                    ++impl_->group_diagnostics.exact_comparisons;
                    exact_match = impl_->kernel.equal_bytes(
                            candidate.data, existing.data, candidate.size);
                }
                if (exact_match) {
                    if (source_group_indexes != nullptr) {
                        source_group_indexes[index] =
                                static_cast<std::uint32_t>(group);
                    }
                    resolved = true;
                    break;
                }
            }
            slot = (slot + 1U) & table_mask;
        }
        if (!resolved) {
            return ErrorCode::kCapacityExceeded;
        }
    }
    *unique_count = written;
    return ErrorCode::kOk;
}

void RequestPlane::Clear() noexcept {
    impl_->Clear();
}

std::size_t RequestPlane::size() const noexcept {
    return impl_->entry_count;
}

std::size_t RequestPlane::capacity() const noexcept {
    return impl_->options.capacity_entries;
}

std::size_t RequestPlane::max_batch_entries() const noexcept {
    return impl_->options.max_batch_entries;
}

std::uint64_t RequestPlane::evictions() const noexcept {
    return impl_->eviction_count;
}

std::uint32_t RequestPlane::min_native_batch_size() const noexcept {
    return impl_->options.min_native_batch_size;
}

GroupBatchDiagnostics RequestPlane::group_batch_diagnostics() const noexcept {
    return impl_->group_diagnostics;
}

KernelKind RequestPlane::kernel_kind() const noexcept {
    return impl_->kernel.kind;
}

const char* RequestPlane::kernel_name() const noexcept {
    return impl_->kernel.name;
}

HostFeatures RequestPlane::host_features() const noexcept {
    return impl_->features;
}

const char* ErrorCodeName(ErrorCode code) noexcept {
    switch (code) {
        case ErrorCode::kOk:
            return "ok";
        case ErrorCode::kInvalidArgument:
            return "invalid_argument";
        case ErrorCode::kCapacityExceeded:
            return "capacity_exceeded";
        case ErrorCode::kOverflow:
            return "overflow";
        case ErrorCode::kUnsupportedKernel:
            return "unsupported_kernel";
        case ErrorCode::kAllocationFailed:
            return "allocation_failed";
        case ErrorCode::kInternal:
            return "internal";
    }
    return "unknown";
}

}  // namespace native
}  // namespace cachekit
