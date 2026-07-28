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
#include <limits>
#include <new>
#include <stdexcept>
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

struct Block {
    std::size_t offset;
    std::size_t size;
};

class ByteArena {
public:
    ByteArena(std::size_t capacity, std::size_t max_free_blocks)
            : bytes_(capacity) {
        free_.reserve(max_free_blocks);
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
        for (std::size_t index = 0; index < free_.size(); ++index) {
            Block& block = free_[index];
            if (block.size < size) {
                continue;
            }
            *offset = block.offset;
            block.offset += size;
            block.size -= size;
            if (block.size == 0) {
                free_.erase(free_.begin() + static_cast<std::ptrdiff_t>(index));
            }
            return true;
        }
        return false;
    }

    void Release(std::size_t offset, std::size_t size) {
        if (size == 0) {
            return;
        }
        if (offset > bytes_.size() || size > bytes_.size() - offset) {
            return;
        }
        const Block released{offset, size};
        const auto position = std::lower_bound(
                free_.begin(),
                free_.end(),
                released,
                [](const Block& left, const Block& right) {
                    return left.offset < right.offset;
                });
        free_.insert(position, released);
        Coalesce();
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
        free_.clear();
        if (!bytes_.empty()) {
            free_.push_back(Block{0, bytes_.size()});
        }
    }

private:
    void Coalesce() {
        if (free_.size() < 2) {
            return;
        }
        std::size_t output = 0;
        for (std::size_t input = 1; input < free_.size(); ++input) {
            Block& current = free_[output];
            const Block& next = free_[input];
            if (current.offset + current.size == next.offset) {
                current.size += next.size;
            } else {
                ++output;
                free_[output] = next;
            }
        }
        free_.resize(output + 1);
    }

    std::vector<std::uint8_t> bytes_;
    std::vector<Block> free_;
};

struct Entry {
    bool occupied = false;
    bool negative = false;
    std::uint32_t state_id = 0;
    std::uint64_t generation = 0;
    std::size_t key_offset = 0;
    std::size_t key_size = 0;
    std::size_t value_offset = 0;
    std::size_t value_size = 0;
    std::uint64_t last_use = 0;
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
    // Keep the metadata table at or below 50% occupancy: eight entries per
    // sixteen-slot bucket.
    std::size_t required = capacity_entries / 8U;
    if ((capacity_entries % 8U) != 0) {
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
                      options.capacity_entries + 1U) {
        free_entry_ids.reserve(options.capacity_entries);
        for (std::size_t id = options.capacity_entries; id > 0; --id) {
            free_entry_ids.push_back(static_cast<std::uint32_t>(id));
        }
    }

    std::uint64_t NextTick() noexcept {
        if (clock == std::numeric_limits<std::uint64_t>::max()) {
            for (std::size_t id = 1; id < entries.size(); ++id) {
                if (entries[id].occupied) {
                    entries[id].last_use = 1;
                }
            }
            clock = 1;
        }
        return ++clock;
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

    void RemoveEntry(std::uint32_t id, bool eviction) noexcept {
        if (id == 0 || id >= entries.size() || !entries[id].occupied) {
            return;
        }
        Entry& entry = entries[id];
        Bucket& bucket = buckets[entry.bucket_index];
        bucket.tags[entry.slot_index] = kTombstoneTag;
        bucket.entry_ids[entry.slot_index] = 0;
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
    }

    bool EvictOldest(std::uint32_t excluded_id = 0) noexcept {
        std::uint32_t victim = 0;
        std::uint64_t oldest = std::numeric_limits<std::uint64_t>::max();
        for (std::size_t id = 1; id < entries.size(); ++id) {
            const Entry& entry = entries[id];
            if (!entry.occupied || id == excluded_id) {
                continue;
            }
            if (victim == 0 || entry.last_use < oldest) {
                victim = static_cast<std::uint32_t>(id);
                oldest = entry.last_use;
            }
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
            entry.last_use = NextTick();
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
            entry.last_use = NextTick();
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
        entry.last_use = NextTick();
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
            entry.last_use = NextTick();
            entry.bucket_index = location.bucket_index;
            entry.slot_index = location.slot_index;

            Bucket& bucket = buckets[location.bucket_index];
            bucket.tags[location.slot_index] = tag;
            bucket.entry_ids[location.slot_index] = id;
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
        if (entry.generation != key.generation) {
            return result;
        }
        entry.last_use = NextTick();
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
            (!fill.negative && !IsValidBytes(fill.value, fill.value_size))) {
            return FillResult{
                    FillStatus::kInvalidArgument, ErrorCode::kInvalidArgument};
        }
        if (fill.key.size > key_arena.capacity() ||
            (!fill.negative && fill.value_size > value_arena.capacity())) {
            return FillResult{
                    FillStatus::kRejectedCapacity,
                    ErrorCode::kCapacityExceeded};
        }

        const std::uint32_t tag = TagFor(fill.key);
        std::uint32_t existing_id = 0;
        if (FindExact(fill.key, tag, nullptr, &existing_id)) {
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
        entry_count = 0;
        clock = 0;
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
    std::size_t entry_count = 0;
    std::uint64_t clock = 0;
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

    const HostFeatures features = DetectHostFeatures();
    const char* kernel_message = "";
    ErrorCode kernel_error = ErrorCode::kOk;
    const KernelOps* kernel = internal::SelectKernel(
            options.kernel, features, &kernel_error, &kernel_message);
    if (kernel == nullptr) {
        SetError(kernel_error, kernel_message, error, message);
        return nullptr;
    }

    try {
        std::unique_ptr<Impl> impl(new Impl(options, *kernel, features));
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

void RequestPlane::Clear() noexcept {
    impl_->Clear();
}

std::size_t RequestPlane::size() const noexcept {
    return impl_->entry_count;
}

std::size_t RequestPlane::capacity() const noexcept {
    return impl_->options.capacity_entries;
}

std::uint64_t RequestPlane::evictions() const noexcept {
    return impl_->eviction_count;
}

std::uint32_t RequestPlane::min_native_batch_size() const noexcept {
    return impl_->options.min_native_batch_size;
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
