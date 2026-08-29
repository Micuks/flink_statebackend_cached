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

#ifndef CACHEKIT_NATIVE_REQUEST_PLANE_H_
#define CACHEKIT_NATIVE_REQUEST_PLANE_H_

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>

namespace cachekit {
namespace native {

// Native request-plane calls are intentionally batch-oriented.  The value is a
// policy hint for the future JNI bridge, not a restriction on this C++ API.
constexpr std::uint32_t kDefaultMinNativeBatchSize = 64;
// Probe-only sentinel: return the latest exact-key version. Real fills must use a monotonically
// increasing per-key write-order token and never this value.
constexpr std::uint64_t kLatestGeneration =
        std::numeric_limits<std::uint64_t>::max();

enum class KernelPreference : std::uint32_t {
    kAuto = 0,
    kScalar = 1,
    kNeonCrc = 2,
    kSve256 = 3,
};

enum class KernelKind : std::uint32_t {
    kScalar = 1,
    kNeonCrc = 2,
    kSve256 = 3,
    kSse42Crc = 4,
};

enum class ErrorCode : std::uint32_t {
    kOk = 0,
    kInvalidArgument = 1,
    kCapacityExceeded = 2,
    kOverflow = 3,
    kUnsupportedKernel = 4,
    kAllocationFailed = 5,
    kInternal = 6,
};

enum class ProbeStatus : std::uint8_t {
    kMiss = 0,
    kHit = 1,
    kNegative = 2,
};

enum class FillStatus : std::uint8_t {
    kInserted = 0,
    kUpdated = 1,
    kRejectedStaleGeneration = 2,
    kRejectedCapacity = 3,
    kInvalidArgument = 4,
    kInternalError = 5,
    kNotPresent = 6,
};

struct Options {
    std::size_t capacity_entries = 1024;
    // Zero preserves the legacy C++ API contract by using capacity_entries.
    // JNI callers pass their configured batch-entry limit explicitly.
    std::size_t max_batch_entries = 0;
    std::size_t key_arena_bytes = 1U << 20U;
    std::size_t value_arena_bytes = 4U << 20U;
    std::uint32_t min_native_batch_size = kDefaultMinNativeBatchSize;
    KernelPreference kernel = KernelPreference::kAuto;

    // Production callers leave this at all ones.  Tests may reduce it to force
    // fingerprint collisions and verify the mandatory exact-key comparison.
    std::uint32_t fingerprint_mask = 0xffffffffU;
};

struct GroupBatchDiagnostics {
    std::uint64_t batches = 0;
    std::uint64_t fingerprint_calls = 0;
    std::uint64_t probe_steps = 0;
    std::uint64_t exact_comparisons = 0;
    std::uint64_t epoch_resets = 0;
};

struct KeyView {
    std::uint32_t state_id = 0;
    // Monotonic write-order token for conditional fills of this exact state/key. A probe must
    // either provide an exact generation or kLatestGeneration.
    std::uint64_t generation = 0;
    const std::uint8_t* data = nullptr;
    std::size_t size = 0;
};

struct FillView {
    KeyView key;
    const std::uint8_t* value = nullptr;
    std::size_t value_size = 0;
    bool negative = false;
    // Mutation-only controls. check_only advances the state generation watermark and reports
    // exact-key residency without changing the entry. update_only updates an exact resident key
    // but must never insert. Production fill callers leave both false.
    bool update_only = false;
    bool check_only = false;
};

struct ProbeResult {
    ProbeStatus status = ProbeStatus::kMiss;
    ErrorCode error = ErrorCode::kOk;
    const std::uint8_t* value = nullptr;
    std::size_t value_size = 0;
};

struct PresencePartitionSummary {
    std::size_t hits = 0;
    std::size_t negative_hits = 0;
    std::size_t misses = 0;
};

struct FillResult {
    FillStatus status = FillStatus::kInternalError;
    ErrorCode error = ErrorCode::kInternal;
};

struct HostFeatures {
    bool aarch64 = false;
    bool neon = false;
    bool crc32 = false;
    bool sve = false;
    bool sve_vector_length_256 = false;
    bool x86_64 = false;
    bool sse42 = false;
};

// A single RequestPlane is owned by one request-processing thread.  Probe
// results point into the value arena and remain valid until the next mutating
// call (FillBatch/Clear) or destruction.
class RequestPlane final {
public:
    static std::unique_ptr<RequestPlane> Create(
            const Options& options, ErrorCode* error, std::string* message);

    ~RequestPlane();

    RequestPlane(const RequestPlane&) = delete;
    RequestPlane& operator=(const RequestPlane&) = delete;
    RequestPlane(RequestPlane&&) noexcept;
    RequestPlane& operator=(RequestPlane&&) noexcept;

    ErrorCode ProbeBatch(
            const KeyView* keys, ProbeResult* results, std::size_t count) noexcept;
    // Probes exact-key presence and emits only miss source indexes. This avoids
    // materializing a full ProbeResult record for keys already resident in the
    // request plane while preserving exact-key validation in ProbeOne().
    ErrorCode PartitionPresenceBatch(
            const KeyView* keys,
            std::uint32_t* miss_source_indexes,
            std::size_t count,
            PresencePartitionSummary* summary) noexcept;
    ErrorCode FillBatch(
            const FillView* fills, FillResult* results, std::size_t count) noexcept;
    // Keeps first occurrence order and writes source indexes for exact duplicate keys.
    // Fingerprint and equality use the runtime-selected scalar/NEON/SVE kernel.
    ErrorCode CompactBatch(
            const KeyView* keys,
            std::uint32_t* unique_source_indexes,
            std::size_t count,
            std::size_t* unique_count) const noexcept;
    // Additionally maps every source entry to its stable, first-seen group id.
    ErrorCode GroupBatch(
            const KeyView* keys,
            std::uint32_t* unique_source_indexes,
            std::uint32_t* source_group_indexes,
            std::size_t count,
            std::size_t* unique_count) const noexcept;
    // Groups caller-provided 32-bit identity tokens without fingerprinting key
    // bytes.  This is a speculative grouping plan only: callers must still
    // validate exact Java key equality before applying a grouped operation.
    ErrorCode GroupTokenBatch(
            const std::uint32_t* tokens,
            std::uint32_t* first_source_indexes,
            std::uint32_t* source_group_indexes,
            std::uint32_t* group_counts,
            std::size_t count,
            std::size_t* group_count) const noexcept;

    void Clear() noexcept;

    std::size_t size() const noexcept;
    std::size_t capacity() const noexcept;
    std::size_t max_batch_entries() const noexcept;
    std::uint64_t evictions() const noexcept;
    std::uint32_t min_native_batch_size() const noexcept;
    GroupBatchDiagnostics group_batch_diagnostics() const noexcept;
    KernelKind kernel_kind() const noexcept;
    const char* kernel_name() const noexcept;
    HostFeatures host_features() const noexcept;

private:
    struct Impl;
    explicit RequestPlane(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

const char* ErrorCodeName(ErrorCode code) noexcept;
HostFeatures DetectHostFeatures() noexcept;

}  // namespace native
}  // namespace cachekit

#endif  // CACHEKIT_NATIVE_REQUEST_PLANE_H_
