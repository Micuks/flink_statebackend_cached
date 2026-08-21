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

#ifndef CACHEKIT_NATIVE_JNI_BATCH_CODEC_H_
#define CACHEKIT_NATIVE_JNI_BATCH_CODEC_H_

#include "cachekit/native_request_plane.h"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace cachekit {
namespace native {
namespace bridge {

constexpr std::size_t kKeyMetadataRecordBytes = 24;
constexpr std::size_t kKeyStateIdOffset = 0;
constexpr std::size_t kKeyReservedOffset = 4;
constexpr std::size_t kKeyGenerationOffset = 8;
constexpr std::size_t kKeyArenaOffsetOffset = 16;
constexpr std::size_t kKeyLengthOffset = 20;

constexpr std::size_t kFillValueRecordBytes = 16;
constexpr std::size_t kFillValueArenaOffsetOffset = 0;
constexpr std::size_t kFillValueLengthOffset = 4;
constexpr std::size_t kFillValueFlagsOffset = 8;
constexpr std::size_t kFillValueReservedOffset = 12;
constexpr std::uint32_t kFillValueNegativeFlag = 1U;
constexpr std::uint32_t kFillValueUpdateOnlyFlag = 1U;
constexpr std::uint32_t kFillValueCheckOnlyFlag = 1U << 1U;

constexpr std::size_t kFillResultRecordBytes = 8;
constexpr std::size_t kFillResultStatusOffset = 0;
constexpr std::size_t kFillResultErrorOffset = 4;

constexpr std::size_t kProbeResultRecordBytes = 16;
constexpr std::size_t kProbeResultStatusOffset = 0;
constexpr std::size_t kProbeResultErrorOffset = 4;
constexpr std::size_t kProbeResultArenaOffsetOffset = 8;
constexpr std::size_t kProbeResultLengthOffset = 12;

// Token32 packed grouping plan V1, written in native byte order.  The magic is
// the commit marker and is written last; zero always means invalid/incomplete.
constexpr std::uint32_t kTokenPlanMagic = 0x434b4750U;
constexpr std::uint32_t kTokenPlanLayoutVersion = 1U;
constexpr std::size_t kTokenPlanHeaderBytes = 16U;
constexpr std::size_t kTokenPlanMagicOffset = 0U;
constexpr std::size_t kTokenPlanVersionOffset = 4U;
constexpr std::size_t kTokenPlanSourceCountOffset = 8U;
constexpr std::size_t kTokenPlanGroupCountOffset = 12U;
constexpr std::size_t kTokenPlanWorstCaseBaseBytes = 20U;
constexpr std::size_t kTokenPlanWorstCasePerSourceBytes = 12U;

enum class BatchBridgeCode : std::int32_t {
    kOk = 0,
    kInvalidArgument = 1,
    kInvalidMetadata = 2,
    kInputOutOfBounds = 3,
    kOutputTooSmall = 4,
    kAllocationFailed = 5,
    kNativeError = 6,
    kOverflow = 7,
};

struct ConstBuffer {
    const std::uint8_t* data = nullptr;
    std::size_t size = 0;
};

struct MutableBuffer {
    std::uint8_t* data = nullptr;
    std::size_t size = 0;
};

// Single-owner reusable storage for the direct-buffer codec.  ReserveEntries()
// may allocate only when a caller exceeds the largest batch seen so far.
// FillDirectBatch()/ProbeDirectBatch() clear and reuse this storage and are
// therefore allocation-free at steady-state batch sizes.
class BatchScratch final {
public:
    explicit BatchScratch(std::size_t reserve_entries = 0);

    BatchScratch(const BatchScratch&) = delete;
    BatchScratch& operator=(const BatchScratch&) = delete;

    void ReserveEntries(std::size_t count);

    std::size_t reserved_entries() const noexcept {
        return reserved_entries_;
    }

    std::uint64_t growth_count() const noexcept {
        return growth_count_;
    }

private:
    friend BatchBridgeCode FillDirectBatch(
            RequestPlane* plane,
            BatchScratch* scratch,
            ConstBuffer key_arena,
            ConstBuffer key_metadata,
            ConstBuffer value_arena,
            ConstBuffer value_metadata,
            std::size_t count,
            MutableBuffer fill_results) noexcept;

    friend BatchBridgeCode ProbeDirectBatch(
            RequestPlane* plane,
            BatchScratch* scratch,
            ConstBuffer key_arena,
            ConstBuffer key_metadata,
            std::size_t count,
            MutableBuffer value_output,
            MutableBuffer probe_results) noexcept;

    friend BatchBridgeCode CompactDirectBatch(
            RequestPlane* plane,
            BatchScratch* scratch,
            ConstBuffer key_arena,
            ConstBuffer key_metadata,
            std::size_t count,
            MutableBuffer unique_source_indexes,
            std::size_t* unique_count) noexcept;

    friend BatchBridgeCode GroupDirectBatch(
            RequestPlane* plane,
            BatchScratch* scratch,
            ConstBuffer key_arena,
            ConstBuffer key_metadata,
            std::size_t count,
            MutableBuffer unique_source_indexes,
            MutableBuffer source_group_indexes,
            std::size_t* unique_count) noexcept;

    friend BatchBridgeCode GroupTokenPlanDirectBatch(
            RequestPlane* plane,
            BatchScratch* scratch,
            ConstBuffer source_tokens,
            std::size_t count,
            MutableBuffer packed_plan,
            std::size_t* group_count) noexcept;

    std::vector<KeyView> keys_;
    std::vector<FillView> fills_;
    std::vector<FillResult> fill_results_;
    std::vector<ProbeResult> probe_results_;
    std::vector<std::uint32_t> unique_source_indexes_;
    std::vector<std::uint32_t> source_group_indexes_;
    std::vector<std::uint32_t> source_tokens_;
    std::vector<std::uint32_t> group_counts_;
    std::size_t reserved_entries_ = 0;
    std::uint64_t growth_count_ = 0;
};

BatchBridgeCode FillDirectBatch(
        RequestPlane* plane,
        BatchScratch* scratch,
        ConstBuffer key_arena,
        ConstBuffer key_metadata,
        ConstBuffer value_arena,
        ConstBuffer value_metadata,
        std::size_t count,
        MutableBuffer fill_results) noexcept;

BatchBridgeCode FillDirectBatch(
        RequestPlane* plane,
        ConstBuffer key_arena,
        ConstBuffer key_metadata,
        ConstBuffer value_arena,
        ConstBuffer value_metadata,
        std::size_t count,
        MutableBuffer fill_results) noexcept;

BatchBridgeCode ProbeDirectBatch(
        RequestPlane* plane,
        BatchScratch* scratch,
        ConstBuffer key_arena,
        ConstBuffer key_metadata,
        std::size_t count,
        MutableBuffer value_output,
        MutableBuffer probe_results) noexcept;

BatchBridgeCode CompactDirectBatch(
        RequestPlane* plane,
        BatchScratch* scratch,
        ConstBuffer key_arena,
        ConstBuffer key_metadata,
        std::size_t count,
        MutableBuffer unique_source_indexes,
        std::size_t* unique_count) noexcept;

BatchBridgeCode GroupDirectBatch(
        RequestPlane* plane,
        BatchScratch* scratch,
        ConstBuffer key_arena,
        ConstBuffer key_metadata,
        std::size_t count,
        MutableBuffer unique_source_indexes,
        MutableBuffer source_group_indexes,
        std::size_t* unique_count) noexcept;

BatchBridgeCode GroupTokenPlanDirectBatch(
        RequestPlane* plane,
        BatchScratch* scratch,
        ConstBuffer source_tokens,
        std::size_t count,
        MutableBuffer packed_plan,
        std::size_t* group_count) noexcept;

BatchBridgeCode ProbeDirectBatch(
        RequestPlane* plane,
        ConstBuffer key_arena,
        ConstBuffer key_metadata,
        std::size_t count,
        MutableBuffer value_output,
        MutableBuffer probe_results) noexcept;

const char* BatchBridgeCodeName(BatchBridgeCode code) noexcept;

}  // namespace bridge
}  // namespace native
}  // namespace cachekit

#endif  // CACHEKIT_NATIVE_JNI_BATCH_CODEC_H_
