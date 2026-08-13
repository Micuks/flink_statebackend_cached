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

#include "jni_batch_codec.h"

#include <cstring>
#include <limits>
#include <new>
#include <stdexcept>

namespace cachekit {
namespace native {
namespace bridge {
namespace {

template <typename T>
T ReadNative(const std::uint8_t* source) noexcept {
    T value{};
    std::memcpy(&value, source, sizeof(value));
    return value;
}

template <typename T>
void WriteNative(std::uint8_t* destination, T value) noexcept {
    std::memcpy(destination, &value, sizeof(value));
}

bool IsValid(ConstBuffer buffer) noexcept {
    return buffer.size == 0 || buffer.data != nullptr;
}

bool IsValid(MutableBuffer buffer) noexcept {
    return buffer.size == 0 || buffer.data != nullptr;
}

bool RequiredBytes(
        std::size_t count, std::size_t record_bytes, std::size_t* result) noexcept {
    if (count > std::numeric_limits<std::size_t>::max() / record_bytes) {
        return false;
    }
    *result = count * record_bytes;
    return true;
}

bool SliceInBounds(
        std::int32_t offset, std::int32_t length, std::size_t arena_size) noexcept {
    if (offset < 0 || length < 0) {
        return false;
    }
    const std::size_t unsigned_offset = static_cast<std::size_t>(offset);
    const std::size_t unsigned_length = static_cast<std::size_t>(length);
    return unsigned_offset <= arena_size &&
            unsigned_length <= arena_size - unsigned_offset;
}

BatchBridgeCode DecodeKeys(
        ConstBuffer key_arena,
        ConstBuffer key_metadata,
        std::size_t count,
        std::vector<KeyView>* keys) {
    std::size_t required_metadata = 0;
    if (!RequiredBytes(count, kKeyMetadataRecordBytes, &required_metadata)) {
        return BatchBridgeCode::kOverflow;
    }
    if (!IsValid(key_arena) || !IsValid(key_metadata)) {
        return BatchBridgeCode::kInvalidArgument;
    }
    if (key_metadata.size < required_metadata) {
        return BatchBridgeCode::kInvalidMetadata;
    }

    keys->clear();
    for (std::size_t index = 0; index < count; ++index) {
        const std::uint8_t* record =
                key_metadata.data + index * kKeyMetadataRecordBytes;
        const std::int32_t state_id =
                ReadNative<std::int32_t>(record + kKeyStateIdOffset);
        const std::int32_t reserved =
                ReadNative<std::int32_t>(record + kKeyReservedOffset);
        const std::uint64_t generation =
                ReadNative<std::uint64_t>(record + kKeyGenerationOffset);
        const std::int32_t arena_offset =
                ReadNative<std::int32_t>(record + kKeyArenaOffsetOffset);
        const std::int32_t length =
                ReadNative<std::int32_t>(record + kKeyLengthOffset);
        if (reserved != 0) {
            return BatchBridgeCode::kInvalidMetadata;
        }
        if (!SliceInBounds(arena_offset, length, key_arena.size)) {
            return BatchBridgeCode::kInputOutOfBounds;
        }
        keys->push_back(KeyView{
                static_cast<std::uint32_t>(state_id),
                generation,
                length == 0
                        ? nullptr
                        : key_arena.data + static_cast<std::size_t>(arena_offset),
                static_cast<std::size_t>(length)});
    }
    return BatchBridgeCode::kOk;
}

}  // namespace

BatchScratch::BatchScratch(std::size_t reserve_entries) {
    ReserveEntries(reserve_entries);
}

void BatchScratch::ReserveEntries(std::size_t count) {
    if (count <= reserved_entries_) {
        return;
    }
    keys_.reserve(count);
    fills_.reserve(count);
    fill_results_.reserve(count);
    probe_results_.reserve(count);
    unique_source_indexes_.reserve(count);
    source_group_indexes_.reserve(count);
    reserved_entries_ = count;
    ++growth_count_;
}

BatchBridgeCode CompactDirectBatch(
        RequestPlane* plane,
        BatchScratch* scratch,
        ConstBuffer key_arena,
        ConstBuffer key_metadata,
        std::size_t count,
        MutableBuffer unique_source_indexes,
        std::size_t* unique_count) noexcept {
    if (plane == nullptr || scratch == nullptr || unique_count == nullptr ||
        !IsValid(unique_source_indexes)) {
        return BatchBridgeCode::kInvalidArgument;
    }
    std::size_t required_output = 0;
    if (!RequiredBytes(count, sizeof(std::uint32_t), &required_output)) {
        return BatchBridgeCode::kOverflow;
    }
    if (unique_source_indexes.size < required_output) {
        return BatchBridgeCode::kOutputTooSmall;
    }
    try {
        scratch->ReserveEntries(count);
        BatchBridgeCode code =
                DecodeKeys(key_arena, key_metadata, count, &scratch->keys_);
        if (code != BatchBridgeCode::kOk) {
            return code;
        }
        scratch->unique_source_indexes_.resize(count);
        std::size_t written = 0;
        if (plane->CompactBatch(
                    scratch->keys_.data(),
                    scratch->unique_source_indexes_.data(),
                    count,
                    &written) != ErrorCode::kOk) {
            return BatchBridgeCode::kNativeError;
        }
        for (std::size_t index = 0; index < written; ++index) {
            WriteNative<std::uint32_t>(
                    unique_source_indexes.data + index * sizeof(std::uint32_t),
                    scratch->unique_source_indexes_[index]);
        }
        *unique_count = written;
        return BatchBridgeCode::kOk;
    } catch (const std::bad_alloc&) {
        return BatchBridgeCode::kAllocationFailed;
    } catch (const std::length_error&) {
        return BatchBridgeCode::kOverflow;
    } catch (...) {
        return BatchBridgeCode::kNativeError;
    }
}

BatchBridgeCode GroupDirectBatch(
        RequestPlane* plane,
        BatchScratch* scratch,
        ConstBuffer key_arena,
        ConstBuffer key_metadata,
        std::size_t count,
        MutableBuffer unique_source_indexes,
        MutableBuffer source_group_indexes,
        std::size_t* unique_count) noexcept {
    if (plane == nullptr || scratch == nullptr || unique_count == nullptr ||
        !IsValid(unique_source_indexes) || !IsValid(source_group_indexes)) {
        return BatchBridgeCode::kInvalidArgument;
    }
    std::size_t required_output = 0;
    if (!RequiredBytes(count, sizeof(std::uint32_t), &required_output)) {
        return BatchBridgeCode::kOverflow;
    }
    if (unique_source_indexes.size < required_output ||
        source_group_indexes.size < required_output) {
        return BatchBridgeCode::kOutputTooSmall;
    }
    try {
        scratch->ReserveEntries(count);
        BatchBridgeCode code = DecodeKeys(key_arena, key_metadata, count, &scratch->keys_);
        if (code != BatchBridgeCode::kOk) {
            return code;
        }
        scratch->unique_source_indexes_.resize(count);
        scratch->source_group_indexes_.resize(count);
        std::size_t written = 0;
        if (plane->GroupBatch(
                    scratch->keys_.data(),
                    scratch->unique_source_indexes_.data(),
                    scratch->source_group_indexes_.data(),
                    count,
                    &written) != ErrorCode::kOk) {
            return BatchBridgeCode::kNativeError;
        }
        for (std::size_t index = 0; index < written; ++index) {
            WriteNative<std::uint32_t>(
                    unique_source_indexes.data + index * sizeof(std::uint32_t),
                    scratch->unique_source_indexes_[index]);
        }
        for (std::size_t index = 0; index < count; ++index) {
            WriteNative<std::uint32_t>(
                    source_group_indexes.data + index * sizeof(std::uint32_t),
                    scratch->source_group_indexes_[index]);
        }
        *unique_count = written;
        return BatchBridgeCode::kOk;
    } catch (const std::bad_alloc&) {
        return BatchBridgeCode::kAllocationFailed;
    } catch (const std::length_error&) {
        return BatchBridgeCode::kOverflow;
    } catch (...) {
        return BatchBridgeCode::kNativeError;
    }
}

BatchBridgeCode FillDirectBatch(
        RequestPlane* plane,
        BatchScratch* scratch,
        ConstBuffer key_arena,
        ConstBuffer key_metadata,
        ConstBuffer value_arena,
        ConstBuffer value_metadata,
        std::size_t count,
        MutableBuffer fill_results) noexcept {
    if (plane == nullptr || scratch == nullptr || !IsValid(value_arena) ||
        !IsValid(value_metadata) || !IsValid(fill_results)) {
        return BatchBridgeCode::kInvalidArgument;
    }
    std::size_t required_value_metadata = 0;
    std::size_t required_results = 0;
    if (!RequiredBytes(count, kFillValueRecordBytes, &required_value_metadata) ||
        !RequiredBytes(count, kFillResultRecordBytes, &required_results)) {
        return BatchBridgeCode::kOverflow;
    }
    if (value_metadata.size < required_value_metadata) {
        return BatchBridgeCode::kInvalidMetadata;
    }
    if (fill_results.size < required_results) {
        return BatchBridgeCode::kOutputTooSmall;
    }

    try {
        scratch->ReserveEntries(count);
        BatchBridgeCode code =
                DecodeKeys(key_arena, key_metadata, count, &scratch->keys_);
        if (code != BatchBridgeCode::kOk) {
            return code;
        }

        scratch->fills_.clear();
        for (std::size_t index = 0; index < count; ++index) {
            const std::uint8_t* record =
                    value_metadata.data + index * kFillValueRecordBytes;
            const std::int32_t arena_offset =
                    ReadNative<std::int32_t>(
                            record + kFillValueArenaOffsetOffset);
            const std::int32_t length =
                    ReadNative<std::int32_t>(record + kFillValueLengthOffset);
            const std::uint32_t flags =
                    ReadNative<std::uint32_t>(record + kFillValueFlagsOffset);
            const std::uint32_t reserved =
                    ReadNative<std::uint32_t>(record + kFillValueReservedOffset);
            if (reserved != 0 || (flags & ~kFillValueNegativeFlag) != 0) {
                return BatchBridgeCode::kInvalidMetadata;
            }
            const bool negative = (flags & kFillValueNegativeFlag) != 0;
            if (!SliceInBounds(arena_offset, length, value_arena.size) ||
                (negative && (arena_offset != 0 || length != 0))) {
                return BatchBridgeCode::kInputOutOfBounds;
            }
            scratch->fills_.push_back(FillView{
                    scratch->keys_[index],
                    length == 0
                            ? nullptr
                            : value_arena.data +
                                      static_cast<std::size_t>(arena_offset),
                    static_cast<std::size_t>(length),
                    negative});
        }

        scratch->fill_results_.resize(count);
        if (plane->FillBatch(
                    scratch->fills_.data(),
                    scratch->fill_results_.data(),
                    count) !=
            ErrorCode::kOk) {
            return BatchBridgeCode::kNativeError;
        }
        for (std::size_t index = 0; index < count; ++index) {
            std::uint8_t* record =
                    fill_results.data + index * kFillResultRecordBytes;
            WriteNative<std::uint32_t>(
                    record + kFillResultStatusOffset,
                    static_cast<std::uint32_t>(
                            scratch->fill_results_[index].status));
            WriteNative<std::uint32_t>(
                    record + kFillResultErrorOffset,
                    static_cast<std::uint32_t>(
                            scratch->fill_results_[index].error));
        }
        return BatchBridgeCode::kOk;
    } catch (const std::bad_alloc&) {
        return BatchBridgeCode::kAllocationFailed;
    } catch (const std::length_error&) {
        return BatchBridgeCode::kOverflow;
    } catch (...) {
        return BatchBridgeCode::kNativeError;
    }
}

BatchBridgeCode ProbeDirectBatch(
        RequestPlane* plane,
        BatchScratch* scratch,
        ConstBuffer key_arena,
        ConstBuffer key_metadata,
        std::size_t count,
        MutableBuffer value_output,
        MutableBuffer probe_results) noexcept {
    if (plane == nullptr || scratch == nullptr || !IsValid(value_output) ||
        !IsValid(probe_results)) {
        return BatchBridgeCode::kInvalidArgument;
    }
    std::size_t required_results = 0;
    if (!RequiredBytes(count, kProbeResultRecordBytes, &required_results)) {
        return BatchBridgeCode::kOverflow;
    }
    if (probe_results.size < required_results) {
        return BatchBridgeCode::kOutputTooSmall;
    }

    try {
        scratch->ReserveEntries(count);
        BatchBridgeCode code =
                DecodeKeys(key_arena, key_metadata, count, &scratch->keys_);
        if (code != BatchBridgeCode::kOk) {
            return code;
        }
        scratch->probe_results_.resize(count);
        if (plane->ProbeBatch(
                    scratch->keys_.data(),
                    scratch->probe_results_.data(),
                    count) !=
            ErrorCode::kOk) {
            return BatchBridgeCode::kNativeError;
        }

        std::size_t required_values = 0;
        for (const ProbeResult& result : scratch->probe_results_) {
            if (result.error != ErrorCode::kOk) {
                return BatchBridgeCode::kNativeError;
            }
            if (result.value_size >
                std::numeric_limits<std::uint32_t>::max()) {
                return BatchBridgeCode::kOverflow;
            }
            if (result.value_size >
                std::numeric_limits<std::size_t>::max() - required_values) {
                return BatchBridgeCode::kOverflow;
            }
            required_values += result.value_size;
        }
        if (required_values > value_output.size ||
            required_values > std::numeric_limits<std::uint32_t>::max()) {
            return BatchBridgeCode::kOutputTooSmall;
        }

        std::size_t value_offset = 0;
        for (std::size_t index = 0; index < count; ++index) {
            const ProbeResult& result = scratch->probe_results_[index];
            if (result.value_size != 0) {
                std::memcpy(
                        value_output.data + value_offset,
                        result.value,
                        result.value_size);
            }
            std::uint8_t* record =
                    probe_results.data + index * kProbeResultRecordBytes;
            WriteNative<std::uint32_t>(
                    record + kProbeResultStatusOffset,
                    static_cast<std::uint32_t>(result.status));
            WriteNative<std::uint32_t>(
                    record + kProbeResultErrorOffset,
                    static_cast<std::uint32_t>(result.error));
            WriteNative<std::uint32_t>(
                    record + kProbeResultArenaOffsetOffset,
                    result.value_size == 0
                            ? 0U
                            : static_cast<std::uint32_t>(value_offset));
            WriteNative<std::uint32_t>(
                    record + kProbeResultLengthOffset,
                    static_cast<std::uint32_t>(result.value_size));
            value_offset += result.value_size;
        }
        return BatchBridgeCode::kOk;
    } catch (const std::bad_alloc&) {
        return BatchBridgeCode::kAllocationFailed;
    } catch (const std::length_error&) {
        return BatchBridgeCode::kOverflow;
    } catch (...) {
        return BatchBridgeCode::kNativeError;
    }
}

BatchBridgeCode FillDirectBatch(
        RequestPlane* plane,
        ConstBuffer key_arena,
        ConstBuffer key_metadata,
        ConstBuffer value_arena,
        ConstBuffer value_metadata,
        std::size_t count,
        MutableBuffer fill_results) noexcept {
    // Retain the original internal codec ABI for standalone callers. JNI uses
    // the scratch-taking overload owned by BridgeHandle.
    BatchScratch scratch;
    return FillDirectBatch(
            plane,
            &scratch,
            key_arena,
            key_metadata,
            value_arena,
            value_metadata,
            count,
            fill_results);
}

BatchBridgeCode ProbeDirectBatch(
        RequestPlane* plane,
        ConstBuffer key_arena,
        ConstBuffer key_metadata,
        std::size_t count,
        MutableBuffer value_output,
        MutableBuffer probe_results) noexcept {
    // Retain the original internal codec ABI for standalone callers. JNI uses
    // the scratch-taking overload owned by BridgeHandle.
    BatchScratch scratch;
    return ProbeDirectBatch(
            plane,
            &scratch,
            key_arena,
            key_metadata,
            count,
            value_output,
            probe_results);
}

const char* BatchBridgeCodeName(BatchBridgeCode code) noexcept {
    switch (code) {
        case BatchBridgeCode::kOk:
            return "ok";
        case BatchBridgeCode::kInvalidArgument:
            return "invalid_argument";
        case BatchBridgeCode::kInvalidMetadata:
            return "invalid_metadata";
        case BatchBridgeCode::kInputOutOfBounds:
            return "input_out_of_bounds";
        case BatchBridgeCode::kOutputTooSmall:
            return "output_too_small";
        case BatchBridgeCode::kAllocationFailed:
            return "allocation_failed";
        case BatchBridgeCode::kNativeError:
            return "native_error";
        case BatchBridgeCode::kOverflow:
            return "overflow";
    }
    return "unknown";
}

}  // namespace bridge
}  // namespace native
}  // namespace cachekit
