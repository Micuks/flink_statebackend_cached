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

constexpr std::size_t kFillResultRecordBytes = 8;
constexpr std::size_t kFillResultStatusOffset = 0;
constexpr std::size_t kFillResultErrorOffset = 4;

constexpr std::size_t kProbeResultRecordBytes = 16;
constexpr std::size_t kProbeResultStatusOffset = 0;
constexpr std::size_t kProbeResultErrorOffset = 4;
constexpr std::size_t kProbeResultArenaOffsetOffset = 8;
constexpr std::size_t kProbeResultLengthOffset = 12;

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
