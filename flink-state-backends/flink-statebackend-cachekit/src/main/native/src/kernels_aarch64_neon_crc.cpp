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

#include "kernels_internal.h"

#include <arm_acle.h>
#include <arm_neon.h>

#include <cstring>

namespace cachekit {
namespace native {
namespace internal {
namespace {

std::uint32_t NeonCrcFingerprint(
        std::uint32_t state_id, const std::uint8_t* data, std::size_t size) noexcept {
    std::uint32_t crc = __crc32cw(0xffffffffU, state_id);
    std::size_t index = 0;
    while (index + sizeof(std::uint64_t) <= size) {
        std::uint64_t word = 0;
        std::memcpy(&word, data + index, sizeof(word));
        crc = __crc32cd(crc, word);
        index += sizeof(word);
    }
    while (index < size) {
        crc = __crc32cb(crc, data[index]);
        ++index;
    }
    return ~crc;
}

std::uint16_t NeonMatchTags(const Bucket& bucket, std::uint32_t tag) noexcept {
    const uint32x4_t expected = vdupq_n_u32(tag);
    std::uint16_t mask = 0;
    for (std::size_t base = 0; base < kSlotsPerBucket; base += 4) {
        const uint32x4_t values = vld1q_u32(bucket.tags + base);
        const uint32x4_t equal = vceqq_u32(values, expected);
        std::uint32_t lanes[4];
        vst1q_u32(lanes, equal);
        for (std::size_t lane = 0; lane < 4; ++lane) {
            if (lanes[lane] != 0) {
                mask |= static_cast<std::uint16_t>(1U << (base + lane));
            }
        }
    }
    return mask;
}

bool NeonEqualBytes(
        const std::uint8_t* left,
        const std::uint8_t* right,
        std::size_t size) noexcept {
    std::size_t index = 0;
    while (index + 16U <= size) {
        const uint8x16_t left_bytes = vld1q_u8(left + index);
        const uint8x16_t right_bytes = vld1q_u8(right + index);
        const uint8x16_t equal = vceqq_u8(left_bytes, right_bytes);
        if (vminvq_u8(equal) != 0xffU) {
            return false;
        }
        index += 16U;
    }
    return index == size ||
            std::memcmp(left + index, right + index, size - index) == 0;
}

const KernelOps kNeonCrcOps = {
        KernelKind::kNeonCrc,
        "aarch64-neon-crc32c",
        &NeonCrcFingerprint,
        &NeonMatchTags,
        &NeonEqualBytes};

}  // namespace

const KernelOps& NeonCrcKernel() noexcept {
    return kNeonCrcOps;
}

}  // namespace internal
}  // namespace native
}  // namespace cachekit
