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
#include <arm_sve.h>

#include <algorithm>
#include <cstring>

namespace cachekit {
namespace native {
namespace internal {
namespace {

std::uint32_t SveCrcFingerprint(
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

std::uint16_t SveMatchTags(const Bucket& bucket, std::uint32_t tag) noexcept {
    std::uint16_t mask = 0;
    const std::size_t lanes = svcntw();
    for (std::size_t base = 0; base < kSlotsPerBucket; base += lanes) {
        const svbool_t active = svwhilelt_b32(base, kSlotsPerBucket);
        const svuint32_t values = svld1_u32(active, bucket.tags + base);
        const svbool_t equal = svcmpeq_n_u32(active, values, tag);
        const svuint32_t bits = svsel_u32(
                equal, svdup_n_u32(1U), svdup_n_u32(0U));
        std::uint32_t lane_bits[8] = {};
        svst1_u32(active, lane_bits, bits);
        const std::size_t remaining =
                std::min<std::size_t>(lanes, kSlotsPerBucket - base);
        for (std::size_t lane = 0; lane < remaining; ++lane) {
            if (lane_bits[lane] != 0) {
                mask |= static_cast<std::uint16_t>(1U << (base + lane));
            }
        }
    }
    return mask;
}

bool SveEqualBytes(
        const std::uint8_t* left,
        const std::uint8_t* right,
        std::size_t size) noexcept {
    std::size_t index = 0;
    while (index < size) {
        const svbool_t active = svwhilelt_b8(index, size);
        const svuint8_t left_bytes = svld1_u8(active, left + index);
        const svuint8_t right_bytes = svld1_u8(active, right + index);
        const svbool_t equal = svcmpeq_u8(active, left_bytes, right_bytes);
        if (svcntp_b8(active, equal) != svcntp_b8(active, active)) {
            return false;
        }
        index += svcntb();
    }
    return true;
}

const KernelOps kSve256Ops = {
        KernelKind::kSve256,
        "aarch64-sve256-crc32c",
        &SveCrcFingerprint,
        &SveMatchTags,
        &SveEqualBytes};

}  // namespace

const KernelOps& Sve256Kernel() noexcept {
    return kSve256Ops;
}

}  // namespace internal
}  // namespace native
}  // namespace cachekit
