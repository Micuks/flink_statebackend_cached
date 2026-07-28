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
#include <arm_sve.h>

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

std::uint16_t HybridMatchTags(const Bucket& bucket, std::uint32_t tag) noexcept {
    // Kunpeng's 256-bit SVE implementation wins on exact byte comparison, but
    // direct ABBA measurements show a stable regression on the all-miss tag
    // path. A bucket is exactly one 128-byte Kunpeng L3 line; four fixed-width
    // NEON loads scan its 16 tags more cheaply than materialising SVE predicate
    // lanes. Keep SVE for the hit-only exact-key comparison below.
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
        "aarch64-sve256-hybrid-crc32c",
        &SveCrcFingerprint,
        &HybridMatchTags,
        &SveEqualBytes};

}  // namespace

const KernelOps& Sve256Kernel() noexcept {
    return kSve256Ops;
}

}  // namespace internal
}  // namespace native
}  // namespace cachekit
