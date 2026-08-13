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

#include <cstring>

namespace cachekit {
namespace native {
namespace internal {
namespace {

std::uint32_t Crc32cByte(std::uint32_t crc, std::uint8_t byte) noexcept {
    crc ^= byte;
    for (unsigned bit = 0; bit < 8; ++bit) {
        const std::uint32_t mask =
                static_cast<std::uint32_t>(-static_cast<std::int32_t>(crc & 1U));
        crc = (crc >> 1U) ^ (0x82f63b78U & mask);
    }
    return crc;
}

std::uint32_t ScalarFingerprint(
        std::uint32_t state_id, const std::uint8_t* data, std::size_t size) noexcept {
    std::uint32_t crc = 0xffffffffU;
    for (unsigned shift = 0; shift < 32; shift += 8) {
        crc = Crc32cByte(crc, static_cast<std::uint8_t>(state_id >> shift));
    }
    for (std::size_t index = 0; index < size; ++index) {
        crc = Crc32cByte(crc, data[index]);
    }
    return ~crc;
}

std::uint16_t ScalarMatchTags(const Bucket& bucket, std::uint32_t tag) noexcept {
    std::uint16_t mask = 0;
    for (std::size_t index = 0; index < kSlotsPerBucket; ++index) {
        if (bucket.tags[index] == tag) {
            mask = static_cast<std::uint16_t>(
                    mask | static_cast<std::uint16_t>(1U << index));
        }
    }
    return mask;
}

bool ScalarEqualBytes(
        const std::uint8_t* left,
        const std::uint8_t* right,
        std::size_t size) noexcept {
    return size == 0 || std::memcmp(left, right, size) == 0;
}

const KernelOps kScalarOps = {
        KernelKind::kScalar,
        "scalar-crc32c",
        &ScalarFingerprint,
        &ScalarMatchTags,
        &ScalarEqualBytes};

}  // namespace

const KernelOps& ScalarKernel() noexcept {
    return kScalarOps;
}

}  // namespace internal
}  // namespace native
}  // namespace cachekit
