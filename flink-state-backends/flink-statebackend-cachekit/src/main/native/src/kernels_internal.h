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

#ifndef CACHEKIT_NATIVE_KERNELS_INTERNAL_H_
#define CACHEKIT_NATIVE_KERNELS_INTERNAL_H_

#include "cachekit/native_request_plane.h"

#include <cstddef>
#include <cstdint>

namespace cachekit {
namespace native {
namespace internal {

#if defined(__aarch64__)
constexpr std::size_t kCacheLineBytes = 128;
constexpr std::size_t kSlotsPerBucket = 16;
#else
constexpr std::size_t kCacheLineBytes = 64;
constexpr std::size_t kSlotsPerBucket = 8;
#endif
constexpr std::uint32_t kEmptyTag = 0;
constexpr std::uint32_t kTombstoneTag = 1;

struct alignas(kCacheLineBytes) Bucket {
    std::uint32_t tags[kSlotsPerBucket];
    std::uint32_t entry_ids[kSlotsPerBucket];
};

static_assert(sizeof(Bucket) == kCacheLineBytes, "bucket must occupy one target cache line");
static_assert(alignof(Bucket) == kCacheLineBytes, "bucket alignment must match target cache line");

struct KernelOps {
    KernelKind kind;
    const char* name;
    std::uint32_t (*fingerprint)(
            std::uint32_t state_id, const std::uint8_t* data, std::size_t size) noexcept;
    std::uint16_t (*match_tags)(const Bucket& bucket, std::uint32_t tag) noexcept;
    bool (*equal_bytes)(
            const std::uint8_t* left,
            const std::uint8_t* right,
            std::size_t size) noexcept;
};

const KernelOps& ScalarKernel() noexcept;

#if defined(CACHEKIT_NATIVE_AARCH64_KERNELS)
const KernelOps& NeonCrcKernel() noexcept;
const KernelOps& Sve256Kernel() noexcept;
#endif
#if defined(CACHEKIT_NATIVE_X86_KERNELS)
const KernelOps& Sse42CrcKernel() noexcept;
#endif

const KernelOps* SelectKernel(
        KernelPreference preference,
        const HostFeatures& features,
        ErrorCode* error,
        const char** message) noexcept;

}  // namespace internal
}  // namespace native
}  // namespace cachekit

#endif  // CACHEKIT_NATIVE_KERNELS_INTERNAL_H_
