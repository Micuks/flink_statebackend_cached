/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the
 * Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License.
 */

#include "kernels_internal.h"

#include <nmmintrin.h>
#include <smmintrin.h>

#include <cstring>

namespace cachekit {
namespace native {
namespace internal {
namespace {

std::uint32_t Sse42Fingerprint(
        std::uint32_t state_id, const std::uint8_t* data, std::size_t size) noexcept {
    std::uint32_t crc = _mm_crc32_u32(0xffffffffU, state_id);
    std::size_t index = 0;
#if defined(__x86_64__)
    std::uint64_t wide_crc = crc;
    while (index + sizeof(std::uint64_t) <= size) {
        std::uint64_t word = 0;
        std::memcpy(&word, data + index, sizeof(word));
        wide_crc = _mm_crc32_u64(wide_crc, word);
        index += sizeof(word);
    }
    crc = static_cast<std::uint32_t>(wide_crc);
#endif
    while (index < size) {
        crc = _mm_crc32_u8(crc, data[index]);
        ++index;
    }
    return ~crc;
}

std::uint16_t Sse42MatchTags(const Bucket& bucket, std::uint32_t tag) noexcept {
    const __m128i expected = _mm_set1_epi32(static_cast<int>(tag));
    std::uint16_t mask = 0;
    for (std::size_t base = 0; base < kSlotsPerBucket; base += 4U) {
        const __m128i values = _mm_load_si128(
                reinterpret_cast<const __m128i*>(bucket.tags + base));
        const int bytes = _mm_movemask_epi8(_mm_cmpeq_epi32(values, expected));
        for (std::size_t lane = 0; lane < 4U; ++lane) {
            if ((bytes & (0xf << (lane * 4U))) != 0) {
                mask |= static_cast<std::uint16_t>(1U << (base + lane));
            }
        }
    }
    return mask;
}

bool Sse42EqualBytes(
        const std::uint8_t* left,
        const std::uint8_t* right,
        std::size_t size) noexcept {
    std::size_t index = 0;
    while (index + 16U <= size) {
        const __m128i left_bytes = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(left + index));
        const __m128i right_bytes = _mm_loadu_si128(
                reinterpret_cast<const __m128i*>(right + index));
        if (_mm_movemask_epi8(_mm_cmpeq_epi8(left_bytes, right_bytes)) != 0xffff) {
            return false;
        }
        index += 16U;
    }
    return index == size || std::memcmp(left + index, right + index, size - index) == 0;
}

const KernelOps kSse42Ops = {
        KernelKind::kSse42Crc,
        "x86-sse4.2-crc32c",
        &Sse42Fingerprint,
        &Sse42MatchTags,
        &Sse42EqualBytes};

}  // namespace

const KernelOps& Sse42CrcKernel() noexcept {
    return kSse42Ops;
}

}  // namespace internal
}  // namespace native
}  // namespace cachekit
