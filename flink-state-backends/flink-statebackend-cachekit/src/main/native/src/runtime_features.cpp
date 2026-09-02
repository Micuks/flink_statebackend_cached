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

#if defined(__aarch64__) && defined(CACHEKIT_NATIVE_AARCH64_KERNELS)
#include <asm/hwcap.h>
#include <sys/auxv.h>
#include <sys/prctl.h>

#if __has_include(<linux/prctl.h>)
#include <linux/prctl.h>
#endif
#endif
#if defined(__x86_64__) && defined(CACHEKIT_NATIVE_X86_KERNELS)
#include <cpuid.h>
#endif

namespace cachekit {
namespace native {

HostFeatures DetectHostFeatures() noexcept {
    HostFeatures features;
#if defined(__aarch64__) && defined(CACHEKIT_NATIVE_AARCH64_KERNELS)
    features.aarch64 = true;
    const unsigned long hwcap = getauxval(AT_HWCAP);
#if defined(HWCAP_ASIMD)
    features.neon = (hwcap & HWCAP_ASIMD) != 0;
#endif
#if defined(HWCAP_CRC32)
    features.crc32 = (hwcap & HWCAP_CRC32) != 0;
#endif
#if defined(HWCAP_SVE)
    features.sve = (hwcap & HWCAP_SVE) != 0;
#endif
#if defined(PR_SVE_GET_VL) && defined(PR_SVE_VL_LEN_MASK)
    if (features.sve) {
        const int vector_length = prctl(PR_SVE_GET_VL);
        if (vector_length >= 0) {
            features.sve_vector_length_256 =
                    (vector_length & PR_SVE_VL_LEN_MASK) == 32;
        }
    }
#elif defined(PR_SVE_GET_VL) && defined(PR_SVE_VL_LEN)
    if (features.sve) {
        const int vector_length = prctl(PR_SVE_GET_VL);
        if (vector_length >= 0) {
            features.sve_vector_length_256 = (vector_length & PR_SVE_VL_LEN) == 32;
        }
    }
#endif
#endif
#if defined(__x86_64__) && defined(CACHEKIT_NATIVE_X86_KERNELS)
    features.x86_64 = true;
    unsigned int eax = 0;
    unsigned int ebx = 0;
    unsigned int ecx = 0;
    unsigned int edx = 0;
    if (__get_cpuid(1U, &eax, &ebx, &ecx, &edx) != 0) {
        features.sse42 = (ecx & bit_SSE4_2) != 0;
    }
#endif
    return features;
}

namespace internal {

const KernelOps* SelectKernel(
        KernelPreference preference,
        const HostFeatures& features,
        ErrorCode* error,
        const char** message) noexcept {
#if !defined(CACHEKIT_NATIVE_AARCH64_KERNELS)
    (void) features;
#endif
    if (error != nullptr) {
        *error = ErrorCode::kOk;
    }
    if (message != nullptr) {
        *message = "";
    }

    switch (preference) {
        case KernelPreference::kScalar:
            return &ScalarKernel();
        case KernelPreference::kAuto:
#if defined(CACHEKIT_NATIVE_X86_KERNELS)
            if (features.x86_64 && features.sse42) {
                return &Sse42CrcKernel();
            }
#endif
#if defined(CACHEKIT_NATIVE_AARCH64_KERNELS)
            if (features.sve && features.crc32 && features.sve_vector_length_256) {
                return &Sve256Kernel();
            }
            if (features.neon && features.crc32) {
                return &NeonCrcKernel();
            }
#endif
            return &ScalarKernel();
        case KernelPreference::kNeonCrc:
#if defined(CACHEKIT_NATIVE_AARCH64_KERNELS)
            if (features.neon && features.crc32) {
                return &NeonCrcKernel();
            }
#endif
            break;
        case KernelPreference::kSve256:
#if defined(CACHEKIT_NATIVE_AARCH64_KERNELS)
            if (features.sve && features.crc32 && features.sve_vector_length_256) {
                return &Sve256Kernel();
            }
#endif
            break;
    }

    if (error != nullptr) {
        *error = ErrorCode::kUnsupportedKernel;
    }
    if (message != nullptr) {
        *message = "requested ISA kernel is unavailable on this host";
    }
    return nullptr;
}

}  // namespace internal
}  // namespace native
}  // namespace cachekit
