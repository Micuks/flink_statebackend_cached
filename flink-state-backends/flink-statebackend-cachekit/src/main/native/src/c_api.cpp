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

#include "cachekit/native_request_plane_c.h"

#include "cachekit/native_request_plane.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <new>
#include <string>

using cachekit::native::ErrorCode;
using cachekit::native::FillResult;
using cachekit::native::FillView;
using cachekit::native::HostFeatures;
using cachekit::native::KeyView;
using cachekit::native::Options;
using cachekit::native::ProbeResult;
using cachekit::native::RequestPlane;

struct cachekit_native_plane {
    std::unique_ptr<RequestPlane> implementation;
};

namespace {

cachekit_native_error ToCError(ErrorCode error) noexcept {
    return static_cast<cachekit_native_error>(error);
}

void CopyMessage(
        const std::string& message, char* destination, std::size_t capacity) noexcept {
    if (destination == nullptr || capacity == 0) {
        return;
    }
    const std::size_t count = std::min(capacity - 1U, message.size());
    if (count != 0) {
        std::memcpy(destination, message.data(), count);
    }
    destination[count] = '\0';
}

Options ToCppOptions(const cachekit_native_options& input) {
    Options output;
    output.capacity_entries = input.capacity_entries;
    output.key_arena_bytes = input.key_arena_bytes;
    output.value_arena_bytes = input.value_arena_bytes;
    output.min_native_batch_size = input.min_native_batch_size;
    output.kernel =
            static_cast<cachekit::native::KernelPreference>(input.kernel);
    output.fingerprint_mask = input.fingerprint_mask;
    return output;
}

KeyView ToCppKey(const cachekit_native_key_view& input) noexcept {
    return KeyView{
            input.state_id,
            input.generation,
            input.data,
            input.size};
}

}  // namespace

extern "C" {

void cachekit_native_options_init(cachekit_native_options* options) {
    if (options == nullptr) {
        return;
    }
    const Options defaults;
    options->capacity_entries = defaults.capacity_entries;
    options->key_arena_bytes = defaults.key_arena_bytes;
    options->value_arena_bytes = defaults.value_arena_bytes;
    options->min_native_batch_size = defaults.min_native_batch_size;
    options->kernel = CACHEKIT_NATIVE_KERNEL_AUTO;
    options->fingerprint_mask = defaults.fingerprint_mask;
}

cachekit_native_plane* cachekit_native_plane_create(
        const cachekit_native_options* options,
        cachekit_native_error* error,
        char* error_message,
        size_t error_message_capacity) {
    if (error != nullptr) {
        *error = CACHEKIT_NATIVE_OK;
    }
    CopyMessage("", error_message, error_message_capacity);

    try {
        Options cpp_options;
        if (options != nullptr) {
            cpp_options = ToCppOptions(*options);
        }
        ErrorCode cpp_error = ErrorCode::kOk;
        std::string message;
        std::unique_ptr<RequestPlane> implementation =
                RequestPlane::Create(cpp_options, &cpp_error, &message);
        if (!implementation) {
            if (error != nullptr) {
                *error = ToCError(cpp_error);
            }
            CopyMessage(message, error_message, error_message_capacity);
            return nullptr;
        }
        std::unique_ptr<cachekit_native_plane> plane(new cachekit_native_plane);
        plane->implementation = std::move(implementation);
        return plane.release();
    } catch (const std::bad_alloc&) {
        if (error != nullptr) {
            *error = CACHEKIT_NATIVE_ALLOCATION_FAILED;
        }
        CopyMessage(
                "C ABI batch allocation failed",
                error_message,
                error_message_capacity);
    } catch (...) {
        if (error != nullptr) {
            *error = CACHEKIT_NATIVE_INTERNAL;
        }
        CopyMessage(
                "C ABI request-plane creation failed",
                error_message,
                error_message_capacity);
    }
    return nullptr;
}

void cachekit_native_plane_destroy(cachekit_native_plane* plane) {
    delete plane;
}

cachekit_native_error cachekit_native_probe_batch(
        cachekit_native_plane* plane,
        const cachekit_native_key_view* keys,
        cachekit_native_probe_result* results,
        size_t count) {
    if (plane == nullptr || (count != 0 && (keys == nullptr || results == nullptr))) {
        return CACHEKIT_NATIVE_INVALID_ARGUMENT;
    }
    try {
        for (std::size_t index = 0; index < count; ++index) {
            const KeyView cpp_key = ToCppKey(keys[index]);
            ProbeResult cpp_result;
            const ErrorCode batch_error =
                    plane->implementation->ProbeBatch(&cpp_key, &cpp_result, 1);
            if (batch_error != ErrorCode::kOk) {
                return ToCError(batch_error);
            }
            results[index].status =
                    static_cast<cachekit_native_probe_status>(
                            cpp_result.status);
            results[index].error = ToCError(cpp_result.error);
            results[index].value = cpp_result.value;
            results[index].value_size = cpp_result.value_size;
        }
        return CACHEKIT_NATIVE_OK;
    } catch (const std::bad_alloc&) {
        return CACHEKIT_NATIVE_ALLOCATION_FAILED;
    } catch (...) {
        return CACHEKIT_NATIVE_INTERNAL;
    }
}

cachekit_native_error cachekit_native_fill_batch(
        cachekit_native_plane* plane,
        const cachekit_native_fill_view* fills,
        cachekit_native_fill_result* results,
        size_t count) {
    if (plane == nullptr ||
        (count != 0 && (fills == nullptr || results == nullptr))) {
        return CACHEKIT_NATIVE_INVALID_ARGUMENT;
    }
    try {
        for (std::size_t index = 0; index < count; ++index) {
            const FillView cpp_fill{
                    ToCppKey(fills[index].key),
                    fills[index].value,
                    fills[index].value_size,
                    fills[index].negative != 0};
            FillResult cpp_result;
            const ErrorCode batch_error =
                    plane->implementation->FillBatch(&cpp_fill, &cpp_result, 1);
            if (batch_error != ErrorCode::kOk) {
                return ToCError(batch_error);
            }
            results[index].status =
                    static_cast<cachekit_native_fill_status>(
                            cpp_result.status);
            results[index].error = ToCError(cpp_result.error);
        }
        return CACHEKIT_NATIVE_OK;
    } catch (const std::bad_alloc&) {
        return CACHEKIT_NATIVE_ALLOCATION_FAILED;
    } catch (...) {
        return CACHEKIT_NATIVE_INTERNAL;
    }
}

void cachekit_native_plane_clear(cachekit_native_plane* plane) {
    if (plane != nullptr) {
        plane->implementation->Clear();
    }
}

size_t cachekit_native_plane_size(const cachekit_native_plane* plane) {
    return plane == nullptr ? 0 : plane->implementation->size();
}

size_t cachekit_native_plane_capacity(const cachekit_native_plane* plane) {
    return plane == nullptr ? 0 : plane->implementation->capacity();
}

uint64_t cachekit_native_plane_evictions(const cachekit_native_plane* plane) {
    return plane == nullptr ? 0 : plane->implementation->evictions();
}

uint32_t cachekit_native_plane_min_batch_size(const cachekit_native_plane* plane) {
    return plane == nullptr ? 0 : plane->implementation->min_native_batch_size();
}

uint32_t cachekit_native_plane_kernel_kind(const cachekit_native_plane* plane) {
    return plane == nullptr
            ? 0
            : static_cast<std::uint32_t>(plane->implementation->kernel_kind());
}

const char* cachekit_native_plane_kernel_name(const cachekit_native_plane* plane) {
    return plane == nullptr ? "invalid" : plane->implementation->kernel_name();
}

cachekit_native_host_features cachekit_native_detect_host_features(void) {
    const HostFeatures features = cachekit::native::DetectHostFeatures();
    return cachekit_native_host_features{
            static_cast<std::uint8_t>(features.aarch64),
            static_cast<std::uint8_t>(features.neon),
            static_cast<std::uint8_t>(features.crc32),
            static_cast<std::uint8_t>(features.sve),
            static_cast<std::uint8_t>(features.sve_vector_length_256)};
}

const char* cachekit_native_error_name(cachekit_native_error error) {
    return cachekit::native::ErrorCodeName(static_cast<ErrorCode>(error));
}

}  // extern "C"
