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

#include <jni.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <utility>

namespace {

using cachekit::native::ErrorCode;
using cachekit::native::HostFeatures;
using cachekit::native::KernelPreference;
using cachekit::native::Options;
using cachekit::native::RequestPlane;
using cachekit::native::bridge::BatchBridgeCode;
using cachekit::native::bridge::BatchBridgeCodeName;
using cachekit::native::bridge::BatchScratch;
using cachekit::native::bridge::ConstBuffer;
using cachekit::native::bridge::FillDirectBatch;
using cachekit::native::bridge::MutableBuffer;
using cachekit::native::bridge::ProbeDirectBatch;

constexpr const char* kIllegalArgument = "java/lang/IllegalArgumentException";
constexpr const char* kIllegalState = "java/lang/IllegalStateException";
constexpr const char* kOutOfMemory = "java/lang/OutOfMemoryError";

struct BridgeHandle final {
    BridgeHandle(
            std::unique_ptr<RequestPlane> request_plane,
            std::size_t reserve_entries)
        : plane(std::move(request_plane)), scratch(reserve_entries) {}

    std::unique_ptr<RequestPlane> plane;
    BatchScratch scratch;
};

void Throw(JNIEnv* environment, const char* class_name, const std::string& message) {
    jclass exception_class = environment->FindClass(class_name);
    if (exception_class != nullptr) {
        environment->ThrowNew(exception_class, message.c_str());
    }
}

BridgeHandle* FromHandle(jlong handle) noexcept {
    return reinterpret_cast<BridgeHandle*>(static_cast<std::uintptr_t>(handle));
}

jlong ToHandle(BridgeHandle* bridge) noexcept {
    return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(bridge));
}

bool GetConstBuffer(
        JNIEnv* environment, jobject object, const char* name, ConstBuffer* result) {
    if (object == nullptr) {
        Throw(environment, kIllegalArgument, std::string(name) + " is null");
        return false;
    }
    const jlong capacity = environment->GetDirectBufferCapacity(object);
    void* address = environment->GetDirectBufferAddress(object);
    if (capacity < 0 || (capacity != 0 && address == nullptr)) {
        Throw(
                environment,
                kIllegalArgument,
                std::string(name) + " must be a direct ByteBuffer");
        return false;
    }
    if (static_cast<unsigned long long>(capacity) >
        std::numeric_limits<std::size_t>::max()) {
        Throw(environment, kIllegalArgument, std::string(name) + " is too large");
        return false;
    }
    *result = ConstBuffer{
            static_cast<const std::uint8_t*>(address),
            static_cast<std::size_t>(capacity)};
    return true;
}

bool GetMutableBuffer(
        JNIEnv* environment, jobject object, const char* name, MutableBuffer* result) {
    ConstBuffer input;
    if (!GetConstBuffer(environment, object, name, &input)) {
        return false;
    }
    *result = MutableBuffer{
            const_cast<std::uint8_t*>(input.data),
            input.size};
    return true;
}

bool ValidateCall(
        JNIEnv* environment,
        jlong handle,
        jint count,
        BridgeHandle** bridge,
        std::size_t* unsigned_count) {
    *bridge = FromHandle(handle);
    if (*bridge == nullptr) {
        Throw(environment, kIllegalState, "native request plane is closed");
        return false;
    }
    if (count < 0) {
        Throw(environment, kIllegalArgument, "entry count must not be negative");
        return false;
    }
    *unsigned_count = static_cast<std::size_t>(count);
    return true;
}

void ThrowBridgeFailure(JNIEnv* environment, BatchBridgeCode code) {
    const char* exception_class = kIllegalArgument;
    if (code == BatchBridgeCode::kAllocationFailed) {
        exception_class = kOutOfMemory;
    } else if (code == BatchBridgeCode::kNativeError) {
        exception_class = kIllegalState;
    }
    Throw(
            environment,
            exception_class,
            std::string("native batch bridge rejected request: ") +
                    BatchBridgeCodeName(code));
}

}  // namespace

extern "C" {

JNIEXPORT jlong JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_nativeplane_NativeRequestPlaneBridge_nativeCreate(
        JNIEnv* environment,
        jclass,
        jint capacity_entries,
        jlong key_arena_bytes,
        jlong value_arena_bytes,
        jint kernel_preference) {
    try {
        if (capacity_entries <= 0 || key_arena_bytes < 0 || value_arena_bytes < 0 ||
            kernel_preference <
                    static_cast<jint>(KernelPreference::kAuto) ||
            kernel_preference >
                    static_cast<jint>(KernelPreference::kSve256)) {
            Throw(environment, kIllegalArgument, "invalid native request-plane options");
            return 0;
        }
        if (static_cast<unsigned long long>(key_arena_bytes) >
                    std::numeric_limits<std::size_t>::max() ||
            static_cast<unsigned long long>(value_arena_bytes) >
                    std::numeric_limits<std::size_t>::max()) {
            Throw(environment, kIllegalArgument, "native arena size is too large");
            return 0;
        }
        Options options;
        options.capacity_entries = static_cast<std::size_t>(capacity_entries);
        options.key_arena_bytes = static_cast<std::size_t>(key_arena_bytes);
        options.value_arena_bytes = static_cast<std::size_t>(value_arena_bytes);
        options.kernel = static_cast<KernelPreference>(kernel_preference);

        ErrorCode error = ErrorCode::kInternal;
        std::string message;
        std::unique_ptr<RequestPlane> plane =
                RequestPlane::Create(options, &error, &message);
        if (!plane) {
            Throw(
                    environment,
                    error == ErrorCode::kAllocationFailed ? kOutOfMemory
                                                          : kIllegalState,
                    std::string("cannot create native request plane: ") +
                            cachekit::native::ErrorCodeName(error) + ":" + message);
            return 0;
        }
        std::unique_ptr<BridgeHandle> bridge =
                std::make_unique<BridgeHandle>(
                        std::move(plane), options.capacity_entries);
        return ToHandle(bridge.release());
    } catch (const std::bad_alloc&) {
        Throw(environment, kOutOfMemory, "native request-plane creation allocation failed");
    } catch (const std::exception& exception) {
        Throw(environment, kIllegalState, exception.what());
    } catch (...) {
        Throw(environment, kIllegalState, "unknown native request-plane creation failure");
    }
    return 0;
}

JNIEXPORT void JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_nativeplane_NativeRequestPlaneBridge_nativeDestroy(
        JNIEnv*, jclass, jlong handle) {
    delete FromHandle(handle);
}

JNIEXPORT jint JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_nativeplane_NativeRequestPlaneBridge_nativeFill(
        JNIEnv* environment,
        jclass,
        jlong handle,
        jobject key_arena_object,
        jobject key_metadata_object,
        jint count,
        jobject value_arena_object,
        jobject value_metadata_object,
        jobject fill_results_object) {
    try {
        BridgeHandle* bridge = nullptr;
        std::size_t unsigned_count = 0;
        ConstBuffer key_arena;
        ConstBuffer key_metadata;
        ConstBuffer value_arena;
        ConstBuffer value_metadata;
        MutableBuffer fill_results;
        if (!ValidateCall(
                    environment, handle, count, &bridge, &unsigned_count) ||
            !GetConstBuffer(
                    environment, key_arena_object, "keyArena", &key_arena) ||
            !GetConstBuffer(
                    environment, key_metadata_object, "keyMetadata", &key_metadata) ||
            !GetConstBuffer(
                    environment, value_arena_object, "valueArena", &value_arena) ||
            !GetConstBuffer(
                    environment,
                    value_metadata_object,
                    "valueMetadata",
                    &value_metadata) ||
            !GetMutableBuffer(
                    environment, fill_results_object, "fillResults", &fill_results)) {
            return -1;
        }
        const BatchBridgeCode code = FillDirectBatch(
                bridge->plane.get(),
                &bridge->scratch,
                key_arena,
                key_metadata,
                value_arena,
                value_metadata,
                unsigned_count,
                fill_results);
        if (code != BatchBridgeCode::kOk) {
            ThrowBridgeFailure(environment, code);
            return -1;
        }
        return count;
    } catch (const std::bad_alloc&) {
        Throw(environment, kOutOfMemory, "JNI fill allocation failed");
    } catch (const std::exception& exception) {
        Throw(environment, kIllegalState, exception.what());
    } catch (...) {
        Throw(environment, kIllegalState, "unknown JNI fill failure");
    }
    return -1;
}

JNIEXPORT jint JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_nativeplane_NativeRequestPlaneBridge_nativeProbe(
        JNIEnv* environment,
        jclass,
        jlong handle,
        jobject key_arena_object,
        jobject key_metadata_object,
        jint count,
        jobject value_output_object,
        jobject probe_results_object) {
    try {
        BridgeHandle* bridge = nullptr;
        std::size_t unsigned_count = 0;
        ConstBuffer key_arena;
        ConstBuffer key_metadata;
        MutableBuffer value_output;
        MutableBuffer probe_results;
        if (!ValidateCall(
                    environment, handle, count, &bridge, &unsigned_count) ||
            !GetConstBuffer(
                    environment, key_arena_object, "keyArena", &key_arena) ||
            !GetConstBuffer(
                    environment, key_metadata_object, "keyMetadata", &key_metadata) ||
            !GetMutableBuffer(
                    environment, value_output_object, "valueOutput", &value_output) ||
            !GetMutableBuffer(
                    environment,
                    probe_results_object,
                    "probeResults",
                    &probe_results)) {
            return -1;
        }
        const BatchBridgeCode code = ProbeDirectBatch(
                bridge->plane.get(),
                &bridge->scratch,
                key_arena,
                key_metadata,
                unsigned_count,
                value_output,
                probe_results);
        if (code != BatchBridgeCode::kOk) {
            ThrowBridgeFailure(environment, code);
            return -1;
        }
        return count;
    } catch (const std::bad_alloc&) {
        Throw(environment, kOutOfMemory, "JNI probe allocation failed");
    } catch (const std::exception& exception) {
        Throw(environment, kIllegalState, exception.what());
    } catch (...) {
        Throw(environment, kIllegalState, "unknown JNI probe failure");
    }
    return -1;
}

JNIEXPORT jstring JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_nativeplane_NativeRequestPlaneBridge_nativeKernelName(
        JNIEnv* environment, jclass, jlong handle) {
    BridgeHandle* bridge = FromHandle(handle);
    if (bridge == nullptr) {
        Throw(environment, kIllegalState, "native request plane is closed");
        return nullptr;
    }
    return environment->NewStringUTF(bridge->plane->kernel_name());
}

JNIEXPORT jlong JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_nativeplane_NativeRequestPlaneBridge_nativeFeatureBits(
        JNIEnv* environment, jclass, jlong handle) {
    BridgeHandle* bridge = FromHandle(handle);
    if (bridge == nullptr) {
        Throw(environment, kIllegalState, "native request plane is closed");
        return 0;
    }
    const HostFeatures features = bridge->plane->host_features();
    std::uint64_t bits = 0;
    bits |= static_cast<std::uint64_t>(features.aarch64) << 0U;
    bits |= static_cast<std::uint64_t>(features.neon) << 1U;
    bits |= static_cast<std::uint64_t>(features.crc32) << 2U;
    bits |= static_cast<std::uint64_t>(features.sve) << 3U;
    bits |= static_cast<std::uint64_t>(features.sve_vector_length_256) << 4U;
    return static_cast<jlong>(bits);
}

}  // extern "C"
