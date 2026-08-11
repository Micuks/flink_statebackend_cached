#include "cachekit_snapshot_table.h"
#include "cachekit_byte_snapshot_table.h"
#include "rocks_jni_bridge.h"

#include <jni.h>

#include <cstring>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace {

using cachekit::ProbeKernel;
using cachekit::SnapshotKind;
using cachekit::SnapshotTable;
using cachekit::ByteSnapshotTable;

SnapshotTable* Table(jlong handle) {
    return reinterpret_cast<SnapshotTable*>(static_cast<std::uintptr_t>(handle));
}

jlong Handle(SnapshotTable* table) {
    return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(table));
}

ProbeKernel Kernel(jint value) {
    switch (value) {
        case 0:
            return ProbeKernel::kScalar;
        case 1:
            return ProbeKernel::kNeon;
        case 2:
            return ProbeKernel::kSve;
        case 3:
            return ProbeKernel::kAuto;
        default:
            throw std::invalid_argument("invalid kernel id");
    }
}

std::uint64_t Pack(const cachekit::LookupResult& result) {
    if (!result.found) {
        return 0;
    }
    return (static_cast<std::uint64_t>(result.entry_id) << 8)
            | static_cast<std::uint8_t>(result.kind);
}

void ThrowIllegalArgument(JNIEnv* env, const char* message) {
    jclass type = env->FindClass("java/lang/IllegalArgumentException");
    if (type != nullptr) {
        env->ThrowNew(type, message);
    }
}

void ThrowIllegalState(JNIEnv* env, const char* message) {
    jclass type = env->FindClass("java/lang/IllegalStateException");
    if (type != nullptr) {
        env->ThrowNew(type, message);
    }
}

class JniByteSnapshotCache {
public:
    JniByteSnapshotCache(
            JNIEnv* env,
            std::size_t max_entries,
            ProbeKernel kernel,
            jobject miss_sentinel,
            jobject empty_sentinel,
            jobject multi_sentinel,
            bool classifier_enabled)
            : table_(max_entries, kernel),
              miss_sentinel_(env->NewGlobalRef(miss_sentinel)),
              empty_sentinel_(env->NewGlobalRef(empty_sentinel)),
              multi_sentinel_(env->NewGlobalRef(multi_sentinel)),
              rocks_bridge_(nullptr) {
        if (miss_sentinel_ == nullptr || empty_sentinel_ == nullptr || multi_sentinel_ == nullptr) {
            if (miss_sentinel_ != nullptr) {
                env->DeleteGlobalRef(miss_sentinel_);
            }
            if (empty_sentinel_ != nullptr) {
                env->DeleteGlobalRef(empty_sentinel_);
            }
            if (multi_sentinel_ != nullptr) {
                env->DeleteGlobalRef(multi_sentinel_);
            }
            throw std::runtime_error("failed to retain Native snapshot sentinels");
        }
        try {
            if (classifier_enabled) {
                rocks_bridge_ = cachekit::RocksJniBridge::Load();
            }
        } catch (...) {
            env->DeleteGlobalRef(miss_sentinel_);
            env->DeleteGlobalRef(empty_sentinel_);
            env->DeleteGlobalRef(multi_sentinel_);
            throw;
        }
    }

    cachekit::PutResult Put(
            JNIEnv* env,
            const std::uint8_t* key,
            std::size_t key_size,
            SnapshotKind kind,
            jobject user_key) {
        jobject retained_user_key = nullptr;
        std::vector<std::uint8_t> payload;
        if (kind == SnapshotKind::kSingle) {
            retained_user_key = env->NewGlobalRef(user_key);
            if (retained_user_key == nullptr) {
                return cachekit::PutResult::kRejected;
            }
            payload.resize(sizeof(jobject));
            std::memcpy(payload.data(), &retained_user_key, sizeof(jobject));
        }

        std::vector<std::uint8_t> displaced;
        cachekit::PutResult result = cachekit::PutResult::kRejected;
        try {
            result = table_.Put(
                    key,
                    key_size,
                    kind,
                    payload.empty() ? nullptr : payload.data(),
                    payload.size(),
                    &displaced);
        } catch (...) {
            if (retained_user_key != nullptr) {
                env->DeleteGlobalRef(retained_user_key);
            }
            DeletePayload(env, displaced);
            throw;
        }
        if (result == cachekit::PutResult::kRejected && retained_user_key != nullptr) {
            env->DeleteGlobalRef(retained_user_key);
        }
        DeletePayload(env, displaced);
        return result;
    }

    jobject Lookup(const std::uint8_t* key, std::size_t key_size) {
        const cachekit::ByteLookupResult result = table_.Lookup(key, key_size);
        if (!result.found) {
            return miss_sentinel_;
        }
        if (result.kind == SnapshotKind::kEmpty) {
            return empty_sentinel_;
        }
        return DecodePayload(result.payload, result.payload_size);
    }

    jobject Classify(
            JNIEnv* env,
            jlong db_handle,
            jlong column_family_handle,
            jlong read_options_handle,
            jbyteArray prefix,
            jint compare_offset) const {
        if (rocks_bridge_ == nullptr) {
            throw std::runtime_error("Native snapshot classifier is disabled");
        }
        cachekit::PrefixClassification result = rocks_bridge_->Classify(
                env,
                db_handle,
                column_family_handle,
                read_options_handle,
                prefix,
                compare_offset);
        if (env->ExceptionCheck()) {
            return nullptr;
        }
        if (result.kind == cachekit::PrefixKind::kEmpty) {
            return env->NewLocalRef(empty_sentinel_);
        }
        if (result.kind == cachekit::PrefixKind::kMulti) {
            return env->NewLocalRef(multi_sentinel_);
        }
        return result.single_key;
    }

    const char* bridge_description() const {
        return rocks_bridge_ == nullptr ? "disabled" : rocks_bridge_->description().c_str();
    }

    bool Remove(
            JNIEnv* env,
            const std::uint8_t* key,
            std::size_t key_size) {
        std::vector<std::uint8_t> removed;
        const bool found = table_.Remove(key, key_size, &removed);
        DeletePayload(env, removed);
        return found;
    }

    void Clear(JNIEnv* env) {
        std::vector<std::vector<std::uint8_t>> removed;
        table_.Clear(&removed);
        for (const std::vector<std::uint8_t>& payload : removed) {
            DeletePayload(env, payload);
        }
    }

    void Destroy(JNIEnv* env) {
        Clear(env);
        env->DeleteGlobalRef(miss_sentinel_);
        env->DeleteGlobalRef(empty_sentinel_);
        env->DeleteGlobalRef(multi_sentinel_);
        miss_sentinel_ = nullptr;
        empty_sentinel_ = nullptr;
        multi_sentinel_ = nullptr;
    }

    std::size_t size() const { return table_.size(); }
    const char* active_kernel_name() const { return table_.active_kernel_name(); }

private:
    static jobject DecodePayload(const std::uint8_t* payload, std::size_t payload_size) {
        if (payload == nullptr || payload_size != sizeof(jobject)) {
            throw std::runtime_error("invalid Native snapshot object payload");
        }
        jobject value = nullptr;
        std::memcpy(&value, payload, sizeof(jobject));
        return value;
    }

    static void DeletePayload(JNIEnv* env, const std::vector<std::uint8_t>& payload) {
        if (!payload.empty()) {
            env->DeleteGlobalRef(DecodePayload(payload.data(), payload.size()));
        }
    }

    ByteSnapshotTable table_;
    jobject miss_sentinel_;
    jobject empty_sentinel_;
    jobject multi_sentinel_;
    std::unique_ptr<cachekit::RocksJniBridge> rocks_bridge_;
};

JniByteSnapshotCache* ByteCache(jlong handle) {
    return reinterpret_cast<JniByteSnapshotCache*>(static_cast<std::uintptr_t>(handle));
}

jlong ByteHandle(JniByteSnapshotCache* cache) {
    return static_cast<jlong>(reinterpret_cast<std::uintptr_t>(cache));
}

}  // namespace

extern "C" JNIEXPORT jboolean JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_nativebench_NativeSnapshotBench_kernelSupported(
        JNIEnv*, jclass, jint kernel) {
    if (kernel == 0) {
        return JNI_TRUE;
    }
    if (kernel == 1) {
        return cachekit::NeonAvailable() ? JNI_TRUE : JNI_FALSE;
    }
    if (kernel == 2) {
        return cachekit::SveAvailable() && cachekit::SveVectorBytes() != 0 ? JNI_TRUE : JNI_FALSE;
    }
    return JNI_FALSE;
}

extern "C" JNIEXPORT jlong JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_nativebench_NativeSnapshotBench_create(
        JNIEnv* env, jclass, jint capacity, jint kernel) {
    try {
        return Handle(new SnapshotTable(static_cast<std::size_t>(capacity), Kernel(kernel)));
    } catch (const std::exception& error) {
        ThrowIllegalArgument(env, error.what());
        return 0;
    }
}

extern "C" JNIEXPORT void JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_nativebench_NativeSnapshotBench_destroy(
        JNIEnv*, jclass, jlong handle) {
    delete Table(handle);
}

extern "C" JNIEXPORT jboolean JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_nativebench_NativeSnapshotBench_put(
        JNIEnv*,
        jclass,
        jlong handle,
        jlong key,
        jlong name_space,
        jint kind,
        jint entry_id) {
    const SnapshotKind snapshot_kind =
            kind == 1 ? SnapshotKind::kEmpty : SnapshotKind::kSingle;
    return Table(handle)->Put(
                   static_cast<std::uint64_t>(key),
                   static_cast<std::uint64_t>(name_space),
                   snapshot_kind,
                   static_cast<std::uint32_t>(entry_id))
            ? JNI_TRUE
            : JNI_FALSE;
}

extern "C" JNIEXPORT jlong JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_nativebench_NativeSnapshotBench_lookup(
        JNIEnv*, jclass, jlong handle, jlong key, jlong name_space) {
    return static_cast<jlong>(Pack(Table(handle)->Lookup(
            static_cast<std::uint64_t>(key), static_cast<std::uint64_t>(name_space))));
}

extern "C" JNIEXPORT void JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_nativebench_NativeSnapshotBench_lookupBatch(
        JNIEnv* env,
        jclass,
        jlong handle,
        jobject input,
        jobject output,
        jint start,
        jint count) {
    auto* queries = static_cast<std::uint64_t*>(env->GetDirectBufferAddress(input));
    auto* results = static_cast<std::uint64_t*>(env->GetDirectBufferAddress(output));
    if (queries == nullptr || results == nullptr || start < 0 || count < 0) {
        ThrowIllegalArgument(env, "batch buffers must be direct and ranges non-negative");
        return;
    }
    SnapshotTable* table = Table(handle);
    for (jint index = start; index < start + count; ++index) {
        results[index] = Pack(table->Lookup(queries[index * 2], queries[index * 2 + 1]));
    }
}

extern "C" JNIEXPORT jstring JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_nativebench_NativeSnapshotBench_kernelName(
        JNIEnv* env, jclass, jlong handle) {
    return env->NewStringUTF(Table(handle)->active_kernel_name());
}

extern "C" JNIEXPORT jint JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_nativebench_NativeSnapshotBench_vectorBytes(
        JNIEnv*, jclass, jlong handle) {
    return static_cast<jint>(Table(handle)->vector_bytes());
}

extern "C" JNIEXPORT jlong JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_state_NativeMapSnapshotCache_nativeCreate(
        JNIEnv* env,
        jclass,
        jint max_entries,
        jint kernel,
        jobject miss_sentinel,
        jobject empty_sentinel,
        jobject multi_sentinel,
        jboolean classifier_enabled) {
    try {
        if (max_entries <= 0 || miss_sentinel == nullptr || empty_sentinel == nullptr
                || multi_sentinel == nullptr) {
            throw std::invalid_argument("invalid Native snapshot creation arguments");
        }
        return ByteHandle(new JniByteSnapshotCache(
                env,
                static_cast<std::size_t>(max_entries),
                Kernel(kernel),
                miss_sentinel,
                empty_sentinel,
                multi_sentinel,
                classifier_enabled == JNI_TRUE));
    } catch (const std::exception& error) {
        if (!env->ExceptionCheck()) {
            ThrowIllegalArgument(env, error.what());
        }
        return 0;
    }
}

extern "C" JNIEXPORT void JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_state_NativeMapSnapshotCache_nativeDestroy(
        JNIEnv* env, jclass, jlong handle) {
    if (handle != 0) {
        ByteCache(handle)->Destroy(env);
        delete ByteCache(handle);
    }
}

extern "C" JNIEXPORT jint JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_state_NativeMapSnapshotCache_nativePut(
        JNIEnv* env,
        jclass,
        jlong handle,
        jbyteArray key,
        jint key_offset,
        jint key_size,
        jint kind,
        jobject user_key) {
    const jsize key_capacity = key == nullptr ? 0 : env->GetArrayLength(key);
    if (handle == 0 || key == nullptr || key_offset < 0 || key_size <= 0
            || key_offset > key_capacity || key_size > key_capacity - key_offset
            || (kind != 1 && kind != 2)
            || (kind == 2 && user_key == nullptr)) {
        ThrowIllegalArgument(env, "invalid native snapshot put arguments");
        return 0;
    }
    jbyte* key_bytes = env->GetByteArrayElements(key, nullptr);
    if (key_bytes == nullptr) {
        return 0;
    }
    const SnapshotKind snapshot_kind =
            kind == 1 ? SnapshotKind::kEmpty : SnapshotKind::kSingle;
    cachekit::PutResult result = cachekit::PutResult::kRejected;
    try {
        result = ByteCache(handle)->Put(
                env,
                reinterpret_cast<const std::uint8_t*>(key_bytes + key_offset),
                static_cast<std::size_t>(key_size),
                snapshot_kind,
                user_key);
    } catch (const std::exception& error) {
        if (!env->ExceptionCheck()) {
            ThrowIllegalState(env, error.what());
        }
    }
    env->ReleaseByteArrayElements(key, key_bytes, JNI_ABORT);
    return static_cast<jint>(result);
}

extern "C" JNIEXPORT jobject JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_state_NativeMapSnapshotCache_nativeLookup(
        JNIEnv* env,
        jclass,
        jlong handle,
        jbyteArray key,
        jint key_offset,
        jint key_size) {
    const jsize key_capacity = key == nullptr ? 0 : env->GetArrayLength(key);
    if (handle == 0 || key == nullptr || key_offset < 0 || key_size <= 0
            || key_offset > key_capacity || key_size > key_capacity - key_offset) {
        ThrowIllegalArgument(env, "invalid native snapshot lookup arguments");
        return nullptr;
    }
    jbyte* key_bytes = static_cast<jbyte*>(env->GetPrimitiveArrayCritical(key, nullptr));
    if (key_bytes == nullptr) {
        return nullptr;
    }
    jobject retained_result = nullptr;
    try {
        retained_result = ByteCache(handle)->Lookup(
                reinterpret_cast<const std::uint8_t*>(key_bytes + key_offset),
                static_cast<std::size_t>(key_size));
    } catch (const std::exception& error) {
        env->ReleasePrimitiveArrayCritical(key, key_bytes, JNI_ABORT);
        if (!env->ExceptionCheck()) {
            ThrowIllegalState(env, error.what());
        }
        return nullptr;
    }
    env->ReleasePrimitiveArrayCritical(key, key_bytes, JNI_ABORT);
    return env->NewLocalRef(retained_result);
}

extern "C" JNIEXPORT jboolean JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_state_NativeMapSnapshotCache_nativeRemove(
        JNIEnv* env,
        jclass,
        jlong handle,
        jbyteArray key,
        jint key_offset,
        jint key_size) {
    const jsize key_capacity = key == nullptr ? 0 : env->GetArrayLength(key);
    if (handle == 0 || key == nullptr || key_offset < 0 || key_size <= 0
            || key_offset > key_capacity || key_size > key_capacity - key_offset) {
        ThrowIllegalArgument(env, "invalid native snapshot remove arguments");
        return JNI_FALSE;
    }
    jbyte* key_bytes = env->GetByteArrayElements(key, nullptr);
    if (key_bytes == nullptr) {
        return JNI_FALSE;
    }
    const bool removed = ByteCache(handle)->Remove(
            env,
            reinterpret_cast<const std::uint8_t*>(key_bytes + key_offset),
            static_cast<std::size_t>(key_size));
    env->ReleaseByteArrayElements(key, key_bytes, JNI_ABORT);
    return removed ? JNI_TRUE : JNI_FALSE;
}

extern "C" JNIEXPORT void JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_state_NativeMapSnapshotCache_nativeClear(
        JNIEnv* env, jclass, jlong handle) {
    if (handle == 0) {
        ThrowIllegalState(env, "native snapshot cache is closed");
        return;
    }
    try {
        ByteCache(handle)->Clear(env);
    } catch (const std::exception& error) {
        ThrowIllegalState(env, error.what());
    }
}

extern "C" JNIEXPORT jint JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_state_NativeMapSnapshotCache_nativeSize(
        JNIEnv* env, jclass, jlong handle) {
    if (handle == 0) {
        ThrowIllegalState(env, "native snapshot cache is closed");
        return 0;
    }
    return static_cast<jint>(ByteCache(handle)->size());
}

extern "C" JNIEXPORT jstring JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_state_NativeMapSnapshotCache_nativeKernelName(
        JNIEnv* env, jclass, jlong handle) {
    if (handle == 0) {
        ThrowIllegalState(env, "native snapshot cache is closed");
        return nullptr;
    }
    return env->NewStringUTF(ByteCache(handle)->active_kernel_name());
}

extern "C" JNIEXPORT jstring JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_state_NativeMapSnapshotCache_nativeBridgeDescription(
        JNIEnv* env, jclass, jlong handle) {
    if (handle == 0) {
        ThrowIllegalState(env, "native snapshot cache is closed");
        return nullptr;
    }
    return env->NewStringUTF(ByteCache(handle)->bridge_description());
}

extern "C" JNIEXPORT jobject JNICALL
Java_org_apache_flink_contrib_streaming_state_cachekit_state_NativeMapSnapshotCache_nativeClassifyPrefix(
        JNIEnv* env,
        jclass,
        jlong handle,
        jlong db_handle,
        jlong column_family_handle,
        jlong read_options_handle,
        jbyteArray prefix,
        jint compare_offset) {
    const jsize prefix_size = prefix == nullptr ? 0 : env->GetArrayLength(prefix);
    if (handle == 0 || db_handle == 0 || column_family_handle == 0
            || read_options_handle == 0 || prefix == nullptr || prefix_size <= 0
            || compare_offset < 0 || compare_offset >= prefix_size) {
        ThrowIllegalArgument(env, "invalid native snapshot classifier arguments");
        return nullptr;
    }
    try {
        return ByteCache(handle)->Classify(
                env,
                db_handle,
                column_family_handle,
                read_options_handle,
                prefix,
                compare_offset);
    } catch (const std::exception& error) {
        if (!env->ExceptionCheck()) {
            ThrowIllegalState(env, error.what());
        }
        return nullptr;
    }
}
