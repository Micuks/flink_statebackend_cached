#include "rocks_jni_bridge.h"

#include <dlfcn.h>
#include <elf.h>
#include <link.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace cachekit {
namespace {

#if defined(__aarch64__)
constexpr const char* kExpectedBuildId = "b4d1b52ddf0f5a33b41010a1dc981eefd834af74";
#elif defined(__x86_64__)
constexpr const char* kExpectedBuildId = "8c4b38a727cfd1af305092d0b35a4cf736bbe297";
#else
#error "Native snapshot classifier supports only Linux aarch64 and x86_64"
#endif

std::size_t Align4(std::size_t value) {
    return (value + 3U) & ~std::size_t{3U};
}

std::string Hex(const std::uint8_t* bytes, std::size_t size) {
    std::ostringstream output;
    output << std::hex << std::setfill('0');
    for (std::size_t index = 0; index < size; ++index) {
        output << std::setw(2) << static_cast<unsigned int>(bytes[index]);
    }
    return output.str();
}

std::string BuildId(const dl_phdr_info* info) {
    for (ElfW(Half) index = 0; index < info->dlpi_phnum; ++index) {
        const ElfW(Phdr)& header = info->dlpi_phdr[index];
        if (header.p_type != PT_NOTE) {
            continue;
        }
        const auto* cursor = reinterpret_cast<const std::uint8_t*>(
                info->dlpi_addr + header.p_vaddr);
        const auto* end = cursor + header.p_memsz;
        while (cursor + sizeof(ElfW(Nhdr)) <= end) {
            const auto* note = reinterpret_cast<const ElfW(Nhdr)*>(cursor);
            cursor += sizeof(ElfW(Nhdr));
            const std::size_t name_size = Align4(note->n_namesz);
            const std::size_t description_size = Align4(note->n_descsz);
            if (cursor + name_size + description_size > end) {
                break;
            }
            const auto* name = cursor;
            const auto* description = cursor + name_size;
            if (note->n_type == NT_GNU_BUILD_ID && note->n_namesz >= 3
                    && std::memcmp(name, "GNU", 3) == 0) {
                return Hex(description, note->n_descsz);
            }
            cursor += name_size + description_size;
        }
    }
    return "";
}

struct LoadedRocksLibrary {
    std::string path;
    std::string build_id;
};

int FindRocksLibrary(dl_phdr_info* info, std::size_t, void* data) {
    const char* path = info->dlpi_name;
    if (path == nullptr || std::strstr(path, "librocksdbjni") == nullptr) {
        return 0;
    }
    static_cast<std::vector<LoadedRocksLibrary>*>(data)->push_back({path, BuildId(info)});
    return 0;
}

template <typename T>
T Symbol(void* library, const char* name) {
    dlerror();
    void* symbol = dlsym(library, name);
    const char* error = dlerror();
    if (error != nullptr || symbol == nullptr) {
        throw std::runtime_error(std::string("missing FRocksDB JNI symbol ") + name);
    }
    return reinterpret_cast<T>(symbol);
}

bool Matches(
        JNIEnv* env,
        jbyteArray raw_key,
        const std::vector<jbyte>& prefix,
        jint compare_offset) {
    const jsize raw_size = env->GetArrayLength(raw_key);
    if (raw_size < static_cast<jsize>(prefix.size())) {
        return false;
    }
    const jsize compare_size = static_cast<jsize>(prefix.size()) - compare_offset;
    std::vector<jbyte> raw_prefix(static_cast<std::size_t>(compare_size));
    env->GetByteArrayRegion(raw_key, compare_offset, compare_size, raw_prefix.data());
    return !env->ExceptionCheck()
            && std::memcmp(
                       raw_prefix.data(),
                       prefix.data() + compare_offset,
                       static_cast<std::size_t>(compare_size)) == 0;
}

bool Equals(JNIEnv* env, jbyteArray left, jbyteArray right) {
    const jsize size = env->GetArrayLength(left);
    if (size != env->GetArrayLength(right)) {
        return false;
    }
    std::vector<jbyte> left_bytes(static_cast<std::size_t>(size));
    std::vector<jbyte> right_bytes(static_cast<std::size_t>(size));
    env->GetByteArrayRegion(left, 0, size, left_bytes.data());
    env->GetByteArrayRegion(right, 0, size, right_bytes.data());
    return !env->ExceptionCheck()
            && std::memcmp(left_bytes.data(), right_bytes.data(), static_cast<std::size_t>(size))
                    == 0;
}

}  // namespace

RocksJniBridge::RocksJniBridge(void* library, std::string description)
        : library_(library), description_(std::move(description)) {
    iterator_cf_ = Symbol<IteratorCf>(library_, "Java_org_rocksdb_RocksDB_iteratorCF__JJJ");
    iterator_dispose_ = Symbol<IteratorDispose>(
            library_, "Java_org_rocksdb_RocksIterator_disposeInternal");
    iterator_is_valid_ = Symbol<IteratorIsValid>(
            library_, "Java_org_rocksdb_RocksIterator_isValid0");
    iterator_seek_ = Symbol<IteratorSeek>(library_, "Java_org_rocksdb_RocksIterator_seek0");
    iterator_next_ = Symbol<IteratorNext>(library_, "Java_org_rocksdb_RocksIterator_next0");
    iterator_status_ = Symbol<IteratorStatus>(library_, "Java_org_rocksdb_RocksIterator_status0");
    iterator_key_ = Symbol<IteratorKey>(library_, "Java_org_rocksdb_RocksIterator_key0");
    iterator_value_ = Symbol<IteratorValue>(library_, "Java_org_rocksdb_RocksIterator_value0");
}

RocksJniBridge::~RocksJniBridge() {
    if (library_ != nullptr) {
        dlclose(library_);
    }
}

std::unique_ptr<RocksJniBridge> RocksJniBridge::Load() {
    std::vector<LoadedRocksLibrary> libraries;
    dl_iterate_phdr(FindRocksLibrary, &libraries);
    if (libraries.size() != 1) {
        throw std::runtime_error(
                "Native snapshot classifier requires exactly one loaded librocksdbjni; found "
                + std::to_string(libraries.size()));
    }
    const LoadedRocksLibrary& library = libraries.front();
    if (library.build_id != kExpectedBuildId) {
        throw std::runtime_error(
                "unsupported librocksdbjni build-id " + library.build_id
                + "; expected " + kExpectedBuildId + " at " + library.path);
    }
    void* handle = dlopen(library.path.c_str(), RTLD_NOW | RTLD_NOLOAD);
    if (handle == nullptr) {
        throw std::runtime_error(
                "failed to acquire loaded librocksdbjni " + library.path + ": " + dlerror());
    }
    try {
        return std::unique_ptr<RocksJniBridge>(new RocksJniBridge(
                handle, library.path + " build-id=" + library.build_id));
    } catch (...) {
        dlclose(handle);
        throw;
    }
}

PrefixClassification RocksJniBridge::Classify(
        JNIEnv* env,
        jlong db_handle,
        jlong column_family_handle,
        jlong read_options_handle,
        jbyteArray prefix,
        jint compare_offset) const {
    const jsize prefix_size = env->GetArrayLength(prefix);
    std::vector<jbyte> prefix_bytes(static_cast<std::size_t>(prefix_size));
    env->GetByteArrayRegion(prefix, 0, prefix_size, prefix_bytes.data());
    if (env->ExceptionCheck()) {
        return {PrefixKind::kEmpty, nullptr};
    }

    const jlong iterator = iterator_cf_(
            env, nullptr, db_handle, column_family_handle, read_options_handle);
    if (iterator == 0 || env->ExceptionCheck()) {
        return {PrefixKind::kEmpty, nullptr};
    }

    jbyteArray first_key = nullptr;
    PrefixKind kind = PrefixKind::kEmpty;
    iterator_seek_(env, nullptr, iterator, prefix, prefix_size);
    if (!env->ExceptionCheck() && iterator_is_valid_(env, nullptr, iterator)) {
        first_key = iterator_key_(env, nullptr, iterator);
        if (first_key != nullptr && !env->ExceptionCheck()
                && Matches(env, first_key, prefix_bytes, compare_offset)) {
            kind = PrefixKind::kSingle;
            iterator_next_(env, nullptr, iterator);
            if (!env->ExceptionCheck() && iterator_is_valid_(env, nullptr, iterator)) {
                jbyteArray second_key = iterator_key_(env, nullptr, iterator);
                if (second_key != nullptr && !env->ExceptionCheck()
                        && Matches(env, second_key, prefix_bytes, compare_offset)) {
                    kind = PrefixKind::kMulti;
                }
                if (second_key != nullptr) {
                    env->DeleteLocalRef(second_key);
                }
            }
        }
    }

    if (!env->ExceptionCheck()) {
        iterator_status_(env, nullptr, iterator);
    }
    iterator_dispose_(env, nullptr, iterator);
    if (env->ExceptionCheck()) {
        if (first_key != nullptr) {
            env->DeleteLocalRef(first_key);
        }
        return {PrefixKind::kEmpty, nullptr};
    }
    if (kind != PrefixKind::kSingle && first_key != nullptr) {
        env->DeleteLocalRef(first_key);
        first_key = nullptr;
    }
    return {kind, first_key};
}

jobjectArray RocksJniBridge::ReadPrefixBatch(
        JNIEnv* env,
        jlong db_handle,
        jlong column_family_handle,
        jlong read_options_handle,
        jbyteArray prefix,
        jint compare_offset,
        jbyteArray start_after,
        jint max_entries,
        jobject end_sentinel,
        jobject more_sentinel) const {
    const jsize prefix_size = env->GetArrayLength(prefix);
    std::vector<jbyte> prefix_bytes(static_cast<std::size_t>(prefix_size));
    env->GetByteArrayRegion(prefix, 0, prefix_size, prefix_bytes.data());
    if (env->ExceptionCheck()) {
        return nullptr;
    }

    jclass object_class = env->FindClass("java/lang/Object");
    if (object_class == nullptr) {
        return nullptr;
    }
    jobjectArray output = env->NewObjectArray(max_entries * 2 + 1, object_class, nullptr);
    env->DeleteLocalRef(object_class);
    if (output == nullptr) {
        return nullptr;
    }

    const jlong iterator = iterator_cf_(
            env, nullptr, db_handle, column_family_handle, read_options_handle);
    if (iterator == 0 || env->ExceptionCheck()) {
        env->DeleteLocalRef(output);
        return nullptr;
    }

    iterator_seek_(
            env,
            nullptr,
            iterator,
            start_after == nullptr ? prefix : start_after,
            start_after == nullptr ? prefix_size : env->GetArrayLength(start_after));
    if (!env->ExceptionCheck() && start_after != nullptr
            && iterator_is_valid_(env, nullptr, iterator)) {
        jbyteArray current_key = iterator_key_(env, nullptr, iterator);
        if (current_key != nullptr && !env->ExceptionCheck()
                && Equals(env, current_key, start_after)) {
            iterator_next_(env, nullptr, iterator);
        }
        if (current_key != nullptr) {
            env->DeleteLocalRef(current_key);
        }
    }

    jint count = 0;
    while (!env->ExceptionCheck() && count < max_entries
            && iterator_is_valid_(env, nullptr, iterator)) {
        jbyteArray key = iterator_key_(env, nullptr, iterator);
        if (key == nullptr || env->ExceptionCheck()
                || !Matches(env, key, prefix_bytes, compare_offset)) {
            if (key != nullptr) {
                env->DeleteLocalRef(key);
            }
            break;
        }
        jbyteArray value = iterator_value_(env, nullptr, iterator);
        if (value == nullptr || env->ExceptionCheck()) {
            env->DeleteLocalRef(key);
            if (value != nullptr) {
                env->DeleteLocalRef(value);
            }
            break;
        }
        env->SetObjectArrayElement(output, count * 2, key);
        env->SetObjectArrayElement(output, count * 2 + 1, value);
        env->DeleteLocalRef(key);
        env->DeleteLocalRef(value);
        ++count;
        iterator_next_(env, nullptr, iterator);
    }

    bool has_more = false;
    if (!env->ExceptionCheck() && iterator_is_valid_(env, nullptr, iterator)) {
        jbyteArray next_key = iterator_key_(env, nullptr, iterator);
        if (next_key != nullptr && !env->ExceptionCheck()) {
            has_more = Matches(env, next_key, prefix_bytes, compare_offset);
            env->DeleteLocalRef(next_key);
        }
    }
    if (!env->ExceptionCheck()) {
        iterator_status_(env, nullptr, iterator);
    }
    iterator_dispose_(env, nullptr, iterator);
    if (env->ExceptionCheck()) {
        env->DeleteLocalRef(output);
        return nullptr;
    }
    env->SetObjectArrayElement(output, count * 2, has_more ? more_sentinel : end_sentinel);
    return output;
}

}  // namespace cachekit
