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

constexpr const char* kExpectedBuildId = "b4d1b52ddf0f5a33b41010a1dc981eefd834af74";

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

}  // namespace cachekit
