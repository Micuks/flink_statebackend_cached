#ifndef CACHEKIT_ROCKS_JNI_BRIDGE_H
#define CACHEKIT_ROCKS_JNI_BRIDGE_H

#include <jni.h>

#include <memory>
#include <string>

namespace cachekit {

enum class PrefixKind {
    kEmpty,
    kSingle,
    kMulti,
};

struct PrefixClassification {
    PrefixKind kind;
    jbyteArray single_key;
};

class RocksJniBridge {
public:
    static std::unique_ptr<RocksJniBridge> Load();
    ~RocksJniBridge();

    PrefixClassification Classify(
            JNIEnv* env,
            jlong db_handle,
            jlong column_family_handle,
            jlong read_options_handle,
            jbyteArray prefix,
            jint compare_offset) const;

    jobjectArray ReadPrefixBatch(
            JNIEnv* env,
            jlong db_handle,
            jlong column_family_handle,
            jlong read_options_handle,
            jbyteArray prefix,
            jint compare_offset,
            jbyteArray start_after,
            jint max_entries,
            jobject end_sentinel,
            jobject more_sentinel) const;

    const std::string& description() const { return description_; }

private:
    RocksJniBridge(void* library, std::string description);

    void* library_;
    std::string description_;

    using IteratorCf = jlong (*)(JNIEnv*, jobject, jlong, jlong, jlong);
    using IteratorDispose = void (*)(JNIEnv*, jobject, jlong);
    using IteratorIsValid = jboolean (*)(JNIEnv*, jobject, jlong);
    using IteratorSeek = void (*)(JNIEnv*, jobject, jlong, jbyteArray, jint);
    using IteratorNext = void (*)(JNIEnv*, jobject, jlong);
    using IteratorStatus = void (*)(JNIEnv*, jobject, jlong);
    using IteratorKey = jbyteArray (*)(JNIEnv*, jobject, jlong);
    using IteratorValue = jbyteArray (*)(JNIEnv*, jobject, jlong);

    IteratorCf iterator_cf_;
    IteratorDispose iterator_dispose_;
    IteratorIsValid iterator_is_valid_;
    IteratorSeek iterator_seek_;
    IteratorNext iterator_next_;
    IteratorStatus iterator_status_;
    IteratorKey iterator_key_;
    IteratorValue iterator_value_;
};

}  // namespace cachekit

#endif
