#ifndef CACHEKIT_BYTE_SNAPSHOT_TABLE_H
#define CACHEKIT_BYTE_SNAPSHOT_TABLE_H

#include "cachekit_snapshot_table.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace cachekit {

enum class PutResult : std::uint8_t {
    kRejected = 0,
    kUpdated = 1,
    kInserted = 2,
    kInsertedWithEviction = 3,
};

std::uint64_t HashByteKey(const std::uint8_t* bytes, std::size_t size);
bool KunpengSnapshotFeatureAvailable();

struct ByteLookupResult {
    bool found;
    SnapshotKind kind;
    const std::uint8_t* payload;
    std::size_t payload_size;
};

class ByteSnapshotTable {
public:
    ByteSnapshotTable(std::size_t max_entries, ProbeKernel requested_kernel);
    ~ByteSnapshotTable();

    ByteSnapshotTable(const ByteSnapshotTable&) = delete;
    ByteSnapshotTable& operator=(const ByteSnapshotTable&) = delete;

    PutResult Put(
            const std::uint8_t* key,
            std::size_t key_size,
            SnapshotKind kind,
            const std::uint8_t* payload,
            std::size_t payload_size,
            std::vector<std::uint8_t>* displaced_payload = nullptr,
            std::uint64_t* displaced_hash = nullptr);
    ByteLookupResult Lookup(const std::uint8_t* key, std::size_t key_size);
    bool Remove(
            const std::uint8_t* key,
            std::size_t key_size,
            std::vector<std::uint8_t>* removed_payload = nullptr);
    void Clear(std::vector<std::vector<std::uint8_t>>* removed_payloads = nullptr);

    std::size_t size() const;
    ProbeKernel active_kernel() const;
    std::size_t vector_bytes() const;
    const char* active_kernel_name() const;
    const char* hash_name() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace cachekit

#endif
