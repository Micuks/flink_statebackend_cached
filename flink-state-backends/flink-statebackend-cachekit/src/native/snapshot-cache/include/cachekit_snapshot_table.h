#ifndef CACHEKIT_SNAPSHOT_TABLE_H
#define CACHEKIT_SNAPSHOT_TABLE_H

#include <cstddef>
#include <cstdint>
#include <memory>

namespace cachekit {

enum class ProbeKernel {
    kScalar,
    kNeon,
    kSve,
    kAuto,
};

enum class SnapshotKind : std::uint8_t {
    kEmpty = 1,
    kSingle = 2,
};

struct LookupResult {
    bool found;
    SnapshotKind kind;
    std::uint32_t entry_id;
};

class SnapshotTable {
public:
    SnapshotTable(std::size_t capacity, ProbeKernel requested_kernel);
    ~SnapshotTable();

    SnapshotTable(const SnapshotTable&) = delete;
    SnapshotTable& operator=(const SnapshotTable&) = delete;

    bool Put(
            std::uint64_t key,
            std::uint64_t name_space,
            SnapshotKind kind,
            std::uint32_t entry_id);
    LookupResult Lookup(std::uint64_t key, std::uint64_t name_space);
    bool Remove(std::uint64_t key, std::uint64_t name_space);
    void Clear();

    std::size_t size() const;
    std::size_t capacity() const;
    ProbeKernel active_kernel() const;
    std::size_t vector_bytes() const;
    const char* active_kernel_name() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

bool NeonAvailable();
bool SveAvailable();
std::size_t SveVectorBytes();

}  // namespace cachekit

#endif
