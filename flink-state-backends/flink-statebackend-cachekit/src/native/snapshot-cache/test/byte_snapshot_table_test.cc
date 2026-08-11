#include "cachekit_byte_snapshot_table.h"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

namespace {

void Require(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        std::exit(1);
    }
}

cachekit::PutResult Put(
        cachekit::ByteSnapshotTable& table,
        const std::string& key,
        cachekit::SnapshotKind kind,
        const std::string& payload = std::string()) {
    return table.Put(
            reinterpret_cast<const std::uint8_t*>(key.data()),
            key.size(),
            kind,
            payload.empty() ? nullptr : reinterpret_cast<const std::uint8_t*>(payload.data()),
            payload.size());
}

cachekit::ByteLookupResult Lookup(
        cachekit::ByteSnapshotTable& table,
        const std::string& key) {
    return table.Lookup(
            reinterpret_cast<const std::uint8_t*>(key.data()), key.size());
}

void RunKernel(cachekit::ProbeKernel kernel) {
    cachekit::ByteSnapshotTable table(3, kernel);
    Require(Put(table, "row-key-1", cachekit::SnapshotKind::kEmpty)
                    == cachekit::PutResult::kStored,
            "empty snapshot put failed");
    Require(Put(table, "row-key-2", cachekit::SnapshotKind::kSingle, "user-row-2")
                    == cachekit::PutResult::kStored,
            "single snapshot put failed");

    cachekit::ByteLookupResult result = Lookup(table, "row-key-1");
    Require(result.found && result.kind == cachekit::SnapshotKind::kEmpty,
            "empty snapshot lookup failed");
    result = Lookup(table, "row-key-2");
    Require(result.found && result.kind == cachekit::SnapshotKind::kSingle,
            "single snapshot lookup failed");
    Require(std::string(reinterpret_cast<const char*>(result.payload), result.payload_size)
                    == "user-row-2",
            "single snapshot payload differs");

    const std::string updated_payload = "updated";
    std::vector<std::uint8_t> displaced;
    Require(table.Put(
                    reinterpret_cast<const std::uint8_t*>("row-key-2"),
                    9,
                    cachekit::SnapshotKind::kSingle,
                    reinterpret_cast<const std::uint8_t*>(updated_payload.data()),
                    updated_payload.size(),
                    &displaced)
                    == cachekit::PutResult::kStored,
            "snapshot update failed");
    Require(std::string(displaced.begin(), displaced.end()) == "user-row-2",
            "snapshot update did not return displaced payload");
    Require(Put(table, "row-key-3", cachekit::SnapshotKind::kEmpty)
                    == cachekit::PutResult::kStored,
            "third snapshot put failed");

    // key-1 was touched before key-2 and is now the LRU entry.
    Require(Put(table, "row-key-4", cachekit::SnapshotKind::kEmpty)
                    == cachekit::PutResult::kStoredWithEviction,
            "bounded LRU did not report eviction");
    Require(!Lookup(table, "row-key-1").found, "LRU entry was not evicted");
    Require(Lookup(table, "row-key-2").found, "recent entry was incorrectly evicted");

    std::vector<std::uint8_t> removed;
    Require(table.Remove(
                    reinterpret_cast<const std::uint8_t*>("row-key-2"), 9, &removed),
            "remove failed");
    Require(std::string(removed.begin(), removed.end()) == "updated",
            "remove did not return payload");
    Require(!Lookup(table, "row-key-2").found, "removed entry remained visible");
    Require(Put(table, "row-key-5", cachekit::SnapshotKind::kSingle, "clear-me")
                    == cachekit::PutResult::kStored,
            "pre-clear snapshot put failed");
    std::vector<std::vector<std::uint8_t>> cleared;
    table.Clear(&cleared);
    Require(table.size() == 0, "clear did not reset size");
    Require(cleared.size() == 1
                    && std::string(cleared[0].begin(), cleared[0].end()) == "clear-me",
            "clear did not return live payload");
    std::cout << "PASS bytes kernel=" << table.active_kernel_name()
              << " vector_bytes=" << table.vector_bytes() << '\n';
}

}  // namespace

int main() {
    std::vector<cachekit::ProbeKernel> kernels = {cachekit::ProbeKernel::kScalar};
    if (cachekit::NeonAvailable()) {
        kernels.push_back(cachekit::ProbeKernel::kNeon);
    }
    if (cachekit::SveAvailable() && cachekit::SveVectorBytes() != 0) {
        kernels.push_back(cachekit::ProbeKernel::kSve);
    }
    for (cachekit::ProbeKernel kernel : kernels) {
        RunKernel(kernel);
    }
    return 0;
}
