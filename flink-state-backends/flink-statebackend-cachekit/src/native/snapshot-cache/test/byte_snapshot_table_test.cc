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
                    == cachekit::PutResult::kInserted,
            "empty snapshot put failed");
    Require(Put(table, "row-key-2", cachekit::SnapshotKind::kSingle, "user-row-2")
                    == cachekit::PutResult::kInserted,
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
                    == cachekit::PutResult::kUpdated,
            "snapshot update failed");
    Require(std::string(displaced.begin(), displaced.end()) == "user-row-2",
            "snapshot update did not return displaced payload");
    Require(Put(table, "row-key-3", cachekit::SnapshotKind::kEmpty)
                    == cachekit::PutResult::kInserted,
            "third snapshot put failed");

    // key-1 was touched before key-2 and is now the LRU entry.
    std::uint64_t evicted_hash = 0;
    Require(table.Put(
                    reinterpret_cast<const std::uint8_t*>("row-key-4"),
                    9,
                    cachekit::SnapshotKind::kEmpty,
                    nullptr,
                    0,
                    nullptr,
                    &evicted_hash)
                    == cachekit::PutResult::kInsertedWithEviction,
            "bounded LRU did not report eviction");
    Require(evicted_hash == cachekit::HashByteKey(
                    reinterpret_cast<const std::uint8_t*>("row-key-1"), 9),
            "eviction did not return the displaced key hash");
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
                    == cachekit::PutResult::kInserted,
            "pre-clear snapshot put failed");
    std::vector<std::vector<std::uint8_t>> cleared;
    table.Clear(&cleared);
    Require(table.size() == 0, "clear did not reset size");
    Require(cleared.size() == 1
                    && std::string(cleared[0].begin(), cleared[0].end()) == "clear-me",
            "clear did not return live payload");

    for (int index = 0; index < 8192; ++index) {
        const std::string churn_key = "churn-" + std::to_string(index);
        Require(Put(table, churn_key, cachekit::SnapshotKind::kEmpty)
                        == cachekit::PutResult::kInserted,
                "churn put failed");
        Require(table.Remove(
                        reinterpret_cast<const std::uint8_t*>(churn_key.data()),
                        churn_key.size()),
                "churn remove failed");
    }
    Require(table.size() == 0, "churn left live entries");
    Require(!Lookup(table, "never-present").found, "churn miss returned an entry");
    Require(Put(table, "post-churn", cachekit::SnapshotKind::kSingle, "still-works")
                    == cachekit::PutResult::kInserted,
            "post-churn put failed");
    result = Lookup(table, "post-churn");
    Require(result.found && result.kind == cachekit::SnapshotKind::kSingle,
            "post-churn lookup failed");
    std::cout << "PASS bytes kernel=" << table.active_kernel_name()
              << " vector_bytes=" << table.vector_bytes() << '\n';
}

void RunRebuildPreservesLiveEntries(cachekit::ProbeKernel kernel) {
    cachekit::ByteSnapshotTable table(70, kernel);
    Require(Put(table, "keep-a", cachekit::SnapshotKind::kSingle, "payload-a")
                    == cachekit::PutResult::kInserted,
            "first retained put failed");
    Require(Put(table, "keep-b", cachekit::SnapshotKind::kSingle, "payload-b")
                    == cachekit::PutResult::kInserted,
            "second retained put failed");
    for (int index = 0; index < 128; ++index) {
        const std::string churn_key = "live-churn-" + std::to_string(index);
        Require(Put(table, churn_key, cachekit::SnapshotKind::kEmpty)
                        == cachekit::PutResult::kInserted,
                "live churn put failed");
        Require(table.Remove(
                        reinterpret_cast<const std::uint8_t*>(churn_key.data()),
                        churn_key.size()),
                "live churn remove failed");
    }
    Require(table.size() == 2, "rebuild changed live size");
    cachekit::ByteLookupResult result = Lookup(table, "keep-a");
    Require(result.found
                    && std::string(
                               reinterpret_cast<const char*>(result.payload),
                               result.payload_size)
                            == "payload-a",
            "rebuild lost first live payload");
    result = Lookup(table, "keep-b");
    Require(result.found
                    && std::string(
                               reinterpret_cast<const char*>(result.payload),
                               result.payload_size)
                            == "payload-b",
            "rebuild lost second live payload");
}

void RunSixteenByteHashPath(cachekit::ProbeKernel kernel) {
    cachekit::ByteSnapshotTable table(2, kernel);
    const std::string first = "0123456789abcdef";
    const std::string second = "1123456789abcdef";
    const std::string third = "2123456789abcdef";
    Require(Put(table, first, cachekit::SnapshotKind::kEmpty)
                    == cachekit::PutResult::kInserted,
            "16-byte first put failed");
    Require(Put(table, second, cachekit::SnapshotKind::kEmpty)
                    == cachekit::PutResult::kInserted,
            "16-byte second put failed");
    Require(Lookup(table, first).found, "16-byte lookup failed");
    std::uint64_t evicted_hash = 0;
    Require(table.Put(
                    reinterpret_cast<const std::uint8_t*>(third.data()),
                    third.size(),
                    cachekit::SnapshotKind::kEmpty,
                    nullptr,
                    0,
                    nullptr,
                    &evicted_hash)
                    == cachekit::PutResult::kInsertedWithEviction,
            "16-byte eviction failed");
    Require(evicted_hash == cachekit::HashByteKey(
                    reinterpret_cast<const std::uint8_t*>(second.data()), second.size()),
            "16-byte eviction changed the Java membership hash protocol");
    Require(!Lookup(table, second).found, "16-byte LRU entry remained visible");
    Require(Lookup(table, first).found && Lookup(table, third).found,
            "16-byte live entries were lost");
}

void RunKunpengFeatureGate() {
#if !CACHEKIT_ENABLE_KUNPENG_SNAPSHOT
    Require(!cachekit::KunpengSnapshotFeatureAvailable(),
            "Kunpeng snapshot gate opened in a disabled build");
#endif
}

}  // namespace

int main() {
    RunKunpengFeatureGate();
    std::vector<cachekit::ProbeKernel> kernels = {
            cachekit::ProbeKernel::kAuto, cachekit::ProbeKernel::kScalar};
    if (cachekit::NeonAvailable()) {
        kernels.push_back(cachekit::ProbeKernel::kNeon);
    }
    if (cachekit::SveAvailable() && cachekit::SveVectorBytes() != 0) {
        kernels.push_back(cachekit::ProbeKernel::kSve);
    }
    for (cachekit::ProbeKernel kernel : kernels) {
        RunKernel(kernel);
        RunRebuildPreservesLiveEntries(kernel);
        RunSixteenByteHashPath(kernel);
    }
    return 0;
}
