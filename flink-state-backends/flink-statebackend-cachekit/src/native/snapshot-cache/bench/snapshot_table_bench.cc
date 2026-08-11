#include "cachekit_snapshot_table.h"

#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <vector>

namespace {

struct Query {
    std::uint64_t key;
    std::uint64_t name_space;
};

void Run(cachekit::ProbeKernel kernel, const std::vector<Query>& queries) {
    constexpr std::size_t kCapacity = 4096;
    constexpr std::size_t kEntries = 2048;
    constexpr int kRounds = 8;
    cachekit::SnapshotTable table(kCapacity, kernel);
    for (std::uint32_t index = 0; index < kEntries; ++index) {
        table.Put(index, index % 7, cachekit::SnapshotKind::kSingle, index);
    }

    std::uint64_t checksum = 0;
    const auto start = std::chrono::steady_clock::now();
    for (int round = 0; round < kRounds; ++round) {
        for (const Query& query : queries) {
            const cachekit::LookupResult result = table.Lookup(query.key, query.name_space);
            checksum += result.found ? result.entry_id + 1 : 0;
        }
    }
    const auto elapsed = std::chrono::steady_clock::now() - start;
    const double operations = static_cast<double>(queries.size()) * kRounds;
    const double nanoseconds =
            std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
    std::cout << std::left << std::setw(8) << table.active_kernel_name()
              << " vector_bytes=" << std::setw(3) << table.vector_bytes()
              << " ns/op=" << std::fixed << std::setprecision(2) << nanoseconds / operations
              << " Mops/s=" << std::setprecision(2) << operations * 1000.0 / nanoseconds
              << " checksum=" << checksum << '\n';
}

}  // namespace

int main() {
    constexpr std::size_t kQueryCount = 1 << 20;
    std::vector<Query> queries;
    queries.reserve(kQueryCount);
    std::uint64_t random = UINT64_C(0x243f6a8885a308d3);
    for (std::size_t index = 0; index < kQueryCount; ++index) {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        const bool hit = (index & 3) != 0;
        const std::uint64_t key = hit ? random & 2047 : 4096 + (random & 2047);
        queries.push_back({key, key % 7});
    }

    Run(cachekit::ProbeKernel::kScalar, queries);
    if (cachekit::NeonAvailable()) {
        Run(cachekit::ProbeKernel::kNeon, queries);
    }
    if (cachekit::SveAvailable() && cachekit::SveVectorBytes() != 0) {
        Run(cachekit::ProbeKernel::kSve, queries);
    }
    return 0;
}
