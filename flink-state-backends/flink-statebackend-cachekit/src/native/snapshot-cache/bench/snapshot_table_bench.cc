#include "cachekit_snapshot_table.h"
#include "cachekit_byte_snapshot_table.h"

#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <array>
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

template <std::size_t Size>
using ByteKey = std::array<std::uint8_t, Size>;

template <std::size_t Size>
ByteKey<Size> EncodeKey(std::uint64_t value) {
    ByteKey<Size> key{};
    for (std::size_t index = 0; index < key.size(); ++index) {
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        key[index] = static_cast<std::uint8_t>(value);
    }
    return key;
}

template <std::size_t Size>
void RunBytes(cachekit::ProbeKernel kernel, const std::vector<ByteKey<Size>>& queries) {
    constexpr std::size_t kCapacity = 4096;
    constexpr std::size_t kEntries = 2048;
    constexpr int kRounds = 8;
    cachekit::ByteSnapshotTable table(kCapacity, kernel);
    for (std::uint64_t index = 0; index < kEntries; ++index) {
        const ByteKey<Size> key = EncodeKey<Size>(index);
        table.Put(
                key.data(),
                key.size(),
                cachekit::SnapshotKind::kSingle,
                reinterpret_cast<const std::uint8_t*>(&index),
                sizeof(index));
    }

    std::uint64_t checksum = 0;
    const auto start = std::chrono::steady_clock::now();
    for (int round = 0; round < kRounds; ++round) {
        for (const ByteKey<Size>& key : queries) {
            const cachekit::ByteLookupResult result = table.Lookup(key.data(), key.size());
            checksum += result.found ? result.payload_size : 0;
        }
    }
    const auto elapsed = std::chrono::steady_clock::now() - start;
    const double operations = static_cast<double>(queries.size()) * kRounds;
    const double nanoseconds =
            std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
    std::cout << std::left << std::setw(8) << table.active_kernel_name()
              << " byte-key-" << Size << " hash=" << std::setw(9) << table.hash_name()
              << " vector_bytes=" << std::setw(3) << table.vector_bytes()
              << " ns/op=" << std::fixed << std::setprecision(2) << nanoseconds / operations
              << " Mops/s=" << std::setprecision(2) << operations * 1000.0 / nanoseconds
              << " checksum=" << checksum << '\n';
}

void RunRemoveMissAfterChurn(cachekit::ProbeKernel kernel) {
    constexpr std::size_t kCapacity = 2000;
    constexpr std::size_t kChurnOperations = 32768;
    constexpr std::size_t kMeasuredOperations = 1 << 18;
    cachekit::ByteSnapshotTable table(kCapacity, kernel);
    for (std::uint64_t index = 0; index < kChurnOperations; ++index) {
        const ByteKey<32> key = EncodeKey<32>(index);
        table.Put(key.data(), key.size(), cachekit::SnapshotKind::kEmpty, nullptr, 0);
        table.Remove(key.data(), key.size());
    }

    std::size_t removed = 0;
    const auto start = std::chrono::steady_clock::now();
    for (std::uint64_t index = 0; index < kMeasuredOperations; ++index) {
        const ByteKey<32> key = EncodeKey<32>(kChurnOperations + index);
        removed += table.Remove(key.data(), key.size()) ? 1 : 0;
    }
    const auto elapsed = std::chrono::steady_clock::now() - start;
    const double operations = static_cast<double>(kMeasuredOperations);
    const double nanoseconds =
            std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
    std::cout << std::left << std::setw(8) << table.active_kernel_name()
              << " remove-miss-after-churn ns/op=" << std::fixed << std::setprecision(2)
              << nanoseconds / operations
              << " Mops/s=" << std::setprecision(2) << operations * 1000.0 / nanoseconds
              << " removed=" << removed << '\n';
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

    std::vector<ByteKey<16>> byte_queries_16;
    std::vector<ByteKey<32>> byte_queries_32;
    byte_queries_16.reserve(queries.size());
    byte_queries_32.reserve(queries.size());
    for (const Query& query : queries) {
        byte_queries_16.push_back(EncodeKey<16>(query.key));
        byte_queries_32.push_back(EncodeKey<32>(query.key));
    }

    Run(cachekit::ProbeKernel::kScalar, queries);
    RunBytes(cachekit::ProbeKernel::kScalar, byte_queries_16);
    RunBytes(cachekit::ProbeKernel::kScalar, byte_queries_32);
    RunRemoveMissAfterChurn(cachekit::ProbeKernel::kScalar);
    if (cachekit::NeonAvailable()) {
        Run(cachekit::ProbeKernel::kNeon, queries);
        RunBytes(cachekit::ProbeKernel::kNeon, byte_queries_16);
        RunBytes(cachekit::ProbeKernel::kNeon, byte_queries_32);
        RunRemoveMissAfterChurn(cachekit::ProbeKernel::kNeon);
    }
    if (cachekit::SveAvailable() && cachekit::SveVectorBytes() != 0) {
        Run(cachekit::ProbeKernel::kSve, queries);
        RunBytes(cachekit::ProbeKernel::kSve, byte_queries_16);
        RunBytes(cachekit::ProbeKernel::kSve, byte_queries_32);
        RunRemoveMissAfterChurn(cachekit::ProbeKernel::kSve);
    }
    return 0;
}
