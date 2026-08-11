#include "cachekit_snapshot_table.h"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <unordered_map>
#include <vector>

namespace {

struct Expected {
    cachekit::SnapshotKind kind;
    std::uint32_t entry_id;
};

void Require(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        std::exit(1);
    }
}

std::uint64_t PairKey(std::uint64_t key, std::uint64_t name_space) {
    return key * 17 + name_space;
}

void RunKernel(cachekit::ProbeKernel kernel) {
    cachekit::SnapshotTable table(4096, kernel);
    std::unordered_map<std::uint64_t, Expected> expected;
    std::uint64_t random = UINT64_C(0x123456789abcdef0);

    for (std::uint32_t step = 0; step < 200000; ++step) {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        const std::uint64_t key = random & 2047;
        const std::uint64_t name_space = key % 5;
        const std::uint64_t pair = PairKey(key, name_space);
        const std::uint32_t operation = static_cast<std::uint32_t>((random >> 32) % 10);

        if (operation < 4) {
            const cachekit::SnapshotKind kind = (random & 1) == 0
                    ? cachekit::SnapshotKind::kEmpty
                    : cachekit::SnapshotKind::kSingle;
            Require(table.Put(key, name_space, kind, step), "put rejected below 50% load");
            expected[pair] = {kind, step};
        } else if (operation < 6) {
            const bool removed = table.Remove(key, name_space);
            const bool expected_removed = expected.erase(pair) != 0;
            Require(removed == expected_removed, "remove result differs");
        } else {
            const cachekit::LookupResult actual = table.Lookup(key, name_space);
            const auto found = expected.find(pair);
            Require(actual.found == (found != expected.end()), "lookup presence differs");
            if (actual.found) {
                Require(actual.kind == found->second.kind, "lookup kind differs");
                Require(actual.entry_id == found->second.entry_id, "lookup entry id differs");
            }
        }
    }

    Require(table.size() == expected.size(), "size differs after random operations");
    for (std::uint64_t key = 0; key < 2048; ++key) {
        const std::uint64_t name_space = key % 5;
        const cachekit::LookupResult actual = table.Lookup(key, name_space);
        const auto found = expected.find(PairKey(key, name_space));
        Require(actual.found == (found != expected.end()), "final scan presence differs");
    }

    table.Clear();
    Require(table.size() == 0, "clear did not reset size");
    Require(!table.Lookup(1, 1).found, "clear left an entry visible");
    std::cout << "PASS kernel=" << table.active_kernel_name()
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
