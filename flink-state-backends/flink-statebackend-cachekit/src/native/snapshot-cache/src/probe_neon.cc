#include "probe.h"

#include <arm_neon.h>

namespace cachekit {
namespace {

constexpr std::uint8_t kEmpty = 0;

}  // namespace

int FindSlotScalar(
        const std::uint8_t* control,
        const std::uint64_t* hashes,
        const std::uint64_t* keys,
        const std::uint64_t* namespaces,
        std::size_t capacity,
        std::uint64_t hash,
        std::uint8_t fingerprint,
        std::uint64_t key,
        std::uint64_t name_space) {
    const std::size_t mask = capacity - 1;
    std::size_t slot = hash & mask;
    for (std::size_t probes = 0; probes < capacity; ++probes) {
        const std::uint8_t marker = control[slot];
        if (marker == kEmpty) {
            return -1;
        }
        if (marker == fingerprint && hashes[slot] == hash && keys[slot] == key
                && namespaces[slot] == name_space) {
            return static_cast<int>(slot);
        }
        slot = (slot + 1) & mask;
    }
    return -1;
}

int FindSlotNeon(
        const std::uint8_t* control,
        const std::uint64_t* hashes,
        const std::uint64_t* keys,
        const std::uint64_t* namespaces,
        std::size_t capacity,
        std::uint64_t hash,
        std::uint8_t fingerprint,
        std::uint64_t key,
        std::uint64_t name_space) {
    constexpr std::size_t kWidth = 16;
    const std::size_t mask = capacity - 1;
    const std::size_t start = hash & mask;
    const uint8x16_t empty_value = vdupq_n_u8(kEmpty);
    const uint8x16_t fingerprint_value = vdupq_n_u8(fingerprint);
    alignas(16) std::uint8_t empty_lanes[kWidth];
    alignas(16) std::uint8_t matching_lanes[kWidth];

    for (std::size_t probes = 0; probes < capacity; probes += kWidth) {
        const std::size_t base = (start + probes) & mask;
        const uint8x16_t markers = vld1q_u8(control + base);
        vst1q_u8(empty_lanes, vceqq_u8(markers, empty_value));
        vst1q_u8(matching_lanes, vceqq_u8(markers, fingerprint_value));
        for (std::size_t lane = 0; lane < kWidth; ++lane) {
            if (empty_lanes[lane] != 0) {
                return -1;
            }
            if (matching_lanes[lane] != 0) {
                const std::size_t slot = (base + lane) & mask;
                if (hashes[slot] == hash && keys[slot] == key
                        && namespaces[slot] == name_space) {
                    return static_cast<int>(slot);
                }
            }
        }
    }
    return -1;
}

}  // namespace cachekit
