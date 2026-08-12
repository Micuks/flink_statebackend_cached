#include "byte_probe.h"

#include <arm_neon.h>

#include <cstring>

namespace cachekit {
namespace {

bool SameKey(
        const std::vector<std::uint8_t>& stored,
        const std::uint8_t* key,
        std::size_t key_size) {
    return stored.size() == key_size
            && (key_size == 0 || std::memcmp(stored.data(), key, key_size) == 0);
}

}  // namespace

int FindByteSlotNeon(
        const std::uint8_t* control,
        const std::uint64_t* hashes,
        const std::vector<std::uint8_t>* keys,
        std::size_t capacity,
        std::uint64_t hash,
        std::uint8_t fingerprint,
        const std::uint8_t* key,
        std::size_t key_size) {
    constexpr std::size_t kWidth = 16;
    const std::size_t mask = capacity - 1;
    const std::size_t start = hash & mask;
    const uint8x16_t empty_value = vdupq_n_u8(0);
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
                if (hashes[slot] == hash && SameKey(keys[slot], key, key_size)) {
                    return static_cast<int>(slot);
                }
            }
        }
    }
    return -1;
}

}  // namespace cachekit
