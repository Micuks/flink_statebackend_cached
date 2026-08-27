#include "probe.h"

#include <arm_sve.h>

namespace cachekit {

int FindSlotSve(
        const std::uint8_t* control,
        const std::uint64_t* hashes,
        const std::uint64_t* keys,
        const std::uint64_t* namespaces,
        std::size_t capacity,
        std::uint64_t hash,
        std::uint8_t fingerprint,
        std::uint64_t key,
        std::uint64_t name_space) {
    const std::size_t width = svcntb();
    const std::size_t mask = capacity - 1;
    const std::size_t start = hash & mask;
    const svbool_t all = svptrue_b8();
    alignas(64) std::uint8_t empty_lanes[256];
    alignas(64) std::uint8_t matching_lanes[256];

    for (std::size_t probes = 0; probes < capacity; probes += width) {
        const std::size_t base = (start + probes) & mask;
        const svuint8_t markers = svld1_u8(all, control + base);
        const svbool_t empty = svcmpeq_n_u8(all, markers, 0);
        const svbool_t matching = svcmpeq_n_u8(all, markers, fingerprint);
        svst1_u8(all, empty_lanes, svdup_u8_z(empty, 1));
        svst1_u8(all, matching_lanes, svdup_u8_z(matching, 1));
        for (std::size_t lane = 0; lane < width; ++lane) {
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
