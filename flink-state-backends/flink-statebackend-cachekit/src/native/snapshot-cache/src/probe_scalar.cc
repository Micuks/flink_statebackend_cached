#include "probe.h"

namespace cachekit {

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
        if (marker == 0) {
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

}  // namespace cachekit
