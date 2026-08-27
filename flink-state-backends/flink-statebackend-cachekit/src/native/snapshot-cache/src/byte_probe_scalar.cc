#include "byte_probe.h"

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

int FindByteSlotScalar(
        const std::uint8_t* control,
        const std::uint64_t* hashes,
        const std::vector<std::uint8_t>* keys,
        std::size_t capacity,
        std::uint64_t hash,
        std::uint8_t fingerprint,
        const std::uint8_t* key,
        std::size_t key_size) {
    const std::size_t mask = capacity - 1;
    std::size_t slot = hash & mask;
    for (std::size_t probes = 0; probes < capacity; ++probes) {
        const std::uint8_t marker = control[slot];
        if (marker == 0) {
            return -1;
        }
        if (marker == fingerprint && hashes[slot] == hash
                && SameKey(keys[slot], key, key_size)) {
            return static_cast<int>(slot);
        }
        slot = (slot + 1) & mask;
    }
    return -1;
}

}  // namespace cachekit
