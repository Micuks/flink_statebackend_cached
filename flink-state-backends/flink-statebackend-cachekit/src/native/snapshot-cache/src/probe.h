#ifndef CACHEKIT_SNAPSHOT_PROBE_H
#define CACHEKIT_SNAPSHOT_PROBE_H

#include <cstddef>
#include <cstdint>

namespace cachekit {

using FindSlotFunction = int (*)(
        const std::uint8_t* control,
        const std::uint64_t* hashes,
        const std::uint64_t* keys,
        const std::uint64_t* namespaces,
        std::size_t capacity,
        std::uint64_t hash,
        std::uint8_t fingerprint,
        std::uint64_t key,
        std::uint64_t name_space);

int FindSlotScalar(
        const std::uint8_t* control,
        const std::uint64_t* hashes,
        const std::uint64_t* keys,
        const std::uint64_t* namespaces,
        std::size_t capacity,
        std::uint64_t hash,
        std::uint8_t fingerprint,
        std::uint64_t key,
        std::uint64_t name_space);

int FindSlotNeon(
        const std::uint8_t* control,
        const std::uint64_t* hashes,
        const std::uint64_t* keys,
        const std::uint64_t* namespaces,
        std::size_t capacity,
        std::uint64_t hash,
        std::uint8_t fingerprint,
        std::uint64_t key,
        std::uint64_t name_space);

int FindSlotSve(
        const std::uint8_t* control,
        const std::uint64_t* hashes,
        const std::uint64_t* keys,
        const std::uint64_t* namespaces,
        std::size_t capacity,
        std::uint64_t hash,
        std::uint8_t fingerprint,
        std::uint64_t key,
        std::uint64_t name_space);

}  // namespace cachekit

#endif
