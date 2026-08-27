#ifndef CACHEKIT_BYTE_SNAPSHOT_PROBE_H
#define CACHEKIT_BYTE_SNAPSHOT_PROBE_H

#include <cstddef>
#include <cstdint>
#include <vector>

namespace cachekit {

using ByteFindSlotFunction = int (*)(
        const std::uint8_t* control,
        const std::uint64_t* hashes,
        const std::vector<std::uint8_t>* keys,
        std::size_t capacity,
        std::uint64_t hash,
        std::uint8_t fingerprint,
        const std::uint8_t* key,
        std::size_t key_size);

int FindByteSlotScalar(
        const std::uint8_t* control,
        const std::uint64_t* hashes,
        const std::vector<std::uint8_t>* keys,
        std::size_t capacity,
        std::uint64_t hash,
        std::uint8_t fingerprint,
        const std::uint8_t* key,
        std::size_t key_size);

int FindByteSlotNeon(
        const std::uint8_t* control,
        const std::uint64_t* hashes,
        const std::vector<std::uint8_t>* keys,
        std::size_t capacity,
        std::uint64_t hash,
        std::uint8_t fingerprint,
        const std::uint8_t* key,
        std::size_t key_size);

int FindByteSlotSve(
        const std::uint8_t* control,
        const std::uint64_t* hashes,
        const std::vector<std::uint8_t>* keys,
        std::size_t capacity,
        std::uint64_t hash,
        std::uint8_t fingerprint,
        const std::uint8_t* key,
        std::size_t key_size);

}  // namespace cachekit

#endif
