#include "cachekit_byte_snapshot_table.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#if defined(__linux__) && defined(__aarch64__)
#include <arm_acle.h>
#include <asm/hwcap.h>
#include <sys/auxv.h>
#endif

namespace cachekit {
namespace {

constexpr std::uint8_t kSlotEmpty = 0;
constexpr std::uint8_t kSlotDeleted = 1;
constexpr std::size_t kMinimumTableCapacity = 256;
constexpr std::uint64_t kMidrImplementerMask = UINT64_C(0xff000000);
constexpr std::uint64_t kMidrPartMask = UINT64_C(0x0000fff0);
constexpr std::uint64_t kHiSiliconImplementer = UINT64_C(0x48000000);
constexpr std::uint64_t kHiSiliconTsv110 = UINT64_C(0x0000d010);
constexpr std::uint64_t kHiSiliconHip09 = UINT64_C(0x0000d020);

#if CACHEKIT_ENABLE_KUNPENG_SNAPSHOT \
        && defined(__linux__) && defined(__aarch64__) && defined(HWCAP_CRC32)
bool IsKunpengSnapshotTarget() {
    std::ifstream midr_file(
            "/sys/devices/system/cpu/cpu0/regs/identification/midr_el1");
    std::string value;
    if (midr_file >> value) {
        char* end = nullptr;
        const std::uint64_t midr = std::strtoull(value.c_str(), &end, 0);
        if (end != value.c_str() && *end == '\0') {
            const std::uint64_t implementer = midr & kMidrImplementerMask;
            const std::uint64_t part = midr & kMidrPartMask;
            return implementer == kHiSiliconImplementer
                    && (part == kHiSiliconTsv110 || part == kHiSiliconHip09);
        }
    }

    std::ifstream cpuinfo("/proc/cpuinfo");
    bool hi_silicon = false;
    bool supported_part = false;
    std::string line;
    while (std::getline(cpuinfo, line)) {
        if (line.find("CPU implementer") != std::string::npos
                && line.find("0x48") != std::string::npos) {
            hi_silicon = true;
        }
        if (line.find("CPU part") != std::string::npos
                && (line.find("0xd01") != std::string::npos
                    || line.find("0xd02") != std::string::npos)) {
            supported_part = true;
        }
    }
    return hi_silicon && supported_part;
}
#endif

std::uint64_t Avalanche(std::uint64_t hash) {
    hash ^= hash >> 30;
    hash *= UINT64_C(0xbf58476d1ce4e5b9);
    hash ^= hash >> 27;
    hash *= UINT64_C(0x94d049bb133111eb);
    return hash ^ (hash >> 31);
}

std::uint64_t HashByteKeyFnv(const std::uint8_t* bytes, std::size_t size) {
    std::uint64_t hash = UINT64_C(1469598103934665603);
    for (std::size_t index = 0; index < size; ++index) {
        hash ^= bytes[index];
        hash *= UINT64_C(1099511628211);
    }
    return Avalanche(hash);
}

bool KunpengCrc32Available() {
#if defined(CACHEKIT_FORCE_FNV_HASH)
    return false;
#else
    return KunpengSnapshotFeatureAvailable();
#endif
}

std::uint64_t HashTableKey(
        const std::uint8_t* bytes,
        std::size_t size,
        bool use_kunpeng_crc32) {
#if defined(__linux__) && defined(__aarch64__)
    if (use_kunpeng_crc32 && size == 16) {
        std::uint64_t first;
        std::uint64_t second;
        std::memcpy(&first, bytes, sizeof(first));
        std::memcpy(&second, bytes + sizeof(first), sizeof(second));
        std::uint32_t crc = __crc32cd(UINT32_C(0x9e3779b9), first);
        crc = __crc32cd(crc, second);
        return Avalanche(crc);
    }
#else
    (void) use_kunpeng_crc32;
#endif
    return HashByteKeyFnv(bytes, size);
}

}  // namespace

std::uint64_t HashByteKey(const std::uint8_t* bytes, std::size_t size) {
    return HashByteKeyFnv(bytes, size);
}

bool KunpengSnapshotFeatureAvailable() {
#if CACHEKIT_ENABLE_KUNPENG_SNAPSHOT \
        && defined(__linux__) && defined(__aarch64__) && defined(HWCAP_CRC32)
    static const bool available =
            IsKunpengSnapshotTarget() && (getauxval(AT_HWCAP) & HWCAP_CRC32) != 0;
    return available;
#else
    return false;
#endif
}

namespace {

std::uint8_t Fingerprint(std::uint64_t hash) {
    return static_cast<std::uint8_t>(2 + ((hash >> 57) & 0x7f));
}

std::size_t TableCapacity(std::size_t max_entries) {
    if (max_entries == 0
            || max_entries > static_cast<std::size_t>(std::numeric_limits<int>::max() / 2)) {
        throw std::invalid_argument("max_entries must be in [1, INT_MAX/2]");
    }
    const std::size_t target = std::max(kMinimumTableCapacity, max_entries * 2);
    std::size_t capacity = 1;
    while (capacity < target) {
        capacity <<= 1;
    }
    return capacity;
}

}  // namespace

class ByteSnapshotTable::Impl {
public:
    explicit Impl(std::size_t max_entries)
            : max_entries_(max_entries),
              capacity_(TableCapacity(max_entries)),
              mask_(capacity_ - 1),
              use_kunpeng_crc32_(KunpengCrc32Available()),
              control_(capacity_, kSlotEmpty),
              hashes_(capacity_),
              keys_(capacity_),
              kinds_(capacity_),
              payloads_(capacity_),
              lru_prev_(capacity_, -1),
              lru_next_(capacity_, -1) {}

    PutResult Put(
            const std::uint8_t* key,
            std::size_t key_size,
            SnapshotKind kind,
            const std::uint8_t* payload,
            std::size_t payload_size,
            std::vector<std::uint8_t>* displaced_payload,
            std::uint64_t* displaced_hash) {
        if (displaced_payload != nullptr) {
            displaced_payload->clear();
        }
        if (displaced_hash != nullptr) {
            *displaced_hash = 0;
        }
        if (key == nullptr || key_size == 0
                || (kind == SnapshotKind::kSingle && (payload == nullptr || payload_size == 0))) {
            return PutResult::kRejected;
        }
        const std::uint64_t hash = TableHash(key, key_size);
        int slot = Find(key, key_size, hash);
        if (slot >= 0) {
            MovePayload(slot, displaced_payload);
            kinds_[slot] = static_cast<std::uint8_t>(kind);
            Assign(payloads_[slot], payload, payload_size);
            Touch(slot);
            return PutResult::kUpdated;
        }

        bool evicted = false;
        if (size_ == max_entries_) {
            if (displaced_hash != nullptr) {
                const std::vector<std::uint8_t>& displaced_key = keys_[lru_head_];
                *displaced_hash = HashByteKey(displaced_key.data(), displaced_key.size());
            }
            RemoveSlot(lru_head_, displaced_payload);
            evicted = true;
        }
        MaybeRebuild();
        slot = FindInsertionSlot(hash);
        if (slot < 0) {
            return PutResult::kRejected;
        }
        const std::size_t index = static_cast<std::size_t>(slot);
        if (control_[index] == kSlotDeleted) {
            --deleted_;
        }
        control_[index] = Fingerprint(hash);
        hashes_[index] = hash;
        keys_[index].assign(key, key + key_size);
        kinds_[index] = static_cast<std::uint8_t>(kind);
        Assign(payloads_[index], payload, payload_size);
        Append(slot);
        ++size_;
        return evicted ? PutResult::kInsertedWithEviction : PutResult::kInserted;
    }

    ByteLookupResult Lookup(const std::uint8_t* key, std::size_t key_size) {
        if (key == nullptr || key_size == 0) {
            return {false, SnapshotKind::kEmpty, nullptr, 0};
        }
        const std::uint64_t hash = TableHash(key, key_size);
        const int slot = Find(key, key_size, hash);
        if (slot < 0) {
            return {false, SnapshotKind::kEmpty, nullptr, 0};
        }
        Touch(slot);
        const std::vector<std::uint8_t>& payload = payloads_[slot];
        return {
                true,
                static_cast<SnapshotKind>(kinds_[slot]),
                payload.empty() ? nullptr : payload.data(),
                payload.size()};
    }

    bool Remove(
            const std::uint8_t* key,
            std::size_t key_size,
            std::vector<std::uint8_t>* removed_payload) {
        if (removed_payload != nullptr) {
            removed_payload->clear();
        }
        if (key == nullptr || key_size == 0) {
            return false;
        }
        const std::uint64_t hash = TableHash(key, key_size);
        const int slot = Find(key, key_size, hash);
        if (slot < 0) {
            return false;
        }
        RemoveSlot(slot, removed_payload);
        MaybeRebuild();
        return true;
    }

    void Clear(std::vector<std::vector<std::uint8_t>>* removed_payloads) {
        if (removed_payloads != nullptr) {
            removed_payloads->clear();
        }
        std::fill(control_.begin(), control_.end(), kSlotEmpty);
        for (std::size_t slot = 0; slot < capacity_; ++slot) {
            keys_[slot].clear();
            if (removed_payloads != nullptr && !payloads_[slot].empty()) {
                removed_payloads->push_back(std::move(payloads_[slot]));
            } else {
                payloads_[slot].clear();
            }
        }
        std::fill(lru_prev_.begin(), lru_prev_.end(), -1);
        std::fill(lru_next_.begin(), lru_next_.end(), -1);
        lru_head_ = -1;
        lru_tail_ = -1;
        size_ = 0;
        deleted_ = 0;
    }

    std::size_t size() const { return size_; }
    const char* hash_name() const { return use_kunpeng_crc32_ ? "crc32c-16" : "fnv64"; }

private:
    std::uint64_t TableHash(const std::uint8_t* key, std::size_t key_size) const {
        return HashTableKey(key, key_size, use_kunpeng_crc32_);
    }

    static void Assign(
            std::vector<std::uint8_t>& destination,
            const std::uint8_t* source,
            std::size_t size) {
        if (size == 0) {
            destination.clear();
        } else {
            destination.assign(source, source + size);
        }
    }

    int Find(
            const std::uint8_t* key,
            std::size_t key_size,
            std::uint64_t hash) const {
        const std::uint8_t fingerprint = Fingerprint(hash);
        std::size_t slot = hash & mask_;
        for (std::size_t probes = 0; probes < capacity_; ++probes) {
            const std::uint8_t marker = control_[slot];
            if (marker == kSlotEmpty) {
                return -1;
            }
            const std::vector<std::uint8_t>& stored = keys_[slot];
            if (marker == fingerprint && hashes_[slot] == hash
                    && stored.size() == key_size
                    && (key_size == 0
                        || std::memcmp(stored.data(), key, key_size) == 0)) {
                return static_cast<int>(slot);
            }
            slot = (slot + 1) & mask_;
        }
        return -1;
    }

    int FindInsertionSlot(std::uint64_t hash) const {
        std::size_t slot = hash & mask_;
        int first_deleted = -1;
        for (std::size_t probes = 0; probes < capacity_; ++probes) {
            const std::uint8_t marker = control_[slot];
            if (marker == kSlotEmpty) {
                return first_deleted >= 0 ? first_deleted : static_cast<int>(slot);
            }
            if (marker == kSlotDeleted && first_deleted < 0) {
                first_deleted = static_cast<int>(slot);
            }
            slot = (slot + 1) & mask_;
        }
        return first_deleted;
    }

    void Append(int slot) {
        lru_prev_[slot] = lru_tail_;
        lru_next_[slot] = -1;
        if (lru_tail_ >= 0) {
            lru_next_[lru_tail_] = slot;
        } else {
            lru_head_ = slot;
        }
        lru_tail_ = slot;
    }

    void Touch(int slot) {
        if (slot == lru_tail_) {
            return;
        }
        Unlink(slot);
        Append(slot);
    }

    void Unlink(int slot) {
        const int previous = lru_prev_[slot];
        const int next = lru_next_[slot];
        if (previous >= 0) {
            lru_next_[previous] = next;
        } else {
            lru_head_ = next;
        }
        if (next >= 0) {
            lru_prev_[next] = previous;
        } else {
            lru_tail_ = previous;
        }
        lru_prev_[slot] = -1;
        lru_next_[slot] = -1;
    }

    void MovePayload(int slot, std::vector<std::uint8_t>* removed_payload) {
        if (removed_payload != nullptr) {
            *removed_payload = std::move(payloads_[slot]);
        } else {
            payloads_[slot].clear();
        }
    }

    void RemoveSlot(int slot, std::vector<std::uint8_t>* removed_payload) {
        control_[static_cast<std::size_t>(slot)] = kSlotDeleted;
        Unlink(slot);
        keys_[slot].clear();
        MovePayload(slot, removed_payload);
        --size_;
        ++deleted_;
    }

    void MaybeRebuild() {
        const std::size_t threshold = std::max<std::size_t>(64, capacity_ / 8);
        if (deleted_ < threshold) {
            return;
        }

        std::vector<std::uint8_t> new_control(capacity_, kSlotEmpty);
        std::vector<std::uint64_t> new_hashes(capacity_);
        std::vector<std::vector<std::uint8_t>> new_keys(capacity_);
        std::vector<std::uint8_t> new_kinds(capacity_);
        std::vector<std::vector<std::uint8_t>> new_payloads(capacity_);
        std::vector<int> new_lru_prev(capacity_, -1);
        std::vector<int> new_lru_next(capacity_, -1);
        int new_head = -1;
        int new_tail = -1;

        for (int old_slot = lru_head_; old_slot >= 0; old_slot = lru_next_[old_slot]) {
            const std::uint64_t hash = hashes_[old_slot];
            std::size_t new_slot = hash & mask_;
            while (new_control[new_slot] != kSlotEmpty) {
                new_slot = (new_slot + 1) & mask_;
            }
            const std::uint8_t fingerprint = Fingerprint(hash);
            new_control[new_slot] = fingerprint;
            new_hashes[new_slot] = hash;
            new_keys[new_slot] = std::move(keys_[old_slot]);
            new_kinds[new_slot] = kinds_[old_slot];
            new_payloads[new_slot] = std::move(payloads_[old_slot]);
            new_lru_prev[new_slot] = new_tail;
            if (new_tail >= 0) {
                new_lru_next[new_tail] = static_cast<int>(new_slot);
            } else {
                new_head = static_cast<int>(new_slot);
            }
            new_tail = static_cast<int>(new_slot);
        }

        control_.swap(new_control);
        hashes_.swap(new_hashes);
        keys_.swap(new_keys);
        kinds_.swap(new_kinds);
        payloads_.swap(new_payloads);
        lru_prev_.swap(new_lru_prev);
        lru_next_.swap(new_lru_next);
        lru_head_ = new_head;
        lru_tail_ = new_tail;
        deleted_ = 0;
    }

    const std::size_t max_entries_;
    const std::size_t capacity_;
    const std::size_t mask_;
    const bool use_kunpeng_crc32_;
    std::vector<std::uint8_t> control_;
    std::vector<std::uint64_t> hashes_;
    std::vector<std::vector<std::uint8_t>> keys_;
    std::vector<std::uint8_t> kinds_;
    std::vector<std::vector<std::uint8_t>> payloads_;
    std::vector<int> lru_prev_;
    std::vector<int> lru_next_;
    int lru_head_ = -1;
    int lru_tail_ = -1;
    std::size_t size_ = 0;
    std::size_t deleted_ = 0;
};

ByteSnapshotTable::ByteSnapshotTable(std::size_t max_entries)
        : impl_(new Impl(max_entries)) {}

ByteSnapshotTable::~ByteSnapshotTable() = default;

PutResult ByteSnapshotTable::Put(
        const std::uint8_t* key,
        std::size_t key_size,
        SnapshotKind kind,
        const std::uint8_t* payload,
        std::size_t payload_size,
        std::vector<std::uint8_t>* displaced_payload,
        std::uint64_t* displaced_hash) {
    return impl_->Put(
            key, key_size, kind, payload, payload_size, displaced_payload, displaced_hash);
}

ByteLookupResult ByteSnapshotTable::Lookup(const std::uint8_t* key, std::size_t key_size) {
    return impl_->Lookup(key, key_size);
}

bool ByteSnapshotTable::Remove(
        const std::uint8_t* key,
        std::size_t key_size,
        std::vector<std::uint8_t>* removed_payload) {
    return impl_->Remove(key, key_size, removed_payload);
}

void ByteSnapshotTable::Clear(
        std::vector<std::vector<std::uint8_t>>* removed_payloads) {
    impl_->Clear(removed_payloads);
}

std::size_t ByteSnapshotTable::size() const {
    return impl_->size();
}

const char* ByteSnapshotTable::hash_name() const {
    return impl_->hash_name();
}

}  // namespace cachekit
