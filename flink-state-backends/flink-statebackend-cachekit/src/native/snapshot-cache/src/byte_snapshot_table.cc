#include "cachekit_byte_snapshot_table.h"

#include "byte_probe.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

namespace cachekit {
namespace {

constexpr std::uint8_t kSlotEmpty = 0;
constexpr std::uint8_t kSlotDeleted = 1;
constexpr std::size_t kNeonBytes = 16;
constexpr std::size_t kMaximumVectorBytes = 256;

std::uint64_t HashBytes(const std::uint8_t* bytes, std::size_t size) {
    std::uint64_t hash = UINT64_C(1469598103934665603);
    for (std::size_t index = 0; index < size; ++index) {
        hash ^= bytes[index];
        hash *= UINT64_C(1099511628211);
    }
    hash ^= hash >> 30;
    hash *= UINT64_C(0xbf58476d1ce4e5b9);
    hash ^= hash >> 27;
    hash *= UINT64_C(0x94d049bb133111eb);
    return hash ^ (hash >> 31);
}

std::uint8_t Fingerprint(std::uint64_t hash) {
    return static_cast<std::uint8_t>(2 + ((hash >> 57) & 0x7f));
}

std::size_t TableCapacity(std::size_t max_entries) {
    if (max_entries == 0
            || max_entries > static_cast<std::size_t>(std::numeric_limits<int>::max() / 2)) {
        throw std::invalid_argument("max_entries must be in [1, INT_MAX/2]");
    }
    const std::size_t target = std::max(kMaximumVectorBytes, max_entries * 2);
    std::size_t capacity = 1;
    while (capacity < target) {
        capacity <<= 1;
    }
    return capacity;
}

ProbeKernel SelectKernel(ProbeKernel requested) {
    if (requested == ProbeKernel::kAuto) {
        if (SveAvailable() && SveVectorBytes() != 0) {
            return ProbeKernel::kSve;
        }
        return NeonAvailable() ? ProbeKernel::kNeon : ProbeKernel::kScalar;
    }
    if (requested == ProbeKernel::kSve && (!SveAvailable() || SveVectorBytes() == 0)) {
        throw std::invalid_argument("SVE was requested but is unavailable");
    }
    if (requested == ProbeKernel::kNeon && !NeonAvailable()) {
        throw std::invalid_argument("NEON was requested but is unavailable");
    }
    return requested;
}

std::size_t VectorBytes(ProbeKernel kernel) {
    if (kernel == ProbeKernel::kSve) {
        return SveVectorBytes();
    }
    return kernel == ProbeKernel::kNeon ? kNeonBytes : 1;
}

ByteFindSlotFunction FindFunction(ProbeKernel kernel) {
    if (kernel == ProbeKernel::kSve) {
        return FindByteSlotSve;
    }
    return kernel == ProbeKernel::kNeon ? FindByteSlotNeon : FindByteSlotScalar;
}

const char* KernelName(ProbeKernel kernel) {
    switch (kernel) {
        case ProbeKernel::kScalar:
            return "scalar";
        case ProbeKernel::kNeon:
            return "neon";
        case ProbeKernel::kSve:
            return "sve";
        case ProbeKernel::kAuto:
            break;
    }
    return "invalid";
}

}  // namespace

#if !defined(CACHEKIT_HAS_SVE_OBJECT)
int FindByteSlotSve(
        const std::uint8_t*,
        const std::uint64_t*,
        const std::vector<std::uint8_t>*,
        std::size_t,
        std::uint64_t,
        std::uint8_t,
        const std::uint8_t*,
        std::size_t) {
    return -1;
}
#endif

class ByteSnapshotTable::Impl {
public:
    Impl(std::size_t max_entries, ProbeKernel requested_kernel)
            : max_entries_(max_entries),
              capacity_(TableCapacity(max_entries)),
              mask_(capacity_ - 1),
              kernel_(SelectKernel(requested_kernel)),
              vector_bytes_(VectorBytes(kernel_)),
              find_slot_(FindFunction(kernel_)),
              control_(capacity_ + vector_bytes_, kSlotEmpty),
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
            std::vector<std::uint8_t>* displaced_payload) {
        if (displaced_payload != nullptr) {
            displaced_payload->clear();
        }
        if (key == nullptr || key_size == 0
                || (kind == SnapshotKind::kSingle && (payload == nullptr || payload_size == 0))) {
            return PutResult::kRejected;
        }
        const std::uint64_t hash = HashBytes(key, key_size);
        int slot = Find(key, key_size, hash);
        if (slot >= 0) {
            MovePayload(slot, displaced_payload);
            kinds_[slot] = static_cast<std::uint8_t>(kind);
            Assign(payloads_[slot], payload, payload_size);
            Touch(slot);
            return PutResult::kStored;
        }

        bool evicted = false;
        if (size_ == max_entries_) {
            RemoveSlot(lru_head_, displaced_payload);
            evicted = true;
        }
        slot = FindInsertionSlot(hash);
        if (slot < 0) {
            return PutResult::kRejected;
        }
        const std::size_t index = static_cast<std::size_t>(slot);
        SetControl(index, Fingerprint(hash));
        hashes_[index] = hash;
        keys_[index].assign(key, key + key_size);
        kinds_[index] = static_cast<std::uint8_t>(kind);
        Assign(payloads_[index], payload, payload_size);
        Append(slot);
        ++size_;
        return evicted ? PutResult::kStoredWithEviction : PutResult::kStored;
    }

    ByteLookupResult Lookup(const std::uint8_t* key, std::size_t key_size) {
        if (key == nullptr || key_size == 0) {
            return {false, SnapshotKind::kEmpty, nullptr, 0};
        }
        const std::uint64_t hash = HashBytes(key, key_size);
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
        const std::uint64_t hash = HashBytes(key, key_size);
        const int slot = Find(key, key_size, hash);
        if (slot < 0) {
            return false;
        }
        RemoveSlot(slot, removed_payload);
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
    }

    std::size_t size() const { return size_; }
    ProbeKernel active_kernel() const { return kernel_; }
    std::size_t vector_bytes() const { return vector_bytes_; }
    const char* active_kernel_name() const { return KernelName(kernel_); }

private:
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
        return find_slot_(
                control_.data(),
                hashes_.data(),
                keys_.data(),
                capacity_,
                hash,
                Fingerprint(hash),
                key,
                key_size);
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

    void SetControl(std::size_t slot, std::uint8_t value) {
        control_[slot] = value;
        if (slot < vector_bytes_) {
            control_[capacity_ + slot] = value;
        }
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
        SetControl(static_cast<std::size_t>(slot), kSlotDeleted);
        Unlink(slot);
        keys_[slot].clear();
        MovePayload(slot, removed_payload);
        --size_;
    }

    const std::size_t max_entries_;
    const std::size_t capacity_;
    const std::size_t mask_;
    const ProbeKernel kernel_;
    const std::size_t vector_bytes_;
    const ByteFindSlotFunction find_slot_;
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
};

ByteSnapshotTable::ByteSnapshotTable(
        std::size_t max_entries,
        ProbeKernel requested_kernel)
        : impl_(new Impl(max_entries, requested_kernel)) {}

ByteSnapshotTable::~ByteSnapshotTable() = default;

PutResult ByteSnapshotTable::Put(
        const std::uint8_t* key,
        std::size_t key_size,
        SnapshotKind kind,
        const std::uint8_t* payload,
        std::size_t payload_size,
        std::vector<std::uint8_t>* displaced_payload) {
    return impl_->Put(
            key, key_size, kind, payload, payload_size, displaced_payload);
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

ProbeKernel ByteSnapshotTable::active_kernel() const {
    return impl_->active_kernel();
}

std::size_t ByteSnapshotTable::vector_bytes() const {
    return impl_->vector_bytes();
}

const char* ByteSnapshotTable::active_kernel_name() const {
    return impl_->active_kernel_name();
}

}  // namespace cachekit
