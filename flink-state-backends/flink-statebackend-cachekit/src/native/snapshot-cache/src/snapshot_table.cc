#include "cachekit_snapshot_table.h"

#include "probe.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <vector>

#if defined(__linux__) && defined(__aarch64__)
#include <asm/hwcap.h>
#include <sys/auxv.h>
#include <sys/prctl.h>
#endif

namespace cachekit {
namespace {

constexpr std::uint8_t kSlotEmpty = 0;
constexpr std::uint8_t kSlotDeleted = 1;
constexpr std::size_t kNeonBytes = 16;
constexpr std::size_t kMaximumVectorBytes = 256;

bool IsPowerOfTwo(std::size_t value) {
    return value != 0 && (value & (value - 1)) == 0;
}

std::uint64_t Mix64(std::uint64_t value) {
    value ^= value >> 30;
    value *= UINT64_C(0xbf58476d1ce4e5b9);
    value ^= value >> 27;
    value *= UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31);
}

std::uint64_t CompositeHash(std::uint64_t key, std::uint64_t name_space) {
    return Mix64(key ^ (Mix64(name_space) + UINT64_C(0x9e3779b97f4a7c15)));
}

std::uint8_t Fingerprint(std::uint64_t hash) {
    return static_cast<std::uint8_t>(2 + ((hash >> 57) & 0x7f));
}

}  // namespace

#if !defined(CACHEKIT_HAS_NEON_OBJECT)
int FindSlotNeon(
        const std::uint8_t*,
        const std::uint64_t*,
        const std::uint64_t*,
        const std::uint64_t*,
        std::size_t,
        std::uint64_t,
        std::uint8_t,
        std::uint64_t,
        std::uint64_t) {
    return -1;
}
#endif

#if !defined(CACHEKIT_HAS_SVE_OBJECT)
int FindSlotSve(
        const std::uint8_t*,
        const std::uint64_t*,
        const std::uint64_t*,
        const std::uint64_t*,
        std::size_t,
        std::uint64_t,
        std::uint8_t,
        std::uint64_t,
        std::uint64_t) {
    return -1;
}
#endif

bool NeonAvailable() {
#if defined(CACHEKIT_HAS_NEON_OBJECT) && defined(__linux__) && defined(__aarch64__)
    return (getauxval(AT_HWCAP) & HWCAP_ASIMD) != 0;
#else
    return false;
#endif
}

bool SveAvailable() {
#if defined(CACHEKIT_HAS_SVE_OBJECT) && defined(__linux__) && defined(__aarch64__) \
        && defined(HWCAP_SVE)
    return (getauxval(AT_HWCAP) & HWCAP_SVE) != 0;
#else
    return false;
#endif
}

std::size_t SveVectorBytes() {
#if defined(CACHEKIT_HAS_SVE_OBJECT) && defined(__linux__) && defined(__aarch64__) \
        && defined(PR_SVE_GET_VL) && defined(PR_SVE_VL_LEN_MASK)
    if (!SveAvailable()) {
        return 0;
    }
    const int result = prctl(PR_SVE_GET_VL);
    return result < 0 ? 0 : static_cast<std::size_t>(result & PR_SVE_VL_LEN_MASK);
#else
    return 0;
#endif
}

class SnapshotTable::Impl {
public:
    Impl(std::size_t requested_capacity, ProbeKernel requested_kernel)
            : capacity_(requested_capacity),
              mask_(requested_capacity - 1),
              kernel_(SelectKernel(requested_kernel)),
              vector_bytes_(VectorBytes(kernel_)),
              find_slot_(FindFunction(kernel_)),
              control_(capacity_ + vector_bytes_, kSlotEmpty),
              hashes_(capacity_),
              keys_(capacity_),
              namespaces_(capacity_),
              kinds_(capacity_),
              entry_ids_(capacity_),
              lru_prev_(capacity_, -1),
              lru_next_(capacity_, -1) {
        if (!IsPowerOfTwo(capacity_) || capacity_ < kMaximumVectorBytes
                || capacity_ > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
            throw std::invalid_argument("capacity must be a power of two in [256, INT_MAX]");
        }
    }

    bool Put(
            std::uint64_t key,
            std::uint64_t name_space,
            SnapshotKind kind,
            std::uint32_t entry_id) {
        const std::uint64_t hash = CompositeHash(key, name_space);
        const std::uint8_t fingerprint = Fingerprint(hash);
        int slot = Find(key, name_space, hash, fingerprint);
        if (slot >= 0) {
            kinds_[slot] = static_cast<std::uint8_t>(kind);
            entry_ids_[slot] = entry_id;
            Touch(slot);
            return true;
        }
        if ((size_ + 1) * 4 > capacity_ * 3) {
            return false;
        }
        slot = FindInsertionSlot(hash);
        if (slot < 0) {
            return false;
        }
        SetControl(static_cast<std::size_t>(slot), fingerprint);
        hashes_[slot] = hash;
        keys_[slot] = key;
        namespaces_[slot] = name_space;
        kinds_[slot] = static_cast<std::uint8_t>(kind);
        entry_ids_[slot] = entry_id;
        Append(slot);
        ++size_;
        return true;
    }

    LookupResult Lookup(std::uint64_t key, std::uint64_t name_space) {
        const std::uint64_t hash = CompositeHash(key, name_space);
        const int slot = Find(key, name_space, hash, Fingerprint(hash));
        if (slot < 0) {
            return {false, SnapshotKind::kEmpty, 0};
        }
        Touch(slot);
        return {true, static_cast<SnapshotKind>(kinds_[slot]), entry_ids_[slot]};
    }

    bool Remove(std::uint64_t key, std::uint64_t name_space) {
        const std::uint64_t hash = CompositeHash(key, name_space);
        const int slot = Find(key, name_space, hash, Fingerprint(hash));
        if (slot < 0) {
            return false;
        }
        SetControl(static_cast<std::size_t>(slot), kSlotDeleted);
        Unlink(slot);
        --size_;
        return true;
    }

    void Clear() {
        std::fill(control_.begin(), control_.end(), kSlotEmpty);
        std::fill(lru_prev_.begin(), lru_prev_.end(), -1);
        std::fill(lru_next_.begin(), lru_next_.end(), -1);
        lru_head_ = -1;
        lru_tail_ = -1;
        size_ = 0;
    }

    std::size_t size() const { return size_; }
    std::size_t capacity() const { return capacity_; }
    ProbeKernel active_kernel() const { return kernel_; }
    std::size_t vector_bytes() const { return vector_bytes_; }

    const char* active_kernel_name() const {
        switch (kernel_) {
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

private:
    static ProbeKernel SelectKernel(ProbeKernel requested) {
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

    static std::size_t VectorBytes(ProbeKernel kernel) {
        if (kernel == ProbeKernel::kSve) {
            return SveVectorBytes();
        }
        return kernel == ProbeKernel::kNeon ? kNeonBytes : 1;
    }

    static FindSlotFunction FindFunction(ProbeKernel kernel) {
        if (kernel == ProbeKernel::kSve) {
            return FindSlotSve;
        }
        return kernel == ProbeKernel::kNeon ? FindSlotNeon : FindSlotScalar;
    }

    int Find(
            std::uint64_t key,
            std::uint64_t name_space,
            std::uint64_t hash,
            std::uint8_t fingerprint) const {
        return find_slot_(
                control_.data(),
                hashes_.data(),
                keys_.data(),
                namespaces_.data(),
                capacity_,
                hash,
                fingerprint,
                key,
                name_space);
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

    const std::size_t capacity_;
    const std::size_t mask_;
    const ProbeKernel kernel_;
    const std::size_t vector_bytes_;
    const FindSlotFunction find_slot_;
    std::vector<std::uint8_t> control_;
    std::vector<std::uint64_t> hashes_;
    std::vector<std::uint64_t> keys_;
    std::vector<std::uint64_t> namespaces_;
    std::vector<std::uint8_t> kinds_;
    std::vector<std::uint32_t> entry_ids_;
    std::vector<int> lru_prev_;
    std::vector<int> lru_next_;
    int lru_head_ = -1;
    int lru_tail_ = -1;
    std::size_t size_ = 0;
};

SnapshotTable::SnapshotTable(std::size_t capacity, ProbeKernel requested_kernel)
        : impl_(new Impl(capacity, requested_kernel)) {}

SnapshotTable::~SnapshotTable() = default;

bool SnapshotTable::Put(
        std::uint64_t key,
        std::uint64_t name_space,
        SnapshotKind kind,
        std::uint32_t entry_id) {
    return impl_->Put(key, name_space, kind, entry_id);
}

LookupResult SnapshotTable::Lookup(std::uint64_t key, std::uint64_t name_space) {
    return impl_->Lookup(key, name_space);
}

bool SnapshotTable::Remove(std::uint64_t key, std::uint64_t name_space) {
    return impl_->Remove(key, name_space);
}

void SnapshotTable::Clear() {
    impl_->Clear();
}

std::size_t SnapshotTable::size() const {
    return impl_->size();
}

std::size_t SnapshotTable::capacity() const {
    return impl_->capacity();
}

ProbeKernel SnapshotTable::active_kernel() const {
    return impl_->active_kernel();
}

std::size_t SnapshotTable::vector_bytes() const {
    return impl_->vector_bytes();
}

const char* SnapshotTable::active_kernel_name() const {
    return impl_->active_kernel_name();
}

}  // namespace cachekit
