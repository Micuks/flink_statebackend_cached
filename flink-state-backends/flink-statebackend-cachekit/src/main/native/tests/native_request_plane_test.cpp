/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "cachekit/native_request_plane.h"
#include "cachekit/native_request_plane_c.h"

#include "kernels_internal.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

using cachekit::native::ErrorCode;
using cachekit::native::FillResult;
using cachekit::native::FillStatus;
using cachekit::native::FillView;
using cachekit::native::KernelKind;
using cachekit::native::KernelPreference;
using cachekit::native::KeyView;
using cachekit::native::Options;
using cachekit::native::ProbeResult;
using cachekit::native::ProbeStatus;
using cachekit::native::RequestPlane;

#define CHECK(condition)                                                       \
    do {                                                                       \
        if (!(condition)) {                                                     \
            std::cerr << __FILE__ << ':' << __LINE__                           \
                      << ": CHECK failed: " #condition << std::endl;           \
            std::exit(1);                                                       \
        }                                                                      \
    } while (false)

std::unique_ptr<RequestPlane> MakePlane(Options options = Options{}) {
    ErrorCode error = ErrorCode::kInternal;
    std::string message;
    std::unique_ptr<RequestPlane> plane =
            RequestPlane::Create(options, &error, &message);
    if (!plane) {
        std::cerr << "plane creation failed: "
                  << cachekit::native::ErrorCodeName(error) << ": " << message
                  << std::endl;
        std::exit(1);
    }
    return plane;
}

KeyView Key(
        std::uint32_t state_id,
        std::uint64_t generation,
        const std::string& bytes) {
    return KeyView{
            state_id,
            generation,
            reinterpret_cast<const std::uint8_t*>(bytes.data()),
            bytes.size()};
}

FillResult Fill(
        RequestPlane& plane,
        const KeyView& key,
        const std::string& value,
        bool negative = false) {
    const FillView fill{
            key,
            reinterpret_cast<const std::uint8_t*>(value.data()),
            value.size(),
            negative};
    FillResult result;
    CHECK(plane.FillBatch(&fill, &result, 1) == ErrorCode::kOk);
    return result;
}

ProbeResult Probe(RequestPlane& plane, const KeyView& key) {
    ProbeResult result;
    CHECK(plane.ProbeBatch(&key, &result, 1) == ErrorCode::kOk);
    return result;
}

std::string ResultValue(const ProbeResult& result) {
    if (result.value_size == 0) {
        return "";
    }
    return std::string(
            reinterpret_cast<const char*>(result.value), result.value_size);
}

void TestBucketLayoutAndForcedScalar() {
    using cachekit::native::internal::Bucket;
    CHECK(sizeof(Bucket) == 128);
    CHECK(alignof(Bucket) == 128);

    Options options;
    options.kernel = KernelPreference::kScalar;
    std::unique_ptr<RequestPlane> plane = MakePlane(options);
    CHECK(plane->kernel_kind() == KernelKind::kScalar);
    CHECK(std::string(plane->kernel_name()) == "scalar-crc32c");
    CHECK(plane->min_native_batch_size() == 64);
    CHECK(plane->capacity() == options.capacity_entries);

    CHECK(plane->ProbeBatch(nullptr, nullptr, 0) == ErrorCode::kOk);
    CHECK(plane->FillBatch(nullptr, nullptr, 0) == ErrorCode::kOk);
    CHECK(plane->ProbeBatch(nullptr, nullptr, 1) ==
          ErrorCode::kInvalidArgument);
    CHECK(plane->FillBatch(nullptr, nullptr, 1) ==
          ErrorCode::kInvalidArgument);
}

#if defined(CACHEKIT_NATIVE_AARCH64_KERNELS)
void CheckKernelMatchesScalar(
        const cachekit::native::internal::KernelOps& candidate) {
    const auto& scalar = cachekit::native::internal::ScalarKernel();
    const std::vector<std::string> values = {
            "", "a", "seven77", std::string(31, 'x'), std::string(129, 'y')};
    for (std::uint32_t state_id : {0U, 1U, 0xfeedbeefU}) {
        for (const std::string& value : values) {
            const auto* bytes =
                    reinterpret_cast<const std::uint8_t*>(value.data());
            CHECK(candidate.fingerprint(state_id, bytes, value.size()) ==
                  scalar.fingerprint(state_id, bytes, value.size()));
            CHECK(candidate.equal_bytes(bytes, bytes, value.size()));
            if (!value.empty()) {
                std::string changed = value;
                changed[value.size() / 2U] ^= 1;
                CHECK(!candidate.equal_bytes(
                        bytes,
                        reinterpret_cast<const std::uint8_t*>(changed.data()),
                        value.size()));
            }
        }
    }

    alignas(128) cachekit::native::internal::Bucket bucket{};
    for (std::size_t index = 0;
         index < cachekit::native::internal::kSlotsPerBucket;
         ++index) {
        bucket.tags[index] = (index % 3U) == 0 ? 42U
                                               : static_cast<std::uint32_t>(index);
    }
    CHECK(candidate.match_tags(bucket, 42U) == scalar.match_tags(bucket, 42U));
}

void TestAvailableAarch64KernelsMatchScalar() {
    const auto features = cachekit::native::DetectHostFeatures();
    if (features.neon && features.crc32) {
        CheckKernelMatchesScalar(cachekit::native::internal::NeonCrcKernel());
    }
    if (features.sve && features.crc32 && features.sve_vector_length_256) {
        CheckKernelMatchesScalar(cachekit::native::internal::Sve256Kernel());
    }
}
#endif

void TestCollisionRequiresExactCompare() {
    Options options;
    options.capacity_entries = 8;
    options.key_arena_bytes = 256;
    options.value_arena_bytes = 256;
    options.kernel = KernelPreference::kScalar;
    options.fingerprint_mask = 0;  // Every key receives the same stored tag.
    std::unique_ptr<RequestPlane> plane = MakePlane(options);

    const std::string alpha = "alpha";
    const std::string beta = "beta";
    const std::string gamma = "alpha";
    const std::string one = "one";
    const std::string two = "two";
    const std::string three = "different-state";
    CHECK(Fill(*plane, Key(7, 3, alpha), one).status == FillStatus::kInserted);
    CHECK(Fill(*plane, Key(7, 3, beta), two).status == FillStatus::kInserted);
    CHECK(Fill(*plane, Key(8, 3, gamma), three).status == FillStatus::kInserted);

    const ProbeResult alpha_result = Probe(*plane, Key(7, 3, alpha));
    CHECK(alpha_result.status == ProbeStatus::kHit);
    CHECK(ResultValue(alpha_result) == one);
    const ProbeResult beta_result = Probe(*plane, Key(7, 3, beta));
    CHECK(beta_result.status == ProbeStatus::kHit);
    CHECK(ResultValue(beta_result) == two);
    const ProbeResult state_result = Probe(*plane, Key(8, 3, gamma));
    CHECK(state_result.status == ProbeStatus::kHit);
    CHECK(ResultValue(state_result) == three);
    CHECK(Probe(*plane, Key(7, 3, std::string("missing"))).status ==
          ProbeStatus::kMiss);
}

void TestGenerationAndNegativeEntries() {
    Options options;
    options.capacity_entries = 4;
    options.key_arena_bytes = 64;
    options.value_arena_bytes = 64;
    options.kernel = KernelPreference::kScalar;
    std::unique_ptr<RequestPlane> plane = MakePlane(options);

    const std::string key = "account";
    CHECK(Fill(*plane, Key(1, 2, key), std::string("v2")).status ==
          FillStatus::kInserted);
    CHECK(Probe(*plane, Key(1, 1, key)).status == ProbeStatus::kMiss);
    CHECK(ResultValue(Probe(*plane, Key(1, 2, key))) == "v2");
    CHECK(ResultValue(Probe(*plane, Key(1, cachekit::native::kLatestGeneration, key))) ==
          "v2");

    const FillResult stale = Fill(*plane, Key(1, 1, key), std::string("stale"));
    CHECK(stale.status == FillStatus::kRejectedStaleGeneration);
    CHECK(stale.error == ErrorCode::kOk);
    CHECK(ResultValue(Probe(*plane, Key(1, 2, key))) == "v2");

    CHECK(Fill(*plane, Key(1, 3, key), std::string(), true).status ==
          FillStatus::kUpdated);
    CHECK(Probe(*plane, Key(1, 2, key)).status == ProbeStatus::kMiss);
    const ProbeResult negative =
            Probe(*plane, Key(1, cachekit::native::kLatestGeneration, key));
    CHECK(negative.status == ProbeStatus::kNegative);
    CHECK(negative.value == nullptr);
    CHECK(negative.value_size == 0);

    CHECK(Fill(*plane, Key(1, 4, key), std::string()).status ==
          FillStatus::kUpdated);
    const ProbeResult empty_positive = Probe(*plane, Key(1, 4, key));
    CHECK(empty_positive.status == ProbeStatus::kHit);
    CHECK(empty_positive.value_size == 0);

    // A write-heavy exact-key trace must not invalidate an unrelated key. The explicit latest
    // sentinel observes that key's newest write, while ordinary generation mismatches remain misses.
    const std::string stable_key = "stable";
    CHECK(Fill(*plane, Key(1, 4, stable_key), std::string("stable-value")).status ==
          FillStatus::kInserted);
    for (std::uint64_t epoch = 5; epoch <= 100; ++epoch) {
        CHECK(Fill(*plane, Key(1, epoch, key), std::to_string(epoch)).status ==
              FillStatus::kUpdated);
        CHECK(ResultValue(
                      Probe(*plane,
                            Key(1,
                                cachekit::native::kLatestGeneration,
                                stable_key))) ==
              "stable-value");
    }
    const FillResult old_async_fill =
            Fill(*plane, Key(1, 50, key), std::string("old-async"));
    CHECK(old_async_fill.status == FillStatus::kRejectedStaleGeneration);
    CHECK(Probe(*plane, Key(1, 101, key)).status == ProbeStatus::kMiss);
    CHECK(ResultValue(
                  Probe(*plane,
                        Key(1, cachekit::native::kLatestGeneration, key))) ==
          "100");
    CHECK(Fill(*plane,
               Key(1, cachekit::native::kLatestGeneration, key),
               std::string("invalid"))
                  .status == FillStatus::kInvalidArgument);
}

void TestStateGenerationWatermarkSurvivesEvictionAndAdmissionFailure() {
    Options eviction_options;
    eviction_options.capacity_entries = 1;
    eviction_options.key_arena_bytes = 16;
    eviction_options.value_arena_bytes = 16;
    eviction_options.kernel = KernelPreference::kScalar;
    std::unique_ptr<RequestPlane> eviction_plane =
            MakePlane(eviction_options);

    const std::string key = "k";
    const std::string other = "other";
    CHECK(Fill(*eviction_plane, Key(17, 10, key), std::string("v10")).status ==
          FillStatus::kInserted);
    CHECK(Fill(*eviction_plane, Key(17, 11, other), std::string("v11")).status ==
          FillStatus::kInserted);
    CHECK(Probe(*eviction_plane,
                Key(17, cachekit::native::kLatestGeneration, key))
                  .status == ProbeStatus::kMiss);

    const FillResult delayed =
            Fill(*eviction_plane, Key(17, 5, key), std::string("stale"));
    CHECK(delayed.status == FillStatus::kRejectedStaleGeneration);
    CHECK(delayed.error == ErrorCode::kOk);
    CHECK(Probe(*eviction_plane,
                Key(17, cachekit::native::kLatestGeneration, key))
                  .status == ProbeStatus::kMiss);
    CHECK(ResultValue(
                  Probe(*eviction_plane,
                        Key(17,
                            cachekit::native::kLatestGeneration,
                            other))) ==
          "v11");

    Options batch_options;
    batch_options.capacity_entries = 2;
    batch_options.key_arena_bytes = 16;
    batch_options.value_arena_bytes = 16;
    batch_options.kernel = KernelPreference::kScalar;
    std::unique_ptr<RequestPlane> batch_plane = MakePlane(batch_options);
    const std::string first = "a";
    const std::string second = "b";
    const std::string first_value = "A";
    const std::string second_value = "B";
    const FillView same_generation[] = {
            FillView{
                    Key(23, 21, first),
                    reinterpret_cast<const std::uint8_t*>(
                            first_value.data()),
                    first_value.size(),
                    false},
            FillView{
                    Key(23, 21, second),
                    reinterpret_cast<const std::uint8_t*>(
                            second_value.data()),
                    second_value.size(),
                    false}};
    FillResult same_generation_results[2];
    CHECK(batch_plane->FillBatch(
                  same_generation, same_generation_results, 2) ==
          ErrorCode::kOk);
    CHECK(same_generation_results[0].status == FillStatus::kInserted);
    CHECK(same_generation_results[1].status == FillStatus::kInserted);

    // A newer mutation of one key must fence delayed fills for the state but
    // must not invalidate a latest probe for another untouched resident key.
    CHECK(Fill(*batch_plane, Key(23, 22, first), std::string("A2")).status ==
          FillStatus::kUpdated);
    CHECK(ResultValue(
                  Probe(*batch_plane,
                        Key(23,
                            cachekit::native::kLatestGeneration,
                            second))) ==
          "B");
    CHECK(Fill(*batch_plane, Key(23, 21, second), std::string("old-B")).status ==
          FillStatus::kRejectedStaleGeneration);
    CHECK(ResultValue(
                  Probe(*batch_plane,
                        Key(23,
                            cachekit::native::kLatestGeneration,
                            second))) ==
          "B");
    const std::string oversized_value(17, 'x');
    CHECK(Fill(*batch_plane, Key(23, 23, first), oversized_value).status ==
          FillStatus::kRejectedCapacity);
    CHECK(Probe(*batch_plane,
                Key(23, cachekit::native::kLatestGeneration, first))
                  .status == ProbeStatus::kMiss);
    CHECK(ResultValue(
                  Probe(*batch_plane,
                        Key(23,
                            cachekit::native::kLatestGeneration,
                            second))) ==
          "B");
    CHECK(Fill(*batch_plane, Key(23, 22, second), std::string("old-B2")).status ==
          FillStatus::kRejectedStaleGeneration);
    CHECK(ResultValue(
                  Probe(*batch_plane,
                        Key(23,
                            cachekit::native::kLatestGeneration,
                            second))) ==
          "B");

    Options admission_options;
    admission_options.capacity_entries = 1;
    admission_options.key_arena_bytes = 2;
    admission_options.value_arena_bytes = 2;
    admission_options.kernel = KernelPreference::kScalar;
    std::unique_ptr<RequestPlane> admission_plane =
            MakePlane(admission_options);
    const std::string oversized_key = "too-large";
    CHECK(Fill(*admission_plane,
               Key(31, 30, oversized_key),
               std::string(),
               true)
                  .status == FillStatus::kRejectedCapacity);
    const FillResult before_failed_clear =
            Fill(*admission_plane, Key(31, 29, key), std::string("x"));
    CHECK(before_failed_clear.status ==
          FillStatus::kRejectedStaleGeneration);
    CHECK(before_failed_clear.error == ErrorCode::kOk);

    // The explicit plane-wide lifecycle reset intentionally resets the
    // watermark along with every entry and arena.
    admission_plane->Clear();
    CHECK(Fill(*admission_plane, Key(31, 29, key), std::string("x")).status ==
          FillStatus::kInserted);
}

void TestEvictionAndArenaReuse() {
    Options options;
    options.capacity_entries = 2;
    options.key_arena_bytes = 16;
    options.value_arena_bytes = 16;
    options.kernel = KernelPreference::kScalar;
    std::unique_ptr<RequestPlane> plane = MakePlane(options);

    const std::string a = "a";
    const std::string b = "b";
    const std::string c = "c";
    CHECK(Fill(*plane, Key(1, 1, a), std::string("aaaa")).status ==
          FillStatus::kInserted);
    CHECK(Fill(*plane, Key(1, 1, b), std::string("bbbb")).status ==
          FillStatus::kInserted);
    CHECK(Probe(*plane, Key(1, 1, a)).status == ProbeStatus::kHit);
    CHECK(Fill(*plane, Key(1, 1, c), std::string("cccc")).status ==
          FillStatus::kInserted);
    CHECK(plane->size() == 2);
    CHECK(plane->evictions() == 1);
    CHECK(Probe(*plane, Key(1, 1, a)).status == ProbeStatus::kHit);
    CHECK(Probe(*plane, Key(1, 1, b)).status == ProbeStatus::kMiss);
    CHECK(Probe(*plane, Key(1, 1, c)).status == ProbeStatus::kHit);

    plane->Clear();
    CHECK(plane->size() == 0);
    CHECK(plane->evictions() == 0);
    CHECK(Fill(*plane, Key(1, 2, b), std::string("reused")).status ==
          FillStatus::kInserted);
    CHECK(ResultValue(Probe(*plane, Key(1, 2, b))) == "reused");
}

void TestIntrusiveLruOrderAndSustainedCapacityChurn() {
    Options options;
    options.capacity_entries = 4;
    options.key_arena_bytes = 128;
    options.value_arena_bytes = 256;
    options.kernel = KernelPreference::kScalar;
    std::unique_ptr<RequestPlane> plane = MakePlane(options);

    const std::string a = "a";
    const std::string b = "b";
    const std::string c = "c";
    const std::string d = "d";
    const std::string e = "e";
    const std::string f = "f";
    const std::string g = "g";
    for (const std::string* key : {&a, &b, &c, &d}) {
        CHECK(Fill(*plane, Key(3, 1, *key), *key).status ==
              FillStatus::kInserted);
    }

    // Insertion order is a,b,c,d. A probe and an update must both move the
    // exact entry to the MRU tail, producing c,d,a,b before the first churn.
    CHECK(Probe(*plane, Key(3, 1, a)).status == ProbeStatus::kHit);
    CHECK(Fill(*plane, Key(3, 2, b), std::string("b2")).status ==
          FillStatus::kUpdated);
    CHECK(Fill(*plane, Key(3, 2, e), e).status == FillStatus::kInserted);
    CHECK(Probe(*plane, Key(3, 1, c)).status == ProbeStatus::kMiss);
    CHECK(Fill(*plane, Key(3, 2, f), f).status == FillStatus::kInserted);
    CHECK(Probe(*plane, Key(3, 1, d)).status == ProbeStatus::kMiss);
    CHECK(Probe(*plane, Key(3, 1, a)).status == ProbeStatus::kHit);
    CHECK(Fill(*plane, Key(3, 2, g), g).status == FillStatus::kInserted);
    CHECK(Probe(*plane, Key(3, 2, b)).status == ProbeStatus::kMiss);
    CHECK(plane->size() == options.capacity_entries);
    CHECK(plane->evictions() == 3);

    // Keep a full plane under many more insert/evict cycles. Fixed-size keys
    // and varying values exercise both arena release/reuse lists without
    // permitting entry-count growth or capacity rejection.
    constexpr std::size_t kChurnCapacity = 256;
    constexpr std::size_t kChurnOperations = 50000;
    Options churn_options;
    churn_options.capacity_entries = kChurnCapacity;
    churn_options.key_arena_bytes = kChurnCapacity * 32U;
    churn_options.value_arena_bytes = kChurnCapacity * 128U;
    churn_options.kernel = KernelPreference::kScalar;
    std::unique_ptr<RequestPlane> churn = MakePlane(churn_options);
    std::array<char, 32> key_bytes{};
    std::array<char, 96> value_bytes{};
    std::fill(value_bytes.begin(), value_bytes.end(), 'v');
    for (std::size_t operation = 0;
         operation < kChurnCapacity + kChurnOperations;
         ++operation) {
        std::fill(key_bytes.begin(), key_bytes.end(), '\0');
        const std::string suffix = std::to_string(operation);
        std::copy(suffix.begin(), suffix.end(), key_bytes.begin());
        const std::string key(key_bytes.data(), key_bytes.size());
        const std::size_t value_size = 16U + (operation % 6U) * 16U;
        const std::string value(value_bytes.data(), value_size);
        CHECK(Fill(*churn, Key(9, 1, key), value).status ==
              FillStatus::kInserted);
    }
    CHECK(churn->size() == kChurnCapacity);
    CHECK(churn->evictions() == kChurnOperations);
}

void TestUpdateCanReclaimFragmentedArena() {
    Options options;
    options.capacity_entries = 3;
    options.key_arena_bytes = 12;
    options.value_arena_bytes = 12;
    options.kernel = KernelPreference::kScalar;
    std::unique_ptr<RequestPlane> plane = MakePlane(options);
    const std::string a = "a";
    const std::string b = "b";
    const std::string c = "c";
    CHECK(Fill(*plane, Key(2, 1, a), std::string("1111")).status ==
          FillStatus::kInserted);
    CHECK(Fill(*plane, Key(2, 1, b), std::string("2222")).status ==
          FillStatus::kInserted);
    CHECK(Fill(*plane, Key(2, 1, c), std::string("3333")).status ==
          FillStatus::kInserted);
    CHECK(Fill(*plane, Key(2, 2, c), std::string("abcdefgh")).status ==
          FillStatus::kUpdated);
    CHECK(ResultValue(Probe(*plane, Key(2, 2, c))) == "abcdefgh");
    CHECK(plane->evictions() == 2);
    CHECK(plane->size() == 1);
}

void TestCapacityAndOverflowRejection() {
    ErrorCode error = ErrorCode::kOk;
    std::string message;
    Options invalid;
    invalid.capacity_entries = 0;
    CHECK(!RequestPlane::Create(invalid, &error, &message));
    CHECK(error == ErrorCode::kInvalidArgument);

    Options overflow;
    overflow.capacity_entries =
            static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max());
    CHECK(!RequestPlane::Create(overflow, &error, &message));
    CHECK(error == ErrorCode::kOverflow);

    Options options;
    options.capacity_entries = 2;
    options.key_arena_bytes = 4;
    options.value_arena_bytes = 4;
    options.kernel = KernelPreference::kScalar;
    std::unique_ptr<RequestPlane> plane = MakePlane(options);

    const std::string too_large_key = "12345";
    FillResult result =
            Fill(*plane, Key(1, 1, too_large_key), std::string("x"));
    CHECK(result.status == FillStatus::kRejectedCapacity);
    CHECK(result.error == ErrorCode::kCapacityExceeded);
    CHECK(plane->size() == 0);

    const std::string key = "k";
    result = Fill(*plane, Key(1, 1, key), std::string("12345"));
    CHECK(result.status == FillStatus::kRejectedCapacity);
    CHECK(plane->size() == 0);

    const KeyView bad_key{1, 1, nullptr, 1};
    const FillView bad_fill{bad_key, nullptr, 1, false};
    CHECK(plane->FillBatch(&bad_fill, &result, 1) == ErrorCode::kOk);
    CHECK(result.status == FillStatus::kInvalidArgument);
    const ProbeResult bad_probe = Probe(*plane, bad_key);
    CHECK(bad_probe.status == ProbeStatus::kMiss);
    CHECK(bad_probe.error == ErrorCode::kInvalidArgument);

    const KeyView empty_key{9, 1, nullptr, 0};
    CHECK(Fill(*plane, empty_key, std::string()).status ==
          FillStatus::kInserted);
    CHECK(Probe(*plane, empty_key).status == ProbeStatus::kHit);
}

struct ReferenceValue {
    std::uint64_t generation = 0;
    bool negative = false;
    std::string value;
};

std::string ReferenceKey(std::uint32_t state_id, const std::string& key) {
    return std::to_string(state_id) + ":" + key;
}

void TestRandomDifferentialAgainstReference() {
    Options options;
    options.capacity_entries = 128;
    options.key_arena_bytes = 16U * 1024U;
    options.value_arena_bytes = 16U * 1024U;
    options.kernel = KernelPreference::kScalar;
    options.fingerprint_mask = 0xffU;
    std::unique_ptr<RequestPlane> plane = MakePlane(options);
    std::unordered_map<std::string, ReferenceValue> reference;
    std::unordered_map<std::uint32_t, std::uint64_t> state_watermarks;
    std::mt19937 random(0x5a17U);

    std::vector<std::string> keys;
    for (int index = 0; index < 24; ++index) {
        keys.push_back("key-" + std::to_string(index));
    }

    for (int iteration = 0; iteration < 1000; ++iteration) {
        const std::string& key = keys[random() % keys.size()];
        const std::uint32_t state_id =
                1U + static_cast<std::uint32_t>(random() % 3U);
        const std::string reference_key = ReferenceKey(state_id, key);
        const bool do_fill = (random() % 100U) < 55U;
        auto found = reference.find(reference_key);

        if (do_fill) {
            const std::uint64_t watermark = state_watermarks[state_id];
            const bool stale =
                    watermark > 0 && (random() % 10U) == 0;
            const std::uint64_t generation = stale
                    ? watermark - 1U
                    : std::max(
                              watermark,
                              found == reference.end()
                                      ? 1U
                                      : found->second.generation + 1U);
            const bool negative = (random() % 5U) == 0;
            const std::string value =
                    "value-" + std::to_string(iteration) + "-" + key;
            const FillResult actual =
                    Fill(*plane, Key(state_id, generation, key), value, negative);
            if (stale) {
                CHECK(actual.status == FillStatus::kRejectedStaleGeneration);
            } else {
                CHECK(actual.status == (found == reference.end()
                                                ? FillStatus::kInserted
                                                : FillStatus::kUpdated));
                state_watermarks[state_id] =
                        std::max(watermark, generation);
                reference[reference_key] =
                        ReferenceValue{generation, negative, negative ? "" : value};
            }
        } else {
            const std::uint32_t probe_mode = random() % 4U;
            const std::uint64_t generation = found == reference.end()
                    ? 1U
                    : (probe_mode == 0
                               ? found->second.generation + 1U
                               : (probe_mode == 1
                                          ? cachekit::native::kLatestGeneration
                                          : found->second.generation));
            const ProbeResult actual =
                    Probe(*plane, Key(state_id, generation, key));
            if (found == reference.end() ||
                (generation != cachekit::native::kLatestGeneration &&
                 generation != found->second.generation)) {
                CHECK(actual.status == ProbeStatus::kMiss);
            } else if (found->second.negative) {
                CHECK(actual.status == ProbeStatus::kNegative);
            } else {
                CHECK(actual.status == ProbeStatus::kHit);
                CHECK(ResultValue(actual) == found->second.value);
            }
        }
    }
    CHECK(plane->evictions() == 0);
}

void TestCAbiBatchSmoke() {
    cachekit_native_options options;
    cachekit_native_options_init(&options);
    options.capacity_entries = 4;
    options.key_arena_bytes = 64;
    options.value_arena_bytes = 64;
    options.kernel = CACHEKIT_NATIVE_KERNEL_SCALAR;

    cachekit_native_error error = CACHEKIT_NATIVE_INTERNAL;
    char message[128] = {};
    cachekit_native_plane* plane = cachekit_native_plane_create(
            &options, &error, message, sizeof(message));
    CHECK(plane != nullptr);
    CHECK(error == CACHEKIT_NATIVE_OK);
    CHECK(cachekit_native_plane_min_batch_size(plane) == 64);
    CHECK(cachekit_native_plane_kernel_kind(plane) ==
          static_cast<std::uint32_t>(KernelKind::kScalar));

    const std::string key = "c-key";
    const std::string value = "c-value";
    const cachekit_native_fill_view fill{
            cachekit_native_key_view{
                    5,
                    7,
                    reinterpret_cast<const std::uint8_t*>(key.data()),
                    key.size()},
            reinterpret_cast<const std::uint8_t*>(value.data()),
            value.size(),
            0};
    cachekit_native_fill_result fill_result{};
    CHECK(cachekit_native_fill_batch(plane, &fill, &fill_result, 1) ==
          CACHEKIT_NATIVE_OK);
    CHECK(fill_result.status == CACHEKIT_NATIVE_INSERTED);

    const cachekit_native_key_view probe_key = fill.key;
    cachekit_native_probe_result probe_result{};
    CHECK(cachekit_native_probe_batch(plane, &probe_key, &probe_result, 1) ==
          CACHEKIT_NATIVE_OK);
    CHECK(probe_result.status == CACHEKIT_NATIVE_HIT);
    CHECK(std::string(
                  reinterpret_cast<const char*>(probe_result.value),
                  probe_result.value_size) == value);
    CHECK(cachekit_native_plane_size(plane) == 1);

    const std::string second_key = "c-key-2";
    const std::string second_value = "c-value-2";
    const cachekit_native_fill_view fills[] = {
            fill,
            cachekit_native_fill_view{
                    cachekit_native_key_view{
                            5,
                            7,
                            reinterpret_cast<const std::uint8_t*>(
                                    second_key.data()),
                            second_key.size()},
                    reinterpret_cast<const std::uint8_t*>(second_value.data()),
                    second_value.size(),
                    0}};
    cachekit_native_fill_result fill_results[2] = {};
    CHECK(cachekit_native_fill_batch(plane, fills, fill_results, 2) ==
          CACHEKIT_NATIVE_OK);
    CHECK(fill_results[0].status == CACHEKIT_NATIVE_UPDATED);
    CHECK(fill_results[1].status == CACHEKIT_NATIVE_INSERTED);

    const std::string missing_key = "missing";
    const cachekit_native_key_view probe_keys[] = {
            fill.key,
            fills[1].key,
            cachekit_native_key_view{
                    5,
                    7,
                    reinterpret_cast<const std::uint8_t*>(missing_key.data()),
                    missing_key.size()}};
    cachekit_native_probe_result probe_results[3] = {};
    CHECK(cachekit_native_probe_batch(plane, probe_keys, probe_results, 3) ==
          CACHEKIT_NATIVE_OK);
    CHECK(probe_results[0].status == CACHEKIT_NATIVE_HIT);
    CHECK(probe_results[1].status == CACHEKIT_NATIVE_HIT);
    CHECK(probe_results[2].status == CACHEKIT_NATIVE_MISS);
    CHECK(std::string(
                  reinterpret_cast<const char*>(probe_results[1].value),
                  probe_results[1].value_size) == second_value);
    CHECK(cachekit_native_plane_size(plane) == 2);
    cachekit_native_plane_destroy(plane);
}

void TestCompactBatchPreservesFirstOccurrenceAndIdentity() {
    Options options;
    options.capacity_entries = 16;
    options.key_arena_bytes = 1024;
    options.value_arena_bytes = 1024;
    options.kernel = KernelPreference::kScalar;
    std::unique_ptr<RequestPlane> plane = MakePlane(options);
    const std::string a = "alpha";
    const std::string b = "beta";
    const KeyView keys[] = {
            Key(7, 3, a),
            Key(7, 3, b),
            Key(7, 3, a),
            Key(8, 3, a),
            Key(7, 4, a),
            Key(7, 3, b)};
    std::uint32_t indexes[6] = {};
    std::size_t unique_count = 0;
    CHECK(plane->CompactBatch(keys, indexes, 6, &unique_count) == ErrorCode::kOk);
    CHECK(unique_count == 4);
    CHECK(indexes[0] == 0);
    CHECK(indexes[1] == 1);
    CHECK(indexes[2] == 3);
    CHECK(indexes[3] == 4);
}

}  // namespace

int main() {
    TestBucketLayoutAndForcedScalar();
#if defined(CACHEKIT_NATIVE_AARCH64_KERNELS)
    TestAvailableAarch64KernelsMatchScalar();
#endif
    TestCollisionRequiresExactCompare();
    TestGenerationAndNegativeEntries();
    TestStateGenerationWatermarkSurvivesEvictionAndAdmissionFailure();
    TestEvictionAndArenaReuse();
    TestIntrusiveLruOrderAndSustainedCapacityChurn();
    TestUpdateCanReclaimFragmentedArena();
    TestCapacityAndOverflowRejection();
    TestRandomDifferentialAgainstReference();
    TestCompactBatchPreservesFirstOccurrenceAndIdentity();
    TestCAbiBatchSmoke();
    std::cout << "all native request-plane tests passed" << std::endl;
    return 0;
}
