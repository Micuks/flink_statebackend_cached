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

#include "jni_batch_codec.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace {

using cachekit::native::ErrorCode;
using cachekit::native::KernelPreference;
using cachekit::native::Options;
using cachekit::native::RequestPlane;
using namespace cachekit::native::bridge;

#define CHECK(condition)                                                       \
    do {                                                                       \
        if (!(condition)) {                                                     \
            std::cerr << __FILE__ << ':' << __LINE__                           \
                      << ": CHECK failed: " #condition << std::endl;           \
            std::exit(1);                                                       \
        }                                                                      \
    } while (false)

template <typename T>
void Write(std::vector<std::uint8_t>* buffer, std::size_t offset, T value) {
    std::memcpy(buffer->data() + offset, &value, sizeof(value));
}

template <typename T>
T Read(const std::vector<std::uint8_t>& buffer, std::size_t offset) {
    T value{};
    std::memcpy(&value, buffer.data() + offset, sizeof(value));
    return value;
}

std::unique_ptr<RequestPlane> MakePlane() {
    Options options;
    options.capacity_entries = 8;
    options.key_arena_bytes = 128;
    options.value_arena_bytes = 128;
    options.kernel = KernelPreference::kScalar;
    ErrorCode error = ErrorCode::kInternal;
    std::string message;
    std::unique_ptr<RequestPlane> plane =
            RequestPlane::Create(options, &error, &message);
    CHECK(plane != nullptr);
    CHECK(error == ErrorCode::kOk);
    return plane;
}

void WriteKeyRecord(
        std::vector<std::uint8_t>* metadata,
        std::size_t index,
        std::int32_t state_id,
        std::uint64_t generation,
        std::int32_t arena_offset,
        std::int32_t length) {
    const std::size_t base = index * kKeyMetadataRecordBytes;
    Write(metadata, base + kKeyStateIdOffset, state_id);
    Write(metadata, base + kKeyReservedOffset, std::int32_t{0});
    Write(metadata, base + kKeyGenerationOffset, generation);
    Write(metadata, base + kKeyArenaOffsetOffset, arena_offset);
    Write(metadata, base + kKeyLengthOffset, length);
}

void WriteValueRecord(
        std::vector<std::uint8_t>* metadata,
        std::size_t index,
        std::int32_t arena_offset,
        std::int32_t length,
        std::uint32_t flags) {
    const std::size_t base = index * kFillValueRecordBytes;
    Write(metadata, base + kFillValueArenaOffsetOffset, arena_offset);
    Write(metadata, base + kFillValueLengthOffset, length);
    Write(metadata, base + kFillValueFlagsOffset, flags);
    Write(metadata, base + kFillValueReservedOffset, std::uint32_t{0});
}

void TestFillAndProbeRoundTrip() {
    std::unique_ptr<RequestPlane> plane = MakePlane();
    const std::string key_arena = "alpha-beta-missing";
    std::vector<std::uint8_t> key_metadata(3 * kKeyMetadataRecordBytes);
    WriteKeyRecord(&key_metadata, 0, 7, 11, 0, 5);
    WriteKeyRecord(&key_metadata, 1, 7, 12, 6, 4);
    WriteKeyRecord(&key_metadata, 2, 8, 13, 11, 7);

    const std::string value_arena = "value-alpha";
    std::vector<std::uint8_t> value_metadata(2 * kFillValueRecordBytes);
    WriteValueRecord(&value_metadata, 0, 0, 11, 0);
    WriteValueRecord(&value_metadata, 1, 0, 0, kFillValueNegativeFlag);
    std::vector<std::uint8_t> fill_results(2 * kFillResultRecordBytes, 0xff);

    CHECK(FillDirectBatch(
                  plane.get(),
                  {reinterpret_cast<const std::uint8_t*>(key_arena.data()),
                   key_arena.size()},
                  {key_metadata.data(), 2 * kKeyMetadataRecordBytes},
                  {reinterpret_cast<const std::uint8_t*>(value_arena.data()),
                   value_arena.size()},
                  {value_metadata.data(), value_metadata.size()},
                  2,
                  {fill_results.data(), fill_results.size()}) ==
          BatchBridgeCode::kOk);
    CHECK(plane->size() == 2);
    CHECK(Read<std::uint32_t>(fill_results, kFillResultStatusOffset) == 0);
    CHECK(Read<std::uint32_t>(fill_results, kFillResultErrorOffset) == 0);
    CHECK(Read<std::uint32_t>(
                  fill_results,
                  kFillResultRecordBytes + kFillResultStatusOffset) == 0);

    std::vector<std::uint8_t> value_output(32, 0);
    std::vector<std::uint8_t> probe_results(3 * kProbeResultRecordBytes, 0xff);
    CHECK(ProbeDirectBatch(
                  plane.get(),
                  {reinterpret_cast<const std::uint8_t*>(key_arena.data()),
                   key_arena.size()},
                  {key_metadata.data(), key_metadata.size()},
                  3,
                  {value_output.data(), value_output.size()},
                  {probe_results.data(), probe_results.size()}) ==
          BatchBridgeCode::kOk);

    CHECK(Read<std::uint32_t>(probe_results, kProbeResultStatusOffset) == 1);
    CHECK(Read<std::uint32_t>(probe_results, kProbeResultErrorOffset) == 0);
    CHECK(Read<std::uint32_t>(probe_results, kProbeResultArenaOffsetOffset) == 0);
    CHECK(Read<std::uint32_t>(probe_results, kProbeResultLengthOffset) == 11);
    CHECK(std::string(
                  reinterpret_cast<const char*>(value_output.data()), 11) ==
          value_arena);

    const std::size_t negative = kProbeResultRecordBytes;
    CHECK(Read<std::uint32_t>(
                  probe_results, negative + kProbeResultStatusOffset) == 2);
    CHECK(Read<std::uint32_t>(
                  probe_results, negative + kProbeResultLengthOffset) == 0);
    const std::size_t miss = 2 * kProbeResultRecordBytes;
    CHECK(Read<std::uint32_t>(probe_results, miss + kProbeResultStatusOffset) == 0);
    CHECK(Read<std::uint32_t>(probe_results, miss + kProbeResultLengthOffset) == 0);
}

void TestMalformedFillIsRejectedBeforeMutation() {
    std::unique_ptr<RequestPlane> plane = MakePlane();
    const std::string key_arena = "key";
    std::vector<std::uint8_t> key_metadata(kKeyMetadataRecordBytes);
    WriteKeyRecord(&key_metadata, 0, 1, 1, 0, 3);
    Write(
            &key_metadata,
            kKeyReservedOffset,
            std::int32_t{9});
    std::vector<std::uint8_t> value_metadata(kFillValueRecordBytes);
    WriteValueRecord(&value_metadata, 0, 0, 0, 0);
    std::vector<std::uint8_t> results(kFillResultRecordBytes, 0xa5);

    CHECK(FillDirectBatch(
                  plane.get(),
                  {reinterpret_cast<const std::uint8_t*>(key_arena.data()),
                   key_arena.size()},
                  {key_metadata.data(), key_metadata.size()},
                  {nullptr, 0},
                  {value_metadata.data(), value_metadata.size()},
                  1,
                  {results.data(), results.size()}) ==
          BatchBridgeCode::kInvalidMetadata);
    CHECK(plane->size() == 0);
    for (std::uint8_t byte : results) {
        CHECK(byte == 0xa5);
    }

    Write(&key_metadata, kKeyReservedOffset, std::int32_t{0});
    Write(&key_metadata, kKeyLengthOffset, std::int32_t{4});
    CHECK(FillDirectBatch(
                  plane.get(),
                  {reinterpret_cast<const std::uint8_t*>(key_arena.data()),
                   key_arena.size()},
                  {key_metadata.data(), key_metadata.size()},
                  {nullptr, 0},
                  {value_metadata.data(), value_metadata.size()},
                  1,
                  {results.data(), results.size()}) ==
          BatchBridgeCode::kInputOutOfBounds);
    CHECK(plane->size() == 0);
}

void TestProbeOutputTooSmallDoesNotWritePartialRecords() {
    std::unique_ptr<RequestPlane> plane = MakePlane();
    const std::string key_arena = "key";
    std::vector<std::uint8_t> key_metadata(kKeyMetadataRecordBytes);
    WriteKeyRecord(&key_metadata, 0, 1, 1, 0, 3);
    const std::string value_arena = "payload";
    std::vector<std::uint8_t> value_metadata(kFillValueRecordBytes);
    WriteValueRecord(
            &value_metadata,
            0,
            0,
            static_cast<std::int32_t>(value_arena.size()),
            0);
    std::vector<std::uint8_t> fill_results(kFillResultRecordBytes);
    CHECK(FillDirectBatch(
                  plane.get(),
                  {reinterpret_cast<const std::uint8_t*>(key_arena.data()),
                   key_arena.size()},
                  {key_metadata.data(), key_metadata.size()},
                  {reinterpret_cast<const std::uint8_t*>(value_arena.data()),
                   value_arena.size()},
                  {value_metadata.data(), value_metadata.size()},
                  1,
                  {fill_results.data(), fill_results.size()}) ==
          BatchBridgeCode::kOk);

    std::vector<std::uint8_t> too_small(value_arena.size() - 1, 0xcc);
    std::vector<std::uint8_t> probe_results(kProbeResultRecordBytes, 0xa5);
    CHECK(ProbeDirectBatch(
                  plane.get(),
                  {reinterpret_cast<const std::uint8_t*>(key_arena.data()),
                   key_arena.size()},
                  {key_metadata.data(), key_metadata.size()},
                  1,
                  {too_small.data(), too_small.size()},
                  {probe_results.data(), probe_results.size()}) ==
          BatchBridgeCode::kOutputTooSmall);
    for (std::uint8_t byte : probe_results) {
        CHECK(byte == 0xa5);
    }
    for (std::uint8_t byte : too_small) {
        CHECK(byte == 0xcc);
    }
}

void TestGenerationMismatchLatestProbeAndZeroCount() {
    std::unique_ptr<RequestPlane> plane = MakePlane();
    const std::string key_arena = "key";
    std::vector<std::uint8_t> key_metadata(kKeyMetadataRecordBytes);
    WriteKeyRecord(&key_metadata, 0, 3, 7, 0, 3);
    const std::string value_arena = "value";
    std::vector<std::uint8_t> value_metadata(kFillValueRecordBytes);
    WriteValueRecord(
            &value_metadata,
            0,
            0,
            static_cast<std::int32_t>(value_arena.size()),
            0);
    std::vector<std::uint8_t> fill_results(kFillResultRecordBytes);
    CHECK(FillDirectBatch(
                  plane.get(),
                  {reinterpret_cast<const std::uint8_t*>(key_arena.data()),
                   key_arena.size()},
                  {key_metadata.data(), key_metadata.size()},
                  {reinterpret_cast<const std::uint8_t*>(value_arena.data()),
                   value_arena.size()},
                  {value_metadata.data(), value_metadata.size()},
                  1,
                  {fill_results.data(), fill_results.size()}) ==
          BatchBridgeCode::kOk);

    WriteKeyRecord(&key_metadata, 0, 3, 8, 0, 3);
    std::vector<std::uint8_t> value_output(value_arena.size(), 0xcc);
    std::vector<std::uint8_t> probe_results(kProbeResultRecordBytes, 0xa5);
    CHECK(ProbeDirectBatch(
                  plane.get(),
                  {reinterpret_cast<const std::uint8_t*>(key_arena.data()),
                   key_arena.size()},
                  {key_metadata.data(), key_metadata.size()},
                  1,
                  {value_output.data(), value_output.size()},
                  {probe_results.data(), probe_results.size()}) ==
          BatchBridgeCode::kOk);
    CHECK(Read<std::uint32_t>(probe_results, kProbeResultStatusOffset) == 0);
    CHECK(Read<std::uint32_t>(probe_results, kProbeResultErrorOffset) == 0);
    CHECK(Read<std::uint32_t>(probe_results, kProbeResultLengthOffset) == 0);

    WriteKeyRecord(
            &key_metadata,
            0,
            3,
            cachekit::native::kLatestGeneration,
            0,
            3);
    CHECK(ProbeDirectBatch(
                  plane.get(),
                  {reinterpret_cast<const std::uint8_t*>(key_arena.data()),
                   key_arena.size()},
                  {key_metadata.data(), key_metadata.size()},
                  1,
                  {value_output.data(), value_output.size()},
                  {probe_results.data(), probe_results.size()}) ==
          BatchBridgeCode::kOk);
    CHECK(Read<std::uint32_t>(probe_results, kProbeResultStatusOffset) == 1);
    CHECK(Read<std::uint32_t>(probe_results, kProbeResultLengthOffset) ==
          value_arena.size());
    CHECK(std::memcmp(value_output.data(), value_arena.data(), value_arena.size()) == 0);

    CHECK(FillDirectBatch(
                  plane.get(), {nullptr, 0}, {nullptr, 0}, {nullptr, 0}, {nullptr, 0}, 0,
                  {nullptr, 0}) == BatchBridgeCode::kOk);
    CHECK(ProbeDirectBatch(
                  plane.get(), {nullptr, 0}, {nullptr, 0}, 0, {nullptr, 0}, {nullptr, 0}) ==
          BatchBridgeCode::kOk);
}

void TestScratchCapacityIsReusedAcrossFillAndProbe() {
    std::unique_ptr<RequestPlane> plane = MakePlane();
    BatchScratch scratch(2);
    CHECK(scratch.reserved_entries() == 2);
    const std::uint64_t initial_growth_count = scratch.growth_count();
    CHECK(initial_growth_count == 1);

    const std::string key_arena = "alphabeta";
    std::vector<std::uint8_t> key_metadata(2 * kKeyMetadataRecordBytes);
    const std::string value_arena = "onetwo";
    std::vector<std::uint8_t> value_metadata(2 * kFillValueRecordBytes);
    WriteValueRecord(&value_metadata, 0, 0, 3, 0);
    WriteValueRecord(&value_metadata, 1, 3, 3, 0);
    std::vector<std::uint8_t> fill_results(2 * kFillResultRecordBytes);
    std::vector<std::uint8_t> value_output(16);
    std::vector<std::uint8_t> probe_results(2 * kProbeResultRecordBytes);

    for (std::uint64_t generation = 1; generation <= 32; ++generation) {
        WriteKeyRecord(&key_metadata, 0, 1, generation, 0, 5);
        WriteKeyRecord(&key_metadata, 1, 1, generation, 5, 4);
        CHECK(FillDirectBatch(
                      plane.get(),
                      &scratch,
                      {reinterpret_cast<const std::uint8_t*>(key_arena.data()),
                       key_arena.size()},
                      {key_metadata.data(), key_metadata.size()},
                      {reinterpret_cast<const std::uint8_t*>(value_arena.data()),
                       value_arena.size()},
                      {value_metadata.data(), value_metadata.size()},
                      2,
                      {fill_results.data(), fill_results.size()}) ==
              BatchBridgeCode::kOk);
        CHECK(ProbeDirectBatch(
                      plane.get(),
                      &scratch,
                      {reinterpret_cast<const std::uint8_t*>(key_arena.data()),
                       key_arena.size()},
                      {key_metadata.data(), key_metadata.size()},
                      2,
                      {value_output.data(), value_output.size()},
                      {probe_results.data(), probe_results.size()}) ==
              BatchBridgeCode::kOk);
        CHECK(
                Read<std::uint32_t>(
                        probe_results, kProbeResultStatusOffset) == 1);
        CHECK(
                Read<std::uint32_t>(
                        probe_results,
                        kProbeResultRecordBytes + kProbeResultStatusOffset) == 1);
        CHECK(scratch.growth_count() == initial_growth_count);
        CHECK(scratch.reserved_entries() == 2);
    }

    scratch.ReserveEntries(4);
    CHECK(scratch.reserved_entries() == 4);
    CHECK(scratch.growth_count() == initial_growth_count + 1);
    scratch.ReserveEntries(3);
    CHECK(scratch.reserved_entries() == 4);
    CHECK(scratch.growth_count() == initial_growth_count + 1);
}

std::vector<std::uint8_t> TokenBytes(
        const std::vector<std::uint32_t>& tokens) {
    std::vector<std::uint8_t> bytes(tokens.size() * sizeof(std::uint32_t));
    for (std::size_t index = 0; index < tokens.size(); ++index) {
        Write<std::uint32_t>(&bytes, index * sizeof(std::uint32_t), tokens[index]);
    }
    return bytes;
}

void TestGroupTokenPlanUsesStablePackedLayout() {
    std::unique_ptr<RequestPlane> plane = MakePlane();
    BatchScratch scratch(5);
    const std::vector<std::uint8_t> token_bytes =
            TokenBytes({7U, 8U, 7U, 9U, 8U});
    std::vector<std::uint8_t> plan(64U, 0xa5U);
    std::size_t group_count = 99U;
    CHECK(GroupTokenPlanDirectBatch(
                  plane.get(),
                  &scratch,
                  {token_bytes.data(), token_bytes.size()},
                  5U,
                  {plan.data(), plan.size()},
                  &group_count) == BatchBridgeCode::kOk);
    CHECK(group_count == 3U);
    CHECK(Read<std::uint32_t>(plan, kTokenPlanMagicOffset) == kTokenPlanMagic);
    CHECK(Read<std::uint32_t>(plan, kTokenPlanVersionOffset) ==
          kTokenPlanLayoutVersion);
    CHECK(Read<std::uint32_t>(plan, kTokenPlanSourceCountOffset) == 5U);
    CHECK(Read<std::uint32_t>(plan, kTokenPlanGroupCountOffset) == 3U);

    const std::size_t first_sources = kTokenPlanHeaderBytes;
    CHECK(Read<std::uint32_t>(plan, first_sources) == 0U);
    CHECK(Read<std::uint32_t>(plan, first_sources + 4U) == 1U);
    CHECK(Read<std::uint32_t>(plan, first_sources + 8U) == 3U);
    const std::size_t offsets = first_sources + 12U;
    CHECK(Read<std::uint32_t>(plan, offsets) == 0U);
    CHECK(Read<std::uint32_t>(plan, offsets + 4U) == 2U);
    CHECK(Read<std::uint32_t>(plan, offsets + 8U) == 4U);
    CHECK(Read<std::uint32_t>(plan, offsets + 12U) == 5U);
    const std::size_t source_groups = offsets + 16U;
    CHECK(Read<std::uint32_t>(plan, source_groups) == 0U);
    CHECK(Read<std::uint32_t>(plan, source_groups + 4U) == 1U);
    CHECK(Read<std::uint32_t>(plan, source_groups + 8U) == 0U);
    CHECK(Read<std::uint32_t>(plan, source_groups + 12U) == 2U);
    CHECK(Read<std::uint32_t>(plan, source_groups + 16U) == 1U);
}

void TestGroupTokenPlanEmptyBatchAndFailureMagic() {
    std::unique_ptr<RequestPlane> plane = MakePlane();
    BatchScratch scratch;
    std::vector<std::uint8_t> empty_plan(kTokenPlanWorstCaseBaseBytes, 0xa5U);
    std::size_t group_count = 99U;
    CHECK(GroupTokenPlanDirectBatch(
                  plane.get(),
                  &scratch,
                  {nullptr, 0U},
                  0U,
                  {empty_plan.data(), empty_plan.size()},
                  &group_count) == BatchBridgeCode::kOk);
    CHECK(group_count == 0U);
    CHECK(Read<std::uint32_t>(empty_plan, kTokenPlanMagicOffset) ==
          kTokenPlanMagic);
    CHECK(Read<std::uint32_t>(empty_plan, kTokenPlanSourceCountOffset) == 0U);
    CHECK(Read<std::uint32_t>(empty_plan, kTokenPlanGroupCountOffset) == 0U);
    CHECK(Read<std::uint32_t>(empty_plan, kTokenPlanHeaderBytes) == 0U);

    const std::vector<std::uint8_t> token_bytes = TokenBytes({1U, 2U});
    std::vector<std::uint8_t> failure_plan(64U, 0xa5U);
    Write<std::uint32_t>(&failure_plan, kTokenPlanMagicOffset, kTokenPlanMagic);
    group_count = 99U;
    CHECK(GroupTokenPlanDirectBatch(
                  plane.get(),
                  &scratch,
                  {token_bytes.data(), token_bytes.size() - 1U},
                  2U,
                  {failure_plan.data(), failure_plan.size()},
                  &group_count) == BatchBridgeCode::kInputOutOfBounds);
    CHECK(group_count == 0U);
    CHECK(Read<std::uint32_t>(failure_plan, kTokenPlanMagicOffset) == 0U);

    Write<std::uint32_t>(&failure_plan, kTokenPlanMagicOffset, kTokenPlanMagic);
    group_count = 99U;
    CHECK(GroupTokenPlanDirectBatch(
                  nullptr,
                  &scratch,
                  {token_bytes.data(), token_bytes.size()},
                  2U,
                  {failure_plan.data(), failure_plan.size()},
                  &group_count) == BatchBridgeCode::kInvalidArgument);
    CHECK(group_count == 0U);
    CHECK(Read<std::uint32_t>(failure_plan, kTokenPlanMagicOffset) == 0U);

    std::vector<std::uint8_t> too_small(4U, 0xa5U);
    Write<std::uint32_t>(&too_small, 0U, kTokenPlanMagic);
    group_count = 99U;
    CHECK(GroupTokenPlanDirectBatch(
                  plane.get(),
                  &scratch,
                  {token_bytes.data(), token_bytes.size()},
                  2U,
                  {too_small.data(), too_small.size()},
                  &group_count) == BatchBridgeCode::kOutputTooSmall);
    CHECK(group_count == 0U);
    CHECK(Read<std::uint32_t>(too_small, kTokenPlanMagicOffset) == 0U);
}

void TestGroupTokenPlanReusesScratchCapacity() {
    std::unique_ptr<RequestPlane> plane = MakePlane();
    BatchScratch scratch(4U);
    const std::uint64_t growth_count = scratch.growth_count();
    const std::vector<std::uint8_t> token_bytes = TokenBytes({1U, 2U, 1U, 3U});
    std::vector<std::uint8_t> plan(
            kTokenPlanWorstCaseBaseBytes +
                    kTokenPlanWorstCasePerSourceBytes * 4U);
    for (std::size_t iteration = 0; iteration < 256U; ++iteration) {
        std::size_t group_count = 0;
        CHECK(GroupTokenPlanDirectBatch(
                      plane.get(),
                      &scratch,
                      {token_bytes.data(), token_bytes.size()},
                      4U,
                      {plan.data(), plan.size()},
                      &group_count) == BatchBridgeCode::kOk);
        CHECK(group_count == 3U);
        CHECK(scratch.growth_count() == growth_count);
    }
}

}  // namespace

int main() {
    TestFillAndProbeRoundTrip();
    TestMalformedFillIsRejectedBeforeMutation();
    TestProbeOutputTooSmallDoesNotWritePartialRecords();
    TestGenerationMismatchLatestProbeAndZeroCount();
    TestScratchCapacityIsReusedAcrossFillAndProbe();
    TestGroupTokenPlanUsesStablePackedLayout();
    TestGroupTokenPlanEmptyBatchAndFailureMagic();
    TestGroupTokenPlanReusesScratchCapacity();
    std::cout << "all JNI batch codec tests passed" << std::endl;
    return 0;
}
