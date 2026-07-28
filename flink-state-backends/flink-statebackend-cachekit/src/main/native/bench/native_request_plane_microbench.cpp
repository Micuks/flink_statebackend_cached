#include "cachekit/native_request_plane.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

using cachekit::native::ErrorCode;
using cachekit::native::FillResult;
using cachekit::native::FillStatus;
using cachekit::native::FillView;
using cachekit::native::HostFeatures;
using cachekit::native::KernelPreference;
using cachekit::native::KeyView;
using cachekit::native::Options;
using cachekit::native::ProbeResult;
using cachekit::native::ProbeStatus;
using cachekit::native::RequestPlane;

constexpr std::uint64_t kDefaultSeed = 0x43414348454b4954ULL;
constexpr std::array<std::size_t, 4> kKeyLengths = {16, 32, 64, 128};
constexpr std::array<std::size_t, 4> kBatchSizes = {8, 32, 64, 128};
constexpr std::array<unsigned, 3> kHitRatios = {0, 50, 100};
constexpr const char* kAbbaOrder = "A-B-B-A";
constexpr const char* kMetric = "probe_batch_plus_one_result_sample";

volatile std::uint64_t g_checksum_sink = 0;

struct Config {
    std::string output_path = "-";
    std::size_t cycles = 5;
    std::size_t warmup_keys = 8192;
    std::size_t measured_keys = 32768;
    std::size_t workload_batches = 8;
    std::uint64_t seed = kDefaultSeed;
};

struct Candidate {
    const char* label;
    KernelPreference preference;
};

constexpr Candidate kScalar = {"scalar", KernelPreference::kScalar};

constexpr std::array<Candidate, 3> kCandidates = {{
        {"neon_crc", KernelPreference::kNeonCrc},
        {"sve256", KernelPreference::kSve256},
        {"auto", KernelPreference::kAuto},
}};

std::size_t ParsePositiveSize(const char* option, const std::string& text) {
    std::size_t consumed = 0;
    const unsigned long long parsed = std::stoull(text, &consumed, 0);
    if (consumed != text.size() || parsed == 0 ||
        parsed > std::numeric_limits<std::size_t>::max()) {
        throw std::invalid_argument(
                std::string(option) + " requires a positive size");
    }
    return static_cast<std::size_t>(parsed);
}

std::uint64_t ParseSeed(const std::string& text) {
    std::size_t consumed = 0;
    const unsigned long long parsed = std::stoull(text, &consumed, 0);
    if (consumed != text.size()) {
        throw std::invalid_argument("--seed requires an integer");
    }
    return static_cast<std::uint64_t>(parsed);
}

void PrintUsage(const char* executable) {
    std::cerr
            << "Usage: " << executable << " [options]\n"
            << "  --output PATH          CSV path, or - for stdout (default -)\n"
            << "  --cycles N             ABBA cycles per pair (default 5)\n"
            << "  --warmup-keys N        minimum warmup keys per kernel (default 8192)\n"
            << "  --measured-keys N      minimum timed keys per ABBA leg (default 32768)\n"
            << "  --workload-batches N   deterministic hot batches retained (default 8)\n"
            << "  --seed N               fixed workload seed (default 0x43414348454b4954)\n"
            << "  --help                 show this message\n"
            << "The matrix is fixed at key lengths 16/32/64/128, batches "
               "8/32/64/128, and hit ratios 0/50/100.\n";
}

Config ParseArgs(int argc, char** argv) {
    Config config;
    for (int index = 1; index < argc; ++index) {
        const std::string argument = argv[index];
        if (argument == "--help") {
            PrintUsage(argv[0]);
            std::exit(0);
        }
        if (index + 1 >= argc) {
            throw std::invalid_argument("missing value for " + argument);
        }
        const std::string value = argv[++index];
        if (argument == "--output") {
            config.output_path = value;
        } else if (argument == "--cycles") {
            config.cycles = ParsePositiveSize("--cycles", value);
        } else if (argument == "--warmup-keys") {
            config.warmup_keys = ParsePositiveSize("--warmup-keys", value);
        } else if (argument == "--measured-keys") {
            config.measured_keys = ParsePositiveSize("--measured-keys", value);
        } else if (argument == "--workload-batches") {
            config.workload_batches =
                    ParsePositiveSize("--workload-batches", value);
        } else if (argument == "--seed") {
            config.seed = ParseSeed(value);
        } else {
            throw std::invalid_argument("unknown option: " + argument);
        }
    }
    return config;
}

std::string SeedString(std::uint64_t seed) {
    std::ostringstream stream;
    stream << "0x" << std::hex << std::setw(16) << std::setfill('0') << seed;
    return stream.str();
}

std::string Number(std::uint64_t value) {
    return std::to_string(value);
}

std::string Number(double value) {
    std::ostringstream stream;
    stream << std::fixed << std::setprecision(9) << value;
    return stream.str();
}

std::string Boolean(bool value) {
    return value ? "1" : "0";
}

std::string CsvEscape(const std::string& value) {
    if (value.find_first_of(",\"\r\n") == std::string::npos) {
        return value;
    }
    std::string escaped = "\"";
    for (char character : value) {
        if (character == '"') {
            escaped += '"';
        }
        escaped += character;
    }
    escaped += '"';
    return escaped;
}

class CsvWriter {
public:
    explicit CsvWriter(std::ostream& output) : output_(output) {
        Write({
                "record_type",
                "status",
                "reason",
                "seed",
                "aarch64",
                "neon",
                "crc32",
                "sve",
                "sve_vl256",
                "pair",
                "cycle",
                "leg",
                "requested_kernel",
                "selected_kernel",
                "key_length",
                "batch_size",
                "hit_ratio",
                "warmup_keys",
                "measured_keys",
                "elapsed_ns",
                "ns_per_key",
                "mkeys_per_s",
                "checksum",
                "samples",
                "abba_order",
                "workload_batches",
                "clock",
                "metric"});
    }

    void Write(const std::vector<std::string>& fields) {
        if (fields.size() != 28U) {
            throw std::logic_error("CSV row has the wrong number of fields");
        }
        for (std::size_t index = 0; index < fields.size(); ++index) {
            if (index != 0) {
                output_ << ',';
            }
            output_ << CsvEscape(fields[index]);
        }
        output_ << '\n';
        if (!output_) {
            throw std::runtime_error("failed to write benchmark CSV");
        }
    }

private:
    std::ostream& output_;
};

std::vector<std::string> CommonRow(
        const char* record_type,
        const char* status,
        const std::string& reason,
        const Config& config,
        const HostFeatures& features) {
    return {
            record_type,
            status,
            reason,
            SeedString(config.seed),
            Boolean(features.aarch64),
            Boolean(features.neon),
            Boolean(features.crc32),
            Boolean(features.sve),
            Boolean(features.sve_vector_length_256),
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            Number(config.warmup_keys),
            Number(config.measured_keys),
            "",
            "",
            "",
            "",
            "",
            kAbbaOrder,
            Number(config.workload_batches),
            "steady_clock",
            kMetric};
}

void StoreU64(std::vector<std::uint8_t>* bytes, std::size_t offset, std::uint64_t value) {
    for (unsigned shift = 0; shift < 64; shift += 8) {
        (*bytes)[offset++] = static_cast<std::uint8_t>(value >> shift);
    }
}

struct Workload {
    std::size_t key_length = 0;
    std::size_t batch_size = 0;
    unsigned hit_ratio = 0;
    std::size_t batch_count = 0;
    std::size_t expected_hits = 0;
    std::vector<std::vector<std::uint8_t>> key_storage;
    std::vector<KeyView> keys;
    std::vector<std::uint8_t> expected_hit;
};

Workload BuildWorkload(
        std::size_t key_length,
        std::size_t batch_size,
        unsigned hit_ratio,
        std::size_t workload_batches,
        std::uint64_t seed) {
    if (workload_batches > std::numeric_limits<std::size_t>::max() / batch_size) {
        throw std::overflow_error("workload size overflow");
    }
    Workload workload;
    workload.key_length = key_length;
    workload.batch_size = batch_size;
    workload.hit_ratio = hit_ratio;
    workload.batch_count = workload_batches;
    const std::size_t total_keys = workload_batches * batch_size;
    workload.key_storage.reserve(total_keys);
    workload.expected_hit.reserve(total_keys);

    const std::size_t hits_per_batch = batch_size * hit_ratio / 100U;
    std::mt19937_64 random(
            seed ^ (static_cast<std::uint64_t>(key_length) << 40U) ^
            (static_cast<std::uint64_t>(batch_size) << 24U) ^ hit_ratio);
    for (std::size_t batch = 0; batch < workload_batches; ++batch) {
        std::vector<std::uint8_t> hit_flags(batch_size, 0);
        std::fill_n(hit_flags.begin(), hits_per_batch, std::uint8_t{1});
        std::shuffle(hit_flags.begin(), hit_flags.end(), random);
        for (std::size_t slot = 0; slot < batch_size; ++slot) {
            const std::size_t logical_id = batch * batch_size + slot;
            const bool hit = hit_flags[slot] != 0;
            std::vector<std::uint8_t> key(key_length);
            std::uint64_t state =
                    seed ^ (static_cast<std::uint64_t>(logical_id) *
                            0x9e3779b97f4a7c15ULL) ^
                    (hit ? 0xd1b54a32d192ed03ULL : 0x94d049bb133111ebULL);
            for (std::size_t byte = 0; byte < key_length; ++byte) {
                state ^= state >> 12U;
                state ^= state << 25U;
                state ^= state >> 27U;
                key[byte] = static_cast<std::uint8_t>(state >> 56U);
            }
            StoreU64(&key, 0, static_cast<std::uint64_t>(logical_id));
            StoreU64(
                    &key,
                    8,
                    (static_cast<std::uint64_t>(hit) << 63U) |
                            static_cast<std::uint64_t>(hit_ratio));
            workload.key_storage.push_back(std::move(key));
            workload.expected_hit.push_back(static_cast<std::uint8_t>(hit));
            workload.expected_hits += static_cast<std::size_t>(hit);
        }
    }

    workload.keys.reserve(total_keys);
    for (std::size_t index = 0; index < total_keys; ++index) {
        workload.keys.push_back(KeyView{
                1U + static_cast<std::uint32_t>(index % 7U),
                11,
                workload.key_storage[index].data(),
                workload.key_storage[index].size()});
    }
    return workload;
}

struct Runner {
    const char* requested_label = "";
    std::unique_ptr<RequestPlane> plane;
    std::string selected_kernel;
};

bool PrepareRunner(
        const char* label,
        KernelPreference preference,
        const Workload& workload,
        Runner* runner,
        std::string* reason) {
    Options options;
    options.capacity_entries =
            std::max<std::size_t>(64, workload.expected_hits + 16U);
    options.key_arena_bytes =
            std::max<std::size_t>(
                    4096,
                    workload.expected_hits * workload.key_length + 128U);
    options.value_arena_bytes =
            std::max<std::size_t>(4096, workload.expected_hits * 16U + 128U);
    options.kernel = preference;

    ErrorCode error = ErrorCode::kInternal;
    std::string message;
    std::unique_ptr<RequestPlane> plane =
            RequestPlane::Create(options, &error, &message);
    if (!plane) {
        *reason = std::string(cachekit::native::ErrorCodeName(error)) + ":" + message;
        return false;
    }

    const std::array<std::uint8_t, 16> value = {
            0x43, 0x61, 0x63, 0x68, 0x65, 0x4b, 0x69, 0x74,
            0x2d, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x21};
    std::vector<FillView> fills;
    fills.reserve(workload.expected_hits);
    for (std::size_t index = 0; index < workload.keys.size(); ++index) {
        if (workload.expected_hit[index] != 0) {
            fills.push_back(FillView{
                    workload.keys[index], value.data(), value.size(), false});
        }
    }
    std::vector<FillResult> fill_results(fills.size());
    const ErrorCode fill_error =
            plane->FillBatch(fills.data(), fill_results.data(), fills.size());
    if (fill_error != ErrorCode::kOk) {
        throw std::runtime_error("FillBatch failed while preparing benchmark");
    }
    for (const FillResult& result : fill_results) {
        if (result.status != FillStatus::kInserted || result.error != ErrorCode::kOk) {
            throw std::runtime_error("benchmark cache preparation rejected a fill");
        }
    }

    std::vector<ProbeResult> probe_results(workload.batch_size);
    std::size_t observed_hits = 0;
    for (std::size_t batch = 0; batch < workload.batch_count; ++batch) {
        const std::size_t offset = batch * workload.batch_size;
        const ErrorCode probe_error = plane->ProbeBatch(
                workload.keys.data() + offset,
                probe_results.data(),
                workload.batch_size);
        if (probe_error != ErrorCode::kOk) {
            throw std::runtime_error("ProbeBatch failed during workload validation");
        }
        for (std::size_t slot = 0; slot < workload.batch_size; ++slot) {
            const bool expected = workload.expected_hit[offset + slot] != 0;
            const bool observed = probe_results[slot].status == ProbeStatus::kHit;
            if (probe_results[slot].error != ErrorCode::kOk || expected != observed) {
                throw std::runtime_error("observed hit ratio does not match workload");
            }
            observed_hits += static_cast<std::size_t>(observed);
        }
    }
    if (observed_hits != workload.expected_hits) {
        throw std::runtime_error("validated hit count mismatch");
    }

    runner->requested_label = label;
    runner->selected_kernel = plane->kernel_name();
    runner->plane = std::move(plane);
    return true;
}

struct Measurement {
    std::uint64_t elapsed_ns = 0;
    std::size_t measured_keys = 0;
    double ns_per_key = 0.0;
    double mkeys_per_second = 0.0;
    std::uint64_t checksum = 0;
};

Measurement Measure(
        Runner* runner, const Workload& workload, std::size_t minimum_keys) {
    if (minimum_keys >
        std::numeric_limits<std::size_t>::max() - (workload.batch_size - 1U)) {
        throw std::overflow_error("measured key count overflow");
    }
    const std::size_t timed_batches =
            std::max<std::size_t>(
                    1, (minimum_keys + workload.batch_size - 1U) /
                               workload.batch_size);
    std::vector<ProbeResult> results(workload.batch_size);
    std::uint64_t checksum = 0xcbf29ce484222325ULL;
    const auto start = std::chrono::steady_clock::now();
    for (std::size_t batch = 0; batch < timed_batches; ++batch) {
        const std::size_t workload_batch = batch % workload.batch_count;
        const ErrorCode error = runner->plane->ProbeBatch(
                workload.keys.data() + workload_batch * workload.batch_size,
                results.data(),
                workload.batch_size);
        if (error != ErrorCode::kOk) {
            throw std::runtime_error("timed ProbeBatch failed");
        }
        const ProbeResult& sampled = results[batch % workload.batch_size];
        checksum ^= static_cast<std::uint64_t>(sampled.status) +
                (static_cast<std::uint64_t>(sampled.value_size) << 8U);
        checksum *= 0x100000001b3ULL;
    }
    const auto stop = std::chrono::steady_clock::now();
    const auto elapsed =
            std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    if (elapsed <= 0) {
        throw std::runtime_error("steady_clock returned a non-positive interval");
    }
    const std::size_t measured_keys = timed_batches * workload.batch_size;
    g_checksum_sink =
            (g_checksum_sink ^ checksum ^
             static_cast<std::uint64_t>(measured_keys)) *
            0x100000001b3ULL;
    const double elapsed_ns = static_cast<double>(elapsed);
    return Measurement{
            static_cast<std::uint64_t>(elapsed),
            measured_keys,
            elapsed_ns / static_cast<double>(measured_keys),
            static_cast<double>(measured_keys) * 1000.0 / elapsed_ns,
            checksum};
}

double Median(std::vector<double> values) {
    if (values.empty()) {
        throw std::logic_error("cannot compute an empty median");
    }
    std::sort(values.begin(), values.end());
    const std::size_t middle = values.size() / 2U;
    if ((values.size() % 2U) != 0) {
        return values[middle];
    }
    return (values[middle - 1U] + values[middle]) / 2.0;
}

void WriteMeasurementRow(
        CsvWriter* writer,
        const Config& config,
        const HostFeatures& features,
        const std::string& pair,
        std::size_t cycle,
        const char* leg,
        const Runner& runner,
        const Workload& workload,
        const Measurement& measurement) {
    std::vector<std::string> row =
            CommonRow("raw", "ok", "", config, features);
    row[9] = pair;
    row[10] = Number(cycle);
    row[11] = leg;
    row[12] = runner.requested_label;
    row[13] = runner.selected_kernel;
    row[14] = Number(workload.key_length);
    row[15] = Number(workload.batch_size);
    row[16] = Number(static_cast<std::size_t>(workload.hit_ratio));
    row[18] = Number(measurement.measured_keys);
    row[19] = Number(measurement.elapsed_ns);
    row[20] = Number(measurement.ns_per_key);
    row[21] = Number(measurement.mkeys_per_second);
    row[22] = Number(measurement.checksum);
    row[23] = "1";
    writer->Write(row);
}

void WriteSummaryRow(
        CsvWriter* writer,
        const Config& config,
        const HostFeatures& features,
        const std::string& pair,
        const Runner& runner,
        const Workload& workload,
        const std::vector<double>& samples) {
    const double median_ns = Median(samples);
    std::vector<std::string> row =
            CommonRow("summary", "ok", "", config, features);
    row[9] = pair;
    row[12] = runner.requested_label;
    row[13] = runner.selected_kernel;
    row[14] = Number(workload.key_length);
    row[15] = Number(workload.batch_size);
    row[16] = Number(static_cast<std::size_t>(workload.hit_ratio));
    row[20] = Number(median_ns);
    row[21] = Number(1000.0 / median_ns);
    row[23] = Number(samples.size());
    writer->Write(row);
}

void WriteSkipRow(
        CsvWriter* writer,
        const Config& config,
        const HostFeatures& features,
        const std::string& pair,
        const Candidate& candidate,
        const Workload& workload,
        const std::string& reason) {
    std::vector<std::string> row =
            CommonRow("skip", "unsupported", reason, config, features);
    row[9] = pair;
    row[12] = candidate.label;
    row[14] = Number(workload.key_length);
    row[15] = Number(workload.batch_size);
    row[16] = Number(static_cast<std::size_t>(workload.hit_ratio));
    writer->Write(row);
}

void RunPair(
        CsvWriter* writer,
        const Config& config,
        const HostFeatures& features,
        const Candidate& baseline,
        const Candidate& candidate,
        const Workload& workload) {
    const std::string pair =
            std::string(baseline.label) + "_vs_" + candidate.label;
    Runner candidate_runner;
    std::string reason;
    if (!PrepareRunner(
                candidate.label,
                candidate.preference,
                workload,
                &candidate_runner,
                &reason)) {
        WriteSkipRow(
                writer, config, features, pair, candidate, workload, reason);
        return;
    }

    Runner baseline_runner;
    if (!PrepareRunner(
                baseline.label,
                baseline.preference,
                workload,
                &baseline_runner,
                &reason)) {
        WriteSkipRow(
                writer, config, features, pair, baseline, workload, reason);
        return;
    }

    std::cerr << "pair=" << pair << " key=" << workload.key_length
              << " batch=" << workload.batch_size
              << " hit=" << workload.hit_ratio
              << " selected_A=" << baseline_runner.selected_kernel
              << " selected_B=" << candidate_runner.selected_kernel << '\n';
    (void) Measure(&baseline_runner, workload, config.warmup_keys);
    (void) Measure(&candidate_runner, workload, config.warmup_keys);

    std::vector<double> baseline_samples;
    std::vector<double> candidate_samples;
    baseline_samples.reserve(config.cycles * 2U);
    candidate_samples.reserve(config.cycles * 2U);
    constexpr std::array<unsigned, 4> kOrder = {0, 1, 1, 0};
    constexpr std::array<const char*, 4> kLegs = {"A1", "B1", "B2", "A2"};
    for (std::size_t cycle = 0; cycle < config.cycles; ++cycle) {
        std::uint64_t expected_checksum = 0;
        for (std::size_t position = 0; position < kOrder.size(); ++position) {
            Runner* runner =
                    kOrder[position] == 0 ? &baseline_runner : &candidate_runner;
            const Measurement measurement =
                    Measure(runner, workload, config.measured_keys);
            if (position == 0) {
                expected_checksum = measurement.checksum;
            } else if (measurement.checksum != expected_checksum) {
                throw std::runtime_error("kernel checksum mismatch in ABBA cycle");
            }
            WriteMeasurementRow(
                    writer,
                    config,
                    features,
                    pair,
                    cycle,
                    kLegs[position],
                    *runner,
                    workload,
                    measurement);
            (kOrder[position] == 0 ? baseline_samples : candidate_samples)
                    .push_back(measurement.ns_per_key);
        }
    }
    WriteSummaryRow(
            writer,
            config,
            features,
            pair,
            baseline_runner,
            workload,
            baseline_samples);
    WriteSummaryRow(
            writer,
            config,
            features,
            pair,
            candidate_runner,
            workload,
            candidate_samples);
}

int Run(int argc, char** argv) {
    const Config config = ParseArgs(argc, argv);
    std::ofstream file;
    std::ostream* output = &std::cout;
    if (config.output_path != "-") {
        file.open(config.output_path, std::ios::out | std::ios::trunc);
        if (!file) {
            throw std::runtime_error("cannot open CSV output: " + config.output_path);
        }
        output = &file;
    }

    const HostFeatures features = cachekit::native::DetectHostFeatures();
    std::cerr << "detected_features"
              << " aarch64=" << features.aarch64
              << " neon=" << features.neon
              << " crc32=" << features.crc32
              << " sve=" << features.sve
              << " sve_vl256=" << features.sve_vector_length_256
              << " seed=" << SeedString(config.seed) << '\n';

    CsvWriter writer(*output);
    writer.Write(CommonRow("metadata", "ok", "", config, features));
    for (std::size_t key_length : kKeyLengths) {
        for (std::size_t batch_size : kBatchSizes) {
            for (unsigned hit_ratio : kHitRatios) {
                const Workload workload = BuildWorkload(
                        key_length,
                        batch_size,
                        hit_ratio,
                        config.workload_batches,
                        config.seed);
                for (const Candidate& candidate : kCandidates) {
                    RunPair(
                            &writer,
                            config,
                            features,
                            kScalar,
                            candidate,
                            workload);
                }
                // This is the architecture-specific comparison. Both sides use
                // the AArch64 CRC32C instructions, so it does not confound SVE
                // with the software scalar CRC implementation.
                RunPair(
                        &writer,
                        config,
                        features,
                        kCandidates[0],
                        kCandidates[1],
                        workload);
            }
        }
    }
    std::vector<std::string> complete =
            CommonRow("complete", "ok", "", config, features);
    complete[22] = Number(g_checksum_sink);
    writer.Write(complete);
    output->flush();
    if (!*output) {
        throw std::runtime_error("failed to flush benchmark CSV");
    }
    std::cerr << "benchmark_complete checksum=" << g_checksum_sink << '\n';
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        return Run(argc, argv);
    } catch (const std::exception& exception) {
        std::cerr << "benchmark_failed: " << exception.what() << '\n';
        return 1;
    }
}
