#!/usr/bin/env bash
set -euo pipefail

expected_source=7a9e568dcbd75ab4f24e7a33a4339f5cd1cf8000
frozen_source=bbd39affde9278d44b9f78d201849bf929249d54
expected_frozen_sha=869ec0e983dfb2a4e35b93ebfe9d50f5595757d7cebd94a4c14a2d5b58d2ed5e

usage() {
  echo "usage: $0 /absolute/path/to/frozen-bbd39-cachekit.jar" >&2
  exit 64
}

[[ $# -eq 1 ]] || usage
frozen_jar=$1
[[ $frozen_jar = /* && -f $frozen_jar ]] || usage

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel)
actual_source=$(git -C "$repo" rev-parse HEAD)
[[ $actual_source == "$expected_source" ]] || {
  echo "source mismatch: expected $expected_source, got $actual_source" >&2
  exit 65
}
[[ -z $(git -C "$repo" status --porcelain --untracked-files=all) ]] || {
  echo "tracked worktree changes present; refusing artifact build" >&2
  exit 66
}
git -C "$repo" diff --quiet "$frozen_source" "$expected_source" -- \
  flink-state-backends/flink-statebackend-rocksdb flink-table/flink-table-runtime || {
  echo "frozen RocksDB/table overlay sources differ from current source" >&2
  exit 67
}

actual_frozen_sha=$(sha256sum "$frozen_jar" | awk '{print $1}')
[[ $actual_frozen_sha == "$expected_frozen_sha" ]] || {
  echo "frozen jar mismatch: expected $expected_frozen_sha, got $actual_frozen_sha" >&2
  exit 68
}

common_maven=(
  -DskipITs
  -Dcheckstyle.skip
  -Drat.skip
  -Dspotless.check.skip=true
)

cd "$repo"
./mvnw -pl flink-streaming-java "${common_maven[@]}" \
  -Dtest=StreamRecordBatchOutputTest test
./mvnw -pl flink-state-backends/flink-statebackend-cachekit "${common_maven[@]}" \
  -Dtest=CachedInternalValueStateTest test
./mvnw -pl flink-state-backends/flink-statebackend-cachekit "${common_maven[@]}" \
  -DskipTests package

streaming_classes=$repo/flink-streaming-java/target/classes
candidate=$repo/flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT.jar
[[ -f $candidate ]] || { echo "candidate jar missing: $candidate" >&2; exit 69; }

overlay=$(mktemp -d)
cleanup() { rm -rf -- "$overlay"; }
trap cleanup EXIT

# These two modules are byte-identical between frozen_source and expected_source.
# Reuse their audited classes from the frozen runtime artifact instead of an
# unrelated local Maven snapshot.
mapfile -t frozen_entries < <(
  jar tf "$frozen_jar" | awk '
    /^org\/apache\/flink\/contrib\/streaming\/state\/(AbstractRocksDBState|RocksDBBatchValueReader|RocksDBConfigurableOptions|RocksDBResourceContainer|RocksDBValueState).*\.class$/ ||
    /^org\/apache\/flink\/table\/runtime\/operators\/aggregate\/GroupAggFunction.*\.class$/ ||
    /^org\/apache\/flink\/table\/runtime\/operators\/deduplicate\/RowTimeDeduplicateFunction.*\.class$/
  '
)
(( ${#frozen_entries[@]} > 0 )) || { echo "no frozen overlay entries" >&2; exit 70; }
(cd "$overlay" && jar xf "$frozen_jar" "${frozen_entries[@]}")

streaming_stems=(
  org/apache/flink/streaming/api/operators/BatchProcessingOperator
  org/apache/flink/streaming/api/operators/BatchableKeyedFunction
  org/apache/flink/streaming/api/operators/CommutativeKeyedOperator
  org/apache/flink/streaming/runtime/io/BatchOutput
  org/apache/flink/streaming/runtime/io/CollapseProbe
  org/apache/flink/streaming/runtime/io/LocalPreagg
  org/apache/flink/streaming/runtime/io/StreamRecordBatchOutput
  org/apache/flink/streaming/runtime/io/StreamOneInputProcessor
  org/apache/flink/streaming/runtime/tasks/BatchedKeyedOperatorAdapter
  org/apache/flink/streaming/runtime/tasks/OneInputStreamTask
  org/apache/flink/streaming/runtime/tasks/StatePrefetcher
)
for stem in "${streaming_stems[@]}"; do
  found=0
  while IFS= read -r -d '' class_file; do
    found=1
    relative=${class_file#"$streaming_classes"/}
    mkdir -p "$overlay/$(dirname "$relative")"
    cp "$class_file" "$overlay/$relative"
  done < <(find "$streaming_classes/$(dirname "$stem")" -maxdepth 1 \
    -type f -name "$(basename "$stem")*.class" -print0)
  (( found == 1 )) || { echo "missing current streaming class stem: $stem" >&2; exit 71; }
done

(cd "$overlay" && jar uf "$candidate" .)
unzip -t "$candidate" >/dev/null

candidate_sha=$(sha256sum "$candidate" | awk '{print $1}')
manifest=$repo/flink-state-backends/flink-statebackend-cachekit/target/CANDIDATE_BUILD.txt
{
  echo "source_commit=$actual_source"
  echo "frozen_overlay_commit=$frozen_source"
  echo "frozen_overlay_sha256=$actual_frozen_sha"
  echo "candidate_sha256=$candidate_sha"
  echo "candidate_path=$candidate"
} >"$manifest"

echo "candidate=$candidate"
echo "sha256=$candidate_sha"
echo "manifest=$manifest"
