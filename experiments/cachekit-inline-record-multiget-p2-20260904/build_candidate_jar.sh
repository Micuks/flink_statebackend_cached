#!/usr/bin/env bash
set -euo pipefail

expected_source=a7aa558791becb34164b27c787ab6cffdebf0dc2
expected_p1_runtime_sha=97487e04d293f154b8856e43decf2573fabaaf35e36077ea635a1f55d0f59dfc

usage() {
  echo "usage: $0 /absolute/path/to/p1-aarch64-runtime.jar" >&2
  exit 64
}

[[ $# -eq 1 ]] || usage
p1_runtime=$1
[[ $p1_runtime = /* && -f $p1_runtime ]] || usage

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel)
branch_head=$(git -C "$repo" rev-parse HEAD)
git -C "$repo" merge-base --is-ancestor "$expected_source" "$branch_head" || {
  echo "expected source is not an ancestor of branch head: $branch_head" >&2
  exit 65
}
git -C "$repo" diff --quiet "$expected_source" "$branch_head" -- \
  flink-streaming-java flink-state-backends/flink-statebackend-cachekit \
  flink-state-backends/flink-statebackend-rocksdb flink-table/flink-table-runtime || {
  echo "runtime source differs from expected commit $expected_source" >&2
  exit 65
}
[[ -z $(git -C "$repo" status --porcelain --untracked-files=all) ]] || {
  echo "worktree changes present; refusing artifact build" >&2
  exit 66
}

p1_runtime_sha=$(sha256sum "$p1_runtime" | awk '{print $1}')
[[ $p1_runtime_sha == "$expected_p1_runtime_sha" ]] || {
  echo "P1 runtime mismatch: expected $expected_p1_runtime_sha, got $p1_runtime_sha" >&2
  exit 67
}

common_maven=(
  -DskipITs
  -Dcheckstyle.skip
  -Drat.skip
  -Dspotless.check.skip=true
)

cd "$repo"
./mvnw -pl flink-streaming-java "${common_maven[@]}" \
  -Dtest=StreamRecordBatchOutputTest,StreamOneInputProcessorTest,StatePrefetcherTest test
./mvnw -pl flink-state-backends/flink-statebackend-cachekit "${common_maven[@]}" \
  -Dtest=PrefetchExecutorTest,CachedInternalValueStateTest,CacheKitKeyedStateBackendLifecycleTest test
./mvnw -pl flink-state-backends/flink-statebackend-cachekit "${common_maven[@]}" \
  -DskipTests package

streaming_classes=$repo/flink-streaming-java/target/classes
candidate=$repo/flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT.jar
[[ -f $candidate ]] || { echo "candidate jar missing: $candidate" >&2; exit 68; }

overlay=$(mktemp -d /tmp/cachekit-p2-overlay.XXXXXX)
cleanup() { rm -rf -- "$overlay"; }
trap cleanup EXIT

# P1 independently proved these RocksDB/table entries identical to the frozen bbd39 runtime.
# Reuse only those exact entries; P2 CacheKit classes come from the current package below.
mapfile -t frozen_entries < <(
  jar tf "$p1_runtime" | awk '
    /^org\/apache\/flink\/contrib\/streaming\/state\/(AbstractRocksDBState|RocksDBBatchValueReader|RocksDBConfigurableOptions|RocksDBResourceContainer|RocksDBValueState).*\.class$/ ||
    /^org\/apache\/flink\/table\/runtime\/operators\/aggregate\/GroupAggFunction.*\.class$/ ||
    /^org\/apache\/flink\/table\/runtime\/operators\/deduplicate\/RowTimeDeduplicateFunction.*\.class$/
  '
)
(( ${#frozen_entries[@]} > 0 )) || { echo "no frozen overlay entries" >&2; exit 69; }
(cd "$overlay" && jar xf "$p1_runtime" "${frozen_entries[@]}")

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
  (( found == 1 )) || { echo "missing current streaming class stem: $stem" >&2; exit 70; }
done

(cd "$overlay" && jar uf "$candidate" .)
unzip -t "$candidate" >/dev/null
while IFS= read -r -d '' overlay_file; do
  relative=${overlay_file#"$overlay"/}
  cmp -s "$overlay_file" <(unzip -p "$candidate" "$relative") || {
    echo "final jar overlay mismatch: $relative" >&2
    exit 71
  }
done < <(find "$overlay" -type f -name '*.class' -print0)

candidate_sha=$(sha256sum "$candidate" | awk '{print $1}')
manifest=$repo/flink-state-backends/flink-statebackend-cachekit/target/P2_CANDIDATE_BUILD.txt
{
  echo "source_commit=$expected_source"
  echo "branch_head=$branch_head"
  echo "p1_runtime_sha256=$p1_runtime_sha"
  echo "candidate_sha256=$candidate_sha"
  echo "candidate_path=$candidate"
  echo "frozen_overlay_entry_count=${#frozen_entries[@]}"
} >"$manifest"

echo "candidate=$candidate"
echo "sha256=$candidate_sha"
echo "manifest=$manifest"
