#!/usr/bin/env bash
set -euo pipefail

expected_source=a95bcc56d2207a5ac6cd3ba2e62459bc5d409ce4
expected_p3_arm_runtime_sha=0a1981cbd3591b4c99dbd5d7d3a32170ab6c630fa1dcdb4faa16005b9acf5f49
expected_p1_x86_runtime_sha=58676b125fe20a5e7f9994e4531f06f8f076e1b3fc150ee1f222fabbe4e20602

usage() {
  echo "usage: $0 /absolute/path/to/{p3-aarch64|p1-x86}-runtime.jar" >&2
  exit 64
}

[[ $# -eq 1 ]] || usage
base_runtime=$1
[[ $base_runtime = /* && -f $base_runtime ]] || usage

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
git -C "$repo" diff --quiet || {
  echo "tracked worktree changes present; refusing artifact build" >&2
  exit 66
}

base_runtime_sha=$(sha256sum "$base_runtime" | awk '{print $1}')
case $base_runtime_sha in
  "$expected_p3_arm_runtime_sha")
    base_label=p3-aarch64
    candidate_name=flink-statebackend-cachekit-1.16-SNAPSHOT-p4-aarch64.jar
    ;;
  "$expected_p1_x86_runtime_sha")
    base_label=p1-x86
    candidate_name=flink-statebackend-cachekit-1.16-SNAPSHOT-p4-x86.jar
    ;;
  *)
    echo "unsupported frozen base runtime: $base_runtime_sha" >&2
    exit 67
    ;;
esac

common_maven=(
  -DskipITs
  -Dcheckstyle.skip
  -Drat.skip
  -Dspotless.check.skip=true
)

cd "$repo"
./mvnw -pl flink-state-backends/flink-statebackend-rocksdb "${common_maven[@]}" \
  -Dtest=RocksDBStateBackendConfigTest test

classes=$repo/flink-state-backends/flink-statebackend-rocksdb/target/classes
candidate=$repo/flink-state-backends/flink-statebackend-cachekit/target/$candidate_name
cp "$base_runtime" "$candidate"

overlay=$(mktemp -d /tmp/cachekit-p4-overlay.XXXXXX)
cleanup() {
  [[ $overlay == /tmp/cachekit-p4-overlay.* && -d $overlay ]] && rm -rf -- "$overlay"
}
trap cleanup EXIT

stems=(
  org/apache/flink/contrib/streaming/state/RocksDBConfigurableOptions
  org/apache/flink/contrib/streaming/state/RocksDBResourceContainer
)
for stem in "${stems[@]}"; do
  found=0
  while IFS= read -r -d '' class_file; do
    found=1
    relative=${class_file#"$classes"/}
    mkdir -p "$overlay/$(dirname "$relative")"
    cp "$class_file" "$overlay/$relative"
  done < <(find "$classes/$(dirname "$stem")" -maxdepth 1 \
    -type f -name "$(basename "$stem")*.class" -print0)
  ((found == 1)) || { echo "missing RocksDB class stem: $stem" >&2; exit 68; }
done

(cd "$overlay" && jar uf "$candidate" org)
unzip -t "$candidate" >/dev/null

base_entries=$overlay/base.entries
candidate_entries=$overlay/candidate.entries
overlay_entries=$overlay/overlay.entries
unzip -Z1 "$base_runtime" | LC_ALL=C sort -u >"$base_entries"
unzip -Z1 "$candidate" | LC_ALL=C sort -u >"$candidate_entries"
find "$overlay/org" -type f -name '*.class' -printf '%P\n' | \
  sed 's#^#org/#' | LC_ALL=C sort -u >"$overlay_entries"
cmp -s "$base_entries" "$candidate_entries" || {
  echo "candidate entry set differs from frozen P3 runtime" >&2
  diff -u "$base_entries" "$candidate_entries" >&2 || true
  exit 69
}

while IFS= read -r entry; do
  if grep -Fxq "$entry" "$overlay_entries"; then
    cmp -s "$overlay/$entry" <(unzip -p "$candidate" "$entry") || {
      echo "RocksDB overlay mismatch: $entry" >&2
      exit 70
    }
  else
    cmp -s <(unzip -p "$base_runtime" "$entry") <(unzip -p "$candidate" "$entry") || {
      echo "non-overlay entry changed: $entry" >&2
      exit 71
    }
  fi
done <"$base_entries"

candidate_sha=$(sha256sum "$candidate" | awk '{print $1}')
class_count=$(wc -l <"$overlay_entries")
manifest=$candidate.BUILD.txt
{
  echo "source_commit=$expected_source"
  echo "branch_head=$branch_head"
  echo "base_runtime_label=$base_label"
  echo "base_runtime_sha256=$base_runtime_sha"
  echo "candidate_sha256=$candidate_sha"
  echo "candidate_path=$candidate"
  echo "rocksdb_overlay_class_count=$class_count"
  echo "non_overlay_entries_identical=true"
} >"$manifest"

echo "candidate=$candidate"
echo "sha256=$candidate_sha"
echo "manifest=$manifest"
