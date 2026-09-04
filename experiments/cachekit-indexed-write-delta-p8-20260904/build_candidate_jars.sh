#!/usr/bin/env bash
set -euo pipefail

expected_source=bff4e00f1c18783e58a29f270dac58b05e9f1f4b
expected_x86_base_sha=66f1bac9c25c6e8a777b2ca370e2b21621cccc57e020d869db2fd7c245c6a46b
expected_arm_base_sha=3c52ba5be2c2f903f348a991659036fa60a44bc161d4a3efb150b00cbd49c723

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
git -C "$repo" diff --quiet && git -C "$repo" diff --cached --quiet || {
  echo "tracked worktree changes present; refusing artifact build" >&2
  exit 66
}

common_maven=(
  -DskipITs
  -Dcheckstyle.skip
  -Drat.skip
  -Dspotless.check.skip=true
)

cd "$repo"
./mvnw -pl flink-state-backends/flink-statebackend-rocksdb -am \
  "${common_maven[@]}" \
  -Dtest=RocksDBWriteBatchWrapperTest,RocksDBBatchMapReaderTest,RocksDBStateBackendConfigTest \
  -Dsurefire.failIfNoSpecifiedTests=false test

classes=$repo/flink-state-backends/flink-statebackend-rocksdb/target/classes
target_dir=$repo/flink-state-backends/flink-statebackend-cachekit/target
overlay=$(mktemp -d /tmp/cachekit-p8-overlay.XXXXXX)
cleanup() {
  [[ $overlay == /tmp/cachekit-p8-overlay.* && -d $overlay ]] && rm -rf -- "$overlay"
}
trap cleanup EXIT

stems=(
  org/apache/flink/contrib/streaming/state/RocksDBConfigurableOptions
  org/apache/flink/contrib/streaming/state/RocksDBResourceContainer
  org/apache/flink/contrib/streaming/state/RocksDBKeyedStateBackendBuilder
  org/apache/flink/contrib/streaming/state/RocksDBKeyedStateBackend
  org/apache/flink/contrib/streaming/state/RocksDBMapState
  org/apache/flink/contrib/streaming/state/RocksDBWriteBatchWrapper
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

build_one() {
  local arch=$1 expected_base_sha=$2 base candidate base_sha candidate_sha
  base=$target_dir/flink-statebackend-cachekit-1.16-SNAPSHOT-p4-$arch.jar
  candidate=$target_dir/flink-statebackend-cachekit-1.16-SNAPSHOT-p8-$arch.jar
  [[ -f $base ]] || { echo "missing frozen P4 base: $base" >&2; exit 67; }
  base_sha=$(sha256sum "$base" | awk '{print $1}')
  [[ $base_sha == "$expected_base_sha" ]] || {
    echo "unexpected $arch P4 base: $base_sha" >&2
    exit 67
  }

  cp "$base" "$candidate"
  (cd "$overlay" && jar uf "$candidate" org)
  unzip -t "$candidate" >/dev/null

  unzip -Z1 "$base" | LC_ALL=C sort -u >"$overlay/$arch.base.entries"
  unzip -Z1 "$candidate" | LC_ALL=C sort -u >"$overlay/$arch.candidate.entries"
  find "$overlay/org" -type f -name '*.class' -printf '%P\n' | \
    sed 's#^#org/#' | LC_ALL=C sort -u >"$overlay/overlay.entries"
  cmp -s "$overlay/$arch.base.entries" "$overlay/$arch.candidate.entries" || {
    echo "$arch candidate entry set differs from frozen P4 runtime" >&2
    exit 69
  }

  while IFS= read -r entry; do
    if grep -Fxq "$entry" "$overlay/overlay.entries"; then
      cmp -s "$overlay/$entry" <(unzip -p "$candidate" "$entry") || {
        echo "RocksDB overlay mismatch: $entry" >&2
        exit 70
      }
    else
      cmp -s <(unzip -p "$base" "$entry") <(unzip -p "$candidate" "$entry") || {
        echo "non-overlay entry changed: $entry" >&2
        exit 71
      }
    fi
  done <"$overlay/$arch.base.entries"

  candidate_sha=$(sha256sum "$candidate" | awk '{print $1}')
  {
    echo "source_commit=$expected_source"
    echo "branch_head=$branch_head"
    echo "base_runtime_sha256=$base_sha"
    echo "candidate_sha256=$candidate_sha"
    echo "candidate_path=$candidate"
    echo "rocksdb_overlay_class_count=$(wc -l <"$overlay/overlay.entries")"
    echo "non_overlay_entries_identical=true"
  } >"$candidate.BUILD.txt"
  echo "$arch candidate=$candidate"
  echo "$arch sha256=$candidate_sha"
}

build_one x86 "$expected_x86_base_sha"
build_one aarch64 "$expected_arm_base_sha"
