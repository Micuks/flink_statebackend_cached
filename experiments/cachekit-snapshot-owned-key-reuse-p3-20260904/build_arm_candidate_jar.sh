#!/usr/bin/env bash
set -euo pipefail

expected_source=18cfb52bdb6e740d9ae716da0a03761481e885e2
expected_p2_runtime_sha=164411a963b78ccd508524c9ca4e1a0d50b497830394a59c016ed3ef644fd741

usage() {
  echo "usage: $0 /absolute/path/to/p2-aarch64-runtime.jar" >&2
  exit 64
}

[[ $# -eq 1 ]] || usage
p2_runtime=$1
[[ $p2_runtime = /* && -f $p2_runtime ]] || usage

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel)
branch_head=$(git -C "$repo" rev-parse HEAD)
[[ $branch_head == "$expected_source" ]] || {
  echo "branch head mismatch: expected $expected_source, got $branch_head" >&2
  exit 65
}
[[ -z $(git -C "$repo" status --porcelain --untracked-files=all) ]] || {
  echo "worktree changes present; refusing artifact build" >&2
  exit 66
}

p2_runtime_sha=$(sha256sum "$p2_runtime" | awk '{print $1}')
[[ $p2_runtime_sha == "$expected_p2_runtime_sha" ]] || {
  echo "P2 runtime mismatch: expected $expected_p2_runtime_sha, got $p2_runtime_sha" >&2
  exit 67
}

common_maven=(
  -DskipITs
  -Dcheckstyle.skip
  -Drat.skip
  -Dspotless.check.skip=true
)

cd "$repo"
./mvnw -pl flink-state-backends/flink-statebackend-cachekit "${common_maven[@]}" test

classes=$repo/flink-state-backends/flink-statebackend-cachekit/target/classes
prefix=org/apache/flink/contrib/streaming/state/cachekit
[[ -d $classes/$prefix ]] || {
  echo "compiled CacheKit classes missing: $classes/$prefix" >&2
  exit 68
}

candidate=$repo/flink-state-backends/flink-statebackend-cachekit/target/flink-statebackend-cachekit-1.16-SNAPSHOT-p3-aarch64.jar
cp "$p2_runtime" "$candidate"

overlay=$(mktemp -d /tmp/cachekit-p3-overlay.XXXXXX)
cleanup() { rm -rf -- "$overlay"; }
trap cleanup EXIT
mkdir -p "$overlay/$prefix"
cp -a "$classes/$prefix/." "$overlay/$prefix/"

(cd "$overlay" && jar uf "$candidate" .)
unzip -t "$candidate" >/dev/null

base_entries=$overlay/base.entries
candidate_entries=$overlay/candidate.entries
unzip -Z1 "$p2_runtime" | LC_ALL=C sort -u >"$base_entries"
unzip -Z1 "$candidate" | LC_ALL=C sort -u >"$candidate_entries"
cmp -s "$base_entries" "$candidate_entries" || {
  echo "candidate entry set differs from frozen P2 runtime" >&2
  diff -u "$base_entries" "$candidate_entries" >&2 || true
  exit 69
}

while IFS= read -r entry; do
  [[ $entry == "$prefix/"* ]] && continue
  cmp -s <(unzip -p "$p2_runtime" "$entry") <(unzip -p "$candidate" "$entry") || {
    echo "non-CacheKit entry changed: $entry" >&2
    exit 70
  }
done <"$base_entries"

while IFS= read -r -d '' overlay_file; do
  relative=${overlay_file#"$overlay"/}
  cmp -s "$overlay_file" <(unzip -p "$candidate" "$relative") || {
    echo "CacheKit overlay mismatch: $relative" >&2
    exit 71
  }
done < <(find "$overlay/$prefix" -type f -name '*.class' -print0)

candidate_sha=$(sha256sum "$candidate" | awk '{print $1}')
class_count=$(find "$overlay/$prefix" -type f -name '*.class' | wc -l)
manifest=$candidate.BUILD.txt
{
  echo "source_commit=$expected_source"
  echo "branch_head=$branch_head"
  echo "p2_runtime_sha256=$p2_runtime_sha"
  echo "candidate_sha256=$candidate_sha"
  echo "candidate_path=$candidate"
  echo "cachekit_overlay_class_count=$class_count"
  echo "non_cachekit_entries_identical=true"
} >"$manifest"

echo "candidate=$candidate"
echo "sha256=$candidate_sha"
echo "manifest=$manifest"
