#!/usr/bin/env bash
set -euo pipefail

expected_source=8780838608a9c4ef1f374b91873ad3be7f576782
expected_x86_p9_sha=2d9214f7afbe25997b2762e66025647acfc11258954eadb148940d55aceaafb5
expected_arm_p9_sha=c80df5f40e3604db17638602917fd7fb813a8f3828590fcb79145768705590c3

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel)
branch_head=$(git -C "$repo" rev-parse HEAD)

[[ $branch_head == "$expected_source" ]] || {
  echo "expected source $expected_source, found $branch_head" >&2
  exit 65
}
git -C "$repo" diff --quiet && git -C "$repo" diff --cached --quiet || {
  echo "tracked worktree changes present; refusing artifact build" >&2
  exit 66
}

cd "$repo"
./mvnw -pl flink-state-backends/flink-statebackend-cachekit \
  -DskipITs -Dcheckstyle.skip -Drat.skip -Dspotless.check.skip=true \
  -Dtest=CachedInternalMapStateTest test

classes=$repo/flink-state-backends/flink-statebackend-cachekit/target/classes
target_dir=$repo/flink-state-backends/flink-statebackend-cachekit/target
stem=org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalMapState
overlay=$(mktemp -d /tmp/cachekit-p10-overlay.XXXXXX)
cleanup() {
  [[ $overlay == /tmp/cachekit-p10-overlay.* && -d $overlay ]] && rm -rf -- "$overlay"
}
trap cleanup EXIT

found=0
while IFS= read -r -d '' class_file; do
  found=1
  relative=${class_file#"$classes"/}
  mkdir -p "$overlay/$(dirname "$relative")"
  cp "$class_file" "$overlay/$relative"
done < <(find "$classes/$(dirname "$stem")" -maxdepth 1 \
  -type f -name "$(basename "$stem")*.class" -print0)
((found == 1)) || { echo "missing class stem: $stem" >&2; exit 68; }

find "$overlay/org" -type f -name '*.class' -printf '%P\n' | \
  sed 's#^#org/#' | LC_ALL=C sort -u >"$overlay/overlay.entries"

build_one() {
  local arch=$1 expected_p9_sha=$2 base candidate base_sha candidate_sha entry
  base=$target_dir/flink-statebackend-cachekit-1.16-SNAPSHOT-p9-$arch.jar
  candidate=$target_dir/flink-statebackend-cachekit-1.16-SNAPSHOT-p10-$arch.jar
  [[ -f $base ]] || { echo "missing frozen P9 base: $base" >&2; exit 67; }
  base_sha=$(sha256sum "$base" | awk '{print $1}')
  [[ $base_sha == "$expected_p9_sha" ]] || {
    echo "unexpected $arch P9 base: $base_sha" >&2
    exit 67
  }

  cp "$base" "$candidate"
  (cd "$overlay" && jar uf "$candidate" org)
  unzip -t "$candidate" >/dev/null

  unzip -Z1 "$base" | LC_ALL=C sort -u >"$overlay/$arch.base.entries"
  unzip -Z1 "$candidate" | LC_ALL=C sort -u >"$overlay/$arch.candidate.entries"
  cmp -s "$overlay/$arch.base.entries" "$overlay/$arch.candidate.entries" || {
    echo "$arch candidate entry set differs from P9 base" >&2
    exit 69
  }

  while IFS= read -r entry; do
    if grep -Fxq "$entry" "$overlay/overlay.entries"; then
      cmp -s "$overlay/$entry" <(unzip -p "$candidate" "$entry") || {
        echo "overlay mismatch: $entry" >&2
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
    echo "p9_runtime_sha256=$base_sha"
    echo "candidate_sha256=$candidate_sha"
    echo "candidate_path=$candidate"
    echo "overlay_class_count=$(wc -l <"$overlay/overlay.entries")"
    echo "entry_set_identical_to_p9=true"
    echo "non_overlay_entries_identical=true"
  } >"$candidate.BUILD.txt"
  echo "$arch candidate=$candidate"
  echo "$arch sha256=$candidate_sha"
}

build_one x86 "$expected_x86_p9_sha"
build_one aarch64 "$expected_arm_p9_sha"
