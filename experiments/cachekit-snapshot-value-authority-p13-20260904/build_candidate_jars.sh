#!/usr/bin/env bash
set -euo pipefail

expected_source=920c7748eaa9d8fcc75164521d6aabf0f9e5077c
expected_x86_p11_sha=9714d3582ba72bec45147e7a49d008832405508d231f086b1158750c05cc8308
expected_arm_p11_sha=de935deec5eb709c8afbae79e1161a9ecaadc30c5e7cc89602b04404ca2c3e29

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$script_dir" rev-parse --show-toplevel)
git -C "$repo" merge-base --is-ancestor "$expected_source" HEAD || {
  echo "expected runtime source is not an ancestor" >&2
  exit 65
}
git -C "$repo" diff --quiet "$expected_source" HEAD -- \
  flink-state-backends/flink-statebackend-cachekit || {
  echo "runtime source differs from $expected_source" >&2
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
overlay=$(mktemp -d /tmp/cachekit-p13-overlay.XXXXXX)
cleanup() {
  [[ $overlay == /tmp/cachekit-p13-overlay.* && -d $overlay ]] && rm -rf -- "$overlay"
}
trap cleanup EXIT

stem=org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalMapState
found=0
while IFS= read -r -d '' class_file; do
  found=1
  relative=${class_file#"$classes"/}
  mkdir -p "$overlay/$(dirname "$relative")"
  cp "$class_file" "$overlay/$relative"
done < <(find "$classes/$(dirname "$stem")" -maxdepth 1 \
  -type f -name "$(basename "$stem")*.class" -print0)
(( found == 1 )) || { echo "missing class stem: $stem" >&2; exit 67; }
find "$overlay/org" -type f -name '*.class' -printf '%P\n' | \
  sed 's#^#org/#' | LC_ALL=C sort -u >"$overlay/overlay.entries"

build_one() {
  local arch=$1 expected_base_sha=$2 base candidate base_sha candidate_sha entry
  base=$target_dir/flink-statebackend-cachekit-1.16-SNAPSHOT-p11-$arch.jar
  candidate=$target_dir/flink-statebackend-cachekit-1.16-SNAPSHOT-p13-$arch.jar
  [[ -f $base ]] || { echo "missing P11 base: $base" >&2; exit 68; }
  base_sha=$(sha256sum "$base" | awk '{print $1}')
  [[ $base_sha == "$expected_base_sha" ]] || {
    echo "unexpected $arch P11 base: $base_sha" >&2
    exit 69
  }
  cp "$base" "$candidate"
  (cd "$overlay" && jar uf "$candidate" org)
  unzip -t "$candidate" >/dev/null

  unzip -Z1 "$base" | LC_ALL=C sort -u >"$overlay/$arch.base.entries"
  unzip -Z1 "$candidate" | LC_ALL=C sort -u >"$overlay/$arch.candidate.entries"
  cmp -s "$overlay/$arch.base.entries" "$overlay/$arch.candidate.entries" || {
    echo "$arch candidate entry set differs from P11 base" >&2
    exit 70
  }
  while IFS= read -r entry; do
    if grep -Fxq "$entry" "$overlay/overlay.entries"; then
      cmp -s "$overlay/$entry" <(unzip -p "$candidate" "$entry") || {
        echo "overlay mismatch: $entry" >&2
        exit 71
      }
    else
      cmp -s <(unzip -p "$base" "$entry") <(unzip -p "$candidate" "$entry") || {
        echo "non-overlay entry changed: $entry" >&2
        exit 72
      }
    fi
  done <"$overlay/$arch.base.entries"
  javap -private -classpath "$candidate" \
    org.apache.flink.contrib.streaming.state.cachekit.state.CachedInternalMapState | \
    grep -q 'snapshotValueAuthorityEnabled'
  candidate_sha=$(sha256sum "$candidate" | awk '{print $1}')
  printf '%s %s %s %s\n' "$arch" "$base_sha" "$candidate_sha" "$candidate"
}

mkdir -p "$script_dir/build"
{
  echo "schema=cachekit-p13-artifact-overlay-audit-v1"
  echo "source_commit=$expected_source"
  echo "focused_tests=42"
  echo "overlay_stem=$stem"
  build_one x86 "$expected_x86_p11_sha"
  build_one aarch64 "$expected_arm_p11_sha"
} | tee "$script_dir/build/ARTIFACT_OVERLAY_AUDIT.txt"
