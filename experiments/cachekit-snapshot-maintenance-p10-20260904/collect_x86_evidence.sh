#!/usr/bin/env bash
set -euo pipefail

host=root@114.116.229.206
p9_remote=/home/wuql/flink-cluster/experiments/cachekit-p9-dirty-overlay-q9-100m-x86-20260904
p10_remote=/home/wuql/flink-cluster/experiments/cachekit-p10-snapshot-maintenance-q9-100m-x86-20260904
effective5_remote=/home/wuql/flink-cluster/experiments/cachekit-p10-effective5-100m-x86-20260904

through=effective5
if [[ $# -gt 0 ]]; then
  [[ $# -eq 2 && $1 == --through ]] || {
    echo "usage: $0 [--through p9|p10|effective5]" >&2
    exit 64
  }
  through=$2
fi
case "$through" in
  p9 | p10 | effective5) ;;
  *)
    echo "invalid collection phase: $through" >&2
    exit 64
    ;;
esac

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
evidence_root=$script_dir/remote-evidence
mkdir -p "$evidence_root"

verify_archive() {
  local archive=$1 expected_legs=$2 required_summary=$3
  local count checksum

  [[ -f $archive/CAMPAIGN_COMPLETE ]]
  [[ -f $archive/identity.json ]]
  [[ -f $archive/CONFIG_DIFF_AUDIT.json ]]
  [[ -f $archive/inputs/ARTIFACTS.SHA256SUMS ]]
  [[ -f $archive/$required_summary ]]
  python3 -m json.tool "$archive/identity.json" >/dev/null
  python3 -m json.tool "$archive/CONFIG_DIFF_AUDIT.json" >/dev/null
  python3 -m json.tool "$archive/$required_summary" >/dev/null
  (
    cd "$archive"
    sha256sum -c inputs/ARTIFACTS.SHA256SUMS
  )

  count=$(find "$archive/results/raw" -mindepth 2 -maxdepth 2 -name LEG_COMPLETE | wc -l)
  [[ $count -eq $expected_legs ]] || {
    echo "unexpected completed-leg count in $archive: $count != $expected_legs" >&2
    return 1
  }
  while IFS= read -r -d '' checksum; do
    (
      cd "$(dirname "$checksum")"
      sha256sum -c LEG.SHA256SUMS
      [[ -f LEG_COMPLETE ]]
    )
  done < <(find "$archive/results/raw" -mindepth 2 -maxdepth 2 -name LEG.SHA256SUMS -print0)
  [[ $(find "$archive/results/raw" -mindepth 2 -maxdepth 2 -name LEG.SHA256SUMS | wc -l) -eq $expected_legs ]]
}

collect_one() {
  local label=$1 remote=$2 expected_legs=$3 required_summary=$4
  local destination=$evidence_root/$label
  local partial=$evidence_root/.$label.partial
  local owner=$partial/.cachekit-evidence-source

  ssh -o BatchMode=yes "$host" test -f "$remote/CAMPAIGN_COMPLETE"
  ssh -o BatchMode=yes "$host" test -f "$remote/$required_summary"

  if [[ -d $destination ]]; then
    verify_archive "$destination" "$expected_legs" "$required_summary"
  else
    mkdir -p "$partial"
    if [[ -f $owner ]]; then
      [[ $(<"$owner") == "$host:$remote" ]] || {
        echo "partial archive has a different owner: $partial" >&2
        return 1
      }
    else
      [[ -z $(find "$partial" -mindepth 1 -maxdepth 1 -print -quit) ]] || {
        echo "unowned partial archive is not empty: $partial" >&2
        return 1
      }
      printf '%s\n' "$host:$remote" >"$owner"
    fi
    rsync -a --partial "$host:$remote/" "$partial/"
    verify_archive "$partial" "$expected_legs" "$required_summary"
    mv "$partial" "$destination"
  fi

  (
    cd "$destination"
    find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum
  ) >"$evidence_root/$label.SHA256SUMS"
}

ssh -o BatchMode=yes "$host" bash -s -- "$p9_remote" <<'REMOTE'
set -euo pipefail
experiment=$1
[[ -f $experiment/CAMPAIGN_COMPLETE ]]
if [[ ! -f $experiment/final/P9_SCREEN_SUMMARY.json ]]; then
  python3 "$experiment/summarize_x86.py" "$experiment" \
    >"$experiment/logs/summarize-x86.stdout"
fi
REMOTE

collect_one x86-p9 "$p9_remote" 3 final/P9_SCREEN_SUMMARY.json
if [[ $through == p9 ]]; then
  printf 'evidence_collected_through=p9 at=%s\n' "$(date -Is)"
  exit 0
fi
collect_one x86-p10-screen "$p10_remote" 6 final/P10_SCREEN_SUMMARY.json
if [[ $through == p10 ]]; then
  printf 'evidence_collected_through=p10 at=%s\n' "$(date -Is)"
  exit 0
fi
collect_one x86-effective5 "$effective5_remote" 12 final/P10_EFFECTIVE5_SUMMARY.json

printf 'evidence_collected_through=effective5 at=%s\n' "$(date -Is)"
