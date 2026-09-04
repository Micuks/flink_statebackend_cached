#!/usr/bin/env python3
"""Repair the disappearing-foreign-container race in an already staged P7 runner."""

from __future__ import annotations

import datetime as dt
import hashlib
import pathlib
import sys


RUNNER_FUNCTION = r'''fail_on_foreign_containers() {
  local id inspect owner_name owner_project owner_cpus remainder foreign=0
  while read -r id; do
    [[ -n $id ]] || continue
    inspect=$(docker inspect -f '{{.Name}}|{{index .Config.Labels "com.docker.compose.project"}}|{{.HostConfig.CpusetCpus}}' "$id" 2>/dev/null) || continue
    owner_name=${inspect%%|*}
    remainder=${inspect#*|}
    owner_project=${remainder%%|*}
    owner_cpus=${remainder#*|}
    if [[ $owner_project != "$project" ]]; then
      if [[ $allow_disjoint_foreign == true && -n $owner_cpus ]] &&
          ! python3 - "$owner_cpus" "$target_cpuset" <<'PY'
import sys


def expand(spec):
    result = set()
    for part in spec.split(','):
        bounds = [int(value) for value in part.split('-', 1)]
        result.update(range(bounds[0], bounds[-1] + 1))
    return result


raise SystemExit(0 if expand(sys.argv[1]) & expand(sys.argv[2]) else 1)
PY
      then
        echo "allowed_disjoint_container=$owner_name cpus=$owner_cpus" >&2
      else
        echo "foreign_container=$owner_name project=$owner_project cpus=$owner_cpus" >&2
        foreign=1
      fi
    fi
  done < <(docker ps -q)
  (( foreign == 0 )) || return 1
}'''


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} EXPERIMENT_DIR")
    experiment = pathlib.Path(sys.argv[1])
    runner = experiment / "run_campaign.sh"
    text = runner.read_text()
    start = text.index("fail_on_foreign_containers() {")
    end = text.index("\n}\n\nwait_cluster()", start) + 2
    old = text[start:end]
    if old == RUNNER_FUNCTION:
        print("foreign-container race repair already applied")
        return
    if "docker inspect -f '{{index .Config.Labels" not in old:
        raise SystemExit("unexpected pre-repair foreign-container function")
    updated = text[:start] + RUNNER_FUNCTION + text[end:]
    runner.write_text(updated)
    note = experiment / "HARNESS_FOREIGN_CONTAINER_RACE_REPAIR.txt"
    note.write_text(
        "\n".join(
            [
                "schema=cachekit-p7-harness-foreign-container-race-repair-v1",
                f"repaired_at={dt.datetime.now().astimezone().isoformat()}",
                "scope=read-only foreign-container occupancy preflight",
                "symptom=docker ps returned a foreign container that disappeared before docker inspect",
                "failure=the vanished ID was conservatively misclassified as an overlapping container",
                "repair=inspect each ID once and skip IDs that have already disappeared",
                "treatment_config_changed=false",
                "completed_q5_preserved=true",
                f"old_function_sha256={sha256(old)}",
                f"new_function_sha256={sha256(RUNNER_FUNCTION)}",
                "",
            ]
        )
    )
    print(note)


if __name__ == "__main__":
    main()
