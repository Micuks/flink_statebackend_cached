#!/usr/bin/env python3
"""Fail when recursive TaskManager process trees overlap.

The check is host-side and read-only: it uses ``docker compose ps`` and
``docker top`` and never execs into a benchmark container.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


TM_MARKER = "TaskManagerRunner"


@dataclass(frozen=True)
class Process:
    pid: int
    ppid: int
    args: str


def run(command: list[str]) -> str:
    completed = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"command failed ({completed.returncode}): {' '.join(command)}\n{detail}")
    return completed.stdout


def parse_top(text: str) -> list[Process]:
    processes: list[Process] = []
    for line in text.splitlines()[1:]:
        fields = line.strip().split(maxsplit=2)
        if len(fields) != 3 or not fields[0].isdigit() or not fields[1].isdigit():
            continue
        processes.append(Process(int(fields[0]), int(fields[1]), fields[2]))
    return processes


def ancestor_overlaps(processes: Iterable[Process]) -> list[tuple[int, int]]:
    rows = list(processes)
    parents = {row.pid: row.ppid for row in rows}
    roots = {row.pid for row in rows if TM_MARKER in row.args}
    overlaps: set[tuple[int, int]] = set()

    for descendant in roots:
        seen: set[int] = set()
        current = parents.get(descendant)
        while current is not None and current not in seen:
            if current in roots:
                overlaps.add((current, descendant))
            seen.add(current)
            current = parents.get(current)
    return sorted(overlaps)


def compose_prefix(compose: Path, project_name: str | None) -> list[str]:
    command = ["docker", "compose", "-f", str(compose)]
    if project_name:
        command.extend(["-p", project_name])
    return command


def taskmanager_services(compose: Path, project_name: str | None) -> list[str]:
    services = [
        line.strip()
        for line in run(
            compose_prefix(compose, project_name) + ["config", "--services"]
        ).splitlines()
        if line.strip()
    ]
    return [service for service in services if "taskmanager" in service.lower()]


def container_ids(compose: Path, project_name: str | None, service: str) -> list[str]:
    return [
        line.strip()
        for line in run(
            compose_prefix(compose, project_name) + ["ps", "-q", service]
        ).splitlines()
        if line.strip()
    ]


def self_test() -> None:
    overlapping = """PID PPID ARGS
100 10 java TaskManagerRunner
200 100 java TaskManagerRunner
300 100 java TaskManagerRunner
400 100 java TaskManagerRunner
"""
    disjoint = """PID PPID ARGS
100 10 java TaskManagerRunner
200 10 java TaskManagerRunner
300 10 java TaskManagerRunner
400 10 java TaskManagerRunner
"""
    assert ancestor_overlaps(parse_top(overlapping)) == [
        (100, 200),
        (100, 300),
        (100, 400),
    ]
    assert ancestor_overlaps(parse_top(disjoint)) == []
    print("PASS: overlap detector self-test")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--compose",
        type=Path,
        default=Path(os.environ.get("BENCH_COMPOSE_PATH", "docker-compose.yml")),
        help="Docker Compose file for the running Nexmark stack",
    )
    parser.add_argument(
        "--expected-tms",
        type=int,
        default=int(os.environ.get("BENCH_EXPECTED_TMS", "8")),
        help="Expected total TaskManagerRunner JVM count",
    )
    parser.add_argument(
        "--project-name",
        default=os.environ.get("COMPOSE_PROJECT_NAME"),
        help="Explicit Docker Compose project name, when the stack uses one",
    )
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return 0
    if not args.compose.is_file():
        parser.error(f"compose file does not exist: {args.compose}")

    services = taskmanager_services(args.compose, args.project_name)
    if not services:
        raise RuntimeError("compose config contains no taskmanager services")

    total_tms = 0
    failures: list[str] = []
    checked: list[str] = []
    for service in services:
        ids = container_ids(args.compose, args.project_name, service)
        if not ids:
            failures.append(f"{service}: no running container")
            continue
        for container_id in ids:
            top = run(["docker", "top", container_id, "-eo", "pid,ppid,args"])
            processes = parse_top(top)
            tm_pids = sorted(row.pid for row in processes if TM_MARKER in row.args)
            total_tms += len(tm_pids)
            overlaps = ancestor_overlaps(processes)
            checked.append(f"{service}/{container_id[:12]}: TM PIDs {tm_pids}")
            for ancestor, descendant in overlaps:
                failures.append(
                    f"{service}/{container_id[:12]}: TM PID {ancestor} is an ancestor "
                    f"of TM PID {descendant}"
                )

    if total_tms != args.expected_tms:
        failures.append(f"expected {args.expected_tms} TM JVMs, observed {total_tms}")

    for line in checked:
        print(line)
    if failures:
        print("FAIL: overlapping or incomplete TaskManager CPU metric coverage", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        print(
            "Recursive ProcfsBasedProcessTree metrics are unsafe; do not use cores or "
            "throughput/core from this topology.",
            file=sys.stderr,
        )
        return 2

    print(f"PASS: {total_tms} TM JVMs have disjoint process trees")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(3)
