#!/usr/bin/env python3
"""Audit ValueState prefetch activation and waste counters from captured leg logs."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


PREFIX = "[CACHEKIT VALUE PREFETCH]"
PAIR = re.compile(r"([A-Za-z][A-Za-z0-9]*)=([^ ]+)")
COUNTERS = (
    "tasksBuilt",
    "tasksExecuted",
    "tasksDropped",
    "keysPrepared",
    "keysDeduplicated",
    "multiGetCalls",
    "multiGetKeys",
    "pointGetCalls",
    "immediatePointGetCalls",
    "smallBatchDrops",
    "smallBatchKeysDropped",
    "staged",
    "missingStaged",
    "promoted",
    "admissionDrops",
    "staleAborts",
    "liveReadRacedInFlight",
    "liveReadCancellations",
    "dispatchKeysExamined",
    "dispatchCancellations",
    "dispatchAlreadyStaged",
    "dispatchNoReservation",
    "workerCancelledBeforeRead",
    "workerDiscardedAfterRead",
    "asyncValuesRead",
    "asyncUsefulValues",
    "unusedStagedOnClose",
    "buildFailures",
    "workerFailures",
)


def ratio(numerator: int, denominator: int):
    return None if denominator == 0 else numerator / denominator


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("leg_dir", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    log_dir = args.leg_dir / "container-logs"
    files = sorted(log_dir.glob("*.log"))
    if not files:
        raise SystemExit(f"no container logs under {log_dir}")

    summaries = []
    for path in files:
        for line_number, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
            if PREFIX not in line:
                continue
            values = dict(PAIR.findall(line.split(PREFIX, 1)[1]))
            summaries.append(
                {
                    "file": path.name,
                    "line": line_number,
                    "delegate": values.get("delegate"),
                    "access_observed": values.get("accessObserved") == "true",
                    "counters": {name: int(values.get(name, "0")) for name in COUNTERS},
                    "worker_queue_avg_us": int(values.get("workerQueueAvgUs", "0")),
                    "worker_queue_max_us": int(values.get("workerQueueMaxUs", "0")),
                    "worker_run_avg_us": int(values.get("workerRunAvgUs", "0")),
                    "worker_run_max_us": int(values.get("workerRunMaxUs", "0")),
                    "live_read_race_avg_us": int(values.get("liveReadRaceAvgUs", "0")),
                    "live_read_race_max_us": int(values.get("liveReadRaceMaxUs", "0")),
                }
            )

    if not summaries:
        raise SystemExit(f"no {PREFIX} summaries under {log_dir}")

    aggregate = {name: sum(item["counters"][name] for item in summaries) for name in COUNTERS}
    task_weight = max(1, aggregate["tasksExecuted"])
    race_weight = max(1, aggregate["liveReadRacedInFlight"])
    weighted_queue = sum(
        item["worker_queue_avg_us"] * item["counters"]["tasksExecuted"]
        for item in summaries
    ) / task_weight
    weighted_run = sum(
        item["worker_run_avg_us"] * item["counters"]["tasksExecuted"]
        for item in summaries
    ) / task_weight
    weighted_race = sum(
        item["live_read_race_avg_us"] * item["counters"]["liveReadRacedInFlight"]
        for item in summaries
    ) / race_weight

    result = {
        "schema": "cachekit-value-prefetch-activation-audit-v1",
        "valid": aggregate["buildFailures"] == 0 and aggregate["workerFailures"] == 0,
        "leg_dir": str(args.leg_dir),
        "summary_lines": len(summaries),
        "states_with_access": sum(item["access_observed"] for item in summaries),
        "async_lookahead_active": aggregate["tasksBuilt"] > 0 and aggregate["tasksExecuted"] > 0,
        "aggregate": aggregate,
        "rates": {
            "task_execution_rate": ratio(aggregate["tasksExecuted"], aggregate["tasksBuilt"]),
            "promotion_rate": ratio(aggregate["promoted"], aggregate["staged"]),
            "async_useful_rate": ratio(aggregate["asyncUsefulValues"], aggregate["asyncValuesRead"]),
            "live_race_per_prepared_key": ratio(
                aggregate["liveReadRacedInFlight"], aggregate["keysPrepared"]
            ),
            "worker_cancel_or_discard_per_prepared_key": ratio(
                aggregate["workerCancelledBeforeRead"] + aggregate["workerDiscardedAfterRead"],
                aggregate["keysPrepared"],
            ),
        },
        "timing_us": {
            "worker_queue_weighted_avg": weighted_queue,
            "worker_queue_max": max(item["worker_queue_max_us"] for item in summaries),
            "worker_run_weighted_avg": weighted_run,
            "worker_run_max": max(item["worker_run_max_us"] for item in summaries),
            "live_read_race_weighted_avg": weighted_race,
            "live_read_race_max": max(item["live_read_race_max_us"] for item in summaries),
        },
        "summaries": summaries,
    }
    output = args.output or args.leg_dir / "VALUE_PREFETCH_AUDIT.json"
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(output)


if __name__ == "__main__":
    main()
