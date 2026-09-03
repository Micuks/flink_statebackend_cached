#!/usr/bin/env python3
"""Audit ready-gate activation from the per-leg Prometheus snapshot."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path


SUM_METRICS = (
    "batchesStarted",
    "readyBeforeDispatch",
    "prefetchWaitNanos",
    "dispatchBeforeReadyFallbacks",
    "timeoutFallbacks",
    "failureFallbacks",
    "notProvenFallbacks",
    "forcedFallbacks",
    "readyRecordsDispatched",
    "fallbackRecordsDispatched",
    "ringFullNanos",
    "workerQueueNanos",
    "workerServiceNanos",
    "valuesStaged",
    "stagedValuesConsumed",
    "stagedValuesDiscarded",
    "authoritativeReadsAvoided",
    "workerDiscardedAfterRead",
    "stagingAdmissionDrops",
)
MAX_METRICS = (
    "maxInFlightDepth",
    "maxRetainedRecords",
    "maxRetainedReferenceBytes",
)
REQUIRED = (
    "active",
    "batchesStarted",
    "readyBeforeDispatch",
    "dispatchBeforeReadyFallbacks",
    "maxInFlightDepth",
    "valuesStaged",
    "stagedValuesConsumed",
)


def ratio(numerator: float, denominator: float):
    return None if denominator == 0 else numerator / denominator


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("leg_dir", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    source = args.leg_dir / "ready-gate-prometheus.json"
    payload = json.loads(source.read_text())
    assert payload.get("status") == "success", payload
    series = payload.get("data", {}).get("result", [])
    observed = defaultdict(list)
    raw = []
    for item in series:
        metric = item.get("metric", {})
        name = metric.get("__name__", "")
        try:
            value = float(item["value"][1])
        except (KeyError, IndexError, TypeError, ValueError):
            continue
        raw.append({"name": name, "labels": metric, "value": value})
        for short in set(SUM_METRICS + MAX_METRICS + REQUIRED):
            if name.endswith("_readyGate_" + short):
                observed[short].append(value)
                break

    missing = sorted(name for name in REQUIRED if not observed[name])
    sums = {name: sum(observed[name]) for name in SUM_METRICS}
    maxima = {name: max(observed[name], default=0) for name in MAX_METRICS}
    active_series = observed["active"]
    result = {
        "schema": "cachekit-ready-gated-prefetch-activation-v1",
        "leg_dir": str(args.leg_dir),
        "valid": not missing,
        "missing_metrics": missing,
        "series_count": len(raw),
        "active_series": len(active_series),
        "active_series_positive": sum(value > 0 for value in active_series),
        "sum": sums,
        "max": maxima,
        "rates": {
            "ready_batch_rate": ratio(
                sums["readyBeforeDispatch"], sums["batchesStarted"]
            ),
            "ready_record_rate": ratio(
                sums["readyRecordsDispatched"],
                sums["readyRecordsDispatched"] + sums["fallbackRecordsDispatched"],
            ),
            "staged_consumption_rate": ratio(
                sums["stagedValuesConsumed"], sums["valuesStaged"]
            ),
        },
        "activation_pass": (
            not missing
            and sum(value > 0 for value in active_series) > 0
            and sums["batchesStarted"] > 0
            and sums["readyBeforeDispatch"] > 0
            and maxima["maxInFlightDepth"] >= 2
            and sums["valuesStaged"] > 0
            and sums["stagedValuesConsumed"] > 0
            and sums["failureFallbacks"] == 0
            and sums["stagingAdmissionDrops"] == 0
        ),
        "raw_series": raw,
    }
    output = args.output or args.leg_dir / "READY_GATE_AUDIT.json"
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(output)


if __name__ == "__main__":
    main()
