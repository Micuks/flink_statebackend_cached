#!/usr/bin/env python3
"""Audit activation of synchronous record-key MultiGet from a Prometheus snapshot."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path


METRICS = (
    "active",
    "batchesAttempted",
    "batchesHandled",
    "recordsAttempted",
    "recordsHandled",
    "backendMultiGetBatches",
    "backendMultiGetKeys",
    "backendValuesStaged",
    "backendSmallBatchSkips",
    "backendFailures",
    "stagedValuesConsumed",
)
REQUIRED = METRICS


def ratio(numerator: float, denominator: float):
    return None if denominator == 0 else numerator / denominator


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("leg_dir", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    source = args.leg_dir / "immediate-record-prometheus.json"
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
        for short in METRICS:
            if name.endswith("_immediateRecord_" + short):
                observed[short].append(value)
                break

    missing = sorted(name for name in REQUIRED if not observed[name])
    sums = {name: sum(observed[name]) for name in METRICS if name != "active"}
    active_series = observed["active"]
    activation_pass = (
        not missing
        and sum(value > 0 for value in active_series) > 0
        and sums["batchesAttempted"] > 0
        and sums["batchesHandled"] > 0
        and sums["recordsAttempted"] > 0
        and sums["recordsHandled"] > 0
        and sums["backendMultiGetBatches"] > 0
        and sums["backendMultiGetKeys"] > 0
        and sums["backendValuesStaged"] > 0
        and sums["stagedValuesConsumed"] > 0
        and sums["backendFailures"] == 0
    )
    result = {
        "schema": "cachekit-inline-record-multiget-activation-v1",
        "leg_dir": str(args.leg_dir),
        "valid": not missing,
        "missing_metrics": missing,
        "series_count": len(raw),
        "active_series": len(active_series),
        "active_series_positive": sum(value > 0 for value in active_series),
        "sum": sums,
        "rates": {
            "batch_handle_rate": ratio(
                sums["batchesHandled"], sums["batchesAttempted"]
            ),
            "record_handle_rate": ratio(
                sums["recordsHandled"], sums["recordsAttempted"]
            ),
            "backend_stage_rate": ratio(
                sums["backendValuesStaged"], sums["backendMultiGetKeys"]
            ),
            "staged_consumption_rate": ratio(
                sums["stagedValuesConsumed"], sums["backendValuesStaged"]
            ),
        },
        "activation_pass": activation_pass,
        "raw_series": raw,
    }
    output = args.output or args.leg_dir / "IMMEDIATE_RECORD_AUDIT.json"
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(output)


if __name__ == "__main__":
    main()
