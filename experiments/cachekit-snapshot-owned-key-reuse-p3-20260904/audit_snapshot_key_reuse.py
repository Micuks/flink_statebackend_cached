#!/usr/bin/env python3
"""Audit snapshot-owned key-reuse activation from a Prometheus snapshot."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path


METRICS = (
    "map_snapshot_owned_key_reuse_active_states",
    "map_snapshot_owned_internal_keys_reused",
    "map_snapshot_internal_key_copies_avoided",
    "map_snapshot_exposed_key_copies_deferred",
    "map_snapshot_exposed_key_copies_materialized",
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("leg_dir", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    source = args.leg_dir / "snapshot-key-reuse-prometheus.json"
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
            if name == short or name.endswith("_" + short):
                observed[short].append(value)
                break

    missing = sorted(name for name in METRICS if not observed[name])
    sums = {name: sum(observed[name]) for name in METRICS}
    activation_pass = (
        not missing
        and sums["map_snapshot_owned_key_reuse_active_states"] > 0
        and sums["map_snapshot_owned_internal_keys_reused"] > 0
        and sums["map_snapshot_internal_key_copies_avoided"] > 0
        and sums["map_snapshot_exposed_key_copies_deferred"] > 0
        and sums["map_snapshot_exposed_key_copies_materialized"] >= 0
    )
    result = {
        "schema": "cachekit-snapshot-owned-key-reuse-activation-v1",
        "leg_dir": str(args.leg_dir),
        "valid": not missing,
        "missing_metrics": missing,
        "series_count": len(raw),
        "series_per_metric": {name: len(observed[name]) for name in METRICS},
        "sum": sums,
        "activation_pass": activation_pass,
        "raw_series": raw,
    }
    output = args.output or args.leg_dir / "SNAPSHOT_KEY_REUSE_AUDIT.json"
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(output)


if __name__ == "__main__":
    main()
