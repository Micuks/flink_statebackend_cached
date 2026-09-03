#!/usr/bin/env python3
"""Summarize the P3 q9 paired screen and its activation/performance gates."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import statistics
from collections import defaultdict
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("expdir", type=Path)
    parser.add_argument("--control", default="control")
    parser.add_argument("--output-prefix", default="P3_RESULT")
    args = parser.parse_args()

    rows = []
    for result_path in sorted((args.expdir / "results" / "raw").glob("*/result.json")):
        leg = result_path.parent
        if not (leg / "LEG_COMPLETE").is_file():
            continue
        result = json.loads(result_path.read_text())
        measurement = result["measurement"]
        activation_path = leg / "SNAPSHOT_KEY_REUSE_AUDIT.json"
        activation = (
            json.loads(activation_path.read_text()) if activation_path.is_file() else None
        )
        rows.append(
            {
                "leg": leg.name,
                "query": result["query"],
                "round": result["round"],
                "variant": result["variant"],
                "valid": result["valid"] is True,
                "raw_kps": measurement["raw_throughput_kps"],
                "cores": measurement["cores"],
                "kps_core": measurement["throughput_kps_core"],
                "config_sha256": result["config_sha256"],
                "artifact_sha256": result["artifact_sha256"],
                "activation": activation,
            }
        )

    by_key = {(row["query"], row["round"], row["variant"]): row for row in rows}
    uplifts = []
    by_variant = defaultdict(list)
    for row in rows:
        if row["variant"] == args.control:
            continue
        control = by_key.get((row["query"], row["round"], args.control))
        if control is None or not row["valid"] or not control["valid"]:
            continue
        uplift = 100.0 * (row["kps_core"] / control["kps_core"] - 1.0)
        item = {
            "query": row["query"],
            "round": row["round"],
            "variant": row["variant"],
            "candidate_kps_core": row["kps_core"],
            "control_kps_core": control["kps_core"],
            "uplift_pct": uplift,
        }
        uplifts.append(item)
        by_variant[row["variant"]].append(uplift)

    ranking = sorted(
        (
            {
                "variant": variant,
                "paired_count": len(values),
                "arithmetic_mean_uplift_pct": statistics.fmean(values),
                "median_uplift_pct": statistics.median(values),
                "minimum_uplift_pct": min(values),
                "maximum_uplift_pct": max(values),
            }
            for variant, values in by_variant.items()
        ),
        key=lambda item: item["arithmetic_mean_uplift_pct"],
        reverse=True,
    )
    candidate_rows = [row for row in rows if row["variant"] != args.control]
    activation_pass = bool(candidate_rows) and all(
        row["activation"] and row["activation"]["activation_pass"]
        for row in candidate_rows
    )
    mean_uplift = ranking[0]["arithmetic_mean_uplift_pct"] if ranking else None
    now = dt.datetime.now().astimezone()
    stamp = now.strftime("%Y%m%d_%H%M%S")
    result = {
        "schema": "cachekit-snapshot-owned-key-reuse-p3-summary-v1",
        "generated_at": now.isoformat(),
        "control": args.control,
        "valid_leg_count": sum(row["valid"] for row in rows),
        "rows": rows,
        "paired_uplifts": uplifts,
        "ranking": ranking,
        "headline_definition": "arithmetic mean of paired per-query per-round K/s/core uplift percentages",
        "q9_screen_gate": {
            "required_uplift_pct": 3.0,
            "activation_pass": activation_pass,
            "performance_pass": mean_uplift is not None and mean_uplift >= 3.0,
        },
        "effective_query_goal_gate": {
            "required_mean_uplift_pct": 10.0,
            "activation_pass": activation_pass,
            "performance_pass": mean_uplift is not None and mean_uplift >= 10.0,
        },
    }
    for gate in (result["q9_screen_gate"], result["effective_query_goal_gate"]):
        gate["pass"] = gate["activation_pass"] and gate["performance_pass"]

    lines = [
        "# CacheKit snapshot-owned key reuse P3 q9 result",
        "",
        f"Generated: `{result['generated_at']}`",
        "",
        "Headline: arithmetic mean of paired per-query, per-round K/s/core uplift percentages.",
        "",
        "| Query | Round | Variant | Raw K/s/core | Raw K/s | Cores | Valid | Activation |",
        "| --- | ---: | --- | ---: | ---: | ---: | --- | --- |",
    ]
    for row in rows:
        is_control = row["variant"] == args.control
        lines.append(
            f"| {row['query']} | {row['round']} | {row['variant']} | "
            f"{row['kps_core']:.2f} | {row['raw_kps']:.2f} | {row['cores']:.2f} | "
            f"{'yes' if row['valid'] else 'no'} | "
            f"{'control' if is_control else ('yes' if row['activation'] and row['activation']['activation_pass'] else 'no')} |"
        )
    lines.extend(["", "| Variant | Pairs | Mean uplift | Median | Minimum | Maximum |", "| --- | ---: | ---: | ---: | ---: | ---: |"])
    for item in ranking:
        lines.append(
            f"| {item['variant']} | {item['paired_count']} | "
            f"{item['arithmetic_mean_uplift_pct']:+.2f}% | "
            f"{item['median_uplift_pct']:+.2f}% | {item['minimum_uplift_pct']:+.2f}% | "
            f"{item['maximum_uplift_pct']:+.2f}% |"
        )
    lines.extend(
        [
            "",
            f"- Activation: {'PASS' if activation_pass else 'FAIL'}",
            f"- q9 screen >= +3.00%: {'PASS' if result['q9_screen_gate']['pass'] else 'FAIL'}",
            f"- effective-query goal >= +10.00%: {'PASS' if result['effective_query_goal_gate']['pass'] else 'FAIL'}",
        ]
    )

    final = args.expdir / "final"
    final.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(result, indent=2, sort_keys=True) + "\n"
    markdown = "\n".join(lines) + "\n"
    for path in (
        final / f"{args.output_prefix}_{stamp}.json",
        final / f"{args.output_prefix}.json",
    ):
        path.write_text(payload)
    for path in (
        final / f"{args.output_prefix}_{stamp}.md",
        final / f"{args.output_prefix}.md",
    ):
        path.write_text(markdown)
    print(final / f"{args.output_prefix}.md")


if __name__ == "__main__":
    main()
