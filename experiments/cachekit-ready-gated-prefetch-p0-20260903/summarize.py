#!/usr/bin/env python3
"""Summarize valid CacheKit P0 legs with paired arithmetic per-query uplift."""

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
    parser.add_argument("--output-prefix", default="P0_RESULT")
    args = parser.parse_args()

    rows = []
    for result_path in sorted((args.expdir / "results" / "raw").glob("*/result.json")):
        leg = result_path.parent
        if not (leg / "LEG_COMPLETE").is_file():
            continue
        result = json.loads(result_path.read_text())
        measurement = result["measurement"]
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

    now = dt.datetime.now().astimezone()
    stamp = now.strftime("%Y%m%d_%H%M%S")
    result = {
        "schema": "cachekit-ready-gated-prefetch-p0-summary-v1",
        "generated_at": now.isoformat(),
        "control": args.control,
        "valid_leg_count": sum(row["valid"] for row in rows),
        "rows": rows,
        "paired_uplifts": uplifts,
        "ranking": ranking,
        "headline_definition": "arithmetic mean of paired per-query per-round K/s/core uplift percentages",
    }

    lines = [
        "# CacheKit ready-gated prefetch P0 result",
        "",
        f"Generated: `{result['generated_at']}`",
        "",
        "Headline: arithmetic mean of paired per-query, per-round K/s/core uplift percentages.",
        "",
        "## Valid legs",
        "",
        "| Query | Round | Variant | Raw K/s/core | Raw K/s | Cores | Valid |",
        "| --- | ---: | --- | ---: | ---: | ---: | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row['query']} | {row['round']} | {row['variant']} | "
            f"{row['kps_core']:.2f} | {row['raw_kps']:.2f} | {row['cores']:.2f} | "
            f"{'yes' if row['valid'] else 'no'} |"
        )
    lines.extend(
        [
            "",
            "## Paired ranking",
            "",
            "| Variant | Pairs | Mean uplift | Median | Minimum | Maximum |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for item in ranking:
        lines.append(
            f"| {item['variant']} | {item['paired_count']} | "
            f"{item['arithmetic_mean_uplift_pct']:+.2f}% | "
            f"{item['median_uplift_pct']:+.2f}% | {item['minimum_uplift_pct']:+.2f}% | "
            f"{item['maximum_uplift_pct']:+.2f}% |"
        )
    markdown = "\n".join(lines) + "\n"

    final = args.expdir / "final"
    final.mkdir(parents=True, exist_ok=True)
    timestamp_json = final / f"{args.output_prefix}_{stamp}.json"
    timestamp_md = final / f"{args.output_prefix}_{stamp}.md"
    fixed_json = final / f"{args.output_prefix}.json"
    fixed_md = final / f"{args.output_prefix}.md"
    payload = json.dumps(result, indent=2, sort_keys=True) + "\n"
    for path in (timestamp_json, fixed_json):
        path.write_text(payload)
    for path in (timestamp_md, fixed_md):
        path.write_text(markdown)
    manifest = final / "MANIFEST.md"
    with manifest.open("a") as handle:
        handle.write(
            f"- {result['generated_at']} `{timestamp_json.name}` / `{timestamp_md.name}` "
            f"-> `{fixed_json.name}` / `{fixed_md.name}`\n"
        )
    print(fixed_md)


if __name__ == "__main__":
    main()
