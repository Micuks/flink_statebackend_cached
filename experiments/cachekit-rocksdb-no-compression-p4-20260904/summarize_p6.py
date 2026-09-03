#!/usr/bin/env python3
"""Summarize the P6 high-memory candidate against frozen Kunpeng controls."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import statistics
from pathlib import Path


def load_row(path: Path) -> dict:
    result = json.loads((path / "result.json").read_text())
    measurement = result["measurement"]
    return {
        "leg": path.name,
        "query": result["query"],
        "variant": result["variant"],
        "valid": result["valid"] is True and (path / "LEG_COMPLETE").is_file(),
        "raw_kps": measurement["raw_throughput_kps"],
        "cores": measurement["cores"],
        "kps_core": measurement["throughput_kps_core"],
        "artifact_sha256": result["artifact_sha256"],
        "config_sha256": result["config_sha256"],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("candidate_exp", type=Path)
    parser.add_argument("q9_control_exp", type=Path)
    parser.add_argument("effective4_control_exp", type=Path)
    args = parser.parse_args()

    controls = {
        "q9": args.q9_control_exp / "results/raw/001-r1-q9-control",
        "q5": args.effective4_control_exp / "results/raw/001-r1-q5-control",
        "q11": args.effective4_control_exp / "results/raw/003-r1-q11-control",
        "q15": args.effective4_control_exp / "results/raw/005-r1-q15-control",
        "q18": args.effective4_control_exp / "results/raw/007-r1-q18-control",
    }
    candidates = {}
    for path in sorted((args.candidate_exp / "results/raw").glob("*/result.json")):
        row = load_row(path.parent)
        candidates[row["query"]] = path.parent

    expected_queries = ["q5", "q9", "q11", "q15", "q18"]
    if sorted(candidates) != sorted(expected_queries):
        raise SystemExit(f"candidate query set mismatch: {sorted(candidates)}")

    rows = []
    uplifts = []
    for query in expected_queries:
        control = load_row(controls[query])
        candidate = load_row(candidates[query])
        compression = json.loads(
            (candidates[query] / "COMPRESSION_AUDIT.json").read_text()
        )
        control_compression = json.loads(
            (controls[query] / "COMPRESSION_AUDIT.json").read_text()
        )
        if not control["valid"] or not candidate["valid"]:
            raise SystemExit(f"invalid pair for {query}")
        if control["artifact_sha256"] != candidate["artifact_sha256"]:
            raise SystemExit(f"artifact mismatch for {query}")
        uplift = 100.0 * (candidate["kps_core"] / control["kps_core"] - 1.0)
        uplifts.append(uplift)
        rows.append(
            {
                "query": query,
                "control": control,
                "candidate": candidate,
                "uplift_pct": uplift,
                "compression_activation": compression,
                "control_compression_activation": control_compression,
            }
        )

    for row in rows:
        candidate_activation = row["compression_activation"]
        control_activation = row["control_compression_activation"]
        row["storage_activation"] = {
            "policy": (
                "candidate-sst-required"
                if row["query"] != "q15"
                else "candidate-sst-or-control-to-memtable-residency"
            ),
            "candidate_sst_effect_pass": candidate_activation["sst_effect_pass"],
            "control_sst_effect_pass": control_activation.get("sst_effect_pass"),
            "memtable_residency_effect_pass": (
                row["query"] == "q15"
                and control_activation.get("sst_effect_pass") is True
                and not candidate_activation["sst_effect_pass"]
            ),
        }
        row["storage_activation"]["pass"] = candidate_activation[
            "sst_effect_pass"
        ] or row["storage_activation"]["memtable_residency_effect_pass"]
    activation_pass = all(
        row["compression_activation"]["mode_activation_pass"]
        and row["storage_activation"]["pass"]
        for row in rows
    )
    mean_uplift = statistics.fmean(uplifts)
    now = dt.datetime.now().astimezone()
    result = {
        "schema": "cachekit-p6-highmem-effective5-summary-v1",
        "generated_at": now.isoformat(),
        "candidate_exp": str(args.candidate_exp),
        "q9_control_exp": str(args.q9_control_exp),
        "effective4_control_exp": str(args.effective4_control_exp),
        "headline_definition": "arithmetic mean of the five paired per-query K/s/core uplift percentages",
        "rows": rows,
        "arithmetic_mean_uplift_pct": mean_uplift,
        "median_uplift_pct": statistics.median(uplifts),
        "minimum_uplift_pct": min(uplifts),
        "maximum_uplift_pct": max(uplifts),
        "activation_pass": activation_pass,
        "activation_definition": (
            "exact NO_COMPRESSION mode for every query; candidate SST evidence for q5/q9/q11/q18; "
            "q15 may instead pass when its frozen control produced SST and the 2 GiB HIGH_MEM candidate produced none"
        ),
        "goal_gate": {
            "required_mean_uplift_pct": 10.0,
            "performance_pass": mean_uplift >= 10.0,
            "activation_pass": activation_pass,
            "pass": activation_pass and mean_uplift >= 10.0,
        },
    }

    lines = [
        "# CacheKit P6 high-memory effective-5 result",
        "",
        f"Generated: `{result['generated_at']}`",
        "",
        "| Query | Control K/s/core | Candidate K/s/core | Candidate raw K/s | Cores | Uplift | SST files | SST bytes |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        candidate = row["candidate"]
        compression = row["compression_activation"]
        lines.append(
            f"| {row['query']} | {row['control']['kps_core']:.2f} | "
            f"{candidate['kps_core']:.2f} | {candidate['raw_kps']:.2f} | "
            f"{candidate['cores']:.2f} | {row['uplift_pct']:+.2f}% | "
            f"{compression['sst_file_count']} | {compression['sst_total_bytes']} |"
        )
    lines.extend(
        [
            "",
            f"- Arithmetic mean: `{mean_uplift:+.2f}%`",
            f"- Activation: `{'PASS' if activation_pass else 'FAIL'}`",
            f"- Effective-query mean >= +10.00%: `{'PASS' if result['goal_gate']['pass'] else 'FAIL'}`",
        ]
    )

    final = args.candidate_exp / "final"
    final.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(result, indent=2, sort_keys=True) + "\n"
    markdown = "\n".join(lines) + "\n"
    stamp = now.strftime("%Y%m%d_%H%M%S")
    for path in (
        final / "P6_EFFECTIVE5_RESULT.json",
        final / f"P6_EFFECTIVE5_RESULT_{stamp}.json",
    ):
        path.write_text(payload)
    for path in (
        final / "P6_EFFECTIVE5_RESULT.md",
        final / f"P6_EFFECTIVE5_RESULT_{stamp}.md",
    ):
        path.write_text(markdown)
    print(final / "P6_EFFECTIVE5_RESULT.md")


if __name__ == "__main__":
    main()
