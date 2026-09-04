#!/usr/bin/env python3
"""Summarize the P7 source-level hot-tier compression experiments."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import statistics
from pathlib import Path


P7_X86_ARTIFACT = "27466030bae4e792777a6002ca4ee4e831b2b92411340e642c00484f549516bc"
P7_ARM_ARTIFACT = "6f6411d5a73126983eaba65f5c1c3335f4e269054ed0151392afad63bc6e7d57"
P4_ARM_ARTIFACT = "3c52ba5be2c2f903f348a991659036fa60a44bc161d4a3efb150b00cbd49c723"
SOURCE_COMMIT = "fb4d62eb92a2fce445d4b19b6a3fdf900e0bf1ce"


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
        "source_commit": result["source_commit"],
        "config_sha256": result["config_sha256"],
    }


def load_hot_audit(path: Path) -> dict:
    return json.loads((path / "HOT_LEVEL_COMPRESSION_AUDIT.json").read_text())


def load_global_audit(path: Path) -> dict:
    return json.loads((path / "COMPRESSION_AUDIT.json").read_text())


def rows_by_query(exp: Path) -> dict[str, Path]:
    result = {}
    for path in sorted((exp / "results/raw").glob("*/result.json")):
        row = load_row(path.parent)
        if row["query"] in result:
            raise SystemExit(f"duplicate query in {exp}: {row['query']}")
        result[row["query"]] = path.parent
    return result


def write_result(exp: Path, stem: str, result: dict, lines: list[str]) -> None:
    final = exp / "final"
    final.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(result, indent=2, sort_keys=True) + "\n"
    markdown = "\n".join(lines) + "\n"
    stamp = dt.datetime.fromisoformat(result["generated_at"]).strftime("%Y%m%d_%H%M%S")
    for path in (final / f"{stem}.json", final / f"{stem}_{stamp}.json"):
        path.write_text(payload)
    for path in (final / f"{stem}.md", final / f"{stem}_{stamp}.md"):
        path.write_text(markdown)
    print(final / f"{stem}.md")


def summarize_x86(args: argparse.Namespace) -> None:
    expected = {"highmem-snappy": 0, "hot1": 1, "hot2": 2}
    paths = {}
    for path in sorted((args.candidate_exp / "results/raw").glob("*/result.json")):
        row = load_row(path.parent)
        paths[row["variant"]] = path.parent
    if set(paths) != set(expected):
        raise SystemExit(f"x86 variant set mismatch: {sorted(paths)}")

    rows = []
    control = load_row(paths["highmem-snappy"])
    for variant, hot_levels in expected.items():
        path = paths[variant]
        row = load_row(path)
        audit = load_hot_audit(path)
        if not row["valid"]:
            raise SystemExit(f"invalid x86 row: {path}")
        if row["artifact_sha256"] != P7_X86_ARTIFACT:
            raise SystemExit(f"unexpected P7 x86 artifact: {path}")
        if row["source_commit"] != SOURCE_COMMIT:
            raise SystemExit(f"unexpected source commit: {path}")
        if audit["expected_hot_levels"] != hot_levels or not audit["activation_pass"]:
            raise SystemExit(f"hot-level activation failed: {path}")
        rows.append(
            {
                **row,
                "hot_levels": hot_levels,
                "uplift_vs_same_artifact_snappy_pct": 100.0
                * (row["kps_core"] / control["kps_core"] - 1.0),
                "activation": audit,
            }
        )

    global_path = args.global_no_compression_exp / "results/raw/001-r1-q9-highmem"
    global_row = load_row(global_path)
    global_audit = load_global_audit(global_path)
    hot2 = next(row for row in rows if row["variant"] == "hot2")
    hot2_audit = hot2["activation"]
    if not global_row["valid"] or not global_audit["activation_pass"]:
        raise SystemExit("invalid frozen global-no-compression reference")

    now = dt.datetime.now().astimezone()
    result = {
        "schema": "cachekit-p7-hot-level-compression-x86-screen-summary-v1",
        "generated_at": now.isoformat(),
        "candidate_exp": str(args.candidate_exp),
        "global_no_compression_exp": str(args.global_no_compression_exp),
        "comparison_contract": {
            "primary": "same P7 artifact; hot1 and hot2 versus hot-level-count-zero Snappy",
            "secondary": "same-host frozen P6 global-no-compression absolute reference; artifact differs",
        },
        "rows": rows,
        "global_no_compression_reference": {
            **global_row,
            "compression_activation": global_audit,
        },
        "hot2_vs_global_no_compression": {
            "kps_core_delta_pct": 100.0
            * (hot2["kps_core"] / global_row["kps_core"] - 1.0),
            "throughput_retained_pct": 100.0
            * hot2["kps_core"]
            / global_row["kps_core"],
            "sst_bytes_delta_pct": 100.0
            * (hot2_audit["sst_total_bytes"] / global_audit["sst_total_bytes"] - 1.0),
            "sst_bytes_reduction_pct": 100.0
            * (1.0 - hot2_audit["sst_total_bytes"] / global_audit["sst_total_bytes"]),
        },
        "screen_pass": all(row["activation"]["activation_pass"] for row in rows)
        and hot2["uplift_vs_same_artifact_snappy_pct"] > 10.0,
    }
    lines = [
        "# CacheKit P7 x86 source-policy screen",
        "",
        f"Generated: `{result['generated_at']}`",
        "",
        "| Variant | Hot levels | K/s/core | Raw K/s | Cores | vs same-artifact Snappy | SST files | SST bytes |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        audit = row["activation"]
        lines.append(
            f"| {row['variant']} | {row['hot_levels']} | {row['kps_core']:.2f} | "
            f"{row['raw_kps']:.2f} | {row['cores']:.2f} | "
            f"{row['uplift_vs_same_artifact_snappy_pct']:+.2f}% | "
            f"{audit['sst_file_count']} | {audit['sst_total_bytes']} |"
        )
    comparison = result["hot2_vs_global_no_compression"]
    lines.extend(
        [
            "",
            f"- hot2 throughput retained versus frozen global no-compression: `{comparison['throughput_retained_pct']:.2f}%`",
            f"- hot2 SST-byte reduction versus frozen global no-compression: `{comparison['sst_bytes_reduction_pct']:.2f}%`",
            f"- Source-policy screen: `{'PASS' if result['screen_pass'] else 'FAIL'}`",
        ]
    )
    write_result(args.candidate_exp, "P7_X86_SCREEN_RESULT", result, lines)


def summarize_kunpeng(args: argparse.Namespace) -> None:
    expected_queries = ["q5", "q9", "q11", "q15", "q18"]
    control_paths = {
        "q9": args.q9_control_exp / "results/raw/001-r1-q9-control",
        "q5": args.effective4_control_exp / "results/raw/001-r1-q5-control",
        "q11": args.effective4_control_exp / "results/raw/003-r1-q11-control",
        "q15": args.effective4_control_exp / "results/raw/005-r1-q15-control",
        "q18": args.effective4_control_exp / "results/raw/007-r1-q18-control",
    }
    candidate_paths = rows_by_query(args.candidate_exp)
    p6_paths = rows_by_query(args.p6_exp)
    if sorted(candidate_paths) != sorted(expected_queries):
        raise SystemExit(f"candidate query set mismatch: {sorted(candidate_paths)}")
    if sorted(p6_paths) != sorted(expected_queries):
        raise SystemExit(f"P6 query set mismatch: {sorted(p6_paths)}")

    rows = []
    uplifts = []
    for query in expected_queries:
        control = load_row(control_paths[query])
        candidate = load_row(candidate_paths[query])
        p6 = load_row(p6_paths[query])
        hot_audit = load_hot_audit(candidate_paths[query])
        p6_audit = load_global_audit(p6_paths[query])
        if not control["valid"] or not candidate["valid"] or not p6["valid"]:
            raise SystemExit(f"invalid comparison row for {query}")
        if control["artifact_sha256"] != P4_ARM_ARTIFACT:
            raise SystemExit(f"unexpected control artifact for {query}")
        if p6["artifact_sha256"] != P4_ARM_ARTIFACT:
            raise SystemExit(f"unexpected P6 artifact for {query}")
        if candidate["artifact_sha256"] != P7_ARM_ARTIFACT:
            raise SystemExit(f"unexpected P7 artifact for {query}")
        if candidate["source_commit"] != SOURCE_COMMIT:
            raise SystemExit(f"unexpected P7 source commit for {query}")
        if hot_audit["expected_hot_levels"] != 2 or not hot_audit["activation_pass"]:
            raise SystemExit(f"P7 runtime activation failed for {query}")
        if not p6_audit["mode_activation_pass"]:
            raise SystemExit(f"P6 runtime activation failed for {query}")
        uplift = 100.0 * (candidate["kps_core"] / control["kps_core"] - 1.0)
        uplifts.append(uplift)
        storage_delta = None
        if p6_audit["sst_total_bytes"]:
            storage_delta = 100.0 * (
                hot_audit["sst_total_bytes"] / p6_audit["sst_total_bytes"] - 1.0
            )
        rows.append(
            {
                "query": query,
                "control": control,
                "candidate": candidate,
                "p6_global_no_compression": p6,
                "uplift_pct": uplift,
                "delta_vs_p6_kps_core_pct": 100.0
                * (candidate["kps_core"] / p6["kps_core"] - 1.0),
                "hot_level_activation": hot_audit,
                "p6_compression_activation": p6_audit,
                "sst_bytes_delta_vs_p6_pct": storage_delta,
            }
        )

    mean_uplift = statistics.fmean(uplifts)
    activation_pass = all(row["hot_level_activation"]["activation_pass"] for row in rows)
    now = dt.datetime.now().astimezone()
    result = {
        "schema": "cachekit-p7-hot-level-compression-kunpeng-effective5-summary-v1",
        "generated_at": now.isoformat(),
        "candidate_exp": str(args.candidate_exp),
        "q9_control_exp": str(args.q9_control_exp),
        "effective4_control_exp": str(args.effective4_control_exp),
        "p6_exp": str(args.p6_exp),
        "headline_definition": "arithmetic mean of the five paired per-query K/s/core uplift percentages",
        "artifact_comparison": {
            "control_and_p6_sha256": P4_ARM_ARTIFACT,
            "p7_sha256": P7_ARM_ARTIFACT,
            "p7_source_commit": SOURCE_COMMIT,
            "p7_build_boundary": "two audited RocksDB policy class stems over the frozen P4 artifact; all non-overlay entries identical",
        },
        "rows": rows,
        "arithmetic_mean_uplift_pct": mean_uplift,
        "median_uplift_pct": statistics.median(uplifts),
        "minimum_uplift_pct": min(uplifts),
        "maximum_uplift_pct": max(uplifts),
        "activation_pass": activation_pass,
        "goal_gate": {
            "required_mean_uplift_pct": 10.0,
            "performance_pass": mean_uplift >= 10.0,
            "activation_pass": activation_pass,
            "pass": activation_pass and mean_uplift >= 10.0,
        },
    }
    lines = [
        "# CacheKit P7 source-policy Kunpeng effective-5 result",
        "",
        f"Generated: `{result['generated_at']}`",
        "",
        "| Query | Control K/s/core | hot2 K/s/core | hot2 raw K/s | Cores | Uplift | vs P6 K/s/core | SST files | SST bytes |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        candidate = row["candidate"]
        audit = row["hot_level_activation"]
        lines.append(
            f"| {row['query']} | {row['control']['kps_core']:.2f} | "
            f"{candidate['kps_core']:.2f} | {candidate['raw_kps']:.2f} | "
            f"{candidate['cores']:.2f} | {row['uplift_pct']:+.2f}% | "
            f"{row['delta_vs_p6_kps_core_pct']:+.2f}% | "
            f"{audit['sst_file_count']} | {audit['sst_total_bytes']} |"
        )
    lines.extend(
        [
            "",
            f"- Arithmetic mean: `{mean_uplift:+.2f}%`",
            f"- Exact hot2 runtime activation: `{'PASS' if activation_pass else 'FAIL'}`",
            f"- Effective-query mean >= +10.00%: `{'PASS' if result['goal_gate']['pass'] else 'FAIL'}`",
        ]
    )
    write_result(args.candidate_exp, "P7_KUNPENG_EFFECTIVE5_RESULT", result, lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    x86 = subparsers.add_parser("x86")
    x86.add_argument("candidate_exp", type=Path)
    x86.add_argument("global_no_compression_exp", type=Path)
    x86.set_defaults(func=summarize_x86)
    kunpeng = subparsers.add_parser("kunpeng")
    kunpeng.add_argument("candidate_exp", type=Path)
    kunpeng.add_argument("q9_control_exp", type=Path)
    kunpeng.add_argument("effective4_control_exp", type=Path)
    kunpeng.add_argument("p6_exp", type=Path)
    kunpeng.set_defaults(func=summarize_kunpeng)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
