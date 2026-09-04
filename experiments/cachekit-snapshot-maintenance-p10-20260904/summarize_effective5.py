#!/usr/bin/env python3
import argparse
import hashlib
import json
import pathlib
import statistics


QUERIES = ("q5", "q9", "q11", "q15", "q18")
VARIANTS = ("baseline", "hot2-a", "hot2-maintained-64k")
ARTIFACT_SHA256 = "09f5cd7078005bc803107e14a010263a921fb4e6307a92bb52ac9d358c503251"
SOURCE_COMMIT = "8780838608a9c4ef1f374b91873ad3be7f576782"


def load(path):
    return json.loads(path.read_text())


def uplift(treatment, control):
    return (
        treatment["throughput_kps_core"] / control["throughput_kps_core"] - 1.0
    ) * 100.0


def measurement(result):
    item = result["measurement"]
    return {
        "raw_throughput_kps": item["raw_throughput_kps"],
        "cores": item["cores"],
        "throughput_kps_core": item["throughput_kps_core"],
        "valid": result["valid"],
    }


def collect_results(root, queries):
    results = {}
    for path in sorted((root / "results" / "raw").glob("*/result.json")):
        result = load(path)
        query = result.get("query")
        variant = result.get("variant")
        if query in queries and variant in VARIANTS:
            key = (query, variant)
            if key in results:
                raise SystemExit(f"duplicate result for {key}")
            results[key] = result
    return results


def validate_result(result, query, variant, identity):
    if result.get("query") != query or result.get("variant") != variant:
        raise SystemExit(f"result identity mismatch for {(query, variant)}")
    if result.get("valid") is not True:
        raise SystemExit(f"invalid leg for {(query, variant)}")
    if result.get("artifact_sha256") != ARTIFACT_SHA256:
        raise SystemExit(f"artifact mismatch for {(query, variant)}")
    if result.get("source_commit") != SOURCE_COMMIT:
        raise SystemExit(f"source mismatch for {(query, variant)}")
    expected_config = identity["variant_config_sha256"][variant]
    if result.get("config_sha256") != expected_config:
        raise SystemExit(f"config mismatch for {(query, variant)}")
    item = measurement(result)
    if min(
        item["raw_throughput_kps"],
        item["cores"],
        item["throughput_kps_core"],
    ) <= 0:
        raise SystemExit(f"nonpositive measurement for {(query, variant)}")

    overlay = result["dirty_overlay_activation"]
    maintenance = result["snapshot_maintenance_activation"]
    if variant == "hot2-maintained-64k":
        if overlay.get("enabled") is not True or maintenance.get("enabled") is not True:
            raise SystemExit(f"missing A+B gate activation for {(query, variant)}")
        if overlay.get("marker_count", 0) <= 0 or maintenance.get("marker_count", 0) <= 0:
            raise SystemExit(f"missing A+B terminal markers for {(query, variant)}")
    elif overlay.get("enabled") or maintenance.get("enabled"):
        raise SystemExit(f"unexpected source gate activation for {(query, variant)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("screen", type=pathlib.Path)
    parser.add_argument("promotion", type=pathlib.Path)
    args = parser.parse_args()

    screen = args.screen.resolve()
    promotion = args.promotion.resolve()
    for root in (screen, promotion):
        if not (root / "CAMPAIGN_COMPLETE").is_file():
            raise SystemExit(f"campaign is not complete: {root}")

    screen_identity = load(screen / "identity.json")
    promotion_identity = load(promotion / "identity.json")
    if screen_identity.get("artifact_sha256") != ARTIFACT_SHA256:
        raise SystemExit("screen identity artifact mismatch")
    if promotion_identity.get("artifact_sha256") != ARTIFACT_SHA256:
        raise SystemExit("promotion identity artifact mismatch")
    if screen_identity.get("source_commit") != SOURCE_COMMIT:
        raise SystemExit("screen identity source mismatch")
    if promotion_identity.get("source_commit") != SOURCE_COMMIT:
        raise SystemExit("promotion identity source mismatch")

    screen_summary_path = screen / "final" / "P10_SCREEN_SUMMARY.json"
    screen_summary = load(screen_summary_path)
    expected_screen_summary_sha = hashlib.sha256(screen_summary_path.read_bytes()).hexdigest()
    if promotion_identity.get("screen_summary_sha256") != expected_screen_summary_sha:
        raise SystemExit("promotion identity does not seal the current q9 screen summary")
    if screen_summary["maintenance_64k_vs_overlay_64k_uplift_percent_kps_core"] <= 0:
        raise SystemExit("q9 directly isolated 64K source effect is not positive")

    screen_results = collect_results(screen, {"q9"})
    promotion_results = collect_results(promotion, set(QUERIES) - {"q9"})
    expected_screen = {("q9", variant) for variant in VARIANTS}
    expected_promotion = {
        (query, variant)
        for query in QUERIES
        if query != "q9"
        for variant in VARIANTS
    }
    if set(screen_results) != expected_screen:
        raise SystemExit(f"unexpected screen result set: {sorted(screen_results)}")
    if set(promotion_results) != expected_promotion:
        raise SystemExit(f"unexpected promotion result set: {sorted(promotion_results)}")

    results = {**screen_results, **promotion_results}
    for query in QUERIES:
        identity = screen_identity if query == "q9" else promotion_identity
        for variant in VARIANTS:
            validate_result(results[(query, variant)], query, variant, identity)

    query_rows = []
    measurement_rows = []
    for query in QUERIES:
        baseline = measurement(results[(query, "baseline")])
        a = measurement(results[(query, "hot2-a")])
        a_plus_b = measurement(results[(query, "hot2-maintained-64k")])
        query_rows.append(
            {
                "query": query,
                "a_vs_baseline_uplift_percent_kps_core": uplift(a, baseline),
                "a_plus_b_vs_baseline_uplift_percent_kps_core": uplift(
                    a_plus_b, baseline
                ),
                "a_plus_b_vs_a_uplift_percent_kps_core": uplift(a_plus_b, a),
            }
        )
        for variant, item in (
            ("baseline", baseline),
            ("hot2-a", a),
            ("hot2-maintained-64k", a_plus_b),
        ):
            measurement_rows.append({"query": query, "variant": variant, **item})

    mean_a = statistics.fmean(
        row["a_vs_baseline_uplift_percent_kps_core"] for row in query_rows
    )
    mean_integrated = statistics.fmean(
        row["a_plus_b_vs_baseline_uplift_percent_kps_core"] for row in query_rows
    )
    mean_incremental = statistics.fmean(
        row["a_plus_b_vs_a_uplift_percent_kps_core"] for row in query_rows
    )
    q9_integrated = next(
        row["a_plus_b_vs_baseline_uplift_percent_kps_core"]
        for row in query_rows
        if row["query"] == "q9"
    )
    if abs(q9_integrated - screen_summary["a_plus_b_vs_baseline_uplift_percent_kps_core"]) > 1e-9:
        raise SystemExit("q9 screen summary does not match raw result recomputation")

    treatment_activation = {}
    for query in QUERIES:
        result = results[(query, "hot2-maintained-64k")]
        overlay = result["dirty_overlay_activation"]
        maintenance = result["snapshot_maintenance_activation"]
        treatment_activation[query] = {
            "dirty_overlay_marker_count": overlay.get("marker_count", 0),
            "dirty_overlay_totals": overlay.get("totals", []),
            "snapshot_maintenance_marker_count": maintenance.get("marker_count", 0),
            "snapshot_maintenance_totals": maintenance.get("totals", []),
            "map_snapshot_activation": result["map_snapshot_activation"],
        }

    gate = {
        "all_15_legs_valid": True,
        "q9_direct_64k_source_effect_positive": True,
        "required_integrated_mean_uplift_percent": 10.0,
        "integrated_mean_uplift_percent": mean_integrated,
        "pass": mean_integrated >= 10.0,
    }
    summary = {
        "schema": "cachekit-p10-effective5-summary-v1",
        "headline_definition": (
            "arithmetic mean of the five per-query A+B versus fresh same-artifact "
            "baseline K/s/core uplift percentages"
        ),
        "artifact_sha256": ARTIFACT_SHA256,
        "source_commit": SOURCE_COMMIT,
        "queries": list(QUERIES),
        "measurement_rows": measurement_rows,
        "query_uplifts": query_rows,
        "arithmetic_mean_a_vs_baseline_uplift_percent_kps_core": mean_a,
        "arithmetic_mean_a_plus_b_vs_baseline_uplift_percent_kps_core": (
            mean_integrated
        ),
        "arithmetic_mean_a_plus_b_vs_a_uplift_percent_kps_core": mean_incremental,
        "q9_direct_source_effect_percent_kps_core": screen_summary[
            "maintenance_64k_vs_overlay_64k_uplift_percent_kps_core"
        ],
        "treatment_activation": treatment_activation,
        "gate": gate,
    }

    final = promotion / "final"
    final.mkdir(exist_ok=True)
    (final / "P10_EFFECTIVE5_SUMMARY.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    measurement_table = []
    for row in measurement_rows:
        measurement_table.append(
            f"| {row['query']} | {row['variant']} | "
            f"{row['throughput_kps_core']:.2f} | {row['raw_throughput_kps']:.2f} | "
            f"{row['cores']:.2f} | {'VALID' if row['valid'] else 'INVALID'} |"
        )
    uplift_table = []
    for row in query_rows:
        uplift_table.append(
            f"| {row['query']} | {row['a_vs_baseline_uplift_percent_kps_core']:+.2f}% | "
            f"{row['a_plus_b_vs_baseline_uplift_percent_kps_core']:+.2f}% | "
            f"{row['a_plus_b_vs_a_uplift_percent_kps_core']:+.2f}% |"
        )
    markdown = "\n".join(
        [
            "# P10 x86 effective-5 result",
            "",
            "| Query | Variant | K/s/core | Raw K/s | Cores | Leg |",
            "|---|---|---:|---:|---:|---|",
            *measurement_table,
            "",
            "| Query | A vs baseline | A+B vs baseline | A+B vs A |",
            "|---|---:|---:|---:|",
            *uplift_table,
            "",
            f"A mean vs baseline: {mean_a:+.2f}%.",
            f"A+B mean vs baseline: {mean_integrated:+.2f}%.",
            f"A+B mean vs A: {mean_incremental:+.2f}%.",
            "q9 directly isolated 64K source effect: "
            f"{summary['q9_direct_source_effect_percent_kps_core']:+.2f}%.",
            f"Effective-5 >= 10.00% gate: {'PASS' if gate['pass'] else 'FAIL'}.",
        ]
    )
    (final / "P10_EFFECTIVE5_RESULTS.md").write_text(markdown + "\n")
    if gate["pass"]:
        (final / "EFFECTIVE5_GATE_PASS").write_text("PASS\n")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
