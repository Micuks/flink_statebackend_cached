#!/usr/bin/env python3
import argparse
import json
import pathlib


def load(path):
    return json.loads(path.read_text())


def measurement(result):
    item = result["measurement"]
    return {
        "raw_throughput_kps": item["raw_throughput_kps"],
        "cores": item["cores"],
        "throughput_kps_core": item["throughput_kps_core"],
        "valid": result["valid"],
    }


def uplift(treatment, control):
    return (
        treatment["throughput_kps_core"] / control["throughput_kps_core"] - 1.0
    ) * 100.0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("experiment", type=pathlib.Path)
    args = parser.parse_args()

    root = args.experiment.resolve()
    if not (root / "CAMPAIGN_COMPLETE").is_file():
        raise SystemExit("campaign is not complete")
    results = {}
    for path in sorted((root / "results" / "raw").glob("*/result.json")):
        item = load(path)
        if item.get("query") == "q9":
            results[item["variant"]] = item
    expected = {"hot2-a", "hot2-overlay", "hot2-maintained"}
    if set(results) != expected:
        raise SystemExit(f"unexpected q9 variants: {sorted(results)}")

    control = measurement(results["hot2-a"])
    overlay = measurement(results["hot2-overlay"])
    maintained = measurement(results["hot2-maintained"])
    if not all(item["valid"] for item in (control, overlay, maintained)):
        raise SystemExit("one or more screen legs are invalid")

    overlay_activation = results["hot2-overlay"]["dirty_overlay_activation"]
    maintained_overlay_activation = results["hot2-maintained"][
        "dirty_overlay_activation"
    ]
    maintenance_activation = results["hot2-maintained"][
        "snapshot_maintenance_activation"
    ]
    overlay_totals = overlay_activation.get("totals", [])
    maintained_overlay_totals = maintained_overlay_activation.get("totals", [])
    maintenance_totals = maintenance_activation.get("totals", [])
    if (
        not overlay_activation.get("enabled")
        or not maintained_overlay_activation.get("enabled")
        or len(overlay_totals) != 6
        or len(maintained_overlay_totals) != 6
        or min(overlay_totals[:3]) <= 0
        or min(maintained_overlay_totals[:3]) <= 0
    ):
        raise SystemExit("dirty overlay did not meet activation gate")
    if (
        not maintenance_activation.get("enabled")
        or len(maintenance_totals) != 5
        or maintenance_totals[0] <= 0
        or maintenance_totals[2] <= 0
    ):
        raise SystemExit(
            f"snapshot maintenance did not meet activation gate: {maintenance_activation}"
        )

    snapshot = {
        variant: results[variant]["map_snapshot_activation"] for variant in expected
    }
    if snapshot["hot2-maintained"]["single_short_circuits"] <= 0:
        raise SystemExit("maintained leg did not execute a single-entry short circuit")
    iterator_reduction = (
        1.0
        - maintained_overlay_totals[0] / overlay_totals[0]
        if overlay_totals[0]
        else 0.0
    ) * 100.0

    summary = {
        "schema": "cachekit-snapshot-maintenance-p10-x86-summary-v1",
        "query": "q9",
        "control": {"variant": "hot2-a", **control},
        "overlay": {"variant": "hot2-overlay", **overlay},
        "treatment": {"variant": "hot2-maintained", **maintained},
        "overlay_vs_a_uplift_percent_kps_core": uplift(overlay, control),
        "maintenance_vs_overlay_uplift_percent_kps_core": uplift(
            maintained, overlay
        ),
        "a_plus_b_vs_a_uplift_percent_kps_core": uplift(maintained, control),
        "dirty_overlay_iterator_reduction_vs_overlay_percent": iterator_reduction,
        "overlay_activation": overlay_activation,
        "maintained_overlay_activation": maintained_overlay_activation,
        "snapshot_maintenance_activation": maintenance_activation,
        "map_snapshot_activation": snapshot,
        "causal_claim": (
            "same host and P10 artifact; maintained-vs-overlay changes only the "
            "snapshot-maintenance runtime gate"
        ),
    }

    final = root / "final"
    final.mkdir(exist_ok=True)
    (final / "P10_SCREEN_SUMMARY.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    rows = []
    for key in ("control", "overlay", "treatment"):
        item = summary[key]
        rows.append(
            f"| {item['variant']} | {item['raw_throughput_kps']:.2f} | "
            f"{item['cores']:.2f} | {item['throughput_kps_core']:.2f} | "
            f"{'VALID' if item['valid'] else 'INVALID'} |"
        )
    markdown = "\n".join(
        [
            "# P10 x86 q9 screen",
            "",
            "| Variant | Raw K/s | Cores | K/s/core | Leg |",
            "|---|---:|---:|---:|---|",
            *rows,
            "",
            "Overlay vs A K/s/core uplift: "
            f"{summary['overlay_vs_a_uplift_percent_kps_core']:.2f}%.",
            "Maintenance vs overlay K/s/core uplift: "
            f"{summary['maintenance_vs_overlay_uplift_percent_kps_core']:.2f}%.",
            "A+B vs A K/s/core uplift: "
            f"{summary['a_plus_b_vs_a_uplift_percent_kps_core']:.2f}%.",
            "Dirty-overlay iterator reduction vs overlay: "
            f"{iterator_reduction:.2f}%.",
            "",
            "The maintained-vs-overlay contrast isolates the P10 source mechanism.",
        ]
    )
    (final / "P10_SCREEN_RESULTS.md").write_text(markdown + "\n")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
