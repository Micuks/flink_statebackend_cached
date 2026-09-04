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
    expected = {
        "baseline",
        "hot2-a",
        "hot2-overlay",
        "hot2-maintained",
        "hot2-overlay-64k",
        "hot2-maintained-64k",
    }
    if set(results) != expected:
        raise SystemExit(f"unexpected q9 variants: {sorted(results)}")
    if any(result.get("platform") != "kunpeng" for result in results.values()):
        raise SystemExit("one or more screen legs are not Kunpeng results")
    source_commits = {result.get("source_commit") for result in results.values()}
    artifact_hashes = {result.get("artifact_sha256") for result in results.values()}
    if len(source_commits) != 1 or len(artifact_hashes) != 1:
        raise SystemExit("screen legs do not share one source commit and artifact")

    baseline = measurement(results["baseline"])
    a_control = measurement(results["hot2-a"])
    overlay = measurement(results["hot2-overlay"])
    maintained = measurement(results["hot2-maintained"])
    overlay_64k = measurement(results["hot2-overlay-64k"])
    maintained_64k = measurement(results["hot2-maintained-64k"])
    if not all(
        item["valid"]
        for item in (
            baseline,
            a_control,
            overlay,
            maintained,
            overlay_64k,
            maintained_64k,
        )
    ):
        raise SystemExit("one or more screen legs are invalid")

    overlay_activation = results["hot2-overlay"]["dirty_overlay_activation"]
    maintained_overlay_activation = results["hot2-maintained"][
        "dirty_overlay_activation"
    ]
    maintenance_activation = results["hot2-maintained"][
        "snapshot_maintenance_activation"
    ]
    overlay_64k_activation = results["hot2-overlay-64k"][
        "dirty_overlay_activation"
    ]
    maintained_64k_overlay_activation = results["hot2-maintained-64k"][
        "dirty_overlay_activation"
    ]
    maintenance_64k_activation = results["hot2-maintained-64k"][
        "snapshot_maintenance_activation"
    ]
    overlay_totals = overlay_activation.get("totals", [])
    maintained_overlay_totals = maintained_overlay_activation.get("totals", [])
    overlay_64k_totals = overlay_64k_activation.get("totals", [])
    maintained_64k_overlay_totals = maintained_64k_overlay_activation.get("totals", [])
    maintenance_totals = maintenance_activation.get("totals", [])
    maintenance_64k_totals = maintenance_64k_activation.get("totals", [])
    if (
        not overlay_activation.get("enabled")
        or not maintained_overlay_activation.get("enabled")
        or not overlay_64k_activation.get("enabled")
        or not maintained_64k_overlay_activation.get("enabled")
        or len(overlay_totals) != 6
        or len(maintained_overlay_totals) != 6
        or len(overlay_64k_totals) != 6
        or len(maintained_64k_overlay_totals) != 6
        or min(overlay_totals[:3]) <= 0
        or min(maintained_overlay_totals[:3]) <= 0
        or min(overlay_64k_totals[:3]) <= 0
        or min(maintained_64k_overlay_totals[:3]) <= 0
    ):
        raise SystemExit("dirty overlay did not meet activation gate")
    for variant, activation, totals in (
        ("hot2-maintained", maintenance_activation, maintenance_totals),
        ("hot2-maintained-64k", maintenance_64k_activation, maintenance_64k_totals),
    ):
        if (
            not activation.get("enabled")
            or len(totals) != 5
            or totals[0] <= 0
            or totals[2] <= 0
        ):
            raise SystemExit(
                f"snapshot maintenance did not meet activation gate for {variant}: "
                f"{activation}"
            )

    snapshot = {
        variant: results[variant]["map_snapshot_activation"] for variant in expected
    }
    if snapshot["hot2-maintained"]["single_short_circuits"] <= 0:
        raise SystemExit("maintained leg did not execute a single-entry short circuit")
    if snapshot["hot2-maintained-64k"]["single_short_circuits"] <= 0:
        raise SystemExit("maintained-64k leg did not execute a single-entry short circuit")
    iterator_reduction_2k = (
        1.0
        - maintained_overlay_totals[0] / overlay_totals[0]
        if overlay_totals[0]
        else 0.0
    ) * 100.0
    iterator_reduction_64k = (
        1.0
        - maintained_64k_overlay_totals[0] / overlay_64k_totals[0]
        if overlay_64k_totals[0]
        else 0.0
    ) * 100.0
    if iterator_reduction_2k <= 0.0 or iterator_reduction_64k <= 0.0:
        raise SystemExit(
            "snapshot maintenance did not reduce dirty-overlay base iterator requests: "
            f"2k={iterator_reduction_2k:.2f}% 64k={iterator_reduction_64k:.2f}%"
        )

    summary = {
        "schema": "cachekit-snapshot-maintenance-p10-kunpeng-summary-v1",
        "query": "q9",
        "baseline": {"variant": "baseline", **baseline},
        "a": {"variant": "hot2-a", **a_control},
        "overlay": {"variant": "hot2-overlay", **overlay},
        "mechanism_treatment": {"variant": "hot2-maintained", **maintained},
        "capacity_control": {"variant": "hot2-overlay-64k", **overlay_64k},
        "treatment": {"variant": "hot2-maintained-64k", **maintained_64k},
        "a_vs_baseline_uplift_percent_kps_core": uplift(a_control, baseline),
        "overlay_vs_a_uplift_percent_kps_core": uplift(overlay, a_control),
        "maintenance_2k_vs_overlay_uplift_percent_kps_core": uplift(
            maintained, overlay
        ),
        "capacity_64k_vs_2k_without_maintenance_uplift_percent_kps_core": uplift(
            overlay_64k, overlay
        ),
        "maintenance_64k_vs_overlay_64k_uplift_percent_kps_core": uplift(
            maintained_64k, overlay_64k
        ),
        "capacity_64k_vs_2k_with_maintenance_uplift_percent_kps_core": uplift(
            maintained_64k, maintained
        ),
        "a_plus_b_vs_a_uplift_percent_kps_core": uplift(
            maintained_64k, a_control
        ),
        "a_plus_b_vs_baseline_uplift_percent_kps_core": uplift(
            maintained_64k, baseline
        ),
        "dirty_overlay_iterator_reduction_2k_vs_overlay_percent": (
            iterator_reduction_2k
        ),
        "dirty_overlay_iterator_reduction_64k_vs_overlay_percent": (
            iterator_reduction_64k
        ),
        "overlay_activation": overlay_activation,
        "maintained_overlay_activation": maintained_overlay_activation,
        "overlay_64k_activation": overlay_64k_activation,
        "maintained_64k_overlay_activation": maintained_64k_overlay_activation,
        "snapshot_maintenance_activation": maintenance_activation,
        "snapshot_maintenance_64k_activation": maintenance_64k_activation,
        "map_snapshot_activation": snapshot,
        "causal_claim": (
            "same Kunpeng NUMA2 cluster and P10 artifact; the recorded coexistence "
            "preflight proves the selected CPUs idle and captures any foreign "
            "containers; maintained-vs-overlay changes only the "
            "snapshot-maintenance runtime gate at each capacity, while each 64k-vs-2k "
            "contrast changes only exact-membership cache capacity and baseline-vs-A "
            "changes only uncompressed hot-level count"
        ),
    }

    final = root / "final"
    final.mkdir(exist_ok=True)
    (final / "P10_SCREEN_SUMMARY.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    rows = []
    for key in (
        "baseline",
        "a",
        "overlay",
        "mechanism_treatment",
        "capacity_control",
        "treatment",
    ):
        item = summary[key]
        rows.append(
            f"| {item['variant']} | {item['raw_throughput_kps']:.2f} | "
            f"{item['cores']:.2f} | {item['throughput_kps_core']:.2f} | "
            f"{'VALID' if item['valid'] else 'INVALID'} |"
        )
    markdown = "\n".join(
        [
            "# P10 Kunpeng NUMA2 q9 screen",
            "",
            "| Variant | Raw K/s | Cores | K/s/core | Leg |",
            "|---|---:|---:|---:|---|",
            *rows,
            "",
            "A vs baseline K/s/core uplift: "
            f"{summary['a_vs_baseline_uplift_percent_kps_core']:.2f}%.",
            "Overlay vs A K/s/core uplift: "
            f"{summary['overlay_vs_a_uplift_percent_kps_core']:.2f}%.",
            "Maintenance 2K vs overlay K/s/core uplift: "
            f"{summary['maintenance_2k_vs_overlay_uplift_percent_kps_core']:.2f}%.",
            "Capacity 64K vs 2K without maintenance K/s/core uplift: "
            f"{summary['capacity_64k_vs_2k_without_maintenance_uplift_percent_kps_core']:.2f}%.",
            "Maintenance 64K vs overlay 64K K/s/core uplift: "
            f"{summary['maintenance_64k_vs_overlay_64k_uplift_percent_kps_core']:.2f}%.",
            "Capacity 64K vs 2K with maintenance K/s/core uplift: "
            f"{summary['capacity_64k_vs_2k_with_maintenance_uplift_percent_kps_core']:.2f}%.",
            "A+B vs A K/s/core uplift: "
            f"{summary['a_plus_b_vs_a_uplift_percent_kps_core']:.2f}%.",
            "A+B vs baseline K/s/core uplift: "
            f"{summary['a_plus_b_vs_baseline_uplift_percent_kps_core']:.2f}%.",
            "Dirty-overlay iterator reduction, maintained 2K vs overlay: "
            f"{iterator_reduction_2k:.2f}%.",
            "Dirty-overlay iterator reduction, maintained 64K vs overlay: "
            f"{iterator_reduction_64k:.2f}%.",
            "",
            "The maintained-vs-overlay contrasts isolate the P10 source mechanism at "
            "both capacities; the 64K-vs-2K contrasts isolate exact-membership capacity.",
        ]
    )
    (final / "P10_SCREEN_RESULTS.md").write_text(markdown + "\n")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
