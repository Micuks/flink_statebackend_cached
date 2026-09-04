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
    parser.add_argument("--p8-control", type=pathlib.Path)
    args = parser.parse_args()

    root = args.experiment.resolve()
    if not (root / "CAMPAIGN_COMPLETE").is_file():
        raise SystemExit("campaign is not complete")
    results = {}
    for path in sorted((root / "results" / "raw").glob("*/result.json")):
        item = load(path)
        if item.get("query") == "q9":
            results[item["variant"]] = item
    expected = {"hot2-a", "hot2-cache", "hot2-overlay"}
    if set(results) != expected:
        raise SystemExit(f"unexpected q9 variants: {sorted(results)}")

    control = measurement(results["hot2-a"])
    cache = measurement(results["hot2-cache"])
    treatment = measurement(results["hot2-overlay"])
    activation = results["hot2-overlay"]["dirty_overlay_activation"]
    totals = activation.get("totals", [])
    if (
        not activation.get("enabled")
        or len(totals) != 6
        or totals[0] <= 0
        or totals[1] <= 0
        or totals[2] <= 0
        or sum(totals[3:]) <= 0
    ):
        raise SystemExit(f"dirty overlay did not meet activation gate: {activation}")

    summary = {
        "schema": "cachekit-dirty-overlay-p9-x86-summary-v1",
        "query": "q9",
        "control": {"variant": "hot2-a", **control},
        "cache_only": {"variant": "hot2-cache", **cache},
        "treatment": {"variant": "hot2-overlay", **treatment},
        "cache_only_vs_a_uplift_percent_kps_core": uplift(cache, control),
        "overlay_vs_cache_only_uplift_percent_kps_core": uplift(treatment, cache),
        "a_plus_b_vs_a_uplift_percent_kps_core": uplift(treatment, control),
        "dirty_overlay_activation": activation,
        "causal_claim": (
            "same host and artifact; cache-only isolates capacity, while overlay-vs-cache "
            "isolates the source mechanism"
        ),
    }
    if args.p8_control:
        reference = measurement(load(args.p8_control.resolve()))
        summary["p8_control_context"] = {
            **reference,
            "p9_a_control_drift_percent_kps_core": uplift(control, reference),
            "claim_boundary": "contextual repeatability check, not a causal denominator",
        }

    final = root / "final"
    final.mkdir(exist_ok=True)
    (final / "P9_SCREEN_SUMMARY.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    rows = []
    for key in ("control", "cache_only", "treatment"):
        item = summary[key]
        rows.append(
            f"| {item['variant']} | {item['raw_throughput_kps']:.2f} | "
            f"{item['cores']:.2f} | {item['throughput_kps_core']:.2f} | "
            f"{'VALID' if item['valid'] else 'INVALID'} |"
        )
    markdown = "\n".join(
        [
            "# P9 x86 q9 screen",
            "",
            "| Variant | Raw K/s | Cores | K/s/core | Leg |",
            "|---|---:|---:|---:|---|",
            *rows,
            "",
            "Cache-only vs A K/s/core uplift: "
            f"{summary['cache_only_vs_a_uplift_percent_kps_core']:.2f}%.",
            "Overlay vs cache-only K/s/core uplift: "
            f"{summary['overlay_vs_cache_only_uplift_percent_kps_core']:.2f}%.",
            "A+B vs A K/s/core uplift: "
            f"{summary['a_plus_b_vs_a_uplift_percent_kps_core']:.2f}%.",
            "",
            "The overlay-vs-cache-only contrast is the source-mechanism attribution.",
        ]
    )
    (final / "P9_SCREEN_RESULTS.md").write_text(markdown + "\n")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
