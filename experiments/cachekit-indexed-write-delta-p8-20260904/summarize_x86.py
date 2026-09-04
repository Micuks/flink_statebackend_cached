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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("experiment", type=pathlib.Path)
    parser.add_argument("--p7-reference", type=pathlib.Path)
    args = parser.parse_args()

    root = args.experiment.resolve()
    if not (root / "CAMPAIGN_COMPLETE").is_file():
        raise SystemExit("campaign is not complete")
    results = {}
    for path in sorted((root / "results" / "raw").glob("*/result.json")):
        item = load(path)
        if item.get("query") == "q9":
            results[item["variant"]] = item
    if set(results) != {"hot2-a", "hot2-indexed"}:
        raise SystemExit(f"unexpected q9 variants: {sorted(results)}")

    control = measurement(results["hot2-a"])
    treatment = measurement(results["hot2-indexed"])
    uplift = (
        treatment["throughput_kps_core"] / control["throughput_kps_core"] - 1.0
    ) * 100.0
    activation = results["hot2-indexed"]["indexed_write_batch_activation"]
    if not activation["enabled"] or activation["entries_per_flush"] <= 1.0:
        raise SystemExit(f"indexed delta did not meet activation gate: {activation}")

    summary = {
        "schema": "cachekit-indexed-mapstate-delta-p8-x86-summary-v1",
        "query": "q9",
        "control": {"variant": "hot2-a", **control},
        "treatment": {"variant": "hot2-indexed", **treatment},
        "uplift_percent_kps_core": uplift,
        "indexed_write_batch_activation": activation,
        "causal_claim": "same host, same artifact, indexed delta flag only",
    }
    if args.p7_reference:
        p7 = load(args.p7_reference.resolve())
        reference = measurement(p7)
        summary["p7_hot2_context"] = {
            **reference,
            "a_control_drift_percent_kps_core": (
                control["throughput_kps_core"] / reference["throughput_kps_core"] - 1.0
            )
            * 100.0,
            "claim_boundary": "contextual repeatability check, not the causal denominator",
        }

    final = root / "final"
    final.mkdir(exist_ok=True)
    (final / "P8_SCREEN_SUMMARY.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    rows = []
    for key in ("control", "treatment"):
        item = summary[key]
        rows.append(
            f"| {item['variant']} | {item['raw_throughput_kps']:.2f} | "
            f"{item['cores']:.2f} | {item['throughput_kps_core']:.2f} | "
            f"{'VALID' if item['valid'] else 'INVALID'} |"
        )
    markdown = "\n".join(
        [
            "# P8 x86 q9 screen",
            "",
            "| Variant | Raw K/s | Cores | K/s/core | Leg |",
            "|---|---:|---:|---:|---|",
            *rows,
            "",
            f"A+B vs A K/s/core uplift: {uplift:.2f}%.",
            "",
            "Causal denominator: same host and P8 artifact; only "
            "`state.backend.rocksdb.write-batch-with-index.enabled` differs.",
            "",
            f"Indexed entries per flush: {activation['entries_per_flush']:.2f}.",
        ]
    )
    (final / "P8_SCREEN_RESULTS.md").write_text(markdown + "\n")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
