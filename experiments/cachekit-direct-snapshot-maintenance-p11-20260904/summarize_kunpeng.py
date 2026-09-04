#!/usr/bin/env python3
import hashlib
import json
import pathlib
import sys


EXPECTED_VARIANTS = (
    "hot2-a",
    "hot2-direct-maintained",
    "hot2-direct-maintained-64k",
)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def uplift(candidate, control):
    return 100.0 * (candidate / control - 1.0)


def compact(row):
    measurement = row["measurement"]
    return {
        "variant": row["variant"],
        "raw_throughput_kps": measurement["raw_throughput_kps"],
        "cores": measurement["cores"],
        "throughput_kps_core": measurement["throughput_kps_core"],
        "valid": row["valid"],
    }


def main():
    root = pathlib.Path(sys.argv[1])
    if not (root / "CAMPAIGN_COMPLETE").is_file():
        raise SystemExit("campaign is incomplete")

    rows = {}
    for path in sorted((root / "results/raw").glob("*/result.json")):
        leg = path.parent
        if not (leg / "LEG_COMPLETE").is_file():
            raise SystemExit(f"leg is incomplete: {leg}")
        for line in (leg / "LEG.SHA256SUMS").read_text().splitlines():
            digest, file_name = line.split(None, 1)
            artifact = pathlib.Path(file_name.strip())
            if sha256(artifact) != digest:
                raise SystemExit(f"leg manifest mismatch: {artifact}")
        row = json.loads(path.read_text())
        if row.get("valid") is not True or row.get("query") != "q9":
            raise SystemExit(f"invalid q9 row: {path}")
        rows[row["variant"]] = row
    if tuple(sorted(rows)) != tuple(sorted(EXPECTED_VARIANTS)):
        raise SystemExit(f"unexpected variants: {sorted(rows)}")

    source_commits = {row["source_commit"] for row in rows.values()}
    artifact_hashes = {row["artifact_sha256"] for row in rows.values()}
    if len(source_commits) != 1 or len(artifact_hashes) != 1:
        raise SystemExit("screen legs do not share one source/artifact identity")
    if {row["platform"] for row in rows.values()} != {"kunpeng"}:
        raise SystemExit("screen legs are not Kunpeng results")

    a = rows["hot2-a"]
    treatments = {
        name: rows[name]
        for name in EXPECTED_VARIANTS
        if name != "hot2-a"
    }
    if a["dirty_overlay_activation"] != {"enabled": False, "marker_count": 0}:
        raise SystemExit("A unexpectedly activated dirty overlay")
    if a["snapshot_maintenance_activation"] != {
        "enabled": False,
        "marker_count": 0,
    }:
        raise SystemExit("A unexpectedly activated snapshot maintenance")

    for name, row in treatments.items():
        if row["dirty_overlay_activation"] != {
            "enabled": False,
            "marker_count": 0,
        }:
            raise SystemExit(f"dirty overlay activated in {name}")
        activation = row["snapshot_maintenance_activation"]
        totals = activation.get("totals", [])
        if not (
            activation.get("enabled") is True
            and activation.get("marker_count", 0) > 0
            and len(totals) == 5
            and totals[0] > 0
            and totals[3] > 0
        ):
            raise SystemExit(f"direct maintenance did not activate in {name}")
        if row["map_snapshot_activation"]["single_short_circuits"] <= 0:
            raise SystemExit(f"snapshot short circuit did not activate in {name}")

    a_kpc = a["measurement"]["throughput_kps_core"]
    selected_name = max(
        treatments,
        key=lambda name: treatments[name]["measurement"]["throughput_kps_core"],
    )
    selected = treatments[selected_name]
    selected_uplift = uplift(
        selected["measurement"]["throughput_kps_core"], a_kpc
    )
    summary = {
        "schema": "cachekit-direct-snapshot-maintenance-p11-kunpeng-summary-v1",
        "query": "q9",
        "source_commit": next(iter(source_commits)),
        "artifact_sha256": next(iter(artifact_hashes)),
        "a": compact(a),
        "treatments": {name: compact(row) for name, row in treatments.items()},
        "maintenance_activation": {
            name: row["snapshot_maintenance_activation"]
            for name, row in treatments.items()
        },
        "map_snapshot_activation": {
            name: row["map_snapshot_activation"] for name, row in rows.items()
        },
        "uplift_percent_kps_core_vs_a": {
            name: uplift(row["measurement"]["throughput_kps_core"], a_kpc)
            for name, row in treatments.items()
        },
        "selection_rule": "highest valid q9 K/s/core among the two predeclared capacities",
        "selected_treatment": selected_name,
        "selected_uplift_percent_kps_core_vs_a": selected_uplift,
        "promotion_threshold_percent": 10.0,
        "promoted": selected_uplift >= 10.0,
        "claim_boundary": (
            "same Kunpeng NUMA0 physical cluster and P11 artifact; dirty overlay "
            "disabled in every leg; treatment changes only direct snapshot-maintenance "
            "activation and the predeclared snapshot-cache capacity"
        ),
    }
    final = root / "final"
    final.mkdir(exist_ok=True)
    (final / "P11_SCREEN_SUMMARY.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    lines = [
        "# P11 direct snapshot-maintenance q9 screen",
        "",
        "| Variant | Raw K/s | Cores | K/s/core | vs A | Valid |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for name in EXPECTED_VARIANTS:
        row = rows[name]
        measurement = row["measurement"]
        delta = "—" if name == "hot2-a" else (
            f"{summary['uplift_percent_kps_core_vs_a'][name]:+.2f}%"
        )
        lines.append(
            f"| {name} | {measurement['raw_throughput_kps']:.2f} | "
            f"{measurement['cores']:.2f} | "
            f"{measurement['throughput_kps_core']:.2f} | {delta} | yes |"
        )
    lines.extend(
        [
            "",
            f"Selected: `{selected_name}` ({selected_uplift:+.2f}% vs A).",
            f"Promotion gate: {'PASS' if summary['promoted'] else 'REJECT'} (>=10.00%).",
        ]
    )
    (final / "P11_SCREEN_RESULTS.md").write_text("\n".join(lines) + "\n")
    (final / "P11_SCREEN_COMPLETE").touch()
    if summary["promoted"]:
        (final / "P11_SCREEN_PROMOTED").touch()
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
