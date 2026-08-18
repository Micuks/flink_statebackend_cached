#!/usr/bin/env python3
import argparse
import csv
import hashlib
import json
import pathlib
import statistics
import subprocess

QUERIES = ["q4", "q5", "q8", "q9", "q11", "q18", "q19", "q20",
           "q3", "q7", "q12", "q13", "q15", "q16", "q17"]
GROUPS = {
    "大状态8Q": QUERIES[:8],
    "小状态7Q": QUERIES[8:],
    "ValueState11Q": ["q4", "q5", "q7", "q8", "q9", "q11", "q12", "q15", "q16", "q17", "q18"],
    "15Q总计": QUERIES,
}
AUDITS = ("VALIDATION.json", "CHECKPOINT_DISABLED.json", "TOPOLOGY_2X4.json",
          "NATIVE_RUNTIME_AUDIT.json")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--expdir", type=pathlib.Path, required=True)
    parser.add_argument("--platform", required=True)
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    parser.add_argument("--variants", default="control retained retained-preagg")
    args = parser.parse_args()
    variants = args.variants.split()
    assert len(variants) == 3
    assert (args.expdir / "CAMPAIGN_COMPLETE").is_file()

    found = {}
    for leg in sorted((args.expdir / "results/raw").glob("*-r1-q*-*")):
        if not (leg / "LEG_COMPLETE").is_file():
            continue
        subprocess.run(["sha256sum", "-c", "LEG.sha256"], cwd=leg,
                       check=True, stdout=subprocess.DEVNULL)
        wrapped = json.loads((leg / "result.json").read_text())
        query, variant, measurement = wrapped["query"], wrapped["variant"], wrapped["measurement"]
        for audit_name in AUDITS:
            audit = json.loads((leg / audit_name).read_text())
            assert audit.get("valid") is True, (leg, audit_name, audit)
        status = measurement["job_status"]
        assert measurement["events"] == 200_000_000
        assert measurement["throughput_kps_core"] > 0 and measurement["cores"] > 0
        assert measurement["raw_throughput_kps"] > 0
        assert status["real_job_completed"] is True and status["real_job_failed"] is False
        assert status["measurement_integrity_valid"] is True
        assert all(sample["tms"] == 8 for sample in measurement["cpu_metric_samples"])
        assert (query, variant) not in found
        found[query, variant] = measurement
    assert len(found) == 45, len(found)

    rows = []
    for query in QUERIES:
        control, retained, preagg = (found[query, variant] for variant in variants)
        c, r, p = (float(x["throughput_kps_core"]) for x in (control, retained, preagg))
        rows.append({
            "query": query,
            "control_kps_core": c,
            "retained_kps_core": r,
            "retained_preagg_kps_core": p,
            "retained_vs_control_pct": (r / c - 1) * 100,
            "preagg_vs_retained_pct": (p / r - 1) * 100,
            "all_vs_control_pct": (p / c - 1) * 100,
            "control_raw_kps": float(control["raw_throughput_kps"]),
            "retained_raw_kps": float(retained["raw_throughput_kps"]),
            "retained_preagg_raw_kps": float(preagg["raw_throughput_kps"]),
            "control_cores": float(control["cores"]),
            "retained_cores": float(retained["cores"]),
            "retained_preagg_cores": float(preagg["cores"]),
        })
    by_query = {row["query"]: row for row in rows}
    summaries = []
    for group, queries in GROUPS.items():
        summaries.append({
            "group": group,
            "query_count": len(queries),
            "retained_vs_control_pct": statistics.fmean(by_query[q]["retained_vs_control_pct"] for q in queries),
            "preagg_vs_retained_pct": statistics.fmean(by_query[q]["preagg_vs_retained_pct"] for q in queries),
            "all_vs_control_pct": statistics.fmean(by_query[q]["all_vs_control_pct"] for q in queries),
        })

    args.output_dir.mkdir(parents=True, exist_ok=True)
    payload = {"schema": "cachekit-native-fullopt-200m-r1-v1", "platform": args.platform,
               "source_variants": variants,
               "normalized_variants": ["control", "retained", "retained-preagg"],
               "rows": rows, "summaries": summaries, "valid": True}
    (args.output_dir / "RESULTS.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    with (args.output_dir / "RESULTS.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    with (args.output_dir / "SUMMARY.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    hashes = []
    for path in sorted(args.output_dir.glob("*.csv")) + sorted(args.output_dir.glob("*.json")):
        hashes.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n")
    (args.output_dir / "RESULTS.sha256").write_text("".join(hashes))
    (args.output_dir / "AUDIT_COMPLETE").touch()
    print(json.dumps({"valid": True, "platform": args.platform, "legs": len(found),
                      "summaries": summaries}, ensure_ascii=False))


if __name__ == "__main__":
    main()
