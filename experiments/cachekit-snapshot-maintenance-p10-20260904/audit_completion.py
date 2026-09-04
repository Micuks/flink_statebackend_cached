#!/usr/bin/env python3
import hashlib
import json
import math
import pathlib
import statistics


SOURCE_COMMIT = "8780838608a9c4ef1f374b91873ad3be7f576782"
ARTIFACT_SHA256 = "09f5cd7078005bc803107e14a010263a921fb4e6307a92bb52ac9d358c503251"
QUERIES = ("q5", "q9", "q11", "q15", "q18")
VARIANTS = ("baseline", "hot2-a", "hot2-maintained-64k")


def load(path):
    return json.loads(path.read_text())


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_manifest(archive, manifest):
    checked = 0
    for line in manifest.read_text().splitlines():
        expected, relative = line.split(None, 1)
        relative = relative.strip()
        path = archive / relative
        if not path.is_file() or sha256(path) != expected:
            raise SystemExit(f"evidence manifest mismatch: {path}")
        checked += 1
    if checked == 0:
        raise SystemExit(f"empty evidence manifest: {manifest}")
    return checked


def collect_results(root, expected_legs):
    results = {}
    leg_dirs = sorted((root / "results" / "raw").glob("*"))
    complete = [path for path in leg_dirs if (path / "LEG_COMPLETE").is_file()]
    if len(complete) != expected_legs:
        raise SystemExit(f"unexpected leg count in {root}: {len(complete)}")
    for leg in complete:
        result = load(leg / "result.json")
        key = (result["query"], result["variant"])
        if key in results:
            raise SystemExit(f"duplicate raw result: {key}")
        results[key] = result
    return results


def validate_result(result, identity):
    if result.get("valid") is not True:
        raise SystemExit(f"invalid raw leg: {(result.get('query'), result.get('variant'))}")
    if result.get("source_commit") != SOURCE_COMMIT:
        raise SystemExit("raw leg source mismatch")
    if result.get("artifact_sha256") != ARTIFACT_SHA256:
        raise SystemExit("raw leg artifact mismatch")
    variant = result["variant"]
    if result.get("config_sha256") != identity["variant_config_sha256"][variant]:
        raise SystemExit(f"raw leg config mismatch: {(result['query'], variant)}")
    measurement = result["measurement"]
    raw = float(measurement["raw_throughput_kps"])
    cores = float(measurement["cores"])
    per_core = float(measurement["throughput_kps_core"])
    if min(raw, cores, per_core) <= 0.0 or cores > 16.05:
        raise SystemExit(f"invalid measurement: {(result['query'], variant)}")
    if not math.isclose(per_core, raw / cores, rel_tol=0.005):
        raise SystemExit(f"K/s/core mismatch: {(result['query'], variant)}")
    return {"raw_throughput_kps": raw, "cores": cores, "throughput_kps_core": per_core}


def uplift(treatment, control):
    return (treatment / control - 1.0) * 100.0


def main():
    script_dir = pathlib.Path(__file__).resolve().parent
    evidence = script_dir / "remote-evidence"
    p9 = evidence / "x86-p9"
    p10 = evidence / "x86-p10-screen"
    effective5 = evidence / "x86-effective5"
    for root in (p9, p10, effective5):
        if not (root / "CAMPAIGN_COMPLETE").is_file():
            raise SystemExit(f"missing completed local evidence archive: {root}")

    manifest_counts = {
        label: verify_manifest(root, evidence / f"{label}.SHA256SUMS")
        for label, root in (
            ("x86-p9", p9),
            ("x86-p10-screen", p10),
            ("x86-effective5", effective5),
        )
    }
    correctness = load(script_dir / "CORRECTNESS_AUDIT.json")
    if correctness.get("valid") is not True or correctness.get("source_commit") != SOURCE_COMMIT:
        raise SystemExit("correctness audit is not valid for P10")

    profile_path = (
        script_dir.parent
        / "cachekit-ready-gated-prefetch-profile-20260904"
        / "remote-evidence/attempt3/PROFILE_SUMMARY.json"
    )
    profile = load(profile_path)
    if profile.get("schema") != "cachekit-ready-gated-profile-summary-v1":
        raise SystemExit("profile evidence schema mismatch")
    join_scope = profile["events"]["cpu"]["control"]["thread_scopes"]["join"]
    frames = {row["frame"] for row in join_scope["top_inclusive"]}
    required_frames = {
        "org/apache/flink/table/runtime/operators/join/stream/StreamingJoinOperator.processElement",
        "org/apache/flink/table/runtime/operators/join/stream/AbstractStreamingJoinOperator$AssociatedRecords.of",
        "org/apache/flink/contrib/streaming/state/RocksDBMapState$RocksDBMapIterator.hasNext",
    }
    if not required_frames <= frames:
        raise SystemExit("profile does not contain the required q9 join/iterator evidence")

    p9_identity = load(p9 / "identity.json")
    p10_identity = load(p10 / "identity.json")
    effective_identity = load(effective5 / "identity.json")
    for identity in (p10_identity, effective_identity):
        if identity.get("source_commit") != SOURCE_COMMIT:
            raise SystemExit("experiment identity source mismatch")
        if identity.get("artifact_sha256") != ARTIFACT_SHA256:
            raise SystemExit("experiment identity artifact mismatch")
    for root in (p9, p10, effective5):
        if load(root / "CONFIG_DIFF_AUDIT.json").get("valid") is not True:
            raise SystemExit(f"invalid config-diff audit: {root}")

    p9_results = collect_results(p9, 3)
    p9_summary = load(p9 / "final/P9_SCREEN_SUMMARY.json")
    if set(p9_results) != {("q9", name) for name in ("hot2-a", "hot2-cache", "hot2-overlay")}:
        raise SystemExit("unexpected P9 raw result set")
    p9_activation = p9_results[("q9", "hot2-overlay")]["dirty_overlay_activation"]
    p9_totals = p9_activation.get("totals", [])
    if not p9_activation.get("enabled") or len(p9_totals) != 6 or min(p9_totals[:3]) <= 0:
        raise SystemExit("P9 overlay activation is not demonstrated")
    for result in p9_results.values():
        variant = result["variant"]
        if result.get("valid") is not True:
            raise SystemExit(f"invalid P9 raw leg: {variant}")
        if result.get("source_commit") != p9_identity["source_commit"]:
            raise SystemExit(f"P9 source mismatch: {variant}")
        if result.get("artifact_sha256") != p9_identity["artifact_sha256"]:
            raise SystemExit(f"P9 artifact mismatch: {variant}")
        if result.get("config_sha256") != p9_identity["variant_config_sha256"][variant]:
            raise SystemExit(f"P9 config mismatch: {variant}")
    if p9_summary.get("schema") != "cachekit-dirty-overlay-p9-x86-summary-v1":
        raise SystemExit("P9 summary schema mismatch")

    p10_results = collect_results(p10, 6)
    screen_summary = load(p10 / "final/P10_SCREEN_SUMMARY.json")
    if screen_summary["maintenance_64k_vs_overlay_64k_uplift_percent_kps_core"] <= 0.0:
        raise SystemExit("P10 direct 64K source effect is not positive")
    if screen_summary["dirty_overlay_iterator_reduction_2k_vs_overlay_percent"] <= 0.0:
        raise SystemExit("P10 2K treatment did not reduce base iterator requests")
    if screen_summary["dirty_overlay_iterator_reduction_64k_vs_overlay_percent"] <= 0.0:
        raise SystemExit("P10 64K treatment did not reduce base iterator requests")
    q9_expected = {("q9", variant) for variant in VARIANTS}
    if not q9_expected <= set(p10_results):
        raise SystemExit("P10 screen is missing final q9 variants")
    for result in p10_results.values():
        validate_result(result, p10_identity)
    q9_treatment = p10_results[("q9", "hot2-maintained-64k")]
    q9_maintenance = q9_treatment["snapshot_maintenance_activation"]
    q9_snapshot = q9_treatment["map_snapshot_activation"]
    if not q9_maintenance.get("enabled") or min(q9_maintenance.get("totals", [0])[:3]) <= 0:
        raise SystemExit("P10 q9 maintenance counters are not active")
    if q9_snapshot.get("single_short_circuits", 0) <= 0:
        raise SystemExit("P10 q9 singleton short circuit is not active")

    promotion_results = collect_results(effective5, 12)
    expected_promotion = {
        (query, variant) for query in QUERIES if query != "q9" for variant in VARIANTS
    }
    if set(promotion_results) != expected_promotion:
        raise SystemExit("unexpected effective-five promotion result set")
    all_results = {key: p10_results[key] for key in q9_expected}
    all_results.update(promotion_results)

    measurement_rows = []
    query_rows = []
    for query in QUERIES:
        identity = p10_identity if query == "q9" else effective_identity
        measurements = {
            variant: validate_result(all_results[(query, variant)], identity)
            for variant in VARIANTS
        }
        for variant in VARIANTS:
            result = all_results[(query, variant)]
            overlay = result["dirty_overlay_activation"]
            maintenance = result["snapshot_maintenance_activation"]
            if variant == "hot2-maintained-64k":
                if not overlay.get("enabled") or not maintenance.get("enabled"):
                    raise SystemExit(f"A+B source gates are not enabled: {(query, variant)}")
                if overlay.get("marker_count", 0) <= 0 or maintenance.get("marker_count", 0) <= 0:
                    raise SystemExit(f"A+B terminal counters are missing: {(query, variant)}")
            elif overlay.get("enabled") or maintenance.get("enabled"):
                raise SystemExit(f"source gate leaked into control: {(query, variant)}")
        baseline = measurements["baseline"]["throughput_kps_core"]
        a = measurements["hot2-a"]["throughput_kps_core"]
        treatment = measurements["hot2-maintained-64k"]["throughput_kps_core"]
        query_rows.append(
            {
                "query": query,
                "a_vs_baseline_uplift_percent_kps_core": uplift(a, baseline),
                "a_plus_b_vs_baseline_uplift_percent_kps_core": uplift(treatment, baseline),
                "a_plus_b_vs_a_uplift_percent_kps_core": uplift(treatment, a),
            }
        )
        for variant in VARIANTS:
            measurement_rows.append({"query": query, "variant": variant, **measurements[variant]})

    mean_integrated = statistics.fmean(
        row["a_plus_b_vs_baseline_uplift_percent_kps_core"] for row in query_rows
    )
    if mean_integrated < 10.0:
        raise SystemExit(f"effective-five mean is below 10%: {mean_integrated:.2f}%")
    final_summary = load(effective5 / "final/P10_EFFECTIVE5_SUMMARY.json")
    claimed = final_summary["arithmetic_mean_a_plus_b_vs_baseline_uplift_percent_kps_core"]
    if not math.isclose(mean_integrated, claimed, abs_tol=1e-9):
        raise SystemExit("effective-five summary does not match raw recomputation")
    if final_summary.get("gate", {}).get("pass") is not True:
        raise SystemExit("effective-five summary gate is not PASS")

    audit = {
        "schema": "cachekit-p10-goal-completion-audit-v1",
        "valid": True,
        "source_commit": SOURCE_COMMIT,
        "artifact_sha256": ARTIFACT_SHA256,
        "profile_summary_sha256": sha256(profile_path),
        "correctness_audit_sha256": sha256(script_dir / "CORRECTNESS_AUDIT.json"),
        "evidence_manifest_file_counts": manifest_counts,
        "all_15_legs_valid": True,
        "measurement_rows": measurement_rows,
        "query_uplifts": query_rows,
        "arithmetic_mean_a_plus_b_vs_baseline_uplift_percent_kps_core": mean_integrated,
        "q9_direct_source_effect_percent_kps_core": screen_summary[
            "maintenance_64k_vs_overlay_64k_uplift_percent_kps_core"
        ],
        "iterator_reduction_2k_percent": screen_summary[
            "dirty_overlay_iterator_reduction_2k_vs_overlay_percent"
        ],
        "iterator_reduction_64k_percent": screen_summary[
            "dirty_overlay_iterator_reduction_64k_vs_overlay_percent"
        ],
    }
    (script_dir / "COMPLETION_AUDIT.json").write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n"
    )
    rows = [
        f"| {row['query']} | {row['variant']} | {row['throughput_kps_core']:.2f} | "
        f"{row['raw_throughput_kps']:.2f} | {row['cores']:.2f} |"
        for row in measurement_rows
    ]
    (script_dir / "COMPLETION_AUDIT.md").write_text(
        "\n".join(
            (
                "# P10 goal completion audit",
                "",
                "| Query | Variant | K/s/core | Raw K/s | Cores |",
                "|---|---|---:|---:|---:|",
                *rows,
                "",
                f"Effective-five arithmetic mean A+B vs baseline: {mean_integrated:+.2f}%.",
                "All 15 legs, source/artifact/config identities, mechanism counters, correctness, profiling, and local raw-evidence manifests passed independent verification.",
            )
        )
        + "\n"
    )
    print(json.dumps(audit, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
