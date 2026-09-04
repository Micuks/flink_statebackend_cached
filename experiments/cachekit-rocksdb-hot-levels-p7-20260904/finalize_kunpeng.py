#!/usr/bin/env python3
"""Apply the final validity and host-cleanliness gate to the P7 Kunpeng result."""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import subprocess
import sys


EXPECTED_QUERIES = ["q5", "q9", "q11", "q15", "q18"]
EXPECTED_ARTIFACT = "6f6411d5a73126983eaba65f5c1c3335f4e269054ed0151392afad63bc6e7d57"
EXPECTED_SOURCE = "fb4d62eb92a2fce445d4b19b6a3fdf900e0bf1ce"


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} EXPERIMENT_DIR")
    root = pathlib.Path(sys.argv[1])
    assert (root / "CAMPAIGN_COMPLETE").is_file()
    assert json.loads((root / "CONFIG_DIFF_AUDIT.json").read_text())["valid"]
    assert json.loads((root / "FOREIGN_CONTAINER_PREFLIGHT.json").read_text())["valid"]
    summary = json.loads(
        (root / "final/P7_KUNPENG_EFFECTIVE5_RESULT.json").read_text()
    )
    assert summary["goal_gate"]["pass"] is True
    assert summary["arithmetic_mean_uplift_pct"] >= 10.0

    rows = []
    for result_path in sorted((root / "results/raw").glob("*/result.json")):
        leg = result_path.parent
        result = json.loads(result_path.read_text())
        measurement = result["measurement"]
        audit = json.loads((leg / "HOT_LEVEL_COMPRESSION_AUDIT.json").read_text())
        assert result["valid"] is True
        assert (leg / "LEG_COMPLETE").is_file()
        assert result["artifact_sha256"] == EXPECTED_ARTIFACT
        assert result["source_commit"] == EXPECTED_SOURCE
        assert 0.0 < measurement["cores"] <= 16.05
        assert measurement["raw_throughput_kps"] > 0.0
        assert measurement["throughput_kps_core"] > 0.0
        status = measurement["job_status"]
        assert status["measurement_integrity_valid"] is True
        assert status["real_job_completed"] is True
        assert status["real_job_failed"] is False
        assert status["expected_cpu_metric_tms"] == 8
        assert set(status["observed_cpu_metric_tms"]) == {8}
        assert audit["expected_hot_levels"] == 2
        assert audit["mode_activation_pass"] is True
        assert audit["activation_pass"] is True
        rows.append(
            {
                "leg": leg.name,
                "query": result["query"],
                "kps_core": measurement["throughput_kps_core"],
                "raw_kps": measurement["raw_throughput_kps"],
                "cores": measurement["cores"],
                "activation_pass": audit["activation_pass"],
            }
        )
    assert sorted(row["query"] for row in rows) == sorted(EXPECTED_QUERIES)

    running = subprocess.check_output(
        ["docker", "ps", "--format", "{{json .}}"], text=True
    ).splitlines()
    assert not running, f"containers remain after campaign: {running}"
    now = dt.datetime.now().astimezone()
    postflight = {
        "schema": "cachekit-p7-kunpeng-host-postflight-v1",
        "captured_at": now.isoformat(),
        "running_containers": running,
        "target_project": "ckkp5a9p7",
        "target_cpuset": "38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74",
        "target_cpuset_mems": "0",
        "rows": rows,
        "valid": True,
    }
    (root / "HOST_POSTFLIGHT.json").write_text(
        json.dumps(postflight, indent=2, sort_keys=True) + "\n"
    )
    (root / "final/HOST_RESULT_COMPLETE").write_text(
        "\n".join(
            [
                "schema=cachekit-p7-kunpeng-host-result-complete-v1",
                f"completed_at={now.isoformat()}",
                f"arithmetic_mean_uplift_pct={summary['arithmetic_mean_uplift_pct']}",
                "goal_gate_pass=true",
                "runtime_activation_pass=true",
                "host_postflight_valid=true",
                "",
            ]
        )
    )
    print(root / "final/HOST_RESULT_COMPLETE")


if __name__ == "__main__":
    main()
