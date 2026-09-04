#!/usr/bin/env python3
import pathlib
import subprocess


HOST = "root@173.154.10.2"
EXPERIMENT = pathlib.PurePosixPath(
    "/home/wuql/flink-cluster/experiments/"
    "cachekit-p10-snapshot-maintenance-q9-100m-kunpeng-numa0-20260904"
)
LEG_NAME = "004-r1-q9-hot2-maintained"
SOURCE_COMMIT = "8780838608a9c4ef1f374b91873ad3be7f576782"
ARTIFACT_SHA256 = (
    "ffc55699efb16639e41b35fe29eb9f00ad150c0088840d335d52d2b409f94e39"
)


REMOTE = r'''
import hashlib
import json
import pathlib
import subprocess
import sys


experiment = pathlib.Path(sys.argv[1])
leg_name = sys.argv[2]
source_commit = sys.argv[3]
artifact_sha = sys.argv[4]
runner = experiment / "run_campaign.sh"
leg = experiment / "results/raw" / leg_name
config = experiment / "variants/hot2-maintained/flink-conf.yaml"

if (experiment / "CAMPAIGN_COMPLETE").exists():
    raise SystemExit("refusing to repair a completed campaign")
if not runner.is_file() or not leg.is_dir():
    raise SystemExit("runner or failed leg is missing")
if (leg / "LEG_COMPLETE").exists() or (leg / "result.json").exists():
    raise SystemExit("leg is already finalized")
if subprocess.check_output(
    ["docker", "ps", "-q", "--filter", "name=ckkp5a9p10n0_"], text=True
).strip():
    raise SystemExit("campaign containers are still running")

measurement = json.loads((leg / "measurement-result.json").read_text())
status = measurement["job_status"]
if not (
    measurement["schema"] == "cachekit-golden-runner-result-v1"
    and measurement["query"] == "q9"
    and measurement["plan_index"] == 4
    and measurement["events"] == 100000000
    and status["measurement_integrity_valid"] is True
    and status["real_job_completed"] is True
    and status["real_job_failed"] is False
):
    raise SystemExit("measurement is not safe to recover")

text = runner.read_text()
old_assertion = "assert totals[0]>0 and totals[2]>0"
new_assertion = "assert totals[0]>0 and totals[3]>0"
if text.count(old_assertion) != 1 or new_assertion in text:
    raise SystemExit("unexpected maintenance assertion shape")
text = text.replace(old_assertion, new_assertion, 1)
runner.write_text(text)

lines = text.splitlines()
start = lines.index("import hashlib,json,pathlib,re,sys")
end = lines.index("PY", start)
validator = "\n".join(lines[start:end])
saved_argv = sys.argv
try:
    sys.argv = [
        "validator",
        str(leg),
        "q9",
        "1",
        "4",
        "hot2-maintained",
        "100000000",
        str(config),
        source_commit,
        artifact_sha,
        str(experiment / "identity.json"),
    ]
    exec(compile(validator, str(runner) + ":embedded-validator", "exec"), {})
finally:
    sys.argv = saved_argv

result = json.loads((leg / "result.json").read_text())
activation = result["snapshot_maintenance_activation"]
if not (
    result["valid"] is True
    and activation["enabled"] is True
    and activation["totals"][0] > 0
    and activation["totals"][3] > 0
):
    raise SystemExit("recovered result lacks maintenance activation")

repair_record = {
    "schema": "cachekit-validator-repair-v1",
    "leg": leg_name,
    "cause": "q9 has no known-noop updates; activation requires put attempts and applied updates",
    "old_assertion": old_assertion,
    "new_assertion": new_assertion,
    "measurement_reused": True,
    "measurement_valid": True,
}
(leg / "VALIDATOR_REPAIR.json").write_text(
    json.dumps(repair_record, indent=2, sort_keys=True) + "\n"
)

subprocess.run(
    [
        "python3",
        str(experiment / "audit_value_prefetch.py"),
        str(leg),
        "--output",
        str(leg / "VALUE_PREFETCH_AUDIT.json"),
    ],
    check=True,
    text=True,
    stdout=(leg / "value-prefetch-audit.stdout").open("w"),
)
subprocess.run(
    [
        "python3",
        str(experiment / "audit_hot_levels.py"),
        str(leg),
        "--expected-hot-levels",
        "2",
        "--require-sst",
        "--output",
        str(leg / "HOT_LEVEL_COMPRESSION_AUDIT.json"),
    ],
    check=True,
    text=True,
    stdout=(leg / "hot-level-compression-audit.stdout").open("w"),
)

manifest_names = [
    "measurement-result.json",
    "result.json",
    "flink-conf.yaml",
    "ready-gate-prometheus.json",
    "VALUE_PREFETCH_AUDIT.json",
    "HOT_LEVEL_COMPRESSION_AUDIT.json",
    "VALIDATOR_REPAIR.json",
    "rocksdb-sst-inventory.tsv",
    "rocksdb-sst-live-inventory.tsv",
]
manifest_lines = []
for name in manifest_names:
    path = leg / name
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest_lines.append(f"{digest}  {path}")
(leg / "LEG.SHA256SUMS").write_text("\n".join(manifest_lines) + "\n")
(leg / "LEG_COMPLETE").touch()
print(json.dumps({
    "leg": leg_name,
    "throughput_kps_core": measurement["throughput_kps_core"],
    "maintenance_totals": activation["totals"],
    "recovered": True,
}, sort_keys=True))
'''


def main():
    command = [
        "ssh",
        "-o",
        "BatchMode=yes",
        HOST,
        "python3",
        "-",
        str(EXPERIMENT),
        LEG_NAME,
        SOURCE_COMMIT,
        ARTIFACT_SHA256,
    ]
    subprocess.run(command, input=REMOTE, text=True, check=True)


if __name__ == "__main__":
    main()
