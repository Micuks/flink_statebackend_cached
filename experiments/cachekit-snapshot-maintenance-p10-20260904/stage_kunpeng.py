#!/usr/bin/env python3
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import textwrap


HOST = "root@173.154.10.2"
RUNNER_HOST = "root@114.116.229.206"
RUNNER_PATH = (
    "/home/wuql/flink-cluster/experiments/"
    "cachekit-p10-snapshot-maintenance-q9-100m-x86-20260904/run_campaign.sh"
)
RUNNER_SHA256 = (
    "1e93db23506920483bfd38a483e80de987128a4f094cafb8b1533a768a6df13a"
)
SOURCE_EXPERIMENT = (
    "/home/wuql/flink-cluster/experiments/"
    "cachekit-p7-hot-level-compression-effective5-100m-kunpeng-20260904"
)
TARGET_EXPERIMENT = (
    "/home/wuql/flink-cluster/experiments/"
    "cachekit-p10-snapshot-maintenance-q9-100m-kunpeng-numa0-20260904"
)
TARGET_PROJECT = "ckkp5a9p10n0"
SOURCE_SCRATCH = "/tmp/ckkp5a9p7"
TARGET_SCRATCH = "/tmp/ckkp5a9p10n0"
SOURCE_PORTS = ("10814", "11877", "11878")
TARGET_PORTS = ("10850", "11949", "11950")
SOURCE_COMMIT = "8780838608a9c4ef1f374b91873ad3be7f576782"
ARTIFACT_SHA256 = (
    "ffc55699efb16639e41b35fe29eb9f00ad150c0088840d335d52d2b409f94e39"
)
TARGET_CPUSET = (
    "38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74"
)
TARGET_CPUSET_MEMS = "0"
REMOTE_STAGE = "/tmp/cachekit-p10-snapshot-maintenance-kunpeng-stage-20260904"


def run(command, **kwargs):
    return subprocess.run(command, check=True, text=True, **kwargs)


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


REMOTE_SCRIPT = r'''
import hashlib
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import time


(
    source_exp_text,
    target_exp_text,
    target_project,
    source_scratch,
    target_scratch,
    source_rest_port,
    source_prom_port,
    source_push_port,
    target_rest_port,
    target_prom_port,
    target_push_port,
    source_commit,
    artifact_sha,
    target_cpuset,
    target_cpuset_mems,
    stage_text,
) = sys.argv[1:]
source_exp = pathlib.Path(source_exp_text)
final_target_exp = pathlib.Path(target_exp_text)
target_exp = pathlib.Path(target_exp_text + ".partial")
stage = pathlib.Path(stage_text)
variants = {
    "baseline": {
        "hot_levels": "0",
        "map_cache": "0",
        "snapshot_cache": "2000",
        "overlay": "false",
        "maintenance": "false",
    },
    "hot2-a": {
        "hot_levels": "2",
        "map_cache": "0",
        "snapshot_cache": "2000",
        "overlay": "false",
        "maintenance": "false",
    },
    "hot2-overlay": {
        "hot_levels": "2",
        "map_cache": "65536",
        "snapshot_cache": "2000",
        "overlay": "true",
        "maintenance": "false",
    },
    "hot2-maintained": {
        "hot_levels": "2",
        "map_cache": "65536",
        "snapshot_cache": "2000",
        "overlay": "true",
        "maintenance": "true",
    },
    "hot2-overlay-64k": {
        "hot_levels": "2",
        "map_cache": "65536",
        "snapshot_cache": "65536",
        "overlay": "true",
        "maintenance": "false",
    },
    "hot2-maintained-64k": {
        "hot_levels": "2",
        "map_cache": "65536",
        "snapshot_cache": "65536",
        "overlay": "true",
        "maintenance": "true",
    },
}


def command(args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def hash_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def replace_once(text, old, new, label):
    count = text.count(old)
    if count != 1:
        raise SystemExit(
            f"{label} replacement count {count} for {old!r}; expected 1"
        )
    return text.replace(old, new)


def expand_cpuset(spec):
    result = set()
    for part in spec.split(","):
        bounds = [int(value) for value in part.split("-", 1)]
        result.update(range(bounds[0], bounds[-1] + 1))
    return result


if not (source_exp / "CAMPAIGN_COMPLETE").is_file():
    raise SystemExit("source campaign is incomplete")
if not (source_exp / "final/HOST_RESULT_COMPLETE").is_file():
    raise SystemExit("source host result is incomplete")
if final_target_exp.exists():
    raise SystemExit(f"target already exists: {final_target_exp}")
if target_exp.exists():
    raise SystemExit(f"partial target already exists: {target_exp}")
for port in (target_rest_port, target_prom_port, target_push_port):
    occupied = subprocess.run(
        ["ss", "-ltnH", f"sport = :{port}"],
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()
    if occupied:
        raise SystemExit(f"port already in use: {port}")

target_cpus = expand_cpuset(target_cpuset)
foreign_containers = []
for container_id in subprocess.check_output(
    ["docker", "ps", "-q"], text=True
).split():
    data = json.loads(
        subprocess.check_output(["docker", "inspect", container_id], text=True)
    )[0]
    cpus = data["HostConfig"].get("CpusetCpus", "")
    overlap = sorted(target_cpus & expand_cpuset(cpus)) if cpus else sorted(target_cpus)
    item = {
        "name": data["Name"].lstrip("/"),
        "project": data["Config"].get("Labels", {}).get(
            "com.docker.compose.project"
        ),
        "cpus": cpus,
        "mems": data["HostConfig"].get("CpusetMems", ""),
        "target_cpu_overlap": overlap,
    }
    foreign_containers.append(item)
if any(item["target_cpu_overlap"] for item in foreign_containers):
    raise SystemExit(
        "target cpuset overlaps running containers: "
        + json.dumps(foreign_containers, sort_keys=True)
    )

target_exp.mkdir(parents=True)
(target_exp / "logs").mkdir()
shutil.copytree(source_exp / "inputs", target_exp / "inputs")
for variant in variants:
    destination = target_exp / "variants" / variant
    destination.mkdir(parents=True)
    shutil.copy2(
        source_exp / "variants/hot2/flink-conf.yaml",
        destination / "flink-conf.yaml",
    )
    shutil.copy2(
        source_exp / "variants/hot2/docker-compose.yml",
        destination / "docker-compose.yml",
    )
for name in ("identity.json", "audit_value_prefetch.py", "audit_hot_levels.py"):
    shutil.copy2(source_exp / name, target_exp / name)
shutil.copy2(stage / "run_campaign.template.sh", target_exp / "run_campaign.sh")
shutil.copy2(stage / "summarize_kunpeng.py", target_exp / "summarize_kunpeng.py")
shutil.copy2(
    stage / "flink-statebackend-cachekit-1.16-SNAPSHOT-p10-aarch64.jar",
    target_exp / "inputs/runtime/flink-statebackend-cachekit-1.16-SNAPSHOT.jar",
)

runner = target_exp / "run_campaign.sh"
runner_text = runner.read_text()
old_mems_json = (
    "export GOLDEN_CONTAINER_CPUSET_MEMS_JSON="
    "'{\"ckx865a9p10_jobmanager_1\":\"0\","
    "\"ckx865a9p10_taskmanager1_1\":\"0\","
    "\"ckx865a9p10_taskmanager2_1\":\"1\","
    "\"ckx865a9p10_prometheus_1\":\"1\","
    "\"ckx865a9p10_pushgateway_1\":\"1\"}'"
)
new_mems_json = (
    "export GOLDEN_CONTAINER_CPUSET_MEMS_JSON="
    f"'{{\"{target_project}_jobmanager_1\":\"{target_cpuset_mems}\","
    f"\"{target_project}_taskmanager1_1\":\"{target_cpuset_mems}\","
    f"\"{target_project}_taskmanager2_1\":\"{target_cpuset_mems}\","
    f"\"{target_project}_prometheus_1\":\"{target_cpuset_mems}\","
    f"\"{target_project}_pushgateway_1\":\"{target_cpuset_mems}\"}}'"
)
runner_text = replace_once(
    runner_text, old_mems_json, new_mems_json, "runner mems json"
)
project_count = runner_text.count("ckx865a9p10")
if project_count != 2:
    raise SystemExit(
        f"runner project replacement count {project_count}; expected 2"
    )
runner_text = runner_text.replace("ckx865a9p10", target_project)
if f"scratch={target_scratch}" not in runner_text:
    raise SystemExit("runner scratch was not updated with the project token")
for old, new in (
    (
        "/home/wuql/flink-cluster/experiments/"
        "cachekit-p10-snapshot-maintenance-q9-100m-x86-20260904",
        target_exp_text,
    ),
    ("http://127.0.0.1:10834", f"http://127.0.0.1:{target_rest_port}"),
    ("http://127.0.0.1:11917", f"http://127.0.0.1:{target_prom_port}"),
    (
        "source_commit=8780838608a9c4ef1f374b91873ad3be7f576782",
        f"source_commit={source_commit}",
    ),
    (
        "artifact_sha=09f5cd7078005bc803107e14a010263a921fb4e6307a92bb52ac9d358c503251",
        f"artifact_sha={artifact_sha}",
    ),
    ("allow_disjoint_foreign=false", "allow_disjoint_foreign=true"),
    ("target_cpuset=0-31", f"target_cpuset={target_cpuset}"),
    ("target_cpuset_mems=", f"target_cpuset_mems={target_cpuset_mems}"),
    ("export GOLDEN_HOST=x86", "export GOLDEN_HOST=kunpeng"),
    (
        "export GOLDEN_CONTAINER_FLINK_HOME=/opt/flink",
        "export GOLDEN_CONTAINER_FLINK_HOME=/opt/flink-1.16.3",
    ),
    (
        "runtime_manifest=$expdir/inputs/artifacts/opt/RUNTIME_BUNDLE.json",
        "runtime_manifest=$expdir/inputs/runtime/RUNTIME_BUNDLE.json",
    ),
    (
        "assert values['state.backend.cachekit.bp-prefetch."
        "async-chunks.enabled']=='false'",
        "assert values['state.backend.cachekit.bp-prefetch."
        "async-chunks.enabled']=='true'",
    ),
    ("'platform':'x86'", "'platform':'kunpeng'"),
):
    runner_text = replace_once(runner_text, old, new, "runner")
runner.write_text(runner_text)

compose_cpu_replacements = (
    ("cpuset: '54'", "cpuset: '54'"),
    (
        "cpuset: 38,40,42,44,46,48,50,52",
        "cpuset: 38,40,42,44,46,48,50,52",
    ),
    (
        "cpuset: 56,58,60,62,64,66,68,70",
        "cpuset: 56,58,60,62,64,66,68,70",
    ),
    ("cpuset: '72'", "cpuset: '72'"),
    ("cpuset: '74'", "cpuset: '74'"),
)
for variant, settings in variants.items():
    compose = target_exp / "variants" / variant / "docker-compose.yml"
    text = compose.read_text()
    text = text.replace(source_exp_text, target_exp_text)
    text = text.replace(source_scratch, target_scratch)
    for old, new in (
        (f"{source_rest_port}:8081", f"{target_rest_port}:8081"),
        (f"{source_prom_port}:9090", f"{target_prom_port}:9090"),
        (f"{source_push_port}:9091", f"{target_push_port}:9091"),
        *compose_cpu_replacements,
    ):
        text = replace_once(text, old, new, f"compose {variant}")
    old_mount = f"{target_exp_text}/variants/hot2/flink-conf.yaml"
    new_mount = f"{target_exp_text}/variants/{variant}/flink-conf.yaml"
    if text.count(old_mount) != 4:
        raise SystemExit(
            f"compose mount count {text.count(old_mount)} "
            f"in {compose}; expected 4"
        )
    text = text.replace(old_mount, new_mount)
    env_pattern = re.compile(
        r"(?m)^(\s*)NEXMARK_SQL_SUBMIT_TIMEOUT_SEC: '180'$"
    )
    matches = list(env_pattern.finditer(text))
    if len(matches) != 4:
        raise SystemExit(
            f"runtime gate insertion count {len(matches)} "
            f"in {compose}; expected 4"
        )
    text = env_pattern.sub(
        lambda match: match.group(0)
        + "\n"
        + match.group(1)
        + "CACHEKIT_MAP_DIRTY_OVERLAY_ENABLED: '"
        + settings["overlay"]
        + "'\n"
        + match.group(1)
        + "CACHEKIT_MAP_SNAPSHOT_MAINTENANCE_ENABLED: '"
        + settings["maintenance"]
        + "'",
        text,
    )
    compose.write_text(text)

    config = target_exp / "variants" / variant / "flink-conf.yaml"
    text = config.read_text()
    for key, old_value, new_value in (
        (
            "state.backend.cachekit.map.cache.max-entries",
            "0",
            settings["map_cache"],
        ),
        (
            "state.backend.cachekit.map.snapshot.cache.max-entries",
            "2000",
            settings["snapshot_cache"],
        ),
        (
            "state.backend.rocksdb.compression.uncompressed-hot-levels",
            "2",
            settings["hot_levels"],
        ),
    ):
        text = replace_once(
            text,
            f"{key}: {old_value}",
            f"{key}: {new_value}",
            f"config {variant}",
        )
    indexed_key = "state.backend.rocksdb.write-batch-with-index.enabled"
    if indexed_key not in text:
        text += (
            "\n# Same-artifact P10 control; indexed write batch remains disabled.\n"
            f"{indexed_key}: false\n"
        )
    config.write_text(text)

runtime_path = target_exp / "inputs/runtime/RUNTIME_BUNDLE.json"
runtime = json.loads(runtime_path.read_text())
runtime["source_commit"] = source_commit
for artifact in runtime["artifacts"]:
    artifact["path"] = artifact["path"].replace(
        source_exp_text, target_exp_text
    )
    if artifact["role"] == "cachekit_module":
        artifact["sha256"] = artifact_sha
        artifact["size_bytes"] = (
            target_exp
            / "inputs/runtime"
            / artifact["container_filename"]
        ).stat().st_size
runtime_payload = json.dumps(runtime, indent=2, sort_keys=True) + "\n"
runtime_path.write_text(runtime_payload)
(runtime_path.parent / "RUNTIME_BUNDLE.sha256").write_text(
    hashlib.sha256(runtime_payload.encode()).hexdigest()
    + "  RUNTIME_BUNDLE.json\n"
)

identity_path = target_exp / "identity.json"
identity = json.loads(identity_path.read_text())
identity.update(
    {
        "schema": "cachekit-snapshot-maintenance-p10-kunpeng-screen-v1",
        "phase": (
            "kunpeng-numa0-q9-same-artifact-"
            "incremental-exact-snapshot-screen"
        ),
        "source_commit": source_commit,
        "artifact_sha256": artifact_sha,
        "artifact_size_bytes": (
            target_exp
            / "inputs/runtime/flink-statebackend-cachekit-1.16-SNAPSHOT.jar"
        ).stat().st_size,
        "queries": ["q9"],
        "variants": list(variants),
        "primary_control": "baseline",
        "a_control": "hot2-a",
        "mechanism_control": "hot2-overlay",
        "mechanism_treatment": "hot2-maintained",
        "capacity_control": "hot2-overlay-64k",
        "treatment": "hot2-maintained-64k",
        "execution_environment": (
            "kunpeng-numa0-isolated-disjoint-container-compatible"
        ),
        "numa_binding": {
            "node": 0,
            "cpuset_mems": "0",
            "all_service_cpus": target_cpuset,
            "taskmanager1_cpus": "38,40,42,44,46,48,50,52",
            "jobmanager_cpu": "54",
            "taskmanager2_cpus": "56,58,60,62,64,66,68,70",
            "prometheus_cpu": "72",
            "pushgateway_cpu": "74",
            "runner_cpu": "76",
        },
        "claim_boundary": (
            "same Kunpeng NUMA0 physical cluster and P10 artifact; the "
            "coexistence preflight proves the selected CPUs idle and records "
            "any foreign containers; dirty-overlay and incrementally maintained "
            "exact-snapshot gates are explicit"
        ),
    }
)
identity["artifact_build"] = {
    "p9_aarch64_base_sha256": (
        "c80df5f40e3604db17638602917fd7fb813a8f3828590fcb79145768705590c3"
    ),
    "runtime_overlay_source_commit": source_commit,
    "entry_set_identical_to_p9": True,
    "non_overlay_entries_identical_to_p9": True,
}
identity["variant_config_sha256"] = {
    variant: hash_file(
        target_exp / "variants" / variant / "flink-conf.yaml"
    )
    for variant in variants
}
identity["variant_compose_sha256"] = {
    variant: hash_file(
        target_exp / "variants" / variant / "docker-compose.yml"
    )
    for variant in variants
}
identity_path.write_text(json.dumps(identity, indent=2, sort_keys=True) + "\n")


def config(path):
    result = {}
    for line in path.read_text().splitlines():
        if ":" in line and not line.lstrip().startswith("#"):
            key, value = line.split(":", 1)
            result[key.strip()] = value.strip()
    return result


configs = {
    variant: config(
        target_exp / "variants" / variant / "flink-conf.yaml"
    )
    for variant in variants
}
config_differences = {}
for left, right in (
    ("baseline", "hot2-a"),
    ("hot2-a", "hot2-overlay"),
    ("hot2-overlay", "hot2-maintained"),
    ("hot2-overlay", "hot2-overlay-64k"),
    ("hot2-overlay-64k", "hot2-maintained-64k"),
    ("hot2-maintained", "hot2-maintained-64k"),
):
    values = {
        key: {left: configs[left].get(key), right: configs[right].get(key)}
        for key in sorted(set(configs[left]) | set(configs[right]))
        if configs[left].get(key) != configs[right].get(key)
    }
    config_differences[f"{left}_vs_{right}"] = values
expected_config = {
    "baseline_vs_hot2-a": {
        "state.backend.rocksdb.compression.uncompressed-hot-levels"
    },
    "hot2-a_vs_hot2-overlay": {
        "state.backend.cachekit.map.cache.max-entries"
    },
    "hot2-overlay_vs_hot2-maintained": set(),
    "hot2-overlay_vs_hot2-overlay-64k": {
        "state.backend.cachekit.map.snapshot.cache.max-entries"
    },
    "hot2-overlay-64k_vs_hot2-maintained-64k": set(),
    "hot2-maintained_vs_hot2-maintained-64k": {
        "state.backend.cachekit.map.snapshot.cache.max-entries"
    },
}
compose_env = {}
for variant, settings in variants.items():
    compose_text = (
        target_exp / "variants" / variant / "docker-compose.yml"
    ).read_text()
    overlay_token = (
        f"CACHEKIT_MAP_DIRTY_OVERLAY_ENABLED: '{settings['overlay']}'"
    )
    maintenance_token = (
        "CACHEKIT_MAP_SNAPSHOT_MAINTENANCE_ENABLED: "
        f"'{settings['maintenance']}'"
    )
    if compose_text.count(overlay_token) != 4:
        raise SystemExit(f"invalid dirty-overlay env for {variant}")
    if compose_text.count(maintenance_token) != 4:
        raise SystemExit(f"invalid snapshot-maintenance env for {variant}")
    compose_env[variant] = {
        "dirty_overlay": settings["overlay"],
        "snapshot_maintenance": settings["maintenance"],
    }
valid_config = all(
    set(config_differences[pair]) == expected
    for pair, expected in expected_config.items()
)
audit = {
    "schema": "cachekit-p10-config-diff-v1",
    "config_differences": config_differences,
    "expected_config_difference_keys": {
        pair: sorted(expected) for pair, expected in expected_config.items()
    },
    "compose_runtime_env": compose_env,
    "valid": valid_config,
}
if not audit["valid"]:
    raise SystemExit(f"invalid config diff: {audit}")
(target_exp / "CONFIG_DIFF_AUDIT.json").write_text(
    json.dumps(audit, indent=2, sort_keys=True) + "\n"
)


def read_cpu():
    result = {}
    for line in pathlib.Path("/proc/stat").read_text().splitlines():
        fields = line.split()
        if fields and fields[0].startswith("cpu") and fields[0][3:].isdigit():
            result[int(fields[0][3:])] = tuple(map(int, fields[1:]))
    return result


selected = sorted(target_cpus)
before = read_cpu()
time.sleep(2)
after = read_cpu()


def utilization(cpu):
    delta_total = sum(after[cpu]) - sum(before[cpu])
    delta_idle = sum(after[cpu][3:5]) - sum(before[cpu][3:5])
    return 100.0 * (1.0 - delta_idle / delta_total) if delta_total else 0.0


topology = {}
for line in subprocess.check_output(
    ["lscpu", "-p=CPU,NODE"], text=True
).splitlines():
    if line.startswith("#"):
        continue
    cpu, node = map(int, line.split(",")[:2])
    topology[cpu] = node
if any(topology[cpu] != 0 for cpu in selected):
    raise SystemExit("selected CPUs are not all on NUMA0")
preflight = {
    "schema": "cachekit-p10-kunpeng-numa-coexistence-preflight-v1",
    "target_node": 0,
    "target_cpuset": target_cpuset,
    "target_cpuset_mems": target_cpuset_mems,
    "sample_seconds": 2,
    "selected_cpu_mean_utilization_percent": (
        sum(utilization(cpu) for cpu in selected) / len(selected)
    ),
    "selected_cpu_max_utilization_percent": max(
        utilization(cpu) for cpu in selected
    ),
    "selected_cpu_utilization_percent": {
        str(cpu): utilization(cpu) for cpu in selected
    },
    "foreign_containers": foreign_containers,
}
preflight["valid"] = (
    preflight["selected_cpu_mean_utilization_percent"] < 5.0
    and preflight["selected_cpu_max_utilization_percent"] < 30.0
)
if not preflight["valid"]:
    raise SystemExit(f"NUMA0 target CPUs are busy: {preflight}")
(target_exp / "COEXISTENCE_PREFLIGHT.json").write_text(
    json.dumps(preflight, indent=2, sort_keys=True) + "\n"
)

artifact_path = (
    target_exp
    / "inputs/runtime/flink-statebackend-cachekit-1.16-SNAPSHOT.jar"
)
if hash_file(artifact_path) != artifact_sha:
    raise SystemExit("staged artifact hash mismatch")
artifact_manifest_paths = (
    "inputs/runtime/flink-dist-1.16.3.jar",
    "inputs/runtime/flink-statebackend-cachekit-1.16-SNAPSHOT.jar",
    "inputs/runtime/libcachekit_native_request_plane_jni.so",
)
(target_exp / "inputs/ARTIFACTS.SHA256SUMS").write_text(
    "".join(
        f"{hash_file(target_exp / relative)}  {relative}\n"
        for relative in artifact_manifest_paths
    )
)
for path in (
    target_exp / "run_campaign.sh",
    target_exp / "audit_hot_levels.py",
    target_exp / "summarize_kunpeng.py",
):
    path.chmod(path.stat().st_mode | 0o111)
command(["bash", "-n", str(target_exp / "run_campaign.sh")])
command(
    [
        "python3",
        "-m",
        "py_compile",
        str(target_exp / "summarize_kunpeng.py"),
    ]
)
for path in (
    target_exp / "identity.json",
    target_exp / "CONFIG_DIFF_AUDIT.json",
    target_exp / "COEXISTENCE_PREFLIGHT.json",
):
    json.loads(path.read_text())
staging_audit = {
    "schema": "cachekit-p10-kunpeng-staging-audit-v1",
    "valid": True,
    "source_experiment": source_exp_text,
    "target_experiment": target_exp_text,
    "source_commit": source_commit,
    "artifact_sha256": artifact_sha,
    "run_campaign_sha256": hash_file(target_exp / "run_campaign.sh"),
    "summarizer_sha256": hash_file(target_exp / "summarize_kunpeng.py"),
    "identity_sha256": hash_file(target_exp / "identity.json"),
    "config_diff_audit_sha256": hash_file(
        target_exp / "CONFIG_DIFF_AUDIT.json"
    ),
    "coexistence_preflight_sha256": hash_file(
        target_exp / "COEXISTENCE_PREFLIGHT.json"
    ),
}
(target_exp / "STAGING_AUDIT.json").write_text(
    json.dumps(staging_audit, indent=2, sort_keys=True) + "\n"
)
target_exp.rename(final_target_exp)
print(json.dumps(staging_audit, indent=2, sort_keys=True))
'''


def main():
    script_dir = pathlib.Path(__file__).resolve().parent
    repo = subprocess.check_output(
        ["git", "-C", str(script_dir), "rev-parse", "--show-toplevel"],
        text=True,
    ).strip()
    repo = pathlib.Path(repo)
    candidate = (
        repo
        / "flink-state-backends/flink-statebackend-cachekit/target/"
        "flink-statebackend-cachekit-1.16-SNAPSHOT-p10-aarch64.jar"
    )
    summarizer = script_dir / "summarize_kunpeng.py"
    if not candidate.is_file():
        raise SystemExit(f"candidate missing: {candidate}")
    if not summarizer.is_file():
        raise SystemExit(f"summarizer missing: {summarizer}")
    if sha256(candidate) != ARTIFACT_SHA256:
        raise SystemExit("candidate hash mismatch")

    with tempfile.TemporaryDirectory(prefix="cachekit-p10-kunpeng-stage-") as tmp:
        runner = pathlib.Path(tmp) / "run_campaign.template.sh"
        run(["scp", "-q", f"{RUNNER_HOST}:{RUNNER_PATH}", str(runner)])
        if sha256(runner) != RUNNER_SHA256:
            raise SystemExit("runner template hash mismatch")
        run(["ssh", "-o", "BatchMode=yes", HOST, "mkdir", "-p", REMOTE_STAGE])
        run(
            [
                "scp",
                "-q",
                str(candidate),
                str(runner),
                str(summarizer),
                f"{HOST}:{REMOTE_STAGE}/",
            ]
        )

    remote_args = [
        SOURCE_EXPERIMENT,
        TARGET_EXPERIMENT,
        TARGET_PROJECT,
        SOURCE_SCRATCH,
        TARGET_SCRATCH,
        *SOURCE_PORTS,
        *TARGET_PORTS,
        SOURCE_COMMIT,
        ARTIFACT_SHA256,
        TARGET_CPUSET,
        TARGET_CPUSET_MEMS,
        REMOTE_STAGE,
    ]
    run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            HOST,
            "taskset",
            "-c",
            "76",
            "python3",
            "-",
            *remote_args,
        ],
        input=REMOTE_SCRIPT,
    )


if __name__ == "__main__":
    main()
