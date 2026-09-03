#!/usr/bin/env python3
"""Materialize the Kunpeng P1 ready-gated prefetch depth-two canary."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

import yaml

from prepare import (
    BASE_SOURCE_COMMIT,
    COMMON_EXPECTED,
    FROZEN_OVERLAY_COMMIT,
    SHAPE_DIFFERENCE_KEYS,
    SOURCE_COMMIT,
    TREATMENT_KEYS,
    VARIANTS,
    parse_config,
    rewrite_strings,
    set_config,
    sha256,
    variant_updates,
)


SOURCE_EXPDIR = (
    "/home/wuql/flink-cluster/experiments/"
    "cachekit-native-stage123-javafullopt-15q-100m-r1-r3-kunpeng-20260903"
)
SOURCE_SCRATCH = "/tmp/ckkpbbd39abl100"
TARGET_SCRATCH = "/tmp/ckkp5a9p1"
SOURCE_PROJECT = "ckkpbbd39abl"
TARGET_PROJECT = "ckkp5a9p1"
EXPECTED_BASE_CONFIG_SHA256 = (
    "8b6b83826aa849b402d24e88c34ed60e9160890cbf461a5f20678a4bc5f78388"
)
EXPECTED_BASE_COMPOSE_SHA256 = (
    "c6c96ca9f485292637d22123aebc1cffb06610586873c06f5d0f7b181e9f15a7"
)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--remote-expdir", required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--base-config", type=Path, required=True)
    parser.add_argument("--base-compose", type=Path, required=True)
    args = parser.parse_args()

    if args.output.exists():
        raise SystemExit(f"refusing to overwrite {args.output}")
    for path in (args.artifact, args.base_config, args.base_compose):
        if not path.is_file():
            raise SystemExit(f"input missing: {path}")
    base_config_hash = file_sha256(args.base_config)
    base_compose_hash = file_sha256(args.base_compose)
    if base_config_hash != EXPECTED_BASE_CONFIG_SHA256:
        raise SystemExit(f"base config mismatch: {base_config_hash}")
    if base_compose_hash != EXPECTED_BASE_COMPOSE_SHA256:
        raise SystemExit(f"base compose mismatch: {base_compose_hash}")

    here = Path(__file__).resolve().parent
    base_config = args.base_config.read_text()
    base_compose = yaml.safe_load(args.base_compose.read_text())
    configs = {}

    for variant, enabled in VARIANTS.items():
        variant_dir = args.output / "variants" / variant
        variant_dir.mkdir(parents=True)
        config = set_config(base_config, variant_updates(enabled), variant)
        (variant_dir / "flink-conf.yaml").write_text(config)
        configs[variant] = parse_config(config)

        replacements = [
            (SOURCE_EXPDIR, args.remote_expdir),
            ("/variants/java/", f"/variants/{variant}/"),
            (SOURCE_SCRATCH, TARGET_SCRATCH),
            (SOURCE_PROJECT, TARGET_PROJECT),
            ("118,120,122,124,126,128,130,132", "38,40,42,44,46,48,50,52"),
            ("136,138,140,142,144,146,148,150", "56,58,60,62,64,66,68,70"),
            ("134", "54"),
            ("152", "72"),
            ("154", "74"),
            ("10088:8081", "10789:8081"),
            ("11121:9090", "11823:9090"),
            ("11122:9091", "11824:9091"),
        ]
        compose = rewrite_strings(base_compose, replacements)
        for service in compose["services"].values():
            assert "cpuset" in service
        rendered = yaml.safe_dump(compose, sort_keys=False)
        for stale in (SOURCE_EXPDIR, SOURCE_SCRATCH, SOURCE_PROJECT):
            assert stale not in rendered, stale
        (variant_dir / "docker-compose.yml").write_text(rendered)

    for variant, values in configs.items():
        assert "execution.checkpointing.interval" not in values, variant
        bad = {
            key: (values.get(key), expected)
            for key, expected in COMMON_EXPECTED.items()
            if values.get(key) != expected
        }
        assert not bad, (variant, bad)

    keys = set().union(*(set(values) for values in configs.values()))
    differences = {
        key: {variant: configs[variant].get(key) for variant in VARIANTS}
        for key in sorted(keys)
        if len({configs[variant].get(key) for variant in VARIANTS}) > 1
    }
    unexpected = sorted(set(differences) - set(TREATMENT_KEYS))
    assert not unexpected, unexpected
    assert set(differences) == set(SHAPE_DIFFERENCE_KEYS), differences

    artifact_hash = sha256(args.artifact)
    identity = {
        "schema": "cachekit-ready-gated-prefetch-p1-v1",
        "source_commit": SOURCE_COMMIT,
        "base_source_commit": BASE_SOURCE_COMMIT,
        "frozen_overlay_commit": FROZEN_OVERLAY_COMMIT,
        "platform": "kunpeng",
        "architecture": "aarch64",
        "phase": "q9-ready-depth-two-canary",
        "events": 100_000_000,
        "queries": ["q9"],
        "rounds": [1],
        "variants": list(VARIANTS),
        "taskmanagers": 8,
        "slots": 16,
        "checkpointing": "disabled by absence of execution.checkpointing.interval",
        "object_reuse": False,
        "primary_control": "control",
        "execution_environment": "numa-isolated-colocated-quick-validation",
        "numa_binding": {
            "node": 0,
            "node_cpulist": "0-79",
            "allocated_cpus": (
                "38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74"
            ),
            "container_cpuset_mems": "0",
        },
        "claim_boundary": (
            "same-host paired quick validation under disjoint NUMA CPU binding; "
            "not idle-host final performance evidence"
        ),
        "artifact_sha256": artifact_hash,
        "artifact_size_bytes": args.artifact.stat().st_size,
        "arm_native_origin": {
            "source_experiment": SOURCE_EXPDIR,
            "source_artifact_sha256": (
                "35092a30d3ac979477a4445c5ead455258ab8a6a5278c815ad29a4301a3a7efd"
            ),
            "native_entry": "META-INF/native/libcachekit_snapshot_jni.so",
            "native_entry_sha256": (
                "6765775306c7ed00b1606de5340f0ba18ae5adc8fe242b49b336f79f96749fe2"
            ),
        },
        "template_provenance": {
            "source_experiment": SOURCE_EXPDIR,
            "base_config_sha256": base_config_hash,
            "base_compose_sha256": base_compose_hash,
        },
        "variant_config_sha256": {
            variant: sha256(args.output / "variants" / variant / "flink-conf.yaml")
            for variant in VARIANTS
        },
    }
    (args.output / "identity.json").write_text(
        json.dumps(identity, indent=2, sort_keys=True) + "\n"
    )
    (args.output / "CONFIG_DIFF_AUDIT.json").write_text(
        json.dumps(
            {
                "schema": "cachekit-ready-gated-prefetch-p1-config-diff-v1",
                "valid": True,
                "allowed_difference_keys": sorted(TREATMENT_KEYS),
                "required_shape_difference_keys": sorted(SHAPE_DIFFERENCE_KEYS),
                "actual_differences": differences,
                "unexpected_difference_keys": unexpected,
                "only_treatment": (
                    "state.backend.cachekit.bp-prefetch.ready-gated.enabled"
                ),
                "common_invariants": COMMON_EXPECTED,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )

    harness = (here / "run_campaign.sh.in").read_text()
    replacements = {
        "@EXPDIR@": args.remote_expdir,
        "@ARTIFACT_SHA@": artifact_hash,
        "@PROJECT@": TARGET_PROJECT,
        "@COMPOSE@": "/home/wuql/bin/docker-compose",
        "@REST@": "http://127.0.0.1:10789",
        "@PROM@": "http://127.0.0.1:11823",
        "@SCRATCH@": TARGET_SCRATCH,
        "@RUNTIME_MANIFEST@": "$expdir/inputs/runtime/RUNTIME_BUNDLE.json",
        "@PLATFORM@": "kunpeng",
        "@GOLDEN_HOST@": "kunpeng",
        "@CONTAINER_FLINK_HOME@": "/opt/flink-1.16.3",
        "@CPUSET_MEMS_JSON@": (
            '{"ckkp5a9p1_jobmanager_1":"0",'
            '"ckkp5a9p1_taskmanager1_1":"0",'
            '"ckkp5a9p1_taskmanager2_1":"0",'
            '"ckkp5a9p1_prometheus_1":"0",'
            '"ckkp5a9p1_pushgateway_1":"0"}'
        ),
        "@ALLOW_DISJOINT_FOREIGN@": "true",
        "@TARGET_CPUSET@": "38,40,42,44,46,48,50,52,54,56,58,60,62,64,66,68,70,72,74",
        "@TARGET_CPUSET_MEMS@": "0",
    }
    for old, new in replacements.items():
        harness = harness.replace(old, new)
    assert not re.search(r"@[A-Z_]+@", harness), "unresolved harness token"
    harness_path = args.output / "run_campaign.sh"
    harness_path.write_text(harness)
    harness_path.chmod(0o755)
    print(args.output)


if __name__ == "__main__":
    main()
