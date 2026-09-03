#!/usr/bin/env python3
"""Materialize the x86 P1 ready-gated prefetch depth-two canary."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Dict

import yaml


BASE_SOURCE_COMMIT = "7a9e568dcbd75ab4f24e7a33a4339f5cd1cf8000"
SOURCE_COMMIT = "5a9d1e656715a403afac72ee1a516876ddbfb7f1"
FROZEN_OVERLAY_COMMIT = "bbd39affde9278d44b9f78d201849bf929249d54"
OLD_EXPDIR = (
    "/home/wuql/flink-cluster/experiments/"
    "cachekit-native-stage123-javafullopt-15q-100m-r1-r3-x86-20260903"
)
OLD_SCRATCH = "/tmp/ckx86bbd39abl100"
OLD_PROJECT = "ckx86bbd39abl"
NEW_SCRATCH = "/tmp/ckx865a9p1"
NEW_PROJECT = "ckx865a9p1"

VARIANTS = {
    "control": False,
    "ready-d2": True,
}

TREATMENT_KEYS = (
    "state.backend.cachekit.bp-prefetch.ready-gated.enabled",
)

SHAPE_DIFFERENCE_KEYS = TREATMENT_KEYS

COMMON_EXPECTED = {
    "state.backend": "org.apache.flink.contrib.streaming.state.cachekit.CacheKitStateBackendFactory",
    "parallelism.default": "16",
    "taskmanager.numberOfTaskSlots": "2",
    "pipeline.object-reuse": "false",
    "state.backend.cachekit.value.cache.max-entries": "8000",
    "state.backend.cachekit.map.cache.max-entries": "0",
    "state.backend.cachekit.map.snapshot.cache.max-entries": "2000",
    "state.backend.cachekit.map.presence.cache.max-entries": "0",
    "state.backend.cachekit.bp-prefetch.enabled": "true",
    "state.backend.cachekit.bp-prefetch.distance": "64",
    "state.backend.cachekit.bp-prefetch.backpressure-gated": "false",
    "state.backend.cachekit.bp-prefetch.async.enabled": "true",
    "state.backend.cachekit.bp-prefetch.multiget.enabled": "true",
    "state.backend.cachekit.bp-prefetch.multiget.chunk-size": "64",
    "state.backend.cachekit.bp-prefetch.multiget.min-batch-size": "8",
    "state.backend.cachekit.bp-prefetch.async-chunks.enabled": "false",
    "state.backend.cachekit.bp-prefetch.async-chunks.size": "16",
    "state.backend.cachekit.bp-prefetch.head-guard-records": "0",
    "state.backend.cachekit.bp-prefetch.sliding-drain-records": "0",
    "state.backend.cachekit.bp-prefetch.cancel-on-dispatch.enabled": "false",
    "state.backend.cachekit.bp-prefetch.ready-gated.max-in-flight-batches": "2",
    "state.backend.cachekit.bp-prefetch.ready-gated.timeout-us": "5000",
    "state.backend.cachekit.mailbox-batch.enabled": "true",
    "state.backend.cachekit.local-preagg.enabled": "true",
    "state.backend.cachekit.list-state.cow": "false",
    "state.backend.cachekit.list-state.ryw": "false",
    "state.backend.cachekit.priority-queue.opt": "false",
    "table.exec.mini-batch.enabled": "false",
    "state.backend.cachekit.native.request-plane.enabled": "false",
    "state.backend.cachekit.native.value-cache.enabled": "false",
    "state.backend.cachekit.native.map-cache.enabled": "false",
    "state.backend.cachekit.native.map-snapshot.enabled": "false",
    "state.backend.cachekit.native.local-preagg.enabled": "false",
    "state.backend.cachekit.native.mailbox-batch.enabled": "false",
    "state.backend.cachekit.native.prefetch.enabled": "false",
    "state.backend.cachekit.native.map-distinct-batch-prefetch.enabled": "false",
    "state.backend.cachekit.native.fastlocal.runtime-dispatch.enabled": "false",
    "state.backend.cachekit.map.snapshot.cache.native.enabled": "false",
    "state.backend.rocksdb.memtable.arm-point.enabled": "false",
    "state.backend.rocksdb.memtable.arm-point.map-flat-authority": "false",
    "state.backend.rocksdb.memtable.arm-point.map-keyhead-point-index.enabled": "false",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_config(text: str) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for line in text.splitlines():
        if ":" in line and not line.lstrip().startswith("#"):
            key, value = line.split(":", 1)
            values[key.strip()] = value.strip()
    return values


def set_config(text: str, updates: Dict[str, str], variant: str) -> str:
    seen = set()
    output = [
        f"# Generated P1 ready-gated prefetch variant: {variant}",
        f"# Source: {SOURCE_COMMIT}",
    ]
    for line in text.splitlines():
        if ":" in line and not line.lstrip().startswith("#"):
            key = line.split(":", 1)[0].strip()
            if key in updates:
                output.append(f"{key}: {updates[key]}")
                seen.add(key)
                continue
        output.append(line)
    missing = [key for key in updates if key not in seen]
    if missing:
        output.extend(["", "# Ready-gated prefetch controls (generated)."])
        output.extend(f"{key}: {updates[key]}" for key in missing)
    return "\n".join(output) + "\n"


def rewrite_strings(value, replacements):
    if isinstance(value, str):
        for old, new in replacements:
            value = value.replace(old, new)
        return value
    if isinstance(value, list):
        return [rewrite_strings(item, replacements) for item in value]
    if isinstance(value, dict):
        return {key: rewrite_strings(item, replacements) for key, item in value.items()}
    return value


def variant_updates(enabled: bool) -> Dict[str, str]:
    return {
        "pipeline.object-reuse": "false",
        "state.backend.cachekit.bp-prefetch.multiget.min-batch-size": "8",
        "state.backend.cachekit.bp-prefetch.async-chunks.enabled": "false",
        "state.backend.cachekit.bp-prefetch.async-chunks.size": "16",
        "state.backend.cachekit.bp-prefetch.head-guard-records": "0",
        "state.backend.cachekit.bp-prefetch.sliding-drain-records": "0",
        "state.backend.cachekit.bp-prefetch.cancel-on-dispatch.enabled": "false",
        "state.backend.cachekit.bp-prefetch.ready-gated.enabled": str(enabled).lower(),
        "state.backend.cachekit.bp-prefetch.ready-gated.max-in-flight-batches": "2",
        "state.backend.cachekit.bp-prefetch.ready-gated.timeout-us": "5000",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--remote-expdir", required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    args = parser.parse_args()

    if args.output.exists():
        raise SystemExit(f"refusing to overwrite {args.output}")
    if not args.artifact.is_file():
        raise SystemExit(f"artifact missing: {args.artifact}")

    here = Path(__file__).resolve().parent
    template = here / "templates" / "x86"
    base = (template / "flink-conf.yaml").read_text()
    base_compose = yaml.safe_load((template / "docker-compose.yml").read_text())
    configs = {}

    for variant, enabled in VARIANTS.items():
        variant_dir = args.output / "variants" / variant
        variant_dir.mkdir(parents=True)
        config = set_config(base, variant_updates(enabled), variant)
        (variant_dir / "flink-conf.yaml").write_text(config)
        configs[variant] = parse_config(config)

        replacements = [
            (OLD_EXPDIR, args.remote_expdir),
            ("/variants/java/", f"/variants/{variant}/"),
            (OLD_SCRATCH, NEW_SCRATCH),
            (OLD_PROJECT, NEW_PROJECT),
            ("10588:8081", "10788:8081"),
            ("11621:9090", "11821:9090"),
            ("11622:9091", "11822:9091"),
        ]
        compose = rewrite_strings(base_compose, replacements)
        (variant_dir / "docker-compose.yml").write_text(
            yaml.safe_dump(compose, sort_keys=False)
        )

    for variant, values in configs.items():
        assert "execution.checkpointing.interval" not in values, variant
        bad = {key: (values.get(key), expected) for key, expected in COMMON_EXPECTED.items()
               if values.get(key) != expected}
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
        "platform": "x86",
        "architecture": "x86_64",
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
        "artifact_sha256": artifact_hash,
        "artifact_size_bytes": args.artifact.stat().st_size,
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
                "only_treatment": "state.backend.cachekit.bp-prefetch.ready-gated.enabled",
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
        "@PROJECT@": NEW_PROJECT,
        "@COMPOSE@": "/home/wuql/bin/docker-compose",
        "@REST@": "http://127.0.0.1:10788",
        "@PROM@": "http://127.0.0.1:11821",
        "@SCRATCH@": NEW_SCRATCH,
        "@RUNTIME_MANIFEST@": "$expdir/inputs/artifacts/opt/RUNTIME_BUNDLE.json",
        "@PLATFORM@": "x86",
        "@GOLDEN_HOST@": "x86",
        "@CONTAINER_FLINK_HOME@": "/opt/flink",
        "@CPUSET_MEMS_JSON@": (
            '{"ckx865a9p1_jobmanager_1":"0",'
            '"ckx865a9p1_taskmanager1_1":"0",'
            '"ckx865a9p1_taskmanager2_1":"1",'
            '"ckx865a9p1_prometheus_1":"1",'
            '"ckx865a9p1_pushgateway_1":"1"}'
        ),
        "@ALLOW_DISJOINT_FOREIGN@": "false",
        "@TARGET_CPUSET@": "0-31",
        "@TARGET_CPUSET_MEMS@": "",
    }
    for old, new in replacements.items():
        harness = harness.replace(old, new)
    harness_path = args.output / "run_campaign.sh"
    harness_path.write_text(harness)
    harness_path.chmod(0o755)
    print(args.output)


if __name__ == "__main__":
    main()
