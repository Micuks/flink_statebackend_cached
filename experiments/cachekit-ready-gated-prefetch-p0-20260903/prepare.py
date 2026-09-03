#!/usr/bin/env python3
"""Materialize the x86 P0 ready-gated prefetch shape screen."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Dict

import yaml


SOURCE_COMMIT = "7a9e568dcbd75ab4f24e7a33a4339f5cd1cf8000"
FROZEN_OVERLAY_COMMIT = "bbd39affde9278d44b9f78d201849bf929249d54"
OLD_EXPDIR = (
    "/home/wuql/flink-cluster/experiments/"
    "cachekit-native-stage123-javafullopt-15q-100m-r1-r3-x86-20260903"
)
OLD_SCRATCH = "/tmp/ckx86bbd39abl100"
OLD_PROJECT = "ckx86bbd39abl"
NEW_SCRATCH = "/tmp/ckx867a9p0"
NEW_PROJECT = "ckx867a9p0"

VARIANTS = {
    "control": (False, 16, 0, 0, False),
    "c16-g8-d16": (True, 16, 8, 16, False),
    "c16-g16-d16": (True, 16, 16, 16, False),
    "c32-g8-d16": (True, 32, 8, 16, False),
    "c32-g16-d16": (True, 32, 16, 16, False),
    "c32-g16-d32": (True, 32, 16, 32, False),
}

TREATMENT_KEYS = (
    "state.backend.cachekit.bp-prefetch.async-chunks.enabled",
    "state.backend.cachekit.bp-prefetch.async-chunks.size",
    "state.backend.cachekit.bp-prefetch.head-guard-records",
    "state.backend.cachekit.bp-prefetch.sliding-drain-records",
    "state.backend.cachekit.bp-prefetch.cancel-on-dispatch.enabled",
)

SHAPE_DIFFERENCE_KEYS = TREATMENT_KEYS[:-1]

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
        f"# Generated P0 ready-gated prefetch variant: {variant}",
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


def variant_updates(shape) -> Dict[str, str]:
    enabled, chunk, guard, drain, cancel = shape
    return {
        "pipeline.object-reuse": "false",
        "state.backend.cachekit.bp-prefetch.multiget.min-batch-size": "8",
        "state.backend.cachekit.bp-prefetch.async-chunks.enabled": str(enabled).lower(),
        "state.backend.cachekit.bp-prefetch.async-chunks.size": str(chunk),
        "state.backend.cachekit.bp-prefetch.head-guard-records": str(guard),
        "state.backend.cachekit.bp-prefetch.sliding-drain-records": str(drain),
        "state.backend.cachekit.bp-prefetch.cancel-on-dispatch.enabled": str(cancel).lower(),
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

    for variant, shape in VARIANTS.items():
        variant_dir = args.output / "variants" / variant
        variant_dir.mkdir(parents=True)
        config = set_config(base, variant_updates(shape), variant)
        (variant_dir / "flink-conf.yaml").write_text(config)
        configs[variant] = parse_config(config)

        replacements = [
            (OLD_EXPDIR, args.remote_expdir),
            ("/variants/java/", f"/variants/{variant}/"),
            (OLD_SCRATCH, NEW_SCRATCH),
            (OLD_PROJECT, NEW_PROJECT),
            ("10588:8081", "10688:8081"),
            ("11621:9090", "11721:9090"),
            ("11622:9091", "11722:9091"),
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
        "schema": "cachekit-ready-gated-prefetch-p0-v1",
        "source_commit": SOURCE_COMMIT,
        "frozen_overlay_commit": FROZEN_OVERLAY_COMMIT,
        "platform": "x86",
        "architecture": "x86_64",
        "phase": "q9-shape-screen",
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
                "schema": "cachekit-ready-gated-prefetch-p0-config-diff-v1",
                "valid": True,
                "allowed_difference_keys": sorted(TREATMENT_KEYS),
                "required_shape_difference_keys": sorted(SHAPE_DIFFERENCE_KEYS),
                "actual_differences": differences,
                "unexpected_difference_keys": unexpected,
                "cancel_on_dispatch_common_value": "false",
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
    }
    for old, new in replacements.items():
        harness = harness.replace(old, new)
    harness_path = args.output / "run_campaign.sh"
    harness_path.write_text(harness)
    harness_path.chmod(0o755)
    print(args.output)


if __name__ == "__main__":
    main()
