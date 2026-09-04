#!/usr/bin/env python3
"""Audit the source-level RocksDB per-level compression policy in a benchmark leg."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path


PATTERN = re.compile(
    r"Configured RocksDB compression type: ([A-Z0-9_]+), "
    r"uncompressed hot levels: ([0-9]+), per-level policy: \[([^]]*)\]"
)
LEVEL_COUNT = 7


def expected_policy(compression_type: str, hot_levels: int) -> list[str]:
    if hot_levels == 0:
        return []
    return [
        "NO_COMPRESSION" if level < hot_levels else compression_type
        for level in range(LEVEL_COUNT)
    ]


def load_sst_sizes(path: Path) -> list[int]:
    sizes = []
    if not path.is_file():
        return sizes
    for line in path.read_text().splitlines():
        size, _, _ = line.partition("\t")
        if size:
            sizes.append(int(size))
    return sizes


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("leg_dir", type=Path)
    parser.add_argument("--expected-type", default="SNAPPY_COMPRESSION")
    parser.add_argument("--expected-hot-levels", type=int, required=True)
    parser.add_argument("--require-sst", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    policy = expected_policy(args.expected_type, args.expected_hot_levels)
    observed: Counter[str] = Counter()
    by_file: dict[str, Counter[str]] = defaultdict(Counter)
    evidence = []
    log_paths = sorted((args.leg_dir / "container-logs").glob("*taskmanager*.log"))
    for path in log_paths:
        for line_number, line in enumerate(
            path.read_text(errors="replace").splitlines(), start=1
        ):
            match = PATTERN.search(line)
            if not match:
                continue
            compression_type = match.group(1)
            hot_levels = int(match.group(2))
            levels = [value.strip() for value in match.group(3).split(",") if value.strip()]
            signature = f"{compression_type}|{hot_levels}|{','.join(levels)}"
            observed[signature] += 1
            by_file[path.name][signature] += 1
            evidence.append(
                {
                    "file": path.name,
                    "line_number": line_number,
                    "compression_type": compression_type,
                    "hot_levels": hot_levels,
                    "compression_per_level": levels,
                    "line_sha256": hashlib.sha256(line.encode()).hexdigest(),
                }
            )

    mismatches = [
        item
        for item in evidence
        if item["compression_type"] != args.expected_type
        or item["hot_levels"] != args.expected_hot_levels
        or item["compression_per_level"] != policy
    ]
    mode_activation_pass = bool(evidence) and not mismatches

    inventories = {
        name: load_sst_sizes(args.leg_dir / name)
        for name in ("rocksdb-sst-inventory.tsv", "rocksdb-sst-live-inventory.tsv")
        if (args.leg_dir / name).is_file()
    }
    selected = max(
        inventories,
        key=lambda name: (sum(inventories[name]), len(inventories[name])),
        default=None,
    )
    sst_sizes = inventories.get(selected, [])
    sst_effect_pass = bool(sst_sizes)
    activation_pass = mode_activation_pass and (
        sst_effect_pass if args.require_sst else True
    )
    result = {
        "schema": "cachekit-rocksdb-hot-level-compression-activation-v1",
        "leg_dir": str(args.leg_dir),
        "expected_compression_type": args.expected_type,
        "expected_hot_levels": args.expected_hot_levels,
        "expected_compression_per_level": policy,
        "taskmanager_log_count": len(log_paths),
        "observed_counts": dict(sorted(observed.items())),
        "observed_counts_by_file": {
            name: dict(sorted(values.items())) for name, values in sorted(by_file.items())
        },
        "mismatch_count": len(mismatches),
        "mode_activation_pass": mode_activation_pass,
        "sst_inventory_required": args.require_sst,
        "sst_inventory_selected": selected,
        "sst_file_count": len(sst_sizes),
        "sst_total_bytes": sum(sst_sizes),
        "sst_effect_pass": sst_effect_pass,
        "activation_pass": activation_pass,
        "evidence": evidence,
    }
    output = args.output or args.leg_dir / "HOT_LEVEL_COMPRESSION_AUDIT.json"
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(output)
    if not activation_pass:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
