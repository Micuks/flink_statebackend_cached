#!/usr/bin/env python3
"""Audit the RocksDB compression mode that was actually applied in a leg."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path


PATTERN = re.compile(r"Configured RocksDB compression type: ([A-Z0-9_]+)")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("leg_dir", type=Path)
    parser.add_argument("--expected", required=True)
    parser.add_argument("--require-sst", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    counts: Counter[str] = Counter()
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
            mode = match.group(1)
            counts[mode] += 1
            by_file[path.name][mode] += 1
            evidence.append(
                {
                    "file": path.name,
                    "line_number": line_number,
                    "mode": mode,
                    "line_sha256": hashlib.sha256(line.encode()).hexdigest(),
                }
            )

    inventory_path = args.leg_dir / "rocksdb-sst-inventory.tsv"
    sst_sizes = []
    if inventory_path.is_file():
        for line in inventory_path.read_text().splitlines():
            size, _, _path = line.partition("\t")
            if size:
                sst_sizes.append(int(size))

    unexpected = sorted(mode for mode in counts if mode != args.expected)
    mode_activation_pass = counts[args.expected] > 0 and not unexpected
    sst_effect_pass = bool(sst_sizes)
    activation_pass = mode_activation_pass and (
        sst_effect_pass if args.require_sst else True
    )
    result = {
        "schema": "cachekit-rocksdb-compression-activation-v1",
        "leg_dir": str(args.leg_dir),
        "expected_mode": args.expected,
        "taskmanager_log_count": len(log_paths),
        "observed_counts": dict(sorted(counts.items())),
        "observed_counts_by_file": {
            name: dict(sorted(values.items())) for name, values in sorted(by_file.items())
        },
        "unexpected_modes": unexpected,
        "mode_activation_pass": mode_activation_pass,
        "sst_inventory_required": args.require_sst,
        "sst_file_count": len(sst_sizes),
        "sst_total_bytes": sum(sst_sizes),
        "sst_effect_pass": sst_effect_pass,
        "activation_pass": activation_pass,
        "evidence": evidence,
    }
    output = args.output or args.leg_dir / "COMPRESSION_AUDIT.json"
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(output)
    if not activation_pass:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
