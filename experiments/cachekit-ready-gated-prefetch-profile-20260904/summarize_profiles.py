#!/usr/bin/env python3
"""Aggregate control/candidate async-profiler collapsed stacks."""

import argparse
import collections
import json
import pathlib


def read_collapsed(paths):
    total = 0
    leaf = collections.Counter()
    inclusive = collections.Counter()
    for path in paths:
        for line in path.read_text(errors="replace").splitlines():
            stack, separator, raw_weight = line.rpartition(" ")
            if not separator:
                continue
            try:
                weight = int(raw_weight)
            except ValueError:
                continue
            frames = [frame for frame in stack.split(";") if frame]
            if not frames or weight <= 0:
                continue
            total += weight
            leaf[frames[-1]] += weight
            for frame in set(frames):
                inclusive[frame] += weight
    return total, leaf, inclusive


def classify_thread(frame):
    if frame.startswith("[Rank["):
        return "rank"
    if "cachekit-bp-prefetch" in frame:
        return "prefetch-worker"
    if frame.startswith("[Source:"):
        return "source"
    if frame.startswith("[Join["):
        return "join"
    if frame.startswith("[GC") or frame.startswith("[G1"):
        return "gc"
    return "other"


def read_scopes(paths):
    scopes = collections.defaultdict(
        lambda: {
            "total": 0,
            "leaf": collections.Counter(),
            "inclusive": collections.Counter(),
        }
    )
    for path in paths:
        for line in path.read_text(errors="replace").splitlines():
            stack, separator, raw_weight = line.rpartition(" ")
            if not separator:
                continue
            try:
                weight = int(raw_weight)
            except ValueError:
                continue
            frames = [frame for frame in stack.split(";") if frame]
            if not frames or weight <= 0:
                continue
            role = classify_thread(frames[0])
            scope = scopes[role]
            scope["total"] += weight
            scope["leaf"][frames[-1]] += weight
            for frame in set(frames):
                scope["inclusive"][frame] += weight
    return scopes


def ranked(counter, total, limit=50):
    return [
        {"frame": frame, "weight": weight, "share_pct": 100.0 * weight / total}
        for frame, weight in counter.most_common(limit)
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("profile_root", type=pathlib.Path)
    parser.add_argument("--output-prefix", type=pathlib.Path, required=True)
    args = parser.parse_args()

    variants = {
        "control": "001-r1-q9-control",
        "ready-d2": "002-r1-q9-ready-d2",
    }
    result = {"schema": "cachekit-ready-gated-profile-summary-v1", "events": {}}
    for event in ("cpu", "alloc"):
        aggregates = {}
        for variant, leg in variants.items():
            paths = sorted((args.profile_root / leg).glob(f"*/{event}.collapsed"))
            if len(paths) != 2:
                raise SystemExit(f"expected two {event} profiles for {leg}, got {paths}")
            total, leaf, inclusive = read_collapsed(paths)
            if total <= 0:
                raise SystemExit(f"empty aggregate for {leg}/{event}")
            aggregates[variant] = (total, leaf, inclusive, paths)

        frames = set(aggregates["control"][2]) | set(aggregates["ready-d2"][2])
        deltas = []
        control_total, _, control_inc, _ = aggregates["control"]
        candidate_total, _, candidate_inc, _ = aggregates["ready-d2"]
        for frame in frames:
            control_share = 100.0 * control_inc[frame] / control_total
            candidate_share = 100.0 * candidate_inc[frame] / candidate_total
            deltas.append(
                {
                    "frame": frame,
                    "control_share_pct": control_share,
                    "candidate_share_pct": candidate_share,
                    "delta_percentage_points": candidate_share - control_share,
                }
            )
        deltas.sort(key=lambda row: abs(row["delta_percentage_points"]), reverse=True)
        result["events"][event] = {
            variant: {
                "total_weight": total,
                "files": [str(path) for path in paths],
                "top_leaf": ranked(leaf, total),
                "top_inclusive": ranked(inclusive, total),
                "thread_scopes": {
                    role: {
                        "total_weight": scope["total"],
                        "share_of_all_pct": 100.0 * scope["total"] / total,
                        "top_leaf": ranked(scope["leaf"], scope["total"], 100),
                        "top_inclusive": ranked(
                            scope["inclusive"], scope["total"], 100
                        ),
                    }
                    for role, scope in sorted(read_scopes(paths).items())
                    if scope["total"] > 0
                },
            }
            for variant, (total, leaf, inclusive, paths) in aggregates.items()
        }
        result["events"][event]["largest_inclusive_share_deltas"] = deltas[:80]

    output_json = args.output_prefix.with_suffix(".json")
    output_md = args.output_prefix.with_suffix(".md")
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")

    lines = ["# Ready-gated q9 async-profiler summary", ""]
    for event in ("cpu", "alloc"):
        lines.extend([f"## {event}", ""])
        for variant in ("control", "ready-d2"):
            lines.extend(
                [
                    f"### {variant} top inclusive frames",
                    "",
                    "| Frame | Share |",
                    "| --- | ---: |",
                ]
            )
            for row in result["events"][event][variant]["top_inclusive"][:25]:
                lines.append(f"| `{row['frame']}` | {row['share_pct']:.2f}% |")
            lines.append("")
            for role in ("rank", "prefetch-worker"):
                scope = result["events"][event][variant]["thread_scopes"].get(role)
                if not scope:
                    continue
                lines.extend(
                    [
                        f"### {variant} {role} scope",
                        "",
                        f"Share of all samples: {scope['share_of_all_pct']:.2f}%.",
                        "",
                        "| Frame | Share within scope |",
                        "| --- | ---: |",
                    ]
                )
                for row in scope["top_inclusive"][:30]:
                    lines.append(f"| `{row['frame']}` | {row['share_pct']:.2f}% |")
                lines.append("")
        lines.extend(
            [
                "### Largest candidate-minus-control inclusive deltas",
                "",
                "| Frame | Control | ready-d2 | Delta |",
                "| --- | ---: | ---: | ---: |",
            ]
        )
        for row in result["events"][event]["largest_inclusive_share_deltas"][:40]:
            lines.append(
                f"| `{row['frame']}` | {row['control_share_pct']:.2f}% | "
                f"{row['candidate_share_pct']:.2f}% | "
                f"{row['delta_percentage_points']:+.2f} pp |"
            )
        lines.append("")
    output_md.write_text("\n".join(lines) + "\n")
    print(output_json)
    print(output_md)


if __name__ == "__main__":
    main()
