#!/usr/bin/env python3
import hashlib
import json
import pathlib
import subprocess
import xml.etree.ElementTree as ET


SOURCE_COMMIT = "8780838608a9c4ef1f374b91873ad3be7f576782"
X86_SHA256 = "09f5cd7078005bc803107e14a010263a921fb4e6307a92bb52ac9d358c503251"
AARCH64_SHA256 = "ffc55699efb16639e41b35fe29eb9f00ad150c0088840d335d52d2b409f94e39"
RUNTIME_PATHS = (
    "flink-streaming-java",
    "flink-state-backends/flink-statebackend-cachekit",
    "flink-state-backends/flink-statebackend-rocksdb",
    "flink-table/flink-table-runtime",
)
P10_TESTS = {
    "testSnapshotMaintenanceKeepsKnownSingleEntryAfterUpdate",
    "testSnapshotMaintenanceUpdatesKnownSingleEntryToEmptyOnRemove",
    "testSnapshotMaintenanceInvalidatesWhenExactCapacityWouldOverflow",
}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(repo, *args):
    return subprocess.run(
        ("git", "-C", str(repo), *args),
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout.strip()


def main():
    script_dir = pathlib.Path(__file__).resolve().parent
    repo = pathlib.Path(git(script_dir, "rev-parse", "--show-toplevel"))
    module = repo / "flink-state-backends" / "flink-statebackend-cachekit"
    reports = module / "target" / "surefire-reports"
    x86_jar = module / "target" / "flink-statebackend-cachekit-1.16-SNAPSHOT-p10-x86.jar"
    arm_jar = module / "target" / "flink-statebackend-cachekit-1.16-SNAPSHOT-p10-aarch64.jar"

    head = git(repo, "rev-parse", "HEAD")
    git(repo, "merge-base", "--is-ancestor", SOURCE_COMMIT, head)
    runtime_diff = git(repo, "diff", "--name-only", SOURCE_COMMIT, head, "--", *RUNTIME_PATHS)
    if runtime_diff:
        raise SystemExit(f"runtime source differs after P10 source commit: {runtime_diff}")
    if sha256(x86_jar) != X86_SHA256 or sha256(arm_jar) != AARCH64_SHA256:
        raise SystemExit("P10 candidate JAR hash mismatch")

    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    report_hashes = {}
    map_test_names = set()
    for report in sorted(reports.glob("TEST-*.xml")):
        suite = ET.parse(report).getroot()
        for key in totals:
            totals[key] += int(float(suite.attrib.get(key, 0)))
        report_hashes[report.name] = sha256(report)
        if report.name.endswith("CachedInternalMapStateTest.xml"):
            map_test_names = {
                case.attrib["name"] for case in suite.findall("testcase")
            }

    expected_totals = {"tests": 315, "failures": 0, "errors": 0, "skipped": 17}
    if totals != expected_totals:
        raise SystemExit(f"unexpected module test totals: {totals}")
    missing = sorted(P10_TESTS - map_test_names)
    if missing:
        raise SystemExit(f"missing P10 focused tests: {missing}")
    if len(map_test_names) != 38:
        raise SystemExit(f"unexpected CachedInternalMapStateTest count: {len(map_test_names)}")

    source_file = module / "src/main/java/org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalMapState.java"
    test_file = module / "src/test/java/org/apache/flink/contrib/streaming/state/cachekit/state/CachedInternalMapStateTest.java"
    audit = {
        "schema": "cachekit-p10-correctness-audit-v1",
        "valid": True,
        "source_commit": SOURCE_COMMIT,
        "runtime_source_identical_to_source_commit": True,
        "focused_map_state_tests": {
            "tests": len(map_test_names),
            "failures": 0,
            "errors": 0,
            "required_p10_tests": sorted(P10_TESTS),
        },
        "module_tests": totals,
        "platform_skips": 17,
        "artifacts": {
            "x86_sha256": sha256(x86_jar),
            "aarch64_sha256": sha256(arm_jar),
        },
        "source_sha256": sha256(source_file),
        "test_source_sha256": sha256(test_file),
        "surefire_report_sha256": report_hashes,
    }
    json_path = script_dir / "CORRECTNESS_AUDIT.json"
    md_path = script_dir / "CORRECTNESS_AUDIT.md"
    json_path.write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n")
    md_path.write_text(
        "\n".join(
            (
                "# P10 correctness audit",
                "",
                f"- Valid: `{str(audit['valid']).lower()}`",
                f"- Source commit: `{SOURCE_COMMIT}`",
                "- Current runtime source: identical to the source commit",
                "- Focused `CachedInternalMapStateTest`: 38 tests, 0 failures, 0 errors",
                "- Module: 315 tests, 0 failures, 0 errors, 17 platform skips",
                f"- x86 JAR SHA256: `{X86_SHA256}`",
                f"- AArch64 JAR SHA256: `{AARCH64_SHA256}`",
                "",
                "The JSON companion seals every Surefire XML report plus the implementation and test-source hashes.",
            )
        )
        + "\n"
    )
    print(json.dumps(audit, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
