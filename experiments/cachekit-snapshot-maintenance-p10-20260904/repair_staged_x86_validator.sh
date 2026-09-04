#!/usr/bin/env bash
set -euo pipefail

host=root@114.116.229.206
runner=/home/wuql/flink-cluster/experiments/cachekit-p10-snapshot-maintenance-q9-100m-x86-20260904/run_campaign.sh
experiment=/home/wuql/flink-cluster/experiments/cachekit-p10-snapshot-maintenance-q9-100m-x86-20260904

ssh -o BatchMode=yes "$host" python3 - "$runner" "$experiment" <<'PY'
import pathlib
import sys

runner = pathlib.Path(sys.argv[1])
experiment = pathlib.Path(sys.argv[2])
if (experiment / "CAMPAIGN_COMPLETE").exists():
    raise SystemExit("refusing to repair a completed campaign")
raw = experiment / "results" / "raw"
if raw.exists() and any(raw.iterdir()):
    raise SystemExit("refusing to repair a campaign after a leg has started")

text = runner.read_text()
assertion = (
    "assert values['state.backend.rocksdb.compression.uncompressed-hot-levels']"
    "==expected_hot"
)
assignment = (
    "expected_hot={'baseline':'0','hot2-a':'2','hot2-overlay':'2',"
    "'hot2-maintained':'2','hot2-overlay-64k':'2',"
    "'hot2-maintained-64k':'2'}[variant]"
)
if text.count(assertion) != 1 or text.count(assignment) != 1:
    raise SystemExit("unexpected expected_hot validator shape")
if text.index(assignment) > text.index(assertion):
    text = text.replace(assignment + "\n", "", 1)
    text = text.replace(assertion, assignment + "\n" + assertion, 1)

old_import = "import hashlib,json,pathlib,sys"
new_import = "import hashlib,json,pathlib,re,sys"
if text.count(old_import) == 1 and text.count(new_import) == 0:
    text = text.replace(old_import, new_import, 1)
elif text.count(old_import) != 0 or text.count(new_import) != 1:
    raise SystemExit("unexpected result-validator import shape")
runner.write_text(text)

verified = runner.read_text()
if verified.count(assertion) != 1 or verified.count(assignment) != 1:
    raise SystemExit("validator repair did not preserve unique statements")
if verified.index(assignment) > verified.index(assertion):
    raise SystemExit("expected_hot is still assigned after use")
if verified.count("import hashlib,json,pathlib,re,sys") != 1:
    raise SystemExit("result validator does not import re exactly once")
PY

ssh -o BatchMode=yes "$host" bash -n "$runner"
ssh -o BatchMode=yes "$host" python3 - "$runner" <<'PYVALIDATE'
import pathlib
import re
import sys

lines = pathlib.Path(sys.argv[1]).read_text().splitlines()
compiled = 0
index = 0
while index < len(lines):
    match = re.search(r"<<'([A-Za-z_][A-Za-z0-9_]*)'", lines[index])
    if not match or "python3" not in lines[index]:
        index += 1
        continue
    delimiter = match.group(1)
    end = index + 1
    while end < len(lines) and lines[end] != delimiter:
        end += 1
    if end == len(lines):
        raise SystemExit(f"unterminated Python heredoc at line {index + 1}")
    compile("\n".join(lines[index + 1 : end]), f"{sys.argv[1]}:{index + 2}", "exec")
    compiled += 1
    index = end + 1
if compiled == 0:
    raise SystemExit("no embedded Python validators found")
print(f"embedded_python_blocks_compiled={compiled}")
PYVALIDATE
ssh -o BatchMode=yes "$host" grep -n expected_hot "$runner"
