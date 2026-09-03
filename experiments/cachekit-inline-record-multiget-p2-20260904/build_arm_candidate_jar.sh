#!/usr/bin/env bash
set -euo pipefail

expected_source=a7aa558791becb34164b27c787ab6cffdebf0dc2
expected_arm_base_sha=97487e04d293f154b8856e43decf2573fabaaf35e36077ea635a1f55d0f59dfc
expected_arm_native_sha=6765775306c7ed00b1606de5340f0ba18ae5adc8fe242b49b336f79f96749fe2
native_entry=META-INF/native/libcachekit_snapshot_jni.so

usage() {
  echo "usage: $0 JAVA_CANDIDATE_JAR ARM_BASE_JAR OUTPUT_JAR" >&2
  exit 64
}

[[ $# -eq 3 ]] || usage
java_candidate=$1
arm_base=$2
output=$3
[[ -f $java_candidate && -f $arm_base && ! -e $output ]] || usage

# The native entry is extracted after changing into a temporary directory.  Resolve all
# caller-provided paths first so relative paths remain valid throughout the build.
java_candidate=$(realpath "$java_candidate")
arm_base=$(realpath "$arm_base")
output=$(realpath -m "$output")

repo=$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)
[[ $(git -C "$repo" rev-parse "$expected_source") == "$expected_source" ]] || exit 65
arm_base_sha=$(sha256sum "$arm_base" | awk '{print $1}')
[[ $arm_base_sha == "$expected_arm_base_sha" ]] || {
  echo "ARM base mismatch: $arm_base_sha" >&2
  exit 66
}

cp "$java_candidate" "$output"
overlay=$(mktemp -d /tmp/cachekit-p2-arm-native.XXXXXX)
cleanup() { rm -rf -- "$overlay"; }
trap cleanup EXIT
(cd "$overlay" && unzip -q "$arm_base" "$native_entry")
(cd "$overlay" && jar uf "$output" "$native_entry")
unzip -t "$output" >/dev/null

python3 - "$java_candidate" "$arm_base" "$output" \
  "$native_entry" "$expected_arm_native_sha" <<'PY'
import hashlib
import sys
import zipfile

java_candidate, arm_base, output, native_entry, expected_native = sys.argv[1:]


def entries(path):
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        assert len(names) == len(set(names)), f"duplicate ZIP entries in {path}"
        return {
            name: hashlib.sha256(archive.read(name)).hexdigest()
            for name in names
            if not name.endswith("/")
        }


java_entries = entries(java_candidate)
arm_entries = entries(arm_base)
output_entries = entries(output)
assert set(java_entries) == set(output_entries)
for name, digest in java_entries.items():
    if name != native_entry:
        assert output_entries[name] == digest, name
assert output_entries[native_entry] == arm_entries[native_entry]
assert output_entries[native_entry] == expected_native
with zipfile.ZipFile(output) as archive:
    native = archive.read(native_entry)
assert native[:4] == b"\x7fELF"
assert native[18:20] == bytes((183, 0)), "embedded JNI is not AArch64"
print(f"validated_entries={len(output_entries)}")
print(f"arm_native_sha256={output_entries[native_entry]}")
PY

output_sha=$(sha256sum "$output" | awk '{print $1}')
manifest=${output}.BUILD.txt
{
  echo "source_commit=$expected_source"
  echo "java_candidate_sha256=$(sha256sum "$java_candidate" | awk '{print $1}')"
  echo "arm_base_sha256=$arm_base_sha"
  echo "embedded_arm_native_sha256=$expected_arm_native_sha"
  echo "candidate_sha256=$output_sha"
  echo "candidate_path=$output"
} >"$manifest"
echo "candidate=$output"
echo "sha256=$output_sha"
echo "manifest=$manifest"
