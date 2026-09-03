#!/usr/bin/env bash
set -euo pipefail

expected_java_candidate_sha=58676b125fe20a5e7f9994e4531f06f8f076e1b3fc150ee1f222fabbe4e20602
expected_arm_base_sha=35092a30d3ac979477a4445c5ead455258ab8a6a5278c815ad29a4301a3a7efd
expected_arm_native_sha=6765775306c7ed00b1606de5340f0ba18ae5adc8fe242b49b336f79f96749fe2
expected_output_sha=97487e04d293f154b8856e43decf2573fabaaf35e36077ea635a1f55d0f59dfc
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

java_sha=$(sha256sum "$java_candidate" | awk '{print $1}')
arm_base_sha=$(sha256sum "$arm_base" | awk '{print $1}')
[[ $java_sha == "$expected_java_candidate_sha" ]] || {
  echo "Java candidate mismatch: $java_sha" >&2
  exit 65
}
[[ $arm_base_sha == "$expected_arm_base_sha" ]] || {
  echo "ARM base mismatch: $arm_base_sha" >&2
  exit 66
}

cp "$java_candidate" "$output"
overlay=$(mktemp -d /tmp/cachekit-arm-native-overlay.XXXXXX)
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
[[ $output_sha == "$expected_output_sha" ]] || {
  echo "ARM candidate mismatch: $output_sha" >&2
  exit 67
}
echo "candidate=$output"
echo "sha256=$output_sha"
