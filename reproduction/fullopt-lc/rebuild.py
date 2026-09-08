#!/usr/bin/env python3
"""Default: one CacheKit fat JAR. Explicit legacy mode reconstructs archived JARs."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import zipfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
MODULES = {
    'cachekit': 'flink-state-backends/flink-statebackend-cachekit',
    'rocksdb': 'flink-state-backends/flink-statebackend-rocksdb',
    'table': 'flink-table/flink-table-common',
}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def migrate_gate(data):
    """Permit only the two requested constant-pool UTF8 name changes."""
    for old, new in [
        ('flink.table.binary-string.lazy-copy.enabled', 'cachekit.binary-string.lazy-copy.enabled'),
        ('FLINK_TABLE_BINARY_STRING_LAZY_COPY_ENABLED', 'CACHEKIT_BINARY_STRING_LAZY_COPY_ENABLED'),
    ]:
        old, new = old.encode(), new.encode()
        needle = len(old).to_bytes(2, 'big') + old
        require(data.count(needle) == 1, 'historical gate constant missing/duplicated')
        data = data.replace(needle, len(new).to_bytes(2, 'big') + new)
    return data


def overlay(reference, output, replacements):
    with zipfile.ZipFile(reference) as src:
        names = src.namelist()
        require(len(names) == len(set(names)), 'duplicate ZIP entries')
        require(set(replacements) <= set(names), 'missing reference class')
        with zipfile.ZipFile(output, 'x') as dst:
            for info in src.infolist():
                data = replacements.get(info.filename)
                if data is None:
                    data = src.read(info.filename)
                dst.writestr(copy.copy(info), data)
        with zipfile.ZipFile(output) as dst:
            require(dst.testzip() is None, 'corrupt ZIP output')
            require(dst.namelist() == names, 'ZIP entry order changed')
            require(all(dst.read(n) == replacements.get(n, src.read(n)) for n in names),
                    'runtime contents differ from verified replacements')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference-runtime', type=Path)
    parser.add_argument('--legacy-multi-jar', action='store_true',
                        help='historical archive reconstruction only; not the delivery format')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--compile', action='store_true',
                        help='compile target modules (requires project Maven dependencies)')
    args = parser.parse_args()
    if not args.legacy_multi_jar:
        command = ['python3', str(HERE / 'build_fat_jar.py'), '--output', str(args.output)]
        if not args.compile:
            command.append('--skip-build')
        subprocess.run(command, check=True)
        return
    if args.reference_runtime is None:
        parser.error('--legacy-multi-jar requires --reference-runtime')
    lock = json.loads((HERE / 'lock.json').read_text())
    require(not args.output.exists(), 'output must be a new directory')
    for name, expected in lock['runtime_sources'].items():
        require(digest((ROOT / name).read_bytes()) == expected, 'source mismatch: ' + name)
    require(digest((HERE / 'kunpeng-flink-conf.yaml').read_bytes()) ==
            lock['expected_config_sha256'], 'configuration hash mismatch')
    for name, expected in lock['reference_jars'].items():
        require(digest((args.reference_runtime / name).read_bytes()) == expected,
                'reference JAR mismatch: ' + name)
    if args.compile:
        # Ordered install is needed for the new RocksDBBatchValueReader interface.
        # No full-distribution rebuild is substituted for the frozen runtime.
        flags = ['-DskipTests', '-DskipITs', '-Dcheckstyle.skip', '-Drat.skip',
                 '-Dspotless.check.skip=true', '-Dmaven.javadoc.skip=true']
        for group in ['rocksdb', 'cachekit', 'table']:
            action = 'install' if group == 'rocksdb' else 'compile'
            subprocess.run([str(ROOT / 'mvnw'), '-pl', MODULES[group]] + flags + [action],
                           cwd=ROOT, check=True)
    classes = {}
    for group, module in MODULES.items():
        target = ROOT / module / 'target/classes'
        expected = {n.split('/', 1)[1]: h for n, h in lock['class_sha256'].items()
                    if n.startswith(group + '/')}
        actual_names = {str(p.relative_to(target)) for stem in lock['stems'][group]
                        for p in (target / stem).parent.glob(Path(stem).name + '*.class')}
        require(actual_names == set(expected), 'missing/stale compiled class family: ' + group)
        classes[group] = {}
        for name, expected_hash in expected.items():
            data = (target / name).read_bytes()
            if name == 'org/apache/flink/table/data/binary/BinaryStringData.class':
                with zipfile.ZipFile(args.reference_runtime / 'flink-table-api-java-uber-1.16.3.jar') as ref:
                    original = ref.read(name)
                require(digest(original) == expected_hash, 'historical class mismatch')
                expected_hash = digest(migrate_gate(original))
            require(digest(data) == expected_hash, 'compiled bytecode mismatch: ' + name)
            classes[group][name] = data
    args.output.mkdir(parents=True)
    audits = []
    for name in lock['reference_jars']:
        source = args.reference_runtime / name
        with zipfile.ZipFile(source) as jar:
            names = set(jar.namelist())
        selected = {n: data for group in classes.values() for n, data in group.items()
                    if n in names}
        require(bool(selected), 'no target classes in ' + name)
        output = args.output / name
        overlay(source, output, selected)
        audits.append({'name': name, 'sha256': digest(output.read_bytes()),
                       'reference_sha256': lock['reference_jars'][name],
                       'replacement_classes': len(selected),
                       'non_overlay_entries_identical': True,
                       'verified_gate_rename_only': True})
    report = {'valid': True, 'source_commit': subprocess.check_output(
        ['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'worktree_dirty': bool(subprocess.check_output(
            ['git', 'status', '--porcelain'], cwd=ROOT, text=True)),
        'runtime_source_hashes': lock['runtime_sources'],
        'verified_class_count': sum(map(len, classes.values())),
        'artifacts': audits, 'performance_rerun': False,
        'boundary': lock['boundary']}
    (args.output / 'BUILD_AUDIT.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
