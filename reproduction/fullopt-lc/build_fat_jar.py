#!/usr/bin/env python3
"""Build one CacheKit fat JAR; no patched distribution/table JARs are delivered."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import xml.etree.ElementTree as ET
import zipfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
MODULE = ROOT / 'flink-state-backends/flink-statebackend-cachekit'
FILENAME = 'flink-statebackend-cachekit-1.16-SNAPSHOT.jar'
DELIVERY_NAME = '00-cachekit-fullopt-lc.jar'
NS = {'m': 'http://maven.apache.org/POM/4.0.0'}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify(jar):
    """Require every unpacked class family, including nested classes, in the deliverable."""
    pom = ET.parse(MODULE / 'pom.xml')
    execution = next(e for e in pom.findall('.//m:execution', NS)
                     if e.findtext('m:id', namespaces=NS) == 'overlay-patched-classes')
    patterns = []
    for value in execution.findall('.//m:includes', NS):
        patterns.extend(x.strip() for x in value.text.split(',') if x.strip())
    import fnmatch
    with zipfile.ZipFile(jar) as z:
        names = z.namelist()
        if len(names) != len(set(names)) or z.testzip() is not None:
            raise RuntimeError('duplicate/corrupt JAR entries')
        classes = {}
        for pattern in patterns:
            source = list((MODULE / 'target/classes').glob(pattern))
            if not source:
                raise RuntimeError('missing compiled family: ' + pattern)
            expected = {str(p.relative_to(MODULE / 'target/classes')) for p in source}
            actual = {n for n in names if fnmatch.fnmatchcase(n, pattern)}
            if expected != actual:
                raise RuntimeError('missing/stale packaged family: ' + pattern)
            for name in actual:
                classes[name] = hashlib.sha256(z.read(name)).hexdigest()
        main = z.read('org/apache/flink/table/data/binary/BinaryStringData.class')
        if (b'cachekit.binary-string.lazy-copy.enabled' not in main
                or b'CACHEKIT_BINARY_STRING_LAZY_COPY_ENABLED' not in main
                or b'flink.table.binary-string.lazy-copy.enabled' in main):
            raise RuntimeError('wrong LC gate in packaged class')
        for prefix in ['org/apache/flink/contrib/streaming/state/cachekit/shaded/caffeine/',
                       'org/apache/flink/contrib/streaming/state/cachekit/shaded/fastutil/']:
            if not any(n.startswith(prefix) and n.endswith('.class') for n in names):
                raise RuntimeError('missing bundled dependency: ' + prefix)
    return classes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--skip-build', action='store_true', help='verify/package already built targets')
    parser.add_argument('--classpath', type=Path, help='text file containing additional runtime classpath')
    args = parser.parse_args()
    if args.output.exists():
        parser.error('output must be new')
    flags = ['-DskipTests', '-DskipITs', '-Dcheckstyle.skip', '-Drat.skip',
             '-Dspotless.check.skip=true', '-Dmaven.javadoc.skip=true']
    if not args.skip_build:
        for module, goal in [('flink-state-backends/flink-statebackend-rocksdb', 'install'),
                             ('flink-table/flink-table-common', 'install'),
                             ('flink-streaming-java', 'install'),
                             ('flink-table/flink-table-runtime', 'install'),
                             ('flink-state-backends/flink-statebackend-cachekit', 'package')]:
            subprocess.run([str(ROOT / 'mvnw'), '-pl', module] + flags + ['clean', goal],
                           cwd=ROOT, check=True)
    artifact = MODULE / 'target' / FILENAME
    classes = verify(artifact)
    cp = args.classpath
    if cp is None:
        cp = MODULE / 'target/single-jar-smoke.classpath'
        subprocess.run([str(ROOT / 'mvnw'), '-pl', str(MODULE.relative_to(ROOT)),
                        'dependency:build-classpath', '-DincludeScope=test',
                        '-Dmdep.outputFile=' + str(cp)], cwd=ROOT, check=True)
    with_cp = str(artifact) + os.pathsep + cp.read_text().strip()
    smoke_logs = {}
    for enabled in ['false', 'true']:
        done = subprocess.run(['java', '-Dcachekit.binary-string.lazy-copy.enabled=' + enabled,
                               '-cp', with_cp, str(HERE / 'SingleJarSmoke.java'), str(artifact)],
                              text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if done.returncode:
            raise RuntimeError(done.stdout)
        smoke_logs[enabled] = done.stdout
    args.output.mkdir(parents=True)
    shutil.copy2(artifact, args.output / DELIVERY_NAME)
    for mode, text in smoke_logs.items():
        (args.output / ('class-origin-' + mode + '.log')).write_text(text)
    report = {'schema': 'cachekit-single-fat-jar-v1', 'valid': True,
              'source_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
              'worktree_dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT)),
              'artifacts': [{'name': DELIVERY_NAME, 'sha256': sha(artifact)}],
              'overlay_classes': classes, 'local_class_origin_smoke': True,
              'performance_rerun': False,
              'boundary': 'One CacheKit extension JAR; existing Flink distribution/dependencies remain prerequisites. Local smoke is not a JM/TM deployment or a performance result.'}
    (args.output / 'BUILD_AUDIT.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
