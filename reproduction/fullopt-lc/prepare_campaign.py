#!/usr/bin/env python3
"""Clone the archived Kunpeng harness into a NEW campaign; do not launch Docker."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import yaml


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--archive', type=Path, required=True)
    p.add_argument('--runtime', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--project', required=True)
    p.add_argument('--port-base', type=int, required=True)
    a = p.parse_args()
    a.archive, a.runtime, a.output = [x.resolve() for x in (a.archive, a.runtime, a.output)]
    if a.output.exists() or a.output == a.archive or a.archive in a.output.parents:
        p.error('output must be a new directory outside archive')
    if not re.fullmatch(r'[a-z][a-z0-9]{5,30}', a.project) or a.project == 'ckp31kp0908':
        p.error('use a new isolated lowercase alphanumeric project')
    if not 1024 <= a.port_base <= 65533 or a.port_base in range(12978, 12983):
        p.error('choose three new nonprivileged ports')
    lock = json.loads(Path(__file__).with_name('lock.json').read_text())
    build = json.loads((a.runtime / 'BUILD_AUDIT.json').read_text())
    if not build['valid'] or build['verified_class_count'] != 43:
        p.error('runtime needs a valid rebuild audit')
    if build['runtime_source_hashes'] != lock['runtime_sources']:
        p.error('runtime source identity mismatch')
    if {x['name'] for x in build['artifacts']} != set(lock['reference_jars']):
        p.error('runtime artifact set mismatch')
    archived = json.loads((a.archive / 'identity.json').read_text())
    if archived['source_commit'] != lock['historical_source']:
        p.error('wrong reference campaign')
    config = a.archive / 'variants/p29p30/flink-conf.yaml'
    if sha(config) != lock['expected_config_sha256']:
        p.error('wrong reference configuration')
    for item in build['artifacts']:
        if (sha(a.runtime / item['name']) != item['sha256']
                or not item['all_entries_identical']
                or item['reference_sha256'] != lock['reference_jars'][item['name']]):
            p.error('rebuilt artifact changed')
    a.output.mkdir(parents=True)
    # No historical results/logs/resume markers are inherited. Existing archives
    # and external jobs are never modified or cleaned by this preparation tool.
    shutil.copytree(a.archive / 'inputs', a.output / 'inputs')
    shutil.copytree(a.archive / 'variants/p29p30', a.output / 'variants/p29p30')
    for source in a.archive.iterdir():
        if source.is_file() and source.suffix in {'.py', '.sh', '.json'}:
            shutil.copy2(source, a.output / source.name)
    oldproject = archived['project']

    def remap(text):
        text = text.replace(str(a.archive), str(a.output)).replace(oldproject, a.project)
        for old, new in zip((12980, 12981, 12982), range(a.port_base, a.port_base + 3)):
            text = text.replace(str(old), str(new))
        return text

    # Remap root harness metadata, but do not rewrite frozen measurement scripts.
    for file in a.output.iterdir():
        if file.is_file() and file.suffix in {'.py', '.sh', '.json'}:
            file.write_text(remap(file.read_text()))
    runtime = a.output / 'inputs/runtime'
    for item in build['artifacts']:
        shutil.copy2(a.runtime / item['name'], runtime / item['name'])
    manifest_path = runtime / 'RUNTIME_BUNDLE.json'
    manifest = json.loads(remap(manifest_path.read_text()))
    for item in manifest['artifacts']:
        path = runtime / Path(item['path']).name
        item.update(path=str(path), sha256=sha(path), size_bytes=path.stat().st_size)
    manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')
    manifest_path.with_suffix('.sha256').write_text(sha(manifest_path) + '  ' + manifest_path.name + '\n')
    cf = a.output / 'variants/p29p30/docker-compose.yml'
    composed = yaml.safe_load(remap(cf.read_text()))
    names = set(lock['reference_jars'])
    for service in [composed.get('x-flink-common', {})] + list(composed['services'].values()):
        mounts = []
        for mount in service.get('volumes', []):
            parts = mount.split(':')
            if len(parts) >= 2 and Path(parts[1]).name in names:
                parts[0] = str(runtime / Path(parts[1]).name)
            mounts.append(':'.join(parts))
        service['volumes'] = mounts
    cf.write_text(yaml.safe_dump(composed, sort_keys=False))
    identity_path = a.output / 'identity.json'
    identity = json.loads(identity_path.read_text())
    identity['historical_overlay_audits'] = identity.pop('overlay_audits', [])
    identity.update(reproduction_build=build, historical_reference=str(a.archive),
                    performance_rerun=False,
                    variant_compose_sha256={'p29p30': sha(cf)})
    # Historical source pin remains the byte-identical five-family runtime pin;
    # the branch build commit is separately retained in reproduction_build.
    identity_path.write_text(json.dumps(identity, indent=2) + '\n')
    sums = a.output / 'inputs/ARTIFACTS.SHA256SUMS'
    lines = []
    for line in sums.read_text().splitlines():
        _, name = line.split(None, 1)
        name = remap(name.lstrip('*'))
        file = Path(name) if Path(name).is_absolute() else a.output / name
        lines.append(sha(file) + '  ' + name)
    sums.write_text('\n'.join(lines) + '\n')
    subprocess.run(['bash', '-n', str(a.output / 'run_campaign.sh')], check=True)
    subprocess.run(['sha256sum', '-c', 'inputs/ARTIFACTS.SHA256SUMS'], cwd=a.output, check=True)
    print(json.dumps({'prepared_only': True, 'campaign': str(a.output),
                      'project': a.project, 'ports': list(range(a.port_base, a.port_base + 3)),
                      'launch_command': 'bash ' + str(a.output / 'run_campaign.sh')}))


if __name__ == '__main__':
    main()
