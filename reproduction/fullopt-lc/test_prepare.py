"""Offline staging test: no Docker, SSH or benchmark launch."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
import yaml

HERE = Path(__file__).resolve().parent


class PrepareTests(unittest.TestCase):
    def test_isolated_staging_and_repeat_rejection(self):
        lock = json.loads((HERE / 'lock.json').read_text())
        with tempfile.TemporaryDirectory() as d:
            base = Path(d)
            archive, built, output = [base / n for n in ['archive', 'built', 'output']]
            (archive / 'inputs/runtime').mkdir(parents=True)
            (archive / 'variants/p29p30').mkdir(parents=True)
            (archive / 'results').mkdir()
            (archive / 'results/DO_NOT_COPY').write_text('historical')
            built.mkdir()
            shutil.copy2(HERE / 'kunpeng-flink-conf.yaml', archive / 'variants/p29p30/flink-conf.yaml')
            identity = {'source_commit': lock['historical_source'], 'project': 'ckp31kp0908'}
            (archive / 'identity.json').write_text(json.dumps(identity))
            (archive / 'run_campaign.sh').write_text(
                '#!/bin/bash\nsource_commit=' + lock['historical_source'] +
                '\nexit 99 # must not run\n')
            manifest = {'source_commit': lock['historical_source'], 'artifacts': []}
            audits = []
            for name, ref in lock['reference_jars'].items():
                data = name.encode()
                digest = hashlib.sha256(data).hexdigest()
                (built / name).write_bytes(data)
                (archive / 'inputs/runtime' / name).write_bytes(b'old')
                manifest['artifacts'].append({'path': str(archive / 'inputs/runtime' / name)})
                audits.append({'name': name, 'sha256': digest, 'reference_sha256': ref,
                               'non_overlay_entries_identical': True,
                               'verified_gate_rename_only': True})
            (archive / 'inputs/runtime/RUNTIME_BUNDLE.json').write_text(json.dumps(manifest))
            (built / 'BUILD_AUDIT.json').write_text(json.dumps({
                'valid': True, 'source_commit': 'new-branch-commit',
                'verified_class_count': 43, 'runtime_source_hashes': lock['runtime_sources'],
                'artifacts': audits}))
            (archive / 'inputs/ARTIFACTS.SHA256SUMS').write_text(
                'old  inputs/runtime/flink-dist-1.16.3.jar\n')
            services = {n: {'environment': {'FLINK_TABLE_BINARY_STRING_LAZY_COPY_ENABLED': 'true'},
                            'volumes': [str(archive / 'inputs/runtime' / jar) + ':/opt/flink/lib/' + jar + ':ro'
                                       for jar in lock['reference_jars']]}
                        for n in ['jobmanager', 'taskmanager1', 'taskmanager2']}
            (archive / 'variants/p29p30/docker-compose.yml').write_text(yaml.safe_dump({'services': services}))
            command = [sys.executable, str(HERE / 'prepare_campaign.py'), '--legacy-multi-jar', '--archive', str(archive),
                       '--runtime', str(built), '--output', str(output), '--project', 'cklctest01',
                       '--port-base', '13980']
            done = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(done.returncode, 0, done.stderr)
            self.assertFalse((output / 'results').exists())
            self.assertEqual(json.loads((archive / 'identity.json').read_text()), identity)
            compose = yaml.safe_load((output / 'variants/p29p30/docker-compose.yml').read_text())
            for service in compose['services'].values():
                self.assertTrue(all(v.startswith(str(output / 'inputs/runtime')) for v in service['volumes']))
                self.assertEqual(service['environment'], {'CACHEKIT_BINARY_STRING_LAZY_COPY_ENABLED': 'true'})
            self.assertEqual(json.loads((output / 'identity.json').read_text())['source_commit'], 'new-branch-commit')
            self.assertIn('source_commit=new-branch-commit', (output / 'run_campaign.sh').read_text())
            repeated = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(repeated.returncode, 0)


if __name__ == '__main__':
    unittest.main()
