import importlib.util
from pathlib import Path
import tempfile
import unittest
import zipfile

spec = importlib.util.spec_from_file_location('rebuild', Path(__file__).with_name('rebuild.py'))
rebuild = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rebuild)


class OverlayTests(unittest.TestCase):
    def test_same_bytes_preserve_all_entries(self):
        with tempfile.TemporaryDirectory() as d:
            src, dst = Path(d) / 'src.jar', Path(d) / 'dst.jar'
            with zipfile.ZipFile(src, 'w', compression=zipfile.ZIP_DEFLATED) as z:
                z.writestr('A.class', b'original')
                z.writestr('resource', b'keep')
            rebuild.overlay(src, dst, {'A.class': b'original'})
            with zipfile.ZipFile(dst) as z:
                self.assertEqual(z.read('resource'), b'keep')

    def test_verified_replacement_is_written(self):
        with tempfile.TemporaryDirectory() as d:
            src, dst = Path(d) / 'src.jar', Path(d) / 'dst.jar'
            with zipfile.ZipFile(src, 'w') as z:
                z.writestr('A.class', b'original')
            rebuild.overlay(src, dst, {'A.class': b'changed'})
            with zipfile.ZipFile(dst) as z:
                self.assertEqual(z.read('A.class'), b'changed')

    def test_migration_rejects_unexpected_reference(self):
        with self.assertRaisesRegex(RuntimeError, 'constant missing'):
            rebuild.migrate_gate(b'not a historical class')

    def test_missing_class_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            src, dst = Path(d) / 'src.jar', Path(d) / 'dst.jar'
            with zipfile.ZipFile(src, 'w') as z:
                z.writestr('A.class', b'original')
            with self.assertRaisesRegex(RuntimeError, 'missing reference'):
                rebuild.overlay(src, dst, {'B.class': b'new'})


if __name__ == '__main__':
    unittest.main()
