import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

spec = importlib.util.spec_from_file_location('fat', Path(__file__).with_name('build_fat_jar.py'))
fat = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fat)


class FatJarTests(unittest.TestCase):
    def fixture(self, root, omit_nested=False):
        stem = 'org/apache/flink/table/data/binary/BinaryStringData'
        (root / 'pom.xml').write_text('''<project xmlns="http://maven.apache.org/POM/4.0.0">
          <execution><id>overlay-patched-classes</id><includes>''' + stem + '''*.class</includes>
          </execution></project>''')
        data = b'cachekit.binary-string.lazy-copy.enabled CACHEKIT_BINARY_STRING_LAZY_COPY_ENABLED'
        for suffix in ['.class', '$Nested.class']:
            path = root / 'target/classes' / (stem + suffix)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        jar = root / 'cachekit.jar'
        with zipfile.ZipFile(jar, 'w') as z:
            z.writestr(stem + '.class', data)
            if not omit_nested:
                z.writestr(stem + '$Nested.class', data)
            for dep in ['caffeine', 'fastutil']:
                z.writestr('org/apache/flink/contrib/streaming/state/cachekit/shaded/' + dep + '/A.class', b'x')
        return jar

    def test_complete_family(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            jar = self.fixture(root)
            with patch.object(fat, 'MODULE', root):
                self.assertEqual(len(fat.verify(jar)), 2)

    def test_missing_nested_class_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            jar = self.fixture(root, omit_nested=True)
            with patch.object(fat, 'MODULE', root):
                with self.assertRaisesRegex(RuntimeError, 'missing/stale packaged family'):
                    fat.verify(jar)
