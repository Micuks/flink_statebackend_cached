import io
import unittest
import zipfile
from zip_overlay import copy_entry


class ZipOverlayTest(unittest.TestCase):
    def test_dropped_prefix_does_not_mutate_source_offsets(self):
        original=io.BytesIO()
        with zipfile.ZipFile(original,'w') as z:
            z.writestr('drop.class',b'drop'*100)
            z.writestr('keep.txt',b'preserve')
        with zipfile.ZipFile(original) as src:
            info=src.getinfo('keep.txt'); offset=info.header_offset
            output=io.BytesIO()
            with zipfile.ZipFile(output,'w') as dst: copy_entry(src,dst,info)
            self.assertEqual(info.header_offset,offset)
            with zipfile.ZipFile(output) as dst:
                self.assertEqual(src.read('keep.txt'),dst.read('keep.txt'))
                self.assertIsNone(dst.testzip())


if __name__=='__main__':unittest.main()
