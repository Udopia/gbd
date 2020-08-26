import unittest
import io
import importlib.util as imp
import os.path
#from gbd_tool.gbd_hash import gbd_hash_inner as gbd_hash

class TestGBDHash(unittest.TestCase):

    def setUp(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'gbd_tool/gbd_hash.py')
        spec = imp.spec_from_file_location('gbd_hash', path)
        self.mod = imp.module_from_spec(spec)
        spec.loader.exec_module(self.mod)
        self.reference = self.mod.gbd_hash_inner(io.BytesIO(b"1 2 0 3 0"))

    def tearDown(self):
        pass

    def gbd_hash(self, byte_string):
        return self.mod.gbd_hash_inner(byte_string)
    
    def test_normalize_header(self):
        variant = self.gbd_hash(io.BytesIO(b"p cnf 2 2\n1 2 0 3 0"))
        self.assertEqual(self.reference, variant)

    def test_normalize_linebreaks(self):
        variant1 = self.gbd_hash(io.BytesIO(b"p cnf 2 2\n1 2 0\r\n 3 0"))
        variant2 = self.gbd_hash(io.BytesIO(b"\np cnf 2 2\n1 2 0\r\n 3 0\n"))
        self.assertEqual(self.reference, variant1)
        self.assertEqual(self.reference, variant2)

    def test_normalize_comments(self):
        variant = self.gbd_hash(io.BytesIO(b"c test\np cnf 2 2\n1 2 0\r\n 3 0\n"))
        variant2 = self.gbd_hash(io.BytesIO(b"c test\np cnf 2 2\n1 2 0\r\n 3 0\nc test"))
        variant3 = self.gbd_hash(io.BytesIO(b"c test\np cnf 2 2\n1 2 0\r\n 3\nc test"))
        self.assertEqual(self.reference, variant)
        self.assertEqual(self.reference, variant2)
        self.assertEqual(self.reference, variant3)

    def test_normalize_spaces(self):
        variant = self.gbd_hash(io.BytesIO(b" c test\np cnf 2 2 \n1 2 0     \r\n 3 0 \n "))
        self.assertEqual(self.reference, variant)

    def test_normalize_missing_trailing_zero(self):
        variant = self.gbd_hash(io.BytesIO(b"p cnf 2 2\n1 2 0\n3"))
        variant2 = self.gbd_hash(io.BytesIO(b"p cnf 2 2\n1 2 0\n3\n"))
        self.assertEqual(self.reference, variant)
        self.assertEqual(self.reference, variant2)


if __name__ == '__main__':
    unittest.main()