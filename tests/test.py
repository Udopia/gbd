import unittest
import io

from gbd_tool.gbd_hash import gbd_hash_inner as gbd_hash

class TestGBDHash(unittest.TestCase):

    def setUp(self):
        self.reference = gbd_hash(io.BytesIO(b"1 2 0 3 0"))

    def tearDown(self):
        pass
    
    def test_normalize_header(self):
        variant = gbd_hash(io.BytesIO(b"p cnf 2 2\n1 2 0 3 0"))
        self.assertEqual(self.reference, variant)

    def test_normalize_linebreaks(self):
        variant1 = gbd_hash(io.BytesIO(b"p cnf 2 2\n1 2 0\r\n 3 0"))
        variant2 = gbd_hash(io.BytesIO(b"\np cnf 2 2\n1 2 0\r\n 3 0\n"))
        self.assertEqual(self.reference, variant1)
        self.assertEqual(self.reference, variant2)

    def test_normalize_comments(self):
        variant = gbd_hash(io.BytesIO(b"c test\np cnf 2 2\n1 2 0\r\n 3 0\n"))
        self.assertEqual(self.reference, variant)

    def test_normalize_spaces(self):
        variant = gbd_hash(io.BytesIO(b" c test\np cnf 2 2 \n1 2 0     \r\n 3 0 \n "))
        self.assertEqual(self.reference, variant)

    def test_normalize_missing_trailing_zero(self):
        variant = gbd_hash(io.BytesIO(b"p cnf 2 2\n1 2 0\n3"))
        self.assertEqual(self.reference, variant)


if __name__ == '__main__':
    unittest.main()