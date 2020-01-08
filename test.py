import unittest
import io

from gbd_tool.hashing.gbd_hash import gbd_hash_inner as gbd_hash

class TestGBDHash(unittest.TestCase):

    def __init__(self):
        self.normalized = b"1 2 0 3 0"
        self.with_header = b"p cnf 2 2\n1 2 0 3 0"
        self.linebreaks = b"p cnf 2 2\n1 2 0\r\n 3 0"
        self.linebreaks2 = b"\np cnf 2 2\n1 2 0\r\n 3 0\n"
        self.comments = b"c test\np cnf 2 2\n1 2 0\r\n 3 0\n"
        self.spaces = b" c test\np cnf 2 2 \n1 2 0     \r\n 3 0 \n "
        self.missing0 = b"p cnf 2 2\n1 2 0\n3"

    def test_gbd_hash(self):
        reference = gbd_hash(io.BytesIO(self.normalized))
        self.assertEqual(reference, io.BytesIO(self.with_header))
        self.assertEqual(reference, io.BytesIO(self.linebreaks))
        self.assertEqual(reference, io.BytesIO(self.linebreaks2))
        self.assertEqual(reference, io.BytesIO(self.comments))
        self.assertEqual(reference, io.BytesIO(self.spaces))
        self.assertEqual(reference, io.BytesIO(self.missing0))  


if __name__ == '__main__':
    unittest.main()