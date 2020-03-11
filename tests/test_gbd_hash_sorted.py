import unittest
import io

from gbd_tool.gbd_hash import gbd_hash_sorted


class TestGBDHashSorted(unittest.TestCase):

    def setUp(self):
        self.reference = gbd_hash_sorted(io.BytesIO(b"1 0 3 0 1 2 0 1 4 0 2 1 0"))

    def tearDown(self):
        pass
    
    def test_permutation_inverted(self):
        variant = gbd_hash_sorted(io.BytesIO(b"2 1 0 1 4 0 1 2 0 3 0 1 0"))
        self.assertEqual(self.reference, variant)

    def test_permutation_shuffled1(self):
        variant = gbd_hash_sorted(io.BytesIO(b"1 4 0 2 1 0 1 2 0 1 0 3 0"))
        self.assertEqual(self.reference, variant)

    def test_permutation_shuffled2(self):
        variant = gbd_hash_sorted(io.BytesIO(b"1 4 0 3 0 1 2 0 1 0 2 1 0"))
        self.assertEqual(self.reference, variant)


if __name__ == '__main__':
    unittest.main()