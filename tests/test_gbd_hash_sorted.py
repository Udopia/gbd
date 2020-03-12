import unittest
import io

import itertools
import random

from gbd_tool.gbd_hash import gbd_hash_sorted, gbd_hash_inner


class TestGBDHashSorted(unittest.TestCase):
    
    def setUp(self):
        self.clauses = [ [ 1 ], [ 3 ], [ 1, 2 ], [ 1, 4 ], [ 2, 3 ] ]
        self.reference = gbd_hash_sorted(io.BytesIO(b" 0 ".join([b" ".join(str(lit).encode('utf-8') for lit in clause) for clause in self.clauses])))

    def tearDown(self):
        pass
    
    def test_conformance(self):
        reference = gbd_hash_inner(io.BytesIO(b" 0 ".join([b" ".join([str(lit).encode('utf-8') for lit in clause]) for clause in self.clauses])))
        variant = gbd_hash_sorted(io.BytesIO(b" 0 ".join([b" ".join([str(lit).encode('utf-8') for lit in clause]) for clause in self.clauses])))
        print('\n')
        self.assertEqual(reference, variant)
    
    def test_conformance_breaks(self):
        reference = gbd_hash_inner(io.BytesIO(b" 0 ".join([b" ".join([str(lit).encode('utf-8') for lit in clause]) for clause in self.clauses])))
        variant = gbd_hash_sorted(io.BytesIO(b" 0 ".join([b" \n".join([str(lit).encode('utf-8') for lit in clause]) for clause in self.clauses])))
        print('\n')
        self.assertEqual(reference, variant)
    
    def test_conformance_comments_and_header(self):
        reference = gbd_hash_inner(io.BytesIO(b" 0 ".join([b" ".join([str(lit).encode('utf-8') for lit in clause]) for clause in self.clauses])))
        variant = gbd_hash_sorted(io.BytesIO(b"p 4 5\nc asdf\n" + b" 0 ".join([b" ".join([str(lit).encode('utf-8') for lit in clause]) for clause in self.clauses])))
        print('\n')
        self.assertEqual(reference, variant)
    
    def test_shuffle(self):
        shuffled_clauses = self.clauses.copy()
        for _ in itertools.repeat(None, 10):
            for clause in shuffled_clauses:
                random.shuffle(clause)
            random.shuffle(shuffled_clauses)
            variant = gbd_hash_sorted(io.BytesIO(b" 0 ".join([b" ".join([str(lit).encode('utf-8') for lit in clause]) for clause in shuffled_clauses])))
            self.assertEqual(self.reference, variant)


if __name__ == '__main__':
    unittest.main()