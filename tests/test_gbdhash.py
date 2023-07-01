import unittest
import random
import os

from gbd_core.contexts import identify
from tests import util


class TestGBDHash(unittest.TestCase):

    def setUp(self):
        self.reference = util.get_random_formula()
        self.ref_file = "reference.cnf"
        with open(self.ref_file, 'w') as ref:
            ref.write(self.reference)
        self.reference_hash = identify(self.ref_file)

    def tearDown(self):
        if self.currentResult.wasSuccessful():
            os.remove(self.ref_file)

    def run(self, result=None):
        self.currentResult = result
        unittest.TestCase.run(self, result)

    def get_random_character(self):
        c = chr(random.randint(0, 255))
        return c if not c.isspace() else ' '

    def get_random_string(self, min_length=0, max_length=20):
        return ''.join([self.get_random_character() for _ in range(random.randint(min_length, max_length))])

    def get_random_whitespace_character(self):
        r = random.random()
        return '\t' if r < 0.25 else '\r' if r < 0.5 else '\n' if r < 0.75 else ' '

    def get_random_whitespace(self, min_length=0, max_length=3):
        return ''.join([self.get_random_whitespace_character() for _ in range(random.randint(min_length, max_length))])
    
    def get_random_header(self, p=0.5):
        return "p cnf {} {}\n".format(random.randint(1, 100), random.randint(1, 100)) if random.random() < p else ""

    def get_random_comment(self, p=0.5):
        return "c {}\n".format(self.get_random_string()) if random.random() < p else ""

    def test_randomized_variants(self):
        for _ in range(100):
            variant = self.get_random_whitespace()
            variant += self.get_random_comment()
            variant += self.get_random_header()
            variant += self.get_random_whitespace()
            for c in self.reference:
                if c.isspace():
                    variant += self.get_random_whitespace()
                variant += c
                if c.isspace():
                    variant += self.get_random_whitespace()
            variant += self.get_random_whitespace()

            var_file = "variant.cnf"
            with open(var_file, 'w') as f:
                f.write(variant)
            variant_hash = identify(var_file)
            if self.reference_hash == variant_hash:
                os.remove(var_file)

            self.assertEqual(self.reference_hash, variant_hash)
