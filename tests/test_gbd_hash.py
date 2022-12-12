import unittest
import io
import importlib.util as imp
import os.path
import random
#from gbd_tool.gbd_hash import gbd_hash_inner as gbd_hash

class TestGBDHash(unittest.TestCase):

    def gbd_hash(self, string):
        return self.mod.gbd_hash_inner(io.BytesIO(string.encode('utf-8')))

    def get_random_clause(self, max_len=30):
        return ' '.join([str(random.randint(-2**31, 2**31-1)) for _ in range(random.randint(0, max_len))]) + ' 0'

    def get_random_formula(self, max_num=100):
        return '\n'.join([self.get_random_clause() for _ in range(random.randint(0, max_num))]) + '\n'

    def setUp(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'gbd_tool/gbd_hash.py')
        spec = imp.spec_from_file_location('gbd_hash', path)
        self.mod = imp.module_from_spec(spec)
        spec.loader.exec_module(self.mod)
        self.reference = self.get_random_formula()
        self.reference_hash = self.gbd_hash(self.reference)

    def tearDown(self):
        pass

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
            variant_hash = self.gbd_hash(variant)
            if self.reference_hash != variant_hash:
                print("Reference:")
                print(self.reference)
                print("Variant:")
                print(variant)
                print("Reference hash: {}".format(self.reference_hash))
                print("Variant hash: {}".format(variant_hash))
            self.assertEqual(self.reference_hash, variant_hash)
