import unittest

from main.gbd_tool.database.gbd_hash import gbd_hash

class TestGBDHash(unittest.TestCase):

    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')


if __name__ == '__main__':
    unittest.main()