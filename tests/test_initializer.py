
import os
import unittest
import pandas as pd
import random

from gbd_core.database import Database
from gbd_core.schema import Schema
from gbd_core.api import GBD, GBDException
from gbd_init.initializer import Initializer

from tests import util

class SchemaTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.file = util.get_random_unique_filename('test', '.db')
        self.name = Schema.dbname_from_path(self.file)
        self.db = Database([self.file], verbose=False)
        return super().setUp()

    def tearDown(self) -> None:
        if os.path.exists(self.file):
            os.remove(self.file)
        return super().tearDown()

    def init_random(self, hash, path, limits):
        return [ ('random', hash, random.randint(1, 1000)) ]

    def test_init_random(self):
        api = GBD(self.file, jobs=1)
        init = Initializer(['cnf'], ['cnf'], api, self.name, [('random', 0)], self.init_random)
        init.create_features()
        self.assertTrue(api.feature_exists('random'))
        df = pd.DataFrame([(str(n), None) for n in range(100)], columns=["hash", "local"])
        init.run(df)
        df = api.query("random > 0", [], ["random"])
        self.assertEqual(len(df.index), 100)