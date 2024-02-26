
import os
import unittest
import pandas as pd
import random
import sqlite3

from gbd_core.database import Database
from gbd_core.schema import Schema
from gbd_core.api import GBD, GBDException
from gbd_init.initializer import Initializer
from gbd_core.contexts import identify
from gbd_init.feature_extractors import init_local, init_features_generic, generic_extractors

from tests import util

class InitTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.file = util.get_random_unique_filename('test', '.db')
        sqlite3.connect(self.file).close()
        self.name = Schema.dbname_from_path(self.file)
        self.db = Database([self.file], verbose=False)
        self.benchmark = "benchmark.cnf"
        self.dir = os.path.dirname(os.path.realpath(self.benchmark))
        with open(self.benchmark, 'w') as file:            
            file.write(util.get_random_formula(20))
        self.reference_hash = identify(self.benchmark)
        return super().setUp()

    def tearDown(self) -> None:
        if os.path.exists(self.file):
            os.remove(self.file)
        if os.path.exists(self.benchmark):
            os.remove(self.benchmark)
        return super().tearDown()

    def init_random(self, hash, path, limits):
        return [ ('random', hash, random.randint(1, 1000)) ]

    def test_init_random(self):
        api = GBD([self.file], verbose=False)
        rlimits = { 'jobs': 1, 'tlim': 5000, 'mlim': 2000, 'flim': 1000 }
        init = Initializer(api, rlimits, self.name, [('random', 0)], self.init_random)
        init.create_features()
        self.assertTrue(api.feature_exists('random'))
        df = pd.DataFrame([(str(n), None) for n in range(100)], columns=["hash", "local"])
        init.run(df)
        df = api.query("random > 0", [], ["random"])
        self.assertEqual(len(df.index), 100)

    def test_init_local(self):
        api = GBD([self.file], verbose=False)
        rlimits = { 'jobs': 1, 'tlim': 5000, 'mlim': 2000, 'flim': 1000 }
        init_local(api, rlimits, self.dir, self.name)
        self.assertTrue(api.feature_exists('local'))
        df = api.query("local like %benchmark.cnf", [], ["local"])
        self.assertEqual(len(df.index), 1)
        self.assertEqual(df.iloc[0]['local'], os.path.realpath(self.benchmark))
        self.assertEqual(df.iloc[0]['hash'], self.reference_hash)

    def test_init_cnf_features_generic(self):
        api = GBD([self.file], verbose=False)
        rlimits = { 'jobs': 1, 'tlim': 5000, 'mlim': 2000, 'flim': 1000 }
        init_local(api, rlimits, self.dir, self.name)
        df = api.query("local like %benchmark.cnf", [], ["local"])
        for key in generic_extractors.keys():
            if 'cnf' in generic_extractors[key]['contexts']:
                init_features_generic(key, api, rlimits, df, self.name)
                for feature in generic_extractors[key]['features']:
                    self.assertTrue(api.feature_exists(feature[0]))

    