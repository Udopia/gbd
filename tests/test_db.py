#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import sys

gbdroot=os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
if gbdroot in sys.path:
    sys.path.remove(gbdroot)
sys.path.insert(0, gbdroot)
print(sys.path)

import unittest

from gbd_tool.util import eprint, make_alnum_ul
from gbd_tool.gbd_api import GBD
from gbd_tool.query_builder import GBDQuery
from gbd_tool.db import Database

class DatabaseTestCase(unittest.TestCase):

    TDB = "./test.db"
    TDBN = make_alnum_ul(os.path.basename(TDB))

    def test_create_db(self):
        os.remove(self.TDB)
        with Database([self.TDB], verbose=True) as db:
            assert(db.is_database(self.TDB))
            assert(len(db.databases()) == 2)
            eprint(db.features())
            assert(len(db.features()) == 0)
            assert(len(db.tables()) == 0)
        with Database([self.TDB], verbose=True) as db:
            assert(db.dpath(self.TDBN) == self.TDB)
            assert(db.dmain(self.TDBN))
            assert(len(db.dcontexts(self.TDBN)) == 0)
            assert(len(db.dtables(self.TDBN)) == 0)
            assert(len(db.dviews(self.TDBN)) == 0)

    def test_create_feature(self):
        os.remove(self.TDB)
        FEAT = "featA"
        NAME = self.TDBN + "." + FEAT
        with Database([self.TDB], verbose=True) as db:
            db.create_feature(FEAT)
            assert(FEAT in db.features())
            assert(NAME in db.tables())
        with Database([self.TDB], verbose=True) as db:
            assert(db.ftable(FEAT) == NAME)
            assert(db.fcolumn(FEAT) == "value")
            assert(db.fdefault(FEAT) == None)
            assert(not db.fvirtual(FEAT))
            assert(db.fcontext(FEAT) == "cnf")
            assert(db.fdatabase(FEAT) == self.TDBN)

    def test_create_unique_feature(self):
        os.remove(self.TDB)
        FEAT = "featB"
        NAME = self.TDBN + ".features"
        with Database([self.TDB], verbose=True) as db:
            db.create_feature(FEAT, "empty")
            assert(FEAT in db.features())
            assert(NAME in db.tables())
        with Database([self.TDB], verbose=True) as db:
            assert(db.ftable(FEAT) == NAME)
            assert(db.fcolumn(FEAT) == FEAT)
            assert(db.fdefault(FEAT) == "empty")
            assert(not db.fvirtual(FEAT))
            assert(db.fcontext(FEAT) == "cnf")
            assert(db.fdatabase(FEAT) == self.TDBN)

    def test_insert_values(self):
        os.remove(self.TDB)
        FEAT = "letter"
        NAME = self.TDBN + ".features"
        with Database([self.TDB], verbose=True) as db:
            db.create_feature(FEAT, "empty")
            db.insert(FEAT, "a", [1, 2, 3])
            db.insert(FEAT, "b", [4, 5, 6])
            q = GBDQuery(db)
            r = db.query(q.build_query(resolve=[FEAT]))
            eprint(r)
            r = db.query(q.build_query("{}=a".format(FEAT), resolve=[FEAT]))
            eprint(r)


if __name__ == '__main__':
    unittest.main()