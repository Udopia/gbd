import unittest
import sqlite3
import os

from gbd_core.schema import Schema
from gbd_core.database import Database
from gbd_core.query import GBDQuery

import tests.util as util

class QueryNonUniqueTestCase(unittest.TestCase):

    feat = "nonuniquefeature"
    feat2 = "nonuniquefeature2"
    feat3 = "numericfeature"
    val1 = "value1"
    val2 = "value2"
    hashes = [ "a", "b", "c" ]

    def setUp(self) -> None:
        self.file1 = util.get_random_unique_filename('test1', '.db')
        self.file2 = util.get_random_unique_filename('test2', '.db')
        sqlite3.connect(self.file1).close()
        sqlite3.connect(self.file2).close()
        self.dbname1 = Schema.dbname_from_path(self.file1)
        self.dbname2 = Schema.dbname_from_path(self.file2)
        self.db = Database([self.file1,self.file2], verbose=False)

        self.db.create_feature(self.feat, default_value=None, target_db=self.dbname1)
        self.db.set_values({self.feat: self.val1}, self.hashes)

        self.db.create_feature(self.feat, default_value=None, target_db=self.dbname2)
        self.db.set_values({self.feat: self.val1}, self.hashes[:1], target_db=self.dbname2)
        self.db.set_values({self.feat: self.val2}, self.hashes, target_db=self.dbname2)

        self.db.create_feature(self.feat2, default_value=None, target_db=self.dbname2)
        self.db.set_values({self.feat2: self.val2}, self.hashes)

        self.db.create_feature(self.feat3, default_value=0, target_db=self.dbname1)
        self.db.set_values({self.feat3: 1}, self.hashes[0])
        self.db.set_values({self.feat3: 10}, self.hashes[1])
        self.db.set_values({self.feat3: 100}, self.hashes[2])

        return super().setUp()

    def tearDown(self) -> None:
        if os.path.exists(self.file1):
            os.remove(self.file1)
        if os.path.exists(self.file2):
            os.remove(self.file2)
        return super().tearDown()
    
    def simple_query(self, feat, val, dbname=None):
        if dbname is None:
            return self.query("{}={}".format(feat, val))
        else:
            return self.query("{}:{}={}".format(dbname, feat, val))
    
    def query(self, query):
        q = GBDQuery(self.db, query).build_query()
        return [ hash for (hash, ) in self.db.query(q) ]

    # def dump(self):
    #     import sqlite3
    #     conn = sqlite3.connect(self.file)
    #     for line in conn.iterdump():
    #         print(line)
    #     conn.close()


    def test_feature_precedence_rules(self):
        res = self.simple_query(self.feat, self.val1)
        self.assertEqual(len(res), 3)
        res = self.simple_query(self.feat, self.val2)
        self.assertEqual(len(res), 0)
        res = self.simple_query(self.feat, self.val1, self.dbname1)
        self.assertEqual(len(res), 3)
        res = self.simple_query(self.feat, self.val2, self.dbname2)
        self.assertEqual(len(res), 3)

    def test_string_inequality(self):
        res = self.query("{} < {}".format(self.feat, self.val2))
        self.assertEqual(len(res), 3)
        res = self.query("{} > {}".format(self.feat, self.val1))
        self.assertEqual(len(res), 0)

    def test_numeric_inequality(self):
        res = self.query("{} < 2".format(self.feat3))
        self.assertEqual(len(res), 1)
        
    def test_multivalued_subselect(self):
        res = self.query("{db}:{f} != {v1} and {db}:{f} = {v2}".format(f=self.feat, v1=self.val1, v2=self.val2, db=self.dbname2))
        self.assertEqual(len(res), 2)

    def test_feature_accessible(self):
        res = self.simple_query(self.feat2, self.val2)
        self.assertEqual(len(res), 3)

    # ---- additional coverage -------------------------------------------------

    def build_and_run(self, query_str, **kwargs):
        """Build a SQL query with extra kwargs and return all raw rows."""
        q = GBDQuery(self.db, query_str).build_query(**kwargs)
        return self.db.query(q)

    def test_or_operator(self):
        # feat3 is 1:1 with values a=1, b=10, c=100
        res = self.query("{f} = 1 or {f} = 10".format(f=self.feat3))
        self.assertSetEqual(set(res), {"a", "b"})

    def test_not_operator(self):
        # "not feat3 = 1" should return b and c
        res = self.query("not {f} = 1".format(f=self.feat3))
        self.assertSetEqual(set(res), {"b", "c"})

    def test_empty_query_returns_all_hashes(self):
        res = self.query("")
        self.assertSetEqual(set(res), {"a", "b", "c"})

    def test_hash_filter_restricts_results(self):
        q = GBDQuery(self.db, "").build_query(hashes=["a"])
        res = [h for (h,) in self.db.query(q)]
        self.assertEqual(res, ["a"])

    def test_hash_filter_with_condition(self):
        # Filter to hashes ["a", "b"] AND feat3 > 5 → only "b" (feat3=10)
        q = GBDQuery(self.db, "{f} > 5".format(f=self.feat3)).build_query(hashes=["a", "b"])
        res = [h for (h,) in self.db.query(q)]
        self.assertEqual(res, ["b"])

    def test_numeric_eq_1to1(self):
        res = self.query("{f} = 10".format(f=self.feat3))
        self.assertEqual(res, ["b"])

    def test_collapse_none_returns_multiple_rows_for_multivalued_hash(self):
        # In db2, hash "a" has both val1 and val2; collapse=None must return 2 rows for "a"
        rows = self.build_and_run("", resolve=["{}:{}".format(self.dbname2, self.feat)], collapse=None)
        hash_a_rows = [r for r in rows if r[0] == "a"]
        self.assertEqual(len(hash_a_rows), 2)

    def test_collapse_group_concat_returns_one_row_per_hash(self):
        rows = self.build_and_run("", resolve=["{}:{}".format(self.dbname2, self.feat)], collapse="group_concat")
        # One aggregated row per hash
        self.assertEqual(len(rows), 3)
        # hash "a" row should contain both values concatenated
        hash_a_row = next(r for r in rows if r[0] == "a")
        self.assertIn(self.val1, hash_a_row[1])
        self.assertIn(self.val2, hash_a_row[1])


class LikeQueryTestCase(unittest.TestCase):
    """Tests for 'like' / 'unlike' query operators on a 1:n (multi-valued) feature.

    Each test also prints the generated SQL so it is easy to compare
    against what the CLI actually sends when the query "stops working".

    setUp creates both 'filename' and 'local' features so that the scenario
        gbd get "filename like xorshift%" -r local
    is reproduced exactly (including the LEFT JOIN on 'local').
    """

    hashes = ["hash_xorshift", "hash_scheduling", "hash_planning"]
    filenames = ["xorshift.cnf", "scheduling.cnf", "planning.cnf"]
    locals_ = ["/data/xorshift.cnf", "/data/scheduling.cnf", "/data/planning.cnf"]

    def setUp(self) -> None:
        self.file = util.get_random_unique_filename("testlike", ".db")
        sqlite3.connect(self.file).close()
        self.dbname = Schema.dbname_from_path(self.file)
        self.db = Database([self.file], verbose=False)
        self.db.create_feature("filename", default_value=None, target_db=self.dbname)
        self.db.create_feature("local", default_value=None, target_db=self.dbname)
        for h, fn, loc in zip(self.hashes, self.filenames, self.locals_):
            self.db.set_values({"filename": fn}, [h])
            self.db.set_values({"local": loc}, [h])
        return super().setUp()

    def tearDown(self) -> None:
        if os.path.exists(self.file):
            os.remove(self.file)
        return super().tearDown()

    def query(self, query_str, resolve=[], collapse=None):
        q = GBDQuery(self.db, query_str).build_query(resolve=resolve, collapse=collapse)
        return self.db.query(q)

    # -- prefix match: filename like xorshift%  (exactly what the user reported) --

    def test_like_prefix(self):
        res = [h for (h,) in self.query("filename like xorshift%")]
        self.assertEqual(len(res), 1, "prefix like should return exactly one match")
        self.assertIn("hash_xorshift", res)

    def test_unlike_prefix(self):
        res = [h for (h,) in self.query("filename unlike xorshift%")]
        self.assertEqual(len(res), 2, "prefix unlike should exclude the xorshift hash")
        self.assertNotIn("hash_xorshift", res)

    # -- suffix match: filename like %cnf --

    def test_like_suffix(self):
        res = [h for (h,) in self.query("filename like %cnf")]
        self.assertEqual(len(res), 3, "suffix like %%cnf should match all three filenames")

    # -- infix match: filename like %scheduling% --

    def test_like_infix(self):
        res = [h for (h,) in self.query("filename like %scheduling%")]
        self.assertEqual(len(res), 1, "infix like should return exactly one match")
        self.assertIn("hash_scheduling", res)

    # -- combined: like inside an AND expression --

    def test_like_combined_with_and(self):
        res = [h for (h,) in self.query("filename like xorshift% and filename like %shift%")]
        self.assertEqual(len(res), 1)
        self.assertIn("hash_xorshift", res)

    # -- exact match via like (no wildcards) --

    def test_like_exact(self):
        res = [h for (h,) in self.query("filename like planning.cnf")]
        self.assertEqual(len(res), 1, "like without wildcards should behave like equality")
        self.assertIn("hash_planning", res)

    # -- no results expected --

    def test_like_no_match(self):
        res = self.query("filename like doesnotexist%")
        self.assertEqual(len(res), 0, "like with no matching value should return empty result")

    # -- mirror of CLI: gbd get "filename like xorshift%" -r local --
    # collapse=group_concat + resolve=["local"] is exactly what the CLI uses.
    # If this passes but the CLI still returns all benchmarks, the bug is
    # outside the query builder (shell quoting, missing feature in DB, etc.).

    def test_like_prefix_with_resolve_and_collapse(self):
        rows = self.query("filename like xorshift%", resolve=["local"], collapse="group_concat")
        hashes = [h for (h, _) in rows]
        locals_ = [loc for (_, loc) in rows]
        self.assertEqual(len(hashes), 1, "should return exactly one row")
        self.assertIn("hash_xorshift", hashes)
        self.assertIn("/data/xorshift.cnf", locals_)

    def test_like_no_match_with_resolve_and_collapse(self):
        rows = self.query("filename like doesnotexist%", resolve=["local"], collapse="group_concat")
        self.assertEqual(len(rows), 0)