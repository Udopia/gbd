#!/usr/bin/python3
# -*- coding: utf-8 -*-

#import os
#import sys
#sys.path.insert(0, os.path.abspath( os.path.join(os.path.dirname(__file__), '../') ))

import unittest

#import gbd_tool
from gbd_tool.util import eprint
from gbd_tool.gbd_api import GBD

class InitTestCase(unittest.TestCase):

    def test_init_local(self):
        context = 'cnf'
        db=["/raid/gbd/meta.db"]
        eprint("Sanitizing local path entries ... ")
        feature="local" if context == 'cnf' else "{}.local".format(context)
        with GBD(db) as api:
            paths = [path[0] for path in api.query_search(group_by=feature)]
            eprint(paths)
            hashes = [hash[0] for hash in api.query_search()]
            eprint(hashes)
            feature = api.query_search(resolve=["family"])
            eprint(feature)
            values = api.query_search(hashes=[hashes[0]], resolve=["family"])[0][1]
            eprint(values)
            values = api.query_search(hashes=[hashes[0]], resolve=["local"])[0][1].split(',')
            eprint(values)
            records = api.query_search(hashes=[hashes[0]], resolve=["local", "filename"], collapse="MIN")
            eprint(records)

if __name__ == '__main__':
    unittest.main()