
# MIT License

# Copyright (c) 2023 Markus Iser, Karlsruhe Institute of Technology (KIT)

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.


import os
from functools import reduce

from gbd_core import contexts
from gbd_core.api import GBD, GBDException
from gbd_core import util

from gbd_core.contexts import identify
from gbd_init.initializer import Initializer, InitializerException

import gbdc


# Transform SAT Problem to k-Independent Set Problem
def kis_filename(path):
    kispath = reduce(lambda path, suffix: path[:-len(suffix)] if path.endswith(suffix) else path, contexts.suffix_list('cnf'), path)
    return kispath + '.kis'

def cnf2kis(hash, path, limits):
    kispath = kis_filename(path)
    util.eprint('Transforming {} to k-ISP {}'.format(path, kispath))
    try:
        result = gbdc.cnf2kis(path, kispath, 2**32, 2**32, limits['tlim'], limits['mlim'], limits['flim'])
        if "local" in result:
            kishash = result['hash']
            return [ ('local', kishash, result['local']), ('to_cnf', kishash, hash),
                        ('nodes', kishash, result['nodes']), ('edges', kishash, result['edges']), ('k', kishash, result['k']) ]
        else:
            raise GBDException("CNF2KIS failed for {} due to {}".format(path, result['hash']))
    except Exception as e:
        util.eprint(str(e))
        if os.path.exists(kispath):
            os.remove(kispath)

    return [ ]

def init_transform_cnf_to_kis(api: GBD, rlimits, query, hashes, target_db, source):
    if source != 'cnf':
        raise InitializerException("Source database must be in context 'cnf'")
    if api.database.dcontext(target_db) != 'kis':
        raise InitializerException("Target database must be in context 'kis'")
    features = [ ('local', None), ('to_cnf', None), ('nodes', 'empty'), ('edges', 'empty'), ('k', 'empty') ]
    transformer = Initializer(api, rlimits, target_db, features, cnf2kis)
    transformer.create_features()

    df = api.query(query, hashes, [source+":local"], collapse=None)
    dfilter = df['local'].apply(lambda x: x and not os.path.isfile(kis_filename(x)))

    transformer.run(df[dfilter])


# Sanitize CNF
def sanitized_filename(path):
    sanpath = reduce(lambda path, suffix: path[:-len(suffix)] if path.endswith(suffix) else path, contexts.suffix_list('cnf'), path)
    return sanpath + '.sanitized.cnf'

def sanitize_cnf(hash, path, limits):
    util.eprint('Sanitizing {}'.format(path))

    sanname = sanitized_filename(path)
    try:
        with open(sanname, 'w') as f, util.stdout_redirected(f):
            if gbdc.sanitize(path): 
                sanhash = identify(sanname)
                return [ ('local', sanhash, sanname), ('to_cnf', sanhash, hash) ]
            else:
                raise GBDException("Sanitization failed for {}".format(path))
    except Exception as e:
        util.eprint(str(e))
        os.remove(sanname)

    return [ ]

def init_sani(api: GBD, rlimits, query, hashes, target_db, source):
    if source != 'cnf':
        raise InitializerException("Source database must be in context 'cnf'")
    if api.database.dcontext(target_db) != 'sancnf':
        raise InitializerException("Target database must be in context 'sancnf'")
    features = [ ('local', None) , ('to_cnf', None) ]
    transformer = Initializer(api, rlimits, target_db, features, sanitize_cnf)
    transformer.create_features()

    df = api.query(query, hashes, [source+":local"], collapse=None)
    dfilter = df['local'].apply(lambda x: x and not os.path.isfile(sanitized_filename(x)))

    transformer.run(df[dfilter])

