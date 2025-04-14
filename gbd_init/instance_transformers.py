
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

try:
    from gbdc import cnf2kis, sanitise, normalise
except ImportError:
    def cnf2kis(ipath, opath):
        raise ModuleNotFoundError("gbdc not found", name="gbdc")
    
    def sanitise(ipath, opath):
        raise ModuleNotFoundError("gbdc not found", name="gbdc")
    
    def normalise(ipath, opath):
        raise ModuleNotFoundError("gbdc not found", name="gbdc")


def kis_filename(path):
    kispath = reduce(lambda path, suffix: path[:-len(suffix)] if path.endswith(suffix) else path, contexts.suffixes('cnf'), path)
    return kispath + '.kis'

def sanitised_filename(path):
    sanpath = reduce(lambda path, suffix: path[:-len(suffix)] if path.endswith(suffix) else path, contexts.suffixes('cnf'), path)
    return sanpath + '.sanitized.cnf'

def normalised_filename(path):
    normpath = reduce(lambda path, suffix: path[:-len(suffix)] if path.endswith(suffix) else path, contexts.suffixes('cnf'), path)
    return normpath + '.normalised.cnf'


def wrap_cnf2kis(hash, path, limits):
    kispath = kis_filename(path)
    util.eprint('Transforming {} to k-ISP {}'.format(path, kispath))
    try:
        result = cnf2kis(path, kispath)
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

def wrap_sanitise(hash, path, limits):
    sanpath = sanitised_filename(path)
    util.eprint('Sanitising {}'.format(path))
    try:
        with open(sanpath, 'w') as f, util.stdout_redirected(f):
            result = sanitise(path, sanpath)
            if "local" in result:
                sanhash = result['hash']
                return [ ('local', sanhash, result['local']), ('to_cnf', sanhash, hash) ]
            else:
                raise GBDException("Sanitization failed for {}".format(path))
    except Exception as e:
        util.eprint(str(e))
        if os.path.exists(sanpath):
            os.remove(sanpath)

    return [ ]

def wrap_normalise(hash, path, limits):
    normpath = normalised_filename(path)
    util.eprint('Normalising {}'.format(path))
    try:
        with open(normpath, 'w') as f, util.stdout_redirected(f):
            result = normalise(path, normpath)
            normhash = result['hash']
            if "local" in result and hash == normhash:
                return [ ('local', normhash, result['local']) ]
            else:
                raise GBDException("Normalisation failed for {}".format(path))
    except Exception as e:
        util.eprint(str(e))
        if os.path.exists(normpath):
            os.remove(normpath)

    return [ ]


def transform_instances_generic(key: str, api: GBD, rlimits, query, hashes, target_db, source):
    einfo = generic_transformers[key]
    context = api.database.dcontext(target_db)
    if not context in einfo["target"]:
        raise InitializerException("Target database context must be in {}".format(einfo["target"]))
    if not source in einfo["source"]:
        raise InitializerException("Source database context must be in {}".format(einfo["source"]))
    transformer = Initializer(api, rlimits, target_db, einfo["features"], einfo["compute"])
    transformer.create_features()

    df = api.query(query, hashes, [source+":local"], collapse=None)
    dfilter = df['local'].apply(lambda x: x and not os.path.isfile(einfo["filename"](x)))

    transformer.run(df[dfilter])


generic_transformers = {
    "sanitise" : {
        "description" : "Sanitise CNF files. ",
        "source" : [ "cnf" ],
        "target" : [ "sancnf" ],
        "features" : [ ('local', None) , ('to_cnf', None) ],
        "compute" : wrap_sanitise,
        "filename" : sanitised_filename,
    },
    "normalise" : {
        "description" : "Normalise CNF files. ",
        "source" : [ "cnf" ],
        "target" : [ "cnf" ],
        "features" : [ ('local', None) ],
        "compute" : wrap_normalise,
        "filename" : normalised_filename,
    },
    "cnf2kis" : {
        "description" : "Transform CNF files to k-ISP instances. ",
        "source" : [ "cnf" ],
        "target" : [ "kis" ],
        "features" : [ ('local', None), ('to_cnf', None), ('nodes', 'empty'), ('edges', 'empty'), ('k', 'empty') ],
        "compute" : wrap_cnf2kis,
        "filename" : kis_filename,
    },
}