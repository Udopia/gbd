
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

import io
import hashlib

import gzip
import bz2
import lzma

from gbd_core.contexts import get_context_by_suffix


def open_file(filename, mode):
    if filename.endswith('.gz'):
        return gzip.open(filename, mode)
    elif filename.endswith('.bz2'):
        return bz2.open(filename, mode)
    elif filename.endswith('.lzma') or filename.endswith('.xz'):
        return lzma.open(filename, mode)
    else:
        return open(filename, mode)


try:
    from gbdc import gbdhash as cnf_hash
except ImportError:
    try:
        from gbdhashc import gbdhash as cnf_hash
    except ImportError:
        def cnf_hash(filename):
            file = open_file(filename, 'rb')
            buff = io.BufferedReader(file, io.DEFAULT_BUFFER_SIZE * 16)

            space = False
            skip = False
            start = True
            cldelim = True
            hash_md5 = hashlib.md5()

            for byte in iter(lambda: buff.read(1), b''):
                if not skip and (byte >= b'0' and byte <= b'9' or byte == b'-'):
                    cldelim = byte == b'0' and (space or start)
                    start = False
                    if space:
                        space = False
                        hash_md5.update(b' ')
                    hash_md5.update(byte)
                elif byte <= b' ':
                    space = not start # remember non-leading space characters 
                    skip = skip and byte != b'\n' and byte != b'\r' # comment line ended
                else: #byte == b'c' or byte == b'p':
                    skip = True  # do not hash comment and header line

            if not cldelim:
                hash_md5.update(b' 0')

            file.close()

            return hash_md5.hexdigest()


try:
    from gbdc import opbhash as opb_hash
except ImportError:
    raise Exception("Unable to import opbhash. Please install or update gbdc.")
    

def identify(path, ct=None):
    context = ct or get_context_by_suffix(path)
    if context is None:
        raise Exception("Unable to associate context: " + path)
    elif context in ['cnf', 'sancnf', 'kis', 'wecnf']:
        return cnf_hash(path)
    elif context in ['opb']:
        return opb_hash(path)
    else:
        raise Exception("Unable to identify: " + path)