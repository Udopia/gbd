
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

from gbd_core.util import open_cnf_file

try:
    from gbdc import gbdhash as gbd_hash
except ImportError:
    try:
        from gbdhashc import gbdhash as gbd_hash
    except ImportError:
        def gbd_hash(filename):
            file = open_cnf_file(filename, 'rb')
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