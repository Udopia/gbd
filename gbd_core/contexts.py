
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


packed = [ "", ".gz", ".lzma", ".xz", ".bz2" ]

context_data = {
    "cnf" : {
        "id" : 100,
        "suffixes" : [ ".cnf" + p for p in packed ],
    },
    "sancnf" : {
        "id" : 101,
        "suffixes" : [ ".sanitized.cnf" + p for p in packed ],
    },
    "kis" : {
        "id" : 200,
        "suffixes" : [ ".kis" + p for p in packed ],
    }
}

def suffix_list(context):
    return context_data[context]['suffixes']

def contexts():
    return context_data.keys()
