# GBD Benchmark Database (GBD)
# Copyright (C) 2020 Markus Iser, Karlsruhe Institute of Technology (KIT)
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


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
