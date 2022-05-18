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



contexts_ = {
    "cnf" : [".cnf", ".cnf.gz", ".cnf.lzma", ".cnf.xz", ".cnf.bz2"],
    "cnf2" : [".cnf", ".cnf.gz", ".cnf.lzma", ".cnf.xz", ".cnf.bz2"],
    "sancnf" : [".cnf", ".cnf.gz", ".cnf.lzma", ".cnf.xz", ".cnf.bz2"],
    "kis" : [".kis", ".kis.gz", ".kis.lzma", ".kis.xz", ".kis.bz2"]
}

def suffix_list(context):
    return contexts_[context]

def contexts():
    return contexts_.keys()