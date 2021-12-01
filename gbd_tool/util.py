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

import sys
import bz2
import gzip
import lzma
import re

from gbd_tool import config

__all__ = ['eprint', 'read_hashes', 'confirm', 'open_cnf_file', 'is_number', 'context_from_name', 'prepend_context']


def make_alnum_ul(str):
    return re.sub("[^a-zA-Z0-9]", "_", str)

def prepend_context(feature, context):
    return feature if context == 'cnf' else "{}_{}".format(context, feature)

def context_from_name(name):
    pair = name.split('_')
    if len(pair) > 1 and pair[0] in config.contexts():
        return pair[0]
    else:
        return 'cnf'



def slice_iterator(data, slice_len):
    it = iter(data)
    while True:
        items = []
        for index in range(slice_len):
            try:
                item = next(it)
            except StopIteration:
                if items == []:
                    return # we are done
                else:
                    break # exits the "for" loop
            items.append(item)
        yield items


def is_number(s):
    if s is None:
        return False
    try:
        float(s)
        return True
    except ValueError:
        return False


def open_cnf_file(filename, mode):
    """
    Opens a CNF file (this is badly guarded, by file-extension only)
    """
    if filename.endswith('.cnf.gz'):
        return gzip.open(filename, mode)
    elif filename.endswith('.cnf.bz2'):
        return bz2.open(filename, mode)
    elif filename.endswith('.cnf.lzma') or filename.endswith('.cnf.xz'):
        return lzma.open(filename, mode)
    elif filename.endswith('.cnf'):
        return open(filename, mode)
    else:
        raise Exception("Unknown File Extension. Use .cnf, .cnf.bz2, .cnf.lzma, .cnf.xz, or .cnf.gz")


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def read_hashes():
    eprint("Reading hashes from stdin ...")
    hashes = list()
    try:
        while True:
            line = sys.stdin.readline().split()
            if len(line) == 0:
                return hashes
            hashes.extend(line)
    except KeyboardInterrupt:
        return hashes
    return hashes


def confirm(prompt='Confirm', resp=False):
    """
    prompts for yes or no response from the user. Returns True for yes and False for no.
    'resp' should be set to the default value assumed by the caller when user simply types ENTER.
    """
    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

    while True:
        ans = 'z'
        try:
            ans = input(prompt)
        except EOFError:
            # This hack is for OSX and Linux only 
            # There EOFError occurs when hashes were read from stdin before
            # Reopening stdin in order to facilitate subsequent user input:
            sys.stdin = open("/dev/tty", mode="r")
            ans = input()
        if not ans:
            return resp
        if ans not in ['y', 'Y', 'n', 'N']:
            print('please enter y or n.')
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False
