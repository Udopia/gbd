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
import os


# Thanks to Boris V. for this code https://stackoverflow.com/questions/4675728/redirect-stdout-to-a-file-in-python
from contextlib import contextmanager

def fileno(file_or_fd):
    fd = getattr(file_or_fd, 'fileno', lambda: file_or_fd)()
    if not isinstance(fd, int):
        raise ValueError("Expected a file (`.fileno()`) or a file descriptor")
    return fd

@contextmanager
def stdout_redirected(to=os.devnull, stdout=None):
    if stdout is None:
       stdout = sys.stdout

    stdout_fd = fileno(stdout)
    # copy stdout_fd before it is overwritten
    #NOTE: `copied` is inheritable on Windows when duplicating a standard stream
    with os.fdopen(os.dup(stdout_fd), 'wb') as copied: 
        stdout.flush()  # flush library buffers that dup2 knows nothing about
        try:
            os.dup2(fileno(to), stdout_fd)  # $ exec >&to
        except ValueError:  # filename
            with open(to, 'wb') as to_file:
                os.dup2(to_file.fileno(), stdout_fd)  # $ exec > to
        try:
            yield stdout # allow code to be run with the redirected stdout
        finally:
            # restore stdout to its previous value
            #NOTE: dup2 makes stdout_fd inheritable unconditionally
            stdout.flush()
            os.dup2(copied.fileno(), stdout_fd)  # $ exec >&copied


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
    try:
        if s is not None:
            float(s)
            return True
    except ValueError:
        return False
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
