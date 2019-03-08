import sys
import os
import datetime
from zipfile import ZipInfo

__all__ = ['eprint', 'read_hashes', 'confirm']


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def read_hashes():
    hashes = set()
    try:
        while True:
            line = sys.stdin.readline()
            if len(line.strip()) == 0:
                return hashes
            hashes.add(line.strip())
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
        ans = input(prompt)
        if not ans:
            return resp
        if ans not in ['y', 'Y', 'n', 'N']:
            print('please enter y or n.')
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False


def delete_old_cached_files(directory, max_hours, max_minutes):
    """
        Delete all ZIP files in list if they are older than x hours or x min
    """
    if max_hours is not None:
        if max_hours < 0 or max_hours >= 24:
            return -1
    elif max_minutes is not None:
        if max_minutes < 0 or max_minutes >= 60:
            return -1
    files = os.listdir(directory)
    for file in files:
        path = "{}/{}".format(directory, file)
        zf = ZipInfo.from_file(path, arcname=None)
        accessed_on_datetime = datetime.datetime(*zf.date_time)
        current_datetime = datetime.datetime.now()
        diff_year = current_datetime.year - accessed_on_datetime.year
        diff_month = current_datetime.month - accessed_on_datetime.month
        diff_hour = current_datetime.hour - accessed_on_datetime.hour
        diff_minute = current_datetime.minute - accessed_on_datetime.minute
        if diff_year != 0 or diff_month != 0:
            os.remove(path)
        elif max_hours is not None and diff_hour >= max_hours and max_minutes is None:
            os.remove(path)
        elif max_minutes is not None and diff_minute >= max_minutes and max_hours is None:
            os.remove(path)
        elif diff_hour >= max_hours and diff_minute >= max_minutes:
            os.remove(path)
    return 0
