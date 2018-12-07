import sys

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
