import os
import random

def get_random_clause(max_len=10, max_vars=30):
    return ' '.join([str(random.randint(-max_vars, max_vars)) for _ in range(random.randint(0, max_len))]) + ' 0'

def get_random_formula(max_num=50):
    return '\n'.join([get_random_clause() for _ in range(random.randint(0, max_num))]) + '\n'

def get_random_unique_filename(prefix='random', suffix='.cnf'):
    filename = prefix + suffix
    while os.path.exists(filename):
        filename = f"{prefix}{random.randint(0, 1000)}{suffix}"
    return filename

def get_random_cnffile(max_num=50):
    filename = get_random_unique_filename()
    with open(filename, 'w') as f:
        f.write(f'p cnf {random.randint(1, 100)} {random.randint(1, 100)}\n')
        f.write(get_random_formula(max_num))
    return filename