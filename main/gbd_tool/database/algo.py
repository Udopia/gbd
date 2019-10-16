from gbd_tool.util import eprint


def run_horn_algo(api, database):
    api.add_attribute_group("clauses_positive", "integer", 0)
    api.add_attribute_group("clauses_negative", "integer", 0)
    api.add_attribute_group("clauses_horn", "integer", 0)

    resultset = api.query_search("(clauses_horn = 0) and (clauses_positive = 0) and (clauses_negative = 0)", ["benchmarks"])
    for result in resultset:
        c_horn = 0
        c_pos = 0
        c_neg = 0
        hashvalue = result[0].split(',')[0]
        path = result[1].split(',')[0]
        eprint(hashvalue)
        eprint(path)
        cnffile = None
        if path.endswith('.cnf.gz'):
            cnffile = gzip.open(path, 'rt')
        elif path.endswith('.cnf.bz2'):
            cnffile = bz2.open(path, 'rt')
        else:
            cnffile = open(path, 'rt')
        
        eprint("Parsing {}".format(path))
        for line in cnffile:
            if line.strip() and len(line.strip().split()) > 1:
                parts = line.strip().split()[:-1]
                if parts[0][0] == 'c' or parts[0][0] == 'p':
                    continue
                n_neg = sum(int(part) < 0 for part in parts)
                if n_neg < 2:
                    c_horn += 1
                    if n_neg == 0:
                        c_pos += 1
                elif n_neg == len(parts):
                    c_neg += 1
        api.set_attribute("clauses_positive", c_pos, [ hashvalue ], True)
        api.set_attribute("clauses_negative", c_neg, [ hashvalue ], True)
        api.set_attribute("clauses_horn", c_horn, [ hashvalue ], True)
        cnffile.close()


def run_vars_algo(api, database):
    api.add_attribute_group("variables", "integer", 0)
    api.add_attribute_group("clauses", "integer", 0)

    resultset = api.query_search("(variables = 0) and (clauses = 0)", ["benchmarks"])
    for result in resultset:
        c_vars = 0
        c_clauses = 0
        hashvalue = result[0].split(',')[0]
        path = result[1].split(',')[0]
        eprint(hashvalue)
        eprint(path)
        cnffile = None
        if path.endswith('.cnf.gz'):
            cnffile = gzip.open(path, 'rt')
        elif path.endswith('.cnf.bz2'):
            cnffile = bz2.open(path, 'rt')
        else:
            cnffile = open(path, 'rt')
        
        eprint("Parsing {}".format(path))
        for line in cnffile:
            if line.strip() and len(line.strip().split()) > 1:
                parts = line.strip().split()[:-1]
                if parts[0][0] == 'c' or parts[0][0] == 'p':
                    continue
                n_vars = max(n_vars, max(int(part) for part in parts))
                n_clauses += 1
        api.set_attribute("variables", c_vars, [ hashvalue ], True)
        api.set_attribute("clauses", c_clauses, [ hashvalue ], True)
        cnffile.close()