import numpy as np
import pandas as pd
import gbd_tool.util as util
import piskle
import argparse
import os
import sys
import traceback

from sklearn import tree
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

from gbd_tool.gbd_api import GBD
from gbd_tool import contexts
from gbd_tool import util

class GBDException(Exception):
    pass


def classify(api: GBD, query, feature, hashes, features, collapse, group_by, timeout_memout, filename, replace, flag):
    # what values to replace
    if len(replace) == 0:
        replace = [("timeout", np.nan), ("memout", np.nan)]

    res = features + timeout_memout + [ feature ]

    df = api.query_search2(query + " and {f} != unknown and {f} != empty".format(f=feature), hashes, res, collapse, group_by, replace)

    df.drop(["hash"], axis=1, inplace=True)

    # right-hand side (of classifier)
    categories = df.pop(feature).astype("category")
    y = categories.cat.codes.to_numpy()

    # left-hand side (of classifier)
    x = np.nan_to_num(df.to_numpy().astype(np.float32))

    # create classificator based on data given
    if flag == 0:
        clf = tree.DecisionTreeClassifier()
        clf = clf.fit(x, y)
        piskle.dump(clf, filename + '.pskl')

    #apply existing classificator
    elif flag == 1:
        clf = piskle.load(filename)
        cl = clf.predict(x)
        class_df = pd.DataFrame(cl, columns = ['predicted'])
        print(classification_report(y, class_df))

    # 5 fold cross validation
    elif flag ==3:
        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=0)
        clf = tree.DecisionTreeClassifier()
        clf = clf.fit(x_train, y_train)
        cl = clf.predict(x_test)
        class_df = pd.DataFrame(cl, columns = ['predicted'])
        pd.set_option('display.max_rows', None)
        file = open(filename, 'w')
        file.writelines("Classification report: " + classification_report(y_test, class_df) + ".\n")
        file.close()


def cli_classify(api: GBD, args):
    from gbd_apps import classification
    classification.classify(api, args.query, args.feature, args.hashes, args.resolve, args.collapse, args.group_by, args.timeout_memout, args.save, args.dict, args.mode)


### Argument Types for Input Sanitation in ArgParse Library
def directory_type(path):
    if not os.path.isdir(path):
        raise argparse.ArgumentTypeError('{0} is not a directory'.format(path))
    if os.access(path, os.R_OK):
        return os.path.abspath(path)
    else:
        raise argparse.ArgumentTypeError('{0} is not readable'.format(path))

def file_type(path):
    if not os.path.isfile(path):
        raise argparse.ArgumentTypeError('{0} is not a regular file'.format(path))
    if os.access(path, os.R_OK):
        return os.path.abspath(path)
    else:
        raise argparse.ArgumentTypeError('{0} is not readable'.format(path))

def column_type(s):
    pat = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")
    if not pat.match(s):
        raise argparse.ArgumentTypeError('Column "{0}" does not match regular expression {1}'.format(s, pat.pattern))
    return s

def key_value_type(s):
    tup = s.split('=', 1)
    if len(tup) != 2:
        raise argparse.ArgumentTypeError('key-value type: {0} must be separated by exactly one = '.format(s))
    return (column_type(tup[0]), tup[1])

def gbd_db_type(dbstr):
    if not dbstr:
        default=os.environ.get('GBD_DB')
        if not default:
            raise argparse.ArgumentTypeError("Datasources Missing: Set GBD_DB environment variable (Get databases: http://gbd.iti.kit.edu/)")
        return default
    return dbstr


def add_query_and_hashes_arguments(parser: argparse.ArgumentParser):
    parser.add_argument('query', help='GBD Query', nargs='?')
    parser.add_argument('--hashes', help='Give Hashes as ARGS or via STDIN', nargs='*', default=[])


def main():
    parser = argparse.ArgumentParser(description='GBD Benchmark Database')

    parser.add_argument('-d', "--db", help='Specify database to work with', type=gbd_db_type, nargs='?', default=os.environ.get('GBD_DB'))
    parser.add_argument('-v', '--verbose', help='Print additional (or diagnostic) information to stderr', action='store_true')
    parser.add_argument('-w', '--subselect', help='Move where to subselect', action='store_true')

    parser.add_argument('-t', '--tlim', help="Time limit (sec) per instance for 'init' sub-commands (also used for score calculation in 'eval' and 'plot')", default=5000, type=int)
    parser.add_argument('-m', '--mlim', help="Memory limit (MB) per instance for 'init' sub-commands", default=2000, type=int)
    parser.add_argument('-f', '--flim', help="File size limit (MB) per instance for 'init' sub-commands which create files", default=1000, type=int)

    parser.add_argument('-s', "--separator", help="Feature separator (delimiter used in import and output", choices=[" ", ",", ";"], default=" ")
    parser.add_argument("--join-type", help="Join Type: treatment of missing values in queries", choices=["INNER", "OUTER", "LEFT"], default="LEFT")
    parser.add_argument('-c', '--context', default='cnf', choices=contexts.contexts(), 
                            help='Select context (affects selection of hash/identifier and available feature-extractors in init)')

    subparsers = parser.add_subparsers(help='Available Commands:', required=True, dest='gbd command')

    #CLASSIFICATION
    parser_classify = subparsers.add_parser('classify', help='trains the classifier and interprets it')
    add_query_and_hashes_arguments(parser_classify)
    parser_classify.add_argument('-f', '--feature', help='Feature that should be classified', required=True)
    parser_classify.add_argument('-r', '--resolve', help='List of features to resolve against', nargs='+', required=True)
    parser_classify.add_argument('-c', '--collapse', default='group_concat',
                                   choices=['group_concat', 'min', 'max', 'avg', 'count', 'sum', 'none'],
                                   help='Treatment of multiple values per hash (or grouping value resp.)')
    parser_classify.add_argument('-s', '--save', help='Filename')
    parser_classify.add_argument('-g', '--group_by', default='hash', help='Group by specified attribute value')
    parser_classify.add_argument('-d', '--dict', default=[], help='Dictionary to replace the margin values')
    parser_classify.add_argument('-m', '--mode', default ='0', help='How to evaluate the classification. 0: generates and stores classifier. 1: applies given clas0sifier. 2: generates a classifier and evaluates it.' )
    parser_classify.add_argument('-o', '--timeout_memout', default = [],  help='List of features to resolve against that can have a memout or timeout', nargs ='+')
    parser_classify.set_defaults(func=cli_classify)

    # PARSE ARGUMENTS
    args = parser.parse_args()
    try:
        if hasattr(args, 'hashes') and not sys.stdin.isatty():
            if not args.hashes or len(args.hashes) == 0:
                args.hashes = util.read_hashes()  # read hashes from stdin
        with GBD(args.db.split(os.pathsep), args.context, int(args.jobs), args.tlim, args.mlim, args.flim, args.separator, args.join_type, args.verbose) as api:
            args.func(api, args)
    except Exception as e:
        util.eprint("{}: {}".format(type(e), str(e)))
        if args.verbose:
            util.eprint(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()

