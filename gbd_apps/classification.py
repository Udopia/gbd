#!/usr/bin/python3

import numpy as np
import pandas as pd
import gbd.util as util
import piskle
import argparse
import os
import sys
import traceback

from sklearn import tree
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

from gbd.api import GBD
from gbd import util
from gbd.util_argparse import *

class GBDException(Exception):
    pass


def classify(api: GBD, query, feature, hashes, features, collapse, group_by, filename, flag):
    df = api.query(query + " and {f} != unknown and {f} != empty".format(f=feature), hashes, features + [ feature ], collapse, group_by)

    for (key, value) in [("timeout", np.nan), ("memout", np.nan)]:
        df.replace(key, value, inplace=True)

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
    classification.classify(api, args.query, args.feature, args.hashes, args.resolve, args.collapse, args.group_by, args.timeout_memout, args.save, args.mode)


def main():
    parser = get_gbd_argparser()
    
    add_query_and_hashes_arguments(parser)
    parser.add_argument('-f', '--feature', help='Feature that should be classified', required=True)
    parser.add_argument('-r', '--resolve', help='List of features to resolve against', nargs='+', required=True)
    parser.add_argument('-c', '--collapse', default='group_concat',
                                   choices=['group_concat', 'min', 'max', 'avg', 'count', 'sum', 'none'],
                                   help='Treatment of multiple values per hash (or grouping value resp.)')
    parser.add_argument('-s', '--save', help='Filename')
    parser.add_argument('-g', '--group_by', default='hash', help='Group by specified attribute value')
    parser.add_argument('-d', '--dict', default=[], help='Dictionary to replace the margin values')
    parser.add_argument('-m', '--mode', default ='0', help='How to evaluate the classification. 0: generates and stores classifier. 1: applies given classifier. 2: generates a classifier and evaluates it.' )
    parser.set_defaults(func=cli_classify)

    # PARSE ARGUMENTS
    args = parser.parse_args()
    try:
        if hasattr(args, 'hashes') and not sys.stdin.isatty():
            if not args.hashes or len(args.hashes) == 0:
                args.hashes = util.read_hashes()  # read hashes from stdin
        with GBD(args.db.split(os.pathsep), args.context, int(args.jobs), args.tlim, args.mlim, args.flim, args.join_type, args.verbose) as api:
            args.func(api, args)
    except Exception as e:
        util.eprint("{}: {}".format(type(e), str(e)))
        if args.verbose:
            util.eprint(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()

