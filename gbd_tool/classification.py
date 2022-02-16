import numpy as np
import pandas as pd
import gbd_tool.util as util
import gbd_tool.gbd_api
import math
import piskle
import json

from sklearn import tree
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

from gbd_tool.gbd_api import GBD

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




