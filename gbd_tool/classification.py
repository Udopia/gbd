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


def classify(api: GBD, query, feature, hashes, features, collapse, group_by, timeout_memout, filename, replace_dict, flag):

    # no features selected error
    if features == []:
        raise GBDException("No features selected.")
    if feature == '':
        raise GBDException("No classification feature selected.")

    #elimination of unnecessary features
    features = features.remove('hash')

    # what values to replace
    if dict == "default":
        replace_dict = {
            "replace_tuples": [("timeout", np.inf), ("memout", np.inf), ("error", np.NaN)],
        }

    #make dataframe
    resultset = api.query_search2(query, feature, hashes, features, collapse, group_by,
                                  timeout_memout, replace_dict)

    # delete unknown feature entries
    for i in range(len(resultset)):
        if df.at[i, feature] == 'unknown' or df.at[i, feature] == 'empty':
            df = df.drop(i)

    df = df.reset_index(drop=True)

    # convert to floats and ints where possible
    for col in resultset.columns:
        for i in range(len(resultset)):
            e = resultset.iloc[i][col]

            if util.is_number(e):
                if float(e).is_integer():
                    resultset.at[i, col] = int(float(e))
                else:
                    resultset.at[i, col] = float(e)

    #Creates dictionaries for the translation of non-numerical and conituous numerical entries
    dict_str_e = {}
    dict_flt_f ={}
    counter_str = -1
    counter_flt = -1

    #Fills two set with values that should be replaced
    for i in range(len(resultset)):
        e = resultset.iloc[i][feature]
        if util.is_number(e):
            if not float(e).is_integer() and not float(e) in dict_flt_f:
                dict_flt_f[e] = counter_flt
                counter_flt = counter_flt-1
            resultset.at[i][feature] = dict_flt_f[e]
        else:
            if e not in dict_str_e:
                dict_str_e[e] = counter_str
                counter_str = counter_str - 1
            resultset.at[i][feature] = dict_str_e[e]


    #Converts dataframe to the int-type.
    df = resultset.astype(np.int)

    # split data for classification
    x = df
    y = df.pop(feature)

    # create classificator based on data given
    if flag == 0:

        clf = tree.DecisionTreeClassifier()
        clf = clf.fit(x.to_numpy(), y.to_numpy())
        piskle.dump(clf, filename + '.pskl')

    #apply existing classificator
    elif flag == 1:
        clf = piskle.load(filename)
        cl = clf.predict(x)
        class_df = pd.DataFrame(cl)
        class_df.columns = ['predicted']
        print(classification_report(y, class_df))

    # 5 fold cross validation
    elif flag ==3:
        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=0)
        clf = tree.DecisionTreeClassifier()
        clf = clf.fit(x_train.to_numpy(), y_train.to_numpy())
        cl = clf.predict(x_test)
        class_df = pd.DataFrame(cl)
        class_df.columns = ['predicted']
        pd.set_option('display.max_rows', None)
        file = open(filename, 'w')
        file.writelines("Classification report: " + classification_report(y_test, class_df) + ".\n")
        file.close()




