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


def classify2(api: GBD, query, feature, hashes, features, collapse,group_by, timeout_memout, filenames, replace_dict):

    # no features selected error
    if features == []:
        raise GBDException("No features selected.")
    if feature == '':
        raise GBDException("No classification feature selected.")

    #elimination of the hash
    features = features.remove('hash')

    # what values to replace
    if dict == "default":
        replace_dict = {
            "replace_tuples": [("timeout", np.inf), ("memout", np.inf), ("error", np.NaN)],
        }

    #make dataframe
    resultset = api.query_search2(query, feature, hashes, features, collapse, group_by,
                                  timeout_memout, replace_dict, filenames)


    #Creates dictionaries for the translation of non-numerical and conituous numerical entries
    dict_str_e = {}
    dict_flt_f ={}




    dict_str_e[feature] = {}
    dict_flt_f[feature] = {}
    s_str = set()
    s_flt = set()

    #Fills two set with values that should be replaced
    for i in range(len(resultset)):
        e = resultset.iloc[i][feature]
        if util.is_number(e):
            if not float(e).is_integer():
               s_flt.add(float(e))
        else:
            s_str.add(e)

    #Fills the dictionaries with a translation of the set-values and a counter
    counter_str = -1
    for e in s_str:
        dict_str_e[feature][e] = counter_str
        counter_str = counter_str - 1

    counter_flt = -1
    for f in s_flt:
        dict_flt_f[feature][f] = counter_flt
        counter_flt = counter_flt-1

    #Replaces the undesired values by the integer using the dictionaries
    for i in range(len(resultset)):
        e = resultset.iloc[i][feature]
        if e in dict_str_e[feature]:
            resultset.at[i, feature] = dict_str_e[feature][e]
        elif float(e) in dict_flt_f[feature]:
            resultset.at[i, feature] = dict_flt_f[feature][float(e)]


    #Converts dataframe to the int-type.
    df = resultset.astype(np.int)



    #split train and test data
    x = df
    y = df.pop(feature)

    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size = 0.2, random_state = 0)

    #create and apply classifier
    clf = tree.DecisionTreeClassifier()
    clf = clf.fit(x_train.to_numpy(), y_train.to_numpy())

    #s = clf.score(x_test, y_test)

    # store classificator and dictionary
    if (filenames != []):
        piskle.dump(clf, filenames[0] + '.pskl')

    if(len(filenames) == 3):
        # read classificator and dictionary
        clf2 = piskle.load(filenames[1])


        cl = clf2.predict(x_test)

        class_df = pd.DataFrame(cl)
        class_df.columns = ['predicted']


        print(classification_report(y_test, class_df))

        # stores the result in case a filename is given
        #if (filenames[2] != 'empty'):
        pd.set_option('display.max_rows', None)
        file = open(filenames[2], 'w')
        #file.writelines("Score: "+ str(s)+".\n")
        file.writelines("Classification report: "+ classification_report(y_test, class_df)+".\n")
        file.close()



