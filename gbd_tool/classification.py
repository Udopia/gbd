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



'''
Creates the dataframe for further classification work.
'''
def set_up_dictionnaries(df):
    # MI: this should be obsolete

    #Creates dictionaries for the translation of non-numerical and conituous numerical entries
    dict_str_c = {}
    dict_str_e = {}

    dict_flt_c = {}
    dict_flt_f ={}


    for col in df.columns:

        dict_str_c[col] = {}
        dict_str_e[col] = {}
        dict_flt_c[col] = {}
        dict_flt_f[col] = {}
        s_str = set()
        s_flt = set()

        #Fills two set with values that should be replaced
        for i in range(len(df)):
            e = df.iloc[i][col]

            if util.is_number(e):
                if not float(e).is_integer():
                    s_flt.add(float(e))
            else:
                s_str.add(e)

        #Fills the dictionaries with a translation of the set-values and a counter
        counter_str = -1
        for e in s_str:
            dict_str_c[col][counter_str] = e
            dict_str_e[col][e] = counter_str
            counter_str = counter_str - 1

        counter_flt = -1
        for f in s_flt:
            dict_flt_c[col][counter_flt] = f
            dict_flt_f[col][f] = counter_flt
            counter_flt = counter_flt-1

        #Replaces the undesired values by the integer using the dictionaries
        for i in range(len(df)):
            e = df.iloc[i][col]
            if e in dict_str_e[col]:
                df.at[i, col] = dict_str_e[col][e]
            elif float(e) in dict_flt_f[col]:
                df.at[i, col] = dict_flt_f[col][float(e)]

    #Converts dataframe to the int-type.
    df = df.astype(np.int)
    return df, dict_str_c

def classify(api: GBD, feature, resultset, features, filename):

    (df, dict_str_c) = set_up_dictionnaries(resultset)

    #Generates a training dataset and a test dataset and resets their row indexes.
    train_df = df.iloc[lambda x: x.index % 5 != 0]
    test_df = df.iloc[lambda x: x.index % 5 == 0]

    train_df = train_df.reset_index(drop=True)
    test_df = test_df.reset_index(drop=True)

    #Prepares the training data
    x = train_df.copy()
    y = pd.DataFrame(x.pop(feature), columns=[feature])

    #Builds the classificator on the training data
    clf = tree.DecisionTreeClassifier()
    clf = clf.fit(x.to_numpy(), y.to_numpy())

    #Prepares the test data
    test_classes = test_df.pop(feature)
    test_classes = test_classes.reset_index(drop=True)

    #Applies the classificator on the test data
    cl = clf.predict(test_df)

    class_df = pd.DataFrame(cl)
    class_df.columns = ['predicted']

    #Creates a dataframe with the comparison of the actual and the predicted features with the re-translation
    diff = pd.concat([test_classes, class_df], axis=1, join="inner")

    diff = diff.astype(str)

    for col in diff.columns:
        for i in range(len(diff)):
            c = str(diff.at[i, col])
            if c in str(dict_str_c[feature]):
                diff.at[i, col] = str(dict_str_c[feature][int(c)])
            else:
                diff.at[i, col] = "invalid"

    print(diff)

    #Counts how often the data was predicted wrongly
    misses = 0

    for i in range(len(diff)):
        if diff.at[i, feature] != diff.at[i, 'predicted']:
            misses = misses + 1

    print("The classifier has a total of "+str(misses)+" misses.")
    print("This makes up a total of "+ str(misses / len(diff))+ " percent fault rate.")

    #stores the result in case a filename is given
    if(filename != 'empty'):
        pd.set_option('display.max_rows', None)
        file = open(filename, 'w')
        print(diff, file=file)
        file.writelines("The classifier has a total of "+str(misses)+" misses.\n")
        file.write("This makes up a total of "+ str(misses / len(diff))+ " percent fault rate.")
        file.close()

def classify2(api: GBD, feature, resultset, features, filename):
    (df, dict_str_c) = set_up_dictionnaries(resultset)

    x = resultset
    y = resultset.pop(feature)

    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size = 0.4, random_state = 0)

    clf = tree.DecisionTreeClassifier()
    clf = clf.fit(x_train.to_numpy(), y_train.to_numpy())

    #s = clf.score(x_test, y_test)

    cl = clf.predict(x_test)

    class_df = pd.DataFrame(cl)
    class_df.columns = ['predicted']


    print(classification_report(y_test, class_df))

    # stores the result in case a filename is given
    if (filename != 'empty'):
        pd.set_option('display.max_rows', None)
        file = open(filename, 'w')
        #file.writelines("Score: "+ str(s)+".\n")
        file.writelines("Classification report: "+ classification_report(y_test, class_df)+".\n")
        file.close()



    
def classify_train(api: GBD, feature, resultset, features, filename):

    (df, dict_str_c) = set_up_dictionnaries(resultset)

    #Prepares the training data
    x = df.copy()
    y = pd.DataFrame(x.pop(feature), columns=[feature])

    #Builds the classificator on the training data
    clf = tree.DecisionTreeClassifier()
    clf = clf.fit(x.to_numpy(), y.to_numpy())

    # store classificator and dictionary
    with open(filename+'_dict.txt', 'w') as convert_file:
        convert_file.write(json.dumps(dict_str_c))
    piskle.dump(clf, filename+'.pskl')


def classify_test(api: GBD, feature, df, features, filename):

    # read classificator and dictionary
    clf = piskle.load(filename)
    dict_str_c = {}
    with open(filename+'_dict.txt') as f:
        for line in f:
            (key, val) = line.split()
            dict_str_c[int(key)] = val

    #Prepares the test data
    test_classes = df.pop(feature)
    test_classes = test_classes.reset_index(drop=True)

    #Applies the classificator on the test data
    cl = clf.predict(df)

    class_df = pd.DataFrame(cl)
    class_df.columns = ['predicted']

    #Creates a dataframe with the comparison of the actual and the predicted features with the re-translation
    diff = pd.concat([test_classes, class_df], axis=1, join="inner")

    diff = diff.astype(str)

    for col in diff.columns:
        for i in range(len(diff)):
            c = str(diff.at[i, col])
            if c in str(dict_str_c[feature]):
                diff.at[i, col] = str(dict_str_c[feature][int(c)])
            else:
                diff.at[i, col] = "invalid"

    print(diff)

    #Counts how often the data was predicted wrongly
    misses = 0

    for i in range(len(diff)):
        if diff.at[i, feature] != diff.at[i, 'predicted']:
            misses = misses + 1

    print("The classifier has a total of "+str(misses)+" misses.")
    print("This makes up a total of "+ str(misses / len(diff))+ " percent fault rate.")

    '''#stores the result in case a filename is given
    if(filename != 'empty'):
        pd.set_option('display.max_rows', None)
        file = open(filename, 'w')
        print(diff, file=file)
        file.writelines("The classifier has a total of "+str(misses)+" misses.\n")
        file.write("This makes up a total of "+ str(misses / len(diff))+ " percent fault rate.")
        file.close()'''

