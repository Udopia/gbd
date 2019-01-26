import htmlGenerator

from tatsu import exceptions
from core.main import groups, search
from core.database.db import Database
from sqlite3 import OperationalError


from flask import Flask, render_template, request, url_for

from zipfile import ZipFile, ZIP_DEFLATED
from os.path import realpath, dirname, join

import io

app = Flask(__name__)

DATABASE = join(dirname(realpath(__file__)), 'local.db')


@app.route("/", methods={'GET'})
def welcome():
    return render_template('home.html')


@app.route("/resolve", methods=['POST'])
def resolve():
    hashed = request.values.to_dict()["hash"]
    group = request.values.to_dict()["group"]
    result = htmlGenerator.generate_html_header("en")
    result += htmlGenerator.generate_head("Results")
    with Database(DATABASE) as database:
        entries = []
        if group == "":
            allgroups = groups.reflect(database)
            for attribute in allgroups:
                if attribute != "__version":
                    value = search.resolve(database, attribute, hashed)
                    entries.append([attribute, value])
            result += htmlGenerator.generate_resolve_table_div(entries)
        else:
            try:
                value = search.resolve(database, group, hashed)
                entries.append([group, value])
                result += htmlGenerator.generate_resolve_table_div(entries)
            except OperationalError:
                result += htmlGenerator.generate_warning("Group not found")
            except IndexError:
                result += htmlGenerator.generate_warning("Hash not found in our database")
    return result


@app.route("/query_form", methods=['GET'])
def query_form():
    return render_template('query_form.html')


@app.route("/query", methods=['POST'])   # query string über post
def query():
    response = htmlGenerator.generate_html_header("en")
    response += htmlGenerator.generate_head("Results")
    query = request.values.to_dict()["query"]
    with ZipFile('benchmarks.zip', 'w', ZIP_DEFLATED) as myzip:
        with Database(DATABASE) as database:
            try:
                list = search.find_hashes(database, query)
                for hash in list:
                    myzip.write(*search.resolve(database, "benchmarks", hash))
                response += htmlGenerator.generate_num_table_div(list)
                response += htmlGenerator.generate_zip_download("benchmarks.zip")
            except exceptions.FailedParse:
                response += htmlGenerator.generate_warning("Non-valid query")
            except OperationalError:
                response += htmlGenerator.generate_warning("Group not found")

    return response


@app.route("/reflect", methods=['GET'])
def reflect():
    with Database(DATABASE) as database:
        list = groups.reflect(database)

    return None


@app.route("/benchmarks.zip", methods=['GET'])
def download():
    return
