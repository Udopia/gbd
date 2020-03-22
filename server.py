#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Global Benchmark Database (GBD)
# Copyright (C) 2019 Markus Iser, Luca Springer, Karlsruhe Institute of Technology (KIT)
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import datetime
import logging
import os
import re
import threading
import random
import argparse
import sys
from gbd_tool.util import eprint
from os.path import basename, isfile
from zipfile import ZipFile, ZipInfo

import gbd_server

import tatsu
import flask
from flask import Flask, request, send_file, json, Response
from flask import render_template
from flask.logging import default_handler
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from gbd_tool.gbd_api import GbdApi
from gbd_tool import gbd_hash
from gbd_tool.http_client import USER_AGENT_CLI
from tatsu import exceptions
from werkzeug.middleware.proxy_fix import ProxyFix

limiter = None
DATABASE = None
gbd_api = None
app = Flask(__name__)

QUERY_PATTERNS = [
    'competition_track = main_2019', 
    'local like %vliw%', 
    'variables > 5000000', 
    '(clauses_horn / clauses) > .9'
]

@app.route("/", methods=['GET'])
def quick_search():
    available_groups = sorted(gbd_api.get_all_groups())
    available_groups.remove("local")
    return render_template('quick_search.html', 
        groups=available_groups, checked_groups=["filename"], 
        results=[], query="", query_patterns=QUERY_PATTERNS)


@app.route("/results", methods=['POST'])
def quick_search_results():
    query = request.values.get('query')
    selected_groups = request.values.getlist('groups')
    if not len(selected_groups):
        selected_groups.append("filename")
    available_groups = sorted(gbd_api.get_all_groups())
    available_groups.remove("local")
    groups = sorted(list(set(available_groups) & set(selected_groups)))
    try:
        rows = list(gbd_api.query_search(query, groups))
        return render_template('quick_search_content.html', 
            groups=available_groups, checked_groups=selected_groups, 
            results=rows, query=query, query_patterns=QUERY_PATTERNS)
    except tatsu.exceptions.FailedParse:
        return Response("Malformed Query", status=400)
    except ValueError:
        return Response("Attribute not Available", status=404)


@app.route("/exportcsv", methods=['POST'])
def get_csv_file():
    query = request.values.get('query')
    checked_groups = request.values.getlist('groups')
    results = gbd_api.query_search(query, checked_groups)
    headers = ["hash", "filename"] if len(checked_groups) == 0 else ["hash"] + checked_groups
    content = "\n".join([" ".join([str(entry) for entry in result]) for result in results])
    app.logger.info('Sending CSV file to {} at {}'.format(request.remote_addr, datetime.datetime.now()))
    return Response(" ".join(headers) + "\n" + content, mimetype='text/csv', headers={"Content-Disposition": "attachment; filename=\"query_result.csv\""})


@app.route("/getinstances", methods=['POST'])
def get_url_file():
    query = request.values.get('query')
    result = gbd_api.query_search(query, ["local"])
    #hashes = [row[0] for row in result]
    #content = "\n".join([flask.url_for("get_file", hashvalue=hv, _external=True) for hv in hashes])
    print(str(result))
    content = "\n".join([os.path.join(flask.url_for("get_file", hashvalue=row[0], _external=True), os.path.basename(row[1])) for row in result])
    app.logger.info('Sending URL file to {} at {}'.format(request.remote_addr, datetime.datetime.now()))
    return Response(content, mimetype='text/uri-list', headers={"Content-Disposition": "attachment; filename=\"query_result.uri\""})


@app.route("/query", methods=['POST'])  # query string post
def query_for_cli():
    query = request.values.get('query')
    try:
        hashset = gbd_api.query_search(query, ["local"])
        return json.dumps(list(hashset))
    except tatsu.exceptions.FailedParse:
        return Response("Malformed Query", status=400)


@app.route('/attribute/<attribute>/<hashvalue>')
def get_attribute(attribute, hashvalue):    
    try:
        values = gbd_api.search(attribute, hashvalue)
        if len(values) == 0:
            return "No entry in attribute table associated with this hash"
        return str(",".join(str(value) for value in values))
    except ValueError as err:
        return "Value Error: {}".format(err)


@app.route('/file/<hashvalue>', defaults={'filename': None})
@app.route('/file/<hashvalue>/<filename>')
def get_file(hashvalue, filename):
    values = gbd_api.search("local", hashvalue)
    if len(values) == 0:
        return "No according file found in our database"
    try:
        path = values.pop()
        return send_file(path, as_attachment=True, attachment_filename=os.path.basename(path))
    except FileNotFoundError:
        return "Sorry, I don't have access to the files right now :(\n"


@app.route('/info/<hashvalue>')
def get_all_attributes(hashvalue):
    groups = gbd_api.get_all_groups()
    info = dict([])
    for attribute in groups:
        values = gbd_api.search(attribute, hashvalue)
        info.update({attribute: str(",".join(str(value) for value in values))})
    return json.dumps(info)


@app.route("/getdatabase", methods=['GET'])
def get_default_database_file():
    global DATABASE
    app.logger.info('Sending database to {} at {}'.format(request.remote_addr, datetime.datetime.now()))
    return send_file(DATABASE, attachment_filename=basename(DATABASE), as_attachment=True)


def main():
    parser = argparse.ArgumentParser(description='Web- and Micro- Services to access global benchmark database.')
    parser.add_argument('-d', "--db", help='Specify database to work with', default=os.environ.get('GBD_DB'), nargs='?')
    parser.add_argument('-p', "--port", help='Specify port on which to listen', type=int)
    args = parser.parse_args()
    if not args.db:
        eprint("""No database path is given. 
A database path can be given in two ways:
-- by setting the environment variable GBD_DB
-- by giving a path via --db=[path]
A database file containing some attributes of instances used in the SAT Competitions can be obtained at http://gbd.iti.kit.edu/getdatabase
Don't forget to initialize each database with the paths to your benchmarks by using the init-command. """)
    else:
        logging.basicConfig(filename='server.log', level=logging.DEBUG)
        logging.getLogger().addHandler(default_handler)
        global DATABASE
        DATABASE = args.db
        global gbd_api
        gbd_api = GbdApi(DATABASE)
        global app
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1)
        app.config['database'] = DATABASE
        app.static_folder=os.path.join(os.path.dirname(os.path.abspath(gbd_server.__file__)), "static")
        app.template_folder=os.path.join(os.path.dirname(os.path.abspath(gbd_server.__file__)), "templates")
        global limiter
        limiter = Limiter(app, key_func=get_remote_address)
        app.run(host='0.0.0.0', port=args.port)


if __name__ == '__main__':
    main()
