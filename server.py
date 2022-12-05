#!/usr/bin/python3
# -*- coding: utf-8 -*-

# GBD Benchmark Database (GBD)
# Copyright (C) 2020 Markus Iser, Luca Springer, Karlsruhe Institute of Technology (KIT)
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

import logging
import os
import re
import argparse
import operator

import flask
from flask import Flask, request, send_file, json, Response
from flask import render_template

from logging.handlers import TimedRotatingFileHandler

from werkzeug.middleware.proxy_fix import ProxyFix

from gbd_tool import contexts, util
from gbd_tool.db import DatabaseException
from gbd_tool.schema import Schema
from gbd_tool.gbd_api import GBD, GBDException

import gbd_server

app = Flask(__name__)


def request_query(request):
    query = None
    if "query" in request.form:
        query = request.form.get('query')
    elif len(request.args) > 0:
        query = " and ".join(["{}={}".format(key, value) for (key, value) in request.args.items()])
    return query

def request_features(request):
    selected_features = []
    if "selected_features" in request.form:
        selected_features = list(filter(lambda x: x != '', request.form.getlist('selected_features')))
    return list(set(app.config['features_flat']) & set(selected_features))


def query_to_name(query):
    return re.sub(r'[^\w]', '_', query)


def error_response(msg, addr, errno=404):
    app.logger.error("{}: {}".format(addr, msg))
    return Response(msg, status=errno, mimetype="text/plain")

def file_response(text_blob, filename, mimetype, addr):
    app.logger.info("{}: Sending generated file {}".format(addr, filename))
    return Response(text_blob, mimetype=mimetype, headers={"Content-Disposition": "attachment; filename=\"{}\"".format(filename), "filename": filename})

def path_response(path, filename, mimetype, addr):
    app.logger.info("{}: Sending file {}".format(addr, path))
    return send_file(path, as_attachment=True, download_name=filename, mimetype=mimetype)

def json_response(json_blob, msg, addr):
    app.logger.info("{}: {}".format(addr, msg))
    return Response(json_blob, status=200, mimetype="application/json")

def page_response(query, features):
    with GBD(app.config['database'], verbose=app.config['verbose']) as gbd_api:
        try:
            rows = gbd_api.query_search(query, resolve=features, collapse="MIN")
            return render_template('index.html', query=query, result=rows, selected=features, features=app.config["features"])
        except (GBDException, DatabaseException) as err:
            return error_response("{}, {}".format(type(err), str(err)), request.remote_addr, errno=500)


# Returns main index page
@app.route("/", methods=['POST', 'GET'])
def quick_search():
    query = request_query(request) or ""
    features = request_features(request) or ["filename"]
    return page_response(query, features)


# Expects POST form with a query as text input and selected features as checkbox inputs,
# sends csv version of the result as a file
@app.route("/exportcsv/", methods=['POST', 'GET'])
@app.route("/exportcsv/<context>", methods=['POST', 'GET'])
def get_csv_file(context='cnf'):
    with GBD(app.config['database'], verbose=app.config['verbose']) as gbd_api:
        query = request_query(request)
        features = request_features(request)
        group = contexts.prepend_context("hash", context)
        try:
            results = gbd_api.query_search(query, [], features, group_by=group)
        except (GBDException, DatabaseException) as err:
            return error_response("{}, {}".format(type(err), str(err)), request.remote_addr, errno=500)
        titles = " ".join([ group ] + features) + "\n"
        content = "\n".join([" ".join([str(entry) for entry in result]) for result in results])
        return file_response(titles + content, query_to_name(query) + ".csv", "text/csv", request.remote_addr)


# Generates a list of URLs. Given query (text field of POST form) is executed and the hashes of the result are resolved
# against the filename feature. Every filename is associated with a URL to enable flexible downloading of these files
@app.route("/getinstances/", methods=['POST', 'GET'])
@app.route("/getinstances/<context>", methods=['POST', 'GET'])
def get_url_file(context='cnf'):
    with GBD(app.config['database'], verbose=app.config['verbose']) as gbd_api:
        query = request_query(request)
        try:
            result = gbd_api.query_search(query, group_by=contexts.prepend_context("hash", context))
        except (GBDException, DatabaseException) as err:
            return error_response("{}, {}".format(type(err), str(err)), request.remote_addr, errno=500)
        if context == 'cnf':
            content = "\n".join([ flask.url_for("get_file", hashvalue=row[0], _external=True) for row in result ])
        else:
            content = "\n".join([ flask.url_for("get_file", hashvalue=row[0], context=context, _external=True) for row in result ])
        return file_response(content, query_to_name(query) + ".uri", "text/uri-list", request.remote_addr)


# Return list of databases
@app.route("/listdatabases", methods=["GET"])
def list_databases():
    return json_response(json.dumps(app.config['dbnames']), "Sending list of databases", request.remote_addr)


# Get all features or features in a specified database
@app.route('/listfeatures/')
@app.route('/listfeatures/<database>')
def list_features(database=None):
    dbname=database if database and database in app.config['dbnames'] else 'all'
    return json_response(json.dumps(app.config['features'][dbname]), "Sending list of features", request.remote_addr)


# Send database file
@app.route('/getdatabase/')
@app.route('/getdatabase/<database>')
def get_database_file(database=None):
    dbname=database if database and database in app.config['dbnames'] else app.config['dbnames'][0]
    dbpath=app.config['dbpaths'][dbname]
    return path_response(dbpath, os.path.basename(dbpath), 'application/x-sqlite3', request.remote_addr)


# Find the file corresponding to the hashvalue and send it to the client
@app.route('/file/<hashvalue>/')
@app.route('/file/<hashvalue>/<context>')
def get_file(hashvalue, context='cnf'):
    with GBD(app.config['database'], verbose=app.config['verbose'])as gbd_api:
        hash = contexts.prepend_context("hash", context)
        local = contexts.prepend_context("local", context)
        filename = contexts.prepend_context("filename", context)
        records = gbd_api.query_search(hashes=[hashvalue], resolve=[local, filename], collapse="MIN", group_by=hash)
        if len(records) == 0:
            return error_response("Hash '{}' not found".format(hashvalue), request.remote_addr)
        hashv, path, file = operator.itemgetter(0, 1, 2)(records[0])
        if not os.path.exists(path):
            return error_response("Files temporarily not accessible", request.remote_addr)
        return path_response(path, hashv+"-"+file, 'text/plain', request.remote_addr)
        

# Resolves a hashvalue against a attribute and returns the result values
@app.route('/attribute/<feature>/<hashvalue>')
def get_attribute(feature, hashvalue):
    app.logger.info("Resolving '{}' with feature '{}' for IP {}".format(hashvalue, feature, request.remote_addr))
    with GBD(app.config['database'], verbose=app.config['verbose']) as gbd_api:
        try:
            records = gbd_api.query_search(hashes=[hashvalue], resolve=[feature])
            if len(records) == 0:
                return error_response("Hash '{}' not found".format(hashvalue), request.remote_addr)
            return records[0][1]
        except (GBDException, DatabaseException) as err:
            return error_response("{}, {}".format(type(err), str(err)), request.remote_addr, errno=500)


# Get all attributes associated with the hashvalue (resolving against all features)
@app.route('/info/<hashvalue>/')
@app.route('/info/<hashvalue>/<context>')
def get_all_attributes(hashvalue, context='cnf'):
    app.logger.info("Listing all attributes of hashvalue {} for IP {}".format(hashvalue, request.remote_addr))
    with GBD(app.config['database'], verbose=app.config['verbose']) as gbd_api:
        features = app.config['features_flat']
        try:
            records = gbd_api.query_search(hashes=[hashvalue], resolve=features, group_by=contexts.prepend_context("hash", context))
            if len(records) == 0:
                return error_response("Hash '{}' not found".format(hashvalue), request.remote_addr)
            return json_response(json.dumps(dict(zip(features, records[0]))), "Sending list of attributes", request.remote_addr)
        except (GBDException, DatabaseException) as err:
            return error_response("{}, {}".format(type(err), str(err)), request.remote_addr, errno=500)


# Main method which configures Flask app at startup
def main():
    pwd=os.path.dirname(os.path.abspath(gbd_server.__file__))
    parser = argparse.ArgumentParser(description='Web- and Micro- Services to access global benchmark database.')
    parser.add_argument('-d', "--db", help='Specify database to work with', default=os.environ.get('GBD_DB'), nargs='?')
    parser.add_argument('-l', "--logdir", help='Specify logging dir', default=os.environ.get('GBD_LOGGING_DIR') or pwd, nargs='?')
    parser.add_argument('-p', "--port", help='Specify port on which to listen', type=int)
    parser.add_argument('-v', "--verbose", help='Verbose Mode', action='store_true')
    args = parser.parse_args()
    formatter = logging.Formatter(fmt='[%(asctime)s, %(name)s, %(levelname)s] %(module)s.%(filename)s.%(funcName)s():%(lineno)d\n%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logging.getLogger().setLevel(logging.DEBUG)
    # Add sys.stdout to logging output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(console_handler)
    # Add handler to write in rotating logging files
    file_handler = TimedRotatingFileHandler(os.path.join(args.logdir, "gbd-server-log"), when="midnight", backupCount=10)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.WARNING)
    logging.getLogger().addHandler(file_handler)
    global app
    if not args.db:
        app.logger.error("No Database Given")
        exit(1)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1)
    app.config['database'] = args.db.split(os.pathsep)
    app.config['verbose'] = args.verbose
    app.config["CACHE_TYPE"] = "null"
    app.jinja_env.trim_blocks = True
    app.jinja_env.lstrip_blocks = True
    try:
        with GBD(app.config['database'], verbose=app.config['verbose']) as gbd:
            app.config['dbnames'] = gbd.get_databases()
            if Schema.IN_MEMORY_DB_NAME in app.config['dbnames']:
                app.config['dbnames'].remove(Schema.IN_MEMORY_DB_NAME)
            app.config['features_flat'] = gbd.get_features()
            for context in contexts.contexts():
                local = contexts.prepend_context("local", context)
                if local in app.config['features_flat']:
                    app.config['features_flat'].remove(local)
            app.config['dbpaths'] = dict()
            app.config['features'] = dict()
            for db in app.config['dbnames']:
                if db != 'main':
                    app.config['features'][db] = gbd.get_features(dbname=db)
                    for context in contexts.contexts():
                        local = contexts.prepend_context("local", context)
                        if local in app.config['features'][db]:
                            app.config['features'][db].remove(local)
                    app.config['dbpaths'][db] = gbd.get_database_path(db)
    except Exception as e:
        app.logger.error(str(e))
        exit(1)
    app.static_folder = os.path.join(pwd, "static")
    app.template_folder = os.path.join(pwd, "templates")
    #app.run(host='0.0.0.0', port=args.port)
    from waitress import serve
    serve(app, host="0.0.0.0", port=5000)


if __name__ == '__main__':
    main()
