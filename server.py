#!/usr/bin/python3

# MIT License

# Copyright (c) 2023 Markus Iser, Karlsruhe Institute of Technology (KIT)

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

import os
import re
import argparse
import traceback
import logging

import flask
from flask import Flask, request, send_file, json, Response
from flask import render_template

from logging.handlers import TimedRotatingFileHandler

from werkzeug.middleware.proxy_fix import ProxyFix

from gbd_core import contexts
from gbd_core.database import DatabaseException
from gbd_core.schema import Schema
from gbd_core.api import GBD, GBDException
from gbd_core.grammar import ParserException

import gbd_server

app = Flask(__name__)


def request_query(request):
    query = ""
    if "query" in request.form:
        query = request.form.get('query')
    elif len(request.args) > 0:
        query = " and ".join(["{}={}".format(key, value) for (key, value) in request.args.items()])
    return query

def request_database(request):
    if "selected_db" in request.form and request.form.get('selected_db') in app.config['dbnames']:
        return request.form.get('selected_db')
    else:
        return app.config['dbnames'][0]

def request_page(request):
    return int(request.form.get('page')) if "page" in request.form else 0

def request_action(request):
    return request.form.get('action') if "action" in request.form else "default"


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

def page_response(query, database, page=0):
    with GBD(app.config['database'], verbose=app.config['verbose']) as gbd:
        start = page * 1000
        end = start + 1000
        try:
            df = gbd.query(query, resolve=app.config["features"][database], collapse="MIN")
            #for col in df.columns:
            #    df[col] = df[col].apply(lambda x: round(float(x), 2) if util.is_number(x) and '.' in x else x)
        except (GBDException, DatabaseException, ParserException) as err:
            return error_response("{}, {}".format(type(err), str(err)), request.remote_addr, errno=500)
        return render_template('index.html', 
            query=query, 
            result=df.iloc[start:end, :].values.tolist(), 
            total=len(df.index),
            page=page,
            pages=int(len(df.index) / 1000),
            selected=database, 
            features=app.config["features"][database],
            databases=app.config["dbnames"],
            action=request_action(request))


# Returns main index page
@app.route("/", methods=['POST', 'GET'])
def quick_search():
    query = request_query(request)
    database = request_database(request)
    page = request_page(request)
    return page_response(query, database, page)


# Expects POST form with a query as text input and selected features as checkbox inputs,
# sends csv version of the result as a file
@app.route("/exportcsv/", methods=['POST', 'GET'])
def get_csv_file():
    with GBD(app.config['database'], verbose=app.config['verbose']) as gbd:
        query = request_query(request)
        db = request_database(request)
        features = app.config['features'][db]
        try:
            df = gbd.query(query, [], features)
        except (GBDException, DatabaseException, ParserException) as err:
            return error_response("{}, {}".format(type(err), str(err)), request.remote_addr, errno=500)
        return file_response(df.to_csv(), query_to_name(query) + ".csv", "text/csv", request.remote_addr)


# Generates a list of URLs. Given query (text field of POST form) is executed and the hashes of the result are resolved
# against the filename feature. Every filename is associated with a URL to enable flexible downloading of these files
@app.route("/getinstances/", methods=['POST', 'GET'])
def get_url_file():
    with GBD(app.config['database'], verbose=app.config['verbose']) as gbd:
        query = request_query(request)
        try:
            df = gbd.query(query)
        except (GBDException, DatabaseException, ParserException) as err:
            return error_response("{}, {}".format(type(err), str(err)), request.remote_addr, errno=500)
        content = "\n".join([ flask.url_for("get_file", hashvalue=val, _external=True) for val in df['hash'].tolist() ])
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
def get_file(hashvalue):
    with GBD(app.config['database'], verbose=app.config['verbose'])as gbd:
        df = gbd.query(hashes=[hashvalue], resolve=['local', 'filename'], collapse="MIN")
        if not len(df.index):
            return error_response("Hash '{}' not found".format(hashvalue), request.remote_addr)
        row = df.iloc[0]
        if not os.path.exists(row['local']):
            return error_response("Files temporarily not accessible", request.remote_addr)
        return path_response(row['local'], row['hash'] + "-" + row['filename'], 'text/plain', request.remote_addr)
        

# Resolves a hashvalue against a attribute and returns the result values
@app.route('/attribute/<feature>/<hashvalue>')
def get_attribute(feature, hashvalue):
    app.logger.info("Resolving '{}' with feature '{}' for IP {}".format(hashvalue, feature, request.remote_addr))
    with GBD(app.config['database'], verbose=app.config['verbose']) as gbd:
        try:
            df = gbd.query(hashes=[hashvalue], resolve=[feature])
            if not len(df.index):
                return error_response("Hash '{}' not found".format(hashvalue), request.remote_addr)
            return df.head(1)[feature]
        except (GBDException, DatabaseException) as err:
            return error_response("{}, {}".format(type(err), str(err)), request.remote_addr, errno=500)


# Get all attributes associated with the hashvalue (resolving against all features)
@app.route('/info/<hashvalue>/')
def get_all_attributes(hashvalue):
    app.logger.info("Listing all attributes of hashvalue {} for IP {}".format(hashvalue, request.remote_addr))
    with GBD(app.config['database'], verbose=app.config['verbose']) as gbd:
        features = app.config['features_flat']
        try:
            df = gbd.query(hashes=[hashvalue], resolve=features)
            if not len(df.index):
                return error_response("Hash '{}' not found".format(hashvalue), request.remote_addr)
            return json_response(json.dumps(dict(zip(['hash'] + features, df.head(0)))), "Sending list of attributes", request.remote_addr)
        except (GBDException, DatabaseException) as err:
            return error_response("{}, {}".format(type(err), str(err)), request.remote_addr, errno=500)


# Main method which configures Flask app at startup
def main():
    pwd=os.path.dirname(os.path.abspath(gbd_server.__file__))
    parser = argparse.ArgumentParser(description='Web- and Micro- Services to access global benchmark database.')
    parser.add_argument('-d', "--db", help='Specify database to work with', default=os.environ.get('GBD_DB'), nargs='?')
    parser.add_argument('-l', "--logdir", help='Specify logging dir', default=os.environ.get('GBD_LOGGING_DIR') or pwd, nargs='?')
    parser.add_argument('-p', "--port", help='Specify port on which to listen', default=5000, type=int)
    parser.add_argument('-v', "--verbose", help='Verbose Mode', action='store_true')
    parser.add_argument('-c', '--context', default='cnf', choices=contexts.contexts(), help='Select context')
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
    app.config['context'] = args.context
    app.jinja_env.trim_blocks = True
    app.jinja_env.lstrip_blocks = True

    try:
        with GBD(app.config['database'], verbose=app.config['verbose']) as gbd:
            app.config['dbnames'] = [ db for db in gbd.get_databases() if db != Schema.IN_MEMORY_DB_NAME ]
            app.config['features_flat'] = [ f for f in gbd.get_features() if not f in [ "hash", "local" ] ]
            app.config['dbpaths'] = dict()
            app.config['features'] = dict()
            for db in app.config['dbnames']:
                app.config['features'][db] = [ f for f in gbd.get_features(db) if not f in [ "hash", "local" ] ]
                app.config['dbpaths'][db] = gbd.get_database_path(db)
    except Exception as e:
        app.logger.error(str(e))
        print(traceback.format_exc())
        exit(1)

    app.static_folder = os.path.join(pwd, "static")
    app.template_folder = os.path.join(pwd, "templates")
    from waitress import serve  
    serve(app, host='0.0.0.0', port=args.port)


if __name__ == '__main__':
    main()
