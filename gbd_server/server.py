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

from logging.handlers import TimedRotatingFileHandler
import os
import re

import flask
import logging
import waitress
from werkzeug.middleware.proxy_fix import ProxyFix

from gbd_core.database import DatabaseException
from gbd_core.api import GBD, GBDException
from gbd_core.grammar import ParserException
from gbd_core.util import is_number

app = flask.Flask(__name__)

def request_query(request):
    query = ""
    if "query" in request.values:
        query = request.values.get('query')
    elif len(request.args) > 0:
        query = " and ".join(["{}={}".format(key, value) for (key, value) in request.args.items() if key != "context"])
    return query

def request_database(request):
    if "selected_db" in request.values and request.values.get('selected_db') in app.config['dbnames']:
        dbname = request.values.get('selected_db')
        context = request_context(request)
        if dbname in [ GBD.get_database_name(c) for c in app.config['contextdbs'][context] ]:
            return dbname
        else:
            return GBD.get_database_name(app.config['contextdbs'][context][0])
    else:
        return app.config['dbnames'][0]

def request_page(request):
    return int(request.values.get('page')) if "page" in request.values else 0

def request_action(request):
    return request.values.get('action') if "action" in request.values else "default"

def request_context(request):
    return request.values.get('context') if "context" in request.values else "cnf"


def query_to_name(query):
    return re.sub(r'[^\w]', '_', query) if query else "allinstances"


def error_response(msg, addr, errno=404):
    app.logger.error("{}: {}".format(addr, msg))
    return flask.Response(msg, status=errno, mimetype="text/plain")

def file_response(text_blob, filename, mimetype, addr):
    app.logger.info("{}: Sending generated file {}".format(addr, filename))
    return flask.Response(text_blob, mimetype=mimetype, headers={"Content-Disposition": "attachment; filename=\"{}\"".format(filename), "filename": filename})

def path_response(path, filename, mimetype, addr):
    app.logger.info("{}: Sending file {}".format(addr, path))
    return flask.send_file(path, as_attachment=True, download_name=filename, mimetype=mimetype)

def json_response(json_blob, msg, addr):
    app.logger.info("{}: {}".format(addr, msg))
    return flask.Response(json_blob, status=200, mimetype="application/json")

def page_response(context, query, database, page=0):
    with GBD(app.config['contextdbs'][context]) as gbd:
        start = page * 1000
        end = start + 1000
        error = None
        try:
            df = gbd.query(query, resolve=['{}:{}'.format(database, f) for f in app.config["features"][database]], collapse="GROUP_CONCAT")
        except GBDException as err:
            error = "GBDException: {}".format(str(err))
        except DatabaseException as err:
            error = "DatabaseException: {}".format(str(err))
        except ParserException as err:
            error = "ParserException: {}".format(str(err))
        except Exception as err:
            error = "An Unhandled Exception Occurred"
        return flask.render_template('index.html', 
            context=context,
            error=error,
            contexts=app.config['contexts'],
            query=query,
            query_name=query_to_name(query), 
            result=df.iloc[start:end, :].values.tolist() if error is None else [], 
            total=len(df.index) if error is None else 0,
            page=page,
            pages=int(len(df.index) / 1000) if error is None else 0,
            selected=database, 
            features=app.config["features"][database],
            databases=[ gbd.get_database_name(db) for db in app.config["contextdbs"][context] ],
            action=request_action(flask.request))


# Returns main index page
@app.route("/", methods=['POST', 'GET'])
def quick_search():
    context = request_context(flask.request)
    query = request_query(flask.request)
    database = request_database(flask.request)
    context_databases = [ GBD.get_database_name(db) for db in app.config["contextdbs"][context] ]
    if not database in context_databases:
        database = context_databases[0]
    page = request_page(flask.request)
    return page_response(context, query, database, page)


# Generates a list of URLs. Given query (text field of POST form) is executed and the hashes of the result are resolved
# against the filename feature. Every filename is associated with a URL to enable flexible downloading of these files
@app.route("/getinstances/", methods=['POST', 'GET'])
@app.route("/getinstances", methods=['POST', 'GET'])
def get_url_file():
    context = request_context(flask.request)
    with GBD(app.config['contextdbs'][context]) as gbd:
        query = request_query(flask.request)
        try:
            df = gbd.query(query)
        except (GBDException, DatabaseException, ParserException) as err:
            return error_response("{}, {}".format(type(err), str(err)), flask.request.remote_addr, errno=500)
        if context == "cnf":
            content = "\n".join([ flask.url_for("get_file", hashvalue=val, _external=True) for val in df['hash'].tolist() ])
        else:
            content = "\n".join([ flask.url_for("get_file", hashvalue=val, context=context, _external=True) for val in df['hash'].tolist() ])
        return file_response(content, query_to_name(query) + ".uri", "text/uri-list", flask.request.remote_addr)


# Send database file
@app.route('/getdatabase/')
@app.route('/getdatabase')
@app.route('/getdatabase/<database>/')
@app.route('/getdatabase/<database>')
def get_database_file(database=None):
    dbname=database if database and database in app.config['dbnames'] else app.config['dbnames'][0]
    dbpath=app.config['dbpaths'][dbname]
    return path_response(dbpath, os.path.basename(dbpath), 'application/x-sqlite3', flask.request.remote_addr)


# Find the file corresponding to the hashvalue and send it to the client
@app.route('/file/<hashvalue>/')
@app.route('/file/<hashvalue>')
def get_file(hashvalue):
    context = request_context(flask.request)
    print(context, app.config['contextdbs'][context])
    with GBD(app.config['contextdbs'][context]) as gbd:
        df = gbd.query(hashes=[hashvalue], resolve=['local', 'filename'], collapse="MIN")
        if not len(df.index):
            return error_response("Hash '{}' not found".format(hashvalue), flask.request.remote_addr)
        row = df.iloc[0]
        if not os.path.exists(row['local']):
            return error_response("Files temporarily not accessible", flask.request.remote_addr)
        return path_response(row['local'], row['hash'] + "-" + row['filename'], 'application/x-xz', flask.request.remote_addr)


# start the server
def serve(gbd: GBD, port: int = 5000, logdir: str = "/tmp"):
    formatter = logging.Formatter(fmt='[%(asctime)s, %(name)s, %(levelname)s] %(module)s.%(filename)s.%(funcName)s():%(lineno)d\n%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logging.getLogger().setLevel(logging.DEBUG)
    # Add sys.stdout to logging output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(console_handler)
    # Add handler to write in rotating logging files
    file_handler = TimedRotatingFileHandler(logdir + "/trfile.log", when="midnight", backupCount=10)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.WARNING)
    logging.getLogger().addHandler(file_handler)

    global app
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1)
    
    app.jinja_env.trim_blocks = True
    app.jinja_env.lstrip_blocks = True
    
    app.jinja_env.tests['link_field'] = lambda field: field is not None and field.startswith("http")
    app.jinja_env.tests['num_field'] = lambda field: field is not None and is_number(field)
    app.jinja_env.tests['int_field'] = lambda field: field is not None and field.isnumeric()

    path = os.path.dirname(__file__)
    app.static_folder = os.path.join(path, "static")
    app.template_folder = os.path.join(path, "templates")

    app.config['contexts'] = gbd.get_contexts()
    app.config['dbnames'] = gbd.get_databases()
    # group databases by context
    app.config['contextdbs'] = dict()
    for ctxt in app.config['contexts']:
        app.config['contextdbs'][ctxt] = [ gbd.get_database_path(c) for c in gbd.get_databases(ctxt) ]
    # group features by database
    app.config['dbpaths'] = dict()
    app.config['features'] = dict()
    for db in app.config['dbnames']:
        app.config['features'][db] = [ f for f in gbd.get_features(db) if not f in [ "hash", "local" ] ]
        app.config['dbpaths'][db] = gbd.get_database_path(db)
    app.config['features_flat'] = [ f for f in gbd.get_features() if not f in [ "hash", "local" ] ]

    waitress.serve(app, host='0.0.0.0', port=port)
