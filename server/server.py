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
from os.path import basename, isfile
from zipfile import ZipFile, ZipInfo

import interface
import tatsu
import util
from flask import Flask, render_template, request, send_file, json
from flask.logging import default_handler
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from gbd_tool.gbd_api import GbdApi
from gbd_tool.hashing import gbd_hash
from gbd_tool.http_client import USER_AGENT_CLI
from tatsu import exceptions
from werkzeug.middleware.proxy_fix import ProxyFix

limiter = None
DATABASE = None
gbd_api = None
app = Flask(__name__, static_folder="static", template_folder="templates")

CACHE_PATH = 'cache'
MAX_HOURS = None  # time in hours the ZIP file remain in the cache
MAX_MINUTES = 1  # time in minutes the ZIP files remain in the cache
THRESHOLD_ZIP_SIZE = 5  # size in MB the server should zip at max
ZIP_SEMAPHORE = threading.Semaphore(4)
ZIP_PREFIX = '_'

CSV_FILE_NAME = 'gbd'
standard_attribute = interface.standard_attribute
request_semaphore = threading.Semaphore(10)
csv_mutex = threading.Semaphore(1)  # shall stay a mutex - don't edit
zip_mutex = threading.Semaphore(1)  # shall stay a mutex - don't edit


def create_app(application, db):
    logging.basicConfig(filename='server.log', level=logging.DEBUG)
    logging.getLogger().addHandler(default_handler)
    application.wsgi_app = ProxyFix(application.wsgi_app, num_proxies=1)
    if db is None:
        fallback = os.environ.get('GBD_DB')
        if fallback is None:
            raise RuntimeError("No database given.")
        else:
            application.config['database'] = fallback
    else:
        application.config['database'] = db
    global DATABASE
    DATABASE = application.config['database']
    global limiter
    limiter = Limiter(application, key_func=get_remote_address)
    global gbd_api
    gbd_api = GbdApi(interface.SERVER_CONFIG_PATH, DATABASE)
    return application


@app.route("/", methods=['GET'])
def quick_search():
    request_semaphore.acquire()
    all_groups = get_group_tuples()
    request_semaphore.release()
    return render_template('quick_search.html', groups=all_groups, is_result=False, has_query=False)


def get_group_tuples():
    group_list = []
    all_groups = gbd_api.get_all_groups()
    for group in all_groups:
        group_name = group.__str__()
        is_system_table = re.match('_{2}.*', group_name)
        if not is_system_table:
            group_list.append([group, False])
    all_groups = sorted(group_list)
    return all_groups


@app.route("/results", methods=['POST'])
def quick_search_results():
    request_semaphore.acquire()
    q = request.values.get('query')
    all_groups = get_group_tuples()
    checked_groups = request.values.getlist('groups')
    groups_list = []
    for group_tuple in all_groups:
        if group_tuple[0] in checked_groups:
            groups_list.append([group_tuple[0], True])
        else:
            groups_list.append([group_tuple[0], False])
    all_groups = groups_list
    try:
        if len(checked_groups) == 0:
            if q != "":
                results = list(gbd_api.query_search(query=q, resolve=[standard_attribute], collapse=False))
            else:
                results = list(gbd_api.query_search(query=None, resolve=[standard_attribute], collapse=False))
        else:
            if q == "":
                results = list(gbd_api.query_search(query=None, resolve=checked_groups, collapse=False))
            else:
                results = list(gbd_api.query_search(query=q, resolve=checked_groups, collapse=False))
        request_semaphore.release()
        return render_quick_search(
            groups=all_groups,
            is_result=True,
            results=results,
            results_json=json.dumps(results),
            checked_groups=checked_groups,
            checked_groups_json=json.dumps(checked_groups),
            contains_error=False, error_message=None,
            has_query=True,
            query=q)
    except tatsu.exceptions.FailedParse:
        request_semaphore.release()
        return render_quick_search(
            groups=all_groups,
            is_result=True,
            results=None,
            results_json=None,
            checked_groups=checked_groups, checked_groups_json=json.dumps(checked_groups),
            contains_error=True,
            error_message="Whoops! Non-valid query...",
            has_query=True,
            query=q)
    except ValueError:
        request_semaphore.release()
        return render_quick_search(
            groups=all_groups,
            is_result=True,
            results=None,
            results_json=None,
            checked_groups=checked_groups, checked_groups_json=json.dumps(checked_groups),
            contains_error=True,
            error_message="Whoops! Something went wrong...",
            has_query=True,
            query=q)


def render_quick_search(groups,
                        is_result, results, results_json,
                        checked_groups, checked_groups_json,
                        contains_error, error_message,
                        has_query, query):
    return render_template('quick_search_content.html',
                           groups=groups,
                           is_result=is_result,
                           results=results,
                           results_json=results_json,
                           checked_groups=checked_groups,
                           checked_groups_json=checked_groups_json,
                           contains_error=contains_error,
                           error_message=error_message,
                           has_query=has_query,
                           query=query)


@app.route("/exportcsv", methods=['POST'])
def get_csv_file():
    request_semaphore.acquire()
    checked_groups = json.loads(request.values.get('checked_groups'))
    results = json.loads(request.values.get('results'))
    if len(results) == 0:
        return render_quick_search(
            groups=get_group_tuples(),
            is_result=True,
            results=None,
            results_json=None,
            checked_groups=checked_groups, checked_groups_json=json.dumps(checked_groups),
            contains_error=True,
            error_message="You don't want empty CSV files, believe me.",
            has_query=True,
            query=request.values.get('query'))
    csv_mutex.acquire()
    csv_file = create_csv_file(checked_groups, results)
    csv_mutex.release()
    request_semaphore.release()
    app.logger.info('Sent file {} to {} at {}'.format(csv_file, request.remote_addr,
                                                      datetime.datetime.now()))
    return send_file(csv_file, attachment_filename='{}.csv'.format(CSV_FILE_NAME), as_attachment=True)


def create_csv_file(checked_groups, results):
    if not os.path.isdir('{}'.format(CACHE_PATH)):
        os.makedirs('{}'.format(CACHE_PATH))
    postfix = '{}'.format(random.randint(1, 10000))
    csv = CSV_FILE_NAME + postfix
    csv_file = ''.join('{}/{}.csv'.format(CACHE_PATH, csv))
    f = open(csv_file, 'w')
    f.write(util.create_csv_string(checked_groups, results))
    f.close()
    util.delete_old_cached_files(CACHE_PATH, MAX_HOURS, MAX_MINUTES)
    return csv_file


@app.route("/getzip", methods=['POST'])
def get_zip_file():
    request_semaphore.acquire()
    query = request.values.get('query')
    checked_groups = request.values.get('checked_groups')
    result = sorted(gbd_api.query_search(query, collapse=True))
    if len(result) == 0:
        return render_quick_search(
            groups=get_group_tuples(),
            is_result=True,
            results=None,
            results_json=None,
            checked_groups=checked_groups, checked_groups_json=json.dumps(checked_groups),
            contains_error=True,
            error_message="You don't want empty ZIP files, believe me.",
            has_query=True,
            query=query)
    if not os.path.isdir('{}'.format(CACHE_PATH)):
        os.makedirs('{}'.format(CACHE_PATH))
    hash_list = []
    benchmark_files = []
    for result_tuple in result:
        hash_list.append(result_tuple[0])
        benchmark_files.append(result_tuple[1])
    result_hash = gbd_hash.hash_hashlist(sorted(list(hash_list)))
    zipfile_busy = ''.join('{}/{}{}.zip'.format(CACHE_PATH, ZIP_PREFIX, result_hash))
    zipfile_ready = zipfile_busy.replace(ZIP_PREFIX, '')

    zip_mutex.acquire()
    if isfile(zipfile_ready):
        with open(zipfile_ready, 'a'):
            os.utime(zipfile_ready, None)
        util.delete_old_cached_files(CACHE_PATH, MAX_HOURS, MAX_MINUTES)
        zip_mutex.release()
        request_semaphore.release()
        app.logger.info('Sent file {} to {} at {}'.format(zipfile_ready, request.remote_addr,
                                                          datetime.datetime.now()))
        return send_file(zipfile_ready,
                         attachment_filename='benchmarks.zip',
                         as_attachment=True)
    elif not isfile(zipfile_busy):
        size = 0
        for file in benchmark_files:
            zf = ZipInfo.from_file(file, arcname=None)
            size += zf.file_size
        divisor = 1024 << 10
        if size / divisor < THRESHOLD_ZIP_SIZE:
            create_zip(zipfile_ready, benchmark_files, ZIP_PREFIX)
            zip_mutex.release()
            request_semaphore.release()
            return send_file(zipfile_ready,
                             attachment_filename="benchmarks.zip",
                             as_attachment=True)
        else:
            request_semaphore.release()
            return render_quick_search(
                groups=get_group_tuples(),
                is_result=True,
                results=None,
                results_json=None,
                checked_groups=checked_groups, checked_groups_json=json.dumps(checked_groups),
                contains_error=True,
                error_message="The ZIP file is too large (more than {} MB)".format(THRESHOLD_ZIP_SIZE),
                has_query=True,
                query=query)
    else:
        zip_mutex.release()
        request_semaphore.release()
        return get_zip_file()


def create_zip(zipfile, zip_files, prefix):
    ZIP_SEMAPHORE.acquire()
    with ZipFile(zipfile, 'w') as zf:
        for file in zip_files:
            zf.write(file, basename(file))
    zf.close()
    os.rename(zipfile, zipfile.replace(prefix, ''))
    ZIP_SEMAPHORE.release()


@app.route("/query", methods=['POST'])  # query string post
def query_for_cli():
    request_semaphore.acquire()
    query = request.values.get('query')
    ua = request.headers.get('User-Agent')
    if ua == USER_AGENT_CLI:
        if query == 'None':
            hashset = gbd_api.query_search()
        else:
            try:
                hashset = gbd_api.query_search(query)
            except tatsu.exceptions.FailedParse:
                return "Illegal query"
        response = []
        for hash in hashset:
            response.append(hash)
        request_semaphore.release()
        return json.dumps(response)
    else:
        request_semaphore.release()
        return "Not allowed"


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('-d')
    args = parser.parse_args()
    database = args.d
    app = create_app(app, database)
    app.run(host='0.0.0.0')
