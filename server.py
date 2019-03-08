import os
from threading import Thread
from zipfile import ZipInfo

from tatsu import exceptions

from main import htmlGenerator
from main.core import zipper, util
from main.core.database import groups, search
from main.core.database.db import Database
from sqlite3 import OperationalError

from flask import Flask, render_template, request, send_file

from os.path import realpath, dirname, join, isfile

from main.core.hashing import gbd_hash

app = Flask(__name__)

DATABASE = join(dirname(realpath(__file__)), 'local.db')
ZIPCACHE_PATH = 'zipcache'
ZIP_BUSY_PREFIX = '_'
MAX_HOURS_ZIP_FILES = None  # time in hours the ZIP file remain in the cache
MAX_MIN_ZIP_FILES = 30  # time in minutes the ZIP files remain in the cache
THRESHOLD_ZIP_SIZE = 5  # size in MB the server should zip at max


@app.route("/", methods={'GET'})
def welcome():
    return render_template('home.html')


@app.route("/query/form", methods=['GET'])
def query_form():
    return render_template('query_form.html')


@app.route("/query", methods=['POST'])  # query string über post
def query():
    response = htmlGenerator.generate_html_header("en")
    response += htmlGenerator.generate_head("Results")
    query = request.values.to_dict()["query"]
    with Database(DATABASE) as database:
        try:
            hashlist = search.find_hashes(database, query)
            response += htmlGenerator.generate_num_table_div(hashlist)
        except exceptions.FailedParse:
            response += htmlGenerator.generate_warning("Non-valid query")
        except OperationalError:
            response += htmlGenerator.generate_warning("Group not found")
    return response


@app.route("/queryzip", methods=['POST'])
def queryzip():
    response = htmlGenerator.generate_html_header("en")
    query = request.values.to_dict()["query"]
    with Database(DATABASE) as database:
        try:
            sorted_hash_set = sorted(search.find_hashes(database, query))
            if len(sorted_hash_set) != 0:
                if not os.path.isdir('{}'.format(ZIPCACHE_PATH)):
                    os.makedirs('{}'.format(ZIPCACHE_PATH))
                result_hash = gbd_hash.hash_hashlist(sorted_hash_set)
                zipfile_busy = ''.join('{}/_{}.zip'.format(ZIPCACHE_PATH, result_hash))
                zipfile_ready = zipfile_busy.replace(ZIP_BUSY_PREFIX, '')
                if isfile(zipfile_ready):
                    with open(zipfile_ready, 'a'):
                        os.utime(zipfile_ready, None)
                    util.delete_old_cached_files(ZIPCACHE_PATH, MAX_HOURS_ZIP_FILES, MAX_MIN_ZIP_FILES)
                    return send_file(zipfile_ready,
                                     attachment_filename='benchmarks.zip',
                                     as_attachment=True)
                elif not isfile(zipfile_busy):
                    util.delete_old_cached_files(ZIPCACHE_PATH, MAX_HOURS_ZIP_FILES, MAX_MIN_ZIP_FILES)
                    files = []
                    for h in sorted_hash_set:
                        files.append(search.resolve(database, 'benchmarks', h))
                    size = 0
                    for file in files:
                        zf = ZipInfo.from_file(*file, arcname=None)
                        size += zf.file_size
                    divisor = 1024 << 10
                    if size/divisor < THRESHOLD_ZIP_SIZE:
                        thread = Thread(target=zipper.create_zip_with_marker,
                                        args=(zipfile_busy, files, ZIP_BUSY_PREFIX))
                        thread.start()
                        return htmlGenerator.generate_zip_busy_page(zipfile_busy, float(round(size/divisor, 2)))
                    else:
                        response += '<hr>' \
                                    '{}'.format(htmlGenerator.generate_warning("ZIP too large (size >{} MB)")
                                                .format(THRESHOLD_ZIP_SIZE))
        except exceptions.FailedParse:
            response += '<hr>'
            response += htmlGenerator.generate_warning("Non-valid query")
        except OperationalError:
            response += '<hr>'
            response += htmlGenerator.generate_warning("Group not found")
    return response


@app.route("/resolve/form", methods=['GET'])
def resolve_form():
    return render_template('resolve_form.html')


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


@app.route("/groups/all", methods=['GET'])
def reflect():
    response = htmlGenerator.generate_html_header('en')
    url = '/static/resources/gbd_logo_small.png'
    response += "<body>" \
                "<nav class=\"navbar navbar-expand-lg navbar-dark bg-dark\">" \
                "   <a href=\"/\" class=\"navbar-left\"><img style=\"max-width:50px\" src=\"{}\"></a>" \
                "   <a class=\"navbar-brand\" href=\"#\"></a>" \
                "   <button class=\"navbar-toggler\" type=\"button\" data-toggle=\"collapse\" " \
                "       data-target=\"#navbarNavAltMarkup\"" \
                "       aria-controls=\"navbarNavAltMarkup\" " \
                "       aria-expanded=\"false\"" \
                "       aria-label=\"Toggle navigation\">" \
                "       <span class=\"navbar-toggler-icon\"></span>" \
                "   </button>" \
                "   <div class=\"collapse navbar-collapse\" id=\"navbarNavAltMarkup\">" \
                "       <div class=\"navbar-nav\">" \
                "           <a class=\"nav-item nav-link\" href=\"/\">Home</a>" \
                "           <a class=\"nav-item nav-link active\" href=\"#\">Groups" \
                "                   <span class=\"sr-only\">(current)</span></a>" \
                "           <a class=\"nav-item nav-link\" href=\"/query/form\">Search</a>" \
                "           <a class=\"nav-item nav-link\" href=\"/resolve/form\">Resolve</a>" \
                "       </div>" \
                "   </div>" \
                "</nav>" \
                "<hr>".format(url)
    with Database(DATABASE) as database:
        list = groups.reflect(database)
        response += htmlGenerator.generate_num_table_div(list)
        return response


@app.route("/groups/reflect", methods=['GET'])
def reflect_group():
    with Database(DATABASE) as database:
        try:
            group = request.args.get('group')
            list = ["Name: {}".format(group),
                    "Type: {}".format(groups.reflect_type(database, group)),
                    "Unique: {}".format(groups.reflect_unique(database, group)),
                    "Default: {}".format(groups.reflect_default(database, group)),
                    "Size: {}".format(groups.reflect_size(database, group))]
            return list.__str__()
        except IndexError:
            return "Group not found"


@app.route("/zips/busy", methods=['GET'])
def get_zip():
    zipfile = request.args.get('file')
    if isfile(zipfile.replace(ZIP_BUSY_PREFIX, '')):
        return send_file(zipfile.replace(ZIP_BUSY_PREFIX, ''), attachment_filename='benchmarks.zip', as_attachment=True)
    elif not isfile('_{}'.format(zipfile)):
        return htmlGenerator.generate_zip_busy_page(zipfile)
