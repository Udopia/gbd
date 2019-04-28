from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from flask import json


# Get url, and dictionaries of parameters and headers with which then form a get request
def get_request(url, params, headers):
    params = urlencode(params).encode()
    parsed_url = urlparse(url)
    if parsed_url.scheme != 'https' or parsed_url.scheme != 'http':
        url = 'http://{}?{}'.format(url, params)
    request = Request(url, data=None, headers=headers, method='GET')
    return json.loads(urlopen(request).read().decode())


# Get url and dictionaries of parameters and headers with which then form a post request
def post_request(url, params, headers):
    parsed_url = urlparse(url)
    if parsed_url.scheme != 'https' or parsed_url.scheme != 'http':
        url = 'http://{}'.format(url)
    request = Request(url, data=urlencode(params).encode(), headers=headers, method='POST')
    return json.loads(urlopen(request).read().decode())


# return true if given string represents a valid URL
def is_url(url):
    try:
        urlparse(url)
        return True
    except ValueError:
        return False
