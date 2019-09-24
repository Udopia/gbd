from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from flask import json

USER_AGENT_CLI = "gbd_tool-cli"

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
    print(url)
    parsed_url = urlparse(url)
    print(parsed_url)
    if parsed_url.scheme != 'https' and parsed_url.scheme != 'http':
        url = 'http://{}'.format(url)
    print(url)
    request = Request(url, data=urlencode(params).encode(), headers=headers, method='POST')
    return json.loads(urlopen(request).read().decode())


# return true if given string represents a valid URL
def is_url(url):
    try:
        tuple = urlparse(url)
        return len(tuple[0]) > 0 and len(tuple[1]) > 0
    except ValueError:
        return False


def get_user_agent():
    return ''.join(USER_AGENT_CLI)