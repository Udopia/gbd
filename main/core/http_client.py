from http import client
from urllib.parse import urlparse


def establish_connection(url):
    try:
        if is_url(url):
            parsed_url = urlparse(url)
            if parsed_url.scheme == 'https':
                connection = client.HTTPSConnection(parsed_url.netloc, parsed_url.port)
                connection.connect()
            else:
                connection = client.HTTPConnection(parsed_url.netloc, parsed_url.port)
                connection.connect()
            return connection
    except client.HTTPException:
        return None


def get_request(host, url):
    connection = establish_connection(host)
    connection.request("GET", url)
    response = connection.getresponse()
    connection.close()
    return response.read()


def post_request(host, url, params, headers):
    connection = establish_connection(host)
    connection.request("POST", url, params, headers)
    response = connection.getresponse()
    connection.close()
    return response.read()


def is_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False
