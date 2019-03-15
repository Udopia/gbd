from http import client
from urllib.parse import urlparse


def establish_connection(url):
    # TODO: replace magic numbers and strings
    try:
        if is_url(url):
            print(urlparse(url))
            parsed_url = urlparse(url)
            if parsed_url.scheme == 'https':
                connection = client.HTTPSConnection("localhost", 5000)
                connection.connect()
            else:
                connection = client.HTTPConnection("localhost", 5000)
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
    print("{}{}".format(host, url))
    connection = establish_connection(host)
    connection.request("POST", url, params, headers)
    response = connection.getresponse()
    connection.close()
    return response.read()


def is_url(url):
    try:
        urlparse(url)
        return True
    except ValueError:
        return False
