from http import client


def establish_connection(url):
    try:
        return client.HTTPConnection(url)
    except client.HTTPException:
        return None
