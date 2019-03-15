from http import client


def establish_connection(url):
    try:
        return client.HTTPSConnection(url)
    except client.HTTPException:
        return None
