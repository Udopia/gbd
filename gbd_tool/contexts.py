from gbd_tool.gbd_api import GBD, GBDException

from gbd_tool import config

def merge(api: GBD, source, target):
    if not source in config.contexts():
        raise GBDException("source context not found: " + source)
    if not target in config.contexts():
        raise GBDException("target context not found: " + target)
    