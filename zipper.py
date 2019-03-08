import os
import server
from os.path import basename
from zipfile import ZipFile


def create_zip_with_marker(zipfile, files, prefix):
    server.ZIP_SEMAPHORE.acquire()
    with ZipFile(zipfile, 'w') as zf:
        for file in files:
            zf.write(*file, basename(*file))
    zf.close()
    os.rename(zipfile, zipfile.replace(prefix, ''))
    server.ZIP_SEMAPHORE.release()
