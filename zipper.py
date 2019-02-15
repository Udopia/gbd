import os
from os.path import basename
from zipfile import ZipFile


def create_zip_with_marker(zipfile, files, prefix):
    with ZipFile(zipfile, 'w') as zf:
        for file in files:
            zf.write(*file, basename(*file))
    zf.close()
    os.rename(zipfile, zipfile.replace(prefix, ''))
