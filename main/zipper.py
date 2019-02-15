from os.path import basename
from zipfile import ZipFile

from main.core.database import search


def create_zip(zipfile, hashlist, database):
    with ZipFile(zipfile, 'w') as zf:
        for h in hashlist:
            file = search.resolve(database, "benchmarks", h)
            zf.write(*file, basename(*file))
    zf.close()
