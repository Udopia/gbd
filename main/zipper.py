from os.path import basename
from zipfile import ZipFile

from main.core.database import search


def create_zip_with_marker(zipfile, hashlist, database, prefix):
    name = ''.join(zipfile)
    with ZipFile(name, 'w') as zf:
        for h in hashlist:
            file = search.resolve(database, "benchmarks", h)
            zf.write(*file, basename(*file))
    zf.close()
    zf.filename = name.strip(prefix)
