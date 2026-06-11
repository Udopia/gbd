# MIT License

# Copyright (c) 2025 Ashlin Iser, Karlsruhe Institute of Technology (KIT)

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

import sqlite3
import typing

from pprint import pprint

from gbd_core.util import eprint
from gbd_core.schema import Schema, FeatureInfo
from gbd_core import contexts


class DatabaseException(Exception):
    """Raised for database-level errors such as missing features, name collisions,
    or unsupported SQLite operations."""
    pass


class Database:
    """Manages multiple ATTACHed SQLite databases as a single virtual feature namespace.

    One or more ``.db`` files (and/or CSV files loaded into shared in-memory SQLite)
    are ATTACHed to a central in-memory connection.  This allows cross-database SQL JOINs
    to work transparently within a single cursor.

    **Data model**

    Each attached database centres on a ``features`` table::

        features(hash UNIQUE NOT NULL, feat_a TEXT DEFAULT 'v', feat_b TEXT DEFAULT 'w', ...)

    * **1:1 features** (``FeatureInfo.default != None``): stored as columns in ``features``;
      each hash has exactly one value.
    * **1:n features** (``FeatureInfo.default is None``): stored in a separate table::

          {name}(hash TEXT NOT NULL, value TEXT NOT NULL, UNIQUE(hash, value))

      A same-named column in ``features`` echoes the hash value as a foreign-key reference
      so the table is reachable via a JOIN.  A sentinel row ``(hash='None', value='None')``
      is inserted at creation time (see ``Issues.md`` #7).

    **Feature precedence**

    When the same feature name exists in multiple databases, the database that appears first
    in *path_list* takes precedence.  Queries can bypass precedence by using the
    ``database:feature`` or ``context:feature`` syntax.

    Usage::

        with Database(["cnf_sc2021.db", "gate_sc2021.db"]) as db:
            sql = GBDQuery(db, "vars > 1000").build_query(
                resolve=["local"], collapse="group_concat"
            )
            rows = db.query(sql)
    """

    def __init__(self, path_list: list, verbose=False, autocommit=True):
        """
        Args:
            path_list (list[str]): Ordered list of paths to ``.db`` or CSV files.
                The first entry becomes the default database for write operations.
                Ordering also determines feature precedence when names collide.
            verbose (bool): Print every executed SQL statement to stderr.
            autocommit (bool): Commit after every :py:meth:`execute` call.  Set to
                ``False`` for batched writes and call :py:meth:`commit` manually.
        """
        self.verbose = verbose
        self.schemas = self.init_schemas(path_list)
        self.features = self.init_features()
        self.connection = sqlite3.connect("file::memory:?cache=shared", uri=True, timeout=10)
        self.cursor = self.connection.cursor()
        self.maindb = None
        self.autocommit = autocommit
        schema: Schema
        for schema in self.schemas.values():
            if not schema.is_in_memory():
                self.execute("ATTACH DATABASE '{}' AS {}".format(schema.path, schema.dbname))
            else:
                self.execute("ATTACH DATABASE 'file:{}?mode=memory&cache=shared' AS {}".format(schema.dbname, schema.dbname))
            # first database is the default database:
            if not self.maindb:
                self.maindb = schema.dbname

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.connection.commit()
        self.connection.close()

    # returns major version of sqlite3 as float
    @classmethod
    def sqlite3_version(cls):
        return float(sqlite3.sqlite_version.rsplit(".", 1)[0])

    def init_schemas(self, path_list) -> typing.Dict[str, Schema]:
        """Load each path as a :py:class:`~gbd_core.schema.Schema` and return a mapping
        of logical database name → Schema.

        In-memory schemas (CSV sources) sharing the same database name are merged via
        :py:meth:`~gbd_core.schema.Schema.absorb`.  On-disk databases with colliding
        names raise :py:exc:`DatabaseException`.

        Args:
            path_list (list[str]): Paths to ``.db`` or CSV files.

        Returns:
            dict[str, Schema]: Ordered mapping of database name to Schema instance.
        """
        result = dict()
        for path in path_list:
            schema = Schema.create(path)
            if not schema.dbname in result:
                result[schema.dbname] = schema
            elif schema.is_in_memory():
                result[schema.dbname].absorb(schema)
            else:
                raise DatabaseException("Database name collision on " + schema.dbname)
        return result

    def init_features(self) -> typing.Dict[str, FeatureInfo]:
        """Build the global feature registry across all attached schemas.

        Each feature name maps to an ordered list of :py:class:`~gbd_core.schema.FeatureInfo`
        objects; the first entry is used by default (highest precedence, determined by
        database order in *path_list*).  The ``hash`` column of the ``features`` table is
        always placed first when present, as it serves as the primary join key.

        Returns:
            dict[str, list[FeatureInfo]]: Feature name -> list of FeatureInfo (highest
            precedence first).
        """
        result = dict()
        schema: Schema
        for schema in self.schemas.values():
            feature: FeatureInfo
            for feature in schema.features.values():
                # first found feature is used: (=feature precedence by database position)
                if not feature.name in result:
                    result[feature.name] = [feature]
                elif feature.column == "hash" and feature.table == "features":
                    # first found features table is the one that serves the hash
                    if result[feature.name][0].table != "features":
                        result[feature.name].insert(0, feature)
                    else:
                        result[feature.name].append(feature)
                else:
                    result[feature.name].append(feature)
        return result

    def query(self, q):
        """Execute a raw SQL SELECT and return all rows.

        Args:
            q (str): SQL SELECT statement.

        Returns:
            list[tuple]: All result rows as tuples.
        """
        if self.verbose:
            eprint(q)
        return self.cursor.execute(q).fetchall()

    def execute(self, q):
        """Execute a raw SQL DDL/DML statement and optionally auto-commit.

        Args:
            q (str): SQL statement (e.g. INSERT, UPDATE, ALTER TABLE, CREATE TABLE).
        """
        if self.verbose:
            eprint(q)
        self.cursor.execute(q)
        if self.autocommit:
            self.commit()

    def commit(self):
        self.connection.commit()

    def set_auto_commit(self, autocommit):
        self.autocommit = autocommit

    def dexists(self, dbname):
        """Return ``True`` if *dbname* is an attached database."""
        return dbname in self.schemas.keys()

    def dmain(self, dbname):
        """Return ``True`` if *dbname* is the default (first-attached) database."""
        return dbname == self.maindb

    def dpath(self, dbname):
        """Return the file-system path of *dbname*.

        Raises:
            DatabaseException: If *dbname* is not attached.
        """
        if not dbname in self.schemas:
            raise DatabaseException("Database '{}' not found".format(dbname))
        return self.schemas[dbname].path

    def dcontext(self, dbname):
        """Return the context name (e.g. ``"cnf"``, ``"kis"``) of *dbname*.

        Raises:
            DatabaseException: If *dbname* is not attached.
        """
        if not dbname in self.schemas:
            raise DatabaseException("Database '{}' not found".format(dbname))
        return self.schemas[dbname].context

    def dtables(self, dbname):
        """Return the list of SQLite table names in *dbname*.

        Raises:
            DatabaseException: If *dbname* is not attached.
        """
        if not dbname in self.schemas:
            raise DatabaseException("Database '{}' not found".format(dbname))
        return self.schemas[dbname].get_tables()

    def finfo(self, fname, db=None):
        """Return the :py:class:`~gbd_core.schema.FeatureInfo` for *fname*.

        Args:
            fname (str): Bare feature name (no ``db:`` prefix).
            db (str | None): Restrict lookup to this database name.

        Returns:
            FeatureInfo: Highest-precedence info object for the feature.

        Raises:
            DatabaseException: If the feature does not exist (or not in *db*).
        """
        if fname in self.features and len(self.features[fname]) > 0:
            if db is None:
                return self.features[fname][0]
            else:
                infos = [info for info in self.features[fname] if info.database == db]
                if len(infos) == 0:
                    raise DatabaseException("Feature '{}' does not exists in database {}".format(fname, db))
                return infos[0]
        else:
            raise DatabaseException("Feature '{}' does not exists".format(fname))

    def faddr_column(self, feature):
        """Return the fully-qualified column address ``database.table.column`` for *feature*.

        Args:
            feature (str): Feature identifier (bare name, or ``db:name`` / ``context:name``).

        Returns:
            str: e.g. ``"cnf_sc2021.local.value"``
        """
        finfo = self.find(feature)
        return "{}.{}.{}".format(finfo.database, finfo.table, finfo.column)

    def faddr_table(self, feature):
        """Return the fully-qualified table address ``database.table`` for *feature*.

        Used to build subquery references in
        :py:meth:`~gbd_core.grammar.Parser.get_sql` for 1:n features.

        Args:
            feature (str): Feature identifier.

        Returns:
            str: e.g. ``"cnf_sc2021.local"``
        """
        finfo = self.find(feature)
        return "{}.{}".format(finfo.database, finfo.table)

    def find(self, fid: str, db: str = None):
        """Find a feature by name or qualified identifier.

        Args:
            fid (str): Feature identifier - one of:
                ``"feature"`` (bare), ``"database:feature"``, or ``"context:feature"``.
            db (str | None): Restrict lookup to this database name.  Raises if *fid*
                already contains a different database prefix.

        Returns:
            FeatureInfo: Info object for the highest-precedence matching feature.
            Precedence follows the order of databases in *path_list*.

        Raises:
            DatabaseException: If the feature is not found or database identifiers are
                ambiguous.
        """
        parts = fid.split(":")
        if db is not None:
            if len(parts) > 1:
                if parts[0] != db:
                    raise DatabaseException("Ambiguous database identifiers: '{}' and '{}'".format(parts[0], db))
                else:
                    return self.finfo(parts[1], parts[0])
            return self.finfo(fid, db)
        elif len(parts) == 1:
            return self.finfo(fid)
        elif parts[0] in self.get_databases():
            return self.finfo(parts[1], parts[0])
        elif parts[0] in self.get_contexts():
            db = self.get_databases(parts[0])[0]
            return self.finfo(parts[1], db)
        else:
            raise DatabaseException("Feature '{}' not found".format(fid))

    def faddr(self, fid: str, with_column=True):
        """Return the fully-qualified SQL address for *fid*.

        Args:
            fid (str): Feature identifier (bare name, ``db:name``, or ``context:name``).
            with_column (bool): If ``True`` (default), return ``database.table.column``;
                if ``False``, return ``database.table``.

        Returns:
            str: Fully-qualified address usable in SQL expressions.
        """
        finfo = self.find(fid)

        if with_column:
            return "{}.{}.{}".format(finfo.database, finfo.table, finfo.column)
        else:
            return "{}.{}".format(finfo.database, finfo.table)

    def get_databases(self, context: str = None):
        """Return all attached database names, optionally filtered by *context*.

        Args:
            context (str | None): If given, only databases of this context are returned.

        Returns:
            list[str]: Database names in attachment order.
        """
        return [dbname for (dbname, schema) in self.schemas.items() if not context or context == schema.context]

    def get_contexts(self, dbs=[]):
        """Return the unique context names of the attached databases.

        Args:
            dbs (list[str]): If non-empty, restrict to these database names.

        Returns:
            list[str]: Unique context names (order not guaranteed).
        """
        return list(set([s.context for s in self.schemas.values() if not dbs or s.dbname in dbs]))

    def get_features(self, dbs=[]):
        """Return the names of all known features, optionally filtered by database.

        Args:
            dbs (list[str]): If non-empty, only features from these databases are included.

        Returns:
            list[str]: Feature names (may contain duplicates if a feature spans databases).
        """
        return [name for (name, infos) in self.features.items() for info in infos if not dbs or info.database in dbs]

    def get_tables(self, dbs=[]):
        """Return the unique table names across all features, optionally filtered by database.

        Args:
            dbs (list[str]): If non-empty, restrict to these database names.

        Returns:
            list[str]: Unique table names.
        """
        tables = [info.table for infos in self.features.values() for info in infos if not dbs or info.database in dbs]
        return list(set(tables))

    def create_feature(self, name, default_value=None, target_db=None, permissive=False):
        """Create a new feature in *target_db* and register it in the global registry.

        Delegates DDL to :py:meth:`~gbd_core.schema.Schema.create_feature`.

        Args:
            name (str): Feature name; must be a valid identifier.
            default_value (str | None): ``None`` creates a **1:n** feature (separate
                ``{name}(hash, value)`` table); any string creates a **1:1** feature
                (column in ``features`` with that default).
            target_db (str | None): Target database name; defaults to the first database.
            permissive (bool): If ``True``, silently skip if the feature already exists
                and bypass name validation (for internal use by initialisers).
        """
        db = target_db or self.maindb
        created = self.schemas[db].create_feature(name, default_value, permissive)
        for finfo in created:
            if not finfo.name in self.features.keys():
                self.features[finfo.name] = [finfo]
            else:
                # this code disregards feature precedence by database position:
                self.features[finfo.name].append(finfo)

    def set_values(self, fname, value, hashes, target_db=None):
        """Set *value* for feature *fname* on each hash in *hashes*.

        For **1:1 features** this is an upsert on the ``features`` table column.
        For **1:n features** a new ``(hash, value)`` row is inserted (or silently ignored
        if the pair already exists), preserving all other values for the same hash.

        Args:
            fname (str): Feature name.
            value: Value to assign (coerced to ``TEXT`` by SQLite).
            hashes (list[str] | str): One or more benchmark hashes.
            target_db (str | None): Target database; uses the feature's registered
                database when ``None``.
        """
        finfo = self.finfo(fname, target_db)
        self.schemas[finfo.database].set_values(fname, value, hashes)

    def rename_feature(self, fname, new_fname, target_db=None):
        """Rename feature *fname* to *new_fname* in its database.

        For 1:n features, also renames the underlying separate table.
        Updates the in-memory feature registry accordingly.

        Args:
            fname (str): Current feature name.
            new_fname (str): New feature name; must pass :py:meth:`~gbd_core.schema.Schema.valid_feature_or_raise`.
            target_db (str | None): Restrict to this database when the feature name is ambiguous.
        """
        Schema.valid_feature_or_raise(new_fname)
        finfo = self.finfo(fname, target_db)
        self.execute("ALTER TABLE {}.features RENAME COLUMN {} TO {}".format(finfo.database, fname, new_fname))
        if finfo.default is None:
            con = sqlite3.connect(self.schemas[finfo.database].path)
            with con as cursor:
                cursor.execute("ALTER TABLE {} RENAME TO {}".format(fname, new_fname))
            con.close()
        self.features[fname].remove(finfo)
        if not len(self.features[fname]):
            del self.features[fname]
        finfo.name = new_fname
        if not new_fname in self.features.keys():
            self.features[new_fname] = [finfo]
        else:
            # this code disregards feature precedence by database position:
            self.features[new_fname].append(finfo)

    def delete_feature(self, fname, target_db=None):
        """Delete feature *fname* and all its stored values.

        For 1:n features, drops the separate table.
        For 1:1 features, drops the column (requires SQLite >= 3.35).

        Args:
            fname (str): Feature name to delete.
            target_db (str | None): Restrict to this database when ambiguous.

        Raises:
            DatabaseException: If a 1:1 feature is requested on SQLite < 3.35.
        """
        finfo = self.finfo(fname, target_db)
        if finfo.default is None:
            self.execute("DROP TABLE IF EXISTS {}.{}".format(finfo.database, fname))
        elif Database.sqlite3_version() >= 3.35:
            self.execute("ALTER TABLE {}.{} DROP COLUMN {}".format(finfo.database, finfo.table, fname))
        else:
            raise DatabaseException("Cannot delete unique feature {} with SQLite versions < 3.35".format(fname))
        self.features[fname].remove(finfo)
        if not len(self.features[fname]):
            del self.features[fname]

    def delete(self, fname, values=[], hashes=[], target_db=None):
        """Delete specific (hash, value) pairs or reset values to their default.

        For **1:n features**: deletes matching rows from the feature table.  For any
        hash that now has no remaining values, resets the FK column in ``features`` to
        ``'None'``.
        For **1:1 features**: resets the column to its default value for matching hashes.

        Args:
            fname (str): Feature name.
            values (list[str]): Value filter; empty list means no value restriction.
            hashes (list[str]): Hash filter; empty list means no hash restriction.
            target_db (str | None): Restrict to this database when ambiguous.
        """
        finfo = self.finfo(fname, target_db)
        w1 = "{cl} IN ('{v}')".format(cl=finfo.column, v="', '".join(values))
        w2 = "hash IN ('{h}')".format(h="', '".join(hashes))
        where = "{} AND {}".format(w1 if len(values) else "1=1", w2 if len(hashes) else "1=1")
        db = finfo.database
        if finfo.default is None:
            hashlist = [r[0] for r in self.query("SELECT DISTINCT(hash) FROM {d}.{tab} WHERE {w}".format(d=db, tab=fname, w=where))]
            self.execute("DELETE FROM {d}.{tab} WHERE {w}".format(d=db, tab=fname, w=where))
            remaining = [
                r[0] for r in self.query("SELECT DISTINCT(hash) FROM {d}.{tab} WHERE hash in ('{h}')".format(d=db, tab=fname, h="', '".join(hashlist)))
            ]
            setnone = [h for h in hashlist if not h in remaining]
            self.execute("UPDATE {d}.features SET {col} = 'None' WHERE hash IN ('{h}')".format(d=db, col=fname, h="', '".join(setnone)))
        else:
            self.execute("UPDATE {d}.features SET {col} = '{default}' WHERE {w}".format(d=db, col=fname, default=finfo.default, w=where))

    def delete_hashes_entirely(self, hashes, target_db=None):
        tables = self.get_tables([target_db])
        for table in tables:
            self.execute("DELETE FROM {}.{} WHERE hash IN ('{h}')".format(target_db, table, h="', '".join(hashes)))

    def copy_feature(self, old_name, new_name, target_db, hashlist=[]):
        """Copy values from *old_name* into *new_name* for the given hashes.

        *new_name* must already exist in *target_db*.

        Args:
            old_name (str): Source feature name.
            new_name (str): Destination feature name.
            target_db (str): Database for the destination feature.
            hashlist (list[str]): Restrict the copy to these hashes.
        """
        old_finfo = self.find(old_name)
        data = self.query(
            "SELECT hash, {col} FROM {d}.{tab} WHERE hash IN ('{h}')".format(
                d=old_finfo.database, col=old_finfo.column, tab=old_finfo.table, h="', '".join(hashlist)
            )
        )
        for hash, value in data:
            self.set_values(new_name, value, [hash], target_db)
