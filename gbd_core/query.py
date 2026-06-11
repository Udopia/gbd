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


from gbd_core.database import Database, DatabaseException
from gbd_core.grammar import Parser
from gbd_core import contexts
from gbd_core.schema import Schema


class GBDQuery:
    """Translates a GBD query string and parameters into a complete SQLite SELECT statement.

    A GBD query spans one or more ATTACHed SQLite databases managed by
    :py:class:`Database`.  Each database belongs to a *context*
    (e.g. ``cnf``, ``wcnf``, ``pbo``).  Features within the same context are joined
    directly; features from a different context require a *translator feature* - a 1:n
    feature named ``to_{context}`` that maps hashes between contexts.

    Typical usage::

        q   = GBDQuery(db, "filename like foo%")
        sql = q.build_query(resolve=["local"], collapse="group_concat")
        rows = db.query(sql)
    """

    def __init__(self, db: Database, query):
        """
        Args:
            db (Database): Multi-database instance used for feature resolution.
            query (str | None): GBD filter expression, or ``None`` / empty string for
                an unconditional (match-all) query.
        """
        self.db = db
        self.parser = Parser(query)
        self.features = self.parser.get_features()

    def features_exist_or_throw(self, features):
        """Raise :py:exc:`DatabaseException` if any feature in
        *features* does not exist in the attached databases.

        Args:
            features (list[str]): Feature identifiers to validate.
        """
        for feature in features:
            self.db.find(feature)

    # Generate SQL Query from given GBD Query
    def build_query(self, hashes=[], resolve=[], group_by=None, join_type="LEFT", collapse=None):
        """Build and return a complete SQL SELECT statement.

        Args:
            hashes (list[str]): Restrict results to these benchmark hashes.
            resolve (list[str]): Features to include as output columns.
            group_by (str | None): Group results by this feature instead of the default
                ``context:hash`` column; inferred from *resolve* when ``None``.
            join_type (str): ``"LEFT"`` (default, include unmatched hashes) or
                ``"INNER"`` (exclude them).
                Cross-context joins are always ``INNER`` (see ``Issues.md`` #4).
            collapse (str | None): Aggregate function for resolved columns; one of
                ``"group_concat"``, ``"min"``, ``"max"``, ``"avg"``, ``"count"``,
                ``"sum"``.  ``None`` returns one raw row per join result.

        Returns:
            str: Ready-to-execute SQL query.
        """
        group = group_by or self.determine_group_by(resolve)

        self.features_exist_or_throw(resolve + [group] + list(self.features))

        sql_select = self.build_select(group, resolve, collapse)

        sql_from = self.build_from(group, set(resolve) | self.features, join_type)

        sql_where = self.build_where(hashes, group)

        sql_groupby = "GROUP BY {}".format(self.db.faddr(group)) if collapse else ""
        sql_orderby = "ORDER BY {}".format(self.db.faddr(group))

        return "{} {} WHERE {} {} {}".format(sql_select, sql_from, sql_where, sql_groupby, sql_orderby)

    def determine_group_by(self, resolve):
        """Return the default ``context:hash`` column used as the GROUP BY key.

        Uses the database of the first feature in *resolve*, or the primary ``hash``
        feature when *resolve* is empty.

        When *resolve* spans multiple contexts, only the first feature's context is considered (see ``Issues.md`` #5).

        Args:
            resolve (list[str]): Features that will be resolved in the query.

        Returns:
            str: Feature identifier of the form ``"context:hash"``.
        """
        if len(resolve) == 0:
            return self.db.dcontext(self.db.find("hash").database) + ":hash"
        else:
            return self.db.dcontext(self.db.find(resolve[0]).database) + ":hash"

    def build_select(self, group_by, resolve, collapse=None):
        """Build the SELECT clause.

        When *collapse* is given, every selected column (including the group-by column)
        is wrapped with the aggregate function.  Without *collapse*, ``SELECT DISTINCT``
        deduplicates rows.

        The group-by column is also aggregated, which is redundant when ``GROUP BY`` is present (see ``Issues.md`` #6).

        Args:
            group_by (str): Primary output column (always first in the SELECT list).
            resolve (list[str]): Additional output columns.
            collapse (str | None): Aggregate function name (e.g. ``"group_concat"``),
                or ``None`` for no aggregation.

        Returns:
            str: SQL SELECT clause, e.g.
                ``"SELECT DISTINCT cnf_db.features.hash, cnf_db.local.value"``.
        """
        result = [self.db.faddr(f) for f in [group_by] + resolve]
        if collapse and collapse != "none":
            result = ["{}(DISTINCT {})".format(collapse, r) for r in result]
        return "SELECT DISTINCT " + ", ".join(result)

    def find_translator_feature(self, source_context, target_context):
        """Find the 1:n translator feature that bridges two contexts.

        A translator feature is a 1:n feature named ``to_{target_context}`` stored in a
        database of *source_context*, or ``to_{source_context}`` stored in a database of
        *target_context*.  It provides a hash mapping used by :py:meth:`build_from` to
        construct cross-context JOINs.

        Args:
            source_context (str): Context of the primary (group-by) table.
            target_context (str): Context of the feature being resolved.

        Returns:
            FeatureInfo: Info object for the translator feature.

        Raises:
            DatabaseException: If no translator feature exists for the context pair.
        """
        for dbname in self.db.get_databases(source_context):
            # eprint("Checking database {} for translator".format(dbname))
            if "to_" + target_context in self.db.get_features([dbname]):
                return self.db.find("to_" + target_context, dbname)

        for dbname in self.db.get_databases(target_context):
            # eprint("Checking database {} for translator".format(dbname))
            if "to_" + source_context in self.db.get_features([dbname]):
                return self.db.find("to_" + source_context, dbname)

        raise DatabaseException("No translator feature found for contexts {} and {}".format(source_context, target_context))

    def build_from(self, group, features, join_type="LEFT"):
        """Build the FROM / JOIN clause.

        Three JOIN strategies depending on the feature's context relative to the
        group-by column:

        1. **Same-context, 1:1 feature** (column in ``features`` table):
           ``{join_type} JOIN db.features ON db.features.hash = base.hash``
        2. **Same-context, 1:n feature** (separate table):
           ensures ``db.features`` is joined first, then
           ``{join_type} JOIN db.{name} ON db.{name}.hash = db.features.{name}``
        3. **Cross-context**: always ``INNER JOIN`` via the translator feature table
           regardless of *join_type* (see ``Issues.md`` #4).

        Args:
            group (str): Feature identifier of the group-by column; its database is the
                base ``FROM`` table.
            features (set[str]): All features that must appear in the clause
                (filter features + resolved features).
            join_type (str): ``"LEFT"`` or ``"INNER"`` applied to same-context joins.

        Returns:
            str: SQL FROM / JOIN clause.
        """
        result = dict()

        gdatabase = self.db.find(group).database
        gtable = self.db.find(group).table
        gcontext = self.db.dcontext(gdatabase)
        gaddress = gdatabase + "." + gtable
        result[gaddress] = "FROM {}".format(gaddress)

        tables = set([(finfo.database, finfo.table) for finfo in [self.db.find(f) for f in features]])
        for fdatabase, ftable in tables:
            faddress = fdatabase + "." + ftable
            ffeatures_address = fdatabase + ".features"
            if not faddress in result:  # join only once
                fcontext = self.db.dcontext(fdatabase)
                if fcontext == gcontext:
                    if faddress == ffeatures_address:  # join features table directly
                        result[faddress] = "{j} JOIN {t} ON {t}.hash = {g}.hash".format(j=join_type, t=ffeatures_address, g=gaddress)
                    else:  # join non-unique features table via features table
                        fname = ftable
                        if not ffeatures_address in result:
                            result[ffeatures_address] = "{j} JOIN {t} ON {t}.hash = {g}.hash".format(j=join_type, t=ffeatures_address, g=gaddress)
                        result[faddress] = "{j} JOIN {t} ON {t}.hash = {ft}.{n}".format(j=join_type, t=faddress, ft=ffeatures_address, n=fname)
                else:
                    tfeat = self.find_translator_feature(gcontext, fcontext)
                    direction = ("hash", "value") if self.db.dcontext(tfeat.database) == gcontext else ("value", "hash")

                    taddress = tfeat.database + "." + tfeat.table
                    if not taddress in result:
                        result[taddress] = "INNER JOIN {trans} ON {group}.hash = {trans}.{dir0}".format(trans=taddress, group=gaddress, dir0=direction[0])

                    result[faddress] = "INNER JOIN {feat} ON {trans}.{dir1} = {feat}.hash".format(feat=faddress, trans=taddress, dir1=direction[1])

        return " ".join(result.values())

    def build_where(self, hashes, group_by):
        """Build the WHERE clause body.

        Combines three conditions with ``AND``:

        1. ``group_column != 'None'`` excludes the sentinel null-hash row present in
           every 1:n feature table (see ``Issues.md`` #7 - sentinel design).
        2. The SQL fragment compiled from the GBD filter expression.
        3. An optional ``hash IN (...)`` restriction when *hashes* is non-empty.

        Args:
            hashes (list[str]): Benchmark hashes to restrict results to; empty list
                means no hash restriction.
            group_by (str): Feature identifier of the group-by column.

        Returns:
            str: SQL WHERE clause body (without the ``WHERE`` keyword).
        """
        group_column = self.db.faddr(group_by)
        group_table = self.db.faddr_table(group_by)
        result = group_column + " != 'None' AND " + self.parser.get_sql(self.db)
        if len(hashes):
            result = result + " AND {}.hash in ('{}')".format(group_table, "', '".join(hashes))
        return result
