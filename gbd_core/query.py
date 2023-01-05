
# MIT License

# Copyright (c) 2023 Markus Iser, Karlsruhe Institute of Technology (KIT)

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

class GBDQuery:

    def __init__(self, db: Database, query):
        self.db = db
        self.parser = Parser(query)
        self.features = self.parser.get_features()


    def features_exist_or_throw(self, features):
        for feature in features:
            if not feature in self.db.get_features():
                raise DatabaseException("Unknown feature '{}'".format(feature))


    # Generate SQL Query from given GBD Query 
    def build_query(self, hashes=[], resolve=[], group_by="hash", join_type="LEFT", collapse=None, where_in_subselect=False):
        self.features_exist_or_throw(resolve + [group_by] + list(self.features))

        sql_select = self.build_select(group_by, resolve, collapse)

        sql_from = self.build_from(group_by, set(resolve) | self.features, join_type)
        
        sql_where = self.build_where(hashes, group_by, where_in_subselect)

        sql_groupby = "GROUP BY {}".format(self.db.faddr_column(group_by)) if collapse else ""
        sql_orderby = "ORDER BY {}".format(self.db.faddr_column(group_by))
        
        return "{} {} WHERE {} {} {}".format(sql_select, sql_from, sql_where, sql_groupby, sql_orderby)


    def build_select(self, group_by, resolve, collapse=None):
        result = [ self.db.faddr_column(f) for f in [group_by] + resolve ]
        if collapse and collapse != "none":
            result = [ "{}(DISTINCT({}))".format(collapse, r) for r in result ]
        return "SELECT " + ", ".join(result)


    def find_translator(self, source_context, target_context):
        for dbname in self.db.get_databases(source_context):
            if "to_"+target_context in self.db.get_tables([ dbname ]):
                return (source_context, dbname, "to_" + target_context)
        
        for dbname in self.db.get_databases(target_context):
            if "to_"+source_context in self.db.get_tables([ dbname ]):
                return (target_context, dbname, "to_" + source_context)
        
        raise DatabaseException("No translator table found for contexts {} and {}".format(source_context, target_context))


    def build_from(self, group, features, join_type="LEFT"):
        result = dict()

        gdatabase = self.db.finfo(group).database
        gtable = self.db.finfo(group).table
        gcontext = self.db.dcontext(gdatabase)
        gaddress = gdatabase + "." + gtable
        result[gaddress] = "FROM {}".format(gaddress)

        tables = set([ (finfo.database, finfo.table) for finfo in [ self.db.finfo(f) for f in features ] ])
        for (fdatabase, ftable) in tables:
            faddress = fdatabase + "." + ftable
            if not faddress in result:
                fcontext = self.db.dcontext(fdatabase)
                if fcontext == gcontext:
                    if ftable == "features":
                        result[faddress] = "{} JOIN {} ON {}.hash = {}.hash".format(join_type, faddress, gaddress, faddress)
                    else:
                        address = fdatabase + ".features"
                        if not address in result:
                            result[address] = "{} JOIN {} ON {}.hash = {}.hash".format(join_type, address, gaddress, address)
                        result[faddress] = "{} JOIN {} ON {}.{} = {}.hash".format(join_type, faddress, address, ftable, faddress)
                else:
                    (tcontext, tdatabase, ttable) = self.find_translator(gcontext, fcontext)
                    direction = ("hash", "value") if tcontext == gcontext else ("value", "hash")
                    
                    taddress = tdatabase + "." + ttable
                    if not taddress in result:                           
                        result[taddress] = "INNER JOIN {trans} ON {group}.hash = {trans}.{dir0}".format(trans=taddress, group=gaddress, dir0=direction[0])
                        
                    result[faddress] = "INNER JOIN {feat} ON {trans}.{dir1} = {feat}.hash".format(feat=faddress, trans=taddress, dir1=direction[1])

        return " ".join(result.values())


    def build_where(self, hashes, group_by, subselect=False):
        result = "{} != 'None'".format(self.db.faddr_column(group_by))
        if subselect:
            fro = self.build_from(group_by, self.features)
            whe = self.parser.get_sql(self.db)
            result = result + " AND {}.hash in (SELECT {}.hash FROM {} WHERE {})".format(self.db.faddr_table(group_by), self.db.faddr_table(group_by), fro, whe)
        else:
            result = result + " AND " + self.parser.get_sql(self.db)
        if len(hashes):
            result = result + " AND {}.hash in ('{}')".format(self.db.faddr_table(group_by), "', '".join(hashes))
        return result
