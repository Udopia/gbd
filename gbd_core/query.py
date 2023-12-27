
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
from gbd_core.schema import Schema

class GBDQuery:

    def __init__(self, db: Database, query):
        self.db = db
        self.parser = Parser(query)
        self.features = self.parser.get_features()


    def features_exist_or_throw(self, features):
        for feature in features:
            self.db.find(feature)


    # Generate SQL Query from given GBD Query 
    def build_query(self, hashes=[], resolve=[], group_by=None, join_type="LEFT", collapse=None):
        group = group_by or self.determine_group_by(resolve)

        self.features_exist_or_throw(resolve + [group] + list(self.features))

        sql_select = self.build_select(group, resolve, collapse)

        sql_from = self.build_from(group, set(resolve) | self.features, join_type)
        
        sql_where = self.build_where(hashes, group)

        sql_groupby = "GROUP BY {}".format(self.db.faddr(group)) if collapse else ""
        sql_orderby = "ORDER BY {}".format(self.db.faddr(group))
        
        return "{} {} WHERE {} {} {}".format(sql_select, sql_from, sql_where, sql_groupby, sql_orderby)
    

    def determine_group_by(self, resolve):
        if len(resolve) == 0:
            return self.db.dcontext(self.db.find("hash").database) + ":hash"
        else:
            return self.db.dcontext(self.db.find(resolve[0]).database) + ":hash"
        

    def build_select(self, group_by, resolve, collapse=None):
        result = [ self.db.faddr(f) for f in [group_by] + resolve ]
        if collapse and collapse != "none":
            result = [ "{}(DISTINCT {})".format(collapse, r) for r in result ]
        return "SELECT DISTINCT " + ", ".join(result)


    def find_translator_feature(self, source_context, target_context):
        for dbname in self.db.get_databases(source_context):
            #eprint("Checking database {} for translator".format(dbname))
            if "to_"+target_context in self.db.get_features([ dbname ]):
                return self.db.find("to_"+target_context, dbname)
        
        for dbname in self.db.get_databases(target_context):
            #eprint("Checking database {} for translator".format(dbname))
            if "to_"+source_context in self.db.get_features([ dbname ]):
                return self.db.find("to_"+source_context, dbname)
        
        raise DatabaseException("No translator feature found for contexts {} and {}".format(source_context, target_context))


    def build_from(self, group, features, join_type="LEFT"):
        result = dict()

        gdatabase = self.db.find(group).database
        gtable = self.db.find(group).table
        gcontext = self.db.dcontext(gdatabase)
        gaddress = gdatabase + "." + gtable
        result[gaddress] = "FROM {}".format(gaddress)

        tables = set([ (finfo.database, finfo.table) for finfo in [ self.db.find(f) for f in features ] ])
        for (fdatabase, ftable) in tables:
            faddress = fdatabase + "." + ftable
            ffeatures_address = fdatabase + ".features"
            if not faddress in result: # join only once
                fcontext = self.db.dcontext(fdatabase)
                if fcontext == gcontext:
                    if faddress == ffeatures_address: # join features table directly
                        result[faddress] = "{j} JOIN {t} ON {t}.hash = {g}.hash".format(j=join_type, t=ffeatures_address, g=gaddress)
                    else: # join non-unique features table via features table
                        fname = ftable
                        if not ffeatures_address in result:
                            result[ffeatures_address] = "{j} JOIN {t} ON {t}.hash = {g}.hash".format(j=join_type, t=ffeatures_address, g=gaddress)
                        result[faddress] = "{j} JOIN {t} ON {t}.hash = {ft}.{n}".format(j=join_type, t=faddress, ft=ffeatures_address, n=fname)
                else:
                    tfeat = self.find_translator_feature(gcontext, fcontext)
                    direction = ("hash", tfeat.name) if self.db.dcontext(tfeat.database) == gcontext else (tfeat.name, "hash")
                    
                    taddress = tfeat.database + "." + tfeat.table
                    if not taddress in result:                           
                        result[taddress] = "INNER JOIN {trans} ON {group}.hash = {trans}.{dir0}".format(trans=taddress, group=gaddress, dir0=direction[0])
                        
                    result[faddress] = "INNER JOIN {feat} ON {trans}.{dir1} = {feat}.hash".format(feat=faddress, trans=taddress, dir1=direction[1])

        return " ".join(result.values())


    def build_where(self, hashes, group_by):
        group_column = self.db.faddr(group_by)
        group_table = self.db.faddr_table(group_by)
        result = group_column + " != 'None' AND " + self.parser.get_sql(self.db)
        if len(hashes):
            result = result + " AND {}.hash in ('{}')".format(group_table, "', '".join(hashes))
        return result
