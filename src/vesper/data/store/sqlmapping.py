#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
__all__ = ['SqlMapping']

import os, os.path


from vesper.backports import *
from vesper.data.base import * # XXX

import sqlalchemy
from sqlalchemy import engine, sql, create_engine, text
from sqlalchemy.types import *
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import func, or_, asc
from sqlalchemy.schema import Table, Column, MetaData, UniqueConstraint, Index
from sqlalchemy.engine import reflection

import logging 
log = logging.getLogger("sqlmapping")

RSRC_DELIM='/'
VAL_OPEN_DELIM='{'
VAL_CLOSE_DELIM='}'

class SqlMappingStore(Model):
    '''
    JSON-SQL mapping engine 
    '''
    def __init__(self, source=None, mapping=None, autocommit=False, **kw):
        '''
        Create an instance of a json-to-sql mapping class for the schema defined by the
        vesper.data.DataStore.BasicStore dict 'store' - a matching schema must exist in
        the current backend database, whatever that might be, and all tables and columns
        and datatypes must precisely match the contents of the input mapping description json file.
        '''
        # instantiate SqlAlchemy DB connection based upon uri passed to this method 
        self.engine = create_engine(source, echo=False)

        # reflect the designated db schema into python space for examination
        self.md = MetaData(self.engine, reflect=True)
        self.insp = reflection.Inspector.from_engine(self.engine)

        # save our json map, parse it into a list of {tbl,pkey,cols[]} dicts for efficient internal use
        self.mapping = mapping
        self.parsedTables = []
        [self.parsedTables.append(self._getColumnsOfInterest(tbl)) for tbl in self.insp.get_table_names()]

        # divine our resource format strings here, now
        if 'idpattern' in self.mapping:
            self.baseUri = self.mapping['idpattern']
            print "baseUri: ", self.baseUri

        # private matters
        self.conn = None
        self.trans = None
        self.autocommit = autocommit

        print '===BEGIN SCHEMA INFO==========================================================='
        print 'JSON mapping::'
        print json.dumps(self.mapping, sort_keys=True, indent=4)
        # and get_view_names someday
        print 'SQL schema::'
        for tbl in self.insp.get_table_names():
            print '\t', tbl
            for c in self.insp.get_columns(tbl):
                print '\t\t', c['name'], c['type'], 'PRIMARY KEY' if 'primary_key' in c else ''
            for i in self.insp.get_indexes(tbl):
                print 'Index::' 
                print '\t\t', i['name'], 'UNIQUE' if 'unique' in i else '', 'ON', \
                    [ic.encode('ascii') for ic in i['column_names']]
            for k in self.insp.get_foreign_keys(tbl):
                print 'ForeignKey::'
                print '\t\t', [cc.encode('ascii') for cc in k['constrained_columns']], 'ON', \
                    k['referred_table'], [rc.encode('ascii') for rc in k['referred_columns']]
        print '===END SCHEMA INFO============================================================='


    def _checkConnection(self):
        if self.conn is None:
            self.conn = self.engine.connect()
        if self.autocommit is False:
            if not self.conn.in_transaction():
                self.trans = self.conn.begin()
        self.conn.execution_options(autocommit=self.autocommit)


    def getStatements(self, subject=None, predicate=None, object=None,
                      objecttype=None, context=None, asQuad=True, hints=None):
        ''' 
        our query loop - input is full URI- [or someday soon, shorthand prefix-) identified
        "RDF JSON" data descriptors, which we parse into table/col/data elements and thencely
        interrogate the underlying SQL database with.
        '''
        if context:
            print "contexts not supported"

        pkName = colName = None
        tables = []
        if subject: 
            tables = [self._getTableFromResourceId(subject)]
            pkName = self._getPropNameFromResourceId(subject)
            pkValue = self._getValueFromResourceId(subject)
        if predicate:
            colName = self._getPropNameFromResourceId(predicate)
            if not subject:
                # if all we have is a column name... it better be unique
                tables = self._getTablesWithProperty(predicate)
                for td in self.parsedTables:
                    if td['tableName'] == tables[0].name:
                        pkName = td['pKeyName']
                        break
        else:
            # I really will return * tables, so deal
            tables = self.md.sorted_tables

        stmts = []
        for table in tables:
            query = select([table])
            pattern = None
            if not subject and not predicate and not object:
                # * * * => select * from table
                pattern = 'multicol'
            elif subject and not predicate and not object:
                # s * * => select * from table where id = s
                query = select([table]).where(pkName == pkValue)
                pattern = 'multicol'
            elif not subject and predicate and not object:
                # * p * => select id, p from table
                query = select([table.c[pkName], table.c[colName]])
                pattern = 'unicol'
            elif subject and predicate and not object:
                # s p * => select p from table where id = s
                query = select(table.c[colName]).where(pkName == pkValue)
                pattern = 'unicol'
            elif not subject and predicate and object:
                # * p o => select id from table where p = object
                query = select([table.c[pkName]]).where(colName == object)
                pattern = 'unicol'
            self._checkConnection()
            print query
            result = self.conn.execute(query)
            # ridin' bareback here - should test for errors etc but the hell with that
            stmts.extend(self._generateStatementAssignments(result, table.name, colName, pattern))

        return stmts


    def _generateStatementAssignments(self, fetchedRows=None, tableName=None, colName=None, pattern=None):
        stmts = []
        td = None
        for td in self.parsedTables:
            if td['tableName'] == tableName:
                pkName = td['pKeyName']
                break;
        print "fetchedRows: ", fetchedRows
        for r in fetchedRows:
            subj = pkName + '{' + str(r[pkName]) + '}'
            stmts.append(Statement(subj, 'id', r[pkName], None, None))
            if pattern is 'multicol':
                [stmts.append(Statement(subj, c, r[c], None, None)) for c in td['colNames']]
            elif pattern is 'unicol':
                stmts.append(Statement(subj, colName, r[colName], None, None))
        return stmts

                            
    def _getColumnsOfInterest(self, tableName=None):
        # use json map and sql schema to determine columns of interest, make a list
        tableDesc = self.mapping["tables"][tableName]
        if 'id' in tableDesc:
            pKeyName = tableDesc['id']
        if 'relationship'in tableDesc:
            if tableDesc['relationship'] == True:
                # this is a correlation definition
                pass
        colNames = []
        if 'properties' in tableDesc:
            # walk through table's properties list collecting column names
            for prop in tableDesc['properties']:
                if isinstance(prop, dict):
                    if 'id' in prop:
                        print "id: ", prop['id'], "ignored in ", tableName  
                    if 'references' in prop:
                        print "uh foreign key: ", prop['references'], "ignored in ", tableName  
                    if 'view' in prop:
                        print "view definition: ", prop['view'], "ignored in", tableName
                    if 'relationship' in prop:
                        print "relationship: ", prop['relationship'], "ignored in", tableName
                    else:
                        print "column name/s: ", prop, "ignored in", tableName
                elif prop == "*":
                    # turn to sql schema to compile list of all (non-pkey) column names
                    for c in self.insp.get_columns(tableName):
                        if c['name'] not in self.insp.get_primary_keys(tableName):
                            colNames.append(c['name'])
                else:
                    # a [single] specific column name - add to list
                    colNames.append(p)

        # return dict w/all column names tagged in json map, ready to permute through
        return {'tableName':tableName, 'pKeyName':pKeyName, 'colNames':colNames}


    def _getTableFromResourceId(self, uri):
        # extract table name from URI and return matching and return corresponding SQLA object
        tName = uri
        if self.baseUri in tName:
            tName = tName.rsplit(RSRC_DELIM, 3)[2]
        #        print "tName, uri: ", tName, uri
        for t in self.md.sorted_tables:
            if t.name == tName:
                return t
        return None


    def _getPropNameFromResourceId(self, uri):
        # generic tool to extract primary key or property name from uri
        pName = uri
        if self.baseUri in pName:
            pName = (pName.rsplit(RSRC_DELIM, 3)[3]).rsplit(VAL_OPEN_DELIM)[0]
        #        print "pName, uri: ", pName, uri
        return pName


    def _getValueFromResourceId(self, uri):
        # extract value ie. row cell value from URI
        val = uri
        if self.baseUri in val:
            val = val.rsplit(VAL_OPEN_DELIM)[1].rstrip(VAL_CLOSE_DELIM)
        #        print "val, uri: ", val, uri
        return val


    def _getTablesWithProperty(self, uri):
        # search our db for tables w/columns matching prop, return list of Table objects
        pName = self._getPropNameFromResourceId(uri)
        #        print "pName, uri: ", pName, uri
        tbls = []
        for t in self.md.sorted_tables:
            for c in t.c:
                if c.name == pName:
                    tbls.append(t)
                    break
        return tbls


    def addStatement(self, stmt):
        # tragically due to the incremental-update nature of how the RDF model hands us row elements,
        # we are forced to use a generic UPSERT algorithm 
        s, p, o, ot, c = stmt
        table = self._getTableFromResourceId(s)
        pkName = self._getPropNameFromResourceId(s)
        pkValue = self._getValueFromResourceId(s)
        colName = self._getPropNameFromResourceId(p)
        if colName == 'id:':
            # this is a primary key def... ideally should be cached and delayed
            colName = pkName
        self._checkConnection()
        upd = table.update().where(table.c[pkName] == pkValue)
        result = self.conn.execute(upd, {colName : o})
        if not result.rowcount:
            ins = table.insert()
            result = self.conn.execute(ins, {pkName : pkValue, colName : o})
        return result.rowcount


    def addStatements(self, stmts):
        # tragically due to the incremental-update nature of how the RDF model hands us row elements,
        # we are forced to use a generic UPSERT algorithm 
        rc = 0
        for stmt in stmts:
             rc += self.addStatement(stmt)
        return rc
        

    def removeStatement(self, stmt):
        '''
        subject, p, o, ot, c = stmt

        table = None
        if p == "rdf:type":
            for t in self.md.sorted_tables:
                if t.name == o:
                    table = t
            s = self._getPrimaryKeyNameFromResourceId(subject)
            sValue = self._getValueFromResourceId(subject)
            # delete from table where id = subj
            rmv = table.delete().where(table.c[s] == sValue)
        else:
            propinfo = self.store[p]
            if propinfo.references:
                if relationship = True:
                    delete row
                else:
                    if key != "id":
                        "update table set " + key+" = null" 
            else:
                #  NB: CAN'T NULL COLUMNS!!!   
                update table set propinfo.column = null
                '''
        return 0


    def removeStatements(self, stmts=None):
        return 0


    def commit(self, **kw):
        if self.conn is not None:
            if self.conn.in_transaction():
                self.trans.commit()


    def rollback(self):
        if self.conn is not None:
            if self.conn.in_transaction():
                self.trans.rollback()


    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None
