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

        # debug/devel output
        print '===BEGIN==========================================================='
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
        print '===END============================================================='


    def _checkConnection(self):
        if self.conn is None:
            self.conn = self.engine.connect()
        if self.autocommit is False:
            if not self.conn.in_transaction():
                self.trans = self.conn.begin()
        self.conn.execution_options(autocommit=self.autocommit)


    def getStatements(self, subject=None, predicate=None, object=None,
                      objecttype=None, context=None, asQuad=True, hints=None):
        if context:
            print "contexts not supported"

        pkName = colName = None
        tables = []
        if subject: 
            # subject = self.baseUri + RSRC_DELIM + 'artist/artistid/{somePrimaryKeyVal}'
            tables = [self._getTableFromResourceId(subject)]
            pkName = self._getPropNameFromResourceId(subject)
            pkValue = self._getValueFromResourceId(subject)
        elif predicate:
            # predicate = self.baseUri + RSRC_DELIM + 'artist/artistname/{someVal}'
            tables = self._getTablesWithProperty(predicate)
            colName = self._getPropNameFromResourceId(predicate)
            colValue = self._getValueFromResourceId(predicate)
        else:
            tables = self.md.sorted_tables

        query = None
        pattern = None
        # only a hypothetical loop - for now we just consider one table at a time
        for table in tables:
            if not pkName and not colName and not object:
                # * * * => select * from table
                query = table.select()
            elif pkName and not colName and not object:
                # s * * => select * from table where id = s
                query = table.select().where(table.c[pkName] == pkValue)
                pattern = 's'
            elif pkName and colName and not object:
                # s p * => select p from table where id = s
                query = table.select(table.c[colName]).where(table.c[pkName] == pkValue)
                pattern = 'sp'
            elif not pkName and colName and object:
                # * p o => select p from table where p = object
                query = table.select(table.c[colName]).where(table.c[colName] == object)
                pattern = 'po'

            self._checkConnection()
            print query
            result = self.conn.execute(query)
            # ridin' bareback here - should test for errors etc but the hell with that
            stmts = self._generateStatementAssignments(result, table.name, colName, pattern)
            print stmts

        return stmts


    def _generateStatementAssignments(self, fetchedRows=None, tableName=None, colName=None, pattern=None):
        td = None
        for td in self.parsedTables:
            if td['tableName'] == tableName:
                break;
        stmts = []


        # XXXXX SUBSTITUE ACTUAL COLUMN NAMES FOR r['subject'] etc!!

        if pattern is None:
            # for EVERY PrimaryKey value, for EVERY column, for EVERY cell value add a Statement with:
            # PrimaryKeys ==> subject, ColumnNames ==> predicate, ColumnElements ==> object, ColumnTypes ==> objecttype
            for r in fetchedRows:
                stmts = [Statement(td['pKeyName'], 'id', r['subject'], None, None)]
                [stmts.append(Statement(td['pKeyName'], c, r['object'], None, None)) for c in td['colNames']]

        elif pattern == 's':
            # for A PARTICULAR PrimaryKey value, for EVERY column, for EVERY cell value, add a Statement with:
            # PrimaryKey ==> subject, ColumnNames ==> predicate, ColumnElements ==> object, ColumnTypes ==> objecttype
            # XXX degenerate use of for..in construct: only ONE row expected
            for r in fetchedRows:
                print "r: ", r
                stmts = [Statement(td['pKeyName'], 'id', r['subject'], None, None)]
                [stmts.append(Statement(td['pKeyName'], c, r['object'], None, None)) for c in td['colNames']]

        elif pattern == 'sp':
            # for A PARTICULAR PrimaryKey value, for A PARTICULAR column, for EVERY cell value, add a Statement with:
            # PrimaryKey ==> subject, ColumnName ==> predicate, ColumnElement ==> object, ColumnType ==> objecttype
            # XXX degenerate use of for..in construct: only ONE row expected
            for r in fetchedRows:
                stmts = [Statement(td['pKeyName'], 'id', r['subject'], None, None),
                         Statement(td['pKeyName'], colName, r['object'], None, None)]

        elif pattern == 'po':
            # for ANY PrimaryKey value, for a PARTICULAR column, for A PARTICULAR cell value, add a Statement with:
            # PrimaryKeys ==> subject, ColumnName ==> predicate, CellValue ==> object, ColumnType ==> objecttype
            for r in fetchedRows:
                stmts.append(Statement(td['pKeyName'], 'id', r['subject'], None, None),
                             Statement(td['pKeyName'], colName, r['object'], None, None))

        return stmts

                            
    def _getColumnsOfInterest(self, tableName=None):
        # use json map and sql schema to determine columns of interest, make a list
        tableDesc = self.mapping["table"][tableName]
        if 'id' in tableDesc:
            pKeyName = tableDesc['id']
        colNames = []
        if 'properties' in tableDesc:
            # walk through table's properties list collecting column names
            for p in tableDesc['properties']:
                if p == "*":
                    # turn to sql schema to compile list of all (non-pkey) column names
                    for c in self.insp.get_columns(tableName):
                        if c['name'] not in self.insp.get_primary_keys(tableName):
                            colNames.append(c['name'])
                elif isinstance(p, dict):
                    # seems to be a foreign key reference, process accordingly
                    if 'references' in p:
                        print "uh foreign key ", p['references'],  " ignored in ", tableName  
                else:
                    # a [single] specific column name - add to list
                    colNames.append(p)
        # return dict w/all column names tagged in json map, ready to permute through
        return {'tableName':tableName, 'pKeyName':pKeyName, 'colNames':colNames}
       

    def _getTableFromResourceId(self, uri):
        # extract table name from URI and return matching and return corresponding SQLA object
        tName = uri
        if self.baseUri in tName:
            tName = tName.rsplit(RSRC_DELIM, 3)[1]
        print "tName, uri: ", tName, uri
        for t in self.md.sorted_tables:
            if t.name == tName:
                return t
        return None


    def _getValueFromResourceId(self, uri):
        # extract value ie. row cell value from URI
        val = uri
        if self.baseUri in val:
            val = (val.rsplit(RSRC_DELIM, 2)[2]).lstrip(VAL_OPEN_DELIM).rstrip(VAL_CLOSE_DELIM)
        print "val, uri: ", val, uri
        return val


    def _getPropNameFromResourceId(self, uri):
        # generic tool to extract primary key or property name from uri
        pName = uri
        if self.baseUri in pName:
            pName = pName.rsplit(RSRC_DELIM, 3)[2]
        print "pName, uri: ", pName, uri
        return pName


    def _getTablesWithProperty(self, uri):
        # search our db for tables w/columns matching prop, return list of Table objects
        pName = _getPropNameFromResourceId(uri)
        print "pName, uri: ", pName, uri
        tbls = []
        for t in self.md.sorted_tables:
            for c in t.c:
                if c.name == pName:
                    tbls.append(t)
                    break
        return tbls


    def addStatement(self, stmt):
        return 0


    def addStatements(self, stmts):
        return 0


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
