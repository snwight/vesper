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

# XXX fakery
RSRC_TEMPLATE="http:/souzis.com/"
RSRC_DELIM="/"

class SqlMappingStore(Model):
    '''
    JSON-SQL mapping engine 
    '''
    def __init__(self, source=None, store={}, autocommit=False, **kw):
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
        insp = reflection.Inspector.from_engine(self.engine)

        # one level of data abstraction for devel purposes
        self.tables = self.md.sorted_tables

        # save our json map object
        self.jsonStore = store
        
        # private matters
        self.conn = None
        self.trans = None
        self.autocommit = autocommit

        # debug/devel output
        print '=============================================================='
        print 'JSON store::\n', store.storage_template
        print 'SQL schema::'
        for tbl in insp.get_table_names():
            cols = insp.get_columns(tbl)
            pks = insp.get_primary_keys(tbl)
            fks = insp.get_foreign_keys(tbl)
            idx = insp.get_indexes(tbl)
            print '\t', tbl
            for c in cols:
                p = ''
                if 'primary_key' in c:
                    if c['primary_key']: 
                        p = 'PRIMARY KEY'
                print '\t\t', c['name'], c['type'], p
            if idx:
                print 'Indexes::' 
                for i in idx:
                    u = ''
                    if 'unique' in i:
                        if i['unique']:
                            u = 'UNIQUE'
                    print '\t\t', i['name'], u, 'ON', [ic.encode('ascii') for ic in i['column_names']]
            if fks:
                print 'ForeignKeys::'
                for k in fks:
                    print '\t\t', [cc.encode('ascii') for cc in k['constrained_columns']], 'ON', \
                        k['referred_table'], [rc.encode('ascii') for rc in k['referred_columns']]


    def getStatements(self, subject=None, predicate=None, object=None,
                      objecttype=None, context=None, asQuad=True, hints=None):
        if context:
            raise "contexts not supported"
        
        s = p = ''
        tbls = []
        if subject: 
            # XXX TESTING
            subject = RSRC_TEMPLATE + 'artist/artistid/{somePrimaryKeyVal}'
            tbls = [self.getTableFromResourceId(subject)]
            s = self.getPrimaryKeyNameFromResourceId(subject)
            sValue = self.getValueFromResourceId(subject)
        elif predicate:
            # XXX TESTING
            predicate = RSRC_TEMPLATE + 'artist/artistname/{someVal}'
            tbls = self.getTablesWithProperty(predicate)
            p = self.getPropertyNameFromResourceId(predicate)
        else:
            tbls = self.tables

        query = None
        stmts = []
        for table in tbls:
            if not s and not p and not object:
                # * * * => select * from table
                query = table.select()
            elif s and not p and not object:
                # s * * => select * from table where id = s 
                query = table.select().where(table.c[s] == sValue)
            elif s and p and not object:
                # s p * => select p from table where id = s
                query = table.select(table.c[p]).where(table.c[s] == sValue)
            elif not s and p and object:
                # * p o => select p from table where p = object
                query = table.select(table.c[p]).where(table.c[p] == object)
            elif not s and not p and object:
                # * * o => select o from table where o.type = objecttype
                for column in table.columns:
                #  if self.jsonStore.isCompatibleType(objecttype, column.type):
                    query = table.select(column).where(column.type == objecttype)

            self._checkConnection()
            print query
            result = self.conn.execute(query)
#            for r in result:
#                stmts.append( Statement(r['subject'], r['predicate'], r['object'], r['objecttype'], r['context']) )
           
        return stmts

                            
    def _checkConnection(self):
        if self.conn is None:
            self.conn = self.engine.connect()
        if self.autocommit is False:
            if not self.conn.in_transaction():
                self.trans = self.conn.begin()
        self.conn.execution_options(autocommit=self.autocommit)


    def getTableFromResourceId(self, subj):
        # extract table name from URI and return matching and return corresponding SQLA object
        # XXX note that here we ASSUME subj IS RSRC_TEMPLATE/TABLENAME/COLUMNAME/{Value}
        print "subj: ", subj
        tName = subj.rsplit(RSRC_DELIM, 3)[1]
        print "tName: ", tName
        for t in self.tables:
            if t.name == tName:
                return t


    def getTablesWithProperty(self, prop):
        # search our db for tables w/columns matching prop, return list of Table objects
        # XXX note that here we ASSUME prop IS RSRC_TEMPLATE/TABLENAME/COLUMNNAME/{Value}
        print "prop: ", prop
        pName = getPropertyNameFromResourceId(prop)
        print "pName: ", pName
        tbls = [] 
        for t in self.tables:
            for c in t.c:
                if c.name == pName:
                    tbls.append(t)
                    break
        return tbls


    def getPrimaryKeyNameFromResourceId(self, subj):
        # extract subject, ie. primary key column name from URI 
        # XXX note that here we ASSUME subj IS RSRC_TEMPLATE/TABLENAME/COLUMNAME/{Value}
        return subj.rsplit(RSRC_DELIM, 3)[2]
        

    def getPropertyNameFromResourceId(self, prop):
        # extract property, ie. column name from URI
        # XXX note that here we ASSUME subj IS RSRC_TEMPLATE/TABLENAME/COLUMNAME/{Value}
        return subj.rsplit(RSRC_DELIM, 3)[2]
        

    def getValueFromResourceId(self, subj):
        # extract value ie. row cell value from URI
        # XXX note that here we ASSUME subj IS RSRC_TEMPLATE/TABLENAME/COLUMNAME/{Value}
        v = subj.rsplit(RSRC_DELIM, 2)[2]
        return v.lstrip('{').rstrip('}')


    def addStatement(self, stmt):
        return 0


    def removeStatement(self, stmt):
        '''
        if prop == "rdf:type":
            table = obj
            delete from table where id = subj
        else:
            propinfo = mappings[prop]
            if propinfo.references:
                if relationship = True:
                    delete row
                else:
                    if key != "id" 
                    "update table set " + key+" = null" 
            else:
                #  NB: CAN'T NULL COLUMNS!!!   
                update table set propinfo.column = null

                '''
        return 0


    def addStatements(self, stmts):
        return 0


    def removeStatements(self, stmts=None):
        return 0
