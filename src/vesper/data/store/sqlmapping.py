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
# log = logging.getLogger("sqlmapping")

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
        
        tbls = []
        if subject: 
            tbls = [self.getTableFromResourceId('artist')]   #subject)
        elif predicate:
            tbls = self.getTablesWithProperty('trackname')   #predicate)
        else:
            tbls = self.tables

        query = None
        stmts = []
        for table in tbls:
            if not subject and not predicate and not object:
                # * * * => select * from table
                query = table.select()
            elif subject and not predicate and not object:
                # s * * => select * from table where id = s  
                query = table.select()           # .where(table.id == subject)
            elif subject and predicate and not object:
                # s p * => select p from table where id = s
                query = table.select()            # .property], table.id == subject)
            elif not subject and predicate and object:
                # * p o => select p from table where p = object
                query = table.select()            # .property], table.property == object)
            elif not subject and not predicate and object:
                # * * o => select o from table where o.type = objecttype
                for column in table.columns:
                #                    if self.jsonStore.isCompatibleType(objecttype, column.type):
                    query = table.select()         # .column], table.column == object)

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
        # extract table name from URI and return matching Table object from our db
        print "subj: ", subj
        for t in self.tables:
            if subj == t.name:
                return t


    def getTablesWithProperty(self, prop):
        # search our db for tables w/columns matching prop, return list of Table objects
        print "prop: ", prop
        tbls = [] 
        for t in self.tables:
            for c in t.c:
                if c.name == prop:
                    tbls.append(t)
                    break
        return tbls


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
