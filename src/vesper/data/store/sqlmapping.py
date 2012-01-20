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


    def getStatements(self, jsonStmt=None, subject=None, predicate=None, object=None,
                      objecttype=None, context=None, asQuad=True, hints=None):

        '''        if context: raise "contexts not supported"

        if subj: 
            tables = [extractTablefromResourceId(subj)]
        elif prop:
            tables = tableswithproperty(prop)
        else:
            tables = alltables
            
        for table in tables:
            patterns:
            if not subj and not prop:
                if not obj:
                    select * from table;
                else:
                    for column in table.columns:
                        if compatible(objecttype, columntype):
                            select column from table where column = obj
                        else:

                            s * * => select * from table where id = subj  
                            s property * => select property from table where id = s
                            * property object => select property from table where property = object
                            '''
        return []

    def addStatement(self, stmt):
        '''add the specified statement to the model'''
        
        '''
        argDict = {'subject' : stmt[0],
        'predicate' : stmt[1],
        'object' : stmt[2],
        'objecttype' : stmt[3],
        'context' : stmt[4]}
        '''
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
        '''adds multiple statements to the model'''
 
        '''
        argDictList = [{'subject' : stmt[0],
                        'predicate' : stmt[1],
                        'object' : stmt[2],
                        'objecttype' : stmt[3], 
                        'context' : stmt[4]} for stmt in stmts]
                        '''
        return 0

    def removeStatements(self, stmts=None):
        '''removes multiple statements from the model'''

        '''
        wc = []
        [wc.append((self.vesper_stmts.c.subject == stmt[0]) & 
                   (self.vesper_stmts.c.predicate == stmt[1]) & 
                   (self.vesper_stmts.c.object == stmt[2]) & 
                   (self.vesper_stmts.c.objecttype == stmt[3]) & 
                   (self.vesper_stmts.c.context == stmt[4])) for stmt in stmts]
                   '''
        return 0
