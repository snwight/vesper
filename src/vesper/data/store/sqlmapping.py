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

        print '===BEGIN SCHEMA INFO==========================================================='
        print 'SQL schema::'
        for tbl in self.insp.get_table_names():
            print tbl
            for c in self.insp.get_columns(tbl):
                print '\t', c['name'], c['type']
            for pk in self.insp.get_primary_keys(tbl):
                print '\tPRIMARY KEY:'
                print '\t\t', pk
            for i in self.insp.get_indexes(tbl):
                print '\tINDEX:' 
                print '\t\t', i['name'], 'UNIQUE' if 'unique' in i else '', 'ON', \
                    [ic.encode('ascii') for ic in i['column_names']]
            for fk in self.insp.get_foreign_keys(tbl):
                print '\tFOREIGN KEY:'
                print '\t\t', [cc.encode('ascii') for cc in fk['constrained_columns']], 'ON', \
                    fk['referred_table'], [rc.encode('ascii') for rc in fk['referred_columns']]
        for vw in self.insp.get_view_names():
            print vw
            q = self.insp.get_view_definition(vw)
            for c in self.insp.get_columns(vw):
                print '\t', c['name'], c['type']
            print 'VIEW DEFINITION:'
            print '\t', q
            
        # XXXXXXXXXXXXX
        mapping = None
        # XXXXXXXXXXXXX
        if mapping:
            self.mapping = mapping
        else:
            self._generateMapFromSchema()
        print 'JSON mapping::'
        print json.dumps(self.mapping, sort_keys=True, indent=4)
        print '===END SCHEMA INFO============================================================='

        # divine our resource format strings here, now
        if 'idpattern' in self.mapping:
            self.baseUri = self.mapping['idpattern']
            print "baseUri: ", self.baseUri

        # save our json map, parse it into a list of {tbl,pkey,cols[]} dicts for efficient internal use
        self.parsedTables = []
        [self.parsedTables.append(self._getColumnsOfInterest(tbl)) for tbl in self.insp.get_table_names()]

        # private matters
        self.conn = None
        self.trans = None
        self.autocommit = autocommit


    def _generateMapFromSchema(self):
        # generate our best guess at a JSON-from-SQL mapping
        self.mapping = {
            "tablesprop": "type",
            "idpattern": "http://souzis.com/",
            "tables": { }
            }
        for tbl in self.insp.get_table_names():
            self.mapping['tables'][tbl] = {
                'id' : self.insp.get_primary_keys(tbl[0]), 'properties' : ['*'] 
                }
            for fk in self.insp.get_foreign_keys(tbl):
                for cc in fk['constrained_columns']:
                    for rc in fk['referred_columns']:
                        (self.mapping['tables'][tbl]['properties']).append( {
                                cc.encode('ascii') : {'references' : fk['referred_table'],
                                 'key': rc.encode('ascii') }})


    '''
    for vw in self.insp.get_view_names():
    q = self.insp.get_view_definition(vw)
    for c in self.insp.get_columns(vw):
    print '\t', c['name'], c['type']
    '''


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

        tables = []
        pkName = pkValue = colName = None
        if subject:
            tables = [self._getTableFromResourceId(subject)]
            pkName = self._getPropNameFromResourceId(subject)
            pkValue = self._getValueFromResourceId(subject)
        if predicate:
            colName = self._getPropNameFromResourceId(predicate)
            if not subject:
                tables = [self._getTableWithProperty(predicate)]
                for td in self.parsedTables:
                    if td['tableName'] == tables[0].name:
                        pkName = td['pKeyName']
                        break
        if not subject and not predicate:
            for st in self.md.sorted_tables:
                for td in self.parsedTables: 
                    if st.name == td['tableName']:
                       tables.append(st)
        
        print "pkName pkValue colName: ", pkName, pkValue, colName

        pattern = None
        stmts = []
        for table in tables:
            # set our fall-through action: => select all rows from this table
            query = select([table])
            pattern = 'multicol'
            if subject and pkValue and not predicate and not object:
                # s * * => select row from table where id = s
                query = select([table]).where(table.c[pkName] == pkValue)
                pattern = 'multicol'
            elif not subject and predicate and not object:
                # * p * => select id, p from table
                query = select([table.c[pkName], table.c[colName]])
                pattern = 'unicol'
            elif subject and predicate and not object:
                # s p * => select p from table where id = s
                query = select([table.c[pkName], table.c[colName]]).where(table.c[pkName] == pkValue)
                pattern = 'unicol'
            elif not subject and predicate and object:
                # * p o => select id from table where p = object
                query = select([table.c[pkName]]).where(table.c[colName] == object)
                pattern = None

            self._checkConnection()
            print query
            result = self.conn.execute(query)
            # ridin' bareback here - should test for errors etc but the hell with that
            stmts.extend(self._generateStatementAssignments(result, table.name, colName, pattern))

        return stmts


    def _generateStatementAssignments(self, fetchedRows=None, tableName=None, colName=None, pattern=None):
        for td in self.parsedTables:
            if td['tableName'] == tableName:
                pkName = td['pKeyName']
                break;
        stmts = []
        for r in fetchedRows:
            print "r: ", r
            subj = pkName + '{' + str(r[pkName]) + '}'
            stmts.append(Statement(subj, 'id', r[pkName], None, None))
            if pattern is 'multicol':
                [stmts.append(Statement(subj, c, r[c], None, None)) for c in td['colNames']]
            elif pattern is 'unicol':
                stmts.append(Statement(subj, colName, r[colName], None, None))
        for s in stmts:
            print s, '\n'
        return stmts

                            
    def _getColumnsOfInterest(self, tableName=None):
        # use json map and sql schema to determine columns of interest, make a list
        tableDesc = self.mapping["tables"][tableName]
        if 'id' in tableDesc:
            pKeyName = tableDesc['id']
        if 'relationship' in tableDesc:
            if tableDesc['relationship'] == True:
                # this is a correlation definition
                pass
        colNames = []
        if 'properties' in tableDesc:
            # walk through table's properties list collecting column names
            for prop in tableDesc['properties']:
                if isinstance(prop, dict):
                    if 'id' in prop:
                        print "property: id", prop['id'], "for", tableName  
                    if 'key' in prop:
                        print "property: key", prop['key'], "for", tableName  
                    if 'references' in prop:
                        print "property: references", prop['references'], "for", tableName  
                    if 'view' in prop:
                        print "property: view", prop['view'], "for", tableName
                    else:
                        print "column name/s: ", prop, "for", tableName
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
        # extract table name from URI and return corresponding SQLA object
        if not uri:
            return None
        tName = uri
        if self.baseUri in uri:
            uri = uri[len(self.baseUri):]
            tName = uri.split(RSRC_DELIM)[0]
        for td in self.parsedTables:
            if td['tableName'] == tName:
                for t in self.md.sorted_tables:
                    if t.name == tName:
                        # print "tName, uri: ", tName, uri
                        return t
        return None


    def _getPropNameFromResourceId(self, uri):
        # generic tool to extract primary key or property name from uri
        if not uri:
            return None
        pName = uri
        if self.baseUri in uri:
            uri = uri[len(self.baseUri):]
            if VAL_OPEN_DELIM in uri:
                uri = uri.split(VAL_OPEN_DELIM)[0]
            pName = uri.split(RSRC_DELIM, 2)[1]        
        return pName


    def _getValueFromResourceId(self, uri):
        # extract value ie. row cell value from URI
        if not uri:
            return None
        val = uri
        if self.baseUri in uri:
            uri = uri[len(self.baseUri):]
            if VAL_OPEN_DELIM in uri:
                # this is our normal case - unique pkey/ID is given
                val = uri.split(VAL_OPEN_DELIM)[1].rstrip(VAL_CLOSE_DELIM)
            else:
                val = None                
        # print "val, uri: ", val, uri
        return val


    def _getTableWithProperty(self, uri):
        # search our db for tables w/columns matching prop, return list of Table objects
        if not uri:
            return None
        pName = self._getPropNameFromResourceId(uri)
        #        print "pName, uri: ", pName, uri
        for t in self.md.sorted_tables:
            for c in t.c:
                if c.name == pName:
                    return t
        return None


    def addStatement(self, stmt):
        s, p, o, ot, c = stmt
        table = self._getTableFromResourceId(s)
        pkName = self._getPropNameFromResourceId(s)
        pkValue = self._getValueFromResourceId(s)
        colName = self._getPropNameFromResourceId(p)
        if colName == 'id':
            # this is a primary key def... ideally should be cached and delayed
            colName = pkName

        print "ADD: ", pkName, pkValue, colName, stmt

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
        subject, predicate, object, ot, context = stmt
        if context:
            print "contexts not supported"

        table = pKeyName = pKeyValue = None
        rmv = None
        if predicate == "rdf:type":
            for st in self.md.sorted_tables:
                if st.name == object:
                    table = st
            pKeyName = self._getPrimaryKeyNameFromResourceId(subject)
            pKeyValue = self._getValueFromResourceId(subject)
            # delete from table where id = subj
            rmv = table.delete().where(table.c[pKeyName] == pKeyValue)
        else:
            '''
            propinfo = self.mapping[predicate]
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
            pass
        self._checkConnection()
        result = self.conn.execute(rmv)
        return result.rowcount


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
