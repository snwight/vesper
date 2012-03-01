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
    def __init__(self, source=None, mapping=None, autocommit=False, loadVesperTable=True, **kw):
        '''
        Create an instance of a json-to-sql mapping class - 
        '''
        # instantiate SqlAlchemy DB connection based upon uri passed to this method 
        self.engine = create_engine(source, echo=False)

        # reflect the designated db schema into python space for examination
        self.md = MetaData(self.engine, reflect=True)
        self.insp = reflection.Inspector.from_engine(self.engine)

        # is a JSON mapping is supplied, use it otherwise infer our best guess at one
        self.mapping = {}
        if mapping:
            self.mapping = mapping
        else:
            self._generateMapFromSchema()

        # output readable json map and sql schema as diagnostic
        self._printSchemata()
        
        # divine our resource format strings here, now
        self.baseUri = ""
        if 'idpattern' in self.mapping:
            self.baseUri = self.mapping['idpattern']

        # create a list of {tbl,pkey,cols[]} dicts for efficient internal use
        self.parsedTables = []
        self._getColumnsOfInterest(self.insp.get_table_names())

        self.vesper_stmts = None
        if loadVesperTable:
            print "LOADING VESPER TABLE"
            self.vesper_stmts = Table('vesper_stmts', self.md, 
                                      Column('subject', String(255)),
                                      Column('predicate', String(255)),
                                      Column('object', String(255)),
                                      Column('objecttype', String(8)),
                                      Column('context', String(8)),
                                      UniqueConstraint('subject', 'predicate', 'object', 'objecttype', 'context'),
                                      mysql_engine='InnoDB', 
                                      keep_existing = True)
            Index('idx_vs', self.vesper_stmts.c.subject, self.vesper_stmts.c.predicate, self.vesper_stmts.c.object) 
            self.md.create_all(self.engine)
            
            # create our duplicate-insert-exception-ignoring stored procedure for postgresql backend
            if self.engine.name == 'postgresql':
                self.engine.execute("""
                CREATE OR REPLACE FUNCTION 
                insert_ignore_duplicates (s text, p text, o text, ot text, c text)
                RETURNS void AS $$ 
                BEGIN 
                    LOOP
                        BEGIN 
                            INSERT INTO vesper_stmts VALUES (s, p, o, ot, c); 
                                RETURN; 
                            EXCEPTION WHEN unique_violation THEN RETURN; 
                        END; 
                    END LOOP; 
                END $$ LANGUAGE plpgsql
                """)

        # private matters
        self.conn = None
        self.trans = None
        self.autocommit = autocommit


    def _printSchemata(self):        
        print '===BEGIN SCHEMA INFO==========================================================='
        print 'SQL schema::'
        for tbl in self.insp.get_table_names():
            if tbl == 'vesper_stmts':
                continue
            print 'TABLE:', tbl
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
            print 'VIEW:', vw
            q = self.insp.get_view_definition(vw)
            for c in self.insp.get_columns(vw):
                print '\t', c['name'], c['type']
            print 'VIEW DEFINITION:'
            print '\t', q
        print '===END SCHEMA INFO============================================================='
        print '===BEGIN JSON MAP=============================================================='
        print json.dumps(self.mapping, sort_keys=True, indent=4)
        print '===END JSON MAP================================================================'


    def _generateMapFromSchema(self):
        # generate our best guess at a JSON-from-SQL mapping, or leave self.mapping unbothered
        mDict = {
            "tablesprop": "HYPEtype",
            "idpattern": "http://souzis.com/",
            "tables": { }
            }
        for tbl in self.insp.get_table_names():
            if tbl == 'vesper_stmts':
                continue
            mDict['tables'][tbl] = {
                'id' : self.insp.get_primary_keys(tbl)[0], 'properties' : ['*'] 
                }
            for fk in self.insp.get_foreign_keys(tbl):
                for cc in fk['constrained_columns']:
                    (mDict['tables'][tbl]['properties']).append({
                            cc.encode('ascii') : {
                                'key': fk['referred_columns'][0].encode('ascii'),
                                'references' : fk['referred_table']}})
        if mDict['tables']:
            self.mapping = mDict

#    for vw in self.insp.get_view_names():
#    q = self.insp.get_view_definition(vw)
#    for c in self.insp.get_columns(vw):
#    print '\t', c['name'], c['type']


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
        table = pKeyName = pKeyValue = colName = None
        if subject:
            table = self._getTableFromResourceId(subject)
            if table is not None:
                pKeyName = self._getPropNameFromResourceId(subject)
                pKeyValue = self._getValueFromResourceId(subject)
            else:
                # we have a subject to look for but no table is named - vesperize it
                table = self.vesper_stmts

        if predicate:
            colName = self._getPropNameFromResourceId(predicate)
            if not subject:
                table = self._getTableFromResourceId(predicate)
                if table is not None:
                    for td in self.parsedTables:
                        if td['tableName'] == table.name:
                            pKeyName = td['pKeyName']
                            break
                else:
                    # we have a predicate to match but no table has a col of this name - vesperize it
                    table = self.vesper_stmts

        if table is None:
            # if no URI is specified we cannot infer the target table!
            # we finally believe this is a select * query on the vesper_stmts db
            table = self.vesper_stmts

        stmts = []
        if table.name == 'vesper_stmts':
            # special case, we are implicitly querying our private statment store
            query = self._buildVesperQuery(subject, predicate, object, objecttype, context, asQuad, hints)
            pattern = 'vespercols'
        elif not pKeyValue and not predicate and not object:
            # * * * => select all rows from this table (subject w/out ID ==> table)
            query = select([table])
            pattern = 'multicol'
        elif subject and pKeyValue and not predicate and not object:
            # s * * => select row from table where id = s
            query = select([table]).where(table.c[pKeyName] == pKeyValue)
            pattern = 'multicol'
        elif not subject and predicate and not object:
            # * p * => select id, p from table
            query = select([table.c[pKeyName], table.c[colName]])
            pattern = 'unicol'
        elif subject and predicate and not object:
            # s p * => select p from table where id = s
            query = select([table.c[pKeyName], table.c[colName]]).where(table.c[pKeyName] == pKeyValue)
            pattern = 'unicol'
        elif not subject and predicate and object:
            # * p o => select id from table where p = object
            query = select([table.c[pKeyName]]).where(table.c[colName] == object)
            pattern = 'id'

        print query
        self._checkConnection()
        result = self.conn.execute(query)
        stmts.extend(self._generateStatementAssignments(result, table.name, colName, pattern))
        return stmts


    def _generateStatementAssignments(self, fetchedRows=None, tableName=None, colName=None, pattern=None):
        '''
        prepare returned rows from getStatements() query as sets of Statement tuples, using designated
        'pattern' flag to guide us wrt what the returned rows actually contain, which is dependent upon 
        the nature of the query
        '''
        if pattern != 'vespercols':
            # derive the primary key column name, i.e. subject ID property name, for this table
            for td in self.parsedTables:
                if td['tableName'] == tableName:
                    pKeyName = td['pKeyName']
                    break;
        stmts = []
        for r in fetchedRows:
            print "r: ", r
            if pattern == 'vespercols':
                # special case for our private table, returned values are always packed the same way
                stmts.append(Statement(r['subject'], r['predicate'], r['object'], r['objecttype'], r['context']) )
            else:
                subj = pKeyName + '{' + str(r[pKeyName]) + '}'
                if pattern == 'id':
                    # return one subject/ID (e.g. "find rowids for rows where prop == x")
                    stmts.append(Statement(subj, None, None, None, None))
                elif pattern == 'unicol':
                    # return a triple representing one subject/ID, one property name, and one object value
                    stmts.append(Statement(subj, colName, r[colName], None, None))
                elif pattern == 'multicol':
                    # return a set of triples representing all properties and values for one subject/ID
                    [stmts.append(Statement(subj, c, r[c], None, None)) for c in td['colNames']]
        for s in stmts:
            print s, '\n'
        return stmts

                            
    def _getColumnsOfInterest(self, tableNames):
        '''
        using our json mapping object (possibly derived by we ourselves based on inspection of the 
        active backend schema), collate and store basic information about the tables that we are to
        use into a list of simple dicts: 
        {table name, primary key column name, [list of all relevant columns]}.
        this allows us to ignore all other backend tables, and even unused columns in active tables 
        that we are using
        '''
        self.parsedTables = []
        for tbl in tableNames:
            # if our private secret table is passed in here, politely decline to parse it 
            if tbl == 'vesper_stmts':
                continue
            # use json map and sql schema to determine columns of interest, make a list
            tableDesc = self.mapping["tables"][tbl]
            if 'id' in tableDesc:
                pKeyName = tableDesc['id']
            if 'relationship' in tableDesc:
                if tableDesc['relationship'] == True:
                    # this is a correlation definition
                    pass
            colNames = []
            if 'properties' in tableDesc:
                for prop in tableDesc['properties']:
                    if isinstance(prop, dict):
                        if 'id' in prop:
                            print "property: id", prop['id'], "for", tbl
                        if 'key' in prop:
                            print "property: key", prop['key'], "for", tbl  
                        if 'references' in prop:
                            print "property: references", prop['references'], "for", tbl
                        if 'view' in prop:
                            print "property: view", prop['view'], "for", tbl
                        else:
                            print "column name/s: ", prop, "for", tbl
                    elif prop == "*":
                        # turn to sql schema to compile list of all (non-pkey) column names
                        for c in self.insp.get_columns(tbl):
                            if c['name'] not in self.insp.get_primary_keys(tbl):
                                colNames.append(c['name'])
                    else:
                        # a [single] specific column name - add to list
                        colNames.append(p)

                # add this dict to our list
                self.parsedTables.append({'tableName':tbl, 'pKeyName':pKeyName, 'colNames':colNames})


    def _buildVesperQuery(self, subject=None, predicate=None, object=None,
                          objecttype=None, context=None, asQuad=True, hints=None):
        '''
        this implements queries against the single-purpose private table that we use to store 
        non-URI-resource statements in
        '''
        hints = hints or {}
        limit = hints.get('limit')
        offset = hints.get('offset')
        if object:
            if isinstance(object, ResourceUri):
                object = object.uri
                objecttype = True
                objecttype = OBJECT_TYPE_RESOURCE
            elif not objecttype:
                objecttype = OBJECT_TYPE_LITERAL

        if not asQuad and not context:
            query = select(['subject', 'predicate', 'object', 'objecttype',
                            func.min(self.vesper_stmts.c.context).label('context')]).\
                            group_by('subject', 'predicate', 'object', 'objecttype')
        else:
            query = self.vesper_stmts.select()
        if subject:
            query = query.where(self.vesper_stmts.c.subject == subject)
        if predicate:
            query = query.where(self.vesper_stmts.c.predicate == predicate)
        if object:
            query = query.where(self.vesper_stmts.c.object == object)
        if objecttype: 
            query = query.where(self.vesper_stmts.c.objecttype == objecttype)
        if context:
            query = query.where(self.vesper_stmts.c.context == context)
        if limit:
            if self.engine.name == 'postgresql':
                query = query.order_by('subject', 'predicate', 'object','objecttype', 'context')
            query = query.limit(limit)
            if offset:
                query = query.offset(offset)
        return query


    def _getTableFromResourceId(self, uri):
        '''
        extract table name from (possibly non-URI) resource string and return 
        corresponding Table object
        '''
        if not uri:
            return None
        if self.baseUri in uri:
            uri = uri[len(self.baseUri):]
        tName = uri.split(RSRC_DELIM)[0]
        for td in self.parsedTables:
            # only interested here in known, 'json-mapped' tables
            if td['tableName'] == tName:
                for t in self.md.sorted_tables:
                    if t.name == tName:
                        return t
        return None


    def _getPropNameFromResourceId(self, uri):
        '''
        generic tool to extract property name from (possibly non-URI) resource string
        '''
        if not uri:
            return None
        pName = uri
        if self.baseUri in uri:
            uri = uri[len(self.baseUri):]
        if VAL_OPEN_DELIM in uri:
            uri = uri.split(VAL_OPEN_DELIM)[0]
            pName = uri.split(RSRC_DELIM, 2)[1]
        elif RSRC_DELIM in uri:
            pName = uri.split(RSRC_DELIM)[1]
        return pName


    def _getValueFromResourceId(self, uri):
        '''
        extract "{value}" from (possibly non-URI) resource string
        '''
        if not uri:
            return None
        val = uri
        if self.baseUri in uri:
            uri = uri[len(self.baseUri):]
        if VAL_OPEN_DELIM in uri:
            val = uri.split(VAL_OPEN_DELIM)[1].rstrip(VAL_CLOSE_DELIM)
        else:
            val = None                
        return val


    def _getTableWithProperty(self, uri):
        '''
        search our db for tables w/columns matching property name, return 
        corresponding Table object
        '''
        if not uri:
            return None
        pName = self._getPropNameFromResourceId(uri)
        for t in self.md.sorted_tables:
            for c in t.c:
                if c.name == pName:
                    return t
        return None


    def addStatement(self, stmt):
        s, p, o, ot, c = stmt
        argDict = {}
        colName = None
        table = self._getTableFromResourceId(s)

        self._checkConnection()
        if table is not None:
            pKeyName = self._getPropNameFromResourceId(s)
            pKeyValue = self._getValueFromResourceId(s)
            colName = self._getPropNameFromResourceId(p)
            argDict = {colName : o}
            # try update first - if it fails we'll drop through to insert
            upd = table.update().where(table.c[pKeyName] == pKeyValue)
            print "UPDATE: ", table.name, pKeyName, pKeyValue, colName, o, ot, argDict
            result = self.conn.execute(upd, argDict)
            if result.rowcount:
                return result.rowcount
        else:
            table = self.vesper_stmts
            pKeyName = "subject"
            pKeyValue = s
            argDict = {"predicate":p, "object":o, "objecttype":ot, "context":c}
        
        print "ADD: ", table.name, pKeyName, pKeyValue, colName, o, argDict

        # update failed - try inserting new row 
        argDict[pKeyName] = pKeyValue
        ins = table.insert()
        if self.engine.name == 'postgresql':
            if table == self.vesper_stmts:
                ins = text('''
                    select insert_ignore_duplicates(:subject, :predicate, :object, :objecttype, :context)
                    ''').execution_options(autocommit=self.autocommit)
        else:
            if self.engine.name == "sqlite":
                ins = ins.prefix_with("OR IGNORE")
            elif self.engine.name == "mysql":
                ins = ins.prefix_with("IGNORE")
        result = self.conn.execute(ins, argDict)
        return result.rowcount


    def addStatements(self, stmts):
        rc = 0
        for stmt in stmts:
             rc += self.addStatement(stmt)
        return rc
        

    def removeStatement(self, stmt):
        '''
        subj/id none none ==> delete row where subj == id
        subj/id pred none ==> null pred where subj == id
        subj/id pred obj ==> null pred where subj == id and pred == obj
        subj pred none ==> null pred
        '''
        subject, predicate, object, ot, context = stmt
        table = cmd = pKeyName = pKeyValue = colName = None
        table = self._getTableFromResourceId(subject)
        if table is None:
            # vesperize it and fall through for quick exit
            cmd = self.vesper_stmts.delete().where(
                (self.vesper_stmts.c.subject == stmt[0]) &
                (self.vesper_stmts.c.predicate == stmt[1]) &
                (self.vesper_stmts.c.object == stmt[2]) &
                (self.vesper_stmts.c.objecttype == stmt[3]) &
                (self.vesper_stmts.c.context == stmt[4]))
        else:
            # otherwise remove from client tables, selectively
            pKeyName = self._getPropNameFromResourceId(subject)
            pKeyValue = self._getValueFromResourceId(subject)
            if predicate:
                colName = self._getPropNameFromResourceId(predicate)
                cmd = table.update().values({table.c[colName] : ''})
                if object:
                    cmd = cmd.where(table.c[colName] == object)
            else:
                # no predicate - this is a row deletion at least, possibly table clear-out
                cmd = table.delete()
                if pKeyValue:
                     # set controls for 'delete from table where id = subj'
                    cmd = cmd.where(table.c[pKeyName] == pKeyValue)

                '''
                prop = self.mapping[predicate]
                if props['references']:
                    # this is a foreign key 
                    if key != "id":
                        #  cmd = update([table] set " + key+" = null 
                        {colName : ''}
                        upd = table.update().where(table.c[pKeyName] == pKeyValue)

                elif props['relationship']:
                    # delete row
                    cmd = table.delete().where(table.c[pKeyName] == pKeyValue)
                elif something
                    update table set propinfo.column = null
                '''
        self._checkConnection()
        result = self.conn.execute(cmd)
        return result.rowcount


    def removeStatements(self, stmts=None):
        rc = 0
        for stmt in stmts:
            rc += self.removeStatement(stmt)
        return rc


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
