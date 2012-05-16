#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
__all__ = ['JsonAlchemyStore']

from vesper.data.base import *
from sqlalchemy import engine, create_engine, text
from sqlalchemy.types import String
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import func
from sqlalchemy.schema import Table, Column, MetaData, UniqueConstraint, Index
from sqlalchemy.engine import reflection
from jsonalchemymapper import *
import string, random
import logging 
log = logging.getLogger("jsonalchemy")

SPEW = False

class JsonAlchemyStore(Model):
    def __init__(self, source=None, mapping=None, autocommit=False, 
                 loadVesperTable=True, **kw):
        '''
        Create an instance of a json-to-sql store 
        '''
        self.engine = create_engine(source, echo=False)
        self.md = MetaData(self.engine)
        self.md.reflect(views=True)
        self.jmap = JsonAlchemyMapper(mapping, self.engine)
        self.vesper_stmts = None
        if loadVesperTable:
            self.vesper_stmts = Table(
                'vesper_stmts', self.md, 
                Column('subject', String(255)),
                Column('predicate', String(255)),
                Column('object', String(255)),
                Column('objecttype', String(8)),
                Column('context', String(8)),
                UniqueConstraint('subject', 'predicate', 'object', 
                                 'objecttype', 'context'),
                mysql_engine='InnoDB', 
                keep_existing = True)
            Index('idx_vs', self.vesper_stmts.c.subject, 
                  self.vesper_stmts.c.predicate, self.vesper_stmts.c.object) 
            self.md.create_all(self.engine)
            if self.engine.name == 'postgresql':
                # create our duplicate-insert-ignoring plpgsql stored proc
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


    def _checkConnection(self):
        if self.conn is None:
            self.conn = self.engine.connect()
        if self.autocommit is False:
            if not self.conn.in_transaction():
                self.trans = self.conn.begin()
        self.conn.execution_options(autocommit=self.autocommit)


    def _getTableObject(self, tName=None):
        '''
        extract table name, match in reflected schema, return SQLA Table object
        '''
        if tName:
            for t in self.md.sorted_tables:
                if t.name == tName:
                    return t
        return None


    def getStatements(self, subject=None, predicate=None, object=None, 
                      objecttype=None, context=None, 
                      asQuad=True, hints=None):
        ''' 
        our query loop - input is full URI- [or someday soon, shorthand 
        prefix-) identified "RDF JSON" data descriptors, which we parse 
        into table/col/data elements and thencely interrogate the underlying 
        SQL database with.
        '''
        stmts = []
        table = tableName =  colName = None
        pKeyNames = []
        pKeyDict = {}
        if subject:
            tableName = self.jmap.getTableNameFromResource(subject)
            table = self._getTableObject(tableName)
            if table is not None:
                pKeyDict = self.jmap.getPKeyDictFromResource(subject)
            else:
                # we have a subject but no table is named - vesperize it
                table = self.vesper_stmts
        if predicate:
            if predicate == "rdf:type":
                tableName = object
                object = None
                predicate = None
            elif tableName:
                # predicate must be 'referencing property'
                refFKeys = self.jmap.getRefFKeysFromTable(table.name)
                if refFKeys:
                    print "refFKeys:"
                    pprint.PrettyPrinter(indent=2).pprint(refFKeys)
                
                    
            table = self._getTableObject(tableName)
            if table is not None:
                colName = self.jmap.getColFromPred(table.name, predicate)
                if not pKeyDict:
                    subject = None
                    pKeyNames = self.jmap.getPKeyNamesFromTable(table.name)
        if table is None:
            # we finally believe this is a select * on vesper_stmts
            table = self.vesper_stmts

        # construct our query
        if table.name == 'vesper_stmts':
            # special case, we are querying our private statement store
            query = self._buildVesperQuery(subject, predicate, object, 
                                           objecttype, context, asQuad, hints)
            pattern = 'vespercols'
        elif not pKeyDict and not predicate and not object:
            # * * * => select all rows from this table 
            query = select([table])
            pattern = 'multicol'
        elif subject and pKeyDict and not predicate and not object:
            # s * * => select row from table where id[...] = s[...]
            query = select([table])
            for pk, pv in pKeyDict.items():
                query = query.where(table.c[pk] == pv)
            pattern = 'multicol'
        elif not subject and predicate and not object:
            # * p * => select id[...], p from table
            sList = [table.c[colName]]
            [sList.append(table.c[pkn]) for pkn in pKeyNames]
            query = select(sList)
            pattern = 'unicol'
        elif subject and predicate and not object:
            # s p * => select p from table where id[...] = s[...]
            sList = [table.c[colName]]
            [sList.append(table.c[pkn]) for pkn in pKeyDict.keys()]
            query = select(sList)
            for pk, pv in pKeyDict.items():
                query = query.where(table.c[pk] == pv)
            pattern = 'unicol'
        elif not subject and predicate and object:
            # * p o => select id[...] from table where p = object
            sList = []
            [sList.append(table.c[pkn]) for pkn in pKeyNames]
            query = select(sList).where(table.c[colName] == object)
            pattern = 'id'
        if SPEW:
            print query
        self._checkConnection()
        result = self.conn.execute(query)
        stmts.extend(self._generateStatementAssignments(result,
                                                        table.name,
                                                        colName,
                                                        pattern))
        return stmts


    def _buildVesperQuery(self, subject=None, predicate=None, object=None,
                          objecttype=None, context=None, 
                          asQuad=True, hints=None):
        '''
        this implements queries against the single-purpose private table that 
        we use to store non-URI-resource statements in
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
            query = select(
                ['subject', 'predicate', 'object', 'objecttype', 
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
                query = query.order_by(
                    'subject', 'predicate', 'object','objecttype', 'context')
            query = query.limit(limit)
            if offset:
                query = query.offset(offset)
        return query


    def _generateStatementAssignments(self, fetchedRows=None, tableName=None, 
                                      colName=None, pattern=None):
        '''
        prepare returned rows from getStatements() query as sets of Statement 
        tuples, using designated 'pattern' flag to guide us wrt what the 
        returned rows actually contain, which is dependent upon the nature of 
        the query
        '''
        if pattern != 'vespercols':
            pKeyNames = self.jmap.getPKeyNamesFromTable(tableName)
        stmts = []
        for r in fetchedRows:
            if SPEW: print "r: ", r
            if pattern == 'vespercols':
                # for our private table we know all column names
                stmts.append(
                    Statement(r['subject'], r['predicate'], r['object'], 
                              r['objecttype'], r['context']) )
            else:
                if pKeyNames:
                    subj = tableName + '/'
                    i = len(pKeyNames)
                    for pkn in pKeyNames:
                        i = i - 1
                        subj = subj + pkn + '#' + str(r[pkn])
                        if i:
                            subj = subj + '.'
                else:
                    uniqueBlankNode = ''.join(
                        random.choice(string.ascii_uppercase + string.digits)
                        for x in range(6))
                    subj = tableName + '/' + 'blank_node:#' + uniqueBlankNode
                if pattern == 'id':
                    # subject/ID (e.g. "find rowids for rows where prop == x")
                    stmts.append(Statement(subj, None, None, None, None))
                elif pattern == 'unicol':
                    # subject/ID, one property name, and one object value
                    stmts.append(
                        Statement(subj, colName, r[colName], None, None))
                elif pattern == 'multicol':
                    # all properties and values for one or more rows 
                    for td in self.jmap.parsedTables:
                        if td['tableName'] == tableName:
                            [stmts.append(
                                    Statement(subj, k, r[v], None, None)) \
                                 for k,v in td['colNames'].items()]
        if SPEW: 
            for s in stmts: 
                print s, '\n'
        return stmts


    def addStatement(self, stmt):
        s, p, o, ot, c = stmt
        colName = None
        argDict = {}
        pKeyDict = {}
        # first, verify this is write-worthy table
        tableName = self.jmap.getTableNameFromResource(s)
        if self.jmap.isReadOnly(tableName):
            # XXX raise an error
            return
        table = self._getTableObject(tableName)
        self._checkConnection()
        if table is None:
            # private table is under an insert/ignore duplicate regime
            table = self.vesper_stmts
            pKeyDict["subject"] = s
            argDict = {"predicate":p, "object":o, "objecttype":ot, "context":c}
        else:
            # try update first - if it fails we'll drop through to insert
            pKeyDict = self.jmap.getPKeyDictFromResource(s)
            colName = self.jmap.getColFromPred(table.name, p)
            if colName:
                argDict = {colName : o}
                upd = table.update()
                for k, v in pKeyDict.items():
                    upd = upd.where(table.c[k] == v)
                if SPEW:
                    print "UPDATE:", table.name, pKeyDict, colName, \
                        o, ot, argDict
                result = self.conn.execute(upd, argDict)
                if result.rowcount:
                    return result.rowcount

        # update failed - try inserting new row
        for k, v in pKeyDict.items():
            argDict[k] = v

        # we're responsible for inserting foreign keys in referencing tables!?
        # refFKeys ==> [(tbl, col, vals),...]
        # refFKeys = self.jmap.getRefFKeysFromTable(table.name)
        if SPEW:
            if refFKeys:
                print "refFKeys:"
                pprint.PrettyPrinter(indent=2).pprint(refFKeys)

        if SPEW:
            print "ADD:", table.name, pKeyName, pKeyValues, colName, o, argDict
        ins = table.insert()
        if self.engine.name == 'postgresql':
            if table == self.vesper_stmts:
                ins = text('''
                           select insert_ignore_duplicates(
                               :subject, :predicate, :object, 
                               :objecttype, :context)''').\
                           execution_options(autocommit=self.autocommit)
        else:
            if self.engine.name == "sqlite":
                ins = ins.prefix_with("OR IGNORE")
            elif self.engine.name == "mysql":
                ins = ins.prefix_with("IGNORE")
        result = self.conn.execute(ins, argDict)
        return result.rowcount


    def _addCompoundStatement(self, compoundStmt):
        '''
        This is a stretch - compoundStmt is a pretty direct implementation 
        of http://www.w3.org/TR/rdb-direct-mapping/#no-pk ...
        e.g. list containing an rdf:type identifying the (non-primary-key)
        table, followed by a set of blank node triples which, taken as a
        whole, describe a complete row to be inserted into this table.
        If the insert throws a duplicate error we should ignore and return. 
        tuple compoundStmt:
        (Statement(RSRC_path, "rdf:type", <table_name>), [Statement1,...N])
        '''
        argDict = {}
        colNames = []
        (tblID, tblData) = compoundStmt
        tableName = tblID.object
        table = self._getTableObject(tableName)
        for stmt in tblData:
            argDict[stmt.predicate] = stmt.object
        self._checkConnection()
        ins = table.insert()
        if self.engine.name == 'postgresql':
            print "postgres insert ignore function NOT defined!"
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
            if isinstance(stmt[1], list):
                if SPEW: print stmt
                rc += self._addCompoundStatement(stmt)
            else:
                rc += self.addStatement(stmt)
        return rc


    def removeStatement(self, stmt):
        s, p, o, ot, c = stmt
        cmd = pKeyDict = colName = None
        if s:
            tableName = self.jmap.getTableNameFromResource(s)
            pKeyDict = self.jmap.getPKeyDictFromResource(s)
        if not tableName:
            if p == 'rdf:type':
                tableName = o
        if self.jmap.isReadOnly(tableName):
            # raise an error
            return
        table = self._getTableObject(tableName)
        if table is None:
            # vesperize it and fall through for quick exit
            cmd = self.vesper_stmts.delete().where(
                (self.vesper_stmts.c.subject == stmt[0]) &
                (self.vesper_stmts.c.predicate == stmt[1]) &
                (self.vesper_stmts.c.object == stmt[2]) &
                (self.vesper_stmts.c.objecttype == stmt[3]) &
                (self.vesper_stmts.c.context == stmt[4]))
        else:
            if p == 'rdf:type':
                # uri 'rdf:type' tbl ==> delete * from table <<uri/tbl>>
                cmd = table.delete()
            elif not p and pKeyDict:
                # subj/id none none ==> delete row where subj == id
                cmd = table.delete()
                for k, v in pKeyDict.items():
                    cmd = cmd.where(table.c[k] == v)
            elif p and pKeyDict and not o:
                # subj/id pred none ==> null pred where subj == id
                colName = self.jmap.getColFromPred(table.name, p)
                cmd = table.update().values({colName : ''})
                for k, v in pKeyDict.items():
                    cmd = cmd.where(table.c[k] == v)
            elif p and pKeyDict and o:
                # subj/id pred obj ==> null pred where subj==id and pred==obj
                colName = self.jmap.getColFromPred(table.name, p)
                cmd = table.update().values({colName : ''})
                cmd = cmd.where(table.c[colName] == o)
                for k, v in pKeyDict.items():
                    cmd = cmd.where(table.c[k] == v)
            elif p and not pKeyDict and not o:
                # tbl col none ==> null pred
                colName = self.jmap.getColFromPred(table.name, p)
                cmd = table.update().values({colName : ''})
            else:
                pass

                '''
                prop = self.mapping[p]
                if props['references']:
                    # this is a foreign key 
                    if key != "id":
                        #  cmd = update([table] set " + key+" = null 
                        {colName : ''}
                        upd = table.update().\
                            where(table.c[pKeyName] == pKeyValue)
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
