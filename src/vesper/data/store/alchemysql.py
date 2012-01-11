#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
__all__ = ['AlchemySQLStore']

import os, os.path

import sqlalchemy
from sqlalchemy import engine, sql, create_engine
from sqlalchemy.types import *
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import func, or_, asc
from sqlalchemy.schema import Table, Column, MetaData, UniqueConstraint, Index

from vesper.backports import *
from vesper.data.base import * # XXX
import logging 
log = logging.getLogger("alchemysql")

class AlchemySqlStore(Model):
    '''
    datastore using SQLAlchemy meta-SQL Python package
   
    create table vesper_stmts (
      subject UNIQUE
      predicate UNIQUE
      object UNIQUE
      objecttype UNIQUE
      context UNIQUE
    )
    '''
    def __init__(self, engine=None, md=None, autocommit=False, **kw):
        # XXX test for life needed
        self.engine = engine
        self.md = md

        # create our simple quin-tuple store
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

        # create our duplicate-insert-exception-ignoring stored procedure for postgresql backend
        if self.engine.name == 'postgresql':
            self.engine.execute(
"CREATE OR REPLACE FUNCTION insert_ignore_duplicates (s text, p text, o text, ot text, c text ) \
RETURNS void AS $$ BEGIN LOOP BEGIN INSERT INTO vesper_stmts VALUES (s, p, o, ot, c); RETURN; \
EXCEPTION WHEN unique_violation THEN RETURN; END; END LOOP; END $$ LANGUAGE plpgsql")

        self.md.create_all(self.engine)
        
        # attend to private matters of state
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

    def getStatements(self, subject=None, predicate=None, object=None,
                      objecttype=None, context=None, asQuad=True, hints=None):
        ''' 
        Return all the statements in the model that match the given arguments.
        Any combination of subject and predicate can be None, and any None slot is
        treated as a wildcard that matches any value in the model.
        '''
        fs = subject is not None
        fp = predicate is not None
        fo = object is not None
        fot = objecttype is not None
        fc = context is not None 
        hints = hints or {}
        limit = hints.get('limit')
        offset = hints.get('offset')

        log.debug("s p o ot c quad lim offset: ", fs, fp, fo, fot, fc, asQuad, limit, offset)

        if fo:
            if isinstance(object, ResourceUri):
                object = object.uri
                fot = True
                objecttype = OBJECT_TYPE_RESOURCE
            elif not fot:
                objecttype = OBJECT_TYPE_LITERAL

        if not asQuad and not fc:
            query = select(['subject', 'predicate', 'object', 'objecttype',
                            func.min(self.vesper_stmts.c.context).label('context')]).\
                            group_by('subject', 'predicate', 'object', 'objecttype')
        else:
            query = self.vesper_stmts.select()

        if fs:
            query = query.where(self.vesper_stmts.c.subject == subject)
        if fp:
            query = query.where(self.vesper_stmts.c.predicate == predicate)
        if fo:
            query = query.where(self.vesper_stmts.c.object == object)
        if fot: 
            query = query.where(self.vesper_stmts.c.objecttype == objecttype)
        if fc:
            query = query.where(self.vesper_stmts.c.context == context)

        if limit:
            if self.engine.name == 'postgresql':
                query = query.order_by('subject', 'predicate', 'object','objecttype', 'context')
            query = query.limit(limit)
            if offset:
                query = query.offset(offset)

        stmts = []
        self._checkConnection()
        result = self.conn.execute(query)
        for r in result:
            stmts.append( Statement(r['subject'], r['predicate'], r['object'], r['objecttype'], r['context']) )
            
        log.debug("stmts returned: ", len(stmts), stmts)
        return stmts

    def addStatement(self, stmt):
        '''add the specified statement to the model'''
        log.debug("addStatement called with ", stmt)
        
        argDict = {'subject' : stmt[0],
                   'predicate' : stmt[1],
                   'object' : stmt[2],
                   'objecttype' : stmt[3],
                   'context' : stmt[4]}

        self._checkConnection()
        if self.engine.name == 'postgresql':
            result = self.conn.execute(
                func.insert_ignore_duplicates(stmt[0], stmt[1], stmt[2], stmt[3], stmt[4]),
                argDict)
        else:
            ins = self.vesper_stmts.insert()
            if self.engine.name == "sqlite":
                ins = ins.prefix_with("OR IGNORE")
            elif self.engine.name == "mysql":
                ins = ins.prefix_with("IGNORE")
            result = self.conn.execute(ins, argDict)

        return result.rowcount

    def addStatements(self, stmts):
        '''adds multiple statements to the model'''
        log.debug("addStatement called with ", stmts)
 
        argDictList = [{'subject' : stmt[0],
                        'predicate' : stmt[1],
                        'object' : stmt[2],
                        'objecttype' : stmt[3], 
                        'context' : stmt[4]} for stmt in stmts]

        self._checkConnection()
        if self.engine.name == 'postgresql':
            # XXX injection threat -snwight
            result = self.conn.execute(
                "select insert_ignore_duplicates(%(subject)s, %(predicate)s, %(object)s, %(objecttype)s, %(context)s)", 
                argDictList)
        else:
            ins = self.vesper_stmts.insert()
            if self.engine.name == "sqlite":
                ins = ins.prefix_with("OR IGNORE")
            elif self.engine.name == "mysql":
                ins = ins.prefix_with("IGNORE")
            result = self.conn.execute(ins, argDictList)

        return result.rowcount

    def removeStatement(self, stmt):
        '''removes the statement from the model'''
        log.debug("removeStatement called with: ", stmt)
        
        rmv = self.vesper_stmts.delete().where(
                (self.vesper_stmts.c.subject == stmt[0]) &
                (self.vesper_stmts.c.predicate == stmt[1]) &
                (self.vesper_stmts.c.object == stmt[2]) &
                (self.vesper_stmts.c.objecttype == stmt[3]) &
                (self.vesper_stmts.c.context == stmt[4]))
        self._checkConnection()
        result = self.conn.execute(rmv)
        return result.rowcount

    def removeStatements(self, stmts=None):
        '''removes multiple statements from the model'''
        log.debug("removeStatements called with: ", stmts)

        wc = []
        [wc.append((self.vesper_stmts.c.subject == stmt[0]) & 
                   (self.vesper_stmts.c.predicate == stmt[1]) & 
                   (self.vesper_stmts.c.object == stmt[2]) & 
                   (self.vesper_stmts.c.objecttype == stmt[3]) & 
                   (self.vesper_stmts.c.context == stmt[4])) for stmt in stmts]

        rmv = self.vesper_stmts.delete()         
        if stmts is not None:
            rmv = self.vesper_stmts.delete().where(or_(*wc))
            
        self._checkConnection()
        result = self.conn.execute(rmv)
        return result.rowcount

    def commit(self, **kw):
        if self.conn is not None:
            if self.conn.in_transaction():
                self.trans.commit()

    def rollback(self):
        if self.conn is not None:
            if self.conn.in_transaction():
                self.trans.rollback()

    def close(self):
        log.debug("closing!")
        if self.conn is not None:
            self.conn.close()
            self.conn = None
