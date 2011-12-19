#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
__all__ = ['AlchemySQLStore']

import os, os.path

# MADNESS!
import sqlalchemy
from sqlalchemy import engine, sql, create_engine
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import func, or_, and_
from sqlalchemy.types import *
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

    def __init__(self, source = None, defaultStatements = None, **kw):
        if source is None:
            # this seems like a reasonable default thing to do 
            source = 'sqlite://'
            log.debug("SQLite in-memory database being opened")

        # We take source to be a SQLAlchemy-style dbapi spec: 
        # dialect+driver://username:password@host:port/database
        # connection is made JIT on first connect()
        log.debug("sqla engine being created with:", source)

        self.engine = create_engine(source, echo=True)
        self.md = sqlalchemy.schema.MetaData()
        # utterly insufficient datatypes. just for first pass
        # technically the keep_existing bool is redundant as create_all() default is "check first"
        self.vesper_stmts = Table('vesper_stmts', self.md, 
                             Column('subject', String(255)),    # primary_key = True),
                             Column('predicate', String(255)),  # primary_key = True),
                             Column('object', String(255)),     # primary_key = True),
                             Column('objecttype', String(8)),
                             Column('context', String(8)),
                             UniqueConstraint('subject', 'predicate', 'object', 'objecttype', 'context'),
                             keep_existing = True)
        Index('idx_vs', self.vesper_stmts.c.subject, self.vesper_stmts.c.predicate, self.vesper_stmts.c.object) 
        self.md.create_all(self.engine)
        self.trans = None
        self.conn = None

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

        arity = False                              # True ==> concatenated condition
        # Build the select clause
        if not asQuad and not fc:
            query = select([self.vesper_stmts.c.subject, 
                            self.vesper_stmts.c.predicate,
                            self.vesper_stmts.c.object, 
                            self.vesper_stmts.c.objecttype,
                            func.min(self.vesper_stmts.c.context).label('context')])
        else:        # asQuad is True
            query = self.vesper_stmts.select()
        if fs:
            query = query.where(self.vesper_stmts.c.subject == subject)
            arity = True
        if fp:
            query = query.where(self.vesper_stmts.c.predicate == predicate)
            arity = True
        if fo:
            query = query.where(self.vesper_stmts.c.object == object)
            arity = True
        if fot: 
            query = query.where(self.vesper_stmts.c.objecttype == objecttype)
            arity = True
        if fc:
            query = query.where(self.vesper_stmts.c.context == context)
        if not asQuad and not fc:
            query = query.group_by(self.vesper_stmts.c.subject,
                                   self.vesper_stmts.c.predicate,
                                   self.vesper_stmts.c.object,
                                   self.vesper_stmts.c.objecttype)
        if limit is not None:
            query = query.limit(limit)
        if offset is not None:
            query = query.offset(offset)

        # our query is contructed, let's get some rows
        stmts = []
        self.conn = self.engine.connect()
        result = self.conn.execute(query)
        for r in result:
            stmts.append( Statement(r['subject'], r['predicate'], r['object'], r['objecttype'], r['context']) )
           
        log.debug("stmts returned: ", len(stmts), stmts)
        return stmts

    def addStatement(self, stmt):
        '''add the specified statement to the model'''
        log.debug("addStatement called with ", stmt)
        
        result = self.engine.execute(
            self.vesper_stmts.insert(prefixes=['OR IGNORE']),
            {'subject' : stmt[0],
             'predicate' : stmt[1],
             'object' : stmt[2],
             'objecttype' : stmt[3],
             'context' : stmt[4]})
        return result.rowcount

    def addStatements(self, stmts):
        '''adds multiple statements to the model'''
        log.debug("addStatement called with ", stmts)
        
        result = self.engine.execute(
            self.vesper_stmts.insert(prefixes=['OR IGNORE']),
            [{'subject' : stmt[0],
              'predicate' : stmt[1],
              'object' : stmt[2],
              'objecttype' : stmt[3], 
              'context' : stmt[4]} for stmt in stmts])
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
        result = self.engine.execute(rmv)
        return result.rowcount

    def removeStatements(self, stmts):
        '''removes multiple statements from the model'''
        log.debug("removeStatements called with: ", stmts)

        wc = []
        for stmt in stmts:
            wc.append((self.vesper_stmts.c.subject == stmt[0]) & 
                      (self.vesper_stmts.c.predicate == stmt[1]) & 
                      (self.vesper_stmts.c.object == stmt[2]) & 
                      (self.vesper_stmts.c.objecttype == stmt[3]) & 
                      (self.vesper_stmts.c.context == stmt[4]))
        rmv = self.vesper_stmts.delete().where(or_(*wc))
        result = self.engine.execute(rmv)
        return result.rowcount

    def begin(self):
        if self.trans is None:
            if self.conn is not None:
                self.trans = self.conn.begin()

    def commit(self, **kw):
        if self.trans is not None:
            if self.conn is not None:
                self.trans = self.conn.commit()
        self.trans = None

    def rollback(self):
        if self.trans is not None:
            if self.conn is not None:
                self.conn.rollback()
        self.trans = None

    def close(self):
        log.debug("closing!")
        if self.conn is not None:
            self.conn.close()

