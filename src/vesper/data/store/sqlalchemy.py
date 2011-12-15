#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
__all__ = ['SqliteStore']

import os, os.path

import sqlalchemy
# from sqlalchemy import Table, Column, Integer, String, MetaData

from vesper.backports import *
from vesper.data.base import * # XXX
import logging 
log = logging.getLogger("sqlalchemy")

class SqlAlchemyStore(Model):
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
        self.engine = create_engine(source)
        self.metadata = MetaData()
        # utterly insufficient datatypes. just for first pass
        # technically the keep_existing bool is redundant as create_all() default is "check first"
        vesper_stmts = Table('vesper_stmts', self.metadata, 
                             Column('subject', String(255), primary_key = True),
                             Column('predicate', String(255), primary_key = True),
                             Column('object', String(255)),
                             Column('objecttype', String(8)),
                             Column('context', Integer),
                             UniqueConstraint('subject', 'predicate', 'object', 'objecttype', 'context'),
                             keep_existing = True)
        Index('idx_vs', vesper_stmts.c.subject, vesper_stmts.c.predicate, vesper_stmts.c.object) 
        metadata.create_all(self.engine)

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
        query = vesper_stmts.select()
        if not asQuad and not fc:
            query.select(vesper_stmts.c.subject, \
                             vesper_stmts.c.predicate, \
                             vesper_stmts.c.object, \
                             vesper_stmts.c.objecttype \
                             [func.min(vesper_stmts.c.context).label('cntxt')])
        else:        # asQuad is True
            query.select(vesper_stmts.c.subject, \
                             vesper_stmts.c.predicate, \
                             vesper_stmts.c.object, \
                             vesper_stmts.c.objecttype \
                             (vesper_stmts.c.context).label('cntxt'))
            
        if fs:
            if arity: 
                query = query.append_whereclause(vesper_stmts.c.subject == subject)
            else:
                query = query.where(vesper_stmts.c.subject == subject)
            arity = True
        if fp:
            if arity: 
                query = query.append_whereclause(vesper_stmts.c.predicate == predicate)
            else:
                query = query.where(vesper_stmts.c.predicate == predicate)
            arity = True
        if fo:
            if arity: 
                query = query.append_whereclause(vesper_stmts.c.object == object)
            else:
                query = query.where(vesper_stmts.c.object == object)
            arity = True
        if fot: 
            if arity: 
                query = query.append_whereclause(vesper_stmts.c.objecttype == objecttype)
            else:
                query = query.where(vesper_stmts.c.objecttype == objecttype)
            arity = True
        if fc:
            if arity: 
                query = query.append_whereclause(vesper_stmts.c.context == context)
            else:
                query = query.where(vesper_stmts.c.context == context)

        if not asQuad and not fc:
            query.group_by(vesper_stmts.c.subject, \
                               vesper_stmts.c.predicate, \
                               vesper_stmts.c.object, \
                               vesper_stmts.c.objecttype)
        if limit is not None:
            query = query.limit(limit)
        if offset is not None:
            query = query.offset(offset)

        # our query is contructed, let's get some rows
        stmts = []
        self.conn = self.engine.connect()
        result = self.conn.execute(query)
        for r in result:
            stmts.append( Statement(r['subject'], r['predicate'], r['object'], r['objecttype'], r['cntxt']) )
            
        # sqlite returns -1 on successful select()... 
        log.debug("stmts returned: ", stmts)
        return stmts

    def addStatement(self, stmt):
        '''add the specified statement to the model'''
        log.debug("addStatement called with ", stmt)

#        curs.execute("insert or ignore into vesper_stmts values (?, ?, ?, ?, ?)",  stmt)

        return True    # curs.rowcount == 1

    def addStatements(self, stmts):
        '''adds multiple statements to the model'''
        log.debug("addStatement called with ", stmts)

#        curs.executemany("insert or ignore into vesper_stmts values (?, ?, ?, ?, ?)",  stmts)

        return True    # curs.rowcount > 0

    def removeStatement(self, stmt):
        '''removes the statement from the model'''
        log.debug("removeStatement called with: ", stmt)

#        curs.execute("delete from vesper_stmts where (\
v#subject = ? AND predicate = ? AND object = ? AND objecttype = ? AND context = ? )",  stmt)
 
       return True     # curs.rowcount == 1

    def removeStatements(self, stmts):
        '''removes multiple statements from the model'''
        log.debug("removeStatements called with: ", stmts)

#        curs.executemany("delete from vesper_stmts where (\
#subject = ? AND predicate = ? AND object = ? AND objecttype = ? AND context = ? )",  stmts)

        return True     # curs.rowcount > 0

    def begin(self):
        if self.trans is None:
            self.trans = self.conn.begin()

    def commit(self, **kw):
        if self.trans is not None:
            self.trans = self.conn.commit()
        self.trans = None

    def rollback(self):
        if self.trans is not None:
            self.conn.rollback()
        self.trans = None

    def close(self):
        log.debug("closing!")
        self.conn.close()
