#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
__all__ = ['SqliteStore']

import os, os.path
import sqlite3
from vesper.backports import *
from vesper.data.base import * # XXX
import logging 
log = logging.getLogger("sqlite")

class SqliteStore(Model):
    '''
    datastore using SQLite DB using Python's sqlite3 module
    
    create table vesper_stmts (
      subject 
      predicate
      object 
      objecttype
      context
    )

    '''
    
    def __init__(self, source = None, defaultStatements = None, **kw):
        if source is None:
            source = ':memory:'
            log.debug("in-memory database being opened")
        else:
            source = os.path.abspath(source)
            log.debug("on-disk database being opened at ", source)

#        self.conn = sqlite3.connect(source, isolation_level=None)     # 'DEFERRED') 
        self.conn = sqlite3.connect(source)
        curs = self.conn.cursor()
        curs.execute("create table if not exists vesper_stmts (\
subject, predicate, object, objecttype, context not null, \
unique (subject, predicate, object, objecttype, context) )" )

    def _set_autocommit(self, set):
        if set:
            self.conn.isolation_level = None
        else:
            self.conn.isolation_level = 'DEFERRED'

    autocommit = property(lambda self: not self.conn.isolation_level, _set_autocommit)

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
        sqlparams = []                             # argument list for prepare/execute
        sqlstmt = 'select * from vesper_stmts' 
        if not asQuad and not fc:
            sqlstmt = 'select subject, predicate, object, objecttype, min(context) from vesper_stmts'

        if fs | fp | fo | fot | fc:
            sqlstmt += ' where'                   # at least one column constraint
        if fs: 
            if arity: 
                sqlstmt += ' AND'
            sqlstmt += ' subject = ?'
            sqlparams.append(subject)
            arity = True
        if fp:
            if arity: 
                sqlstmt += ' AND'
            sqlstmt += ' predicate = ?'
            sqlparams.append(predicate)
            arity = True
        if fo:
            if arity: 
                sqlstmt += ' AND'
            sqlstmt += ' object = ?'
            sqlparams.append(object)
            arity = True
        if fot: 
            if arity: 
                sqlstmt += ' AND'
            sqlstmt += ' objecttype = ?'
            sqlparams.append(objecttype)
            arity = True
        if fc:
            if arity: 
                sqlstmt += ' AND'
            sqlstmt += ' context = ?'  
            sqlparams.append(context)
        if not asQuad and not fc:
            sqlstmt += ' group by subject, predicate, object, objecttype'
        if limit is not None:
            sqlstmt += ' limit ?'
            sqlparams.append(str(limit))
        if offset is not None:
            sqlstmt += ' offset ?'
            sqlparams.append(str(offset))

        # our query is contructed, let's get some rows
        #print "sqlstmt: ", sqlstmt
        #print "sqlparams: ", sqlparams

        self.conn.row_factory = sqlite3.Row
        self.conn.text_factory = sqlite3.OptimizedUnicode
        curs = self.conn.cursor()
        curs.execute(sqlstmt, sqlparams)
        stmts = []
        for r in curs:
#            stmts.append( Statement(r['subject'], r['predicate'], r['object'], r['objecttype'], r['context']) ) 
            stmts.append( Statement(r[0], r[1], r[2], r[3], r[4]) ) 
            
        # sqlite returns -1 on successful select()... 
        log.debug("stmts returned: ", stmts)
        return stmts

    def addStatement(self, stmt):
        '''add the specified statement to the model'''
        log.debug("addStatement called with ", stmt)
        curs = self.conn.cursor()
        curs.execute("insert or ignore into vesper_stmts values (?, ?, ?, ?, ?)",  stmt)
        return curs.rowcount == 1

    def addStatements(self, stmts):
        '''adds multiple statements to the model'''
        log.debug("addStatement called with ", stmts)
        curs = self.conn.cursor()
        curs.executemany("insert or ignore into vesper_stmts values (?, ?, ?, ?, ?)",  stmts)
        return curs.rowcount > 0

    def removeStatement(self, stmt):
        '''removes the statement from the model'''
        log.debug("removeStatement called with: ", stmt)
        curs = self.conn.cursor()
        curs.execute("delete from vesper_stmts where (\
subject = ? AND predicate = ? AND object = ? AND objecttype = ? AND context = ? )",  stmt)
        return curs.rowcount == 1

    def removeStatements(self, stmts):
        '''removes multiple statements from the model'''
        log.debug("removeStatements called with: ", stmts)
        curs = self.conn.cursor()
        curs.executemany("delete from vesper_stmts where (\
subject = ? AND predicate = ? AND object = ? AND objecttype = ? AND context = ? )",  stmts)
        return curs.rowcount > 0

    def commit(self, **kw):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        log.debug("closing!")
        self.conn.close()
