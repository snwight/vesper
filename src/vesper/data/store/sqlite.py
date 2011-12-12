#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
__all__ = ['SqliteStore', 'TransactionSqliteStore']

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
            print("in-memory database being opened")
        else:
            source = os.path.abspath(source)
            log.debug("on-disk database being opened at ", source)
            print "on-disk database being opened at ", source

#        self.conn = sqlite3.connect(source, isolation_level=None)     # 'DEFERRED') 
        self.conn = sqlite3.connect(source)
        curs = self.conn.cursor()
        self.txnState = TxnState.BEGIN
        curs.execute("create table if not exists vesper_stmts (\
subject, predicate, object, objecttype, context not null, \
unique (subject, predicate, object, objecttype, context) )" )

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

        arity = False                              # True ==> concatenated conditions
        sqlparams = []                             # argument list for prepare/execute
        sqlstmt = 'select * from vesper_stmts' 
        if not asQuad:
            sqlstmt += ' where context = ( select min(context) from vesper_stmts'
            fc = False                            # override any specified context 
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
        if not asQuad:
            sqlstmt += ' )'                      # close up that sub-select clause
        if limit is not None:
            sqlstmt += ' limit ?'
            sqlparams.append(str(limit))
        if offset is not None:
            sqlstmt += ' offset ?'
            sqlparams.append(str(offset))

        # our query is contructed, let's get some rows
        #        print "sqlstmt: ", sqlstmt
        #        print "sqlparams: ", sqlparams

        self.conn.row_factory = sqlite3.Row
        self.conn.text_factory = sqlite3.OptimizedUnicode
        curs = self.conn.cursor()
        curs.execute(sqlstmt, sqlparams)
        stmts = []
        for r in curs:
            print r
            stmts.append( Statement(r['subject'], r['predicate'], r['object'], r['objecttype'], r['context']) ) 

        # sqlite returns -1 on successful select()... 
        log.debug("stmts returned: ", stmts)
        return stmts

    def addStatement(self, stmt):
        '''add the specified statement to the model'''
        log.debug("addStatement called with ", stmt)
        self.txnState = TxnState.DIRTY
        curs = self.conn.cursor()
        curs.execute("insert or ignore into vesper_stmts values (?, ?, ?, ?, ?)",  stmt)
        return curs.rowcount == 1

    def addStatements(self, stmts):
        '''adds multiple statements to the model'''
        log.debug("addStatement called with ", stmts)
        self.txnState = TxnState.DIRTY
        curs = self.conn.cursor()
        curs.executemany("insert or ignore into vesper_stmts values (?, ?, ?, ?, ?)",  stmts)
        return curs.rowcount > 0

    def removeStatement(self, stmt):
        '''removes the statement from the model'''
        log.debug("removeStatement called with: ", stmt)
        self.txnState = TxnState.DIRTY
        curs = self.conn.cursor()
        curs.execute("delete from vesper_stmts where (\
subject = ? AND predicate = ? AND object = ? AND objecttype = ? AND context = ? )",  stmt)
        return curs.rowcount == 1

    def commit(self):
        log.debug("commit called with: " , self.txnState)
        if self.txnState == TxnState.DIRTY: 
            self.conn.commit()
        self.txnState = TxnState.BEGIN

    def rollback(self):
        log.debug("rollback called with: ", self.txnState)
        if self.txnState == TxnState.DIRTY:
            self.conn.rollback()
        self.txnState = TxnState.BEGIN

    def close(self):
        log.debug("closing!")
        self.conn.close()
