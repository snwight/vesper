#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
__all__ = ['SqliteStore', 'TransactionSqliteStore']

import os, os.path
import sqlite3
from vesper.backports import *
from vesper.data.base import * # XXX
import logging 
log = logging.getLogger("sqlite")

def stmt_generator(stmts):
    for elem in stmts:
        yield elem

class SqliteStore(Model):
    '''
    datastore using SQLite DB using Python's sqlite3 module
    
    create table vesper_stmts (
      create index (
      subject 
      predicate
      ) 
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

        self.conn = sqlite3.connect(source)
        curs = self.conn.cursor()
        curs.execute("create table if not exists vesper_stmts (\
subject, predicate, object, objecttype, context, \
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
        print "s p o ot c quad lim offset: ", fs, fp, fo, fot, fc, asQuad, limit, offset

        if fo:
            if isinstance(object, ResourceUri):
                object = object.uri
                fot = True
                objecttype = OBJECT_TYPE_RESOURCE
            elif not fot:
                objecttype = OBJECT_TYPE_LITERAL

        sqlstmt = []
        sqlstmt = 'select * from vesper_stmts'

        if fs | fp | fo | fot | fc:
            sqlstmt += ' where'
        
        sqlparams = []
        arity = False                              # terribly ugly but so
        if fs: 
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
        if not asQuad:
            if arity: 
                sqlstmt += ' AND'
            else:
                sqlstmt += ' where'        # I know! I know!!!
            sqlstmt += ' context = (select min(context) from vesper_stmts)'
            limit = 1
            offset = 0
        elif fc:
            if arity: 
                sqlstmt += ' AND'
            sqlstmt += ' context = ?'  
            sqlparams.append(context)

        sqlstmt += ' group by subject, predicate, object, objecttype, context '

        if limit is not None:
            sqlstmt += ' limit ?'
            sqlparams.append(str(limit))
        if offset is not None:
            sqlstmt += ' offset ?'
            sqlparams.append(str(limit))

        # our query is contructed, let's get some rows
        print "sqlstmt: ", sqlstmt
        print "sqlparams: ", sqlparams

        self.conn.row_factory = sqlite3.Row
        self.conn.text_factory = sqlite3.OptimizedUnicode
        curs = self.conn.cursor()
        curs.execute(sqlstmt, sqlparams)
        stmts = []
        for r in curs:
            print r
            stmts.append( Statement(r['subject'], r['predicate'], r['object'], r['objecttype'], r['context']) )

#        stmts.sort()
#        if not asQuad:
#            # find distinct s p o ot sets, ignore contexts
#            stmts = removeDupStatementsFromSortedList(stmts, asQuad)

        log.debug("stmts returned: ", stmts)
        return stmts

    def addStatement(self, stmt):
        '''add the specified statement to the model'''
        log.debug("addStatement called with ", stmt)
        print "addStatement called with ", stmt
        self.conn.execute("insert or ignore into vesper_stmts values (?, ?, ?, ?, ?)",  stmt)
        return True

    def addStatements(self, stmts):
        log.debug("addStatement called with ", stmts)
        print "addStatements called with ", stmts
#        for elem in stmts:
#            self.addStatement(elem)
        self.conn.executemany("insert or ignore into vesper_stmts values (?, ?, ?, ?, ?)",  stmt_generator(stmts))
        return True

    def removeStatement(self, stmt):
        '''removes the statement'''
        s, p, o, t, c = stmt
        log.debug("removeStatement called with: ", stmt)
        self.conn.execute("delete from vesper_stmts where (\
subject = ? AND predicate = ? AND object = ? AND objecttype = ? AND context = ? )",  (s, p, o, t, c))
        return True

    def commit(self):
        log.debug("committing!")
        self.conn.commit()

    def rollback(self):
        log.debug("rolling back!")
        self.conn.rollback()

    def close(self):
        # are we committed?
        log.debug("closing!")
        self.conn.close()

class TransactionSqliteStore(TransactionModel, SqliteStore):
    '''
    Provides in-memory transactions to BdbStore

    '''
