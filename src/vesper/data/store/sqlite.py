#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
__all__ = ['SqliteStore', 'TransactionSqliteStore']

import os, os.path
import logging

import sqlite3

from vesper.backports import *
from vesper.data.base import * # XXX

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
        if source is None or source == ":memory:":
            print "in-memory database being opened"
        else:
            source = os.path.abspath(source)
            print "on-disk database being opened at ", source
        self.conn = sqlite3.connect(source)
        curs = self.conn.cursor()
        curs.execute("create table if not exists vesper_stmts ( \
subject text, predicate text, object text, objecttype text, context text )")
        curs.execute("create index if not exists vesper_stmts_idx on vesper_stmts ( \
subject, predicate )")


    def getStatements(self, subject=None, predicate=None, object=None,
                      objecttype=None, context=None, asQuad=True, hints=None):
        ''' 
        Return all the statements in the model that match the given arguments.
        Any combination of subject and predicate can be None, and any None slot is
        treated as a wildcard that matches any value in the model.
        nb:
        p o t => c s
        s => p o t c 
        
        If subject is specified, use subject index, 
        get_both if predicate is specified 
        
        if predicate, use property index
        
        if only object or scope is specified, get all and search manually
        
        XXX else: get all: use subject index, regenerate json_seq stmts
        
        ? do a manual scan if subject list bnode

        '''
        fs = subject is not None
        fp = predicate is not None
        fo = object is not None
        fot = objecttype is not None
        fc = context is not None 
        hints = hints or {}
        limit = hints.get('limit')
        offset = hints.get('offset')

        print "s p o ot c quad lim offset: ", fs, fp, fo, fot, fc, asQuad, limit, offset

        if fo:
            if isinstance(object, ResourceUri):
                object = object.uri
                fot = True
                objecttype = OBJECT_TYPE_RESOURCE
            elif not fot:
                objecttype = OBJECT_TYPE_LITERAL

        sqlstmt = 'select distinct * from vesper_stmts'
        if fs | fp | fo | fot | fc:
            sqlstmt += ' where'
        arity = False                              # terribly ugly but so
        if fs: 
            sqlstmt += ' subject = \'' + subject + '\''
            arity = True
        if fp:
            if arity: 
                sqlstmt += ' AND'
            sqlstmt += ' predicate = \'' + predicate + '\''
            arity = True
        if fo:
            if arity: 
                sqlstmt += ' AND'
            sqlstmt += ' object = \'' + object + '\''
            arity = True
        if fot: 
            if arity: 
                sqlstmt += ' AND'
            sqlstmt += ' objecttype = \'' + objecttype + '\''
            arity = True
            
        if fc:
            if arity: 
                sqlstmt += ' AND'
            sqlstmt += ' context = \'' + context + '\''

        # and we always sort results
        sqlstmt += ' order by subject'

        # paginate results as requested
        if limit:
            sqlstmt += ' limit ' + str(limit)
        if offset:
            sqlstmt += ' offset ' + str(offset)

        # our query is contructed, let's get some rows
        print "query stmt: ", sqlstmt 
        self.conn.row_factory = sqlite3.Row
        self.conn.text_factory = sqlite3.OptimizedUnicode
        curs = self.conn.cursor()
        curs.execute(sqlstmt) 
        stmts = []
        for r in curs:
            stmts.append( Statement(r['subject'], r['predicate'], r['object'], r['objecttype'], r['context']) )            

        if not asQuad:
            # find distinct s p o ot sets, ignore contexts
            stmts = removeDupStatementsFromSortedList(stmts, asQuad)

        print "stmts returned: ", stmts 
        return stmts

    def addStatement(self, stmt):
        '''add the specified statement to the model'''
        print "addStatement called with ", stmt 
        s, p, o, t, c = stmt
        self.conn.execute("insert into vesper_stmts values (?, ?, ?, ?, ?)",  (s, p, o, t, c))
        return True
        
    def addStatements(self, stmts):
        print "addStatements called with ", stmts 
        for elem in stmts:
            self.addStatement(elem)
        return True

    def removeStatement(self, stmt):
        '''removes the statement'''
        # p o t => c s
        # s => p o t c
        # I assume here that statement is fully specified! -snwight
        s, p, o, t, c = stmt
        self.conn.execute("delete from vesper_stmts where (\
subject = ? AND predicate = ? AND object = ? AND objecttype = ? AND context = ? )",  (s, p, o, t, c))
        return True

    def commit(self):
        # are we committed?
        print "committing!"
        self.conn.commit()

    def close(self):
        # are we committed?
        self.conn.close()

class TransactionSqliteStore(TransactionModel, SqliteStore):
    '''
    Provides in-memory transactions to BdbStore

    '''
