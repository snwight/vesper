#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
'''
A Berkeley DB adapter
=====================

DB Maintenance notes
--------------------

Need to run checkpoint periodically: "Therefore, deciding how frequently to run a checkpoint 
is one of the most common tuning activity for DB applications."
http://download.oracle.com/docs/cd/E17076_02/html/gsg_txn/C/filemanagement.html#checkpoints

Need to delete log files periodically:
"By default DB does not delete log files for you. For this reason, DB's log files 
will eventually grow to consume an unnecessarily large amount of disk space. 
To guard against this, you should periodically take administrative action 
to remove log files that are no longer in use by your application." 
http://download.oracle.com/docs/cd/E17076_02/html/gsg_txn/C/logfileremoval.html
'''

__all__ = ['BdbStore']

import os, os.path
import logging

try:
    import bsddb, bsddb.db
except ImportError:
    #OS X default python install doesn't include bsddb
    #need to Berkeley DB and then easy_install bsddb3
    #or use the Python version found on python.org
    import bsddb3 as bsddb 
    import bsddb3.db
    assert bsddb.db


from vesper.backports import *
from vesper.data.base import * # XXX

try:
    bsddb.db.DB_GET_BOTH_RANGE
except AttributeError:
    #you have an old version of bsddb
    bdbver = bsddb.db.version()
    if bdbver < (4,5):    
        bsddb.db.DB_GET_BOTH_RANGE = 12
    else:
        bsddb.db.DB_GET_BOTH_RANGE = 10

log = logging.getLogger("bdb")

def _to_safe_str(s):
    "Convert any unicode strings to utf-8 encoded 'str' types"
    if isinstance(s, unicode):
        s = s.encode('utf-8')
    elif not isinstance(s, str):
        s = str(s)
    if '\0' in s:
        raise RuntimeError(r'strings with \0 can not be saved in BdbStore')
    return s 

def _encodeValues(*args):
    ''' 
    ensure (a,bb) is before (ab,a)
    We do this by using \\0 as the delimiter
    And don't allow \\0 as a valid character 
    '''
    return '\0'.join(map(_to_safe_str, args))
    
def _btopen(env, file, txn=None, mode=0666,
            btflags=0, cachesize=None, maxkeypage=None, minkeypage=None,
            pgsize=None, lorder=None):

    # bsddb._checkflag(flag, file)
    flags = bsddb.db.DB_CREATE | bsddb.db.DB_MULTIVERSION
    if not txn: #autocommit:
        flags |= bsddb.db.DB_AUTO_COMMIT
    d = bsddb.db.DB(env)
    if pgsize is not None: d.set_pagesize(pgsize)
    if lorder is not None: d.set_lorder(lorder)
    #XXX btflags |= DB_REVSPLITOFF see:
    #http://download.oracle.com/docs/cd/E17076_02/html/gsg_txn/C/reversesplit.html
    d.set_flags(btflags)
    if minkeypage is not None: d.set_bt_minkey(minkeypage)
    if maxkeypage is not None: d.set_bt_maxkey(maxkeypage)
    d.open(file, dbtype=bsddb.db.DB_BTREE, flags=flags, mode=mode, txn=txn)
    return bsddb._DBWithCursor(d)

class BdbStore(Model):
    '''
    datastore using Berkeley DB using Python's bsddb module
    
    two b-tree databases with sorted duplicates

    p o t => c s
    
    s => p o t c 

    where
        
    s subject
    p predicate
    o object value
    t objecttype
    c context (scope)
    
    keys are stored so that lexigraphic sort work properly
    '''
    #add list info to each object 
    
    debug=0
    updateAdvisory = True
     
    def __init__(self, source, defaultStatements=None, autocommit = False, **kw):
        if source is not None:
            source = os.path.abspath(source) # bdb likes absolute paths for everything
            log.debug("opening db at:" + source)
            # source should specify a directory
            if not os.path.exists(source):
                os.makedirs(source)
            assert os.path.isdir(source), "Bdb source must be a directory"
            
            pPath = os.path.join(source, 'pred_db')
            sPath = os.path.join(source, 'subj_db')
            newdb = not os.path.exists(pPath)
        else:
            newdb = True
            pPath = sPath = None

        self.autocommit = autocommit

        log.debug("pPath:" + pPath)
        log.debug("sPath:" + sPath)
        log.debug("is new:" + str(newdb))

        db = bsddb.db
        self.env = db.DBEnv()
        self.env.set_lk_detect(db.DB_LOCK_DEFAULT)
        #for flags see http://docs.oracle.com/cd/E17076_02/html/gsg_txn/C/enabletxn.html
        self.env.open(source, db.DB_CREATE | db.DB_INIT_LOG | db.DB_INIT_MPOOL | db.DB_INIT_TXN | db.DB_INIT_LOCK)
        #for performance set: DB_TXN_WRITE_NOSYNC or DB_TXN_NOSYNC 
        #or even env.log_set_config(db.DB_LOG_IN_MEMORY, True) 

        self._txn = None
        self._checkAutoCommit()

        # DB_DUPSORT is faster than DB_DUP
        self.pDb = _btopen(self.env, pPath, self._txn, btflags=bsddb.db.DB_DUPSORT) 
        self.pDb.db.set_get_returns_none(2)
        self.sDb = _btopen(self.env, sPath, self._txn, btflags=bsddb.db.DB_DUPSORT)         
        self.sDb.db.set_get_returns_none(2)
        if newdb and defaultStatements:            
            self.addStatements(defaultStatements) 
        self.commit()

    def __del__(self):
        if self._txn:
            log.debug("aborting txn in close")
            self._txn.abort()

    def close(self):
        log.debug("closing db")
        if self._txn:
            log.debug("aborting txn in close")
            self._txn.abort()
        self._txn = None        
        self.pDb.close()
        self.sDb.close()
        self.env.close()
        
    def getStatements(self, subject = None, predicate = None, object = None,
                      objecttype=None,context=None, asQuad=True, hints=None):
        ''' 
        Return all the statements in the model that match the given arguments.
        Any combination of subject and predicate can be None, and any None slot is
        treated as a wildcard that matches any value in the model.
        '''
        #if subject is specified, use subject index, 
        #  with/get_both if predicate is specified 
        #if predicate, use property index
        #if only object or scope is specified, get all and search manually
        #else: get all: use subject index, regenerate json_seq stmts
        #do a manual scan if subject list bnode
        fs = subject is not None
        fp = predicate is not None
        fo = object is not None
        fot = objecttype is not None
        fc = context is not None
        hints = hints or {}
        limit=hints.get('limit')
        offset=hints.get('offset')
        
        #to prevent locking when reading we only use a txn if its already been created
        #and use DB_TXN_SNAPSHOT (db needs to be DB_MULTIVERSION)
        cursorflags = bsddb.db.DB_TXN_SNAPSHOT
        txn = self._txn
        #XXX we want to use bsddb.db.DB_CURSOR_BULK but not in bsdbd.db defines
        
        if fo:
            if isinstance(object, ResourceUri):
                object = object.uri
                fot = True
                objecttype = OBJECT_TYPE_RESOURCE
            elif not fot:
                objecttype = OBJECT_TYPE_LITERAL

        stmts = []
        if fs: 
            subject = _to_safe_str(subject)
            #if subject is specified, use subject index            
            scursor = self.sDb.db.cursor(txn, flags=cursorflags)
            if fp:
                val = _to_safe_str(predicate)
                if fo:
                    val += '\0'+ _to_safe_str(object)
                    if fot: 
                        val += '\0'+ _to_safe_str(objecttype)
                        if fc:
                            val += '\0'+_to_safe_str(context)
                #duplicates are sorted so we can position the cursor at the
                #first value we're interested
                rec = scursor.get(subject, val, bsddb.db.DB_GET_BOTH_RANGE)
            else:
                rec = scursor.set(subject)
            while rec:
                #s => p o t c 
                s, value = rec
                assert s == subject
                p, o, t, c = value.split('\0')                
                if fp:
                    #since dups are sorted we can break
                    if p != predicate:
                        break
                    if fo:
                        if o != object:
                            break
                        if fot:
                            if t != objecttype:
                                break
                            if fc:
                                if c != context:
                                    break      
                
                if ((not fo or o == object)
                    and (not fot or t == objecttype)
                    and (not fc or c == context)):            
                    stmts.append( Statement(s, p, o, t, c) )            
                rec = scursor.next_dup()
                
        elif fp:
            pcursor = self.pDb.db.cursor(txn, flags=cursorflags)            
            key = _to_safe_str(predicate)
            val = None
            if fo:
                key += '\0'+_to_safe_str(object)
                if fot: 
                    key += '\0'+_to_safe_str(objecttype)
                    if fc:
                        val = _to_safe_str(context)
                        rec = pcursor.get(key, val, bsddb.db.DB_GET_BOTH_RANGE)
            if val is None:
                rec = pcursor.set_range(key)                
                        
            while rec:                
                key, value = rec
                p, o, t = key.split('\0')                
                if p != predicate or (fo and o != object) or (fot and t != objecttype):
                    break  #we're finished with the range of the key we're interested in               
                c, s = value.split('\0')                            
                if not fc or c == context:                     
                    stmts.append( Statement(s, p, o, t, c) )                
                rec = pcursor.next()
                            
        else:            
            #get all            
            scursor = self.sDb.db.cursor(txn, flags=cursorflags)
            rec = scursor.first()
            while rec:
                s, value = rec
                p, o, t, c = value.split('\0')
                if ((not fo or o == object)
                    and (not fot or t == objecttype)
                    and (not fc or c == context)):
                    stmts.append( Statement(s, p, o, t, c) )
                rec = scursor.next()

        stmts.sort()        
        stmts = removeDupStatementsFromSortedList(stmts, asQuad, 
                                            limit=limit, offset=offset)
        return stmts
    
    def _checkAutoCommit(self):
        if self.autocommit:
            if self._txn:
                self._txn.commit()
                self._txn = None
        else:
            if not self._txn:
                #for debugging deadlocks setting bsddb.db.DB_TXN_NOWAIT is useful
                flags = 0 
                self._txn = self.env.txn_begin(flags=flags)
        
    def addStatement(self, stmt):
        '''add the specified statement to the model'''
        self._checkAutoCommit()
        
        try:
            #p o t => c s        
            self.pDb.db.put(_encodeValues(stmt[1], stmt[2], stmt[3]), 
                            _encodeValues(stmt[4], stmt[0]), 
                            txn=self._txn, flags=bsddb.db.DB_NODUPDATA)
            
            #s => p o t c
            self.sDb.db.put(_to_safe_str(stmt[0]), 
                _encodeValues(stmt[1], stmt[2], stmt[3], stmt[4]), 
                txn=self._txn, flags=bsddb.db.DB_NODUPDATA)
            
            return True
        except bsddb.db.DBKeyExistError:
            return False
        
    def removeStatement(self, stmt):
        '''removes the statement'''
        self._checkAutoCommit()
        
        #p o t => c s
        pcursor = self.pDb.db.cursor(self._txn)
        if pcursor.set_both(_encodeValues(stmt[1], stmt[2], stmt[3]), _encodeValues(stmt[4], stmt[0])):
            pcursor.delete()

        #s => p o t c
        scursor = self.sDb.db.cursor(self._txn)
        if scursor.set_both(_to_safe_str(stmt[0]), _encodeValues(stmt[1], stmt[2], stmt[3], stmt[4]) ):
            scursor.delete()
            return True
        return False
    
    def commit(self, **kw):
        if self._txn:
            self._txn.commit()
        self._txn = None
    
    def rollback(self):
        if self._txn:
            self._txn.abort()
        self._txn = None
