#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    SQLAlchemy model unit tests
"""
import unittest
import subprocess, tempfile, os, signal, sys
import string, random, shutil, time

import modelTest 
from vesper.data.store.sqlalchemy import SqlAlchemyStore

import os.path
class SqlAlchemyInMemoryModelTestCase(modelTest.BasicModelTestCase):
    ''' 
    Defaults to SQLite in-memory database 
    '''
    def getModel(self):
        self.persistentStore = False
        model = SqlAlchemyStore(None)
        return self._getModel(model)

    def getTransactionModel(self):
        self.persistentStore = False        
        model = SqlAlchemyStore(None)
        return self._getModel(model)

class SqlAlchemyModelTestCase(modelTest.BasicModelTestCase):
    
    def getModel(self):
        model = SqlAlchemyStore(self.configurl)
        return self._getModel(model)

    def getTransactionModel(self):
        model = SqlAlchemyStore(self.configurl)
        return self._getModel(model)

    def setUp(self):
        '''
        Until I figure out a decent way to compartmentalize the particulars for more than
        one SQL database flavor, I hard-code this configuration right here for testing purposes
        using SQLA configuration parameter canning device, to wit (sqla 0.7.4): 
        class sqlalchemy.engine.url.URL(drivername, 
            username=None, 
            password=None, 
            host=None, 
            port=None, 
            database=None, 
            query=None)
        ''' 
        self.tmpdir = tempfile.mkdtemp(prefix="rhizometest")
        self.tmpfilename = os.path.join(self.tmpdir, 'test.sqlite') 
        self.tmpfilename = os.path.abspath(self.tmpfilename)

        import sqlalchemy
        self.configurl = sqlalchemy.engine.url.URL('sqlite3', self.tmpfilename)
       
    def tearDown(self):
        shutil.rmtree(self.tmpdir)

if __name__ == '__main__':
    modelTest.main(SqlAlchemyModelTestCase)
