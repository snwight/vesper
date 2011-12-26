#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    AlchemySql model unit tests
"""
import unittest
import subprocess, tempfile, os, signal, sys
import string, random, shutil, time

import modelTest
from sqlalchemy import engine
from vesper.data.store.alchemysql import AlchemySqlStore

import os.path
class AlchemySqlInMemoryModelTestCase(modelTest.BasicModelTestCase):
    ''' 
    Defaults to SQLite in-memory database 
    '''
    def getModel(self):
        self.persistentStore = False
        model = AlchemySqlStore(None)
        return self._getModel(model)

    def getTransactionModel(self):
        self.persistentStore = False        
        model = AlchemySqlStore(None)
        return self._getModel(model)

class AlchemySqlModelTestCase(modelTest.BasicModelTestCase):
    
    def getModel(self):
        model = AlchemySqlStore(self.tmpfilename)
        return self._getModel(model)

    def getTransactionModel(self):
        model = AlchemySqlStore(self.tmpfilename)
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
        fname = os.path.abspath(os.path.join(self.tmpdir, 'test.sqlite'))
        self.tmpfilename = "sqlite:///{0}".format(fname)
       
    def tearDown(self):
        shutil.rmtree(self.tmpdir)

if __name__ == '__main__':
    modelTest.main(AlchemySqlModelTestCase)
