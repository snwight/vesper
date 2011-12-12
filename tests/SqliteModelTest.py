#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    SQLite model unit tests
"""
import unittest
import subprocess, tempfile, os, signal, sys
import string, random, shutil, time

import modelTest 
from vesper.data.store.sqlite import SqliteStore

import os.path
class SqliteInMemoryModelTestCase(modelTest.BasicModelTestCase):
    # None ==> :memory:

    def getModel(self):
        self.persistentStore = False
        model = SqliteStore(None)
        return self._getModel(model)

    def getTransactionModel(self):
        self.persistentStore = False        
        model = SqliteStore(None)
        return self._getModel(model)

class SqliteModelTestCase(modelTest.BasicModelTestCase):
    
    def getModel(self):
        model = SqliteStore(self.tmpfilename)
        return self._getModel(model)

    def getTransactionModel(self):
        model = SqliteStore(self.tmpfilename)
        return self._getModel(model)

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="rhizometest")
        self.tmpfilename = os.path.join(self.tmpdir, 'test.sqlite') 
       
    def tearDown(self):
        shutil.rmtree(self.tmpdir)

if __name__ == '__main__':
    modelTest.main(SqliteModelTestCase)
