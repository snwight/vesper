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

class AlchemySqlModelTestCase(modelTest.BasicModelTestCase):
    
    def getModel(self):
        model = AlchemySqlStore(self.tmpfilename, autocommit=True)
        return self._getModel(model)

    def getTransactionModel(self):
        model = AlchemySqlStore(self.tmpfilename, autocommit=False)
        return self._getModel(model)

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="rhizometest")

        # sqlite via sqlite3/pysql - default
        fname = os.path.abspath(os.path.join(self.tmpdir, 'test.sqlite'))
        self.tmpfilename = "sqlite:///{0}".format(fname)
                
        # mysql via mysqldb - default
        # 'mysql+mysqldb://<user>:<password>@<host>[:<port>]/<dbname>'
#        self.tmpfilename = "mysql+mysqldb://vesper:ve$per@localhost:3306/vesper_db"
        
        # postgresql via pscyopg2 - default
        # 'postgresql+psycopg2://user:password@host:port/dbname[?key=value&key=value...]'
#        self.tmpfilename = "postgresql+psycopg2://vesper:ve$per@localhost:5432/vesper_db"
        
    def tearDown(self):
        shutil.rmtree(self.tmpdir)

if __name__ == '__main__':
    modelTest.main(AlchemySqlModelTestCase)
