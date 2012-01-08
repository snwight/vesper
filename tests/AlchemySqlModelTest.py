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
        model = AlchemySqlStore(self.tmpfilename, pStore=self.persistentStore, autocommit=True)
        return self._getModel(model)

    def getTransactionModel(self):
        model = AlchemySqlStore(self.tmpfilename, pStore=self.persistentStore, autocommit=False)
        return self._getModel(model)

    def setUp(self):
        # sqlite is our backend default
        # sqlite via sqlite3/pysql - (default python driver)
        self.tmpdir = tempfile.mkdtemp(prefix="rhizometest")
        fname = os.path.abspath(os.path.join(self.tmpdir, 'test.sqlite'))
        self.tmpfilename = "sqlite:///{0}".format(fname)
       
    def tearDown(self):
        shutil.rmtree(self.tmpdir)


class SqlaPostgresqlModelTestCase(AlchemySqlModelTestCase):

    def setUp(self):
        # postgresql via pscyopg2 - (default python driver)
        # 'postgresql+psycopg2://user:password@host:port/dbname[?key=value&key=value...]'
        self.tmpfilename = "postgresql+psycopg2://vesper:vspr@localhost:5432/vesper_db"

    def tearDown(self):
        pass

class SqlaMysqlModelTestCase(AlchemySqlModelTestCase):

    def setUp(self):
        # mysql via mysqldb - (default python driver)
        # 'mysql+mysqldb://<user>:<password>@<host>[:<port>]/<dbname>'
        self.tmpfilename = "mysql+mysqldb://vesper:ve$per@localhost:3306/vesper_db"

    def tearDown(self):
        pass


if __name__ == '__main__':
    modelTest.main(AlchemySqlModelTestCase)
