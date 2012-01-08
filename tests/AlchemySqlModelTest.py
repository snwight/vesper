#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    AlchemySql model unit tests
"""
import unittest
import subprocess, tempfile, os, signal, sys
import string, random, shutil, time
import modelTest

import sqlalchemy
from sqlalchemy import engine, create_engine
from sqlalchemy.schema import MetaData 

from vesper.data.store.alchemysql import AlchemySqlStore


class AlchemySqlModelTestCase(modelTest.BasicModelTestCase):
    
    def getModel(self):
        model = AlchemySqlStore(engine=self.engine, autocommit=True)
        return self._getModel(model)

    def getTransactionModel(self):
        model = AlchemySqlStore(engine=self.engine, autocommit=False)
        return self._getModel(model)

    def setUp(self):
        # sqlite is our backend default
        # sqlite via sqlite3/pysql - (default python driver)
        self.tmpdir = tempfile.mkdtemp(prefix="rhizometest")
        fname = os.path.abspath(os.path.join(self.tmpdir, 'test.sqlite'))
        self.tmpfilename = "sqlite:///{0}".format(fname)
        self.engine = create_engine(self.tmpfilename)
    
    def tearDown(self):
        shutil.rmtree(self.tmpdir)


if os.getenv("SQLA_TEST_POSTGRESQL"):
    class SqlaPostgresqlModelTestCase(AlchemySqlModelTestCase):
            
        def setUp(self):
            # postgresql via pscyopg2 - (default python driver)
            # 'postgresql+psycopg2://user:password@host:port/dbname[?key=value&key=value...]'
            # "postgresql+psycopg2://vesper:vspr@localhost:5432/vesper_db"
            self.tmpfilename = os.getenv("SQLA_TEST_POSTGRESQL")

            # dialect+driver://username:password@host:port/database
            self.engine = create_engine(self.tmpfilename)

        def tearDown(self):
            self.engine.execute("drop table if exists vesper_stmts;")


if os.getenv("SQLA_TEST_MYSQL"):
    class SqlaMysqlModelTestCase(AlchemySqlModelTestCase):
        
        def setUp(self):
            # mysql via mysqldb - (default python driver)
            # 'mysql+mysqldb://<user>:<password>@<host>[:<port>]/<dbname>'
            # "mysql+mysqldb://vesper:ve$per@localhost:3306/vesper_db"
            self.tmpfilename = os.getenv("SQLA_TEST_MYSQL")

            # dialect+driver://username:password@host:port/database
            self.engine = create_engine(self.tmpfilename)
        
        def tearDown(self):
            self.engine.execute("drop table if exists vesper_stmts;")


if __name__ == '__main__':
    modelTest.main(AlchemySqlModelTestCase)
