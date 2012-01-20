#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    AlchemySql model unit tests
"""
import unittest
import subprocess, tempfile, os, signal, sys
from subprocess import check_call, call
import string, random, shutil, time
import modelTest

import sqlalchemy

from vesper.data.store.alchemysql import AlchemySqlStore

class AlchemySqlModelTestCase(modelTest.BasicModelTestCase):
    
    def getModel(self):
        model = AlchemySqlStore(source=self.tmpfilename, autocommit=True)
        return self._getModel(model)

    def getTransactionModel(self):
        model = AlchemySqlStore(source=self.tmpfilename, autocommit=False)
        return self._getModel(model)

    def setUp(self):
        # sqlite is our backend default
        # sqlite via sqlite3/pysql - (default python driver)
        self.tmpdir = tempfile.mkdtemp(prefix="rhizometest")
        fname = os.path.abspath(os.path.join(self.tmpdir, 'vesper_db'))
        self.tmpfilename = "sqlite:///{0}".format(fname)
        
    def tearDown(self):
        shutil.rmtree(self.tmpdir)



if os.getenv("SQLA_TEST_POSTGRESQL"):
    class SqlaPostgresqlModelTestCase(AlchemySqlModelTestCase):
           
        def setUp(self):
            # postgresql via pscyopg2 - (default python driver)
            # 'postgresql+psycopg2://user:password@host:port/dbname[?key=value&key=value...]'
            self.tmpfilename = '/'.join([os.getenv("SQLA_TEST_POSTGRESQL"), "vesper_db"])
            call("psql -q -c \"create database vesper_db\" postgres", shell=True)


        def tearDown(self):
            # destroy all zombies
            cmd = "select pg_terminate_backend(procpid) from pg_stat_activity where datname = \'vesper_db\'"
            call("psql -q -c \"{0}\" postgres >> /dev/null".format(cmd), shell=True)
            call("psql -q -c \"drop database if exists vesper_db\" postgres", shell=True)



if os.getenv("SQLA_TEST_MYSQL"):
    class SqlaMysqlModelTestCase(AlchemySqlModelTestCase):
        
        def setUp(self):
            # mysql via mysqldb - (default python driver)
            # 'mysql+mysqldb://<user>:<password>@<host>[:<port>]/<dbname>'
            self.tmpfilename = '/'.join([os.getenv("SQLA_TEST_MYSQL"), "vesper_db"])
            call("mysqladmin -f -pve\$per -u vesper create vesper_db", shell=True)

        def tearDown(self):
            call("mysqladmin -f -pve\$per -u vesper drop vesper_db >> /dev/null", shell=True)


if __name__ == '__main__':
    modelTest.main(AlchemySqlModelTestCase)
