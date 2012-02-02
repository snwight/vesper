#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    SqlMapping model unit tests
"""
import unittest
import json
import subprocess, tempfile, os, signal, sys
from subprocess import check_call, call
import string, random, shutil, time
import logging

# import mappingTest
import modelTest
import vesper.app
from vesper import utils
from vesper.data.store.sqlmapping import SqlMappingStore

class SqlMappingModelTestCase(modelTest.SqlMappingModelTestCase):
    
    # initialize our json-to-sql mapping engine w/ coordinated SQL and JSON mapthingies
    sqlSchemaPath = os.path.join(os.getcwd(), 'map_file_1.sql')
    jsonMapPath = os.path.join(os.getcwd(), 'map_file_1.json')
    mapping = json.loads(open(jsonMapPath).read())

    def getModel(self):
        model = SqlMappingStore(source=self.tmpfilename, mapping=self.mapping, autocommit=True)
        return self._getModel(model)


    def getTransactionModel(self):
        model = SqlMappingStore(source=self.tmpfilename, mapping=self.mapping, autocommit=False)
        return self._getModel(model)


    def setUp(self):
        # sqlite via sqlite3/pysql - (default python driver)
        self.tmpdir = tempfile.mkdtemp(prefix="rhizometest")
        fname = os.path.abspath(os.path.join(self.tmpdir, 'jsonmap_db'))
        self.tmpfilename = "sqlite:///{0}".format(fname)
       
        # create our sqlite test db and schema 
        cmd = "sqlite {0} < {1}".format(fname, self.sqlSchemaPath)
        call(cmd, shell=True)


    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        pass


if os.getenv("SQLA_TEST_POSTGRESQL"):
    class PgsqlMappingModelTestCase(SqlMappingModelTestCase):
           
        def setUp(self):
            # test against postgresql backend 
            self.tmpfilename = '/'.join([os.getenv("SQLA_TEST_POSTGRESQL"), "jsonmap_db"])

            # create our postgresql test db 
            call("psql -q -c \"create database jsonmap_db\" postgres", shell=True)

            # then load test schema FROM FILE
            cmd = "psql -q -U vesper -d jsonmap_db < {0}".format(self.sqlSchemaPath)
            call(cmd, shell=True)


        def tearDown(self):
            # destroy all zombies
            cmd = "select pg_terminate_backend(procpid) from pg_stat_activity where datname = \'jsonmap_db\'"
            call("psql -q -c \"{0}\" postgres >> /dev/null".format(cmd), shell=True)
            call("psql -q -c \"drop database if exists jsonmap_db\" postgres", shell=True)



if os.getenv("SQLA_TEST_MYSQL"):
    class MysqlMappingModelTestCase(SqlMappingModelTestCase):

        def setUp(self):
            # test against mysql backend 
            self.tmpfilename = '/'.join([os.getenv("SQLA_TEST_MYSQL"), "jsonmap_db"])
            
            # create our mysql test db
            call("mysqladmin -pve\$per -u vesper create jsonmap_db", shell=True)

            # then load test schema FROM FILE
            cmd = "mysql -p've$per' -u vesper jsonmap_db < {0}".format(self.sqlSchemaPath)
            call(cmd, shell=True)

            
        def tearDown(self):
            call("mysqladmin -f -pve\$per -u vesper drop jsonmap_db >> /dev/null", shell=True)



if __name__ == '__main__':
    modelTest.main(SqlMappingModelTestCase)
