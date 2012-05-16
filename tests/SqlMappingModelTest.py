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
import pprint
import logging

import modelTest
import vesper.app
from vesper import utils
from vesper.data import base
import vesper.data.base.utils
from vesper.data.store.basic import *
from vesper.data.store.jsonalchemy import JsonAlchemyStore

# <<HACK!COUGH!>>
RSRC_URI = "http://souzis.com/"

class SqlMappingModelTestCase(modelTest.BasicModelTestCase):
    
    # initialize our json-to-sql mapping engine SQL and JSON scripts
    sqlSchemaPath = os.path.join(os.getcwd(), 'map_file_1.sql')
    jsonMapPath = os.path.join(os.getcwd(), 'map_file_1.json')
    mapping = json.loads(open(jsonMapPath).read())
    
    # XXX TESTING
    # mapping = None
    # XXX

    sqlaConfiguration = None
    persistentStore = True

    def setUp(self):
        # sqlite via sqlite3/pysql - (default python driver)
        self.tmpdir = tempfile.mkdtemp(prefix="rhizometest")
        fname = os.path.abspath(os.path.join(self.tmpdir, 'jsonmap_db'))
        self.sqlaConfiguration = '/'.join(
            [os.getenv("SQLA_TEST_SQLITE"), fname])

        # create our sqlite test db and schema 
        cmd = "sqlite3 {0} < {1}".format(fname, self.sqlSchemaPath)
        call(cmd, shell=True)
                

    def tearDown(self):
        shutil.rmtree(self.tmpdir)


    def getModel(self):
        return  JsonAlchemyStore(source=self.sqlaConfiguration, 
                                 mapping=self.mapping, autocommit=True)


    def getTransactionModel(self):
        return JsonAlchemyStore(source=self.sqlaConfiguration, 
                                 mapping=self.mapping, autocommit=False)


    def _getModel(self, model):
        return model


    def testStore(self):
        "basic storage test"
        global RSRC_URI
        model = self.getModel()
        # confirm a newly created subject does not exist
        subj = RSRC_URI + 'artist/artistid#8'
        rows = model.getStatements(subject=subj)
        self.assertEqual(set(), set(rows))
        # add a new statement and confirm the search succeeds
        aStmts = [Statement(RSRC_URI + 'artist/artistid#8', 
                            'name', 'john', 'en', None),
                  Statement(RSRC_URI + 'artist/artistid#8', 
                            'birthdate', '06061960', 'en', None),
                  Statement(RSRC_URI + 'artist/artistid#8', 
                            'gender', 'TV', 'en', None),
        ]
        model.addStatements(aStmts)
        rows = model.getStatements(subject = subj)
        self.assertEqual(4, len(rows))
        model.commit()
        model.close()
        if not self.persistentStore:
            return
        # confirm persistent store
        model = self.getModel()
        rows = model.getStatements(subject=subj)
        self.assertEqual(4, len(rows))
        model.close()
    

    def testGetStatements(self, asQuad=True):
        '''
        query fully pre-loaded DB
        '''
        # <HACK>...<COUGH>...
        global RSRC_URI

        model = self.getModel()
        # verify select all rows from a single table (x column count)
        rows = model.getStatements(subject=RSRC_URI, 
                                   predicate='rdf:type',
                                   object='artist')
        self.assertEqual(28, len(rows))
        # verify select all elements from one row of one table
        rows = model.getStatements(subject=RSRC_URI + 'artist/artistid#1')
        self.assertEqual(4, len(rows))
        # verify select all objects with a particular property from one table
        rows = model.getStatements(subject=RSRC_URI + 'artist',
                                   predicate='name')
        self.assertEqual(7, len(rows))
        # verify select a property's object given subject ID  
        rows = model.getStatements(subject=RSRC_URI + 'artist/artistid#1', 
                                   predicate='gender')
        self.assertEqual('M', rows[0][2])
        # verify select subject ID given a property and object value
        rows = model.getStatements(subject=RSRC_URI + 'artist',
                                   predicate='birthdate',
                                   object='5151961')
        self.assertEqual('artist/artistid#1', rows[0][0])
        # REPEAT the above tests against another table
        # verify select all rows from a single table
        rows = model.getStatements(subject=RSRC_URI,
                                   predicate='rdf:type',
                                   object='track')
        self.assertEqual(52, len(rows))
        # verify select all elements from one row of one table
        rows = model.getStatements(subject=RSRC_URI + 'track/trackid#1')
        self.assertEqual(4, len(rows))
        # verify select all objects with a particular property from one table
        rows = model.getStatements(subject=RSRC_URI + 'track',
                                   predicate='title')
        self.assertEqual(13, len(rows))
        # verify select a property's object given subject ID  
        rows = model.getStatements(subject=RSRC_URI + 'track/trackid#1', 
                                   predicate='title')
        self.assertEqual('love song one', rows[0][2])
        # verify select subject ID given a property and object value
        rows = model.getStatements(subject=RSRC_URI + 'track',
                                   predicate='tracklength',
                                   object=360)
        self.assertEqual('track/trackid#1', rows[0][0])
        # verify retrieve rows from an SQL defined view (no primary key)
        rows = model.getStatements(subject=RSRC_URI, 
                                   predicate='rdf:type',
                                   object='artist_discography')
        self.assertEqual(123, len(rows))

        # TEST REFERRING PROPERTIES
        model.close()


    def testRemove(self):
        """
        basic removal test

        current seq    expected
        ------- ---    --------
        exists  r,a    no-op 
        doesnt  r,a    adds  
        exists  a,r    remove
        doesnt  a,r    no-op
        """
        model = self.getModel()
#        checkr = model.updateAdvisory
        checkr = True

        tStmts = [
            Statement(RSRC_URI + 'track/trackid#12',
                      'title', 'nothing happened part 1', 'en-1', None),
            Statement(RSRC_URI + 'track/trackid#13',
                      'title', 'nothing happened part 2', 'en-1', None),
        ]
        ret = model.addStatements(tStmts)
        if checkr:
            self.assertEqual(ret,  len(tStmts), 'added count is wrong')
        # confirm search for subject finds all related properties and values 
        rows = model.getStatements(subject=tStmts[0].subject)
        self.assertEqual(4, len(rows))
        # remove one subject and confirm that it's gone
        ret = model.removeStatement(
            Statement(tStmts[0].subject, None, None, None, None))
        self.assertEqual(1, ret)
        rows = model.getStatements(subject=tStmts[0].subject)
        self.assertEqual(set(), set(rows))
        # try to remove the deleted statement again
        ret = model.removeStatement(
            Statement(tStmts[0].subject, None, None, None, None))
        self.assertEqual(0, ret)
        # add statement twice without duplicate
        model.addStatement(tStmts[0])
        r1 = model.getStatements(subject=tStmts[0].subject)
        model.addStatement(tStmts[0])
        r2 = model.getStatements(subject=tStmts[0].subject)
        self.assertEqual(r1, r2)
        # remove row by ID and verify it is gone
        ret = model.addStatements(tStmts)
        self.assertEqual(2, ret)
        ret = model.removeStatement(tStmts[0])
        self.assertEqual(1, ret)
        rows = model.getStatements(subject=RSRC_URI,
                                   predicate="rdf:type",
                                   object='track')
        self.assertEqual(52, len(rows))
        # remove all objects of one property type - clear this column
        ret = model.addStatements(tStmts)
        self.assertEqual(2, ret)
        ts = Statement(RSRC_URI + 'track', 
                       'title', None, None, None)
        ret = model.removeStatement(ts)
        self.assertEqual(13, ret)
        rows = model.getStatements(subject=RSRC_URI + 'track',
                                   predicate='title', object='')
        self.assertEqual(13, len(rows))
        model.commit()
        model.close()


if os.getenv("SQLA_TEST_POSTGRESQL"):
    class PgsqlMappingModelTestCase(SqlMappingModelTestCase):

        def setUp(self):
            # test against postgresql backend 
            self.sqlaConfiguration = '/'.join(
                [os.getenv("SQLA_TEST_POSTGRESQL"), "jsonmap_db"])
            # create our postgresql test db
            call("createdb -U vesper jsonmap_db", shell=True)
            # then load test schema FROM FILE
            cmd = "psql -q -U vesper -d jsonmap_db < {0}".format(
                self.sqlSchemaPath)
            call(cmd, shell=True)


        def tearDown(self):
            # destroy all zombies
            cmd = '''
                select 
                pg_terminate_backend(procpid) 
                    from pg_stat_activity 
                    where datname = \'jsonmap_db\'
            '''
            call("psql -q -U vesper -c \"{0}\" postgres >> /dev/null".format(cmd), 
                 shell=True)
            call("psql -q -U vesper -c \"drop database if exists jsonmap_db\" postgres",
                 shell=True)


if os.getenv("SQLA_TEST_MYSQL"):
    class MysqlMappingModelTestCase(SqlMappingModelTestCase):

        def setUp(self):
            # test against mysql backend 
            self.sqlaConfiguration = '/'.join(
                [os.getenv("SQLA_TEST_MYSQL"), "jsonmap_db"])
            # create our mysql test db
            call("mysqladmin -pve\$per -u vesper create jsonmap_db", 
                 shell=True)
            # then load test schema FROM FILE
            cmd = "mysql -p've$per' -u vesper jsonmap_db < {0}".format(
                self.sqlSchemaPath)
            call(cmd, shell=True)

            
        def tearDown(self):
            call('''
                mysqladmin -f -pve\$per -u vesper drop jsonmap_db >> /dev/null
            ''', shell=True)


if __name__ == '__main__':
    modelTest.main(SqlMappingModelTestCase)
