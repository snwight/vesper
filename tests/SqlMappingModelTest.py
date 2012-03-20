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

import modelTest
import vesper.app
from vesper import utils
from vesper.data import base
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
        cmd = "sqlite {0} < {1}".format(fname, self.sqlSchemaPath)
        call(cmd, shell=True)
                

    def tearDown(self):
        shutil.rmtree(self.tmpdir)


    def getModel(self):
        model = JsonAlchemyStore(source=self.sqlaConfiguration, 
                                 mapping=self.mapping, autocommit=True)
        return self._getModel(model)


    def getTransactionModel(self):
        model = JsonAlchemyStore(source=self.sqlaConfiguration, 
                                 mapping=self.mapping, autocommit=False)
        return self._getModel(model)


    def _getModel(self, model):
        return model


#    def getModel(self):
#        model = MemStore()
#        self.persistentStore = False
#        return self._getModel(model)


    def testStore(self):
        "basic storage test"
        global RSRC_URI
        model = self.getModel()
        # confirm a newly created subject does not exist
        subj = RSRC_URI + 'track/trackid{1}'
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set())
        # add a new statement and confirm the search succeeds
        aStmts = [Statement(RSRC_URI + 'artist/artistid{1}', 
                            RSRC_URI + 'artist/artistname', 'ralph', 
                            'en', None)
        ]
        tStmts = [Statement(subj, 'track/trackname', 'track 1')]
        model.addStatements(aStmts)
        model.addStatements(tStmts)
        r1 = model.getStatements(subject = subj)
        self.assertEqual([s[2] for s in tStmts], [r[2] for r in r1])
        model.commit()
        model.close()
        if not self.persistentStore:
            return 
        model = self.getModel()
        r1 = model.getStatements(subject=subj)
        self.assertEqual([s[2] for s in tStmts], [r[2] for r in r1])

        # NOT IMPLEMENTED YET
        # - removal object value/s (null row element/s)
        # - removal of subject key/ID ignored 
        # 
        # - duplicate values. duplicate subject key/IDs
        '''
        model.removeStatement()
        s3 = Statement(subj, 'track/trackartist', 1)
        model.addStatement(s3)
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set([s1, s3]))
        model.commit()
        model.close()
        model = self.getModel()
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set([s1, s3]))
        '''
        model.close()
    

    def testGetStatements(self, asQuad=True):
        # <HACK>...<COUGH>...
        global RSRC_URI

        aStmts = [
        Statement(RSRC_URI + 'artist/artistid{1}', 
                  RSRC_URI + 'artist/artistname', 'ralph', 'en', None),
        Statement(RSRC_URI + 'artist/artistid{2}', 
                  RSRC_URI + 'artist/artistname', 'lauren', 'en-1', None),
        Statement(RSRC_URI + 'artist/artistid{3}', 
                  RSRC_URI + 'artist/artistname', 'diane', 'en-1', None)
        ]
        
        tStmts = [
        Statement(RSRC_URI + 'track/trackid{1}', 
                  RSRC_URI + 'track/trackname', 'track 1', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{2}', 
                  RSRC_URI + 'track/trackname', 'track 2', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{3}', 
                  RSRC_URI + 'track/trackname', 'track 3', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{4}', 
                  RSRC_URI + 'track/trackname', 'track A ', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{5}', 
                  RSRC_URI + 'track/trackname', 'track B', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{6}', 
                  RSRC_URI + 'track/trackname', 'track C', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{7}',
                  RSRC_URI + 'track/trackname', 'song 1', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{8}',
                  RSRC_URI + 'track/trackname', 'song 2', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{9}', 
                  RSRC_URI + 'track/trackname', 'song 3', 'en-1', None),
        ]
        model = self.getModel()
        # load our two 'arbitrary' tables
        model.addStatements(aStmts)
        model.addStatements(tStmts)
        # verify select all rows from a single table
        rows = model.getStatements(subject=RSRC_URI + 'artist/artistid')
        self.assertEqual([a[2] for a in aStmts], [r[2] for r in rows])
        # verify select all elements from one row of one table
        rows = model.getStatements(subject=RSRC_URI + 'artist/artistid{1}')
        self.assertEqual(aStmts[0][2], rows[0][2])
        # verify select all objects with a particular property from one table
        rows = model.getStatements(predicate=RSRC_URI + 'artist/artistname')
        self.assertEqual([a[2] for a in aStmts], [r[2] for r in rows])
        # verify select a property's object given subject ID  
        rows = model.getStatements(subject=RSRC_URI + 'artist/artistid{1}', 
                                   predicate=RSRC_URI + 'artist/artistname')
        self.assertEqual('ralph', rows[0][2])
        # verify select subject ID given a property and object value
        rows = model.getStatements(predicate=RSRC_URI + 'artist/artistname', 
                                   object='lauren')
        self.assertEqual('artistid{2}', rows[0][0])

        # REPEAT the above tests against another (bigger) table
        # verify select all rows from a single table
        rows = model.getStatements(subject=RSRC_URI + 'track/trackid')
        self.assertEqual([t[2] for t in tStmts], [r[2] for r in rows])
        # verify select all elements from one row of one table
        rows = model.getStatements(subject=RSRC_URI + 'track/trackid{1}')
        self.assertEqual(tStmts[0][2], rows[0][2])
        # verify select all objects with a particular property from one table
        rows = model.getStatements(predicate=RSRC_URI + 'track/trackname')
        self.assertEqual([t[2] for t in tStmts], [r[2] for r in rows])
        # verify select a property's object given subject ID  
        rows = model.getStatements(subject=RSRC_URI + 'track/trackid{1}', 
                                   predicate=RSRC_URI + 'track/trackname')
        self.assertEqual('track 1', rows[0][2])
        # verify select subject ID given a property and object value
        rows = model.getStatements(predicate=RSRC_URI + 'track/trackname', 
                                   object='track 1')
        self.assertEqual('trackid{1}', rows[0][0])
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

        aStmts = [
            Statement(RSRC_URI + 'artist/artistid{1}', 
                      RSRC_URI + 'artist/artistname', 'ralph', 'en', None)
        ]
        tStmts = [
            Statement(RSRC_URI + 'track/trackid{1}', 
                      RSRC_URI + 'track/trackname', 'track 1', 'en-1', None),
            Statement(RSRC_URI + 'track/trackid{2}', 
                      RSRC_URI + 'track/trackname', 'track 2', 'en-1', None),
        ]
        ret = model.addStatements(aStmts)
        ret = model.addStatements(tStmts)
        if checkr:
            self.assertEqual(ret, len(tStmts), 'added count is wrong')
        # confirm search for subject finds all related properties and values 
        rows = model.getStatements(subject=tStmts[0].subject)
        self.assertEqual(tStmts[0][2], rows[0][2])
        # remove the subject and confirm that all objects are gone
        ret = model.removeStatement(Statement(tStmts[0].subject, 
                                              None, None, None, None))
        if checkr:
            assert ret, "statement should have been removed"
        rows = model.getStatements(subject=tStmts[0].subject)
        self.assertEqual([], rows)
        # remove the statement again
        ret = model.removeStatement(Statement(tStmts[0].subject, 
                                              None, None, None, None))
        if checkr:
            assert not ret, "statement shouldn't have been removed"
        # add statement twice without duplicate
        model.addStatement(tStmts[0])
        r1 = model.getStatements(subject=tStmts[0].subject)
        model.addStatement(tStmts[0])
        r2 = model.getStatements(subject=tStmts[0].subject)
        self.assertEqual(r1, r2)
        # clear entire  table
        ts = [Statement(tStmts[0].subject, None, None, None, None), 
              Statement(tStmts[1].subject, None, None, None, None)]
        ret = model.removeStatements(ts)
        rows = model.getStatements()
        self.assertEqual([], rows)
        # reload table
        ret = model.addStatements(tStmts)
        if checkr:
            self.assertEqual(ret, len(tStmts), 'added count is wrong')
        # remove object from specific rsrc
        ret = model.removeStatement(tStmts[0])
        rows = model.getStatements(subject=tStmts[0].subject, 
                                   predicate=tStmts[0].predicate)
        self.assertEqual('', rows[0][2])
        # remove all objects of one property type from all rsrcs
        ts = Statement(RSRC_URI + 'track/trackid', tStmts[0].predicate, 
                       None, None, None)
        ret = model.removeStatement(ts)
        rows = model.getStatements(predicate=tStmts[0].predicate)
        model.commit()
        model.close()


if os.getenv("SQLA_TEST_POSTGRESQL"):
    class PgsqlMappingModelTestCase(SqlMappingModelTestCase):
           
        def setUp(self):
            # test against postgresql backend 
            self.sqlaConfiguration = '/'.join(
                [os.getenv("SQLA_TEST_POSTGRESQL"), "jsonmap_db"])
            # create our postgresql test db 
            call("psql -q -c \"create database jsonmap_db\" postgres", 
                 shell=True)
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
            call("psql -q -c \"{0}\" postgres >> /dev/null".format(cmd), 
                 shell=True)
            call("psql -q -c \"drop database if exists jsonmap_db\" postgres",
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
