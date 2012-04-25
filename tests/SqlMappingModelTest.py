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
        subj = RSRC_URI + 'artist/artistid#1'
        rows = model.getStatements(subject=subj)
        self.assertEqual(set(), set(rows))
        # add a new statement and confirm the search succeeds
        aStmts = [Statement(RSRC_URI + 'artist/artistid#1', 
                            'name', 'john', 'en', None),
                  Statement(RSRC_URI + 'artist/artistid#1', 
                            'birthdate', '06061960', 'en', None),
                  Statement(RSRC_URI + 'artist/artistid#1', 
                            'gender', 'TV', 'en', None),
        ]
        model.addStatements(aStmts)
        rows = model.getStatements(subject = subj)
        self.assertEqual(3, len(rows))
        model.commit()
        model.close()
        if not self.persistentStore:
            return
        # confirm persistent store
        model = self.getModel()
        rows = model.getStatements(subject=subj)
        self.assertEqual(3, len(rows))

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

        artStmts = [
            Statement(RSRC_URI + 'artist/artistid#1',
                      'name', 'ralph', 'en', None),
            Statement(RSRC_URI + 'artist/artistid#2',
                      'name', 'lauren', 'en-1', None),
            Statement(RSRC_URI + 'artist/artistid#3',
                      'name', 'diane', 'en-1', None),
            Statement(RSRC_URI + 'artist/artistid#1',
                      'birthdate', '05151961', 'en', None),
            Statement(RSRC_URI + 'artist/artistid#2',
                      'birthdate', '07081960', 'en-1', None),
            Statement(RSRC_URI + 'artist/artistid#3',
                      'birthdate', '04151980', 'en-1', None),
            Statement(RSRC_URI + 'artist/artistid#1',
                      'gender', 'M', 'en', None),
            Statement(RSRC_URI + 'artist/artistid#2',
                      'gender', 'F', 'en-1', None),
            Statement(RSRC_URI + 'artist/artistid#3',
                      'gender', 'TV', 'en-1', None)
        ]
        trkStmts = [
            Statement(RSRC_URI + 'track/trackid#1',
                      'title', 'track 1', 'en-1', None),
            Statement(RSRC_URI + 'track/trackid#2',
                      'title', 'track 2', 'en-1', None),
            Statement(RSRC_URI + 'track/trackid#3',
                      'title', 'track 3', 'en-1', None),
            Statement(RSRC_URI + 'track/trackid#1',
                      'date', '09162011', 'en-1', None),
            Statement(RSRC_URI + 'track/trackid#2',
                      'date', '12252011', 'en-1', None),
            Statement(RSRC_URI + 'track/trackid#3',
                      'date', '04012012', 'en-1', None),
            Statement(RSRC_URI + 'track/trackid#1',
                      RSRC_URI + 'track/tracklength', 120, 'en-1', None),
            Statement(RSRC_URI + 'track/trackid#2',
                      RSRC_URI + 'track/tracklength', 360, 'en-1', None),
            Statement(RSRC_URI + 'track/trackid#3',
                      RSRC_URI + 'track/tracklength', 37, 'en-1', None)
        ]
        albStmts = [
            Statement(RSRC_URI + 'album/albumid#1',
                      'title', 'Saucerful of Secrets', 'en-1', None),
            Statement(RSRC_URI + 'album/albumid#1',
                      'label', 'Polydor', 'en-1', None),
            Statement(RSRC_URI + 'album/albumid#1',
                      'albumdate', '060601968', 'en-1', None),
            Statement(RSRC_URI + 'album/albumid#2',
                      'title', 'Death Walks Behind You', 'en-1', None),
            Statement(RSRC_URI + 'album/albumid#2',
                      'label', 'London', 'en-1', None),
            Statement(RSRC_URI + 'album/albumid#2',
                      'albumdate', '070601967', 'en-1', None)
            ]
        alb_trk_Stmts = [
            Statement(RSRC_URI + 'album_tracks/albumid#1',
                      'trackid', 1, 'en-1', None),
            Statement(RSRC_URI + 'album_tracks/albumid#1',
                      'trackid', 2, 'en-1', None),
            Statement(RSRC_URI + 'album_tracks/albumid#1',
                      'trackid', 3, 'en-1', None),
            Statement(RSRC_URI + 'album_tracks/trackid#1',
                      'albumid', 2, 'en-1', None),
            Statement(RSRC_URI + 'album_tracks/trackid#1',
                      'albumid', 2, 'en-1', None),
            Statement(RSRC_URI + 'album_tracks/trackid#3',
                      'albumid', 2, 'en-1', None),
            ]
        trk_art_Stmts = [
            Statement(RSRC_URI + 'track_artist/artistid#1',
                      'trackid', 1, 'en-1', None),
            Statement(RSRC_URI + 'track_artist/artistid#1',
                      'trackid', 2, 'en-1', None),
            Statement(RSRC_URI + 'track_artist/artistid#1',
                      'trackid', 3, 'en-1', None),
            Statement(RSRC_URI + 'track_artist/trackid#1',
                      'artistid', 2, 'en-1', None),
            Statement(RSRC_URI + 'track_artist/trackid#2',
                      'artistid', 2, 'en-1', None),
            Statement(RSRC_URI + 'track_artist/trackid#3',
                      'artistid', 3, 'en-1', None),
            ]

        model = self.getModel()
        model.addStatements(artStmts + trkStmts + albStmts)
        model.addStatements(alb_trk_Stmts + trk_art_Stmts)

        # verify select all rows from a single table (x column count)
        rows = model.getStatements(subject=RSRC_URI, 
                                   predicate='rdf:type',
                                   object='artist')
        self.assertEqual(len(rows), len(artStmts))

        # verify select all elements from one row of one table
        rows = model.getStatements(subject=RSRC_URI + 'artist/artistid#1')
        self.assertEqual(artStmts[0][2], rows[1][2])

        # verify select all objects with a particular property from one table
        rows = model.getStatements(subject=RSRC_URI + 'artist',
                                   predicate='name')
        self.assertEqual(3, len(rows))

        # verify select a property's object given subject ID  
        rows = model.getStatements(subject=RSRC_URI + 'artist/artistid#1', 
                                   predicate='gender')
        self.assertEqual('M', rows[0][2])

        # verify select subject ID given a property and object value
        rows = model.getStatements(subject=RSRC_URI + 'artist',
                                   predicate='birthdate',
                                   object='07081960')
        self.assertEqual('artistid#2', rows[0][0])

        # REPEAT the above tests against another (bigger) table
        # verify select all rows from a single table
        rows = model.getStatements(subject=RSRC_URI,
                                   predicate='rdf:type',
                                   object='track')
        self.assertEqual(len(trkStmts), len(rows))

        # verify select all elements from one row of one table
        rows = model.getStatements(subject=RSRC_URI + 'track/trackid#1')
        self.assertEqual(3, len(rows))

        # verify select all objects with a particular property from one table
        rows = model.getStatements(subject=RSRC_URI + 'track',
                                   predicate='title')
        self.assertEqual(3, len(rows))

        # verify select a property's object given subject ID  
        rows = model.getStatements(subject=RSRC_URI + 'track/trackid#1', 
                                   predicate='title')
        self.assertEqual('track 1', rows[0][2])

        # verify select subject ID given a property and object value
        rows = model.getStatements(subject=RSRC_URI + 'track',
                                   predicate='tracklength', 
                                   object=360)
        self.assertEqual('trackid#2', rows[0][0])
        
        # verify retrieve rows from an SQL defined view 
        #        rows = model.getStatements(subject=RSRC_URI + 'artist_discography')
        rows = model.getStatements(subject=RSRC_URI, 
                                   predicate='rdf:type',
                                   object='artist_discography')
        self.assertEqual(6, len(rows))
        import pprint
        pprint.PrettyPrinter(indent=2).pprint(rows)
        
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
            Statement(RSRC_URI + 'track/trackid#1',
                      'title', 'track 1', 'en-1', None),
            Statement(RSRC_URI + 'track/trackid#2',
                      'title', 'track 2', 'en-1', None),
        ]
        ret = model.addStatements(tStmts)
        if checkr:
            self.assertEqual(ret,  len(tStmts), 'added count is wrong')

        # confirm search for subject finds all related properties and values 
        rows = model.getStatements(subject=tStmts[0].subject)
        self.assertEqual(3, len(rows))

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

        # fill then clear entire table
        ret = model.addStatements(tStmts)
        self.assertEqual(2, ret)
        ts = [Statement(tStmts[0].subject, None, None, None, None), 
              Statement(tStmts[1].subject, None, None, None, None)]
        ret = model.removeStatements(ts)
        self.assertEqual(2, ret)
        rows = model.getStatements(subject=RSRC_URI,
                                   predicate="rdf:type",
                                   object='track')
        self.assertEqual(set(), set(rows))

        # remove row by ID and verify it is gone
        ret = model.addStatements(tStmts)
        self.assertEqual(2, ret)
        ret = model.removeStatement(ts[0])
        self.assertEqual(1, ret)
        rows = model.getStatements(subject=RSRC_URI,
                                   predicate="rdf:type",
                                   object='track')
        self.assertEqual('trackid#2', rows[0][0])

        # remove all objects of one property type - clear this column
        ret = model.addStatements(tStmts)
        self.assertEqual(2, ret)
        ts = Statement(RSRC_URI + 'track', 
                       'title', None, None, None)
        ret = model.removeStatement(ts)
        self.assertEqual(2, ret)
        rows = model.getStatements(subject=RSRC_URI + 'track',
                                   predicate='title')
        self.assertEqual(['', ''], [r[2] for r in rows])

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
