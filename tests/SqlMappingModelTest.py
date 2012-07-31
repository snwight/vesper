#:copyright: Copyright 2009-2012 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licenses, see LICENSE.
"""
    SqlMapping model unit tests
"""
import unittest
import json
import subprocess, tempfile, os, signal, sys
from subprocess import check_call, call
import string, random, shutil, time, datetime
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
    mFile = open(jsonMapPath)
    mapping = json.loads(mFile.read())
    mFile.close()
    # XXX TESTING
    # mapping = None
    # XXX
    sqlaConfiguration = None
    persistentStore = True

    def setUp(self):
        # sqlite via pysql - (default python driver)
        self.tmpdir = tempfile.mkdtemp(prefix="rhizometest")
        fname = os.path.abspath(os.path.join(self.tmpdir, 'jsonmap_db'))
        self.sqlaConfiguration = '/'.join(["sqlite:///", fname])

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
        subj = RSRC_URI+'artist/artistid#8'
        rows = model.getStatements(subject=subj)
        self.assertEqual(set(), set(rows))
        # add a new statement and confirm the search succeeds
        bd = datetime.date(1960, 6, 6)
        aStmts = [
            Statement(RSRC_URI+'artist/artistid#8', 'name', 'john', 'en', None),
            Statement(RSRC_URI+'artist/artistid#8', 'birthdate', bd, 'en', None),
            Statement(RSRC_URI+'artist/artistid#8', 'gender', 'TX', 'en', None)]
        ret = model.addStatements(aStmts)
        self.assertEqual(ret, 3)
        rows = model.getStatements(subject = subj)
        expected = [
            ('artist/artistid#8', 'artistid', 8, None, None),
            ('artist/artistid#8', 'birthdate', bd, None, None),
            ('artist/artistid#8', 'gender', 'TX', None, None),
            ('artist/artistid#8', 'name', 'john', None, None)]
        self.assertEqual(expected, sorted(rows))
        model.commit()
        model.close()
        if not self.persistentStore:
            return
        # confirm persistent store
        model = self.getModel()
        rows = model.getStatements(subject=subj)
        expected = [
            ('artist/artistid#8', 'artistid', 8, None, None),
            ('artist/artistid#8', 'birthdate', bd, None, None),
            ('artist/artistid#8', 'gender', 'TX', None, None),
            ('artist/artistid#8', 'name', 'john', None, None)]
        self.assertEqual(expected, sorted(rows))
        model.close()


    def testGetStatements(self, asQuad=True):
        '''
        query fully pre-loaded DB
        '''
        # <HACK>...<COUGH>...
        global RSRC_URI

        model = self.getModel()
        # verify select all rows from a single table (x column count)
        rows = model.getStatements(subject=RSRC_URI, predicate='rdf:type',
                                   object='artist')
        self.assertEqual(28, len(rows))
        artist_table = rows
        # verify select all elements from one row of one table
        rows = model.getStatements(subject=RSRC_URI+'artist/artistid#1')
        bd = datetime.date(1961, 5, 15)
        expected = [
            ('artist/artistid#1', 'artistid', 1, None, None),
            ('artist/artistid#1', 'birthdate', bd, None, None),
            ('artist/artistid#1', 'gender', 'M', None, None),
            ('artist/artistid#1', 'name', 'bobby', None, None)]
        self.assertEqual(expected, sorted(rows))
        # verify select all objects with a particular property from one table
        rows = model.getStatements(subject=RSRC_URI+'artist', predicate='name')
        expected = [
            ('artist/artistid#1', 'artistname', 'bobby', None, None),
            ('artist/artistid#2', 'artistname', 'diane', None, None),
            ('artist/artistid#3', 'artistname', 'lashana', None, None),
            ('artist/artistid#4', 'artistname', 'lucy', None, None),
            ('artist/artistid#5', 'artistname', 'sid', None, None),
            ('artist/artistid#6', 'artistname', 'brian', None, None),
            ('artist/artistid#7', 'artistname', 'nancy', None, None)]
        self.assertEqual(expected, sorted(rows))
        # verify select a property's object given subject ID
        rows = model.getStatements(subject=RSRC_URI+'artist/artistid#1',
                                   predicate='gender')
        expected = [('artist/artistid#1', 'artistgender', 'M', None, None)]
        # verify select subject ID given a property and object value
        rows = model.getStatements(subject=RSRC_URI+'artist',
                                   predicate='birthdate',
                                   object=datetime.date(1961, 5, 15))
        expected = [('artist/artistid#1', None, None, None, None)]
        self.assertEqual(expected, rows)
        # REPEAT the above tests against another table
        # verify select all rows from a single table
        rows = model.getStatements(subject=RSRC_URI, predicate='rdf:type',
                                   object='track')
        self.assertEqual(52, len(rows))
        track_table = rows
        # verify select all elements from one row of one table
        rows = model.getStatements(subject=RSRC_URI+'track/trackid#1')
        expected = [
            ('track/trackid#1', 'date', datetime.date(2008, 8, 8), None, None),
            ('track/trackid#1', 'title', 'love song one', None, None),
            ('track/trackid#1', 'trackid', 1, None, None),
            ('track/trackid#1', 'tracklength', 360, None, None)]
        self.assertEqual(expected, sorted(rows))
        # verify select all objects with a particular property from one table
        rows = model.getStatements(subject=RSRC_URI+'track', predicate='title')
        expected = [
            ('track/trackid#1', 'trackname', 'love song one', None, None),
            ('track/trackid#2', 'trackname', 'love song two', None, None),
            ('track/trackid#3', 'trackname', 'love song three', None, None),
            ('track/trackid#4', 'trackname', 'love song four', None, None),
            ('track/trackid#5', 'trackname', 'love song five', None, None),
            ('track/trackid#6', 'trackname', 'hate song one', None, None),
            ('track/trackid#7', 'trackname', 'hate song two', None, None),
            ('track/trackid#8', 'trackname', 'hate song three', None, None),
            ('track/trackid#9', 'trackname', 'hate song four', None, None),
            ('track/trackid#10', 'trackname', 'something happened part 1',
             None, None),
            ('track/trackid#11', 'trackname', 'something happened part 2',
             None, None),
            ('track/trackid#12', 'trackname', 'nothing happened part 1',
             None, None),
            ('track/trackid#13', 'trackname', 'nothing happened part 2', 
             None, None)]
        self.assertEqual(expected, rows)
        # verify select a property's object given subject ID  
        rows = model.getStatements(subject=RSRC_URI+'track/trackid#1',
                                   predicate='title')
        expected = [
            ('track/trackid#1', 'trackname', 'love song one', None, None)]
        self.assertEqual(expected, rows)
        # verify select subject ID given a property and object value
        rows = model.getStatements(subject=RSRC_URI+'track', 
                                   predicate='tracklength',
                                   object=360)
        expected = [('track/trackid#1', None, None, None, None)]
        self.assertEqual(expected, rows)
        # verify retrieve all rows from an SQL defined view
        rows = model.getStatements(subject=RSRC_URI, predicate='rdf:type',
                                   object='artist_discography')
        self.assertEqual(246, len(rows))
        # TEST REFERRING PROPERTIES
        rows = model.getStatements(subject=RSRC_URI+'track/trackid#1',
                                   predicate='artists')
        expected = [
            ('track/trackid#1', 'artists', 1, None, None),
            ('track/trackid#1', 'artists', 2, None, None)]
        self.assertEqual(expected, sorted(rows))
        rows = model.getStatements(subject=RSRC_URI+'track/trackid#1',
                                   predicate='albums')
        expected = [
            ('track/trackid#1', 'albums', 2, None, None)]
        self.assertEqual(expected, sorted(rows))
        rows = model.getStatements(subject=RSRC_URI+'album/albumid#1',
                                   predicate='tracks')
        expected = [
            ('album/albumid#1', 'tracks', 6, None, None),
            ('album/albumid#1', 'tracks', 7, None, None),
            ('album/albumid#1', 'tracks', 8, None, None),
            ('album/albumid#1', 'tracks', 9, None, None)]
        self.assertEqual(expected, rows)
        rows = model.getStatements(subject=RSRC_URI+'artist/artistid#1',
                                   predicate='tracks')
        expected = [
            ('artist/artistid#1', 'tracks', 1, None, None),
            ('artist/artistid#1', 'tracks', 2, None, None),
            ('artist/artistid#1', 'tracks', 3, None, None),
            ('artist/artistid#1', 'tracks', 4, None, None),
            ('artist/artistid#1', 'tracks', 5, None, None)]
        self.assertEqual(expected, sorted(rows))
        rows = model.getStatements(subject=RSRC_URI+'label/labelid#1',
                                   predicate='albumsA')
        expected = [
            ('label/labelid#1', 'albumsA', 1, None, None),
            ('label/labelid#1', 'albumsA', 2, None, None)]
        self.assertEqual(expected, sorted(rows))
        rows = model.getStatements(subject=RSRC_URI+'label/labelid#1',
                                   predicate='albumsB')
        expected = [
            ('label/labelid#1', 'albumsB', 1, None, None), 
            ('label/labelid#1', 'albumsB', 2, None, None)]
        self.assertEqual(expected, sorted(rows))
        rows = model.getStatements(subject=RSRC_URI+'album/albumid#1',
                                   predicate='grammyclasses')
        expected = [
            ('album/albumid#1', 'grammyclasses', 'exotica', None, None),
            ('album/albumid#1', 'grammyclasses', 'shmaltz', None, None)]
        self.assertEqual(expected, sorted(rows))
        # TEST VIEW REF PROPERTY
        rows = model.getStatements(subject=RSRC_URI+'album/albumid#1',
                                   predicate='artists')
        expected = [
            ('album/albumid#1', 'artists', 'lashana', None, None),
            ('album/albumid#1', 'artists', 'lucy', None, None)]
        self.assertEqual(expected, sorted(rows))
        rows = model.getStatements(subject=RSRC_URI+'artist/artistid#1',
                                   predicate='albumname')
        expected = [
            ('artist/artistid#1', 'albumname', 'Blended Up in Black', None, None),
            ('artist/artistid#1', 'albumname', 'Greatest Hits', None, None)]
        self.assertEqual(expected, sorted(rows))
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
        tStmts = [
            Statement(RSRC_URI+'track/trackid#14',
                      'title', 'dumb song p1', 'en-1', None),
            Statement(RSRC_URI+'track/trackid#14',
                      'tracklength', 129, 'en-1', None),
            Statement(RSRC_URI+'track/trackid#14',
                      'date', datetime.date(2011, 07, 30), 'en-1', None),

            Statement(RSRC_URI+'track/trackid#15',
                      'title', 'dumb song p2', 'en-1', None),
            Statement(RSRC_URI+'track/trackid#15',
                      'tracklength', 345, 'en-1', None),
            Statement(RSRC_URI+'track/trackid#15',
                      'date', datetime.date(2011, 07, 21), 'en-1', None),

            Statement(RSRC_URI+'track/trackid#16',
                      'title', 'atonica', 'en-1', None),
            Statement(RSRC_URI+'track/trackid#16',
                      'tracklength', 423, 'en-1', None),
            Statement(RSRC_URI+'track/trackid#16',
                      'date', datetime.date(2011, 06, 12), 'en-1', None),
            ]
        model = self.getModel()
        ret = model.addStatements(tStmts)
        # confirm search for subject ID finds all related properties and values
        rows = model.getStatements(subject=tStmts[0].subject)
        expected = [
            ('track/trackid#14', 'date', datetime.date(2011, 07, 30), None, None), 
            ('track/trackid#14', 'title', 'dumb song p1', None, None),
            ('track/trackid#14', 'trackid', 14, None, None),
            ('track/trackid#14', 'tracklength', 129, None, None)]
        self.assertEqual(expected, sorted(rows))
        # remove one subject by ID and confirm that it's gone
        ts0 = Statement(tStmts[0].subject, None, None, None, None)
        ret = model.removeStatement(ts0)
        self.assertEqual(1, ret)
        rows = model.getStatements(subject=tStmts[0].subject)
        self.assertEqual(0, len(rows))
        # try to remove the deleted statement again
        ret = model.removeStatement(ts0)
        self.assertEqual(0, ret)
        # add statement twice without duplicate
        model.addStatement(tStmts[0])
        r1 = model.getStatements(subject=tStmts[0].subject)
        model.addStatement(tStmts[0])
        r2 = model.getStatements(subject=tStmts[0].subject)
        self.assertEqual(r1, r2)
        # add list of statements twice without duplicate
        ret = model.addStatements(tStmts)
        self.assertEqual(9, ret)
        rows = model.getStatements(subject=RSRC_URI+'track', predicate='title')
        expected = [
            (u'track/trackid#1', 'trackname', u'love song one', None, None),
            (u'track/trackid#2', 'trackname', u'love song two', None, None),
            (u'track/trackid#3', 'trackname', u'love song three', None, None),
            (u'track/trackid#4', 'trackname', u'love song four', None, None),
            (u'track/trackid#5', 'trackname', u'love song five', None, None),
            (u'track/trackid#6', 'trackname', u'hate song one', None, None),
            (u'track/trackid#7', 'trackname', u'hate song two', None, None),
            (u'track/trackid#8', 'trackname', u'hate song three', None, None),
            (u'track/trackid#9', 'trackname', u'hate song four', None, None),
            (u'track/trackid#10', 'trackname', u'something happened part 1', None, None),
            (u'track/trackid#11', 'trackname', u'something happened part 2', None, None),
            (u'track/trackid#12', 'trackname', u'nothing happened part 1', None, None),
            (u'track/trackid#13', 'trackname', u'nothing happened part 2', None, None),
            (u'track/trackid#14', 'trackname', u'dumb song p1', None, None),
            (u'track/trackid#15', 'trackname', u'dumb song p2', None, None),
            (u'track/trackid#16', 'trackname', u'atonica', None, None)]
        self.assertEqual(expected, rows)
        ret = model.addStatements(tStmts)
        rows = model.getStatements(subject=RSRC_URI+'track', predicate='title')
        expected = [
            (u'track/trackid#1', 'trackname', u'love song one', None, None),
            (u'track/trackid#2', 'trackname', u'love song two', None, None),
            (u'track/trackid#3', 'trackname', u'love song three', None, None),
            (u'track/trackid#4', 'trackname', u'love song four', None, None),
            (u'track/trackid#5', 'trackname', u'love song five', None, None),
            (u'track/trackid#6', 'trackname', u'hate song one', None, None),
            (u'track/trackid#7', 'trackname', u'hate song two', None, None),
            (u'track/trackid#8', 'trackname', u'hate song three', None, None),
            (u'track/trackid#9', 'trackname', u'hate song four', None, None),
            (u'track/trackid#10', 'trackname', u'something happened part 1', None, None),
            (u'track/trackid#11', 'trackname', u'something happened part 2', None, None),
            (u'track/trackid#12', 'trackname', u'nothing happened part 1', None, None),
            (u'track/trackid#13', 'trackname', u'nothing happened part 2', None, None),
            (u'track/trackid#14', 'trackname', u'dumb song p1', None, None),
            (u'track/trackid#15', 'trackname', u'dumb song p2', None, None),
            (u'track/trackid#16', 'trackname', u'atonica', None, None)]
        self.assertEqual(expected, rows)
        # now remove a list of statements by ID
        rmvStmts = [
            Statement(tStmts[0].subject, None, None, None, None),
            Statement(tStmts[3].subject, None, None, None, None),
            Statement(tStmts[6].subject, None, None, None, None)]
        ret = model.removeStatements(rmvStmts)
        self.assertEqual(3, ret)
        rows = model.getStatements(subject=RSRC_URI+'track', predicate='title')
        expected = [
            (u'track/trackid#1', 'trackname', u'love song one', None, None),
            (u'track/trackid#2', 'trackname', u'love song two', None, None),
            (u'track/trackid#3', 'trackname', u'love song three', None, None),
            (u'track/trackid#4', 'trackname', u'love song four', None, None),
            (u'track/trackid#5', 'trackname', u'love song five', None, None),
            (u'track/trackid#6', 'trackname', u'hate song one', None, None),
            (u'track/trackid#7', 'trackname', u'hate song two', None, None),
            (u'track/trackid#8', 'trackname', u'hate song three', None, None),
            (u'track/trackid#9', 'trackname', u'hate song four', None, None),
            (u'track/trackid#10', 'trackname', u'something happened part 1', None, None),
            (u'track/trackid#11', 'trackname', u'something happened part 2', None, None),
            (u'track/trackid#12', 'trackname', u'nothing happened part 1', None, None),
            (u'track/trackid#13', 'trackname', u'nothing happened part 2', None, None)]
        self.assertEqual(expected, rows)
        # remove all objects of one property type - clear this column
        ts = Statement(RSRC_URI+'track', 'title', None, None, None)
        ret = model.removeStatement(ts)
        self.assertEqual(13, ret)
        rows = model.getStatements(subject=RSRC_URI+'track',
                                   predicate='title', object='')
        expected = [
            ('track/trackid#1', 'trackname', '', None, None), 
            ('track/trackid#2', 'trackname', '', None, None), 
            ('track/trackid#3', 'trackname', '', None, None), 
            ('track/trackid#4', 'trackname', '', None, None), 
            ('track/trackid#5', 'trackname', '', None, None), 
            ('track/trackid#6', 'trackname', '', None, None), 
            ('track/trackid#7', 'trackname', '', None, None), 
            ('track/trackid#8', 'trackname', '', None, None), 
            ('track/trackid#9', 'trackname', '', None, None), 
            ('track/trackid#10', 'trackname', '', None, None), 
            ('track/trackid#11', 'trackname', '', None, None), 
            ('track/trackid#12', 'trackname', '', None, None), 
            ('track/trackid#13', 'trackname', '', None, None)]
        self.assertEqual(expected, rows)
        # remove all rows correspnding to reference property
        ts = Statement(RSRC_URI+'track/trackid#1', 'artists',
                       None, None, None)
        ret = model.removeStatement(ts)
        self.assertEqual(2, ret)
        rows = model.getStatements(ts.subject, ts.predicate)
        self.assertEqual(0, len(rows))
        # try with another table relation
        ts = Statement(RSRC_URI+'track/trackid#2', 'albums',
                       None, None, None)
        ret = model.removeStatement(ts)
        self.assertEqual(2, ret)
        rows = model.getStatements(ts.subject, ts.predicate)
        self.assertEqual(0, len(rows))
        # try with another table relation
        ts = Statement(RSRC_URI+'album/albumid#3', 'tracks',
                       None, None, None)
        ret = model.removeStatement(ts)
        self.assertEqual(4, ret)
        rows = model.getStatements(ts.subject, ts.predicate)
        self.assertEqual(0, len(rows))
        # try with another table relation
        ts = Statement(RSRC_URI+'album/albumid#1', 'grammyclasses',
                       None, None, None)
        ret = model.removeStatement(ts)
        self.assertEqual(2, ret)
        rows = model.getStatements(ts.subject, ts.predicate)
        self.assertEqual(0, len(rows))
        # remove entire contents of a table
        ts = Statement(RSRC_URI, "rdf:type", 'label', None, None)
        ret = model.removeStatement(ts)
        self.assertEqual(3, ret)
        rows = model.getStatements(ts.subject, ts.predicate)
        self.assertEqual(0, len(rows))

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
            cmd = "psql -q -U vesper -d jsonmap_db < {0} 2>/dev/null".format(
                self.sqlSchemaPath)
            call(cmd, shell=True)


        def tearDown(self):
            # destroy all zombies
            call('''
                 psql -q -U vesper -c \
                 \"select pg_terminate_backend(procpid) from pg_stat_activity 
                   where datname = \'jsonmap_db\'\" postgres >> /dev/null''', 
                 shell=True)
            call('''
                 psql -q -U vesper -c \
                 \"drop database if exists jsonmap_db\" postgres''', 
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
