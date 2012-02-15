#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    model unit tests
"""
import unittest
import subprocess, tempfile, os, signal, sys
import string, random, shutil, time

from vesper.data.base import *
from vesper.data import base
from vesper.data.base import graph
from vesper.data.store.basic import *

graphManagerClass = graph.MergeableGraphManager
#graphManagerClass = graph.NamedGraphManager

def random_name(length):
    return ''.join(random.sample(string.ascii_letters, length))

class SimpleModelTestCase(unittest.TestCase):
    "Tests basic features of a store"
    persistentStore = True

    def _getModel(self, model):
        return model

    def getModel(self):
        model = MemStore()
        self.persistentStore = False
        return self._getModel(model)
    
    def testStore(self):
        "basic storage test"
        model = self.getModel()

        # confirm a randomly created subject does not exist
        subj = random_name(12)
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set())

        # add a new statement and confirm the search succeeds
        s1 = Statement(subj, 'pred', "obj")
        model.addStatement(s1)
        s2 = Statement(subj, 'pred2', "obj2")
        model.addStatement(s2)
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set([s1, s2]))
        model.commit()
        model.close()

        if not self.persistentStore:
            return 

        model = self.getModel()
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set([s1, s2]))
        model.removeStatement(s2)
        s3 = Statement(subj, 'pred3', "obj3")
        model.addStatement(s3)
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set([s1, s3]))
        model.commit()
        model.close()
        model = self.getModel()
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set([s1, s3]))
        model.close()


    def _testGetStatements(self, asQuad=True):
        model = self.getModel()
                        
        stmts = [Statement('s', 'p', 'o', 'en', 'c'),
        Statement('s', 'p', 'o', 'en', 'c1'),
        Statement('s', 'p', 'o', 'en-1', 'c1'),
        Statement('s', 'p', 'o1', 'en-1', 'c1'),
        Statement('s', 'p1', 'o1', 'en-1', 'c1'),
        Statement('s1', 'p1', 'o1', 'en-1', 'c1')
        ]
        model.addStatements(stmts)
        
        conditions = ['subject', 's', 
            'predicate', 'p', 
            'object', 'o',
            'objecttype', 'en',
            'context', 'c']
        pairs = [ (('subject', 's'),), 
            (('predicate', 'p'),), 
            #look up object and objectype together:
            (('object', 'o'), ('objecttype', 'en')),
            (('context', 'c'),)]
        
        #each additional condition eliminates one of the matches
        beginMatches = 5+1
        while pairs:
            matches = beginMatches
            kw = {}
            for p in pairs:
                #first match each condition individually
                matches -= len(p)
                q = dict(p)
                r1 = model.getStatements(**q) #{k : v})                
                self.assertEqual(len(r1), matches, 'getstatements(**%s) found %s but expected length %d' % (q,r1,matches))
                self.assertEqual(set(r1), set(stmts[:matches]))
                #add to group
                kw.update(q) #kw[k] = v
                kw['asQuad'] = asQuad
                r2 = model.getStatements(**kw)
                expected = stmts[:matches]
                count = matches
                if not asQuad and len(expected) > 1:
                    del expected[1] #delete ('s', 'p', 'o', 'en', 'c1')
                    count -= 1
                #print 'query', kw
                #print r2
                #print 'expected', expected
                self.assertEqual(len(r2), count)
                self.assertEqual(set(r2), set(expected))
                
            #repeat tests but start matching at next position
            popped = pairs.pop(0)
            beginMatches -= len(popped)
        
        more =  [
        Statement('s', 'p1', 'o2', 'en-1', 'c1'),
        Statement('s', 'p1', 'o1', 'en-1', 'c2'),
        Statement('s2', 'p1', 'o2', 'en-1', 'c2'),
        ]
        model.addStatements(more)
        
        r = model.getStatements(predicate='p1', context='c2')
        self.assertEqual(set(r), set(more[1:]) )
        
        r = model.getStatements(subject='s', predicate='p1')
        self.assertEqual(set(r), set( (more[0], more[1], stmts[-2]) ) )
        
        r = model.getStatements(predicate='p1')
        self.assertEqual(set(r), set( more + stmts[-2:] ) )
        
        r = model.getStatements(predicate='p1', object='o')
        self.assertEqual(r, [])
        
        r = model.getStatements(predicate='p1', object='o2', objecttype='en-1')
        self.assertEqual(set(r), set( (more[0], more[-1]) ) )

        model.close()

    def testGetStatements(self):
        self._testGetStatements(asQuad=True)

    def testGetStatementsAsTriples(self):
        self._testGetStatements(asQuad=False)
        
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
        checkr = model.updateAdvisory

        # set up the model with one randomly named statement
        subj = random_name(12)
        s1 = Statement(subj, random_name(24), random_name(12))
        s2 = Statement(subj+'2', random_name(24), random_name(12))
        s3 = Statement(subj+'3', random_name(24), random_name(12))
        ret = model.addStatements([s1, s2, s3])
        if checkr:
            self.assertEqual(ret, 3, 'added count is wrong')

        # confirm a search for the subject finds it
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set([s1])) # object exists

        # remove the statement and confirm that it's gone
        ret = model.removeStatement(s1)
        if checkr:
            assert ret, "statement should have been removed"

        r2 = model.getStatements(subject=subj)
        self.assertEqual(r2, []) # object is gone

        # remove the statement again
        ret = model.removeStatement(s1)
        if checkr:
            assert not ret, "statement shouldn't have been removed"

        ret = model.addStatement(s1)
        if checkr:
            assert ret, "statement should have been added"

        r2 = model.getStatements(subject=subj)
        self.assertEqual(r2, [s1])

        #add statement that already exists
        ret = model.addStatement(s2)
        if checkr:
            assert not ret, "statement shouldn't have been added"

        #remove it (and another one for good measure)
        ret = model.removeStatements([s2, s3])
        if checkr:
            self.assertEqual(ret, 2, 'remove count is wrong')
        
        #confirm that it's been removed
        r3 = model.getStatements(subject=s2.subject)
        self.assertEqual(r3, [])

        model.commit()
        model.close()
        
        if self.persistentStore:
            model = self.getModel()
            r2 = model.getStatements(subject=subj)
            self.assertEqual(r2, [s1])

            # remove the statement and confirm that it's gone
            ret = model.removeStatement(s1)
            if checkr:
                assert ret, "statement should have been removed"

            r2 = model.getStatements(subject=subj)
            self.assertEqual(r2, []) # object is gone

            # remove the statement again
            ret = model.removeStatement(s1)
            if checkr:
                assert not ret, "statement shouldn't have been removed"  

            model.close()


    def testSetBehavior(self):
        "confirm model behaves as a set"
        model = self.getModel()
        checkr = model.updateAdvisory

        s1 = Statement("sky", "is", "blue")
        s2 = Statement("sky", "has", "clouds")
        s3 = Statement("ocean", "is", "blue")

        # before adding anything db should be empty
        r1 = model.getStatements()
        self.assertEqual(set(r1), set())

        # add a single statement and confirm it is returned
        ret = model.addStatement(s1)
        if checkr:
          assert ret, "statement should have been added"

        model.debug = 1
        r2 = model.getStatements()
        model.debug = 0
        self.assertEqual(set(r2), set([s1]))

        # add the same statement again & the set should be unchanged
        ret = model.addStatement(s1)
        if checkr:
          assert not ret, "statement shouldn't have been added"
        
        r3 = model.getStatements()
        self.assertEqual(r3, [s1])

        r3 = model.getStatements(asQuad=True)
        self.assertEqual(r3, [s1])
        
        # add a second statement with the same subject as s1
        model.addStatement(s2)
        
        r4 = model.getStatements()
        self.assertEqual(set(r4), set([s1, s2]))

        # add a third statement with same predicate & object as s1
        model.addStatement(s3)
        r5 = model.getStatements()
        self.assertEqual(set(r5), set([s1,s2,s3]))

        model.close()


    def testQuads(self):
        "test (somewhat confusing) quad behavior"
        model = self.getModel()

        # add 3 identical statements with differing contexts
        statements = [Statement("one", "two", "three", "fake", "100"),
                      Statement("one", "two", "three", "fake", "101"),
                      Statement("one", "two", "three", "fake", "102")]
        model.addStatements(statements)

        # asQuad=True should return all 3 statements
        r1 = model.getStatements(asQuad=True)
        self.assertEqual(set(r1), set(statements))

        # asQuad=False (the default) should only return one
        expected = set()
        expected.add(statements[0])
        r2 = model.getStatements(asQuad=False)
        self.assertEqual(len(r2), 1)
        self.failUnless(r2[0] in statements)

        model.close()


    def testHints(self):
        "test limit and offset hints"
        model = self.getModel()
        
        # add 20 statements, subject strings '01' to '20'
        model.addStatements([Statement("%02d" % x, "pred", "obj") for x in range(1,21)])
        
        # test limit (should contain 1 to 5)
        r1 = model.getStatements(hints={'limit':5})
        self.assertEqual(set(r1), set([Statement("%02d" % x, "pred", "obj") for x in range(1,6)]))
        
        # test offset (should contain 11 to 20)
        r2 = model.getStatements(hints={'limit':10, 'offset':10})
        self.assertEqual(set(r2), set([Statement("%02d" % x, "pred", "obj") for x in range(11,21)]))
        
        # test limit and offset (should contain 13 & 14)
        r3 = model.getStatements(hints={'limit':2, 'offset':12})
        self.assertEqual(set(r3), set([Statement("%02d" % x, "pred", "obj") for x in range(13,15)]))
        
        #add statements that vary by predicate and context
        statements = [Statement('r4', p, 'v', 'L', c) for p in 'abc' for c in '123']
        model.addStatements(statements)
        r4 = model.getStatements(subject='r4', hints={'limit':2, 'offset':1}, asQuad=False)

        r4.sort()
        expected = [('r4', 'b', 'v', 'L'), ('r4', 'c', 'v', 'L')]
        self.assertEqual(len(r4), len(expected))
        for a, b in zip(r4, expected):
          self.assertEqual(a[:4], b) #ignore context

        r5 = model.getStatements(subject='r4', hints={'limit':2, 'offset':1}, asQuad=True)
        self.assertEquals(len(r5), 2)
        r5.sort()
        self.assertEquals(r5, statements[1:3])

        model.close()

'''
RSRC_URI = "http://souzis.com/"
class SqlMappingModelTestCase(unittest.TestCase):
    # Tests basic features of a JSON RDF SQL mapping store

    persistentStore = True

    def _getModel(self, model):
        return model


    def getModel(self):
        model = MemStore()
        self.persistentStore = False
        return self._getModel(model)


    def testStore(self):
        "basic storage test"

        global RSRC_URI

        model = self.getModel()

        # confirm a randomly created subject does not exist
        subj = RSRC_URI + 'track/trackid{1}'
        r1 = model.getStatements(subject=subj)
        self.assertEqual(set(r1), set())

        # add a new statement and confirm the search succeeds
        stmts = [
            Statement(subj, 'rdf:type', 'track'),
            Statement(subj, 'track/trackname', 'track 1'),
            Statement(subj, 'track/trackartist', 1)
            ]
        model.addStatements(stmts)
        r1 = model.getStatements(subject = subj)
        # XXX - EXPLICIT knowledge of number of tables needed for this test mode
        self.assertEqual([stmts[i][2] for i in range(0, 3)], [r[2] for r in r1])

        model.commit()
        model.close()

        if not self.persistentStore:
            return 

        model = self.getModel()
        r1 = model.getStatements(subject=subj)
        # XXX - EXPLICIT knowledge of number of columns per table is needed for this test mode
        self.assertEqual([stmts[i][2] for i in range(0, 3)], [r[2] for r in r1])

        # NOT IMPLEMENTED YET
        # - removal object value/s (null row element/s)
        # - removal of subject key/ID ignored 
        # 
        # - duplicate values. duplicate subject key/IDs
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
        model.close()

    

    def testGetStatements(self, asQuad=True):
        # <HACK>...<COUGH>...
        global RSRC_URI

        aStmts = [
        Statement(RSRC_URI + 'artist/artistid{1}', 'rdf:type', 'artist', 'en', None),
        Statement(RSRC_URI + 'artist/artistid{1}', RSRC_URI + 'artist/artistname', 'ralph', 'en', None),

        Statement(RSRC_URI + 'artist/artistid{2}', 'rdf:type', 'artist', 'en-1', None),
        Statement(RSRC_URI + 'artist/artistid{2}', RSRC_URI + 'artist/artistname', 'lauren', 'en-1', None),

        Statement(RSRC_URI + 'artist/artistid{3}', 'rdf:type', 'artist', 'en-1', None),
        Statement(RSRC_URI + 'artist/artistid{3}', RSRC_URI + 'artist/artistname', 'diane', 'en-1', None)
        ]
        
        tStmts = [
        Statement(RSRC_URI + 'track/trackid{1}', 'rdf:type', 'track', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{1}', RSRC_URI + 'track/trackname', 'track 1', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{1}', RSRC_URI + 'track/trackartist', 1, 'en-1', None),

        Statement(RSRC_URI + 'track/trackid{2}', 'rdf:type', 'track', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{2}', RSRC_URI + 'track/trackname', 'track 2', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{2}', RSRC_URI + 'track/trackartist', 1, 'en-1', None),

        Statement(RSRC_URI + 'track/trackid{3}', 'rdf:type', 'track', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{3}', RSRC_URI + 'track/trackname', 'track 3', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{3}', RSRC_URI + 'track/trackartist', 1, 'en-1', None),

        Statement(RSRC_URI + 'track/trackid{4}', 'rdf:type', 'track', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{4}', RSRC_URI + 'track/trackname', 'track A ', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{4}', RSRC_URI + 'track/trackartist', 2, 'en-1', None),

        Statement(RSRC_URI + 'track/trackid{5}', 'rdf:type', 'track', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{5}', RSRC_URI + 'track/trackname', 'track B', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{5}', RSRC_URI + 'track/trackartist', 2, 'en-1', None),

        Statement(RSRC_URI + 'track/trackid{6}', 'rdf:type', 'track', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{6}', RSRC_URI + 'track/trackname', 'track C', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{6}', RSRC_URI + 'track/trackartist', 2, 'en-1', None),

        Statement(RSRC_URI + 'track/trackid{7}', 'rdf:type', 'track', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{7}', RSRC_URI + 'track/trackname', 'song 1', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{7}', RSRC_URI + 'track/trackartist', 3, 'en-1', None),

        Statement(RSRC_URI + 'track/trackid{8}', 'rdf:type', 'track', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{8}', RSRC_URI + 'track/trackname', 'song 2', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{8}', RSRC_URI + 'track/trackartist', 3, 'en-1', None),

        Statement(RSRC_URI + 'track/trackid{9}', 'rdf:type', 'track', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{9}', RSRC_URI + 'track/trackname', 'song 3', 'en-1', None),
        Statement(RSRC_URI + 'track/trackid{9}', RSRC_URI + 'track/trackartist', 3, 'en-1', None),
        ]

        model = self.getModel()
        
        # load our two 'arbitrary' tables
        model.addStatements(aStmts)
        model.addStatements(tStmts)
        
        # verify select all rows from both tables
        rows = model.getStatements()
        self.assertEqual(len(aStmts) + len(tStmts), len(rows))

        # verify select all rows from a single table
        rows = model.getStatements(subject=RSRC_URI + 'artist/artistid')
        self.assertEqual([a[2] for a in aStmts], [r[2] for r in rows])

        # verify select all elements from one row of one table
        rows = model.getStatements(subject=RSRC_URI + 'artist/artistid{1}')
        # XXX - EXPLICIT knowledge of number of columns per table is needed for this test mode
        self.assertEqual([aStmts[i][2] for i in range(0, 2)], [r[2] for r in rows])

        # verify select all objects with a particular property from one table
        rows = model.getStatements(predicate=RSRC_URI + 'artist/artistname')
        self.assertEqual([s[2] for s in aStmts], [r[2] for r in rows])
        
        # verify select a property's object given subject ID  
        rows = model.getStatements(subject=RSRC_URI + 'artist/artistid{1}', predicate=RSRC_URI + 'artist/artistname')
        self.assertEqual('ralph', rows[1][2])
        
        # verify select subject ID given a property and object value
        rows = model.getStatements(predicate=RSRC_URI + 'artist/artistname', object='lauren')
        self.assertEqual('artistid{2}', rows[0][0])

        # REPEAT the above tests against another (bigger) table

        # verify select all rows from a single table
        rows = model.getStatements(subject=RSRC_URI + 'track/trackid')
        self.assertEqual([t[2] for t in tStmts], [r[2] for r in rows])

        # verify select all elements from one row of one table
        rows = model.getStatements(subject=RSRC_URI + 'track/trackid{1}')
        # XXX - EXPLICIT knowledge of number of columns per table is needed for this test mode
        self.assertEqual([tStmts[i][2] for i in range(0, 3)], [r[2] for r in rows])

        # verify select all objects with a particular property from one table
        rows = model.getStatements(predicate=RSRC_URI + 'track/trackname')
        def rmvMatches(x): return 'trackartist' not in x[1] 
        self.assertEqual([t[2] for t in filter(rmvMatches, tStmts)], [r[2] for r in rows])
    
        # verify select a property's object given subject ID  
        rows = model.getStatements(subject=RSRC_URI + 'track/trackid{1}', predicate=RSRC_URI + 'track/trackname')
        self.assertEqual('track 1', rows[1][2])
        
        # verify select subject ID given a property and object value
        rows = model.getStatements(predicate=RSRC_URI + 'track/trackname', object='track 1')
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

        tStmts = [
            # each unique subject key/ID needs a set of Statements to denote all its properties - one
            # Statement per column, plus one to denote the logical table which contains it all...
            # in order to properly test addition or removal of any given property value to the backend store, 
            # we need to:
            # - gain intimate knowledge of the target schema through a JSON mapping object - this will
            #   establish a namespace for us to use to refer to abstract resources and their properties 
            #   independently of backend namespace details (table names and column names) 
            # - insert a unique subject key/ID into some target table (create a row to store property 
            #   values for this resource as they are inserted)
            # - assign values to each property (insert into row cell) individually 
            # - remove each value from each property (null the row cell) individually
            # - "delete" the subject key/ID (null out all row elements, but leave empty row in backend DB)
            #
            # Remove stmt:                                                                                            
            # - id : null row, keep pri key
            # - id, prop :  null cell, keep pri key
            # - prop : null this cell @ every row
            # - prop, obj : null this cell iff cell =obj
            # 
            # Tests:                                                                                                               # - must be based on json map of logical tables/views property relationships
            # - names are retrieved dynamically at getModel time by sqlmapping instance.
            # - from test pov there is an 'arbitrary' number of logical tables w/arbitrary collections of 
            #   properties, ie cols
            # - current api implies shared knowledge of json map, which makes sense 
            # - should we expose getColsofInterest & parsedMapping data structures to test/user api?
            # - current test suite never refers to resource identifiers! Just context-free syntactic atoms.
            # - this works because we have the vesperstmts tbl ready to use for underspecified rsrcs
            # - preds are stored in generic "predicate" column cells rather than as columns
            # - subjs should be allowed to be duplicated for unique preds! Currently constraining 
            #   single pred ie property per subj ie+primary key/id!
            # 
            # Ideas:
            # - always load vesperstmts schema
            # - divert column name/tbl name to vsprstmts iff name not found in parsedMapping 
            # - iff no rsrc uri && iff no recognized (parsed from json map) table/property name then use vesperstmts. 
            #
            Statement(RSRC_URI + 'track/trackid{1}', 'rdf:type', 'track', 'en-1', None),
            Statement(RSRC_URI + 'track/trackid{1}', RSRC_URI + 'track/trackname', 'track 1', 'en-1', None),
            Statement(RSRC_URI + 'track/trackid{1}', RSRC_URI + 'track/trackartist', 1, 'en-1', None),
        ]
        
        ret = model.addStatements(tStmts)
        if checkr:
            self.assertEqual(ret, 3, 'added count is wrong')

        # confirm a search for the subject finds all related properties and values 
        rows = model.getStatements(subject=tStmts[0][0])
        # XXX - EXPLICIT knowledge of number of columns per table is needed for this test mode
        self.assertEqual([tStmts[i][2] for i in range(0, 3)], [r[2] for r in rows])

        # remove the subject  and confirm that it's gone
        ret = model.removeStatement(tStmts[0])
        if checkr:
            assert ret, "statement should have been removed"

        rows = model.getStatements(subject=tStmts[0][0])
        self.assertEqual([], rows)     # object is gone?

        # remove the statement again
        ret = model.removeStatement(tStmts[0])
        if checkr:
            assert not ret, "statement shouldn't have been removed"

        ret = model.addStatements(tStmts)
        if checkr:
            assert ret, "statement should have been added"

        rows = model.getStatements(subject=tStmts[0][0])
        self.assertEqual([t[2] for t in tStmts], [r[2] for r in rows])

        #add statement that already exists
        ret = model.addStatement(tStmts[0])
        if checkr:
            assert not ret, "statement shouldn't have been added"

        #remove it (and another one for good measure)
        print [tStmts[0], tStmts[1]]
        ret = model.removeStatements([tStmts[0], tStmts[1]])
        if checkr:
            self.assertEqual(ret, 6, 'remove count is wrong')
        
        #confirm that it's been removed
        rows = model.getStatements(subject=tStmts[1].subject)
        self.assertEqual([], rows)

        model.commit()
        model.close()
'''        


class BasicModelTestCase(SimpleModelTestCase):

    def getTransactionModel(self):
        model = TransactionMemStore()
        self.persistentStore = False
        return self._getModel(model)


    def testAutocommit(self):
        statements = [Statement("one", "equals", " one "),
                      Statement("two", "equals", " two "),
                      Statement("three", "equals", " three ")]
        
        model = self.getTransactionModel()
        model.autocommit = True
        model.addStatements(statements)
        # rollback should have no effect
        model.rollback() 
        r1 = model.getStatements()
        self.assertEqual(set(r1), set(statements))
        model.close()

        if self.persistentStore:
            modelA = self.getTransactionModel()
            modelA.autocommit = True
            modelB = self.getTransactionModel()
            # add statements and confirm the both A and B see them 
            # even though we didn't explicitly commit
            modelA.addStatements(statements)
            r2a = modelA.getStatements()
            self.assertEqual(set(r2a), set(statements))
            r2b = modelB.getStatements()
            self.assertEqual(set(r2b), set(statements))
            # turn off autocommit
            modelA.autocommit = False
            # add more statements and confirm A sees them and B doesn't
            s2 = [Statement("sky", "is", "blue")]
            modelA.addStatements(s2)
            r3a = modelA.getStatements()
            self.assertEqual(set(r3a), set(statements+s2))
            r3b = modelB.getStatements()
            self.assertEqual(set(r3b), set(statements))
            modelA.close()
            modelB.close()

    def testTransactionCommitAndRollback(self):
        "test simple commit and rollback on a single model instance"
        model = self.getTransactionModel()

        s1 = Statement("sky", "is", "blue")
        s2 = Statement("sky", "has", "clouds")

        # confirm that database is initially empty
        r1 = model.getStatements()
        self.assertEqual(set(r1), set())

        # add first statement and commit, confirm it's there
        model.addStatement(s1)
        model.commit()
        r2 = model.getStatements()
        self.assertEqual(set(r2), set([s1]))

        # add second statement and rollback, confirm it's not there
        model.addStatement(s2) 
        r3 = model.getStatements()
        self.assertEqual(set(r3), set([s1, s2]))
        model.rollback()
        r3 = model.getStatements()
        self.assertEqual(set(r3), set([s1]))

        model.close()


    def testTransactionIsolationCommit(self):
        "test commit transaction isolation across 2 models"
        modelA = self.getTransactionModel()
        modelB = self.getTransactionModel()
        
        #include spaces in value so it looks like a literal, not a resource
        statements = [Statement("one", "equals", " one "),
                      Statement("two", "equals", " two "),
                      Statement("three", "equals", " three ")]

        # confirm models are empty
        r1a = modelA.getStatements()
        r1b = modelB.getStatements()
        self.assertEqual(set(), set(r1a), set(r1b))

        # add statements and confirm A sees them and B doesn't
        modelA.addStatements(statements)
        r2a = modelA.getStatements()
        self.assertEqual(set(r2a), set(statements))
        r2b = modelB.getStatements()
        self.assertEqual(set(r2b), set())

        # commit A and confirm both models see the statements
        modelA.commit()
        r3a= modelA.getStatements()
        r3b = modelB.getStatements()
        self.assertEqual(set(statements), set(r3a), set(r3b))
        
        modelA.close()
        modelB.close()
        
        #reload the data
        if not self.persistentStore:
            return
            
        modelC = self.getTransactionModel()
        r3c = modelC.getStatements()
        self.assertEqual(set(statements), set(r3c))

        modelC.close()


    def testTransactionIsolationRollback(self):
        "test rollback transaction isolation across 2 models"
        modelA = self.getTransactionModel()
        modelB = self.getTransactionModel()

        statements = [Statement("one", "equals", "one"),
                      Statement("two", "equals", "two"),
                      Statement("three", "equals", "three")]

        # confirm models are empty
        r1a = modelA.getStatements()
        r1b = modelB.getStatements()
        self.assertEqual(set(), set(r1a), set(r1b))

        # add statements and confirm A sees them and B doesn't
        modelA.addStatements(statements)
        r2a = modelA.getStatements()
        self.assertEqual(set(r2a), set(statements))
        r2b = modelB.getStatements()
        self.assertEqual(set(r2b), set())

        # rollback A and confirm both models see nothing
        modelA.rollback()
        r3a = modelA.getStatements()
        r3b = modelB.getStatements()
        self.assertEqual(set(), set(r3a), set(r3b))

        modelA.close()
        modelB.close()


    def testInsert(self):
        model = self.getModel()
        print 'start insert with %s objects (-b to change)' % BIG 
        start = time.time()

        n = 0
        for i in xrange(BIG):
            subj = random_name(12)
            for j in xrange(7):
                n += model.addStatement(Statement(subj, 'pred'+str(j), 'obj'+str(j)))
        print 'added %s statements in %s seconds' % (n, time.time() - start)
        model.commit()

        try:
            if self.persistentStore:
                if hasattr(model, 'close'):
                    sys.stdout.flush()
                    start = time.time()
                    model.close()
                    print 'closed in %s seconds, re-opening' % (time.time() - start)                    
                model = self.getModel()
        except:
            import traceback
            traceback.print_exc()
            model.close()
            sys.stdout.flush()
            raise

        print 'getting statements'
        sys.stdout.flush()
        start = time.time()
        stmts = model.getStatements()
        print 'got %s statements in %s seconds' % (len(stmts), time.time() - start)
        self.assertEqual(len(stmts), BIG * 7)
        
        start = time.time()
        lastSubject = None
        for i, s in enumerate(stmts):
            if i > BIG: 
                break
            if s[0] != lastSubject:
                lastSubject = s[0]
                self.assertEqual(len(model.getStatements(s[0])), 7)
        print 'did %s subject lookups in %s seconds' % (BIG, time.time() - start)

        model.close()


class TransactionModelTestCase(SimpleModelTestCase):
    persistentStore = False

    def getModel(self):
        model = TransactionMemStore()
        return self._getModel(model)

class GraphModelTestCase(BasicModelTestCase):

    def _getModel(self, model):
        modelUri = base.generateBnode()
        return graphManagerClass(model, None, modelUri)

class SplitGraphModelTestCase(BasicModelTestCase):

    def _getModel(self, model):
        modelUri = base.generateBnode()
        revmodel = TransactionMemStore()
        return graphManagerClass(model, revmodel, modelUri)

    
BIG = 100 #10000
def main(testCaseClass):
    if '-b' in sys.argv:
        i = sys.argv.index("-b")
        global BIG
        BIG = int(sys.argv[i+1])
        del sys.argv[i:i+2]
        
    try:
        test=sys.argv[sys.argv.index("-r")+1]
    except (IndexError, ValueError):
        #we need to specify the testCaseClass module to prevent
        #BasicModelTestCase from running
        unittest.main(testCaseClass.__module__)
    else:
        path = test.split('.')
        if len(path) > 1:
            testCaseClass = getattr(__import__(testCaseClass.__module__), path[0])
            test = path[1]
        tc = testCaseClass(test)
            
        testfunc = getattr(tc, test)
        tc.setUp()
        try:
            testfunc() #run test
        finally:
            tc.tearDown()

if __name__ == '__main__':
    main(BasicModelTestCase)
