from sjson import * 
from pprint import pprint

def assert_json_match(expected, result, dosort=False):
    if dosort and isinstance(expected, list):
        result.sort()
        expected.sort()
    result = json.dumps(result,sort_keys=True)
    expected = json.dumps(expected, sort_keys=True)
    assert result == expected, pprint((result, '!=', expected))

def assert_stmts_match(expected_stmts, result_stmts):
    assert set(result_stmts) == set(expected_stmts), (
                        pprint(result_stmts), '!=', pprint(expected_stmts))

def assert_json_and_back_match(src, expectedstmts=None):
    test_json = [ json.loads(src) ]
    result_stmts = sjson().to_rdf( { 'results' : test_json} )
    #pprint( result_stmts)
    if expectedstmts is not None:
        assert_stmts_match(expectedstmts, result_stmts)
    
    result_json = sjson()._to_sjson( result_stmts )
    #pprint( result_json )
    assert_json_match(result_json, test_json) 

def assert_stmts_and_back_match(stmts, expectedobj = None):
    result = sjson()._to_sjson( stmts )
    if expectedobj is not None:
        assert_json_match(expectedobj, result, True)
    
    result_stmts = sjson().to_rdf( result )
    assert_stmts_match(stmts, result_stmts)

import unittest
class SjsonTestCase(unittest.TestCase):
    def testAll(self):
        test()
        
def test():
              
    dc = 'http://purl.org/dc/elements/1.1/'
    r1 = "http://example.org/book#1";     
    r2 = "http://example.org/book#2"; 
    stmts = [
    Statement(r1, dc+'title', u"SPARQL - the book",OBJECT_TYPE_LITERAL,''),
    Statement(r1, dc+'description', u"A book about SPARQL",OBJECT_TYPE_LITERAL,''),
    Statement(r2, dc+'title', u"Advanced SPARQL",OBJECT_TYPE_LITERAL,''),
    ]
    
    expected =[{'http://purl.org/dc/elements/1.1/description': 'A book about SPARQL',
            'http://purl.org/dc/elements/1.1/title': 'SPARQL - the book',
            'id': 'http://example.org/book#1'},
            {'http://purl.org/dc/elements/1.1/title': 'Advanced SPARQL',
            'id': 'http://example.org/book#2'}]
    
    assert_stmts_and_back_match(stmts, expected)
        
    stmts.extend( 
        [Statement("http://example.org/book#2", 'test:sequelto' , 'http://example.org/book#1', OBJECT_TYPE_RESOURCE),]
    )

    expected = [{"http://purl.org/dc/elements/1.1/title": "Advanced SPARQL",
    "id": "http://example.org/book#2",
    "test:sequelto": {
        "http://purl.org/dc/elements/1.1/description": "A book about SPARQL",
        "http://purl.org/dc/elements/1.1/title": "SPARQL - the book",
        "id": "http://example.org/book#1"
        }
    }]

    assert_stmts_and_back_match(stmts, expected)

    src = '''
    { "id" : "atestid",
       "foo" : { "id" : "bnestedid", "prop" : "value" }
    }'''
    assert_json_and_back_match(src)

    src = '''
    { "id" : "testid", 
    "foo" : ["1","3"],
     "bar" : [],
     "baz" : { "nestedobj" : { "id" : "anotherid", "prop" : "value" }}
    } 
    '''

    assert_json_and_back_match(src)

    #test nested lists
    src = '''
    { "id" : "testid",
    "foo" : [1,  ["nested1",
                       { "id": "nestedid", "nestedprop" : [ "nested3" ] },
                    "nested2"],
            3],
    "bar" : [ [] ]
    }
    '''
    assert_json_and_back_match(src)

    #test numbers and nulls
    src = '''
    { "id" : "test",
    "float" : 1.0,
      "integer" : 2,
      "null" : null,
      "list" : [ 1.0, 2, null ]
    }
    '''
    assert_json_and_back_match(src)

    src = '''
    { "id" : "test",
     "circular" : "test",
      "circularlist" : ["test"],
      "circularlist2" : [["test"],["test"]]
    }
    '''
    assert_json_and_back_match(src)

    #test missing ids and exclude_blankids
    #test shared
    print 'tests pass'


if __name__  == "__main__":
    test()
