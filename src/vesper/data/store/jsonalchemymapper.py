import json
import pprint
from sqlalchemy import engine
from sqlalchemy.engine import reflection
from collections import Counter

RSRC_DELIM='/'
VAL_OPEN_DELIM='{'
VAL_CLOSE_DELIM='}'

class JsonAlchemyMapper():

    def __init__(self, mapping=None, engine=None):
        if engine is None:
            print "JsonAlchemyMapper requires a valid alchemy engine parameter"
            exit
        self.insp = reflection.Inspector.from_engine(engine)
        self._generateMapping(mapping)
        self._getColumnsOfInterest()
        # output readable json map and sql schema as diagnostic
        self._printSchemata()
        

    def _printSchemata(self):        
        print '===BEGIN SCHEMA INFO==========================================='
        print 'SQL schema::'
        for tbl in self.insp.get_table_names():
            if tbl == 'vesper_stmts':
                continue
            print 'TABLE:', tbl
            for c in self.insp.get_columns(tbl):
                print '\t', c['name'], c['type']
            for pk in self.insp.get_primary_keys(tbl):
                print '\tPRIMARY KEY:'
                print '\t\t', pk
            for i in self.insp.get_indexes(tbl):
                print '\tINDEX:' 
                print '\t\t', i['name'], 'UNIQUE' if 'unique' in i else '',\
                    'ON', [ic.encode('ascii') for ic in i['column_names']]
            for fk in self.insp.get_foreign_keys(tbl):
                print '\tFOREIGN KEY:'
                print '\t\t',\
                    [cc.encode('ascii') for cc in fk['constrained_columns']],\
                    'ON', fk['referred_table'],\
                    [rc.encode('ascii') for rc in fk['referred_columns']]
        for vw in self.insp.get_view_names():
            print 'VIEW:', vw
            q = self.insp.get_view_definition(vw)
            for c in self.insp.get_columns(vw):
                print '\t', c['name'], c['type']
            print 'VIEW DEFINITION:'
            print '\t', q
        print '===END SCHEMA INFO============================================'
        print '===BEGIN JSON MAP============================================='
        print json.dumps(self.mapping, indent=2)
        print '===END JSON MAP==============================================='


    def _getColumnsOfInterest(self):
        '''
        using our json mapping object (possibly derived by we ourselves based 
        on inspection of the active SQL schema), collate and store vital
        information about the tables that we are to use into a list of simple 
        dicts - this allows us to ignore all unused tables and columns
        '''
        self.parsedTables = []
        for tableName in self.insp.get_table_names():
            if tableName == 'vesper_stmts':
                # don't analyze our private special purpose table
                continue
            readonly = False
            pKeyName = None
            tableDesc = self.mapping["tables"][tableName]
            if 'relationship' in tableDesc:
                # we don't need to store info on 'correlation' tables
                #                continue
                # but I'm experimenting with "view" reference parsing so...
                pass
            if 'id' in tableDesc:
                pKeyName = tableDesc['id']
            if 'readonly' in tableDesc:
                readonly = tableDesc['readonly']
            if 'properties' in tableDesc:
                colNames = {}
                viewRefs = []
                joinCols = []
                refFKeys = []
                for p in tableDesc['properties']:
                    if isinstance(p, dict):
                        (vr, jc, rfk) = self._parseRefDict(p)
                        if vr:
                            viewRefs.append(vr)
                        if jc:
                            joinCols.append(jc)
                        if rfk:
                            refFKeys.append(rfk)
                    elif p == "*":
                        for c in self.insp.get_columns(tableName):
                            if not c['primary_key']:
                                colNames[c['name']] = c['name']
                    else:
                        if p not in colNames.keys() and not p['primary_key']:
                            colNames[p] = p
            self.parsedTables.append({'tableName': tableName,
                                      'readOnly': readonly,
                                      'pKeyName': pKeyName,
                                      'colNames': colNames,
                                      'refFKeys': refFKeys,
                                      'viewRefs': viewRefs, 
                                      'joinCols': joinCols, 
                                      'refFKeys': refFKeys})
        self.pp = pprint.PrettyPrinter(indent=2)
        self.pp.pprint(self.parsedTables)


    def _parseRefDict(self, refDict):
        '''
        collect foreign key declarations that point into the current table
        as described by JSON mapping 'references' properties, build a
        compiled dictionary of relevant column names and relations - also
        take note of left-side join columns and view membership properties
        '''
        viewRef = ()
        joinCols = ()
        refFKeys = ()
        for k,v in refDict.items():
            if "key" in v:
                joinCols = (v)
            if "view" in v:
                r = v['view']
                if isinstance(r, dict):
                    vName = r['name']
                    if 'column' in r:
                        vCol = r['column']
                    else:
                        vCol = k
                    if 'key' in r:
                        vKey = r['key']
                    viewRef = (vName, vCol, vKey)
                else:
                    viewRef = (r, k, "id")
            if 'references' in v:
                r = v['references']
                if isinstance(r, dict):
                    tbl = r['table']
                    col = r['key']
                    vals = []
                    if 'value' in r:
                        [vals.append(k) for k in r['value'].keys()]
                    refFKeys = (tbl, col, vals)
        return (viewRef, joinCols, refFKeys)


    def readOnly(self, tableName):
        for td in self.parsedTables:
            if td['tableName'] == tableName:
                return td['readOnly'] == 'true'


    def getColFromPred(self, tableName, predicate):
        if not tableName or not predicate:
            return None
        pName = self.getPropNameFromResourceId(predicate)
        for td in self.parsedTables:
            if td['tableName'] == tableName:
                for k, v in td['colNames'].items():
                    if k == pName:
                        return v
        return None


    def getTableFromResourceId(self, uri):
        '''
        extract table name from (possibly non-URI) resource string
        '''
        if not uri:
            return None
        if self.mapping['idpattern'] in uri:
            uri = uri[len(self.mapping['idpattern']):]
        tName = uri.split(RSRC_DELIM)[0]
        for td in self.parsedTables:
            if td['tableName'] == tName:
                return tName
        return None

    
    def getPrimaryKeyName(self, tableName):
        '''
        if tableName is in our 'active' list, return its primary key col name
        '''
        if not tableName:
            return None
        for td in self.parsedTables:
            if td['tableName'] == tableName:
                return td['pKeyName']
        return None


    def getPropNameFromResourceId(self, uri):
        '''
        extract property name from (possibly non-URI) resource string
        '''
        if not uri:
            return None
        pName = uri
        if self.mapping['idpattern'] in uri:
            uri = uri[len(self.mapping['idpattern']):]
        if VAL_OPEN_DELIM in uri:
            uri = uri.split(VAL_OPEN_DELIM)[0]
            pName = uri.split(RSRC_DELIM, 2)[1]
        elif RSRC_DELIM in uri:
            pName = uri.split(RSRC_DELIM)[1]
        return pName


    def getValueFromResourceId(self, uri):
        '''
        extract "{value}" from (possibly non-URI) resource string
        '''
        if not uri:
            return None
        val = None                
        if self.mapping['idpattern'] in uri:
            uri = uri[len(self.mapping['idpattern']):]
        if VAL_OPEN_DELIM in uri:
            val = uri.split(VAL_OPEN_DELIM)[1].rstrip(VAL_CLOSE_DELIM)
        return val


    def getTableWithProperty(self, uri):
        '''
        search our db for tables w/columns matching property name
        '''
        if not uri:
            return None
        pName = self._getPropNameFromResourceId(uri)
        for td in self.parsedTables:
            for k, v in td['colNames'].items():
                if k == pName:
                    return td['tableName']
        return None


    def _generateMapping(self, mapping=None):
        if mapping:
            # simple! user has passed us a json-sql mapping
            self.mapping = mapping
            return

        # no map - derive a best guess based on SQLA schema inspection
        mDict = {
            "tablesprop": "HYPEtype",
            "idpattern": "http://souzis.com/",
            "tables": {}
            }
        for tbl in self.insp.get_table_names():
            if tbl == 'vesper_stmts':
                # don't need to decipher our private Statement table
                continue
            # no way of ruling out any properties here, so always include '*'
            mDict['tables'][tbl] = {'properties': ['*']}
            if self.insp.get_primary_keys(tbl):
                # primary key ==> 'id' property
                mDict['tables'][tbl]['id'] = self.insp.get_primary_keys(tbl)[0]

        # we've collected the list of all tables, now review it with an eye
        # for relationships and foreign keys 
        for tbl in mDict['tables']:
            cNames = [c['name'] for c in self.insp.get_columns(tbl)]
            fKeys = self.insp.get_foreign_keys(tbl)
            fkNames = []
            [fkNames.extend(f['constrained_columns']) for f in fKeys]
            if Counter(cNames) == Counter(fkNames):
                # looks like a correlation table, all foreign keys
                mDict['tables'][tbl]['relationship'] = 'true'

            # for each foreign key in this table, add a prop to referred table
            for fk in fKeys:
                # this preliminary nonsense prepares our 'value' property
                cc = fk['constrained_columns'][0]
                def f(x): return x['constrained_columns'][0] !=  cc
                vDict = {}
                for vKeys in filter(f, fKeys):
                    vDict[vKeys['constrained_columns'][0]] = {
                        'references': vKeys['referred_table']}
                (mDict['tables'][fk['referred_table']]['properties']).append(
                    {"{}_ref".format(tbl) : 
                     {'references': 
                      {'table': "{}".format(tbl),
                       'key': "{}".format(cc),
                       'value': vDict}}})

        # SQL views are marked as "readonly" tables
        for vw in self.insp.get_view_names():
            mDict['tables'][vw] = {'properties': ['*'], 'readonly': 'true'}

        # check that we found some relevant tables before moving on
        if mDict['tables']:
            self.mapping = mDict
