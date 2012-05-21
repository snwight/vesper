import json
import pprint
from sqlalchemy import engine
from sqlalchemy.engine import reflection
from collections import Counter

import vesper.data.base.utils

RSRC_DELIM='/'
PKEY_VAL_DELIM='#'
PKEY_DELIM = '.'

SPEW=False
SPEW_SCHEMA=False

idkey = object()

class JsonAlchemyMapper():

    def __init__(self, mapping=None, engine=None):
        if engine is None:
            #XXX throw exception
            print "JsonAlchemyMapper requires a valid alchemy engine parameter"
            exit
        self.insp = reflection.Inspector.from_engine(engine)
        self._generateMapping(mapping)
        self._getColumnsOfInterest()
        # output readable json map and sql schema as diagnostic
        if SPEW_SCHEMA:
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


    def _generateMapping(self, mapping=None):
        '''
        if we have no JSON-SQL property mapping supplied to us by the caller
        we use this utility to derive a best guess mapping object based on 
        SQLA schema inspection
        '''
        if mapping:
            # simple! user has passed us a json-sql mapping
            self.mapping = mapping
            return
        mDict = {
            "tablesprop": "GENERATEDtype",
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
                # primary keys ==> 'id' property list
                mDict['tables'][tbl]['id'] = self.insp.get_primary_keys(tbl)
        # we've collected the list of all tables, now review it with an eye
        # for relationships and foreign keys
        for tbl in mDict['tables']:
            cNames = [c['name'] for c in self.insp.get_columns(tbl)]
            fKeys = self.insp.get_foreign_keys(tbl)
            fkNames = []
            [fkNames.extend(f['constrained_columns']) for f in fKeys]
            if Counter(cNames) == Counter(fkNames):
                # heuristic: all foreign keys ==>  correlation table
                mDict['tables'][tbl]['relationship'] = 'true'
            for fk in fKeys:
                # this preliminary nonsense prepares our 'value' property
                cc = fk['constrained_columns'][0]
                def f(x): return x['constrained_columns'][0] !=  cc
                vDict = {}
                # for each fk in this table, add a prop to referred table
                for vKeys in filter(f, fKeys):
                    vDict[vKeys['constrained_columns'][0]] = {
                        'references': vKeys['referred_table']}
                (mDict['tables'][fk['referred_table']]['properties']).append(
                    {"{}_ref".format(tbl): 
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


    def _getColumnsOfInterest(self):
        '''
        using our json mapping object (possibly derived by we ourselves based 
        on inspection of the active SQL schema), collate and store vital
        information about the tables that we are to use into a list of simple 
        dicts - this allows us to ignore all unused tables and columns
        '''
        self.parsedTables = []
        for tableName, tableDesc in self.mapping["tables"].items():
            readonly = relation = False
            pKeyNames = []
            if 'relationship' in tableDesc:
                relation = tableDesc['relationship']
            if 'id' in tableDesc:
                pKeyNames = tableDesc['id']
            if 'readonly' in tableDesc:
                readonly = tableDesc['readonly']
            if 'properties' in tableDesc:
                colNames = {}
                viewRefs = []
                joinCols = []
                refFKeys = []
                for p in tableDesc['properties']:
                    if isinstance(p, dict):
                        (vr, jc, rfk, cn) = self._parsePropDict(p)
                        if vr: viewRefs.append(vr)
                        if jc: joinCols.append(jc)
                        if rfk: refFKeys.append(rfk)
                        if cn:
                            [(propNm, colNm)] = cn.items()
                            if colNm in colNames:
                                # "*" got it already - pop & replace!
                                del(colNames[colNm])
                            colNames[propNm] = colNm
                    elif p == "*":
                        for c in self.insp.get_columns(tableName):
                            if c['name'] not in colNames.values():
                                colNames[c['name']] = c['name']
                    else:
                        print "properties list contains unknown obj:", p
            # print "vref:", viewRefs
            self.parsedTables.append({'tableName': tableName,
                                      'readOnly': readonly,
                                      'relation': relation,
                                      'pKeyNames': pKeyNames,
                                      'colNames': colNames,
                                      'viewRefs': viewRefs, 
                                      'joinCols': joinCols, 
                                      'refFKeys': refFKeys})
        if SPEW: pprint.PrettyPrinter(indent=2).pprint(self.parsedTables)


    def _parsePropDict(self, refDict):
        '''
        handle each dictionary found in json mapping 'properties' list -
        collect foreign key declarations that point into the current table
        as described by JSON mapping 'references' properties, build a
        compiled dictionary of relevant column names and relations - also
        take note of left-side join columns and view membership properties
        '''
        joinCols = {}
        viewRef = {}
        refFKey = {}
        colName = {}
        for propName, v in refDict.items():
            if "key" in v:
                r = v['key']
                if isinstance(r, list):
                    joinCols[propName] = r
                elif r == 'id':
                    joinCols[propName] = [idkey]
                else:
                    joinCols[propName] = [r]
            elif "view" in v:
                r = v['view']
                if isinstance(r, dict):
                    vName  = r['name']
                    col = r['column']
                    if r['key'] == 'id':
                        vKey = idkey
                    else:
                        vKey = r['key']
                    viewRef[propName] = (vName, col, vKey)
                else:
                    viewRef[propName] = r
            elif 'references' in v:
                r = v['references']
                if isinstance(r, dict):
                    tbl = r['table']
                    if r['key'] == 'id':
                        col = idkey
                    else:
                        col = r['key']
                    vals = []
                    if 'value' in r:
                        [vals.append(rk) for rk in r['value'].keys()]
                    refFKey[propName] = (tbl, col, vals)
                else:
                    refFKey[propName] = r
            else:
                colName[propName] = v
        return (viewRef, joinCols, refFKey, colName)


    def _getParsedValueFromTable(self, tableName, key):
        if tableName:
            for td in self.parsedTables:
                if td['tableName'] == tableName:
                    return td[key]
        return None


    def _stripUriPrefix(self, uri):
        if self.mapping['idpattern'] in uri:
            uri = uri[len(self.mapping['idpattern']):]
        return uri


    def getUriPrefix(self):
        if 'idpattern' in self.mapping:
            return self.mapping['idpattern']
        return None


    def _getPropNameFromResource(self, uri, index):
        rsrc = self._stripUriPrefix(uri) 
        if RSRC_DELIM in rsrc:
            rsrc = rsrc.split(RSRC_DELIM)[index]
        return rsrc


    def getTableNameFromResource(self, uri):
        return self._getPropNameFromResource(uri, 0)


    def getColNameFromResource(self, uri):
        return self._getPropNameFromResource(uri, 1)


    def isReadOnly(self, tableName):
        return self._getParsedValueFromTable(tableName, 'readOnly')


    def isRelation(self, tableName):
        return self._getParsedValueFromTable(tableName, 'relation')
         

    def getPKeyNamesFromTable(self, tableName):
        return self._getParsedValueFromTable(tableName, 'pKeyNames')


    def getRefFKeysFromTable(self, tableName):
        return self._getParsedValueFromTable(tableName, 'refFKeys')


    def getJoinColsFromTable(self, tableName):
        return self._getParsedValueFromTable(tableName, 'joinCols')


    def getViewRefsFromTable(self, tableName):
        return self._getParsedValueFromTable(tableName, 'viewRefs')


    def getColFromPred(self, tableName, predicate):
        cns = self._getParsedValueFromTable(tableName, 'colNames')
        pnm = self.getColNameFromResource(predicate)
        if pnm and cns:
            if pnm in cns:
                return cns[pnm]
        return None


    def getPKeyDictFromResource(self, uri):
        '''
        extract (possibly multiple) primary key names from resource string
        i.e. "RSRC_URI:/tablename/pkey1#pkval1.pkey2#pkval2", 
        render unto dictionary of {pkey1:pkval1, pkey2:pkval2,...} pairs
        '''
        if not uri:
            return None
        pk = pv = None
        pkDict = {}
        if RSRC_DELIM in uri and PKEY_VAL_DELIM in uri:
            for pkv in (uri.rsplit(RSRC_DELIM, 1)[1]).split(PKEY_DELIM):
                if pkv.find(PKEY_VAL_DELIM) > 0:
                    pk, pv = pkv.split(PKEY_VAL_DELIM)
                    pkDict[pk] = pv
        return pkDict


    def getTableNameFromProperty(self, uri):
        if uri:
            cn = self.getColNameFromResource(uri)
            for td in self.parsedTables:
                if cn in td['colNames']:
                    return td['tableName']
        return None
