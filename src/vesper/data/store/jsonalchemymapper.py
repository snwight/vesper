import json
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
        print json.dumps(self.mapping, sort_keys=True, indent=4)
        print '===END JSON MAP==============================================='


    def _parsePropertyDict(self, prop, i=0):
        '''
        walk through possibly nested dictionary of properties
        '''
        for (k, v) in prop.items():
            t = ''
            for x in range(0, i):
                t = '\t' + t
            print t, "iter =", i, "k =", k, "v =", v

            if k == 'references':
                '''
                "references" : { 
                "table" : <<tablename>>
                "key"...
                "value"...
                '''
                if isinstance(v, dict):
                    tbl = v['table']
                    col = v['key']
                    if col == "id":
                        col = self.insp.get_primary_keys(tbl)
                    refTbl, refCol = None
                    val = v['value']
                    if val == "id":
                        val = self.insp.get_primary_keys(tbl)
                    elif isinstance(val, dict):
                        refCol, v1 = val.items()
                        refTbl = v1['references']
                    (tbl, col, refTbl, refCol)
                else:
                    '''
                    "references" : <<tablename>> ||
                    '''
                    pass
            else:
                # we have a json property name for a column, store both
                #  colNames[k] = v
                pass 


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
            if 'id' in tableDesc:
                pKeyName = tableDesc['id']
            if 'readonly' in tableDesc:
                # this table is an immutable SQL view - query but don't modify
                readonly = tableDesc['readonly']
            if 'relationship' in tableDesc:
                # we don't need to store info on 'correlation' tables, they're
                # maintained implicitly when referents are updated
                pass
            if 'properties' in tableDesc:
                colNames = {}
                referringKeys = {}
                for p in tableDesc['properties']:
                    if isinstance(p, dict):
                        referringKeys = self._parseReferences(p)
                    elif p == "*":
                        # collect all non-primary key column names
                        for c in self.insp.get_columns(tableName):
                            if not c['primary_key']:
                                colNames[c['name']] = c['name']
                    else:
                        # add new column name to property:column pairs 
                        if p not in colNames.keys() and not p['primary_key']:
                            colNames[p] = p
            self.parsedTables.append({'tableName': tableName,
                                      'readOnly': readonly,
                                      'pKeyName': pKeyName,
                                      'referringKeys': referringKeys,
                                      'colNames': colNames})
            

    def getColFromPred(self, tableName, predicate):
        if not tableName or not predicate:
            return None
        pName = self.getPropNameFromResourceId(predicate)
        for t in self.parsedTables:
            if t['tableName'] == tableName:
                for k, v in t['colNames'].items():
                    if k == pName:
                        return v
        return None


    def _parseReferences(self, refDict):
        for k,v in refDict.items():
            if 'references' in v:
                r = v['references']
                # a foreign key is looking at our primary key
                if isinstance(v, dict):
                    tbl = r['table']
                    col = r['key']
                    if col == "id":
                        # primary key of referring tbl is fk
                        col = self.insp.get_primary_keys(tbl)[0]
                    referringKeys = {}
                    if 'value' in r:
                        vals = []
                        [vals.append(k) for k in r['value'].keys()]
                        referringKeys[tbl] = {col:vals}
                    else:
                        referringKeys[tbl] = {}
                    print referringKeys


    def getTableFromResourceId(self, uri):
        '''
        extract table name from (possibly non-URI) resource string
        '''
        if not uri:
            return None
        if self.mapping['idpattern'] in uri:
            uri = uri[len(self.mapping['idpattern']):]
        tName = uri.split(RSRC_DELIM)[0]
        if tName in self.parsedTables:
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
        for t in self.parsedTables:
            for k, v in t['colNames'].items():
                if k == pName:
                    return t['tableName']
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
