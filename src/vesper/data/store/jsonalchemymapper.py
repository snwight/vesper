import json
from sqlalchemy import engine
from sqlalchemy.engine import reflection

RSRC_DELIM='/'
VAL_OPEN_DELIM='{'
VAL_CLOSE_DELIM='}'

class JsonAlchemyMapper():

    def __init__(self, mapping=None, engine=None):

        if engine is None:
            print "JsonAlchemyMapper requires a valid alchemy engine parameter"
            exit
            
        # create our SqlAlchemy 'fine-grained schema inspector'
        self.insp = reflection.Inspector.from_engine(engine)
      
        # is a JSON mapping is supplied, use it otherwise infer our best guess at one
        self.mapping = {}
        if mapping:
            self.mapping = mapping
        else:
            self._generateMapFromSchema()

        # create a compact list of relevant table info for efficient internal use
        self._getColumnsOfInterest()

        # output readable json map and sql schema as diagnostic
        self._printSchemata()
        
        # divine our resource format strings here, now
        self.baseUri = ""
        if 'idpattern' in self.mapping:
            self.baseUri = self.mapping['idpattern']


    def _printSchemata(self):        
        print '===BEGIN SCHEMA INFO==========================================================='
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
                print '\t\t', i['name'], 'UNIQUE' if 'unique' in i else '', 'ON', \
                    [ic.encode('ascii') for ic in i['column_names']]
            for fk in self.insp.get_foreign_keys(tbl):
                print '\tFOREIGN KEY:'
                print '\t\t', [cc.encode('ascii') for cc in fk['constrained_columns']], 'ON', \
                    fk['referred_table'], [rc.encode('ascii') for rc in fk['referred_columns']]
        for vw in self.insp.get_view_names():
            print 'VIEW:', vw
            q = self.insp.get_view_definition(vw)
            for c in self.insp.get_columns(vw):
                print '\t', c['name'], c['type']
            print 'VIEW DEFINITION:'
            print '\t', q
        print '===END SCHEMA INFO============================================================='
        print '===BEGIN JSON MAP=============================================================='
        print json.dumps(self.mapping, sort_keys=True, indent=4)
        print '===END JSON MAP================================================================'


    def _parsePropertyDict(self, prop, i=0):
        '''
        walk through possibly nested dictionary of properties, leave a popcorn trail
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
                # we have a json property name for a column - store both for lookup
                pass       #  colNames[k] = v


    def _getColumnsOfInterest(self):
        '''
        using our json mapping object (possibly derived by we ourselves based on inspection of the 
        active backend schema), collate and store basic information about the tables that we are to
        use into a list of simple dicts: 
        {table name, primary key column name, [list of all relevant columns]}.
        this allows us to ignore all other backend tables, and even unused columns in active tables 
        that we are using
        '''
        self.parsedTables = []
        for tableName in self.insp.get_table_names():
            # if our private secret table is passed in here, politely decline to parse it 
            if tableName == 'vesper_stmts':
                continue
            # use json map and sql schema to determine columns of interest, make a list
            readonly = False
            pKeyName = None
            tableDesc = self.mapping["tables"][tableName]
            if 'id' in tableDesc:
                pKeyName = tableDesc['id']
            if 'readonly' in tableDesc:
                # this table is in fact an immutable SQL view - query but don't update
                readonly = tableDesc['readonly']
            if 'relationship' in tableDesc:
                # under our regime, 'correlation' tables are pointed TO in json map from foreign key subjects,
                # and updated when subjects are modified, so we don't need them in canonical storage
                pass
            if 'properties' in tableDesc:
                colNames = {}
                for prop in tableDesc['properties']:
                    if isinstance(prop, str):
                        if prop == "*":
                            for c in self.insp.get_columns(tableName):
                                if c['name'] not in self.insp.get_primary_keys(tableName):
                                    colNames[c['name']] = c['name']
                        else:
                            if prop not in colNames.keys() and prop not in self.insp.get_primary_keys(tableName):
                                colNames[prop] = prop
                    if isinstance(prop, dict):
                        self._parsePropertyDict(prop)
            self.parsedTables.append({'tableName':tableName, 'pKeyName':pKeyName, 'readOnly':readonly, 'colNames':colNames})


    def getColNameFromPredicate(self, tableName, predicate):
        pName = self.getPropNameFromResourceId(predicate)
        for t in self.parsedTables:
            if t['tableName'] == tableName:
                for k, v in t['colNames'].items():
                    if k == pName:
                        return v
        return pName


    def getTableFromResourceId(self, uri):
        '''
        extract table name from (possibly non-URI) resource string
        '''
        if not uri:
            return None
        if self.baseUri in uri:
            uri = uri[len(self.baseUri):]
        tName = uri.split(RSRC_DELIM)[0]
        if tName in self.parsedTables:
            return tName
        return None

    
    def getPrimaryKeyName(self, tableName):
        for td in self.parsedTables:
            if td['tableName'] == tableName:
                return td['pKeyName']
        return None


    def getPropNameFromResourceId(self, uri):
        '''
        generic tool to extract property name from (possibly non-URI) resource string
        '''
        if not uri:
            return None
        pName = uri
        if self.baseUri in uri:
            uri = uri[len(self.baseUri):]
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
        val = uri
        if self.baseUri in uri:
            uri = uri[len(self.baseUri):]
        if VAL_OPEN_DELIM in uri:
            val = uri.split(VAL_OPEN_DELIM)[1].rstrip(VAL_CLOSE_DELIM)
        else:
            val = None                
        return val


    def getTableWithProperty(self, uri):
        '''
        search our db for tables w/columns matching property name
        '''
        if uri:
            pName = self._getPropNameFromResourceId(uri)
            for t in self.parsedTables:
                for k, v in t['colNames'].items():
                    if k == pName:
                        return t['tableName']
        return None


    def _generateMapFromSchema(self):
        # generate our best guess at a JSON-SQL mapping based entirely on SQL schema inspection
        mDict = {
            "tablesprop": "HYPEtype",
            "idpattern": "http://souzis.com/",
            "tables": { }
            }
        for tbl in self.insp.get_table_names():
            if tbl == 'vesper_stmts':
                continue
            mDict['tables'][tbl] = {'properties': ['*']}
            if self.insp.get_primary_keys(tbl):
                mDict['tables'][tbl]['id'] = self.insp.get_primary_keys(tbl)[0]
            for fk in self.insp.get_foreign_keys(tbl):
                for cc in fk['constrained_columns']:
                    (mDict['tables'][tbl]['properties']).append({
                            cc.encode('ascii') : {
                                'key': fk['referred_columns'][0].encode('ascii'),
                                'references' : fk['referred_table']}})
        for vw in self.insp.get_view_names():
            mDict['tables'][vw] = {'properties': ['*'], 'readonly': 'true'}
        if mDict['tables']:
            self.mapping = mDict
