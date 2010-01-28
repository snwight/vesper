"""
    Engine and helper classes for Raccoon

    Copyright (c) 2003-5 by Adam Souzis <asouzis@users.sf.net>
    All rights reserved, see COPYING for details.
    http://rx4rdf.sf.net    
"""
from rx import utils, glock, RxPath, MRUCache, DataStore, transactions, store
from rx.transaction_processor import TransactionProcessor
import os, time, sys, base64, mimetypes, types, traceback
import urllib, re

try:
    import cStringIO
    StringIO = cStringIO
except ImportError:
    import StringIO

import logging
DEFAULT_LOGLEVEL = logging.INFO

#logging.BASIC_FORMAT = "%(asctime)s %(levelname)s %(name)s:%(message)s"
#logging.root.setLevel(DEFAULT_LOGLEVEL)
#logging.basicConfig()

log = logging.getLogger("raccoon")
_defexception = utils.DynaExceptionFactory(__name__)

_defexception('CmdArgError')
_defexception('RaccoonError')
_defexception('unusable namespace error')
_defexception('not authorized')

class DoNotHandleException(Exception):
    '''
    RequestProcessor.doActions() will not invoke error handler actions on
    exceptions derived from this class.
    '''

class ActionWrapperException(utils.NestedException):
    def __init__(self):
        return utils.NestedException.__init__(self,useNested=True)

############################################################
##Raccoon defaults
############################################################

DefaultNsMap = { 'owl': 'http://www.w3.org/2002/07/owl#',
           'rdf' : 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'rdfs' : 'http://www.w3.org/2000/01/rdf-schema#',
           'bnode': RxPath.BNODE_BASE,
        }

############################################################
##Helper classes and functions
############################################################    
class Requestor(object):
    '''
    Requestor is a helper class that allows python code to invoke a
    Raccoon request as if it was function call

    Usage:
    response = __requestor__.requestname(**kw)
    where kw is the optional request parameters

    An AttributeError exception is raised if the server does not
    recognize the request
    '''
    def __init__(self, server, triggerName = None):
        self.server = server
        self.triggerName = triggerName

    #the trailing __ so you can have requests named 'invoke' without conflicting
    def invoke__(self, name, **kw):
        return self.invokeEx__(name, kw)[0]

    def invokeEx__(self, name, kwargs):
        kw = self.server.requestContext[-1].copy()
        kw.update(kwargs)#overrides request context kw

        kw['_name']=name
        if not kw.has_key('_path'):
            kw['_path'] = name
        #print 'invoke', kw
        #defaultTriggerName let's us have different trigger type per thread
        #allowing site:/// urls to rely on the defaultTriggerName
        triggerName = self.triggerName or self.server.defaultRequestTrigger
        result = self.server.runActions(triggerName, kw, newTransaction=False)
        if result is not None: #'cause '' is OK
            return (result, kw)
        else:
            raise AttributeError, name

    def __getattr__(self, name):
        if name in ['__hash__','__nonzero__', '__cmp__', '__del__']:
            #undefined but reserved attribute names
            raise AttributeError("'Requestor' object has no attribute '%s'" %name)
        return lambda **k: self.invoke__(name, **k)
        #else:raise AttributeError, name #we can't do this yet since
        #we may need the parameters to figure out what to invoke (like a multimethod)

def defaultActionCacheKeyPredicateFactory(action, cacheKeyPredicate):
    '''
    Returns a predicate to calculate a key for the action
    based on a given request.
    This function gives an action a chance to
    customize the cacheKeyPredicate for the particulars of the
    action instance. At the very least it should bind the action
    instance with the cacheKeyPredicate to disambiguate keys from
    different actions.
    '''
    actionid = id(action) #do this to avoid memory leaks
    return lambda kw, retVal: (actionid, cacheKeyPredicate(kw, retVal))

def notCacheableKeyPredicate(*args, **kw):
    raise MRUCache.NotCacheable

def defaultActionValueCacheableCalc(hkey, value, kw, retResult):
    if value is retResult:
        #when the result hasn't changed, store NotModified in the cache
        #instead of the result. This way the retVal won't need to be part
        #of the cache key
        return Action.NotModified
    else:
        return value

class Action(object):
    '''
The Action class encapsulates a step in the request processing pipeline.

An Action has two parts, one or more match expressions and an action
function that is invoked if the request metadata matches one of the
match expressions. The action function returns a value which is passed
onto the next Action in the sequence.
    '''
    NotModified = ('notmodified',)

    def __init__(self, action,
            cachePredicate=notCacheableKeyPredicate,
            sideEffectsPredicate=None, sideEffectsFunc=None,
            isValueCacheableCalc=defaultActionValueCacheableCalc,
            cachePredicateFactory=defaultActionCacheKeyPredicateFactory,
            debug=False):
        '''
action must be a function with this signature:    
def action(kw, retVal) where:
kw is the dictionary of metadata associated with the request
retVal was the return value of the last action invoked in the in action sequence or None
'''        
        self.action = action
        self.cacheKeyPredicate = cachePredicateFactory(self, cachePredicate)
        self.cachePredicateFactory = cachePredicateFactory
        self.sideEffectsPredicate = sideEffectsPredicate
        self.sideEffectsFunc = sideEffectsFunc
        self.isValueCacheableCalc = isValueCacheableCalc
        self.debug = debug

    def __call__(self, kw, retVal):
        return self.action(kw, retVal)

class Result(object):
    def __init__(self, retVal):
        self.value = retVal

    @property
    def asBytes(self):
        value = self.value
        if isinstance(value, unicode):
            return value.decode('utf8')
        elif hasattr(value, 'read'):
            self.value = value.read()
        return str(self.value)

    @property
    def asUnicode(self):
        if hasattr(self.value, 'read'):
            self.value = value.read()
        if isinstance(self.value, str):
            return self.value.encode('utf8')
        elif isinstance(self.value, unicode):
            return self.value
        else:
            return unicode(self.value)

def assignVars(self, kw, varlist, default):
    '''
    Helper function for assigning variables from the config file.
    '''
    import copy
    for name in varlist:
        try:
            defaultValue = copy.copy(default)
        except TypeError:
            #probably ok, can't copy certain non-mutable objects like functions
            defaultValue = default
        value = kw.get(name, defaultValue)
        if default is not None and not isinstance(value, type(default)):
            raise RaccoonError('config variable %s (of type %s)'
                               'must be compatible with type %s'
                               % (name, type(value), type(default)))
        setattr(self, name, value)

############################################################
##Raccoon main class
############################################################
class RequestProcessor(TransactionProcessor):
    DEFAULT_CONFIG_PATH = ''#'raccoon-default-config.py'

    requestsRecord = None

    defaultGlobalVars = ['_name', '_noErrorHandling',
            '__current-transaction', '__readOnly'
            '__requestor__', '__server__',
            '_prevkw', '__argv__', '_errorInfo'
            ]

    def __init__(self,
                 #correspond to equivalentl command line args:
                 a=None, m=None, p=None, argsForConfig=None,
                 #correspond to equivalently named config settings
                 appBase='/', model_uri=None, appName='',
                 #dictionary of config settings, overrides the config
                 appVars=None):
                 
        # XXX copy and paste from
        self.initThreadLocals(requestContext=None, inErrorHandler=0, previousResolvers=None)
        self.BASE_MODEL_URI = model_uri
        self.requestContext = [{}] #stack of dicts
        self.lock = None
        self.log = log
        #######################
        
        #variables you want made available to anyone during this request
        configpath = a or self.DEFAULT_CONFIG_PATH
        self.source = m
        self.PATH = p or os.environ.get('RACCOONPATH',os.getcwd())
        #use first directory on the PATH as the base for relative paths
        #unless this was specifically set it will be the current dir
        self.baseDir = self.PATH.split(os.pathsep)[0]
        self.appBase = appBase or '/'
        self.appName = appName
        # self.cmd_usage = DEFAULT_cmd_usage XXXX
        self.cmd_usage = ""
        self.loadConfig(configpath, argsForConfig, appVars)
        if self.template_path:
            from mako.lookup import TemplateLookup
            self.template_loader = TemplateLookup(directories=self.template_path, 
                default_filters=['decode.utf8'], module_directory='mako_modules',
                output_encoding='utf-8', encoding_errors='replace')
        self.requestDispatcher = Requestor(self)
        #self.resolver = SiteUriResolver(self)
        self.loadModel()
        self.handleCommandLine(argsForConfig or [])

    def handleCommandLine(self, argv):
        '''  the command line is translated into XPath variables
        as follows:

        * arguments beginning with a '-' are treated as a variable
        name with its value being the next argument unless that
        argument also starts with a '-'

        * the entire command line is assigned to the variable '_cmdline'
        '''
        kw = argsToKw(argv, self.cmd_usage)
        kw['_cmdline'] = '"' + '" "'.join(argv) + '"'
        self.runActions('run-cmds', kw)

    # XXX this method should use AppConfig/loadApp etc. methods
    def loadConfig(self, path, argsForConfig=None, appVars=None):
        if not path and not appVars:
            #todo: path = self.DEFAULT_CONFIG_PATH (e.g. server-config.py)
            raise CmdArgError('you must specify a config file using -a')
        if path and not os.path.exists(path):
            raise CmdArgError('%s not found' % path)

        if not self.BASE_MODEL_URI:
            import socket
            self.BASE_MODEL_URI= 'http://' + socket.getfqdn() + '/'

        kw = dict(Action=Action)
        
        if path:
            def includeConfig(path):
                 kw['__configpath__'].append(os.path.abspath(path))
                 execfile(path, globals(), kw)
                 kw['__configpath__'].pop()

            kw['__server__'] = self
            kw['__argv__'] = argsForConfig or []
            kw['__include__'] = includeConfig # XX appears unused?
            kw['__configpath__'] = [os.path.abspath(path)]
            execfile(path, kw)

        if appVars:
            kw.update(appVars)        
        self.config = utils.defaultattrdict(appVars or {})

        if kw.get('beforeConfigHook'):
            kw['beforeConfigHook'](kw)

        def initConstants(varlist, default):
            return assignVars(self, kw, varlist, default)

        initConstants( [ 'nsMap', 'extFunctions', 'actions',
                         'authorizationDigests',
                         'NOT_CACHEABLE_FUNCTIONS', ], {} )
        initConstants( ['DEFAULT_MIME_TYPE'], '')

        initConstants( ['appBase'], self.appBase)
        assert self.appBase[0] == '/', "appBase must start with a '/'"
        initConstants( ['BASE_MODEL_URI'], self.BASE_MODEL_URI)
        initConstants( ['appName'], self.appName)
        #appName is a unique name for this request processor instance
        if not self.appName:
            self.appName = re.sub(r'\W','_', self.BASE_MODEL_URI)
        self.log = logging.getLogger("raccoon." + self.appName)

        useFileLock = kw.get('useFileLock')
        if useFileLock:
            if isinstance(useFileLock, type):
                self.LockFile = useFileLock
            else:
                self.LockFile = glock.LockFile
        else:
            self.LockFile = glock.NullLockFile #the default
        
        self.loadDataStore(kw)
        
        if 'before-new' in self.actions:            
            #newResourceHook is optional since it's expensive
            self.dataStore.newResourceTrigger = self.txnSvc.newResourceHook
        
        self.defaultRequestTrigger = kw.get('DEFAULT_TRIGGER','http-request')
        initConstants( ['globalRequestVars', 'static_path', 'template_path'], [])
        self.globalRequestVars.extend( self.defaultGlobalVars )
        self.defaultPageName = kw.get('defaultPageName', 'index')
        #cache settings:
        initConstants( ['LIVE_ENVIRONMENT', 'SECURE_FILE_ACCESS', 'useEtags'], 1)
        self.defaultExpiresIn = kw.get('defaultExpiresIn', 0)
        initConstants( ['ACTION_CACHE_SIZE'], 1000)
        #disable by default(default cache size used to be 10000000 (~10mb))
        initConstants( ['maxCacheableStream','FILE_CACHE_SIZE'], 0)

        self.PATH = kw.get('PATH', self.PATH)
        
        self.authorizeMetadata = kw.get('authorizeMetadata',
                                        lambda *args: True)
        self.validateExternalRequest = kw.get('validateExternalRequest',
                                        lambda *args: True)
        self.getPrincipleFunc = kw.get('getPrincipleFunc', lambda kw: '')

        self.MODEL_RESOURCE_URI = kw.get('MODEL_RESOURCE_URI',
                                         self.BASE_MODEL_URI)

        # XXX
        # self.cmd_usage = DEFAULT_cmd_usage + kw.get('cmd_usage', '')

        self.nsMap.update(DefaultNsMap)
        
        if kw.get('configHook'):
            kw['configHook'](kw)

    def loadModel(self):
        self.actionCache = MRUCache.MRUCache(self.ACTION_CACHE_SIZE,
                                             digestKey=True)
        super(RequestProcessor, self).loadModel()
        self.runActions('load-model')

###########################################
## request handling engine
###########################################

    def runActions(self, triggerName, kw = None, initVal=None, newTransaction=True):
        '''
        Retrieve the action sequences associated with the triggerName.
        Each Action has a list of RxPath expressions that are evaluated after
        mapping runActions keyword parameters to RxPath variables.  If an
        expression returns a non-empty nodeset the Action is invoked and the
        value it returns is passed to the next invoked Action until the end of
        the sequence, upon which the final return value is return by this function.
        '''
        kw = utils.attrdict(kw or {})
        sequence = self.actions.get(triggerName)
        if sequence:
            errorSequence = self.actions.get(triggerName+'-error')
            return self.doActions(sequence, kw, retVal=initVal,
                errorSequence=errorSequence, newTransaction=newTransaction)

    def _doActionsBare(self, sequence, kw, retVal):
        try:
            if not isinstance(sequence, (list, tuple)):
                sequence = sequence(kw)

            for action in sequence:
                retResult = Result(retVal)
                #try to retrieve action result from cache
                #when an action is not cachable (the default)
                #just calls the action
                newRetVal = self.actionCache.getOrCalcValue(
                    action, kw, retResult,
                    hashCalc=action.cacheKeyPredicate,
                    sideEffectsCalc=action.sideEffectsPredicate,
                    sideEffectsFunc=action.sideEffectsFunc,
                    isValueCacheableCalc=action.isValueCacheableCalc)

                if (newRetVal is not retResult
                        and newRetVal is not Action.NotModified):
                    retVal = newRetVal
        except:
            exc = ActionWrapperException()
            exc.state = retVal
            raise exc
        return retVal

    def _doActionsTxn(self, sequence, kw, retVal):
        func = lambda: self._doActionsBare(sequence, kw, retVal)
        return self.executeTransaction(func, kw, retVal)

    def doActions(self, sequence, kw=None, retVal=None,
                  errorSequence=None, newTransaction=False):
        if kw is None: 
            kw = utils.attrdict()

        kw['__requestor__'] = self.requestDispatcher
        kw['__server__'] = self

        try:
            if newTransaction:
                retVal = self._doActionsTxn(sequence, kw, retVal)
            else:
                retVal = self._doActionsBare(sequence, kw, retVal)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            #print newTransaction, self.txnSvc.state.timestamp
            exc_info = sys.exc_info()
            if isinstance(exc_info[1], ActionWrapperException):
                retVal = exc_info[1].state
                exc_info = exc_info[1].nested_exc_info

            if self.inErrorHandler or kw.get('_noErrorHandling'):
                #avoid endless loops
                raise exc_info[1] or exc_info[0], None, exc_info[2]
            else:
                self.inErrorHandler += 1
            try:
                if isinstance(exc_info[1], DoNotHandleException):
                    raise exc_info[1] or exc_info[0], None, exc_info[2]

                if errorSequence and sequence is not errorSequence:
                    import traceback as traceback_module
                    def extractErrorInfo(type, value):
                        #value may be either the nested exception
                        #or the wrapper exception
                        message = str(value)
                        module = '.'.join( str(type).split('.')[:-1] )
                        name = str(type).split('.')[-1].strip("'>")
                        errorCode = getattr(value, 'errorCode', '')
                        return message, module, name, errorCode

                    def getErrorKWs():
                        type, value, traceback = exc_info
                        if (isinstance(value, utils.NestedException)
                                and value.useNested):
                            message, module, name, errorCode=extractErrorInfo(
                                 value.nested_exc_info[0],
                                 value.nested_exc_info[1])
                        else:
                            message, module, name, errorCode=extractErrorInfo(
                                                             type, value)
                        #these should always be the wrapper exception:
                        (fileName, lineNumber, functionName,
                            text) = traceback_module.extract_tb(
                                                    traceback, 1)[0]
                        details = ''.join(
                            traceback_module.format_exception(
                                        type, value, traceback) )
                        return utils.attrdict(locals())

                    kw['_errorInfo'] = getErrorKWs()
                    self.log.warning("invoking error handler on exception:\n"+
                                     kw['_errorInfo']['details'])
                    try:
                        #if we're creating a new transaction,
                        #it has been aborted by now, so start a new one
                        #however if the error was thrown during commit we're in the midst 
                        #of a bad transaction and its not safe to create a new one
                        newTransaction = newTransaction and not self.txnSvc.isActive()
                        return self.callActions(errorSequence, kw, retVal,
                            newTransaction=newTransaction)
                    finally:
                        del kw['_errorInfo']
                else:
                    #traceback.print_exception(*exc_info)
                    raise exc_info[1] or exc_info[0], None, exc_info[2]
            finally:
                self.inErrorHandler -= 1
        return retVal

    def callActions(self, actions, kw, retVal, errorSequence=None, globalVars=None, newTransaction=False):
        '''
        process another set of actions using the current context as input,
        but without modified the current context.
        Particularly useful for template processing.
        '''
        globalVars = self.globalRequestVars + (globalVars or [])

        #merge previous prevkw, overriding vars as necessary
        prevkw = kw.get('_prevkw', {}).copy()
        templatekw = utils.attrdict()
        for k, v in kw.items():
            #initialize the templates variable map copying the
            #core request kws and copy the r est (the application
            #specific kws) to _prevkw this way the template
            #processing doesn't mix with the orginal request but
            #are made available in the 'previous' namespace (think
            #of them as template parameters)
            if k in globalVars:
                templatekw[k] = v
            elif k != '_metadatachanges':
                prevkw[k] = v
        templatekw['_prevkw'] = prevkw
        templatekw['_contents'] = Result(retVal)

        return self.doActions(actions, templatekw,
            errorSequence=errorSequence, newTransaction=newTransaction)


#################################################
##command line handling
#################################################
def argsToKw(argv, cmd_usage):
    kw = { }

    i = iter(argv)
    try:
        arg = i.next()
        while 1:
            if arg[0] != '-':
                raise CmdArgError('missing arg')
            name = arg.lstrip('-')
            kw[name] = True
            arg = i.next()
            if arg[0] != '-':
                kw[name] = arg
                arg = i.next()
    except StopIteration: pass
    #print 'args', kw
    return kw

def translateCmdArgs(data):
    """
    translate raccoonrunner vars into shell args suitable for RequestProcessor init
    """
    replacements = [("CONFIG_PATH", "a"), ("SOURCE", "m"), ("RACCOON_PATH", "p"),
                    ("APP_BASE", "appBase"), ("APP_NAME", "appName"), ("MODEL_URI", "model_uri")]
    for x in replacements:
      if x[0] in data:
        data[x[1]] = data[x[0]]
        del data[x[0]]
    return data

def initLogConfig(logConfig):
    import logging.config as log_config
    if isinstance(logConfig,(str,unicode)) and logConfig.lstrip()[:1] in ';#[':
        #looks like a logging configuration 
        import textwrap
        logConfig = StringIO.StringIO(textwrap.dedent(logConfig))
    log_config.fileConfig(logConfig)
    #any logger already created and not explicitly
    #specified in the log config file is disabled this
    #seems like a bad design -- certainly took me a while
    #to why understand things weren't getting logged so
    #re-enable the loggers
    for logger in logging.Logger.manager.loggerDict.itervalues():
        logger.disabled = 0
    
class AppConfig(utils.attrdict):
    _server = None
    
    def load(self):
        if self.get('STORAGE_URL'):        
            (proto, path) = self['STORAGE_URL'].split('://')

            self['modelFactory'] = store.get_factory(proto)
            self['STORAGE_PATH'] = path
        #XXX if modelFactory is set should override STORAGE_URL
        if self.get('logconfig'):
            initLogConfig(self['logconfig'])

        kw = translateCmdArgs(self)
        from web import HTTPRequestProcessor
        root = HTTPRequestProcessor(a=kw.get('a'), appName='root', appVars=kw)
        dict.__setattr__(self, '_server', root)
        return self._server
        
    def run(self, startserver=True, out=sys.stdout):
        root = self._server
        if not root:
            root = self.load()

        if 'DEBUG_FILENAME' in self:
            self.playbackRequestHistory(self['DEBUG_FILENAME'], out)

        if self.get('RECORD_REQUESTS'):
            root.requestsRecord = []
            root.requestRecordPath = 'debug-wiki.pkl'

        if not self.get('EXEC_CMD_AND_EXIT', not startserver):
            port = self.get('PORT', 8000)
            if self.get('firepython_enabled'):
                import firepython.middleware
                middleware = firepython.middleware.FirePythonWSGI
            else:
                middleware = None
            httpserver = self.get('httpserver')
            print>>out, "Starting HTTP on port %d..." % port
            #runs forever:
            root.runWsgiServer(port, httpserver, middleware)

        return root

def createStore(json='', storageURL = 'mem://', idGenerator='counter', **kw):
    #XXX very confusing that storageURL spelling doesn't match STORAGE_URL 
    root = createApp(
        STORAGE_URL = storageURL,
        STORAGE_TEMPLATE = json,
        storageTemplateOptions = dict(generateBnode=idGenerator),
        **kw    
    ).run(False)
    return root.dataStore

_current_config = AppConfig()
_current_configpath = [None]

def _normpath(basedir, path):
    return [os.path.isabs(dir) and dir or os.path.normpath(
                        os.path.join(basedir, dir)) for dir in path]

def _importApp(baseapp):
    '''
    Executes the given config file and returns a Python module-like object that contains the global variables defined by it.
    If `createApp()` was called during execution, it have an attribute called `_app_config` set to the app configuration returned by `createApp()`.
    '''
    baseglobals = utils.attrdict(Action=Action, createApp=createApp)
    #set this global so we can resolve relative paths against the location
    #of the config file they appear in
    _current_configpath.append( os.path.dirname(os.path.abspath(baseapp)) )
    #assuming the baseapp file calls createApp(), it will set _current_config
    execfile(baseapp, baseglobals)
    _current_configpath.pop()
    baseglobals._app_config = _current_config
    return baseglobals

def createApp(fromFile=None, **config):
    """
    Return an 'AppConfig' initialized with keyword arguments.  If a path to a 
    file containing config variables is given, they will be merged with the
    config args
    """
    global _current_config
    _current_config = AppConfig()
    _current_config.update(config)
    if fromFile:
        execfile(fromFile, _current_config, _current_config)
    return _current_config

def loadApp(baseapp, static_path=(), template_path=(), actions=None, **config):
    '''
    Return an 'AppConfig' by loading an existing app (a file with a call to createApp)
    
    'baseapp' is a path to the file of the app to load
    'static_path', 'template_path', and 'actions' are appended to the variables
    of the same name defined in the base app
    
    Any other keyword arguments will override values in the base app
    '''
    global _current_config
    
    assert isinstance(baseapp, (str, unicode))
    baseapp = _importApp(baseapp)
    _current_config = baseapp._app_config
    
    #config variables that shouldn't be simply overwritten should be specified 
    #as an explicit function args
    _current_config.update(config)
    
    if 'actions' in _current_config:
        if actions:
            _current_config.actions.update(actions)
    else:
        _current_config.actions = actions or {}
    
    basedir = _current_configpath[-1] or os.getcwd()
    
    if isinstance(static_path, (str, unicode)):
        static_path = [static_path]     
    static_path = list(static_path) + _current_config.get('static_path',[])
    _current_config.static_path = _normpath(basedir, static_path)

    if isinstance(template_path, (str, unicode)):
        template_path = [template_path]    
    template_path = list(template_path) + _current_config.get('template_path',[])
    _current_config.template_path = _normpath(basedir, template_path)
    
    _current_config.load()
    return _current_config
