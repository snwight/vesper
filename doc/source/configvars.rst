.. :copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
.. :license: Dual licenced under the GPL or Apache2 licences, see LICENSE.

configuration variables 
-----------------------

general configuration variables 
===============================

.. confval:: logconfig 

   A string that is either a Python log configuration or a path to the configuration file

   Default: ``logconfig=None``

.. confval:: exec_cmd_and_exit 

  If set to True, invoking the app will not start the web server -- it will just execute 
  any given command line arguments and exit. Equivalent to the ``--exit`` command line option.
  
  Default: False

.. confval:: work_dir

   Directory where temporary files and directories are created.

   Default: ``work_dir="vesper_work"``
   
.. confval:: stores

  A dictionary of stores, whose keys are the names of the store and values are dictionaries 
  containing the datastore configuration settings for that store.
  
  If more than one store is specified, a default store must be indicated, in one of two ways:
  either by naming the store "default" or by including a "default_store" setting in the stores' 
  dictionary of configuration settings.

  If `stores` is set, then other datastore configuration settings are ignored. 
  
  Example: ``stores = {'config': {'storage_path': 'config.json'}, 'data': {'storage_path': 'data.json', 'default_store': True}}``

.. confval:: storeDefaults

  A dictionary containing datastore configuration settings that are applied to each store defined in the configuration. 
  Any other datastore settings override the settings here.
  If the value of the settings is another dictionary of settings (e.g. ``model_options``) those dictionaries are merged recursively.
  
  Example ``storeDefaults = {'model_options': {'serializeOptions': {'pjson': 'omitEmbeddedIds': True}}}``

.. confval:: use_file_lock 

  If True `vesper.app` will use interprocess file lock when committing 
  a transaction. Alternately use_file_lock can be a reference to a class or factory
  function that conforms to the glock.LockFile interface.

  Default is False

  ``use_file_lock=True #enable``

.. confval:: file_lock_path

  The path name for the lock file. If `file_lock_path` is not set, a path name is generated 
  using the os's temp directory and a file name based on a hash of the `model_resource_uri` 
  (this is to ensure that any process opening the same datastore will share the same lock file).

  ``file_lock_path='./appinstance1.lock'``

datastore configuration variables 
=================================

These configuration variables can be specified either directly in the configuration 
(if configuring only one store) or in each store in the ``stores`` configuration. 
They can also appear in the ``storeDefaults`` dictionary. 

.. confval:: datastore_factory

  The class or factory function called to instantiate the application's primary data store
  It is passed as keyword arguments the dictionary of the variables contained in the config file
  note that this is a callable object which may need to be imported into the config file

  Default: ``vesper.DataStore.BasicDataStore``

.. confval:: model_uri

  The resource that represents the model this instance of the application is running.

  Default: 'http://' + socket.getfqdn() + '/'

  Example: ``model_uri='http://example.com/'``

.. confval:: storage_url 

  A pseudo-URL that describes the connection to the data store.
  Todo: document how this overrides storage_path and modelFactory

  Default: ``storage_url='mem:``

.. confval:: storage_path

    The location of the store. Usually a file path but the appropriate value depends on 'modelFactory'
    default is '' 
    storage_path = 'mywebsite.json'

.. confval:: transaction_log
 
    The path of the transaction log. The transaction log records in NTriples format a log 
    of the statements added and removed from the model along with comments on when and by whom. If the value is True, a file path is created by appending ".log.nt" to ``storeage_path``.

    default is False (no transaction log)
    
    Example: ``transaction_log='/logs/auditTrail.log.nt'``

.. confval:: storage_template

    A string that is used as the initial data when creating a new store  
    (which happens when the file specified by storage_path is not found)
    
    storage_template='''{
    "id": "1",
    "content": "hello world"
    }
    '''

.. confval:: storage_template_path

    Path to a file that will be used as the ``storage_template``. 

.. confval:: model_factory

    The class or factory function used by RxPathDomStore to load or create a new RDF document or database
    note that this is a callable object which may need to be imported into the config file
    
    default is RxPath.IncrementalNTriplesFileModel
    
    model_factory=RxPath.RedlandHashBdbModel

.. confval:: version_storage_path

    The location of a separate RDF model for storing the history of changes to the database.
    Usually a file path but the appropriate value depends on 'version_model_factory'
    
    default is '' (history not stored separately)
    
    version_storage_path = 'mywebsite.history.nt'

.. confval:: version_model_factory

    The class or factory function used by RxPathDomStore to load or create the version history RDF database
    #note that this is a callable object which may need to be imported into the config file
    
    default is whatever 'model_factory' is set to
    
    ``version_model_factory=RxPath.RedlandHashBdbModel``
    
.. confval:: save_history 

    Default: ``save_history = False``

.. confval:: storage_template_options
 
    Default: ``storage_template_options=None``

.. confval:: model_options 

    Default: ``model_options=None``

.. confval:: trunk_id 

    Default: ``trunk_id = '0A'``

.. confval:: branch_id 

    Default: ``branch_id = None``

.. confval:: replication_hosts 

    Default: ``replication_hosts = None``

.. confval:: replication_channel 

    Default: ``replication_channel = None``

.. confval:: send_stomp_ack

    Default: ``send_stomp_ack = True``

web configuration variables 
=================================
 
.. confval:: port 

    Default: ``port=8000``

.. confval:: httpserver 

  The Python class (or callable object) of the WSGI server that is instantiated
  when the app is started

  Default: ``httpserver=wsgiref.simple_server``

.. confval:: wsgi_middleware 

   A WSGI middleware Python class or callable object which, if specified, will be instantiated 
   with the Vesper WSGI app (wrapping it).

   Default: ``wsgi_middleware=None``
   
   Example: ``import firepython.middleware; wsgi_middleware = firepython.middleware.FirePythonWSGI``

.. confval:: record_requests 

  Any HTTP requests made are saved to a file. They can be played-back using the ``debug_filename``
  option.

.. confval:: debug_filename 

   If specified, the given file containing a history of requests recorded by ``record_requests``
   is played back before starting the server.

.. confval:: static_path

    A string or list specifying the directories that will be searched when resolving static URLs

    Default: the current working directory of the process running the app

    Example: ``static_path = 'static'``

.. confval:: template_path

    A string or list specifying the directories that will be searched when resolving Mako templates.

    Default: the current working directory of the process running the app

    Example: ``template_path = 'templates'``
  
.. confval:: default_page_name

    The name of the page to be invoke if the request URL doesn't include a path 
    e.g. http://www.example.org/ is equivalent to http://www.example.org/index 
    
    default is: 'index.html'
    
    `default_page_name='home.html'`

.. confval:: default_mime_type

    The MIME type sent on any request that doesn't set its own mimetype 
    and the app can't guess its MIME type
    default is '' (not set)
    default_mime_type='text/plain'

.. confval:: default_expires_in

    What to do about Expires HTTP response header if it 
    hasn't already set by the application. If it's value is 0 or None the header 
    will not be sent, otherwise the value is the number of seconds in the future 
    that responses should expire. To indicate that they already expired set it to -1;
    to indicate that they never expires set it to 31536000 (1 year).
    default is 3600 (1 hour)
    default_expires_in = 0 #disable setting the Expires header by default

.. confval:: use_etags 

    If True, If-None-Match request headers are honors and an etag based 
    on a MD5 hash of the response content will be set with every response
    
    default is True
    use_etags = False #disable

.. confval:: mako_module_dir

    Specifies the directory where the mako templates are compiled. 
    This property sets the `module_directory` parameter 
    in the `mako.lookup.TemplateLookup` constructor.
    
    Default is `"work_dir/mako_module"` where `work_dir`:confval: is that given configuration value.
    
.. confval:: template_options

  This setting is a dictionary that contains keyword arguments for the 
  `mako.lookup.TemplateLookup` constructor used when initializing the template engine.
  Keys in this dictionary override the default values for that parameter.
  
  Default is `{}`

.. confval:: secure_file_access

    Limits URLs access to only the directories reachable through `static_path` or `templates_path`

    default is True

    secure_file_access = True

configuration variables for command line handling 
=================================================

.. confval:: cmd_args

   List of arguments to be passed to 'run-cmds' action. By default this will be set with any command line argument 
   that wasn't applied to a built-in command option.

.. confval:: cmdline_handler

  A function called during command line processing at startup. It takes two arguments: the AppConfig object and the list of command arguments (excluding any arguments applied to built-in command options). Returns a list of arguments to be applied as config settings and set as `cmd_args` if `cmd_args` is not set.
      
  Default: ``lambda app, args: args``

.. confval:: cmd_usage

  Command usage string 
  
  Default: `"%prog [options] [settings]"`

.. confval:: cmd_help

  Help text appended to command line help message (invoked by -h or --help)
  
  Default: `"Settings:\n--[name]=VALUE Add [name] to config settings"`

advanced configuration variables 
================================

.. confval:: app_name

  A short name for this application, must be unique within the current ``vesper.app`` process

  Default: `"root"`
  
  Example: ``app_name = 'root'``
  
.. confval:: actions

      The dictionary that defines the Actions the app should use.
      The key is the name of the trigger and the value is a list of Actions that are invoked in that order. Vesper currently uses these triggers:
* 'http-request' is invoked by HTTPRequestProcessor.handleHTTPRequest
* 'load-model' is invoked on start-up after the app's stores have been initialized
* 'run-cmds' is invoked on start-up (after 'load-model') to handle command line arguements
* 'before-add' and 'before-remove' is invoked when data is added or removed from a store
* 'before-new' is invoked when a new resource is added to a store
* 'before-commit' is invoked at the end of a transaction but trigger still has a chance to modify it
* 'finalize-commit' is invoked after all transaction participants have successfully prepared to commit, one last chance to abort about the transaction
* 'after-commit' is invoked after a transaction has completed successfully 
* 'after-abort' is invoked after a transaction was aborted
* triggerName + '-error' is invoked when an exception is raised while processing a trigger (e.g. the `http-request-error` trigger is invoked if an error occurs while running the `http-request` trigger).

.. confval:: default_trigger 

      Used by Requestor objects as the trigger to use to invoke a request
      default is 'http-request'.
      
      Example: ``default_trigger='http-request'``

.. confval:: global_request_vars

      A list of request metadata variables that should be preserved 
      when invoking callActions() (e.g. to invoke templates or an error handler)
      default is [] (but `vesper.app`  will always adds the following: 
      '_name', '_noErrorHandling', '__current-transaction', and '__readOnly')

      Example: ``global_request_vars = [ '__account', '_static']``

.. confval:: get_principal_func

      A function that is called to retrieve the 
      application-specific Principal (in the security sense) object 
      for the current request context.
      It takes one argument that is the dictionary of metadata for the current request
      default: lambda kw: '' 
      get_principal_func = lambda kw: kw.get('__account','')

.. confval:: validate_external_request

      A function that is called when receiving an external request (e.g. an http request)
      It is called before invoking runActions(). Use it to make sure the request 
      doesn't contain metadata that could dangerously confuse request processing.
      Its signature looks like:
      ``def validate_external_request(kw)``
      where `kw` is the request metadata dictionary (which can be modified if necessary).
      It should raise vesper.app.NotAuthorized if the request should not be processed.
      
      default is lambda *args: True

.. confval:: action_cache_size

    Sets the maximum number of items to be stored in the Action cache. Set to 0 to disable.

    default is 0

    action_cache_size=1000
