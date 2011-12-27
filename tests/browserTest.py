#:copyright: Copyright 2009-2010 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
import webbrowser, unittest
import multiprocessing, random, tempfile, os.path, subprocess
from vesper.utils import Uri
from vesper.backports import json
from vesper import app
from vesper.web.route import Route

logconfig = '''
[loggers]
keys=root,datarequest

[handlers]
keys=hand01

[formatters]
keys=form01

[logger_root]
level=INFO
handlers=hand01

[logger_datarequest]
level=DEBUG
handlers=hand01
propagate=0
qualname=datarequest

[handler_hand01]
class=StreamHandler
level=NOTSET
formatter=form01
args=(sys.stdout,)

[formatter_form01]
format=%(asctime)s %(levelname)s %(name)s %(message)s
datefmt=%d %b %H:%M:%S
'''

def startVesperInstance(port, queue):
    try:
        import coverage, sys, signal, atexit
        coverage.process_startup()        
        
        def safeterminate(num, frame):
            #coverage registers an atexit function
            #so have atexit functions called when terminating            
            atexit._run_exitfuncs() #for some reason sys.exit isn't calling this
            sys.exit()
        
        signal.signal(signal.SIGTERM, safeterminate)
    except ImportError:
        pass
        
    @app.Action
    def sendServerStartAction(kw, retVal):
        # print "startReplication callback!"
        queue.put('server ready')
    
    @Route('testresult')#, REQUEST_METHOD='POST')
    def handleTestresult(kw, retval):
        queue.put(json.loads(kw._postContent))
        kw._responseHeaders['Content-Type'] = 'application/json'
        return '"OK"'
    
    tmpdir = tempfile.gettempdir()
    print "creating vesper instance on port %d" % (port),'tmp at', tmpdir
    app.createApp(__name__, 'vesper.web.admin', port=port, storage_url="mem://", 
        static_path='browser', 
        actions = {'load-model':[sendServerStartAction]},
        template_path='browser/templates',
        mako_module_dir = os.path.join(tmpdir, 'browserTest_makomodules')
        ,logconfig=logconfig
    ).run()
    # blocks forever

def startServer():
    port = 5555 #random.randrange(5000,9999)
    queue = multiprocessing.Queue()        
    serverProcess = multiprocessing.Process(target=startVesperInstance, args=(port,queue))
    serverProcess.start()        
    return serverProcess, queue, port

def run():
    serverProcess, queue, port = startServer()
    queue.get(True, 5) #raise Queue.EMPTY if server isn't ready in 5 second 
    try:
        serverProcess.join() #block
    except:
        serverProcess.terminate()

class BrowserTestRunnerTest(unittest.TestCase):

    def testBrowserTests(self):
        serverProcess, queue, port = startServer()
        urls = ['static/binder_tests.html', 'static/db_tests.html', 'data_test.html']
        try: 
            queue.get(True, 5) #raise Queue.EMPTY if server isn't ready in 5 second 
            for name in urls:
                url = 'http://localhost:%d/%s' % (port, name)
                print 'running ', url
                pageTimeout = 20
                #to run with phantomjs:
                #export BROWSER="phantomjs browser/phantomjs.coffee %s"
                webbrowser.open('%s#%d' % (url, pageTimeout))
                testResults = queue.get(True, pageTimeout) #raise Queue.EMPTY if browser unittests haven't finished in 20 seconds
                print '%(total)d total, %(passed)d passed %(failed)d failed %(ignored)d ignored' % testResults
                self.assertEqual(testResults['passed'], testResults['total'])
        finally:
            if not keepRunnng:
                serverProcess.terminate()
            else:
                try:
                    serverProcess.join() #block
                except:
                    serverProcess.terminate()

keepRunnng = False
if __name__ == '__main__':
    import sys
    if '--run' in sys.argv:
        run()
        sys.exit()
    elif '--wait' in sys.argv:
        keepRunnng = True
        sys.argv.remove('--wait')
    unittest.main()
