page = new WebPage()

runPage = (url, callback = null) ->
    page = new WebPage()
    
    page.onConsoleMessage = (msg) ->
        console.log "phantomjs: #{msg}"
        if msg.indexOf("test run complete") > -1
            phantom.exit()
    
    page.open url, (status) ->
       if status isnt 'success'
           console.log "phantomjs: Unable to load '#{url}'"
           phantom.exit(-1)
       if callback
           callback url, status

urls = [ phantom.args[0] ]
urls.forEach (url, pos, a) ->
    [url, timeout]= url.split("#")
    console.log "phantomjs: running #{url} timeout: #{timeout}"
    runPage url, (url, status) ->
        if timeout 
            window.setTimeout (-> phantom.exit(-1)), timeout*1000