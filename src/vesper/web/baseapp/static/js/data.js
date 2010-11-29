/*
 * Copyright 2009-2010 by the Vesper team, see AUTHORS.
 * Dual licenced under the GPL or Apache2 licences, see LICENSE.
 */
if (!window.console) {
    var konsole = {
        log : function() {}
    };
} else {
    var konsole = window.console;
}

var Txn = function() {
  this.autocommit = false;
  this.requests = [];
  this.txnId = ++Txn.prototype.idcounter;
  //this.pendingChanges = {};
  //this.successmsg = '';
  //this.errormsg = '';
}

Txn.prototype = {
    
    idcounter : 0, 
    
    url : '/datarequest',
    
    execute : function(action, data, callback, elem) {
        var requestId = Math.random(); 
        //XXX allow requestid specified to replace a previous request

        //XXX if a new request updates an object in a previous request
        //    update the previous object
        //different requests but the same id
        /*        
        if (typeof data.id != 'undefined') {
             if (this.pendingChanges[data.id]) {
                if (action == 'update') {
                    $.extend(this.pendingChanges[data.id],data);
                } else if (action == 'add') {
                    throw new Error('already added');
                }
            }    
            this.pendingChanges[data.id] = data;
        }
        */
        
        //JSON-RPC 2.0 see http://groups.google.com/group/json-rpc/web/json-rpc-2-0
        this.requests.push( {
            jsonrpc : '2.0',
            method : action, 
            params : data, 
            id : requestId
        });
                
        if (callback) {
            //note: if elem is undefined will bind on doc            
            $(elem).one('dbdata.'+this.txnId, function(event) {
                //the response is a list and jquery turns that into arguments  
                konsole.log('thiscallback', arguments);
                var responses = arguments; //note: first item is the event
                for (var i=1; i < responses.length; i++) {
                    var response = responses[i];
                    if (response.id == null && response.error) {
                        callback(response);
                        break
                    } else if (response.id == requestId) {
                        if (response.error)
                            callback.call(elem, response);
                        else
                            callback.call(elem, response.result);
                    }
                }
            });
        }
    
        if (this.autocommit) 
            this.commit();

        return requestId;
   },

    /* 
    */
    commit : function(callback, elem) {
        var txnId = this.txnId;
        if (callback) { //callback signature: function(event, *responses)
            //note: if elem is undefined will bind on doc            
            $(elem).one('dbdata.'+txnId, callback);
        }        
        
        //var clientErrorMsg = this.clientErrorMsg;
        function ajaxCallback(data, textStatus) {
            //responses should be a list of successful responses
            //if any request failed it should be an http-level error
            //console.log('saved!', data, textStatus, 'dbdata.'+txnId);
            if (textStatus == 'success') {
                $(elem).trigger('dbdata.'+txnId, data);
                $(elem).trigger('dbdata-*', data);
            } else {
                //when textStatus != 'success', data param will be a XMLHttpRequest obj                
                var errorObj = {"jsonrpc": "2.0", "id": null,
                  "error": {"code": -32000, 
                        "message": data.statusText || textStatus,
                        'data' : data.responseText
                  } 
                };
                $(elem).trigger('dbdata.'+txnId, errorObj);
                $(elem).trigger('dbdata-*', errorObj);
            }
         };
    
        /*
        //XXX consolidate updates to the same object 
        var changes = [];
        for (var name in pendingChanges ) {
            changes.push( pendingChanges[name] );
        }   
        this.pendingChanges = {}; 
        */
        //XXX path should be configurable
        konsole.log('requests', this.requests);
        if (this.requests.length) {
            var requests = JSON.stringify(this.requests);
            this.requests = [];
            $.ajax({
              type: 'POST',
              url: this.url,
              data: requests,
              processData: false, 
              contentType: 'application/json',
              success: ajaxCallback,
              error: ajaxCallback,
              dataType: "json"
            });
        }
  }
};

/*
Add a jquery plugin that adds methods for serializing forms and HTML5-style microdata annotated elements.

//This example saves immediately, and invokes a callback on successs on each element.
$('.elem').dbUpdate(function(data) { $(this); });

//This example atomically commits multiple changes
$('.elems').dbBegin().dbUpdate().dbAdd({ id : '@this', anotherprop : 1}).dbCommit();

var txn = new Txn()
$(.objs).dbUpdate(txn);
$(.objs).dbUpdate(txn, function(data){ $(this); });
txn.commit();
*/

(function($) {
    $.fn.extend({    
      _executeTxn : function() {
         //copy to make real Array
         var args = Array.prototype.slice.call( arguments );         
         var action = args.shift();
         var commitNow = false;
         if (args[0] && args[0] instanceof Txn) {   
              var txn = args.shift();              
         } else {
            var txn = this.data('currentTxn');
            if (!txn) {
                txn = new Txn();
                commitNow = true;
            }
         }
         if (args[0] && !jQuery.isFunction(args[0]) ) {
             var data = args.shift();
         } else {
             var data = null;
         }
         var callback = args[0] || null;     
         konsole.log('execute', data, callback);                  
         if (data) {
            if (action == 'query') {
                if (typeof data == 'string') {
                    data = { query : data };
                } 
                //assert data.query;
                var thisid = this.attr('itemid');
                if (thisid) {
                    if (!data.bindvars) {
                        data.bindvars = {}
                    }
                    data.bindvars['this'] =  thisid;
                }                
            }
            txn.execute(action, data, callback, this[0]);
         } else {
            this.each(function() {
                var obj = bindElement(this);
                konsole.log('about to', action, 'obj', obj);
                txn.execute(action, obj, callback, this);
            });
         }
        
         if (commitNow)
            txn.commit(null, this);
         return this;
     },

     /*
     [txn] [data] [callback]
     */
     dbAdd : function(a1, a2, a3) {
         return this._executeTxn('add', a1,a2, a3);
     },
     dbCreate : function(a1, a2, a3) {
         return this._executeTxn('create', a1,a2, a3);
     },
     dbUpdate : function(a1, a2, a3) {
         return this._executeTxn('update', a1,a2, a3);
     },     
     dbReplace : function(a1, a2, a3) {
         return this._executeTxn('replace', a1,a2, a3);
     },     
     dbQuery : function(a1, a2, a3) {
         return this._executeTxn('query', a1,a2, a3);
     },     
     dbRemove : function(a1,a2,a3){
          return this._executeTxn('remove', a1,a2, a3);
      },
     dbBegin : function() {
        this.data('currentTxn', new Txn());
        return this;
     },
     dbCommit : function(callback) {
        var txn = this.data('currentTxn');
        if (txn)
            txn.commit(callback, this);
        this.removeData('currentTxn');
        return this;
     },
     dbRollback : function() {
        var txn = this.data('currentTxn');
        if (txn) {
            konsole.log('rollback with', txn);
            var errorObj = {"jsonrpc": "2.0", "id": null,
              "error": {"code": -32001, 
                  "message": "client-side rollback",
                  'data' : null
                } 
            };
            this.trigger('dbdata.'+txn.txnId, errorObj);
            this.trigger('dbdata-*', errorObj);
        } else {
            konsole.log('rollback with no txn');
        }
        this.removeData('currentTxn');
        return this;        
     }
   })
})(jQuery);

function bindElement(elem) {    
    if (elem.nodeName == 'FORM') {
        var binder = Binder.FormBinder.bind( elem ); 
        return binder.serialize();
    }
    
    //otherwise emulate HTML microdata scheme
    //see http://dev.w3.org/html5/spec/microdata.html
    var itemElems = [];

    function getItem(elem) {    
        var item = $(elem).data('item');
        if (typeof item != 'undefined') {
            return item;
        }
        item = {};
        var id = $(elem).attr('itemid');
        if (id) 
            item['id'] = id;
        var type = $(elem).attr('itemtype');
        if (type) 
            item['type'] = type;

        $(elem).data('item', item);
        itemElems.push(elem);
        return item;
    }

    function getPropValue(elem) {
        var $this = $(elem);
        if ($this.attr('itemscope') || $this.attr('itemid')) {
            //XXX this need to return a ref, not an item
            return getItem(elem);
        }
        var attr = { 'META' : 'content',
          'IMG' : 'src',
          'EMBED' : 'src',
          'IFRAME' : 'src',
          'AUDIO' : 'src',
          'SOURCE' : 'src',
          'VIDEO' : 'src',
          'A' : 'href',
          'AREA' : 'href',
          'LINK' : 'href',
          'OBJECT' : 'data',
          'TIME' : 'datetime'
        }[elem.tagName];
        if (attr) {
            //XXX resolve urls to absolute urls
            return $this.attr(attr) || '';        
        } else {
            return $this.text();
        }
    }

    function addProp(item, elem) {
        var $this = $(elem);
        var propnames = $this.attr('itemprop').split(/\s+/);
        var value = getPropValue(elem);    
        for (var i=0; i<propnames.length; i++){
            var propname = propnames[i];
            var current = item[ propname ];
            if (typeof current != 'undefined') {
                if ($.isArray(current)) {
                    current.push(value);
                } else {
                    item[propname] = [current, value];
                }
            } else {
                item[propname] = value;
            }
        }
    }
    
    $('[itemprop]', elem).each(function(){
       var $this = $(this);
       var parent = $this.parent().closest('[itemscope],[itemid]');
       if (parent.length) {
           //XXX exclude if parent is outside of elem
           var item = getItem(parent.get(0));
           addProp(item, this);
       }
    });
 
    $('[itemref]', elem).each(function(){
        var item = getItem(this);
        var refs = $(this).attr('itemref').split(/\s+/);
        $('#'+refs.join(',#')).each(function() {
            //XXX each of these refs is a or contains itemprops
            //maybe outside bindelem scope
            //need to recursively follow itemref in itemprops            
        });
    });
    
    var itemDict = {};
    var items = $.map($.unique(itemElems), function(elem) {
        var item = $(elem).data('item');        
        if (item.id) {            
            var existing = itemDict[item.id];
            if (existing) {
                //items with same id but appear on multiple elements should be merged
                $.extend(existing, item);
                return null; //don't include 
            } else {
                itemDict[item.id] = item;
            }
        }
        return item;
    });
    $(itemElems).removeData('item');
    return items;     
}

// binder-0.3.js
// Copyright 2008 Steven Bazyl
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
 //  limitations under the License.

var Binder = {};
Binder.Util = {
  isFunction: function( obj ) {
    return obj != undefined 
            && typeof(obj) == "function"
            && typeof(obj.constructor) == "function"
            && obj.constructor.prototype.hasOwnProperty( "call" );
  },
  isArray: function( obj ) {
    return obj != undefined && ( obj instanceof Array || obj.construtor == "Array" );
  },
  isString: function( obj ) {
    return typeof(obj) == "string" || obj instanceof String;
  },
  isNumber: function( obj ) {
    return typeof(obj) == "number" || obj instanceof Number;
  },
  isBoolean: function( obj ) {
    return typeof(obj) == "boolean" || obj instanceof Boolean;
  },
  isDate: function( obj ) {
    return obj instanceof Date;
  },
  isBasicType: function( obj ) {
    return this.isString( obj ) || this.isNumber( obj ) || this.isBoolean( obj ) || this.isDate( obj );
  },
  isNumeric: function( obj ) {
    return this.isNumber( obj ) ||  ( this.isString(obj) && !isNaN( Number(obj) ) );
  },
  filter: function( array, callback ) {
    var nv = [];
    for( var i = 0; i < array.length; i++ ) {
      if( callback( array[i] ) ) {
        nv.push( array[i] );
      }
    }
    return nv;
  }
};

Binder.PropertyAccessor =  function( obj ) {
  this.target = obj || {};
  this.index_regexp = /(.*)\[(.*?)\]/;
};
Binder.PropertyAccessor.prototype = {
  _setProperty: function( obj, path, value ) {
    if( path.length == 0 || obj == undefined) {
      return value;
    }
    var current = path.shift();
    if( current.indexOf( "[" ) >= 0 ) {
      var match = current.match( this.index_regexp );
      var index = match[2];
      current = match[1];
      obj[current] = obj[current] || ( Binder.Util.isNumeric( index ) ? [] : {} );
      if( index ) {
        obj[current][index] = this._setProperty( obj[current][index] || {}, path, value );
      } else {
        var nv = this._setProperty( {}, path, value );
        if( Binder.Util.isArray( nv ) ) {
          obj[current] = nv;
        } else {
          obj[current].push( nv );
        }
      }
    } else {
      obj[current] = this._setProperty( obj[current] || {}, path, value );
    }
    return obj;
  },
  _getProperty: function( obj, path ) {
    if( path.length == 0 || obj == undefined ) {
      return obj;
    }
    var current = path.shift();
    if( current.indexOf( "[" ) >= 0 ) {
      var match = current.match( this.index_regexp );
      current = match[1];
      if( match[2] ) {
        return this._getProperty( obj[current][match[2]], path );
      } else {
        return obj[current];
      }
    } else {
      return this._getProperty( obj[current], path );
    }
  },
  _enumerate: function( collection, obj, path ) {
    if( Binder.Util.isArray( obj ) ) {
      for( var i = 0; i < obj.length; i++ ) {
        this._enumerate( collection, obj[i], path + "["+i+"]" );
      }
    } else if( Binder.Util.isBasicType( obj ) ) {
      collection.push( path );
    } else {
      for( property in obj ) {
        if( !Binder.Util.isFunction( property ) ) {
          this._enumerate( collection, obj[property], path == "" ? property : path + "." + property );
        }
      }
    }
  },
  isIndexed: function( property ) {
    return property.match( this.index_regexp ) != undefined;
  },
  set: function(  property, value ) {
    var path = property.split( "." );
    return this._setProperty( this.target, path, value );
  },
  get: function(  property ) {
    var path = property.split( "." );
    return this._getProperty( this.target || {}, path );
  },
  properties: function() {
    var props = [];
    this._enumerate( props, this.target, "" );
    return props;
  }
};
Binder.PropertyAccessor.bindTo = function( obj ) {
  return new Binder.PropertyAccessor( obj ); 
}

Binder.TypeRegistry = {
  'string': {
    format: function( value ) {
      return String(value);
    },
    parse: function( value ) {
      return value && value != "" ? value : undefined;
    }
  },
  'number': {
    format: function( value ) {
      return String(value);
    },
    parse: function( value ) {
      return Number( value );
    }
  },
  'boolean': {
    format: function( value ) {
      return String(value);
    },
    parse: function( value ) {
      if( value ) {
        value = value.toLowerCase();
        return "true" == value || "yes" == value;
      }
      return false;
    }
  },
  'json': {
    format: function( value ) {
      return JSON.stringify(value);
    },
    parse: function( value ) {
      return value && value != "" ? JSON.parse(value) : undefined;
    }
  },
  'null': {
    format: function( value ) {
      return '';
    },
    parse: function( value ) {
      return value ? value : null; 
    }
  }   
};

Binder.FormBinder = function( form, accessor ) {
  this.form = form;
  this.accessor = this._getAccessor( accessor );
  this.type_regexp = /type\[(.*)\]/;
};
Binder.FormBinder.prototype = {
  _isSelected: function( value, options ) {
    if( Binder.Util.isArray( options ) ) {
      for( var i = 0; i < options.length; ++i ) {
        if( value == options[i] ) {
          return true;
        }
      }
    } else if( value != ""  && value != "on" ) {
      return value == options;
    } else {
      return Boolean(options);
    }
  },
  _getType: function( element ) {
    if( element.className ) {
      var m = element.className.match( this.type_regexp );
      if( m && m[1] ) {
        return m[1];
      }
    }
    return "string";
  },
  _format: function( path, value, element ) {
    var type = this._getType( element );
    var handler = Binder.TypeRegistry[type];
    if( Binder.Util.isArray( value ) && handler ) {
      var nv = [];
      for( var i = 0; i < value.length; i++ ) {
        nv[i] = handler.format( value[i] );
      }
      return nv;
    }
    return handler ? handler.format( value ) : String(value);
  },
  _parse: function( path, value, element ) {
    var type = this._getType( element );
    var handler = Binder.TypeRegistry[type];
    if( Binder.Util.isArray( value ) && handler ) {
      var nv = [];
      for( var i = 0; i < value.length; i++ ) {
        nv[i] = handler.parse( value[i] );
      }
      return nv;
    }
    return handler ? handler.parse( value ) : String(value);
  },
  _getAccessor: function( obj ) {
    if( obj == undefined ) {
      return this.accessor || new Binder.PropertyAccessor( obj );
    } else if( obj instanceof Binder.PropertyAccessor ) {
      return obj;
    } 
    return new Binder.PropertyAccessor( obj );
  },
  serialize: function( obj ) {
    var accessor = this._getAccessor( obj );
    for( var i = 0; i < this.form.elements.length; i++) {
      this.serializeField( this.form.elements[i], accessor );
    }
    return accessor.target;
  },
  serializeField: function( element, obj ) {
    if (!element.name) //added if (!element.name) check
        return; //skip unnamed fields
    var accessor = this._getAccessor( obj );
    var value = undefined;
    if( element.type == "radio" || element.type == "checkbox" )  {
      if( element.value != "" && element.value != "on" ) {
        value = this._parse( element.name, element.value, element );        
        if( element.checked ) {
          accessor.set( element.name, value );
        } else if( accessor.isIndexed( element.name ) ) {
          var values = accessor.get( element.name );
          values = Binder.Util.filter( values, function( item) { return item != value; } );
          accessor.set( element.name, values );
        } 
      } else { 
        value = element.checked;
        accessor.set( element.name, value );
      }
    } else if ( element.type == "select-one" || element.type == "select-multiple" ) {
      accessor.set( element.name, accessor.isIndexed( element.name ) ? [] : undefined );
      for( var j = 0; j < element.options.length; j++ ) {
        var v = this._parse( element.name, element.options[j].value, element );
        if( element.options[j].selected ) {
          accessor.set( element.name, v );
        }
      }
    } else {      
        value = this._parse( element.name, element.value, element );
        accessor.set( element.name, value );      
    }
  },
  deserialize: function( obj ) {
    var accessor = this._getAccessor( obj );
    for( var i = 0; i < this.form.elements.length; i++) {
      this.deserializeField( this.form.elements[i], accessor );
    }
    return accessor.target;
  },
  deserializeField: function( element, obj ) {
    var accessor = this._getAccessor( obj );
    var value = accessor.get( element.name );
    value = this._format( element.name, value, element );
    if( element.type == "radio" || element.type == "checkbox" )  {
      element.checked = this._isSelected( element.value, value );
    } else if ( element.type == "select-one" || element.type == "select-multiple" ) {
      for( var j = 0; j < element.options.length; j++ ) {
        element.options[j].selected = this._isSelected( element.options[j].value, value );
      }
    } else {
      element.value = value || "";
    }
  }
};
Binder.FormBinder.bind = function( form, obj ) {
  return new Binder.FormBinder( form, obj );
};

/* not currently used
the idea is enable method dispatching based on a jQuery selector
so DOM elements can be typed.
E.g.
$('.class1').dBind({
  save : function() {...}
});
$('.class2').dBind({
  save : function() {...}
});
//"base class":
$('div').dBind({
   save : function() {}
});
$('container [itemid]').dCall('save', 1,2);

var _funcmap = {};
function addDispatches(selector, methods) {
    for (var name in methods) {
        var funcs = _funcmap[name];
        if (!funcs) {
            funcs = {};
            _funcmap[name] = funcs;
        }
        funcs[selector] = methods[name];
        //XXX selectors should be sorted by specificity
        //so defaults like "*" go last
    }   
}

function dispatch(name, args) {    
    //preserves order but can be slower
    var funcs = _funcmap[name];
    this.each(function() {        
        for (var selector in funcs) {
            var $this = $(this);
            if ($this.is(selector)) {
                funcs[selector].call($this, args || []);
                break;
            }
        }
    });
    return this;
}
*/