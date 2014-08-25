/**
 * Created by dob on 05.05.14.
 */
var elasticsearch = require('elasticsearch');
var indices = require('./lib/indices');
var instance;
var async = require('async');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
//var _ = require('underscore');

/**
 * Module definition
 */
var mongolastic = function() {
  this.connection = null;
  this.prefix = null;
};

util.inherits(mongolastic, EventEmitter);

/////////////////////
// singleton stuff
////////////////////
instance = null;

var getInstance = function(){
  return instance || (instance = new mongolastic());
};

/**
 * Connects and tests the connection with a ping
 * @param prefix
 * @param options
 * @param callback
 */
mongolastic.prototype.connect = function(prefix, options, callback) {
  var self = this;
  // check if the prefix has been defined
  if(!this.prefix) {
    this.prefix = prefix;
  }

  // check if the connection has been defined
  if(!this.connection) {
    /*if(!options) {
      options = {};
    }
    options.log = {
      level: 'trace'
    }*/
    this.connection = new elasticsearch.Client(options);
  }

  if(!this.indices) {
    this.indices = new indices(this);
  }

  // check the connection with a ping to the cluster and reply the connection
  this.connection.ping({
    requestTimeout: 1000,
    hello: 'elasticsearch!'
  },function(err) {
    if(err) {
      callback(err);
    } else {
      callback(null, self.connection);
    }
  });
};

/**
 * Populates object references according to their elastic-options. Invoked on pre(save) and sync to enable synchronisation
 * of full object trees to elasticsearch index
 * @param doc
 * @param schema
 * @param callback
 */
mongolastic.prototype.populate = function populate(doc, schema, callback) {
  var elastic = getInstance();

  function populateReferences(options, currentpath, callback) {
    if(options && options.ref) {
      if(options.elastic && options.elastic.avoidpop ) {
        callback();
      } else {
        if(options.elastic && options.elastic.populate) {
          elastic.populateSubdoc(doc, schema, currentpath, options.elastic.populate, callback);
        } else if(options.elastic && options.elastic.popfields) {
          doc.populate(currentpath, options.elastic.popfields, callback);
        } else {
          doc.populate(currentpath, callback);
        }
      }
    } else {
      callback();
    }
  }

  async.each(Object.keys(schema.paths), function(currentpath, callback) {
    if(schema.paths[currentpath] && schema.paths[currentpath].options) {
      var options = schema.paths[currentpath].options;

      if(options.type instanceof Array) { //hande 1:n relationships []
        if(options.type[0] && options.type[0].type) { // direct object references
          options = schema.paths[currentpath].options.type[0];
          populateReferences(options, currentpath, callback);
        } else if(options.type[0]) {
          async.each(Object.keys(options.type[0]), function(key, cb) {
            var suboptions = options.type[0][key];
            var subpath = currentpath + '.' + key;
            populateReferences(suboptions, subpath, cb);
          }, function() {
            callback();
          });
        } else {
          callback();
        }
      } else {
        populateReferences(options, currentpath, callback);
      }
    }
  }, function(err) {
    if(err) {
      callback(new Error('Could not populate document: ' + err));
    }
    callback();
  });
};


/**
 * populateSubdoc - Populate at sub document before indexing it with elastic
 *
 * @param  {object} doc         the document
 * @param  {object} schema      the schema
 * @param  {string} currentpath the current path in schema
 * @param  {object} options     additional options
 * @param  {function} callback    the callback function
 * @callback
 */
mongolastic.prototype.populateSubdoc = function populateSubdoc(doc, schema, currentpath, options, callback) {

  var populateProperties = function(doc, properties, callback) {
    async.each(Object.keys(properties), function(property, cb) {
      doc.populate(property, function() {
        cb();
      });
    }, function(err) {
      if(err) {
        return callback(err);
      }
      callback();
    });
  };

  var populateRecursive = function(doc, key, options, callback) {
    if(doc.get(key) && options) {
      if(doc.get(key) instanceof Array) {
        async.each(doc.get(key), function(subdoc, cb) {
          populateProperties(subdoc, options, cb);
        },function(err) {
          if(err) {
            return callback(err);
          }
          return callback();
        });
      } else {
        populateProperties(doc.get(key), options, callback);
      }
    } else {
      callback();
    }
  };

  // first the currentpath has to be populated to get the subdocument(s)
  doc.populate(currentpath, function(err) {
    if(err) {
      callback(err);
    } else {
      populateRecursive(doc, currentpath, options, callback);
    }
  });
};


/**
 * plugin - Mongoose model plugin
 *
 * @param  {object} schema  The mongoose schema
 * @param  {object} options Additional options
 */
mongolastic.prototype.plugin = function plugin(schema, options) {
  if(options.modelname) {
    var elastic = getInstance();

    schema.pre('save', function(next, done) {
      var self = this;
      elastic.populate(self, schema, function(err) {
        if(!err) {
          elastic.index(options.modelname, self, function(err) {
            if(!err) {
              next();
            } else {
              done(new Error('Could not save in Elasticsearch index: ' + err));
            }
          });
        } else {
          done(new Error('Could not save in Elasticsearch: '+err));
        }
      });
    });

    schema.post('remove', function() {
      elastic.delete(options.modelname, this.id, function(err) {
        if(err) {
          console.error(err);
        }
      });
    });
    /**
     * Search on current model with predefined index
     * @param query
     * @param cb
     */
    schema.methods.search = function(query, cb) {
      query.index = elastic.getIndexName(options.modelname);
      elastic.search(query, cb);
    };

    /**
     * Search with specifiing a model or index
     * @type {search|Function|string|api.indices.stats.params.search|Boolean|commandObject.search|*}
     */
    schema.statics.search = elastic.search;

    schema.statics.sync = function (callback) {
      return elastic.sync(this, options.modelname, callback);
    };

  } else {
    console.error('missing modelname');
  }
};

/**
 * Render the mapping for the model
 * @param model
 * @param callback
 */
mongolastic.prototype.renderMapping = function(model, callback) {
  var deepen = function deepen(o) {
    var oo = {}, t, orig_parts, parts, part;
    for (var k in o) {
      if (o.hasOwnProperty(k)) {
        t = oo;
        orig_parts = k.split('.');
        var key = orig_parts.pop();
        parts = [];
        // if it's nested the schema needs the properties object added for every second element
        for (var i = 0; i < orig_parts.length; i ++) {
          parts.push(orig_parts[i]);
          parts.push('properties');
        }
        while (parts.length) {
          part = parts.shift();
          var mypart = t[part] = t[part] || {};
          t = mypart;
        }
        t[key] = o[k];
      }
    }
    return oo;
  };

  var mapping = {};
  mapping[model.modelName] = {
    properties: {

    }
  };

  async.series([
    function(callback) {
      async.each(Object.keys(model.schema.paths), function(currentkey, cb) {
        var currentPath = model.schema.paths[currentkey];
        if(currentPath && currentPath.options && currentPath.options.elastic && currentPath.options.elastic.mapping) {
          mapping[model.modelName].properties[currentkey] = currentPath.options.elastic.mapping;
          cb();
        } else {
          cb();
        }
      }, function(err) {
        callback(err);
      });
    },
    function(callback) {
      if(model.elastic && model.elastic.mapping) {
        async.each(Object.keys(model.elastic.mapping), function(currentkey, cb) {
          mapping[model.modelName].properties[currentkey] = model.elastic.mapping[currentkey];
          cb();
        }, function(err) {
          callback(err);
        });
      } else {
        callback();
      }
    }
  ],function(err) {
    var map = deepen(mapping[model.modelName].properties);
    mapping[model.modelName].properties = map;
    callback(err, mapping);
  });
};


 /**
  * The default save handler
  *
  * @todo This is currently not working correctly. You have to implement this
  * functionality in your project to be sure it is executed on errors
  *
  * @param {object} error The error object
  * @param {object} result The database result
  * @param {object} options Additional options
  * @param {function} callback The callback function
  */
mongolastic.prototype.defaultSaveHandler = function(err, result, options, callback) {
  var elastic = getInstance();
  if(err && options.isNew && options.doc) {
    // delete document from eleasticsearch
    var docid = options.doc._id;
    if(docid) {
      elastic.delete(options.modelName, docid, function() {
        callback();
      });
    } else {
      callback();
    }
  } else {
    callback();
  }
};

/**
 * When registering a new mongoose model
 * @param model
 * @param callback
 */
mongolastic.prototype.registerModel = function(model, callback) {
  var elastic = getInstance();
  /**
  * Change the save function of the model
  * @deprecated Caused mongoose does not support this
  **/
  //model.prototype.saveOrig = model.prototype.save;

  model.registerSaveHandler = function(saveHandler) {
    model.saveHandlers.push(saveHandler);
  };

  model.saveHandlers = [];

  // This iss currently disabled caused by the handling of mongoose and has to
  // be implemented by a project specific library from you
  model.registerSaveHandler(elastic.defaultSaveHandler);

  /**
   * save - Overwrites the save function of the model
   *
   * @todo this is not working correctly cause mongoose seems to ignore it if a
   * validation error occurs
   *
   * @param  {function} cb the callback function
   * @deprecated as mongoose does not support this
   */
  /*
  model.prototype.save = function save(cb) {
    var self = this;

    // add some options
    var options = {
      isNew: self.isNew
    };

    // call the original save function
    model.prototype.saveOrig.call(this, function(err, result) {
      async.eachSeries(model.saveHandlers, function(item, callback) {
        // check if the saveHandler item is a function
        if('function' === typeof item) {
          item(err, result, options, callback);
        } else {
          callback();
        }
      }, function(err) {
        cb(err, result);
      });
    });
  };*/


  /**
   * Creates the index for the model with the correct mapping
   */
  elastic.indices.checkCreateByModel(model,
    function(err) {
      callback(err, model);
    }
  );
};

mongolastic.prototype.save = function(document, callback) {
  var self = this;
  // add some options
  var options = {
    isNew: document.isNew,
    model: document.constructor,
    modelName: document.constructor.modelName,
    doc: document
  };

  var model = options.model;

  /**
   * Original save function of mongoose
   */
  document.save(function(saveerr, result) {
    if(model.saveHandlers) {

      /**
       * asyncHandler - Async serial iterator over the registered save handlers
       *
       * @param  {function} item saveHandler function
       * @param  {function} cb   callback function of async
       * @callback
       */
      var asyncHandler = function asyncHandler(item, cb) {
        // check if the saveHandler item is a function
        if('function' === typeof item) {
          item(saveerr, result, options, cb);
        } else {
          cb();
        }
      };
      async.eachSeries(model.saveHandlers, asyncHandler, function(err) {
        if(!err) {
          self.emit('mongolastic::saveHandler:success', result);
          callback(saveerr, result);
        } else {
          self.emit('mongolastic::saveHandler:error', err, options.doc);
          async.eachSeries(model.saveHandlers, asyncHandler, function(cleanuperr) {
            callback(cleanuperr, result);
          });
        }
      });
    } else {
      self.emit('mongolastic::saveHandler:none', saveerr, options.doc);
      callback(saveerr, result);
    }
  });
};

/**
 * Index data
 * @param modelname
 * @param entry
 * @param callback
 */
mongolastic.prototype.index = function(modelname, entry, callback) {
  var elastic = getInstance();

  var myid;
  if(entry && entry._id) {
    myid = entry._id.toString();
  }

  elastic.connection.index({
    index: elastic.getIndexName(modelname),
    type: modelname,
    id: myid,
    body: entry,
    refresh: true
  }, callback);
};

/**
 * Index data
 * @param modelname
 * @param entry
 * @param callback
 */
mongolastic.prototype.bulk = function(body, callback) {
  var elastic = getInstance();

  elastic.connection.bulk({
    //index: elastic.getIndexName(modelname),
    //type: modelname,
    body: body,
    refresh: true
  }, callback);
};

/**
 * Delete function
 * @param modelname
 * @param entry
 * @param callback
 */
mongolastic.prototype.delete = function(modelname, id, callback) {
  var elastic = getInstance();
  elastic.connection.delete({
    index: elastic.getIndexName(modelname),
    type: modelname,
    id: id
  }, callback);
};

/**
 * Search function
 * @param query
 * @param callback
 */
mongolastic.prototype.search = function(query, callback) {
  var elastic = getInstance();
  if(!query.index) {
    query.index = elastic.prefix + '-*';
  }
  elastic.connection.search(query, callback);
};

/**
 * Sync function for database model
 * @param model
 * @param modelname
 * @param callback
 */
mongolastic.prototype.sync = function sync(model, modelname, callback) {
  var elastic = getInstance();
  var stream = model.find().stream();
  var schema = model.schema;
  var errcount = 0;
  var rescount = 0;
  var doccount = 0;
  var donecount = 0;
  var bulk = [];
  var size = 1000;
  var step = 0;
  stream.on('data', function (doc) {
    doccount = doccount +1;
    stream.pause();
    elastic.populate(doc, schema, function(err) {
      step = step + 1;
      donecount = donecount +1;

      if(!err) {
        var action = {
          index: {
            '_index': elastic.getIndexName(modelname),
            '_type': modelname,
            '_id': doc._id.toString()
          }
        };
        bulk.push(action);
        bulk.push(doc);
      } else {
        console.err('error populate doc ' + doc._id + ' ' + err);
        if(err) {
          errcount = errcount +1;
        } else {
          rescount = rescount +1;
        }
      }

      if(step >= size) {
        elastic.bulk(bulk, function(err) {
          if(err) {
            console.err(err);
          }
          bulk = [];
          step = 0;
          stream.resume();
        });
      } else {
        stream.resume();
      }
    });
  });

  stream.on('end', function() {
    elastic.bulk(bulk, function(err) {
      if(err) {
        console.err(err);
      }
      callback(errcount, donecount);
    });
  });
};

/**
 * Delete whole index
 * @param modelname
 * @param callback
 */
mongolastic.prototype.deleteIndex = function deleteIndex(modelname, callback) {
  this.connection.indices.delete({index: this.getIndexName(modelname)}, callback);
};

/**
 * Helper for hamornising namespaces
 * @param modelname
 * @returns {string}
 */
mongolastic.prototype.getIndexName = function(name) {
  var mlelast = getInstance();
  if(mlelast.prefix) {
    if(name.indexOf(mlelast.prefix+'-') === 0) {
      return name.toLowerCase();
    } else {
      return mlelast.prefix + '-' + name.toLowerCase();
    }
  } else {
    return name.toLowerCase();
  }
};


module.exports = getInstance();
