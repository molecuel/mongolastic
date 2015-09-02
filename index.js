/**
 * Created by dob on 05.05.14.
 */
var elasticsearch = require('elasticsearch');
var indices = require('./lib/indices');
var async = require('async');
var util = require('util');
var _ = require('lodash');
var EventEmitter = require('events').EventEmitter;

/**
 * Module definition
 */
var mongolastic = function () {
    this.connection = null;
    this.prefix = null;
    this.indexPreprocessors = [];
};

util.inherits(mongolastic, EventEmitter);

/////////////////////
// singleton stuff
////////////////////
var instance = null;

var getInstance = function () {
    return instance || (instance = new mongolastic());
};

/**
 * Connects and tests the connection with a ping
 * @param prefix
 * @param options
 * @param {function} callback
 */
mongolastic.prototype.connect = function (prefix, options, callback) {

    var self = this;

    // check if the prefix has been defined
    if (!this.prefix) {
        this.prefix = prefix;
    }

    // check if the connection has been defined
    if (!this.connection) {
        this.connection = new elasticsearch.Client(options);
    }

    if (!this.indices) {
        this.indices = new indices(this);
    }

    // check the connection with a ping to the cluster and reply the connection
    this.connection.ping({
        requestTimeout: 1000,
        hello: 'elasticsearch!'
    }, function (err) {

        if (err) {
            return callback(err);
        }

        return callback(null, self.connection);
    });
};

/**
 * Registers handler for individual populating elements on before es-indexing
 *
 * @param  {type} handler description
 */
mongolastic.prototype.registerIndexPreprocessor = function registerIndexPreprocessor(handler) {
    this.indexPreprocessors.push(handler);
};

/**
 * Populates object references according to their elastic-options. Invoked on pre(save) and sync to enable synchronisation
 * of full object trees to elasticsearch index
 * @param doc
 * @param schema
 * @param callback
 */
mongolastic.prototype.populate = function populate(doc, schema, options, callback) {
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
      var conf = properties[property];
      if(_.isObject(conf)) {
        var fields = conf.fields || {};
        doc.populate(property, fields, function() {
          if(conf.docs) {
            populateRecursive(doc, property, conf.docs, cb);
          } else {
            cb();
          }
        });
        /*
        var fields = conf.fields || {};

        doc.populate(property, {}, function() {
          cb();
        });
        **/
      } else {
        doc.populate(property, cb);
      }
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
  if(options.modelName) {
    var elastic = getInstance();

    schema.pre('save', function(next, done) {
      var self = this;
      elastic.populate(self, schema, options, function(err) {
        if(!err) {
          var entry = self.toObject();
          elastic.index(options.modelName, entry, function(err) {
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
      elastic.delete(options.modelName, this.id, function(err) {
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
      query.index = elastic.getIndexName(options.modelName);
      elastic.search(query, cb);
    };

    /**
     * Search with specifiing a model or index
     * @type {search|Function|string|api.indices.stats.params.search|Boolean|commandObject.search|*}
     */
    schema.statics.search = elastic.search;

    schema.statics.sync = function (callback) {
      return elastic.sync(this, options.modelName, callback);
    };

    schema.statics.syncById = function (id, callback) {
      return elastic.syncById(this, options.modelName, id, callback);
    };
  } else {
    console.error('missing modelName');
  }
};

/**
 * Render the mapping for the model
 * @param model
 * @param callback
 */
mongolastic.prototype.renderMapping = function(model, callback) {

  var mapping = {};

  // Get paths with elasticsearch mapping set in schema
  // and merge all their mappings
  var pathMappings = {};

  _.forOwn(model.schema.paths, function(value, key) {

    if(_.has(value, 'options.elastic.mapping')) {
      pathMappings[key] = value.options.elastic.mapping;
    }
  });

  mapping = _.merge(mapping, pathMappings);


  // Merge "global" mapping that has been set on the model directly
  if (_.has(model, 'elastic.mapping')) {
    mapping = _.merge(mapping, model.elastic.mapping);
  }


  // Elasticsearch requires all nested properties "sub.subSub"
  // to be wrapped as {sub: {properties: {subSub: {properties: ...}}}}
  var nestedMapping = {};

  _.forOwn(mapping, function(value, key) {

    var nestedKeys = key.split('.');
    var nestedValue = value;

    // Top level
    nestedValue = wrapValue(nestedKeys.pop(), nestedValue);

    // Deeper levels need to be wrapped as "properties"
    _.forEachRight(nestedKeys, function(nestedKey) {

      nestedValue = wrapValue(nestedKey, {
        properties: nestedValue
      });
    });

    nestedMapping = _.merge(nestedMapping, nestedValue);
  });


  // Wrap the result so that it has the form
  // { "theModelName": { properties: ...}}
  var result = {};
  result[model.modelName] = {properties: nestedMapping};

  return callback(null, result);
};

function wrapValue(key, value) {

  var result = {};
  result[key] = value;

  return result;
}


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
 * @param {object} model
 * @param {object} [options]
 * @param {function} callback
 */
mongolastic.prototype.registerModel = function (model, options, callback) {
  var elastic = getInstance();

  // Check if options are provided
  // or if the options argument is actually the callback
  if (callback === undefined && _.isFunction(options)) {
    callback = options;
    options = {};
  }

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
  elastic.indices.checkCreateByModel(model, options,
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
 * @param {string} modelName
 * @param {object} doc
 * @param {function} callback
 */
mongolastic.prototype.index = function (modelName, doc, callback) {

    var elastic = getInstance();
    var entry = doc;

    async.each(elastic.indexPreprocessors, function (handler, cb) {
        handler(modelName, entry, cb);
    }, function () {
        var myid;
        if (entry && entry._id) {
            myid = entry._id.toString();
        }
        elastic.connection.index({
            index: elastic.getIndexName(modelName),
            type: modelName,
            id: myid,
            body: entry,
            refresh: true
        }, callback);
    });
};

/**
 * Index data
 * @param {object} body
 * @param {function} callback
 */
mongolastic.prototype.bulk = function (body, callback) {
    var elastic = getInstance();
    elastic.connection.bulk({
        //index: elastic.getIndexName(modelName),
        //type: modelName,
        body: body,
        refresh: true
    }, callback);
};

/**
 * Delete document from elasticsearch index
 * @param {string} modelName
 * @param {string} id
 * @param {function} callback
 */
mongolastic.prototype.delete = function (modelName, id, callback) {
    var elastic = getInstance();
    elastic.connection.delete({
        index: elastic.getIndexName(modelName),
        type: modelName,
        id: id
    }, callback);
};

/**
 * Perform search on elasticsearch index
 * @param {object|string} query
 * @param {function} callback
 */
mongolastic.prototype.search = function (query, callback) {
    var elastic = getInstance();
    if (!query.index) {
        query.index = elastic.prefix + '-*';
    }
    elastic.connection.search(query, callback);
};

/**
 * Sync function for database model
 * @param {object} model
 * @param {string} modelName
 * @param {function} callback
 */
mongolastic.prototype.sync = function sync(model, modelName, callback) {
    var elastic = getInstance();
    var stream = model.find().stream();
    var schema = model.schema;
    var errcount = 0;
    var rescount = 0;
    var doccount = 0;
    var donecount = 0;
    var bulk = [];
    var size = 100;
    var step = 0;
    stream.on('data', function (doc) {
        doccount = doccount + 1;
        stream.pause();
        elastic.populate(doc, schema, null, function (err) {
            step = step + 1;
            donecount = donecount + 1;
            if (!err) {
                var action = {
                    index: {
                        '_index': elastic.getIndexName(modelName),
                        '_type': modelName,
                        '_id': doc._id.toString()
                    }
                };
                bulk.push(action);
                bulk.push(doc);
            } else {
                if (err) {
                    errcount = errcount + 1;
                } else {
                    rescount = rescount + 1;
                }
            }

            if (step >= size) {
                elastic.bulk(bulk, function (err) {
                    if (err) {
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
        console.log(err);
      }
      callback(errcount, donecount);
    });
  });
};

/**
 * SyncById function for database model
 * @param {object} model
 * @param {string} modelName
 * @param {function} callback
 */
mongolastic.prototype.syncById = function syncById(model, modelName, id, callback) {
    var elastic = getInstance();
    var schema = model.schema;
    model.findById(id, function (err, doc) {
        if (doc && !err) {
            elastic.populate(doc, schema, null, function (poperr) {
                if (!poperr) {
                    var entry = doc.toObject();
                    elastic.index(modelName, entry, function (inerr) {
                        if (!inerr) {
                            callback();
                        } else {
                            callback(inerr);
                        }
                    });
                } else {
                    callback(poperr);
                }
            });
        } else {
            callback(err);
        }
    });
};


/**
 * Delete whole index
 * @param {string} modelName
 * @param {function} callback
 */
mongolastic.prototype.deleteIndex = function deleteIndex(modelName, callback) {
    this.connection.indices.delete({index: this.getIndexName(modelName)}, callback);
};

/**
 * Get elasticsearch index name for model
 * @param {string} modelName
 * @returns {string}
 */
mongolastic.prototype.getIndexName = function (modelName) {
    var elastic = getInstance();
    if (elastic.prefix) {
        if (modelName.indexOf(elastic.prefix + '-') === 0) {
            return modelName.toLowerCase();
        } else {
            return elastic.prefix + '-' + modelName.toLowerCase();
        }
    } else {
        return modelName.toLowerCase();
    }
};


//application wide singleton
global.singletons = global.singletons || {};
if (global.singletons['mongolastic']) {
    module.exports = global.singletons['mongolastic'];
} else {
    global.singletons['mongolastic'] = module.exports = getInstance();
}
