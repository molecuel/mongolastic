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
var mongolastic = function() {
  this.connection = null;
  this.prefix = null;
  this.indexPreprocessors = [];
};

util.inherits(mongolastic, EventEmitter);

/////////////////////
// singleton stuff
////////////////////
var instance = null;

var getInstance = function() {
  return instance || (instance = new mongolastic());
};

/**
 * Connects and tests the connection with a ping
 * @param prefix
 * @param options
 * @param {function} callback
 */
mongolastic.prototype.connect = function(prefix, options, callback) {

  var self = this;

  // check if the prefix has been defined
  if(!this.prefix) {
    this.prefix = prefix;
  }

  // check if the connection has been defined
  if(!this.connection) {
    this.connection = new elasticsearch.Client(options);
  }

  if(!this.indices) {
    this.indices = new indices(this);
  }

  // check the connection with a ping to the cluster and reply the connection
  this.connection.ping({
    requestTimeout: 1000,
    hello: 'elasticsearch!'
  }, function(err) {

    if(err) {
      return callback(err, null);
    }

    return callback(null, self.connection);
  });
};


/**
 * Registers handler for individual populating elements on before es-indexing
 * @param  {type} handler description
 */

mongolastic.prototype.registerIndexPreprocessor = function registerIndexPreprocessor(handler) {
  this.indexPreprocessors.push(handler);
};


/**
 * Populates object references according to their elastic-options. Invoked on pre(save) and sync to enable synchronisation
 * of full object trees to elasticsearch index
 * @param {object} doc
 * @param {object} schema
 * @param {object} options
 * @param {function} callback
 */
mongolastic.prototype.populate = function populate(doc, schema, options, callback) {
  var elastic = getInstance();

  function populateReferences(options, path, callback) {

    // TODO Should this return error or just "silently" fail?
    if(!(options && options.ref)) {
      return callback(null, null);
    }

    // TODO Should this return error or just "silently" fail?
    if(_.has(options, 'elastic.avoidpop')) {
      return callback(null, null);
    }

    if(_.has(options, 'elastic.populate')) {
      elastic.populateSubdoc(doc, schema, path, options.elastic.populate, callback);
    } else if(options.elastic && options.elastic.popfields) {
      doc.populate(path, options.elastic.popfields, callback);
    } else {
      doc.populate(path, callback);
    }
  }

  async.each(Object.keys(schema.paths), function(path, callback) {
    if(schema.paths[path] && schema.paths[path].options) {
      var options = schema.paths[path].options;

      if(_.isArray(options.type)) { //hande 1:n relationships []
        if(options.type[0] && options.type[0].type) { // direct object references
          options = schema.paths[path].options.type[0];
          populateReferences(options, path, callback);
        } else if(options.type[0]) {
          async.each(Object.keys(options.type[0]), function(key, cb) {
            var subOptions = options.type[0][key];
            var subPath = path + '.' + key;
            populateReferences(subOptions, subPath, cb);
          }, callback);
        } else {
          return callback(null, null);
        }
      } else {
        populateReferences(options, path, callback);
      }
    }
  }, callback);
};


/**
 * populateSubDoc - Populate at sub document before indexing it with elastic
 *
 * @param  {object} doc         the document
 * @param  {object} schema      the schema
 * @param  {string} path the current path in schema
 * @param  {object} options     additional options
 * @param  {function} callback    the callback function
 * @callback
 */
mongolastic.prototype.populateSubdoc = function populateSubDoc(doc, schema, path, options, callback) {

  var populateProperties = function(doc, properties, callback) {
    async.each(Object.keys(properties), function(path, cb) {

      var value = properties[path];

      if(_.isPlainObject(value)) {
        var fields = value.fields || {};
        doc.populate(path, fields, function() {

          if(!value.docs) {
            return cb(null, null);
          }

          populateRecursive(doc, path, value.docs, cb);
        });
      } else {
        doc.populate(path, cb);
      }
    }, callback);
  };

  var populateRecursive = function(doc, path, options, callback) {

    // TODO Should this return error or just "silently" fail?
    if(!(doc.get(path) && options)) {
      return callback(null, null);
    }

    if(doc.get(path) instanceof Array) {
      async.each(doc.get(path), function(subdoc, cb) {
        populateProperties(subdoc, options, cb);
      }, callback);
    } else {
      populateProperties(doc.get(path), options, callback);
    }
  };

  // first the path has to be populated to get the sub-document(s)
  doc.populate(path, function(err) {
    if(err) {
      return callback(err, null);
    }
    populateRecursive(doc, path, options, callback);
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

          elastic.index(options.modelName, self, function(err) {
            if(!err) {
              next();
            } else {
              done(new Error('Could not save in Elasticsearch index: ' + err));
            }
          });
        } else {
          done(new Error('Could not save in Elasticsearch: ' + err));
        }
      });
    });

    schema.post('remove', function() {
      elastic.delete(options.modelName, this.id, function(err) {
        if(err) {
          // TODO: Error handling feels a bit shaky here?
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
     * Search with specifying a model or index
     * @type {search|Function|string|api.indices.stats.params.search|Boolean|commandObject.search|*}
     */
    schema.statics.search = elastic.search;

    schema.statics.sync = function(callback) {
      return elastic.sync(this, options.modelName, callback);
    };

    schema.statics.syncById = function(id, callback) {
      return elastic.syncById(this, options.modelName, id, callback);
    };
  } else {
    // TODO: Error handling feels a bit shaky here?
    console.error('missing modelName');
  }
};

/**
 * Render the mapping for the model
 * @param {object} model
 * @param {function} callback
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
  if(_.has(model, 'elastic.mapping')) {
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
 * @param {object} err The error object
 * @param {object} result The database result
 * @param {object} options Additional options
 * @param {function} callback The callback function
 */
mongolastic.prototype.defaultSaveHandler = function(err, result, options, callback) {
  var elastic = getInstance();
  if(err && options.isNew && options.doc) {
    // delete document from elasticsearch
    var docid = options.doc._id;
    if(docid) {
      elastic.delete(options.modelName, docid, function() {
        return callback();
      });
    } else {
      return callback();
    }
  } else {
    return callback();
  }
};

/**
 * When registering a new mongoose model
 * @param {object} model
 * @param {object} [options]
 * @param {function} callback
 */
mongolastic.prototype.registerModel = function(model, options, callback) {
  var elastic = getInstance();

  // Check if options are provided
  // or if the options argument is actually the callback
  if(callback === undefined && _.isFunction(options)) {
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
      return callback(err, model);
    }
  );
};

mongolastic.prototype.save = function(document, callback) {
  var self = this;

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
  document.save(function(saveErr, result) {
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
        if(_.isFunction(item)) {
          item(saveErr, result, options, cb);
        } else {
          // TODO: Should this return an error instead?
          return cb(null, null);
        }
      };
      async.eachSeries(model.saveHandlers, asyncHandler, function(err) {
        if(!err) {
          self.emit('mongolastic::saveHandler:success', result);
          return callback(saveErr, result);
        } else {
          self.emit('mongolastic::saveHandler:error', err, options.doc);
          async.eachSeries(model.saveHandlers, asyncHandler, function(cleanupErr) {
            return callback(cleanupErr, result);
          });
        }
      });
    } else {
      self.emit('mongolastic::saveHandler:none', saveErr, options.doc);
      return callback(saveErr, result);
    }
  });
};

/**
 * Index data
 * @param {string} modelName
 * @param {object} doc
 * @param {function} callback
 */
mongolastic.prototype.index = function(modelName, doc, callback) {

  var elastic = getInstance();

  async.each(elastic.indexPreprocessors, function(delegate, cb) {
    delegate(modelName, doc, cb);
  }, function() {

    elastic.connection.index({
      index: elastic.getIndexName(modelName),
      type: modelName,
      id: doc.id,
      body: doc.toObject(),
      refresh: true
    }, callback);
  });
};

/**
 * Index data
 * @param {object} body
 * @param {function} callback
 */
mongolastic.prototype.bulk = function(body, callback) {
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
mongolastic.prototype.delete = function(modelName, id, callback) {
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
mongolastic.prototype.search = function(query, callback) {
  var elastic = getInstance();
  if(!query.index) {
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
  var errCount = 0;
  var docCount = 0;
  var doneCount = 0;
  var bulk = [];
  var size = 100;
  var step = 0;
  stream.on('data', function(doc) {
    docCount = docCount + 1;
    stream.pause();
    elastic.populate(doc, schema, null, function(err) {
      step = step + 1;
      doneCount = doneCount + 1;
      if(!err) {
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
        errCount = errCount + 1;
      }

      if(step >= size) {
        elastic.bulk(bulk, function(err) {
          if(err) {
            // TODO: Error handling feels a bit shaky here?
            console.error(err);
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

      // TODO: Error handling feels a bit shaky here?
      if(err) {
        console.log(err);
      }
      // TODO: This should return proper Error object and not a number
      return callback(errCount, doneCount);
    });
  });
};

/**
 * SyncById function for database model
 * @param {object} model
 * @param {string} modelName
 * @param {string} id
 * @param {function} callback
 */
mongolastic.prototype.syncById = function syncById(model, modelName, id, callback) {
  var elastic = getInstance();
  var schema = model.schema;
  model.findById(id, function(err, doc) {
    if(err) {
      return callback(err, null);
    }

    if(!doc) {
      return callback(new Error('No document found for id %s', id), null);
    }

    elastic.populate(doc, schema, null, function(err) {
      if(err) {
        return callback(err, null);
      }

      elastic.index(modelName, doc, callback);
    });
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
mongolastic.prototype.getIndexName = function(modelName) {
  var elastic = getInstance();

  // If no prefix has been defined,
  // use the mode name as index name
  if(!elastic.prefix) {
    return modelName.toLowerCase();
  }

  // If the defined prefix is already part of the model name
  // use the model name as index name
  if(_.startsWith(modelName, elastic.prefix + '-')) {
    return modelName.toLowerCase();
  }

  // Prepend model name with prefix
  return elastic.prefix + '-' + modelName.toLowerCase();
};


//application wide singleton
global.singletons = global.singletons || {};
if(global.singletons['mongolastic']) {
  module.exports = global.singletons['mongolastic'];
} else {
  global.singletons['mongolastic'] = module.exports = getInstance();
}
