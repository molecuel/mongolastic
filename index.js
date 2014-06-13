/**
 * Created by dob on 05.05.14.
 */
var elasticsearch = require('elasticsearch');
var indices = require('./lib/indices');
var instance;
var async = require('async');

/**
 * Module definition
 */

var mongolastic = function() {
  this.connection = null;
  this.prefix = null;
};

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

mongolastic.prototype.plugin = function plugin(schema, options) {
  if(options.modelname) {
    var elastic = getInstance();

    schema.pre('save', function(next, done) {
      var self = this;
      async.each(Object.keys(schema.paths), function(currentpath, callback) {
        if(schema.paths[currentpath] && schema.paths[currentpath].options && schema.paths[currentpath].options.ref) {
          if(schema.paths[currentpath].options.elastic && schema.paths[currentpath].options.elastic.avoidpop ) {
            callback();
          } else {
            if(schema.paths[currentpath].options.elastic && schema.paths[currentpath].options.elastic.popfields) {
              self.populate(currentpath, schema.paths[currentpath].options.elastic.popfields, callback);
            } else {
              self.populate(currentpath, callback);
            }
          }
        } else {
          callback();
        }
      }, function(err) {
        if(!err) {
          elastic.index(options.modelname, self, function(err) {
            if(!err) {
              next();
            } else {
              done(new Error('Could not save in Elasticsearch'));
            }
          });
        } else {
          done(new Error('Could not save in Elasticsearch'));
        }
      });
    });

    schema.post('remove', function() {
      elastic.delete(options.modelname, this.id, function(err) {
        if(err) {
          console.log(err);
        }
      });
    });
    /**
     * Search on current model with predefined index
     * @param query
     * @param cb
     */
    schema.methods.search = function(query, cb) {
      query.index = elastic.prefix + '-' + options.modelname;
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
    console.log('missing modelname');
  }
};

/**
 * Render the mapping for the model
 * @param model
 * @param callback
 */
mongolastic.prototype.renderMapping = function(model, callback) {
  var deepen = function deepen(o) {
    var oo = {}, t, parts, part;
    for (var k in o) {
      if (o.hasOwnProperty(k)) {
        t = oo;
        parts = k.split('.');
        var key = parts.pop();
        while (parts.length) {
          part = parts.shift();
          t = t[part] = t[part] || {type: 'object'};
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

  async.each(Object.keys(model.schema.paths), function(currentkey, cb) {
    var currentPath = model.schema.paths[currentkey];
    if(currentPath && currentPath.options && currentPath.options.elastic && currentPath.options.elastic.mapping) {
      mapping[model.modelName].properties[currentkey] = currentPath.options.elastic.mapping;
      cb();
    } else {
      cb();
    }
  }, function(err) {
    var map = deepen(mapping[model.modelName].properties);
    mapping[model.modelName].properties = map;
    callback(err, mapping);
  });
};

/**
 * When registering a new mongoose model
 * @param model
 * @param callback
 */
mongolastic.prototype.registerModel = function(model, callback) {
  var elastic = getInstance();
  elastic.indexCheckCreate(model,
    function(err) {
      callback(err, model);
    });
};

/**
 * Create the mapping if it does not exist
 * @param model
 * @param callback - Return if new mapping has been put on server
 */
mongolastic.prototype.checkCreateMapping = function(model, callback) {
  var elastic = getInstance();
  elastic.getMapping(model.modelName, function(err, response, status) {
    if(status === 404) {
      elastic.renderMapping(model, function(err, mapping) {
        if(!err && mapping) {
          elastic.putMapping(model.modelName, mapping, function(err) {
            model.schema.elastic = mapping;
            callback(err, model);
          });
        } else {
          callback(err, model);
        }
      });
    } else {
      callback(err, model);
    }
  });
};

/**
 * Check and create index
 * @param model
 * @param callback - Returns if new index was created or not
 */
mongolastic.prototype.indexCheckCreate = function(model, callback) {
  var elastic = getInstance();
  elastic.indexExists(model.modelName, function(err, response) {
    if(!response) {
      elastic.indexCreate(model, {}, function (err) {
        callback(err, true);
      });
    } else {
      callback(err, false);
    }
  });
};

/**
 * Check if the index exists
 * @param modelname
 * @param callback
 */
mongolastic.prototype.indexExists = function indexExists(modelname, callback) {
  var elastic = getInstance();
  elastic.connection.indices.exists({
    index: elastic.indexNameFromModel(modelname),
    type: modelname
  }, callback);
};

/**
 * Create a index
 * @param modelname
 * @param callback
 */
mongolastic.prototype.indexCreate = function indexCreate(model, options, callback) {
  var elastic = getInstance();
  elastic.renderMapping(model, function(err, mapping) {
    elastic.indices.create(
      model.modelName,
      options,
      mapping,
      callback);
  });
};


/**
 * Get the current mapping
 * @param modelname
 * @param callback
 */
mongolastic.prototype.getMapping = function(modelname, callback) {
  var elastic = getInstance();

  elastic.connection.indices.getMapping({
    index: elastic.indexNameFromModel(modelname),
    type: modelname
  }, callback);
};

/**
 * Put a new mapping
 * @param modelname
 * @param mapping
 * @param callback
 */
mongolastic.prototype.putMapping = function(modelname, mapping, callback) {
  var elastic = getInstance();

  elastic.connection.indices.putMapping({
    index: elastic.indexNameFromModel(modelname),
    type: modelname,
    body: mapping
  }, callback);
};

/**
 * Index data
 * @param modelname
 * @param entry
 * @param callback
 */
mongolastic.prototype.index = function(modelname, entry, callback) {
  var elastic = getInstance();

  var myid = entry._id.toString();

  elastic.connection.index({
    index: elastic.indexNameFromModel(modelname),
    type: modelname,
    id: myid,
    body: entry
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
    index: elastic.indexNameFromModel(modelname),
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
  var errcount = 0;
  var rescount = 0;
  var doccount = 0;
  var donecount = 0;
  stream.on('data', function (doc) {
    doccount = doccount +1;
    elastic.index(modelname, doc, function(err) {
      donecount = donecount +1;
      if(err) {
        errcount = errcount +1;
      } else {
        rescount = rescount +1;
      }
      if(donecount === doccount) {
        callback(errcount, rescount);
      }
    });
  });
};

/**
 * Delete whole index
 * @param modelname
 * @param callback
 */
mongolastic.prototype.deleteIndex = function deleteIndex(modelname, callback) {
  this.connection.indices.delete({index: this.indexNameFromModel(modelname)}, callback);
};

/**
 * Helper for hamornising namespaces
 * @param modelname
 * @returns {string}
 */
mongolastic.prototype.indexNameFromModel = function(modelname) {
  return this.prefix + '-' + modelname.toLowerCase();
};

module.exports = getInstance();