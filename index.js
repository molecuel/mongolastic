/**
 * Created by dob on 05.05.14.
 */
var elasticsearch = require('elasticsearch');
var indices = require('./lib/indices');
var instance;

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
      elastic.index(options.modelname, this, function(err) {
        if(!err) {
          next();
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

mongolastic.prototype.getMapping = function(modelname, callback) {
  var elastic = getInstance();

  elastic.connection.indices.getMapping({
    index: elastic.indexNameFromModel(modelname),
    type: modelname
  }, callback);
};

mongolastic.prototype.putMapping = function(modelname, mapping, callback) {
  var elastic = getInstance();

  console.log(mapping);

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
}

module.exports = getInstance();