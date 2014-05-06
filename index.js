/**
 * Created by dob on 05.05.14.
 */
var elasticsearch = require('elasticsearch');
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

  } else {
    console.log('missing modelname');
  }
};

/**
 * Index data
 * @param modelname
 * @param entry
 * @param callback
 */
mongolastic.prototype.index = function(modelname, entry, callback) {
  var elastic = getInstance();

  elastic.connection.index({
    index: elastic.prefix + '-' + modelname,
    type: modelname,
    id: entry.id,
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
    index: elastic.prefix + '-' + modelname,
    type: modelname,
    id: id
  }, callback);
};

mongolastic.prototype.search = function(query, callback) {
  var elastic = getInstance();
  if(!query.index) {
    query.index = this.prefix + '-*';
  }
  elastic.connection.search(query, callback);
};

mongolastic.prototype.deleteIndex = function deleteIndex(modelname, callback) {
  this.connection.indices.delete({index: this.prefix + '-' +modelname}, callback);
};

module.exports = getInstance();