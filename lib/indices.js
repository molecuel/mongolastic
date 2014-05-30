/**
 * Wrapper for elasticsearch indices api
 * @param mongolastic
 */
var indices = function(mongolastic) {
  this.instance = mongolastic;
  this.connection = mongolastic.connection;
};

/**
 * Return a boolean indicating whether index for given model exists.
 *
 * @param modelname
 * @param callback
 * @see http://www.elasticsearch.org/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-indices-exists
 */
indices.prototype.exists = function(modelname, callback) {
  this.connection.indices.exists({
    index: this.instance.indexNameFromModel(modelname)
  }, callback);
};


/**
 * Create an index for model in Elasticsearch.
 *
 * @param modelname
 * @param settings
 * @param mappings
 * @param callback
 * @see http://www.elasticsearch.org/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-indices-create
 */
indices.prototype.create = function(modelname, settings, mappings, callback) {
  this.connection.indices.create({
    index: this.instance.indexNameFromModel(modelname),
    body: {
      settings: settings,
      mappings: mappings
    }
  }, callback);
};

module.exports = indices;