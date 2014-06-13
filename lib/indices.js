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

/**
 * Create a index
 * @param modelname
 * @param callback
 */
indices.prototype.createByModel = function indexCreate(model, callback) {
  this.instance.renderMapping(model, function(err, mapping) {
    if(!err && mapping) {
      this.create(
        model.modelName,
        {},
        mapping,
        callback);
    }
  });
};

/**
 * Check if the index exists and create a new one by model
 * @param model
 * @param callback
 */
indices.prototype.checkCreateByModel = function(model, callback) {
  this.exists(model.modelName, function(err, response) {
    if(!response) {
      this.createByModel(model, function (err) {
        callback(err, true);
      });
    } else {
      callback(err, false);
    }
  });
};

/**
 * Put a new mapping
 * @param modelname
 * @param mapping
 * @param callback
 */
indices.prototype.putMapping = function(modelname, mapping, callback) {
  this.connection.indices.putMapping({
    index: this.instance.indexNameFromModel(modelname),
    type: modelname,
    body: mapping
  }, callback);
};

/**
 * Get the current mapping
 * @param modelname
 * @param callback
 */
indices.prototype.getMapping = function(modelname, callback) {
  this.connection.indices.getMapping({
    index: this.instance.indexNameFromModel(modelname),
    type: modelname
  }, callback);
};

module.exports = indices;