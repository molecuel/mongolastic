var _ = require('lodash');

/**
 * Wrapper for elasticsearch indices api
 * @param {object} mongolastic
 */
var indices = function (mongolastic) {
  this.instance = mongolastic;
  this.connection = mongolastic.connection;
};

/**
 * Return a boolean indicating whether index for given model exists.
 *
 * @param {string} modelName
 * @param {function} callback
 * @see http://www.elasticsearch.org/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-indices-exists
 */
indices.prototype.exists = function (modelName, callback) {
  var self = this;
  self.connection.indices.exists({
    index: self.instance.getIndexName(modelName)
  }, callback);
};


/**
 * Create an index for model in Elasticsearch.
 *
 * @param {string} modelName
 * @param {object} settings
 * @param {object} mappings
 * @param {function} callback
 * @see http://www.elasticsearch.org/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-indices-create
 */
indices.prototype.create = function (modelName, settings, mappings, callback) {
  var self = this;

  self.connection.indices.create({
    index: self.instance.getIndexName(modelName),
    body: {
      settings: settings,
      mappings: mappings
    }
  }, callback);
};

/**
 * Create a index
 * @param {object} model
 * @param {object} options
 * @param {function} callback
 */
indices.prototype.createByModel = function indexCreate(model, options, callback) {
  var self = this;

  // Check if options are provided
  // or if the options argument is actually the callback
  if (callback === undefined && _.isFunction(options)) {
    callback = options;
    options = {};
  }

  // Ensure settings object
  var settings = _.isPlainObject(options.settings) ? options.settings : {};

  // Render mapping and create index
  self.instance.renderMapping(model, function (err, mapping) {
    if (!err && mapping) {
      self.create(
          model.modelName,
          settings,
          mapping,
          callback);
    }
  });
};

/**
 * Check if the index exists and create a new one by model
 * @param {object} model
 * @param {object} [options]
 * @param {function} callback
 */
indices.prototype.checkCreateByModel = function (model, options, callback) {
  var self = this;

  // Check if options are provided
  // or if the options argument is actually the callback
  if (callback === undefined && _.isFunction(options)) {
    callback = options;
    options = {};
  }

  self.exists(model.modelName, function (err, response) {

    if (err) {
      return callback(err, null);
    }

    if (response) {
      return callback(err, false);
    }

    self.createByModel(model, options, function (err) {
      return callback(err, true);
    });
  });
};

/**
 * Put a new mapping
 * @param {string} modelName
 * @param {object} mapping
 * @param {function} callback
 */
indices.prototype.putMapping = function (modelName, mapping, callback) {
  var self = this;
  this.connection.indices.putMapping({
    index: self.instance.getIndexName(modelName),
    type: modelName,
    body: mapping
  }, callback);
};

/**
 * Get the current mapping
 * @param {string} modelName
 * @param {function} callback
 */
indices.prototype.getMapping = function (modelName, callback) {
  var self = this;
  self.connection.indices.getMapping({
    index: self.instance.getIndexName(modelName),
    type: modelName
  }, callback);
};

/**
 * Get the index settings
 * @param {string} modelName
 * @param {function} callback
 */
indices.prototype.getSettings = function (modelName, callback) {
  var self = this;
  self.connection.indices.getSettings({
    index: self.instance.getIndexName(modelName),
    type: modelName
  }, callback);
};

module.exports = indices;