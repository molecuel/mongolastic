/**
 * Created by dob on 05.05.14.
 */
var assert = require('assert'),
  mongolastic = require('../'),
  should = require('should'),
  mongoose = require('mongoose');

describe('mongolastic', function() {
  //mongoose.set('debug', true);
  var cat;
  var CatSchema;
  var CostumeSchema;
  var costume;
  var DogSchema;
  var dog;
  var FailSchema;
  var myFail;
  var SettingsTestSchema;
  var settingsTest;

  before(function() {
    mongoose.connect('mongodb://localhost/mongolastic');

    CostumeSchema = mongoose.Schema({
      name: {type: String},
      color: {type: String},
      integer: {type: Number, elastic: {mapping: {type: 'integer'}}}
    });
    CostumeSchema.plugin(mongolastic.plugin, {modelName: 'costume'});
    costume = mongoose.model('costume', CostumeSchema);

    CatSchema = mongoose.Schema({
      name: String,
      date: {type: Date, default: Date.now},
      costume: {type: mongoose.Schema.ObjectId, ref: 'costume', elastic: {popfields: 'name'}},
      url: {type: String, elastic: {mapping: {type: 'string', index: 'not_analyzed'}}},
      test: {
        integer: {type: Number, elastic: {mapping: {type: 'integer'}}},
        deep: {
          mystring: {type: String, elastic: {mapping: {type: 'string'}}}
        }
      }
    });
    CatSchema.plugin(mongolastic.plugin, {modelName: 'cat'});
    cat = mongoose.model('cat', CatSchema);

    cat.elastic = {
      mapping: {
        'location.geo': {type: 'geo_point', 'lat_lon': true}
      }
    };

    DogSchema = mongoose.Schema({
      name: String,
      date: {type: Date, default: Date.now},
      costume: {type: mongoose.Schema.ObjectId, ref: 'costume'}
    });
    DogSchema.plugin(mongolastic.plugin, {modelName: 'dog'});
    dog = mongoose.model('dog', DogSchema);

    FailSchema = mongoose.Schema({
      name: String,
      keyword: {type: String, required: true}
    });
    FailSchema.plugin(mongolastic.plugin, {modelName: 'fail'});
    myFail = mongoose.model('fail', FailSchema);

    // Settings test
    SettingsTestSchema = mongoose.Schema({
      name: String
    });
    SettingsTestSchema.plugin(mongolastic.plugin, {modelName: 'settingsTest'});
    settingsTest = mongoose.model('settingsTest', SettingsTestSchema);

  });

  describe('mongolastic', function() {
    it('should be a object', function() {
      assert('object' === typeof mongolastic);
    });
  });

  describe('create connection', function() {
    it('should create a connection', function(done) {
      mongolastic.connect('mongolastic', {
        host: 'localhost:9200'
      }, function(err, conn) {
        should.not.exist(err);
        conn.should.be.an.Object;
        done();
      });
    });

    it('should create the mapping for the cat model', function(done) {
      mongolastic.registerModel(cat, function(err, result) {
        should.not.exist(err);
        result.should.be.a.function;
        done();
      });
    });

    it('should create the mapping for the dog model', function(done) {
      mongolastic.registerModel(dog, function(err, result) {
        should.not.exist(err);
        result.should.be.a.function;
        done();
      });
    });

    it('should create the mapping for the costume model', function(done) {
      mongolastic.registerModel(costume, function(err, result) {
        should.not.exist(err);
        result.should.be.a.function;
        done();
      });
    });

    it('should create the mapping for the myFail model', function(done) {
      mongolastic.registerModel(myFail, function(err, result) {
        should.not.exist(err);
        result.should.be.a.function;
        done();
      });
    });

    it('should return the mappings for the cat model', function(done) {
      mongolastic.indices.getMapping(cat.modelName, function(err, response, status) {
        should.not.exist(err);
        assert(status === 200);
        response['mongolastic-cat'].should.be.object;
        response['mongolastic-cat'].mappings.should.be.object;
        response['mongolastic-cat'].mappings.cat.should.be.object;
        done();
      });
    });

    it('should create the mapping for the settingsTest model', function(done) {

      var options = {
        'settings': {
          'index': {
            'analysis': {
              'filter': {
                'english_stop': {
                  'type': 'stop',
                  'stopwords': '_english_'
                }
              }
            }
          }
        }
      };

      mongolastic.registerModel(settingsTest, options, function(err, result) {
        should.not.exist(err);
        result.should.be.a.function;
        done();
      });
    });

    it('should return the custom index settings for the settingsTest model', function(done) {
      mongolastic.indices.getSettings(settingsTest.modelName, function(err, response, status) {
        should.not.exist(err);
        assert(status === 200);
        response['mongolastic-settingstest'].should.be.object;
        response['mongolastic-settingstest'].settings.should.be.object;
        response['mongolastic-settingstest'].settings.index.should.be.object;
        response['mongolastic-settingstest'].settings.index['uuid'].should.be.a.string;
        response['mongolastic-settingstest'].settings.index['analysis'].should.be.a.object;
        done();
      });
    });

    it('should return default index settings for the cat model', function(done) {
      mongolastic.indices.getSettings(cat.modelName, function(err, response, status) {
        should.not.exist(err);
        assert(status === 200);
        response['mongolastic-cat'].should.be.object;
        response['mongolastic-cat'].settings.should.be.object;
        response['mongolastic-cat'].settings.index.should.be.object;
        response['mongolastic-cat'].settings.index['uuid'].should.be.a.string;
        response['mongolastic-cat'].settings.index.should.not.have.property('analysis');
        done();
      });
    });
  });

  describe('save mongoose model', function() {
    var kitty;
    it('should create a new object in mongoose model', function(done) {
      kitty = new cat({name: 'Zildjian'});
      kitty.save(function(err, result) {
        should.not.exist(err);
        result.should.be.an.Object;
        done();
      });
    });

    it('should update a mongoose object', function(done) {
      this.timeout = 5000;
      kitty.name = 'Zlatko';
      kitty.save(function(err, result) {
        should.not.exist(err);
        result.should.be.an.Object;
        // Add timeout after saving to give elasticsearch some time to index
        setTimeout(function() {
          done();
        }, 1000);
      });
    });

    it('should find the mongoose object', function(done) {
      var query = {
        'body': {
          'query': {
            'match': {'_id': kitty._id}
          }
        }
      };
      kitty.search(query, function(err, result) {
        should.not.exist(err);
        result.hits.hits[0].should.be.an.Object;
        done();
      });
    });

    it('should delete from index', function(done) {
      mongolastic.delete('cat', kitty.id, function(err, result) {
        should.not.exist(err);
        result.should.be.an.Object;
        done();
      });
    });

    it('should reindex mongoose object', function(done) {
      mongolastic.index('cat', kitty, function(err, result) {
        should.not.exist(err);
        result.should.be.an.Object;
      });
      // Add timeout after saving to give elasticsearch some time to index
      setTimeout(function() {
        done();
      }, 1000);
    });

    it('should sync mongodb', function(done) {
      cat.sync(function(errcount, resultcount) {
        errcount.should.eql(0);
        resultcount.should.eql(1);
        done();
      });
    });

    it('should delete the mongoose object', function(done) {
      kitty.remove(function(err) {
        should.not.exist(err);
        done();
      });
    });

    var bat;

    it('should create a new sub object in mongoose model', function(done) {
      bat = new costume({
        name: 'Batman',
        color: 'black'
      });

      mongolastic.save(bat, function(err, result) {
        should.not.exist(err);
        result.should.be.an.Object;
        done();
      });
    });

    it('should create a object with sub object in mongoose model', function(done) {
      var batcat = new cat({
        name: 'Batcat',
        costume: bat._id
      });

      mongolastic.save(batcat, function(err, result) {
        should.not.exist(err);
        result.should.be.an.Object;
        result.costume.should.be.an.Object;
        result.costume._id.should.be.an.Object;
        done();
      });
    });

    it('should create a new object with sub object in mongoose without specified fields', function(done) {
      var dogbat = new dog({
        name: 'DogBat',
        costume: bat._id
      });

      mongolastic.save(dogbat, function(err, result) {
        should.not.exist(err);
        result.should.be.an.Object;
        result.costume.should.be.an.Object;
        result.costume.name.should.be.an.String;
        result.costume.color.should.be.an.String;
        done();
      });
    });

    it('should create a new entry with custom save handler for the object type', function(done) {
      costume.registerSaveHandler(function(err, result, options, callback) {
        result.test = true;
        callback();
      });

      bat = new costume({
        name: 'Batman',
        color: 'black'
      });

      mongolastic.save(bat, function(err, result) {
        should.not.exist(err);
        result.should.be.an.Object;
        result.test.should.be.true;
        done();
      });
    });

    it('should return the mappings for the cat model', function(done) {
      mongolastic.indices.getMapping(cat.modelName, function(err, response, status) {
        should.not.exist(err);
        assert(status === 200);
        response['mongolastic-cat'].should.be.object;
        response['mongolastic-cat'].mappings.should.be.object;
        response['mongolastic-cat'].mappings.cat.should.be.object;
        response['mongolastic-cat'].mappings.cat.properties.should.be.object;
        response['mongolastic-cat'].mappings.cat.properties.test.should.be.object;
        response['mongolastic-cat'].mappings.cat.properties.test.properties.should.be.object;
        response['mongolastic-cat'].mappings.cat.properties.test.properties.deep.should.be.object;
        response['mongolastic-cat'].mappings.cat.properties.test.properties.deep.properties.should.be.object;
        //console.log(JSON.stringify(response));
        done();
      });
    });

    it('should return the correct prefix', function(done) {
      assert.equal(mongolastic.getIndexName('model'), 'mongolastic-model');
      assert.equal(mongolastic.getIndexName('mongolastic-model'), 'mongolastic-model');
      done();
    });

    var testobject3;
    it('should fail to save and remove objects from index', function(done) {
      testobject3 = new myFail({
        title: 'test2name',
        lang: 'en'
      });

      mongolastic.save(testobject3, function(err) {
        var query = {
          'body': {
            'query': {
              'match': {'_id': testobject3._id}
            }
          }
        };
        testobject3.search(query, function(searcherr, result) {
          should.exist(err);
          should.not.exist(searcherr);
          assert(result.hits.total === 0);
          done();
        });
      });
    });
  });

  after(function(done) {
    mongoose.connection.db.dropDatabase(function() {
      mongolastic.deleteIndex('*', function() {
        done();
      });
    });
  });
});
