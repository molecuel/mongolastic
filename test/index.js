/**
 * Created by dob on 05.05.14.
 */
var assert = require('assert'),
  mongolastic = require('../'),
  should = require('should'),
  mongoose = require('mongoose');

describe('mongolastic', function(){
  var cat, CatSchema;

  before(function() {
    mongoose.connect('mongodb://localhost/mongolastic');
    CatSchema = mongoose.Schema({
      name: String,
      date: {type: Date, default: Date.now}
    });
    CatSchema.plugin(mongolastic.plugin, {modelname: 'cat'});
    cat = mongoose.model('cat', CatSchema);
  });

  describe('mongolastic', function () {
    it('should be a object', function () {
      assert('object' === typeof mongolastic);
    });
  });

  describe('create connection', function(){
    it('should create a connection', function(done){
      mongolastic.connect('mongolastic', {
        host: 'localhost:9200',
        sniffOnStart: true
      }, function(err, conn) {
        should.not.exist(err);
        conn.should.be.an.Object;
        done();
      });
    });
  });

  describe('save mongoose model', function() {
    var kitty;
    it('should create a new object in mongoose model', function(done) {
      kitty = new cat({ name: 'Zildjian' });
      kitty.save(function (err, result) {
        should.not.exist(err);
        result.should.be.an.Object;
        done();
      });
    });

    it('should update a mongoose object', function(done) {
      kitty.name = 'Zlatko';
      kitty.save(function (err, result) {
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

    it('should delete the mongoose object', function(done) {
      kitty.remove(function(err) {
        should.not.exist(err);
        done();
      });
    });
  });

  after(function(done) {
    mongoose.connection.db.dropDatabase(function() {
      mongolastic.deleteIndex('cat', function() {
        done();
      });
    });
  });
});