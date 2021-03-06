/**
 * Copyright 2014 Netsend.
 *
 * This file is part of Mastersync.
 *
 * Mastersync is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * Mastersync is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with Mastersync. If not, see <https://www.gnu.org/licenses/>.
 */

'use strict';

/*jshint -W068,-W116, nonew: false */

var should = require('should');
var mongodb = require('mongodb');
var BSON = mongodb.BSON;
var Timestamp = mongodb.Timestamp;
var Readable = require('stream').Readable;

var VersionedCollectionReader = require('../../../lib/versioned_collection_reader');
var logger = require('../../../lib/logger');

var silence;

var db;
var databaseName = 'test_versioned_collection_reader';
var Database = require('../../_database');

// open database connection
var database = new Database(databaseName);
before(function(done) {
  logger({ silence: true }, function(err, l) {
    if (err) { throw err; }
    silence = l;
    database.connect(function(err, dbc) {
      db = dbc;
      done(err);
    });
  });
});

after(function(done) {
  silence.close(function(err) {
    if (err) { throw err; }
    database.disconnect(done);
  });
});

describe('versioned_collection', function() {
  describe('constructor', function() {
    var snapshotCollectionName = 'm3.constructor';
    var snapshotCollection;

    it('needs a capped collection', function(done) {
      snapshotCollection = db.collection(snapshotCollectionName);
      database.createCappedColl(snapshotCollectionName, done);
    });

    it('should require snapshotCollection to be an object', function() {
      (function () { new VersionedCollectionReader(); }).should.throw('snapshotCollection must be an object');
    });

    it('should require opts.localPerspective to be a string', function() {
      (function() {
        new VersionedCollectionReader(snapshotCollection, { localPerspective: {} });
      }).should.throw('opts.localPerspective must be a string');
    });

    it('should default localPerspective to _local', function() {
      var vc = new VersionedCollectionReader(snapshotCollection, { log: silence });
      should.equal(vc._localPerspective, '_local');
    });

    it('should set localPerspective to foo', function() {
      var vc = new VersionedCollectionReader(snapshotCollection, { log: silence, localPerspective: 'foo' });
      should.equal(vc._localPerspective, 'foo');
    });

    it('should require opts.log to be an object', function() {
      (function() {
        new VersionedCollectionReader(snapshotCollection, { log: '' });
      }).should.throw('opts.log must be an object');
    });

    it('should require opts.raw to be a boolean', function() {
      (function() {
        new VersionedCollectionReader(snapshotCollection, { raw: {}, log: silence });
      }).should.throw('opts.raw must be a boolean');
    });

    it('should default _raw to false', function() {
      var vc = new VersionedCollectionReader(snapshotCollection, { log: silence });
      should.equal(vc._raw, false);
    });

    it('should set _raw to true', function() {
      var vc = new VersionedCollectionReader(snapshotCollection, { raw: true, log: silence });
      should.equal(vc._raw, true);
    });

    it('should require opts.filter to be an object', function() {
      (function() {
        new VersionedCollectionReader(snapshotCollection, { filter: 'foo', log: silence });
      }).should.throw('opts.filter must be an object');
    });

    it('should default filter to {}', function() {
      var vc = new VersionedCollectionReader(snapshotCollection, { log: silence });
      should.deepEqual(vc._filter, {});
    });

    it('should set filter to { foo: \'bar\'}', function() {
      var vc = new VersionedCollectionReader(snapshotCollection, { filter: { foo: 'bar' }, log: silence});
      should.deepEqual(vc._filter, {foo: 'bar'});
    });

    it('should require opts.offset to be a string', function() {
      (function() {
        new VersionedCollectionReader(snapshotCollection, { offset: {} });
      }).should.throw('opts.offset must be a string');
    });

    it('should default offset to \'\'', function() {
      var vc = new VersionedCollectionReader(snapshotCollection, { log: silence });
      should.equal(vc._offset, '');
    });

    it('should set offset to \'foo\'', function() {
      var vc = new VersionedCollectionReader(snapshotCollection, { offset: 'foo', log: silence });
      should.equal(vc._offset, 'foo');
    });

    it('should require opts.follow to be a boolean', function() {
      (function() {
        new VersionedCollectionReader(snapshotCollection, { follow: {}, log: silence  });
      }).should.throw('opts.follow must be a boolean');
    });

    it('should default follow to true', function() {
      var vc = new VersionedCollectionReader(snapshotCollection, { log: silence });
      should.equal(vc._follow, true);
    });

    it('should set follow to false', function() {
      var vc = new VersionedCollectionReader(snapshotCollection, { follow: false, log: silence  });
      should.equal(vc._follow, false);
    });

    it('should require opts.hooks to be an array', function() {
      (function() {
        new VersionedCollectionReader(snapshotCollection, { hooks: {}, log: silence  });
      }).should.throw('opts.hooks must be an array');
    });

    it('should default hooks to []', function() {
      var vc = new VersionedCollectionReader(snapshotCollection, { log: silence });
      should.deepEqual(vc._hooks, []);
    });

    it('should set hooks to [\'foo\',\'bar\']', function() {
      var vc = new VersionedCollectionReader(snapshotCollection, { hooks: ['foo', 'bar'], log: silence  });
      should.deepEqual(vc._hooks, ['foo', 'bar']);
    });

    it('should require opts.hooksOpts to be an object', function() {
      (function() {
        new VersionedCollectionReader(snapshotCollection, { hooksOpts: 'foo', log: silence  });
      }).should.throw('opts.hooksOpts must be an object');
    });

    it('should default hooksOpts to {}', function() {
      var vc = new VersionedCollectionReader(snapshotCollection, { log: silence });
      should.deepEqual(vc._hooksOpts, {});
    });

    it('should set hooksOpts to {foo: \'bar\'}', function() {
      var vc = new VersionedCollectionReader(snapshotCollection, { hooksOpts: { foo: 'bar' }, log: silence  } );
      should.deepEqual(vc._hooksOpts, { foo: 'bar'});
    });

    it('should set snapshotCollectionName', function() {
      var vc = new VersionedCollectionReader(snapshotCollection, { log: silence });
      should.equal(vc._snapshotCollectionName, snapshotCollectionName);
    });

    it('should open snapshotCollection', function() {
      var vc = new VersionedCollectionReader(snapshotCollection, { log: silence });
      should.notEqual(vc._snapshotCollection.collectionName, undefined);
    });

    it('should construct', function(done) {
      var vcr = new VersionedCollectionReader(snapshotCollection, { log: silence });
      // needs a data handler resume() to start flowing and needs to flow before an end will be emitted
      vcr.on('end', done);
      vcr.resume();
    });
  });

  describe('stream', function() {
    var snapshotCollectionName = 'm3.stream';
    var snapshotCollection;

    var perspective = 'I';

    var A = {
      _id : { _id: 'foo', _v: 'A', _pe: 'I', _pa: [], _i: 1 },
      _m3: { _ack: true },
      baz : 'qux'
    };

    var B = {
      _id : { _id: 'foo', _v: 'B', _pe: 'I', _pa: ['A'], _i: 2 },
      _m3: { _ack: true },
      foo: 'bar'
    };

    var C = {
      _id : { _id: 'foo', _v: 'C', _pe: 'I', _pa: ['B'], _i: 3 },
      _m3: { _ack: true },
      baz : 'mux',
      foo: 'bar'
    };

    var D = {
      _id : { _id: 'foo', _v: 'D', _pe: 'I', _pa: ['C'], _i: 4 },
      _m3: { _ack: true },
      baz : 'qux'
    };

    var E = {
      _id : { _id: 'foo', _v: 'E', _pe: 'I', _pa: ['B'], _i: 5 },
      _m3: { _ack: true },
    };

    var F = {
      _id : { _id: 'foo', _v: 'F', _pe: 'I', _pa: ['E', 'C'], _i: 6 },
      _m3: { _ack: true },
      foo: 'bar'
    };

    var G = {
      _id : { _id: 'foo', _v: 'G', _pe: 'I', _pa: ['F'], _i: 7 },
      _m3: { _ack: true, _op: new Timestamp(1, 2) },
      baz : 'qux'
    };

    // same but without _id._pe and stripped m3 _ack and m3 _ack
    var rA = { _id : { _id: 'foo', _v: 'A', _pa: [] }, _m3: {},
      baz : 'qux' };
    var rB = { _id : { _id: 'foo', _v: 'B', _pa: ['A'] }, _m3: {},
      foo: 'bar' };
    var rC = { _id : { _id: 'foo', _v: 'C', _pa: ['B'] }, _m3: {},
      baz : 'mux',
      foo: 'bar'  };
    var rD = { _id : { _id: 'foo', _v: 'D', _pa: ['C'] }, _m3: {},
      baz : 'qux' };
    var rE = { _id : { _id: 'foo', _v: 'E', _pa: ['B'] }, _m3: {} };
    var rF = { _id : { _id: 'foo', _v: 'F', _pa: ['E', 'C'] }, _m3: {},
      foo: 'bar' };
    var rG = { _id : { _id: 'foo', _v: 'G', _pa: ['F'] }, _m3: {},
      baz : 'qux' };

    // create the following structure:
    // A <-- B <-- C <-- D
    //        \     \
    //         E <-- F <-- G

    it('needs a capped collection', function(done) {
      snapshotCollection = db.collection(snapshotCollectionName);
      database.createCappedColl(snapshotCollectionName, done);
    });

    it('should work with empty DAG and collection', function(done) {
      var vc = new VersionedCollectionReader(snapshotCollection, { log: silence });
      vc.on('data', function() { throw Error('no data should be emitted'); });
      vc.on('end', done);
    });

    it('should work without offset', function(done) {
      var vc = new VersionedCollectionReader(snapshotCollection, { log: silence });
      vc.resume();
      vc.on('end', done);
    });

    it('should save DAG', function(done) {
      var vc = new VersionedCollectionReader(snapshotCollection, { localPerspective: perspective, follow: false, log: silence });
      vc._snapshotCollection.insert([A, B, C, D, E, F, G], {w: 1}, done);
    });

    it('should return all elements when offset is empty', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var vc = new VersionedCollectionReader(snapshotCollection, { localPerspective: perspective, follow: false, log: silence });
      var docs = [];

      vc.on('data', function(doc) {
        docs.push(doc);
      });

      vc.on('end', function() {
        should.equal(docs.length, 7);
        should.deepEqual(docs, [rA, rB, rC, rD, rE, rF, rG]);
        done();
      });
    });

    it('should return raw buffer instances', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var vc = new VersionedCollectionReader(snapshotCollection, {
        localPerspective: perspective,
        follow: false,
        log: silence,
        raw: true
      });
      var docs = [];

      vc.on('data', function(doc) {
        docs.push(BSON.deserialize(doc));
      });

      vc.on('end', function() {
        should.equal(docs.length, 7);
        should.deepEqual(docs, [rA, rB, rC, rD, rE, rF, rG]);
        done();
      });
    });

    it('should return only the last element if that is the offset', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var vc = new VersionedCollectionReader(snapshotCollection, {
        localPerspective: perspective,
        follow: false,
        log: silence,
        offset: G._id._v
      });
      var docs = [];

      vc.on('data', function(doc) {
        docs.push(doc);
      });

      vc.on('end', function() {
        should.equal(docs.length, 1);
        should.deepEqual(docs, [rG]);
        done();
      });
    });

    it('should return from offset E', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var vc = new VersionedCollectionReader(snapshotCollection, {
        localPerspective: perspective,
        follow: false,
        log: silence,
        offset: E._id._v
      });
      var docs = [];

      vc.on('data', function(doc) {
        docs.push(doc);
      });

      vc.on('end', function() {
        should.equal(docs.length, 3);
        should.deepEqual(docs[0], rE);
        should.deepEqual(docs[1], rF);
        should.deepEqual(docs[2], rG);
        done();
      });
    });

    it('should return everything since offset C (including E)', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var vc = new VersionedCollectionReader(snapshotCollection, {
        localPerspective: perspective,
        follow: false,
        log: silence,
        offset: C._id._v
      });
      var docs = [];

      vc.on('data', function(doc) {
        docs.push(doc);
      });

      vc.on('end', function() {
        should.equal(docs.length, 5);
        should.deepEqual(docs[0], rC);
        should.deepEqual(docs[1], rD);
        should.deepEqual(docs[2], rE);
        should.deepEqual(docs[3], rF);
        should.deepEqual(docs[4], rG);
        done();
      });
    });

    it('should return the complete DAG if filter is empty', function(done) {
      // use tailable is false to stop emitting documents after the last found doc
      var vc = new VersionedCollectionReader(snapshotCollection, {
        localPerspective: perspective,
        follow: false,
        log: silence,
        offset: A._id._v
      });
      var docs = [];

      vc.on('data', function(doc) {
        docs.push(doc);
      });

      vc.on('end', function() {
        should.equal(docs.length, 7);
        should.deepEqual(docs[0], rA);
        should.deepEqual(docs[1], rB);
        should.deepEqual(docs[2], rC);
        should.deepEqual(docs[3], rD);
        should.deepEqual(docs[4], rE);
        should.deepEqual(docs[5], rF);
        should.deepEqual(docs[6], rG);
        done();
      });
    });

    it('should not endup with two same parents A for G since F is a merge but not selected', function(done) {
      // should not find A twice for merge F
      var vc = new VersionedCollectionReader(snapshotCollection, {
        localPerspective: perspective,
        follow: false,
        log: silence,
        offset: A._id._v,
        filter: { baz: 'qux' }
      });
      var docs = [];

      vc.on('data', function(doc) {
        docs.push(doc);
      });

      vc.on('end', function() {
        should.equal(docs.length, 3);
        should.deepEqual(docs[0], { _id : { _id: 'foo', _v: 'A', _pa: [] }, _m3: {}, baz : 'qux' });
        should.deepEqual(docs[1], { _id : { _id: 'foo', _v: 'D', _pa: ['A'] }, _m3: {}, baz : 'qux' });
        should.deepEqual(docs[2], { _id : { _id: 'foo', _v: 'G', _pa: ['A'] }, _m3: {}, baz : 'qux' });
        done();
      });
    });

    it('should return only attrs with baz = mug and change root to C', function(done) {
      // should not find A twice for merge F
      var vc = new VersionedCollectionReader(snapshotCollection, {
        localPerspective: perspective,
        follow: false,
        log: silence,
        offset: A._id._v,
        filter: { baz: 'mux' }
      });
      var docs = [];

      vc.on('data', function(doc) {
        docs.push(doc);
      });

      vc.on('end', function() {
        should.equal(docs.length, 1);
        should.deepEqual(docs[0], { _id : { _id: 'foo', _v: 'C', _pa: [] }, _m3: {}, baz : 'mux', foo: 'bar' });
        done();
      });
    });

    it('should return only attrs with foo = bar and change root to B and alter subsequent parents', function(done) {
      // should not find A twice for merge F
      var vc = new VersionedCollectionReader(snapshotCollection, {
        localPerspective: perspective,
        follow: false,
        log: silence,
        offset: A._id._v,
        filter: { foo: 'bar' }
      });
      var docs = [];

      vc.on('data', function(doc) {
        docs.push(doc);
      });

      vc.on('end', function() {
        should.equal(docs.length, 3);
        should.deepEqual(docs[0], { _id : { _id: 'foo', _v: 'B', _pa: [] }, _m3: {}, foo: 'bar' });
        should.deepEqual(docs[1], { _id : { _id: 'foo', _v: 'C', _pa: ['B'] }, _m3: {}, baz: 'mux', foo: 'bar' });
        should.deepEqual(docs[2], { _id : { _id: 'foo', _v: 'F', _pa: ['B', 'C'] }, _m3: {}, foo: 'bar' });
        done();
      });
    });

    it('should return nothing if filters don\'t match any item', function(done) {
      // should not find A twice for merge F
      var vc = new VersionedCollectionReader(snapshotCollection, {
        localPerspective: perspective,
        follow: false,
        log: silence,
        offset: A._id._v,
        filter: { some: 'none' }
      });
      var docs = [];

      vc.on('data', function(doc) {
        docs.push(doc);
      });

      vc.on('end', function() {
        should.equal(docs.length, 0);
        done();
      });
    });

    it('should execute each hook', function(done) {
      // should not find A twice for merge F
      function transform(db, object, opts, callback) {
        delete object.baz;
        object.checked = true;
        if (i === 1) {
          object.checked = false;
        }
        i++;
        callback(null, object);
      }
      function hook1(db, object, opts, callback) {
        object.hook1 = true;
        if (object._id._v === 'G') { object.hook1g = 'foo'; }
        callback(null, object);
      }
      function hook2(db, object, opts, callback) {
        if (object.hook1) {
          object.hook2 = true;
        }
        callback(null, object);
      }

      var vc = new VersionedCollectionReader(snapshotCollection, {
        localPerspective: perspective,
        follow: false,
        log: silence,
        offset: A._id._v,
        filter: { baz: 'qux' },
        hooks: [transform, hook1, hook2]
      });
      var i = 0;

      var docs = [];

      vc.on('data', function(doc) {
        docs.push(doc);
      });

      vc.on('end', function() {
        should.equal(docs.length, 3);
        should.deepEqual(docs[0], { _id : { _id: 'foo', _v: 'A', _pa: [] }, _m3: {}, checked : true, hook1: true, hook2: true });
        should.deepEqual(docs[1], { _id : { _id: 'foo', _v: 'D', _pa: ['A'] }, _m3: {}, checked : false, hook1: true, hook2: true});
        should.deepEqual(docs[2],
          { _id : { _id: 'foo', _v: 'G', _pa: ['A'] }, _m3: {}, checked : true, hook1g: 'foo', hook1: true, hook2: true});
        done();
      });
    });

    it('should cancel hook execution and skip item if one hook filters', function(done) {
      // should not find A twice for merge F

      function transform(db, object, opts, callback) {
        delete object.baz;
        object.transformed = true;
        callback(null, object);
      }
      // filter F. G should get parents of F which are E and C
      function hook1(db, object, opts, callback) {
        if (object._id._v === 'F') {
          return callback(null, null);
        }
        callback(null, object);
      }
      function hook2(db, object, opts, callback) {
        object.hook2 = true;
        callback(null, object);
      }

      var vc = new VersionedCollectionReader(snapshotCollection, {
        localPerspective: perspective,
        follow: false,
        log: silence,
        offset: E._id._v,
        hooks: [transform, hook1, hook2]
      });

      var docs = [];

      vc.on('data', function(doc) {
        docs.push(doc);
      });

      vc.on('end', function() {
        should.equal(docs.length, 2);
        should.deepEqual(docs[0], { _id : { _id: 'foo', _v: 'E', _pa: ['B'] }, _m3: {}, transformed : true, hook2: true });
        should.deepEqual(docs[1], { _id : { _id: 'foo', _v: 'G', _pa: ['E', 'C'] }, _m3: {}, transformed : true, hook2: true});
        done();
      });
    });

    it('should return only attrs with foo = bar and change root to B and alter subsequent parents, filtered by hook', function(done) {
      function hook(db, object, opts, callback) {
        if (object.foo === 'bar') {
          return callback(null, object);
        }
        callback(null, null);
      }

      var vc = new VersionedCollectionReader(snapshotCollection, {
        localPerspective: perspective,
        follow: false,
        log: silence,
        offset: A._id._v,
        hooks: [hook]
      });
      var docs = [];

      vc.on('data', function(doc) {
        docs.push(doc);
      });

      vc.on('end', function() {
        should.equal(docs.length, 3);
        should.deepEqual(docs[0], { _id : { _id: 'foo', _v: 'B', _pa: [] }, _m3: {}, foo: 'bar' });
        should.deepEqual(docs[1], { _id : { _id: 'foo', _v: 'C', _pa: ['B'] }, _m3: {}, baz: 'mux', foo: 'bar' });
        should.deepEqual(docs[2], { _id : { _id: 'foo', _v: 'F', _pa: ['B', 'C'] }, _m3: {}, foo: 'bar' });
        done();
      });
    });

    it('should return only attrs with foo = bar and change root to B and alter subsequent parents, filtered by hook and offset = B', function(done) {
      function hook(db, object, opts, callback) {
        if (object.foo === 'bar') {
          return callback(null, object);
        }
        callback(null, null);
      }

      var vc = new VersionedCollectionReader(snapshotCollection, {
        localPerspective: perspective,
        follow: false,
        log: silence,
        offset: B._id._v,
        hooks: [hook]
      });

      var docs = [];

      vc.on('data', function(doc) {
        docs.push(doc);
      });

      vc.on('end', function() {
        should.equal(docs.length, 3);
        should.deepEqual(docs[0], { _id : { _id: 'foo', _v: 'B', _pa: [] }, _m3: {}, foo: 'bar' });
        should.deepEqual(docs[1], { _id : { _id: 'foo', _v: 'C', _pa: ['B'] }, _m3: {}, baz: 'mux', foo: 'bar' });
        should.deepEqual(docs[2], { _id : { _id: 'foo', _v: 'F', _pa: ['B', 'C'] }, _m3: {}, foo: 'bar' });
        done();
      });
    });

    it('should be a readable stream', function(done) {
      var vc = new VersionedCollectionReader(snapshotCollection, { log: silence });
      should.strictEqual(vc instanceof Readable, true);
      done();
    });
  });

  describe('close', function() {
    var snapshotCollectionName = 'm3.close';
    var snapshotCollection;

    it('needs a capped collection', function(done) {
      snapshotCollection = db.collection(snapshotCollectionName);
      database.createCappedColl(snapshotCollectionName, done);
    });

    it('should close', function(done) {
      // should not find A twice for merge F
      var vc = new VersionedCollectionReader(snapshotCollection, { follow: true, log: silence });

      vc.on('end', done);
      vc.resume();
      vc.close();
    });
  });
});
