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

/*jshint -W068, -W030 */

var should = require('should');

var syncAttr = require('../../../lib/sync_attr');

var collName1 = 'syncAttr1';
var collName2 = 'syncAttr2';
var tmpColl;

var db;
var databaseName = 'test_sync_attr';
var Database = require('../../_database');

// open database connection
var database = new Database(databaseName);
before(function(done) {
  database.connect(function(err, dbc) {
    if (err) { throw err; }
    db = dbc;
    tmpColl = db.collection(collName1+'.tmp');
    done(err);
  });
});

after(database.disconnect.bind(database));

describe('syncAttr', function() {
  describe('constructor', function() {
    var coll1, coll2;

    it('needs collections', function(done) {
      coll1 = db.collection(collName1);
      coll2 = db.collection(collName2);
      done();
    });

    it('should require coll1 to be a mongodb.Collection', function() {
      (function() { syncAttr(); }).should.throw('coll1 must be a mongodb.Collection');
    });

    it('should require coll2 to be a mongodb.Collection', function() {
      (function() { syncAttr(coll1); }).should.throw('coll2 must be a mongodb.Collection');
    });

    it('should require tmpColl to be a mongodb.Collection', function() {
      (function() { syncAttr(coll1, coll2); }).should.throw('tmpColl must be a mongodb.Collection');
    });

    it('should require attr to be a string', function() {
      (function() { syncAttr(coll1, coll2, tmpColl); }).should.throw('attr must be a string');
    });

    it('should require cb to be a function (without opts)', function() {
      (function() { syncAttr(coll1, coll2, tmpColl, 'foo'); }).should.throw('cb must be a function');
    });

    it('should require cb to be a function (with opts)', function() {
      (function() { syncAttr(coll1, coll2, tmpColl, 'foo', {}); }).should.throw('cb must be a function');
    });

    it('should require opts to be an object', function() {
      (function() { syncAttr(coll1, coll2, tmpColl, 'foo', 'foo', function() {}); }).should.throw('opts must be an object');
    });

    it('should require opts.includeAttrs to not exist', function() {
      (function() { syncAttr(coll1, coll2, tmpColl, 'foo', { includeAttrs: 'foo' }, function() {}); }).should.throw('use of opts.includeAttrs is prohibited');
    });

    it('should require opts.excludeAttrs to not exist', function() {
      (function() { syncAttr(coll1, coll2, tmpColl, 'foo', { excludeAttrs: 'foo' }, function() {}); }).should.throw('use of opts.excludeAttrs is prohibited');
    });

    it('should require opts.matchAttrs', function() {
      (function() { syncAttr(coll1, coll2, tmpColl, '_id', { }, function() {}); }).should.throw('provide opts.matchAttrs');
    });

    it('should return without error', function(done) {
      syncAttr(coll1, coll2, tmpColl, 'foo', { matchAttrs: { 'bar': true } }, function(err) {
        if (err) { throw err; }
        done();
      });
    });
  });

  describe('non unique objects', function() {
    var coll1, coll2;

    it('needs collections', function(done) {
      coll1 = db.collection(collName1 + 'Multiple');
      coll2 = db.collection(collName2 + 'Multiple');
      done();
    });

    it('needs some objects in both collections for further testing', function(done) {
      var itemIS = { _id: 'S', bar: 'baz1', _v: 'B' };
      var itemIT = { _id: 'T', bar: 'baz2', _v: 'B' };
      var itemIU = { _id: 'U', bar: 'baz2', _v: 'B' };

      var itemIIV = { _id: 'V', bar: 'baz1', qux: 'raboof', _v: 'A' };
      var itemIIW = { _id: 'W', bar: 'baz1', qux: 'raboof', _v: 'A' };
      var itemIIX = { _id: 'X', bar: 'baz3', qux: 'raboof', _v: 'A' };
      var itemIIY = { _id: 'Y', bar: 'baz2', qux: 'raboof', _v: 'A' };
      var itemIIZ = { _id: 'Z', bar: 'baz2', qux: 'raboof', _v: 'A' };

      coll1.insert([itemIS, itemIT, itemIU], function(err, inserted) {
        if (err) { throw err; }
        should.equal(inserted.length, 3);

        coll2.insert([itemIIV, itemIIW, itemIIX, itemIIY, itemIIZ], done);
      });
    });

    it('should require the conditions not to resolve to multiple elements', function(done) {
      syncAttr(coll1, coll2, tmpColl, '_id', { debug: false, matchAttrs: { bar: true } }, function(err) {
        should.equal(err.message, 'ambiguous elements in collection2');
        done();
      });
    });
  });

  describe('direct strategy', function() {
    var coll1, coll2;

    it('needs collections', function(done) {
      coll1 = db.collection(collName1 + 'DirectStrategyMatch');
      coll2 = db.collection(collName2 + 'DirectStrategyMatch');
      done();
    });

    it('needs some objects in both collections for further testing', function(done) {
      var itemIS = { _id: 'S', bar: 'baz', _v: 'B', an: ['foo', 'bar'] };
      var itemIT = { _id: 'T', bar: 'baz', _v: 'B', an: ['foo', 'b4r'] };
      var itemIU = { _id: 'U', bar: 'baz', _v: 'B', an: ['f00', 'bar'] };

      var itemIIS = { _id: 'S', qux: 'raboof', _v: 'A', an: ['f00', 'b4r'] };
      var itemIIT = { _id: 'T', qux: 'raboof', _v: 'A', an: ['foo', 'bar'] };
      var itemIIW = { _id: 'W', qux: 'raboof', _v: 'A', an: ['f00', 'bar'] };

      coll1.insert([itemIS, itemIT, itemIU], function(err, inserted) {
        if (err) { throw err; }
        should.equal(inserted.length, 3);

        coll2.insert([itemIIS, itemIIT, itemIIW], function(err, inserted) {
          if (err) { throw err; }
          should.equal(inserted.length, 3);
          done();
        });
      });
    });

    it('should not sync bar because matched on _v', function(done) {
      syncAttr(coll1, coll2, tmpColl, 'bar', { debug: false, matchAttrs: { _v: true } }, function(err, updated) {
        if (err) { throw err; }
        should.strictEqual(updated, 0);

        coll1.find({}, { sort: '_id' }).toArray(function(err, items1) {
          if (err) { throw err; }
          should.equal(items1.length, 3);

          coll2.find({}, { sort: '_id' }).toArray(function(err, items2) {
            if (err) { throw err; }
            should.equal(items2.length, 3);

            should.deepEqual(items1, [
              { _id: 'S', bar: 'baz', _v: 'B', an: ['foo', 'bar'] },
              { _id: 'T', bar: 'baz', _v: 'B', an: ['foo', 'b4r'] },
              { _id: 'U', bar: 'baz', _v: 'B', an: ['f00', 'bar'] }
            ]);
            should.deepEqual(items2, [
              { _id: 'S', qux: 'raboof', _v: 'A', an: ['f00', 'b4r'] },
              { _id: 'T', qux: 'raboof', _v: 'A', an: ['foo', 'bar'] },
              { _id: 'W', qux: 'raboof', _v: 'A', an: ['f00', 'bar'] }
            ]);
            done();
          });
        });
      });
    });

    it('should not sync _v because matched on qux', function(done) {
      syncAttr(coll1, coll2, tmpColl, '_v', { debug: false, matchAttrs: { qux: true } }, function(err, updated) {
        if (err) { throw err; }
        should.strictEqual(updated, 0);

        coll1.find({}, { sort: '_id' }).toArray(function(err, items1) {
          if (err) { throw err; }
          should.equal(items1.length, 3);

          coll2.find({}, { sort: '_id' }).toArray(function(err, items2) {
            if (err) { throw err; }
            should.equal(items2.length, 3);

            should.deepEqual(items1, [
              { _id: 'S', bar: 'baz', _v: 'B', an: ['foo', 'bar'] },
              { _id: 'T', bar: 'baz', _v: 'B', an: ['foo', 'b4r'] },
              { _id: 'U', bar: 'baz', _v: 'B', an: ['f00', 'bar'] }
            ]);
            should.deepEqual(items2, [
              { _id: 'S', qux: 'raboof', _v: 'A', an: ['f00', 'b4r'] },
              { _id: 'T', qux: 'raboof', _v: 'A', an: ['foo', 'bar'] },
              { _id: 'W', qux: 'raboof', _v: 'A', an: ['f00', 'bar'] }
            ]);
            done();
          });
        });
      });
    });

    it('should sync _v because matched on _id', function(done) {
      syncAttr(coll1, coll2, tmpColl, '_v', { debug: false, matchAttrs: { _id: true } }, function(err, updated) {
        if (err) { throw err; }
        should.strictEqual(updated, 2);

        coll1.find({}, { sort: '_id' }).toArray(function(err, items1) {
          if (err) { throw err; }
          should.equal(items1.length, 3);

          coll2.find({}, { sort: '_id' }).toArray(function(err, items2) {
            if (err) { throw err; }
            should.equal(items2.length, 3);

            should.deepEqual(items1, [
              { _id: 'S', bar: 'baz', _v: 'A', an: ['foo', 'bar'] },
              { _id: 'T', bar: 'baz', _v: 'A', an: ['foo', 'b4r'] },
              { _id: 'U', bar: 'baz', _v: 'B', an: ['f00', 'bar'] }
            ]);
            should.deepEqual(items2, [
              { _id: 'S', qux: 'raboof', _v: 'A', an: ['f00', 'b4r'] },
              { _id: 'T', qux: 'raboof', _v: 'A', an: ['foo', 'bar'] },
              { _id: 'W', qux: 'raboof', _v: 'A', an: ['f00', 'bar'] }
            ]);
            done();
          });
        });
      });
    });

    it('should sync "an" (array) because matched on _id', function(done) {
      syncAttr(coll1, coll2, tmpColl, 'an', { debug: false, matchAttrs: { _id: true } }, function(err, updated) {
        if (err) { throw err; }
        should.strictEqual(updated, 2);

        coll1.find({}, { sort: '_id' }).toArray(function(err, items1) {
          if (err) { throw err; }
          should.equal(items1.length, 3);

          coll2.find({}, { sort: '_id' }).toArray(function(err, items2) {
            if (err) { throw err; }
            should.equal(items2.length, 3);

            should.deepEqual(items1, [
              { _id: 'S', bar: 'baz', _v: 'A', an: ['f00', 'b4r'] },
              { _id: 'T', bar: 'baz', _v: 'A', an: ['foo', 'bar'] },
              { _id: 'U', bar: 'baz', _v: 'B', an: ['f00', 'bar'] }
            ]);
            should.deepEqual(items2, [
              { _id: 'S', qux: 'raboof', _v: 'A', an: ['f00', 'b4r'] },
              { _id: 'T', qux: 'raboof', _v: 'A', an: ['foo', 'bar'] },
              { _id: 'W', qux: 'raboof', _v: 'A', an: ['f00', 'bar'] }
            ]);
            done();
          });
        });
      });
    });
  });

  describe('temp strategy, unique object', function() {
    var coll1, coll2;

    it('needs collections', function(done) {
      coll1 = db.collection(collName1 + 'TempStrategyInclude');
      coll2 = db.collection(collName2 + 'TempStrategyInclude');
      done();
    });

    it('needs some objects in both collections for further testing', function(done) {
      var itemIS = { _id: 'S', bar: 'baz1', _v: 'B' };
      var itemIT = { _id: 'T', bar: 'baz2', _v: 'B' };
      var itemIU = { _id: 'U', bar: 'baz3', _v: 'B' };

      var itemIIV = { _id: 'V', bar: 'baz1', qux: 'raboof', _v: 'A' };
      var itemIIW = { _id: 'W', bar: 'baz3', qux: 'raboof', _v: 'A' };
      var itemIIX = { _id: 'X', bar: 'baz4', qux: 'raboof', _v: 'A' };
      var itemIIY = { _id: 'Y', bar: 'baz2', qux: 'raboof', _v: 'A' };

      coll1.insert([itemIS, itemIT, itemIU], function(err, inserted) {
        if (err) { throw err; }
        should.equal(inserted.length, 3);

        coll2.insert([itemIIV, itemIIW, itemIIX, itemIIY], function(err, inserted) {
          if (err) { throw err; }
          should.equal(inserted.length, 4);
          done();
        });
      });
    });

    it('should not sync _id because matched on _v and qux', function(done) {
      syncAttr(coll1, coll2, tmpColl, '_id', { debug: false, matchAttrs: { _v: true, qux: true } }, function(err, updated) {
        if (err) { throw err; }
        should.strictEqual(updated, 0);

        coll1.find({}, { sort: '_id' }).toArray(function(err, items1) {
          if (err) { throw err; }
          should.equal(items1.length, 3);

          coll2.find({}, { sort: '_id' }).toArray(function(err, items2) {
            if (err) { throw err; }
            should.equal(items2.length, 4);

            should.deepEqual(items1, [
              { _id: 'S', bar: 'baz1', _v: 'B' },
              { _id: 'T', bar: 'baz2', _v: 'B' },
              { _id: 'U', bar: 'baz3', _v: 'B' }
            ]);
            should.deepEqual(items2, [
              { _id: 'V', bar: 'baz1', qux: 'raboof', _v: 'A' },
              { _id: 'W', bar: 'baz3', qux: 'raboof', _v: 'A' },
              { _id: 'X', bar: 'baz4', qux: 'raboof', _v: 'A' },
              { _id: 'Y', bar: 'baz2', qux: 'raboof', _v: 'A' }
            ]);
            done();
          });
        });
      });
    });

    it('should not sync _id because matched on _v', function(done) {
      syncAttr(coll1, coll2, tmpColl, '_id', { debug: false, matchAttrs: { _v: true } }, function(err, updated) {
        if (err) { throw err; }
        should.strictEqual(updated, 0);

        coll1.find({}, { sort: '_id' }).toArray(function(err, items1) {
          if (err) { throw err; }
          should.equal(items1.length, 3);

          coll2.find({}, { sort: '_id' }).toArray(function(err, items2) {
            if (err) { throw err; }
            should.equal(items2.length, 4);

            should.deepEqual(items1, [
              { _id: 'S', bar: 'baz1', _v: 'B' },
              { _id: 'T', bar: 'baz2', _v: 'B' },
              { _id: 'U', bar: 'baz3', _v: 'B' }
            ]);
            should.deepEqual(items2, [
              { _id: 'V', bar: 'baz1', qux: 'raboof', _v: 'A' },
              { _id: 'W', bar: 'baz3', qux: 'raboof', _v: 'A' },
              { _id: 'X', bar: 'baz4', qux: 'raboof', _v: 'A' },
              { _id: 'Y', bar: 'baz2', qux: 'raboof', _v: 'A' }
            ]);
            done();
          });
        });
      });
    });

    it('should not sync _id because matched on qux', function(done) {
      syncAttr(coll1, coll2, tmpColl, '_id', { debug: false, matchAttrs: { qux: true } }, function(err, updated) {
        if (err) { throw err; }
        should.strictEqual(updated, 0);

        coll1.find({}, { sort: '_id' }).toArray(function(err, items1) {
          if (err) { throw err; }
          should.equal(items1.length, 3);

          coll2.find({}, { sort: '_id' }).toArray(function(err, items2) {
            if (err) { throw err; }
            should.equal(items2.length, 4);

            should.deepEqual(items1, [
              { _id: 'S', bar: 'baz1', _v: 'B' },
              { _id: 'T', bar: 'baz2', _v: 'B' },
              { _id: 'U', bar: 'baz3', _v: 'B' }
            ]);
            should.deepEqual(items2, [
              { _id: 'V', bar: 'baz1', qux: 'raboof', _v: 'A' },
              { _id: 'W', bar: 'baz3', qux: 'raboof', _v: 'A' },
              { _id: 'X', bar: 'baz4', qux: 'raboof', _v: 'A' },
              { _id: 'Y', bar: 'baz2', qux: 'raboof', _v: 'A' }
            ]);
            done();
          });
        });
      });
    });

    it('should sync _id because matched on bar', function(done) {
      syncAttr(coll1, coll2, tmpColl, '_id', { debug: false, matchAttrs: { bar: true } }, function(err, updated) {
        if (err) { throw err; }
        should.strictEqual(updated, 3);

        coll1.find({}, { sort: '_id' }).toArray(function(err, items1) {
          if (err) { throw err; }
          should.equal(items1.length, 3);

          coll2.find({}, { sort: '_id' }).toArray(function(err, items2) {
            if (err) { throw err; }
            should.equal(items2.length, 4);

            should.deepEqual(items1, [
              { _id: 'V', bar: 'baz1', _v: 'B' },
              { _id: 'W', bar: 'baz3', _v: 'B' },
              { _id: 'Y', bar: 'baz2', _v: 'B' }
            ]);
            should.deepEqual(items2, [
              { _id: 'V', bar: 'baz1', qux: 'raboof', _v: 'A' },
              { _id: 'W', bar: 'baz3', qux: 'raboof', _v: 'A' },
              { _id: 'X', bar: 'baz4', qux: 'raboof', _v: 'A' },
              { _id: 'Y', bar: 'baz2', qux: 'raboof', _v: 'A' }
            ]);
            done();
          });
        });
      });
    });
  });
});
