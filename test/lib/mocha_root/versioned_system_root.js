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

/*jshint -W068 */

if (process.getuid() !== 0) { 
  console.error('run tests as root');
  process.exit(1);
}

var should = require('should');
var mongodb = require('mongodb');

var Timestamp = mongodb.Timestamp;
var VersionedSystem = require('../../../lib/versioned_system');
var logger = require('../../../lib/logger');

var silence;

var db, db2, oplogDb, oplogColl;
var databaseName = 'test_versioned_system_root';
var databaseName2 = 'test2';
var oplogDatabase = 'local';

var databaseNames = [databaseName, databaseName2, 'foo', 'bar'];
var Database = require('../../_database');

// open database connection
var database = new Database(databaseNames);
before(function(done) {
  logger({ silence: true }, function(err, l) {
    if (err) { throw err; }
    silence = l;
    database.connect(function(err, dbs) {
      if (err) { throw err; }
      db = dbs[0];
      db2 = dbs[1];
      oplogDb = db.db(oplogDatabase);
      oplogColl = oplogDb.collection('oplog.$main');
      done();
    });
  });
});

after(function(done) {
  silence.close(function(err) {
    if (err) { throw err; }
    database.disconnect(done);
  });
});

describe('VersionedSystem', function() {
  describe('initVCs root', function() {
    it('needs two documents in collection for further testing', function(done) {
      // insert two new documents in the collection and see if they're versioned using a
      // rebuild (one of which is already versioned). if a new snapshot collection is
      // created, rebuild should maintain original versions.
      var docs = [{ foo: 'a' }, { bar: 'b', _v: 'qux' }];
      db2.collection('someColl').insert(docs, done);
    });

    it('should rebuild a new snapshot collection', function(done) {
      var vcCfg = {
        test2: {
          someColl: {
            logCfg: { silence: true },
            dbPort: 27019,
            autoProcessInterval: 50,
            size: 1
          }
        }
      };

      var vs = new VersionedSystem(oplogColl, { log: silence });
      vs.initVCs(vcCfg, function(err, oplogReaders) {
        if (err) { throw err; }

        should.strictEqual(Object.keys(oplogReaders).length, 1);

        // oplog reader should never end
        var or = oplogReaders['test2.someColl'];
        var illegalEnd = function() { throw new Error('oplog reader closed'); };
        or.on('end', illegalEnd);

        // should detect two updates in test2.someColl and set ackd
        var i = 0;
        or.on('data', function() {
          i++;
          if (i >= 2) {
            // check if items are ackd, but give vc some time to process oplog items first
            setTimeout(function() {
              db2.collection('m3.someColl').find().toArray(function(err, items) {
                if (err) { throw err; }
                should.strictEqual(items.length, 2);
                delete items[0]._id._id;
                delete items[0]._id._v;
                delete items[0]._m3._op;
                delete items[1]._id._id;
                delete items[1]._m3._op;

                should.deepEqual(items[0], {
                  foo: 'a',
                    _id: {
                      _co: 'someColl',
                      _pe: '_local',
                      _pa: [],
                      _lo: true,
                      _i: 1
                    },
                 _m3: { _ack: true }
                });
                should.deepEqual(items[1], {
                  bar: 'b',
                  _id: {
                    _co: 'someColl',
                    _v: 'qux',
                    _pe: '_local',
                    _pa: [],
                    _lo: true,
                    _i: 2
                  },
                _m3: { _ack: true }
                });
                or.removeListener('end', illegalEnd);
                vs.stopTerm(done);
              });
            }, 60);
          }
        });
      });
    });
  });

  describe('info', function() {
    var collName  = 'info';
    var oplogCollName = '_infoOplog';
    var localOplogColl;

    it('needs a capped collection', function(done) {
      database.createCappedColl(oplogCollName, done);
    });

    it('needs an artificial oplog for testing', function(done) {
      var op1 = { ts: new Timestamp(2, 3) };
      var op2 = { ts: new Timestamp(40, 50) };
      var op3 = { ts: new Timestamp(600, 700) };

      localOplogColl = db.collection(oplogCollName);
      localOplogColl.insert([op1, op2, op3], done);
    });

    it('needs a collection with one object and a collection without objects', function(done) {
      var item = { foo: 'bar' };
      var snapshotItem = { foo: 'bar', _m3: { _op: new Timestamp(8000, 9000) } };
      var cfg = {};
      cfg[databaseName] = {};
      cfg[databaseName][collName] = {
        logCfg: { silence: true },
        dbPort: 27019,
        autoProcessInterval: 50,
        size: 1
      };

      var vs = new VersionedSystem(localOplogColl, { log: silence });
      vs.initVCs(cfg, function(err) {
        if (err) { throw err; }
        db.collection('m3.' + collName).insert(snapshotItem, function(err) {
          if (err) { throw err; }
          db.collection(collName).insert(item, function(err) {
            if (err) { throw err; }
            vs.stopTerm(done);
          });
        });
      });
    });

    it('should show info of the collection and the snapshot collection', function(done) {
      var ns = databaseName + '.' + collName;
      var cfg = {};
      cfg[databaseName] = {};
      cfg[databaseName][collName] = {
        logCfg: { silence: true },
        dbPort: 27019,
        autoProcessInterval: 50,
        size: 1
      };

      var vs = new VersionedSystem(localOplogColl, { log: silence });
      vs.initVCs(cfg, function(err) {
        if (err) { throw err; }
        vs.info(function(err, result) {
          if (err) { throw err; }
          should.strictEqual(result[ns].collection.count, 1);
          should.strictEqual(result[ns].collection.capped, undefined);
          should.strictEqual(result[ns].snapshotCollection.count, 1);
          should.strictEqual(result[ns].snapshotCollection.capped, true);
          vs.stopTerm(done);
        });
      });
    });

    it('should show extended info of the collection and the snapshot collection', function(done) {
      var ns = databaseName + '.' + collName;
      var cfg = {};
      cfg[databaseName] = {};
      cfg[databaseName][collName] = {
        logCfg: { silence: true },
        dbPort: 27019,
        autoProcessInterval: 100,
        size: 1
      };

      var vs = new VersionedSystem(localOplogColl, { log: silence });
      vs.initVCs(cfg, function(err) {
        if (err) { throw err; }
        var opts = { extended: true };
        vs.info(opts, function(err, result) {
          should.equal(err, null);
          should.strictEqual(result[ns].collection.count, 1);
          should.strictEqual(result[ns].collection.capped, undefined);
          should.strictEqual(result[ns].snapshotCollection.count, 1);
          should.strictEqual(result[ns].snapshotCollection.capped, true);
          should.strictEqual(result[ns].extended.ack, 0);
          vs.stopTerm(done);
        });
      });
    });

    it('should work with a custom list of nameSpaces', function(done) {
      var ns = databaseName + '.' + collName;
      var cfg = {};
      cfg[databaseName] = {};
      cfg[databaseName][collName] = {
        logCfg: { silence: true },
        dbPort: 27019,
        autoProcessInterval: 50,
        size: 1
      };

      var vs = new VersionedSystem(localOplogColl, { log: silence });
      vs.initVCs(cfg, function(err) {
        if (err) { throw err; }
        var opts = { nsList: Object.keys(vs._vces) };
        vs.info(opts, function(err, result) {
          if (err) { throw err; }
          should.strictEqual(result[ns].collection.count, 1);
          should.strictEqual(result[ns].collection.capped, undefined);
          should.strictEqual(result[ns].snapshotCollection.count, 1);
          should.strictEqual(result[ns].snapshotCollection.capped, true);
          vs.stopTerm(done);
        });
      });
    });

    it('should work with a custom list of nameSpaces and extended option', function(done) {
      var ns = databaseName + '.' + collName;
      var cfg = {};
      cfg[databaseName] = {};
      cfg[databaseName][collName] = {
        logCfg: { silence: true },
        dbPort: 27019,
        autoProcessInterval: 50,
        size: 1
      };

      var vs = new VersionedSystem(localOplogColl, { log: silence });
      vs.initVCs(cfg, function(err) {
        if (err) { throw err; }
        var opts = { extended: false, nsList: Object.keys(vs._vces) };
        vs.info(opts, function(err, result) {
          if (err) { throw err; }
          should.strictEqual(result[ns].collection.count, 1);
          should.strictEqual(result[ns].collection.capped, undefined);
          should.strictEqual(result[ns].snapshotCollection.count, 1);
          should.strictEqual(result[ns].snapshotCollection.capped, true);
          vs.stopTerm(done);
        });
      });
    });
  });
});
