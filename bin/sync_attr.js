#!/usr/bin/env node

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

var program = require('commander');
var async = require('async');
var mongodb = require('mongodb');
var ObjectID = mongodb.ObjectID;

var _db = require('./_db');
var properties = require('properties');
var fs = require('fs');

var syncAttr = require('../lib/sync_attr');

program
  .version('0.0.1')
  .usage('[-f config] [-v] -a database [-b database] -c collection [-d collection] -m attr(s) attr')
  .description('synchronize attr on items in a.c from corresponding items in b.d')
  .option('-a, --database1 <database>', 'name of the database for collection1')
  .option('-b, --database2 <database>', 'name of the database for collection2 if different from database1')
  .option('-c, --collection1 <collection>', 'name of the collection to report about')
  .option('-d, --collection2 <collection>', 'name of the collection to compare against if different from collection1')
  .option('-f, --config <config>', 'ini config file with database access credentials')
  .option('-m, --match <attrs>', 'comma separated list of attributes that should match', function(val) { return val.split(','); })
  .option('    --ids <ids>', 'comma separated list of (string) ids to copy attr from b.d to a.c', function(val) { return val.split(','); })
  .option('    --oids <oids>', 'comma separated list of object ids to copy attr from b.d to a.c', function(val) { return val.split(','); })
  .option('-v, --verbose', 'verbose')
  .parse(process.argv);

if (!program.database1) { program.help(); }
if (!program.database2) { program.database2 = program.database1; }
if (!program.collection1) { program.help(); }
if (!program.collection2) { program.collection2 = program.collection1; }

if (!program.args[0]) { program.help(); }

var config = {};
var dbCfg = { dbName: program.database1 };

// if relative, prepend current working dir
if (program.config) {
  config = program.config;
  if (config[0] !== '/') {
    config = process.cwd() + '/' + config;
  }

  config = properties.parse(fs.readFileSync(config, { encoding: 'utf8' }), { sections: true, namespaces: true });

  if (config.database) {
    dbCfg = {
      dbName: program.database1,
      dbHost: config.database.path || config.database.host,
      dbPort: config.database.port,
      dbUser: config.database.user,
      dbPass: config.database.pass,
      authDb: config.database.authDb
    };
  }
}

var attr = program.args[0];

var matchAttrs = {};
(program.match || []).forEach(function(attr) {
  matchAttrs[attr] = true;
});

var debug = !!program.verbose;

if (debug && Object.keys(matchAttrs).length) { console.log('match:', program.match); }

//// phase 1: sync version numbers on equal objects if the rest of the object has equal attribute values
//// phase 2: treat all items in collection 2 with equal ids and different objects as parents of collection 1 
function run(db) {
  var coll1 = db.db(program.database1).collection(program.collection1);
  var coll2 = db.db(program.database2).collection(program.collection2);

  if (program.ids || program.oids) {
    var ids = [];
    if (program.ids) {
      program.ids.forEach(function(id) {
        ids.push(id);
      });
    }
    if (program.oids) {
      program.oids.forEach(function(oid) {
        ids.push(new ObjectID(oid));
      });
    }

    var field = {};
    field[attr] = true;
    coll2.find({ _id: { $in: ids } }, field).toArray(function(err, items) {
      if (err) { throw err; }
      async.eachSeries(items, function(item, cb) {
        if (typeof item[attr] === 'undefined') {
          console.log(coll1.db.databaseName, coll1.collectionName, item._id, 'unset', field);
          coll1.update({ _id: item._id }, { $unset: field }, cb);
        } else {
          var setter = {};
          setter[attr] = item[attr];
          console.log(coll1.db.databaseName, coll1.collectionName, item._id, 'set', setter);
          coll1.update({ _id: item._id }, { $set: setter }, cb);
        }
      }, function(err) {
        if (err) { throw err; }
        db.close();
      });
    });
  } else {
    var tmpColl = db.db(program.database1).collection(program.collection1 + '.tmp');

    var opts = {
      matchAttrs: matchAttrs,
      debug: debug
    };

    syncAttr(coll1, coll2, tmpColl, attr, opts, function(err, updated) {
      if (err) { throw err; }
      console.log('updated', updated);
      db.close();
    });
  }
}

// open database
_db(dbCfg, function(err, db) {
  if (err) { throw err; }
  run(db);
});
