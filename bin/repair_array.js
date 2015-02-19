#!/usr/bin/env node

/**
 * Copyright 2015 Netsend.
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

var _db = require('./_db');
var mongodb = require('mongodb');
var async = require('async');
var properties = require('properties');
var fs = require('fs');
var readline = require('readline');
var path = require('path');

var program = require('commander');

program
  .version('0.1.0')
  .usage('[-f config] -d database -c collection')
  .description('check if value occurs in an array and optionally insert')
  .option('-d, --database <database>', 'name of the database')
  .option('-c, --collection <collection>', 'name of the collection')
  .option('-f, --config <config>', 'ini config file with database access credentials')
  .option('-a, --attribute <attribute>', 'name of attribute to lookup (and save) filename to, defaults to photos')
  .option('-s, --save', 'save filename to matching')
  .parse(process.argv);

if (!program.database) { program.help(); }
if (!program.collection) { program.help(); }

var config = {};
var dbCfg = { dbName: program.database };

var lines = 0;
var processed = 0;
var allRead = false;

// if relative, prepend current working dir
if (program.config) {
  config = program.config;
  if (config[0] !== '/') {
    config = process.cwd() + '/' + config;
  }

  config = properties.parse(fs.readFileSync(config, { encoding: 'utf8' }), { sections: true, namespaces: true });

  if (config.database) {
    dbCfg = {
      dbName: program.database,
      dbHost: config.database.path || config.database.host,
      dbPort: config.database.port,
      dbUser: config.database.user,
      dbPass: config.database.pass,
      authDb: config.database.authDb
    };
  }
}

program.attribute = program.attribute || 'photos';

function checkDone(db) {
  if (allRead && lines === processed) {
    console.log('done');
    db.close();
    process.exit(0);
  }
}

function run(db) {
  var coll = db.db(program.database).collection(program.collection);

  var rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false
  });

  rl.on('line', function(filename){
    var basename = path.basename(filename.trim());
    var parts = /tyre_([^_]*)_/.exec(basename);
    if (Array.isArray(parts) && parts.length === 2) {
      lines++;
      var id = parts[1];
      var selector = { _id: id };
      selector[program.attribute] = basename;

      rl.pause();

      if (!program.save) {
        coll.findOne(selector, function(err, found) {
          if (err) { console.log(err); }
          if (!err && !found) {
            console.log(filename);
          }
          processed++;
          rl.resume();
          checkDone(db);
        });
      } else {
        coll.findOne(selector, function(err, found) {
          if (err) { console.log(err); }
          if (!err && !found) {
            var update = { '$push' : {} };
            update['$push'][program.attribute] = basename;

            coll.update({ _id: id}, update, function(err, updates) {
              if (err) { console.log(err); }
              if (updates === 0) {
                console.log('WARNING:', 'update failed for', basename);
              } else {
                console.log('processed', basename);
              }
              processed++;
              rl.resume();
              checkDone(db);
            });
          } else{
            processed++;
            rl.resume();
            checkDone(db);
          }
        });
      }
    }
  });

  rl.on('close', function() {
    allRead = true;
  });

}

// open database
_db(dbCfg, function(err, db) {
  if (err) { throw err; }
  run(db);
});
