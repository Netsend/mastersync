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

var Readable = require('stream').Readable;
var util = require('util');

var async = require('async');
var mongodb = require('mongodb');
var BSON = mongodb.BSON;
var match = require('match-object');

var runHooks = require('./run_hooks');
var walkBranch = require('./walk_branch');

var noop = function() {};

/**
 * VersionedCollectionReader
 *
 * Read the versioned collection reader, optionally starting at a certain offset.
 *
 * @param {Object} snapshotCollection  a mongodb.Collection object
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   localPerspective {String, default _local}  name of the local perspective, remotes can't be named like this.
 *   raw  {Boolean, default false}  whether to emit JavaScript objects or raw Buffers
 *   filter {Object}  conditions a document should hold
 *   offset {String}  version to start tailing after
 *   follow {Boolean, default: true}  whether to keep the tail open or not
 *   tailableRetryInterval {Number, default 2000}  set tailableRetryInterval
 *   hooks {Array}  array of asynchronous functions to execute, each hook has the following signature: db, object, options,
 *                  callback and should callback with an error object, the new item and possibly extra data.
 *   hooksOpts {Object}  options to pass to a hook
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 *
 * @event "data" {Object}  emits one object at a time
 * @event "end"  emitted once the underlying cursor is closed
 *
 * @class represents a VersionedCollectionReader for a certain database.collection
 */
function VersionedCollectionReader(snapshotCollection, opts) {
  /* jshint maxcomplexity: 24 */ /* lot's of parameter checking */

  if (typeof snapshotCollection !== 'object') { throw new TypeError('snapshotCollection must be an object'); }

  if (typeof opts !== 'undefined' && typeof opts !== 'object') { throw new TypeError('opts must be an object'); }
  opts = opts || {};

  if (typeof opts.localPerspective !== 'undefined' && typeof opts.localPerspective !== 'string') { throw new TypeError('opts.localPerspective must be a string'); }
  if (typeof opts.raw !== 'undefined' && typeof opts.raw !== 'boolean') { throw new TypeError('opts.raw must be a boolean'); }
  if (typeof opts.filter !== 'undefined' && typeof opts.filter !== 'object') { throw new TypeError('opts.filter must be an object'); }
  if (typeof opts.offset !== 'undefined' && typeof opts.offset !== 'string') { throw new TypeError('opts.offset must be a string'); }
  if (typeof opts.follow !== 'undefined' && typeof opts.follow !== 'boolean') { throw new TypeError('opts.follow must be a boolean'); }
  if (typeof opts.tailableRetryInterval !== 'undefined' && typeof opts.tailableRetryInterval !== 'number') { throw new TypeError('opts.tailableRetryInterval must be a number'); }
  if (typeof opts.hooks !== 'undefined' && !(opts.hooks instanceof Array)) { throw new TypeError('opts.hooks must be an array'); }
  if (typeof opts.hooksOpts !== 'undefined' && typeof opts.hooksOpts !== 'object') { throw new TypeError('opts.hooksOpts must be an object'); }
  if (typeof opts.log !== 'undefined' && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }

  this._log = opts.log || {
    emerg:   console.error,
    alert:   console.error,
    crit:    console.error,
    err:     console.error,
    warning: console.log,
    notice:  console.log,
    info:    console.log,
    debug:   console.log,
    debug2:  console.log,
    getFileStream: noop,
    getErrorStream: noop,
    close: noop
  };

  this._snapshotCollection = snapshotCollection;

  this._db = snapshotCollection.db;

  this._snapshotCollectionName = snapshotCollection.collectionName;

  this._localPerspective = opts.localPerspective || '_local';
  this._raw = opts.raw || false;
  this._filter = opts.filter || {};
  this._offset = opts.offset || '';

  if (typeof opts.follow === 'boolean') {
    this._follow = opts.follow;
  } else {
    this._follow = true;
  }
  this._hooks = opts.hooks || [];
  this._hooksOpts = opts.hooksOpts || {};

  Readable.call(this, { objectMode: !this._raw });

  var selector = { '_id._pe': this._localPerspective };
  var mongoOpts = { sort: { '$natural': 1 }, tailable: this._follow, comment: 'tail' };
  mongoOpts.tailableRetryInterval = opts.tailableRetryInterval || 2000;

  this._log.notice('vcr selector %j, filter %j, opts %j', selector, this._filter, mongoOpts);

  this._source = this._snapshotCollection.find(selector, mongoOpts).stream();

  var that = this;

  this._maxTries = 0;

  // emit a connected graph by making sure every parent of any filtered item is filtered
  var heads = {};
  var offsetReached = false;
  if (!this._offset) {
    offsetReached = true;
  }
  var i = 0;

  function handleData(item, cb) {
    if (offsetReached) { that._log.debug('vcr _read %j', item._id); }

    // only start emitting after the offset is encountered
    if (!offsetReached) {
      if (item._id._v === that._offset) {
        that._log.info('vcr _read offset reached %j', item._id);
        offsetReached = true;
      } else {
        // the offset should be encountered within maxTries
        i++;
        if (i >= that._maxTries) {
          that._log.err('vcr _read offset not found', that._offset, i, that._maxTries);
          cb(new Error('offset not found'));
          return;
        }
      }
    }

    heads[item._id._v] = [];

    // move previously emitted parents along with this new branch head
    async.eachSeries(item._id._pa, function(p, cb2) {
      if (heads[p]) {
        // ff and takeover parent references from old head
        Array.prototype.push.apply(heads[item._id._v], heads[p]);
        delete heads[p];
        process.nextTick(cb2);
      } else {
        // branched off, find the lowest filtered ancestor of this item and save it as parent reference
        var lastEmitted = {};
        lastEmitted['_id._id'] = item._id._id;
        lastEmitted['_id._pe'] = that._localPerspective;

        walkBranch(lastEmitted, p, that._localPerspective, that._snapshotCollection, function(anItem, cb3) {
          // skip if not all criteria hold on this item
          if (!match(that._filter, anItem)) {
            cb3();
            return;
          }

          // make sure the ancestor is not already in the array, this can happen on merge items.
          if (!heads[item._id._v].some(function(pp) { return pp === anItem._id._v; })) {
            heads[item._id._v].push(anItem._id._v);
          }
          cb3(null, true);
        }, function(err) {
          if (err) {
            that._log.err('vcr could not determine last emitted version', p, lastEmitted, err);
            cb2(err);
            return;
          }
          cb2();
        });
      }
    }, function(err) {
      if (err) { cb(err); return; }

      // don't emit if not all criteria hold on this item
      if (!match(that._filter, item)) { cb(); return; }

      // load all hooks on this item, then, if offset is reached, callback
      runHooks(that._hooks, that._db, item, that._hooksOpts, function(err, afterItem) {
        if (err) { cb(err); return; }

        // return if hooks filter out the item
        if (!afterItem) {
          if (offsetReached) { that._log.info('vcr hook filtered %j', item._id); }
          cb();
          return;
        }

        // set parents to last returned version of this branch
        item._id._pa = heads[item._id._v];

        // then update branch with _id of this item
        heads[item._id._v] = [item._id._v];

        // all criteria hold, so return this item if offset is reached
        if (offsetReached) {
          item = afterItem;

          // remove perspective and local state and run any transformation
          delete item._id._pe;
          delete item._id._lo;
          delete item._id._i;
          delete item._m3._op;
          delete item._m3._ack;

          // push the raw or parsed item out to the reader, and resume if not flooded
          that._log.info('vcr push %j', item._id);
          var proceed = that.push(that._raw ? BSON.serialize(item) : item);
          if (!proceed) { that._queue.pause(); }
        }
        cb();
      });
    });
  }

  this._queue = async.queue(handleData, 1);

  this._queue.saturated = function() {
    that._source.pause();
  };

  this._queue.empty = function() {
    that._source.resume();
  };

  // pause queue to determine maxTries
  this._queue.pause();

  // determine the maximum number of versions to examine before the offset must have been encountered
  this._snapshotCollection.count(function(err, count) {
    if (err) { throw err; }
    that._maxTries = count;
    that._queue.resume();
  });

  this._source.on('data', function(item) {
    that._queue.push(item, function(err) {
      if (err) {
        that._queue.kill();
        that._source.destroy();
        that.emit('error', err);
      }
    });
  });

  // proxy error
  this._source.on('error', function(err) {
    that._log.crit('vcr cursor stream error %s', err);
    that._queue.kill();
    that.emit('error', err);
  });

  this._source.on('close', function() {
    that._log.notice('vcr cursor stream closed');

    // end this stream as soon as the queue is completely processed
    if (that._queue.idle()) {
      that.push(null);
    } else {
      that._queue.drain = function() {
        that.push(null);
      };
    }
  });
}
util.inherits(VersionedCollectionReader, Readable);

module.exports = VersionedCollectionReader;

/**
 * Stop the stream reader. An "end" event will be emitted.
 */
VersionedCollectionReader.prototype.close = function close() {
  this._log.info('vcr closing cursor');
  this._queue.kill();
  this._source.destroy();
};


/////////////////////
//// PRIVATE API ////
/////////////////////

/**
 * Implementation of _read method of Readable stream. This method is not called
 * directly. In this implementation size of read buffer is ignored
 */
VersionedCollectionReader.prototype._read = function() {
  this._queue.resume();
};
